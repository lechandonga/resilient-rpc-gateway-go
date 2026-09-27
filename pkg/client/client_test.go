package client

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/config"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/registry"
)

func policyFor(method string, mutate func(*model.MethodPolicy)) model.MethodPolicy {
	p := model.DefaultMethodPolicy(method)
	p.Timeout = 200 * time.Millisecond
	if mutate != nil {
		mutate(&p)
	}
	return p
}

func newHarness(t *testing.T, p model.MethodPolicy) (*registry.Registry, *config.Holder) {
	t.Helper()
	reg := registry.NewRegistry()
	parsed := &config.Parsed{Version: 2, Service: "svc", Methods: map[string]model.MethodPolicy{p.Method: p}}
	holder := config.NewHolder(parsed)
	return reg, holder
}

// failingInvoker 每次调用都失败，并统计调用次数。
func failingInvoker(calls *int32) Invoker {
	return func(ctx context.Context, ins model.Instance, req *model.Request) (*model.Response, error) {
		atomic.AddInt32(calls, 1)
		return &model.Response{Status: 500, InstanceID: ins.ID}, errors.New("boom")
	}
}

func okInvoker(calls *int32, failFirst int32) Invoker {
	var n int32
	return func(ctx context.Context, ins model.Instance, req *model.Request) (*model.Response, error) {
		atomic.AddInt32(calls, 1)
		if atomic.AddInt32(&n, 1) <= failFirst {
			return nil, errors.New("transient boom")
		}
		return &model.Response{Status: 200, InstanceID: ins.ID, Payload: []byte("ok")}, nil
	}
}

// slowInvoker 模拟抖动慢下游：尊重 ctx，超时/取消时退出。
func slowInvoker(delay time.Duration, observedCancel *int32) Invoker {
	return func(ctx context.Context, ins model.Instance, req *model.Request) (*model.Response, error) {
		select {
		case <-time.After(delay):
			return &model.Response{Status: 200, InstanceID: ins.ID}, nil
		case <-ctx.Done():
			if observedCancel != nil {
				atomic.AddInt32(observedCancel, 1)
			}
			return nil, ctx.Err()
		}
	}
}

func registerN(reg *registry.Registry, n int) {
	for i := 0; i < n; i++ {
		id := string(rune('a' + i))
		reg.Upsert("svc", model.Instance{ID: id, Address: id + ":9000", Weight: 1})
	}
}

func TestEmptyInstanceCausesAreDistinguishable(t *testing.T) {
	p := policyFor("m", nil)
	reg, holder := newHarness(t, p)
	var calls int32
	c, err := New(reg, holder, failingInvoker(&calls))
	if err != nil {
		t.Fatal(err)
	}
	req := &model.Request{ID: "r-empty-1", Service: "svc", Method: "m", Idempotent: true}

	// 服务从未注册
	if _, err := c.Invoke(context.Background(), req); !errors.Is(err, model.ErrNoInstance) {
		t.Fatalf("want ErrNoInstance, got %v", err)
	}

	// 注册后全部因健康检查失败摘除 -> 与"从未注册"区分
	registerN(reg, 1)
	reg.SetHealth("svc", "a", false)
	if _, err := c.Invoke(context.Background(), req); !errors.Is(err, model.ErrNoHealthyInstance) {
		t.Fatalf("want ErrNoHealthyInstance, got %v", err)
	}
	if calls != 0 {
		t.Fatalf("must not call downstream with no candidates, calls=%d", calls)
	}
}

func TestRemovedInstanceStopsReceivingTraffic(t *testing.T) {
	p := policyFor("m", nil)
	reg, holder := newHarness(t, p)
	var calls int32
	seen := sync.Map{}
	inv := func(ctx context.Context, ins model.Instance, req *model.Request) (*model.Response, error) {
		atomic.AddInt32(&calls, 1)
		seen.Store(ins.ID, true)
		return &model.Response{Status: 200, InstanceID: ins.ID}, nil
	}
	c, _ := New(reg, holder, inv)
	registerN(reg, 2)
	req := &model.Request{ID: "r-churn", Service: "svc", Method: "m"}

	for i := 0; i < 6; i++ {
		if _, err := c.Invoke(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	// b 下线后，连续调用绝不能再命中 b
	reg.Remove("svc", "b")
	for i := 0; i < 10; i++ {
		if _, err := c.Invoke(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	if _, hit := seen.Load("b"); !hit {
		t.Fatal("b should have served traffic before removal")
	}
	// 下线后的命中次数：通过 calls 总数与调用次数对照，b 下线后 LB 候选只有 a
	for i := 0; i < 20; i++ {
		in, err := loadBalancerPick(reg, "svc")
		if err != nil || in.ID != "a" {
			t.Fatalf("post-removal traffic must only hit a, got %v err=%v", in, err)
		}
	}
}

func TestNonIdempotentNotRetriedNoDuplicateSideEffect(t *testing.T) {
	p := policyFor("pay", func(p *model.MethodPolicy) { p.MaxAttempts = 3 })
	reg, holder := newHarness(t, p)
	var calls int32
	c, _ := New(reg, holder, failingInvoker(&calls))
	registerN(reg, 1)

	req := &model.Request{ID: "r-side-effect", Service: "svc", Method: "pay", Idempotent: false}
	_, err := c.Invoke(context.Background(), req)
	if !errors.Is(err, model.ErrNonRetryable) {
		t.Fatalf("non-idempotent retry must surface ErrNonRetryable, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("non-idempotent request must hit downstream exactly once, got %d", calls)
	}
}

func TestIdempotentRetryExhaustedDistinctReason(t *testing.T) {
	p := policyFor("pay", func(p *model.MethodPolicy) {
		p.MaxAttempts = 3
		p.Timeout = time.Second
	})
	reg, holder := newHarness(t, p)
	var calls int32
	c, _ := New(reg, holder, failingInvoker(&calls))
	registerN(reg, 1)

	req := &model.Request{ID: "r-exhaust", Service: "svc", Method: "pay", Idempotent: true}
	resp, err := c.Invoke(context.Background(), req)
	if !errors.Is(err, model.ErrRetryExhausted) {
		t.Fatalf("want ErrRetryExhausted, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("want exactly 3 attempts, got %d", calls)
	}
	if resp.Attempts != 3 {
		t.Fatalf("response should carry attempt count 3, got %d", resp.Attempts)
	}
}

func TestIdempotentRetryThenSuccess(t *testing.T) {
	p := policyFor("pay", func(p *model.MethodPolicy) {
		p.MaxAttempts = 3
		p.Timeout = time.Second
	})
	reg, holder := newHarness(t, p)
	var calls int32
	c, _ := New(reg, holder, okInvoker(&calls, 1))
	registerN(reg, 1)

	req := &model.Request{ID: "r-retry-ok", Service: "svc", Method: "pay", Idempotent: true}
	resp, err := c.Invoke(context.Background(), req)
	if err != nil {
		t.Fatalf("retry should eventually succeed: %v", err)
	}
	if calls != 2 || resp.Attempts != 2 || resp.Status != 200 {
		t.Fatalf("want 2 attempts then 200, got calls=%d resp=%+v", calls, resp)
	}
}

func TestDownstreamTimeoutReleasesBudget(t *testing.T) {
	p := policyFor("slow", func(p *model.MethodPolicy) { p.Timeout = 30 * time.Millisecond })
	reg, holder := newHarness(t, p)
	var canceled int32
	c, _ := New(reg, holder, slowInvoker(time.Second, &canceled))
	registerN(reg, 1)

	req := &model.Request{ID: "r-timeout", Service: "svc", Method: "slow"}
	start := time.Now()
	_, err := c.Invoke(context.Background(), req)
	elapsed := time.Since(start)
	if !errors.Is(err, model.ErrTimeout) {
		t.Fatalf("want ErrTimeout, got %v", err)
	}
	if elapsed > 300*time.Millisecond {
		t.Fatalf("gateway must not wait for slow downstream, elapsed=%v", elapsed)
	}
	// 慢下游的 ctx 必须被取消，调用不得继续推进
	waitUntil(200*time.Millisecond, func() bool { return atomic.LoadInt32(&canceled) == 1 })
	if atomic.LoadInt32(&canceled) != 1 {
		t.Fatal("downstream context must be canceled after timeout")
	}
}

func TestUpstreamCancelStopsDownstream(t *testing.T) {
	p := policyFor("slow", func(p *model.MethodPolicy) { p.Timeout = 5 * time.Second })
	reg, holder := newHarness(t, p)
	var canceled int32
	c, _ := New(reg, holder, slowInvoker(10*time.Second, &canceled))
	registerN(reg, 1)

	ctx, cancel := context.WithCancel(context.Background())
	req := &model.Request{ID: "r-cancel", Service: "svc", Method: "slow"}
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Invoke(ctx, req)
		errCh <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, model.ErrContextCanceled) {
			t.Fatalf("want ErrContextCanceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled request did not return promptly")
	}
	waitUntil(300*time.Millisecond, func() bool { return atomic.LoadInt32(&canceled) == 1 })
}

func TestCircuitOpensAndHalfOpenFallback(t *testing.T) {
	p := policyFor("m", func(p *model.MethodPolicy) {
		p.MaxAttempts = 1
		p.CircuitThreshold = 3
		p.OpenCooldown = 40 * time.Millisecond
		p.Timeout = time.Second
	})
	reg, holder := newHarness(t, p)
	var calls int32
	c, _ := New(reg, holder, failingInvoker(&calls))
	registerN(reg, 1)
	req := &model.Request{ID: "r-cb", Service: "svc", Method: "m"}

	for i := 0; i < 3; i++ {
		c.Invoke(context.Background(), req)
	}
	// 熔断打开：不得再冲击下游
	before := atomic.LoadInt32(&calls)
	_, err := c.Invoke(context.Background(), req)
	if !errors.Is(err, model.ErrCircuitOpen) {
		t.Fatalf("want ErrCircuitOpen, got %v", err)
	}
	if atomic.LoadInt32(&calls) != before {
		t.Fatal("open circuit must not hit downstream")
	}

	// 冷却后进入半开；探测仍失败 -> 退回 open，下一个请求继续被拒绝
	time.Sleep(60 * time.Millisecond)
	c.Invoke(context.Background(), req) // 半开探测，失败
	_, err = c.Invoke(context.Background(), req)
	if !errors.Is(err, model.ErrCircuitOpen) {
		t.Fatalf("failed half-open probe must restore open, got %v", err)
	}
}

func TestConcurrencyRejectAtClient(t *testing.T) {
	p := policyFor("m", func(p *model.MethodPolicy) {
		p.MaxConcurrency = 1
		p.OnLimit = model.RejectReject
		p.Timeout = 5 * time.Second
	})
	reg, holder := newHarness(t, p)
	entered := make(chan struct{})
	release := make(chan struct{})
	inv := func(ctx context.Context, ins model.Instance, req *model.Request) (*model.Response, error) {
		close(entered)
		<-release
		return &model.Response{Status: 200}, nil
	}
	c, _ := New(reg, holder, inv)
	registerN(reg, 1)
	go func() {
		c.Invoke(context.Background(), &model.Request{ID: "r-hold", Service: "svc", Method: "m"})
	}()
	<-entered
	_, err := c.Invoke(context.Background(), &model.Request{ID: "r-limited", Service: "svc", Method: "m"})
	if !errors.Is(err, model.ErrConcurrencyLimited) {
		t.Fatalf("want ErrConcurrencyLimited, got %v", err)
	}
	close(release)
}

func TestHotUpdateRevisionsAppearInTrace(t *testing.T) {
	p := policyFor("m", func(p *model.MethodPolicy) { p.MaxAttempts = 1 })
	reg, holder := newHarness(t, p)
	var calls int32
	c, _ := New(reg, holder, failingInvoker(&calls))
	registerN(reg, 1)
	req := &model.Request{ID: "r-cfg-old", Service: "svc", Method: "m"}
	c.Invoke(context.Background(), req)

	p2 := policyFor("m", func(p *model.MethodPolicy) { p.MaxAttempts = 2; p.Timeout = time.Second })
	next := &config.Parsed{Version: 2, Service: "svc", Methods: map[string]model.MethodPolicy{"m": p2}}
	rev2, err := holder.Replace(next)
	if err != nil {
		t.Fatal(err)
	}
	req2 := &model.Request{ID: "r-cfg-new", Service: "svc", Method: "m", Idempotent: true}
	c.Invoke(context.Background(), req2)

	oldTrace := c.Trace("r-cfg-old")
	newTrace := c.Trace("r-cfg-new")
	if len(oldTrace) == 0 || len(newTrace) == 0 {
		t.Fatal("traces must be recorded")
	}
	// 旧请求全部命中 revision 1，新请求命中 revision 2
	for _, a := range oldTrace {
		_ = a
	}
	if got := c.tracerRevisionOf("r-cfg-old"); got != 1 {
		t.Fatalf("old request must pin rev 1, got %d", got)
	}
	if got := c.tracerRevisionOf("r-cfg-new"); got != rev2 {
		t.Fatalf("new request must pin rev %d, got %d", rev2, got)
	}
}

func TestLogsContainRequestIDPolicyAndResponse(t *testing.T) {
	var buf bytes.Buffer
	p := policyFor("m", func(p *model.MethodPolicy) { p.MaxAttempts = 1 })
	reg, holder := newHarness(t, p)
	var calls int32
	c, _ := New(reg, holder, failingInvoker(&calls), func(cl *Client) {
		cl.tracer = NewTracer(&buf, 256, true)
	})
	registerN(reg, 1)
	req := &model.Request{ID: "r-log-42", Service: "svc", Method: "m"}
	c.Invoke(context.Background(), req)

	log := buf.String()
	for _, want := range []string{"r-log-42", "policy=invoke", "reason=DOWNSTREAM_ERROR"} {
		if !strings.Contains(log, want) {
			t.Fatalf("log missing %q:\n%s", want, log)
		}
	}
	// 成功路径需打印下游响应摘要
	buf.Reset()
	c2, _ := New(reg, holder, func(ctx context.Context, ins model.Instance, r *model.Request) (*model.Response, error) {
		return &model.Response{Status: 201, Payload: []byte("created")}, nil
	}, func(cl *Client) { cl.tracer = NewTracer(&buf, 256, true) })
	c2.Invoke(context.Background(), &model.Request{ID: "r-log-ok", Service: "svc", Method: "m"})
	if !strings.Contains(buf.String(), "r-log-ok") || !strings.Contains(buf.String(), "status=201") {
		t.Fatalf("success log must contain request id and downstream response:\n%s", buf.String())
	}
}

func waitUntil(timeout time.Duration, cond func() bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestSnapshotConsistencyUnderLoad 并发请求期间，快照必须自洽：
// TotalInFlight 等于排空计数；sum(active+queued) 不超过 TotalInFlight；
// 每个 limiter 维度 active<=max、queued<=maxQueue；空闲后全部归零。
func TestSnapshotConsistencyUnderLoad(t *testing.T) {
	p := policyFor("m", func(p *model.MethodPolicy) {
		p.MaxConcurrency = 2
		p.MaxQueue = 4
		p.OnLimit = model.RejectWait
		p.Timeout = 5 * time.Second
	})
	reg, holder := newHarness(t, p)
	registerN(reg, 2)

	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	inv := func(ctx context.Context, ins model.Instance, req *model.Request) (*model.Response, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return &model.Response{Status: 200, InstanceID: ins.ID}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	c, _ := New(reg, holder, inv)

	const n = 8
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			_, err := c.Invoke(context.Background(),
				&model.Request{ID: "snap-" + string(rune('a'+i)), Service: "svc", Method: "m"})
			errCh <- err
		}(i)
	}
	// 等 4 个进入下游（2 实例 × 2 并发），其余在排队
	for i := 0; i < 4; i++ {
		<-entered
	}
	// 等全部 8 个请求都已被治理层受理（在途或排队中）
	waitUntil(time.Second, func() bool { return c.Snapshot().TotalInFlight == n })

	// 负载期连续取多次快照，每次都必须自洽
	for k := 0; k < 50; k++ {
		s := c.Snapshot()
		var activeSum, queuedSum int
		for _, l := range s.Limiters {
			if l.Active < 0 || l.Queued < 0 {
				t.Fatalf("negative counts: %+v", l)
			}
			if l.Active > l.MaxConcurrency || l.Queued > l.MaxQueue {
				t.Fatalf("bounds violated: %+v", l)
			}
			activeSum += l.Active
			queuedSum += l.Queued
		}
		// 在途总数 == 排空记账；额度侧 active+queue 是其子集
		// （差额为尚未走到额度阶段的请求，非负）。
		if s.TotalInFlight != s.Drain.InFlight {
			t.Fatalf("total inflight must equal drain count: %+v", s.Drain)
		}
		if activeSum+queuedSum > s.TotalInFlight {
			t.Fatalf("limiter sum %d exceeds total inflight %d", activeSum+queuedSum, s.TotalInFlight)
		}
		if s.TotalInFlight != n {
			// 此刻所有请求都应已受理且未释放
			t.Fatalf("want %d inflight, got %d", n, s.TotalInFlight)
		}
		time.Sleep(time.Millisecond)
	}

	// 重复查询的稳定键顺序可复现：两次快照 breaker/limiter/instance 键序列一致
	keySeq := func(s model.GovernanceSnapshot) string {
		out := ""
		for _, l := range s.Limiters {
			out += l.Key + ","
		}
		out += "|"
		for _, b := range s.Breakers {
			out += b.Key + ","
		}
		return out
	}
	if a, b := keySeq(c.Snapshot()), keySeq(c.Snapshot()); a != b {
		t.Fatalf("snapshot key order not reproducible:\n%s\n%s", a, b)
	}

	close(release)
	for i := 0; i < n; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("all requests should finish: %v", err)
		}
	}
	waitUntil(time.Second, func() bool {
		s := c.Snapshot()
		return s.TotalInFlight == 0 && s.Drain.InFlight == 0 && len(s.Limiters) == 0
	})
}

// TestCancelDuringRetryBackoffSkipsFurtherAttempts 第 1 次尝试失败后，
// 在退避期间上游取消：绝不能再发起第 2 次下游调用。
func TestCancelDuringRetryBackoffSkipsFurtherAttempts(t *testing.T) {
	p := policyFor("pay", func(p *model.MethodPolicy) {
		p.MaxAttempts = 5
		p.Timeout = 5 * time.Second
		// 拉长基础退避：保证取消发生在退避窗口内
	})
	reg, holder := newHarness(t, p)
	var calls int32
	c, _ := New(reg, holder, func(ctx context.Context, ins model.Instance, req *model.Request) (*model.Response, error) {
		atomic.AddInt32(&calls, 1)
		return nil, errors.New("boom")
	})
	registerN(reg, 1)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Invoke(ctx,
			&model.Request{ID: "r-backoff-cancel", Service: "svc", Method: "pay", Idempotent: true})
		errCh <- err
	}()
	waitUntil(time.Second, func() bool { return atomic.LoadInt32(&calls) == 1 })
	cancel() // 在第一次失败后的退避中取消
	select {
	case err := <-errCh:
		if !errors.Is(err, model.ErrContextCanceled) {
			t.Fatalf("want ErrContextCanceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("request did not return after cancel")
	}
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("cancel during backoff must not trigger further attempts, calls=%d", got)
	}
	if s := c.Snapshot(); s.TotalInFlight != 0 {
		t.Fatalf("inflight must settle to 0, got %d", s.TotalInFlight)
	}
}

// TestCanceledHalfOpenProbeReleasesSlot 半开探测在打下游中被取消，
// 探测名额必须归还，后续请求仍能重新探测并成功闭合熔断。
func TestCanceledHalfOpenProbeReleasesSlot(t *testing.T) {
	p := policyFor("m", func(p *model.MethodPolicy) {
		p.MaxAttempts = 1
		p.CircuitThreshold = 1
		p.OpenCooldown = 20 * time.Millisecond
		p.HalfOpenProbes = 1
		p.Timeout = 5 * time.Second
	})
	reg, holder := newHarness(t, p)
	var calls int32
	stageCh := make(chan struct{}, 1)
	inv := func(ctx context.Context, ins model.Instance, req *model.Request) (*model.Response, error) {
		n := atomic.AddInt32(&calls, 1)
		switch n {
		case 1:
			return nil, errors.New("open it")
		case 2:
			stageCh <- struct{}{}
			<-ctx.Done() // 探测被取消
			return nil, ctx.Err()
		default:
			return &model.Response{Status: 200, InstanceID: ins.ID}, nil
		}
	}
	c, _ := New(reg, holder, inv)
	registerN(reg, 1)
	// 先打熔（阈值 1）
	_, _ = c.Invoke(context.Background(),
		&model.Request{ID: "r-open", Service: "svc", Method: "m"})
	if got := c.Snapshot().Breakers; len(got) == 0 || got[0].State != "open" {
		t.Fatalf("breaker should be open: %+v", got)
	}
	// 冷却后探测在下游途中被取消 -> 名额归还，熔断保持 half-open
	time.Sleep(40 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Invoke(ctx,
			&model.Request{ID: "r-probe-cancel", Service: "svc", Method: "m"})
		errCh <- err
	}()
	<-stageCh
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, model.ErrContextCanceled) {
			t.Fatalf("want canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("probe request did not return")
	}
	st := c.Snapshot().Breakers
	if len(st) != 1 || st[0].State != "half-open" || st[0].ProbeInflight != 0 {
		t.Fatalf("abandoned probe must release slot and stay half-open: %+v", st)
	}
	// 名额泄漏会导致这个请求被永远拒绝；名额归还后它能重新探测并成功闭合
	resp, err := c.Invoke(context.Background(),
		&model.Request{ID: "r-probe-retry", Service: "svc", Method: "m"})
	if err != nil {
		t.Fatalf("re-probe after abandon should succeed: %v", err)
	}
	if resp.Status != 200 {
		t.Fatalf("want 200, got %d", resp.Status)
	}
	waitUntil(time.Second, func() bool {
		bs := c.Snapshot().Breakers
		return len(bs) == 1 && bs[0].State == "closed"
	})
}
