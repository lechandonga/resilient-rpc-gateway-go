package integration_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/client"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/config"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/registry"
)

const cancelDrainConfig = `{
  "version": 2,
  "service": {
    "name": "shop",
    "methods": {
      "charge": {"timeout":"5s","max_attempts":4},
      "queue":  {"timeout":"5s","max_attempts":1,"max_concurrency":1,"on_limit":"wait","max_queue":4},
      "slow":   {"timeout":"5s","max_attempts":1}
    }
  }
}`

func newCDHarness(t *testing.T, invoker client.Invoker) (*client.Client, *registry.Registry) {
	t.Helper()
	parsed, err := config.Parse([]byte(cancelDrainConfig))
	if err != nil {
		t.Fatal(err)
	}
	holder := config.NewHolder(parsed)
	reg := registry.NewRegistry()
	reg.Upsert("shop",
		model.Instance{ID: "a", Address: "a:1", Weight: 1},
		model.Instance{ID: "b", Address: "b:1", Weight: 1},
	)
	c, err := client.New(reg, holder, invoker)
	if err != nil {
		t.Fatal(err)
	}
	return c, reg
}

// newCDHarnessSingle 只注册一个实例，便于构造"额度被占满 -> 后续请求必排队"的确定性场景。
func newCDHarnessSingle(t *testing.T, invoker client.Invoker) (*client.Client, *registry.Registry) {
	c, reg := newCDHarness(t, invoker)
	reg.Remove("shop", "b")
	return c, reg
}

// waitForStat 轮询快照直到 pred 成立。
func waitForStat(t *testing.T, c *client.Client, timeout time.Duration, pred func(model.GovernanceSnapshot) bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred(c.Snapshot()) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("snapshot condition not met before timeout")
}

// TestCanceledBeforeReachingDownstream 请求进入时已被上游取消：
// 绝不调用下游、绝不重试、计数不被占用。
func TestCanceledBeforeReachingDownstream(t *testing.T) {
	var calls int32
	invoker := func(ctx context.Context, ins model.Instance, req *model.Request) (*model.Response, error) {
		atomic.AddInt32(&calls, 1)
		return &model.Response{Status: 200}, nil
	}
	c, _ := newCDHarness(t, invoker)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 还没进来就已取消
	_, err := c.Invoke(ctx, &model.Request{
		ID: "pre-cancel", Service: "shop", Method: "charge", Idempotent: true,
	})
	if !errors.Is(err, model.ErrContextCanceled) {
		t.Fatalf("want ErrContextCanceled, got %v", err)
	}
	if calls != 0 {
		t.Fatalf("pre-canceled request must never reach downstream, calls=%d", calls)
	}

	// 已过期的截止时间同理：GATEWAY_TIMEOUT，且不打下游
	expired, ecancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer ecancel()
	_, err = c.Invoke(expired, &model.Request{
		ID: "pre-deadline", Service: "shop", Method: "charge", Idempotent: true,
	})
	if !errors.Is(err, model.ErrTimeout) {
		t.Fatalf("want ErrTimeout, got %v", err)
	}
	if calls != 0 {
		t.Fatalf("pre-expired request must never reach downstream, calls=%d", calls)
	}
	// 治理层在途计数必须回落为 0
	if s := c.Snapshot(); s.TotalInFlight != 0 || s.Drain.InFlight != 0 {
		t.Fatalf("counts must settle to zero: %+v", s)
	}
}

// TestCanceledWhileQueued 排队等待额度期间被取消：
// 退出排队、不发起下游调用，排队计数与在途总数正常回落，额度可被后续请求复用。
func TestCanceledWhileQueued(t *testing.T) {
	var calls int32
	entered := make(chan struct{})
	release := make(chan struct{})
	secondEntered := make(chan struct{}, 1)
	invoker := func(ctx context.Context, ins model.Instance, req *model.Request) (*model.Response, error) {
		if req.ID == "holder" {
			close(entered)
			<-release
			return &model.Response{Status: 200}, nil
		}
		select {
		case secondEntered <- struct{}{}:
		default:
		}
		atomic.AddInt32(&calls, 1)
		return &model.Response{Status: 200}, nil
	}
	c, _ := newCDHarnessSingle(t, invoker)

	holderErr := make(chan error, 1)
	go func() {
		_, err := c.Invoke(context.Background(),
			&model.Request{ID: "holder", Service: "shop", Method: "queue"})
		holderErr <- err
	}()
	<-entered

	// 第二个请求排队
	qctx, qcancel := context.WithCancel(context.Background())
	qErr := make(chan error, 1)
	go func() {
		_, err := c.Invoke(qctx,
			&model.Request{ID: "queued", Service: "shop", Method: "queue"})
		qErr <- err
	}()

	// 等到快照里确实出现 1 active + 1 queued
	waitForStat(t, c, time.Second, func(s model.GovernanceSnapshot) bool {
		var active, queued int
		for _, l := range s.Limiters {
			if l.Key == "a#queue" || l.Key == "b#queue" {
				active += l.Active
				queued += l.Queued
			}
		}
		return active == 1 && queued == 1 && s.TotalInFlight == 2
	})

	qcancel()
	select {
	case err := <-qErr:
		if !errors.Is(err, model.ErrContextCanceled) {
			t.Fatalf("queued cancel want ErrContextCanceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued request did not return after cancel")
	}
	// 排队请求绝不能发起下游调用
	select {
	case <-secondEntered:
		t.Fatal("queued request must never reach downstream after cancel")
	default:
	}
	// 排队计数与在途总数回落到仅持有者
	waitForStat(t, c, time.Second, func(s model.GovernanceSnapshot) bool {
		return s.TotalInFlight == 1 && s.Drain.InFlight == 1
	})

	// 释放持有者：额度回收，全部计数归零，维度可被复用
	close(release)
	select {
	case err := <-holderErr:
		if err != nil {
			t.Fatalf("holder should succeed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("holder did not finish")
	}
	waitForStat(t, c, time.Second, func(s model.GovernanceSnapshot) bool {
		return s.TotalInFlight == 0 && s.Drain.InFlight == 0 && len(s.Limiters) == 0
	})
	// 再发一个请求应立即成功，证明取消没有吃掉/泄漏额度
	_, err := c.Invoke(context.Background(),
		&model.Request{ID: "after", Service: "shop", Method: "queue"})
	if err != nil {
		t.Fatalf("quota must be reusable after queued cancel: %v", err)
	}
	// 释放"after"自身
	waitForStat(t, c, time.Second, func(s model.GovernanceSnapshot) bool { return s.TotalInFlight == 0 })
}

// TestDrainRejectsNewRequests 排空中新请求被明确拒绝（GATEWAY_DRAINING），
// 不产生下游调用；排空结束后进程无在途。
func TestDrainRejectsNewRequests(t *testing.T) {
	var calls int32
	invoker := func(ctx context.Context, ins model.Instance, req *model.Request) (*model.Response, error) {
		atomic.AddInt32(&calls, 1)
		return &model.Response{Status: 200}, nil
	}
	c, _ := newCDHarness(t, invoker)

	s := c.Shutdown(time.Hour) // 无在途，立即干净完成
	if s.Phase != model.DrainComplete || s.TimedOut {
		t.Fatalf("empty shutdown must complete cleanly: %+v", s)
	}
	_, err := c.Invoke(context.Background(),
		&model.Request{ID: "during-drain", Service: "shop", Method: "charge"})
	if !errors.Is(err, model.ErrDraining) {
		t.Fatalf("new request during drain want ErrDraining, got %v", err)
	}
	if calls != 0 {
		t.Fatalf("drain rejection must not reach downstream, calls=%d", calls)
	}
	if s := c.Snapshot(); s.TotalInFlight != 0 || s.Drain.Phase != model.DrainComplete {
		t.Fatalf("drained client must have no in-flight: %+v", s)
	}
}

// TestDrainGraceTimeoutFailsInflight 排空宽限到期：在途请求按
// GATEWAY_DRAIN_TIMEOUT 快速失败，下游 ctx 被取消，计数全部回落。
func TestDrainGraceTimeoutFailsInflight(t *testing.T) {
	var entered int32
	var downstreamCanceled int32
	release := make(chan struct{})
	invoker := func(ctx context.Context, ins model.Instance, req *model.Request) (*model.Response, error) {
		atomic.AddInt32(&entered, 1)
		select {
		case <-release:
			return &model.Response{Status: 200}, nil
		case <-ctx.Done():
			atomic.AddInt32(&downstreamCanceled, 1)
			return nil, ctx.Err()
		}
	}
	c, _ := newCDHarness(t, invoker)
	invokeErr := make(chan error, 1)
	go func() {
		_, err := c.Invoke(context.Background(),
			&model.Request{ID: "inflight", Service: "shop", Method: "slow"})
		invokeErr <- err
	}()
	waitForStat(t, c, time.Second, func(s model.GovernanceSnapshot) bool { return s.TotalInFlight == 1 })

	// 宽限 40ms，下游一直不返回 -> 排空等待超时，强制收尾
	start := time.Now()
	s := c.Shutdown(40 * time.Millisecond)
	elapsed := time.Since(start)
	if !s.TimedOut || s.Phase != model.DrainComplete {
		t.Fatalf("want timed-out drain, got %+v", s)
	}
	if elapsed < 30*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Fatalf("shutdown should wait ~grace, elapsed=%v", elapsed)
	}
	select {
	case err := <-invokeErr:
		if !errors.Is(err, model.ErrDrainTimeout) {
			t.Fatalf("inflight request want ErrDrainTimeout, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("inflight request did not fail after grace")
	}
	// 下游 goroutine 观察到取消是异步的：等待其真正退出，再核对次数，
	// 确认 ctx 被精确取消一次（而非通过 release 通道返回成功）。
	deadline := time.Now().Add(time.Second)
	for atomic.LoadInt32(&downstreamCanceled) != 1 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if atomic.LoadInt32(&downstreamCanceled) != 1 {
		t.Fatalf("downstream ctx must be canceled exactly once, got %d", atomic.LoadInt32(&downstreamCanceled))
	}
	// 排空结束后不得还有调用在继续：计数归零
	waitForStat(t, c, time.Second, func(sn model.GovernanceSnapshot) bool {
		return sn.TotalInFlight == 0 && sn.Drain.InFlight == 0
	})
	close(release)
	if atomic.LoadInt32(&entered) != 1 {
		t.Fatalf("exactly one downstream call expected, got %d", atomic.LoadInt32(&entered))
	}
}

// TestDrainLetsInflightFinishWithinBudget 在途请求在宽限内跑完：干净完成。
func TestDrainLetsInflightFinishWithinBudget(t *testing.T) {
	invoker := func(ctx context.Context, ins model.Instance, req *model.Request) (*model.Response, error) {
		select {
		case <-time.After(20 * time.Millisecond):
			return &model.Response{Status: 200}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	c, _ := newCDHarness(t, invoker)
	type result struct {
		err error
	}
	resCh := make(chan result, 1)
	go func() {
		_, err := c.Invoke(context.Background(),
			&model.Request{ID: "grace-finish", Service: "shop", Method: "slow"})
		resCh <- result{err}
	}()
	waitForStat(t, c, time.Second, func(s model.GovernanceSnapshot) bool { return s.TotalInFlight == 1 })
	s := c.Shutdown(time.Hour)
	if s.TimedOut || s.InFlight != 0 {
		t.Fatalf("inflight finishing within grace must complete cleanly: %+v", s)
	}
	r := <-resCh
	if r.err != nil {
		t.Fatalf("inflight within grace should succeed, got %v", r.err)
	}
}

// TestInstanceDrainStopsNewTrafficKeepsInflight 实例排空：
// 新流量不再选中被摘实例，在途调用不被硬切断；跑完后可彻底摘除。
func TestInstanceDrainStopsNewTrafficKeepsInflight(t *testing.T) {
	var callsA, callsB int32
	entered := make(chan struct{})
	release := make(chan struct{})
	invoker := func(ctx context.Context, ins model.Instance, req *model.Request) (*model.Response, error) {
		if req.ID == "b-inflight" {
			close(entered)
			<-release // 在 b 排空期间继续在途
			return &model.Response{Status: 200, InstanceID: "b"}, nil
		}
		if ins.ID == "a" {
			atomic.AddInt32(&callsA, 1)
		} else {
			atomic.AddInt32(&callsB, 1)
		}
		return &model.Response{Status: 200, InstanceID: ins.ID}, nil
	}
	c, reg := newCDHarness(t, invoker)

	inflightErr := make(chan error, 1)
	go func() {
		_, err := c.Invoke(context.Background(),
			&model.Request{ID: "b-inflight", Service: "shop", Method: "charge"})
		inflightErr <- err
	}()
	<-entered

	// 排空 b：在途不被切断，新请求全部只打 a
	c.DrainInstance("shop", "b")
	for i := 0; i < 10; i++ {
		resp, err := c.Invoke(context.Background(),
			&model.Request{ID: "new-after-drain", Service: "shop", Method: "charge"})
		if err != nil {
			t.Fatalf("healthy instance must serve: %v", err)
		}
		if resp.InstanceID != "a" {
			t.Fatalf("new traffic must not hit draining instance, got %s", resp.InstanceID)
		}
	}
	// 在途 b 调用仍能成功跑完（没有被硬切断）
	close(release)
	select {
	case err := <-inflightErr:
		if err != nil {
			t.Fatalf("inflight on draining instance must finish: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("inflight call got cut during instance drain")
	}
	reg.Remove("shop", "b")
	stats := c.Snapshot()
	for _, in := range stats.Instances {
		if in.Service == "shop" && in.ID == "b" {
			t.Fatal("b must be fully removed after drain completion")
		}
	}
	if atomic.LoadInt32(&callsB) != 0 {
		t.Fatalf("draining b received new calls: %d", atomic.LoadInt32(&callsB))
	}
	if atomic.LoadInt32(&callsA) != 10 {
		t.Fatalf("a should serve all 10 new calls, got %d", atomic.LoadInt32(&callsA))
	}
}
