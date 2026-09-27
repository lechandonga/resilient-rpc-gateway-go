// 取消与优雅排空的端到端用例：
// 验证"不打多余下游、占用计数回落、失败原因可区分、排空后干净退出"。
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

// lifecycleConfig：单实例、可排队、长超时，便于精确制造排队/在途状态。
const lifecycleConfig = `{
  "version": 2,
  "service": {
    "name": "shop",
    "methods": {
      "work": {"timeout":"5s","max_attempts":1,"max_concurrency":1,"on_limit":"wait","max_queue":8}
    }
  }
}`

func lifecycleSetup(t *testing.T, invoker client.Invoker) (*client.Client, *registry.Registry) {
	t.Helper()
	parsed, err := config.Parse([]byte(lifecycleConfig))
	if err != nil {
		t.Fatal(err)
	}
	holder := config.NewHolder(parsed)
	reg := registry.NewRegistry()
	reg.Upsert("shop", model.Instance{ID: "a", Address: "a:1", Weight: 1})
	c, err := client.New(reg, holder, invoker)
	if err != nil {
		t.Fatal(err)
	}
	return c, reg
}

// blockInvoker：进入下游即信号 started，阻塞到 release 关闭或 ctx 取消。
func blockInvoker(calls *int32, started chan<- string, release <-chan struct{}) client.Invoker {
	return func(ctx context.Context, ins model.Instance, req *model.Request) (*model.Response, error) {
		atomic.AddInt32(calls, 1)
		if started != nil {
			started <- ins.ID
		}
		select {
		case <-release:
			return &model.Response{Status: 200, InstanceID: ins.ID}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// TestCanceledBeforeDownstreamNeverCalls 场景一：
// 请求进来时 ctx 已取消——绝不调用下游，也不占用任何额度。
func TestCanceledBeforeDownstreamNeverCalls(t *testing.T) {
	var calls int32
	c, _ := lifecycleSetup(t, blockInvoker(&calls, nil, nil))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 在 Invoke 之前就取消

	_, err := c.Invoke(ctx, &model.Request{ID: "pre-cancel", Service: "shop", Method: "work"})
	if !errors.Is(err, model.ErrContextCanceled) {
		t.Fatalf("want GATEWAY_CANCELED, got %v", err)
	}
	if calls != 0 {
		t.Fatalf("canceled-before-entry request must not hit downstream, calls=%d", calls)
	}
	// 快照中无任何残留在途/排队。
	snap := c.Snapshot()
	if snap.Drain.Inflight != 0 {
		t.Fatalf("global inflight must be 0, got %d", snap.Drain.Inflight)
	}
	if totalWaiting(snap) != 0 {
		t.Fatalf("queue must be empty, got %d", totalWaiting(snap))
	}
}

// TestCanceledWhileQueuedNeverCallsAndReleasesSlot 场景二：
// 排队等待额度期间取消——绝不调用下游，排队计数回落，令牌转给下一个排队者。
func TestCanceledWhileQueuedNeverCallsAndReleasesSlot(t *testing.T) {
	var calls int32
	started := make(chan string, 4)
	release := make(chan struct{})
	c, _ := lifecycleSetup(t, blockInvoker(&calls, started, release))

	// 第一个请求占满唯一执行额度。
	go func() {
		c.Invoke(context.Background(), &model.Request{ID: "holder", Service: "shop", Method: "work"})
	}()
	<-started

	// 第二、第三个请求排队。
	errCh := make(chan error, 2)
	go func() {
		_, e := c.Invoke(context.Background(), &model.Request{ID: "queued-1", Service: "shop", Method: "work"})
		errCh <- e
	}()
	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() {
		_, e := c.Invoke(ctx2, &model.Request{ID: "queued-2", Service: "shop", Method: "work"})
		errCh <- e
	}()
	waitUntilSnap(t, time.Second, c, func(s client.GovernanceSnapshot) bool {
		return totalWaiting(s) == 2 && s.Drain.Inflight == 3
	})

	// 取消其中一个排队请求。
	cancel2()
	select {
	case err := <-errCh:
		if !errors.Is(err, model.ErrContextCanceled) {
			t.Fatalf("queued cancel must surface GATEWAY_CANCELED, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled queued request did not return")
	}
	if calls != 1 {
		t.Fatalf("queued request must not hit downstream, calls=%d", calls)
	}
	waitUntilSnap(t, time.Second, c, func(s client.GovernanceSnapshot) bool {
		return totalWaiting(s) == 1 && s.Drain.Inflight == 2
	})

	// 释放执行额度：剩余排队者应得到令牌并真正打到下游（calls 增加到 2），
	// 证明取消者的令牌被正常向下转移而没有丢失或超发。
	close(release)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && atomic.LoadInt32(&calls) != 2 {
		time.Sleep(2 * time.Millisecond)
	}
	if calls != 2 {
		t.Fatalf("ticket must transfer to the remaining waiter, calls=%d", calls)
	}

	// 收尾：第二个排队请求成功结束，全部计数归零。
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("queued-1 should succeed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued-1 did not finish")
	}
	waitUntilSnap(t, time.Second, c, func(s client.GovernanceSnapshot) bool {
		return totalActive(s) == 0 && totalWaiting(s) == 0 && s.Drain.Inflight == 0
	})
}

func totalWaiting(s client.GovernanceSnapshot) int {
	n := 0
	for _, in := range s.Instances {
		for _, m := range in.Methods {
			n += m.Waiting
		}
	}
	return n
}

func totalActive(s client.GovernanceSnapshot) int {
	n := 0
	for _, in := range s.Instances {
		for _, m := range in.Methods {
			n += m.Active
		}
	}
	return n
}

func waitUntilSnap(t *testing.T, timeout time.Duration, c *client.Client,
	cond func(client.GovernanceSnapshot) bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond(c.Snapshot()) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("snapshot condition not met within %v:\n%+v", timeout, c.Snapshot())
}

// TestDrainRejectsNewRequests 场景三：进程进入排空后，新请求被明确拒绝
// （GATEWAY_DRAIN_REJECTED），不调用下游、不占额度。
func TestDrainRejectsNewRequests(t *testing.T) {
	var calls int32
	release := make(chan struct{})
	c, _ := lifecycleSetup(t, blockInvoker(&calls, make(chan string, 1), release))

	// 持有一个在途请求，再进入排空：在途不被立即切断。
	heldErr := make(chan error, 1)
	go func() {
		_, e := c.Invoke(context.Background(), &model.Request{ID: "held", Service: "shop", Method: "work"})
		heldErr <- e
	}()
	time.Sleep(20 * time.Millisecond) // 等在途真正进入下游
	c.BeginDrain(time.Hour)

	if snap := c.Snapshot(); !snap.Drain.Draining || snap.Drain.Phase != "drain" {
		t.Fatalf("expected drain phase, got %+v", snap.Drain)
	}

	// 排空期间的新请求：拒绝，且不打下游。
	before := atomic.LoadInt32(&calls)
	_, err := c.Invoke(context.Background(), &model.Request{ID: "new-after-drain", Service: "shop", Method: "work"})
	if !errors.Is(err, model.ErrDrainRejected) {
		t.Fatalf("want GATEWAY_DRAIN_REJECTED, got %v", err)
	}
	if atomic.LoadInt32(&calls) != before {
		t.Fatal("new request during drain must not hit downstream")
	}

	// 在途请求在自己的预算内跑完：释放后成功返回，Drain 干净收尾。
	close(release)
	if err := c.Shutdown(0); err != nil {
		t.Fatalf("grace=0 waits for in-flight naturally: %v", err)
	}
	select {
	case e := <-heldErr:
		if e != nil {
			t.Fatalf("in-flight request should finish ok, got %v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("held request did not finish")
	}
	if snap := c.Snapshot(); snap.Drain.Inflight != 0 || snap.Drain.Phase != "drained" {
		t.Fatalf("after shutdown snapshot inconsistent: %+v", snap.Drain)
	}
}

// TestDrainGraceTimeoutCutsInflight 场景四：排空宽限用尽——
// 在途请求收到 GATEWAY_DRAIN_TIMEOUT，与普通取消/超时区分；
// Drain 返回该原因，且返回后进程内没有仍在跑的调用，计数归零。
func TestDrainGraceTimeoutCutsInflight(t *testing.T) {
	var calls int32
	started := make(chan string, 2)
	release := make(chan struct{}) // 永不关闭：模拟还在跑的慢下游
	c, _ := lifecycleSetup(t, blockInvoker(&calls, started, release))

	heldErr := make(chan error, 1)
	go func() {
		_, e := c.Invoke(context.Background(), &model.Request{ID: "slow-held", Service: "shop", Method: "work"})
		heldErr <- e
	}()
	<-started
	waitUntilSnap(t, time.Second, c, func(s client.GovernanceSnapshot) bool { return s.Drain.Inflight == 1 })

	start := time.Now()
	err := c.Shutdown(80 * time.Millisecond)
	elapsed := time.Since(start)
	if !errors.Is(err, model.ErrDrainTimeout) {
		t.Fatalf("want GATEWAY_DRAIN_TIMEOUT from Shutdown, got %v", err)
	}
	if elapsed < 70*time.Millisecond || elapsed > 600*time.Millisecond {
		t.Fatalf("should wait out the grace period, elapsed=%v", elapsed)
	}

	// 在途请求的错误必须能与普通 CANCELED/TIMEOUT 区分。
	select {
	case e := <-heldErr:
		if !errors.Is(e, model.ErrDrainTimeout) {
			t.Fatalf("in-flight must see GATEWAY_DRAIN_TIMEOUT, got %v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("cut request did not return")
	}

	// Drain 返回后：无在途、无排队，且排空后仍拒绝一切新请求。
	snap := c.Snapshot()
	if snap.Drain.Inflight != 0 || totalActive(snap) != 0 || totalWaiting(snap) != 0 {
		t.Fatalf("resources must be released after drain timeout: %+v", snap)
	}
	if !snap.Drain.TimedOut {
		t.Fatal("snapshot must record timed-out drain")
	}
	if _, err := c.Invoke(context.Background(),
		&model.Request{ID: "post-shutdown", Service: "shop", Method: "work"}); !errors.Is(err, model.ErrDrainRejected) {
		t.Fatalf("post-drain requests must be rejected, got %v", err)
	}
}

// TestInstanceDrainStopsSelectionAndWaitsInflight 实例级排空：
// 新流量不再选中该实例并落到其他实例；在途请求跑完后 DrainInstance 干净返回。
func TestInstanceDrainStopsSelectionAndWaitsInflight(t *testing.T) {
	parsed, err := config.Parse([]byte(lifecycleConfig))
	if err != nil {
		t.Fatal(err)
	}
	holder := config.NewHolder(parsed)
	reg := registry.NewRegistry()
	reg.Upsert("shop",
		model.Instance{ID: "a", Address: "a:1", Weight: 1},
		model.Instance{ID: "b", Address: "b:1", Weight: 1},
	)
	var calls int32
	started := make(chan string, 32)
	releaseA := make(chan struct{})
	inv := func(ctx context.Context, ins model.Instance, req *model.Request) (*model.Response, error) {
		atomic.AddInt32(&calls, 1)
		started <- ins.ID
		if ins.ID == "a" {
			select {
			case <-releaseA:
				return &model.Response{Status: 200, InstanceID: "a"}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return &model.Response{Status: 200, InstanceID: ins.ID}, nil
	}
	c, err := client.New(reg, holder, inv)
	if err != nil {
		t.Fatal(err)
	}

	heldErr := make(chan error, 1)
	go func() {
		_, e := c.Invoke(context.Background(), &model.Request{ID: "on-a", Service: "shop", Method: "work"})
		heldErr <- e
	}()
	if id := <-started; id != "a" {
		t.Fatalf("setup expected first call on a, got %s", id)
	}

	drainDone := make(chan error, 1)
	go func() { drainDone <- c.DrainInstance("shop", "a", time.Second) }()
	time.Sleep(20 * time.Millisecond)

	// 排空后连续新请求都只能落到 b。
	for i := 0; i < 6; i++ {
		resp, err := c.Invoke(context.Background(),
			&model.Request{ID: "post-drain", Service: "shop", Method: "work"})
		if err != nil {
			t.Fatalf("other instance must serve traffic: %v", err)
		}
		if resp.InstanceID != "b" {
			t.Fatalf("drained instance a must not receive new traffic, hit %s", resp.InstanceID)
		}
	}

	// 在途完成后实例排空结束，快照记录该实例已排空且无在途。
	close(releaseA)
	select {
	case e := <-drainDone:
		if e != nil {
			t.Fatalf("instance drain should finish after in-flight completes: %v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("DrainInstance did not return")
	}
	if e := <-heldErr; e != nil {
		t.Fatalf("held request should succeed, got %v", e)
	}
	view := findInstance(c.Snapshot(), "shop", "a")
	if view == nil || !view.Drain.Draining || view.Drain.Inflight != 0 {
		t.Fatalf("drained instance view inconsistent: %+v", view)
	}
}

// TestSnapshotSelfConsistentAndReproducible 静止系统上快照前后自洽、
// 重复查询逐字段相同；在途/排队/熔断/排空字段语义对齐。
func TestSnapshotSelfConsistentAndReproducible(t *testing.T) {
	var calls int32
	release := make(chan struct{})
	c, _ := lifecycleSetup(t, blockInvoker(&calls, make(chan string, 1), release))

	// 无流量时：空快照可复现。
	s1 := c.Snapshot()
	s2 := c.Snapshot()
	if !snapshotsEqual(s1, s2) {
		t.Fatal("idle snapshot must be reproducible")
	}
	if s1.Drain.Phase != "active" || len(s1.Instances) != 1 {
		t.Fatalf("unexpected idle snapshot: %+v", s1)
	}

	// 制造一个在途 + 一个排队。
	go func() {
		c.Invoke(context.Background(), &model.Request{ID: "h", Service: "shop", Method: "work"})
	}()
	go func() {
		c.Invoke(context.Background(), &model.Request{ID: "q", Service: "shop", Method: "work"})
	}()
	waitUntilSnap(t, time.Second, c, func(s client.GovernanceSnapshot) bool {
		v := findInstance(s, "shop", "a")
		return v != nil && activeOf(v) == 1 && waitingOf(v) == 1 && v.Inflight == 2
	})

	// 静止在该状态的瞬间反复查询：同一输入结果一致（计数不漂移）。
	var prev client.GovernanceSnapshot
	for i := 0; i < 50; i++ {
		cur := c.Snapshot()
		v := findInstance(cur, "shop", "a")
		if v == nil || activeOf(v) != 1 || waitingOf(v) != 1 || v.Inflight != 2 {
			t.Fatalf("stable-state snapshot drifted: %+v", v)
		}
		if i > 0 {
			pv := findInstance(prev, "shop", "a")
			if activeOf(pv) != activeOf(v) || waitingOf(pv) != waitingOf(v) ||
				pv.Drain.Inflight != v.Drain.Inflight || circuitState(pv) != circuitState(v) {
				t.Fatalf("repeated snapshot not reproducible: %+v vs %+v", pv, v)
			}
		}
		prev = cur
		time.Sleep(time.Millisecond)
	}

	// 熔断字段存在且为合法初态。
	v := findInstance(c.Snapshot(), "shop", "a")
	if cs := circuitState(v); cs != "closed" {
		t.Fatalf("fresh breaker must be closed, got %s", cs)
	}
	close(release)
}

func findInstance(s client.GovernanceSnapshot, service, id string) *client.InstanceView {
	for i := range s.Instances {
		if s.Instances[i].Service == service && s.Instances[i].InstanceID == id {
			return &s.Instances[i]
		}
	}
	return nil
}

func activeOf(v *client.InstanceView) int {
	n := 0
	for _, m := range v.Methods {
		n += m.Active
	}
	return n
}

func waitingOf(v *client.InstanceView) int {
	n := 0
	for _, m := range v.Methods {
		n += m.Waiting
	}
	return n
}

func circuitState(v *client.InstanceView) string {
	for _, m := range v.Methods {
		if m.Circuit != nil {
			return string(m.Circuit.State)
		}
	}
	return ""
}

func snapshotsEqual(a, b client.GovernanceSnapshot) bool {
	if a.Drain.Phase != b.Drain.Phase || len(a.Instances) != len(b.Instances) {
		return false
	}
	for i := range a.Instances {
		x, y := a.Instances[i], b.Instances[i]
		if x.Service != y.Service || x.InstanceID != y.InstanceID || x.Inflight != y.Inflight {
			return false
		}
	}
	return true
}
