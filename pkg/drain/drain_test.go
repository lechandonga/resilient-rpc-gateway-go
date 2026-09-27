package drain

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
)

// TestBindReleaseAccounting 绑定/释放在途计数严格对称。
func TestBindReleaseAccounting(t *testing.T) {
	c := New()
	h1, err := c.Bind(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	h2, _ := c.Bind(context.Background())
	if got := c.Snapshot().Inflight; got != 2 {
		t.Fatalf("inflight want 2, got %d", got)
	}
	h1.Release()
	h1.Release() // 幂等：重复释放不多减
	if got := c.Snapshot().Inflight; got != 1 {
		t.Fatalf("inflight want 1 after release, got %d", got)
	}
	h2.Release()
	if got := c.Snapshot().Inflight; got != 0 {
		t.Fatalf("inflight want 0, got %d", got)
	}
}

// TestAdmitRejectedAfterDrainAndCounted 排空后 Admit/Bind 都被拒绝且计数。
func TestAdmitRejectedAfterDrainAndCounted(t *testing.T) {
	c := New()
	if err := c.Admit(); err != nil {
		t.Fatalf("active must admit: %v", err)
	}
	if err := c.Drain(0); err != nil {
		t.Fatalf("drain with no inflight: %v", err)
	}
	if err := c.Admit(); !errors.Is(err, model.ErrDrainRejected) {
		t.Fatalf("want drain rejected, got %v", err)
	}
	if _, err := c.Bind(context.Background()); !errors.Is(err, model.ErrDrainRejected) {
		t.Fatalf("bind after drain rejected, got %v", err)
	}
	if c.Snapshot().Rejected != 2 {
		t.Fatalf("rejected count want 2, got %d", c.Snapshot().Rejected)
	}
	// 重复 Drain 保持已排空结论。
	if err := c.Drain(time.Second); err != nil {
		t.Fatalf("repeat drain on drained scope: %v", err)
	}
}

// TestDrainGraceCutsContextWithCause 宽限到期：在途 ctx 以
// GATEWAY_DRAIN_TIMEOUT 为 cause 被取消，Drain 返回同一原因。
func TestDrainGraceCutsContextWithCause(t *testing.T) {
	c := New()
	h, _ := c.Bind(context.Background())

	errCh := make(chan error, 1)
	go func() {
		<-h.Context().Done()
		cause := context.Cause(h.Context())
		h.Release() // 在途请求观察到切断后退出并归还在途额度
		errCh <- cause
	}()

	err := c.Drain(30 * time.Millisecond)
	if !errors.Is(err, model.ErrDrainTimeout) {
		t.Fatalf("Drain want ErrDrainTimeout, got %v", err)
	}
	select {
	case cause := <-errCh:
		if !errors.Is(cause, model.ErrDrainTimeout) {
			t.Fatalf("ctx cause want drain timeout, got %v", cause)
		}
	case <-time.After(time.Second):
		t.Fatal("inflight ctx not canceled at grace deadline")
	}
	if got := c.Snapshot(); !got.TimedOut || got.Inflight != 0 || got.Phase != PhaseDrained {
		t.Fatalf("post-drain snapshot inconsistent: %+v", got)
	}
}

// TestDrainZeroGraceWaitsNaturalCompletion grace<=0 只等在途自然结束。
func TestDrainZeroGraceWaitsNaturalCompletion(t *testing.T) {
	c := New()
	h, _ := c.Bind(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Drain(0) }()
	select {
	case <-done:
		t.Fatal("drain must not finish while inflight exists")
	case <-time.After(20 * time.Millisecond):
	}
	h.Release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("natural drain completion: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("drain did not complete after release")
	}
}

// TestParentCancelPropagatesBoundContext 上游取消级联到绑定 ctx。
func TestParentCancelPropagatesBoundContext(t *testing.T) {
	c := New()
	ctx, cancel := context.WithCancel(context.Background())
	h, _ := c.Bind(ctx)
	cancel()
	select {
	case <-h.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("parent cancel must propagate")
	}
	if !errors.Is(context.Cause(h.Context()), context.Canceled) {
		t.Fatalf("want canceled cause, got %v", context.Cause(h.Context()))
	}
	h.Release()
}

// TestInstanceAttachDrainAndRebind 实例排空前后的绑定语义与计数平移。
func TestInstanceAttachDrainAndRebind(t *testing.T) {
	c := New()
	h, _ := c.Bind(context.Background())
	defer h.Release()

	if err := h.Attach("shop", "a"); err != nil {
		t.Fatal(err)
	}
	if got := c.InstanceSnapshot("shop", "a").Inflight; got != 1 {
		t.Fatalf("instance a inflight want 1, got %d", got)
	}

	// 重试换到 b：a 计数归零，b 计为 1，全局始终 1。
	if err := h.Attach("shop", "b"); err != nil {
		t.Fatal(err)
	}
	if got := c.InstanceSnapshot("shop", "a").Inflight; got != 0 {
		t.Fatalf("a inflight want 0 after rebind, got %d", got)
	}
	if got := c.InstanceSnapshot("shop", "b").Inflight; got != 1 {
		t.Fatalf("b inflight want 1, got %d", got)
	}

	// b 开始排空：新请求不能再 Attach 到 b。
	c.BeginInstanceDrain("shop", "b", time.Hour)
	h2, _ := c.Bind(context.Background())
	defer h2.Release()
	if err := h2.Attach("shop", "b"); !errors.Is(err, ErrInstanceDraining) {
		t.Fatalf("attach to draining instance want sentinel, got %v", err)
	}
	if got := c.InstanceSnapshot("shop", "b").Rejected; got != 1 {
		t.Fatalf("instance rejected count want 1, got %d", got)
	}
	if !c.IsInstanceDraining("shop", "b") {
		t.Fatal("b must report draining")
	}
	if c.IsInstanceDraining("shop", "a") {
		t.Fatal("a must not report draining")
	}
}

// TestDrainInstanceCutsOnlyInstanceInflight 实例宽限到期只切断该实例在途，
// 不影响其他实例或全局请求。
func TestDrainInstanceCutsOnlyInstanceInflight(t *testing.T) {
	c := New()
	ha, _ := c.Bind(context.Background())
	hb, _ := c.Bind(context.Background())
	if err := ha.Attach("shop", "a"); err != nil {
		t.Fatal(err)
	}
	if err := hb.Attach("shop", "b"); err != nil {
		t.Fatal(err)
	}

	releaseA := make(chan struct{})
	go func() {
		<-ha.Context().Done()
		ha.Release() // 被切断的在途请求退出并归还额度
		close(releaseA)
	}()

	err := c.DrainInstance("shop", "a", 20*time.Millisecond)
	if !errors.Is(err, model.ErrDrainTimeout) {
		t.Fatalf("want drain timeout, got %v", err)
	}
	select {
	case <-releaseA:
	case <-time.After(time.Second):
		t.Fatal("a in-flight did not exit after cut")
	}
	if !errors.Is(context.Cause(ha.Context()), model.ErrDrainTimeout) {
		t.Fatalf("a ctx cut by drain, cause=%v", context.Cause(ha.Context()))
	}
	select {
	case <-hb.Context().Done():
		t.Fatal("b ctx must remain alive when only a is drained")
	default:
	}
	if c.IsDraining() {
		t.Fatal("global scope must stay active during instance-only drain")
	}
	hb.Release()
}

// TestInstancesListsDrainingScopes 排空实例集合快照。
func TestInstancesListsDrainingScopes(t *testing.T) {
	c := New()
	c.BeginInstanceDrain("shop", "a", time.Hour)
	c.BeginInstanceDrain("pay", "z", time.Hour)
	got := c.Instances()
	if len(got) != 2 {
		t.Fatalf("want 2 draining instances, got %v", got)
	}
}

// TestConcurrentBindAttachReleaseNoLeak 并发交错下：
// 绑定/换绑/释放/实例排空不死锁、不泄漏，最终全部计数归零。
func TestConcurrentBindAttachReleaseNoLeak(t *testing.T) {
	c := New()
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h, err := c.Bind(context.Background())
			if err != nil {
				return
			}
			id := string(rune('a' + i%4))
			if err := h.Attach("shop", id); err != nil {
				h.Release()
				return
			}
			if i%3 == 0 {
				_ = h.Attach("shop", string(rune('a'+(i+1)%4)))
			}
			if i%5 == 0 {
				c.BeginInstanceDrain("shop", id, 20*time.Millisecond)
			}
			time.Sleep(time.Duration(i%3) * time.Millisecond)
			h.Release()
		}(i)
	}
	wg.Wait()
	if got := c.Snapshot().Inflight; got != 0 {
		t.Fatalf("global inflight must settle to 0, got %d", got)
	}
	// 全部 goroutine 退出后，对所有触碰过的实例做排空：必须立即结束、无残留。
	for _, id := range []string{"a", "b", "c", "d"} {
		if err := c.DrainInstance("shop", id, time.Second); err != nil {
			if got := c.InstanceSnapshot("shop", id).Inflight; got != 0 {
				t.Fatalf("instance %s leaked inflight=%d: %v", id, got, err)
			}
		}
	}
}

// TestGlobalDrainConcurrentlyWithRequests 全局排空与在途请求并发：
// 排空后所有 Bind 被拒，已在途的被切断，最终无泄漏。
func TestGlobalDrainConcurrentlyWithRequests(t *testing.T) {
	c := New()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h, err := c.Bind(context.Background())
			if err != nil {
				return // 已排空
			}
			<-h.Context().Done()
			h.Release()
		}()
	}
	if err := c.Drain(100 * time.Millisecond); err != nil &&
		!errors.Is(err, model.ErrDrainTimeout) {
		t.Fatalf("unexpected drain err: %v", err)
	}
	wg.Wait()
	if got := c.Snapshot().Inflight; got != 0 {
		t.Fatalf("inflight must be 0 after drain, got %d", got)
	}
}
