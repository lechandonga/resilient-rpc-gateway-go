package concurrency

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
)

func rejectPolicy(n int) model.MethodPolicy {
	p := model.DefaultMethodPolicy("m")
	p.MaxConcurrency = n
	p.OnLimit = model.RejectReject
	return p
}

func waitPolicy(conc, queue int) model.MethodPolicy {
	p := model.DefaultMethodPolicy("m")
	p.MaxConcurrency = conc
	p.MaxQueue = queue
	p.OnLimit = model.RejectWait
	return p
}

func TestRejectOverConcurrency(t *testing.T) {
	l := New()
	pol := rejectPolicy(1)
	t1, err := l.Acquire(context.Background(), "k", pol)
	if err != nil {
		t.Fatal(err)
	}
	defer t1.Release()
	if _, err := l.Acquire(context.Background(), "k", pol); !errors.Is(err, model.ErrConcurrencyLimited) {
		t.Fatalf("expected ErrConcurrencyLimited, got %v", err)
	}
	if l.InFlight("k") != 1 {
		t.Fatalf("inflight should be 1, got %d", l.InFlight("k"))
	}
}

func TestWaitThenGrantedOnRelease(t *testing.T) {
	l := New()
	pol := waitPolicy(1, 2)
	holder, err := l.Acquire(context.Background(), "k", pol)
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan *Ticket, 1)
	go func() {
		tk, err := l.Acquire(context.Background(), "k", pol)
		if err != nil {
			t.Errorf("queued acquire should succeed: %v", err)
			return
		}
		got <- tk
	}()
	waitFor(t, time.Second, func() bool { return l.InFlight("k") == 2 })
	holder.Release()
	select {
	case tk := <-got:
		tk.Release()
	case <-time.After(time.Second):
		t.Fatal("queued waiter not woken after release")
	}
	waitFor(t, time.Second, func() bool { return l.InFlight("k") == 0 })
}

func TestQueueFullRejected(t *testing.T) {
	l := New()
	pol := waitPolicy(1, 1)
	holder, _ := l.Acquire(context.Background(), "k", pol)
	done := make(chan struct{})
	go func() {
		tk, err := l.Acquire(context.Background(), "k", pol)
		if err != nil {
			t.Errorf("first queued acquire should succeed: %v", err)
			return
		}
		tk.Release()
		close(done)
	}()
	// Acquire 在拿到令牌前不会返回，因此通过在途数确认它已进入队列。
	waitFor(t, time.Second, func() bool { return l.InFlight("k") == 2 })
	if _, err := l.Acquire(context.Background(), "k", pol); !errors.Is(err, model.ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
	// 释放持有者：令牌转移给排队者，排队者随后释放并退出。
	holder.Release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("queued waiter did not complete after handoff")
	}
}

func TestWaitCanceledByUpstream(t *testing.T) {
	l := New()
	pol := waitPolicy(1, 2)
	holder, _ := l.Acquire(context.Background(), "k", pol)
	defer holder.Release()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := l.Acquire(ctx, "k", pol)
		errCh <- err
	}()
	waitFor(t, time.Second, func() bool { return l.InFlight("k") == 2 })
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, model.ErrContextCanceled) {
			t.Fatalf("expected ErrContextCanceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled waiter did not return")
	}
	// 取消后在途数回到仅持有者；后续仍可正常入队
	waitFor(t, time.Second, func() bool { return l.InFlight("k") == 1 })
}

func TestWaitDeadlineExhausted(t *testing.T) {
	l := New()
	pol := waitPolicy(1, 2)
	holder, _ := l.Acquire(context.Background(), "k", pol)
	defer holder.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := l.Acquire(ctx, "k", pol); !errors.Is(err, model.ErrTimeout) {
		t.Fatalf("expected ErrTimeout while waiting, got %v", err)
	}
}

func TestInFlightBoundedAcrossKeys(t *testing.T) {
	l := New()
	pol := rejectPolicy(1)
	for i := 0; i < 100; i++ {
		tk, err := l.Acquire(context.Background(), "distinct-key", pol)
		if err != nil {
			t.Fatalf("fresh keys must not be limited: %v", err)
		}
		tk.Release() // 立即释放 -> 空闲维度被清理，map 不堆积
	}
	if n := l.InFlight("distinct-key"); n != 0 {
		t.Fatalf("expected cleanup, inflight=%d", n)
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}
