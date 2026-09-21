package bulkhead

import (
	"context"
	"testing"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/gerr"
)

func TestRejectFastWhenSaturated(t *testing.T) {
	b := New(Config{MaxConcurrent: 2, Policy: RejectFast})
	var leases []*Lease
	for i := 0; i < 2; i++ {
		l, err := b.Acquire(context.Background(), "k")
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		leases = append(leases, l)
	}
	if _, err := b.Acquire(context.Background(), "k"); err == nil {
		t.Fatal("want rejection when saturated")
	} else if gerr.CodeOf(err) != gerr.CodeConcurrencyLimit {
		t.Fatalf("want concurrency_limit, got %v", err)
	}
	for _, l := range leases {
		l.Release()
	}
	if _, err := b.Acquire(context.Background(), "k"); err != nil {
		t.Fatalf("slot should be available after release: %v", err)
	}
}

func TestBoundedQueueFailsWithQueueLimit(t *testing.T) {
	b := New(Config{MaxConcurrent: 1, MaxQueued: 1, Policy: RejectWait, QueueTimeout: time.Hour})
	l1, _ := b.Acquire(context.Background(), "k")
	// One waiter queues (blocks); run it from a goroutine.
	got := make(chan *Lease, 1)
	go func() {
		l2, qerr := b.Acquire(context.Background(), "k")
		if qerr != nil {
			t.Errorf("queued acquire failed: %v", qerr)
			return
		}
		got <- l2
	}()
	time.Sleep(20 * time.Millisecond)
	_, err := b.Acquire(context.Background(), "k")
	if err == nil || gerr.CodeOf(err) != gerr.CodeQueueLimit {
		t.Fatalf("want queue_limit, got %v", err)
	}
	l1.Release() // hands slot directly to queued waiter
	select {
	case l2 := <-got:
		l2.Release()
	case <-time.After(time.Second):
		t.Fatal("queued waiter never woken after release")
	}
}

func TestWaiterTimeoutDoesNotExceedConcurrency(t *testing.T) {
	b := New(Config{MaxConcurrent: 2, MaxQueued: 10, Policy: RejectWait, QueueTimeout: 30 * time.Millisecond})
	l1, _ := b.Acquire(context.Background(), "k")
	l2, _ := b.Acquire(context.Background(), "k")

	_, err := b.Acquire(context.Background(), "k")
	if err == nil || gerr.CodeOf(err) != gerr.CodeQueueLimit {
		t.Fatalf("queued waiter must fail with queue_limit on timeout, got %v", err)
	}
	inflight, queued := b.Stats("k")
	if inflight != 2 || queued != 0 {
		t.Fatalf("timed-out waiter leaked: inflight=%d queued=%d", inflight, queued)
	}
	l1.Release()
	l2.Release()
}

func TestWaiterHonorsContextCancel(t *testing.T) {
	b := New(Config{MaxConcurrent: 1, MaxQueued: 5, Policy: RejectWait, QueueTimeout: time.Hour})
	holder, _ := b.Acquire(context.Background(), "k")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	_, err := b.Acquire(ctx, "k")
	if err == nil || gerr.CodeOf(err) != gerr.CodeCanceled {
		t.Fatalf("want canceled, got %v", err)
	}
	if _, q := b.Stats("k"); q != 0 {
		t.Fatalf("canceled waiter left in queue: queued=%d", q)
	}
	holder.Release()
}

func TestMaxConcurrentNeverExceededUnderContention(t *testing.T) {
	const max = 4
	b := New(Config{MaxConcurrent: max, MaxQueued: 50, Policy: RejectWait, QueueTimeout: 2 * time.Second})
	var peak int64
	var cur int64
	done := make(chan struct{})
	var wg syncWg
	for i := 0; i < 40; i++ {
		wg.add()
		go func() {
			defer wg.done()
			l, err := b.Acquire(context.Background(), "hot")
			if err != nil {
				return // queue timeout under contention is acceptable
			}
			defer l.Release()
			n := addInt64(&cur, 1)
			recordPeak(&peak, n)
			time.Sleep(5 * time.Millisecond)
			addInt64(&cur, -1)
		}()
	}
	go func() { wg.wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("test hung")
	}
	if peak > max {
		t.Fatalf("inflight peaked at %d > max %d", peak, max)
	}
}
