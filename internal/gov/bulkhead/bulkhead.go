// Package bulkhead enforces bounded in-flight concurrency and a bounded
// wait queue per key (instance or method). Excess requests are either
// rejected immediately or wait for a bounded time; memory use is always
// bounded by MaxConcurrent + MaxQueued per key.
package bulkhead

import (
	"context"
	"sync"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/gerr"
)

// RejectPolicy defines behavior when the concurrency limit is reached.
type RejectPolicy string

const (
	// RejectFast fails immediately once all slots and queue slots are used.
	RejectFast RejectPolicy = "reject"
	// RejectWait queues up to MaxQueued requests until a slot frees or
	// QueueTimeout / ctx deadline fires.
	RejectWait RejectPolicy = "wait"
)

// Config describes per-key resource bounds.
type Config struct {
	MaxConcurrent int
	MaxQueued     int
	QueueTimeout  time.Duration
	Policy        RejectPolicy
}

// Lease is an acquired concurrency slot; Release must be called exactly
// once when the call finishes.
type Lease struct {
	key      string
	release  func()
	released bool
	mu       sync.Mutex
}

// Release returns the slot to the bulkhead.
func (l *Lease) Release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return
	}
	l.released = true
	l.release()
}

type waiter struct {
	ready chan struct{}
}

type keyState struct {
	inflight int
	queued   int
	// pending is FIFO of waiters.
	pending []*waiter
}

// Bulkhead is a keyed concurrency limiter.
type Bulkhead struct {
	cfg Config

	mu    sync.Mutex
	state map[string]*keyState
}

// New creates a bulkhead.
func New(cfg Config) *Bulkhead {
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 1
	}
	return &Bulkhead{cfg: cfg, state: make(map[string]*keyState)}
}

// Acquire obtains a concurrency slot for key.
//
//   - If a slot is free it is granted immediately.
//   - Otherwise with Policy=reject it returns CodeConcurrencyLimit.
//   - With Policy=wait and a free queue slot it blocks until a slot
//     frees, the queue timeout fires, or ctx is canceled; full queue
//     returns CodeQueueLimit.
func (b *Bulkhead) Acquire(ctx context.Context, key string) (*Lease, error) {
	b.mu.Lock()
	st := b.state[key]
	if st == nil {
		st = &keyState{}
		b.state[key] = st
	}
	if st.inflight < b.cfg.MaxConcurrent {
		st.inflight++
		b.mu.Unlock()
		return b.newLease(key), nil
	}

	if b.cfg.Policy == RejectFast || b.cfg.MaxQueued <= 0 {
		b.mu.Unlock()
		return nil, gerr.New(gerr.CodeConcurrencyLimit,
			"concurrency limit reached and reject policy is reject")
	}
	if st.queued >= b.cfg.MaxQueued {
		b.mu.Unlock()
		return nil, gerr.New(gerr.CodeQueueLimit,
			"queue limit reached")
	}
	w := &waiter{ready: make(chan struct{})}
	st.pending = append(st.pending, w)
	st.queued++
	b.mu.Unlock()

	var qTimer *time.Timer
	var qCh <-chan time.Time
	if b.cfg.QueueTimeout > 0 {
		qTimer = time.NewTimer(b.cfg.QueueTimeout)
		qCh = qTimer.C
	}
	defer func() {
		if qTimer != nil {
			qTimer.Stop()
		}
	}()

	select {
	case <-w.ready:
		return b.newLease(key), nil
	case <-ctx.Done():
		b.removeWaiter(key, w)
		if ctx.Err() == context.Canceled {
			return nil, gerr.Wrap(gerr.CodeCanceled, "canceled while queued", ctx.Err())
		}
		return nil, gerr.Wrap(gerr.CodeDeadlineExceeded, "deadline exceeded while queued", ctx.Err())
	case <-qCh:
		b.removeWaiter(key, w)
		return nil, gerr.New(gerr.CodeQueueLimit, "queue timeout while waiting for slot")
	}
}

// Stats reports current in-flight and queued counts for a key.
func (b *Bulkhead) Stats(key string) (inflight, queued int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if st := b.state[key]; st != nil {
		return st.inflight, st.queued
	}
	return 0, 0
}

func (b *Bulkhead) newLease(key string) *Lease {
	return &Lease{
		key: key,
		release: func() {
			b.releaseSlot(key)
		},
	}
}

func (b *Bulkhead) releaseSlot(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	st := b.state[key]
	if st == nil {
		return
	}
	if st.inflight > 0 {
		st.inflight--
	}
	// Wake the head waiter by granting it the slot directly.
	for len(st.pending) > 0 {
		w := st.pending[0]
		st.pending = st.pending[1:]
		if st.queued > 0 {
			st.queued--
		}
		// The waiter is about to hold a slot: inflight stays constant
		// (we decremented above, hand one slot straight over).
		st.inflight++
		close(w.ready)
		return
	}
	if st.inflight == 0 && st.queued == 0 {
		delete(b.state, key)
	}
}

func (b *Bulkhead) removeWaiter(key string, w *waiter) {
	b.mu.Lock()
	defer b.mu.Unlock()
	st := b.state[key]
	if st == nil {
		return
	}
	for i, cand := range st.pending {
		if cand == w {
			st.pending = append(st.pending[:i], st.pending[i+1:]...)
			if st.queued > 0 {
				st.queued--
			}
			break
		}
	}
	if st.inflight == 0 && st.queued == 0 {
		delete(b.state, key)
	}
}
