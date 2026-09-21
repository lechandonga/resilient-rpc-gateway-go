// Package deadline handles timeout and deadline propagation so a
// downstream attempt never outlives its remaining budget and upstream
// cancellation always reaches in-flight downstream calls.
package deadline

import (
	"context"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/gerr"
)

// Remaining returns the time left before ctx's deadline. A context with
// no deadline reports ok=false. Zero/negative remaining means expired.
func Remaining(ctx context.Context) (d time.Duration, ok bool) {
	if dl, has := ctx.Deadline(); has {
		return time.Until(dl), true
	}
	return 0, false
}

// Child derives a downstream context bounded by BOTH the parent
// deadline and the per-attempt timeout, and returns the effective
// attempt budget. If no budget is left it returns
// gerr.CodeTimeoutBeforeDispatch without creating a live timer, so no
// connection or concurrency slot is consumed.
//
// When the parent deadline is the tighter bound a tiny margin is
// reserved so an attempt that runs to its limit does not coincidentally
// exhaust the total budget to the exact nanosecond — callers can then
// reliably distinguish "attempt timed out, total budget still available
// (may retry)" from "total budget exhausted (stop)".
func Child(ctx context.Context, attemptTimeout time.Duration) (context.Context, context.CancelFunc, time.Duration, error) {
	const margin = time.Millisecond
	budget := attemptTimeout
	if remaining, ok := Remaining(ctx); ok {
		if remaining <= margin {
			return nil, func() {}, 0, gerr.New(gerr.CodeTimeoutBeforeDispatch,
				"no deadline budget remains before dispatch")
		}
		headroom := remaining - margin
		if budget <= 0 || headroom < budget {
			budget = headroom
		}
	}
	if budget <= 0 {
		// No per-attempt timeout and no parent deadline: derive only
		// for cancellation propagation.
		cctx, cancel := context.WithCancel(ctx)
		return cctx, cancel, 0, nil
	}
	cctx, cancel := context.WithTimeout(ctx, budget)
	return cctx, cancel, budget, nil
}

// Sleep waits for d, but returns immediately with the mapped
// cancellation/timeout error if ctx fires first — so retry backoff
// never keeps work alive after the caller gave up.
func Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		if err := ctx.Err(); err != nil {
			return mapCtxErr(err)
		}
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return mapCtxErr(ctx.Err())
	case <-t.C:
		return nil
	}
}

func mapCtxErr(err error) error {
	if err == context.Canceled {
		return gerr.Wrap(gerr.CodeCanceled, "request canceled during wait", err)
	}
	return gerr.Wrap(gerr.CodeDeadlineExceeded, "deadline exceeded during wait", err)
}
