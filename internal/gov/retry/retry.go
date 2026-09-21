// Package retry implements retry classification and capped exponential
// backoff. Only explicitly idempotent requests may be retried, so a
// retry can never duplicate a downstream side effect.
package retry

import (
	"math/rand"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/gerr"
)

// Decision tells whether an attempt failure may be retried.
type Decision int

const (
	// Deny means no further attempt. Reason carries the exact cause:
	// non-idempotent request, open circuit, exhausted budget, ...
	Deny Decision = iota
	// Allow means another attempt is permitted.
	Allow
)

// Classifier decides retryability based on idempotency, attempt number,
// remaining budget and the kind of failure.
type Classifier struct {
	enabled    bool
	maxAttempt int // total attempts including the first
}

// NewClassifier creates a classifier. maxAttempts is total attempts;
// values < 1 are normalized to 1 (exactly one attempt, no retry).
func NewClassifier(enabled bool, maxAttempts int) *Classifier {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	return &Classifier{enabled: enabled, maxAttempt: maxAttempts}
}

// Enabled reports whether retry is switched on.
func (c *Classifier) Enabled() bool { return c.enabled }

// MaxAttempts returns the configured total attempt ceiling.
func (c *Classifier) MaxAttempts() int { return c.maxAttempt }

// Classify decides whether the failure on attempt number `attempt`
// (1-based) may trigger another attempt.
//
// Non-idempotent requests are never retried regardless of error.
// Structural failures that another attempt against the same snapshot
// cannot fix (open circuit, missing instances, resource limits, bad
// config) are denied immediately.
//
// NOTE: an ATTEMPT-level deadline (CodeDeadlineExceeded while the total
// budget remains) IS retryable; the caller is responsible for refusing a
// retry once the outer/total budget has actually expired. An upstream
// cancellation (CodeCanceled) is never retried.
func (c *Classifier) Classify(idempotent bool, attempt int, err error) (Decision, gerr.Code) {
	if !idempotent {
		return Deny, gerr.CodeNonRetryable
	}
	code := gerr.CodeOf(err)
	switch code {
	case gerr.CodeCanceled,
		gerr.CodeTimeoutBeforeDispatch, gerr.CodeCircuitOpen,
		gerr.CodeNoInstance, gerr.CodeInstanceUnhealthy,
		gerr.CodeConcurrencyLimit, gerr.CodeQueueLimit,
		gerr.CodeConfigInvalid:
		return Deny, code
	}
	if !c.enabled {
		return Deny, gerr.CodeNonRetryable
	}
	if attempt >= c.maxAttempt {
		return Deny, gerr.CodeRetryBudgetExhausted
	}
	return Allow, code
}

// Backoff computes the wait before the next attempt using capped
// exponential backoff: initial * multiplier^(attempt-1), clamped to max.
// attempt is the attempt that just failed (1-based). jitter, when on,
// scales the value by a deterministic [0.5,1.0) factor from rng.
func Backoff(initial, max time.Duration, multiplier float64, attempt int, jitter bool, rng *rand.Rand) time.Duration {
	if initial <= 0 {
		return 0
	}
	if multiplier <= 1 {
		multiplier = 2
	}
	d := float64(initial)
	for i := 1; i < attempt; i++ {
		d *= multiplier
		if max > 0 && d >= float64(max) {
			d = float64(max)
			break
		}
	}
	if max > 0 && d > float64(max) {
		d = float64(max)
	}
	if jitter {
		if rng == nil {
			rng = rand.New(rand.NewSource(time.Now().UnixNano()))
		}
		d *= 0.5 + rng.Float64()/2
	}
	return time.Duration(d)
}
