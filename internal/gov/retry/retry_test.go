package retry

import (
	"errors"
	"testing"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/gerr"
)

func TestNonIdempotentAlwaysDenied(t *testing.T) {
	c := NewClassifier(true, 5)
	for _, code := range []gerr.Code{gerr.CodeDownstream, gerr.CodeDeadlineExceeded} {
		d, dc := c.Classify(false, 1, gerr.New(code, "x"))
		if d != Deny || dc != gerr.CodeNonRetryable {
			t.Fatalf("non-idempotent must be non_retryable, got %v %s", d, dc)
		}
	}
}

func TestStructuralErrorsDenied(t *testing.T) {
	c := NewClassifier(true, 5)
	for _, code := range []gerr.Code{
		gerr.CodeCircuitOpen, gerr.CodeNoInstance, gerr.CodeCanceled,
		gerr.CodeConcurrencyLimit, gerr.CodeQueueLimit,
	} {
		if d, dc := c.Classify(true, 1, gerr.New(code, "x")); d != Deny || dc != code {
			t.Fatalf("code %s must be denied with same code, got %v %s", code, d, dc)
		}
	}
}

func TestAttemptDeadlineRetryableUntilCeiling(t *testing.T) {
	c := NewClassifier(true, 3)
	if d, _ := c.Classify(true, 1, gerr.New(gerr.CodeDeadlineExceeded, "x")); d != Allow {
		t.Fatal("attempt timeout with total budget left must be retryable")
	}
	if d, code := c.Classify(true, 3, gerr.New(gerr.CodeDeadlineExceeded, "x")); d != Deny || code != gerr.CodeRetryBudgetExhausted {
		t.Fatalf("at ceiling want deny/budget, got %v %s", d, code)
	}
}

func TestRetryDisabledDenies(t *testing.T) {
	c := NewClassifier(false, 3)
	if d, code := c.Classify(true, 1, errors.New("boom")); d != Deny || code != gerr.CodeNonRetryable {
		t.Fatalf("disabled retry want deny/non_retryable, got %v %s", d, code)
	}
}

func TestBackoffCappedExponential(t *testing.T) {
	got := Backoff(100*time.Millisecond, time.Second, 2.0, 3, false, nil)
	if got != 400*time.Millisecond {
		t.Fatalf("100ms * 2^2 want 400ms, got %s", got)
	}
	capped := Backoff(100*time.Millisecond, 300*time.Millisecond, 2.0, 5, false, nil)
	if capped != 300*time.Millisecond {
		t.Fatalf("backoff must cap at max, got %s", capped)
	}
	if j := Backoff(100*time.Millisecond, time.Second, 2.0, 2, true, nil); j < 100*time.Millisecond || j >= 200*time.Millisecond {
		t.Fatalf("jitter out of [100,200)ms: %s", j)
	}
}
