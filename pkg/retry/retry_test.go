package retry

import (
	"errors"
	"testing"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
)

func policy(attempts int) model.MethodPolicy {
	p := model.DefaultMethodPolicy("m")
	p.MaxAttempts = attempts
	return p
}

func TestIdempotencyGate(t *testing.T) {
	s := NewState(policy(3))
	nonID := &model.Request{ID: "r1", Idempotent: false}
	idem := &model.Request{ID: "r2", Idempotent: true}

	if err := s.Allow(nonID, 1); err != nil {
		t.Fatalf("first attempt always allowed: %v", err)
	}
	if err := s.Allow(nonID, 2); !errors.Is(err, model.ErrNonRetryable) {
		t.Fatalf("non-idempotent retry must be ErrNonRetryable, got %v", err)
	}
	if err := s.Allow(idem, 2); err != nil {
		t.Fatalf("idempotent retry should be allowed: %v", err)
	}
	if err := s.Allow(idem, 4); !errors.Is(err, model.ErrRetryExhausted) {
		t.Fatalf("over-attempt must be ErrRetryExhausted, got %v", err)
	}
}

func TestBackoffBoundedAndGrowing(t *testing.T) {
	s := NewState(policy(5))
	d1 := s.NextBackoff(1)
	d2 := s.NextBackoff(2)
	if d2 <= d1 {
		t.Fatalf("backoff should grow: %v -> %v", d1, d2)
	}
	for i := 1; i <= 20; i++ {
		if d := s.NextBackoff(i); d > timeSecond() {
			t.Fatalf("backoff must be capped, got %v", d)
		}
	}
}
