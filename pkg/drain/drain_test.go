package drain

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
)

func TestAdmitRejectAndCount(t *testing.T) {
	m := New()
	if err := m.Admit(); err != nil {
		t.Fatalf("idle must admit: %v", err)
	}
	if got := m.Snapshot(); got.Phase != model.DrainIdle || got.InFlight != 1 {
		t.Fatalf("unexpected snapshot: %+v", got)
	}
	m.Begin(time.Hour)
	if err := m.Admit(); !errors.Is(err, model.ErrDraining) {
		t.Fatalf("draining must reject new, got %v", err)
	}
	// 在途仍为 1，拒绝不增加计数
	if got := m.Snapshot(); got.InFlight != 1 || got.Phase != model.DrainDraining {
		t.Fatalf("unexpected snapshot: %+v", got)
	}
	m.Release()
	select {
	case <-m.doneCh:
	case <-time.After(time.Second):
		t.Fatal("drain must complete after all in-flight released")
	}
	if got := m.Snapshot(); got.Phase != model.DrainComplete || got.TimedOut {
		t.Fatalf("want clean complete, got %+v", got)
	}
}

func TestDrainEmptyCompletesImmediately(t *testing.T) {
	m := New()
	s := m.Shutdown(time.Hour)
	if s.Phase != model.DrainComplete || s.TimedOut {
		t.Fatalf("idle shutdown with no traffic must complete cleanly, got %+v", s)
	}
	if err := m.Admit(); !errors.Is(err, model.ErrDraining) {
		t.Fatalf("completed drain must still reject, got %v", err)
	}
}

func TestDrainGraceTimeoutForcesCancel(t *testing.T) {
	m := New()
	if err := m.Admit(); err != nil {
		t.Fatal(err)
	}
	m.Begin(30 * time.Millisecond)
	start := time.Now()
	m.Wait()
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Fatalf("wait should return around grace, elapsed=%v", elapsed)
	}
	s := m.Snapshot()
	if !s.TimedOut || s.Phase != model.DrainComplete {
		t.Fatalf("want timed-out complete, got %+v", s)
	}
	if err := m.ForceContext().Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("force ctx must be canceled, got %v", err)
	}
	if cause := context.Cause(m.ForceContext()); !errors.Is(cause, model.ErrDrainTimeout) {
		t.Fatalf("cancel cause must be drain timeout, got %v", cause)
	}
}

func TestInFlightCompletesBeforeGrace(t *testing.T) {
	m := New()
	_ = m.Admit()
	_ = m.Admit()
	go func() {
		time.Sleep(20 * time.Millisecond)
		m.Release()
		m.Release()
	}()
	s := m.Shutdown(time.Hour)
	if s.TimedOut || s.InFlight != 0 {
		t.Fatalf("want clean shutdown, got %+v", s)
	}
}

func TestZeroGraceForcesImmediately(t *testing.T) {
	m := New()
	_ = m.Admit()
	s := m.Shutdown(0)
	if !s.TimedOut {
		t.Fatalf("zero grace must time out, got %+v", s)
	}
	if cause := context.Cause(m.ForceContext()); !errors.Is(cause, model.ErrDrainTimeout) {
		t.Fatalf("want drain-timeout cause, got %v", cause)
	}
}

func TestBeginIdempotent(t *testing.T) {
	m := New()
	_ = m.Admit()
	m.Begin(time.Hour)
	first := m.Snapshot().StartedAt
	time.Sleep(5 * time.Millisecond)
	m.Begin(0) // 重复调用不得重置
	got := m.Snapshot()
	if !got.StartedAt.Equal(first) || got.GraceLeft <= 0 {
		t.Fatalf("second Begin must be ignored, got %+v", got)
	}
	m.Release()
	m.Wait()
}

func TestReleaseDoesNotGoNegative(t *testing.T) {
	m := New()
	m.Release()
	m.Release()
	if got := m.Snapshot().InFlight; got != 0 {
		t.Fatalf("inflight must not go negative, got %d", got)
	}
}
