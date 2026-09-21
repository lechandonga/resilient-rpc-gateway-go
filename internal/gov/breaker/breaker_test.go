package breaker

import (
	"testing"
	"time"
)

func TestClosedTripsAfterThreshold(t *testing.T) {
	b := New(Config{FailureThreshold: 3, OpenCooldown: time.Hour, ProbeSuccess: 1})
	for i := 0; i < 3; i++ {
		if !b.Allow() {
			t.Fatalf("attempt %d unexpectedly blocked while closed", i)
		}
		b.OnFailure()
	}
	if b.Allow() {
		t.Fatal("breaker should be open after threshold")
	}
	if got := b.State(); got != StateOpen {
		t.Fatalf("want open, got %s", got)
	}
}

func TestHalfOpenFailureReturnsToOpen(t *testing.T) {
	clock := newFakeClock(time.Unix(1000, 0))
	b := New(Config{FailureThreshold: 1, OpenCooldown: 5 * time.Second, ProbeSuccess: 2})
	b.now = clock.get
	b.OnFailure() // trip immediately
	if b.Allow() {
		t.Fatal("want open immediately after trip")
	}

	clock.advance(6 * time.Second)
	if !b.Allow() {
		t.Fatal("cooldown elapsed: half-open probe should be allowed")
	}
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("want half-open, got %s", got)
	}
	// While the probe is in flight, nobody else is admitted.
	if b.Allow() {
		t.Fatal("a second request must not pass during the single probe")
	}
	// Failed probe -> straight back to open, never released.
	b.OnFailure()
	if got := b.State(); got != StateOpen {
		t.Fatalf("failed probe must return to open, got %s", got)
	}
	if b.Allow() {
		t.Fatal("must remain closed to traffic immediately after failed probe")
	}
}

func TestHalfOpenSuccessClosesAfterProbeCount(t *testing.T) {
	clock := newFakeClock(time.Unix(2000, 0))
	b := New(Config{FailureThreshold: 1, OpenCooldown: time.Second, ProbeSuccess: 2})
	b.now = clock.get
	b.OnFailure()
	clock.advance(2 * time.Second)

	// Probe 1 succeeds: still half-open until ProbeSuccess reached.
	if !b.Allow() {
		t.Fatal("probe 1 should be admitted")
	}
	b.OnSuccess()
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("still need 2 probe successes, got %s", got)
	}
	// Probe 2 succeeds: closes.
	if !b.Allow() {
		t.Fatal("probe 2 should be admitted")
	}
	b.OnSuccess()
	if got := b.State(); got != StateClosed {
		t.Fatalf("want closed after probes, got %s", got)
	}
}

type fakeClock struct{ t time.Time }

func newFakeClock(start time.Time) *fakeClock { return &fakeClock{t: start} }
func (f *fakeClock) get() time.Time           { return f.t }
func (f *fakeClock) advance(d time.Duration)  { f.t = f.t.Add(d) }
