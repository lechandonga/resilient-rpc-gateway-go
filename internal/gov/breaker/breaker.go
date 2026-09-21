// Package breaker implements per-key circuit breakers with closed,
// open and half-open states. While open, requests fail fast without
// touching the downstream; half-open allows a bounded number of probe
// requests and either closes on success or falls back to open on
// failure — a failed probe is never allowed through.
package breaker

import (
	"sync"
	"time"
)

// State of a circuit breaker.
type State int

const (
	// StateClosed routes traffic normally.
	StateClosed State = iota
	// StateOpen rejects traffic until the cooldown elapses.
	StateOpen
	// StateHalfOpen allows a bounded probe; success closes, failure
	// re-opens.
	StateHalfOpen
)

// String renders the state.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

// Config for a breaker.
type Config struct {
	// FailureThreshold consecutive failures that trip the breaker.
	FailureThreshold int
	// OpenCooldown is the mandatory wait before a probe is allowed.
	OpenCooldown time.Duration
	// ProbeSuccess consecutive probe successes required to close.
	ProbeSuccess int
}

// Breaker is a single-key circuit breaker.
type Breaker struct {
	cfg Config
	now func() time.Time

	mu             sync.Mutex
	state          State
	consecFailures int
	probeSuccesses int
	probeInFlight  bool
	openedAt       time.Time
}

// New creates a breaker.
func New(cfg Config) *Breaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.OpenCooldown <= 0 {
		cfg.OpenCooldown = 5 * time.Second
	}
	if cfg.ProbeSuccess <= 0 {
		cfg.ProbeSuccess = 1
	}
	return &Breaker{cfg: cfg, now: time.Now, state: StateClosed}
}

// Allow reports whether a request may proceed. It also performs the
// open -> half-open transition once the cooldown has elapsed. Only one
// probe is admitted at a time.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case StateClosed:
		return true
	case StateOpen:
		if b.now().Sub(b.openedAt) >= b.cfg.OpenCooldown {
			b.state = StateHalfOpen
			b.probeSuccesses = 0
			b.probeInFlight = true
			return true
		}
		return false
	case StateHalfOpen:
		// Admit exactly one probe at a time.
		if b.probeInFlight {
			return false
		}
		b.probeInFlight = true
		return true
	default:
		return false
	}
}

// OnSuccess records a success. In half-open, enough consecutive probe
// successes close the breaker and reset counters.
func (b *Breaker) OnSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case StateHalfOpen:
		b.probeInFlight = false
		b.probeSuccesses++
		if b.probeSuccesses >= b.cfg.ProbeSuccess {
			b.toClosedLocked()
		}
	case StateClosed:
		b.consecFailures = 0
	}
}

// OnFailure records a failure. Consecutive failures in closed trip it
// open; ANY failure in half-open immediately sends it back to open.
func (b *Breaker) OnFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case StateHalfOpen:
		b.toOpenLocked()
	case StateClosed:
		b.consecFailures++
		if b.consecFailures >= b.cfg.FailureThreshold {
			b.toOpenLocked()
		}
	}
}

// State returns the current state, performing a time-based check so a
// stale open state is observed consistently.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == StateOpen && b.now().Sub(b.openedAt) >= b.cfg.OpenCooldown {
		// Do not transition here; Allow() owns the transition. Report
		// open so callers do not race probes.
		return StateOpen
	}
	return b.state
}

// ForceOpen trips the breaker (tests/diagnostics).
func (b *Breaker) ForceOpen() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.toOpenLocked()
}

func (b *Breaker) toOpenLocked() {
	b.state = StateOpen
	b.openedAt = b.now()
	b.probeSuccesses = 0
	b.probeInFlight = false
}

func (b *Breaker) toClosedLocked() {
	b.state = StateClosed
	b.consecFailures = 0
	b.probeSuccesses = 0
	b.probeInFlight = false
}

// Registry holds breakers keyed by instance id or "method/service".
type Registry struct {
	cfg Config

	mu       sync.Mutex
	breakers map[string]*Breaker
}

// NewRegistry creates a breaker registry.
func NewRegistry(cfg Config) *Registry {
	return &Registry{cfg: cfg, breakers: make(map[string]*Breaker)}
}

// Get returns the breaker for a key, creating it lazily.
func (r *Registry) Get(key string) *Breaker {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.breakers[key]
	if !ok {
		b = New(r.cfg)
		r.breakers[key] = b
	}
	return b
}

// Snapshot returns state of every breaker (for logs/tests).
func (r *Registry) Snapshot() map[string]State {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]State, len(r.breakers))
	for k, b := range r.breakers {
		out[k] = b.State()
	}
	return out
}

// Reset removes all breakers (used on config replacement).
func (r *Registry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.breakers = make(map[string]*Breaker)
}
