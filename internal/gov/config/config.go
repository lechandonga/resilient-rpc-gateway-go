// Package config provides versioned governance policy configuration
// with:
//
//   - stable defaults when old configs omit newly introduced fields;
//   - strict parsing that REJECTS unknown fields and conflicting
//     settings instead of silently ignoring them;
//   - atomic hot reload with a monotonically increasing config version,
//     so requests processed before/after a reload are distinguishable
//     and reproducible (every Outcome records the config version it ran).
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/breaker"
	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/bulkhead"
	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/gerr"
	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/lb"
)

// CurrentConfigVersion is the newest understood config version.
const CurrentConfigVersion = 1

// Stable built-in defaults. They are part of the compatibility
// contract: an omitted field ALWAYS means this exact value.
const (
	DefaultAttemptTimeout    = 2 * time.Second
	DefaultTotalTimeout      = 5 * time.Second
	DefaultMaxAttempts       = 3
	DefaultInitialBackoff    = 100 * time.Millisecond
	DefaultMaxBackoff        = 1 * time.Second
	DefaultBackoffMultiplier = 2.0
	DefaultBreakerThreshold  = 5
	DefaultBreakerCooldown   = 5 * time.Second
	DefaultBreakerProbeOK    = 1
	DefaultMaxConcurrent     = 32
	DefaultMaxQueued         = 64
	DefaultQueueTimeout      = 500 * time.Millisecond
)

// Scope values for breakers and bulkheads.
const (
	ScopeInstance = "instance"
	ScopeMethod   = "method"
)

// RawPolicy is the on-wire JSON shape; pointer fields let us tell
// "absent" (apply default) from "explicitly set".
type RawPolicy struct {
	Version int    `json:"version"`
	Service string `json:"service"`

	LBPolicy *string `json:"lb_policy"`

	AttemptTimeout *Duration `json:"attempt_timeout"`
	TotalTimeout   *Duration `json:"total_timeout"`

	RetryEnabled      *bool     `json:"retry_enabled"`
	MaxAttempts       *int      `json:"max_attempts"`
	InitialBackoff    *Duration `json:"initial_backoff"`
	MaxBackoff        *Duration `json:"max_backoff"`
	BackoffMultiplier *float64  `json:"backoff_multiplier"`
	BackoffJitter     *bool     `json:"backoff_jitter"`

	BreakerEnabled          *bool     `json:"breaker_enabled"`
	BreakerScope            *string   `json:"breaker_scope"`
	BreakerFailureThreshold *int      `json:"breaker_failure_threshold"`
	BreakerOpenCooldown     *Duration `json:"breaker_open_cooldown"`
	BreakerProbeSuccess     *int      `json:"breaker_probe_success"`

	BulkheadScope *string   `json:"bulkhead_scope"`
	MaxConcurrent *int      `json:"max_concurrent"`
	MaxQueued     *int      `json:"max_queued"`
	QueueTimeout  *Duration `json:"queue_timeout"`
	QueuePolicy   *string   `json:"queue_policy"`
}

// Policy is the validated, normalized governance policy.
type Policy struct {
	Version int
	Service string

	LBPolicy lb.Policy

	AttemptTimeout time.Duration
	TotalTimeout   time.Duration

	RetryEnabled      bool
	MaxAttempts       int
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64
	BackoffJitter     bool

	BreakerEnabled bool
	BreakerScope   string
	BreakerCfg     breaker.Config

	BulkheadScope string
	BulkheadCfg   bulkhead.Config
}

// ParseJSON strictly parses config. Unknown fields are an error (typos
// and obsolete keys must surface, never be silently ignored).
func ParseJSON(raw []byte) (*RawPolicy, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var rp RawPolicy
	if err := dec.Decode(&rp); err != nil {
		return nil, gerr.Wrap(gerr.CodeConfigInvalid, "config parse error", err)
	}
	if dec.More() {
		return nil, gerr.New(gerr.CodeConfigInvalid, "config must contain exactly one JSON object")
	}
	return &rp, nil
}

// Normalize fills stable defaults for missing fields and validates the
// result. Unknown config versions older than CurrentConfigVersion are
// accepted (forward-from-old semantics: missing fields take defaults);
// a version from the future is rejected.
func Normalize(raw *RawPolicy) (*Policy, error) {
	if raw == nil {
		return nil, gerr.New(gerr.CodeConfigInvalid, "nil policy")
	}
	p := &Policy{Version: raw.Version, Service: raw.Service}
	if p.Version == 0 {
		// Explicit zero is treated as the oldest format.
		p.Version = 1
	}
	if p.Version > CurrentConfigVersion {
		return nil, gerr.New(gerr.CodeConfigInvalid,
			fmt.Sprintf("unsupported config version %d (highest supported: %d)", p.Version, CurrentConfigVersion))
	}

	// Load balancing.
	p.LBPolicy = lb.PolicyRoundRobin
	if raw.LBPolicy != nil {
		switch lb.Policy(*raw.LBPolicy) {
		case lb.PolicyRoundRobin, lb.PolicyWeighted, lb.PolicyRandom:
			p.LBPolicy = lb.Policy(*raw.LBPolicy)
		default:
			return nil, gerr.New(gerr.CodeConfigInvalid, "unknown lb_policy: "+*raw.LBPolicy)
		}
	}

	// Timeouts.
	p.AttemptTimeout = DefaultAttemptTimeout
	if raw.AttemptTimeout != nil {
		p.AttemptTimeout = raw.AttemptTimeout.Std()
	}
	p.TotalTimeout = DefaultTotalTimeout
	if raw.TotalTimeout != nil {
		p.TotalTimeout = raw.TotalTimeout.Std()
	}

	// Retry.
	p.RetryEnabled = true
	if raw.RetryEnabled != nil {
		p.RetryEnabled = *raw.RetryEnabled
	}
	p.MaxAttempts = DefaultMaxAttempts
	if raw.MaxAttempts != nil {
		p.MaxAttempts = *raw.MaxAttempts
	}
	p.InitialBackoff = DefaultInitialBackoff
	if raw.InitialBackoff != nil {
		p.InitialBackoff = raw.InitialBackoff.Std()
	}
	p.MaxBackoff = DefaultMaxBackoff
	if raw.MaxBackoff != nil {
		p.MaxBackoff = raw.MaxBackoff.Std()
	}
	p.BackoffMultiplier = DefaultBackoffMultiplier
	if raw.BackoffMultiplier != nil {
		p.BackoffMultiplier = *raw.BackoffMultiplier
	}
	p.BackoffJitter = true
	if raw.BackoffJitter != nil {
		p.BackoffJitter = *raw.BackoffJitter
	}

	// Breaker.
	p.BreakerEnabled = true
	if raw.BreakerEnabled != nil {
		p.BreakerEnabled = *raw.BreakerEnabled
	}
	p.BreakerScope = ScopeInstance
	if raw.BreakerScope != nil {
		p.BreakerScope = *raw.BreakerScope
	}
	p.BreakerCfg = breaker.Config{
		FailureThreshold: DefaultBreakerThreshold,
		OpenCooldown:     DefaultBreakerCooldown,
		ProbeSuccess:     DefaultBreakerProbeOK,
	}
	if raw.BreakerFailureThreshold != nil {
		p.BreakerCfg.FailureThreshold = *raw.BreakerFailureThreshold
	}
	if raw.BreakerOpenCooldown != nil {
		p.BreakerCfg.OpenCooldown = raw.BreakerOpenCooldown.Std()
	}
	if raw.BreakerProbeSuccess != nil {
		p.BreakerCfg.ProbeSuccess = *raw.BreakerProbeSuccess
	}

	// Bulkhead.
	p.BulkheadScope = ScopeInstance
	if raw.BulkheadScope != nil {
		p.BulkheadScope = *raw.BulkheadScope
	}
	p.BulkheadCfg = bulkhead.Config{
		MaxConcurrent: DefaultMaxConcurrent,
		MaxQueued:     DefaultMaxQueued,
		QueueTimeout:  DefaultQueueTimeout,
		Policy:        bulkhead.RejectWait,
	}
	if raw.MaxConcurrent != nil {
		p.BulkheadCfg.MaxConcurrent = *raw.MaxConcurrent
	}
	if raw.MaxQueued != nil {
		p.BulkheadCfg.MaxQueued = *raw.MaxQueued
	}
	if raw.QueueTimeout != nil {
		p.BulkheadCfg.QueueTimeout = raw.QueueTimeout.Std()
	}
	if raw.QueuePolicy != nil {
		switch bulkhead.RejectPolicy(*raw.QueuePolicy) {
		case bulkhead.RejectFast, bulkhead.RejectWait:
			p.BulkheadCfg.Policy = bulkhead.RejectPolicy(*raw.QueuePolicy)
		default:
			return nil, gerr.New(gerr.CodeConfigInvalid, "unknown queue_policy: "+*raw.QueuePolicy)
		}
	}

	if err := Validate(p); err != nil {
		return nil, err
	}
	return p, nil
}

// Validate rejects conflicting or out-of-range settings.
func Validate(p *Policy) error {
	bad := func(msg string) error { return gerr.New(gerr.CodeConfigInvalid, msg) }
	if p.Service == "" {
		return bad("service is required")
	}
	if p.AttemptTimeout < 0 {
		return bad("attempt_timeout must be >= 0")
	}
	if p.TotalTimeout < 0 {
		return bad("total_timeout must be >= 0")
	}
	if p.AttemptTimeout > 0 && p.TotalTimeout > 0 && p.AttemptTimeout > p.TotalTimeout {
		return bad("attempt_timeout must not exceed total_timeout")
	}
	if p.MaxAttempts < 1 {
		return bad("max_attempts must be >= 1")
	}
	// Explicit conflict: retry enabled but only one attempt allowed.
	if p.RetryEnabled && p.MaxAttempts == 1 {
		return bad("retry_enabled=true conflicts with max_attempts=1 (retry can never happen)")
	}
	if p.InitialBackoff < 0 || p.MaxBackoff < 0 {
		return bad("backoff durations must be >= 0")
	}
	if p.MaxBackoff > 0 && p.InitialBackoff > p.MaxBackoff {
		return bad("initial_backoff must not exceed max_backoff")
	}
	if p.BackoffMultiplier < 1 {
		return bad("backoff_multiplier must be >= 1")
	}
	if p.BreakerScope != ScopeInstance && p.BreakerScope != ScopeMethod {
		return bad("breaker_scope must be 'instance' or 'method'")
	}
	if p.BreakerCfg.FailureThreshold < 1 {
		return bad("breaker_failure_threshold must be >= 1")
	}
	if p.BreakerCfg.OpenCooldown < 0 {
		return bad("breaker_open_cooldown must be >= 0")
	}
	if p.BreakerCfg.ProbeSuccess < 1 {
		return bad("breaker_probe_success must be >= 1")
	}
	if p.BulkheadScope != ScopeInstance && p.BulkheadScope != ScopeMethod {
		return bad("bulkhead_scope must be 'instance' or 'method'")
	}
	if p.BulkheadCfg.MaxConcurrent < 1 {
		return bad("max_concurrent must be >= 1")
	}
	if p.BulkheadCfg.MaxQueued < 0 {
		return bad("max_queued must be >= 0")
	}
	if p.BulkheadCfg.QueueTimeout < 0 {
		return bad("queue_timeout must be >= 0")
	}
	return nil
}

// DefaultPolicy returns the stable built-in policy for a service.
func DefaultPolicy(service string) *Policy {
	p, err := Normalize(&RawPolicy{Service: service})
	if err != nil {
		panic(err) // built-in defaults always validate
	}
	return p
}

// Change is a hot-reload event.
type Change struct {
	Version int64
	Policy  *Policy
}

// Manager holds the current policy and broadcasts replacements.
type Manager struct {
	mu      sync.RWMutex
	current *Policy
	cfgVer  int64
	updates chan Change
}

// NewManager creates a manager seeded with an already validated policy.
func NewManager(initial *Policy) (*Manager, error) {
	if initial == nil {
		return nil, gerr.New(gerr.CodeConfigInvalid, "initial policy is nil")
	}
	if err := Validate(initial); err != nil {
		return nil, err
	}
	m := &Manager{
		current: initial,
		cfgVer:  1,
		updates: make(chan Change, 8),
	}
	return m, nil
}

// Snapshot returns the pinned policy and its monotonically increasing
// config version. Callers keep the returned pointer for the lifetime of
// one request so mid-flight behavior is stable even after a reload.
func (m *Manager) Snapshot() (*Policy, int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current, m.cfgVer
}

// Replace validates and atomically installs a new policy.
func (m *Manager) Replace(p *Policy) error {
	if err := Validate(p); err != nil {
		return err
	}
	m.mu.Lock()
	m.current = p
	m.cfgVer++
	ver := m.cfgVer
	ch := m.updates
	m.mu.Unlock()

	select {
	case ch <- Change{Version: ver, Policy: p}:
	default:
	}
	return nil
}

// ReplaceJSON parses, normalizes and atomically installs new config.
func (m *Manager) ReplaceJSON(raw []byte) (*Policy, error) {
	rp, err := ParseJSON(raw)
	if err != nil {
		return nil, err
	}
	p, err := Normalize(rp)
	if err != nil {
		return nil, err
	}
	if err := m.Replace(p); err != nil {
		return nil, err
	}
	return p, nil
}

// Updates returns the hot-reload event channel.
func (m *Manager) Updates() <-chan Change { return m.updates }
