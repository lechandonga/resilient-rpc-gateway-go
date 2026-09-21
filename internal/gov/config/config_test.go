package config

import (
	"testing"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/gerr"
	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/lb"
)

func TestStableDefaultsForMinimalOldConfig(t *testing.T) {
	// Simulates the oldest possible config: only service is present.
	p, err := Normalize(&RawPolicy{Service: "svc"})
	if err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}
	if p.LBPolicy != lb.PolicyRoundRobin {
		t.Errorf("default lb=%s", p.LBPolicy)
	}
	if p.AttemptTimeout != DefaultAttemptTimeout || p.TotalTimeout != DefaultTotalTimeout {
		t.Errorf("default timeouts wrong: %s / %s", p.AttemptTimeout, p.TotalTimeout)
	}
	if !p.RetryEnabled || p.MaxAttempts != DefaultMaxAttempts {
		t.Errorf("default retry wrong: %v / %d", p.RetryEnabled, p.MaxAttempts)
	}
	if !p.BreakerEnabled || p.BreakerScope != ScopeInstance ||
		p.BreakerCfg.FailureThreshold != DefaultBreakerThreshold {
		t.Errorf("default breaker wrong: %+v", p.BreakerCfg)
	}
	if p.BulkheadCfg.MaxConcurrent != DefaultMaxConcurrent ||
		p.BulkheadCfg.MaxQueued != DefaultMaxQueued {
		t.Errorf("default bulkhead wrong: %+v", p.BulkheadCfg)
	}
}

func TestDefaultsAreStableAcrossNormalizations(t *testing.T) {
	a, _ := Normalize(&RawPolicy{Service: "x"})
	b, _ := Normalize(&RawPolicy{Service: "x"})
	if *a != *b {
		t.Fatal("default normalization must be deterministic")
	}
}

func TestUnknownFieldRejected(t *testing.T) {
	_, err := ParseJSON([]byte(`{"version":1,"service":"x","typo_timeout":"1s"}`))
	if err == nil || gerr.CodeOf(err) != gerr.CodeConfigInvalid {
		t.Fatalf("want config_invalid for unknown field, got %v", err)
	}
}

func TestFutureVersionRejected(t *testing.T) {
	_, err := Normalize(&RawPolicy{Version: CurrentConfigVersion + 1, Service: "x"})
	if err == nil || gerr.CodeOf(err) != gerr.CodeConfigInvalid {
		t.Fatalf("want config_invalid for future version, got %v", err)
	}
}

func TestConflictRetryEnabledWithSingleAttempt(t *testing.T) {
	one := 1
	enabled := true
	_, err := Normalize(&RawPolicy{Service: "x", RetryEnabled: &enabled, MaxAttempts: &one})
	if err == nil {
		t.Fatal("conflict retry_enabled + max_attempts=1 must be rejected")
	}
}

func TestAttemptGreaterThanTotalRejected(t *testing.T) {
	a := Duration(3 * time.Second)
	tot := Duration(time.Second)
	_, err := Normalize(&RawPolicy{Service: "x", AttemptTimeout: &a, TotalTimeout: &tot})
	if err == nil {
		t.Fatal("attempt_timeout > total_timeout must be rejected")
	}
}

func TestUnknownEnumRejected(t *testing.T) {
	bad := "magnetic"
	_, err := Normalize(&RawPolicy{Service: "x", LBPolicy: &bad})
	if err == nil {
		t.Fatal("unknown lb_policy must be rejected")
	}
}

func TestDurationStringAndNumber(t *testing.T) {
	p, err := ParseJSON([]byte(`{"version":1,"service":"x","attempt_timeout":"250ms"}`))
	if err != nil {
		t.Fatal(err)
	}
	norm, err := Normalize(p)
	if err != nil {
		t.Fatal(err)
	}
	if norm.AttemptTimeout != 250*time.Millisecond {
		t.Fatalf("string duration wrong: %s", norm.AttemptTimeout)
	}
	p2, err := ParseJSON([]byte(`{"version":1,"service":"x","attempt_timeout":250000000}`))
	if err != nil {
		t.Fatal(err)
	}
	norm2, _ := Normalize(p2)
	if norm2.AttemptTimeout != 250*time.Millisecond {
		t.Fatalf("numeric duration wrong: %s", norm2.AttemptTimeout)
	}
	if _, err := ParseJSON([]byte(`{"version":1,"service":"x","attempt_timeout":"not-a-duration"}`)); err == nil {
		t.Fatal("invalid duration string must be rejected")
	}
}

func TestHotReloadBumpsVersionAndOldRejected(t *testing.T) {
	m, err := NewManager(DefaultPolicy("x"))
	if err != nil {
		t.Fatal(err)
	}
	p1, v1 := m.Snapshot()
	if v1 != 1 || p1 == nil {
		t.Fatalf("initial snapshot wrong: v=%d", v1)
	}
	disabled := false
	one := 1
	newP, err := Normalize(&RawPolicy{
		Version: 1, Service: "x",
		RetryEnabled: &disabled, MaxAttempts: &one,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Replace(newP); err != nil {
		t.Fatal(err)
	}
	p2, v2 := m.Snapshot()
	if v2 != 2 || p2.RetryEnabled {
		t.Fatalf("reload not visible: v=%d retry=%v", v2, p2.RetryEnabled)
	}
	// An invalid replacement must NOT move the version or policy.
	bad := -5
	if err := m.Replace(&Policy{Service: "x", MaxAttempts: bad,
		BreakerScope: ScopeInstance, BulkheadScope: ScopeInstance,
		BreakerCfg:  DefaultPolicy("x").BreakerCfg,
		BulkheadCfg: DefaultPolicy("x").BulkheadCfg}); err == nil {
		t.Fatal("invalid Replace must error")
	}
	_, v3 := m.Snapshot()
	if v3 != 2 {
		t.Fatalf("failed reload changed version to %d", v3)
	}
}
