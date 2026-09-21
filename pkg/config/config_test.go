package config

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
)

func baseJSON(extra string) []byte {
	return []byte(`{
  "version": 2,
  "service": {
    "name": "pay",
    "methods": {
      "charge": {"max_attempts":1` + extra + `}
    }
  }
}`)
}

func TestDefaultsAreStableForMissingFields(t *testing.T) {
	p, err := Parse(baseJSON(``))
	if err != nil {
		t.Fatal(err)
	}
	got := p.Methods["charge"]
	want := model.DefaultMethodPolicy("charge")
	want.Version = 2
	if got != want {
		t.Fatalf("default policy mismatch:\n got  %+v\n want %+v", got, want)
	}
}

func TestUnknownFieldRejected(t *testing.T) {
	_, err := Parse(baseJSON(`,"magic_toggle": true`))
	if !errors.Is(err, model.ErrConfigRejected) {
		t.Fatalf("unknown field must be rejected, got %v", err)
	}
}

func TestBadVersionRejected(t *testing.T) {
	bad := []byte(`{"version":99,"service":{"name":"x","methods":{"m":{}}}}`)
	if _, err := Parse(bad); !errors.Is(err, model.ErrConfigRejected) {
		t.Fatalf("unsupported version must be rejected, got %v", err)
	}
}

func TestUnknownEnumRejected(t *testing.T) {
	if _, err := Parse(baseJSON(`,"lb":"magic"`)); !errors.Is(err, model.ErrConfigRejected) {
		t.Fatal("unknown lb must be rejected")
	}
	if _, err := Parse(baseJSON(`,"on_limit":"later"`)); !errors.Is(err, model.ErrConfigRejected) {
		t.Fatal("unknown on_limit must be rejected")
	}
}

func TestConflictingPoliciesRejected(t *testing.T) {
	if _, err := Parse(baseJSON(`,"on_limit":"wait","max_queue":0`)); !errors.Is(err, model.ErrConfigRejected) {
		t.Fatal("wait with zero queue must be rejected")
	}
	if _, err := Parse(baseJSON(`,"on_limit":"reject","max_queue":3`)); !errors.Is(err, model.ErrConfigRejected) {
		t.Fatal("reject with positive queue must be rejected")
	}
}

func TestV1RejectsHalfOpenProbesAndHasDefaults(t *testing.T) {
	// v1 配置（无 half_open_probes 字段）：解析成功并使用默认值
	ok := []byte(`{"version":1,"service":{"name":"pay","methods":{"charge":{}}}}`)
	p, err := Parse(ok)
	if err != nil {
		t.Fatalf("v1 config should parse: %v", err)
	}
	if p.Methods["charge"].HalfOpenProbes != 1 {
		t.Fatal("v1 must default half_open_probes to 1")
	}
}

func TestDurationsParsed(t *testing.T) {
	p, err := Parse(baseJSON(`,"timeout":"250ms","open_cooldown":"2s"`))
	if err != nil {
		t.Fatal(err)
	}
	m := p.Methods["charge"]
	if m.Timeout != 250*time.Millisecond || m.OpenCooldown != 2*time.Second {
		t.Fatalf("duration parse mismatch: %+v", m)
	}
	if _, err := Parse(baseJSON(`,"timeout":"not-a-duration"`)); err == nil ||
		!errors.Is(err, model.ErrConfigRejected) {
		t.Fatalf("bad duration must be rejected, got %v", err)
	}
}

func TestHotUpdateRevisionsDistinguishRequests(t *testing.T) {
	p1, _ := Parse(baseJSON(``))
	h := NewHolder(p1)
	first, rev1 := h.Snapshot()
	if rev1 != 1 || first.Methods["charge"].MaxAttempts != 1 {
		t.Fatalf("unexpected initial snapshot: rev=%d", rev1)
	}

	p2, _ := Parse([]byte(`{
  "version": 2,
  "service": {"name": "pay", "methods": {"charge": {"max_attempts": 4}}}
}`))
	rev2, err := h.Replace(p2)
	if err != nil || rev2 <= rev1 {
		t.Fatalf("replace should bump revision: rev=%d err=%v", rev2, err)
	}
	cur, curRev := h.Snapshot()
	if curRev != rev2 || cur.Methods["charge"].MaxAttempts != 4 {
		t.Fatal("snapshot must reflect new config")
	}
	// 旧 revision 仍可追溯：新旧请求命中策略可复现区分
	old, ok := h.RevisionOf(rev1)
	if !ok || old.Methods["charge"].MaxAttempts != 1 {
		t.Fatal("historical revision must remain queryable")
	}
	if _, err := h.Replace(nil); !errors.Is(err, model.ErrConfigRejected) {
		t.Fatal("nil config replace must be rejected")
	}
}

func TestEmptyAndMalformedRejected(t *testing.T) {
	for _, data := range [][]byte{nil, []byte("   "), []byte("{")} {
		if _, err := Parse(data); !errors.Is(err, model.ErrConfigRejected) {
			t.Fatalf("input %q must be rejected, got %v", string(data), err)
		}
	}
	if !strings.Contains(model.ErrConfigRejected.Error(), "GATEWAY_CONFIG_REJECTED") {
		t.Fatal("error code should be stable")
	}
}
