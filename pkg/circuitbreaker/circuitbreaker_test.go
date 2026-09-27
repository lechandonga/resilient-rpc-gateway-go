package circuitbreaker

import (
	"errors"
	"testing"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
)

func cbPolicy() model.MethodPolicy {
	p := model.DefaultMethodPolicy("m")
	p.CircuitThreshold = 3
	p.OpenCooldown = 10 * time.Millisecond
	p.HalfOpenProbes = 1
	return p
}

func TestOpensAfterConsecutiveFailures(t *testing.T) {
	t0 := time.Unix(1000, 0)
	b := New("a#m", cbPolicy())
	for i := 0; i < 2; i++ {
		b.RecordResult(t0, false)
		if err := b.Allow(t0); err != nil {
			t.Fatalf("should still be closed at failure %d", i+1)
		}
	}
	b.RecordResult(t0, false) // 第 3 次失败 -> open
	if err := b.Allow(t0); !errors.Is(err, model.ErrCircuitOpen) {
		t.Fatalf("expected open rejection, got %v", err)
	}
	if b.State(t0) != StateOpen {
		t.Fatalf("expected open, got %s", b.State(t0))
	}
	// 成功不能在 open 期被记录（Allow 已挡住），冷却前始终拒绝
	b.RecordResult(t0.Add(5*time.Millisecond), true)
	if err := b.Allow(t0.Add(9 * time.Millisecond)); !errors.Is(err, model.ErrCircuitOpen) {
		t.Fatalf("must stay open during cooldown, got %v", err)
	}
}

func TestHalfOpenFailureGoesBackToOpen(t *testing.T) {
	t0 := time.Unix(2000, 0)
	b := New("a#m", cbPolicy())
	for i := 0; i < 3; i++ {
		b.RecordResult(t0, false)
	}
	// 冷却结束 -> half-open，放一个探测
	probeTime := t0.Add(10 * time.Millisecond)
	if err := b.Allow(probeTime); err != nil {
		t.Fatalf("half-open probe should be allowed: %v", err)
	}
	if b.State(probeTime) != StateHalfOpen {
		t.Fatalf("expected half-open, got %s", b.State(probeTime))
	}
	// 探测失败 -> 必须退回 open，而不是继续放行
	b.RecordResult(probeTime.Add(time.Millisecond), false)
	if b.State(probeTime.Add(2*time.Millisecond)) != StateOpen {
		t.Fatalf("failed probe must fall back to open, got %s", b.State(probeTime))
	}
	if err := b.Allow(probeTime.Add(2 * time.Millisecond)); !errors.Is(err, model.ErrCircuitOpen) {
		t.Fatalf("request after failed probe must be rejected, got %v", err)
	}
}

func TestHalfOpenSuccessCloses(t *testing.T) {
	t0 := time.Unix(3000, 0)
	b := New("a#m", cbPolicy())
	for i := 0; i < 3; i++ {
		b.RecordResult(t0, false)
	}
	t1 := t0.Add(10 * time.Millisecond)
	if err := b.Allow(t1); err != nil {
		t.Fatal(err)
	}
	b.RecordResult(t1, true)
	if b.State(t1) != StateClosed {
		t.Fatalf("successful probe should close, got %s", b.State(t1))
	}
}

func TestHalfOpenProbeQuotaLimited(t *testing.T) {
	t0 := time.Unix(4000, 0)
	p := cbPolicy()
	p.HalfOpenProbes = 1
	b := New("a#m", p)
	for i := 0; i < 3; i++ {
		b.RecordResult(t0, false)
	}
	t1 := t0.Add(10 * time.Millisecond)
	if err := b.Allow(t1); err != nil {
		t.Fatal(err)
	}
	if err := b.Allow(t1); !errors.Is(err, model.ErrCircuitOpen) {
		t.Fatalf("second probe must be rejected while first in flight, got %v", err)
	}
}

func TestRecordAbandonReturnsHalfOpenSlot(t *testing.T) {
	t0 := time.Unix(5000, 0)
	b := New("a#m", cbPolicy()) // threshold=3, cooldown=10ms, probes=1
	for i := 0; i < 3; i++ {
		b.RecordResult(t0, false)
	}
	t1 := t0.Add(10 * time.Millisecond)
	if err := b.Allow(t1); err != nil {
		t.Fatal(err)
	}
	// 探测名额已占用：第二个请求被拒
	if err := b.Allow(t1); !errors.Is(err, model.ErrCircuitOpen) {
		t.Fatalf("probe slot exhausted, got %v", err)
	}
	// 请求在打下游前被取消 -> 放弃探测，名额归还，状态不变
	b.RecordAbandon(t1)
	if b.State(t1) != StateHalfOpen {
		t.Fatalf("abandon must keep half-open, got %s", b.State(t1))
	}
	if err := b.Allow(t1.Add(time.Millisecond)); err != nil {
		t.Fatalf("abandoned slot must be reusable: %v", err)
	}
}

func TestRecordAbandonInClosedIsNoop(t *testing.T) {
	t0 := time.Unix(6000, 0)
	b := New("a#m", cbPolicy())
	b.RecordAbandon(t0)
	if err := b.Allow(t0); err != nil {
		t.Fatalf("closed breaker unaffected by abandon: %v", err)
	}
}

func TestStatReportsStateAndCooldownLeft(t *testing.T) {
	t0 := time.Unix(7000, 0)
	b := New("a#m", cbPolicy())
	for i := 0; i < 3; i++ {
		b.RecordResult(t0, false)
	}
	st := b.Stat(t0.Add(4 * time.Millisecond))
	if st.State != string(StateOpen) || st.InstanceID != "a" || st.Method != "m" {
		t.Fatalf("bad stat: %+v", st)
	}
	if st.CooldownLeft <= 0 || st.CooldownLeft > 10*time.Millisecond {
		t.Fatalf("cooldown left out of range: %v", st.CooldownLeft)
	}
	if st.Failures != 3 {
		t.Fatalf("failures=%d", st.Failures)
	}
}
