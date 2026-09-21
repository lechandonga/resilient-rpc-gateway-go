package loadbalancer

import (
	"errors"
	"testing"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
)

func TestWeightedRRDistributionAndSmoothness(t *testing.T) {
	b, err := New(model.LBWeightedRoundRobin)
	if err != nil {
		t.Fatal(err)
	}
	cands := []model.Instance{
		{ID: "a", Weight: 5},
		{ID: "b", Weight: 1},
	}
	var seq []string
	counts := map[string]int{}
	for i := 0; i < 60; i++ {
		in, err := b.Pick(cands)
		if err != nil {
			t.Fatal(err)
		}
		seq = append(seq, in.ID)
		counts[in.ID]++
	}
	if counts["a"] != 50 || counts["b"] != 10 {
		t.Fatalf("expected 50/10 distribution, got %v", counts)
	}
	// 平滑性：b 不应连续出现两次
	for i := 1; i < len(seq); i++ {
		if seq[i] == "b" && seq[i-1] == "b" {
			t.Fatalf("weight-1 instance picked consecutively at %d: %v", i, seq)
		}
	}
}

func TestEmptyCandidates(t *testing.T) {
	b, _ := New(model.LBWeightedRoundRobin)
	if _, err := b.Pick(nil); !errors.Is(err, errEmptyCandidates) {
		t.Fatalf("expected empty candidate error, got %v", err)
	}
}

func TestRemovedInstanceStopsReceivingTraffic(t *testing.T) {
	b, _ := New(model.LBWeightedRoundRobin)
	cands := []model.Instance{{ID: "a", Weight: 1}, {ID: "b", Weight: 1}}
	for i := 0; i < 4; i++ {
		if _, err := b.Pick(cands); err != nil {
			t.Fatal(err)
		}
	}
	// b 下线后只能命中 a
	for i := 0; i < 10; i++ {
		in, err := b.Pick([]model.Instance{{ID: "a", Weight: 1}})
		if err != nil || in.ID != "a" {
			t.Fatalf("traffic must stick to alive instance, got %+v err=%v", in, err)
		}
	}
}

func TestUnknownKindRejected(t *testing.T) {
	if _, err := New(model.LBKind("magic")); err == nil {
		t.Fatal("unknown lb kind must be rejected")
	}
}
