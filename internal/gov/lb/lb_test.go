package lb

import (
	"context"
	"testing"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/gerr"
	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/registry"
)

func inst(ids ...string) []registry.Instance {
	out := make([]registry.Instance, 0, len(ids))
	for _, id := range ids {
		out = append(out, registry.Instance{ID: id, Weight: 1, Healthy: true})
	}
	return out
}

func TestRoundRobinDistribution(t *testing.T) {
	b := New(PolicyRoundRobin)
	pool := inst("a", "b", "c")
	seen := map[string]int{}
	for i := 0; i < 9; i++ {
		pick, err := b.Pick(context.Background(), pool)
		if err != nil {
			t.Fatal(err)
		}
		seen[pick.ID]++
	}
	for id, n := range seen {
		if n != 3 {
			t.Fatalf("instance %s picked %d times, want 3", id, n)
		}
	}
}

func TestEmptyPoolFailsWithDistinctCode(t *testing.T) {
	b := New(PolicyRoundRobin)
	_, err := b.Pick(context.Background(), nil)
	if err == nil || gerr.CodeOf(err) != gerr.CodeNoInstance {
		t.Fatalf("want no_instance, got %v", err)
	}
}

func TestFailureCooldownExcludesInstance(t *testing.T) {
	b := New(PolicyRoundRobin)
	b.SetCooldown(time.Hour)
	pool := inst("a", "b")
	b.MarkFailure("a")
	for i := 0; i < 10; i++ {
		pick, err := b.Pick(context.Background(), pool)
		if err != nil {
			t.Fatal(err)
		}
		if pick.ID == "a" {
			t.Fatal("cooling-down instance a must not be selected")
		}
	}
	b.MarkSuccess("a")
	hitA := false
	for i := 0; i < 10; i++ {
		pick, _ := b.Pick(context.Background(), pool)
		if pick.ID == "a" {
			hitA = true
		}
	}
	if !hitA {
		t.Fatal("a should return after MarkSuccess")
	}
}

func TestWeightedSelectionHonorsWeight(t *testing.T) {
	b := New(PolicyWeighted)
	light := registry.Instance{ID: "light", Weight: 1, Healthy: true}
	heavy := registry.Instance{ID: "heavy", Weight: 999, Healthy: true}
	var heavyN int
	for i := 0; i < 1000; i++ {
		pick, err := b.Pick(context.Background(), []registry.Instance{light, heavy})
		if err != nil {
			t.Fatal(err)
		}
		if pick.ID == "heavy" {
			heavyN++
		}
	}
	if heavyN < 950 {
		t.Fatalf("weighted selection ignored weights: heavy=%d/1000", heavyN)
	}
}
