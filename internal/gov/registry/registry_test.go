package registry

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestRegisterListDeregister(t *testing.T) {
	r := New()
	if len(r.List("s")) != 0 {
		t.Fatal("empty service should list nothing")
	}
	if err := r.Register("s", Instance{ID: "a", Healthy: true}); err != nil {
		t.Fatal(err)
	}
	if err := r.Register("s", Instance{ID: "b", Healthy: false}); err != nil {
		t.Fatal(err)
	}
	if got := r.List("s"); len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("List should hide unhealthy, got %+v", got)
	}
	if got := r.ListAll("s"); len(got) != 2 {
		t.Fatalf("ListAll should include both, got %d", len(got))
	}
	if err := r.SetHealth("s", "b", true); err != nil {
		t.Fatal(err)
	}
	if got := r.List("s"); len(got) != 2 {
		t.Fatalf("want 2 healthy, got %d", len(got))
	}
	if err := r.Deregister("s", "a"); err != nil {
		t.Fatal(err)
	}
	if got := r.ListAll("s"); len(got) != 1 {
		t.Fatalf("want 1 after dereg, got %d", len(got))
	}
	if err := r.Register("", Instance{ID: "x"}); err == nil {
		t.Fatal("empty service must error")
	}
}

func TestWeightZeroNormalizedToOne(t *testing.T) {
	r := New()
	if err := r.Register("s", Instance{ID: "a", Weight: 0, Healthy: true}); err != nil {
		t.Fatal(err)
	}
	got := r.ListAll("s")[0]
	if got.EffectiveWeight() != 1 {
		t.Fatalf("zero weight should normalize to 1, got %d", got.Weight)
	}
}

func TestSubscribeReceivesEvents(t *testing.T) {
	r := New()
	ch, unsub := r.Subscribe("s")
	defer unsub()
	if err := r.Register("s", Instance{ID: "a", Healthy: true}); err != nil {
		t.Fatal(err)
	}
	select {
	case u := <-ch:
		if u.Kind != UpdatePut || u.Instance.ID != "a" {
			t.Fatalf("unexpected event %+v", u)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive put event")
	}
	if err := r.SetHealth("s", "a", false); err != nil {
		t.Fatal(err)
	}
	select {
	case u := <-ch:
		if u.Kind != UpdateMarkHealth || u.Healthy {
			t.Fatalf("unexpected health event %+v", u)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive health event")
	}
}

func TestHealthCheckerUpdatesRegistry(t *testing.T) {
	r := New()
	if err := r.Register("s", Instance{ID: "a", Healthy: true}); err != nil {
		t.Fatal(err)
	}
	var probes int64
	hc := NewHealthChecker(r, 10*time.Millisecond, 50*time.Millisecond,
		func(ctx context.Context, inst Instance) bool {
			atomic.AddInt64(&probes, 1)
			return inst.ID == "a" && atomic.LoadInt64(&probes) < 2 // fail from 2nd probe on
		})
	hc.WatchService("s")
	time.Sleep(40 * time.Millisecond)
	hc.Stop()
	if got := r.List("s"); len(got) != 0 {
		t.Fatalf("failing health check must remove instance, got %+v", got)
	}
	if atomic.LoadInt64(&probes) < 2 {
		t.Fatal("expected repeated probes")
	}
}
