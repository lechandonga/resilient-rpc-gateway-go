package registry

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
)

func TestUpsertRemoveAndEmptyCauses(t *testing.T) {
	r := NewRegistry()

	// 服务从未注册 -> 空
	if got := r.ListHealthy("svc"); len(got) != 0 {
		t.Fatalf("expected empty, got %d", len(got))
	}
	if r.Exists("svc") {
		t.Fatal("service should not exist")
	}

	r.Upsert("svc",
		model.Instance{ID: "a", Address: "127.0.0.1:9001", Weight: 2},
		model.Instance{ID: "b", Address: "127.0.0.1:9002", Weight: 1},
	)
	if got := r.ListHealthy("svc"); len(got) != 2 {
		t.Fatalf("expected 2 healthy, got %d", len(got))
	}

	// 健康检查失败：全部不健康，但服务仍存在 -> 与"从未注册"可区分
	r.SetHealth("svc", "a", false)
	r.SetHealth("svc", "b", false)
	if got := r.ListHealthy("svc"); len(got) != 0 {
		t.Fatalf("expected 0 healthy after failures, got %d", len(got))
	}
	if !r.Exists("svc") {
		t.Fatal("service should still exist when all unhealthy")
	}

	// 下线摘除后 List 也不可见
	r.Remove("svc", "a")
	all := r.List("svc")
	for _, in := range all {
		if in.ID == "a" {
			t.Fatal("removed instance must not appear in List")
		}
	}
}

func TestWeightChangeIsImmediate(t *testing.T) {
	r := NewRegistry()
	r.Upsert("svc", model.Instance{ID: "a", Weight: 1})
	r.SetWeight("svc", "a", 9)
	got := r.ListHealthy("svc")
	if len(got) != 1 || got[0].Weight != 9 {
		t.Fatalf("weight update not visible: %+v", got)
	}
	if v1, v2 := r.Version("svc"), r.Version("svc"); v1 != v2 {
		t.Fatal("version should be stable without changes")
	}
}

func TestSubscribeNotified(t *testing.T) {
	r := NewRegistry()
	ch, cancel := r.Subscribe("svc")
	defer cancel()
	r.Upsert("svc", model.Instance{ID: "a", Weight: 1})
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("subscriber not notified")
	}
}

func TestHealthCheckerMarksDownAndRecovers(t *testing.T) {
	r := NewRegistry()
	r.Upsert("svc", model.Instance{ID: "a", Address: "addr"})

	var healthy atomic.Bool
	healthy.Store(true)

	// failThreshold=2：第一次失败不摘，第二次摘
	hc := NewHealthChecker(r, time.Millisecond, 50*time.Millisecond, 2,
		makeProbe(func() bool { return healthy.Load() }))
	ctx, stop := contextBg()
	defer stop()
	hc.Start(ctx)
	defer hc.Stop()

	healthy.Store(false)
	waitUntil(time.Second, func() bool { return len(r.ListHealthy("svc")) == 0 })
	if len(r.ListHealthy("svc")) != 0 {
		t.Fatal("instance should be marked unhealthy after consecutive probe failures")
	}

	healthy.Store(true)
	waitUntil(time.Second, func() bool { return len(r.ListHealthy("svc")) == 1 })
	if len(r.ListHealthy("svc")) != 1 {
		t.Fatal("instance should recover after probe succeeds")
	}
}

func TestDrainExcludesFromHealthy(t *testing.T) {
	r := NewRegistry()
	r.Upsert("svc",
		model.Instance{ID: "a", Address: "a:1", Weight: 1},
		model.Instance{ID: "b", Address: "b:1", Weight: 1},
	)
	r.Drain("svc", "b")
	if got := r.ListHealthy("svc"); len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("draining instance must not be picked, got %+v", got)
	}
	all := r.List("svc")
	var b *model.Instance
	for i := range all {
		if all[i].ID == "b" {
			b = &all[i]
		}
	}
	if b == nil || !b.Draining {
		t.Fatalf("drained instance must remain listed with Draining=true, got %+v", all)
	}
	// Undrain / 重新上线恢复可路由
	r.Undrain("svc", "b")
	if got := r.ListHealthy("svc"); len(got) != 2 {
		t.Fatalf("undrain must restore routing, got %d", len(got))
	}
	// 再次排空后显式 upsert 也应清除标记
	r.Drain("svc", "b")
	r.Upsert("svc", model.Instance{ID: "b", Address: "b:2", Weight: 2})
	if got := r.ListHealthy("svc"); len(got) != 2 {
		t.Fatalf("upsert must clear draining flag, got %d", len(got))
	}
	// 统计快照包含排空标记
	stats := r.Stats()
	if len(stats) != 2 || stats[0].ID != "a" || stats[1].ID != "b" {
		t.Fatalf("stats must be sorted by id: %+v", stats)
	}
}

func TestStatsSortedAcrossServices(t *testing.T) {
	r := NewRegistry()
	r.Upsert("zeta", model.Instance{ID: "z", Weight: 1})
	r.Upsert("alpha", model.Instance{ID: "a", Weight: 1})
	stats := r.Stats()
	if len(stats) != 2 || stats[0].Service != "alpha" || stats[1].Service != "zeta" {
		t.Fatalf("stats must be sorted by service: %+v", stats)
	}
}
