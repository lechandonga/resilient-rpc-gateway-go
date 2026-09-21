package deadline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
)

func TestRemainingTighterOfParentAndPerCall(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	called := 0
	now := func() time.Time { called++; return base }

	// 上游只剩 20ms，单尝试预算 100ms -> 取 20ms
	p := New(base.Add(20*time.Millisecond), 100*time.Millisecond)
	p.now = now
	d, ok := p.Remaining()
	if !ok || d != 20*time.Millisecond {
		t.Fatalf("expected 20ms remaining, got %v ok=%v", d, ok)
	}

	// 没有上游截止时间 -> 取单尝试预算
	p2 := New(time.Time{}, 50*time.Millisecond)
	p2.now = now
	d2, ok := p2.Remaining()
	if !ok || d2 != 50*time.Millisecond {
		t.Fatalf("expected 50ms, got %v ok=%v", d2, ok)
	}

	// 上游已过期 -> 预算耗尽
	p3 := New(base.Add(-time.Millisecond), time.Second)
	p3.now = now
	if _, ok := p3.Remaining(); ok {
		t.Fatal("expired parent deadline must report exhausted")
	}
}

func TestChildContextExpiresOnTightBudget(t *testing.T) {
	ctx, cancel, budget := New(time.Now().Add(15*time.Millisecond), time.Second).ChildContext(context.Background())
	defer cancel()
	if budget > 15*time.Millisecond {
		t.Fatalf("child budget must not exceed parent remaining: %v", budget)
	}
	<-ctx.Done()
	if !errors.Is(Classify(ctx), model.ErrTimeout) {
		t.Fatalf("expected timeout classification, got %v", Classify(ctx))
	}
}

func TestClassifyCanceledVsTimeout(t *testing.T) {
	cctx, ccancel := context.WithCancel(context.Background())
	ccancel()
	if !errors.Is(Classify(cctx), model.ErrContextCanceled) {
		t.Fatal("canceled context must map to ErrContextCanceled")
	}

	dctx, dcancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer dcancel()
	<-dctx.Done()
	if !errors.Is(Classify(dctx), model.ErrTimeout) {
		t.Fatal("deadline context must map to ErrTimeout")
	}
}
