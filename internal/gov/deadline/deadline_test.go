package deadline

import (
	"context"
	"testing"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/gerr"
)

func TestRemaining(t *testing.T) {
	if _, ok := Remaining(context.Background()); ok {
		t.Fatal("background ctx has no deadline")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	d, ok := Remaining(ctx)
	if !ok || d <= 0 || d > 100*time.Millisecond {
		t.Fatalf("bad remaining: %v ok=%v", d, ok)
	}
}

func TestChildBoundedByAttemptTimeout(t *testing.T) {
	ctx := context.Background()
	cctx, cancel, budget, err := Child(ctx, 50*time.Millisecond)
	defer cancel()
	if err != nil {
		t.Fatal(err)
	}
	if budget > 50*time.Millisecond {
		t.Fatalf("budget %s exceeds attempt timeout", budget)
	}
	<-cctx.Done()
	if cctx.Err() != context.DeadlineExceeded {
		t.Fatalf("want deadline exceeded, got %v", cctx.Err())
	}
}

func TestChildBoundedByRemainingParent(t *testing.T) {
	parent, pcancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer pcancel()
	cctx, cancel, _, err := Child(parent, time.Hour)
	defer cancel()
	if err != nil {
		t.Fatal(err)
	}
	<-cctx.Done()
	if cctx.Err() != context.DeadlineExceeded {
		t.Fatalf("want parent deadline to bound child, got %v", cctx.Err())
	}
}

func TestChildFailsFastWhenNoBudget(t *testing.T) {
	parent, pcancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer pcancel()
	time.Sleep(5 * time.Millisecond)
	_, cancel, _, err := Child(parent, time.Second)
	defer cancel()
	if err == nil || gerr.CodeOf(err) != gerr.CodeTimeoutBeforeDispatch {
		t.Fatalf("want timeout_before_dispatch, got %v", err)
	}
}

func TestChildPropagatesCancel(t *testing.T) {
	parent, pcancel := context.WithCancel(context.Background())
	cctx, cancel, _, err := Child(parent, time.Hour)
	defer cancel()
	if err != nil {
		t.Fatal(err)
	}
	pcancel()
	<-cctx.Done()
	if cctx.Err() != context.Canceled {
		t.Fatalf("want canceled, got %v", cctx.Err())
	}
}

func TestSleepInterruptible(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	if err := Sleep(ctx, time.Hour); err == nil || gerr.CodeOf(err) != gerr.CodeCanceled {
		t.Fatalf("want canceled sleep, got %v", err)
	}
}
