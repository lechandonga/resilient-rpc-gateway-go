package registry

import (
	"context"
	"errors"
	"time"
)

func contextBg() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

func waitUntil(timeout time.Duration, cond func() bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// makeProbe 把"当前是否健康"状态函数适配为 ProbeFunc。
func makeProbe(healthy func() bool) ProbeFunc {
	return func(ctx context.Context, _, _ string) error {
		if healthy() {
			return nil
		}
		return errors.New("probe failed")
	}
}
