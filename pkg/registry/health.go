package registry

import (
	"context"
	"sync"
	"time"
)

// ProbeFunc 由使用方提供，对单个实例做健康探测；返回 error 视为失败。
type ProbeFunc func(ctx context.Context, instanceID, address string) error

// HealthChecker 周期性探测所有已注册实例：
// 连续失败达到阈值则标记不健康，后续成功一次即恢复健康。
type HealthChecker struct {
	reg           *Registry
	probe         ProbeFunc
	interval      time.Duration
	probeTimeout  time.Duration
	failThreshold int

	mu      sync.Mutex
	stopped bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewHealthChecker 创建健康检查器。
func NewHealthChecker(r *Registry, interval, probeTimeout time.Duration, failThreshold int, probe ProbeFunc) *HealthChecker {
	if failThreshold <= 0 {
		failThreshold = 1
	}
	if interval <= 0 {
		interval = time.Second
	}
	if probeTimeout <= 0 {
		probeTimeout = interval
	}
	return &HealthChecker{
		reg:           r,
		probe:         probe,
		interval:      interval,
		probeTimeout:  probeTimeout,
		failThreshold: failThreshold,
		stopCh:        make(chan struct{}),
	}
}

// Start 启动后台探测循环（重复调用安全）。
func (h *HealthChecker) Start(ctx context.Context) {
	h.mu.Lock()
	if h.stopped {
		h.mu.Unlock()
		return
	}
	h.wg.Add(1)
	h.mu.Unlock()

	go func() {
		defer h.wg.Done()
		fails := map[string]int{} // service/instance -> 连续失败计数
		ticker := time.NewTicker(h.interval)
		defer ticker.Stop()
		h.round(ctx, fails) // 立即先来一轮，加速测试
		for {
			select {
			case <-ctx.Done():
				return
			case <-h.stopCh:
				return
			case <-ticker.C:
				h.round(ctx, fails)
			}
		}
	}()
}

func (h *HealthChecker) round(ctx context.Context, fails map[string]int) {
	// 快照注册的服务集合（Registry 暂无 Service 列表 API，用空 key 轮询不可行，
	// 因此由注册表暴露 Services）。
	for _, svc := range h.reg.Services() {
		for _, in := range h.reg.List(svc) {
			key := svc + "/" + in.ID
			probeCtx, cancel := context.WithTimeout(ctx, h.probeTimeout)
			err := h.probe(probeCtx, in.ID, in.Address)
			cancel()
			if err != nil {
				fails[key]++
				if fails[key] == h.failThreshold && in.Healthy {
					h.reg.SetHealth(svc, in.ID, false)
				}
			} else {
				fails[key] = 0
				if !in.Healthy {
					h.reg.SetHealth(svc, in.ID, true)
				}
			}
		}
	}
}

// Stop 停止探测循环并等待退出，避免泄漏 goroutine。
func (h *HealthChecker) Stop() {
	h.mu.Lock()
	if h.stopped {
		h.mu.Unlock()
		return
	}
	h.stopped = true
	close(h.stopCh)
	h.mu.Unlock()
	h.wg.Wait()
}
