package circuitbreaker

import (
	"sync"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
)

// Registry 按 key 持有全部熔断器。熔断器的身份是维度键（实例×方法），
// 配置热更新不重建熔断器，从而保留熔断状态；阈值仅在首次创建时生效。
type Registry struct {
	mu       sync.Mutex
	breakers map[string]*Breaker
}

// NewRegistry 创建熔断注册表。
func NewRegistry() *Registry {
	return &Registry{breakers: map[string]*Breaker{}}
}

// Get 取或建某维度的熔断器。
func (r *Registry) Get(key string, policy model.MethodPolicy) *Breaker {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.breakers[key]
	if !ok {
		b = New(key, policy)
		r.breakers[key] = b
	}
	return b
}

// Snapshot 返回当前各熔断器状态（供测试与日志）。
func (r *Registry) Snapshot(now time.Time) map[string]StateName {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]StateName, len(r.breakers))
	for k, b := range r.breakers {
		out[k] = b.State(now)
	}
	return out
}

// SnapshotDetailed 在同一把注册表锁下复制全部熔断器快照，
// 保证返回内容的成员集合与此刻一致，各熔断器内部字段各自自洽。
func (r *Registry) SnapshotDetailed(now time.Time) map[string]SnapshotView {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]SnapshotView, len(r.breakers))
	for k, b := range r.breakers {
		out[k] = b.Snapshot(now)
	}
	return out
}
