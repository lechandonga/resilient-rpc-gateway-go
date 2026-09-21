// Package registry 提供进程内本地注册中心：实例上下线、权重与健康状态维护。
package registry

import (
	"sync"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
)

type svcState struct {
	instances map[string]model.Instance
	version   uint64
	subs      map[uint64]chan struct{}
}

// Registry 是线程安全的本地注册中心。
type Registry struct {
	mu   sync.RWMutex
	svcs map[string]*svcState
}

// NewRegistry 创建空注册中心。
func NewRegistry() *Registry {
	return &Registry{svcs: map[string]*svcState{}}
}

func (r *Registry) stateLocked(service string) *svcState {
	s := r.svcs[service]
	if s == nil {
		s = &svcState{instances: map[string]model.Instance{}, subs: map[uint64]chan struct{}{}}
		r.svcs[service] = s
	}
	return s
}

func (r *Registry) bumpLocked(s *svcState) {
	s.version++
	for _, ch := range s.subs {
		// 非阻塞通知：订阅方错过的通知可通过 Version() 对账。
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Upsert 新增或更新实例（上线 / 权重变更）。更新后实例默认健康。
func (r *Registry) Upsert(service string, ins ...model.Instance) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.stateLocked(service)
	changed := false
	for _, in := range ins {
		if in.ID == "" {
			continue
		}
		if in.Weight <= 0 {
			in.Weight = 1
		}
		if _, ok := s.instances[in.ID]; ok {
			// 显式 upsert 视为重新上线：恢复健康，且始终发布变更。
			in.Healthy = true
			changed = true
		} else {
			in.Healthy = true
			changed = true
		}
		s.instances[in.ID] = in
	}
	if changed {
		r.bumpLocked(s)
	}
}

// Remove 摘除实例（下线）。流量不得再命中被摘除实例。
func (r *Registry) Remove(service, instanceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.svcs[service]
	if !ok {
		return
	}
	if _, ok := s.instances[instanceID]; ok {
		delete(s.instances, instanceID)
		r.bumpLocked(s)
	}
}

// SetHealth 标记实例健康状态；健康检查失败时置为 false。
func (r *Registry) SetHealth(service, instanceID string, healthy bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.svcs[service]
	if !ok {
		return
	}
	in, ok := s.instances[instanceID]
	if !ok || in.Healthy == healthy {
		return
	}
	in.Healthy = healthy
	s.instances[instanceID] = in
	r.bumpLocked(s)
}

// SetWeight 变更实例权重，立即影响后续负载均衡。
func (r *Registry) SetWeight(service, instanceID string, weight int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.svcs[service]
	if !ok {
		return
	}
	in, ok := s.instances[instanceID]
	if !ok || weight <= 0 {
		return
	}
	in.Weight = weight
	s.instances[instanceID] = in
	r.bumpLocked(s)
}

// snapshotLocked 复制实例切片，避免外部持引用观测内部状态。
func snapshotLocked(m map[string]model.Instance) []model.Instance {
	out := make([]model.Instance, 0, len(m))
	for _, in := range m {
		cp := in
		if in.Metadata != nil {
			cp.Metadata = make(map[string]string, len(in.Metadata))
			for k, v := range in.Metadata {
				cp.Metadata[k] = v
			}
		}
		out = append(out, cp)
	}
	return out
}

// List 返回服务当前的实例快照（含不健康实例）。
func (r *Registry) List(service string) []model.Instance {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.svcs[service]
	if !ok {
		return nil
	}
	return snapshotLocked(s.instances)
}

// ListHealthy 仅返回健康实例；空列表的语义由调用方区分：
// 服务从未注册 -> ErrNoInstance；存在但全不健康 -> ErrNoHealthyInstance。
func (r *Registry) ListHealthy(service string) []model.Instance {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.svcs[service]
	if !ok {
		return nil
	}
	out := make([]model.Instance, 0, len(s.instances))
	for _, in := range s.instances {
		if in.Healthy {
			out = append(out, in)
		}
	}
	return out
}

// Exists 报告服务是否曾注册过（即使实例全部不健康或已临时清空）。
func (r *Registry) Exists(service string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.svcs[service]
	return ok
}

// Version 返回服务实例列表的单调版本号，供变更订阅使用。
func (r *Registry) Version(service string) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.svcs[service]
	if !ok {
		return 0
	}
	return s.version
}

// Subscribe 订阅服务实例变更，返回通知通道与取消函数。
// 变更可能合并，消费者应重新 List 并以 Version 对账。
func (r *Registry) Subscribe(service string) (<-chan struct{}, func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.stateLocked(service)
	subID := nextSubID()
	ch := make(chan struct{}, 1)
	s.subs[subID] = ch
	cancel := func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if ss, ok := r.svcs[service]; ok {
			if c, ok := ss.subs[subID]; ok {
				delete(ss.subs, subID)
				close(c)
			}
		}
	}
	return ch, cancel
}

var subCounter struct {
	sync.Mutex
	n uint64
}

func nextSubID() uint64 {
	subCounter.Lock()
	defer subCounter.Unlock()
	subCounter.n++
	return subCounter.n
}

// Services 返回当前注册过的全部服务名。
func (r *Registry) Services() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.svcs))
	for name := range r.svcs {
		out = append(out, name)
	}
	return out
}
