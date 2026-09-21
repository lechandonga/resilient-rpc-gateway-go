package config

import "sync"

// Holder 持有当前生效的策略版本，支持原子热更新。
// 每个 Parsed 带单调递增 revision：请求在开始时固定所读 revision，
// 因此热更新前后的请求可区分、可复现。
type Holder struct {
	mu       sync.RWMutex
	current  *Parsed
	revision uint64
	history  map[uint64]*Parsed // 保留历史版本，便于请求结束时追溯其实际命中的策略
}

// NewHolder 以首份配置初始化。
func NewHolder(first *Parsed) *Holder {
	h := &Holder{history: map[uint64]*Parsed{}}
	if first != nil {
		h.revision = 1
		h.current = first
		h.history[1] = first
	}
	return h
}

// Replace 整体替换配置并返回新 revision；nil 配置被拒绝。
func (h *Holder) Replace(next *Parsed) (uint64, error) {
	if next == nil {
		return 0, ErrNilConfig()
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.revision++
	h.current = next
	h.history[h.revision] = next
	return h.revision, nil
}

// Snapshot 原子读取当前策略与其 revision；请求周期内应只调用一次并复用结果。
func (h *Holder) Snapshot() (*Parsed, uint64) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.current, h.revision
}

// RevisionOf 查询某历史 revision 的策略快照（用于日志与测试复现）。
func (h *Holder) RevisionOf(rev uint64) (*Parsed, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	p, ok := h.history[rev]
	return p, ok
}
