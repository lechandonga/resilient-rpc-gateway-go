// Package loadbalancer 在健康实例集合上做选择：
// 加权平滑轮询（默认，长期流量比例严格等于权重比）与随机两种。
package loadbalancer

import (
	"errors"
	"sync"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
)

var errEmptyCandidates = errors.New("loadbalancer: empty candidate set")

// Balancer 每次 Pick 基于当前健康实例快照选择一个实例。
type Balancer interface {
	Pick(candidates []model.Instance) (model.Instance, error)
	Name() string
}

// New 按策略类型构造负载均衡器；未知类型返回错误，绝不静默回退。
func New(kind model.LBKind) (Balancer, error) {
	switch kind {
	case model.LBWeightedRoundRobin, "":
		return &weightedRR{}, nil
	case model.LBRandom:
		return &randomLB{}, nil
	default:
		return nil, errors.New("loadbalancer: unknown lb kind: " + string(kind))
	}
}

// wrrEntry 是平滑加权轮询的每实例游标。
type wrrEntry struct {
	id      string
	weight  int
	current int
}

// weightedRR 实现 nginx 平滑加权轮询：
// 每次 current += weight，选 current 最大者，再 current -= totalWeight。
// 实例集合变化时以 id 对齐，保留游标，新增实例从 0 开始。
type weightedRR struct {
	mu      sync.Mutex
	entries map[string]*wrrEntry
}

func (w *weightedRR) Name() string { return string(model.LBWeightedRoundRobin) }

func (w *weightedRR) Pick(candidates []model.Instance) (model.Instance, error) {
	if len(candidates) == 0 {
		return model.Instance{}, errEmptyCandidates
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.entries == nil {
		w.entries = map[string]*wrrEntry{}
	}
	alive := map[string]bool{}
	total := 0
	for _, in := range candidates {
		weight := in.Weight
		if weight <= 0 {
			weight = 1
		}
		total += weight
		alive[in.ID] = true
		e, ok := w.entries[in.ID]
		if !ok {
			e = &wrrEntry{id: in.ID, weight: weight}
			w.entries[in.ID] = e
		} else {
			e.weight = weight
		}
	}
	// 清理已消失实例，避免无界增长。
	for id := range w.entries {
		if !alive[id] {
			delete(w.entries, id)
		}
	}

	var best *wrrEntry
	for _, in := range candidates {
		e := w.entries[in.ID]
		e.current += e.weight
		if best == nil || e.current > best.current {
			best = e
		}
	}
	best.current -= total
	for _, in := range candidates {
		if in.ID == best.id {
			return in, nil
		}
	}
	return model.Instance{}, errEmptyCandidates
}
