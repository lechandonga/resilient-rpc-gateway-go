package gateway

import (
	"sync"

	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/breaker"
	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/bulkhead"
	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/config"
	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/lb"
	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/registry"
)

// governorSet is one *generation* of governors bound to one config
// version. Requests pin a generation on entry: even after a hot reload
// installs new breakers/limiters/balancers, in-flight requests keep
// using the generation they started with. Once a generation has zero
// pinned requests it is destroyed (unsubscribing registry watches), so
// memory never accumulates across reloads.
type governorSet struct {
	cfgVer int64
	policy *config.Policy

	breakers *breaker.Registry
	bh       *bulkhead.Bulkhead

	balMu     sync.Mutex
	balancers map[string]*lb.Balancer
	unsubs    map[string]func()
}

func newGovernorSet(p *config.Policy, ver int64, reg *registry.Registry) *governorSet {
	gs := &governorSet{
		cfgVer:    ver,
		policy:    p,
		breakers:  breaker.NewRegistry(p.BreakerCfg),
		bh:        bulkhead.New(p.BulkheadCfg),
		balancers: make(map[string]*lb.Balancer),
		unsubs:    make(map[string]func()),
	}
	// Warm the balancer and its registry subscription eagerly so
	// discovery state is current before the first Pick.
	gs.balancerFor(p.Service, reg)
	return gs
}

func (gs *governorSet) balancerFor(service string, reg *registry.Registry) *lb.Balancer {
	gs.balMu.Lock()
	defer gs.balMu.Unlock()
	b, ok := gs.balancers[service]
	if ok {
		return b
	}
	b = lb.New(gs.policy.LBPolicy)
	b.Update(reg.List(service))
	ch, unsub := reg.Subscribe(service)
	gs.balancers[service] = b
	gs.unsubs[service] = unsub
	go func() {
		for range ch {
			b.Update(reg.List(service))
		}
	}()
	return b
}

func (gs *governorSet) destroy() {
	gs.balMu.Lock()
	defer gs.balMu.Unlock()
	for _, u := range gs.unsubs {
		u()
	}
	gs.unsubs = nil
	gs.balancers = nil
}

// governorManager hands out per-config-version governor generations.
type governorManager struct {
	reg *registry.Registry

	mu      sync.Mutex
	sets    map[int64]*governorSet
	refs    map[int64]int
	current int64
}

func newGovernorManager(reg *registry.Registry) *governorManager {
	return &governorManager{
		reg:  reg,
		sets: make(map[int64]*governorSet),
		refs: make(map[int64]int),
	}
}

// pin returns the governor generation for the manager's current
// policy/config version and increments its reference count.
//
// The CURRENT version's generation is retained across requests (it owns
// breaker/limiter state that must accumulate). Once a newer config
// version supersedes it, the old generation is destroyed as soon as its
// last in-flight request unpins it.
func (m *governorManager) pin(cm *config.Manager) *governorSet {
	p, ver := cm.Snapshot()
	m.mu.Lock()
	defer m.mu.Unlock()
	gs, ok := m.sets[ver]
	if !ok {
		gs = newGovernorSet(p, ver, m.reg)
		m.sets[ver] = gs
	}
	m.current = ver
	m.refs[ver]++
	return gs
}

// unpin releases one reference. A generation with no references is
// destroyed only when it is no longer the current config version; the
// current generation stays alive so cross-request state (open circuits,
// failure counts) persists.
func (m *governorManager) unpin(gs *governorSet) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refs[gs.cfgVer]--
	if gs.cfgVer == m.current {
		return
	}
	if m.refs[gs.cfgVer] <= 0 {
		delete(m.refs, gs.cfgVer)
		delete(m.sets, gs.cfgVer)
		gs.destroy()
	}
}

// activeVersions is used by tests/diagnostics.
func (m *governorManager) activeVersions() []int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]int64, 0, len(m.sets))
	for v := range m.sets {
		out = append(out, v)
	}
	return out
}
