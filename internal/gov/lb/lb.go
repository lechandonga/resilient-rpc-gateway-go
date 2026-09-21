// Package lb provides load balancing over registry instances with
// per-instance failure cooldown so traffic never keeps flowing to a
// known-bad instance.
package lb

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/gerr"
	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/registry"
)

// Policy selects a balancing strategy.
type Policy string

const (
	PolicyRoundRobin Policy = "round_robin"
	PolicyWeighted   Policy = "weighted"
	PolicyRandom     Policy = "random"
)

// DefaultCooldown is how long a freshly failed instance is excluded.
const DefaultCooldown = 2 * time.Second

// Balancer picks instances for one service and reacts to outcomes.
type Balancer struct {
	policy     Policy
	cooldown   time.Duration
	now        func() time.Time
	rng        *rand.Rand
	randomSeed bool

	mu        sync.Mutex
	instances []registry.Instance
	rr        int
	failures  map[string]int
	downUntil map[string]time.Time
}

// New creates a balancer for the given policy.
func New(policy Policy) *Balancer {
	return &Balancer{
		policy:    policy,
		cooldown:  DefaultCooldown,
		now:       time.Now,
		rng:       rand.New(rand.NewSource(1)),
		failures:  make(map[string]int),
		downUntil: make(map[string]time.Time),
	}
}

// SetCooldown overrides the failure cooldown (mainly for tests).
func (b *Balancer) SetCooldown(d time.Duration) {
	b.cooldown = d
}

// Update replaces the instance snapshot from the registry.
func (b *Balancer) Update(instances []registry.Instance) {
	b.mu.Lock()
	defer b.mu.Unlock()
	cp := make([]registry.Instance, len(instances))
	copy(cp, instances)
	b.instances = cp
	alive := make(map[string]struct{}, len(instances))
	for _, inst := range instances {
		alive[inst.ID] = struct{}{}
	}
	for id := range b.failures {
		if _, ok := alive[id]; !ok {
			delete(b.failures, id)
		}
	}
	for id := range b.downUntil {
		if _, ok := alive[id]; !ok {
			delete(b.downUntil, id)
		}
	}
}

// Pick selects one healthy, non-cooling instance. When all candidates
// are in cooldown, the cooldown state is defensively cleared once and
// selection retried; with a truly empty candidate list it returns
// gerr.CodeNoInstance (the caller distinguishes no-instance reasons).
func (b *Balancer) Pick(ctx context.Context, instances []registry.Instance) (registry.Instance, error) {
	b.mu.Lock()
	if len(instances) > 0 {
		cp := make([]registry.Instance, len(instances))
		copy(cp, instances)
		b.instances = cp
	}
	eligible := b.eligibleLocked()
	if len(eligible) == 0 {
		// Defensive recovery: registry says instances exist but every
		// one was marked down. Clear stale cooldowns and retry once
		// rather than blackholing traffic indefinitely.
		b.downUntil = make(map[string]time.Time)
		eligible = b.eligibleLocked()
	}
	if len(eligible) == 0 {
		b.mu.Unlock()
		return registry.Instance{}, gerr.New(gerr.CodeNoInstance, "no eligible healthy instance")
	}
	inst := b.selectLocked(eligible)
	b.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return registry.Instance{}, gerr.Wrap(gerr.CodeCanceled, "canceled while picking instance", err)
	}
	return inst, nil
}

// MarkSuccess clears failure/cooldown state for an instance.
func (b *Balancer) MarkSuccess(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.failures, id)
	delete(b.downUntil, id)
}

// MarkFailure counts a failure; repeated failures put the instance into
// cooldown so it stops receiving traffic.
func (b *Balancer) MarkFailure(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures[id]++
	if b.failures[id] >= 1 {
		b.downUntil[id] = b.now().Add(b.cooldown)
	}
}

// TemporarilyDown excludes an instance for the cooldown period.
func (b *Balancer) TemporarilyDown(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.downUntil[id] = b.now().Add(b.cooldown)
}

func (b *Balancer) eligibleLocked() []registry.Instance {
	now := b.now()
	out := make([]registry.Instance, 0, len(b.instances))
	for _, inst := range b.instances {
		until, down := b.downUntil[inst.ID]
		if down && now.Before(until) {
			continue
		}
		if down {
			delete(b.downUntil, inst.ID)
		}
		out = append(out, inst)
	}
	return out
}

func (b *Balancer) selectLocked(pool []registry.Instance) registry.Instance {
	switch b.policy {
	case PolicyWeighted:
		total := 0
		for _, inst := range pool {
			total += inst.EffectiveWeight()
		}
		if total > 0 {
			n := b.rng.Intn(total)
			for _, inst := range pool {
				n -= inst.EffectiveWeight()
				if n < 0 {
					return inst
				}
			}
		}
	case PolicyRandom:
		return pool[b.rng.Intn(len(pool))]
	case PolicyRoundRobin:
		fallthrough
	default:
	}
	// Deterministic round robin over the current eligible set order
	// (registry map iteration is randomized upstream, fairness holds).
	inst := pool[b.rr%len(pool)]
	b.rr++
	return inst
}
