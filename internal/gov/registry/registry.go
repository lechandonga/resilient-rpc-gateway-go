// Package registry provides a local in-memory service registry with
// instance discovery, weight/health mutation, update subscriptions and
// active health checking.
package registry

import (
	"context"
	"sync"
	"time"
)

// Instance describes one downstream service instance.
type Instance struct {
	ID      string
	Address string
	// Weight is relative selection weight. Non-positive values are
	// normalized to 1 by callers; zero never means "exclude".
	Weight  int
	Healthy bool
	Meta    map[string]string
}

// EffectiveWeight returns the normalized positive selection weight.
func (i Instance) EffectiveWeight() int {
	if i.Weight <= 0 {
		return 1
	}
	return i.Weight
}

// UpdateKind enumerates registry mutation kinds.
type UpdateKind int

const (
	UpdatePut UpdateKind = iota
	UpdateMarkHealth
	UpdateRemove
)

func (k UpdateKind) String() string {
	switch k {
	case UpdatePut:
		return "put"
	case UpdateMarkHealth:
		return "mark_health"
	case UpdateRemove:
		return "remove"
	default:
		return "unknown"
	}
}

// Update is one registry change event.
type Update struct {
	Kind     UpdateKind
	Service  string
	Instance Instance
	Healthy  bool
}

type sub struct {
	ch chan Update
}

// Registry is the local in-memory registry.
type Registry struct {
	mu     sync.RWMutex
	svcs   map[string]map[string]Instance // service -> id -> instance
	subs   map[string]map[*sub]struct{}
	stopCh chan struct{}
}

// New creates an empty registry.
func New() *Registry {
	return &Registry{
		svcs: make(map[string]map[string]Instance),
		subs: make(map[string]map[*sub]struct{}),
	}
}

// Register adds or replaces an instance.
func (r *Registry) Register(service string, inst Instance) error {
	if service == "" || inst.ID == "" {
		return ErrInvalidInstance
	}
	if inst.Weight == 0 {
		inst.Weight = 1
	}
	r.mu.Lock()
	m := r.svcs[service]
	if m == nil {
		m = make(map[string]Instance)
		r.svcs[service] = m
	}
	m[inst.ID] = inst
	fans := append([]sub(nil), r.snapshotSubsLocked(service)...)
	r.mu.Unlock()

	r.fanout(fans, Update{Kind: UpdatePut, Service: service, Instance: inst, Healthy: inst.Healthy})
	return nil
}

// Deregister removes an instance. Removing a missing instance is not an
// error (removal is idempotent).
func (r *Registry) Deregister(service, id string) error {
	r.mu.Lock()
	inst, ok := r.removeLocked(service, id)
	if !ok {
		r.mu.Unlock()
		return nil
	}
	fans := append([]sub(nil), r.snapshotSubsLocked(service)...)
	r.mu.Unlock()

	r.fanout(fans, Update{Kind: UpdateRemove, Service: service, Instance: inst, Healthy: false})
	return nil
}

// SetHealth changes an instance health flag. Healthy=false causes the
// instance to disappear from List immediately.
func (r *Registry) SetHealth(service, id string, healthy bool) error {
	r.mu.Lock()
	m := r.svcs[service]
	inst, ok := m[id]
	if !ok {
		r.mu.Unlock()
		return ErrInstanceNotFound
	}
	if inst.Healthy == healthy {
		r.mu.Unlock()
		return nil
	}
	inst.Healthy = healthy
	m[id] = inst
	fans := append([]sub(nil), r.snapshotSubsLocked(service)...)
	r.mu.Unlock()

	r.fanout(fans, Update{Kind: UpdateMarkHealth, Service: service, Instance: inst, Healthy: healthy})
	return nil
}

// SetWeight changes an instance weight.
func (r *Registry) SetWeight(service, id string, weight int) error {
	if weight < 0 {
		return ErrInvalidInstance
	}
	r.mu.Lock()
	m := r.svcs[service]
	inst, ok := m[id]
	if !ok {
		r.mu.Unlock()
		return ErrInstanceNotFound
	}
	if weight == 0 {
		weight = 1
	}
	inst.Weight = weight
	m[id] = inst
	fans := append([]sub(nil), r.snapshotSubsLocked(service)...)
	r.mu.Unlock()

	r.fanout(fans, Update{Kind: UpdatePut, Service: service, Instance: inst, Healthy: inst.Healthy})
	return nil
}

// Get returns one instance by id.
func (r *Registry) Get(service, id string) (Instance, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m := r.svcs[service]
	inst, ok := m[id]
	return inst, ok
}

// List returns a snapshot of healthy instances of a service.
// It deliberately returns an empty slice when no instance is available;
// callers must distinguish "service unknown / zero instances" from
// "all instances unhealthy" via ListAll.
func (r *Registry) List(service string) []Instance {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Instance
	for _, inst := range r.svcs[service] {
		if inst.Healthy {
			out = append(out, inst)
		}
	}
	return out
}

// ListAll returns a snapshot of every instance including unhealthy ones.
func (r *Registry) ListAll(service string) []Instance {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Instance, 0, len(r.svcs[service]))
	for _, inst := range r.svcs[service] {
		out = append(out, inst)
	}
	return out
}

// Subscribe returns a buffered channel of registry updates for a
// service plus an unsubscribe function. The channel also receives a
// synthetic event when the service first has data so subscribers can
// initialize; slow subscribers drop events rather than block writers
// (governance state is recoverable from List snapshots).
func (r *Registry) Subscribe(service string) (<-chan Update, func()) {
	s := &sub{ch: make(chan Update, 16)}
	r.mu.Lock()
	set := r.subs[service]
	if set == nil {
		set = make(map[*sub]struct{})
		r.subs[service] = set
	}
	set[s] = struct{}{}
	// Build the initial snapshot while holding the lock (do NOT call
	// List here: it needs RLock and would self-deadlock).
	snapshot := make([]Instance, 0)
	for _, inst := range r.svcs[service] {
		if inst.Healthy {
			snapshot = append(snapshot, inst)
		}
	}
	r.mu.Unlock()

	for _, inst := range snapshot {
		s.ch <- Update{Kind: UpdatePut, Service: service, Instance: inst, Healthy: inst.Healthy}
	}
	return s.ch, func() {
		r.mu.Lock()
		if set, ok := r.subs[service]; ok {
			delete(set, s)
		}
		r.mu.Unlock()
	}
}

func (r *Registry) removeLocked(service, id string) (Instance, bool) {
	m := r.svcs[service]
	inst, ok := m[id]
	if !ok {
		return Instance{}, false
	}
	delete(m, id)
	if len(m) == 0 {
		delete(r.svcs, service)
	}
	return inst, true
}

func (r *Registry) snapshotSubsLocked(service string) []sub {
	set := r.subs[service]
	out := make([]sub, 0, len(set))
	for s := range set {
		out = append(out, *s)
	}
	return out
}

func (r *Registry) fanout(subs []sub, u Update) {
	for _, s := range subs {
		select {
		case s.ch <- u:
		default:
			// Drop rather than block; subscriber can resync via List.
		}
	}
}

// HealthChecker periodically probes registered instances and updates
// their health flags in the registry.
type HealthChecker struct {
	reg      *Registry
	probe    func(ctx context.Context, inst Instance) bool
	interval time.Duration
	timeout  time.Duration

	mu      sync.Mutex
	svcs    map[string]struct{}
	stopCh  chan struct{}
	stopped bool
	wg      sync.WaitGroup
}

// NewHealthChecker creates a health checker with probe intervals and
// per-probe timeouts.
func NewHealthChecker(reg *Registry, interval, perProbeTimeout time.Duration, probe func(ctx context.Context, inst Instance) bool) *HealthChecker {
	return &HealthChecker{
		reg:      reg,
		probe:    probe,
		interval: interval,
		timeout:  perProbeTimeout,
		svcs:     make(map[string]struct{}),
		stopCh:   make(chan struct{}),
	}
}

// WatchService adds a service to periodic health probing.
func (h *HealthChecker) WatchService(service string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopped {
		return
	}
	if _, ok := h.svcs[service]; ok {
		return
	}
	h.svcs[service] = struct{}{}
	h.wg.Add(1)
	go h.loop(service)
}

// Start is retained for interface symmetry; use WatchService.
func (h *HealthChecker) Start(ctx context.Context) {
	go func() {
		<-ctx.Done()
		h.Stop()
	}()
}

// Stop halts all health-check loops.
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

func (h *HealthChecker) loop(service string) {
	defer h.wg.Done()
	t := time.NewTicker(h.interval)
	defer t.Stop()
	h.checkOnce(service)
	for {
		select {
		case <-h.stopCh:
			return
		case <-t.C:
			h.checkOnce(service)
		}
	}
}

func (h *HealthChecker) checkOnce(service string) {
	instances := h.reg.ListAll(service)
	var wg sync.WaitGroup
	for _, inst := range instances {
		wg.Add(1)
		go func(inst Instance) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
			defer cancel()
			ok := false
			func() {
				defer func() { _ = recover() }()
				ok = h.probe(ctx, inst)
			}()
			_ = h.reg.SetHealth(service, inst.ID, ok)
		}(inst)
	}
	wg.Wait()
}
