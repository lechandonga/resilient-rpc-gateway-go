// Package drain 提供进程级与实例级的优雅排空：
// 进入排空后不再接受新请求；已在途请求在剩余预算内继续，
// 宽限期用尽则统一按 GATEWAY_DRAIN_TIMEOUT 切断；
// Drain 返回后保证没有任何在途请求仍在推进。
package drain

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
)

// Phase 是排空生命周期阶段。
type Phase string

const (
	PhaseActive  Phase = "active"  // 正常服务，接受新请求
	PhaseDrain   Phase = "drain"   // 排空中：拒绝新请求，等待在途完成
	PhaseDrained Phase = "drained" // 排空结束：宽限到期或在途已清零，全部拒绝
)

// ErrInstanceDraining 是实例级排空的内部哨兵错误：
// 调用方（client）据此跳过该实例另选，不返回给上游。
var ErrInstanceDraining = errors.New("drain: instance is draining")

// ScopeView 是一个排空范围（全局或单个实例）在某一时刻的自洽进度快照。
type ScopeView struct {
	Phase     Phase
	Draining  bool          // 等价于 Phase != active
	StartedAt time.Time     // 进入 drain 的时刻
	Grace     time.Duration // 排空宽限期
	Deadline  time.Time     // 宽限截止时刻；零值表示无截止（grace<=0，只等在途结束）
	Elapsed   time.Duration // 已排空时长
	GraceLeft time.Duration // 宽限剩余（到期后归零）
	Inflight  int           // 已准入且尚未结束的在途请求（执行中+排队中+退避中）
	Rejected  int64         // 进入排空后被拒绝的新请求累计数
	TimedOut  bool          // 宽限是否已用尽（在途已被强制切断）
}

// entry 是一个在途请求在排空层的登记项。
type entry struct {
	id     uint64
	cancel context.CancelCauseFunc
}

// scope 是一个排空范围（全局或单个实例）的可变状态。
type scope struct {
	phase     Phase
	startedAt time.Time
	deadline  time.Time
	grace     time.Duration
	inflight  int
	rejected  int64
	timedOut  bool

	entries map[uint64]*entry
	wg      sync.WaitGroup
	done    chan struct{} // 排空结束信号（drain/drained 后关闭）
}

func newScope() *scope { return &scope{phase: PhaseActive, entries: map[uint64]*entry{}} }

// Coordinator 管理全局与实例级排空，并为每个请求派生可被排空切断的 ctx。
type Coordinator struct {
	mu  sync.Mutex
	now func() time.Time

	global *scope
	insts  map[string]*scope // key: service + "/" + instanceID
	nextID uint64
}

// New 创建排空协调器。
func New() *Coordinator {
	return &Coordinator{now: time.Now, global: newScope(), insts: map[string]*scope{}}
}

// WithClock 注入时钟（测试用）。
func (c *Coordinator) WithClock(now func() time.Time) *Coordinator {
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
	return c
}

func instKey(service, instanceID string) string { return service + "/" + instanceID }

// Admit 判定是否准入新请求；全局已进入排空则返回 ErrDrainRejected 并累计拒绝数。
func (c *Coordinator) Admit() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.global.phase != PhaseActive {
		c.global.rejected++
		return model.ErrDrainRejected
	}
	return nil
}

// IsDraining 报告全局是否已进入排空（drain 或 drained）。
func (c *Coordinator) IsDraining() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.global.phase != PhaseActive
}

// beginLocked 把一个范围推进到 drain；重复调用幂等（保留既有进度）。
func beginLocked(s *scope, now time.Time, grace time.Duration) {
	if s.phase != PhaseActive {
		return
	}
	s.phase = PhaseDrain
	s.startedAt = now
	s.grace = grace
	s.timedOut = false
	if grace > 0 {
		s.deadline = now.Add(grace)
	} else {
		s.deadline = time.Time{}
	}
	s.done = make(chan struct{})
}

// finishLocked 在在途清零或切断完成后收尾，幂等。
func finishLocked(s *scope, now time.Time) {
	if s.phase == PhaseDrained {
		return
	}
	s.phase = PhaseDrained
	if s.deadline.IsZero() || now.Before(s.deadline) {
		s.deadline = now
	}
	if s.done != nil {
		select {
		case <-s.done:
		default:
			close(s.done)
		}
	}
}

// cutLocked 宽限到期：以 ErrDrainTimeout 为 cause 取消该范围下全部在途请求。
func cutLocked(s *scope) {
	s.timedOut = true
	for _, e := range s.entries {
		e.cancel(model.ErrDrainTimeout)
	}
}

// awaitScope 在后台驱动一个范围的排空（用于非阻塞的 Begin* 入口）。
// 在途在宽限内自然清零即提前结束；否则宽限到期切断后再等全部退出。
func (c *Coordinator) awaitScope(s *scope, grace time.Duration) {
	allDone := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(allDone)
	}()
	if grace <= 0 {
		<-allDone
		c.mu.Lock()
		finishLocked(s, c.now())
		c.mu.Unlock()
		return
	}
	t := time.NewTimer(grace)
	select {
	case <-t.C:
		c.mu.Lock()
		cutLocked(s)
		c.mu.Unlock()
		<-allDone // 切断后仍等全部 goroutine 退出，保证无残留在途
	case <-allDone:
		t.Stop()
	}
	c.mu.Lock()
	finishLocked(s, c.now())
	c.mu.Unlock()
}

// BeginDrain 启动进程级排空（非阻塞）：立即拒绝新请求，宽限到期切断在途。
func (c *Coordinator) BeginDrain(grace time.Duration) {
	c.mu.Lock()
	if c.global.phase == PhaseActive {
		beginLocked(c.global, c.now(), grace)
		s := c.global
		c.mu.Unlock()
		go c.awaitScope(s, grace)
		return
	}
	c.mu.Unlock()
}

// runDrain 同步执行"启动-等待-收尾"，供 Drain / DrainInstance 使用。
func runDrain(c *Coordinator, s *scope, grace time.Duration) error {
	c.mu.Lock()
	if s.phase != PhaseActive {
		// 已有排空在进行/已结束：等待其结束并复刻结论。
		done := s.done
		timedOut := s.timedOut
		c.mu.Unlock()
		if done != nil {
			<-done
		}
		c.mu.Lock()
		timedOut = s.timedOut
		c.mu.Unlock()
		if timedOut {
			return model.ErrDrainTimeout
		}
		return nil
	}
	beginLocked(s, c.now(), grace)
	hasInflight := s.inflight > 0
	c.mu.Unlock()

	if grace <= 0 {
		// 不主动切断：只等在途在各自预算内自然结束。
		s.wg.Wait()
		c.mu.Lock()
		finishLocked(s, c.now())
		c.mu.Unlock()
		return nil
	}

	t := time.NewTimer(grace)
	defer t.Stop()
	allDone := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(allDone)
	}()

	if !hasInflight {
		c.mu.Lock()
		finishLocked(s, c.now())
		c.mu.Unlock()
		return nil
	}

	select {
	case <-allDone:
		c.mu.Lock()
		finishLocked(s, c.now())
		timedOut := s.timedOut
		c.mu.Unlock()
		if timedOut {
			return model.ErrDrainTimeout
		}
		return nil
	case <-t.C:
		c.mu.Lock()
		cutLocked(s)
		c.mu.Unlock()
		<-allDone // 必须等在途 goroutine 全部退出，Drain 返回后进程才能干净退出
		c.mu.Lock()
		finishLocked(s, c.now())
		c.mu.Unlock()
		return model.ErrDrainTimeout
	}
}

// Drain 启动进程级排空并阻塞至在途清零或宽限到期；返回后保证无在途请求。
//   - 在途在宽限内全部完成 -> nil；
//   - 宽限用尽并强制切断 -> model.ErrDrainTimeout（此时在途也已全部退出）。
func (c *Coordinator) Drain(grace time.Duration) error {
	c.mu.Lock()
	s := c.global
	c.mu.Unlock()
	return runDrain(c, s, grace)
}

// BeginInstanceDrain 标记单个实例排空（非阻塞），语义见 DrainInstance。
func (c *Coordinator) BeginInstanceDrain(service, instanceID string, grace time.Duration) {
	c.mu.Lock()
	key := instKey(service, instanceID)
	s := c.insts[key]
	if s == nil {
		s = newScope()
		c.insts[key] = s
	}
	start := s.phase == PhaseActive
	if start {
		beginLocked(s, c.now(), grace)
	}
	c.mu.Unlock()
	if start {
		go c.awaitScope(s, grace)
	}
}

// DrainInstance 标记实例排空并等待该实例的在途结束或宽限到期。
// 返回后该实例上不会再有请求在执行；实例不会被自动从注册中心删除，
// 由调用方决定后续摘除（排空期间 client 已不会再选中它）。
func (c *Coordinator) DrainInstance(service, instanceID string, grace time.Duration) error {
	c.mu.Lock()
	key := instKey(service, instanceID)
	s := c.insts[key]
	if s == nil {
		s = newScope()
		c.insts[key] = s
	}
	c.mu.Unlock()
	return runDrain(c, s, grace)
}

// IsInstanceDraining 报告实例是否处于 drain/drained。
func (c *Coordinator) IsInstanceDraining(service, instanceID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.insts[instKey(service, instanceID)]
	return s != nil && s.phase != PhaseActive
}

// Handle 是一个已准入请求在排空层的句柄；必须调用 Release 恰好一次。
type Handle struct {
	c       *Coordinator
	id      uint64
	ctx     context.Context
	cancel  context.CancelCauseFunc
	instKey string // 当前绑定的实例维度键，"" 表示尚未选中实例
}

// Context 返回绑定了排空切断语义的请求 ctx。
func (h *Handle) Context() context.Context { return h.ctx }

// Attach 把在途请求登记到所选实例上（每次选中/重试换实例时调用）：
//   - 实例已在排空 -> 返回 ErrInstanceDraining，不改变当前绑定，调用方应另选实例；
//   - 重试换到新实例时，在途计数从旧实例平移到新实例，绝无重复计数。
func (h *Handle) Attach(service, instanceID string) error {
	key := instKey(service, instanceID)
	c := h.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if s := c.insts[key]; s != nil && s.phase != PhaseActive {
		s.rejected++
		return ErrInstanceDraining
	}
	if h.instKey == key {
		return nil
	}
	if old := h.instKey; old != "" {
		if s := c.insts[old]; s != nil {
			if _, ok := s.entries[h.id]; ok {
				delete(s.entries, h.id)
				s.inflight--
				s.wg.Done()
			}
		}
	}
	s := c.insts[key]
	if s == nil {
		s = newScope()
		c.insts[key] = s
	}
	// 该实例尚未排空：其 wg 只有在进入 drain 后才会被 Wait，
	// 此后 Attach 会被上面的 phase 检查挡住，Add/Wait 不构成竞态。
	s.entries[h.id] = &entry{id: h.id, cancel: h.cancel}
	s.inflight++
	s.wg.Add(1)
	h.instKey = key
	return nil
}

// Release 在请求彻底结束（排队、退避、全部尝试均结束）后调用，幂等。
// 同时从全局与实例范围移除在途登记，计数立即回落。
func (h *Handle) Release() {
	c := h.c
	c.mu.Lock()
	if g := c.global; g != nil {
		if _, ok := g.entries[h.id]; ok {
			delete(g.entries, h.id)
			g.inflight--
			g.wg.Done()
		}
	}
	if k := h.instKey; k != "" {
		if s := c.insts[k]; s != nil {
			if _, ok := s.entries[h.id]; ok {
				delete(s.entries, h.id)
				s.inflight--
				s.wg.Done()
			}
		}
	}
	c.mu.Unlock()
	h.cancel(nil)
}

// Bind 为一个已准入请求登记全局在途，并派生可被排空切断的 ctx。
// 调用前应先通过 Admit；若全局恰好已进入排空则返回 ErrDrainRejected。
func (c *Coordinator) Bind(parent context.Context) (*Handle, error) {
	c.mu.Lock()
	if c.global.phase != PhaseActive {
		c.global.rejected++
		c.mu.Unlock()
		return nil, model.ErrDrainRejected
	}
	c.nextID++
	id := c.nextID
	ctx, cancel := context.WithCancelCause(parent)
	e := &entry{id: id, cancel: cancel}
	c.global.entries[id] = e
	c.global.inflight++
	c.global.wg.Add(1)
	c.mu.Unlock()
	return &Handle{c: c, id: id, ctx: ctx, cancel: cancel}, nil
}

func viewLocked(s *scope, now time.Time) ScopeView {
	v := ScopeView{
		Phase:     s.phase,
		Draining:  s.phase != PhaseActive,
		StartedAt: s.startedAt,
		Grace:     s.grace,
		Deadline:  s.deadline,
		Inflight:  s.inflight,
		Rejected:  s.rejected,
		TimedOut:  s.timedOut,
	}
	if s.phase != PhaseActive {
		v.Elapsed = now.Sub(s.startedAt)
		if !s.deadline.IsZero() {
			left := s.deadline.Sub(now)
			if left < 0 {
				left = 0
			}
			v.GraceLeft = left
		}
	}
	return v
}

// Snapshot 返回全局排空进度的自洽快照。
func (c *Coordinator) Snapshot() ScopeView {
	c.mu.Lock()
	defer c.mu.Unlock()
	return viewLocked(c.global, c.now())
}

// InstanceSnapshot 返回指定实例的排空进度；从未排空返回 active 视图。
func (c *Coordinator) InstanceSnapshot(service, instanceID string) ScopeView {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.insts[instKey(service, instanceID)]
	if s == nil {
		return ScopeView{Phase: PhaseActive}
	}
	return viewLocked(s, c.now())
}

// Instances 返回当前处于 drain/drained 的 "service/instanceID" 键快照。
func (c *Coordinator) Instances() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.insts))
	for k, s := range c.insts {
		if s.phase != PhaseActive {
			out = append(out, k)
		}
	}
	return out
}
