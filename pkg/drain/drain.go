// Package drain 管理进程级优雅排空（graceful drain）生命周期：
//   - idle：正常服务，Admit 放行新请求；
//   - draining：Begin 后进入，拒绝所有新请求（GATEWAY_DRAINING），
//     已在途请求在各自剩余预算内继续执行，宽限期结束仍未完成的请求
//     通过强制取消上下文快速失败（GATEWAY_DRAIN_TIMEOUT）；
//   - complete：在途清零（干净退出）或宽限到期（强制收尾）。
//
// 计数与阶段切换在同一把锁内完成，Admit/Release/Snapshot 永远自洽。
package drain

import (
	"context"
	"sync"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
)

// Manager 是进程级排空管理器；零值不可用，请用 New 创建。
type Manager struct {
	mu       sync.Mutex
	phase    model.DrainPhase
	inFlight int
	started  time.Time
	deadline time.Time
	timedOut bool
	timer    *time.Timer

	forceCtx    context.Context
	forceCancel context.CancelCauseFunc
	doneCh      chan struct{}

	now func() time.Time
}

// New 创建排空管理器，初始为 idle 正常服务状态。
func New() *Manager {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &Manager{
		phase:       model.DrainIdle,
		forceCtx:    ctx,
		forceCancel: cancel,
		doneCh:      make(chan struct{}),
		now:         time.Now,
	}
}

// Admit 申请进入治理层。只有 idle 阶段放行；排空中一律拒绝，
// 拒绝不增加任何计数。
func (m *Manager) Admit() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.phase != model.DrainIdle {
		return model.ErrDraining
	}
	m.inFlight++
	return nil
}

// Release 在请求彻底结束时归还在途计数；排空阶段最后一个请求结束即完成排空。
// 必须与 Admit 一一配对。
func (m *Manager) Release() {
	m.mu.Lock()
	if m.inFlight > 0 {
		m.inFlight--
	}
	finished := m.phase == model.DrainDraining && m.inFlight == 0
	if finished {
		m.finishLocked(false)
	}
	m.mu.Unlock()
}

// Begin 进入排空状态：幂等，重复调用以第一次为准。
// grace<=0 表示不给宽限（立即强制收尾）。进入后排空宽限计时器即启动：
// 即使 Shutdown/Wait 尚未调用，宽限到期也会强制取消残留请求上下文，
// 保证进程内不会留下继续向前跑的调用。
func (m *Manager) Begin(grace time.Duration) {
	m.mu.Lock()
	if m.phase != model.DrainIdle {
		m.mu.Unlock()
		return
	}
	now := m.now()
	m.phase = model.DrainDraining
	m.started = now
	forceNow := false
	if grace > 0 {
		m.deadline = now.Add(grace)
		m.timer = time.AfterFunc(grace, m.forceExpire)
	}
	switch {
	case m.inFlight == 0:
		// 没有任何在途：立即干净完成。
		m.finishLocked(false)
	case grace <= 0:
		m.finishLocked(true)
		forceNow = true
	}
	m.mu.Unlock()
	if forceNow {
		m.forceCancel(model.ErrDrainTimeout)
	}
}

// forceExpire 由宽限计时器回调：标记超时并强制取消全部残留请求。
func (m *Manager) forceExpire() {
	m.mu.Lock()
	if m.phase != model.DrainDraining {
		m.mu.Unlock()
		return
	}
	m.finishLocked(true)
	m.mu.Unlock()
	m.forceCancel(model.ErrDrainTimeout)
}

// finishLocked 把阶段推进到 complete 并放行所有等待者。调用方持锁。
func (m *Manager) finishLocked(timedOut bool) {
	m.phase = model.DrainComplete
	m.timedOut = timedOut
	if m.timer != nil {
		m.timer.Stop()
		m.timer = nil
	}
	if timedOut {
		m.deadline = m.now()
	}
	// 幂等关闭 doneCh：在锁内单线程推进阶段，只会执行一次。
	select {
	case <-m.doneCh:
	default:
		close(m.doneCh)
	}
}

// Wait 阻塞直到排空完成（在途清零或宽限到期）。
func (m *Manager) Wait() { <-m.doneCh }

// Shutdown 等价于 Begin(grace) 后 Wait：先拒绝新请求，再等待在途请求
// 在宽限内跑完。返回结束时的排空快照（TimedOut 区分干净完成与强制收尾）。
func (m *Manager) Shutdown(grace time.Duration) model.DrainSnapshot {
	m.Begin(grace)
	m.Wait()
	return m.snapshot()
}

// ForceContext 返回排空强制上下文：宽限到期时以 ErrDrainTimeout 为 cause
// 被取消。调用方据此把排空信号联动到请求/下游上下文。
func (m *Manager) ForceContext() context.Context { return m.forceCtx }

// Snapshot 返回此刻排空状态的一致快照。
func (m *Manager) Snapshot() model.DrainSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotLocked()
}

func (m *Manager) snapshot() model.DrainSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotLocked()
}

func (m *Manager) snapshotLocked() model.DrainSnapshot {
	s := model.DrainSnapshot{
		Phase:     m.phase,
		StartedAt: m.started,
		Deadline:  m.deadline,
		InFlight:  m.inFlight,
		TimedOut:  m.timedOut,
	}
	if m.phase == model.DrainDraining && !m.deadline.IsZero() {
		if left := m.deadline.Sub(m.now()); left > 0 {
			s.GraceLeft = left
		}
	}
	return s
}
