// Package circuitbreaker 提供按 实例×方法 维度的熔断与半开恢复：
//   - closed：正常放行，连续失败达阈值转 open；
//   - open：一律拒绝 ErrCircuitOpen，冷却结束转 half-open；
//   - half-open：只放行有限个探测，任一失败立即退回 open，
//     全部探测成功才回到 closed。
package circuitbreaker

import (
	"sync"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
)

// StateName 熔断器状态。
type StateName string

const (
	StateClosed   StateName = "closed"
	StateOpen     StateName = "open"
	StateHalfOpen StateName = "half-open"
)

// Breaker 管理一个维度的熔断器。
type Breaker struct {
	mu  sync.Mutex
	key string

	state          StateName
	failures       int // closed 期连续失败数
	openedAt       time.Time
	probeInflight  int // half-open 期已发放的探测额度
	probeSucceeded int // half-open 期成功探测数

	threshold   int
	cooldown    time.Duration
	halfOpenMax int
}

// New 依据策略创建熔断器。
func New(key string, policy model.MethodPolicy) *Breaker {
	threshold := policy.CircuitThreshold
	if threshold <= 0 {
		threshold = 5
	}
	cooldown := policy.OpenCooldown
	if cooldown <= 0 {
		cooldown = 5 * time.Second
	}
	probes := policy.HalfOpenProbes
	if probes <= 0 {
		probes = 1
	}
	return &Breaker{
		key:         key,
		state:       StateClosed,
		threshold:   threshold,
		cooldown:    cooldown,
		halfOpenMax: probes,
	}
}

// maybeTransitionLocked 在读取决策前把过期的 open 推进到 half-open。
func (b *Breaker) maybeTransitionLocked(now time.Time) {
	if b.state == StateOpen && now.Sub(b.openedAt) >= b.cooldown {
		b.state = StateHalfOpen
		b.probeInflight = 0
		b.probeSucceeded = 0
	}
}

// Allow 判定此刻是否放行：
// open 期返回 ErrCircuitOpen；half-open 期探测额度用完同样拒绝，
// 但原因码带 half-open 标记，便于日志解释。
func (b *Breaker) Allow(now time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maybeTransitionLocked(now)
	switch b.state {
	case StateOpen:
		return model.ErrCircuitOpen
	case StateHalfOpen:
		if b.probeInflight >= b.halfOpenMax {
			return model.ErrCircuitOpen // 探测额度已满，其余请求继续快速失败
		}
		b.probeInflight++
		return nil
	default:
		return nil
	}
}

// RecordResult 记录一次尝试结果并驱动状态迁移。
func (b *Breaker) RecordResult(now time.Time, success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maybeTransitionLocked(now)
	switch b.state {
	case StateHalfOpen:
		if !success {
			// 半开探测失败：立即退回 open 并重新计时，绝不放行。
			b.toOpenLocked(now)
			return
		}
		b.probeSucceeded++
		if b.probeSucceeded >= b.halfOpenMax {
			b.state = StateClosed
			b.failures = 0
		}
	case StateClosed:
		if success {
			b.failures = 0
			return
		}
		b.failures++
		if b.failures >= b.threshold {
			b.toOpenLocked(now)
		}
	case StateOpen:
		// open 期理论上不会产生结果（Allow 已拒绝），忽略即可。
	}
}

func (b *Breaker) toOpenLocked(now time.Time) {
	b.state = StateOpen
	b.openedAt = now
	b.probeInflight = 0
	b.probeSucceeded = 0
}

// RecordAbandon 记录一次"没有拿到下游裁决"的尝试：上游取消、预算在发起前
// 耗尽、排空宽限期切断都属于此类——它们不证明下游故障，因此：
//   - 绝不计入连续失败、绝不触发/重置熔断；
//   - half-open 下回收此前 Allow 预占的探测额度，避免取消把探测槽永久吃掉。
func (b *Breaker) RecordAbandon(now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == StateHalfOpen && b.probeInflight > 0 {
		b.probeInflight--
	}
}

// State 返回当前状态（会先推进过期 open，供测试与日志解释）。
func (b *Breaker) State(now time.Time) StateName {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maybeTransitionLocked(now)
	return b.state
}

// SnapshotView 是熔断器某一时刻的自洽状态快照。
type SnapshotView struct {
	State          StateName
	Failures       int           // closed 期连续失败数
	CooldownLeft   time.Duration // open 期冷却剩余时间；其他状态/已过冷却为 0
	ProbeInflight  int           // half-open 期已发放且在途的探测额度
	ProbeSucceeded int           // half-open 期已成功的探测数
	HalfOpenMax    int
}

// Snapshot 返回当前状态与计数的自洽快照（先推进过期 open）。
func (b *Breaker) Snapshot(now time.Time) SnapshotView {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maybeTransitionLocked(now)
	v := SnapshotView{
		State:          b.state,
		Failures:       b.failures,
		ProbeInflight:  b.probeInflight,
		ProbeSucceeded: b.probeSucceeded,
		HalfOpenMax:    b.halfOpenMax,
	}
	if b.state == StateOpen {
		left := b.cooldown - now.Sub(b.openedAt)
		if left > 0 {
			v.CooldownLeft = left
		}
	}
	return v
}

// Key 返回维度键。
func (b *Breaker) Key() string { return b.key }
