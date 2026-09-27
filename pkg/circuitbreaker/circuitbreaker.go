// Package circuitbreaker 提供按 实例×方法 维度的熔断与半开恢复：
//   - closed：正常放行，连续失败达阈值转 open；
//   - open：一律拒绝 ErrCircuitOpen，冷却结束转 half-open；
//   - half-open：只放行有限个探测，任一失败立即退回 open，
//     全部探测成功才回到 closed。
package circuitbreaker

import (
	"strings"
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

// State 返回当前状态（会先推进过期 open，供测试与日志解释）。
func (b *Breaker) State(now time.Time) StateName {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maybeTransitionLocked(now)
	return b.state
}

// RecordAbandon 用于 Allow 已放行（占用了 half-open 探测名额）但下游调用
// 实际没有发生 / 未产生可信结果的情形：在等并发额度、排队、退避期间被取消，
// 或预算已耗尽时，归还探测名额但不改变熔断状态——取消绝不能被算作探测成功或失败，
// 也不能把半开名额永久泄漏。
func (b *Breaker) RecordAbandon(now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maybeTransitionLocked(now)
	if b.state == StateHalfOpen && b.probeInflight > 0 {
		b.probeInflight--
	}
}

// Stat 返回此刻熔断器的自洽统计（先推进过期 open）。
func (b *Breaker) Stat(now time.Time) model.BreakerStat {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maybeTransitionLocked(now)
	st := model.BreakerStat{
		Key:            b.key,
		State:          string(b.state),
		Failures:       b.failures,
		ProbeInflight:  b.probeInflight,
		ProbeSucceeded: b.probeSucceeded,
	}
	if i := strings.IndexByte(b.key, '#'); i >= 0 {
		st.InstanceID = b.key[:i]
		st.Method = b.key[i+1:]
	}
	if b.state == StateOpen {
		if left := b.cooldown - now.Sub(b.openedAt); left > 0 {
			st.CooldownLeft = left
		}
	}
	return st
}

// Key 返回维度键。
func (b *Breaker) Key() string { return b.key }
