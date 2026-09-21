// Package deadline 统一管理上游截止时间与单次调用预算的传播：
// 每次尝试只拿到"上游剩余时间"与"单尝试预算"中更紧的一个。
package deadline

import (
	"context"
	"errors"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
)

// Propagation 绑定一次请求的上游截止时间与单尝试预算。
type Propagation struct {
	parentDeadline time.Time // 零值表示无上游截止时间
	perCall        time.Duration
	now            func() time.Time
}

// New 创建预算传播器：parentDeadline 为上游截止时间（零值表示无），
// perCall 为单次尝试最大耗时（<=0 时仅受上游截止时间约束）。
func New(parentDeadline time.Time, perCall time.Duration) *Propagation {
	return &Propagation{
		parentDeadline: parentDeadline,
		perCall:        perCall,
		now:            time.Now,
	}
}

// Remaining 返回当前剩余预算；ok=false 表示预算已经耗尽或上游已取消。
func (p *Propagation) Remaining() (time.Duration, bool) {
	d, ok := p.remainingAt(p.now())
	return d, ok
}

func (p *Propagation) remainingAt(now time.Time) (time.Duration, bool) {
	d := p.perCall
	if !p.parentDeadline.IsZero() {
		left := p.parentDeadline.Sub(now)
		if d <= 0 || left < d {
			d = left
		}
	}
	if d <= 0 {
		return 0, false
	}
	return d, true
}

// ChildContext 为一次下游尝试派生上下文：
// 超时取上游剩余预算与单尝试预算的更紧者；返回的 cancel 必须调用，
// 保证尝试结束（含因上游取消而中止）后立即释放下游连接与并发额度。
func (p *Propagation) ChildContext(parent context.Context) (context.Context, context.CancelFunc, time.Duration) {
	now := p.now()
	left, ok := p.remainingAt(now)
	if !ok {
		// 预算已耗尽：仍派生一个已到期上下文，调用方据此立即失败，
		// 不向下游发起真实占用。
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel, 0
	}
	ctx, cancel := context.WithTimeout(parent, left)
	return ctx, cancel, left
}

// Classify 把 context 的结束原因映射为可区分的治理错误：
// 上游主动取消 -> ErrContextCanceled；预算到期 -> ErrTimeout。
func Classify(ctx context.Context) error {
	switch {
	case errors.Is(ctx.Err(), context.Canceled):
		return model.ErrContextCanceled
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return model.ErrTimeout
	default:
		return nil
	}
}

// FromContext 提取上游 ctx 携带的截止时间（没有则返回零值）。
func FromContext(ctx context.Context) (time.Time, bool) {
	return ctx.Deadline()
}
