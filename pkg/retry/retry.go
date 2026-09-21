// Package retry 实现带幂等门禁与可区分失败原因的重试控制：
// 只有显式声明幂等的请求允许重试；超过上限返回 ErrRetryExhausted。
package retry

import (
	"errors"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
)

// State 是单次请求的重试游标（每请求一个，非并发安全）。
type State struct {
	maxAttempts int
	baseBackoff time.Duration
}

// NewState 依据策略创建重试状态。
func NewState(policy model.MethodPolicy) *State {
	attempts := policy.MaxAttempts
	if attempts < 1 {
		attempts = 1
	}
	return &State{maxAttempts: attempts, baseBackoff: 10 * time.Millisecond}
}

// Allow 判定第 attempt 次尝试（attempt 从 1 开始）是否被允许：
//   - attempt==1 总是允许（首次调用）；
//   - 非幂等请求的任何后续尝试一律 ErrNonRetryable；
//   - 超过 MaxAttempts 返回 ErrRetryExhausted。
func (s *State) Allow(req *model.Request, attempt int) error {
	if attempt <= 1 {
		return nil
	}
	if req == nil || !req.Idempotent {
		return model.ErrNonRetryable
	}
	if attempt > s.maxAttempts {
		return model.ErrRetryExhausted
	}
	return nil
}

// MaxAttempts 返回总尝试上限。
func (s *State) MaxAttempts() int { return s.maxAttempts }

// NextBackoff 返回第 failedAttempt 次失败后、下一次尝试前的等待时长：
// 指数退避 base * 2^(failedAttempt-1)，封顶 1s。
// 调用方应把该等待放进可被上游 ctx 取消的定时器中。
func (s *State) NextBackoff(failedAttempt int) time.Duration {
	if failedAttempt < 1 {
		return 0
	}
	d := s.baseBackoff
	for i := 1; i < failedAttempt && d < time.Second; i++ {
		d *= 2
	}
	if d > time.Second {
		d = time.Second
	}
	return d
}

// IsExhausted 判断错误是否为重试耗尽（保留 errors.Is 语义）。
func IsExhausted(err error) bool { return errors.Is(err, model.ErrRetryExhausted) }
