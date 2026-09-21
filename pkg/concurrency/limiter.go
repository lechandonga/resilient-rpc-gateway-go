// Package concurrency 提供按 实例×方法（或纯方法）维度的
// 并发额度与有界排队控制，保证在途请求数永不超过 MaxConcurrency+MaxQueue。
package concurrency

import (
	"context"
	"sync"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
)

type waiter struct {
	grant chan struct{} // 缓冲 1：令牌转移信号
}

type state struct {
	active int
	queue  []*waiter
}

// Limiter 按维度持有额度；所有 map 与切片均有界。
type Limiter struct {
	mu     sync.Mutex
	states map[string]*state
}

// New 创建限流器。
func New() *Limiter {
	return &Limiter{states: map[string]*state{}}
}

// Ticket 表示一次占用的执行额度，必须调用 Release 归还。
type Ticket struct {
	limiter *Limiter
	key     string
}

// Release 归还额度；若有排队者则把令牌直接转移给队首。
func (t *Ticket) Release() { t.limiter.release(t.key) }

// Acquire 按策略获取额度：
//   - reject（默认）：执行额度满 -> ErrConcurrencyLimited；不使用队列；
//   - wait：执行额度满时进入长度 MaxQueue 的有界队列（队列满 -> ErrQueueFull），
//     被唤醒后获得额度；ctx 取消 / 上游截止期间退出等待且不产生下游副作用。
func (l *Limiter) Acquire(ctx context.Context, key string, policy model.MethodPolicy) (*Ticket, error) {
	max := policy.MaxConcurrency
	if max <= 0 {
		max = 1
	}

	l.mu.Lock()
	s := l.states[key]
	if s == nil {
		s = &state{}
		l.states[key] = s
	}
	if s.active < max {
		s.active++
		l.mu.Unlock()
		return &Ticket{limiter: l, key: key}, nil
	}

	if policy.OnLimit != model.RejectWait || policy.MaxQueue <= 0 {
		l.mu.Unlock()
		return nil, model.ErrConcurrencyLimited
	}
	if len(s.queue) >= policy.MaxQueue {
		l.mu.Unlock()
		return nil, model.ErrQueueFull
	}
	w := &waiter{grant: make(chan struct{}, 1)}
	s.queue = append(s.queue, w)
	l.mu.Unlock()

	select {
	case <-w.grant:
		// 令牌已转移，active 名额由 release 方保留给本等待者。
		return &Ticket{limiter: l, key: key}, nil
	case <-ctx.Done():
		l.cancelWait(key, w)
		// 与 release 竞争：令牌可能已在退出排队的同时转入本 waiter。
		select {
		case <-w.grant:
			l.release(key) // 立即把意外获得的令牌向下传递
		default:
		}
		return nil, mapCtxErr(ctx)
	}
}

// cancelWait 从队列移除等待者；若已不在队列说明令牌正转移给它。
func (l *Limiter) cancelWait(key string, w *waiter) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.states[key]
	if s == nil {
		return false
	}
	for i, q := range s.queue {
		if q == w {
			s.queue = append(s.queue[:i], s.queue[i+1:]...)
			return true
		}
	}
	return false
}

func (l *Limiter) release(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.states[key]
	if s == nil || s.active == 0 {
		return
	}
	if len(s.queue) > 0 {
		w := s.queue[0]
		s.queue = s.queue[1:]
		// 令牌直接转移给队首：active 计数不变，绝无超发。
		select {
		case w.grant <- struct{}{}:
		default:
		}
		return
	}
	s.active--
	if s.active == 0 {
		delete(l.states, key) // 空闲维度及时清理，避免长期运行后 map 无界增长
	}
}

// InFlight 返回某维度在途请求数（执行中 + 排队中）。
func (l *Limiter) InFlight(key string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.states[key]
	if s == nil {
		return 0
	}
	return s.active + len(s.queue)
}

func mapCtxErr(ctx context.Context) error {
	if ctx.Err() == context.DeadlineExceeded {
		return model.ErrTimeout
	}
	return model.ErrContextCanceled
}
