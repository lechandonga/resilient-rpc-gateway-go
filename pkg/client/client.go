// Package client 组装服务发现、负载均衡、截止时间传播、
// 幂等重试、熔断与并发控制，给出一次 RPC 调用确定且可解释的完整行为。
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/circuitbreaker"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/concurrency"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/config"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/deadline"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/loadbalancer"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/registry"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/retry"
)

// Invoker 是可注入的下游调用（测试用假下游模拟抖动 / 超时 / 副作用）。
type Invoker func(ctx context.Context, ins model.Instance, req *model.Request) (*model.Response, error)

// Client 是治理层入口。
type Client struct {
	reg      *registry.Registry
	holder   *config.Holder
	invoker  Invoker
	limiters *concurrency.Limiter
	breakers *circuitbreaker.Registry
	tracer   *Tracer
	now      func() time.Time

	bals atomic.Pointer[lbCache]
}

// lbCache 缓存 (revision, lbKind) -> Balancer，配置热更新后按新 revision 重建。
type lbCache struct {
	revision uint64
	bals     map[string]loadbalancer.Balancer // 仅一种 kind 时也安全
}

// Option 构造可选项。
type Option func(*Client)

// WithLogger 打开逐条决策日志（请求标识 / 命中策略 / 下游响应）。
func WithLogger(out io.Writer) Option {
	return func(c *Client) { c.tracer = NewTracer(out, 256, true) }
}

// WithClock 注入时钟（测试用）。
func WithClock(now func() time.Time) Option {
	return func(c *Client) { c.now = now }
}

// New 组装治理层客户端；invoker 不可为 nil。
func New(reg *registry.Registry, holder *config.Holder, invoker Invoker, opts ...Option) (*Client, error) {
	if reg == nil {
		return nil, errors.New("client: registry is required")
	}
	if holder == nil {
		return nil, errors.New("client: config holder is required")
	}
	if invoker == nil {
		return nil, errors.New("client: invoker is required")
	}
	c := &Client{
		reg:      reg,
		holder:   holder,
		invoker:  invoker,
		limiters: concurrency.New(),
		breakers: circuitbreaker.NewRegistry(),
		tracer:   NewTracer(io.Discard, 256, false),
		now:      time.Now,
	}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

func (c *Client) balancer(rev uint64, method string, kind model.LBKind) (loadbalancer.Balancer, error) {
	if cache := c.bals.Load(); cache != nil && cache.revision == rev {
		if b, ok := cache.bals[method]; ok {
			return b, nil
		}
	}
	b, err := loadbalancer.New(kind)
	if err != nil {
		return nil, err
	}
	cache := &lbCache{revision: rev, bals: map[string]loadbalancer.Balancer{method: b}}
	if old := c.bals.Load(); old != nil && old.revision == rev {
		// 同 revision 下并发首建：合并而不是互相覆盖。
		for m, lb := range old.bals {
			cache.bals[m] = lb
		}
	}
	c.bals.Store(cache)
	return b, nil
}

func govCode(err error) string {
	var ge *model.GovError
	if errors.As(err, &ge) {
		return ge.Code
	}
	if err == nil {
		return ""
	}
	return "DOWNSTREAM_ERROR"
}

// Invoke 执行完整治理流程。返回的 Response 在失败时也可能非 nil，
// 用于携带尝试次数与原因；调用方应以 error 的原因码为准。
func (c *Client) Invoke(ctx context.Context, req *model.Request) (*model.Response, error) {
	now := c.now()
	parsed, rev := c.holder.Snapshot()
	if parsed == nil {
		return nil, fmt.Errorf("%w: no active policy", model.ErrConfigRejected)
	}
	if req == nil || req.Service == "" || req.Method == "" {
		return nil, fmt.Errorf("%w: request requires service and method", model.ErrConfigRejected)
	}
	policy, ok := parsed.Methods[req.Method]
	if !ok {
		return nil, fmt.Errorf("%w: method %q has no policy (revision %d)", model.ErrConfigRejected, req.Method, rev)
	}

	lb, err := c.balancer(rev, req.Method, policy.LB)
	if err != nil {
		return nil, err
	}
	rstate := retry.NewState(policy)

	// 上游截止时间 + 方法单尝试预算：更紧者生效。
	parentDL, _ := deadline.FromContext(ctx)
	prop := deadline.New(parentDL, policy.Timeout)

	resp := &model.Response{Reason: "UNKNOWN"}
	var lastErr error
	var exhausted bool

	for attempt := 1; attempt <= rstate.MaxAttempts(); attempt++ {
		now = c.now()
		if err := rstate.Allow(req, attempt); err != nil {
			// 非幂等请求尝试重试：直接给出可区分原因。
			c.tracer.Emit(TraceLine{At: c.now(), RequestID: req.ID, Revision: rev, Attempt: attempt,
				Policy: "retry", Outcome: "reject", Reason: govCode(err)})
			return resp, err
		}

		// 每轮重新发现：实例上下线 / 健康变化立刻生效。
		candidates := c.reg.ListHealthy(req.Service)
		if len(candidates) == 0 {
			reason := model.ErrNoInstance
			if c.reg.Exists(req.Service) {
				reason = model.ErrNoHealthyInstance
			}
			c.tracer.Emit(TraceLine{At: c.now(), RequestID: req.ID, Revision: rev, Attempt: attempt,
				Policy: "discovery", Outcome: "reject", Reason: reason.Code})
			return resp, reason
		}

		ins, err := lb.Pick(candidates)
		if err != nil {
			return resp, err
		}

		cbKey := ins.ID + "#" + req.Method
		brk := c.breakers.Get(cbKey, policy)
		if err := brk.Allow(now); err != nil {
			c.tracer.Emit(TraceLine{At: c.now(), RequestID: req.ID, Revision: rev, Attempt: attempt,
				InstanceID: ins.ID, Policy: "circuit:" + string(brk.State(now)),
				Outcome: "reject", Reason: govCode(err)})
			lastErr = err
			// 熔断是实例级故障：换其他实例继续尝试（仍受重试上限与幂等门禁约束）。
			if attempt < rstate.MaxAttempts() {
				if werr := c.waitRetryBudget(ctx, rstate, attempt, prop, rev, req.ID); werr != nil {
					return resp, werr
				}
			}
			continue
		}

		// 单实例×方法维度的并发额度。
		limitKey := ins.ID + "#" + req.Method
		ticket, err := c.limiters.Acquire(ctx, limitKey, policy)
		if err != nil {
			c.tracer.Emit(TraceLine{At: c.now(), RequestID: req.ID, Revision: rev, Attempt: attempt,
				InstanceID: ins.ID, Policy: "concurrency", Outcome: "reject", Reason: govCode(err)})
			lastErr = err
			if attempt < rstate.MaxAttempts() {
				if werr := c.waitRetryBudget(ctx, rstate, attempt, prop, rev, req.ID); werr != nil {
					return resp, werr
				}
			}
			continue
		}

		outcome, callErr := c.doCall(ctx, prop, req, ins, ticket, rev, attempt)
		resp.Attempts = attempt
		now = c.now()
		brk.RecordResult(now, callErr == nil)

		if callErr == nil {
			outcome.InstanceID = ins.ID
			outcome.Attempts = attempt
			outcome.Reason = "OK"
			c.tracer.Emit(TraceLine{At: now, RequestID: req.ID, Revision: rev, Attempt: attempt,
				InstanceID: ins.ID, Policy: "invoke", Outcome: "success",
				Reason: "OK", Response: summarize(outcome)})
			return outcome, nil
		}

		c.tracer.Emit(TraceLine{At: now, RequestID: req.ID, Revision: rev, Attempt: attempt,
			InstanceID: ins.ID, Policy: "invoke", Outcome: "failure",
			Reason: govCode(callErr)})
		lastErr = callErr

		// 上游取消或预算超时都不可重试：剩余空间已不存在，重试只会浪费资源。
		if errors.Is(callErr, model.ErrContextCanceled) || errors.Is(callErr, model.ErrTimeout) {
			return resp, callErr
		}
		if attempt == rstate.MaxAttempts() {
			exhausted = true
			break
		}
		if werr := c.waitRetryBudget(ctx, rstate, attempt, prop, rev, req.ID); werr != nil {
			return resp, werr
		}
	}

	if exhausted {
		// 达到上限：返回可区分原因，并携带最后一次下游错误链。
		return resp, fmt.Errorf("%w: last_error=%v", model.ErrRetryExhausted, lastErr)
	}
	return resp, lastErr
}

// doCall 执行单次下游调用，保证结束时释放预算与并发额度。
func (c *Client) doCall(parent context.Context, prop *deadline.Propagation,
	req *model.Request, ins model.Instance, ticket *concurrency.Ticket,
	rev uint64, attempt int) (*model.Response, error) {

	childCtx, cancel, budget := prop.ChildContext(parent)
	defer cancel()
	defer ticket.Release()

	if budget <= 0 {
		// 剩余预算为零：不占用下游连接，直接失败。
		if gerr := deadline.Classify(parent); gerr != nil {
			return nil, gerr
		}
		return nil, model.ErrTimeout
	}

	type callResult struct {
		resp *model.Response
		err  error
	}
	done := make(chan callResult, 1)
	go func() {
		r, e := c.invoker(childCtx, ins, req)
		done <- callResult{r, e}
	}()

	select {
	case res := <-done:
		if res.err != nil {
			if gerr := deadline.Classify(childCtx); gerr != nil {
				return nil, gerr
			}
			return nil, res.err
		}
		return res.resp, nil
	case <-parent.Done():
		// 上游取消 / 截止：不等待慢下游，goroutine 会经由 childCtx 被驱动退出；
		// 立刻释放额度，不留下仍在推进的占用。
		return nil, deadline.Classify(parent)
	case <-childCtx.Done():
		return nil, deadline.Classify(childCtx)
	}
}

// waitRetryBudget 在退避期间同时遵守上游取消与剩余预算。
func (c *Client) waitRetryBudget(ctx context.Context, rs *retry.State, failedAttempt int,
	prop *deadline.Propagation, rev uint64, reqID string) error {
	backoff := rs.NextBackoff(failedAttempt)
	left, ok := prop.Remaining()
	if !ok {
		c.tracer.Emit(TraceLine{At: c.now(), RequestID: reqID, Revision: rev,
			Attempt: failedAttempt + 1, Policy: "retry", Outcome: "reject",
			Reason: model.ErrTimeout.Code})
		return model.ErrTimeout
	}
	if backoff > left {
		// 退避会耗尽预算，没有重试空间：按超时快速失败而不是做注定失败的尝试。
		c.tracer.Emit(TraceLine{At: c.now(), RequestID: reqID, Revision: rev,
			Attempt: failedAttempt + 1, Policy: "retry", Outcome: "reject",
			Reason: model.ErrTimeout.Code})
		return model.ErrTimeout
	}
	t := time.NewTimer(backoff)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return deadline.Classify(ctx)
	case <-t.C:
		return nil
	}
}

func summarize(r *model.Response) string {
	if r == nil {
		return "<nil>"
	}
	return fmt.Sprintf("status=%d bytes=%d", r.Status, len(r.Payload))
}

// Trace 返回指定请求的决策轨迹。
func (c *Client) Trace(reqID string) []model.Attempt { return c.tracer.Of(reqID) }
