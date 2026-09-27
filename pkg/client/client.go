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
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/drain"
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
	drainer  *drain.Coordinator
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
		drainer:  drain.New(),
		tracer:   NewTracer(io.Discard, 256, false),
		now:      time.Now,
	}
	for _, o := range opts {
		o(c)
	}
	c.drainer = c.drainer.WithClock(c.now)
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
	prop := deadline.New(parentDL, policy.Timeout).WithClock(c.now)

	resp := &model.Response{Reason: "UNKNOWN"}
	var lastErr error
	var exhausted bool

	// 取消在整条链路上最先收敛：请求进来时已被上游取消/超预算，
	// 立刻失败，绝不进入发现、熔断、排队，更不会敲一次下游。
	if gerr := deadline.Classify(ctx); gerr != nil {
		return resp, gerr
	}
	if left, ok := prop.Remaining(); !ok || left <= 0 {
		return resp, model.ErrTimeout
	}

	// 排空准入：进程级排空开始后，新请求一律 GATEWAY_DRAIN_REJECTED，
	// 不占用任何并发/排队/连接资源。
	if err := c.drainer.Admit(); err != nil {
		c.tracer.Emit(TraceLine{At: c.now(), RequestID: req.ID, Revision: rev,
			Policy: "drain", Outcome: "reject", Reason: govCode(err)})
		return resp, err
	}
	// 登记全局在途并取得"可被排空切断"的请求 ctx；defer Release 保证
	// 无论从哪条路径退出（含取消/排空切断/重试耗尽），在途计数都回落。
	handle, err := c.drainer.Bind(ctx)
	if err != nil {
		c.tracer.Emit(TraceLine{At: c.now(), RequestID: req.ID, Revision: rev,
			Policy: "drain", Outcome: "reject", Reason: govCode(err)})
		return resp, err
	}
	defer handle.Release()
	ctx = handle.Context()

	for attempt := 1; attempt <= rstate.MaxAttempts(); attempt++ {
		now = c.now()
		// 每轮入口都检查：等退避、等排队、上一轮处理期间发生的取消/超预算，
		// 都不会再发起新的下游尝试，也不会被后续重试绕过去。
		if gerr := deadline.Classify(ctx); gerr != nil {
			c.tracer.Emit(TraceLine{At: now, RequestID: req.ID, Revision: rev, Attempt: attempt,
				Policy: "lifecycle", Outcome: "reject", Reason: govCode(gerr)})
			return resp, gerr
		}
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

		// 选出一个未在排空的实例：被摘除排空的实例本轮不接受新流量，
		// 但它的在途请求仍在各自预算内继续。先过滤再 Pick，负载均衡游标只推进一次。
		eligible := candidates[:0:0]
		for _, cand := range candidates {
			if !c.drainer.IsInstanceDraining(req.Service, cand.ID) {
				eligible = append(eligible, cand)
			}
		}
		if len(eligible) == 0 {
			// 全部实例都在排空中：等价于"此刻没有健康可接流量的实例"。
			c.tracer.Emit(TraceLine{At: now, RequestID: req.ID, Revision: rev, Attempt: attempt,
				Policy: "drain", Outcome: "reject", Reason: model.ErrNoHealthyInstance.Code})
			return resp, model.ErrNoHealthyInstance
		}
		ins, err := lb.Pick(eligible)
		if err != nil {
			return resp, err
		}
		// 防御性二次确认：即使并发下实例恰好在选中后进入排空，也不在此发起调用。
		if err := handle.Attach(req.Service, ins.ID); err != nil {
			c.tracer.Emit(TraceLine{At: now, RequestID: req.ID, Revision: rev, Attempt: attempt,
				InstanceID: ins.ID, Policy: "drain", Outcome: "reject", Reason: govCode(err)})
			return resp, model.ErrDrainRejected
		}

		cbKey := cbDimension(req.Service, ins.ID, req.Method)
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
		limitKey := cbDimension(req.Service, ins.ID, req.Method)
		ticket, err := c.limiters.Acquire(ctx, limitKey, policy)
		if err != nil {
			c.tracer.Emit(TraceLine{At: c.now(), RequestID: req.ID, Revision: rev, Attempt: attempt,
				InstanceID: ins.ID, Policy: "concurrency", Outcome: "reject", Reason: govCode(err)})
			// 取消/超预算/排空：终态，绝不重试；回收半开探测槽，不留下任何占用。
			if isTerminalContextErr(err) {
				brk.RecordAbandon(now)
				return resp, err
			}
			lastErr = err
			if attempt < rstate.MaxAttempts() {
				if werr := c.waitRetryBudget(ctx, rstate, attempt, prop, rev, req.ID); werr != nil {
					brk.RecordAbandon(now)
					return resp, werr
				}
			}
			continue
		}

		outcome, callErr := c.doCall(ctx, prop, req, ins, ticket, rev, attempt)
		resp.Attempts = attempt
		now = c.now()

		// 熔断记账铁律：只有拿到下游业务裁决才 RecordResult；
		// 取消/超预算/排空（无论发生在发起前、排队后还是在途中）一律 RecordAbandon：
		// 不计失败、不触发熔断，并回收 half-open 预占的探测槽。
		if gerr := asAbandon(callErr); gerr != nil {
			brk.RecordAbandon(now)
			c.tracer.Emit(TraceLine{At: now, RequestID: req.ID, Revision: rev, Attempt: attempt,
				InstanceID: ins.ID, Policy: "lifecycle", Outcome: "reject",
				Reason: govCode(gerr)})
			return resp, gerr
		}
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

		if attempt == rstate.MaxAttempts() {
			exhausted = true
			break
		}
		if werr := c.waitRetryBudget(ctx, rstate, attempt, prop, rev, req.ID); werr != nil {
			brk.RecordAbandon(now)
			return resp, werr
		}
	}

	if exhausted {
		// 达到上限：返回可区分原因，并携带最后一次下游错误链。
		return resp, fmt.Errorf("%w: last_error=%v", model.ErrRetryExhausted, lastErr)
	}
	return resp, lastErr
}

// cbDimension 构造治理维度键：service/instance#method。
// 同 ID 跨服务实例不会互相污染熔断与并发计数。
func cbDimension(service, instanceID, method string) string {
	return service + "/" + instanceID + "#" + method
}

// isTerminalContextErr 判断是否为不可重试的链路终止错误。
func isTerminalContextErr(err error) bool {
	return errors.Is(err, model.ErrContextCanceled) ||
		errors.Is(err, model.ErrTimeout) ||
		errors.Is(err, model.ErrDrainTimeout) ||
		errors.Is(err, model.ErrDrainRejected)
}

// asAbandon 把"未触达下游"的错误映射为熔断放弃原因；普通下游错误返回 nil。
func asAbandon(err error) error {
	switch {
	case errors.Is(err, model.ErrContextCanceled),
		errors.Is(err, model.ErrTimeout),
		errors.Is(err, model.ErrDrainTimeout):
		return err
	}
	return nil
}

// doCall 执行单次下游调用，保证结束时释放预算与并发额度。
// 返回错误中的取消/超时/排空类（asAbandon 可识别）表示没有可用的下游业务裁决，
// 无论 invoker goroutine 是否已启动，调用方都按"放弃"记账，绝不计失败、不重试。
func (c *Client) doCall(parent context.Context, prop *deadline.Propagation,
	req *model.Request, ins model.Instance, ticket *concurrency.Ticket,
	rev uint64, attempt int) (resp *model.Response, err error) {

	childCtx, cancel, budget := prop.ChildContext(parent)
	defer cancel()
	defer ticket.Release()

	// 拿到额度后、真正发起前再检查一次：排队等待期间可能已被取消或切断。
	if gerr := deadline.Classify(parent); gerr != nil {
		return nil, gerr
	}
	if gerr := deadline.Classify(childCtx); gerr != nil {
		return nil, gerr
	}

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
		// 上游取消 / 截止 / 排空切断：不等待慢下游，goroutine 经由 childCtx
		// （parent 取消会级联）被驱动退出；立刻释放额度，不留下仍在推进的占用。
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
