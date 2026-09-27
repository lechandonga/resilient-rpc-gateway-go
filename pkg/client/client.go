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
	tracer   *Tracer
	drainer  *drain.Manager
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
		drainer:  drain.New(),
		now:      time.Now,
	}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

// BeginDrain 进程进入优雅排空：拒绝新请求，已在途请求在宽限内尽量跑完。
// grace 为排空宽限；到期未完成的在途请求被强制取消（GATEWAY_DRAIN_TIMEOUT）。
// 幂等，重复调用以第一次为准。
func (c *Client) BeginDrain(grace time.Duration) { c.drainer.Begin(grace) }

// WaitDrain 阻塞直到排空完成（在途清零或宽限到期）。
func (c *Client) WaitDrain() { c.drainer.Wait() }

// Shutdown 进入排空并等待结束，返回排空结果快照（TimedOut 区分干净/强制收尾）。
func (c *Client) Shutdown(grace time.Duration) model.DrainSnapshot {
	return c.drainer.Shutdown(grace)
}

// DrainInstance 让单个实例进入优雅排空：不再被新请求选中，
// 已在途请求跑完后可由 registry.Remove 彻底摘除。
func (c *Client) DrainInstance(service, instanceID string) {
	c.reg.Drain(service, instanceID)
}

// DrainSnapshot 返回当前进程级排空状态。
func (c *Client) DrainSnapshot() model.DrainSnapshot { return c.drainer.Snapshot() }

// Snapshot 返回此刻治理层各组件状态的一致快照：
// 排空进度、每个实例×方法的在途/排队/熔断/冷却剩余、注册实例状态。
// 各子项均按稳定键排序，同输入重复查询结果可复现。
func (c *Client) Snapshot() model.GovernanceSnapshot {
	now := c.now()
	_, rev := c.holder.Snapshot()
	d := c.drainer.Snapshot()
	return model.GovernanceSnapshot{
		TakenAt:       now,
		Revision:      rev,
		Drain:         d,
		TotalInFlight: d.InFlight,
		Limiters:      c.limiters.Snapshot(),
		Breakers:      c.breakers.Stats(now),
		Instances:     c.reg.Stats(),
	}
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
	// 1) 入口处先看上游：进来时就已经取消/超时的请求，绝不进入治理流程，
	//    不占用并发/排队/连接额度，也不会被后续重试或半开探测绕过去。
	if gerr := deadline.Classify(ctx); gerr != nil {
		return nil, gerr
	}
	// 2) 排空门禁：排空中拒绝一切新请求（原因码可区分于普通调用失败）。
	if err := c.drainer.Admit(); err != nil {
		c.tracer.Emit(TraceLine{At: c.now(), RequestID: reqIDOf(req),
			Policy: "drain", Outcome: "reject", Reason: govCode(err)})
		return nil, err
	}
	defer c.drainer.Release()

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

	// 3) 把请求上下文联动到排空强制上下文：宽限到期时以 GATEWAY_DRAIN_TIMEOUT
	//    为 cause 取消本请求及其全部下游派生 ctx。
	reqCtx, cancelCause := context.WithCancelCause(ctx)
	stop := context.AfterFunc(c.drainer.ForceContext(), func() { cancelCause(model.ErrDrainTimeout) })
	defer stop()
	defer cancelCause(nil)

	lb, err := c.balancer(rev, req.Method, policy.LB)
	if err != nil {
		return nil, err
	}
	rstate := retry.NewState(policy)

	// 上游截止时间 + 方法单尝试预算：更紧者生效。
	parentDL, _ := deadline.FromContext(reqCtx)
	prop := deadline.New(parentDL, policy.Timeout)

	resp := &model.Response{Reason: "UNKNOWN"}
	var lastErr error
	var exhausted bool

	for attempt := 1; attempt <= rstate.MaxAttempts(); attempt++ {
		// 每轮、任何可能等待的动作之前：取消/预算耗尽立即收敛，绝不发起新调用。
		if gerr := preflight(reqCtx, prop); gerr != nil {
			return resp, gerr
		}
		now = c.now()
		if err := rstate.Allow(req, attempt); err != nil {
			// 非幂等请求尝试重试：直接给出可区分原因。
			c.tracer.Emit(TraceLine{At: c.now(), RequestID: req.ID, Revision: rev, Attempt: attempt,
				Policy: "retry", Outcome: "reject", Reason: govCode(err)})
			return resp, err
		}

		// 每轮重新发现：实例上下线 / 健康变化 / 排空摘除立刻生效。
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
		breakerGranted := false
		if err := brk.Allow(now); err != nil {
			c.tracer.Emit(TraceLine{At: c.now(), RequestID: req.ID, Revision: rev, Attempt: attempt,
				InstanceID: ins.ID, Policy: "circuit:" + string(brk.State(now)),
				Outcome: "reject", Reason: govCode(err)})
			lastErr = err
			// 熔断是实例级故障：换其他实例继续尝试（仍受重试上限、幂等门禁与取消约束）。
			if attempt < rstate.MaxAttempts() {
				if werr := c.waitRetryBudget(reqCtx, rstate, attempt, prop, rev, req.ID); werr != nil {
					return resp, werr
				}
			}
			continue
		}
		breakerGranted = true
		// abandon 归还半开探测名额：取消/预算耗尽不是探测结果，
		// 不能把名额永久泄漏，也不能被记成成功或失败。
		abandon := func() {
			if breakerGranted {
				brk.RecordAbandon(c.now())
			}
		}

		// 占用并发额度前再确认一次：取消/预算已尽就不排队、不占额度。
		if gerr := preflight(reqCtx, prop); gerr != nil {
			abandon()
			return resp, gerr
		}

		// 单实例×方法维度的并发额度。
		limitKey := ins.ID + "#" + req.Method
		ticket, err := c.limiters.Acquire(reqCtx, limitKey, policy)
		if err != nil {
			c.tracer.Emit(TraceLine{At: c.now(), RequestID: req.ID, Revision: rev, Attempt: attempt,
				InstanceID: ins.ID, Policy: "concurrency", Outcome: "reject", Reason: govCode(err)})
			// 排队期间被取消/超时/排空强制结束：终态，不重试、不绕路，
			// 并发层已经自行把排队位置移除。
			if isTerminalGovErr(err) {
				abandon()
				return resp, err
			}
			lastErr = err
			if attempt < rstate.MaxAttempts() {
				if werr := c.waitRetryBudget(reqCtx, rstate, attempt, prop, rev, req.ID); werr != nil {
					abandon()
					return resp, werr
				}
			}
			// 本轮不再实际调用：归还本轮占用的半开探测名额（closed 下是空操作）。
			abandon()
			continue
		}

		outcome, callErr, called := c.doCall(reqCtx, prop, req, ins, ticket, rev, attempt)
		resp.Attempts = attempt
		now = c.now()

		if callErr == nil {
			brk.RecordResult(now, true)
			outcome.InstanceID = ins.ID
			outcome.Attempts = attempt
			outcome.Reason = "OK"
			c.tracer.Emit(TraceLine{At: now, RequestID: req.ID, Revision: rev, Attempt: attempt,
				InstanceID: ins.ID, Policy: "invoke", Outcome: "success",
				Reason: "OK", Response: summarize(outcome)})
			return outcome, nil
		}

		// 取消 / 排空强制结束 / 预算超时：剩余空间已不存在，绝不再发起调用。
		if isTerminalGovErr(callErr) {
			// 真正打到下游但因预算到期无结果：保留为一次下游失败信号；
			// 取消、排空强制结束或根本没发起调用：归还探测名额，不计成功失败。
			if called && errors.Is(callErr, model.ErrTimeout) {
				brk.RecordResult(now, false)
			} else {
				abandon()
			}
			c.tracer.Emit(TraceLine{At: now, RequestID: req.ID, Revision: rev, Attempt: attempt,
				InstanceID: ins.ID, Policy: "invoke", Outcome: "abort",
				Reason: govCode(callErr)})
			return resp, callErr
		}

		c.tracer.Emit(TraceLine{At: now, RequestID: req.ID, Revision: rev, Attempt: attempt,
			InstanceID: ins.ID, Policy: "invoke", Outcome: "failure",
			Reason: govCode(callErr)})
		brk.RecordResult(now, false)
		lastErr = callErr

		if attempt == rstate.MaxAttempts() {
			exhausted = true
			break
		}
		if werr := c.waitRetryBudget(reqCtx, rstate, attempt, prop, rev, req.ID); werr != nil {
			return resp, werr
		}
	}

	if exhausted {
		// 达到上限：返回可区分原因，并携带最后一次下游错误链。
		return resp, fmt.Errorf("%w: last_error=%v", model.ErrRetryExhausted, lastErr)
	}
	return resp, lastErr
}

// preflight 在发起任何下游动作/等待前做取消与预算检查。
func preflight(ctx context.Context, prop *deadline.Propagation) error {
	if gerr := deadline.Classify(ctx); gerr != nil {
		return gerr
	}
	if _, ok := prop.Remaining(); !ok {
		return model.ErrTimeout
	}
	return nil
}

// isTerminalGovErr 判定取消/超时类终态错误：这类错误绝不允许重试或换实例绕过。
func isTerminalGovErr(err error) bool {
	return errors.Is(err, model.ErrContextCanceled) ||
		errors.Is(err, model.ErrTimeout) ||
		errors.Is(err, model.ErrDrainTimeout)
}

func reqIDOf(req *model.Request) string {
	if req == nil {
		return ""
	}
	return req.ID
}

// doCall 执行单次下游调用，保证结束时释放预算与并发额度。
// called 报告 invoker 是否真的被启动（false 表示在发起前就被取消/预算耗尽拦截）。
func (c *Client) doCall(parent context.Context, prop *deadline.Propagation,
	req *model.Request, ins model.Instance, ticket *concurrency.Ticket,
	rev uint64, attempt int) (resp *model.Response, err error, called bool) {

	childCtx, cancel, budget := prop.ChildContext(parent)
	defer cancel()
	defer ticket.Release()

	if budget <= 0 {
		// 剩余预算为零：不占用下游连接，直接失败。
		if gerr := deadline.Classify(parent); gerr != nil {
			return nil, gerr, false
		}
		return nil, model.ErrTimeout, false
	}
	// 最后一道闸门：即便额度刚拿到手，上游/排空也可能已经发出取消。
	if gerr := deadline.Classify(parent); gerr != nil {
		return nil, gerr, false
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
	called = true

	select {
	case res := <-done:
		// 下游已返回：但若此刻 ctx 已结束（取消与返回发生竞争），以上游结论为准，
		// 避免把一个已经被取消的调用误记为成功。
		if gerr := deadline.Classify(parent); gerr != nil {
			return nil, gerr, called
		}
		if res.err != nil {
			if gerr := deadline.Classify(childCtx); gerr != nil {
				return nil, gerr, called
			}
			return nil, res.err, called
		}
		return res.resp, nil, called
	case <-parent.Done():
		// 上游取消 / 排空强制结束：不等待慢下游，goroutine 会经由 childCtx 被驱动退出；
		// 立刻释放额度，不留下仍在推进的占用。
		return nil, deadline.Classify(parent), called
	case <-childCtx.Done():
		return nil, deadline.Classify(childCtx), called
	}
}

// waitRetryBudget 在退避期间同时遵守上游取消与剩余预算。
func (c *Client) waitRetryBudget(ctx context.Context, rs *retry.State, failedAttempt int,
	prop *deadline.Propagation, rev uint64, reqID string) error {
	// 退避前先看上游：已取消/已强制排空就不再等、不再试。
	if gerr := deadline.Classify(ctx); gerr != nil {
		return gerr
	}
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
