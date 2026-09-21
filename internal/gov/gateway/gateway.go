// Package gateway orchestrates discovery, load balancing, deadlines,
// retries, circuit breaking and concurrency limits into one
// deterministic, explainable RPC governance layer.
//
// Determinism rules:
//
//   - A policy snapshot (with monotonically increasing config version)
//     is pinned at request entry and used for the whole request, so a
//     hot reload never mixes old/new behavior within one request.
//   - A total-deadline context bounds all attempts and backoff; a
//     canceled upstream never leaves a downstream call running.
//   - Only idempotent requests are ever retried.
//   - Every outcome carries a per-attempt trace: policy hit, chosen
//     instance and mapped error code.
package gateway

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/config"
	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/deadline"
	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/gerr"
	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/registry"
	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/retry"
)

// Request is a governed outbound RPC request.
type Request struct {
	ID         string
	Service    string
	Method     string
	Idempotent bool
	Payload    []byte
}

// Response is a downstream RPC response.
type Response struct {
	InstanceID string
	Status     int
	Payload    []byte
}

// Downstream is the user-supplied transport call.
type Downstream interface {
	Invoke(ctx context.Context, inst registry.Instance, req *Request) (*Response, error)
}

// DownstreamFunc adapts a function to Downstream.
type DownstreamFunc func(ctx context.Context, inst registry.Instance, req *Request) (*Response, error)

// Invoke implements Downstream.
func (f DownstreamFunc) Invoke(ctx context.Context, inst registry.Instance, req *Request) (*Response, error) {
	return f(ctx, inst, req)
}

// AttemptLog records what happened on one attempt (for explainability).
type AttemptLog struct {
	Attempt    int    `json:"attempt"`
	InstanceID string `json:"instance_id"`
	PolicyHit  string `json:"policy_hit"`
	Status     int    `json:"status"`
	ErrCode    string `json:"err_code,omitempty"`
	ErrMsg     string `json:"err_msg,omitempty"`
}

// Outcome is the fully explained result of a governed call.
type Outcome struct {
	RequestID     string       `json:"request_id"`
	Service       string       `json:"service"`
	Method        string       `json:"method"`
	ConfigVersion int64        `json:"config_version"`
	OK            bool         `json:"ok"`
	Response      *Response    `json:"response,omitempty"`
	ErrCode       gerr.Code    `json:"err_code,omitempty"`
	ErrMsg        string       `json:"err_msg,omitempty"`
	Attempts      []AttemptLog `json:"attempts"`
}

// Logger receives per-attempt and final result lines.
type Logger interface {
	Printf(format string, args ...any)
}

// Gateway is the governance orchestrator.
type Gateway struct {
	reg *registry.Registry
	cm  *config.Manager
	ds  Downstream
	log Logger

	govs *governorManager

	rngMu sync.Mutex
	rng   *rand.Rand

	closeOnce sync.Once
}

// Option configures a Gateway.
type Option func(*Gateway)

// WithLogger attaches a logger.
func WithLogger(l Logger) Option {
	return func(g *Gateway) { g.log = l }
}

// New constructs a Gateway.
func New(reg *registry.Registry, cm *config.Manager, ds Downstream, opts ...Option) *Gateway {
	g := &Gateway{
		reg:  reg,
		cm:   cm,
		ds:   ds,
		govs: newGovernorManager(reg),
		rng:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	for _, o := range opts {
		o(g)
	}
	return g
}

// Invoke executes one governed request.
func (g *Gateway) Invoke(ctx context.Context, req *Request) *Outcome {
	// Pin BOTH the policy snapshot and its governor generation for the
	// entire request so a hot reload cannot change behavior mid-request.
	policy, cfgVer := g.cm.Snapshot()
	gs := g.govs.pin(g.cm)
	defer g.govs.unpin(gs)

	out := &Outcome{
		RequestID:     req.ID,
		Service:       req.Service,
		Method:        req.Method,
		ConfigVersion: cfgVer,
		Attempts:      make([]AttemptLog, 0, policy.MaxAttempts),
	}

	// One total deadline for the entire request (all attempts + waits).
	totalCtx := ctx
	var totalCancel context.CancelFunc
	if policy.TotalTimeout > 0 {
		totalCtx, totalCancel = context.WithTimeout(ctx, policy.TotalTimeout)
	}
	defer func() {
		if totalCancel != nil {
			totalCancel()
		}
	}()

	classifier := retry.NewClassifier(policy.RetryEnabled, policy.MaxAttempts)
	bal := gs.balancerFor(req.Service, g.reg)

	g.logAttempt("request-start", req, cfgVer, policy)

	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		// Total budget gate at the top of every attempt: once the
		// upstream deadline/cancellation is done we never dispatch
		// again (no connection, no concurrency slot consumed).
		if err := totalCtx.Err(); err != nil {
			if err == context.Canceled {
				return g.fail(out, gerr.CodeCanceled, "upstream canceled before attempt")
			}
			return g.fail(out, gerr.CodeTimeoutBeforeDispatch,
				"total deadline budget exhausted before dispatch")
		}

		// 1) Discovery: empty instance list fails fast, reason distinguished.
		instances := sortedInstances(g.reg.List(req.Service))
		if len(instances) == 0 {
			all := g.reg.ListAll(req.Service)
			code := gerr.CodeNoInstance
			msg := "no instance registered for service"
			if len(all) > 0 {
				// Distinguishable from "never registered": instances
				// exist but health checks have removed all of them.
				code = gerr.CodeInstanceUnhealthy
				msg = "all instances unhealthy"
			}
			out.Attempts = append(out.Attempts, AttemptLog{
				Attempt: attempt, PolicyHit: "discovery-empty",
				ErrCode: string(code), ErrMsg: msg,
			})
			g.logAttempt("discovery-empty", req, cfgVer, policy)
			return g.fail(out, code, msg)
		}

		// 2) Pick an eligible instance.
		inst, err := bal.Pick(totalCtx, instances)
		if err != nil {
			code := gerr.CodeOf(err)
			out.Attempts = append(out.Attempts, AttemptLog{
				Attempt: attempt, PolicyHit: "lb-pick-failed",
				ErrCode: string(code), ErrMsg: err.Error(),
			})
			return g.fail(out, code, err.Error())
		}

		// 3) Circuit breaker check (instance or method scope).
		brKey := scopeKey(policy.BreakerScope, inst.ID, req)
		br := gs.breakers.Get(brKey)
		if policy.BreakerEnabled && !br.Allow() {
			st := br.State()
			msg := "circuit " + st.String() + " for " + brKey
			out.Attempts = append(out.Attempts, AttemptLog{
				Attempt: attempt, InstanceID: inst.ID,
				PolicyHit: "breaker-open", ErrCode: string(gerr.CodeCircuitOpen), ErrMsg: msg,
			})
			g.logAttempt("breaker-open", req, cfgVer, policy)
			// Open circuit: don't keep selecting the same instance;
			// try another instance if idempotent and attempts remain.
			bal.TemporarilyDown(inst.ID)
			decision, _ := classifier.Classify(req.Idempotent, attempt,
				gerr.New(gerr.CodeCircuitOpen, msg))
			if decision == retry.Allow {
				if werr := g.backoff(totalCtx, policy, attempt); werr != nil {
					return g.fail(out, gerr.CodeOf(werr), werr.Error())
				}
				continue
			}
			return g.fail(out, gerr.CodeCircuitOpen, msg)
		}

		// 4) Concurrency/queue bound (instance or method scope).
		bhKey := scopeKey(policy.BulkheadScope, inst.ID, req)
		lease, err := gs.bh.Acquire(totalCtx, bhKey)
		if err != nil {
			code := gerr.CodeOf(err)
			out.Attempts = append(out.Attempts, AttemptLog{
				Attempt: attempt, InstanceID: inst.ID,
				PolicyHit: "bulkhead-reject", ErrCode: string(code), ErrMsg: err.Error(),
			})
			g.logAttempt("bulkhead-reject", req, cfgVer, policy)
			return g.fail(out, code, err.Error())
		}

		// 5) Attempt-scoped deadline: never exceed remaining total budget.
		aCtx, aCancel, _, err := deadline.Child(totalCtx, policy.AttemptTimeout)
		if err != nil {
			lease.Release()
			code := gerr.CodeOf(err)
			out.Attempts = append(out.Attempts, AttemptLog{
				Attempt: attempt, InstanceID: inst.ID,
				PolicyHit: "deadline-no-budget", ErrCode: string(code), ErrMsg: err.Error(),
			})
			return g.fail(out, code, err.Error())
		}

		// 6) Transport call. Cancellation is wired to the downstream via
		//    aCtx, so upstream cancel always propagates.
		resp, callErr := g.ds.Invoke(aCtx, inst, req)
		aCancel()
		lease.Release()

		if callErr == nil && resp != nil && resp.Status < 500 {
			br.OnSuccess()
			bal.MarkSuccess(inst.ID)
			resp.InstanceID = inst.ID
			out.Attempts = append(out.Attempts, AttemptLog{
				Attempt: attempt, InstanceID: inst.ID,
				PolicyHit: "success", Status: resp.Status,
			})
			out.OK = true
			out.Response = resp
			g.logSuccess(req, cfgVer, policy, attempt, inst.ID, resp)
			return out
		}

		// Failure accounting.
		if callErr == nil {
			callErr = gerr.New(gerr.CodeDownstream, "downstream returned failure status")
		}
		fcode := gerr.CodeOf(callErr)
		status := 0
		if resp != nil {
			status = resp.Status
		}
		br.OnFailure()
		bal.MarkFailure(inst.ID)
		out.Attempts = append(out.Attempts, AttemptLog{
			Attempt: attempt, InstanceID: inst.ID,
			PolicyHit: "downstream-failure", Status: status,
			ErrCode: string(fcode), ErrMsg: callErr.Error(),
		})
		g.logFailure(req, cfgVer, policy, attempt, inst.ID, fcode, callErr)

		// 7) Retry decision. Non-idempotent -> deny(NonRetryable).
		decision, dcode := classifier.Classify(req.Idempotent, attempt, callErr)
		if decision == retry.Deny {
			return g.fail(out, dcode, callErr.Error())
		}

		// 8) Cancellable backoff; a wait that the budget cuts short ends
		//    the request immediately.
		if werr := g.backoff(totalCtx, policy, attempt); werr != nil {
			return g.fail(out, gerr.CodeOf(werr), werr.Error())
		}
	}
	return g.fail(out, gerr.CodeRetryBudgetExhausted, "retry budget exhausted")
}

func (g *Gateway) backoff(ctx context.Context, p *config.Policy, attempt int) error {
	g.rngMu.Lock()
	d := retry.Backoff(p.InitialBackoff, p.MaxBackoff, p.BackoffMultiplier, attempt, p.BackoffJitter, g.rng)
	g.rngMu.Unlock()
	return deadline.Sleep(ctx, d)
}

func scopeKey(scope, instID string, req *Request) string {
	if scope == config.ScopeMethod {
		return req.Service + "/" + req.Method
	}
	return instID
}

func (g *Gateway) fail(out *Outcome, code gerr.Code, msg string) *Outcome {
	out.OK = false
	out.ErrCode = code
	out.ErrMsg = msg
	if g.log != nil {
		g.log.Printf("[gov] request=%s service=%s result=FAIL code=%s msg=%q attempts=%d cfg=%d",
			out.RequestID, out.Service, code, msg, len(out.Attempts), out.ConfigVersion)
	}
	return out
}

// Close releases gateway resources.
func (g *Gateway) Close() {
	g.closeOnce.Do(func() {
		// Pinned generations unsubscribe themselves on unpin; nothing
		// extra is held centrally.
	})
}
