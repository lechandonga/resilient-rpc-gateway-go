// Command demo runs a local, self-contained demonstration of the RPC
// governance layer: instance churn, downstream timeouts, idempotent
// vs non-idempotent retries, circuit breaking, empty registry, and a
// config hot reload.
//
// Run with:  go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/config"
	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/gateway"
	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/gerr"
	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/registry"
)

const service = "demo.svc"

// fakeDownstream simulates per-instance behavior. Every call is counted
// so non-idempotent single-side-effect behavior is observable.
type fakeDownstream struct {
	latency   map[string]time.Duration
	failUntil map[string]int64 // unix-nano: return 500 before this time
	sideCalls *int64
}

func (f *fakeDownstream) Invoke(ctx context.Context, inst registry.Instance, req *gateway.Request) (*gateway.Response, error) {
	atomic.AddInt64(f.sideCalls, 1)
	if d := f.latency[inst.ID]; d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			// Preserve the distinction between explicit upstream
			// cancellation and a budget deadline firing.
			if ctx.Err() == context.Canceled {
				return nil, gerr.Wrap(gerr.CodeCanceled, "simulated downstream canceled", ctx.Err())
			}
			return nil, gerr.Wrap(gerr.CodeDeadlineExceeded, "simulated downstream timeout", ctx.Err())
		}
	}
	if until, ok := f.failUntil[inst.ID]; ok && time.Now().UnixNano() < until {
		return &gateway.Response{InstanceID: inst.ID, Status: 500}, nil
	}
	return &gateway.Response{InstanceID: inst.ID, Status: 200, Payload: []byte("ok-from-" + inst.ID)}, nil
}

func main() {
	logger := log.New(os.Stdout, "", log.Ltime|log.Lmicroseconds)

	reg := registry.New()
	must(reg.Register(service, registry.Instance{ID: "i-1", Address: "127.0.0.1:9001", Weight: 1, Healthy: true}))
	must(reg.Register(service, registry.Instance{ID: "i-2", Address: "127.0.0.1:9002", Weight: 2, Healthy: true}))
	must(reg.Register(service, registry.Instance{ID: "i-3", Address: "127.0.0.1:9003", Weight: 1, Healthy: true}))

	var sideCalls int64
	ds := &fakeDownstream{
		latency:   map[string]time.Duration{},
		failUntil: map[string]int64{"i-2": time.Now().Add(5 * time.Minute).UnixNano()},
		sideCalls: &sideCalls,
	}

	policy := config.DefaultPolicy(service)
	cm, err := config.NewManager(policy)
	if err != nil {
		log.Fatal(err)
	}
	gw := gateway.New(reg, cm, ds, gateway.WithLogger(logger))
	defer gw.Close()

	ctx := context.Background()
	var reqID int64
	nextID := func(scenario string) string {
		n := atomic.AddInt64(&reqID, 1)
		return fmt.Sprintf("req-%03d-%s", n, scenario)
	}
	call := func(id string, idem bool, method string) {
		printOutcome(gw.Invoke(ctx, &gateway.Request{ID: id, Service: service, Method: method, Idempotent: idem}))
	}

	// 1) Happy path: idempotent read hits i-1.
	printHeader("1. happy path (idempotent)")
	call(nextID("happy"), true, "GetThing")

	// 2) Force i-1 to fail as well: idempotent request retries across
	//    instances until healthy i-3 serves it.
	printHeader("2. i-1 & i-2 failing, idempotent -> retried to healthy i-3")
	ds.failUntil["i-1"] = time.Now().Add(5 * time.Minute).UnixNano()
	call(nextID("retry"), true, "GetThing")

	// 3) ALL instances failing, NON-idempotent: even on failure there is
	//    exactly one downstream side effect (no retry).
	printHeader("3. all fail, non-idempotent -> exactly 1 side effect, no retry")
	ds.failUntil["i-3"] = time.Now().Add(5 * time.Minute).UnixNano()
	before := atomic.LoadInt64(&sideCalls)
	call(nextID("once"), false, "CreateOrder")
	fmt.Printf("downstream side-effect calls for this request: %d\n", atomic.LoadInt64(&sideCalls)-before)
	delete(ds.failUntil, "i-3")

	// 4) Slow instance i-3 + healthy others: attempts that land on i-3
	//    are cut off by the 2s attempt deadline; the total budget lets
	//    another attempt succeed. Make i-1/i-2 healthy again but i-3 slow.
	printHeader("4. slow i-3 (3s) -> attempt deadline, retries to peer")
	delete(ds.failUntil, "i-1")
	delete(ds.failUntil, "i-2")
	ds.latency["i-3"] = 3 * time.Second
	call(nextID("slow"), true, "GetThing")
	delete(ds.latency, "i-3")

	// 5) ALL instances slow + upstream cancel: the in-flight downstream
	//    call must be canceled too (code=canceled).
	printHeader("5. upstream canceled mid-call -> downstream canceled")
	ds.latency["i-1"] = 5 * time.Second
	ds.latency["i-2"] = 5 * time.Second
	ds.latency["i-3"] = 5 * time.Second
	cctx, cancel := context.WithCancel(ctx)
	go func() {
		time.Sleep(300 * time.Millisecond)
		cancel()
	}()
	printOutcome(gw.Invoke(cctx, &gateway.Request{ID: nextID("cancel"), Service: service, Method: "GetThing", Idempotent: true}))
	ds.latency = map[string]time.Duration{}

	// 6) Empty registry: fast fail with distinguishable reason.
	printHeader("6. all instances removed -> no_instance")
	must(reg.Deregister(service, "i-1"))
	must(reg.Deregister(service, "i-2"))
	must(reg.Deregister(service, "i-3"))
	call(nextID("empty"), true, "GetThing")

	// 7) Single dead instance trips the breaker: after the configured
	//    consecutive-failure threshold, requests are rejected fast with
	//    circuit_open without touching the downstream.
	printHeader("7. repeated failures trip breaker -> circuit_open fast fail")
	must(reg.Register(service, registry.Instance{ID: "dead-1", Address: "127.0.0.1:9999", Weight: 1, Healthy: true}))
	ds.failUntil["dead-1"] = time.Now().Add(5 * time.Minute).UnixNano()
	for i := 0; i < 7; i++ {
		before := atomic.LoadInt64(&sideCalls)
		printOutcome(gw.Invoke(ctx, &gateway.Request{ID: nextID("brk"), Service: service, Method: "GetThing", Idempotent: true}))
		fmt.Printf("  downstream calls for this request: %d\n", atomic.LoadInt64(&sideCalls)-before)
	}

	// 8) Hot reload: new requests run under cfg version 2 with retry
	//    disabled; each Outcome records the version it actually used.
	printHeader("8. hot reload (retry off, tight timeouts) -> cfg version 2")
	newCfg := []byte(`{
		"version": 1,
		"service": "demo.svc",
		"retry_enabled": false,
		"max_attempts": 1,
		"attempt_timeout": "200ms",
		"total_timeout": "600ms"
	}`)
	p, err := cm.ReplaceJSON(newCfg)
	if err != nil {
		log.Fatalf("reload failed: %v", err)
	}
	fmt.Printf("reloaded: retry=%v maxAttempts=%d attempt=%s total=%s\n",
		p.RetryEnabled, p.MaxAttempts, p.AttemptTimeout, p.TotalTimeout)
	printOutcome(gw.Invoke(ctx, &gateway.Request{ID: nextID("cfg2"), Service: service, Method: "CreateOrder", Idempotent: false}))

	// 9) Bad config is explicitly REJECTED; active policy stays unchanged.
	printHeader("9. invalid configs rejected, active policy retained")
	if _, err := cm.ReplaceJSON([]byte(`{"version":1,"service":"demo.svc","bogus":true}`)); err != nil {
		fmt.Printf("unknown field rejected as expected: %v\n", err)
	}
	if _, err := cm.ReplaceJSON([]byte(`{"version":1,"service":"demo.svc","retry_enabled":true,"max_attempts":1}`)); err != nil {
		fmt.Printf("conflict rejected as expected: %v\n", err)
	}
	if _, err := cm.ReplaceJSON([]byte(`{"version":99,"service":"demo.svc"}`)); err != nil {
		fmt.Printf("future version rejected as expected: %v\n", err)
	}
	cur, ver := cm.Snapshot()
	fmt.Printf("active policy still: cfg=%d retry=%v attempt=%s\n", ver, cur.RetryEnabled, cur.AttemptTimeout)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func printHeader(s string) {
	fmt.Printf("\n==== %s ====\n", s)
}

func printOutcome(o *gateway.Outcome) {
	status := "OK"
	if !o.OK {
		status = "FAIL code=" + string(o.ErrCode) + " (" + o.ErrMsg + ")"
	}
	fmt.Printf("outcome id=%s cfg=%d %s attempts=%d\n", o.RequestID, o.ConfigVersion, status, len(o.Attempts))
	for _, a := range o.Attempts {
		line := fmt.Sprintf("  attempt#%d hit=%s instance=%s", a.Attempt, a.PolicyHit, a.InstanceID)
		if a.Status != 0 {
			line += fmt.Sprintf(" status=%d", a.Status)
		}
		if a.ErrCode != "" {
			line += fmt.Sprintf(" err=%s", a.ErrCode)
		}
		fmt.Println(line)
	}
	if o.Response != nil {
		fmt.Printf("  downstream response: status=%d body=%q instance=%s\n",
			o.Response.Status, string(o.Response.Payload), o.Response.InstanceID)
	}
}
