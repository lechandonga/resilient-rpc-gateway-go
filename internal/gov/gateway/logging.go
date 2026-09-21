package gateway

import (
	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/config"
	"github.com/lechandonga/resilient-rpc-gateway-go/internal/gov/gerr"
)

// All log lines carry the request id, the pinned config version and the
// policy that fired, so test runs are reproducible and explainable.

func (g *Gateway) logAttempt(hit string, req *Request, cfgVer int64, p *config.Policy) {
	if g.log == nil {
		return
	}
	g.log.Printf("[gov] request=%s service=%s method=%s hit=%s cfg=%d "+
		"lb=%s retry=%v/max%d breaker=%s/%s bulkhead=%s/c%dq%d",
		req.ID, req.Service, req.Method, hit, cfgVer,
		p.LBPolicy, p.RetryEnabled, p.MaxAttempts,
		boolOn(p.BreakerEnabled), p.BreakerScope,
		p.BulkheadScope, p.BulkheadCfg.MaxConcurrent, p.BulkheadCfg.MaxQueued)
}

func (g *Gateway) logSuccess(req *Request, cfgVer int64, p *config.Policy, attempt int, instID string, resp *Response) {
	if g.log == nil {
		return
	}
	g.log.Printf("[gov] request=%s hit=downstream-response instance=%s attempt=%d status=%d cfg=%d bytes=%d",
		req.ID, instID, attempt, resp.Status, cfgVer, len(resp.Payload))
}

func (g *Gateway) logFailure(req *Request, cfgVer int64, p *config.Policy, attempt int, instID string, code gerr.Code, err error) {
	if g.log == nil {
		return
	}
	g.log.Printf("[gov] request=%s hit=downstream-failure instance=%s attempt=%d code=%s cfg=%d err=%q",
		req.ID, instID, attempt, code, cfgVer, err.Error())
}

func boolOn(b bool) string {
	if b {
		return "on"
	}
	return "off"
}
