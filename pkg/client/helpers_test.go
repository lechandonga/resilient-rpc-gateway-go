package client

import (
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/loadbalancer"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/registry"
)

func loadBalancerPick(reg *registry.Registry, svc string) (model.Instance, error) {
	b, err := loadbalancer.New(model.LBWeightedRoundRobin)
	if err != nil {
		return model.Instance{}, err
	}
	return b.Pick(reg.ListHealthy(svc))
}

// tracerRevisionOf 读取某请求第一条轨迹命中的配置 revision。
func (c *Client) tracerRevisionOf(reqID string) uint64 {
	c.tracer.mu.Lock()
	defer c.tracer.mu.Unlock()
	for _, l := range c.tracer.lines {
		if l.RequestID == reqID {
			return l.Revision
		}
	}
	return 0
}
