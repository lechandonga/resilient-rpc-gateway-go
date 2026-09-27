package client

import "time"

// BeginDrain 启动进程级优雅排空（非阻塞）：立即拒绝新请求，
// 已在途请求继续，grace 到期统一切断。
func (c *Client) BeginDrain(grace time.Duration) { c.drainer.BeginDrain(grace) }

// Shutdown 进程级优雅排空并等待结束：返回后保证没有在途调用仍在推进。
// grace 内全部完成返回 nil；宽限用尽返回 model.ErrDrainTimeout（在途也已退出）。
func (c *Client) Shutdown(grace time.Duration) error { return c.drainer.Drain(grace) }

// BeginInstanceDrain 摘除单个实例的排空（非阻塞）。
func (c *Client) BeginInstanceDrain(service, instanceID string, grace time.Duration) {
	c.drainer.BeginInstanceDrain(service, instanceID, grace)
}

// DrainInstance 摘除单个实例并优雅排空其在途请求，等待结束或宽限到期。
func (c *Client) DrainInstance(service, instanceID string, grace time.Duration) error {
	return c.drainer.DrainInstance(service, instanceID, grace)
}
