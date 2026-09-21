// Command demo 在本地进程内演示治理层的确定性与可解释行为：
// 实例抖动、下游超时、幂等重试、熔断、并发上限与配置热更新。
//
// 运行： go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/client"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/config"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/registry"
)

func main() {
	// 1) 初始策略（version=2）：支付方法允许 3 次尝试；慢方法单尝试 80ms 超时。
	raw := []byte(`{
  "version": 2,
  "service": {
    "name": "shop",
    "methods": {
      "charge": {"timeout":"1s","max_attempts":3},
      "slow":   {"timeout":"80ms","max_attempts":1,"max_concurrency":2}
    }
  }
}`)
	parsed, err := config.Parse(raw)
	must(err)
	holder := config.NewHolder(parsed)

	// 2) 本地注册中心 + 两个实例。
	reg := registry.NewRegistry()
	reg.Upsert("shop",
		model.Instance{ID: "inst-a", Address: "127.0.0.1:9001", Weight: 2},
		model.Instance{ID: "inst-b", Address: "127.0.0.1:9002", Weight: 1},
	)

	// 3) 假下游：charge 前 2 次失败后成功（模拟抖动），slow 永远慢。
	var chargeCalls int32
	invoker := func(ctx context.Context, ins model.Instance, req *model.Request) (*model.Response, error) {
		switch req.Method {
		case "charge":
			n := atomic.AddInt32(&chargeCalls, 1)
			if n <= 2 {
				return &model.Response{Status: 503, InstanceID: ins.ID}, fmt.Errorf("transient 503 on %s", ins.ID)
			}
			return &model.Response{Status: 200, InstanceID: ins.ID, Payload: []byte(`{"ok":true}`)}, nil
		case "slow":
			select {
			case <-time.After(2 * time.Second):
				return &model.Response{Status: 200}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		default:
			return nil, fmt.Errorf("unknown method %s", req.Method)
		}
	}

	c, err := client.New(reg, holder, invoker, client.WithLogger(os.Stdout))
	must(err)

	section("场景1：下游抖动 + 幂等重试（最终成功，最多 3 次尝试）")
	resp, err := c.Invoke(context.Background(),
		&model.Request{ID: "demo-001", Service: "shop", Method: "charge", Idempotent: true})
	report(resp, err)

	section("场景2：非幂等请求失败后不重试（避免重复副作用）")
	atomic.StoreInt32(&chargeCalls, 0)
	resp, err = c.Invoke(context.Background(),
		&model.Request{ID: "demo-002", Service: "shop", Method: "charge", Idempotent: false})
	report(resp, err)

	section("场景3：下游耗时超过预算 -> 快速失败，不占用连接")
	start := time.Now()
	resp, err = c.Invoke(context.Background(),
		&model.Request{ID: "demo-003", Service: "shop", Method: "slow"})
	fmt.Printf("耗时=%v\n", time.Since(start).Round(time.Millisecond))
	report(resp, err)

	section("场景4：实例列表为空 -> 快速失败且原因可区分")
	reg2 := registry.NewRegistry()
	c2, err := client.New(reg2, holder, invoker, client.WithLogger(os.Stdout))
	must(err)
	_, err = c2.Invoke(context.Background(),
		&model.Request{ID: "demo-004", Service: "ghost", Method: "charge", Idempotent: true})
	fmt.Printf("结果错误: %v\n", err)

	section("场景5：热更新策略（charge 改为只允许 1 次尝试），新旧请求行为可区分")
	raw2 := []byte(`{
  "version": 2,
  "service": {
    "name": "shop",
    "methods": {
      "charge": {"timeout":"1s","max_attempts":1},
      "slow":   {"timeout":"80ms","max_attempts":1}
    }
  }
}`)
	parsed2, err := config.Parse(raw2)
	must(err)
	rev, err := holder.Replace(parsed2)
	must(err)
	fmt.Printf("配置已更新到 revision=%d\n", rev)
	atomic.StoreInt32(&chargeCalls, 0)
	_, err = c.Invoke(context.Background(),
		&model.Request{ID: "demo-005", Service: "shop", Method: "charge", Idempotent: true})
	fmt.Printf("新策略下结果: %v\n", err)
}

func section(title string) {
	fmt.Printf("\n========== %s ==========\n", title)
}

func report(r *model.Response, err error) {
	if err != nil {
		fmt.Printf("失败（可区分原因）: %v\n", err)
		return
	}
	fmt.Printf("成功: instance=%s status=%d attempts=%d payload=%s\n",
		r.InstanceID, r.Status, r.Attempts, string(r.Payload))
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}
