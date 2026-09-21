// Package integration_test 从外部视角验证治理层组合行为，
// 重点：实例抖动、下游超时、重复副作用、配置异常——结果一致、原因可区分。
package integration_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/client"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/config"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/registry"
)

const goodConfig = `{
  "version": 2,
  "service": {
    "name": "shop",
    "methods": {
      "charge": {"timeout":"1s","max_attempts":4},
      "slow":   {"timeout":"40ms","max_attempts":1}
    }
  }
}`

func setup(t *testing.T, invoker client.Invoker) (*client.Client, *registry.Registry, *config.Holder, *bytes.Buffer) {
	t.Helper()
	parsed, err := config.Parse([]byte(goodConfig))
	if err != nil {
		t.Fatal(err)
	}
	holder := config.NewHolder(parsed)
	reg := registry.NewRegistry()
	reg.Upsert("shop",
		model.Instance{ID: "a", Address: "a:1", Weight: 1},
		model.Instance{ID: "b", Address: "b:1", Weight: 1},
	)
	var log bytes.Buffer
	c, err := client.New(reg, holder, invoker, client.WithLogger(&log))
	if err != nil {
		t.Fatal(err)
	}
	return c, reg, holder, &log
}

// TestInstanceChurnEventuallyHitsHealthy 实例抖动：调用中摘除/恢复，
// 健康实例最终能稳定成功，且全程没有流量打到已摘除实例。
func TestInstanceChurnEventuallyHitsHealthy(t *testing.T) {
	var hitDead atomic.Int32
	invoker := func(ctx context.Context, ins model.Instance, req *model.Request) (*model.Response, error) {
		if ins.ID == "b" {
			hitDead.Add(1) // b 被标记不健康后绝不应再被命中
			return nil, errors.New("b is down")
		}
		return &model.Response{Status: 200, InstanceID: "a"}, nil
	}
	c, reg, _, _ := setup(t, invoker)

	// b 健康检查失败摘除
	reg.SetHealth("shop", "b", false)
	for i := 0; i < 20; i++ {
		resp, err := c.Invoke(context.Background(),
			&model.Request{ID: "churn-" + itoa(i), Service: "shop", Method: "charge", Idempotent: true})
		if err != nil {
			t.Fatalf("healthy instance must serve: %v", err)
		}
		if resp.InstanceID != "a" {
			t.Fatalf("traffic hit unhealthy instance: %s", resp.InstanceID)
		}
	}
	if hitDead.Load() != 0 {
		t.Fatalf("unhealthy instance received %d calls", hitDead.Load())
	}

	// 最后一个实例也被摘除 -> 原因必须是"全不健康"而非"无服务"
	reg.SetHealth("shop", "a", false)
	_, err := c.Invoke(context.Background(),
		&model.Request{ID: "churn-all-down", Service: "shop", Method: "charge"})
	if !errors.Is(err, model.ErrNoHealthyInstance) {
		t.Fatalf("want ErrNoHealthyInstance, got %v", err)
	}
}

// TestDeterministicReasonsAcrossRepeatedRuns 相同输入重复执行，
// 失败原因稳定一致且可区分。
func TestDeterministicReasonsAcrossRepeatedRuns(t *testing.T) {
	// 独立配置：抬高熔断阈值，避免跨请求的熔断状态干扰"重试耗尽"确定性验证。
	isolated := `{
  "version": 2,
  "service": {
    "name": "shop",
    "methods": {
      "charge": {"timeout":"1s","max_attempts":4,"circuit_threshold":1000},
      "slow":   {"timeout":"40ms","max_attempts":1,"circuit_threshold":1000}
    }
  }
}`
	parsed, err := config.Parse([]byte(isolated))
	if err != nil {
		t.Fatal(err)
	}
	holder := config.NewHolder(parsed)
	reg := registry.NewRegistry()
	reg.Upsert("shop", model.Instance{ID: "a", Address: "a:1", Weight: 1})
	invoker := func(ctx context.Context, ins model.Instance, req *model.Request) (*model.Response, error) {
		if req.Method == "slow" {
			select {
			case <-time.After(time.Second):
				return &model.Response{Status: 200}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return nil, errors.New("500")
	}
	c, err := client.New(reg, holder, invoker)
	if err != nil {
		t.Fatal(err)
	}

	// 幂等重试耗尽：每次都是 ErrRetryExhausted 且恰好 4 次尝试
	for run := 0; run < 3; run++ {
		resp, err := c.Invoke(context.Background(),
			&model.Request{ID: "det-charge", Service: "shop", Method: "charge", Idempotent: true})
		if !errors.Is(err, model.ErrRetryExhausted) {
			t.Fatalf("run %d: want exhausted, got %v", run, err)
		}
		if resp.Attempts != 4 {
			t.Fatalf("run %d: want 4 attempts, got %d", run, resp.Attempts)
		}
	}
	// 慢方法：每次都是 ErrTimeout
	for run := 0; run < 3; run++ {
		_, err := c.Invoke(context.Background(),
			&model.Request{ID: "det-slow", Service: "shop", Method: "slow"})
		if !errors.Is(err, model.ErrTimeout) {
			t.Fatalf("run %d: want timeout, got %v", run, err)
		}
	}
}

// TestNoDuplicateSideEffects 非幂等写请求在失败抖动下只产生一次副作用。
func TestNoDuplicateSideEffects(t *testing.T) {
	var sideEffects atomic.Int32
	invoker := func(ctx context.Context, ins model.Instance, req *model.Request) (*model.Response, error) {
		sideEffects.Add(1) // 真实副作用：扣款/发消息
		return nil, errors.New("write conflict")
	}
	c, _, _, log := setup(t, invoker)
	_, err := c.Invoke(context.Background(),
		&model.Request{ID: "side-effect-1", Service: "shop", Method: "charge", Idempotent: false})
	if !errors.Is(err, model.ErrNonRetryable) {
		t.Fatalf("want ErrNonRetryable, got %v", err)
	}
	if sideEffects.Load() != 1 {
		t.Fatalf("exactly one side effect allowed, got %d", sideEffects.Load())
	}
	// 日志须记录请求标识、命中的策略
	if !strings.Contains(log.String(), "side-effect-1") ||
		!strings.Contains(log.String(), "policy=retry") {
		t.Fatalf("log must identify request and policy:\n%s", log.String())
	}
}

// TestBadConfigsRejectedBeforeServing 配置异常必须被明确拒绝，
// 热更新坏配置不得影响当前服务。
func TestBadConfigsRejectedBeforeServing(t *testing.T) {
	bad := []string{
		`{"version":2,"service":{"name":"shop","methods":{"charge":{"unknown_x":1}}}}`,
		`{"version":42,"service":{"name":"shop","methods":{"charge":{}}}}`,
		`{"version":2,"service":{"name":"shop","methods":{"charge":{"on_limit":"wait","max_queue":0}}}}`,
		`{"version":2,"service":{"name":"","methods":{}}}`,
	}
	for _, b := range bad {
		if _, err := config.Parse([]byte(b)); !errors.Is(err, model.ErrConfigRejected) {
			t.Fatalf("config must be rejected:\n%s\ngot: %v", b, err)
		}
	}

	invoker := func(ctx context.Context, ins model.Instance, r *model.Request) (*model.Response, error) {
		return &model.Response{Status: 200}, nil
	}
	c, _, holder, _ := setup(t, invoker)
	_, beforeRev := holder.Snapshot()
	// 坏配置替换被拒绝，revision 不变，服务继续按旧配置运行
	badParsed, perr := config.Parse([]byte(bad[0]))
	if perr == nil {
		t.Fatal("expected parse failure")
	}
	if _, err := holder.Replace(badParsed); !errors.Is(err, model.ErrConfigRejected) {
		t.Fatal("nil/garbage replace must be rejected")
	}
	if _, rev := holder.Snapshot(); rev != beforeRev {
		t.Fatal("failed hot update must not change active revision")
	}
	if _, err := c.Invoke(context.Background(),
		&model.Request{ID: "after-bad-config", Service: "shop", Method: "charge"}); err != nil {
		t.Fatalf("service must keep working with old config: %v", err)
	}
}

// TestLogsExplainRequestPolicyAndResponse 日志三要素齐全。
func TestLogsExplainRequestPolicyAndResponse(t *testing.T) {
	invoker := func(ctx context.Context, ins model.Instance, r *model.Request) (*model.Response, error) {
		return &model.Response{Status: 202, Payload: []byte("accepted")}, nil
	}
	c, _, _, log := setup(t, invoker)
	_, err := c.Invoke(context.Background(),
		&model.Request{ID: "trace-xyz", Service: "shop", Method: "charge"})
	if err != nil {
		t.Fatal(err)
	}
	out := log.String()
	for _, want := range []string{"req=trace-xyz", "policy=invoke", "outcome=success", "status=202"} {
		if !strings.Contains(out, want) {
			t.Fatalf("log missing %q:\n%s", want, out)
		}
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [4]byte
	n := len(b)
	for i > 0 {
		n--
		b[n] = byte('0' + i%10)
		i /= 10
	}
	return string(b[n:])
}
