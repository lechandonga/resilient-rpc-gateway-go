package model

import "time"

// 可区分的失败原因：调用方可用 errors.Is 精确判定治理层决策。
var (
	// ErrNoInstance 注册中心没有任何可用实例（列表为空）。
	ErrNoInstance = newGovError("GATEWAY_NO_INSTANCE", "no available service instance")
	// ErrNoHealthyInstance 实例列表非空，但全部处于不健康 / 摘除状态。
	ErrNoHealthyInstance = newGovError("GATEWAY_NO_HEALTHY_INSTANCE", "all instances are unhealthy")
	// ErrTimeout 本次调用超过自身预算或上游截止时间。
	ErrTimeout = newGovError("GATEWAY_TIMEOUT", "request deadline exceeded")
	// ErrContextCanceled 上游已显式取消，下游推进被中止。
	ErrContextCanceled = newGovError("GATEWAY_CANCELED", "request canceled by upstream")
	// ErrNonRetryable 请求未声明幂等，重试被拒绝或失败后不可重试。
	ErrNonRetryable = newGovError("GATEWAY_NON_RETRYABLE", "request is not safe to retry")
	// ErrRetryExhausted 已达到重试上限仍未成功。
	ErrRetryExhausted = newGovError("GATEWAY_RETRY_EXHAUSTED", "retry attempts exhausted")
	// ErrCircuitOpen 命中熔断打开状态，请求未下发。
	ErrCircuitOpen = newGovError("GATEWAY_CIRCUIT_OPEN", "circuit breaker is open")
	// ErrConcurrencyLimited 并发 / 排队额度超限且策略为拒绝。
	ErrConcurrencyLimited = newGovError("GATEWAY_CONCURRENCY_LIMITED", "concurrency limit exceeded")
	// ErrQueueFull 等待队列已满。
	ErrQueueFull = newGovError("GATEWAY_QUEUE_FULL", "pending queue is full")
	// ErrConfigRejected 配置不合法（冲突、未知字段、坏版本），必须显式拒绝。
	ErrConfigRejected = newGovError("GATEWAY_CONFIG_REJECTED", "policy configuration rejected")
)

// Instance 是注册中心中的一个下游实例快照。
type Instance struct {
	ID       string
	Address  string
	Weight   int
	Healthy  bool
	Metadata map[string]string
}

// Request 是一次 RPC 调用的输入声明。
type Request struct {
	ID         string // 请求标识，贯穿日志与重试。
	Service    string // 目标服务名。
	Method     string // 方法名（方法级治理维度）。
	Idempotent bool   // 是否幂等：只有 true 才允许失败重试。
	Payload    []byte
	Metadata   map[string]string
}

// Response 是下游返回结果或治理层合成结果。
type Response struct {
	InstanceID string
	Status     int
	Payload    []byte
	Attempts   int    // 实际尝试次数（含首次）。
	Reason     string // 可解释的失败/放行原因码。
}

// RejectPolicy 超限时的处理策略。
type RejectPolicy string

const (
	RejectReject RejectPolicy = "reject" // 立即拒绝
	RejectWait   RejectPolicy = "wait"   // 排队等待，受队列长度与预算约束
)

// LBKind 负载均衡算法。
type LBKind string

const (
	LBWeightedRoundRobin LBKind = "weighted_round_robin"
	LBRandom             LBKind = "random"
)

// MethodPolicy 是单个方法的治理策略集合（已解析、已校验的内部形态）。
type MethodPolicy struct {
	Version          int
	Method           string
	Timeout          time.Duration
	MaxAttempts      int // 总尝试次数（1 表示不重试）
	LB               LBKind
	CircuitThreshold int           // 连续失败多少次打开熔断
	OpenCooldown     time.Duration // 打开后多久进入半开
	HalfOpenProbes   int           // 半开期允许的探测数
	MaxConcurrency   int           // 单实例并发上限
	MaxQueue         int           // 排队上限（RejectWait 时生效）
	OnLimit          RejectPolicy
}

// DefaultMethodPolicy 返回版本 0/旧配置缺省字段时使用的稳定默认值。
func DefaultMethodPolicy(method string) MethodPolicy {
	return MethodPolicy{
		Version:          0,
		Method:           method,
		Timeout:          time.Second,
		MaxAttempts:      1,
		LB:               LBWeightedRoundRobin,
		CircuitThreshold: 5,
		OpenCooldown:     5 * time.Second,
		HalfOpenProbes:   1,
		MaxConcurrency:   100,
		MaxQueue:         0,
		OnLimit:          RejectReject,
	}
}

// Attempt 记录每次尝试的决策轨迹，用于可解释日志。
type Attempt struct {
	AttemptNo  int
	InstanceID string
	Policy     string
	Outcome    string
	Reason     string
	Duration   time.Duration
}
