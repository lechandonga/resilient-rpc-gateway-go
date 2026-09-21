# Resilient RPC Gateway — 服务网格治理层（Go）

一个**进程内、零外部依赖、可本地运行**的 RPC 治理层。网关在服务实例动态变化、
下游抖动与调用失败时，仍能给出**确定**且**可解释**的行为：每一次放行、拒绝、
重试、熔断都有稳定的原因码，并可在日志中按请求标识追溯。

## 快速开始

```bash
go run ./cmd/demo          # 端到端演示 5 个治理场景
go test ./... -v           # 全部单元 + 集成测试
go vet ./...               # 静态检查
```

运行 demo 会打印每个请求的决策轨迹，例如：

```
req=demo-001 rev=1 attempt=1 instance=inst-a policy=invoke outcome=failure reason=DOWNSTREAM_ERROR resp=
req=demo-001 rev=1 attempt=2 instance=inst-b policy=invoke outcome=failure reason=DOWNSTREAM_ERROR resp=
req=demo-001 rev=1 attempt=3 instance=inst-a policy=invoke outcome=success reason=OK resp=status=200 bytes=11
```

字段含义：`req` 请求标识 · `rev` 命中的配置版本 · `attempt` 第几次尝试 ·
`instance` 下游实例 · `policy` 命中的治理策略 · `outcome/reason` 决策与稳定原因码 ·
`resp` 下游响应摘要。

## 架构与包结构

| 包 | 职责 |
|----|------|
| `pkg/model` | 核心类型与**稳定错误原因码**（`GATEWAY_*`） |
| `pkg/registry` | 本地注册中心：上下线、权重、健康状态；周期性健康检查 |
| `pkg/loadbalancer` | 平滑加权轮询（默认）/ 加权随机；实例集合变化即时生效 |
| `pkg/deadline` | 上游截止时间与单尝试预算传播，取两者更紧者 |
| `pkg/retry` | 幂等门禁 + 指数退避重试游标 |
| `pkg/circuitbreaker` | 按「实例×方法」维度的 closed/open/half-open 状态机 |
| `pkg/concurrency` | 按维度的并发额度与有界排队（令牌转移，无超发） |
| `pkg/config` | 严格配置解析、版本兼容、默认值、带 revision 的原子热更新 |
| `pkg/client` | 编排以上组件的治理层入口与决策轨迹日志 |
| `cmd/demo` | 可运行演示 |
| `test/integration` | 跨组件集成场景（抖动 / 超时 / 副作用 / 配置异常） |

一次调用的决策顺序：

```
固定配置 revision → 服务发现 → 负载均衡选实例 → 熔断判定
   → 并发/排队额度 → 派生更紧预算的下游 ctx → 调用
   → 记录结果驱动熔断 →（失败且幂等且有预算）退避后重试
```

## 治理策略与确定性行为

### 1. 实例发现与负载均衡
- 实例**每次尝试前重新发现**：下线、健康检查失败、权重变更在下一次选择立即生效，
  流量不会持续打向已失效实例（见 `TestRemovedInstanceStopsReceivingTraffic`）。
- 负载均衡器在健康实例快照上选择；已消失实例的内部游标会被清理，**内存有界**。
- 空实例列表**快速失败**且两种原因可区分：
  - 服务从未注册 → `GATEWAY_NO_INSTANCE`
  - 服务存在但实例全不健康/摘除 → `GATEWAY_NO_HEALTHY_INSTANCE`

### 2. 超时与截止时间传播
- 下游单次尝试的预算 = `min(上游 ctx 剩余时间, 方法 timeout)`，更紧者生效。
- 剩余预算为 0 时**不建立下游调用**，直接按 `GATEWAY_TIMEOUT` 失败。
- 下游慢于预算：治理层不等待，立即返回并 `cancel()` 子 ctx、释放并发额度；
  假下游应监听 `ctx.Done()`，从而不在下游留下仍在推进的调用。
- 上游主动取消 → `GATEWAY_CANCELED`，与超时 `GATEWAY_TIMEOUT` **严格区分**，
  且取消/超时均**不触发重试**（已无剩余预算，重试无意义）。

### 3. 重试与幂等
- 只有请求显式标记 `Idempotent=true` 才允许第 2 次及以后的尝试；
  非幂等请求失败即终态，返回 `GATEWAY_NON_RETRYABLE`，**副作用只发生一次**。
- 幂等请求最多尝试 `max_attempts` 次，超限返回 `GATEWAY_RETRY_EXHAUSTED`
  （错误链中带最后一次下游错误），而不是笼统的失败。
- 重试间使用有上限的指数退避；若退避就会耗尽剩余预算，则直接快速失败。
- 熔断打开 / 并发受限属于实例级问题，允许换实例再试（仍受上述门禁约束）。

### 4. 熔断与半开恢复（维度：实例×方法）
- `closed`：连续失败达 `circuit_threshold` → `open`。
- `open`：所有请求 `GATEWAY_CIRCUIT_OPEN` 快速失败，**不冲击故障下游**；
  冷却 `open_cooldown` 后进入 `half-open`。
- `half-open`：只放行 `half_open_probes` 个探测；
  **任一探测失败立即退回 `open` 并重新计时**，绝不"探测失败还放行"；
  探测全部成功才回到 `closed`。

### 5. 并发与资源上限
- 按维度（实例×方法）限制执行并发 `max_concurrency`，超限按 `on_limit`：
  - `reject`（默认）：立即 `GATEWAY_CONCURRENCY_LIMITED`；
  - `wait`：进入长度 `max_queue` 的有界队列，队满 `GATEWAY_QUEUE_FULL`。
- 在途总数恒满足 `active ≤ max_concurrency`、`active+queue ≤ max_concurrency+max_queue`，
  释放时令牌**直接转移**给队首（计数不变，绝无超发）；空闲维度被清理，
  排队期间 ctx 取消/超时会退出排队，**不会无界堆积内存**。

## 配置：兼容规则与热更新

配置为 JSON，当前最高版本 `version: 2`。示例：

```json
{
  "version": 2,
  "service": {
    "name": "shop",
    "methods": {
      "charge": {"timeout":"1s","max_attempts":3},
      "slow":   {"timeout":"80ms","on_limit":"wait","max_queue":16}
    }
  }
}
```

**稳定默认值**（旧配置缺省字段，缺失即落到下列值，见 `model.DefaultMethodPolicy`）：

| 字段 | 默认值 |
|------|--------|
| `timeout` | `1s` |
| `max_attempts` | `1`（不重试） |
| `lb` | `weighted_round_robin` |
| `circuit_threshold` | `5` |
| `open_cooldown` | `5s` |
| `half_open_probes` | `1` |
| `max_concurrency` | `100` |
| `max_queue` | `0` |
| `on_limit` | `reject` |

**明确拒绝（返回 `GATEWAY_CONFIG_REJECTED`，绝不静默忽略）：**
- 未知字段（解析器 `DisallowUnknownFields`）、非法 JSON、多个文档；
- 不支持的版本（`version < 0` 或 `> 2`）；
- 未知枚举（`lb` / `on_limit` 取值非法）；
- 取值越界（attempts/阈值/并发 < 1、队列 < 0、duration 非法或 ≤ 0）；
- 相互冲突：`on_limit=wait` 却 `max_queue=0`；`on_limit=reject` 却 `max_queue>0`；
  旧版本（<2）携带 v2 才有的 `half_open_probes`。

**版本兼容**：接受 `0..2` 的历史版本；旧版本缺失的新字段一律取默认语义。
版本 1 没有半开探测数概念，因此固定为 1 且不允许显式设置。

**热更新可复现**：`config.Holder.Replace` 整体替换并把 `revision` 单调 +1；
请求在进入时一次性读取并**固定**该 revision，因此更新前后的请求命中不同版本，
可通过 `Holder.RevisionOf(rev)` 与日志中的 `rev=` 字段区分、复现。
坏配置在 `Parse` 阶段即被拒绝，根本不会进入 `Replace`，线上策略不受影响。

## 失败原因码对照表

| 原因码 | 含义 | 是否可重试 |
|--------|------|-----------|
| `GATEWAY_NO_INSTANCE` | 服务无任何实例 | 否（发现层） |
| `GATEWAY_NO_HEALTHY_INSTANCE` | 实例全不健康 | 否（发现层） |
| `GATEWAY_TIMEOUT` | 超过自身/上游预算 | 否（预算已尽） |
| `GATEWAY_CANCELED` | 上游主动取消 | 否 |
| `GATEWAY_NON_RETRYABLE` | 非幂等请求拒绝重试 | 否 |
| `GATEWAY_RETRY_EXHAUSTED` | 达到重试上限 | 终态 |
| `GATEWAY_CIRCUIT_OPEN` | 熔断打开/半开额度满 | 可换实例再试 |
| `GATEWAY_CONCURRENCY_LIMITED` | 并发超限且策略为拒绝 | 可换实例再试 |
| `GATEWAY_QUEUE_FULL` | 等待队列已满 | 可换实例再试 |
| `GATEWAY_CONFIG_REJECTED` | 配置非法被拒绝 | 不适用 |

所有原因码均可用 `errors.Is(err, model.ErrXxx)` 精确判定（错误被包装后仍可命中）。

## 本地验证指引

```bash
# 1. 单元测试（每个组件的边界行为）
go test ./pkg/... -v

# 2. 重点场景集成测试
go test ./test/integration/ -v
#   - TestInstanceChurnEventuallyHitsHealthy    实例抖动，不打已摘除实例
#   - TestDeterministicReasonsAcrossRepeatedRuns 重复执行原因一致可区分
#   - TestNoDuplicateSideEffects                非幂等无重复副作用
#   - TestBadConfigsRejectedBeforeServing       配置异常被拒且不影响线上
#   - TestLogsExplainRequestPolicyAndResponse   日志含请求标识/策略/响应

# 3. 可观察演示
go run ./cmd/demo
```

## 设计取舍

- **无外部中间件**：注册中心、健康检查、熔断状态全部在进程内，便于本地确定性测试；
  生产可把 `registry.Registry` / `Invoker` 替换为接真实注册中心与 RPC 传输的实现。
- **每次尝试重新发现 + 每请求固定配置**：实例视图追求"最新"，策略视图追求"可复现"。
- **维度选择**：熔断与并发按「实例×方法」隔离，单实例单方法故障不波及其他方法。
