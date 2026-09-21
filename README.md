# Resilient RPC Gateway — Go 治理层（bootstrap-a）

一套可在**本地运行**的 RPC 服务网格治理层。它位于业务调用与下游传输之间，
在服务实例动态变化、下游抖动与调用失败时，给出**确定且可解释**的行为。

- 无任何第三方依赖，仅用 Go 标准库。
- 每个失败都带一个**稳定的错误码**（不是字符串匹配）。
- 每个请求都返回结构化 `Outcome`：配置版本、每次尝试命中的策略、
  命中的实例、下游状态码与错误码；日志打印请求标识、命中策略与下游响应。
- 配置热更新采用**版本分代（generation）**：请求入口 pin 配置版本，
  在途请求始终使用旧版本行为，新请求使用新版本，结果可复现、可区分。

## 快速开始

```bash
go run ./cmd/demo        # 本地自包含演示（实例抖动/超时/幂等/熔断/热更新/坏配置）
go test ./...            # 全部单元/行为测试
go test -race ./...      # 带竞态检测（需 CGO_ENABLED=1）
go vet ./...
```

## 包结构

```
internal/gov/
  gerr       可区分错误码（no_instance / deadline_exceeded / circuit_open / ...）
  registry   本地内存注册中心：上下线、权重、健康检查、订阅事件
  lb         负载均衡：round_robin / weighted / random + 故障实例冷却摘除
  deadline   超时与截止时间传递：总预算裁剪、可取消退避
  retry      重试分类与指数退避；只有幂等请求可重试
  breaker    熔断器：closed/open/half_open，半开探测失败立即回退 open
  bulkhead   并发与排队上限：有界并发槽 + 有界等待队列，拒绝或有界等待
  config     版本化配置：默认值、严格校验、未知字段拒绝、原子热更新
  gateway    编排层：把上述能力按确定顺序串成一次受治理的调用
cmd/demo     本地可运行演示
```

## 一次请求的处理顺序（确定性管线）

1. **pin 配置快照与治理器分代**（单调递增 `config_version`，整个请求不变）。
2. 建立**总截止时间** `total_timeout`，覆盖所有尝试与退避。
3. **服务发现**：
   - 注册表里该服务从未有实例 → `no_instance`（快速失败）；
   - 存在实例但全部健康检查失败 → `instance_unhealthy`（原因可区分）。
4. **负载均衡**选实例；故障实例进入短暂冷却，避免持续打向已失效实例。
5. **熔断**检查（按 `instance` 或 `method` 维度）：
   open 期间不触碰下游，返回 `circuit_open`。
6. **并发/排队上限**（按 `instance` 或 `method` 维度）：
   超限按策略拒绝（`concurrency_limit` / `queue_limit`）或有界等待。
7. **尝试级截止时间** `attempt_timeout`，并被剩余总预算裁剪；
   派发前预算已耗尽 → `timeout_before_dispatch`，不占连接/不占并发槽。
8. 调用下游，下游 `ctx` 与总预算/上游取消联动：
   上游取消会传播到在途下游调用，不会留下仍在推进的调用。
9. 根据结果更新熔断、LB 成败计数，并做重试决策与可取消退避。

## 错误码（稳定契约）

| 错误码 | 含义 |
| --- | --- |
| `no_instance` | 服务下没有任何已注册实例 |
| `instance_unhealthy` | 有实例，但健康检查后全部不可用 |
| `deadline_exceeded` | 某次尝试超过截止时间（总预算仍在时**允许重试**） |
| `timeout_before_dispatch` | 派发前总预算已耗尽；未占用连接/并发额度 |
| `canceled` | 上游取消；传播到下游，不重试 |
| `non_retryable` | 非幂等请求或关闭重试：失败后不允许再来一次 |
| `retry_budget_exhausted` | 幂等请求已用到 `max_attempts` 上限（与"不可重试"区分） |
| `circuit_open` | 熔断打开/半开窗口内无探测名额 |
| `concurrency_limit` | 并发槽已满且策略为立即拒绝 |
| `queue_limit` | 等待队列已满，或排队超时 |
| `config_invalid` | 配置未知字段、非法枚举、冲突或版本不支持 |
| `downstream_error` | 下游返回失败（如 5xx）或传输错误 |

## 重试与幂等语义

- 只有 `Request.Idempotent == true` 的请求才可能重试；
  非幂等请求**任何情况下只调用一次**下游，因此不会重复产生副作用。
- 结构性失败（熔断打开、无实例、并发/队列上限、上游取消、配置非法）
  立即停止，不浪费重试。
- 尝试级超时在**总预算仍有余量**时允许重试；
  超过 `max_attempts` 返回 `retry_budget_exhausted`，
  与"因非幂等而禁止重试"的 `non_retryable` 明确区分。
- 退避为带上限的指数退避（可关 jitter），退避等待可被取消/超时打断。

## 熔断与半开恢复

- 维度：`breaker_scope: instance | method`。
- 连续失败达到 `breaker_failure_threshold` 后打开；打开期间快速失败。
- 冷却 `breaker_open_cooldown` 过后进入 half-open，**同时只放行一个探测**；
  - 连续 `breaker_probe_success` 次探测成功 → 关闭；
  - 任意一次探测失败 → 立即退回 open，绝不放行。

## 并发与资源上限

- 维度：`bulkhead_scope: instance | method`。
- `max_concurrent`：在途并发上限；`max_queued`：等待队列长度（始终有界）。
- `queue_policy: reject | wait`：
  - `reject`：槽满立即返回 `concurrency_limit`；
  - `wait`：进入有界队列，槽释放后 FIFO 唤醒；
    队列满返回 `queue_limit`，超过 `queue_timeout` 或请求 ctx 结束也返回。
- 每个 key 的内存占用恒为 `max_concurrent + max_queued`，不会无界堆积。

## 配置：版本兼容、默认值与拒绝规则

配置示例（热更新内容即 JSON 文档）：

```json
{
  "version": 1,
  "service": "demo.svc",
  "lb_policy": "round_robin",
  "attempt_timeout": "2s",
  "total_timeout": "5s",
  "retry_enabled": true,
  "max_attempts": 3,
  "initial_backoff": "100ms",
  "max_backoff": "1s",
  "backoff_multiplier": 2.0,
  "backoff_jitter": true,
  "breaker_enabled": true,
  "breaker_scope": "instance",
  "breaker_failure_threshold": 5,
  "breaker_open_cooldown": "5s",
  "breaker_probe_success": 1,
  "bulkhead_scope": "instance",
  "max_concurrent": 32,
  "max_queued": 64,
  "queue_timeout": "500ms",
  "queue_policy": "wait"
}
```

兼容规则（确定且稳定）：

- **旧配置缺失字段** → 一律落入内建默认值（见 `config.DefaultPolicy`），
  默认值是兼容契约的一部分，重复解析结果完全一致。
- **时间字段**同时接受字符串（`"200ms"`、`"1.5s"`）与数字（纳秒）。
- **未知字段** → 解析即拒绝（`DisallowUnknownFields`），不静默忽略拼写错误。
- **未知枚举**（如 `lb_policy: "magnetic"`）、**未来版本号**、
  **相互冲突**（如 `retry_enabled=true` 且 `max_attempts=1`、
  `attempt_timeout > total_timeout`、负数上限等）→ 一律 `config_invalid` 拒绝。
- **热更新**：`Manager.Replace / ReplaceJSON` 校验通过后原子替换，
  `config_version` 单调递增；校验失败时旧配置与版本号保持不变。
  每个 `Outcome.ConfigVersion` 标明该请求实际运行在哪一版，
  因而更新前后请求的行为可区分、可复现。

## 日志与可解释性

每次尝试都会记录 `AttemptLog`（attempt / instance / policy_hit / status /
err_code），日志形如：

```
[gov] request=req-002 service=demo.svc method=GetThing hit=downstream-failure instance=i-1 attempt=1 code=downstream_error cfg=1
[gov] request=req-002 hit=downstream-response instance=i-3 attempt=2 status=200 cfg=1 bytes=11
```

- `request`：请求标识；
- `hit`：命中的治理策略（`discovery-empty` / `breaker-open` /
  `bulkhead-reject` / `deadline-no-budget` / `downstream-failure` /
  `downstream-response` 等）；
- `instance`：本次尝试选中的下游；
- `cfg`：本次请求 pin 的配置版本。

## 本地验证建议

```bash
# 1) 看完整行为演示（含打印的结构化结果）
go run ./cmd/demo

# 2) 针对性的行为测试（实例抖动 / 下游超时 / 重复副作用 / 配置异常）
go test ./internal/gov/gateway/ -v
go test ./internal/gov/breaker/ ./internal/gov/bulkhead/ ./internal/gov/config/ -v

# 3) 竞态与静态检查
CGO_ENABLED=1 go test -race ./...
go vet ./...
```

重点测试映射到需求：

| 关注点 | 测试 |
| --- | --- |
| 实例上下线/健康失败/权重变化后流量迁移 | `TestInstanceChurnTrafficMoves`、`TestInstanceDeregisterMidStream`、`TestHealthCheckFailureRemovesTraffic`、`TestWeightChangeRespected` |
| 空实例列表原因可区分 | `TestNoInstanceNeverRegistered`、`TestAllInstancesUnhealthyIsDistinct` |
| 下游超时/总预算/上游取消 | `TestDownstreamTimeoutRetryThenSuccess`、`TestSlowInstanceDoesNotExceedAttemptDeadline`、`TestUpstreamCancelPropagatesDownstream`、`TestNoBudgetBeforeDispatchFailsFast` |
| 重复副作用/重试上限原因区分 | `TestNonIdempotentNeverRetries`、`TestIdempotentRetriesBoundedAndDistinctCode` |
| 熔断与半开恢复 | `TestBreakerOpensAndStopsDownstreamHits`、breaker 包 `TestHalfOpenFailureReturnsToOpen` 等 |
| 并发/排队上限 | bulkhead 包全部用例、`TestBulkheadRejectsAtMethodScope` |
| 配置异常/默认值/热更新可复现 | config 包全部用例、`TestHotReloadPinsConfigVersionPerRequest`、`TestHotReloadRetriesChangeIsObservable` |
