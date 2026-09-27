# Resilient RPC Gateway — 服务网格治理层（Go）

一个**进程内、零外部依赖、可本地运行**的 RPC 治理层。网关在服务实例动态变化、
下游抖动与调用失败时，仍能给出**确定**且**可解释**的行为：每一次放行、拒绝、
重试、熔断都有稳定的原因码，并可在日志中按请求标识追溯。

## 快速开始

```bash
go run ./cmd/demo          # 端到端演示 6 个治理场景（含优雅排空）
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
| `pkg/drain` | 进程级优雅排空生命周期（拒绝新请求 / 宽限等待 / 强制收尾） |
| `pkg/config` | 严格配置解析、版本兼容、默认值、带 revision 的原子热更新 |
| `pkg/client` | 编排以上组件的治理层入口、决策轨迹日志与治理状态快照 |
| `cmd/demo` | 可运行演示（含优雅排空两种结局） |
| `test/integration` | 跨组件集成场景（抖动 / 超时 / 副作用 / 配置异常 / 取消 / 排空） |

一次调用的决策顺序：

```
入口取消/截止检查 → 排空门禁 Admit → 固定配置 revision → 服务发现 → 负载均衡选实例
   → 熔断判定 → 取消/预算预检 → 并发/排队额度（可取消）
   → 派生更紧预算的下游 ctx → 调用 → 记录结果驱动熔断
   →（失败且幂等且有预算）可被取消的退避后重试
```

## 治理策略与确定性行为

### 1. 实例发现与负载均衡
- 实例**每次尝试前重新发现**：下线、健康检查失败、权重变更在下一次选择立即生效，
  流量不会持续打向已失效实例（见 `TestRemovedInstanceStopsReceivingTraffic`）。
- 负载均衡器在健康实例快照上选择；已消失实例的内部游标会被清理，**内存有界**。
- 空实例列表**快速失败**且两种原因可区分：
  - 服务从未注册 → `GATEWAY_NO_INSTANCE`
  - 服务存在但实例全不健康/摘除 → `GATEWAY_NO_HEALTHY_INSTANCE`

### 2. 超时、取消与截止时间传播（全链路收敛）
- 下游单次尝试的预算 = `min(上游 ctx 剩余时间, 方法 timeout)`，更紧者生效。
- 剩余预算为 0 时**不建立下游调用**，直接按 `GATEWAY_TIMEOUT` 失败。
- **取消在整条链路上彻底收敛**，以下任何一个时点收到取消/截止，都不会再产生新的下游调用，
  也不会被重试、换实例或半开探测绕过：
  - 请求**刚进入 `Invoke` 时**上游 ctx 已取消/已过期：直接返回，不占任何额度；
  - **等待并发额度 / 排队**期间：退出排队，排队位置立即移除，返回取消原因；
  - **重试退避**期间：结束等待，不再发起下一次尝试；
  - **下游调用在途中**：治理层不等待，立即返回并 `cancel()` 子 ctx、释放并发额度，
    下游应监听 `ctx.Done()` 及时退出，进程里不会留下继续向前跑的调用。
- 三类取消原因彼此**严格区分**：上游主动取消 `GATEWAY_CANCELED`、
  预算到期 `GATEWAY_TIMEOUT`、排空宽限到期 `GATEWAY_DRAIN_TIMEOUT`（见下节）。
- 取消 / 超时 / 排空强制结束均为**终态，不触发重试**（已无剩余空间，重试无意义）。
- 取消不算下游成败：半开期占用的探测名额会被**归还**（`RecordAbandon`），
  熔断状态不变；只有"真正打到下游且因预算到期无结果"才按一次失败计入熔断。
  这样取消既不会把半开名额永久泄漏，也不会被误判成探测成功/失败。
- 占用的资源（进程在途计数、并发额度、排队位置、半开探测名额）在请求结束路径上
  一一释放，计数必然回落（集成测试与 `-race` 长跑断言）。

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

### 6. 优雅排空（graceful drain）
两级排空，规则统一：**先拒新、再等在途、宽限到则强制收尾**。

**进程级排空（停服务 / 收到停止信号）**：
- `c.BeginDrain(grace)` 进入排空，或直接 `snap := c.Shutdown(grace)`（进入并等待结束）；
- 进入后排空门禁拒绝**一切新请求**，原因码 `GATEWAY_DRAINING`，拒绝不计数、不打下游；
- 已在途请求继续执行，各自受自己的剩余预算约束，在 `grace` 宽限内尽量跑完；
- 宽限到期仍未完成：以 `GATEWAY_DRAIN_TIMEOUT` 为 cause 强制取消请求及其全部下游派生
  ctx，在途请求**快速失败**，下游 goroutine 经 ctx 取消退出；随后排空完成，
  计数归零，进程可以干净退出，**不会有调用继续**；
- 在途在宽限内全部跑完则**干净完成**（快照 `timed_out=false`）；
  宽限到期强制收尾为 `timed_out=true`，两种结局可区分；
- `BeginDrain` 幂等（以第一次为准）；无在途时立即完成；`grace<=0` 立即强制收尾。

**实例级排空（摘单个实例）**：
- `c.DrainInstance(service, instanceID)`（底层 `registry.Drain`）：实例保留在注册中心，
  在途请求**不被硬切断**、继续跑完，但 `ListHealthy` 立即不再返回它，新请求绝不命中；
- 在途跑完后由 `registry.Remove` 彻底摘除；误摘可 `Undrain` 或重新 `Upsert` 恢复路由。

### 7. 治理状态快照（一致、可复现）
`c.Snapshot()` 返回 `model.GovernanceSnapshot`；`c.DrainSnapshot()` 只看排空。
快照各子项均按稳定键（service / instance#method）排序，**同输入重复查询结果可复现**。

| 字段 | 含义 |
|------|------|
| `TakenAt` | 快照采样时刻 |
| `Revision` | 快照时生效的配置 revision |
| `TotalInFlight` | 治理层此刻受理中的请求总数（排队 + 退避等待 + 下游在途），与 `Drain.InFlight` 恒等 |
| `Drain.Phase` | 排空阶段：`idle` / `draining` / `complete` |
| `Drain.StartedAt` / `Deadline` | 排空开始 / 宽限截止时间（idle 时为零值） |
| `Drain.InFlight` | 排空开始后仍在途的请求数；完成时为 0 |
| `Drain.GraceLeft` | 距离宽限到期剩余时间（仅排空中） |
| `Drain.TimedOut` | 是否因宽限到期强制收尾（false=在途全部跑完的干净完成） |
| `Limiters[].Key` | 维度键 `instanceID#method` |
| `Limiters[].Active` / `Queued` | 正在执行数（恒 ≤ `MaxConcurrency`）/ 排队等待数（≤ `MaxQueue`） |
| `Limiters[].MaxConcurrency` / `MaxQueue` | 该维度生效的上限，便于对账 |
| `Breakers[].State` | `closed` / `open` / `half-open` |
| `Breakers[].Failures` | closed 期连续失败数 |
| `Breakers[].ProbeInflight` / `ProbeSucceeded` | half-open 期已发放未记账 / 已成功的探测数 |
| `Breakers[].CooldownLeft` | open 期距离进入 half-open 的冷却剩余时间（0=已可转换或非 open） |
| `Instances[]` | 每个注册实例的 `Service/ID/Address/Weight/Healthy/Draining` |

**自洽性保证**：进程在途计数由排空管理器在同一把锁内与阶段切换一起维护；
并发/熔断/注册子项各自在自己的锁内一次拷贝，内部字段彼此不矛盾。恒有
`TotalInFlight == Drain.InFlight`，且 `sum(Active+Queued) ≤ TotalInFlight`
（差额是尚未走到额度阶段的在途，非负），`Active ≤ MaxConcurrency`、
`Queued ≤ MaxQueue`。这些不变量在并发负载下由 `TestSnapshotConsistencyUnderLoad`
（`-race`）持续对账。

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

## 取消与排空：与旧行为的兼容范围

本次新增能力对既有调用方**默认无侵入**：

- **错误码只增不改**：新增 `GATEWAY_DRAINING`、`GATEWAY_DRAIN_TIMEOUT`；
  既有 `GATEWAY_CANCELED` / `GATEWAY_TIMEOUT` 的触发条件、`errors.Is` 判定方式不变。
  既有代码若只区分"成功/失败"，无需任何改动；若要区分排空结局，再新增对两个码的判定即可。
- **不需要排空就什么都不用做**：不显式调用 `BeginDrain/Shutdown/DrainInstance` 时，
  `Client` 内部排空管理器恒为 `idle`，`Admit` 永远放行，行为与旧版本完全一致
  （既有用例全部不回归）。
- **取消收敛是行为收紧而非放宽**：旧版本里"排队/退避期间被取消仍可能换实例再试"的边角
  路径被堵死——取消后绝不重试。此前依赖"取消后仍被重试"（理论上无意义且浪费资源）的
  调用方才会观察到差异；正常尊重 ctx 的上游只会更早、更干净地拿回资源。
- **半开探测记账更严**：取消在途探测改为归还名额（不再被计入失败）。
  对尊重 ctx 的系统这更准确（取消不等于下游故障），探测仍会在后续请求时重新进行。
- **实例摘除语义**：`registry.Remove` 仍是立即彻底摘除（既有语义不变）；
  需要"先排空再摘除"的调用方改用 `Drain` → 在途跑完 → `Remove` 两步法。
- **配置 schema 不变**：排空宽限不是方法策略字段，而是运维期 `BeginDrain(grace)` 的
  运行时参数，因此配置版本仍为 `version: 2`，旧配置文件全部继续有效。

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
| `GATEWAY_DRAINING` | 进程/实例排空中，新请求被拒 | 否（排空期不再受理） |
| `GATEWAY_DRAIN_TIMEOUT` | 排空宽限到期，在途被强制快速失败 | 否（终态） |
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

# 3. 取消收敛与优雅排空新增场景（test/integration/lifecycle_test.go）
go test ./test/integration/ -v -run 'Canceled|Drain|InstanceDrain'
#   - TestCanceledBeforeReachingDownstream  进来时已取消/已过期：零下游调用、零占用
#   - TestCanceledWhileQueued               排队中取消：退出排队、不打下游、额度可复用
#   - TestDrainRejectsNewRequests           排空中新请求 GATEWAY_DRAINING，零下游调用
#   - TestDrainGraceTimeoutFailsInflight    宽限到期 GATEWAY_DRAIN_TIMEOUT，ctx 被取消、计数归零
#   - TestDrainLetsInflightFinishWithinBudget 在途在宽限内跑完：干净完成
#   - TestInstanceDrainStopsNewTrafficKeepsInflight 实例排空：不切在途、不接新流量
# 配套 client 层用例：
go test ./pkg/client/ -v -run 'TestCancelDuringRetryBackoff|TestCanceledHalfOpenProbe|TestSnapshotConsistency'
#   - TestCancelDuringRetryBackoffSkipsFurtherAttempts 退避中取消不再尝试
#   - TestCanceledHalfOpenProbeReleasesSlot            取消归还半开名额且不锁死探测
#   - TestSnapshotConsistencyUnderLoad                 并发负载下快照数值对账自洽
# drain 生命周期与注册中心实例排空：
go test ./pkg/drain/ ./pkg/registry/ -v

# 4. 竞态检测全量用例（建议多跑几轮确认稳定）
CGO_ENABLED=1 go test -race -count=1 ./...
for i in 1 2 3 4 5; do CGO_ENABLED=1 go test -race -count=1 ./... || break; done
# 对时序敏感场景做高强度复跑：
CGO_ENABLED=1 go test -race -count=30 ./test/integration/ ./pkg/drain/ ./pkg/client/

# 5. 可观察演示（场景 6 演示优雅排空的两种结局）
go run ./cmd/demo
```

## 设计取舍

- **无外部中间件**：注册中心、健康检查、熔断状态全部在进程内，便于本地确定性测试；
  生产可把 `registry.Registry` / `Invoker` 替换为接真实注册中心与 RPC 传输的实现。
- **每次尝试重新发现 + 每请求固定配置**：实例视图追求"最新"，策略视图追求"可复现"。
- **维度选择**：熔断与并发按「实例×方法」隔离，单实例单方法故障不波及其他方法。
