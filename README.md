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
| `pkg/drain` | 进程级/实例级优雅排空：拒绝新请求、宽限切断、在途计数与干净退出 |
| `pkg/config` | 严格配置解析、版本兼容、默认值、带 revision 的原子热更新 |
| `pkg/client` | 编排以上组件的治理层入口、决策轨迹日志与一致状态快照 |
| `cmd/demo` | 可运行演示 |
| `test/integration` | 跨组件集成场景（抖动 / 超时 / 副作用 / 配置异常 / 取消 / 排空） |

一次调用的决策顺序：

```
入口取消/预算复检 → 排空准入（拒绝新请求）→ 绑定全局在途
   → 每轮：ctx 复检 → 服务发现 → 过滤排空实例 → 负载均衡选实例
   → 实例排空绑定 → 熔断判定 → 并发/排队额度（可被取消中断）
   → 发起前复检 → 派生更紧预算的下游 ctx → 调用
   → 取消类放弃记账 / 业务结果驱动熔断 →（失败且幂等且有预算）退避后重试
   → 结束释放全部在途/排队/连接占用
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

### 6. 取消与超预算的全链路收敛
取消和预算超时在调用链的**每一个等待点**都收敛，保证"被取消的请求绝不再敲一次下游"：

- **入口即取消**：请求进入 `Invoke` 时 ctx 已取消/已过截止，直接返回
  `GATEWAY_CANCELED`/`GATEWAY_TIMEOUT`，不做发现、不查熔断、不排队、不占连接。
- **排队等待中取消**：`on_limit=wait` 排队期间 ctx 结束，立即退出队列并
  `GATEWAY_CANCELED`/`GATEWAY_TIMEOUT`；若恰好在退出瞬间收到令牌，会把令牌
  继续向下传给队首，**不丢令牌、不超发**。
- **退避等待中取消**：重试退避窗口内 ctx 结束，立即返回，不再发起下一次尝试。
- **每轮尝试入口复检**：进入新一轮尝试前再次检查 ctx 与剩余预算，
  上一轮处理/排队期间发生的取消不会被后续重试或半开探测绕过去。
- **发起前复检**：拿到并发额度后、真正调用下游前再检查一次，
  预算为 0 时不建立任何下游调用。
- **在途取消**：下游调用进行中上游取消或预算到期，治理层不等待慢下游，
  立即返回并 `cancel()` 子 ctx、释放并发/在途/连接占用；尊重 ctx 的假下游
  会随之退出，进程里不会留下仍在推进的调用。
- **熔断记账**：取消/超时/排空都属于"没有拿到下游裁决"，**不计连续失败、
  不触发熔断**；若发生在 half-open，预占的探测槽会被回收（`RecordAbandon`），
  避免一次取消把恢复探测永久吃掉。
- 取消（`GATEWAY_CANCELED`）、预算超时（`GATEWAY_TIMEOUT`）、
  排空切断（`GATEWAY_DRAIN_TIMEOUT`）三者严格区分，且**都不可重试**。

### 7. 优雅排空（摘实例 / 停进程）
排空协调器（`pkg/drain`）同时支持进程级与单实例级排空：

- **进入排空即拒绝新请求**：进程排空后所有新请求立即返回
  `GATEWAY_DRAIN_REJECTED`，不占用任何资源；实例排空后该实例被从
  负载均衡候选中过滤（其余实例继续服务），实例维度的新绑定同样被拒。
- **在途请求在剩余预算内跑完**：排空不硬切正在执行的调用；它们继续受各自的
  上游截止时间与方法 timeout 约束。
- **宽限到期快速失败**：宽限期（`grace`）用尽仍未完成的在途请求，其 ctx 以
  `GATEWAY_DRAIN_TIMEOUT` 为 cause 被统一取消，错误与普通 CANCELED/TIMEOUT
  可区分；`grace <= 0` 表示只等在途自然结束、不主动切断。
- **干净退出保证**：`Shutdown`/`DrainInstance` 阻塞到在途全部退出才返回
  （宽限到期时先切断再等 goroutine 退出），返回后进程内不存在仍在推进的调用，
  在途/排队/连接计数全部归零。
- **非阻塞版本**：`BeginDrain`/`BeginInstanceDrain` 立即返回，可配合
  `Snapshot` 轮询排空进度。
- 三类失败原因互不混淆：排空期新请求 → `GATEWAY_DRAIN_REJECTED`；
  宽限等待超时被切断 → `GATEWAY_DRAIN_TIMEOUT`；普通下游失败 → 原原因码。
- 实例排空**不会自动**把实例从注册中心删除，由调用方决定何时 `Remove`；
  排空期间 client 已保证不再向它派发新流量。

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
| `GATEWAY_NO_HEALTHY_INSTANCE` | 实例全不健康（含全部处于排空） | 否（发现层） |
| `GATEWAY_TIMEOUT` | 超过自身/上游预算 | 否（预算已尽） |
| `GATEWAY_CANCELED` | 上游主动取消 | 否 |
| `GATEWAY_DRAIN_REJECTED` | 排空期间拒绝新请求 | 否 |
| `GATEWAY_DRAIN_TIMEOUT` | 排空宽限用尽，在途被切断 | 否 |
| `GATEWAY_NON_RETRYABLE` | 非幂等请求拒绝重试 | 否 |
| `GATEWAY_RETRY_EXHAUSTED` | 达到重试上限 | 终态 |
| `GATEWAY_CIRCUIT_OPEN` | 熔断打开/半开额度满 | 可换实例再试 |
| `GATEWAY_CONCURRENCY_LIMITED` | 并发超限且策略为拒绝 | 可换实例再试 |
| `GATEWAY_QUEUE_FULL` | 等待队列已满 | 可换实例再试 |
| `GATEWAY_CONFIG_REJECTED` | 配置非法被拒绝 | 不适用 |

所有原因码均可用 `errors.Is(err, model.ErrXxx)` 精确判定（错误被包装后仍可命中）。
排空切断通过 `context.Cause` 传播，即使 ctx 表面是 `DeadlineExceeded`
也能用 `errors.Is(err, model.ErrDrainTimeout)` 还原真实原因。

## 运行时治理状态快照

`client.Client.Snapshot()` 返回某一时刻的 `GovernanceSnapshot`，可用于
运维面板、就绪探针与测试对账。字段含义：

顶层：

| 字段 | 含义 |
|------|------|
| `taken_at` | 快照时间戳（使用注入/系统时钟），所有字段都在该时刻解释 |
| `drain` | 进程级排空进度（结构同实例级，见下） |
| `instances[]` | 每个实例一行：注册中心成员 + 仍在排空但已摘除的实例 |

实例级 `instances[i]`：

| 字段 | 含义 |
|------|------|
| `service` / `instance_id` | 实例坐标 |
| `healthy` | 注册中心健康位 |
| `inflight` | 该实例在途请求数；排空中取排空层计数（执行+排队+退避），平时为各方法计数之和 |
| `drain` | 该实例的排空进度（active/drain/drained） |
| `methods[]` | 该实例上每个被治理过的方法一行 |

方法级 `methods[j]`：

| 字段 | 含义 |
|------|------|
| `method` | 方法名 |
| `active` | 正在向下游发起调用的占用数（恒 ≤ `max_active`） |
| `waiting` | 排队等待额度的请求数（恒 ≤ `max_waiting`） |
| `max_active` / `max_waiting` | 当前生效的并发/排队上限（热更新后即时反映） |
| `circuit.state` | `closed` / `open` / `half-open` |
| `circuit.failures` | closed 期连续失败数 |
| `circuit.cooldown_left` | open 期故障冷却剩余时间；非 open 或已过冷却为 0 |
| `circuit.probe_inflight` | half-open 已发放且在途的探测槽位数 |
| `circuit.probe_succeeded` | half-open 已成功的探测数 |
| `circuit.half_open_max` | half-open 探测槽上限 |

排空进度 `drain`（顶层与实例级同构）：

| 字段 | 含义 |
|------|------|
| `phase` | `active` 正常 / `drain` 排空中 / `drained` 已结束 |
| `draining` | 等价于 `phase != active` |
| `started_at` / `grace` / `deadline` | 排空开始时刻、宽限时长、宽限截止（`grace<=0` 时 deadline 为零值） |
| `elapsed` / `grace_left` | 已排空时长 / 宽限剩余（到期归零） |
| `inflight` | 已准入且未结束的在途数（执行+排队+退避） |
| `rejected` | 进入排空后被拒绝的新请求累计数 |
| `timed_out` | 宽限是否已用尽（在途已被强制切断） |

**一致性与可复现**：排空、限流、熔断各自在自身互斥锁内一次性读出，
因此单个实例/方法的 `active/waiting` 与熔断字段在该锁下自洽，
不会出现"在途数对不上排队+执行"或"探测槽自相矛盾"；跨子系统不做全局
大锁以免阻塞请求路径，而是统一固定在 `taken_at` 上解释。
在静止系统（无请求在途变化）上重复查询得到逐字段相同的结果。

## 与旧行为的兼容范围

- **配置完全兼容**：没有新增任何必填配置字段，`version: 0..2` 的旧配置
  解析与默认值规则不变；排空是运行时 API 行为，不依赖配置开关。
- **原因码只增不改**：新增 `GATEWAY_DRAIN_REJECTED` / `GATEWAY_DRAIN_TIMEOUT`，
  既有的 10 个原因码语义与判定方式不变。
- **成功路径与既有重试/熔断/限流行为不变**：未触发取消与排空时，
  决策顺序与结果与旧版本一致（既有用例全部保持通过）。
- **两处行为收紧（更安全，旧行为本就是缺陷窗口）**：
  1. 请求在**进入 `Invoke` 时** ctx 已取消/已过期，旧版本可能仍走到发现甚至
     排队；现在在任何资源占用前即失败，且不打下游——取消请求本来就不应产生调用。
  2. 熔断 half-open 探测在**拿到下游裁决前**被取消/超时，旧版本会白白消耗一个
     探测槽（极端情况下推迟恢复）；现在探测槽回收、取消不计失败。
- **维度键补充了服务前缀**：熔断/并发内部键由 `instance#method` 调整为
  `service/instance#method`，避免跨服务的同名实例 ID 互相污染；该键仅用于内部
  map，不在配置或公开 API 中出现，对调用方透明。
- **取消类错误在熔断上不再计失败**：上游取消、预算到期不代表下游故障；
  若旧有监控以"取消次数"间接观察熔断，需要改用明确的下游业务失败统计。

## 本地验证指引

```bash
# 1. 单元测试（每个组件的边界行为，含取消收敛、探测槽回收、排空记账）
go test ./pkg/... -v

# 2. 重点场景集成测试
go test ./test/integration/ -v
#   - TestCanceledBeforeDownstreamNeverCalls       进来前已取消：零下游调用、零占用
#   - TestCanceledWhileQueuedNeverCallsAndReleasesSlot 排队中取消：不打下游、令牌正常转移
#   - TestDrainRejectsNewRequests                  排空拒绝新请求，在途跑完干净退出
#   - TestDrainGraceTimeoutCutsInflight            排空宽限超时：DRAIN_TIMEOUT 且计数归零
#   - TestInstanceDrainStopsSelectionAndWaitsInflight 摘实例：新流量转移、在途优雅收尾
#   - TestSnapshotSelfConsistentAndReproducible    快照自洽且重复查询可复现

# 3. 竞态检测（CI 与本地都建议开启 cgo），可重复多轮确认稳定
CGO_ENABLED=1 go test -race -count=3 ./...

# 4. 可观察演示（含场景6：预取消零调用 + 优雅排空）
go run ./cmd/demo

# 5. 静态检查
go vet ./...
```

## 设计取舍

- **无外部中间件**：注册中心、健康检查、熔断状态全部在进程内，便于本地确定性测试；
  生产可把 `registry.Registry` / `Invoker` 替换为接真实注册中心与 RPC 传输的实现。
- **每次尝试重新发现 + 每请求固定配置**：实例视图追求"最新"，策略视图追求"可复现"。
- **维度选择**：熔断与并发按「实例×方法」隔离，单实例单方法故障不波及其他方法。
