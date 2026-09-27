package model

import "time"

// DrainPhase 排空阶段。
type DrainPhase string

const (
	// DrainIdle 正常服务，接受新请求。
	DrainIdle DrainPhase = "idle"
	// DrainDraining 排空中：拒绝新请求，等待在途请求完成。
	DrainDraining DrainPhase = "draining"
	// DrainComplete 排空结束：所有在途已完成，或宽限到期后已强制收尾。
	DrainComplete DrainPhase = "complete"
)

// DrainSnapshot 是进程级优雅排空状态。
type DrainSnapshot struct {
	Phase     DrainPhase    // idle / draining / complete
	StartedAt time.Time     // 排空开始时间（idle 时为零值）
	Deadline  time.Time     // 排空宽限截止时间（idle 时为零值）
	InFlight  int           // 排空开始后仍在途的请求数
	GraceLeft time.Duration // 距离宽限到期还剩多久（非排空中为 0）
	TimedOut  bool          // 排空是否因宽限到期而强制收尾
}

// LimiterStat 是单个「实例×方法」维度的并发额度统计。
type LimiterStat struct {
	Key            string // 维度键：instanceID#method
	InstanceID     string
	Method         string
	Active         int // 正在执行的在途数（恒 <= MaxConcurrency）
	Queued         int // 排队等待数
	MaxConcurrency int
	MaxQueue       int
}

// BreakerStat 是单个「实例×方法」维度的熔断统计。
type BreakerStat struct {
	Key            string
	InstanceID     string
	Method         string
	State          string        // closed / open / half-open
	Failures       int           // closed 期连续失败数
	ProbeInflight  int           // half-open 期已发放、尚未记账的探测数
	ProbeSucceeded int           // half-open 期已成功探测数
	CooldownLeft   time.Duration // open 期距离进入 half-open 的剩余时间（0 表示已可转换/非 open）
}

// InstanceStat 是注册中心视角的单个实例状态。
type InstanceStat struct {
	Service  string
	ID       string
	Address  string
	Weight   int
	Healthy  bool
	Draining bool // 实例被摘除排空中：不再被新请求选中，在途请求继续跑完
}

// GovernanceSnapshot 是某一时刻治理层各组件状态的一致快照。
// 所有切片均按键（service/instance/method）排序，同输入重复查询可复现。
// 自洽性保证：TotalInFlight == sum(Limiter.Active) + 未进入额度阶段的在途数，
// 且 TotalInFlight == sum(Limiter.Active+Queued) + 其他阶段在途数（后者非负）。
type GovernanceSnapshot struct {
	TakenAt       time.Time
	Revision      uint64 // 快照时生效的配置 revision
	Drain         DrainSnapshot
	TotalInFlight int // 治理层此刻受理中的请求总数（含排队、重试等待、下游在途）
	Limiters      []LimiterStat
	Breakers      []BreakerStat
	Instances     []InstanceStat
}
