// 治理运行时状态的一致快照：把在途/排队、熔断与半开、故障冷却剩余、
// 排空进度汇总到同一时间戳下，供运行时观察与对账。
package client

import (
	"sort"
	"strings"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/circuitbreaker"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/concurrency"
	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/drain"
)

// CircuitView 是一个「实例×方法」熔断器的状态视图。
type CircuitView struct {
	State          circuitbreaker.StateName `json:"state"`
	Failures       int                      `json:"failures"`
	CooldownLeft   time.Duration            `json:"cooldown_left"`
	ProbeInflight  int                      `json:"probe_inflight"`
	ProbeSucceeded int                      `json:"probe_succeeded"`
	HalfOpenMax    int                      `json:"half_open_max"`
}

// MethodView 是某实例上某方法的运行时计数。
type MethodView struct {
	Method     string       `json:"method"`
	Active     int          `json:"active"`
	Waiting    int          `json:"waiting"`
	MaxActive  int          `json:"max_active"`
	MaxWaiting int          `json:"max_waiting"`
	Circuit    *CircuitView `json:"circuit,omitempty"`
}

// InstanceView 是单个实例的治理状态。
type InstanceView struct {
	Service    string          `json:"service"`
	InstanceID string          `json:"instance_id"`
	Healthy    bool            `json:"healthy"`
	Inflight   int             `json:"inflight"` // 排空层在途（执行+排队+退避）
	Drain      drain.ScopeView `json:"drain"`
	Methods    []MethodView    `json:"methods"`
}

// GovernanceSnapshot 是某一时刻全进程治理状态的自洽快照。
type GovernanceSnapshot struct {
	TakenAt   time.Time       `json:"taken_at"`
	Drain     drain.ScopeView `json:"drain"`
	Instances []InstanceView  `json:"instances"`
}

// parseDimKey 解析 cbDimension 生成的 "service/instance#method" 键。
func parseDimKey(key string) (service, instanceID, method string) {
	service, rest, ok := strings.Cut(key, "/")
	if !ok {
		return "", "", key
	}
	instanceID, method, ok = strings.Cut(rest, "#")
	if !ok {
		return service, rest, ""
	}
	return service, instanceID, method
}

// Snapshot 返回当前治理状态的一致快照。
//
// 一致性边界：排空、限流、熔断三个子系统各自在自身互斥锁内完成读取，
// 因此单个实例/方法的计数与其熔断状态在该锁下自洽；跨子系统部分不做
// 全局加锁（避免阻塞请求路径），但所有字段都固定在同一个 TakenAt 时间
// 戳上解释。静止系统（无请求在途变化）上重复查询必然得到逐字段相同的
// 可复现结果。
func (c *Client) Snapshot() GovernanceSnapshot {
	now := c.now()

	// 1) 先取排空视角：决定实例成员集合与在途/排队计数。
	globalDrain := c.drainer.Snapshot()
	draining := map[string]bool{}
	for _, k := range c.drainer.Instances() {
		draining[k] = true
	}

	// 2) 注册中心：实例成员与健康状态（其内部 RLock 下自洽）。
	services := c.reg.Services()
	type instRef struct{ service, id string }
	refs := make([]instRef, 0)
	healthy := map[instRef]bool{}
	for _, svc := range services {
		for _, in := range c.reg.List(svc) {
			r := instRef{svc, in.ID}
			refs = append(refs, r)
			healthy[r] = in.Healthy
		}
	}
	// 已从注册中心摘除但仍在排空的实例也要出现。
	for k := range draining {
		svc, id, ok := strings.Cut(k, "/")
		if !ok {
			continue
		}
		r := instRef{svc, id}
		if _, seen := healthy[r]; !seen {
			refs = append(refs, r)
			healthy[r] = false
		}
	}

	// 3) 限流与熔断：各自一次性快照。
	lim := map[string]concurrency.KeyState{}
	for _, ks := range c.limiters.Snapshot() {
		lim[ks.Key] = ks
	}
	brks := c.breakers.SnapshotDetailed(now)

	// 4) 把「实例×方法」维度汇总到实例下。
	byInst := map[instRef]*InstanceView{}
	order := make([]instRef, 0, len(refs))
	order = append(order, refs...)
	for _, r := range order {
		byInst[r] = &InstanceView{
			Service:    r.service,
			InstanceID: r.id,
			Healthy:    healthy[r],
			Drain:      c.drainer.InstanceSnapshot(r.service, r.id),
		}
	}
	// 限流/熔断中出现但注册列表暂未列到的维度（防御）。
	addRef := func(r instRef) *InstanceView {
		if v, ok := byInst[r]; ok {
			return v
		}
		v := &InstanceView{
			Service:    r.service,
			InstanceID: r.id,
			Healthy:    healthy[r],
			Drain:      c.drainer.InstanceSnapshot(r.service, r.id),
		}
		byInst[r] = v
		order = append(order, r)
		return v
	}

	methodSeen := map[instRef]map[string]bool{}
	attachMethod := func(r instRef, method string, mv MethodView) {
		v := addRef(r)
		if methodSeen[r] == nil {
			methodSeen[r] = map[string]bool{}
		}
		if methodSeen[r][method] {
			// 同一方法的限流/熔断信息合并而非重复。
			for i := range v.Methods {
				if v.Methods[i].Method == method {
					if mv.Circuit != nil {
						v.Methods[i].Circuit = mv.Circuit
					}
					if mv.Active != 0 || mv.Waiting != 0 {
						v.Methods[i].Active = mv.Active
						v.Methods[i].Waiting = mv.Waiting
						v.Methods[i].MaxActive = mv.MaxActive
						v.Methods[i].MaxWaiting = mv.MaxWaiting
					}
					return
				}
			}
		}
		methodSeen[r][method] = true
		v.Methods = append(v.Methods, mv)
	}

	for dim, ks := range lim {
		svc, id, method := parseDimKey(dim)
		r := instRef{svc, id}
		attachMethod(r, method, MethodView{
			Method: method, Active: ks.Active, Waiting: ks.Waiting,
			MaxActive: ks.MaxActive, MaxWaiting: ks.MaxWaiting,
		})
	}
	for dim, bv := range brks {
		svc, id, method := parseDimKey(dim)
		r := instRef{svc, id}
		cv := &CircuitView{
			State: bv.State, Failures: bv.Failures, CooldownLeft: bv.CooldownLeft,
			ProbeInflight: bv.ProbeInflight, ProbeSucceeded: bv.ProbeSucceeded,
			HalfOpenMax: bv.HalfOpenMax,
		}
		attachMethod(r, method, MethodView{Method: method, Circuit: cv})
	}

	out := GovernanceSnapshot{TakenAt: now, Drain: globalDrain}
	for _, r := range order {
		v := byInst[r]
		// 实例在途以排空层计数为准（覆盖执行中、排队中、退避中的请求）；
		// 未排空时该计数为 0，回退为各方法 active+waiting 之和，便于平时对账。
		v.Inflight = v.Drain.Inflight
		if !v.Drain.Draining {
			n := 0
			for _, m := range v.Methods {
				n += m.Active + m.Waiting
			}
			v.Inflight = n
		}
		sort.Slice(v.Methods, func(i, j int) bool { return v.Methods[i].Method < v.Methods[j].Method })
		out.Instances = append(out.Instances, *v)
	}
	sort.Slice(out.Instances, func(i, j int) bool {
		if out.Instances[i].Service != out.Instances[j].Service {
			return out.Instances[i].Service < out.Instances[j].Service
		}
		return out.Instances[i].InstanceID < out.Instances[j].InstanceID
	})
	return out
}
