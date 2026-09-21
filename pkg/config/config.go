// Package config 负责治理策略的解析、版本兼容与严格校验：
//   - 旧配置缺失字段落到 DefaultMethodPolicy 的稳定默认值；
//   - 未知字段、未知枚举、相互冲突字段、不支持的版本一律 ErrConfigRejected，
//     绝不静默忽略；
//   - 热更新以整体替换生效，并给出单调 revision 用于区分新旧请求。
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
)

// CurrentVersion 是当前支持的最高配置版本；接受 (0,1,2] 的历史版本。
const CurrentVersion = 2

// Raw 是外部 JSON 的原始形态。
type Raw struct {
	Version int `json:"version"`
	Service struct {
		Name    string             `json:"name"`
		Methods map[string]RawRule `json:"methods"`
	} `json:"service"`
}

// RawRule 为单方法原始规则；指针字段用于区分"未提供"与"显式零值"。
type RawRule struct {
	Timeout          *string             `json:"timeout"`
	MaxAttempts      *int                `json:"max_attempts"`
	LB               *model.LBKind       `json:"lb"`
	CircuitThreshold *int                `json:"circuit_threshold"`
	OpenCooldown     *string             `json:"open_cooldown"`
	HalfOpenProbes   *int                `json:"half_open_probes"`
	OnLimit          *model.RejectPolicy `json:"on_limit"`
	MaxConcurrency   *int                `json:"max_concurrency"`
	MaxQueue         *int                `json:"max_queue"`
}

// Parsed 是解析后可直接使用的策略集合。
type Parsed struct {
	Version int
	Service string
	Methods map[string]model.MethodPolicy
}

// Parse 严格解析 JSON 配置。
func Parse(data []byte) (*Parsed, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, reject("empty configuration")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var raw Raw
	if err := dec.Decode(&raw); err != nil {
		return nil, reject("invalid json or unknown field: " + err.Error())
	}
	if dec.More() {
		return nil, reject("configuration contains multiple JSON documents")
	}
	if raw.Version < 0 || raw.Version > CurrentVersion {
		return nil, reject(fmt.Sprintf("unsupported config version %d (supported: 0..%d)", raw.Version, CurrentVersion))
	}
	if raw.Service.Name == "" {
		return nil, reject("service.name is required")
	}
	if len(raw.Service.Methods) == 0 {
		return nil, reject("service.methods must contain at least one method")
	}

	out := &Parsed{
		Version: raw.Version,
		Service: raw.Service.Name,
		Methods: map[string]model.MethodPolicy{},
	}
	for name, rule := range raw.Service.Methods {
		p, err := parseRule(raw.Version, name, rule)
		if err != nil {
			return nil, err
		}
		out.Methods[name] = p
	}
	return out, nil
}

func parseRule(version int, name string, r RawRule) (model.MethodPolicy, error) {
	p := model.DefaultMethodPolicy(name)
	p.Version = version
	var err error

	if r.Timeout != nil {
		if p.Timeout, err = parseDuration("timeout", *r.Timeout); err != nil {
			return p, err
		}
	}
	if r.MaxAttempts != nil {
		if *r.MaxAttempts < 1 {
			return p, reject(name + ": max_attempts must be >= 1")
		}
		p.MaxAttempts = *r.MaxAttempts
	}
	if r.LB != nil {
		switch *r.LB {
		case model.LBWeightedRoundRobin, model.LBRandom:
			p.LB = *r.LB
		default:
			return p, reject(name + ": unknown lb kind: " + string(*r.LB))
		}
	}
	if r.CircuitThreshold != nil {
		if *r.CircuitThreshold < 1 {
			return p, reject(name + ": circuit_threshold must be >= 1")
		}
		p.CircuitThreshold = *r.CircuitThreshold
	}
	if r.OpenCooldown != nil {
		if p.OpenCooldown, err = parseDuration("open_cooldown", *r.OpenCooldown); err != nil {
			return p, err
		}
	}
	if r.HalfOpenProbes != nil {
		if *r.HalfOpenProbes < 1 {
			return p, reject(name + ": half_open_probes must be >= 1")
		}
		p.HalfOpenProbes = *r.HalfOpenProbes
	}
	if r.MaxConcurrency != nil {
		if *r.MaxConcurrency < 1 {
			return p, reject(name + ": max_concurrency must be >= 1")
		}
		p.MaxConcurrency = *r.MaxConcurrency
	}
	if r.MaxQueue != nil {
		if *r.MaxQueue < 0 {
			return p, reject(name + ": max_queue must be >= 0")
		}
		p.MaxQueue = *r.MaxQueue
	}
	if r.OnLimit != nil {
		switch *r.OnLimit {
		case model.RejectReject, model.RejectWait:
			p.OnLimit = *r.OnLimit
		default:
			return p, reject(name + ": unknown on_limit policy: " + string(*r.OnLimit))
		}
	}

	// 冲突显式拒绝：要求排队却给了零队列长度；重试却没有重试次数预算（attempts==1 非冲突，只是无重试）。
	if p.OnLimit == model.RejectWait && p.MaxQueue == 0 {
		return p, reject(name + ": on_limit=wait requires max_queue > 0")
	}
	if p.OnLimit == model.RejectReject && p.MaxQueue > 0 {
		return p, reject(name + ": on_limit=reject conflicts with max_queue > 0")
	}
	// v1 及更早版本没有熔断半开探测数概念：保持默认 1，禁止旧版本显式设置该字段
	// （旧版本 schema 本就无法携带该字段，这里仅做防御）。
	if version < 2 && r.HalfOpenProbes != nil {
		return p, reject(fmt.Sprintf("%s: half_open_probes requires config version >= 2", name))
	}
	return p, nil
}

func parseDuration(field, s string) (time.Duration, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, reject(field + ": invalid duration: " + s)
	}
	if d <= 0 {
		return 0, reject(field + ": duration must be > 0: " + s)
	}
	return d, nil
}

func reject(msg string) error {
	return fmt.Errorf("%w: %s", model.ErrConfigRejected, msg)
}
