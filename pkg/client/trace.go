package client

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
)

// TraceLine 是一次尝试的结构化决策记录。
type TraceLine struct {
	At         time.Time
	RequestID  string
	Revision   uint64
	Attempt    int
	InstanceID string
	Policy     string // 命中的策略（lb / circuit / retry / limit）
	Outcome    string // allow / reject / success / failure
	Reason     string // 稳定原因码
	Response   string // 下游响应摘要
}

func (t TraceLine) String() string {
	return fmt.Sprintf("%s req=%s rev=%d attempt=%d instance=%s policy=%s outcome=%s reason=%s resp=%s",
		t.At.Format(time.RFC3339Nano), t.RequestID, t.Revision, t.Attempt, t.InstanceID,
		t.Policy, t.Outcome, t.Reason, t.Response)
}

// Tracer 有界保存最近轨迹，并同步打印到日志输出。
type Tracer struct {
	mu      sync.Mutex
	lines   []TraceLine
	out     io.Writer
	keep    int
	verbose bool
}

// NewTracer 创建轨迹记录器；keep 为每请求保留的最近行数上限，<=0 用默认值。
func NewTracer(out io.Writer, keep int, verbose bool) *Tracer {
	if keep <= 0 {
		keep = 64
	}
	return &Tracer{out: out, keep: keep, verbose: verbose}
}

// Emit 记录并（verbose 时）打印一行轨迹。
func (t *Tracer) Emit(l TraceLine) {
	t.mu.Lock()
	t.lines = append(t.lines, l)
	if len(t.lines) > t.keep {
		t.lines = t.lines[len(t.lines)-t.keep:]
	}
	out := t.out
	verbose := t.verbose
	t.mu.Unlock()
	if verbose && out != nil {
		fmt.Fprintln(out, l.String())
	}
}

// Of 返回指定请求的全部轨迹。
func (t *Tracer) Of(reqID string) []model.Attempt {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []model.Attempt
	for _, l := range t.lines {
		if l.RequestID == reqID {
			out = append(out, model.Attempt{
				AttemptNo:  l.Attempt,
				InstanceID: l.InstanceID,
				Policy:     l.Policy,
				Outcome:    l.Outcome,
				Reason:     l.Reason,
			})
		}
	}
	return out
}
