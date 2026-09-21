package bulkhead

import "sync"
import "sync/atomic"

type syncWg struct{ wg sync.WaitGroup }

func (w *syncWg) add()  { w.wg.Add(1) }
func (w *syncWg) done() { w.wg.Done() }
func (w *syncWg) wait() { w.wg.Wait() }

func addInt64(p *int64, d int64) int64 { return atomic.AddInt64(p, d) }

func recordPeak(peak *int64, v int64) {
	for {
		old := atomic.LoadInt64(peak)
		if v <= old || atomic.CompareAndSwapInt64(peak, old, v) {
			return
		}
	}
}
