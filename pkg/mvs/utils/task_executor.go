package utils

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type TaskExecutor struct {
	maxConcurrency int
	timeoutNanos   atomic.Int64

	metrics struct {
		submittedCount atomic.Int64
		runningCount   atomic.Int64
		waitingCount   atomic.Int64
		completedCount atomic.Int64
		failedCount    atomic.Int64
		timeoutCount   atomic.Int64
		rejectedCount  atomic.Int64
	}

	gate struct {
		mu      sync.Mutex
		cond    *sync.Cond
		running int
	}

	wg     sync.WaitGroup
	closed atomic.Bool
}

func NewTaskExecutor(maxConcurrency int, timeout time.Duration) *TaskExecutor {
	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}
	exec := &TaskExecutor{
		maxConcurrency: maxConcurrency,
	}
	exec.gate.cond = sync.NewCond(&exec.gate.mu)
	exec.timeoutNanos.Store(int64(timeout))
	return exec
}

// UpdateConfig updates maxConcurrency and timeout dynamically.
// maxConcurrency must be positive; timeout may be 0 (no timeout).
func (e *TaskExecutor) UpdateConfig(maxConcurrency int, timeout time.Duration) {
	if e == nil {
		return
	}
	if maxConcurrency > 0 {
		e.gate.mu.Lock()
		e.maxConcurrency = maxConcurrency
		e.gate.cond.Broadcast()
		e.gate.mu.Unlock()
	}
	if timeout >= 0 {
		e.timeoutNanos.Store(int64(timeout))
	}
}

func (e *TaskExecutor) SetMaxConcurrency(maxConcurrency int) {
	e.UpdateConfig(maxConcurrency, -1)
}

func (e *TaskExecutor) SetTimeout(timeout time.Duration) {
	e.UpdateConfig(0, timeout)
}

func (e *TaskExecutor) Submit(name string, task func() error) {
	if e == nil || task == nil {
		return
	}
	if e.closed.Load() {
		e.metrics.rejectedCount.Add(1)
		return
	}
	e.metrics.submittedCount.Add(1)
	e.metrics.waitingCount.Add(1)
	e.wg.Add(1)
	go e.run(name, task)
}

func (e *TaskExecutor) Close() {
	if e == nil {
		return
	}
	if e.closed.Swap(true) {
		return
	}
	e.gate.mu.Lock()
	e.gate.cond.Broadcast()
	e.gate.mu.Unlock()
	e.wg.Wait()
}

var mockInjectionTimer func(<-chan time.Time) <-chan time.Time

func (e *TaskExecutor) run(name string, task func() error) {
	if !e.acquire() {
		e.metrics.waitingCount.Add(-1)
		e.metrics.rejectedCount.Add(1)
		e.wg.Done()
		return
	}
	e.metrics.waitingCount.Add(-1)
	e.metrics.runningCount.Add(1)

	done := make(chan error, 1)
	go func() {
		done <- task()
	}()

	timeout := time.Duration(e.timeoutNanos.Load())
	if timeout <= 0 {
		err := <-done
		e.release()
		e.wg.Done()
		e.logResult(name, err)
		return
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ch := timer.C
	if intest.InTest {
		if mockInjectionTimer != nil {
			ch = mockInjectionTimer(ch)
		}
	}

	select {
	case err := <-done:
		e.release()
		e.wg.Done()
		e.logResult(name, err)
	case <-ch:
		e.metrics.timeoutCount.Add(1)
		e.release()
		logutil.BgLogger().Warn("mv task timed out, continue in background", zap.String("task", name), zap.Duration("timeout", timeout))
		go func() {
			err := <-done
			e.wg.Done()
			e.logResult(name, err)
		}()
	}
}

var mockInjection func()

func (e *TaskExecutor) acquire() bool {
	e.gate.mu.Lock()
	defer e.gate.mu.Unlock()
	for e.gate.running >= e.maxConcurrency {
		if e.closed.Load() {
			return false
		}
		if intest.InTest {
			if mockInjection != nil {
				mockInjection()
			}
		}
		e.gate.cond.Wait()
	}
	if e.closed.Load() {
		return false
	}
	e.gate.running++
	return true
}

func (e *TaskExecutor) release() {
	e.gate.mu.Lock()
	if e.gate.running > 0 {
		e.gate.running--
	}
	e.gate.mu.Unlock()
	e.gate.cond.Signal()
	e.metrics.runningCount.Add(-1)
}

func (e *TaskExecutor) logResult(name string, err error) {
	if err == nil {
		e.metrics.completedCount.Add(1)
		return
	}
	e.metrics.completedCount.Add(1)
	e.metrics.failedCount.Add(1)
	logutil.BgLogger().Warn("mv task failed", zap.String("task", name), zap.Error(err))
}
