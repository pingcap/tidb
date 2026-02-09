package mvs

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type TaskExecutor struct {
	ctx context.Context

	maxConcurrency atomic.Int64
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

	queue struct {
		mu    sync.Mutex
		cond  *sync.Cond
		tasks []taskRequest
	}

	workers struct {
		count atomic.Int64
		wg    sync.WaitGroup
	}

	tasksWG sync.WaitGroup
	closed  atomic.Bool
}

type taskRequest struct {
	name string
	task func() error
}

func NewTaskExecutor(ctx context.Context, maxConcurrency int, timeout time.Duration) *TaskExecutor {
	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}
	exec := &TaskExecutor{
		ctx: ctx,
	}
	exec.queue.cond = sync.NewCond(&exec.queue.mu)
	exec.maxConcurrency.Store(int64(maxConcurrency))
	exec.timeoutNanos.Store(int64(timeout))
	return exec
}

// Run starts worker goroutines according to current maxConcurrency.
func (e *TaskExecutor) Run() {
	if e == nil || e.closed.Load() {
		return
	}
	workers := int(e.maxConcurrency.Load())
	if workers <= 0 {
		workers = 1
	}
	e.startWorkers(workers)
}

// UpdateConfig updates maxConcurrency and timeout dynamically.
// maxConcurrency must be positive; timeout may be 0 (no timeout).
func (e *TaskExecutor) UpdateConfig(maxConcurrency int, timeout time.Duration) {
	if e == nil {
		return
	}
	if e.closed.Load() {
		return
	}
	if maxConcurrency > 0 {
		prev := int(e.maxConcurrency.Load())
		if maxConcurrency != prev {
			e.maxConcurrency.Store(int64(maxConcurrency))
			if maxConcurrency > prev {
				e.startWorkers(maxConcurrency - prev)
			} else {
				e.queue.mu.Lock()
				e.queue.cond.Broadcast()
				e.queue.mu.Unlock()
			}
		}
	}
	if timeout >= 0 {
		e.timeoutNanos.Store(int64(timeout))
	}
}

func (e *TaskExecutor) setMaxConcurrency(maxConcurrency int) {
	e.UpdateConfig(maxConcurrency, -1)
}

func (e *TaskExecutor) setTimeout(timeout time.Duration) {
	e.UpdateConfig(0, timeout)
}

func (e *TaskExecutor) Submit(name string, task func() error) {
	if e == nil || task == nil {
		return
	}
	if e.closed.Load() || e.ctx.Err() != nil {
		e.metrics.rejectedCount.Add(1)
		return
	}
	e.metrics.submittedCount.Add(1)
	e.metrics.waitingCount.Add(1)
	e.tasksWG.Add(1)

	e.queue.mu.Lock()
	e.queue.tasks = append(e.queue.tasks, taskRequest{name: name, task: task})
	e.queue.cond.Signal()
	e.queue.mu.Unlock()
}

func (e *TaskExecutor) Close() {
	if e == nil {
		return
	}
	if e.closed.Swap(true) {
		return
	}
	e.queue.mu.Lock()
	pending := len(e.queue.tasks)
	e.queue.tasks = nil
	e.queue.cond.Broadcast()
	e.queue.mu.Unlock()
	if pending > 0 {
		e.metrics.waitingCount.Add(-int64(pending))
		for range pending {
			e.tasksWG.Done()
		}
	}
	e.tasksWG.Wait()
	e.workers.wg.Wait()
}

func (e *TaskExecutor) startWorkers(n int) {
	for range n {
		e.workers.count.Add(1)
		e.workers.wg.Add(1)
		go e.workerLoop()
	}
}

func (e *TaskExecutor) workerLoop() {
	defer e.workers.wg.Done()
	for {
		req, ok := e.nextTask()
		if !ok {
			return
		}
		e.runTask(req.name, req.task)
	}
}

func (e *TaskExecutor) nextTask() (taskRequest, bool) {
	e.queue.mu.Lock()
	for !e.closed.Load() && len(e.queue.tasks) == 0 {
		if e.tryExitWorkerWithLock() {
			e.queue.mu.Unlock()
			return taskRequest{}, false
		}
		e.queue.cond.Wait()
	}
	if e.closed.Load() && len(e.queue.tasks) == 0 {
		e.queue.mu.Unlock()
		e.workers.count.Add(-1)
		return taskRequest{}, false
	}
	if e.tryExitWorkerWithLock() {
		e.queue.mu.Unlock()
		return taskRequest{}, false
	}
	req := e.queue.tasks[0]
	e.queue.tasks = e.queue.tasks[1:]
	queued := len(e.queue.tasks)
	e.queue.mu.Unlock()

	e.metrics.waitingCount.Add(-1)

	if intest.InTest && mockInjection != nil && queued > 0 {
		inj := mockInjection
		mockInjection = nil
		inj()
	}

	return req, true
}

func (e *TaskExecutor) tryExitWorkerWithLock() bool {
	for {
		cur := e.workers.count.Load()
		max := e.maxConcurrency.Load()
		if cur <= max {
			return false
		}
		if e.workers.count.CompareAndSwap(cur, cur-1) {
			return true
		}
	}
}

var mockInjectionTimer func(<-chan time.Time) <-chan time.Time

func (e *TaskExecutor) runTask(name string, task func() error) {
	e.metrics.runningCount.Add(1)

	timeout := time.Duration(e.timeoutNanos.Load())
	if timeout <= 0 {
		err := e.safeExecute(name, task)
		e.metrics.runningCount.Add(-1)
		e.tasksWG.Done()
		e.logResult(name, err)
		return
	}

	done := make(chan error, 1)
	go func() {
		done <- e.safeExecute(name, task)
	}()

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
		e.metrics.runningCount.Add(-1)
		e.tasksWG.Done()
		e.logResult(name, err)
	case <-ch:
		e.metrics.timeoutCount.Add(1)
		e.metrics.runningCount.Add(-1)
		logutil.BgLogger().Warn("mv task timed out, continue in background", zap.String("task", name), zap.Duration("timeout", timeout))
		go func() {
			err := <-done
			e.tasksWG.Done()
			e.logResult(name, err)
		}()
	}
}

var mockInjection func()

func (e *TaskExecutor) logResult(name string, err error) {
	if err == nil {
		e.metrics.completedCount.Add(1)
		return
	}
	e.metrics.completedCount.Add(1)
	e.metrics.failedCount.Add(1)
	logutil.BgLogger().Warn("mv task failed", zap.String("task", name), zap.Error(err))
}

func (e *TaskExecutor) safeExecute(_ string, task func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("mv task panicked: %v", r)
		}
	}()
	return task()
}
