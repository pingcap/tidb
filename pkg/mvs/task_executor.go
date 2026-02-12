package mvs

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type TaskExecutor struct {
	ctx context.Context

	maxConcurrency atomic.Int64
	timeoutNanos   atomic.Int64
	lifecycleState atomic.Int32
	backpressure   atomic.Pointer[taskBackpressureHolder]

	metrics struct {
		counters struct {
			submittedCount atomic.Int64
			completedCount atomic.Int64
			failedCount    atomic.Int64
			timeoutCount   atomic.Int64
			rejectedCount  atomic.Int64
		}
		gauges struct {
			runningCount         atomic.Int64
			waitingCount         atomic.Int64
			timedOutRunningCount atomic.Int64
		}
	}

	queue struct {
		mu    sync.Mutex
		cond  *sync.Cond
		tasks taskQueue
	}

	workers struct {
		count atomic.Int64
		wg    sync.WaitGroup
	}

	tasksWG sync.WaitGroup
}

const (
	taskExecutorStateInit int32 = iota
	taskExecutorStateRunning
	taskExecutorStateClosed
)

type taskRequest struct {
	name string
	task func() error
}

type taskBackpressureHolder struct {
	controller TaskBackpressureController
}

type taskQueue struct {
	buf  []taskRequest
	head int
	size int
}

func (q *taskQueue) length() int {
	return q.size
}

func (q *taskQueue) push(req taskRequest) {
	if q.size == len(q.buf) {
		q.grow()
	}
	tail := (q.head + q.size) % len(q.buf)
	q.buf[tail] = req
	q.size++
}

func (q *taskQueue) pop() (taskRequest, bool) {
	if q.size == 0 {
		return taskRequest{}, false
	}
	req := q.buf[q.head]
	q.buf[q.head] = taskRequest{} // Clear references for GC.
	q.head = (q.head + 1) % len(q.buf)
	q.size--
	if q.size == 0 {
		q.head = 0
	}
	return req, true
}

func (q *taskQueue) clear() int {
	pending := q.size
	q.buf = nil
	q.head = 0
	q.size = 0
	return pending
}

func (q *taskQueue) grow() {
	newCap := len(q.buf) * 2
	if newCap == 0 {
		newCap = 4
	}
	newBuf := make([]taskRequest, newCap)
	if q.size > 0 {
		if q.head+q.size <= len(q.buf) {
			copy(newBuf, q.buf[q.head:q.head+q.size])
		} else {
			n := copy(newBuf, q.buf[q.head:])
			copy(newBuf[n:], q.buf[:q.size-n])
		}
	}
	q.buf = newBuf
	q.head = 0
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
// It returns true only when workers are started in this call.
func (e *TaskExecutor) Run() bool {
	if e == nil {
		return false
	}
	if !e.lifecycleState.CompareAndSwap(taskExecutorStateInit, taskExecutorStateRunning) {
		return false
	}
	workers := int(e.maxConcurrency.Load())
	if workers <= 0 {
		workers = 1
	}
	e.startWorkers(workers)
	return true
}

// UpdateConfig updates maxConcurrency and timeout dynamically.
// maxConcurrency must be positive; timeout may be 0 (no timeout).
func (e *TaskExecutor) UpdateConfig(maxConcurrency int, timeout time.Duration) {
	if e == nil {
		return
	}
	if e.lifecycleState.Load() == taskExecutorStateClosed {
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

// GetConfig returns the current execution config.
func (e *TaskExecutor) GetConfig() (maxConcurrency int, timeout time.Duration) {
	if e == nil {
		return 0, 0
	}
	maxConcurrency = int(e.maxConcurrency.Load())
	timeout = time.Duration(e.timeoutNanos.Load())
	return maxConcurrency, timeout
}

// SetBackpressureController sets a task backpressure controller.
// Passing nil disables backpressure.
func (e *TaskExecutor) SetBackpressureController(controller TaskBackpressureController) {
	if e == nil {
		return
	}
	if controller == nil {
		e.backpressure.Store(nil)
		return
	}
	e.backpressure.Store(&taskBackpressureHolder{controller: controller})
}

func (e *TaskExecutor) Submit(name string, task func() error) {
	if e == nil || task == nil {
		return
	}
	e.queue.mu.Lock()
	defer e.queue.mu.Unlock()

	if e.lifecycleState.Load() == taskExecutorStateClosed || e.ctx.Err() != nil {
		e.metrics.counters.rejectedCount.Add(1)
		return
	}
	e.metrics.counters.submittedCount.Add(1)
	e.metrics.gauges.waitingCount.Add(1)
	e.tasksWG.Add(1)
	e.queue.tasks.push(taskRequest{name: name, task: task})
	e.queue.cond.Signal()
}

// Close closes the executor.
// It returns true when this call performs close and all workers/tasks finish within timeout.
func (e *TaskExecutor) Close() bool {
	if e == nil {
		return false
	}
	if e.lifecycleState.Swap(taskExecutorStateClosed) == taskExecutorStateClosed {
		return false
	}
	e.queue.mu.Lock()
	pending := e.queue.tasks.clear()
	e.queue.cond.Broadcast()
	e.queue.mu.Unlock()
	if pending > 0 {
		e.metrics.gauges.waitingCount.Add(-int64(pending))
		for range pending {
			e.tasksWG.Done()
		}
	}
	e.tasksWG.Wait()
	e.workers.wg.Wait()
	return true
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
	for {
		e.queue.mu.Lock()
		for e.lifecycleState.Load() != taskExecutorStateClosed && e.queue.tasks.length() == 0 {
			if e.tryExitWorkerWithLock() {
				e.queue.mu.Unlock()
				return taskRequest{}, false
			}
			e.queue.cond.Wait()
		}
		if e.shouldExitWorkerWithLock() {
			e.queue.mu.Unlock()
			return taskRequest{}, false
		}

		e.queue.mu.Unlock()

		if blocked, delay := e.shouldBackpressure(); blocked {
			mvsSleep(delay)
			continue
		}

		e.queue.mu.Lock()
		if e.shouldExitWorkerWithLock() {
			e.queue.mu.Unlock()
			return taskRequest{}, false
		}
		req, ok := e.queue.tasks.pop()
		e.queue.mu.Unlock()

		if ok {
			e.metrics.gauges.waitingCount.Add(-1)
			return req, true
		}
	}
}

func (e *TaskExecutor) shouldExitWorkerWithLock() bool {
	if e.lifecycleState.Load() == taskExecutorStateClosed && e.queue.tasks.length() == 0 {
		e.workers.count.Add(-1)
		return true
	}
	return e.tryExitWorkerWithLock()
}

func (e *TaskExecutor) shouldBackpressure() (bool, time.Duration) {
	holder := e.backpressure.Load()
	if holder == nil || holder.controller == nil {
		return false, 0
	}
	blocked, delay := holder.controller.ShouldBackpressure()
	if !blocked {
		return false, 0
	}
	if delay <= 0 {
		delay = defaultTaskBackpressureDelay
	}
	return true, delay
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

func (e *TaskExecutor) runTask(name string, task func() error) {
	e.metrics.gauges.runningCount.Add(1)

	timeout := time.Duration(e.timeoutNanos.Load())
	if timeout <= 0 {
		err := e.safeExecute(name, task)
		e.metrics.gauges.runningCount.Add(-1)
		e.tasksWG.Done()
		e.logResult(name, err)
		return
	}

	done := make(chan error, 1)
	go func() {
		done <- e.safeExecute(name, task)
	}()

	timer := mvsNewTimer(timeout)
	defer timer.Stop()

	select {
	case err := <-done:
		e.metrics.gauges.runningCount.Add(-1)
		e.tasksWG.Done()
		e.logResult(name, err)
	case <-timer.C:
		e.metrics.counters.timeoutCount.Add(1)
		e.metrics.gauges.runningCount.Add(-1)
		e.metrics.gauges.timedOutRunningCount.Add(1)
		logutil.BgLogger().Warn("mv task timed out, continue in background", zap.String("task", name), zap.Duration("timeout", timeout))
		go func() {
			err := <-done
			e.metrics.gauges.timedOutRunningCount.Add(-1)
			e.tasksWG.Done()
			e.logResult(name, err)
		}()
	}
}

func (e *TaskExecutor) logResult(name string, err error) {
	if err == nil {
		e.metrics.counters.completedCount.Add(1)
		return
	}
	e.metrics.counters.completedCount.Add(1)
	e.metrics.counters.failedCount.Add(1)
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
