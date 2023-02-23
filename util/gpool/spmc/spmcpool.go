// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spmc

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/resourcemanager"
	"github.com/pingcap/tidb/resourcemanager/pooltask"
	"github.com/pingcap/tidb/resourcemanager/util"
	"github.com/pingcap/tidb/util/gpool"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

// Pool is a single producer, multiple consumer goroutine pool.
// T is the type of the task. We can treat it as input.
// U is the type of the result. We can treat it as output.
// C is the type of the const parameter. if Our task look like y = ax + b, C acts like b as const parameter.
// CT is the type of the context. It needs to be read/written parallel.
// TF is the type of the context getter. It is used to get a context.
// if we don't need to use CT/TF, we can define CT as any and TF as NilContext.
type Pool[T any, U any, C any, CT any, TF pooltask.Context[CT]] struct {
	workerCache  sync.Pool
	lock         sync.Locker
	stopCh       chan struct{}
	consumerFunc func(T, C, CT) U
	cond         *sync.Cond
	taskCh       chan *pooltask.TaskBox[T, U, C, CT, TF]
	workers      *loopQueue[T, U, C, CT, TF]
	options      *Options
	gpool.BasePool
	taskManager        pooltask.TaskManager[T, U, C, CT, TF]
	waitingTask        atomicutil.Uint32
	capacity           atomic.Int32
	running            atomic.Int32
	state              atomic.Int32
	waiting            atomic.Int32 // waiting is the number of goroutines that are waiting for the pool to be available.
	heartbeatDone      atomic.Bool
	concurrencyMetrics prometheus.Gauge
}

// NewSPMCPool create a single producer, multiple consumer goroutine pool.
func NewSPMCPool[T any, U any, C any, CT any, TF pooltask.Context[CT]](name string, size int32, component util.Component, options ...Option) (*Pool[T, U, C, CT, TF], error) {
	opts := loadOptions(options...)
	if expiry := opts.ExpiryDuration; expiry <= 0 {
		opts.ExpiryDuration = gpool.DefaultCleanIntervalTime
	}
	result := &Pool[T, U, C, CT, TF]{
		BasePool:           gpool.NewBasePool(),
		taskCh:             make(chan *pooltask.TaskBox[T, U, C, CT, TF], 128),
		stopCh:             make(chan struct{}),
		lock:               gpool.NewSpinLock(),
		taskManager:        pooltask.NewTaskManager[T, U, C, CT, TF](size),
		concurrencyMetrics: metrics.PoolConcurrencyCounter.WithLabelValues(name),
		options:            opts,
	}
	result.SetName(name)
	result.state.Store(int32(gpool.OPENED))
	result.workerCache.New = func() interface{} {
		return &goWorker[T, U, C, CT, TF]{
			pool: result,
		}
	}
	result.capacity.Add(size)
	result.concurrencyMetrics.Set(float64(size))
	result.workers = newWorkerLoopQueue[T, U, C, CT, TF](int(size))
	result.cond = sync.NewCond(result.lock)
	err := resourcemanager.InstanceResourceManager.Register(result, name, component)
	if err != nil {
		return nil, err
	}
	// Start a goroutine to clean up expired workers periodically.
	go result.purgePeriodically()
	return result, nil
}

// purgePeriodically clears expired workers periodically which runs in an individual goroutine, as a scavenger.
func (p *Pool[T, U, C, CT, TF]) purgePeriodically() {
	heartbeat := time.NewTicker(p.options.ExpiryDuration)
	defer func() {
		heartbeat.Stop()
		p.heartbeatDone.Store(true)
	}()
	for {
		select {
		case <-heartbeat.C:
		case <-p.stopCh:
			return
		}

		if p.IsClosed() {
			break
		}

		p.lock.Lock()
		expiredWorkers := p.workers.retrieveExpiry(p.options.ExpiryDuration)
		p.lock.Unlock()

		// Notify obsolete workers to stop.
		// This notification must be outside the p.lock, since w.task
		// may be blocking and may consume a lot of time if many workers
		// are located on non-local CPUs.
		for i := range expiredWorkers {
			expiredWorkers[i].taskBoxCh <- nil
			expiredWorkers[i] = nil
		}

		// There might be a situation where all workers have been cleaned up(no worker is running),
		// or another case where the pool capacity has been Tuned up,
		// while some invokers still get stuck in "p.cond.Wait()",
		// then it ought to wake all those invokers.
		if p.Running() == 0 || (p.Waiting() > 0 && p.Free() > 0) || p.waitingTask.Load() > 0 {
			p.cond.Broadcast()
		}
	}
}

// Tune changes the capacity of this pool, note that it is noneffective to the infinite or pre-allocation pool.
func (p *Pool[T, U, C, CT, TF]) Tune(size int) {
	capacity := p.Cap()
	if capacity == -1 || size <= 0 || size == capacity {
		return
	}
	if p.taskManager.GetOriginConcurrency()+int32(util.MaxOverclockCount) < int32(size) {
		return
	}
	p.SetLastTuneTs(time.Now())
	p.capacity.Store(int32(size))
	p.concurrencyMetrics.Set(float64(size))
	if size > capacity {
		for i := 0; i < size-capacity; i++ {
			if tid, boostTask := p.taskManager.Overclock(size); boostTask != nil {
				p.addWaitingTask()
				newTask := boostTask.Clone()
				p.taskManager.AddSubTask(tid, newTask)
				p.taskCh <- newTask
			}
		}
		if size-capacity == 1 {
			p.cond.Signal()
			return
		}
		p.cond.Broadcast()
		return
	}
	if size < capacity {
		p.taskManager.Downclock(size)
	}
}

// Running returns the number of workers currently running.
func (p *Pool[T, U, C, CT, TF]) Running() int {
	return int(p.running.Load())
}

// Free returns the number of available goroutines to work, -1 indicates this pool is unlimited.
func (p *Pool[T, U, C, CT, TF]) Free() int {
	c := p.Cap()
	if c < 0 {
		return -1
	}
	return c - p.Running()
}

// Waiting returns the number of tasks which are waiting be executed.
func (p *Pool[T, U, C, CT, TF]) Waiting() int {
	return int(p.waiting.Load())
}

// IsClosed indicates whether the pool is closed.
func (p *Pool[T, U, C, CT, TF]) IsClosed() bool {
	return p.state.Load() == gpool.CLOSED
}

// Cap returns the capacity of this pool.
func (p *Pool[T, U, C, CT, TF]) Cap() int {
	return int(p.capacity.Load())
}

func (p *Pool[T, U, C, CT, TF]) addRunning(delta int) {
	p.running.Add(int32(delta))
}

func (p *Pool[T, U, C, CT, TF]) addWaiting(delta int) {
	p.waiting.Add(int32(delta))
}

func (p *Pool[T, U, C, CT, TF]) addWaitingTask() {
	p.waitingTask.Inc()
}

func (p *Pool[T, U, C, CT, TF]) subWaitingTask() {
	p.waitingTask.Dec()
}

// release closes this pool and releases the worker queue.
func (p *Pool[T, U, C, CT, TF]) release() {
	if !p.state.CompareAndSwap(gpool.OPENED, gpool.CLOSED) {
		return
	}
	p.lock.Lock()
	p.workers.reset()
	p.lock.Unlock()
	// There might be some callers waiting in retrieveWorker(), so we need to wake them up to prevent
	// those callers blocking infinitely.
	p.cond.Broadcast()
}

func isClose(exitCh chan struct{}) bool {
	select {
	case <-exitCh:
		return true
	default:
	}
	return false
}

// ReleaseAndWait is like Release, it waits all workers to exit.
func (p *Pool[T, U, C, CT, TF]) ReleaseAndWait() {
	if p.IsClosed() || isClose(p.stopCh) {
		return
	}

	close(p.stopCh)
	p.release()
	defer resourcemanager.InstanceResourceManager.Unregister(p.Name())
	for {
		// Wait for all workers to exit and all task to be completed.
		if p.Running() == 0 && p.heartbeatDone.Load() && p.waitingTask.Load() == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// SetConsumerFunc is to set ConsumerFunc which is to process the task.
func (p *Pool[T, U, C, CT, TF]) SetConsumerFunc(consumerFunc func(T, C, CT) U) {
	p.consumerFunc = consumerFunc
}

// AddProduceBySlice is to add Produce by a slice.
// Producer need to return ErrProducerClosed when to exit.
func (p *Pool[T, U, C, CT, TF]) AddProduceBySlice(producer func() ([]T, error), constArg C, contextFn TF, options ...TaskOption) (<-chan U, pooltask.TaskController[T, U, C, CT, TF]) {
	opt := loadTaskOptions(options...)
	taskID := p.NewTaskID()
	var wg sync.WaitGroup
	result := make(chan U, opt.ResultChanLen)
	productExitCh := make(chan struct{})
	inputCh := make(chan pooltask.Task[T], opt.TaskChanLen)
	tc := pooltask.NewTaskController[T, U, C, CT, TF](p, taskID, productExitCh, &wg, inputCh, result)
	p.taskManager.RegisterTask(taskID, int32(opt.Concurrency))
	for i := 0; i < opt.Concurrency; i++ {
		err := p.run()
		if err == gpool.ErrPoolClosed {
			break
		}
		taskBox := pooltask.NewTaskBox[T, U, C, CT, TF](constArg, contextFn, &wg, inputCh, result, taskID)
		p.addWaitingTask()
		p.taskManager.AddSubTask(taskID, &taskBox)
		p.taskCh <- &taskBox
	}
	wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logutil.BgLogger().Error("producer panic", zap.Any("recover", r), zap.Stack("stack"))
			}
			close(inputCh)
			wg.Done()
		}()
		for {
			if isClose(productExitCh) {
				return
			}
			tasks, err := producer()
			if err != nil {
				if errors.Is(err, gpool.ErrProducerClosed) {
					return
				}
				log.Error("producer error", zap.Error(err))
				return
			}
			for _, task := range tasks {
				wg.Add(1)
				task := pooltask.Task[T]{
					Task: task,
				}
				inputCh <- task
			}
		}
	}()
	return result, tc
}

// AddProducer is to add producer.
// Producer need to return ErrProducerClosed when to exit.
func (p *Pool[T, U, C, CT, TF]) AddProducer(producer func() (T, error), constArg C, contextFn TF, options ...TaskOption) (<-chan U, pooltask.TaskController[T, U, C, CT, TF]) {
	opt := loadTaskOptions(options...)
	taskID := p.NewTaskID()
	var wg sync.WaitGroup
	result := make(chan U, opt.ResultChanLen)
	productExitCh := make(chan struct{})
	inputCh := make(chan pooltask.Task[T], opt.TaskChanLen)
	p.taskManager.RegisterTask(taskID, int32(opt.Concurrency))
	tc := pooltask.NewTaskController[T, U, C, CT, TF](p, taskID, productExitCh, &wg, inputCh, result)
	for i := 0; i < opt.Concurrency; i++ {
		err := p.run()
		if err == gpool.ErrPoolClosed {
			break
		}
		p.addWaitingTask()
		taskBox := pooltask.NewTaskBox[T, U, C, CT, TF](constArg, contextFn, &wg, inputCh, result, taskID)
		p.taskManager.AddSubTask(taskID, &taskBox)
		p.taskCh <- &taskBox
	}
	wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logutil.BgLogger().Error("producer panic", zap.Any("recover", r), zap.Stack("stack"))
			}
			close(inputCh)
			wg.Done()
		}()
		for {
			if isClose(productExitCh) {
				return
			}
			task, err := producer()
			if err != nil {
				if errors.Is(err, gpool.ErrProducerClosed) {
					return
				}
				log.Error("producer error", zap.Error(err))
				return
			}
			wg.Add(1)
			t := pooltask.Task[T]{
				Task: task,
			}
			inputCh <- t
		}
	}()
	return result, tc
}

func (p *Pool[T, U, C, CT, TF]) run() error {
	if p.IsClosed() {
		return gpool.ErrPoolClosed
	}
	var w *goWorker[T, U, C, CT, TF]
	if w = p.retrieveWorker(); w == nil {
		return gpool.ErrPoolOverload
	}
	return nil
}

// retrieveWorker returns an available worker to run the tasks.
func (p *Pool[T, U, C, CT, TF]) retrieveWorker() (w *goWorker[T, U, C, CT, TF]) {
	spawnWorker := func() {
		w = p.workerCache.Get().(*goWorker[T, U, C, CT, TF])
		w.taskBoxCh = p.taskCh
		w.run()
	}

	p.lock.Lock()

	w = p.workers.detach()
	if w != nil { // first try to fetch the worker from the queue
		p.lock.Unlock()
	} else if capacity := p.Cap(); capacity == -1 || capacity > p.Running() {
		// if the worker queue is empty and we don't run out of the pool capacity,
		// then just spawn a new worker goroutine.
		p.lock.Unlock()
		spawnWorker()
	} else { // otherwise, we'll have to keep them blocked and wait for at least one worker to be put back into pool.
		if p.options.Nonblocking {
			p.lock.Unlock()
			return
		}
	retry:
		if p.options.MaxBlockingTasks != 0 && p.Waiting() >= p.options.MaxBlockingTasks {
			p.lock.Unlock()
			return
		}
		p.addWaiting(1)
		p.cond.Wait() // block and wait for an available worker
		p.addWaiting(-1)

		if p.IsClosed() {
			p.lock.Unlock()
			return
		}

		var nw int
		if nw = p.Running(); nw == 0 { // awakened by the scavenger
			p.lock.Unlock()
			spawnWorker()
			return
		}
		if w = p.workers.detach(); w == nil {
			if nw < p.Cap() {
				p.lock.Unlock()
				spawnWorker()
				return
			}
			goto retry
		}
		p.lock.Unlock()
	}
	return
}

// revertWorker puts a worker back into free pool, recycling the goroutines.
func (p *Pool[T, U, C, CT, TF]) revertWorker(worker *goWorker[T, U, C, CT, TF]) bool {
	if capacity := p.Cap(); capacity > 0 && p.Running() > capacity || p.IsClosed() {
		p.cond.Broadcast()
		return false
	}
	worker.recycleTime.Store(time.Now())
	p.lock.Lock()

	if p.IsClosed() {
		p.lock.Unlock()
		return false
	}

	err := p.workers.insert(worker)
	if err != nil {
		p.lock.Unlock()
		if err == errQueueIsFull && p.waitingTask.Load() > 0 {
			return true
		}
		return false
	}

	// Notify the invoker stuck in 'retrieveWorker()' of there is an available worker in the worker queue.
	p.cond.Signal()
	p.lock.Unlock()
	return true
}

// DeleteTask is to delete task.
// Please don't use it manually.
func (p *Pool[T, U, C, CT, TF]) DeleteTask(id uint64) {
	p.taskManager.DeleteTask(id)
}

// StopTask is to stop task by id
// Please don't use it manually.
func (p *Pool[T, U, C, CT, TF]) StopTask(id uint64) {
	p.taskManager.StopTask(id)
}

// ExitSubTask is to reduce the number of subtasks.
func (p *Pool[T, U, C, CT, TF]) ExitSubTask(id uint64) {
	p.taskManager.ExitSubTask(id)
}
