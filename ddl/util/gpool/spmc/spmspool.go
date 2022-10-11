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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/gpool"
)

// Pool is a single producer, multiple consumer goroutine pool.
type Pool[T any, U any, C any] struct {
	workerCache   sync.Pool
	lock          sync.Locker
	workers       workerArray[T, U, C]
	consumerFunc  func(T, C) U
	options       *Options
	stopCh        chan struct{}
	taskCh        chan *taskBox[T, U, C]
	cond          *sync.Cond
	prodwg        sync.WaitGroup
	capacity      atomic.Int32
	running       atomic.Int32
	state         atomic.Int32
	waiting       atomic.Int32
	heartbeatDone atomic.Int32
}

// NewSPMCPool create a single producer, multiple consumer goroutine pool.
func NewSPMCPool[T any, U any, C any](size int32, options ...Option) *Pool[T, U, C] {
	opts := loadOptions(options...)
	if expiry := opts.ExpiryDuration; expiry <= 0 {
		opts.ExpiryDuration = gpool.DefaultCleanIntervalTime
	}
	result := &Pool[T, U, C]{
		taskCh:  make(chan *taskBox[T, U, C], 128),
		stopCh:  make(chan struct{}),
		lock:    gpool.NewSpinLock(),
		options: opts,
	}
	result.workerCache.New = func() interface{} {
		return &goWorker[T, U, C]{
			pool: result,
			exit: make(chan struct{}),
		}
	}
	result.capacity.Add(size)
	if result.options.PreAlloc {
		result.workers = newWorkerArray[T, U, C](loopQueueType, int(size))
	} else {
		result.workers = newWorkerArray[T, U, C](stackType, 0)
	}
	result.cond = sync.NewCond(result.lock)
	// Start a goroutine to clean up expired workers periodically.
	go result.purgePeriodically()
	return result
}

// purgePeriodically clears expired workers periodically which runs in an individual goroutine, as a scavenger.
func (p *Pool[T, U, C]) purgePeriodically() {
	heartbeat := time.NewTicker(p.options.ExpiryDuration)
	defer func() {
		heartbeat.Stop()
		p.heartbeatDone.Store(1)
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
			expiredWorkers[i].task <- nil
			expiredWorkers[i] = nil
		}

		// There might be a situation where all workers have been cleaned up(no worker is running),
		// or another case where the pool capacity has been Tuned up,
		// while some invokers still get stuck in "p.cond.Wait()",
		// then it ought to wake all those invokers.
		if p.Running() == 0 || (p.Waiting() > 0 && p.Free() > 0) {
			p.cond.Broadcast()
		}
	}
}

// Tune changes the capacity of this pool, note that it is noneffective to the infinite or pre-allocation pool.
func (p *Pool[T, U, C]) Tune(size int) {
	capacity := p.Cap()
	if capacity == -1 || size <= 0 || size == capacity || p.options.PreAlloc {
		return
	}
	p.capacity.Store(int32(size))
	if size > capacity {
		if size-capacity == 1 {
			p.cond.Signal()
			return
		}
		p.cond.Broadcast()
	}
}

// Running returns the number of workers currently running.
func (p *Pool[T, U, C]) Running() int {
	return int(p.running.Load())
}

// Free returns the number of available goroutines to work, -1 indicates this pool is unlimited.
func (p *Pool[T, U, C]) Free() int {
	c := p.Cap()
	if c < 0 {
		return -1
	}
	return c - p.Running()
}

// Waiting returns the number of tasks which are waiting be executed.
func (p *Pool[T, U, C]) Waiting() int {
	return int(p.waiting.Load())
}

// IsClosed indicates whether the pool is closed.
func (p *Pool[T, U, C]) IsClosed() bool {
	return p.state.Load() == gpool.CLOSED
}

// Cap returns the capacity of this pool.
func (p *Pool[T, U, C]) Cap() int {
	return int(p.capacity.Load())
}

func (p *Pool[T, U, C]) addRunning(delta int) {
	p.running.Add(int32(delta))
}

func (p *Pool[T, U, C]) addWaiting(delta int) {
	p.waiting.Add(int32(delta))
}

// Release closes this pool and releases the worker queue.
func (p *Pool[T, U, C]) Release() {
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
func (p *Pool[T, U, C]) ReleaseAndWait() {
	if p.IsClosed() || isClose(p.stopCh) {
		return
	}

	close(p.stopCh)
	p.Release()
	for {
		if p.Running() == 0 && p.heartbeatDone.Load() == 1 {
			return
		}
	}
}

// SetConsumerFunc is to set ConsumerFunc which is to process the task.
func (p *Pool[T, U, C]) SetConsumerFunc(consumerFunc func(T, C) U) {
	p.consumerFunc = consumerFunc
}

// AddProduce is to add Produce.
func (p *Pool[T, U, C]) AddProduce(task T, constArg C) (<-chan U, TaskController) {
	result := make(chan U, 1)
	var wg sync.WaitGroup
	tc := TaskController{
		wg: &wg,
	}
	taskCh := make(chan T)
	taskBox := taskBox[T, U, C]{
		task:      taskCh,
		constArgs: &constArg,
		wg:        &wg,
		resultCh:  result,
	}
	p.taskCh <- &taskBox
	taskCh <- task
	close(taskCh)
	return result, tc
}

// AddProducer is to add producer.
func (p *Pool[T, U, C]) AddProducer(producer func() (T, error), constArg C, size int) (<-chan U, TaskController) {
	result := make(chan U, 10)

	var wg sync.WaitGroup
	closeCh := make(chan struct{})
	tc := NewTaskController(closeCh, &wg)
	taskCh := make(chan T, size)
	for i := 0; i < size; i++ {
		log.Info("create goro")
		err := p.run()
		if err == gpool.ErrPoolClosed {
			break
		}
		taskBox := taskBox[T, U, C]{
			constArgs: &constArg,
			wg:        &wg,
			task:      taskCh,
			resultCh:  result,
		}
		p.taskCh <- &taskBox
	}
	go func(constArg C) {
		defer func() {
			close(closeCh)
			close(taskCh)
		}()
		for {
			task, err := producer()
			if err != nil {
				return
			}
			wg.Add(1)
			taskCh <- task
		}
	}(constArg)
	return result, tc
}

func (p *Pool[T, U, C]) run() error {
	if p.IsClosed() {
		return gpool.ErrPoolClosed
	}
	var w *goWorker[T, U, C]
	if w = p.retrieveWorker(); w == nil {
		return gpool.ErrPoolOverload
	}
	return nil
}

// retrieveWorker returns an available worker to run the tasks.
func (p *Pool[T, U, C]) retrieveWorker() (w *goWorker[T, U, C]) {
	spawnWorker := func() {
		w = p.workerCache.Get().(*goWorker[T, U, C])
		w.task = p.taskCh
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
func (p *Pool[T, U, C]) revertWorker(worker *goWorker[T, U, C]) bool {
	if capacity := p.Cap(); (capacity > 0 && p.Running() > capacity) || p.IsClosed() {
		p.cond.Broadcast()
		return false
	}
	worker.recycleTime = time.Now()
	p.lock.Lock()

	// To avoid memory leaks, add a double check in the lock scope.
	// Issue: https://github.com/panjf2000/ants/issues/113
	if p.IsClosed() {
		p.lock.Unlock()
		return false
	}

	err := p.workers.insert(worker)
	if err != nil {
		p.lock.Unlock()
		return false
	}

	// Notify the invoker stuck in 'retrieveWorker()' of there is an available worker in the worker queue.
	p.cond.Signal()
	p.lock.Unlock()
	return true
}
