// Copyright 2023 PingCAP, Inc.
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

package spool

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/resourcemanager"
	"github.com/pingcap/tidb/resourcemanager/util"
	"github.com/pingcap/tidb/util/gpool"
	"github.com/prometheus/client_golang/prometheus"
)

// Pool is a single producer, multiple consumer goroutine pool.
type Pool struct {
	workerCache sync.Pool
	lock        sync.Locker
	stopCh      chan struct{}
	cond        *sync.Cond

	workers *loopQueue
	options *Options
	gpool.BasePool

	capacity           atomic.Int32
	running            atomic.Int32
	state              atomic.Int32
	waiting            atomic.Int32 // waiting is the number of goroutines that are waiting for the pool to be available.
	heartbeatDone      atomic.Bool
	concurrencyMetrics prometheus.Gauge
}

// NewPool create a single goroutine pool.
func NewPool(name string, size int32, component util.Component, options ...Option) (*Pool, error) {
	opts := loadOptions(options...)
	if expiry := opts.ExpiryDuration; expiry <= 0 {
		opts.ExpiryDuration = gpool.DefaultCleanIntervalTime
	}
	result := &Pool{
		stopCh:             make(chan struct{}),
		lock:               &sync.Mutex{},
		concurrencyMetrics: metrics.PoolConcurrencyCounter.WithLabelValues(name),
		options:            opts,
	}
	result.SetName(name)
	result.state.Store(int32(gpool.OPENED))
	result.workerCache.New = func() interface{} {
		return &goWorker{
			pool: result,
			task: make(chan func()),
		}
	}
	result.capacity.Add(size)
	result.concurrencyMetrics.Set(float64(size))
	result.workers = newWorkerLoopQueue(int(size))
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
func (p *Pool) purgePeriodically() {
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
func (p *Pool) Tune(size int) {
	capacity := p.Cap()
	if capacity == -1 || size <= 0 || size == capacity {
		return
	}
	p.capacity.Store(int32(size))
	p.concurrencyMetrics.Set(float64(size))
	if size > capacity {
		if size-capacity == 1 {
			p.cond.Signal()
			return
		}
		p.cond.Broadcast()
		return
	}
}

// Run submits a task to this pool.
func (p *Pool) Run(task func()) error {
	if p.IsClosed() {
		return gpool.ErrPoolClosed
	}
	var w *goWorker
	if w = p.retrieveWorker(); w == nil {
		return gpool.ErrPoolOverload
	}
	w.task <- task
	return nil
}

// RunWithConcurrency submits a task to this pool with concurrency.
func (p *Pool) RunWithConcurrency(task func(), concurrency int) (err error) {
	var wg sync.WaitGroup
	defer wg.Wait()
	for i := 0; i < concurrency; i++ {
		if p.IsClosed() {
			return gpool.ErrPoolClosed
		}
		var w *goWorker
		if w = p.retrieveWorker(); w == nil {
			if i != 0 {
				return gpool.ErrPoolOverload
			}
			return nil
		}
		t := func() {
			wg.Done()
			task()
		}
		wg.Add(1)
		w.task <- t
	}
	return nil
}

// Running returns the number of workers currently running.
func (p *Pool) Running() int {
	return int(p.running.Load())
}

// Free returns the number of available goroutines to work, -1 indicates this pool is unlimited.
func (p *Pool) Free() int {
	c := p.Cap()
	if c < 0 {
		return -1
	}
	return c - p.Running()
}

// Waiting returns the number of tasks which are waiting be executed.
func (p *Pool) Waiting() int {
	return int(p.waiting.Load())
}

// IsClosed indicates whether the pool is closed.
func (p *Pool) IsClosed() bool {
	return p.state.Load() == gpool.CLOSED
}

// Cap returns the capacity of this pool.
func (p *Pool) Cap() int {
	return int(p.capacity.Load())
}

func (p *Pool) addRunning(delta int) {
	p.running.Add(int32(delta))
}

func (p *Pool) addWaiting(delta int) {
	p.waiting.Add(int32(delta))
}

// release closes this pool and releases the worker queue.
func (p *Pool) release() {
	p.state.Store(gpool.CLOSED)
	p.lock.Lock()
	p.workers.reset()
	p.lock.Unlock()
	// There might be some callers waiting in retrieveWorker(), so we need to wake them up to prevent
	// those callers blocking infinitely.
	p.cond.Broadcast()
}

// ReleaseAndWait is like Release, it waits all workers to exit.
func (p *Pool) ReleaseAndWait() {
	if p.IsClosed() {
		return
	}
	close(p.stopCh)
	p.release()
	defer resourcemanager.InstanceResourceManager.Unregister(p.Name())
	for {
		// Wait for all workers to exit and all task to be completed.
		if p.Running() == 0 && p.heartbeatDone.Load() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Close is a alias of ReleaseAndWait. It is for the implement of Pool in the tikv/client-go.
func (p *Pool) Close() {
	p.ReleaseAndWait()
}

func (p *Pool) run() error {
	if p.IsClosed() {
		return gpool.ErrPoolClosed
	}
	var w *goWorker
	if w = p.retrieveWorker(); w == nil {
		return gpool.ErrPoolOverload
	}
	return nil
}

// retrieveWorker returns an available worker to run the tasks.
func (p *Pool) retrieveWorker() (w *goWorker) {
	spawnWorker := func() {
		w = p.workerCache.Get().(*goWorker)
		w.run()
	}

	p.lock.Lock()
	w = p.workers.detach()
	if w != nil { // first try to fetch the worker from the queue
		p.lock.Unlock()
	} else if capacity := p.Cap(); capacity == -1 || capacity > p.Running() {
		// if the worker queue is empty, and we don't run out of the pool capacity,
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
func (p *Pool) revertWorker(worker *goWorker) bool {
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
		return false
	}

	// Notify the invoker stuck in 'retrieveWorker()' of there is an available worker in the worker queue.
	p.cond.Signal()
	p.lock.Unlock()
	return true
}
