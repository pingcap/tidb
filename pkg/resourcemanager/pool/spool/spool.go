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

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/resourcemanager"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool"
	"github.com/pingcap/tidb/pkg/resourcemanager/poolmanager"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"
)

const waitInterval = 5 * time.Millisecond

// Pool is a single goroutine pool. it can not reuse the goroutine.
type Pool struct {
	wg                 sync.WaitGroup
	mu                 deadlock.RWMutex
	options            *Options
	originCapacity     int32
	capacity           int32
	running            atomic.Int32
	waiting            atomic.Int32
	isStop             atomic.Bool
	condMu             sync.Mutex
	cond               sync.Cond
	concurrencyMetrics prometheus.Gauge
	taskManager        poolmanager.TaskManager
	pool.BasePool
}

// NewPool create a single goroutine pool.
func NewPool(name string, size int32, component util.Component, options ...Option) (*Pool, error) {
	opts := loadOptions(options...)
	result := &Pool{
		options:            opts,
		concurrencyMetrics: metrics.PoolConcurrencyCounter.WithLabelValues(name),
		taskManager:        poolmanager.NewTaskManager(size),
	}
	result.cond = *sync.NewCond(&result.condMu)
	if size == 0 {
		return nil, pool.ErrPoolParamsInvalid
	}
	result.SetName(name)
	result.capacity = size
	result.originCapacity = size
	result.concurrencyMetrics.Set(float64(size))
	err := resourcemanager.InstanceResourceManager.Register(result, name, component)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Tune changes the pool size.
func (p *Pool) Tune(size int32) {
	if size == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.SetLastTuneTs(time.Now())
	old := p.capacity
	p.capacity = size
	p.concurrencyMetrics.Set(float64(size))
	if old == size {
		return
	}
	if old < size && p.running.Load() < size {
		_, task := p.taskManager.Overclock()
		if task != nil {
			p.running.Add(1)
			p.run(func() {
				runTask(task)
			})
		}
		return
	}
	if p.running.Load() > size {
		p.taskManager.Downclock()
	}
}

// Run runs a function in the pool.
func (p *Pool) Run(fn func()) error {
	p.waiting.Add(1)
	defer p.cond.Signal()
	defer p.waiting.Add(-1)
	if p.isStop.Load() {
		return pool.ErrPoolClosed
	}
	_, run := p.checkAndAddRunning(1)
	if !run {
		return pool.ErrPoolOverload
	}
	p.run(fn)
	return nil
}

// Cap returns the capacity of the pool.
func (p *Pool) Cap() int32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.capacity
}

// Running returns the number of running goroutines.
func (p *Pool) Running() int32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.running.Load()
}

func (p *Pool) run(fn func()) {
	p.wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logutil.BgLogger().Error("recover panic", zap.Any("recover", r), zap.Stack("stack"))
			}
			p.wg.Done()
			p.running.Add(-1)
		}()
		fn()
	}()
}

// RunWithConcurrency runs a function in the pool with concurrency.
func (p *Pool) RunWithConcurrency(fns chan func(), concurrency uint32) error {
	p.waiting.Add(1)
	defer p.cond.Signal()
	defer p.waiting.Add(-1)
	if p.isStop.Load() {
		return pool.ErrPoolClosed
	}
	conc, run := p.checkAndAddRunning(concurrency)
	if !run {
		return pool.ErrPoolOverload
	}
	exitCh := make(chan struct{}, 1)
	meta := poolmanager.NewMeta(p.GenTaskID(), exitCh, fns, int32(concurrency))
	p.taskManager.RegisterTask(meta)
	for n := int32(0); n < conc; n++ {
		p.run(func() {
			runTask(meta)
		})
	}
	return nil
}

// checkAndAddRunning is to check if a task can run. If can, add the running number.
func (p *Pool) checkAndAddRunning(concurrency uint32) (conc int32, run bool) {
	for {
		if p.isStop.Load() {
			return 0, false
		}
		p.mu.Lock()
		value, run := p.checkAndAddRunningInternal(int32(concurrency))
		if run {
			p.mu.Unlock()
			return value, run
		}
		if !p.options.Blocking {
			p.mu.Unlock()
			return 0, false
		}
		p.mu.Unlock()
		time.Sleep(waitInterval)
	}
}

func (p *Pool) checkAndAddRunningInternal(concurrency int32) (conc int32, run bool) {
	n := p.capacity - p.running.Load()
	if n <= 0 {
		return 0, false
	}
	// if concurrency is 1 , we must return a goroutine
	// if concurrency is more than 1, we must return at least one goroutine.
	result := min(n, concurrency)
	p.running.Add(result)
	return result, true
}

// ReleaseAndWait releases the pool and waits for all tasks to be completed.
func (p *Pool) ReleaseAndWait() {
	p.isStop.Store(true)
	// wait for all the task in the pending to exit
	p.cond.L.Lock()
	for p.waiting.Load() > 0 {
		p.cond.Wait()
	}
	p.cond.L.Unlock()
	p.wg.Wait()
	resourcemanager.InstanceResourceManager.Unregister(p.Name())
}

func runTask(task *poolmanager.Meta) {
	taskCh := task.GetTaskCh()
	exitCh := task.GetExitCh()
	task.IncTask()
	defer task.DecTask()
	for {
		select {
		case f, ok := <-taskCh:
			if !ok {
				return
			}
			f()
		case <-exitCh:
			return
		}
	}
}

// GetOriginConcurrency return the concurrency of the pool at the init.
func (p *Pool) GetOriginConcurrency() int32 {
	return p.originCapacity
}
