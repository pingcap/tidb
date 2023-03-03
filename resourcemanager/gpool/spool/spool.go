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
	"github.com/pingcap/tidb/resourcemanager/gpool"
	"github.com/pingcap/tidb/resourcemanager/pooltask"
	"github.com/pingcap/tidb/resourcemanager/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// Pool is a single goroutine pool. it can not reuse the goroutine.
type Pool struct {
	wg                 sync.WaitGroup
	mu                 sync.Mutex
	options            *Options
	capacity           atomic.Int32
	running            atomic.Int32
	isStop             atomic.Bool
	concurrencyMetrics prometheus.Gauge
	taskManager        pooltask.TaskManager[any, any, any, any, pooltask.NilContext]
	gpool.BasePool
}

// NewPool create a single goroutine pool.
func NewPool(name string, size int32, component util.Component, options ...Option) (*Pool, error) {
	opts := loadOptions(options...)
	result := &Pool{
		options:            opts,
		concurrencyMetrics: metrics.PoolConcurrencyCounter.WithLabelValues(name),
		taskManager:        pooltask.NewTaskManager[any, any, any, any, pooltask.NilContext](size), // TODO: this general type
	}
	result.SetName(name)
	result.capacity.Store(size)
	result.concurrencyMetrics.Set(float64(size))
	err := resourcemanager.InstanceResourceManager.Register(result, name, component)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Tune changes the pool size.
func (p *Pool) Tune(size int) {
	p.SetLastTuneTs(time.Now())
	p.capacity.Store(int32(size))
	p.concurrencyMetrics.Set(float64(size))
}

// Run runs a function in the pool.
func (p *Pool) Run(fn func()) error {
	if p.isStop.Load() {
		return gpool.ErrPoolClosed
	}
	_, run := p.checkAndAddRunning(1)
	if !run {
		return gpool.ErrPoolOverload
	}
	p.taskManager.RegisterTask(p.NewTaskID(), 1)
	p.run(fn)
	return nil
}

// Cap returns the capacity of the pool.
func (p *Pool) Cap() int {
	return int(p.capacity.Load())
}

// Running returns the number of running goroutines.
func (p *Pool) Running() int {
	return int(p.running.Load())
}

func (p *Pool) run(fn func()) {
	p.wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logutil.BgLogger().Error("recover panic", zap.Any("recover", r))
			}
			p.wg.Done()
			p.running.Add(-1)
		}()
		fn()
	}()
}

// RunWithConcurrency runs a function in the pool with concurrency.
func (p *Pool) RunWithConcurrency(fns chan func(), concurrency int) error {
	if p.isStop.Load() {
		return gpool.ErrPoolClosed
	}
	conc, run := p.checkAndAddRunning(int32(concurrency))
	if !run {
		return gpool.ErrPoolOverload
	}
	// TODO: taskManager need to refactor
	p.taskManager.RegisterTask(p.NewTaskID(), conc)
	for n := int32(0); n < conc; n++ {
		p.run(func() {
			for fn := range fns {
				fn()
			}
		})
	}
	return nil
}

// checkAndAddRunning is to check if a task can run. If can, add the running number.
func (p *Pool) checkAndAddRunning(concurrency int32) (conc int32, run bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		value, run := p.checkAndAddRunningInternal(concurrency)
		if run {
			return value, run
		}
		if !p.options.Blocking {
			return 0, false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (p *Pool) checkAndAddRunningInternal(concurrency int32) (conc int32, run bool) {
	n := p.capacity.Load() - p.running.Load()
	if n <= 0 {
		return 0, false
	}
	// if concurrency is 1 , we must return a goroutine
	// if concurrency is more than 1, we must return at least one goroutine.
	result := mathutil.Min(n, concurrency)
	p.running.Add(result)
	return result, true
}

// ReleaseAndWait releases the pool and waits for all tasks to be completed.
func (p *Pool) ReleaseAndWait() {
	p.isStop.Store(true)
	p.wg.Wait()
	resourcemanager.InstanceResourceManager.Unregister(p.Name())
}
