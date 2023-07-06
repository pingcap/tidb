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

package workerpool

import (
	"time"

	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/resourcemanager"
	"github.com/pingcap/tidb/resourcemanager/util"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/syncutil"
	atomicutil "go.uber.org/atomic"
)

// Worker is worker interface.
type Worker[T any] interface {
	HandleTask(task T)
	Close()
}

// WorkerPool is a pool of workers.
type WorkerPool[T any] struct {
	name          string
	numWorkers    int32
	originWorkers int32
	runningTask   atomicutil.Int32
	taskChan      chan T
	quitChan      chan struct{}
	wg            tidbutil.WaitGroupWrapper
	createWorker  func() Worker[T]
	lastTuneTs    atomicutil.Time
	mu            syncutil.RWMutex
	skipRegister  bool
}

// Option is the config option for WorkerPool.
type Option[T any] interface {
	Apply(pool *WorkerPool[T])
}

// OptionSkipRegister is an option to skip register the worker pool to resource manager.
type OptionSkipRegister[T any] struct{}

// Apply implements the Option interface.
func (OptionSkipRegister[T]) Apply(pool *WorkerPool[T]) {
	pool.skipRegister = true
}

// NewWorkerPool creates a new worker pool.
func NewWorkerPool[T any](name string, component util.Component, numWorkers int,
	createWorker func() Worker[T], opts ...Option[T]) (*WorkerPool[T], error) {
	if numWorkers <= 0 {
		numWorkers = 1
	}

	p := &WorkerPool[T]{
		name:          name,
		numWorkers:    int32(numWorkers),
		originWorkers: int32(numWorkers),
		taskChan:      make(chan T),
		quitChan:      make(chan struct{}),
		createWorker:  createWorker,
	}

	for _, opt := range opts {
		opt.Apply(p)
	}

	if !p.skipRegister {
		err := resourcemanager.InstanceResourceManager.Register(p, name, component)
		if err != nil {
			return nil, err
		}
	}

	// Start default count of workers.
	for i := 0; i < int(p.numWorkers); i++ {
		p.runAWorker()
	}

	return p, nil
}

func (p *WorkerPool[T]) handleTaskWithRecover(w Worker[T], task T) {
	p.runningTask.Add(1)
	defer func() {
		p.runningTask.Add(-1)
	}()
	defer tidbutil.Recover(metrics.LabelWorkerPool, "handleTaskWithRecover", nil, false)

	w.HandleTask(task)
}

func (p *WorkerPool[T]) runAWorker() {
	w := p.createWorker()
	if w == nil {
		return // Fail to create worker, quit.
	}
	p.wg.Run(func() {
		for {
			select {
			case task := <-p.taskChan:
				p.handleTaskWithRecover(w, task)
			case <-p.quitChan:
				w.Close()
				return
			}
		}
	})
}

// AddTask adds a task to the pool.
func (p *WorkerPool[T]) AddTask(task T) {
	p.taskChan <- task
}

// Tune tunes the pool to the specified number of workers.
func (p *WorkerPool[T]) Tune(numWorkers int32) {
	if numWorkers <= 0 {
		numWorkers = 1
	}
	p.lastTuneTs.Store(time.Now())
	p.mu.Lock()
	defer p.mu.Unlock()
	diff := numWorkers - p.numWorkers
	if diff > 0 {
		// Add workers
		for i := 0; i < int(diff); i++ {
			p.runAWorker()
		}
	} else if diff < 0 {
		// Remove workers
		for i := 0; i < int(-diff); i++ {
			p.quitChan <- struct{}{}
		}
	}
	p.numWorkers = numWorkers
}

// LastTunerTs returns the last time when the pool was tuned.
func (p *WorkerPool[T]) LastTunerTs() time.Time {
	return p.lastTuneTs.Load()
}

// Cap returns the capacity of the pool.
func (p *WorkerPool[T]) Cap() int32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.numWorkers
}

// Running returns the number of running workers.
func (p *WorkerPool[T]) Running() int32 {
	return p.runningTask.Load()
}

// Name returns the name of the pool.
func (p *WorkerPool[T]) Name() string {
	return p.name
}

// ReleaseAndWait releases the pool and wait for complete.
func (p *WorkerPool[T]) ReleaseAndWait() {
	close(p.quitChan)
	p.wg.Wait()
	if !p.skipRegister {
		resourcemanager.InstanceResourceManager.Unregister(p.Name())
	}
}

// GetOriginConcurrency return the concurrency of the pool at the init.
func (p *WorkerPool[T]) GetOriginConcurrency() int32 {
	return p.originWorkers
}
