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
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

// TaskMayPanic is a type to remind the developer that need to handle panic in
// the task.
type TaskMayPanic interface {
	// RecoverArgs returns the argument for pkg/util.Recover function of this task.
	RecoverArgs() (metricsLabel string, funcInfo string, recoverFn func(), quit bool)
}

// Worker is worker interface.
type Worker[T TaskMayPanic, R any] interface {
	// HandleTask consumes a task(T) and produces a result(R).
	// The result is sent to the result channel by calling `send` function.
	HandleTask(task T, send func(R))
	Close()
}

type tuneConfig struct {
	wg *sync.WaitGroup
}

// WorkerPool is a pool of workers.
type WorkerPool[T TaskMayPanic, R any] struct {
	ctx           context.Context
	cancel        context.CancelFunc
	name          string
	numWorkers    int32
	originWorkers int32
	runningTask   atomicutil.Int32
	taskChan      chan T
	resChan       chan R
	quitChan      chan tuneConfig
	wg            tidbutil.WaitGroupWrapper
	createWorker  func() Worker[T, R]
	lastTuneTs    atomicutil.Time
	started       atomic.Bool
	mu            syncutil.RWMutex
}

// Option is the config option for WorkerPool.
type Option[T TaskMayPanic, R any] interface {
	Apply(pool *WorkerPool[T, R])
}

// None is a type placeholder for the worker pool that does not have a result receiver.
type None struct{}

// NewWorkerPool creates a new worker pool.
func NewWorkerPool[T TaskMayPanic, R any](
	name string,
	_ util.Component,
	numWorkers int,
	createWorker func() Worker[T, R],
	opts ...Option[T, R],
) *WorkerPool[T, R] {
	if numWorkers <= 0 {
		numWorkers = 1
	}

	p := &WorkerPool[T, R]{
		name:          name,
		numWorkers:    int32(numWorkers),
		originWorkers: int32(numWorkers),
		quitChan:      make(chan tuneConfig),
	}

	for _, opt := range opts {
		opt.Apply(p)
	}

	p.createWorker = createWorker
	return p
}

// SetTaskReceiver sets the task receiver for the pool.
func (p *WorkerPool[T, R]) SetTaskReceiver(recv chan T) {
	p.taskChan = recv
}

// SetResultSender sets the result sender for the pool.
func (p *WorkerPool[T, R]) SetResultSender(sender chan R) {
	p.resChan = sender
}

// Start starts default count of workers.
func (p *WorkerPool[T, R]) Start(ctx context.Context) {
	if p.taskChan == nil {
		p.taskChan = make(chan T)
	}

	if p.resChan == nil {
		var zero R
		var r any = zero
		if _, ok := r.(None); !ok {
			p.resChan = make(chan R)
		}
	}

	p.ctx, p.cancel = context.WithCancel(ctx)

	p.mu.Lock()
	defer p.mu.Unlock()

	for range p.numWorkers {
		p.runAWorker()
	}
	p.started.Store(true)
}

func (p *WorkerPool[T, R]) handleTaskWithRecover(w Worker[T, R], task T) {
	p.runningTask.Add(1)
	defer func() {
		p.runningTask.Add(-1)
	}()
	defer tidbutil.Recover(task.RecoverArgs())

	sendResult := func(r R) {
		if p.resChan == nil {
			return
		}
		select {
		case p.resChan <- r:
		case <-p.ctx.Done():
		}
	}

	w.HandleTask(task, sendResult)
}

func (p *WorkerPool[T, R]) runAWorker() {
	w := p.createWorker()
	if w == nil {
		return // Fail to create worker, quit.
	}
	p.wg.Run(func() {
		for {
			select {
			case task, ok := <-p.taskChan:
				if !ok {
					w.Close()
					return
				}
				p.handleTaskWithRecover(w, task)
			case cfg, ok := <-p.quitChan:
				w.Close()
				if ok {
					cfg.wg.Done()
				}
				return
			case <-p.ctx.Done():
				w.Close()
				return
			}
		}
	})
}

// AddTask adds a task to the pool.
func (p *WorkerPool[T, R]) AddTask(task T) {
	select {
	case <-p.ctx.Done():
	case p.taskChan <- task:
	}
}

// GetResultChan gets the result channel from the pool.
func (p *WorkerPool[T, R]) GetResultChan() <-chan R {
	return p.resChan
}

// Tune tunes the pool to the specified number of workers.
// wait: whether to wait for all workers to close when reducing workers count.
// this method can only be called after Start.
func (p *WorkerPool[T, R]) Tune(numWorkers int32, wait bool) {
	if numWorkers <= 0 {
		numWorkers = 1
	}
	p.lastTuneTs.Store(time.Now())
	p.mu.Lock()
	defer p.mu.Unlock()

	logutil.BgLogger().Info("tune worker pool",
		zap.Int32("from", p.numWorkers), zap.Int32("to", numWorkers))

	// If the pool is not started, just set the number of workers.
	if !p.started.Load() {
		p.numWorkers = numWorkers
		return
	}

	diff := numWorkers - p.numWorkers
	if diff > 0 {
		// Add workers
		for range diff {
			p.runAWorker()
		}
	} else if diff < 0 {
		// Remove workers
		var wg sync.WaitGroup
	outer:
		for range int(-diff) {
			wg.Add(1)
			select {
			case p.quitChan <- tuneConfig{wg: &wg}:
			case <-p.ctx.Done():
				logutil.BgLogger().Info("context done when tuning worker pool",
					zap.Int32("from", p.numWorkers), zap.Int32("to", numWorkers))
				wg.Done()
				break outer
			}
		}
		if wait {
			wg.Wait()
		}
	}
	p.numWorkers = numWorkers
}

// LastTunerTs returns the last time when the pool was tuned.
func (p *WorkerPool[T, R]) LastTunerTs() time.Time {
	return p.lastTuneTs.Load()
}

// Cap returns the capacity of the pool.
func (p *WorkerPool[T, R]) Cap() int32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.numWorkers
}

// Running returns the number of running workers.
func (p *WorkerPool[T, R]) Running() int32 {
	return p.runningTask.Load()
}

// Name returns the name of the pool.
func (p *WorkerPool[T, R]) Name() string {
	return p.name
}

// ReleaseAndWait releases the pool and wait for complete.
func (p *WorkerPool[T, R]) ReleaseAndWait() {
	close(p.quitChan)
	p.Release()
	p.Wait()
}

// Wait waits for all workers to complete.
func (p *WorkerPool[T, R]) Wait() {
	p.wg.Wait()
}

// Release releases the pool.
func (p *WorkerPool[T, R]) Release() {
	if p.cancel != nil {
		p.cancel()
	}
	if p.resChan != nil {
		close(p.resChan)
		p.resChan = nil
	}
}

// GetOriginConcurrency return the concurrency of the pool at the init.
func (p *WorkerPool[T, R]) GetOriginConcurrency() int32 {
	return p.originWorkers
}
