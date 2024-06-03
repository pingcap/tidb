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
	"time"

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	atomicutil "go.uber.org/atomic"
)

// Worker is worker interface.
type Worker[T, R any] interface {
	// HandleTask consumes a task(T) and produces a result(R).
	// The result is sent to the result channel by calling `send` function.
	HandleTask(task T, send func(R))
	Close()
}

// WorkerPool is a pool of workers.
type WorkerPool[T, R any] struct {
	ctx           context.Context
	cancel        context.CancelFunc
	name          string
	numWorkers    int32
	originWorkers int32
	runningTask   atomicutil.Int32
	taskChan      chan T
	resChan       chan R
	quitChan      chan struct{}
	wg            tidbutil.WaitGroupWrapper
	createWorker  func() Worker[T, R]
	lastTuneTs    atomicutil.Time
	mu            syncutil.RWMutex
}

// Option is the config option for WorkerPool.
type Option[T, R any] interface {
	Apply(pool *WorkerPool[T, R])
}

// None is a type placeholder for the worker pool that does not have a result receiver.
type None struct{}

// NewWorkerPool creates a new worker pool.
func NewWorkerPool[T, R any](name string, _ util.Component, numWorkers int,
	createWorker func() Worker[T, R], opts ...Option[T, R]) *WorkerPool[T, R] {
	if numWorkers <= 0 {
		numWorkers = 1
	}

	p := &WorkerPool[T, R]{
		name:          name,
		numWorkers:    int32(numWorkers),
		originWorkers: int32(numWorkers),
		quitChan:      make(chan struct{}),
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

	for i := 0; i < int(p.numWorkers); i++ {
		p.runAWorker()
	}
}

func (p *WorkerPool[T, R]) handleTaskWithRecover(w Worker[T, R], task T) {
	p.runningTask.Add(1)
	defer func() {
		p.runningTask.Add(-1)
	}()
	defer tidbutil.Recover(metrics.LabelWorkerPool, "handleTaskWithRecover", nil, false)

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
			case <-p.quitChan:
				w.Close()
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
func (p *WorkerPool[T, R]) Tune(numWorkers int32) {
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
