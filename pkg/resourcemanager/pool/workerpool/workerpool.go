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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

// Context is the context used for worker pool
type Context struct {
	context.Context
	cancel   context.CancelFunc
	firstErr atomic.Pointer[error]
}

// OnError store the error and cancel the context.
// If the error is already set, it will not overwrite it.
func (ctx *Context) OnError(err error) {
	ctx.firstErr.CompareAndSwap(nil, &err)
	logutil.BgLogger().Error("worker pool encountered error", zap.Error(err))
	ctx.cancel()
}

// OperatorErr returns the error caused by business logic.
func (ctx *Context) OperatorErr() error {
	err := ctx.firstErr.Load()
	if err == nil {
		return nil
	}
	return *err
}

// Cancel cancels the context of the operator.
func (ctx *Context) Cancel() {
	ctx.cancel()
}

// NewContext creates a new Context
func NewContext(
	ctx context.Context,
) *Context {
	cctx, cancel := context.WithCancel(ctx)
	return &Context{
		Context: cctx,
		cancel:  cancel,
	}
}

// TaskMayPanic is a type to remind the developer that need to handle panic in
// the task.
type TaskMayPanic interface {
	// RecoverArgs returns the argument for pkg/util.Recover function of this task.
	// The error returned is which will be passed to upper level, if not provided,
	// we will use the default error.
	RecoverArgs() (metricsLabel string, funcInfo string, err error)
}

// Worker is worker interface.
type Worker[T TaskMayPanic, R any] interface {
	// HandleTask consumes a task(T), either produces a result(R) or return an error.
	// The result is sent to the result channel by calling `send` function, and the
	// error returned will be catched, log, and broadcasted it to other operators
	// by worker pool.
	// TODO(joechenrh): we can pass the context to HandleTask, so we don't need to
	// store the context in each worker implementation.
	HandleTask(task T, send func(R)) error
	Close() error
}

type tuneConfig struct {
	wg *sync.WaitGroup
}

// Tuner is an interface that provides capacity for tuning
// the worker pools. It's used to pass worker pool without import cycle
// caused by generic type.
type Tuner interface {
	Tune(numWorkers int32, wait bool)
}

// WorkerPool is a pool of workers.
type WorkerPool[T TaskMayPanic, R any] struct {
	// wctx are the context used for the whole pipeline, and ctx and cancel are derived
	// from wctx, to notify all workers to quit when the tasks are done or any error occurs.
	wctx   *Context
	ctx    context.Context
	cancel context.CancelFunc

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
func (p *WorkerPool[T, R]) Start(ctx *Context) {
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

	p.wctx = ctx
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

	label, funcInfo, err := task.RecoverArgs()
	recoverFn := func() {
		if err != nil {
			p.wctx.OnError(err)
		} else {
			p.wctx.OnError(errors.Errorf("task panic: %s, func info: %s", label, funcInfo))
		}
	}

	defer tidbutil.Recover(label, funcInfo, recoverFn, false)

	sendResult := func(r R) {
		if p.resChan == nil {
			return
		}
		select {
		case p.resChan <- r:
		case <-p.ctx.Done():
		}
	}

	if err := w.HandleTask(task, sendResult); err != nil {
		p.wctx.OnError(err)
	}
}

func (p *WorkerPool[T, R]) runAWorker() {
	w := p.createWorker()
	if w == nil {
		return // Fail to create worker, quit.
	}
	p.wg.Run(func() {
		var err error
		defer func() {
			if err != nil {
				p.wctx.OnError(err)
			}
		}()

		for {
			select {
			case task, ok := <-p.taskChan:
				if !ok {
					err = w.Close()
					return
				}
				p.handleTaskWithRecover(w, task)
			case cfg, ok := <-p.quitChan:
				err = w.Close()
				if ok {
					cfg.wg.Done()
				}
				return
			case <-p.ctx.Done():
				err = w.Close()
				return
			}
		}
	})
}

// AddTask adds a task to the pool, only used in test.
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
		for range -diff {
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

// CloseAndWait manually closes the pool and wait for complete, only used in test
func (p *WorkerPool[T, R]) CloseAndWait() {
	if p.cancel != nil {
		p.cancel()
	}
	close(p.quitChan)
	p.Release()
}

// Release waits the pool to be released.
// It will wait the input channel to be closed,
// or the context being cancelled by business error.
func (p *WorkerPool[T, R]) Release() {
	// First, wait waits for all workers to complete.
	p.wg.Wait()

	// Cancel tuning workers.
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
