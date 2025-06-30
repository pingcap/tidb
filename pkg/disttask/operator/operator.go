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

package operator

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	"golang.org/x/sync/errgroup"
)

// Operator is the basic operation unit in the task execution.
type Operator interface {
	Open() error
	// Close wait task done and close the operator.
	// TODO: the wait part should be separated from the close part.
	Close() error
	String() string
}

// TunableOperator is the operator which supports modifying pool size.
type TunableOperator interface {
	TuneWorkerPoolSize(workerNum int32, wait bool)
	GetWorkerPoolSize() int32
}

// AsyncOperator process the data in async way.
//
// Eg: The sink of AsyncOperator op1 and the source of op2
// use the same channel, Then op2's worker will handle
// the result from op1.
type AsyncOperator[T workerpool.TaskMayPanic, R any] struct {
	ctx  context.Context
	pool *workerpool.WorkerPool[T, R]
}

// NewAsyncOperatorWithTransform create an AsyncOperator with a transform function.
func NewAsyncOperatorWithTransform[T workerpool.TaskMayPanic, R any](
	ctx context.Context,
	name string,
	workerNum int,
	transform func(T) R,
) *AsyncOperator[T, R] {
	pool := workerpool.NewWorkerPool(name, util.DistTask, workerNum, newAsyncWorkerCtor(transform))
	return NewAsyncOperator(ctx, pool)
}

// NewAsyncOperator create an AsyncOperator.
func NewAsyncOperator[T workerpool.TaskMayPanic, R any](ctx context.Context, pool *workerpool.WorkerPool[T, R]) *AsyncOperator[T, R] {
	return &AsyncOperator[T, R]{
		ctx:  ctx,
		pool: pool,
	}
}

// Open implements the Operator's Open interface.
func (c *AsyncOperator[T, R]) Open() error {
	c.pool.Start(c.ctx)
	return nil
}

// Close implements the Operator's Close interface.
func (c *AsyncOperator[T, R]) Close() error {
	// Wait all tasks done.
	// We don't need to close the task channel because
	// it is maintained outside this operator, see SetSource.
	c.pool.Wait()
	c.pool.Release()
	return nil
}

// String show the name.
func (*AsyncOperator[T, R]) String() string {
	var zT T
	var zR R
	return fmt.Sprintf("AsyncOp[%T, %T]", zT, zR)
}

// SetSource set the source channel.
func (c *AsyncOperator[T, R]) SetSource(ch DataChannel[T]) {
	c.pool.SetTaskReceiver(ch.Channel())
}

// SetSink set the sink channel.
func (c *AsyncOperator[T, R]) SetSink(ch DataChannel[R]) {
	c.pool.SetResultSender(ch.Channel())
}

// TuneWorkerPoolSize tunes the worker pool size.
func (c *AsyncOperator[T, R]) TuneWorkerPoolSize(workerNum int32, wait bool) {
	c.pool.Tune(workerNum, wait)
}

// GetWorkerPoolSize returns the worker pool size.
func (c *AsyncOperator[T, R]) GetWorkerPoolSize() int32 {
	return c.pool.Cap()
}

type asyncWorker[T, R any] struct {
	transform func(T) R
}

func newAsyncWorkerCtor[T workerpool.TaskMayPanic, R any](transform func(T) R) func() workerpool.Worker[T, R] {
	return func() workerpool.Worker[T, R] {
		return &asyncWorker[T, R]{
			transform: transform,
		}
	}
}

func (s *asyncWorker[T, R]) HandleTask(task T, rsFn func(R)) {
	result := s.transform(task)
	rsFn(result)
}

func (*asyncWorker[T, R]) Close() {}

// Context is the context used for worker pool
type Context struct {
	context.Context
	cancel context.CancelFunc
	err    atomic.Pointer[error]
}

// OnError is called when an error occurs in the operator.
func (ctx *Context) OnError(err error) {
	tracedErr := errors.Trace(err)
	ctx.err.CompareAndSwap(nil, &tracedErr)
	ctx.cancel()
}

// OperatorErr returns the error of the operator.
func (ctx *Context) OperatorErr() error {
	err := ctx.err.Load()
	if err == nil {
		return nil
	}
	return *err
}

// Cancel cancels the context of the operator.
// It's used in test.
func (ctx *Context) Cancel() {
	if ctx.cancel != nil {
		ctx.cancel()
	}
}

// NewContext creates a new Context
func NewContext(
	ctx context.Context,
) (*Context, context.CancelFunc) {
	opCtx, cancel := context.WithCancel(ctx)
	return &Context{
		Context: opCtx,
		cancel:  cancel,
	}, cancel
}

// SimpleDataSource is a simple operator which use the given input slice as the data source.
type SimpleDataSource[T workerpool.TaskMayPanic] struct {
	inputs []T
	eg     *errgroup.Group
	ctx    *Context
	ch     chan T
}

// NewSimpleDataSource creates a new SimpleOperator with the given inputs.
func NewSimpleDataSource[T workerpool.TaskMayPanic](
	ctx *Context,
	inputs []T,
) *SimpleDataSource[T] {
	return &SimpleDataSource[T]{
		inputs: inputs,
		eg:     &errgroup.Group{},
		ctx:    ctx,
	}
}

// Open implements the Operator interface.
func (s *SimpleDataSource[T]) Open() error {
	s.eg.Go(func() error {
		// To make this part of code work, we need to call ctx.OnError
		// in downstream operators when they encounter an error or panic.
		for _, input := range s.inputs {
			select {
			case s.ch <- input:
			case <-s.ctx.Done():
				return nil
			}
		}

		return nil
	})
	return nil
}

// Close implements the Operator interface.
func (s *SimpleDataSource[T]) Close() error {
	//nolint: errcheck
	s.eg.Wait()
	close(s.ch)
	return s.ctx.OperatorErr()
}

// String implements the Operator interface.
func (*SimpleDataSource[T]) String() string {
	var zT T
	return fmt.Sprintf("SimpleDataSource[%T]", zT)
}

// SetSink implements the WithSink interface.
func (s *SimpleDataSource[T]) SetSink(ch DataChannel[T]) {
	s.ch = ch.Channel()
}
