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

	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
)

// Operator is the basic operation unit in the task execution.
type Operator interface {
	Open() error
	// Close wait task done and close the operator.
	// TODO: the wait part should be separated from the close part.
	Close() error
	String() string
}

// AsyncOperator process the data in async way.
//
// Eg: The sink of AsyncOperator op1 and the source of op2
// use the same channel, Then op2's worker will handle
// the result from op1.
type AsyncOperator[T, R any] struct {
	ctx  context.Context
	pool *workerpool.WorkerPool[T, R]
}

// NewAsyncOperatorWithTransform create an AsyncOperator with a transform function.
func NewAsyncOperatorWithTransform[T, R any](
	ctx context.Context,
	name string,
	workerNum int,
	transform func(T) R,
) *AsyncOperator[T, R] {
	pool := workerpool.NewWorkerPool(name, util.DistTask, workerNum, newAsyncWorkerCtor(transform))
	return NewAsyncOperator(ctx, pool)
}

// NewAsyncOperator create an AsyncOperator.
func NewAsyncOperator[T, R any](ctx context.Context, pool *workerpool.WorkerPool[T, R]) *AsyncOperator[T, R] {
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

type asyncWorker[T, R any] struct {
	transform func(T) R
}

func newAsyncWorkerCtor[T, R any](transform func(T) R) func() workerpool.Worker[T, R] {
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
