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
	"github.com/pingcap/tidb/resourcemanager/pool/workerpool"
	poolutil "github.com/pingcap/tidb/resourcemanager/util"
)

// AsyncOperator provide basic getter setter for each operator.
type AsyncOperator[T any] struct {
	source DataSource
	sink   DataSink
	pool   *workerpool.WorkerPool[T] // workers running on pool
}

func (op *AsyncOperator[T]) setSink(sink DataSink) {
	op.sink = sink
}

func (op *AsyncOperator[T]) getSink() DataSink {
	return op.sink
}

func (op *AsyncOperator[T]) setSource(source DataSource) {
	op.source = source
}

func (op *AsyncOperator[T]) getSource() DataSource {
	return op.source
}

func (op *AsyncOperator[T]) setPool(pool any) {
	op.pool = pool.(*workerpool.WorkerPool[T])
}

func (op *AsyncOperator[T]) getPool() any {
	return op.pool
}

// AsyncOperatorImpl defines the interface for each operator.
// AsyncOperatorImpl is the basic operation unit in the task execution.
// In each AsyncOperatorImpl, it will use a `workerpool` to run several workers.
type AsyncOperatorImpl interface {
	setSink(sink DataSink)
	getSink() DataSink
	setSource(source DataSource)
	getSource() DataSource
	setPool(pool any)
	getPool() any
	addTask(data any) // addTask into workerpool, then workers can run the task.
	preExecute() error
	postExecute() error
	start()
	wait()
	display() string
}

// NewAsyncOperatorImpl init an impl which use AsyncDataChannel as the DataSource.
func NewAsyncOperatorImpl[T any](
	name string,
	newImpl func() AsyncOperatorImpl,
	component poolutil.Component,
	workerCnt int) AsyncOperatorImpl {
	res := newImpl()
	pool, _ := workerpool.NewWorkerPoolWithoutCreateWorker[T](name, component, workerCnt)
	res.setPool(pool)
	res.setSource(&AsyncDataChannel[T]{pool})
	return res
}

// AsyncDataChannel can serve as DataSource and DataSink.
// Each AsyncOperator can use it to pass tasks.
//
//	Eg: op1 use AsyncDataChannel as sink, op2 use AsyncDataChannel as source.
//	    op1 call sink.Write, then op2's worker will handle the task.
type AsyncDataChannel[T any] struct {
	channel *workerpool.WorkerPool[T]
}

// HasNext check if it has next data.
func (*AsyncDataChannel[T]) HasNext() bool { return false }

// Read data from source.
func (*AsyncDataChannel[T]) Read() (any, error) { return nil, nil }

// Display show the name.
func (*AsyncDataChannel[T]) Display() string { return "AsyncDataChannel" }

// IsFull check if it is full.
func (*AsyncDataChannel[T]) IsFull() bool { return false }

// Write data to sink.
func (c *AsyncDataChannel[T]) Write(data any) error {
	c.channel.AddTask(data.(T))
	return nil
}
