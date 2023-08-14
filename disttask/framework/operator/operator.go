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
)

// BaseOperator have DataSource and DataSink.
type BaseOperator struct {
	Source DataSource
	Sink   DataSink
}

// BaseAysncOperatorImpl defines the interface for each operator.
// BaseAysncOperatorImpl is the basic operation unit in the task execution.
// In each BaseAysncOperatorImpl, it will use a `workerpool` to run several workers.
type BaseAysncOperatorImpl interface {
	open() error
	close()
	display() string
}

type AsyncOperator struct {
	BaseOperator
	impl BaseAysncOperatorImpl
}

func (op *AsyncOperator) open() error {
	return op.impl.open()
}

func (op *AsyncOperator) close() {
	op.impl.close()
}

func (op *AsyncOperator) display() string {
	return op.impl.display()
}

// AsyncDataChannel can serve as DataSource and DataSink.
// Each AsyncOperator can use it to pass tasks.
//
//	Eg: op1 use AsyncDataChannel as sink, op2 use AsyncDataChannel as source.
//	    op1 call sink.Write, then op2's worker will handle the task.
type AsyncDataChannel[T any] struct {
	channel *workerpool.WorkerPool[T]
}

func (*AsyncDataChannel[T]) Start() error { return nil }

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

// NewAsyncOperator generate an AsyncOperator.
func NewAsyncOperator(impl BaseAysncOperatorImpl, source DataSource, sink DataSink) *AsyncOperator {
	res := &AsyncOperator{impl: impl}
	res.Source = source
	res.Sink = sink
	return res
}
