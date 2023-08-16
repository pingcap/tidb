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
type BaseOperator[T any, U any] struct {
	Source DataSource[T]
	Sink   DataSink[U]
}

// AsyncOperator defines the interface for each operator.
// AsyncOperator is the basic operation unit in the task execution.
// In each AsyncOperator, it will use a `workerpool` to run several workers.
type AsyncOperator interface {
	Open() error
	Close()
	Display() string
}

// AsyncDataChannel can serve as DataSource and DataSink.
// Each AsyncOperator can use it to pass tasks.
//
//	Eg: op1 use AsyncDataChannel as sink, op2 use AsyncDataChannel as source.
//	    op1 call sink.Write, then op2's worker will handle the task.
type AsyncDataChannel[T any] struct {
	Channel *workerpool.WorkerPool[T]
}

// Start implement the DataSource Start.
func (*AsyncDataChannel[T]) Start() error { return nil }

// Next read data from source. Not used.
func (*AsyncDataChannel[T]) Next() (T, error) {
	var res T
	return res, nil
}

// Display show the name.
func (*AsyncDataChannel[T]) Display() string { return "AsyncDataChannel" }

// Write data to sink.
func (c *AsyncDataChannel[T]) Write(data T) error {
	c.Channel.AddTask(data)
	return nil
}
