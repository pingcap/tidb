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
	"fmt"
	"sync"

	"github.com/pingcap/tidb/resourcemanager/pool/workerpool"
)

// Operator is the basic operation unit in the task execution.
type Operator interface {
	Open() error
	Close() error
	Display() string
}

// AsyncOperator process the data in async way.
//
//	Eg: The sink of AsyncOperator op1 and the source of op2
//  	use the same channel, Then op2's worker will handle
//   	the result from op1.
type AsyncOperator[T, R any] struct {
	wg   *sync.WaitGroup
	pool *workerpool.WorkerPool[T, R]
}

// Open implements the Operator's Open interface.
func (c *AsyncOperator[T, R]) Open() error {
	c.pool.Start()
	return nil
}

// Close implements the Operator's Close interface.
func (c *AsyncOperator[T, R]) Close() error {
	c.pool.ReleaseAndWait()
	return nil
}

// Display show the name.
func (*AsyncOperator[T, R]) Display() string {
	var zT T
	var zR R
	return fmt.Sprintf("AsyncOperator[%T, %T]", zT, zR)
}

// SetSource set the source channel.
func (c *AsyncOperator[T, R]) SetSource(ch DataChannel[T]) {
	c.pool.SetTaskReceiver(ch.Channel())
}

// SetSink set the sink channel.
func (c *AsyncOperator[T, R]) SetSink(ch DataChannel[R]) {
	c.pool.SetResultSender(ch.Channel())
}
