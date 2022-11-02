// Copyright 2022 PingCAP, Inc.
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

package spmc

import (
	"sync"

	"github.com/pingcap/tidb/util/gpool"
)

// TaskController is a controller that can control or watch the pool.
type TaskController[T any, U any, C any, CT any, TF gpool.Context[CT]] struct {
	pool   *Pool[T, U, C, CT, TF]
	close  chan struct{}
	wg     *sync.WaitGroup
	taskID uint64
}

// NewTaskController create a controller to deal with task's statue.
func NewTaskController[T any, U any, C any, CT any, TF gpool.Context[CT]](p *Pool[T, U, C, CT, TF], taskID uint64, closeCh chan struct{}, wg *sync.WaitGroup) TaskController[T, U, C, CT, TF] {
	return TaskController[T, U, C, CT, TF]{
		pool:   p,
		taskID: taskID,
		close:  closeCh,
		wg:     wg,
	}
}

// Wait is to wait the task to stop.
func (t *TaskController[T, U, C, CT, TF]) Wait() {
	<-t.close
	t.wg.Wait()
	t.pool.taskManager.DeleteTask(t.taskID)
}

// IsProduceClose is to judge whether the producer is completed.
func (t *TaskController[T, U, C, CT, TF]) IsProduceClose() bool {
	select {
	case <-t.close:
		return true
	default:
	}
	return false
}
