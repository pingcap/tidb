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
)

type taskBox[T any, U any, C any] struct {
	constArgs *C
	wg        *sync.WaitGroup
	task      chan T
	resultCh  chan U
}

// TaskController is a controller that can control or watch the pool.
type TaskController struct {
	close chan struct{}
	wg    *sync.WaitGroup
}

// NewTaskController create a controller to deal with task's statue.
func NewTaskController(closeCh chan struct{}, wg *sync.WaitGroup) TaskController {
	return TaskController{
		close: closeCh, wg: wg,
	}
}

// Wait is to wait the task to stop.
func (c *TaskController) Wait() {
	for {
		if c.IsProduceClose() {
			break
		}
	}
	c.wg.Wait()
}

// IsProduceClose is to judge whether the producer is completed.
func (c *TaskController) IsProduceClose() bool {
	select {
	case <-c.close:
		return true
	default:
	}
	return false
}
