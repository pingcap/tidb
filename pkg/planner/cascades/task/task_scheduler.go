// Copyright 2024 PingCAP, Inc.
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

package task

import "github.com/pingcap/tidb/pkg/planner/cascades/base"

var _ base.Scheduler = &SimpleTaskScheduler{}

// SimpleTaskScheduler is defined for serializing scheduling of memo tasks.
type SimpleTaskScheduler struct {
	stack base.Stack
}

// ExecuteTasks implements the interface of TaskScheduler.
func (s *SimpleTaskScheduler) ExecuteTasks() error {
	for !s.stack.Empty() {
		// when use customized stack to drive the tasks, the call-chain state is dived in the stack.
		task := s.stack.Pop()
		if err := task.Execute(); err != nil {
			return err
		}
	}
	return nil
}

// Destroy release all the allocated elements inside stack.
func (s *SimpleTaskScheduler) Destroy() {
	// when step out of the scheduler, if the stack is empty, clean and release it.
	stack := s.stack
	// release parent pointer ref.
	s.stack = nil
	stack.Destroy()
}

// PushTask implements the scheduler's interface, add another task into scheduler.
func (s *SimpleTaskScheduler) PushTask(task base.Task) {
	s.stack.Push(task)
}

// NewSimpleTaskScheduler return a simple task scheduler, init logic included.
func NewSimpleTaskScheduler() base.Scheduler {
	return &SimpleTaskScheduler{
		stack: stackPool.Get().(base.Stack),
	}
}
