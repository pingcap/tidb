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

var _ Scheduler = &SimpleTaskScheduler{}

// Scheduler is a scheduling interface defined for serializing(single thread)/concurrent(multi thread) running.
type Scheduler interface {
	ExecuteTasks()
}

// SimpleTaskScheduler is defined for serializing scheduling of memo tasks.
type SimpleTaskScheduler struct {
	Err          error
	SchedulerCtx SchedulerContext
}

// ExecuteTasks implements the interface of TaskScheduler.
func (s *SimpleTaskScheduler) ExecuteTasks() {
	stack := s.SchedulerCtx.getStack()
	defer func() {
		// when step out of the scheduler, if the stack is empty, clean and release it.
		if !stack.Empty() {
			stack.Destroy()
		}
	}()
	for !stack.Empty() {
		// when use customized stack to drive the tasks, the call-chain state is dived in the stack.
		task := stack.Pop()
		if err := task.execute(); err != nil {
			s.Err = err
			return
		}
	}
}

// SchedulerContext is defined for scheduling logic calling, also facilitate interface-oriented coding and testing.
type SchedulerContext interface {
	// we exported the Stack interface here rather than the basic stack implementation.
	getStack() Stack
	// we exported the only one push action to user, Task is an interface definition.
	pushTask(task Task)
}
