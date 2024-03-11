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

package memo

import (
	"sync"
)

// Task is an interface defined for all type of optimizing work: exploring, implementing, deriving-stats, join-reordering and so on.
type Task interface {
	// task self executing logic
	execute() error
	// task self description string.
	desc() string
}

// TaskStackPool is initialized for memory saving by reusing taskStack.
var TaskStackPool = sync.Pool{
	New: func() any {
		return newTaskStack()
	},
}

// TaskStack is used to store the optimizing tasks created before or during the optimizing process.
type TaskStack struct {
	tasks []Task
}

func newTaskStack() *TaskStack {
	return &TaskStack{
		tasks: make([]Task, 0, 4),
	}
}

// Destroy indicates that when stack itself is useless like in the end of optimizing phase, we can destroy ourselves.
func (ts *TaskStack) Destroy() {
	// when a taskStack itself is useless, we can destroy itself actively.
	TaskStackPool.Put(ts)
}

// Len indicates the length of current stack.
func (ts *TaskStack) Len() int {
	return len(ts.tasks)
}

// Pop indicates to pop one task out of the stack.
func (ts *TaskStack) Pop() Task {
	if !ts.Empty() {
		tmp := ts.tasks[len(ts.tasks)-1]
		ts.tasks = ts.tasks[:len(ts.tasks)-1]
		return tmp
	}
	return nil
}

// Push indicates to push one task into the stack.
func (ts *TaskStack) Push(one Task) {
	ts.tasks = append(ts.tasks, one)
}

// Empty indicates whether taskStack is empty.
func (ts *TaskStack) Empty() bool {
	return ts.Len() == 0
}
