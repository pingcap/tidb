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

import (
	"io"
	"sync"

	"github.com/pingcap/tidb/pkg/planner/cascades/base"
)

// stackPool is initialized for memory saving by reusing TaskStack.
var stackPool = &sync.Pool{
	New: func() any {
		return newTaskStack()
	},
}

// TaskStack is used to store the optimizing tasks created before or during the optimizing process.
type TaskStack struct {
	tasks []base.Task
}

func newTaskStack() *TaskStack {
	return &TaskStack{
		tasks: make([]base.Task, 0, 4),
	}
}

// Destroy indicates that when stack itself is useless like in the end of optimizing phase, we can destroy ourselves.
func (ts *TaskStack) Destroy() {
	// when a TaskStack itself is useless, we can destroy itself actively.
	clear(ts.tasks)
	stackPool.Put(ts)
}

// Desc is used to desc the detail info about current stack state.
// when use customized stack to drive the tasks, the call-chain state is dived in the stack.
func (ts *TaskStack) Desc(w io.StringWriter) {
	for _, one := range ts.tasks {
		one.Desc(w)
		// nolint:errcheck
		w.WriteString("\n")
	}
}

// Len indicates the length of current stack.
func (ts *TaskStack) Len() int {
	return len(ts.tasks)
}

// Pop indicates to pop one task out of the stack.
func (ts *TaskStack) Pop() base.Task {
	if !ts.Empty() {
		tmp := ts.tasks[len(ts.tasks)-1]
		ts.tasks = ts.tasks[:len(ts.tasks)-1]
		return tmp
	}
	return nil
}

// Push indicates to push one task into the stack.
func (ts *TaskStack) Push(one base.Task) {
	ts.tasks = append(ts.tasks, one)
}

// Empty indicates whether TaskStack is empty.
func (ts *TaskStack) Empty() bool {
	return ts.Len() == 0
}

// BenchTest required.
func newTaskStackWithCap(c int) *TaskStack {
	return &TaskStack{
		tasks: make([]base.Task, 0, c),
	}
}
