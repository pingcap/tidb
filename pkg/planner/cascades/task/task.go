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
	"sync"

	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/cascades/util"
)

// stackPool is initialized for memory saving by reusing taskStack.
var stackPool = sync.Pool{
	New: func() any {
		return newTaskStack()
	},
}

// Stack is used to store the optimizing tasks created before or during the optimizing process.
type Stack struct {
	tasks []base.Task
}

func newTaskStack() *Stack {
	return &Stack{
		tasks: make([]base.Task, 0, 4),
	}
}

// Destroy indicates that when stack itself is useless like in the end of optimizing phase, we can destroy ourselves.
func (ts *Stack) Destroy() {
	// when a taskStack itself is useless, we can destroy itself actively.
	clear(ts.tasks)
	stackPool.Put(ts)
}

// Desc is used to desc the detail info about current stack state.
// when use customized stack to drive the tasks, the call-chain state is dived in the stack.
func (ts *Stack) Desc(w util.StrBufferWriter) {
	for _, one := range ts.tasks {
		one.Desc(w)
		w.WriteString("\n")
	}
}

// Len indicates the length of current stack.
func (ts *Stack) Len() int {
	return len(ts.tasks)
}

// Pop indicates to pop one task out of the stack.
func (ts *Stack) Pop() base.Task {
	if !ts.Empty() {
		tmp := ts.tasks[len(ts.tasks)-1]
		ts.tasks = ts.tasks[:len(ts.tasks)-1]
		return tmp
	}
	return nil
}

// Push indicates to push one task into the stack.
func (ts *Stack) Push(one base.Task) {
	ts.tasks = append(ts.tasks, one)
}

// Empty indicates whether taskStack is empty.
func (ts *Stack) Empty() bool {
	return ts.Len() == 0
}

// BenchTest required.
func newTaskStackWithCap(c int) *Stack {
	return &Stack{
		tasks: make([]base.Task, 0, c),
	}
}
