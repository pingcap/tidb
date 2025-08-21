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
	"bytes"
	"strconv"
	"testing"
	"unsafe"

	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/cascades/util"
	"github.com/stretchr/testify/require"
)

type TestTaskImpl struct {
	a int64
}

func (t *TestTaskImpl) Execute() error {
	return nil
}
func (t *TestTaskImpl) Desc(w util.StrBufferWriter) {
	w.WriteString(strconv.Itoa(int(t.a)))
}

func TestTaskStack(t *testing.T) {
	newSS := newTaskStack()
	// size of pointer to Stack{}
	require.Equal(t, int64(unsafe.Sizeof(newSS)), int64(8))
	// size of pointer to Stack.[]Task, cap + len + addr
	require.Equal(t, int64(unsafe.Sizeof(newSS.tasks)), int64(24))
	// size of pointer to Stack's first element Task[0]
	newSS.Push(nil)
	newSS.Push(&TestTaskImpl{a: 1})
	newSS.Push(nil)
	v := unsafe.Sizeof(newSS.tasks[0])
	require.Equal(t, int64(v), int64(16))
	v = unsafe.Sizeof(newSS.tasks[1])
	require.Equal(t, int64(v), int64(16))
}

func TestTaskFunctionality(t *testing.T) {
	taskTaskPool := stackPool.Get()
	require.Equal(t, len(taskTaskPool.(*Stack).tasks), 0)
	require.Equal(t, cap(taskTaskPool.(*Stack).tasks), 4)
	ts := taskTaskPool.(*Stack)
	ts.Push(&TestTaskImpl{a: 1})
	ts.Push(&TestTaskImpl{a: 2})
	one := ts.Pop()
	buf := &bytes.Buffer{}
	w := util.NewStrBuffer(buf)
	one.Desc(w)
	w.Flush()
	require.Equal(t, buf.String(), "2")
	one = ts.Pop()
	buf.Reset()
	one.Desc(w)
	w.Flush()
	require.Equal(t, buf.String(), "1")
	// empty, pop nil.
	one = ts.Pop()
	require.Nil(t, one)

	ts.Push(&TestTaskImpl{a: 3})
	ts.Push(&TestTaskImpl{a: 4})
	ts.Push(&TestTaskImpl{a: 5})
	ts.Push(&TestTaskImpl{a: 6})
	// no clean, put it back
	stackPool.Put(taskTaskPool)

	// require again.
	ts = stackPool.Get().(*Stack)
	require.Equal(t, len(ts.tasks), 4)
	require.Equal(t, cap(ts.tasks), 4)
	// clean the stack
	one = ts.Pop()
	buf.Reset()
	one.Desc(w)
	w.Flush()
	require.Equal(t, buf.String(), "6")
	one = ts.Pop()
	buf.Reset()
	one.Desc(w)
	w.Flush()
	require.Equal(t, buf.String(), "5")
	one = ts.Pop()
	buf.Reset()
	one.Desc(w)
	w.Flush()
	require.Equal(t, buf.String(), "4")
	one = ts.Pop()
	buf.Reset()
	one.Desc(w)
	w.Flush()
	require.Equal(t, buf.String(), "3")
	one = ts.Pop()
	require.Nil(t, one)

	// self destroy.
	ts.Destroy()
	ts = stackPool.Get().(*Stack)
	require.Equal(t, len(ts.tasks), 0)
	require.Equal(t, cap(ts.tasks), 4)
}

// TaskStack2 is used to store the optimizing tasks created before or during the optimizing process.
type taskStackForBench struct {
	tasks []base.Task
}

func newTaskStackForBenchWithCap(c int) *taskStackForBench {
	return &taskStackForBench{
		tasks: make([]base.Task, 0, c),
	}
}

// Push indicates to push one task into the stack.
func (ts *taskStackForBench) Push(one base.Task) {
	ts.tasks = append(ts.tasks, one)
}

// Len indicates the length of current stack.
func (ts *taskStackForBench) Len() int {
	return len(ts.tasks)
}

// Empty indicates whether taskStack is empty.
func (ts *taskStackForBench) Empty() bool {
	return ts.Len() == 0
}

// Pop indicates to pop one task out of the stack.
func (ts *taskStackForBench) Pop() base.Task {
	if !ts.Empty() {
		tmp := ts.tasks[len(ts.tasks)-1]
		ts.tasks = ts.tasks[:len(ts.tasks)-1]
		return tmp
	}
	return nil
}

// Benchmark result explanation:
// On the right side of the function name, you have four values, 43803,27569 ns/op,24000 B/op and 2000 allocs/op
// The former indicates the total number of times the loop was executed, while the latter is the average amount
// of time each iteration took to complete, expressed in nanoseconds per operation. The third is the costed Byte
// of each op, the last one is number of allocs of each op.

// BenchmarkTestStack2Pointer-8   	   43802	     27569 ns/op	   24000 B/op	    2000 allocs/op
// BenchmarkTestStack2Pointer-8   	   42889	     27017 ns/op	   24000 B/op	    2000 allocs/op
// BenchmarkTestStack2Pointer-8   	   43009	     27524 ns/op	   24000 B/op	    2000 allocs/op
func BenchmarkTestStack2Pointer(b *testing.B) {
	stack := newTaskStackForBenchWithCap(1000)
	fill := func() {
		for idx := int64(0); idx < 1000; idx++ {
			stack.Push(&TestTaskImpl{a: idx})
		}
		for idx := int64(0); idx < 1000; idx++ {
			stack.Pop()
		}
	}
	for i := 0; i < b.N; i++ {
		fill()
	}
}

// BenchmarkTestStackInterface-8   	  108644	     10736 ns/op	    8000 B/op	    1000 allocs/op
// BenchmarkTestStackInterface-8   	  110587	     10756 ns/op	    8000 B/op	    1000 allocs/op
// BenchmarkTestStackInterface-8   	  109136	     10850 ns/op	    8000 B/op	    1000 allocs/op
func BenchmarkTestStackInterface(b *testing.B) {
	stack := newTaskStackWithCap(1000)
	fill := func() {
		for idx := int64(0); idx < 1000; idx++ {
			stack.Push(&TestTaskImpl{a: idx})
		}
		for idx := int64(0); idx < 1000; idx++ {
			stack.Pop()
		}
	}
	for i := 0; i < b.N; i++ {
		fill()
	}
}
