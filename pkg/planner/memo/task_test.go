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
	"strconv"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

type TestTaskImpl struct {
	a int64
}

func (t *TestTaskImpl) execute() error {
	return nil
}
func (t *TestTaskImpl) desc() string {
	return strconv.Itoa(int(t.a))
}

func TestTaskStack(t *testing.T) {
	newSS := newTaskStack()
	// size of pointer to TaskStack{}
	require.Equal(t, int64(unsafe.Sizeof(newSS)), int64(8))
	// size of pointer to TaskStack.[]Task, cap + len + addr
	require.Equal(t, int64(unsafe.Sizeof(newSS.tasks)), int64(24))
	// size of pointer to TaskStack's first element Task[0]
	newSS.Push(nil)
	newSS.Push(&TestTaskImpl{a: 1})
	newSS.Push(nil)
	v := unsafe.Sizeof(newSS.tasks[0])
	require.Equal(t, int64(v), int64(16))
	v = unsafe.Sizeof(newSS.tasks[1])
	require.Equal(t, int64(v), int64(16))
}

func TestTaskFunctionality(t *testing.T) {
	taskTaskPool := TaskStackPool.Get()
	require.Equal(t, len(taskTaskPool.(*TaskStack).tasks), 0)
	require.Equal(t, cap(taskTaskPool.(*TaskStack).tasks), 4)
	taskStack := taskTaskPool.(*TaskStack)
	taskStack.Push(&TestTaskImpl{a: 1})
	taskStack.Push(&TestTaskImpl{a: 2})
	one := taskStack.Pop()
	require.Equal(t, one.desc(), "2")
	one = taskStack.Pop()
	require.Equal(t, one.desc(), "1")
	// empty, pop nil.
	one = taskStack.Pop()
	require.Nil(t, one)

	taskStack.Push(&TestTaskImpl{a: 3})
	taskStack.Push(&TestTaskImpl{a: 4})
	taskStack.Push(&TestTaskImpl{a: 5})
	taskStack.Push(&TestTaskImpl{a: 6})
	// no clean, put it back
	TaskStackPool.Put(taskTaskPool)

	// require again.
	taskTaskPool = TaskStackPool.Get()
	require.Equal(t, len(taskTaskPool.(*TaskStack).tasks), 4)
	require.Equal(t, cap(taskTaskPool.(*TaskStack).tasks), 4)
	// clean the stack
	one = taskStack.Pop()
	require.Equal(t, one.desc(), "6")
	one = taskStack.Pop()
	require.Equal(t, one.desc(), "5")
	one = taskStack.Pop()
	require.Equal(t, one.desc(), "4")
	one = taskStack.Pop()
	require.Equal(t, one.desc(), "3")
	one = taskStack.Pop()
	require.Nil(t, one)

	// self destroy.
	taskStack.Destroy()
}
