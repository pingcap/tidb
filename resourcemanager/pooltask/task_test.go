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

package pooltask

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTaskManager(t *testing.T) {
	size := 32
	taskConcurrency := 8
	tm := NewTaskManager[int, int, int, any, NilContext](int32(size))
	tm.RegisterTask(1, int32(taskConcurrency))
	for i := 0; i < taskConcurrency; i++ {
		tid := NewTaskBox[int, int, int, any, NilContext](1, NilContext{}, &sync.WaitGroup{}, make(chan Task[int]), make(chan int), 1)
		tm.AddSubTask(1, &tid)
	}
	for i := 0; i < taskConcurrency; i++ {
		tm.ExitSubTask(1)
	}
	require.Equal(t, int32(0), tm.Running(1))
	require.True(t, tm.hasTask(1))
	tm.DeleteTask(1)
	require.False(t, tm.hasTask(1))
}
