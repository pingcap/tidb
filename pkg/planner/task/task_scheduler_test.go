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
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSchedulerContext is defined to test scheduling logic here.
type TestSchedulerContext struct {
	ts *taskStack
}

func (t *TestSchedulerContext) getStack() Stack {
	return t.ts
}

func (t *TestSchedulerContext) pushTask(task Task) {
	t.ts.Push(task)
}

// TestSchedulerContext is defined to mock special error state in specified task.
type TestTaskImpl2 struct {
	a int64
}

func (t *TestTaskImpl2) execute() error {
	// mock error at special task
	if t.a == 2 {
		return errors.New("mock error at task id = 2")
	}
	return nil
}

func (t *TestTaskImpl2) desc() string {
	return strconv.Itoa(int(t.a))
}

func TestSimpleTaskScheduler(t *testing.T) {
	testSchedulerContext := &TestSchedulerContext{
		newTaskStack(),
	}
	testScheduler := &SimpleTaskScheduler{
		SchedulerCtx: testSchedulerContext,
	}
	testScheduler.SchedulerCtx.pushTask(&TestTaskImpl2{a: 1})
	testScheduler.SchedulerCtx.pushTask(&TestTaskImpl2{a: 2})
	testScheduler.SchedulerCtx.pushTask(&TestTaskImpl2{a: 3})

	var testTaskScheduler Scheduler = testScheduler
	testTaskScheduler.ExecuteTasks()
	require.NotNil(t, testScheduler.Err)
	require.Equal(t, testScheduler.Err.Error(), "mock error at task id = 2")
}
