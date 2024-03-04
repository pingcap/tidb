// Copyright 2023 PingCAP, Inc.
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

package proto

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTaskStep(t *testing.T) {
	// make sure we don't change the value of StepInit accidentally
	require.Equal(t, int64(-1), int64(StepInit))
	require.Equal(t, int64(-2), int64(StepDone))
}

func TestTaskIsDone(t *testing.T) {
	cases := []struct {
		state TaskState
		done  bool
	}{
		{TaskStatePending, false},
		{TaskStateRunning, false},
		{TaskStateSucceed, true},
		{TaskStateReverting, false},
		{TaskStateFailed, true},
		{TaskStateCancelling, false},
		{TaskStatePausing, false},
		{TaskStatePaused, false},
		{TaskStateReverted, true},
	}
	for _, c := range cases {
		require.Equal(t, c.done, (&Task{TaskBase: TaskBase{State: c.state}}).IsDone())
	}
}

func TestTaskCompare(t *testing.T) {
	taskA := Task{TaskBase: TaskBase{
		ID:         100,
		Priority:   NormalPriority,
		CreateTime: time.Date(2023, time.December, 5, 15, 53, 30, 0, time.UTC),
	}}
	taskB := taskA
	require.Equal(t, 0, taskA.CompareTask(&taskB))
	taskB.Priority = 100
	require.Greater(t, taskA.CompareTask(&taskB), 0)
	taskB.Priority = taskA.Priority + 100
	require.Less(t, taskA.CompareTask(&taskB), 0)

	taskB.Priority = taskA.Priority
	taskB.CreateTime = time.Date(2023, time.December, 5, 15, 53, 10, 0, time.UTC)
	require.Greater(t, taskA.CompareTask(&taskB), 0)
	taskB.CreateTime = time.Date(2023, time.December, 5, 15, 53, 40, 0, time.UTC)
	require.Less(t, taskA.CompareTask(&taskB), 0)

	taskB.CreateTime = taskA.CreateTime
	taskB.ID = taskA.ID - 10
	require.Greater(t, taskA.CompareTask(&taskB), 0)
	taskB.ID = taskA.ID + 10
	require.Less(t, taskA.CompareTask(&taskB), 0)
}
