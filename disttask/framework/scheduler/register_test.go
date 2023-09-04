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

package scheduler

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/stretchr/testify/require"
)

func mockSchedulerOptionFunc(op *schedulerRegisterOptions) {}

func mockSchedulerConstructor(_ context.Context, _ int64, task []byte, step int64) (Scheduler, error) {
	return nil, nil
}

func mockSubtaskExecutorConstructor(minimalTask proto.MinimalTask, step int64) (SubtaskExecutor, error) {
	return nil, nil
}

func TestRegisterTaskType(t *testing.T) {
	// other case might add task types, so we need to clear it first
	ClearSchedulers()
	RegisterTaskType("test1")
	require.Len(t, taskTypes, 1)
	require.Equal(t, int32(0), taskTypes["test1"].PoolSize)
	RegisterTaskType("test2", WithPoolSize(10))
	require.Len(t, taskTypes, 2)
	require.Equal(t, int32(10), taskTypes["test2"].PoolSize)
	// register again
	RegisterTaskType("test2", WithPoolSize(123))
	require.Len(t, taskTypes, 2)
	require.Equal(t, int32(123), taskTypes["test2"].PoolSize)
}

func TestRegisterSchedulerConstructor(t *testing.T) {
	RegisterSchedulerConstructor("test1", proto.StepOne, nil)
	require.Len(t, schedulerConstructors, 1)
	require.Len(t, schedulerOptions, 1)
	RegisterSchedulerConstructor("test2", proto.StepOne, mockSchedulerConstructor)
	require.Len(t, schedulerConstructors, 2)
	require.Len(t, schedulerOptions, 2)

	RegisterSchedulerConstructor("test3", proto.StepOne, mockSchedulerConstructor, mockSchedulerOptionFunc)
	require.Len(t, schedulerConstructors, 3)
	require.Len(t, schedulerOptions, 3)
	RegisterSchedulerConstructor("test4", proto.StepOne, mockSchedulerConstructor, mockSchedulerOptionFunc, mockSchedulerOptionFunc, mockSchedulerOptionFunc)
	require.Len(t, schedulerConstructors, 4)
	require.Len(t, schedulerOptions, 4)
	// register again
	RegisterSchedulerConstructor("test4", proto.StepOne, mockSchedulerConstructor, mockSchedulerOptionFunc, mockSchedulerOptionFunc, mockSchedulerOptionFunc)
	require.Len(t, schedulerConstructors, 4)
	require.Len(t, schedulerOptions, 4)
	// register with different step
	RegisterSchedulerConstructor("test4", proto.StepTwo, mockSchedulerConstructor, mockSchedulerOptionFunc, mockSchedulerOptionFunc, mockSchedulerOptionFunc)
	require.Len(t, schedulerConstructors, 5)
	require.Len(t, schedulerOptions, 5)
}

func TestRegisterSubtaskExectorConstructor(t *testing.T) {
	RegisterSubtaskExectorConstructor("test1", proto.StepOne, nil)
	require.Contains(t, subtaskExecutorConstructors, getKey("test1", proto.StepOne))
	require.Contains(t, subtaskExecutorOptions, getKey("test1", proto.StepOne))
	RegisterSubtaskExectorConstructor("test2", proto.StepOne, mockSubtaskExecutorConstructor)
	require.Contains(t, subtaskExecutorConstructors, getKey("test2", proto.StepOne))
	require.Contains(t, subtaskExecutorOptions, getKey("test2", proto.StepOne))

	RegisterSubtaskExectorConstructor("test3", proto.StepOne, mockSubtaskExecutorConstructor)
	require.Contains(t, subtaskExecutorConstructors, getKey("test3", proto.StepOne))
	require.Contains(t, subtaskExecutorOptions, getKey("test3", proto.StepOne))
	RegisterSubtaskExectorConstructor("test4", proto.StepOne, mockSubtaskExecutorConstructor)
	require.Contains(t, subtaskExecutorConstructors, getKey("test4", proto.StepOne))
	require.Contains(t, subtaskExecutorOptions, getKey("test4", proto.StepOne))

	RegisterSubtaskExectorConstructor("test4", proto.StepTwo, mockSubtaskExecutorConstructor)
	require.Contains(t, subtaskExecutorConstructors, getKey("test4", proto.StepTwo))
	require.Contains(t, subtaskExecutorOptions, getKey("test4", proto.StepTwo))
}
