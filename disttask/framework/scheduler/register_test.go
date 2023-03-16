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
	"testing"

	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/stretchr/testify/require"
)

func mockSchedulerOptionFunc(op *schedulerRegisterOptions) {}

func mockSchedulerConstructor(task []byte, step int64) (Scheduler, error) {
	return nil, nil
}

func mockSubtaskExectorOptionFunc(op *subtaskExecutorRegisterOptions) {
	op.PoolSize = 1
}

func mockSubtaskExectorConstructor(minimalTask proto.MinimalTask, step int64) (SubtaskExecutor, error) {
	return nil, nil
}

func TestRegisterSchedulerConstructor(t *testing.T) {
	RegisterSchedulerConstructor("test1", nil)
	require.Len(t, schedulerConstructors, 1)
	require.Len(t, schedulerOptions, 1)
	RegisterSchedulerConstructor("test2", mockSchedulerConstructor)
	require.Len(t, schedulerConstructors, 2)
	require.Len(t, schedulerOptions, 2)

	RegisterSchedulerConstructor("test3", mockSchedulerConstructor, mockSchedulerOptionFunc)
	require.Len(t, schedulerConstructors, 3)
	require.Len(t, schedulerOptions, 3)
	RegisterSchedulerConstructor("test4", mockSchedulerConstructor, mockSchedulerOptionFunc, mockSchedulerOptionFunc, mockSchedulerOptionFunc)
	require.Len(t, schedulerConstructors, 4)
	require.Len(t, schedulerOptions, 4)

	RegisterSchedulerConstructor("test4", mockSchedulerConstructor, mockSchedulerOptionFunc, mockSchedulerOptionFunc, mockSchedulerOptionFunc)
	require.Len(t, schedulerConstructors, 4)
	require.Len(t, schedulerOptions, 4)
}

func TestRegisterSubtaskExectorConstructor(t *testing.T) {
	RegisterSubtaskExectorConstructor("test1", nil)
	require.Contains(t, subtaskExecutorConstructors, "test1")
	require.Contains(t, subtaskExecutorOptions, "test1")
	require.Equal(t, subtaskExecutorOptions["test1"].PoolSize, int32(0))
	RegisterSubtaskExectorConstructor("test2", mockSubtaskExectorConstructor)
	require.Contains(t, subtaskExecutorConstructors, "test2")
	require.Contains(t, subtaskExecutorOptions, "test2")
	require.Equal(t, subtaskExecutorOptions["test2"].PoolSize, int32(0))

	RegisterSubtaskExectorConstructor("test3", mockSubtaskExectorConstructor, mockSubtaskExectorOptionFunc)
	require.Contains(t, subtaskExecutorConstructors, "test3")
	require.Contains(t, subtaskExecutorOptions, "test3")
	require.Equal(t, subtaskExecutorOptions["test3"].PoolSize, int32(1))
	RegisterSubtaskExectorConstructor("test4", mockSubtaskExectorConstructor, mockSubtaskExectorOptionFunc, mockSubtaskExectorOptionFunc, mockSubtaskExectorOptionFunc)
	require.Contains(t, subtaskExecutorConstructors, "test4")
	require.Contains(t, subtaskExecutorOptions, "test4")
	require.Equal(t, subtaskExecutorOptions["test4"].PoolSize, int32(1))

	RegisterSubtaskExectorConstructor("test4", mockSubtaskExectorConstructor, mockSubtaskExectorOptionFunc, mockSubtaskExectorOptionFunc, mockSubtaskExectorOptionFunc)
	require.Contains(t, subtaskExecutorConstructors, "test4")
	require.Contains(t, subtaskExecutorOptions, "test4")
	require.Equal(t, subtaskExecutorOptions["test4"].PoolSize, int32(1))
}
