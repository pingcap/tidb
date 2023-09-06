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

	"github.com/stretchr/testify/require"
)

func TestRegisterTaskType(t *testing.T) {
	// other case might add task types, so we need to clear it first
	ClearSchedulers()
	factoryFn := func(ctx context.Context, id string, taskID int64, taskTable TaskTable, pool Pool) Scheduler {
		return nil
	}
	RegisterTaskType("test1", factoryFn)
	require.Len(t, taskTypes, 1)
	require.Len(t, taskSchedulerFactories, 1)
	require.Equal(t, int32(0), taskTypes["test1"].PoolSize)
	RegisterTaskType("test2", factoryFn, WithPoolSize(10))
	require.Len(t, taskTypes, 2)
	require.Len(t, taskSchedulerFactories, 2)
	require.Equal(t, int32(10), taskTypes["test2"].PoolSize)
	// register again
	RegisterTaskType("test2", factoryFn, WithPoolSize(123))
	require.Len(t, taskTypes, 2)
	require.Len(t, taskSchedulerFactories, 2)
	require.Equal(t, int32(123), taskTypes["test2"].PoolSize)
}
