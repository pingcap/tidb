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

package taskexecutor

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/stretchr/testify/require"
)

func TestRegisterTaskType(t *testing.T) {
	// other case might add task types, so we need to clear it first
	ClearTaskExecutors()
	factoryFn := func(ctx context.Context, id string, task *proto.Task, taskTable TaskTable) TaskExecutor {
		return nil
	}
	RegisterTaskType("test1", factoryFn)
	require.Len(t, taskTypes, 1)
	require.Len(t, taskExecutorFactories, 1)
	RegisterTaskType("test2", factoryFn)
	require.Len(t, taskTypes, 2)
	require.Len(t, taskExecutorFactories, 2)
	// register again
	RegisterTaskType("test2", factoryFn)
	require.Len(t, taskTypes, 2)
	require.Len(t, taskExecutorFactories, 2)
}
