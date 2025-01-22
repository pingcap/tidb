// Copyright 2025 PingCAP, Inc.
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

package example

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestExampleApplication(t *testing.T) {
	scheduler.RegisterSchedulerFactory(proto.TaskTypeExample,
		func(ctx context.Context, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
			return newScheduler(ctx, task, param)
		},
	)
	scheduler.RegisterSchedulerCleanUpFactory(proto.TaskTypeExample, func() scheduler.CleanUpRoutine {
		return &postCleanupImpl{}
	})

	taskexecutor.RegisterTaskType(proto.TaskTypeExample,
		func(ctx context.Context, task *proto.Task, param taskexecutor.Param) taskexecutor.TaskExecutor {
			return newTaskExecutor(ctx, task, param)
		},
	)

	_ = testkit.CreateMockStore(t)
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "scheduler_manager")
	meta := &taskMeta{
		SubtaskCount: 3,
	}
	bytes, err := json.Marshal(meta)
	require.NoError(t, err)
	task, err := handle.SubmitTask(ctx, "test", proto.TaskTypeExample, 1, "", bytes)
	require.NoError(t, err)
	require.NoError(t, handle.WaitTaskDoneByKey(ctx, task.Key))
}
