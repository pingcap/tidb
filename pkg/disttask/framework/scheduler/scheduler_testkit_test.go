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

package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	mockexecute "github.com/pingcap/tidb/pkg/disttask/framework/mock/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/mock/gomock"
)

func runOneTask(t *testing.T, ctx context.Context, mgr *storage.TaskManager) {
	mgr.AddNewGlobalTask(ctx, "key1", proto.TaskTypeExample, 1, nil)
	task, err := mgr.GetGlobalTaskByKey(ctx, "key1")
	require.NoError(t, err)
	scheduler := scheduler.NewBaseScheduler(ctx, "id", 1, mgr)
	scheduler.Run(ctx, task)
}

func getMockSubtaskExecutor(ctrl *gomock.Controller) *mockexecute.MockSubtaskExecutor {
	executor := mockexecute.NewMockSubtaskExecutor(ctrl)
	executor.EXPECT().Init(gomock.Any()).Return(nil).AnyTimes()
	executor.EXPECT().Cleanup(gomock.Any()).Return(nil).AnyTimes()
	executor.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	executor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return executor
}

func TestScheduler(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr := storage.NewTaskManager(pool)

	// mock init
	mockExtension := mock.NewMockExtension(ctrl)
	mockSubtaskExecutor := getMockSubtaskExecutor(ctrl)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, subtask *proto.Subtask) error {
			switch subtask.Step {
			case proto.StepOne:
				logutil.BgLogger().Info("ywq 1")
			case proto.StepTwo:
				logutil.BgLogger().Info("ywq 2")
			default:
				panic("invalid step")
			}
			return nil
		}).AnyTimes()
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil).AnyTimes()
	scheduler.RegisterTaskType(proto.TaskTypeExample,
		func(ctx context.Context, id string, task *proto.Task, taskTable scheduler.TaskTable) scheduler.Scheduler {
			s := scheduler.NewBaseScheduler(ctx, id, task.ID, taskTable)
			s.Extension = mockExtension
			return s
		},
	)
	runOneTask(t, ctx, mgr)
}
