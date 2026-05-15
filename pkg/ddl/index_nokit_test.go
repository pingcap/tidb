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

package ddl

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/errors"
	ddlmock "github.com/pingcap/tidb/pkg/ddl/mock"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	"github.com/pingcap/tidb/pkg/dxf/framework/mock"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestModifyTaskParamLoop(t *testing.T) {
	type env struct {
		ctrl          *gomock.Controller
		ctx           context.Context
		sysTblMgr     *ddlmock.MockManager
		taskMgr       *mock.MockManager
		done          chan struct{}
		jobID, taskID int64

		currentJob  *model.JobW
		modifiedJob *model.JobW
	}
	newEnv := func(t *testing.T) *env {
		ctrl := gomock.NewController(t)
		bak := UpdateDDLJobReorgCfgInterval
		t.Cleanup(func() {
			ctrl.Finish()
			UpdateDDLJobReorgCfgInterval = bak
		})
		UpdateDDLJobReorgCfgInterval = 10 * time.Millisecond
		currentJob := &model.Job{ReorgMeta: &model.DDLReorgMeta{}}
		currentJob.ReorgMeta.SetConcurrency(1)
		currentJob.ReorgMeta.SetBatchSize(2)
		currentJob.ReorgMeta.SetMaxWriteSpeed(3)

		modifiedJob := &model.Job{ReorgMeta: &model.DDLReorgMeta{}}
		modifiedJob.ReorgMeta.SetConcurrency(4)
		modifiedJob.ReorgMeta.SetBatchSize(5)
		modifiedJob.ReorgMeta.SetMaxWriteSpeed(6)
		return &env{
			ctrl:      ctrl,
			ctx:       context.Background(),
			sysTblMgr: ddlmock.NewMockManager(ctrl),
			taskMgr:   mock.NewMockManager(ctrl),
			done:      make(chan struct{}),
			jobID:     int64(1),
			taskID:    int64(1),

			currentJob:  &model.JobW{Job: currentJob},
			modifiedJob: &model.JobW{Job: modifiedJob},
		}
	}
	t.Run("return on done", func(t *testing.T) {
		e := newEnv(t)
		close(e.done)
		modifyTaskParamLoop(e.ctx, e.sysTblMgr, e.taskMgr, e.done,
			e.jobID, e.taskID, 1, 2, 3)
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("retry on get job error; return on job not found", func(t *testing.T) {
		e := newEnv(t)
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(nil, errors.New("some error"))
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(nil, systable.ErrNotFound)
		modifyTaskParamLoop(e.ctx, e.sysTblMgr, e.taskMgr, e.done,
			e.jobID, e.taskID, 1, 2, 3)
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("adjust concurrency failed, retry", func(t *testing.T) {
		e := newEnv(t)
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.currentJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(0, errors.New("some error"))
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(nil, systable.ErrNotFound)
		modifyTaskParamLoop(e.ctx, e.sysTblMgr, e.taskMgr, e.done,
			e.jobID, e.taskID, 1, 2, 3)
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("nothing modified, retry", func(t *testing.T) {
		e := newEnv(t)
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.currentJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(nil, systable.ErrNotFound)
		modifyTaskParamLoop(e.ctx, e.sysTblMgr, e.taskMgr, e.done,
			e.jobID, e.taskID, 1, 2, 3)
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("detect modify, but the task has done", func(t *testing.T) {
		e := newEnv(t)
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.modifiedJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		e.taskMgr.EXPECT().GetTaskByID(e.ctx, e.taskID).Return(nil, storage.ErrTaskNotFound)
		modifyTaskParamLoop(e.ctx, e.sysTblMgr, e.taskMgr, e.done,
			e.jobID, e.taskID, 1, 2, 3)
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("detect modify, fail to get task, after retry, found task state is un-modifiable", func(t *testing.T) {
		e := newEnv(t)
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.modifiedJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		e.taskMgr.EXPECT().GetTaskByID(e.ctx, e.taskID).Return(nil, errors.New("some error"))
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.modifiedJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		e.taskMgr.EXPECT().GetTaskByID(e.ctx, e.taskID).Return(&proto.Task{TaskBase: proto.TaskBase{State: proto.TaskStateCancelling}}, nil)
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.modifiedJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		e.taskMgr.EXPECT().GetTaskByID(e.ctx, e.taskID).Return(nil, storage.ErrTaskNotFound)
		modifyTaskParamLoop(e.ctx, e.sysTblMgr, e.taskMgr, e.done,
			e.jobID, e.taskID, 1, 2, 3)
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("detect modify, success after retry, and we update internal variable to avoid modify twice", func(t *testing.T) {
		e := newEnv(t)
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.modifiedJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		e.taskMgr.EXPECT().GetTaskByID(e.ctx, e.taskID).Return(&proto.Task{TaskBase: proto.TaskBase{State: proto.TaskStateRunning}}, nil)
		modifyParam := &proto.ModifyParam{
			PrevState: proto.TaskStateRunning,
			Modifications: []proto.Modification{
				{Type: proto.ModifyRequiredSlots, To: 4},
				{Type: proto.ModifyBatchSize, To: 5},
				{Type: proto.ModifyMaxWriteSpeed, To: 6},
			},
		}
		e.taskMgr.EXPECT().ModifyTaskByID(e.ctx, e.taskID, modifyParam).Return(errors.New("some error"))
		// retry and success
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.modifiedJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		e.taskMgr.EXPECT().GetTaskByID(e.ctx, e.taskID).Return(&proto.Task{TaskBase: proto.TaskBase{State: proto.TaskStateRunning}}, nil)
		e.taskMgr.EXPECT().ModifyTaskByID(e.ctx, e.taskID, modifyParam).Return(nil)
		// same param, but will continue this time, as nothing modified
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.modifiedJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		// exit loop
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(nil, systable.ErrNotFound)
		modifyTaskParamLoop(e.ctx, e.sysTblMgr, e.taskMgr, e.done,
			e.jobID, e.taskID, 1, 2, 3)
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("modify twice, both success", func(t *testing.T) {
		e := newEnv(t)
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.modifiedJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		e.taskMgr.EXPECT().GetTaskByID(e.ctx, e.taskID).Return(&proto.Task{TaskBase: proto.TaskBase{State: proto.TaskStateRunning}}, nil)
		modifyParam := &proto.ModifyParam{
			PrevState: proto.TaskStateRunning,
			Modifications: []proto.Modification{
				{Type: proto.ModifyRequiredSlots, To: 4},
				{Type: proto.ModifyBatchSize, To: 5},
				{Type: proto.ModifyMaxWriteSpeed, To: 6},
			},
		}
		e.taskMgr.EXPECT().ModifyTaskByID(e.ctx, e.taskID, modifyParam).Return(nil)
		// same param, but will continue this time, as nothing modified
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(e.modifiedJob, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)

		modifiedJob2 := &model.JobW{Job: &model.Job{ReorgMeta: &model.DDLReorgMeta{}}}
		modifiedJob2.ReorgMeta.SetConcurrency(7)
		modifiedJob2.ReorgMeta.SetBatchSize(8)
		modifiedJob2.ReorgMeta.SetMaxWriteSpeed(9)
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(modifiedJob2, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		e.taskMgr.EXPECT().GetTaskByID(e.ctx, e.taskID).Return(&proto.Task{TaskBase: proto.TaskBase{State: proto.TaskStateRunning}}, nil)
		modifyParam2 := &proto.ModifyParam{
			PrevState: proto.TaskStateRunning,
			Modifications: []proto.Modification{
				{Type: proto.ModifyRequiredSlots, To: 7},
				{Type: proto.ModifyBatchSize, To: 8},
				{Type: proto.ModifyMaxWriteSpeed, To: 9},
			},
		}
		e.taskMgr.EXPECT().ModifyTaskByID(e.ctx, e.taskID, modifyParam2).Return(nil)
		// same param, but will continue this time, as nothing modified
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(modifiedJob2, nil)
		e.taskMgr.EXPECT().GetCPUCountOfNode(e.ctx).Return(123, nil)
		// exit loop
		e.sysTblMgr.EXPECT().GetJobByID(e.ctx, e.jobID).Return(nil, systable.ErrNotFound)
		modifyTaskParamLoop(e.ctx, e.sysTblMgr, e.taskMgr, e.done,
			e.jobID, e.taskID, 1, 2, 3)
		require.True(t, e.ctrl.Satisfied())
	})
}
