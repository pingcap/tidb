// Copyright 2026 PingCAP, Inc.
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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	dxfstorage "github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

func setCTASRollbackJobError(job *model.Job, err error) {
	if err != nil {
		job.Error = toTError(err)
		return
	}
	if job.Error == nil || job.Error.Equal(dbterror.ErrCannotCancelDDLJob) {
		job.Error = toTError(dbterror.ErrCancelledDDLJob)
	}
}

func driveCTASLoadTaskTowardRollback(ctx context.Context, task *proto.Task) error {
	if task.State == proto.TaskStatePaused {
		// A paused task will stay parked until it is resumed, so resume it first
		// and let a later DDL retry cancel it once DXF brings it back to running.
		return handle.ResumeTask(ctx, task.Key)
	}
	return handle.CancelTask(ctx, task.Key)
}

func (w *worker) rollbackCTASTableOnCancel(jobCtx *jobContext, job *model.Job, tbInfo *model.TableInfo) (int64, error) {
	ctx := jobCtx.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	task, err := getCTASLoadSubtask(ctx, job)
	if err != nil {
		if !errors.ErrorEqual(err, dxfstorage.ErrTaskNotFound) {
			return 0, err
		}
		setCTASRollbackJobError(job, nil)
		return w.rollbackCTASTable(jobCtx, job, tbInfo)
	}

	if task.IsDone() {
		setCTASRollbackJobError(job, task.Error)
		return w.rollbackCTASTable(jobCtx, job, tbInfo)
	}
	// Keep the DDL job in cancelling state until DXF finishes converging the task.
	return 0, driveCTASLoadTaskTowardRollback(ctx, task)
}