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

package ddl

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	dxfstorage "github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
)

func (w *worker) createTableWithSelect(jobCtx *jobContext, job *model.Job, args *model.CreateTableArgs) (ver int64, err error) {
	tbInfo := args.TableInfo
	switch tbInfo.State {
	case model.StateNone:
		tbInfo, err = createTable(jobCtx, job, &asAutoIDRequirement{
			store:     w.store,
			autoidCli: w.autoidCli,
		}, args)
		if err != nil {
			return ver, errors.Trace(err)
		}
		tbInfo.State = model.StateWriteReorganization
		ver, err = updateVersionAndTableInfo(jobCtx, job, tbInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteReorganization
	case model.StateWriteReorganization:
		if err := submitCTASLoadSubtask(jobCtx.stepCtx, job, args); err != nil {
			// When the current DDL step is interrupted, let the cancel/pause path
			// coordinate CTAS cleanup instead of dropping metadata immediately.
			if jobCtx.stepCtx != nil && jobCtx.stepCtx.Err() != nil {
				return ver, errors.Trace(err)
			}
			ver, rollbackErr := w.rollbackCTASTable(jobCtx, job, tbInfo)
			if rollbackErr != nil {
				return ver, errors.Trace(rollbackErr)
			}
			return ver, errors.Trace(err)
		}
		finished, rollback, err := isCTASLoadSubtaskDone(jobCtx.stepCtx, job)
		if err != nil {
			if !rollback {
				if err == errWaitCTASLoadSubtask {
					time.Sleep(300 * time.Millisecond)
					return ver, nil
				}
				return ver, errors.Trace(err)
			}
			ver, rollbackErr := w.rollbackCTASTable(jobCtx, job, tbInfo)
			if rollbackErr != nil {
				return ver, errors.Trace(rollbackErr)
			}
			return ver, errors.Trace(err)
		}
		if !finished {
			return ver, errWaitCTASLoadSubtask
		}
		tbInfo.State = model.StatePublic
		ver, err = updateVersionAndTableInfo(jobCtx, job, tbInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		createTableEvent := notifier.NewCreateTableEvent(tbInfo)
		err = asyncNotifyEvent(jobCtx, createTableEvent, job, noSubJob, w.sess)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
		return ver, nil
	default:
		return ver, errors.Trace(dbterror.ErrInvalidDDLJob.GenWithStackByArgs("table", tbInfo.State))
	}
	return ver, errors.Trace(err)
}

func ctasTaskKey(jobID int64) string {
	return fmt.Sprintf("%s/%d", proto.CTAS, jobID)
}

func submitCTASLoadSubtask(ctx context.Context, job *model.Job, args *model.CreateTableArgs) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if args.CTASSnapshotTS == 0 {
		return errors.New("ctas snapshot ts is empty")
	}
	if args.CTASSql == "" {
		return errors.New("ctas sql is empty")
	}
	if args.CTASSubmitterExecID == "" {
		return errors.New("ctas submitter exec id is empty")
	}
	if args.CTASSubmitterConnID == 0 {
		return errors.New("ctas submitter conn id is empty")
	}

	ctx = kv.WithInternalSourceType(ctx, kv.InternalDistTask)
	taskKey := ctasTaskKey(job.ID)

	metaBytes, err := json.Marshal(&CTASLoadSubtaskMeta{
		JobID:               job.ID,
		SchemaID:            job.SchemaID,
		TableID:             args.TableInfo.ID,
		CTASSnapshotTS:      args.CTASSnapshotTS,
		CTASSql:             args.CTASSql,
		CTASOnDuplicate:     args.CTASOnDuplicate,
		CTASSubmitterExecID: args.CTASSubmitterExecID,
		CTASSubmitterConnID: args.CTASSubmitterConnID,
	})
	if err != nil {
		return errors.Trace(err)
	}

	task, err := handle.SubmitTask(ctx, taskKey, proto.CTAS, 1, "", 1, metaBytes)
	if err != nil {
		if errors.ErrorEqual(err, dxfstorage.ErrTaskAlreadyExists) {
			return nil
		}
		return err
	}

	logutil.DDLLogger().Info("submitted CTAS load subtask",
		zap.Int64("jobID", job.ID),
		zap.Int64("taskID", task.ID),
		zap.Int64("schemaID", job.SchemaID),
		zap.Int64("tableID", args.TableInfo.ID),
		zap.Uint64("snapshotTS", args.CTASSnapshotTS),
		zap.String("submitterExecID", args.CTASSubmitterExecID),
		zap.Uint64("submitterConnID", args.CTASSubmitterConnID))
	return nil
}

func getCTASLoadSubtask(ctx context.Context, job *model.Job) (*proto.Task, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	taskManager, err := dxfstorage.GetTaskManager()
	if err != nil {
		return nil, err
	}
	return taskManager.GetTaskByKeyWithHistory(ctx, ctasTaskKey(job.ID))
}

func isCTASLoadSubtaskDone(ctx context.Context, job *model.Job) (bool, bool, error) {
	task, err := getCTASLoadSubtask(ctx, job)
	if err != nil {
		if errors.ErrorEqual(err, dxfstorage.ErrTaskNotFound) {
			return false, false, nil
		}
		return false, false, err
	}

	switch task.State {
	case proto.TaskStateSucceed:
		return true, false, nil
	case proto.TaskStatePaused:
		return false, false, dbterror.ErrPausedDDLJob.GenWithStackByArgs(job.ID)
	case proto.TaskStateFailed, proto.TaskStateReverted:
		if task.Error != nil {
			return false, true, task.Error
		}
		return false, true, errors.Errorf("ctas load task %d stopped in state %s", task.ID, task.State)
	default:
		return false, false, errWaitCTASLoadSubtask
	}
}

func (w *worker) rollbackCTASTable(jobCtx *jobContext, job *model.Job, tbInfo *model.TableInfo) (int64, error) {
	metaMut := jobCtx.metaMut
	if err := metaMut.DropTableOrView(job.SchemaID, job.TableID); err != nil {
		if !errors.ErrorEqual(err, meta.ErrTableNotExists) {
			return 0, errors.Trace(err)
		}
	}
	if err := metaMut.GetAutoIDAccessors(job.SchemaID, job.TableID).Del(); err != nil {
		return 0, errors.Trace(err)
	}
	tbInfo.State = model.StateNone
	ver, err := updateSchemaVersion(jobCtx, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tbInfo)
	return ver, nil
}
