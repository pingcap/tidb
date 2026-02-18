// Copyright 2018 PingCAP, Inc.
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
	goerrors "errors"
	"fmt"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	tidblogutil "github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	pdHttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func (w *worker) executeDistTask(jobCtx *jobContext, t table.Table, reorgInfo *reorgInfo) error {
	stepCtx := jobCtx.stepCtx
	taskType := proto.Backfill
	tkBuilder := NewTaskKeyBuilder().
		SetMultiSchema(reorgInfo.Job.MultiSchemaInfo).
		SetMergeTempIndex(reorgInfo.mergingTmpIdx)
	taskKey := tkBuilder.Build(reorgInfo.Job.ID)
	g, ctx := errgroup.WithContext(w.workCtx)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalDistTask)

	done := make(chan struct{})
	// For resuming add index task.
	// Need to fetch task by taskKey in tidb_global_task and tidb_global_task_history tables.
	// When pausing the related ddl job, it is possible that the task with taskKey is succeed and in tidb_global_task_history.
	// As a result, when resuming the related ddl job,
	// it is necessary to check task exits in tidb_global_task and tidb_global_task_history tables.
	taskManager, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		return err
	}
	task, err := taskManager.GetTaskByKeyWithHistory(w.workCtx, taskKey)
	if err != nil && !goerrors.Is(err, storage.ErrTaskNotFound) {
		return err
	}

	var (
		taskID                                              int64
		lastRequiredSlots, lastBatchSize, lastMaxWriteSpeed int
	)
	if task != nil {
		// It's possible that the task state is succeed but the ddl job is paused.
		// When task in succeed state, we can skip the dist task execution/scheduling process.
		if task.State == proto.TaskStateSucceed {
			w.updateDistTaskRowCount(taskKey, reorgInfo.Job.ID)
			logutil.DDLLogger().Info(
				"task succeed, start to resume the ddl job",
				zap.String("task-key", taskKey))
			return nil
		}
		taskMeta := &BackfillTaskMeta{}
		if err := json.Unmarshal(task.Meta, taskMeta); err != nil {
			return errors.Trace(err)
		}
		taskID = task.ID
		lastRequiredSlots = task.RequiredSlots
		lastBatchSize = taskMeta.Job.ReorgMeta.GetBatchSize()
		lastMaxWriteSpeed = taskMeta.Job.ReorgMeta.GetMaxWriteSpeed()
		g.Go(func() error {
			defer close(done)
			backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
			err := handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, logutil.DDLLogger(),
				func(context.Context) (bool, error) {
					return true, handle.ResumeTask(w.workCtx, taskKey)
				},
			)
			if err != nil {
				return err
			}
			err = handle.WaitTaskDoneOrPaused(ctx, task.ID)
			if err := w.isReorgRunnable(stepCtx, true); err != nil {
				if dbterror.ErrPausedDDLJob.Equal(err) {
					logutil.DDLLogger().Warn("job paused by user", zap.Error(err))
					return dbterror.ErrPausedDDLJob.GenWithStackByArgs(reorgInfo.Job.ID)
				}
			}
			return err
		})
	} else {
		job := reorgInfo.Job
		workerCntLimit := job.ReorgMeta.GetConcurrency()
		requiredSlots, err := adjustConcurrency(ctx, taskManager, workerCntLimit)
		if err != nil {
			return err
		}
		logutil.DDLLogger().Info("adjusted add-index task required slots",
			zap.Int("worker-cnt", workerCntLimit), zap.Int("required-slots", requiredSlots),
			zap.String("task-key", taskKey))
		rowSize := estimateTableRowSize(w.workCtx, w.store, w.sess.GetRestrictedSQLExecutor(), t)
		taskMeta := &BackfillTaskMeta{
			Job:             *job.Clone(),
			EleIDs:          extractElemIDs(reorgInfo),
			EleTypeKey:      reorgInfo.currElement.TypeKey,
			CloudStorageURI: w.jobContext(job.ID, job.ReorgMeta).cloudStorageURI,
			MergeTempIndex:  reorgInfo.mergingTmpIdx,
			EstimateRowSize: rowSize,
			Version:         BackfillTaskMetaVersion1,
		}

		metaData, err := json.Marshal(taskMeta)
		if err != nil {
			return err
		}

		targetScope := reorgInfo.ReorgMeta.TargetScope
		maxNodeCnt := reorgInfo.ReorgMeta.MaxNodeCount
		task, err := handle.SubmitTask(ctx, taskKey, taskType, w.store.GetKeyspace(), requiredSlots, targetScope, maxNodeCnt, metaData)
		if err != nil {
			return err
		}

		taskID = task.ID
		lastRequiredSlots = requiredSlots
		lastBatchSize = taskMeta.Job.ReorgMeta.GetBatchSize()
		lastMaxWriteSpeed = taskMeta.Job.ReorgMeta.GetMaxWriteSpeed()

		g.Go(func() error {
			defer close(done)
			err := handle.WaitTaskDoneOrPaused(ctx, task.ID)
			failpoint.InjectCall("pauseAfterDistTaskFinished")
			if err := w.isReorgRunnable(stepCtx, true); err != nil {
				if dbterror.ErrPausedDDLJob.Equal(err) {
					logutil.DDLLogger().Warn("job paused by user", zap.Error(err))
					return dbterror.ErrPausedDDLJob.GenWithStackByArgs(reorgInfo.Job.ID)
				}
			}
			return err
		})
	}

	g.Go(func() error {
		checkFinishTk := time.NewTicker(CheckBackfillJobFinishInterval)
		defer checkFinishTk.Stop()
		updateRowCntTk := time.NewTicker(UpdateBackfillJobRowCountInterval)
		defer updateRowCntTk.Stop()
		for {
			select {
			case <-done:
				w.updateDistTaskRowCount(taskKey, reorgInfo.Job.ID)
				err := w.checkRunnableOrHandlePauseOrCanceled(stepCtx, taskKey)
				return errors.Trace(err)
			case <-checkFinishTk.C:
				err := w.checkRunnableOrHandlePauseOrCanceled(stepCtx, taskKey)
				if err != nil {
					return errors.Trace(err)
				}
			case <-updateRowCntTk.C:
				w.updateDistTaskRowCount(taskKey, reorgInfo.Job.ID)
			}
		}
	})

	g.Go(func() error {
		modifyTaskParamLoop(ctx, jobCtx.sysTblMgr, taskManager, done,
			reorgInfo.Job.ID, taskID, lastRequiredSlots, lastBatchSize, lastMaxWriteSpeed)
		return nil
	})

	err = g.Wait()
	return err
}

func (w *worker) checkRunnableOrHandlePauseOrCanceled(stepCtx context.Context, taskKey string) (err error) {
	if err = w.isReorgRunnable(stepCtx, true); err != nil {
		if dbterror.ErrPausedDDLJob.Equal(err) {
			if err = handle.PauseTask(w.workCtx, taskKey); err != nil {
				logutil.DDLLogger().Warn("pause task error", zap.String("task_key", taskKey), zap.Error(err))
				return nil
			}
			failpoint.InjectCall("syncDDLTaskPause")
		}
		if !dbterror.ErrCancelledDDLJob.Equal(err) {
			return errors.Trace(err)
		}
		if err = handle.CancelTask(w.workCtx, taskKey); err != nil {
			logutil.DDLLogger().Warn("cancel task error", zap.String("task_key", taskKey), zap.Error(err))
			return nil
		}
	}
	return nil
}

// Note: we can achieve the same effect by calling ModifyTaskByID directly inside
// the process of 'ADMIN ALTER DDL JOB xxx', so we can eliminate the goroutine,
// but if the task hasn't been created we need to make sure the task is created
// with config after ALTER DDL JOB is executed. A possible solution is to make
// the DXF task submission and 'ADMIN ALTER DDL JOB xxx' txn conflict with each
// other when they overlap in time, by modify the job at the same time when submit
// task, as we are using optimistic txn. But this will cause WRITE CONFLICT with
// outer txn in transitOneJobStep.
func modifyTaskParamLoop(
	ctx context.Context,
	sysTblMgr systable.Manager,
	taskManager storage.Manager,
	done chan struct{},
	jobID, taskID int64,
	lastRequiredSlots, lastBatchSize, lastMaxWriteSpeed int,
) {
	logger := logutil.DDLLogger().With(zap.Int64("jobID", jobID), zap.Int64("taskID", taskID))
	ticker := time.NewTicker(UpdateDDLJobReorgCfgInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
		}

		latestJob, err := sysTblMgr.GetJobByID(ctx, jobID)
		if err != nil {
			if goerrors.Is(err, systable.ErrNotFound) {
				logger.Info("job not found, might already finished")
				return
			}
			logger.Error("get job failed, will retry later", zap.Error(err))
			continue
		}

		modifies := make([]proto.Modification, 0, 3)
		workerCntLimit := latestJob.ReorgMeta.GetConcurrency()
		requiredSlots, err := adjustConcurrency(ctx, taskManager, workerCntLimit)
		if err != nil {
			logger.Error("adjust required slots failed", zap.Error(err))
			continue
		}
		if requiredSlots != lastRequiredSlots {
			modifies = append(modifies, proto.Modification{
				Type: proto.ModifyRequiredSlots,
				To:   int64(requiredSlots),
			})
		}
		batchSize := latestJob.ReorgMeta.GetBatchSize()
		if batchSize != lastBatchSize {
			modifies = append(modifies, proto.Modification{
				Type: proto.ModifyBatchSize,
				To:   int64(batchSize),
			})
		}
		maxWriteSpeed := latestJob.ReorgMeta.GetMaxWriteSpeed()
		if maxWriteSpeed != lastMaxWriteSpeed {
			modifies = append(modifies, proto.Modification{
				Type: proto.ModifyMaxWriteSpeed,
				To:   int64(maxWriteSpeed),
			})
		}
		if len(modifies) == 0 {
			continue
		}
		currTask, err := taskManager.GetTaskByID(ctx, taskID)
		if err != nil {
			if goerrors.Is(err, storage.ErrTaskNotFound) {
				logger.Info("task not found, might already finished")
				return
			}
			logger.Error("get task failed, will retry later", zap.Error(err))
			continue
		}
		if !currTask.State.CanMoveToModifying() {
			// user might modify param again while another modify is ongoing.
			logger.Info("task state is not suitable for modifying, will retry later",
				zap.String("state", currTask.State.String()))
			continue
		}
		if err = taskManager.ModifyTaskByID(ctx, taskID, &proto.ModifyParam{
			PrevState:     currTask.State,
			Modifications: modifies,
		}); err != nil {
			logger.Error("modify task failed", zap.Error(err))
			continue
		}
		logger.Info("modify task success",
			zap.Int("oldRequiredSlots", lastRequiredSlots), zap.Int("newRequiredSlots", requiredSlots),
			zap.Int("oldBatchSize", lastBatchSize), zap.Int("newBatchSize", batchSize),
			zap.String("oldMaxWriteSpeed", units.HumanSize(float64(lastMaxWriteSpeed))),
			zap.String("newMaxWriteSpeed", units.HumanSize(float64(maxWriteSpeed))),
		)
		lastRequiredSlots = requiredSlots
		lastBatchSize = batchSize
		lastMaxWriteSpeed = maxWriteSpeed
	}
}

func adjustConcurrency(ctx context.Context, taskMgr storage.Manager, workerCnt int) (int, error) {
	cpuCount, err := taskMgr.GetCPUCountOfNode(ctx)
	if err != nil {
		return 0, err
	}
	return min(workerCnt, cpuCount), nil
}

// EstimateTableRowSizeForTest is used for test.
var EstimateTableRowSizeForTest = estimateTableRowSize

// estimateTableRowSize estimates the row size in bytes of a table.
// This function tries to retrieve row size in following orders:
//  1. AVG_ROW_LENGTH column from information_schema.tables.
//  2. region info's approximate key size / key number.
func estimateTableRowSize(
	ctx context.Context,
	store kv.Storage,
	exec sqlexec.RestrictedSQLExecutor,
	tbl table.Table,
) (sizeInBytes int) {
	defer util.Recover(metrics.LabelDDL, "estimateTableRowSize", nil, false)
	var gErr error
	defer func() {
		tidblogutil.Logger(ctx).Info("estimate row size",
			zap.Int64("tableID", tbl.Meta().ID), zap.Int("size", sizeInBytes), zap.Error(gErr))
	}()
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil,
		"select AVG_ROW_LENGTH from information_schema.tables where TIDB_TABLE_ID = %?", tbl.Meta().ID)
	if err != nil {
		gErr = err
		return 0
	}
	if len(rows) == 0 {
		gErr = errors.New("no average row data")
		return 0
	}
	avgRowSize := rows[0].GetInt64(0)
	if avgRowSize != 0 {
		return int(avgRowSize)
	}
	regionRowSize, err := estimateRowSizeFromRegion(ctx, store, tbl)
	if err != nil {
		gErr = err
		return 0
	}
	return regionRowSize
}

func estimateRowSizeFromRegion(ctx context.Context, store kv.Storage, tbl table.Table) (int, error) {
	hStore, ok := store.(helper.Storage)
	if !ok {
		return 0, fmt.Errorf("not a helper.Storage")
	}
	h := &helper.Helper{
		Store:       hStore,
		RegionCache: hStore.GetRegionCache(),
	}
	pdCli, err := h.TryGetPDHTTPClient()
	if err != nil {
		return 0, err
	}
	pid := tbl.Meta().ID
	sk, ek := tablecodec.GetTableHandleKeyRange(pid)
	start, end := hStore.GetCodec().EncodeRegionRange(sk, ek)
	// We use the second region to prevent the influence of the front and back tables.
	regionLimit := 3
	regionInfos, err := pdCli.GetRegionsByKeyRange(ctx, pdHttp.NewKeyRange(start, end), regionLimit)
	if err != nil {
		return 0, err
	}
	if len(regionInfos.Regions) != regionLimit {
		return 0, fmt.Errorf("less than 3 regions")
	}
	sample := regionInfos.Regions[1]
	if sample.ApproximateKeys == 0 || sample.ApproximateSize == 0 {
		return 0, fmt.Errorf("zero approximate size")
	}
	return int(uint64(sample.ApproximateSize)*size.MB) / int(sample.ApproximateKeys), nil
}

func (w *worker) updateDistTaskRowCount(taskKey string, jobID int64) {
	taskMgr, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		logutil.DDLLogger().Warn("cannot get task manager", zap.String("task_key", taskKey), zap.Error(err))
		return
	}
	task, err := taskMgr.GetTaskByKeyWithHistory(w.workCtx, taskKey)
	if err != nil {
		logutil.DDLLogger().Warn("cannot get task", zap.String("task_key", taskKey), zap.Error(err))
		return
	}
	rowCount, err := taskMgr.GetSubtaskRowCount(w.workCtx, task.ID, proto.BackfillStepReadIndex)
	if err != nil {
		logutil.DDLLogger().Warn("cannot get subtask row count", zap.String("task_key", taskKey), zap.Error(err))
		return
	}
	w.getReorgCtx(jobID).setRowCount(rowCount)
}
