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

package importinto

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	tidb "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	statsstorage "github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func (sch *importScheduler) GetNextStep(task *proto.TaskBase) proto.Step {
	switch task.Step {
	case proto.StepInit:
		if sch.GlobalSort {
			return proto.ImportStepEncodeAndSort
		}
		return proto.ImportStepImport
	case proto.ImportStepEncodeAndSort:
		return proto.ImportStepMergeSort
	case proto.ImportStepMergeSort:
		return proto.ImportStepWriteAndIngest
	case proto.ImportStepWriteAndIngest:
		return proto.ImportStepCollectConflicts
	case proto.ImportStepCollectConflicts:
		return proto.ImportStepConflictResolution
	case proto.ImportStepImport, proto.ImportStepConflictResolution:
		return proto.ImportStepPostProcess
	default:
		// current step must be ImportStepPostProcess
		return proto.StepDone
	}
}

func (sch *importScheduler) switchTiKV2NormalMode(ctx context.Context, task *proto.Task, logger *zap.Logger) {
	sch.updateCurrentTask(task)
	if sch.disableTiKVImportMode.Load() {
		return
	}

	sch.mu.Lock()
	defer sch.mu.Unlock()

	// TODO: use the TLS object from TiDB server
	tidbCfg := tidb.GetGlobalConfig()
	tls, err := util.NewTLSConfig(
		util.WithCAPath(tidbCfg.Security.ClusterSSLCA),
		util.WithCertAndKeyPath(tidbCfg.Security.ClusterSSLCert, tidbCfg.Security.ClusterSSLKey),
	)
	if err != nil {
		logger.Warn("get tikv mode switcher failed", zap.Error(err))
		return
	}
	pdHTTPCli := sch.TaskStore.(kv.StorageWithPD).GetPDHTTPClient()
	switcher := importer.NewTiKVModeSwitcher(tls, pdHTTPCli, logger)

	switcher.ToNormalMode(ctx)

	// clear it, so next task can switch TiKV mode again.
	sch.lastSwitchTime.Store(time.Time{})
}

func (sch *importScheduler) updateCurrentTask(task *proto.Task) {
	if sch.currTaskID.Swap(task.ID) != task.ID {
		taskMeta := &TaskMeta{}
		if err := json.Unmarshal(task.Meta, taskMeta); err == nil {
			// for raftkv2, switch mode in local backend
			sch.disableTiKVImportMode.Store(taskMeta.Plan.DisableTiKVImportMode || taskMeta.Plan.IsRaftKV2)
		}
	}
}

// ModifyMeta implements scheduler.Extension interface.
func (*importScheduler) ModifyMeta(oldMeta []byte, _ []proto.Modification) ([]byte, error) {
	return oldMeta, nil
}

func updateMeta(task *proto.Task, taskMeta *TaskMeta) error {
	bs, err := json.Marshal(taskMeta)
	if err != nil {
		return errors.Trace(err)
	}
	task.Meta = bs

	return nil
}

func getStepOfEncode(globalSort bool) proto.Step {
	if globalSort {
		return proto.ImportStepEncodeAndSort
	}
	return proto.ImportStepImport
}

// Store task summary in task meta.
// We will update it in place and make task.Meta point to the new taskMeta.
func updateTaskSummary(
	handle storage.TaskHandle,
	task *proto.Task,
	taskMeta *TaskMeta,
	nextStep proto.Step,
	p *LogicalPlan,
) error {
	// Process row count and data size
	switch nextStep {
	case proto.ImportStepEncodeAndSort, proto.ImportStepImport:
		taskMeta.Summary.EncodeSummary = p.summary
	case proto.ImportStepMergeSort:
		taskMeta.Summary.MergeSummary = p.summary
	case proto.ImportStepWriteAndIngest:
		taskMeta.Summary.IngestSummary = p.summary
	case proto.ImportStepPostProcess:
		subtaskSummaries, err := handle.GetPreviousSubtaskSummary(task.ID, getStepOfEncode(taskMeta.Plan.IsGlobalSort()))
		if err != nil {
			return errors.Trace(err)
		}

		for _, subtaskSummary := range subtaskSummaries {
			taskMeta.Summary.ImportedRows += subtaskSummary.RowCnt.Load()
		}
		if taskMeta.Plan.IsGlobalSort() {
			metas, err := handle.GetPreviousSubtaskMetas(task.ID, proto.ImportStepCollectConflicts)
			if err != nil {
				return err
			}
			var conflictedRowCnt uint64
			for _, bs := range metas {
				var subtaskMeta CollectConflictsStepMeta
				if err = json.Unmarshal(bs, &subtaskMeta); err != nil {
					return errors.Trace(err)
				}
				if subtaskMeta.TooManyConflictsFromIndex {
					// in this case, we can't get the exact conflicted row count, so we
					// keep the original.
					taskMeta.Summary.TooManyConflicts = true
					continue
				}
				conflictedRowCnt += uint64(subtaskMeta.ConflictedRowCount)
			}
			// 'left row count' = 'encoded row count' - 'conflicted row count'
			taskMeta.Summary.ImportedRows -= int64(conflictedRowCnt)
			taskMeta.Summary.ConflictRowCnt = conflictedRowCnt
		}
	}

	return updateMeta(task, taskMeta)
}

func (sch *importScheduler) startJob(ctx context.Context, logger *zap.Logger, taskMeta *TaskMeta, jobStep string) error {
	failpoint.InjectCall("syncBeforeJobStarted", taskMeta.JobID)
	taskManager, err := sch.getTaskMgrForAccessingImportJob()
	if err != nil {
		return err
	}
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	// we consider all errors as retryable errors, except context done.
	// the errors include errors happened when communicate with PD and TiKV.
	// we didn't consider system corrupt cases like system table dropped/altered.
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	err = handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, taskManager.WithNewSession(func(se sessionctx.Context) error {
				exec := se.GetSQLExecutor()
				return importer.StartJob(ctx, exec, taskMeta.JobID, jobStep)
			})
		},
	)
	failpoint.InjectCall("syncAfterJobStarted")
	return err
}

func (sch *importScheduler) job2Step(ctx context.Context, logger *zap.Logger, taskMeta *TaskMeta, step string) error {
	taskManager, err := sch.getTaskMgrForAccessingImportJob()
	if err != nil {
		return err
	}
	// todo: use scheduler.TaskHandle
	// we might call this in taskExecutor later, there's no scheduler.Extension, so we use taskManager here.
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	return handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, taskManager.WithNewSession(func(se sessionctx.Context) error {
				exec := se.GetSQLExecutor()
				return importer.Job2Step(ctx, exec, taskMeta.JobID, step)
			})
		},
	)
}

func (sch *importScheduler) finishJob(ctx context.Context, logger *zap.Logger,
	task *proto.Task, taskMeta *TaskMeta) error {
	// we have already switched import-mode when switch to post-process step.
	sch.unregisterTask(ctx, task)
	taskManager, err := sch.getTaskMgrForAccessingImportJob()
	if err != nil {
		return err
	}

	tableStatsDelta := &statsstorage.DeltaUpdate{
		Delta: variable.TableDelta{
			Delta:    taskMeta.Summary.ImportedRows,
			Count:    taskMeta.Summary.ImportedRows,
			InitTime: time.Now(),
		},
		TableID: taskMeta.Plan.TableInfo.ID,
	}
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	return handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, taskManager.WithNewTxn(ctx, func(se sessionctx.Context) error {
				txn, err2 := se.Txn(true)
				if err2 != nil {
					return err2
				}

				// we only fill the delta change of the table when the task is
				// done, and let auto-analyze to do analyzing. depending on the
				// table size, analyze might take a long time, so we won't wait
				// for it.
				// auto analyze is triggered when the table have more than
				// AutoAnalyzeMinCnt(1000) rows, and tidb_auto_analyze_ratio(0.5)
				// portion of rows are changed, see NeedAnalyzeTable too.
				// so if the table is small, there is no analyze triggered.
				if err := statsstorage.UpdateStatsMeta(ctx, se, txn.StartTS(), tableStatsDelta); err != nil {
					logger.Warn("flush table stats failed", zap.Error(err))
				}
				exec := se.GetSQLExecutor()
				return importer.FinishJob(ctx, exec, taskMeta.JobID, &taskMeta.Summary)
			})
		},
	)
}

func (sch *importScheduler) failJob(ctx context.Context, task *proto.Task,
	taskMeta *TaskMeta, logger *zap.Logger, errorMsg string) error {
	sch.switchTiKV2NormalMode(ctx, task, logger)
	sch.unregisterTask(ctx, task)
	taskManager, err := sch.getTaskMgrForAccessingImportJob()
	if err != nil {
		return err
	}
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	return handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, taskManager.WithNewSession(func(se sessionctx.Context) error {
				exec := se.GetSQLExecutor()
				return importer.FailJob(ctx, exec, taskMeta.JobID, errorMsg, &taskMeta.Summary)
			})
		},
	)
}

func (sch *importScheduler) cancelJob(ctx context.Context, task *proto.Task,
	meta *TaskMeta, logger *zap.Logger) error {
	sch.switchTiKV2NormalMode(ctx, task, logger)
	sch.unregisterTask(ctx, task)
	taskManager, err := sch.getTaskMgrForAccessingImportJob()
	if err != nil {
		return err
	}
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	return handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, taskManager.WithNewSession(func(se sessionctx.Context) error {
				exec := se.GetSQLExecutor()
				return importer.CancelJob(ctx, exec, meta.JobID)
			})
		},
	)
}

func (sch *importScheduler) getTaskMgrForAccessingImportJob() (scheduler.TaskManager, error) {
	if sch.taskKSTaskMgr != nil {
		return sch.taskKSTaskMgr, nil
	}

	if kv.IsUserKS(sch.TaskStore) {
		var taskKSSessPool util.SessionPool
		taskKS := sch.GetTask().Keyspace
		if err := sch.BaseScheduler.WithNewSession(func(se sessionctx.Context) error {
			var err2 error
			taskKSSessPool, err2 = se.GetSQLServer().GetKSSessPool(taskKS)
			return err2
		}); err != nil {
			return nil, errors.Annotatef(errGetCrossKSSessionPool, "keyspace %s", taskKS)
		}
		sch.taskKSTaskMgr = storage.NewTaskManager(taskKSSessPool)
	} else {
		sch.taskKSTaskMgr = sch.GetTaskMgr()
	}
	return sch.taskKSTaskMgr, nil
}

func redactSensitiveInfo(task *proto.Task, taskMeta *TaskMeta) {
	taskMeta.Stmt = ""
	taskMeta.Plan.Path = ast.RedactURL(taskMeta.Plan.Path)
	if taskMeta.Plan.CloudStorageURI != "" {
		taskMeta.Plan.CloudStorageURI = ast.RedactURL(taskMeta.Plan.CloudStorageURI)
	}
	if err := updateMeta(task, taskMeta); err != nil {
		// marshal failed, should not happen
		logutil.BgLogger().Warn("failed to update task meta", zap.Error(err))
	}
}
