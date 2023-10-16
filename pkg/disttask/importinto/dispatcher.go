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
	"strconv"
	"strings"
	"sync"
	"time"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/planner"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	registerTaskTTL        = 10 * time.Minute
	refreshTaskTTLInterval = 3 * time.Minute
	registerTimeout        = 5 * time.Second
)

// NewTaskRegisterWithTTL is the ctor for TaskRegister.
// It is exported for testing.
var NewTaskRegisterWithTTL = utils.NewTaskRegisterWithTTL

type taskInfo struct {
	taskID int64

	// operation on taskInfo is run inside detect-task goroutine, so no need to synchronize.
	lastRegisterTime time.Time

	// initialized lazily in register()
	etcdClient   *etcd.Client
	taskRegister utils.TaskRegister
}

func (t *taskInfo) register(ctx context.Context) {
	if time.Since(t.lastRegisterTime) < refreshTaskTTLInterval {
		return
	}

	if time.Since(t.lastRegisterTime) < refreshTaskTTLInterval {
		return
	}
	logger := logutil.BgLogger().With(zap.Int64("task-id", t.taskID))
	if t.taskRegister == nil {
		client, err := importer.GetEtcdClient()
		if err != nil {
			logger.Warn("get etcd client failed", zap.Error(err))
			return
		}
		t.etcdClient = client
		t.taskRegister = NewTaskRegisterWithTTL(client.GetClient(), registerTaskTTL,
			utils.RegisterImportInto, strconv.FormatInt(t.taskID, 10))
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, registerTimeout)
	defer cancel()
	if err := t.taskRegister.RegisterTaskOnce(timeoutCtx); err != nil {
		logger.Warn("register task failed", zap.Error(err))
	} else {
		logger.Info("register task to pd or refresh lease success")
	}
	// we set it even if register failed, TTL is 10min, refresh interval is 3min,
	// we can try 2 times before the lease is expired.
	t.lastRegisterTime = time.Now()
}

func (t *taskInfo) close(ctx context.Context) {
	logger := logutil.BgLogger().With(zap.Int64("task-id", t.taskID))
	if t.taskRegister != nil {
		timeoutCtx, cancel := context.WithTimeout(ctx, registerTimeout)
		defer cancel()
		if err := t.taskRegister.Close(timeoutCtx); err != nil {
			logger.Warn("unregister task failed", zap.Error(err))
		} else {
			logger.Info("unregister task success")
		}
		t.taskRegister = nil
	}
	if t.etcdClient != nil {
		if err := t.etcdClient.Close(); err != nil {
			logger.Warn("close etcd client failed", zap.Error(err))
		}
		t.etcdClient = nil
	}
}

// ImportDispatcherExt is an extension of ImportDispatcher, exported for test.
type ImportDispatcherExt struct {
	GlobalSort bool
	mu         sync.RWMutex
	// NOTE: there's no need to sync for below 2 fields actually, since we add a restriction that only one
	// task can be running at a time. but we might support task queuing in the future, leave it for now.
	// the last time we switch TiKV into IMPORT mode, this is a global operation, do it for one task makes
	// no difference to do it for all tasks. So we do not need to record the switch time for each task.
	lastSwitchTime atomic.Time
	// taskInfoMap is a map from taskID to taskInfo
	taskInfoMap sync.Map

	// currTaskID is the taskID of the current running task.
	// It may be changed when we switch to a new task or switch to a new owner.
	currTaskID            atomic.Int64
	disableTiKVImportMode atomic.Bool
}

var _ dispatcher.Extension = (*ImportDispatcherExt)(nil)

// OnTick implements dispatcher.Extension interface.
func (dsp *ImportDispatcherExt) OnTick(ctx context.Context, task *proto.Task) {
	// only switch TiKV mode or register task when task is running
	if task.State != proto.TaskStateRunning {
		return
	}
	dsp.switchTiKVMode(ctx, task)
	dsp.registerTask(ctx, task)
}

func (*ImportDispatcherExt) isImporting2TiKV(task *proto.Task) bool {
	return task.Step == StepImport || task.Step == StepWriteAndIngest
}

func (dsp *ImportDispatcherExt) switchTiKVMode(ctx context.Context, task *proto.Task) {
	dsp.updateCurrentTask(task)
	// only import step need to switch to IMPORT mode,
	// If TiKV is in IMPORT mode during checksum, coprocessor will time out.
	if dsp.disableTiKVImportMode.Load() || !dsp.isImporting2TiKV(task) {
		return
	}

	if time.Since(dsp.lastSwitchTime.Load()) < config.DefaultSwitchTiKVModeInterval {
		return
	}

	dsp.mu.Lock()
	defer dsp.mu.Unlock()
	if time.Since(dsp.lastSwitchTime.Load()) < config.DefaultSwitchTiKVModeInterval {
		return
	}

	logger := logutil.BgLogger().With(zap.Int64("task-id", task.ID))
	pdCli, switcher, err := importer.GetTiKVModeSwitcherWithPDClient(ctx, logger)
	if err != nil {
		logger.Warn("get tikv mode switcher failed", zap.Error(err))
		return
	}
	switcher.ToImportMode(ctx)
	pdCli.Close()
	dsp.lastSwitchTime.Store(time.Now())
}

func (dsp *ImportDispatcherExt) registerTask(ctx context.Context, task *proto.Task) {
	val, _ := dsp.taskInfoMap.LoadOrStore(task.ID, &taskInfo{taskID: task.ID})
	info := val.(*taskInfo)
	info.register(ctx)
}

func (dsp *ImportDispatcherExt) unregisterTask(ctx context.Context, task *proto.Task) {
	if val, loaded := dsp.taskInfoMap.LoadAndDelete(task.ID); loaded {
		info := val.(*taskInfo)
		info.close(ctx)
	}
}

// OnNextSubtasksBatch generate batch of next stage's plan.
func (dsp *ImportDispatcherExt) OnNextSubtasksBatch(
	ctx context.Context,
	taskHandle dispatcher.TaskHandle,
	gTask *proto.Task,
	nextStep proto.Step,
) (
	resSubtaskMeta [][]byte, err error) {
	logger := logutil.BgLogger().With(
		zap.Stringer("type", gTask.Type),
		zap.Int64("task-id", gTask.ID),
		zap.String("curr-step", stepStr(gTask.Step)),
		zap.String("next-step", stepStr(nextStep)),
	)
	taskMeta := &TaskMeta{}
	err = json.Unmarshal(gTask.Meta, taskMeta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	logger.Info("on next subtasks batch")

	defer func() {
		taskFinished := err == nil && nextStep == proto.StepDone
		if taskFinished {
			// todo: we're not running in a transaction with task update
			if err2 := dsp.finishJob(ctx, logger, taskHandle, gTask, taskMeta); err2 != nil {
				err = err2
			}
		} else if err != nil && !dsp.IsRetryableErr(err) {
			if err2 := dsp.failJob(ctx, taskHandle, gTask, taskMeta, logger, err.Error()); err2 != nil {
				// todo: we're not running in a transaction with task update, there might be case
				// failJob return error, but task update succeed.
				logger.Error("call failJob failed", zap.Error(err2))
			}
		}
	}()

	previousSubtaskMetas := make(map[proto.Step][][]byte, 1)
	switch nextStep {
	case StepImport, StepEncodeAndSort:
		if metrics, ok := metric.GetCommonMetric(ctx); ok {
			metrics.BytesCounter.WithLabelValues(metric.StateTotalRestore).Add(float64(taskMeta.Plan.TotalFileSize))
		}
		jobStep := importer.JobStepImporting
		if dsp.GlobalSort {
			jobStep = importer.JobStepGlobalSorting
		}
		if err = startJob(ctx, logger, taskHandle, taskMeta, jobStep); err != nil {
			return nil, err
		}
	case StepMergeSort:
		sortAndEncodeMeta, err := taskHandle.GetPreviousSubtaskMetas(gTask.ID, StepEncodeAndSort)
		if err != nil {
			return nil, err
		}
		previousSubtaskMetas[StepEncodeAndSort] = sortAndEncodeMeta
	case StepWriteAndIngest:
		failpoint.Inject("failWhenDispatchWriteIngestSubtask", func() {
			failpoint.Return(nil, errors.New("injected error"))
		})
		// merge sort might be skipped for some kv groups, so we need to get all
		// subtask metas of StepEncodeAndSort step too.
		encodeAndSortMetas, err := taskHandle.GetPreviousSubtaskMetas(gTask.ID, StepEncodeAndSort)
		if err != nil {
			return nil, err
		}
		mergeSortMetas, err := taskHandle.GetPreviousSubtaskMetas(gTask.ID, StepMergeSort)
		if err != nil {
			return nil, err
		}
		previousSubtaskMetas[StepEncodeAndSort] = encodeAndSortMetas
		previousSubtaskMetas[StepMergeSort] = mergeSortMetas
		if err = job2Step(ctx, logger, taskMeta, importer.JobStepImporting); err != nil {
			return nil, err
		}
	case StepPostProcess:
		dsp.switchTiKV2NormalMode(ctx, gTask, logger)
		failpoint.Inject("clearLastSwitchTime", func() {
			dsp.lastSwitchTime.Store(time.Time{})
		})
		if err = job2Step(ctx, logger, taskMeta, importer.JobStepValidating); err != nil {
			return nil, err
		}
		failpoint.Inject("failWhenDispatchPostProcessSubtask", func() {
			failpoint.Return(nil, errors.New("injected error after StepImport"))
		})
		// we need get metas where checksum is stored.
		if err := updateResult(taskHandle, gTask, taskMeta, dsp.GlobalSort); err != nil {
			return nil, err
		}
		step := getStepOfEncode(dsp.GlobalSort)
		metas, err := taskHandle.GetPreviousSubtaskMetas(gTask.ID, step)
		if err != nil {
			return nil, err
		}
		previousSubtaskMetas[step] = metas
		logger.Info("move to post-process step ", zap.Any("result", taskMeta.Result))
	case proto.StepDone:
		return nil, nil
	default:
		return nil, errors.Errorf("unknown step %d", gTask.Step)
	}

	planCtx := planner.PlanCtx{
		Ctx:                  ctx,
		TaskID:               gTask.ID,
		PreviousSubtaskMetas: previousSubtaskMetas,
		GlobalSort:           dsp.GlobalSort,
		NextTaskStep:         nextStep,
	}
	logicalPlan := &LogicalPlan{}
	if err := logicalPlan.FromTaskMeta(gTask.Meta); err != nil {
		return nil, err
	}
	physicalPlan, err := logicalPlan.ToPhysicalPlan(planCtx)
	if err != nil {
		return nil, err
	}
	metaBytes, err := physicalPlan.ToSubtaskMetas(planCtx, nextStep)
	if err != nil {
		return nil, err
	}
	logger.Info("generate subtasks", zap.Int("subtask-count", len(metaBytes)))
	return metaBytes, nil
}

// OnErrStage implements dispatcher.Extension interface.
func (dsp *ImportDispatcherExt) OnErrStage(ctx context.Context, handle dispatcher.TaskHandle, gTask *proto.Task, receiveErrs []error) ([]byte, error) {
	logger := logutil.BgLogger().With(
		zap.Stringer("type", gTask.Type),
		zap.Int64("task-id", gTask.ID),
		zap.String("step", stepStr(gTask.Step)),
	)
	logger.Info("on error stage", zap.Errors("errors", receiveErrs))
	taskMeta := &TaskMeta{}
	err := json.Unmarshal(gTask.Meta, taskMeta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	errStrs := make([]string, 0, len(receiveErrs))
	for _, receiveErr := range receiveErrs {
		errStrs = append(errStrs, receiveErr.Error())
	}
	if err = dsp.failJob(ctx, handle, gTask, taskMeta, logger, strings.Join(errStrs, "; ")); err != nil {
		return nil, err
	}

	gTask.Error = receiveErrs[0]

	errStr := receiveErrs[0].Error()
	// do nothing if the error is resumable
	if isResumableErr(errStr) {
		return nil, nil
	}

	if gTask.Step == StepImport {
		err = rollback(ctx, handle, gTask, logger)
		if err != nil {
			// TODO: add error code according to spec.
			gTask.Error = errors.New(errStr + ", " + err.Error())
		}
	}
	return nil, err
}

// GetEligibleInstances implements dispatcher.Extension interface.
func (*ImportDispatcherExt) GetEligibleInstances(ctx context.Context, gTask *proto.Task) ([]*infosync.ServerInfo, error) {
	taskMeta := &TaskMeta{}
	err := json.Unmarshal(gTask.Meta, taskMeta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(taskMeta.EligibleInstances) > 0 {
		return taskMeta.EligibleInstances, nil
	}
	return dispatcher.GenerateSchedulerNodes(ctx)
}

// IsRetryableErr implements dispatcher.Extension interface.
func (*ImportDispatcherExt) IsRetryableErr(error) bool {
	// TODO: check whether the error is retryable.
	return false
}

// GetNextStep implements dispatcher.Extension interface.
func (dsp *ImportDispatcherExt) GetNextStep(_ dispatcher.TaskHandle, task *proto.Task) proto.Step {
	switch task.Step {
	case proto.StepInit:
		if dsp.GlobalSort {
			return StepEncodeAndSort
		}
		return StepImport
	case StepEncodeAndSort:
		return StepMergeSort
	case StepMergeSort:
		return StepWriteAndIngest
	case StepImport, StepWriteAndIngest:
		return StepPostProcess
	default:
		// current step must be StepPostProcess
		return proto.StepDone
	}
}

func (dsp *ImportDispatcherExt) switchTiKV2NormalMode(ctx context.Context, task *proto.Task, logger *zap.Logger) {
	dsp.updateCurrentTask(task)
	if dsp.disableTiKVImportMode.Load() {
		return
	}

	dsp.mu.Lock()
	defer dsp.mu.Unlock()

	pdCli, switcher, err := importer.GetTiKVModeSwitcherWithPDClient(ctx, logger)
	if err != nil {
		logger.Warn("get tikv mode switcher failed", zap.Error(err))
		return
	}
	switcher.ToNormalMode(ctx)
	pdCli.Close()

	// clear it, so next task can switch TiKV mode again.
	dsp.lastSwitchTime.Store(time.Time{})
}

func (dsp *ImportDispatcherExt) updateCurrentTask(task *proto.Task) {
	if dsp.currTaskID.Swap(task.ID) != task.ID {
		taskMeta := &TaskMeta{}
		if err := json.Unmarshal(task.Meta, taskMeta); err == nil {
			// for raftkv2, switch mode in local backend
			dsp.disableTiKVImportMode.Store(taskMeta.Plan.DisableTiKVImportMode || taskMeta.Plan.IsRaftKV2)
		}
	}
}

type importDispatcher struct {
	*dispatcher.BaseDispatcher
}

func newImportDispatcher(ctx context.Context, taskMgr *storage.TaskManager,
	serverID string, task *proto.Task) dispatcher.Dispatcher {
	metrics := metricsManager.getOrCreateMetrics(task.ID)
	subCtx := metric.WithCommonMetric(ctx, metrics)
	dsp := importDispatcher{
		BaseDispatcher: dispatcher.NewBaseDispatcher(subCtx, taskMgr, serverID, task),
	}
	return &dsp
}

func (dsp *importDispatcher) Init() (err error) {
	defer func() {
		if err != nil {
			// if init failed, close is not called, so we need to unregister here.
			metricsManager.unregister(dsp.Task.ID)
		}
	}()
	taskMeta := &TaskMeta{}
	if err = json.Unmarshal(dsp.BaseDispatcher.Task.Meta, taskMeta); err != nil {
		return errors.Annotate(err, "unmarshal task meta failed")
	}

	dsp.BaseDispatcher.Extension = &ImportDispatcherExt{
		GlobalSort: taskMeta.Plan.CloudStorageURI != "",
	}
	return dsp.BaseDispatcher.Init()
}

func (dsp *importDispatcher) Close() {
	metricsManager.unregister(dsp.Task.ID)
	dsp.BaseDispatcher.Close()
}

// nolint:deadcode
func dropTableIndexes(ctx context.Context, handle dispatcher.TaskHandle, taskMeta *TaskMeta, logger *zap.Logger) error {
	tblInfo := taskMeta.Plan.TableInfo
	tableName := common.UniqueTable(taskMeta.Plan.DBName, tblInfo.Name.L)

	remainIndexes, dropIndexes := common.GetDropIndexInfos(tblInfo)
	for _, idxInfo := range dropIndexes {
		sqlStr := common.BuildDropIndexSQL(tableName, idxInfo)
		if err := executeSQL(ctx, handle, logger, sqlStr); err != nil {
			if merr, ok := errors.Cause(err).(*dmysql.MySQLError); ok {
				switch merr.Number {
				case errno.ErrCantDropFieldOrKey, errno.ErrDropIndexNeededInForeignKey:
					remainIndexes = append(remainIndexes, idxInfo)
					logger.Warn("can't drop index, skip", zap.String("index", idxInfo.Name.O), zap.Error(err))
					continue
				}
			}
			return err
		}
	}
	if len(remainIndexes) < len(tblInfo.Indices) {
		taskMeta.Plan.TableInfo = taskMeta.Plan.TableInfo.Clone()
		taskMeta.Plan.TableInfo.Indices = remainIndexes
	}
	return nil
}

// nolint:deadcode
func createTableIndexes(ctx context.Context, executor storage.SessionExecutor, taskMeta *TaskMeta, logger *zap.Logger) error {
	tableName := common.UniqueTable(taskMeta.Plan.DBName, taskMeta.Plan.TableInfo.Name.L)
	singleSQL, multiSQLs := common.BuildAddIndexSQL(tableName, taskMeta.Plan.TableInfo, taskMeta.Plan.DesiredTableInfo)
	logger.Info("build add index sql", zap.String("singleSQL", singleSQL), zap.Strings("multiSQLs", multiSQLs))
	if len(multiSQLs) == 0 {
		return nil
	}

	err := executeSQL(ctx, executor, logger, singleSQL)
	if err == nil {
		return nil
	}
	if !common.IsDupKeyError(err) {
		// TODO: refine err msg and error code according to spec.
		return errors.Errorf("Failed to create index: %v, please execute the SQL manually, sql: %s", err, singleSQL)
	}
	if len(multiSQLs) == 1 {
		return nil
	}
	logger.Warn("cannot add all indexes in one statement, try to add them one by one", zap.Strings("sqls", multiSQLs), zap.Error(err))

	for i, ddl := range multiSQLs {
		err := executeSQL(ctx, executor, logger, ddl)
		if err != nil && !common.IsDupKeyError(err) {
			// TODO: refine err msg and error code according to spec.
			return errors.Errorf("Failed to create index: %v, please execute the SQLs manually, sqls: %s", err, strings.Join(multiSQLs[i:], ";"))
		}
	}
	return nil
}

// TODO: return the result of sql.
func executeSQL(ctx context.Context, executor storage.SessionExecutor, logger *zap.Logger, sql string, args ...interface{}) (err error) {
	logger.Info("execute sql", zap.String("sql", sql), zap.Any("args", args))
	return executor.WithNewSession(func(se sessionctx.Context) error {
		_, err := se.(sqlexec.SQLExecutor).ExecuteInternal(ctx, sql, args...)
		return err
	})
}

func updateMeta(gTask *proto.Task, taskMeta *TaskMeta) error {
	bs, err := json.Marshal(taskMeta)
	if err != nil {
		return errors.Trace(err)
	}
	gTask.Meta = bs

	return nil
}

// todo: converting back and forth, we should unify struct and remove this function later.
func toChunkMap(engineCheckpoints map[int32]*checkpoints.EngineCheckpoint) map[int32][]Chunk {
	chunkMap := make(map[int32][]Chunk, len(engineCheckpoints))
	for id, ecp := range engineCheckpoints {
		chunkMap[id] = make([]Chunk, 0, len(ecp.Chunks))
		for _, chunkCheckpoint := range ecp.Chunks {
			chunkMap[id] = append(chunkMap[id], toChunk(*chunkCheckpoint))
		}
	}
	return chunkMap
}

func getStepOfEncode(globalSort bool) proto.Step {
	if globalSort {
		return StepEncodeAndSort
	}
	return StepImport
}

// we will update taskMeta in place and make gTask.Meta point to the new taskMeta.
func updateResult(handle dispatcher.TaskHandle, gTask *proto.Task, taskMeta *TaskMeta, globalSort bool) error {
	stepOfEncode := getStepOfEncode(globalSort)
	metas, err := handle.GetPreviousSubtaskMetas(gTask.ID, stepOfEncode)
	if err != nil {
		return err
	}

	subtaskMetas := make([]*ImportStepMeta, 0, len(metas))
	for _, bs := range metas {
		var subtaskMeta ImportStepMeta
		if err := json.Unmarshal(bs, &subtaskMeta); err != nil {
			return errors.Trace(err)
		}
		subtaskMetas = append(subtaskMetas, &subtaskMeta)
	}
	columnSizeMap := make(map[int64]int64)
	for _, subtaskMeta := range subtaskMetas {
		taskMeta.Result.LoadedRowCnt += subtaskMeta.Result.LoadedRowCnt
		for key, val := range subtaskMeta.Result.ColSizeMap {
			columnSizeMap[key] += val
		}
	}
	taskMeta.Result.ColSizeMap = columnSizeMap

	if globalSort {
		taskMeta.Result.LoadedRowCnt, err = getLoadedRowCountOnGlobalSort(handle, gTask)
		if err != nil {
			return err
		}
	}

	return updateMeta(gTask, taskMeta)
}

func getLoadedRowCountOnGlobalSort(handle dispatcher.TaskHandle, gTask *proto.Task) (uint64, error) {
	metas, err := handle.GetPreviousSubtaskMetas(gTask.ID, StepWriteAndIngest)
	if err != nil {
		return 0, err
	}

	var loadedRowCount uint64
	for _, bs := range metas {
		var subtaskMeta WriteIngestStepMeta
		if err = json.Unmarshal(bs, &subtaskMeta); err != nil {
			return 0, errors.Trace(err)
		}
		loadedRowCount += subtaskMeta.Result.LoadedRowCnt
	}
	return loadedRowCount, nil
}

func startJob(ctx context.Context, logger *zap.Logger, taskHandle dispatcher.TaskHandle, taskMeta *TaskMeta, jobStep string) error {
	failpoint.Inject("syncBeforeJobStarted", func() {
		TestSyncChan <- struct{}{}
		<-TestSyncChan
	})
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	// we consider all errors as retryable errors, except context done.
	// the errors include errors happened when communicate with PD and TiKV.
	// we didn't consider system corrupt cases like system table dropped/altered.
	backoffer := backoff.NewExponential(dispatcher.RetrySQLInterval, 2, dispatcher.RetrySQLMaxInterval)
	err := handle.RunWithRetry(ctx, dispatcher.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, taskHandle.WithNewSession(func(se sessionctx.Context) error {
				exec := se.(sqlexec.SQLExecutor)
				return importer.StartJob(ctx, exec, taskMeta.JobID, jobStep)
			})
		},
	)
	failpoint.Inject("syncAfterJobStarted", func() {
		TestSyncChan <- struct{}{}
	})
	return err
}

func job2Step(ctx context.Context, logger *zap.Logger, taskMeta *TaskMeta, step string) error {
	globalTaskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	// todo: use dispatcher.TaskHandle
	// we might call this in scheduler later, there's no dispatcher.TaskHandle, so we use globalTaskManager here.
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	backoffer := backoff.NewExponential(dispatcher.RetrySQLInterval, 2, dispatcher.RetrySQLMaxInterval)
	return handle.RunWithRetry(ctx, dispatcher.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, globalTaskManager.WithNewSession(func(se sessionctx.Context) error {
				exec := se.(sqlexec.SQLExecutor)
				return importer.Job2Step(ctx, exec, taskMeta.JobID, step)
			})
		},
	)
}

func (dsp *ImportDispatcherExt) finishJob(ctx context.Context, logger *zap.Logger,
	taskHandle dispatcher.TaskHandle, gTask *proto.Task, taskMeta *TaskMeta) error {
	dsp.unregisterTask(ctx, gTask)
	summary := &importer.JobSummary{ImportedRows: taskMeta.Result.LoadedRowCnt}
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	backoffer := backoff.NewExponential(dispatcher.RetrySQLInterval, 2, dispatcher.RetrySQLMaxInterval)
	return handle.RunWithRetry(ctx, dispatcher.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, taskHandle.WithNewSession(func(se sessionctx.Context) error {
				exec := se.(sqlexec.SQLExecutor)
				return importer.FinishJob(ctx, exec, taskMeta.JobID, summary)
			})
		},
	)
}

func (dsp *ImportDispatcherExt) failJob(ctx context.Context, taskHandle dispatcher.TaskHandle, gTask *proto.Task,
	taskMeta *TaskMeta, logger *zap.Logger, errorMsg string) error {
	dsp.switchTiKV2NormalMode(ctx, gTask, logger)
	dsp.unregisterTask(ctx, gTask)
	// retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
	backoffer := backoff.NewExponential(dispatcher.RetrySQLInterval, 2, dispatcher.RetrySQLMaxInterval)
	return handle.RunWithRetry(ctx, dispatcher.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, taskHandle.WithNewSession(func(se sessionctx.Context) error {
				exec := se.(sqlexec.SQLExecutor)
				return importer.FailJob(ctx, exec, taskMeta.JobID, errorMsg)
			})
		},
	)
}

func redactSensitiveInfo(gTask *proto.Task, taskMeta *TaskMeta) {
	taskMeta.Stmt = ""
	taskMeta.Plan.Path = ast.RedactURL(taskMeta.Plan.Path)
	if taskMeta.Plan.CloudStorageURI != "" {
		taskMeta.Plan.CloudStorageURI = ast.RedactURL(taskMeta.Plan.CloudStorageURI)
	}
	if err := updateMeta(gTask, taskMeta); err != nil {
		// marshal failed, should not happen
		logutil.BgLogger().Warn("failed to update task meta", zap.Error(err))
	}
}

// isResumableErr checks whether it's possible to rely on checkpoint to re-import data after the error has been fixed.
func isResumableErr(string) bool {
	// TODO: add more cases
	return false
}

func rollback(ctx context.Context, handle dispatcher.TaskHandle, gTask *proto.Task, logger *zap.Logger) (err error) {
	taskMeta := &TaskMeta{}
	err = json.Unmarshal(gTask.Meta, taskMeta)
	if err != nil {
		return errors.Trace(err)
	}

	logger.Info("rollback")

	//	// TODO: create table indexes depends on the option.
	//	// create table indexes even if the rollback is failed.
	//	defer func() {
	//		err2 := createTableIndexes(ctx, handle, taskMeta, logger)
	//		err = multierr.Append(err, err2)
	//	}()

	tableName := common.UniqueTable(taskMeta.Plan.DBName, taskMeta.Plan.TableInfo.Name.L)
	// truncate the table
	return executeSQL(ctx, handle, logger, "TRUNCATE "+tableName)
}

func stepStr(step proto.Step) string {
	switch step {
	case proto.StepInit:
		return "init"
	case StepImport:
		return "import"
	case StepPostProcess:
		return "post-process"
	case StepEncodeAndSort:
		return "encode&sort"
	case StepMergeSort:
		return "merge-sort"
	case StepWriteAndIngest:
		return "write&ingest"
	case proto.StepDone:
		return "done"
	default:
		return "unknown"
	}
}

func init() {
	dispatcher.RegisterDispatcherFactory(proto.ImportInto, newImportDispatcher)
}
