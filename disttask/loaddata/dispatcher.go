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

package loaddata

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
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/etcd"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
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
	logger := logutil.BgLogger().With(zap.Int64("task_id", t.taskID))
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
	logger := logutil.BgLogger().With(zap.Int64("task_id", t.taskID))
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

type flowHandle struct {
	mu sync.RWMutex
	// NOTE: there's no need to sync for below 2 fields actually, since we add a restriction that only one
	// task can be running at a time. but we might support task queuing in the future, leave it for now.
	// the last time we switch TiKV into IMPORT mode, this is a global operation, do it for one task makes
	// no difference to do it for all tasks. So we do not need to record the switch time for each task.
	lastSwitchTime atomic.Time
	// taskInfoMap is a map from taskID to taskInfo
	taskInfoMap sync.Map
}

var _ dispatcher.TaskFlowHandle = (*flowHandle)(nil)

func (h *flowHandle) OnTicker(ctx context.Context, task *proto.Task) {
	// only switch TiKV mode or register task when task is running
	if task.State != proto.TaskStateRunning {
		return
	}
	h.switchTiKVMode(ctx, task)
	h.registerTask(ctx, task)
}

func (h *flowHandle) switchTiKVMode(ctx context.Context, task *proto.Task) {
	if time.Since(h.lastSwitchTime.Load()) < config.DefaultSwitchTiKVModeInterval {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if time.Since(h.lastSwitchTime.Load()) < config.DefaultSwitchTiKVModeInterval {
		return
	}

	logger := logutil.BgLogger().With(zap.Int64("task_id", task.ID))
	switcher, err := importer.GetTiKVModeSwitcher(logger)
	if err != nil {
		logger.Warn("get tikv mode switcher failed", zap.Error(err))
		return
	}
	switcher.ToImportMode(ctx)
	h.lastSwitchTime.Store(time.Now())
}

func (h *flowHandle) registerTask(ctx context.Context, task *proto.Task) {
	val, _ := h.taskInfoMap.LoadOrStore(task.ID, &taskInfo{taskID: task.ID})
	info := val.(*taskInfo)
	info.register(ctx)
}

func (h *flowHandle) unregisterTask(ctx context.Context, task *proto.Task) {
	if val, loaded := h.taskInfoMap.LoadAndDelete(task.ID); loaded {
		info := val.(*taskInfo)
		info.close(ctx)
	}
}

func (h *flowHandle) ProcessNormalFlow(ctx context.Context, handle dispatcher.TaskHandle, gTask *proto.Task) (_ [][]byte, err error) {
	logger := logutil.BgLogger().With(zap.String("component", "dispatcher"), zap.String("type", gTask.Type), zap.Int64("ID", gTask.ID))
	taskMeta := &TaskMeta{}
	err = json.Unmarshal(gTask.Meta, taskMeta)
	if err != nil {
		return nil, err
	}
	logger.Info("process normal flow", zap.Any("task_meta", taskMeta), zap.Any("step", gTask.Step))

	switch gTask.Step {
	case proto.StepInit:
		if err := preProcess(ctx, handle, gTask, taskMeta, logger); err != nil {
			return nil, err
		}
		if err = startJob(ctx, handle, taskMeta); err != nil {
			return nil, err
		}
		subtaskMetas, err := generateSubtaskMetas(ctx, taskMeta)
		if err != nil {
			return nil, err
		}
		logger.Info("generate subtasks", zap.Any("subtask_metas", subtaskMetas))
		metaBytes := make([][]byte, 0, len(subtaskMetas))
		for _, subtaskMeta := range subtaskMetas {
			bs, err := json.Marshal(subtaskMeta)
			if err != nil {
				return nil, err
			}
			metaBytes = append(metaBytes, bs)
		}
		gTask.Step = Import
		return metaBytes, nil
	case Import:
		defer func() {
			if err == nil {
				err = finishJob(ctx, handle, taskMeta, &importer.JobSummary{ImportedRows: taskMeta.Result.LoadedRowCnt})
			} else {
				// todo: we're not running in a transaction with task update, there might be case
				// failJob return error, but task update succeed.
				if err2 := failJob(ctx, handle, taskMeta, err.Error()); err2 != nil {
					logger.Error("call failJob failed", zap.Error(err2))
				}
			}
		}()
		h.switchTiKV2NormalMode(ctx, logutil.BgLogger())
		defer h.unregisterTask(ctx, gTask)
		subtaskMetas, err2 := postProcess(ctx, handle, gTask, taskMeta, logger)
		if err2 != nil {
			return nil, err2
		}
		updateResult(taskMeta, subtaskMetas, logger)
		if err2 = updateMeta(gTask, taskMeta); err2 != nil {
			return nil, err2
		}
		gTask.State = proto.TaskStateSucceed
		return nil, nil
	default:
		return nil, errors.Errorf("unknown step %d", gTask.Step)
	}
}

func (h *flowHandle) ProcessErrFlow(ctx context.Context, handle dispatcher.TaskHandle, gTask *proto.Task, receiveErr [][]byte) ([]byte, error) {
	logger := logutil.BgLogger().With(zap.String("component", "dispatcher"), zap.String("type", gTask.Type), zap.Int64("ID", gTask.ID))
	logger.Info("process error flow", zap.ByteStrings("error message", receiveErr))
	taskMeta := &TaskMeta{}
	err := json.Unmarshal(gTask.Meta, taskMeta)
	if err != nil {
		return nil, err
	}
	errStrs := make([]string, 0, len(receiveErr))
	for _, errStr := range receiveErr {
		errStrs = append(errStrs, string(errStr))
	}
	if err = failJob(ctx, handle, taskMeta, strings.Join(errStrs, "; ")); err != nil {
		return nil, err
	}
	h.switchTiKV2NormalMode(ctx, logger)
	h.unregisterTask(ctx, gTask)

	gTask.Error = receiveErr[0]

	errStr := string(receiveErr[0])
	// do nothing if the error is resumable
	if isResumableErr(errStr) {
		return nil, nil
	}

	// Actually, `processErrFlow` will only be called when there is a failure in the import step.
	if gTask.Step != Import {
		return nil, nil
	}

	err = rollback(ctx, handle, gTask, logger)
	if err != nil {
		// TODO: add error code according to spec.
		gTask.Error = []byte(errStr + ", " + err.Error())
	}
	return nil, err
}

func (*flowHandle) GetEligibleInstances(ctx context.Context, gTask *proto.Task) ([]*infosync.ServerInfo, error) {
	taskMeta := &TaskMeta{}
	err := json.Unmarshal(gTask.Meta, taskMeta)
	if err != nil {
		return nil, err
	}
	if len(taskMeta.EligibleInstances) > 0 {
		return taskMeta.EligibleInstances, nil
	}
	return dispatcher.GenerateSchedulerNodes(ctx)
}

func (*flowHandle) IsRetryableErr(error) bool {
	// TODO: check whether the error is retryable.
	return false
}

func (h *flowHandle) switchTiKV2NormalMode(ctx context.Context, logger *zap.Logger) {
	h.mu.Lock()
	defer h.mu.Unlock()

	switcher, err := importer.GetTiKVModeSwitcher(logger)
	if err != nil {
		logger.Warn("get tikv mode switcher failed", zap.Error(err))
		return
	}
	switcher.ToNormalMode(ctx)

	// clear it, so next task can switch TiKV mode again.
	h.lastSwitchTime.Store(time.Time{})
}

// preProcess does the pre processing for the task.
func preProcess(ctx context.Context, handle dispatcher.TaskHandle, gTask *proto.Task, taskMeta *TaskMeta, logger *zap.Logger) error {
	logger.Info("pre process", zap.Any("table_info", taskMeta.Plan.TableInfo))
	if err := dropTableIndexes(ctx, handle, taskMeta, logger); err != nil {
		return err
	}
	return updateMeta(gTask, taskMeta)
}

// postProcess does the post-processing for the task.
func postProcess(ctx context.Context, handle dispatcher.TaskHandle, gTask *proto.Task, taskMeta *TaskMeta, logger *zap.Logger) (_ []*SubtaskMeta, err error) {
	// create table indexes even if the post process is failed.
	defer func() {
		err2 := createTableIndexes(ctx, handle, taskMeta, logger)
		err = multierr.Append(err, err2)
	}()
	if err = job2Step(ctx, taskMeta, importer.JobStepValidating); err != nil {
		return nil, err
	}

	controller, err := buildController(taskMeta)
	if err != nil {
		return nil, err
	}
	// no need and should not call controller.InitDataFiles, files might not exist on this instance.

	metas, err := handle.GetPreviousSubtaskMetas(gTask.ID, gTask.Step)
	if err != nil {
		return nil, err
	}

	subtaskMetas := make([]*SubtaskMeta, 0, len(metas))
	for _, bs := range metas {
		var subtaskMeta SubtaskMeta
		if err := json.Unmarshal(bs, &subtaskMeta); err != nil {
			return nil, err
		}
		subtaskMetas = append(subtaskMetas, &subtaskMeta)
	}

	logger.Info("post process", zap.Any("task_meta", taskMeta), zap.Any("subtask_metas", subtaskMetas))
	if err := verifyChecksum(ctx, controller, subtaskMetas, logger); err != nil {
		return nil, err
	}

	return subtaskMetas, nil
}

func verifyChecksum(ctx context.Context, controller *importer.LoadDataController, subtaskMetas []*SubtaskMeta, logger *zap.Logger) error {
	if controller.Checksum == config.OpLevelOff {
		return nil
	}
	var localChecksum verify.KVChecksum
	for _, subtaskMeta := range subtaskMetas {
		checksum := verify.MakeKVChecksum(subtaskMeta.Checksum.Size, subtaskMeta.Checksum.KVs, subtaskMeta.Checksum.Sum)
		localChecksum.Add(&checksum)
	}
	logger.Info("local checksum", zap.Object("checksum", &localChecksum))
	return controller.VerifyChecksum(ctx, localChecksum)
}

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

func createTableIndexes(ctx context.Context, handle dispatcher.TaskHandle, taskMeta *TaskMeta, logger *zap.Logger) error {
	tableName := common.UniqueTable(taskMeta.Plan.DBName, taskMeta.Plan.TableInfo.Name.L)
	singleSQL, multiSQLs := common.BuildAddIndexSQL(tableName, taskMeta.Plan.TableInfo, taskMeta.Plan.DesiredTableInfo)
	logger.Info("build add index sql", zap.String("singleSQL", singleSQL), zap.Strings("multiSQLs", multiSQLs))
	if len(multiSQLs) == 0 {
		return nil
	}

	err := executeSQL(ctx, handle, logger, singleSQL)
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
		err := executeSQL(ctx, handle, logger, ddl)
		if err != nil && !common.IsDupKeyError(err) {
			// TODO: refine err msg and error code according to spec.
			return errors.Errorf("Failed to create index: %v, please execute the SQLs manually, sqls: %s", err, strings.Join(multiSQLs[i:], ";"))
		}
	}
	return nil
}

// TODO: return the result of sql.
func executeSQL(ctx context.Context, handle dispatcher.TaskHandle, logger *zap.Logger, sql string, args ...interface{}) (err error) {
	logger.Info("execute sql", zap.String("sql", sql), zap.Any("args", args))
	return handle.ExecInNewSession(func(se sessionctx.Context) error {
		_, err := se.(sqlexec.SQLExecutor).ExecuteInternal(ctx, sql, args...)
		return err
	})
}

func updateResult(taskMeta *TaskMeta, subtaskMetas []*SubtaskMeta, logger *zap.Logger) {
	columnSizeMap := make(map[int64]int64)
	for _, subtaskMeta := range subtaskMetas {
		taskMeta.Result.ReadRowCnt += subtaskMeta.Result.ReadRowCnt
		taskMeta.Result.LoadedRowCnt += subtaskMeta.Result.LoadedRowCnt
		for key, val := range subtaskMeta.Result.ColSizeMap {
			columnSizeMap[key] += val
		}
	}
	taskMeta.Result.ColSizeMap = columnSizeMap
	logger.Info("update result", zap.Any("task_meta", taskMeta))
}

func updateMeta(gTask *proto.Task, taskMeta *TaskMeta) error {
	bs, err := json.Marshal(taskMeta)
	if err != nil {
		return err
	}
	gTask.Meta = bs
	return nil
}

func buildController(taskMeta *TaskMeta) (*importer.LoadDataController, error) {
	idAlloc := kv.NewPanickingAllocators(0)
	tbl, err := tables.TableFromMeta(idAlloc, taskMeta.Plan.TableInfo)
	if err != nil {
		return nil, err
	}

	astArgs, err := importer.ASTArgsFromStmt(taskMeta.Stmt)
	if err != nil {
		return nil, err
	}
	controller, err := importer.NewLoadDataController(&taskMeta.Plan, tbl, astArgs)
	if err != nil {
		return nil, err
	}
	return controller, nil
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

func generateSubtaskMetas(ctx context.Context, taskMeta *TaskMeta) (subtaskMetas []*SubtaskMeta, err error) {
	var chunkMap map[int32][]Chunk
	if len(taskMeta.ChunkMap) > 0 {
		chunkMap = taskMeta.ChunkMap
	} else {
		controller, err2 := buildController(taskMeta)
		if err2 != nil {
			return nil, err2
		}
		if err2 = controller.InitDataFiles(ctx); err2 != nil {
			return nil, err2
		}

		engineCheckpoints, err2 := controller.PopulateChunks(ctx)
		if err2 != nil {
			return nil, err2
		}
		chunkMap = toChunkMap(engineCheckpoints)
	}
	for id := range chunkMap {
		if id == common.IndexEngineID {
			continue
		}
		subtaskMeta := &SubtaskMeta{
			ID:     id,
			Plan:   taskMeta.Plan,
			Chunks: chunkMap[id],
		}
		subtaskMetas = append(subtaskMetas, subtaskMeta)
	}
	return subtaskMetas, nil
}

func startJob(ctx context.Context, handle dispatcher.TaskHandle, taskMeta *TaskMeta) error {
	failpoint.Inject("syncBeforeJobStarted", func() {
		TestSyncChan <- struct{}{}
		<-TestSyncChan
	})
	err := handle.ExecInNewSession(func(se sessionctx.Context) error {
		exec := se.(sqlexec.SQLExecutor)
		return importer.StartJob(ctx, exec, taskMeta.JobID)
	})
	failpoint.Inject("syncAfterJobStarted", func() {
		TestSyncChan <- struct{}{}
	})
	return err
}

func job2Step(ctx context.Context, taskMeta *TaskMeta, step string) error {
	globalTaskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	// todo: use dispatcher.TaskHandle
	// we might call this in scheduler later, there's no dispatcher.TaskHandle, so we use globalTaskManager here.
	return globalTaskManager.WithNewSession(func(se sessionctx.Context) error {
		exec := se.(sqlexec.SQLExecutor)
		return importer.Job2Step(ctx, exec, taskMeta.JobID, step)
	})
}

func finishJob(ctx context.Context, handle dispatcher.TaskHandle, taskMeta *TaskMeta, summary *importer.JobSummary) error {
	return handle.ExecInNewSession(func(se sessionctx.Context) error {
		exec := se.(sqlexec.SQLExecutor)
		return importer.FinishJob(ctx, exec, taskMeta.JobID, summary)
	})
}

func failJob(ctx context.Context, handle dispatcher.TaskHandle, taskMeta *TaskMeta, errorMsg string) error {
	return handle.ExecInNewSession(func(se sessionctx.Context) error {
		exec := se.(sqlexec.SQLExecutor)
		return importer.FailJob(ctx, exec, taskMeta.JobID, errorMsg)
	})
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
		return err
	}

	logger.Info("rollback", zap.Any("task_meta", taskMeta))

	// create table indexes even if the rollback is failed.
	defer func() {
		err2 := createTableIndexes(ctx, handle, taskMeta, logger)
		err = multierr.Append(err, err2)
	}()

	tableName := common.UniqueTable(taskMeta.Plan.DBName, taskMeta.Plan.TableInfo.Name.L)
	// truncate the table
	return executeSQL(ctx, handle, logger, "TRUNCATE "+tableName)
}

func init() {
	dispatcher.RegisterTaskFlowHandle(proto.ImportInto, &flowHandle{})
}
