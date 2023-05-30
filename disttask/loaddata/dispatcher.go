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
	"strings"
	"sync"
	"time"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type flowHandle struct {
	mu sync.RWMutex
	// the last time we switch TiKV into IMPORT mode, this is a global operation, do it for one task makes
	// no difference to do it for all tasks. So we do not need to record the switch time for each task.
	lastSwitchTime atomic.Time
}

var _ dispatcher.TaskFlowHandle = (*flowHandle)(nil)

func (h *flowHandle) OnTicker(ctx context.Context, task *proto.Task) {
	// only switch TiKV mode when task is running and reach the interval
	if task.State != proto.TaskStateRunning || time.Since(h.lastSwitchTime.Load()) < config.DefaultSwitchTiKVModeInterval {
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

func (h *flowHandle) ProcessNormalFlow(ctx context.Context, handle dispatcher.TaskHandle, gTask *proto.Task) ([][]byte, error) {
	logger := logutil.BgLogger().With(zap.String("component", "dispatcher"), zap.String("type", gTask.Type), zap.Int64("ID", gTask.ID))
	taskMeta := &TaskMeta{}
	err := json.Unmarshal(gTask.Meta, taskMeta)
	if err != nil {
		return nil, err
	}
	logger.Info("process normal flow", zap.Any("task_meta", taskMeta), zap.Any("step", gTask.Step))

	switch gTask.Step {
	case Import:
		h.switchTiKV2NormalMode(ctx, logutil.BgLogger())
		if err := postProcess(ctx, handle, gTask, taskMeta, logger); err != nil {
			return nil, err
		}
		gTask.State = proto.TaskStateSucceed
		return nil, nil
	default:
	}

	if err := preProcess(ctx, handle, gTask, taskMeta, logger); err != nil {
		return nil, err
	}

	//	schedulers, err := dispatch.GetAllSchedulerIDs(ctx, gTask.ID)
	//	if err != nil {
	//		return nil, err
	//	}
	subtaskMetas, err := generateSubtaskMetas(ctx, gTask.ID, taskMeta)
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
}

func (h *flowHandle) ProcessErrFlow(ctx context.Context, handle dispatcher.TaskHandle, gTask *proto.Task, receiveErr [][]byte) ([]byte, error) {
	logger := logutil.BgLogger().With(zap.String("component", "dispatcher"), zap.String("type", gTask.Type), zap.Int64("ID", gTask.ID))
	logger.Info("process error flow", zap.ByteStrings("error message", receiveErr))
	h.switchTiKV2NormalMode(ctx, logger)
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

	err := rollback(ctx, handle, gTask, logger)
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
	logger.Info("pre process", zap.Any("task_meta", taskMeta))
	if err := dropTableIndexes(ctx, handle, taskMeta, logger); err != nil {
		return err
	}
	return updateMeta(gTask, taskMeta)
}

// postProcess does the post processing for the task.
func postProcess(ctx context.Context, handle dispatcher.TaskHandle, gTask *proto.Task, taskMeta *TaskMeta, logger *zap.Logger) (err error) {
	// create table indexes even if the post process is failed.
	defer func() {
		err2 := createTableIndexes(ctx, handle, taskMeta, logger)
		err = multierr.Append(err, err2)
	}()

	tableImporter, err := buildTableImporter(ctx, gTask.ID, taskMeta)
	if err != nil {
		return err
	}
	defer func() {
		err2 := tableImporter.Close()
		if err == nil {
			err = err2
		}
	}()

	metas, err := handle.GetPreviousSubtaskMetas(gTask.ID, gTask.Step)
	if err != nil {
		return err
	}

	subtaskMetas := make([]*SubtaskMeta, 0, len(metas))
	for _, bs := range metas {
		var subtaskMeta SubtaskMeta
		if err := json.Unmarshal(bs, &subtaskMeta); err != nil {
			return err
		}
		subtaskMetas = append(subtaskMetas, &subtaskMeta)
	}

	logger.Info("post process", zap.Any("task_meta", taskMeta), zap.Any("subtask_metas", subtaskMetas))
	if err := verifyChecksum(ctx, tableImporter, subtaskMetas, logger); err != nil {
		return err
	}

	updateResult(taskMeta, subtaskMetas, logger)
	return updateMeta(gTask, taskMeta)
}

func verifyChecksum(ctx context.Context, tableImporter *importer.TableImporter, subtaskMetas []*SubtaskMeta, logger *zap.Logger) error {
	if tableImporter.Checksum == config.OpLevelOff {
		return nil
	}
	var localChecksum verify.KVChecksum
	for _, subtaskMeta := range subtaskMetas {
		checksum := verify.MakeKVChecksum(subtaskMeta.Checksum.Size, subtaskMeta.Checksum.KVs, subtaskMeta.Checksum.Sum)
		localChecksum.Add(&checksum)
	}
	logger.Info("local checksum", zap.Object("checksum", &localChecksum))
	return tableImporter.VerifyChecksum(ctx, localChecksum)
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
					logger.Info("can't drop index, skip", zap.String("index", idxInfo.Name.O), zap.Error(err))
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
	return handle.WithNewSession(func(se sessionctx.Context) error {
		_, err := se.(sqlexec.SQLExecutor).ExecuteInternal(ctx, sql, args...)
		return err
	})
}

func updateResult(taskMeta *TaskMeta, subtaskMetas []*SubtaskMeta, logger *zap.Logger) {
	for _, subtaskMeta := range subtaskMetas {
		taskMeta.Result.ReadRowCnt += subtaskMeta.Result.ReadRowCnt
		taskMeta.Result.LoadedRowCnt += subtaskMeta.Result.LoadedRowCnt
	}
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

func buildTableImporter(ctx context.Context, taskID int64, taskMeta *TaskMeta) (*importer.TableImporter, error) {
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
	if err := controller.InitDataFiles(ctx); err != nil {
		return nil, err
	}

	return importer.NewTableImporter(&importer.JobImportParam{
		GroupCtx: ctx,
		Progress: asyncloaddata.NewProgress(false),
		Job:      &asyncloaddata.Job{},
	}, controller, taskID)
}

func generateSubtaskMetas(ctx context.Context, taskID int64, taskMeta *TaskMeta) (subtaskMetas []*SubtaskMeta, err error) {
	tableImporter, err := buildTableImporter(ctx, taskID, taskMeta)
	if err != nil {
		return nil, err
	}
	defer func() {
		err2 := tableImporter.Close()
		if err == nil {
			err = err2
		}
	}()

	engineCheckpoints, err := tableImporter.PopulateChunks(ctx)
	if err != nil {
		return nil, err
	}
	for id, ecp := range engineCheckpoints {
		if id == common.IndexEngineID {
			continue
		}
		subtaskMeta := &SubtaskMeta{
			ID:   id,
			Plan: taskMeta.Plan,
		}
		for _, chunkCheckpoint := range ecp.Chunks {
			subtaskMeta.Chunks = append(subtaskMeta.Chunks, toChunk(*chunkCheckpoint))
		}
		subtaskMetas = append(subtaskMetas, subtaskMeta)
	}
	return subtaskMetas, nil
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
	dispatcher.RegisterTaskFlowHandle(proto.LoadData, &flowHandle{})
}
