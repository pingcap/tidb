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

	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/atomic"
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
		subtaskMetas, err2 := postProcess(ctx, handle, gTask, logger)
		if err2 != nil {
			return nil, err2
		}
		updateResult(taskMeta, subtaskMetas, logger)
		if err2 = updateMeta(gTask, taskMeta); err2 != nil {
			return nil, err2
		}
		if err2 = finishJob(ctx, taskMeta, &importer.JobSummary{ImportedRows: taskMeta.Result.LoadedRowCnt}); err2 != nil {
			return nil, err2
		}
		gTask.State = proto.TaskStateSucceed
		return nil, nil
	default:
	}

	//	schedulers, err := dispatch.GetAllSchedulerIDs(ctx, gTask.ID)
	//	if err != nil {
	//		return nil, err
	//	}
	if err = startJob(ctx, taskMeta); err != nil {
		return nil, err
	}
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

func (h *flowHandle) ProcessErrFlow(ctx context.Context, _ dispatcher.TaskHandle, gTask *proto.Task, receiveErr [][]byte) ([]byte, error) {
	logger := logutil.BgLogger().With(zap.String("component", "dispatcher"), zap.String("type", gTask.Type), zap.Int64("ID", gTask.ID))
	logger.Info("process error flow", zap.ByteStrings("error message", receiveErr))
	taskMeta := &TaskMeta{}
	err := json.Unmarshal(gTask.Meta, taskMeta)
	if err != nil {
		return nil, err
	}
	var errStrs []string
	for _, errStr := range receiveErr {
		errStrs = append(errStrs, string(errStr))
	}
	if err = failJob(ctx, taskMeta, strings.Join(errStrs, "; ")); err != nil {
		return nil, err
	}
	h.switchTiKV2NormalMode(ctx, logger)
	gTask.Error = receiveErr[0]
	return nil, nil
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

// postProcess does the post-processing for the task.
func postProcess(ctx context.Context, handle dispatcher.TaskHandle, gTask *proto.Task, logger *zap.Logger) ([]*SubtaskMeta, error) {
	taskMeta := &TaskMeta{}
	err := json.Unmarshal(gTask.Meta, taskMeta)
	if err != nil {
		return nil, err
	}
	if err = job2Step(ctx, taskMeta, importer.JobStepValidating); err != nil {
		return nil, err
	}

	tableImporter, err := buildTableImporter(ctx, gTask.ID, taskMeta)
	if err != nil {
		return nil, err
	}
	defer func() {
		err2 := tableImporter.Close()
		if err == nil {
			err = err2
		}
	}()

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
	if err := verifyChecksum(ctx, tableImporter, subtaskMetas, logger); err != nil {
		return nil, err
	}

	return subtaskMetas, nil
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

func startJob(ctx context.Context, taskMeta *TaskMeta) error {
	globalTaskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	return globalTaskManager.WithNewSession(func(se sessionctx.Context) error {
		exec := se.(sqlexec.SQLExecutor)
		return importer.StartJob(ctx, exec, taskMeta.JobID)
	})
}

func job2Step(ctx context.Context, taskMeta *TaskMeta, step string) error {
	globalTaskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	return globalTaskManager.WithNewSession(func(se sessionctx.Context) error {
		exec := se.(sqlexec.SQLExecutor)
		return importer.Job2Step(ctx, exec, taskMeta.JobID, step)
	})
}

func finishJob(ctx context.Context, taskMeta *TaskMeta, summary *importer.JobSummary) error {
	bytes, err := json.Marshal(summary)
	if err != nil {
		return err
	}
	globalTaskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	return globalTaskManager.WithNewSession(func(se sessionctx.Context) error {
		exec := se.(sqlexec.SQLExecutor)
		return importer.FinishJob(ctx, exec, taskMeta.JobID, string(bytes))
	})
}

func failJob(ctx context.Context, taskMeta *TaskMeta, errorMsg string) error {
	globalTaskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	return globalTaskManager.WithNewSession(func(se sessionctx.Context) error {
		exec := se.(sqlexec.SQLExecutor)
		return importer.FailJob(ctx, exec, taskMeta.JobID, errorMsg)
	})
}

func init() {
	dispatcher.RegisterTaskFlowHandle(proto.ImportInto, &flowHandle{})
}
