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
	"sync"
	"time"

	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/logutil"
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
		if err := postProcess(ctx, handle, gTask, logger); err != nil {
			return nil, err
		}
		gTask.State = proto.TaskStateSucceed
		return nil, nil
	default:
	}

	//	schedulers, err := dispatch.GetAllSchedulerIDs(ctx, gTask.ID)
	//	if err != nil {
	//		return nil, err
	//	}
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
}

func (h *flowHandle) ProcessErrFlow(ctx context.Context, _ dispatcher.TaskHandle, gTask *proto.Task, receiveErr [][]byte) ([]byte, error) {
	logger := logutil.BgLogger().With(zap.String("component", "dispatcher"), zap.String("type", gTask.Type), zap.Int64("ID", gTask.ID))
	logger.Info("process error flow", zap.ByteStrings("error message", receiveErr))
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
func postProcess(ctx context.Context, handle dispatcher.TaskHandle, gTask *proto.Task, logger *zap.Logger) error {
	taskMeta := &TaskMeta{}
	err := json.Unmarshal(gTask.Meta, taskMeta)
	if err != nil {
		return err
	}
	controller, err := buildController(taskMeta)
	if err != nil {
		return err
	}
	// no need and should not call controller.InitDataFiles, files might not exist on this instance.

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
	if err := verifyChecksum(ctx, controller, subtaskMetas, logger); err != nil {
		return err
	}

	updateResult(taskMeta, subtaskMetas, logger)
	return updateMeta(gTask, taskMeta)
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
		if err = controller.InitDataFiles(ctx); err != nil {
			return nil, err
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

func init() {
	dispatcher.RegisterTaskFlowHandle(proto.LoadData, &flowHandle{})
}
