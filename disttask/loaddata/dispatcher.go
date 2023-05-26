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

	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// FlowHandle is the dispatcher for load data.
type FlowHandle struct{}

var _ dispatcher.TaskFlowHandle = (*FlowHandle)(nil)

// ProcessNormalFlow implements dispatcher.TaskFlowHandle interface.
func (*FlowHandle) ProcessNormalFlow(ctx context.Context, handle dispatcher.TaskHandle, gTask *proto.Task) ([][]byte, error) {
	logger := logutil.BgLogger().With(zap.String("component", "dispatcher"), zap.String("type", gTask.Type), zap.Int64("ID", gTask.ID))
	taskMeta := &TaskMeta{}
	err := json.Unmarshal(gTask.Meta, taskMeta)
	if err != nil {
		return nil, err
	}
	logger.Info("process normal flow", zap.Any("task_meta", taskMeta), zap.Any("step", gTask.Step))

	switch gTask.Step {
	case Import:
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

// ProcessErrFlow implements dispatcher.ProcessErrFlow interface.
func (*FlowHandle) ProcessErrFlow(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task, receiveErr [][]byte) ([]byte, error) {
	logger := logutil.BgLogger().With(zap.String("component", "dispatcher"), zap.String("type", gTask.Type), zap.Int64("ID", gTask.ID))
	logger.Info("process error flow", zap.ByteStrings("error message", receiveErr))
	gTask.Error = receiveErr[0]
	return nil, nil
}

// GetEligibleInstances implements dispatcher.TaskFlowHandle interface.
func (*FlowHandle) GetEligibleInstances(ctx context.Context, gTask *proto.Task) ([]*infosync.ServerInfo, error) {
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

// IsRetryableErr implements dispatcher.IsRetryableErr interface.
func (*FlowHandle) IsRetryableErr(error) bool {
	// TODO: check whether the error is retryable.
	return false
}

// postProcess does the post processing for the task.
func postProcess(ctx context.Context, handle dispatcher.TaskHandle, gTask *proto.Task, logger *zap.Logger) error {
	taskMeta := &TaskMeta{}
	err := json.Unmarshal(gTask.Meta, taskMeta)
	if err != nil {
		return err
	}
	tableImporter, err := buildTableImporter(ctx, taskMeta)
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

func buildTableImporter(ctx context.Context, taskMeta *TaskMeta) (*importer.TableImporter, error) {
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
		Job: &asyncloaddata.Job{
			ID: taskMeta.JobID,
		},
	}, controller)
}

func generateSubtaskMetas(ctx context.Context, taskMeta *TaskMeta) (subtaskMetas []*SubtaskMeta, err error) {
	tableImporter, err := buildTableImporter(ctx, taskMeta)
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

func init() {
	dispatcher.RegisterTaskFlowHandle(proto.LoadData, &FlowHandle{})
}
