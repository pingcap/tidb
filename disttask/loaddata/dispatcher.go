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
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// FlowHandle is the dispatcher for load data.
type FlowHandle struct{}

// ProcessNormalFlow implements dispatcher.TaskFlowHandle interface.
func (*FlowHandle) ProcessNormalFlow(ctx context.Context, _ dispatcher.TaskHandle, gTask *proto.Task) ([][]byte, error) {
	taskMeta := &TaskMeta{}
	err := json.Unmarshal(gTask.Meta, taskMeta)
	if err != nil {
		return nil, err
	}
	logutil.BgLogger().Info("process normal flow", zap.Any("task_meta", taskMeta), zap.Any("step", gTask.Step))

	switch gTask.Step {
	case Import:
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
	logutil.BgLogger().Info("generate subtasks", zap.Any("subtask_metas", subtaskMetas))
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
	logutil.BgLogger().Info("process error flow", zap.ByteStrings("error message", receiveErr))
	gTask.Error = receiveErr[0]
	return nil, nil
}

func generateSubtaskMetas(ctx context.Context, taskMeta *TaskMeta) (subtaskMetas []*SubtaskMeta, err error) {
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

	tableImporter, err := importer.NewTableImporter(&importer.JobImportParam{
		GroupCtx: ctx,
		Progress: asyncloaddata.NewProgress(controller.ImportMode == importer.LogicalImportMode),
		Job: &asyncloaddata.Job{
			ID: taskMeta.JobID,
		},
	}, controller)
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
