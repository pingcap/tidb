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

	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

// FlowHandle is the dispatcher for load data.
type FlowHandle struct{}

// ProcessNormalFlow implements dispatcher.TaskFlowHandle interface.
func (*FlowHandle) ProcessNormalFlow(ctx context.Context, dispatch dispatcher.TaskHandle, gTask *proto.Task) ([][]byte, error) {
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

	schedulers, err := dispatch.GetAllSchedulerIDs(ctx, gTask.ID)
	if err != nil {
		return nil, err
	}
	subtaskMetas, err := generateSubtaskMetas(ctx, taskMeta, len(schedulers))
	if err != nil {
		return nil, err
	}
	logutil.BgLogger().Info("generate subtasks", zap.Any("subtask_metas", subtaskMetas))
	metaBytes := make([][]byte, 0, len(taskMeta.FileInfos))
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
func (*FlowHandle) ProcessErrFlow(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task, errMsg string) ([]byte, error) {
	logutil.BgLogger().Info("process error flow", zap.String("error message", errMsg))
	return nil, nil
}

func generateSubtaskMetas(ctx context.Context, task *TaskMeta, concurrency int) ([]*SubtaskMeta, error) {
	tableRegions, err := makeTableRegions(ctx, task, concurrency)
	if err != nil {
		return nil, err
	}

	subtaskMetaMap := make(map[int32]*SubtaskMeta)
	for _, region := range tableRegions {
		subtaskMeta, ok := subtaskMetaMap[region.EngineID]
		if !ok {
			subtaskMeta = &SubtaskMeta{
				Table:  task.Table,
				Format: task.Format,
				Dir:    task.Dir,
			}
			subtaskMetaMap[region.EngineID] = subtaskMeta
		}
		subtaskMeta.Chunks = append(subtaskMeta.Chunks, Chunk{
			Path:         region.FileMeta.Path,
			Offset:       region.Chunk.Offset,
			EndOffset:    region.Chunk.EndOffset,
			RealOffset:   region.Chunk.RealOffset,
			PrevRowIDMax: region.Chunk.PrevRowIDMax,
			RowIDMax:     region.Chunk.RowIDMax,
		})
	}
	return maps.Values[map[int32]*SubtaskMeta](subtaskMetaMap), nil
}

func init() {
	dispatcher.RegisterTaskFlowHandle(proto.LoadData, &FlowHandle{})
}
