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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// ImportSubtaskExecutor is a subtask executor for load data.
type ImportSubtaskExecutor struct {
	task *MinimalTaskMeta
}

// Run implements the SubtaskExecutor.Run interface.
func (e *ImportSubtaskExecutor) Run(ctx context.Context) error {
	logutil.BgLogger().Info("subtask executor run", zap.Any("task", e.task))
	chunkCheckpoint := toChunkCheckpoint(e.task.Chunk)
	sharedVars := e.task.SharedVars
	if err := importer.ProcessChunk(ctx, &chunkCheckpoint, sharedVars.TableImporter, sharedVars.DataEngine, sharedVars.IndexEngine, sharedVars.Progress, logutil.BgLogger()); err != nil {
		return err
	}

	sharedVars.mu.Lock()
	defer sharedVars.mu.Unlock()
	sharedVars.Checksum.Add(&chunkCheckpoint.Checksum)
	return nil
}

func init() {
	scheduler.RegisterSubtaskExectorConstructor(
		proto.LoadData,
		// The order of the subtask executors is the same as the order of the subtasks.
		func(minimalTask proto.MinimalTask, step int64) (scheduler.SubtaskExecutor, error) {
			task, ok := minimalTask.(MinimalTaskMeta)
			if !ok {
				return nil, errors.Errorf("invalid task type %T", minimalTask)
			}
			return &ImportSubtaskExecutor{task: &task}, nil
		},
	)
}
