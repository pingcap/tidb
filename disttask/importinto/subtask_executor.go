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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// TestSyncChan is used to test.
var TestSyncChan = make(chan struct{})

// ImportMinimalTaskExecutor is a minimal task executor for IMPORT INTO.
type ImportMinimalTaskExecutor struct {
	mTtask *importStepMinimalTask
}

// Run implements the SubtaskExecutor.Run interface.
func (e *ImportMinimalTaskExecutor) Run(ctx context.Context) error {
	logger := logutil.BgLogger().With(zap.String("type", proto.ImportInto), zap.Int64("table-id", e.mTtask.Plan.TableInfo.ID))
	logger.Info("run minimal task")
	failpoint.Inject("waitBeforeSortChunk", func() {
		time.Sleep(3 * time.Second)
	})
	failpoint.Inject("errorWhenSortChunk", func() {
		failpoint.Return(errors.New("occur an error when sort chunk"))
	})
	failpoint.Inject("syncBeforeSortChunk", func() {
		TestSyncChan <- struct{}{}
		<-TestSyncChan
	})
	chunkCheckpoint := toChunkCheckpoint(e.mTtask.Chunk)
	sharedVars := e.mTtask.SharedVars
	if err := importer.ProcessChunk(ctx, &chunkCheckpoint, sharedVars.TableImporter, sharedVars.DataEngine, sharedVars.IndexEngine, sharedVars.Progress, logger); err != nil {
		return err
	}

	sharedVars.mu.Lock()
	defer sharedVars.mu.Unlock()
	sharedVars.Checksum.Add(&chunkCheckpoint.Checksum)
	return nil
}

type postProcessMinimalTaskExecutor struct {
	mTask *postProcessStepMinimalTask
}

func (e *postProcessMinimalTaskExecutor) Run(ctx context.Context) error {
	mTask := e.mTask
	failpoint.Inject("waitBeforePostProcess", func() {
		time.Sleep(5 * time.Second)
	})
	return postProcess(ctx, mTask.taskMeta, &mTask.meta, mTask.logger)
}

func init() {
	scheduler.RegisterSubtaskExectorConstructor(proto.ImportInto, StepImport,
		// The order of the subtask executors is the same as the order of the subtasks.
		func(minimalTask proto.MinimalTask, step int64) (scheduler.SubtaskExecutor, error) {
			task, ok := minimalTask.(*importStepMinimalTask)
			if !ok {
				return nil, errors.Errorf("invalid task type %T", minimalTask)
			}
			return &ImportMinimalTaskExecutor{mTtask: task}, nil
		},
	)
	scheduler.RegisterSubtaskExectorConstructor(proto.ImportInto, StepPostProcess,
		func(minimalTask proto.MinimalTask, step int64) (scheduler.SubtaskExecutor, error) {
			mTask, ok := minimalTask.(*postProcessStepMinimalTask)
			if !ok {
				return nil, errors.Errorf("invalid task type %T", minimalTask)
			}
			return &postProcessMinimalTaskExecutor{mTask: mTask}, nil
		},
	)
}
