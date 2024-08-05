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
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/log"
	verify "github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// MiniTaskExecutor is the interface for a minimal task executor.
// exported for testing.
type MiniTaskExecutor interface {
	Run(ctx context.Context, dataWriter, indexWriter backend.EngineWriter) error
}

// importMinimalTaskExecutor is a minimal task executor for IMPORT INTO.
type importMinimalTaskExecutor struct {
	mTtask *importStepMinimalTask
}

var newImportMinimalTaskExecutor = newImportMinimalTaskExecutor0

func newImportMinimalTaskExecutor0(t *importStepMinimalTask) MiniTaskExecutor {
	return &importMinimalTaskExecutor{
		mTtask: t,
	}
}

func (e *importMinimalTaskExecutor) Run(ctx context.Context, dataWriter, indexWriter backend.EngineWriter) error {
	logger := logutil.BgLogger().With(zap.Stringer("type", proto.ImportInto), zap.Int64("table-id", e.mTtask.Plan.TableInfo.ID))
	logger.Info("execute chunk")
	failpoint.Inject("waitBeforeSortChunk", func() {
		time.Sleep(3 * time.Second)
	})
	failpoint.Inject("errorWhenSortChunk", func() {
		failpoint.Return(errors.New("occur an error when sort chunk"))
	})
	failpoint.InjectCall("syncBeforeSortChunk")
	chunkCheckpoint := toChunkCheckpoint(e.mTtask.Chunk)
	sharedVars := e.mTtask.SharedVars
	checksum := verify.NewKVGroupChecksumWithKeyspace(sharedVars.TableImporter.GetKeySpace())
	if sharedVars.TableImporter.IsLocalSort() {
		if err := importer.ProcessChunk(
			ctx,
			&chunkCheckpoint,
			sharedVars.TableImporter,
			sharedVars.DataEngine,
			sharedVars.IndexEngine,
			sharedVars.Progress,
			logger,
			checksum,
		); err != nil {
			return err
		}
	} else {
		if err := importer.ProcessChunkWithWriter(
			ctx,
			&chunkCheckpoint,
			sharedVars.TableImporter,
			dataWriter,
			indexWriter,
			sharedVars.Progress,
			logger,
			checksum,
		); err != nil {
			return err
		}
	}

	sharedVars.mu.Lock()
	defer sharedVars.mu.Unlock()
	sharedVars.Checksum.Add(checksum)
	return nil
}

// postProcess does the post-processing for the task.
func postProcess(ctx context.Context, store kv.Storage, taskMeta *TaskMeta, subtaskMeta *PostProcessStepMeta, logger *zap.Logger) (err error) {
	failpoint.InjectCall("syncBeforePostProcess", taskMeta.JobID)

	callLog := log.BeginTask(logger, "post process")
	defer func() {
		callLog.End(zap.ErrorLevel, err)
	}()

	if err = importer.RebaseAllocatorBases(ctx, store, subtaskMeta.MaxIDs, &taskMeta.Plan, logger); err != nil {
		return err
	}

	// TODO: create table indexes depends on the option.
	// create table indexes even if the post process is failed.
	// defer func() {
	// 	err2 := createTableIndexes(ctx, globalTaskManager, taskMeta, logger)
	// 	err = multierr.Append(err, err2)
	// }()

	localChecksum := verify.NewKVGroupChecksumForAdd()
	for id, cksum := range subtaskMeta.Checksum {
		callLog.Info(
			"kv group checksum",
			zap.Int64("groupId", id),
			zap.Uint64("size", cksum.Size),
			zap.Uint64("kvs", cksum.KVs),
			zap.Uint64("checksum", cksum.Sum),
		)
		localChecksum.AddRawGroup(id, cksum.Size, cksum.KVs, cksum.Sum)
	}

	taskManager, err := storage.GetTaskManager()
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	if err != nil {
		return err
	}
	return taskManager.WithNewSession(func(se sessionctx.Context) error {
		return importer.VerifyChecksum(ctx, &taskMeta.Plan, localChecksum.MergedChecksum(), se, logger)
	})
}
