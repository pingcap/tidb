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
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	tikvstore "github.com/tikv/client-go/v2/kv"
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

// postProcess does the post-processing for the task.
func postProcess(ctx context.Context, taskMeta *TaskMeta, subtaskMeta *PostProcessStepMeta, logger *zap.Logger) (err error) {
	failpoint.Inject("syncBeforePostProcess", func() {
		TestSyncChan <- struct{}{}
		<-TestSyncChan
	})

	logger.Info("post process")

	// TODO: create table indexes depends on the option.
	// create table indexes even if the post process is failed.
	// defer func() {
	// 	err2 := createTableIndexes(ctx, globalTaskManager, taskMeta, logger)
	// 	err = multierr.Append(err, err2)
	// }()

	return verifyChecksum(ctx, taskMeta, subtaskMeta, logger)
}

func verifyChecksum(ctx context.Context, taskMeta *TaskMeta, subtaskMeta *PostProcessStepMeta, logger *zap.Logger) error {
	if taskMeta.Plan.Checksum == config.OpLevelOff {
		return nil
	}
	localChecksum := verify.MakeKVChecksum(subtaskMeta.Checksum.Size, subtaskMeta.Checksum.KVs, subtaskMeta.Checksum.Sum)
	logger.Info("local checksum", zap.Object("checksum", &localChecksum))

	failpoint.Inject("waitCtxDone", func() {
		<-ctx.Done()
	})

	globalTaskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	remoteChecksum, err := checksumTable(ctx, globalTaskManager, taskMeta, logger)
	if err != nil {
		return err
	}
	if !remoteChecksum.IsEqual(&localChecksum) {
		err2 := common.ErrChecksumMismatch.GenWithStackByArgs(
			remoteChecksum.Checksum, localChecksum.Sum(),
			remoteChecksum.TotalKVs, localChecksum.SumKVS(),
			remoteChecksum.TotalBytes, localChecksum.SumSize(),
		)
		if taskMeta.Plan.Checksum == config.OpLevelOptional {
			logger.Warn("verify checksum failed, but checksum is optional, will skip it", zap.Error(err2))
			err2 = nil
		}
		return err2
	}
	logger.Info("checksum pass", zap.Object("local", &localChecksum))
	return nil
}

func checksumTable(ctx context.Context, executor storage.SessionExecutor, taskMeta *TaskMeta, logger *zap.Logger) (*local.RemoteChecksum, error) {
	var (
		tableName              = common.UniqueTable(taskMeta.Plan.DBName, taskMeta.Plan.TableInfo.Name.L)
		sql                    = "ADMIN CHECKSUM TABLE " + tableName
		remoteChecksum         *local.RemoteChecksum
		maxErrorRetryCount     = 3
		distSQLScanConcurrency int
		rs                     []chunk.Row
		execErr                error
	)

	err := executor.WithNewSession(func(se sessionctx.Context) error {
		if err := se.GetSessionVars().SetSystemVar(variable.TiDBBackOffWeight, strconv.Itoa(3*tikvstore.DefBackOffWeight)); err != nil {
			return err
		}
		distSQLScanConcurrency = se.GetSessionVars().DistSQLScanConcurrency()

		for i := 0; i < maxErrorRetryCount; i++ {
			rs, execErr = storage.ExecSQL(ctx, se, sql)
			if execErr == nil {
				if len(rs) < 1 {
					return errors.New("empty checksum result")
				}
				// ADMIN CHECKSUM TABLE <schema>.<table>  example.
				// 	mysql> admin checksum table test.t;
				// +---------+------------+---------------------+-----------+-------------+
				// | Db_name | Table_name | Checksum_crc64_xor  | Total_kvs | Total_bytes |
				// +---------+------------+---------------------+-----------+-------------+
				// | test    | t          | 8520875019404689597 |   7296873 |   357601387 |
				// +---------+------------+-------------
				remoteChecksum = &local.RemoteChecksum{
					Schema:     rs[0].GetString(0),
					Table:      rs[0].GetString(1),
					Checksum:   rs[0].GetUint64(2),
					TotalKVs:   rs[0].GetUint64(3),
					TotalBytes: rs[0].GetUint64(4),
				}
				return nil
			}
			if !common.IsRetryableError(execErr) {
				return execErr
			}

			logger.Warn("remote checksum failed", zap.String("sql", sql), zap.Error(execErr),
				zap.Int("concurrency", distSQLScanConcurrency), zap.Int("retry", i))
			if distSQLScanConcurrency > local.MinDistSQLScanConcurrency {
				distSQLScanConcurrency = mathutil.Max(distSQLScanConcurrency/2, local.MinDistSQLScanConcurrency)
				se.GetSessionVars().SetDistSQLScanConcurrency(distSQLScanConcurrency)
			}
		}
		return execErr
	})
	return remoteChecksum, err
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
