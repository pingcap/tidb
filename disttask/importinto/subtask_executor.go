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
	"net"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	tidb "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/tikv/client-go/v2/util"
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

	callLog := log.BeginTask(logger, "post process")
	defer func() {
		callLog.End(zap.ErrorLevel, err)
	}()

	if err = rebaseAllocatorBases(ctx, taskMeta, subtaskMeta, logger); err != nil {
		return err
	}

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
		if taskMeta.Plan.Checksum != config.OpLevelOptional {
			return err
		}
		logger.Warn("checksumTable failed, will skip this error and go on", zap.Error(err))
	}
	if remoteChecksum != nil {
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
	}
	return nil
}

func checksumTable(ctx context.Context, executor storage.SessionExecutor, taskMeta *TaskMeta, logger *zap.Logger) (*local.RemoteChecksum, error) {
	var (
		tableName                    = common.UniqueTable(taskMeta.Plan.DBName, taskMeta.Plan.TableInfo.Name.L)
		sql                          = "ADMIN CHECKSUM TABLE " + tableName
		maxErrorRetryCount           = 3
		distSQLScanConcurrencyFactor = 1
		remoteChecksum               *local.RemoteChecksum
		txnErr                       error
	)

	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	for i := 0; i < maxErrorRetryCount; i++ {
		txnErr = executor.WithNewTxn(ctx, func(se sessionctx.Context) error {
			// increase backoff weight
			if err := setBackoffWeight(se, taskMeta, logger); err != nil {
				logger.Warn("set tidb_backoff_weight failed", zap.Error(err))
			}

			distSQLScanConcurrency := se.GetSessionVars().DistSQLScanConcurrency()
			se.GetSessionVars().SetDistSQLScanConcurrency(mathutil.Max(distSQLScanConcurrency/distSQLScanConcurrencyFactor, local.MinDistSQLScanConcurrency))
			defer func() {
				se.GetSessionVars().SetDistSQLScanConcurrency(distSQLScanConcurrency)
			}()

			rs, err := storage.ExecSQL(ctx, se, sql)
			if err != nil {
				return err
			}
			if len(rs) < 1 {
				return errors.New("empty checksum result")
			}

			failpoint.Inject("errWhenChecksum", func() {
				if i == 0 {
					failpoint.Return(errors.New("occur an error when checksum, coprocessor task terminated due to exceeding the deadline"))
				}
			})

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
		})
		if !common.IsRetryableError(txnErr) {
			break
		}
		distSQLScanConcurrencyFactor *= 2
		logger.Warn("retry checksum table", zap.Int("retry count", i+1), zap.Error(txnErr))
	}
	return remoteChecksum, txnErr
}

// TestChecksumTable is used to test checksum table in unit test.
func TestChecksumTable(ctx context.Context, executor storage.SessionExecutor, taskMeta *TaskMeta, logger *zap.Logger) (*local.RemoteChecksum, error) {
	return checksumTable(ctx, executor, taskMeta, logger)
}

func setBackoffWeight(se sessionctx.Context, taskMeta *TaskMeta, logger *zap.Logger) error {
	backoffWeight := local.DefaultBackoffWeight
	if val, ok := taskMeta.Plan.ImportantSysVars[variable.TiDBBackOffWeight]; ok {
		if weight, err := strconv.Atoi(val); err == nil && weight > backoffWeight {
			backoffWeight = weight
		}
	}
	logger.Info("set backoff weight", zap.Int("weight", backoffWeight))
	return se.GetSessionVars().SetSystemVar(variable.TiDBBackOffWeight, strconv.Itoa(backoffWeight))
}

func rebaseAllocatorBases(ctx context.Context, taskMeta *TaskMeta, subtaskMeta *PostProcessStepMeta, logger *zap.Logger) (err error) {
	callLog := log.BeginTask(logger, "rebase allocators")
	defer func() {
		callLog.End(zap.ErrorLevel, err)
	}()

	if !common.TableHasAutoID(taskMeta.Plan.DesiredTableInfo) {
		return nil
	}

	tidbCfg := tidb.GetGlobalConfig()
	hostPort := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(tidbCfg.Status.StatusPort)))
	tls, err2 := common.NewTLS(
		tidbCfg.Security.ClusterSSLCA,
		tidbCfg.Security.ClusterSSLCert,
		tidbCfg.Security.ClusterSSLKey,
		hostPort,
		nil, nil, nil,
	)
	if err2 != nil {
		return err2
	}

	// no need to close kvStore, since it's a cached store.
	kvStore, err2 := importer.GetCachedKVStoreFrom(tidbCfg.Path, tls)
	if err2 != nil {
		return errors.Trace(err2)
	}
	return errors.Trace(common.RebaseTableAllocators(ctx, subtaskMeta.MaxIDs,
		kvStore, taskMeta.Plan.DBID, taskMeta.Plan.DesiredTableInfo))
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
