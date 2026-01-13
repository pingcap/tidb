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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	verify "github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// MiniTaskExecutor is the interface for a minimal task executor.
// exported for testing.
type MiniTaskExecutor interface {
	Run(ctx context.Context, dataWriter, indexWriter backend.EngineWriter, collector execute.Collector) error
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

func (e *importMinimalTaskExecutor) Run(
	ctx context.Context,
	dataWriter, indexWriter backend.EngineWriter,
	collector execute.Collector,
) error {
	logger := e.mTtask.logger
	failpoint.Inject("beforeSortChunk", func() {})
	failpoint.Inject("errorWhenSortChunk", func() {
		failpoint.Return(errors.New("occur an error when sort chunk"))
	})
	failpoint.InjectCall("syncBeforeSortChunk")
	sharedVars := e.mTtask.SharedVars

	chunkCheckpoint := toChunkCheckpoint(e.mTtask.Chunk)
	chunkCheckpoint.FileMeta.ParquetMeta = mydump.ParquetFileMeta{
		Loc: sharedVars.TableImporter.Location,
	}

	checksum := verify.NewKVGroupChecksumWithKeyspace(sharedVars.TableImporter.GetKeySpace())
	if sharedVars.TableImporter.IsLocalSort() {
		if err := importer.ProcessChunk(
			ctx,
			&chunkCheckpoint,
			sharedVars.TableImporter,
			sharedVars.DataEngine,
			sharedVars.IndexEngine,
			logger,
			checksum,
			collector,
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
			logger,
			checksum,
			collector,
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
func (p *postProcessStepExecutor) postProcess(ctx context.Context, subtaskMeta *PostProcessStepMeta, logger *zap.Logger) (err error) {
	failpoint.InjectCall("syncBeforePostProcess", p.taskMeta.JobID)

	callLog := log.BeginTask(logger, "post process")
	defer func() {
		callLog.End(zap.ErrorLevel, err)
	}()

	plan := &p.taskMeta.Plan
	if err = importer.RebaseAllocatorBases(ctx, p.store, subtaskMeta.MaxIDs, plan, logger); err != nil {
		return err
	}

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
	encodeStepChecksum := localChecksum.MergedChecksum()
	deletedRowsChecksum := subtaskMeta.DeletedRowsChecksum.ToKVChecksum()
	finalChecksum := encodeStepChecksum
	finalChecksum.Sub(deletedRowsChecksum)
	callLog.Info("checksum info", zap.Stringer("encodeStepSum", &encodeStepChecksum),
		zap.Stringer("deletedRowsSum", deletedRowsChecksum),
		zap.Stringer("final", &finalChecksum))
	if subtaskMeta.TooManyConflictsFromIndex {
		callLog.Info("too many conflicts from index, skip verify checksum, as the checksum of deleted rows may be inaccurate")
		return nil
	}

	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	if kerneltype.IsNextGen() {
		bfWeight := importer.GetBackoffWeight(plan)
		mgr := local.NewTiKVChecksumManagerForImportInto(p.store, p.taskID,
			uint(plan.DistSQLScanConcurrency), bfWeight, resourcegroup.DefaultResourceGroupName)
		defer mgr.Close()
		return importer.VerifyChecksum(ctx, plan, finalChecksum, logger,
			func() (*local.RemoteChecksum, error) {
				ctxWithLogger := logutil.WithLogger(ctx, logger)
				return mgr.Checksum(ctxWithLogger, &checkpoints.TidbTableInfo{
					DB:   plan.DBName,
					Name: plan.TableInfo.Name.L,
					Core: plan.TableInfo,
				})
			},
		)
	}

	return p.taskTbl.WithNewSession(func(se sessionctx.Context) error {
		err = importer.VerifyChecksum(ctx, plan, finalChecksum, logger,
			func() (*local.RemoteChecksum, error) {
				return importer.RemoteChecksumTableBySQL(ctx, se, plan, logger)
			},
		)
		if kerneltype.IsClassic() {
			failpoint.Inject("skipPostProcessAlterTableMode", func() {
				failpoint.Return(err)
			})
			// log error instead of raise error to avoid user rerun task,
			// clean up will alter table mode to normal finally.
			err2 := ddl.AlterTableMode(domain.GetDomain(se).DDLExecutor(), se, model.TableModeNormal, p.taskMeta.Plan.DBID, p.taskMeta.Plan.TableInfo.ID)
			if err2 != nil {
				callLog.Warn("alter table mode to normal failure", zap.Error(err2))
			}
		}
		return err
	})
}
