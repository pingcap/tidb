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
	goerrors "errors"
	"sort"
	"time"

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
	"github.com/pingcap/tidb/pkg/tici"
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

const importIntoTiCIIndexReadyPollInterval = 15 * time.Second

var (
	finishTiCIIndexUpload       = tici.FinishIndexUpload
	checkTiCIAddIndexProgress   = tici.CheckAddIndexProgress
	waitTiCIIndexProgressPollFn = waitTiCIIndexProgressPoll
)

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

	ticiIndexIDs, shouldWaitTiCIIndexReady, ticiSummary := finishTiCIIndexUploadForPostProcess(
		ctx, p.store, p.taskID, p.taskMeta.JobID, plan, logger)
	if ticiSummary != nil {
		subtaskMeta.TiCIIndexSummary = ticiSummary
	}
	waitTiCIIndexReady := func() error {
		if !shouldWaitTiCIIndexReady {
			return nil
		}
		ticiSummary, err := waitTiCIIndexesReadyForPostProcess(ctx, p.store, p.taskID, plan.TableInfo.ID, ticiIndexIDs, logger)
		if err != nil {
			return err
		}
		if ticiSummary != nil {
			subtaskMeta.TiCIIndexSummary = ticiSummary
		}
		return nil
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
	encodeStepChecksum := importer.MainChecksumForValidation(plan, localChecksum)
	deletedRowsChecksum := subtaskMeta.DeletedRowsChecksum.ToKVChecksum()
	finalChecksum := encodeStepChecksum
	finalChecksum.Sub(deletedRowsChecksum)
	callLog.Info("checksum info", zap.Stringer("encodeStepSum", &encodeStepChecksum),
		zap.Stringer("deletedRowsSum", deletedRowsChecksum),
		zap.Stringer("final", &finalChecksum))
	if subtaskMeta.TooManyConflictsFromIndex {
		callLog.Info("too many conflicts from index, skip verify checksum, as the checksum of deleted rows may be inaccurate")
		return waitTiCIIndexReady()
	}

	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	if kerneltype.IsNextGen() {
		bfWeight := importer.GetBackoffWeight(plan)
		mgr := local.NewTiKVChecksumManagerForImportInto(p.store, p.taskID,
			uint(plan.DistSQLScanConcurrency), bfWeight, resourcegroup.DefaultResourceGroupName)
		defer mgr.Close()
		checksumTableInfo := importer.TableInfoForRemoteChecksumValidation(plan)
		if err = importer.VerifyChecksum(ctx, plan, finalChecksum, logger,
			func() (*local.RemoteChecksum, error) {
				ctxWithLogger := logutil.WithLogger(ctx, logger)
				return mgr.Checksum(ctxWithLogger, &checkpoints.TidbTableInfo{
					DB:   plan.DBName,
					Name: plan.TableInfo.Name.L,
					Core: checksumTableInfo,
				})
			},
		); err != nil {
			return err
		}
		return waitTiCIIndexReady()
	}

	if err = p.taskTbl.WithNewSession(func(se sessionctx.Context) error {
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
	}); err != nil {
		return err
	}
	return waitTiCIIndexReady()
}

func finishTiCIIndexUploadForPostProcess(
	ctx context.Context,
	store kv.Storage,
	taskID int64,
	jobID int64,
	plan *importer.Plan,
	logger *zap.Logger,
) ([]int64, bool, *importer.TiCIIndexSummary) {
	if plan == nil || plan.TableInfo == nil {
		return nil, false, nil
	}

	ticiIndexIDs := tici.GetTiCIIndexIDs(plan.TableInfo)
	if len(ticiIndexIDs) == 0 {
		return nil, false, nil
	}

	tidbTaskID := ticiTaskIDForImportInto(jobID)
	if err := finishTiCIIndexUpload(ctx, store, tidbTaskID); err != nil {
		logger.Warn(
			"failed to finish TiCI index upload for post process",
			zap.Int64("task-id", taskID),
			zap.Int64s("tici-index-ids", ticiIndexIDs),
			zap.Error(err),
		)
		return ticiIndexIDs, false, &importer.TiCIIndexSummary{
			Incomplete:      true,
			TableID:         plan.TableInfo.ID,
			IndexIDs:        cloneSortedInt64s(ticiIndexIDs),
			PendingIndexIDs: cloneSortedInt64s(ticiIndexIDs),
			Reason:          "finish-index-upload-failed",
			ErrorMessage:    err.Error(),
		}
	}

	logger.Info(
		"finished TiCI index upload for post process",
		zap.Int64("task-id", taskID),
		zap.Int64s("tici-index-ids", ticiIndexIDs),
	)
	return ticiIndexIDs, true, nil
}

func waitTiCIIndexesReadyForPostProcess(
	ctx context.Context,
	store kv.Storage,
	taskID int64,
	tableID int64,
	ticiIndexIDs []int64,
	logger *zap.Logger,
) (*importer.TiCIIndexSummary, error) {
	if len(ticiIndexIDs) == 0 {
		return nil, nil
	}

	allIndexIDs := cloneSortedInt64s(ticiIndexIDs)
	pending := make(map[int64]struct{}, len(allIndexIDs))
	for _, indexID := range allIndexIDs {
		pending[indexID] = struct{}{}
	}
	readyIndexIDs := make([]int64, 0, len(allIndexIDs))

	logger.Info(
		"start checking TiCI indexes readiness for post process",
		zap.Int64("task-id", taskID),
		zap.Int64("table-id", tableID),
		zap.Int64s("tici-index-ids", allIndexIDs),
	)

	for len(pending) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for _, indexID := range sortedPendingTiCIIndexIDs(pending) {
			ready, err := checkTiCIAddIndexProgress(ctx, store, tableID, indexID)
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return nil, ctxErr
				}
				if goerrors.Is(err, context.Canceled) || goerrors.Is(err, context.DeadlineExceeded) {
					return nil, err
				}
				logger.Warn(
					"failed to check TiCI index progress for post process",
					zap.Int64("task-id", taskID),
					zap.Int64("table-id", tableID),
					zap.Int64("tici-index-id", indexID),
					zap.Int64s("all-tici-index-ids", allIndexIDs),
					zap.Error(err),
				)
				return &importer.TiCIIndexSummary{
					Incomplete:      true,
					TableID:         tableID,
					IndexIDs:        allIndexIDs,
					ReadyIndexIDs:   cloneSortedInt64s(readyIndexIDs),
					PendingIndexIDs: sortedPendingTiCIIndexIDs(pending),
					ErrorIndexIDs:   []int64{indexID},
					Reason:          "check-add-index-progress-failed",
					ErrorMessage:    err.Error(),
				}, nil
			}
			if ready {
				delete(pending, indexID)
				readyIndexIDs = append(readyIndexIDs, indexID)
				logger.Info(
					"TiCI index is ready for post process",
					zap.Int64("task-id", taskID),
					zap.Int64("table-id", tableID),
					zap.Int64("tici-index-id", indexID),
				)
			}
		}
		if len(pending) == 0 {
			logger.Info(
				"all TiCI indexes are ready for post process",
				zap.Int64("task-id", taskID),
				zap.Int64("table-id", tableID),
				zap.Int64s("tici-index-ids", allIndexIDs),
			)
			return nil, nil
		}
		logger.Debug(
			"waiting for TiCI indexes to be ready for post process",
			zap.Int64("task-id", taskID),
			zap.Int64("table-id", tableID),
			zap.Int64s("pending-tici-index-ids", sortedPendingTiCIIndexIDs(pending)),
		)
		if err := waitTiCIIndexProgressPollFn(ctx); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func waitTiCIIndexProgressPoll(ctx context.Context) error {
	timer := time.NewTimer(importIntoTiCIIndexReadyPollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func cloneSortedInt64s(input []int64) []int64 {
	if len(input) == 0 {
		return nil
	}
	output := append([]int64(nil), input...)
	sort.Slice(output, func(i, j int) bool {
		return output[i] < output[j]
	})
	return output
}

func sortedPendingTiCIIndexIDs(pending map[int64]struct{}) []int64 {
	indexIDs := make([]int64, 0, len(pending))
	for indexID := range pending {
		indexIDs = append(indexIDs, indexID)
	}
	sort.Slice(indexIDs, func(i, j int) bool {
		return indexIDs[i] < indexIDs[j]
	})
	return indexIDs
}
