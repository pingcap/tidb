// Copyright 2022 PingCAP, Inc.
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

package ingest

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	tikv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/common"
	lightning "github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

// MockDMLExecutionStateBeforeImport is a failpoint to mock the DML execution state before import.
var MockDMLExecutionStateBeforeImport func()

// BackendCtx is the backend context for one add index reorg task.
type BackendCtx interface {
	// Register create a new engineInfo for each index ID and register it to the
	// backend context. If the index ID is already registered, it will return the
	// associated engines. Only one group of index ID is allowed to register for a
	// BackendCtx.
	//
	// Register is only used in local disk based ingest.
	Register(indexIDs []int64, uniques []bool, tbl table.Table) ([]Engine, error)
	// FinishAndUnregisterEngines finishes the task and unregisters all engines that
	// are Register-ed before. It's safe to call it multiple times.
	//
	// FinishAndUnregisterEngines is only used in local disk based ingest.
	FinishAndUnregisterEngines(opt UnregisterOpt) error

	FlushController

	GetCheckpointManager() *CheckpointManager

	// GetLocalBackend exposes local.Backend. It's only used in global sort based
	// ingest.
	GetLocalBackend() *local.Backend
	// CollectRemoteDuplicateRows collects duplicate entry error for given index as
	// the supplement of FlushController.Flush.
	//
	// CollectRemoteDuplicateRows is only used in global sort based ingest.
	CollectRemoteDuplicateRows(indexID int64, tbl table.Table) error

	GetDiskUsage() uint64
	Close()
}

// FlushMode is used to control how to flush.
type FlushMode byte

const (
	// FlushModeAuto means caller does not enforce any flush, the implementation can
	// decide it.
	FlushModeAuto FlushMode = iota
	// FlushModeForceFlushAndImport means flush and import all data to TiKV.
	FlushModeForceFlushAndImport
)

// litBackendCtx implements BackendCtx.
type litBackendCtx struct {
	engines map[int64]*engineInfo
	memRoot MemRoot
	jobID   int64
	tbl     table.Table
	backend *local.Backend
	ctx     context.Context
	cfg     *local.BackendConfig
	sysVars map[string]string

	flushing        atomic.Bool
	timeOfLastFlush atomicutil.Time
	updateInterval  time.Duration
	checkpointMgr   *CheckpointManager
	etcdClient      *clientv3.Client
	initTS          uint64

	// unregisterMu prevents concurrent calls of `FinishAndUnregisterEngines`.
	// For details, see https://github.com/pingcap/tidb/issues/53843.
	unregisterMu sync.Mutex
}

func (bc *litBackendCtx) handleErrorAfterCollectRemoteDuplicateRows(
	err error,
	indexID int64,
	tbl table.Table,
	hasDupe bool,
) error {
	if err != nil && !common.ErrFoundIndexConflictRecords.Equal(err) {
		logutil.Logger(bc.ctx).Error(LitInfoRemoteDupCheck, zap.Error(err),
			zap.String("table", tbl.Meta().Name.O), zap.Int64("index ID", indexID))
		return errors.Trace(err)
	} else if hasDupe {
		logutil.Logger(bc.ctx).Error(LitErrRemoteDupExistErr,
			zap.String("table", tbl.Meta().Name.O), zap.Int64("index ID", indexID))

		if common.ErrFoundIndexConflictRecords.Equal(err) {
			tErr, ok := errors.Cause(err).(*terror.Error)
			if !ok {
				return errors.Trace(tikv.ErrKeyExists)
			}
			if len(tErr.Args()) != 4 {
				return errors.Trace(tikv.ErrKeyExists)
			}
			//nolint: forcetypeassert
			indexName := tErr.Args()[1].(string)
			//nolint: forcetypeassert
			keyCols := tErr.Args()[2].([]string)
			return errors.Trace(tikv.GenKeyExistsErr(keyCols, indexName))
		}
		return errors.Trace(tikv.ErrKeyExists)
	}
	return nil
}

// CollectRemoteDuplicateRows collects duplicate rows from remote TiKV.
func (bc *litBackendCtx) CollectRemoteDuplicateRows(indexID int64, tbl table.Table) error {
	return bc.collectRemoteDuplicateRows(indexID, tbl)
}

func (bc *litBackendCtx) collectRemoteDuplicateRows(indexID int64, tbl table.Table) error {
	dupeController := bc.backend.GetDupeController(bc.cfg.WorkerConcurrency, nil)
	hasDupe, err := dupeController.CollectRemoteDuplicateRows(bc.ctx, tbl, tbl.Meta().Name.L, &encode.SessionOptions{
		SQLMode:     mysql.ModeStrictAllTables,
		SysVars:     bc.sysVars,
		IndexID:     indexID,
		MinCommitTS: bc.initTS,
	}, lightning.ErrorOnDup)
	return bc.handleErrorAfterCollectRemoteDuplicateRows(err, indexID, tbl, hasDupe)
}

func (bc *litBackendCtx) TryFlush(ctx context.Context, taskID int, count int) error {
	if bc.checkpointMgr != nil {
		bc.checkpointMgr.UpdateWrittenKeys(taskID, count)
	}
	shouldFlush, shouldImport := bc.checkFlush(FlushModeAuto)
	if !shouldFlush {
		return nil
	}
	if !bc.flushing.CompareAndSwap(false, true) {
		return nil
	}
	defer bc.flushing.Store(false)
	err := bc.flushEngines(ctx)
	if err != nil {
		return err
	}
	bc.timeOfLastFlush.Store(time.Now())

	if !shouldImport {
		if bc.checkpointMgr != nil {
			err := bc.checkpointMgr.AdvanceWatermark(false)
			if err != nil {
				return err
			}
		}
		return nil
	}

	release, err := bc.tryAcquireDistLock()
	if err != nil {
		return err
	}
	if release != nil {
		defer release()
	}

	err = bc.unsafeImportAndResetAllEngines(ctx)
	if err != nil {
		return err
	}
	if bc.checkpointMgr != nil {
		err := bc.checkpointMgr.AdvanceWatermark(true)
		if err != nil {
			return err
		}
	}
	return nil
}

// Flush implements FlushController.
func (bc *litBackendCtx) Flush(ctx context.Context) error {
	err := bc.flushEngines(ctx)
	if err != nil {
		return err
	}

	release, err := bc.tryAcquireDistLock()
	if err != nil {
		return err
	}
	if release != nil {
		defer release()
	}

	failpoint.Inject("mockDMLExecutionStateBeforeImport", func(_ failpoint.Value) {
		if MockDMLExecutionStateBeforeImport != nil {
			MockDMLExecutionStateBeforeImport()
		}
	})

	err = bc.unsafeImportAndResetAllEngines(ctx)
	if err != nil {
		return err
	}

	if bc.checkpointMgr != nil {
		// Try to advance watermark even if there is an error.
		err1 := bc.checkpointMgr.AdvanceWatermark(true)
		if err1 != nil {
			return err1
		}
	}

	return nil
}

func (bc *litBackendCtx) flushEngines(ctx context.Context) error {
	for _, ei := range bc.engines {
		ei.flushLock.Lock()
		//nolint: all_revive,revive
		defer ei.flushLock.Unlock()

		if err := ei.Flush(); err != nil {
			logutil.Logger(ctx).Error("flush error", zap.Error(err))
			return err
		}
	}
	return nil
}

func (bc *litBackendCtx) tryAcquireDistLock() (func(), error) {
	if bc.etcdClient == nil {
		return nil, nil
	}
	key := fmt.Sprintf("/tidb/distributeLock/%d", bc.jobID)
	return owner.AcquireDistributedLock(bc.ctx, bc.etcdClient, key, 10)
}

func (bc *litBackendCtx) unsafeImportAndResetAllEngines(ctx context.Context) error {
	for indexID, ei := range bc.engines {
		if err := bc.unsafeImportAndReset(ctx, ei); err != nil {
			if common.ErrFoundDuplicateKeys.Equal(err) {
				idxInfo := model.FindIndexInfoByID(bc.tbl.Meta().Indices, indexID)
				if idxInfo == nil {
					logutil.Logger(bc.ctx).Error(
						"index not found",
						zap.Int64("indexID", indexID))
					err = tikv.ErrKeyExists
				} else {
					err = TryConvertToKeyExistsErr(err, idxInfo, bc.tbl.Meta())
				}
			}
			logutil.Logger(ctx).Error("import error", zap.Error(err))
			return err
		}
	}
	return nil
}

func (bc *litBackendCtx) unsafeImportAndReset(ctx context.Context, ei *engineInfo) error {
	logger := log.FromContext(bc.ctx).With(
		zap.Stringer("engineUUID", ei.uuid),
	)
	logger.Info(LitInfoUnsafeImport,
		zap.Int64("index ID", ei.indexID),
		zap.String("usage info", LitDiskRoot.UsageInfo()))

	closedEngine := backend.NewClosedEngine(bc.backend, logger, ei.uuid, 0)
	var ingestTS uint64
	if bc.checkpointMgr != nil {
		ingestTS = bc.checkpointMgr.GetTS()
		logger.Info("set ingest ts before import", zap.Int64("jobID", bc.jobID), zap.Uint64("ts", ingestTS))
	}
	err := bc.backend.SetTSBeforeImportEngine(ctx, ei.uuid, ingestTS)
	if err != nil {
		logger.Error("set TS failed", zap.Int64("index ID", ei.indexID))
		return err
	}

	regionSplitSize := int64(lightning.SplitRegionSize) * int64(lightning.MaxSplitRegionSizeRatio)
	regionSplitKeys := int64(lightning.SplitRegionKeys)
	if err := closedEngine.Import(ctx, regionSplitSize, regionSplitKeys); err != nil {
		logger.Error(LitErrIngestDataErr, zap.Int64("index ID", ei.indexID),
			zap.String("usage info", LitDiskRoot.UsageInfo()))
		return err
	}

	// TS will be set before local backend import. We don't need to alloc a new one when reset.
	err = bc.backend.ResetEngineSkipAllocTS(ctx, ei.uuid)
	failpoint.Inject("mockResetEngineFailed", func() {
		err = fmt.Errorf("mock reset engine failed")
	})
	if err != nil {
		logger.Error(LitErrResetEngineFail, zap.Int64("index ID", ei.indexID))
		err1 := closedEngine.Cleanup(bc.ctx)
		if err1 != nil {
			logutil.Logger(ei.ctx).Error(LitErrCleanEngineErr, zap.Error(err1),
				zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
		}
		ei.openedEngine = nil
		return err
	}
	return nil
}

// ForceSyncFlagForTest is a flag to force sync only for test.
var ForceSyncFlagForTest = false

func (bc *litBackendCtx) checkFlush(mode FlushMode) (shouldFlush bool, shouldImport bool) {
	failpoint.Inject("forceSyncFlagForTest", func() {
		// used in a manual test
		ForceSyncFlagForTest = true
	})
	if mode == FlushModeForceFlushAndImport || ForceSyncFlagForTest {
		return true, true
	}
	LitDiskRoot.UpdateUsage()
	shouldImport = LitDiskRoot.ShouldImport()
	interval := bc.updateInterval
	// This failpoint will be manually set through HTTP status port.
	failpoint.Inject("mockSyncIntervalMs", func(val failpoint.Value) {
		if v, ok := val.(int); ok {
			interval = time.Duration(v) * time.Millisecond
		}
	})
	shouldFlush = shouldImport ||
		time.Since(bc.timeOfLastFlush.Load()) >= interval
	return shouldFlush, shouldImport
}

// GetLocalBackend returns the local backend.
func (bc *litBackendCtx) GetLocalBackend() *local.Backend {
	return bc.backend
}

// GetCheckpointManager returns the checkpoint manager.
func (bc *litBackendCtx) GetCheckpointManager() *CheckpointManager {
	return bc.checkpointMgr
}

// GetDiskUsage returns current disk usage of underlying backend.
func (bc *litBackendCtx) GetDiskUsage() uint64 {
	_, _, bcDiskUsed, _ := local.CheckDiskQuota(bc.backend, math.MaxInt64)
	return uint64(bcDiskUsed)
}

// Close closes underlying backend and remove it from disk root.
func (bc *litBackendCtx) Close() {
	logutil.Logger(bc.ctx).Info(LitInfoCloseBackend, zap.Int64("jobID", bc.jobID),
		zap.Int64("current memory usage", LitMemRoot.CurrentUsage()),
		zap.Int64("max memory quota", LitMemRoot.MaxMemoryQuota()))
	bc.backend.Close()
	LitDiskRoot.Remove(bc.jobID)
	BackendCounterForTest.Dec()
}
