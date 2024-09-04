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
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
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

	AttachCheckpointManager(*CheckpointManager)
	GetCheckpointManager() *CheckpointManager

	// GetLocalBackend exposes local.Backend. It's only used in global sort based
	// ingest.
	GetLocalBackend() *local.Backend
	// CollectRemoteDuplicateRows collects duplicate entry error for given index as
	// the supplement of FlushController.Flush.
	//
	// CollectRemoteDuplicateRows is only used in global sort based ingest.
	CollectRemoteDuplicateRows(indexID int64, tbl table.Table) error
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
	engines  map[int64]*engineInfo
	memRoot  MemRoot
	diskRoot DiskRoot
	jobID    int64
	tbl      table.Table
	backend  *local.Backend
	ctx      context.Context
	cfg      *local.BackendConfig
	sysVars  map[string]string

	flushing        atomic.Bool
	timeOfLastFlush atomicutil.Time
	updateInterval  time.Duration
	checkpointMgr   *CheckpointManager
	etcdClient      *clientv3.Client

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
		SQLMode: mysql.ModeStrictAllTables,
		SysVars: bc.sysVars,
		IndexID: indexID,
	}, lightning.ErrorOnDup)
	return bc.handleErrorAfterCollectRemoteDuplicateRows(err, indexID, tbl, hasDupe)
}

func acquireLock(ctx context.Context, se *concurrency.Session, key string) (*concurrency.Mutex, error) {
	mu := concurrency.NewMutex(se, key)
	err := mu.Lock(ctx)
	if err != nil {
		return nil, err
	}
	return mu, nil
}

// Flush implements FlushController.
func (bc *litBackendCtx) Flush(mode FlushMode) (flushed, imported bool, err error) {
	shouldFlush, shouldImport := bc.checkFlush(mode)
	if !shouldFlush {
		return false, false, nil
	}
	if !bc.flushing.CompareAndSwap(false, true) {
		return false, false, nil
	}
	defer bc.flushing.Store(false)

	for _, ei := range bc.engines {
		ei.flushLock.Lock()
		//nolint: all_revive,revive
		defer ei.flushLock.Unlock()

		if err = ei.Flush(); err != nil {
			return false, false, err
		}
	}
	bc.timeOfLastFlush.Store(time.Now())

	if !shouldImport {
		return true, false, nil
	}

	// Use distributed lock if run in distributed mode).
	if bc.etcdClient != nil {
		distLockKey := fmt.Sprintf("/tidb/distributeLock/%d", bc.jobID)
		se, _ := concurrency.NewSession(bc.etcdClient)
		mu, err := acquireLock(bc.ctx, se, distLockKey)
		if err != nil {
			return true, false, errors.Trace(err)
		}
		logutil.Logger(bc.ctx).Info("acquire distributed flush lock success", zap.Int64("jobID", bc.jobID))
		defer func() {
			err = mu.Unlock(bc.ctx)
			if err != nil {
				logutil.Logger(bc.ctx).Warn("release distributed flush lock error", zap.Error(err), zap.Int64("jobID", bc.jobID))
			} else {
				logutil.Logger(bc.ctx).Info("release distributed flush lock success", zap.Int64("jobID", bc.jobID))
			}
			err = se.Close()
			if err != nil {
				logutil.Logger(bc.ctx).Warn("close session error", zap.Error(err))
			}
		}()
	}
	failpoint.Inject("mockDMLExecutionStateBeforeImport", func(_ failpoint.Value) {
		if MockDMLExecutionStateBeforeImport != nil {
			MockDMLExecutionStateBeforeImport()
		}
	})

	for indexID, ei := range bc.engines {
		if err = bc.unsafeImportAndReset(ei); err != nil {
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
			return true, false, err
		}
	}

	var newTS uint64
	if mgr := bc.GetCheckpointManager(); mgr != nil {
		// for local disk case, we need to refresh TS because duplicate detection
		// requires each ingest to have a unique TS.
		//
		// TODO(lance6716): there's still a chance that data is imported but because of
		// checkpoint is low-watermark, the data will still be imported again with
		// another TS after failover. Need to refine the checkpoint mechanism.
		newTS, err = mgr.refreshTSAndUpdateCP()
		if err == nil {
			for _, ei := range bc.engines {
				ei.openedEngine.SetTS(newTS)
			}
		}
	}

	return true, true, err
}

func (bc *litBackendCtx) unsafeImportAndReset(ei *engineInfo) error {
	logger := log.FromContext(bc.ctx).With(
		zap.Stringer("engineUUID", ei.uuid),
	)
	logger.Info(LitInfoUnsafeImport,
		zap.Int64("index ID", ei.indexID),
		zap.String("usage info", bc.diskRoot.UsageInfo()))

	closedEngine := backend.NewClosedEngine(bc.backend, logger, ei.uuid, 0)

	regionSplitSize := int64(lightning.SplitRegionSize) * int64(lightning.MaxSplitRegionSizeRatio)
	regionSplitKeys := int64(lightning.SplitRegionKeys)
	if err := closedEngine.Import(bc.ctx, regionSplitSize, regionSplitKeys); err != nil {
		logutil.Logger(bc.ctx).Error(LitErrIngestDataErr, zap.Int64("index ID", ei.indexID),
			zap.String("usage info", bc.diskRoot.UsageInfo()))
		return err
	}

	resetFn := bc.backend.ResetEngineSkipAllocTS
	mgr := bc.GetCheckpointManager()
	if mgr == nil {
		// disttask case, no need to refresh TS.
		//
		// TODO(lance6716): for disttask local sort case, we need to use a fixed TS. But
		// it doesn't have checkpoint, so we need to find a way to save TS.
		resetFn = bc.backend.ResetEngine
	}

	err := resetFn(bc.ctx, ei.uuid)
	failpoint.Inject("mockResetEngineFailed", func() {
		err = fmt.Errorf("mock reset engine failed")
	})
	if err != nil {
		logutil.Logger(bc.ctx).Error(LitErrResetEngineFail, zap.Int64("index ID", ei.indexID))
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
	bc.diskRoot.UpdateUsage()
	shouldImport = bc.diskRoot.ShouldImport()
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

// AttachCheckpointManager attaches a checkpoint manager to the backend context.
func (bc *litBackendCtx) AttachCheckpointManager(mgr *CheckpointManager) {
	bc.checkpointMgr = mgr
}

// GetCheckpointManager returns the checkpoint manager attached to the backend context.
func (bc *litBackendCtx) GetCheckpointManager() *CheckpointManager {
	return bc.checkpointMgr
}

// GetLocalBackend returns the local backend.
func (bc *litBackendCtx) GetLocalBackend() *local.Backend {
	return bc.backend
}
