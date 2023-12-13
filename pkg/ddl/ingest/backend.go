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
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	lightning "github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	tikv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/generic"
	"github.com/pingcap/tidb/pkg/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

// BackendCtx is the backend context for add index reorg task.
type BackendCtx interface {
	Register(jobID, indexID int64, schemaName, tableName string) (Engine, error)
	Unregister(jobID, indexID int64)

	CollectRemoteDuplicateRows(indexID int64, tbl table.Table) error
	FinishImport(indexID int64, unique bool, tbl table.Table) error
	ResetWorkers(jobID int64)
	Flush(indexID int64, mode FlushMode) (flushed, imported bool, err error)
	Done() bool
	SetDone()

	AttachCheckpointManager(*CheckpointManager)
	GetCheckpointManager() *CheckpointManager

	GetLocalBackend() *local.Backend
}

// FlushMode is used to control how to flush.
type FlushMode byte

const (
	// FlushModeAuto means flush when the memory table size reaches the threshold.
	FlushModeAuto FlushMode = iota
	// FlushModeForceLocal means flush all data to local storage.
	FlushModeForceLocal
	// FlushModeForceLocalAndCheckDiskQuota means flush all data to local storage and check disk quota.
	FlushModeForceLocalAndCheckDiskQuota
	// FlushModeForceGlobal means import all data in local storage to global storage.
	FlushModeForceGlobal
)

// litBackendCtx store a backend info for add index reorg task.
type litBackendCtx struct {
	generic.SyncMap[int64, *engineInfo]
	MemRoot  MemRoot
	DiskRoot DiskRoot
	jobID    int64
	backend  *local.Backend
	ctx      context.Context
	cfg      *lightning.Config
	sysVars  map[string]string
	diskRoot DiskRoot
	done     bool

	timeOfLastFlush atomicutil.Time
	updateInterval  time.Duration
	checkpointMgr   *CheckpointManager
	etcdClient      *clientv3.Client
}

// CollectRemoteDuplicateRows collects duplicate rows from remote TiKV.
func (bc *litBackendCtx) CollectRemoteDuplicateRows(indexID int64, tbl table.Table) error {
	errorMgr := errormanager.New(nil, bc.cfg, log.Logger{Logger: logutil.Logger(bc.ctx)})
	// backend must be a local backend.
	// todo: when we can separate local backend completely from tidb backend, will remove this cast.
	//nolint:forcetypeassert
	dupeController := bc.backend.GetDupeController(bc.cfg.TikvImporter.RangeConcurrency*2, errorMgr)
	hasDupe, err := dupeController.CollectRemoteDuplicateRows(bc.ctx, tbl, tbl.Meta().Name.L, &encode.SessionOptions{
		SQLMode: mysql.ModeStrictAllTables,
		SysVars: bc.sysVars,
		IndexID: indexID,
	})
	if err != nil {
		logutil.Logger(bc.ctx).Error(LitInfoRemoteDupCheck, zap.Error(err),
			zap.String("table", tbl.Meta().Name.O), zap.Int64("index ID", indexID))
		return err
	} else if hasDupe {
		logutil.Logger(bc.ctx).Error(LitErrRemoteDupExistErr,
			zap.String("table", tbl.Meta().Name.O), zap.Int64("index ID", indexID))
		return tikv.ErrKeyExists
	}
	return nil
}

// FinishImport imports all the key-values in engine into the storage, collects the duplicate errors if any, and
// removes the engine from the backend context.
func (bc *litBackendCtx) FinishImport(indexID int64, unique bool, tbl table.Table) error {
	ei, exist := bc.Load(indexID)
	if !exist {
		return dbterror.ErrIngestFailed.FastGenByArgs("ingest engine not found")
	}

	err := ei.ImportAndClean()
	if err != nil {
		return err
	}

	failpoint.Inject("mockFinishImportErr", func() {
		failpoint.Return(fmt.Errorf("mock finish import error"))
	})

	// Check remote duplicate value for the index.
	if unique {
		errorMgr := errormanager.New(nil, bc.cfg, log.Logger{Logger: logutil.Logger(bc.ctx)})
		// backend must be a local backend.
		// todo: when we can separate local backend completely from tidb backend, will remove this cast.
		//nolint:forcetypeassert
		dupeController := bc.backend.GetDupeController(bc.cfg.TikvImporter.RangeConcurrency*2, errorMgr)
		hasDupe, err := dupeController.CollectRemoteDuplicateRows(bc.ctx, tbl, tbl.Meta().Name.L, &encode.SessionOptions{
			SQLMode: mysql.ModeStrictAllTables,
			SysVars: bc.sysVars,
			IndexID: ei.indexID,
		})
		if err != nil {
			logutil.Logger(bc.ctx).Error(LitInfoRemoteDupCheck, zap.Error(err),
				zap.String("table", tbl.Meta().Name.O), zap.Int64("index ID", indexID))
			return err
		} else if hasDupe {
			logutil.Logger(bc.ctx).Error(LitErrRemoteDupExistErr,
				zap.String("table", tbl.Meta().Name.O), zap.Int64("index ID", indexID))
			return tikv.ErrKeyExists
		}
	}
	return nil
}

func acquireLock(ctx context.Context, se *concurrency.Session, key string) (*concurrency.Mutex, error) {
	mu := concurrency.NewMutex(se, key)
	err := mu.Lock(ctx)
	if err != nil {
		return nil, err
	}
	return mu, nil
}

// Flush checks the disk quota and imports the current key-values in engine to the storage.
func (bc *litBackendCtx) Flush(indexID int64, mode FlushMode) (flushed, imported bool, err error) {
	ei, exist := bc.Load(indexID)
	if !exist {
		logutil.Logger(bc.ctx).Error(LitErrGetEngineFail, zap.Int64("index ID", indexID))
		return false, false, dbterror.ErrIngestFailed.FastGenByArgs("ingest engine not found")
	}

	shouldFlush, shouldImport := bc.ShouldSync(mode)
	if !shouldFlush {
		return false, false, nil
	}
	if !ei.flushing.CompareAndSwap(false, true) {
		return false, false, nil
	}
	defer ei.flushing.Store(false)
	ei.flushLock.Lock()
	defer ei.flushLock.Unlock()

	err = ei.Flush()
	if err != nil {
		return false, false, err
	}
	bc.timeOfLastFlush.Store(time.Now())

	if !shouldImport {
		return true, false, nil
	}

	// Use distributed lock if run in distributed mode).
	if bc.etcdClient != nil {
		distLockKey := fmt.Sprintf("/tidb/distributeLock/%d/%d", bc.jobID, indexID)
		se, _ := concurrency.NewSession(bc.etcdClient)
		mu, err := acquireLock(bc.ctx, se, distLockKey)
		if err != nil {
			return true, false, err
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
	err = bc.unsafeImportAndReset(ei)
	if err != nil {
		return true, false, err
	}
	return true, true, nil
}

func (bc *litBackendCtx) unsafeImportAndReset(ei *engineInfo) error {
	logutil.Logger(bc.ctx).Info(LitInfoUnsafeImport, zap.Int64("index ID", ei.indexID),
		zap.String("usage info", bc.diskRoot.UsageInfo()))
	logger := log.FromContext(bc.ctx).With(
		zap.Stringer("engineUUID", ei.uuid),
	)

	ei.closedEngine = backend.NewClosedEngine(bc.backend, logger, ei.uuid, 0)

	regionSplitSize := int64(lightning.SplitRegionSize) * int64(lightning.MaxSplitRegionSizeRatio)
	regionSplitKeys := int64(lightning.SplitRegionKeys)
	if err := ei.closedEngine.Import(bc.ctx, regionSplitSize, regionSplitKeys); err != nil {
		logutil.Logger(bc.ctx).Error(LitErrIngestDataErr, zap.Int64("index ID", ei.indexID),
			zap.String("usage info", bc.diskRoot.UsageInfo()))
		return err
	}

	err := bc.backend.ResetEngine(bc.ctx, ei.uuid)
	if err != nil {
		logutil.Logger(bc.ctx).Error(LitErrResetEngineFail, zap.Int64("index ID", ei.indexID))
		err1 := ei.closedEngine.Cleanup(bc.ctx)
		if err1 != nil {
			logutil.Logger(ei.ctx).Error(LitErrCleanEngineErr, zap.Error(err1),
				zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
		}
		ei.openedEngine = nil
		ei.closedEngine = nil
		return err
	}
	return nil
}

// ForceSyncFlagForTest is a flag to force sync only for test.
var ForceSyncFlagForTest = false

func (bc *litBackendCtx) ShouldSync(mode FlushMode) (shouldFlush bool, shouldImport bool) {
	if mode == FlushModeForceGlobal || ForceSyncFlagForTest {
		return true, true
	}
	if mode == FlushModeForceLocal {
		return true, false
	}
	bc.diskRoot.UpdateUsage()
	shouldImport = bc.diskRoot.ShouldImport()
	if mode == FlushModeForceLocalAndCheckDiskQuota {
		shouldFlush = true
	} else {
		interval := bc.updateInterval
		// This failpoint will be manually set through HTTP status port.
		failpoint.Inject("mockSyncIntervalMs", func(val failpoint.Value) {
			if v, ok := val.(int); ok {
				interval = time.Duration(v) * time.Millisecond
			}
		})
		shouldFlush = shouldImport ||
			time.Since(bc.timeOfLastFlush.Load()) >= interval
	}
	return shouldFlush, shouldImport
}

// Done returns true if the lightning backfill is done.
func (bc *litBackendCtx) Done() bool {
	return bc.done
}

// SetDone sets the done flag.
func (bc *litBackendCtx) SetDone() {
	bc.done = true
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
