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
	"time"

	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	lightning "github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	tikv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/generic"
	"github.com/pingcap/tidb/util/logutil"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

// BackendCtx is the backend context for add index reorg task.
type BackendCtx interface {
	Register(jobID, indexID int64, schemaName, tableName string) (Engine, error)
	Unregister(jobID, indexID int64)

	CollectRemoteDuplicateRows(indexID int64, tbl table.Table) error
	FinishImport(indexID int64, unique bool, tbl table.Table) error
	ResetWorkers(jobID, indexID int64)
	Flush(indexID int64, mode FlushMode) (flushed, imported bool, err error)
	Done() bool
	SetDone()

	AttachCheckpointManager(*CheckpointManager)
	GetCheckpointManager() *CheckpointManager
}

// FlushMode is used to control how to flush.
type FlushMode byte

const (
	// FlushModeAuto means flush when the memory table size reaches the threshold.
	FlushModeAuto FlushMode = iota
	// FlushModeForceLocal means flush all data to local storage.
	FlushModeForceLocal
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
}

// CollectRemoteDuplicateRows collects duplicate rows from remote TiKV.
func (bc *litBackendCtx) CollectRemoteDuplicateRows(indexID int64, tbl table.Table) error {
	errorMgr := errormanager.New(nil, bc.cfg, log.Logger{Logger: logutil.BgLogger()})
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
		logutil.BgLogger().Error(LitInfoRemoteDupCheck, zap.Error(err),
			zap.String("table", tbl.Meta().Name.O), zap.Int64("index ID", indexID))
		return err
	} else if hasDupe {
		logutil.BgLogger().Error(LitErrRemoteDupExistErr,
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

	// Check remote duplicate value for the index.
	if unique {
		errorMgr := errormanager.New(nil, bc.cfg, log.Logger{Logger: logutil.BgLogger()})
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
			logutil.BgLogger().Error(LitInfoRemoteDupCheck, zap.Error(err),
				zap.String("table", tbl.Meta().Name.O), zap.Int64("index ID", indexID))
			return err
		} else if hasDupe {
			logutil.BgLogger().Error(LitErrRemoteDupExistErr,
				zap.String("table", tbl.Meta().Name.O), zap.Int64("index ID", indexID))
			return tikv.ErrKeyExists
		}
	}
	return nil
}

// Flush checks the disk quota and imports the current key-values in engine to the storage.
func (bc *litBackendCtx) Flush(indexID int64, mode FlushMode) (flushed, imported bool, err error) {
	ei, exist := bc.Load(indexID)
	if !exist {
		logutil.BgLogger().Error(LitErrGetEngineFail, zap.Int64("index ID", indexID))
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
	logutil.BgLogger().Info(LitInfoUnsafeImport, zap.Int64("index ID", indexID),
		zap.String("usage info", bc.diskRoot.UsageInfo()))
	err = bc.backend.UnsafeImportAndReset(bc.ctx, ei.uuid, int64(lightning.SplitRegionSize)*int64(lightning.MaxSplitRegionSizeRatio), int64(lightning.SplitRegionKeys))
	if err != nil {
		logutil.BgLogger().Error(LitErrIngestDataErr, zap.Int64("index ID", indexID),
			zap.String("usage info", bc.diskRoot.UsageInfo()))
		return true, false, err
	}
	return true, true, nil
}

func (bc *litBackendCtx) ShouldSync(mode FlushMode) (shouldFlush bool, shouldImport bool) {
	if mode == FlushModeForceGlobal {
		return true, true
	}
	if mode == FlushModeForceLocal {
		return true, false
	}
	bc.diskRoot.UpdateUsage()
	shouldImport = bc.diskRoot.ShouldImport()
	shouldFlush = shouldImport ||
		time.Since(bc.timeOfLastFlush.Load()) >= bc.updateInterval
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
