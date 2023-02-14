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

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	lightning "github.com/pingcap/tidb/br/pkg/lightning/config"
	tikv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// BackendContext store a backend info for add index reorg task.
type BackendContext struct {
	jobID    int64
	backend  *backend.Backend
	ctx      context.Context
	cfg      *lightning.Config
	EngMgr   engineManager
	sysVars  map[string]string
	diskRoot DiskRoot
	done     bool
}

// FinishImport imports all the key-values in engine into the storage, collects the duplicate errors if any, and
// removes the engine from the backend context.
func (bc *BackendContext) FinishImport(indexID int64, unique bool, tbl table.Table) error {
	ei, exist := bc.EngMgr.Load(indexID)
	if !exist {
		return dbterror.ErrIngestFailed.FastGenByArgs("ingest engine not found")
	}

	err := ei.ImportAndClean()
	if err != nil {
		return err
	}

	// Check remote duplicate value for the index.
	if unique {
		hasDupe, err := bc.backend.CollectRemoteDuplicateRows(bc.ctx, tbl, tbl.Meta().Name.L, &kv.SessionOptions{
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

const importThreshold = 0.85

// Flush checks the disk quota and imports the current key-values in engine to the storage.
func (bc *BackendContext) Flush(indexID int64) error {
	ei, exist := bc.EngMgr.Load(indexID)
	if !exist {
		logutil.BgLogger().Error(LitErrGetEngineFail, zap.Int64("index ID", indexID))
		return dbterror.ErrIngestFailed.FastGenByArgs("ingest engine not found")
	}

	err := bc.diskRoot.UpdateUsageAndQuota()
	if err != nil {
		logutil.BgLogger().Error(LitErrUpdateDiskStats, zap.Int64("index ID", indexID))
		return err
	}

	if bc.diskRoot.CurrentUsage() >= uint64(importThreshold*float64(bc.diskRoot.MaxQuota())) {
		// TODO: it should be changed according checkpoint solution.
		// Flush writer cached data into local disk for engine first.
		err := ei.Flush()
		if err != nil {
			return err
		}
		logutil.BgLogger().Info(LitInfoUnsafeImport, zap.Int64("index ID", indexID),
			zap.Uint64("current disk usage", bc.diskRoot.CurrentUsage()),
			zap.Uint64("max disk quota", bc.diskRoot.MaxQuota()))
		err = bc.backend.UnsafeImportAndReset(bc.ctx, ei.uuid, int64(lightning.SplitRegionSize)*int64(lightning.MaxSplitRegionSizeRatio), int64(lightning.SplitRegionKeys))
		if err != nil {
			logutil.BgLogger().Error(LitErrIngestDataErr, zap.Int64("index ID", indexID),
				zap.Error(err), zap.Uint64("current disk usage", bc.diskRoot.CurrentUsage()),
				zap.Uint64("max disk quota", bc.diskRoot.MaxQuota()))
			return err
		}
	}
	return nil
}

// Done returns true if the lightning backfill is done.
func (bc *BackendContext) Done() bool {
	return bc.done
}

// SetDone sets the done flag.
func (bc *BackendContext) SetDone() {
	bc.done = true
}
