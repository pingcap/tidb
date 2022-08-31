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

package lightning

import (
	"context"
	"strconv"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/util/generic"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// One engine for one index reorg task, each task will create several new writers under the
// Opened Engine. Note engineInfo is not thread safe.
type engineInfo struct {
	indexID      int64
	backCtx      *BackendContext
	openedEngine *backend.OpenedEngine
	uuid         uuid.UUID
	cfg          *backend.EngineConfig
	tableName    string
	writerCount  int
	writerCache  generic.SyncMap[int, *backend.LocalEngineWriter]
	memRoot      MemRoot
	diskRoot     DiskRoot
}

// NewEngineInfo create a new EngineInfo struct.
func NewEngineInfo(id int64, cfg *backend.EngineConfig, bCtx *BackendContext,
	en *backend.OpenedEngine, tblName string, uuid uuid.UUID, wCnt int, memRoot MemRoot, diskRoot DiskRoot) *engineInfo {
	return &engineInfo{
		indexID:      id,
		cfg:          cfg,
		backCtx:      bCtx,
		openedEngine: en,
		uuid:         uuid,
		tableName:    tblName,
		writerCount:  wCnt,
		writerCache:  generic.NewSyncMap[int, *backend.LocalEngineWriter](wCnt),
		memRoot:      memRoot,
		diskRoot:     diskRoot,
	}
}

// Flush imports all the key-values in engine to the storage.
func (ei *engineInfo) Flush(ctx context.Context) error {
	err := ei.openedEngine.Flush(ctx)
	if err != nil {
		logutil.BgLogger().Error(LitErrFlushEngineErr, zap.Int64("index ID", ei.indexID))
		return err
	}
	return nil
}

func (ei *engineInfo) Clean() {
	if ei.openedEngine == nil {
		return
	}
	indexEngine := ei.openedEngine
	closedEngine, err := indexEngine.Close(ei.backCtx.ctx, ei.cfg)
	if err != nil {
		logutil.BgLogger().Error(LitErrCloseEngineErr, zap.Int64("index ID", ei.indexID))
	}
	ei.openedEngine = nil
	// Here the local intermediate files will be removed.
	err = closedEngine.Cleanup(ei.backCtx.ctx)
	if err != nil {
		logutil.BgLogger().Error(LitErrCleanEngineErr, zap.Int64("index ID", ei.indexID))
	}
}

func (ei *engineInfo) ImportAndClean() error {
	// Close engine and finish local tasks of lightning.
	logutil.BgLogger().Info(LitInfoCloseEngine, zap.Int64("job ID", ei.backCtx.jobID), zap.Int64("index ID", ei.indexID))
	indexEngine := ei.openedEngine
	closeEngine, err1 := indexEngine.Close(ei.backCtx.ctx, ei.cfg)
	if err1 != nil {
		logutil.BgLogger().Error(LitErrCloseEngineErr, zap.Int64("job ID", ei.backCtx.jobID), zap.Int64("index ID", ei.indexID))
		return errors.New(LitErrCloseEngineErr)
	}
	ei.openedEngine = nil

	err := ei.diskRoot.UpdateUsageAndQuota()
	if err != nil {
		logutil.BgLogger().Error(LitErrUpdateDiskStats, zap.Int64("job ID", ei.backCtx.jobID), zap.Int64("index ID", ei.indexID))
		return err
	}

	// Ingest data to TiKV.
	logutil.BgLogger().Info(LitInfoStartImport, zap.Int64("job ID", ei.backCtx.jobID),
		zap.Int64("index ID", ei.indexID),
		zap.String("split region size", strconv.FormatInt(int64(config.SplitRegionSize), 10)))
	err = closeEngine.Import(ei.backCtx.ctx, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
	if err != nil {
		logutil.BgLogger().Error(LitErrIngestDataErr, zap.Int64("job ID", ei.backCtx.jobID), zap.Int64("index ID", ei.indexID))
		return errors.New(LitErrIngestDataErr)
	}

	// Clean up the engine local workspace.
	err = closeEngine.Cleanup(ei.backCtx.ctx)
	if err != nil {
		logutil.BgLogger().Error(LitErrCloseEngineErr, zap.Int64("job ID", ei.backCtx.jobID), zap.Int64("index ID", ei.indexID))
		return errors.New(LitErrCloseEngineErr)
	}
	return nil
}

// WriterContext is used to keep a lightning local writer for each backfill worker.
type WriterContext struct {
	ctx    context.Context
	lWrite *backend.LocalEngineWriter
}

func (ei *engineInfo) NewWorkerCtx(id int) (*WriterContext, error) {
	memRequire := StructSizeWriterCtx
	// First to check the memory usage.
	ei.memRoot.RefreshConsumption()
	ok := ei.memRoot.TestConsume(memRequire)
	if !ok {
		return nil, logAllocMemFailedEngine(ei.memRoot, ei.backCtx.jobID, ei.indexID)
	}

	wCtx, err := ei.newWorkerContext(id)
	if err != nil {
		logutil.BgLogger().Error(LitErrCreateContextFail, zap.Int64("index ID", ei.indexID),
			zap.Int("worker ID", id))
		return nil, err
	}

	ei.memRoot.Consume(memRequire)
	logutil.BgLogger().Info(LitInfoCreateWrite, zap.Int64("index ID", ei.indexID),
		zap.Int("worker ID", id),
		zap.Int64("allocate memory", memRequire),
		zap.Int64("current memory usage", ei.memRoot.CurrentUsage()),
		zap.Int64("max memory quota", ei.memRoot.MaxMemoryQuota()))
	return wCtx, err
}

// InitWorkerContext will get worker local writer from engine info writer cache first, if exists.
// If local writer not exist, then create new one and store it into engine info writer cache.
// note: operate ei.writeCache map is not thread safe please make sure there is sync mechanism to
// make sure the safe.
func (ei *engineInfo) newWorkerContext(workerID int) (*WriterContext, error) {
	// First get local writer from engine cache.
	lWrite, exist := ei.writerCache.Load(workerID)
	// If not exist then build one
	if !exist {
		var err error
		lWrite, err = ei.openedEngine.LocalWriter(ei.backCtx.ctx, &backend.LocalWriterConfig{})
		if err != nil {
			return nil, err
		}
		// Cache the local writer.
		ei.writerCache.Store(workerID, lWrite)
	}
	return &WriterContext{
		ctx:    ei.backCtx.ctx,
		lWrite: lWrite,
	}, nil
}

// WriteRow Write one row into local writer buffer.
func (wCtx *WriterContext) WriteRow(key, idxVal []byte) error {
	var kvs []common.KvPair = make([]common.KvPair, 1)
	kvs[0].Key = key
	kvs[0].Val = idxVal
	row := kv.MakeRowsFromKvPairs(kvs)
	return wCtx.lWrite.WriteRows(wCtx.ctx, nil, row)
}
