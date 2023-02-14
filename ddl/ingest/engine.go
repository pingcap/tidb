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
	"strconv"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/util/generic"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// One engine for one index reorg task, each task will create several new writers under the
// Opened Engine. Note engineInfo is not thread safe.
type engineInfo struct {
	ctx          context.Context
	jobID        int64
	indexID      int64
	openedEngine *backend.OpenedEngine
	uuid         uuid.UUID
	cfg          *backend.EngineConfig
	writerCount  int
	writerCache  generic.SyncMap[int, *backend.LocalEngineWriter]
	memRoot      MemRoot
	diskRoot     DiskRoot
	rowSeq       atomic.Int64
}

// NewEngineInfo create a new EngineInfo struct.
func NewEngineInfo(ctx context.Context, jobID, indexID int64, cfg *backend.EngineConfig,
	en *backend.OpenedEngine, uuid uuid.UUID, wCnt int, memRoot MemRoot, diskRoot DiskRoot) *engineInfo {
	return &engineInfo{
		ctx:          ctx,
		jobID:        jobID,
		indexID:      indexID,
		cfg:          cfg,
		openedEngine: en,
		uuid:         uuid,
		writerCount:  wCnt,
		writerCache:  generic.NewSyncMap[int, *backend.LocalEngineWriter](wCnt),
		memRoot:      memRoot,
		diskRoot:     diskRoot,
	}
}

// Flush imports all the key-values in engine to the storage.
func (ei *engineInfo) Flush() error {
	err := ei.openedEngine.Flush(ei.ctx)
	if err != nil {
		logutil.BgLogger().Error(LitErrFlushEngineErr, zap.Error(err),
			zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
		return err
	}
	return nil
}

func (ei *engineInfo) Clean() {
	if ei.openedEngine == nil {
		return
	}
	indexEngine := ei.openedEngine
	closedEngine, err := indexEngine.Close(ei.ctx, ei.cfg)
	if err != nil {
		logutil.BgLogger().Error(LitErrCloseEngineErr, zap.Error(err),
			zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
	}
	ei.openedEngine = nil
	err = ei.closeWriters()
	if err != nil {
		logutil.BgLogger().Error(LitErrCloseWriterErr, zap.Error(err),
			zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
	}
	// Here the local intermediate files will be removed.
	err = closedEngine.Cleanup(ei.ctx)
	if err != nil {
		logutil.BgLogger().Error(LitErrCleanEngineErr, zap.Error(err),
			zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
	}
}

func (ei *engineInfo) ImportAndClean() error {
	// Close engine and finish local tasks of lightning.
	logutil.BgLogger().Info(LitInfoCloseEngine, zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
	indexEngine := ei.openedEngine
	closeEngine, err1 := indexEngine.Close(ei.ctx, ei.cfg)
	if err1 != nil {
		logutil.BgLogger().Error(LitErrCloseEngineErr, zap.Error(err1),
			zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
		return err1
	}
	ei.openedEngine = nil
	err := ei.closeWriters()
	if err != nil {
		logutil.BgLogger().Error(LitErrCloseWriterErr, zap.Error(err),
			zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
		return err
	}

	err = ei.diskRoot.UpdateUsageAndQuota()
	if err != nil {
		logutil.BgLogger().Error(LitErrUpdateDiskStats, zap.Error(err),
			zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
		return err
	}

	// Ingest data to TiKV.
	logutil.BgLogger().Info(LitInfoStartImport, zap.Int64("job ID", ei.jobID),
		zap.Int64("index ID", ei.indexID),
		zap.String("split region size", strconv.FormatInt(int64(config.SplitRegionSize), 10)))
	err = closeEngine.Import(ei.ctx, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
	if err != nil {
		logutil.BgLogger().Error(LitErrIngestDataErr, zap.Error(err),
			zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
		return err
	}

	// Clean up the engine local workspace.
	err = closeEngine.Cleanup(ei.ctx)
	if err != nil {
		logutil.BgLogger().Error(LitErrCloseEngineErr, zap.Error(err),
			zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
		return err
	}
	return nil
}

// WriterContext is used to keep a lightning local writer for each backfill worker.
type WriterContext struct {
	ctx    context.Context
	rowSeq func() int64
	lWrite *backend.LocalEngineWriter
}

func (ei *engineInfo) NewWriterCtx(id int, unique bool) (*WriterContext, error) {
	ei.memRoot.RefreshConsumption()
	ok := ei.memRoot.CheckConsume(StructSizeWriterCtx)
	if !ok {
		return nil, genEngineAllocMemFailedErr(ei.memRoot, ei.jobID, ei.indexID)
	}

	wCtx, err := ei.newWriterContext(id, unique)
	if err != nil {
		logutil.BgLogger().Error(LitErrCreateContextFail, zap.Error(err),
			zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID),
			zap.Int("worker ID", id))
		return nil, err
	}

	ei.memRoot.Consume(StructSizeWriterCtx)
	logutil.BgLogger().Info(LitInfoCreateWrite, zap.Int64("job ID", ei.jobID),
		zap.Int64("index ID", ei.indexID), zap.Int("worker ID", id),
		zap.Int64("allocate memory", StructSizeWriterCtx),
		zap.Int64("current memory usage", ei.memRoot.CurrentUsage()),
		zap.Int64("max memory quota", ei.memRoot.MaxMemoryQuota()))
	return wCtx, err
}

// newWriterContext will get worker local writer from engine info writer cache first, if exists.
// If local writer not exist, then create new one and store it into engine info writer cache.
// note: operate ei.writeCache map is not thread safe please make sure there is sync mechanism to
// make sure the safe.
func (ei *engineInfo) newWriterContext(workerID int, unique bool) (*WriterContext, error) {
	lWrite, exist := ei.writerCache.Load(workerID)
	if !exist {
		var err error
		lWrite, err = ei.openedEngine.LocalWriter(ei.ctx, &backend.LocalWriterConfig{})
		if err != nil {
			return nil, err
		}
		// Cache the local writer.
		ei.writerCache.Store(workerID, lWrite)
	}
	wc := &WriterContext{
		ctx:    ei.ctx,
		lWrite: lWrite,
	}
	if unique {
		wc.rowSeq = func() int64 {
			return ei.rowSeq.Add(1)
		}
	}
	return wc, nil
}

func (ei *engineInfo) closeWriters() error {
	var firstErr error
	for _, wid := range ei.writerCache.Keys() {
		if w, ok := ei.writerCache.Load(wid); ok {
			_, err := w.Close(ei.ctx)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
			}
		}
		ei.writerCache.Delete(wid)
	}
	return firstErr
}

// WriteRow Write one row into local writer buffer.
func (wCtx *WriterContext) WriteRow(key, idxVal []byte) error {
	kvs := make([]common.KvPair, 1)
	kvs[0].Key = key
	kvs[0].Val = idxVal
	if wCtx.rowSeq != nil {
		kvs[0].RowID = wCtx.rowSeq()
	}
	row := kv.MakeRowsFromKvPairs(kvs)
	return wCtx.lWrite.WriteRows(wCtx.ctx, nil, row)
}
