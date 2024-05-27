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
	"sync"

	"github.com/google/uuid"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/util/generic"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// Engine is the interface for the engine that can be used to write key-value pairs.
type Engine interface {
	Flush() error
	ImportAndClean() error
	Clean()
	CreateWriter(id int) (Writer, error)
}

// Writer is the interface for the writer that can be used to write key-value pairs.
type Writer interface {
	// WriteRow writes one row into downstream.
	// To enable uniqueness check, the handle should be non-empty.
	WriteRow(ctx context.Context, idxKey, idxVal []byte, handle tidbkv.Handle) error
	LockForWrite() (unlock func())
}

// engineInfo is the engine for one index reorg task, each task will create several new writers under the
// Opened Engine. Note engineInfo is not thread safe.
type engineInfo struct {
	ctx          context.Context
	jobID        int64
	indexID      int64
	openedEngine *backend.OpenedEngine
	// closedEngine is set only when all data is finished written and all writers are
	// closed.
	closedEngine *backend.ClosedEngine
	uuid         uuid.UUID
	cfg          *backend.EngineConfig
	litCfg       *config.Config
	writerCache  generic.SyncMap[int, backend.EngineWriter]
	memRoot      MemRoot
	flushLock    *sync.RWMutex
}

// newEngineInfo create a new engineInfo struct.
func newEngineInfo(
	ctx context.Context,
	jobID, indexID int64,
	cfg *backend.EngineConfig,
	litCfg *config.Config,
	en *backend.OpenedEngine,
	uuid uuid.UUID,
	memRoot MemRoot,
) *engineInfo {
	return &engineInfo{
		ctx:          ctx,
		jobID:        jobID,
		indexID:      indexID,
		cfg:          cfg,
		litCfg:       litCfg,
		openedEngine: en,
		uuid:         uuid,
		writerCache:  generic.NewSyncMap[int, backend.EngineWriter](4),
		memRoot:      memRoot,
		flushLock:    &sync.RWMutex{},
	}
}

// Flush imports all the key-values in engine to the storage.
func (ei *engineInfo) Flush() error {
	if ei.openedEngine == nil {
		logutil.Logger(ei.ctx).Warn("engine is not open, skipping flush",
			zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
		return nil
	}
	err := ei.openedEngine.Flush(ei.ctx)
	if err != nil {
		logutil.Logger(ei.ctx).Error(LitErrFlushEngineErr, zap.Error(err),
			zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
		return err
	}
	return nil
}

// Clean closes the engine and removes the local intermediate files.
func (ei *engineInfo) Clean() {
	if ei.openedEngine == nil {
		return
	}
	indexEngine := ei.openedEngine
	closedEngine, err := indexEngine.Close(ei.ctx)
	if err != nil {
		logutil.Logger(ei.ctx).Error(LitErrCloseEngineErr, zap.Error(err),
			zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
		return
	}
	ei.openedEngine = nil
	err = ei.closeWriters()
	if err != nil {
		logutil.Logger(ei.ctx).Error(LitErrCloseWriterErr, zap.Error(err),
			zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
	}
	// Here the local intermediate files will be removed.
	err = closedEngine.Cleanup(ei.ctx)
	if err != nil {
		logutil.Logger(ei.ctx).Error(LitErrCleanEngineErr, zap.Error(err),
			zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
	}
}

// ImportAndClean imports the engine data to TiKV and cleans up the local intermediate files.
func (ei *engineInfo) ImportAndClean() error {
	if ei.openedEngine != nil {
		logutil.Logger(ei.ctx).Info(LitInfoCloseEngine, zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
		closeEngine, err1 := ei.openedEngine.Close(ei.ctx)
		if err1 != nil {
			logutil.Logger(ei.ctx).Error(LitErrCloseEngineErr, zap.Error(err1),
				zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
			return err1
		}
		err := ei.closeWriters()
		if err != nil {
			logutil.Logger(ei.ctx).Error(LitErrCloseWriterErr, zap.Error(err),
				zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
			return err
		}
		ei.openedEngine = nil
		ei.closedEngine = closeEngine
	}
	if ei.closedEngine != nil {
		// Ingest data to TiKV.
		logutil.Logger(ei.ctx).Info(LitInfoStartImport, zap.Int64("job ID", ei.jobID),
			zap.Int64("index ID", ei.indexID),
			zap.String("split region size", strconv.FormatInt(int64(config.SplitRegionSize), 10)))
		err := ei.closedEngine.Import(ei.ctx, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
		if err != nil {
			logLevel := zap.ErrorLevel
			if common.ErrFoundDuplicateKeys.Equal(err) {
				logLevel = zap.WarnLevel
			}
			logutil.Logger(ei.ctx).Log(logLevel, LitErrIngestDataErr, zap.Error(err),
				zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
			return err
		}

		// Clean up the engine local workspace.
		err = ei.closedEngine.Cleanup(ei.ctx)
		if err != nil {
			logutil.Logger(ei.ctx).Error(LitErrCloseEngineErr, zap.Error(err),
				zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
			return err
		}
		ei.closedEngine = nil
	}
	return nil
}

// writerContext is used to keep a lightning local writer for each backfill worker.
type writerContext struct {
	ctx    context.Context
	lWrite backend.EngineWriter
	fLock  *sync.RWMutex
}

// CreateWriter creates a new writerContext.
func (ei *engineInfo) CreateWriter(id int) (Writer, error) {
	ei.memRoot.RefreshConsumption()
	ok := ei.memRoot.CheckConsume(structSizeWriterCtx)
	if !ok {
		return nil, genWriterAllocMemFailedErr(ei.ctx, ei.memRoot, ei.jobID, ei.indexID)
	}

	wCtx, err := ei.newWriterContext(id)
	if err != nil {
		logutil.Logger(ei.ctx).Error(LitErrCreateContextFail, zap.Error(err),
			zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID),
			zap.Int("worker ID", id))
		return nil, err
	}

	ei.memRoot.Consume(structSizeWriterCtx)
	logutil.Logger(ei.ctx).Info(LitInfoCreateWrite, zap.Int64("job ID", ei.jobID),
		zap.Int64("index ID", ei.indexID), zap.Int("worker ID", id),
		zap.Int64("allocate memory", structSizeWriterCtx),
		zap.Int64("current memory usage", ei.memRoot.CurrentUsage()),
		zap.Int64("max memory quota", ei.memRoot.MaxMemoryQuota()))
	return wCtx, err
}

// newWriterContext will get worker local writer from engine info writer cache first, if exists.
// If local writer not exist, then create new one and store it into engine info writer cache.
// note: operate ei.writeCache map is not thread safe please make sure there is sync mechanism to
// make sure the safe.
func (ei *engineInfo) newWriterContext(workerID int) (*writerContext, error) {
	lWrite, exist := ei.writerCache.Load(workerID)
	if !exist {
		ok := ei.memRoot.CheckConsume(int64(ei.litCfg.TikvImporter.LocalWriterMemCacheSize))
		if !ok {
			return nil, genWriterAllocMemFailedErr(ei.ctx, ei.memRoot, ei.jobID, ei.indexID)
		}
		var err error
		lWrite, err = ei.openedEngine.LocalWriter(ei.ctx, &backend.LocalWriterConfig{})
		if err != nil {
			return nil, err
		}
		// Cache the local writer.
		ei.writerCache.Store(workerID, lWrite)
		ei.memRoot.Consume(int64(ei.litCfg.TikvImporter.LocalWriterMemCacheSize))
	}
	wc := &writerContext{
		ctx:    ei.ctx,
		lWrite: lWrite,
		fLock:  ei.flushLock,
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
			ei.memRoot.Release(int64(ei.litCfg.TikvImporter.LocalWriterMemCacheSize))
		}
		ei.writerCache.Delete(wid)
		ei.memRoot.Release(structSizeWriterCtx)
	}
	return firstErr
}

// WriteRow Write one row into local writer buffer.
func (wCtx *writerContext) WriteRow(ctx context.Context, key, idxVal []byte, handle tidbkv.Handle) error {
	kvs := make([]common.KvPair, 1)
	kvs[0].Key = key
	kvs[0].Val = idxVal
	if handle != nil {
		kvs[0].RowID = handle.Encoded()
	}
	row := kv.MakeRowsFromKvPairs(kvs)
	return wCtx.lWrite.AppendRows(ctx, nil, row)
}

// LockForWrite locks the local writer for write.
func (wCtx *writerContext) LockForWrite() (unlock func()) {
	wCtx.fLock.RLock()
	return func() {
		wCtx.fLock.RUnlock()
	}
}
