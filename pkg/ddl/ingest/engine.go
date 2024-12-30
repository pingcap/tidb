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
	"sync"

	"github.com/google/uuid"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/util/generic"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// Engine is the interface for the engine that can be used to write key-value pairs.
type Engine interface {
	Flush() error
	Close(cleanup bool)
	CreateWriter(id int, writerCfg *backend.LocalWriterConfig) (Writer, error)
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
	unique       bool
	openedEngine *backend.OpenedEngine

	uuid        uuid.UUID
	cfg         *backend.EngineConfig
	writerCache generic.SyncMap[int, backend.EngineWriter]
	memRoot     MemRoot
	flushLock   *sync.RWMutex
}

// newEngineInfo create a new engineInfo struct.
func newEngineInfo(
	ctx context.Context,
	jobID, indexID int64,
	unique bool,
	cfg *backend.EngineConfig,
	en *backend.OpenedEngine,
	uuid uuid.UUID,
	memRoot MemRoot,
) *engineInfo {
	return &engineInfo{
		ctx:          ctx,
		jobID:        jobID,
		indexID:      indexID,
		unique:       unique,
		cfg:          cfg,
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

// Close closes the engine and `cleanup` controls whether removes the local intermediate files.
func (ei *engineInfo) Close(cleanup bool) {
	if ei.openedEngine == nil {
		return
	}
	err := ei.closeWriters()
	if err != nil {
		logutil.Logger(ei.ctx).Error(LitErrCloseWriterErr, zap.Error(err),
			zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
	}

	indexEngine := ei.openedEngine
	closedEngine, err := indexEngine.Close(ei.ctx)
	if err != nil {
		logutil.Logger(ei.ctx).Error(LitErrCloseEngineErr, zap.Error(err),
			zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
		return
	}
	ei.openedEngine = nil
	if cleanup {
		// local intermediate files will be removed.
		err = closedEngine.Cleanup(ei.ctx)
		if err != nil {
			logutil.Logger(ei.ctx).Error(LitErrCleanEngineErr, zap.Error(err),
				zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID))
		}
	}
}

// writerContext is used to keep a lightning local writer for each backfill worker.
type writerContext struct {
	ctx    context.Context
	lWrite backend.EngineWriter
	fLock  *sync.RWMutex
}

// CreateWriter creates a new writerContext.
func (ei *engineInfo) CreateWriter(id int, writerCfg *backend.LocalWriterConfig) (Writer, error) {
	ei.memRoot.RefreshConsumption()
	ok := ei.memRoot.CheckConsume(structSizeWriterCtx)
	if !ok {
		return nil, genWriterAllocMemFailedErr(ei.ctx, ei.memRoot, ei.jobID, ei.indexID)
	}

	wCtx, err := ei.newWriterContext(id, writerCfg)
	if err != nil {
		logutil.Logger(ei.ctx).Error(LitErrCreateContextFail, zap.Error(err),
			zap.Int64("job ID", ei.jobID), zap.Int64("index ID", ei.indexID),
			zap.Int("worker ID", id))
		return nil, err
	}

	ei.memRoot.Consume(structSizeWriterCtx)
	logutil.Logger(ei.ctx).Info(LitInfoCreateWrite, zap.Int64("job ID", ei.jobID),
		zap.Int64("index ID", ei.indexID), zap.Int("worker ID", id),
		zap.Int64("allocate memory", structSizeWriterCtx+writerCfg.Local.MemCacheSize),
		zap.Int64("current memory usage", ei.memRoot.CurrentUsage()),
		zap.Int64("max memory quota", ei.memRoot.MaxMemoryQuota()))
	return wCtx, err
}

// newWriterContext will get worker local writer from engine info writer cache first, if exists.
// If local writer not exist, then create new one and store it into engine info writer cache.
// note: operate ei.writeCache map is not thread safe please make sure there is sync mechanism to
// make sure the safe.
func (ei *engineInfo) newWriterContext(workerID int, writerCfg *backend.LocalWriterConfig) (*writerContext, error) {
	lWrite, exist := ei.writerCache.Load(workerID)
	if !exist {
		ok := ei.memRoot.CheckConsume(writerCfg.Local.MemCacheSize)
		if !ok {
			return nil, genWriterAllocMemFailedErr(ei.ctx, ei.memRoot, ei.jobID, ei.indexID)
		}
		var err error
		lWrite, err = ei.openedEngine.LocalWriter(ei.ctx, writerCfg)
		if err != nil {
			return nil, err
		}
		// Cache the local writer.
		ei.writerCache.Store(workerID, lWrite)
		ei.memRoot.ConsumeWithTag(encodeBackendTag(ei.jobID), writerCfg.Local.MemCacheSize)
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
