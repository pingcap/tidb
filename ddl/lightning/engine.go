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
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// One engine for one index reorg task, each task will create several new writers under the
// Opened Engine. Note engineInfo is not thread safe.
type engineInfo struct {
	id  int64
	key string

	backCtx      *BackendContext
	openedEngine *backend.OpenedEngine
	uuid         uuid.UUID
	cfg          *backend.EngineConfig
	tableName    string
	writerCount  int
	writerCache  resourceManager[backend.LocalEngineWriter]
	memRoot      MemRoot
}

// NewEngineInfo create a new EngineInfo struct.
func NewEngineInfo(id int64, key string, cfg *backend.EngineConfig, bCtx *BackendContext,
	en *backend.OpenedEngine, tblName string, uuid uuid.UUID, wCnt int, memRoot MemRoot) *engineInfo {
	ei := engineInfo{
		id:           id,
		key:          key,
		cfg:          cfg,
		backCtx:      bCtx,
		openedEngine: en,
		uuid:         uuid,
		tableName:    tblName,
		writerCount:  wCnt,
		memRoot:      memRoot,
	}
	ei.writerCache.init(wCnt)
	return &ei
}

// Flush imports all the key-values in engine to the storage.
func (ei *engineInfo) Flush(ctx context.Context) error {
	err := ei.openedEngine.Flush(ctx)
	if err != nil {
		logutil.BgLogger().Error(LitErrFlushEngineErr, zap.String("Engine key:", ei.key))
		return err
	}
	return nil
}

func (ei *engineInfo) Clean() {
	indexEngine := ei.openedEngine
	closedEngine, err := indexEngine.Close(ei.backCtx.ctx, ei.cfg)
	if err != nil {
		logutil.BgLogger().Error(LitErrCloseEngineErr, zap.String("Engine key", ei.key))
	}
	// Here the local intermediate file will be removed.
	err = closedEngine.Cleanup(ei.backCtx.ctx)
	if err != nil {
		logutil.BgLogger().Error(LitErrCleanEngineErr, zap.String("Engine key", ei.key))
	}
}

func (ei *engineInfo) ImportAndClean() error {
	keyMsg := "backend key:" + ei.backCtx.key + "Engine key:" + ei.key
	// Close engine and finish local tasks of lightning.
	logutil.BgLogger().Info(LitInfoCloseEngine, zap.String("backend key", ei.backCtx.key), zap.String("Engine key", ei.key))
	indexEngine := ei.openedEngine
	closeEngine, err1 := indexEngine.Close(ei.backCtx.ctx, ei.cfg)
	if err1 != nil {
		errMsg := LitErrCloseEngineErr + keyMsg
		logutil.BgLogger().Error(errMsg)
		return errors.New(errMsg)
	}

	// Reset disk quota before ingest, if user changed it.
	GlobalEnv.checkAndResetQuota()

	// Ingest data to TiKV.
	logutil.BgLogger().Info(LitInfoStartImport, zap.String("backend key", ei.backCtx.key),
		zap.String("Engine key", ei.key),
		zap.String("Split Region Size", strconv.FormatInt(int64(config.SplitRegionSize), 10)))
	err := closeEngine.Import(ei.backCtx.ctx, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
	if err != nil {
		errMsg := LitErrIngestDataErr + keyMsg
		logutil.BgLogger().Error(errMsg)
		return errors.New(errMsg)
	}

	// Clean up the engine local workspace.
	err = closeEngine.Cleanup(ei.backCtx.ctx)
	if err != nil {
		errMsg := LitErrCloseEngineErr + keyMsg
		logutil.BgLogger().Error(errMsg)
		return errors.New(errMsg)
	}
	return nil
}

// WorkerContext used keep one lightning local writer for one backfill worker.
type WorkerContext struct {
	ctx    context.Context
	lWrite *backend.LocalEngineWriter
}

func (ei *engineInfo) NewWorkerCtx(id int) (*WorkerContext, error) {
	memRequire := StructSizeWorkerCtx
	// First to check the memory usage.
	ei.memRoot.RefreshConsumption()
	err := ei.memRoot.TryConsume(memRequire)
	if err != nil {
		logutil.BgLogger().Error(LitErrAllocMemFail, zap.String("Engine key", ei.key),
			zap.String("worker Id:", strconv.Itoa(id)),
			zap.String("Memory allocate:", strconv.FormatInt(memRequire, 10)),
			zap.String("Current Memory Usage:", strconv.FormatInt(ei.memRoot.CurrentUsage(), 10)),
			zap.String("Memory limitation:", strconv.FormatInt(ei.memRoot.MaxMemoryQuota(), 10)))
		return nil, err
	}

	wCtx, err := ei.newWorkerContext(id)
	if err != nil {
		logutil.BgLogger().Error(LitErrCreateContextFail, zap.String("Engine key", ei.key),
			zap.String("worker Id:", strconv.Itoa(id)),
			zap.String("Memory allocate:", strconv.FormatInt(memRequire, 10)),
			zap.String("Current Memory Usage:", strconv.FormatInt(ei.memRoot.CurrentUsage(), 10)),
			zap.String("Memory limitation:", strconv.FormatInt(ei.memRoot.MaxMemoryQuota(), 10)))
		return nil, err
	}

	ei.memRoot.Consume(memRequire)
	logutil.BgLogger().Info(LitInfoCreateWrite, zap.String("Engine key", ei.key),
		zap.String("worker Id:", strconv.Itoa(id)),
		zap.String("Memory allocate:", strconv.FormatInt(memRequire, 10)),
		zap.String("Current Memory Usage:", strconv.FormatInt(ei.memRoot.CurrentUsage(), 10)),
		zap.String("Memory limitation:", strconv.FormatInt(ei.memRoot.MaxMemoryQuota(), 10)))
	return wCtx, err
}

// InitWorkerContext will get worker local writer from engine info writer cache first, if exists.
// If local writer not exist, then create new one and store it into engine info writer cache.
// note: operate ei.writeCache map is not thread safe please make sure there is sync mechanism to
// make sure the safe.
func (ei *engineInfo) newWorkerContext(workerID int) (*WorkerContext, error) {
	wCtxKey := ei.key + strconv.Itoa(workerID)
	// First get local writer from engine cache.
	lWrite, exist := ei.writerCache.Load(wCtxKey)
	// If not exist then build one
	if !exist {
		var err error
		lWrite, err = ei.openedEngine.LocalWriter(ei.backCtx.ctx, &backend.LocalWriterConfig{})
		if err != nil {
			return nil, err
		}
		// Cache the lwriter, here we do not lock, because this is done under mem root alloc
		// process it own the lock already while alloc object.
		ei.writerCache.Store(wCtxKey, lWrite)
	}
	return &WorkerContext{
		ctx:    ei.backCtx.ctx,
		lWrite: lWrite,
	}, nil
}

// WriteRow Write one row into local writer buffer.
func (wCtx *WorkerContext) WriteRow(key, idxVal []byte) error {
	var kvs []common.KvPair = make([]common.KvPair, 1)
	kvs[0].Key = key
	kvs[0].Val = idxVal
	row := kv.MakeRowsFromKvPairs(kvs)
	return wCtx.lWrite.WriteRows(wCtx.ctx, nil, row)
}

// GenEngineInfoKey generate one engine key with jobID and indexID.
func GenEngineInfoKey(jobID int64, indexID int64) string {
	return strconv.FormatInt(jobID, 10) + strconv.FormatInt(indexID, 10)
}

// CanRestoreReorgTask only when backend and Engine still be cached, then the task could be restored,
// otherwise return false to let reorg task restart.
func CanRestoreReorgTask(jobID int64, indexID int64) bool {
	bc, bcExist := BackCtxMgr.Load(jobID)
	if !bcExist {
		logutil.BgLogger().Warn(LitWarnBackendNOTExist, zap.Int64("backend key:", jobID))
		return false
	}
	engineInfoKey := GenEngineInfoKey(jobID, indexID)
	_, enExist := bc.EngMgr.Load(engineInfoKey)
	if enExist && bcExist {
		return true
	}
	return false
}
