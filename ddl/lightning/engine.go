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
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var (
	compactMem        int64 = 1 * _gb
	compactConcurr    int = 4
)

// One engine for one index reorg task, each task will create new writer under the
// OpenEngine. Note engineInfo is not thread safe.
type engineInfo struct {
	id           int32
	key          string

	backCtx      *BackendContext
	openedEngine *backend.OpenedEngine
	uuid         uuid.UUID
	cfg          *backend.EngineConfig
	tableName    string
	WriterCount  int
	writerCache   map[string]*backend.LocalEngineWriter
}

func NewEngineInfo(
	id int32, key string, cfg *backend.EngineConfig, bCtx *BackendContext,
	en *backend.OpenedEngine, tblName string, uuid uuid.UUID, wCnt int) *engineInfo {
	ei := engineInfo{
		id:           id,
		key:          key,
		cfg:          cfg,
		backCtx:      bCtx,
		openedEngine: en,
		uuid:         uuid,
		tableName:    tblName,
		WriterCount:  wCnt,
		writerCache:   make(map[string]*backend.LocalEngineWriter, wCnt),
	}
	return &ei
}

func GenEngineInfoKey(jobid int64, indexId int64) string {
	return strconv.FormatInt(jobid, 10) + strconv.FormatInt(indexId, 10)
}

func CreateEngine(
	ctx context.Context,
	job *model.Job,
	backendKey string,
	engineKey string,
	indexId int32,
	wCnt int) (err error) {
	var cfg backend.EngineConfig
	cfg.Local = &backend.LocalEngineConfig{
		Compact:            true,
		CompactThreshold:   compactMem,
		CompactConcurrency: compactConcurr,
	}
	// Open lightning engine
	bc := GlobalLightningEnv.LitMemRoot.backendCache[backendKey]
	be := bc.Backend

	en, err := be.OpenEngine(ctx, &cfg, job.TableName, indexId)
	if err != nil {
		errMsg := LERR_CREATE_ENGINE_FAILED + err.Error()
		log.L().Error(errMsg)
		return errors.New(errMsg)
	}
	uuid := en.GetEngineUuid()
	ei := NewEngineInfo(indexId, engineKey, &cfg, bc, en, job.TableName, uuid, wCnt)
	GlobalLightningEnv.LitMemRoot.EngineMgr.StoreEngineInfo(engineKey, ei)
	bc.EngineCache[engineKey] = ei
	return nil
}

func FinishIndexOp(ctx context.Context, engineInfoKey string, tbl table.Table, unique bool) (err error) {
	var errMsg string
	var keyMsg string
	ei, exist := GlobalLightningEnv.LitMemRoot.EngineMgr.LoadEngineInfo(engineInfoKey)
	if !exist {
		return errors.New(LERR_GET_ENGINE_FAILED)
	}
	defer func() {
		GlobalLightningEnv.LitMemRoot.EngineMgr.ReleaseEngine(engineInfoKey)
	}()

	keyMsg = "backend key:" + ei.backCtx.Key + "Engine key:" + ei.key
	// Close engine
	log.L().Info(LINFO_CLOSE_ENGINE, zap.String("backend key", ei.backCtx.Key), zap.String("Engine key", ei.key))
	indexEngine := ei.openedEngine
	closeEngine, err1 := indexEngine.Close(ei.backCtx.Ctx, ei.cfg)
	if err1 != nil {
		errMsg = LERR_CLOSE_ENGINE_ERR + keyMsg
		log.L().Error(errMsg)
		return errors.New(errMsg)
	}

	// Ingest data to TiKV
	log.L().Info(LINFO_START_TO_IMPORT, zap.String("backend key", ei.backCtx.Key),
		zap.String("Engine key", ei.key),
		zap.String("Split Region Size", strconv.FormatInt(int64(config.SplitRegionSize), 10)))
	err = closeEngine.Import(ctx, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
	if err != nil {
		errMsg = LERR_INGEST_DATA_ERR + keyMsg
		log.L().Error(errMsg)
		return errors.New(errMsg)
	}

	// Clean up the engine
	err = closeEngine.Cleanup(ctx)
	if err != nil {
		errMsg = LERR_CLOSE_ENGINE_ERR + keyMsg
		log.L().Error(errMsg)
		return errors.New(errMsg)
	}

	// Check Remote duplicate value for index
	if unique {
		hasDupe, err := ei.backCtx.Backend.CollectRemoteDuplicateRows(ctx, tbl, ei.tableName, &kv.SessionOptions{

			SQLMode: mysql.ModeStrictAllTables,
			SysVars: ei.backCtx.sysVars,
		})
		if hasDupe {
			errMsg = LERR_REMOTE_DUP_EXIST_ERR + keyMsg
			log.L().Error(errMsg)
			return errors.New(errMsg)
		} else if err != nil {
			errMsg = LERR_REMOTE_DUP_CHECK_ERR + keyMsg
			log.L().Error(errMsg)
			return errors.New(errMsg)
		}
	}
	return nil
}

func FlushEngine(engineKey string, ei *engineInfo) error {
	err := ei.openedEngine.Flush(ei.backCtx.Ctx)
	if err != nil {
		log.L().Error(LERR_FLUSH_ENGINE_ERR, zap.String("Engine key:", engineKey))
		return err
	}
	return nil
}

// Check if the disk quota arrived, if yes then ingest temp file into TiKV
func UnsafeImportEngineData(jobId int64, indexId int64) error {
	engineKey := GenEngineInfoKey(jobId, indexId)
	ei, exist := GlobalLightningEnv.LitMemRoot.EngineMgr.LoadEngineInfo(engineKey)
	if !exist {
		log.L().Error(LERR_GET_ENGINE_FAILED, zap.String("Engine key:", engineKey))
		return errors.New(LERR_GET_ENGINE_FAILED)
	}

	totalStorageAvail := GlobalLightningEnv.LitMemRoot.TotalDiskAvailable()
	GlobalLightningEnv.checkAndResetQuota()
	if GlobalLightningEnv.NeedImportEngineData(totalStorageAvail) {
		// ToDo it should be handle when do checkpoint solution.
		// Flush wirter cached data into local disk for engine first.
		err := FlushEngine(engineKey, ei)
		if err != nil {
			return err
		}
		
		log.L().Info(LINFO_UNSAFE_IMPORT, zap.String("Engine key:", engineKey), zap.String("Current total available disk:", strconv.FormatUint(totalStorageAvail, 10)))
		err = ei.backCtx.Backend.UnsafeImportAndReset(ei.backCtx.Ctx, ei.uuid, int64(config.SplitRegionSize) * int64(config.MaxSplitRegionSizeRatio), int64(config.SplitRegionKeys))
		if err != nil {
			log.L().Error(LERR_FLUSH_ENGINE_ERR, zap.String("Engine key:", engineKey),
				zap.String("import partial file failed, current disk storage remains", strconv.FormatUint(totalStorageAvail, 10)))
			return err
		}
	}
	return nil
}

type WorkerContext struct {
	eInfo  *engineInfo
	lWrite *backend.LocalEngineWriter
}

// Init Worker Context will get worker local writer from engine info writer cache first, if exist.
// If local wirter not exist, then create new one and store it into engine info writer cache.
// note operate ei.writeCache map is not thread safe please make sure there is sync mechaism to
// make sure the safe.
func (wCtx *WorkerContext) InitWorkerContext(engineKey string, workerid int) (err error) {
	wCtxKey := engineKey + strconv.Itoa(workerid)
	ei, exist := GlobalLightningEnv.LitMemRoot.EngineMgr.enginePool[engineKey]
	if !exist {
		return errors.New(LERR_GET_ENGINE_FAILED)
	}
	wCtx.eInfo = ei

	// Fisrt get local writer from engine cache.
	wCtx.lWrite, exist = ei.writerCache[wCtxKey]
	// If not exist then build one
	if !exist {
		wCtx.lWrite, err = ei.openedEngine.LocalWriter(ei.backCtx.Ctx, &backend.LocalWriterConfig{})
		if err != nil {
			return err
		}
		// Cache the lwriter, here we do not lock, because this is called in mem root alloc
		// process it will lock while alloc object.
		ei.writerCache[wCtxKey] = wCtx.lWrite
	}
	return nil
}

func (wCtx *WorkerContext) WriteRow(key, idxVal []byte) {
	var kvs []common.KvPair = make([]common.KvPair, 1, 1)
	kvs[0].Key = key
	kvs[0].Val = idxVal
	wCtx.lWrite.WriteRow(wCtx.eInfo.backCtx.Ctx, nil, kvs)
}

// Only when backend and Engine still be cached, then the task could be restore,
// otherwise return false to let reorg task restart.
func CanRestoreReorgTask(jobId int64, indexId int64) bool {
	engineInfoKey := GenEngineInfoKey(jobId, indexId)
	bcKey := GenBackendContextKey(jobId)
	_, enExist := GlobalLightningEnv.LitMemRoot.EngineMgr.LoadEngineInfo(engineInfoKey)
	_, bcExist := GlobalLightningEnv.LitMemRoot.getBackendContext(bcKey, true)
	if enExist && bcExist {
		return true
	}
	return false
}
