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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// Register create a new engineInfo and register it to the backend context.
func (bc *litBackendCtx) Register(jobID, indexID int64, schemaName, tableName string) (Engine, error) {
	// Calculate lightning concurrency degree and set memory usage
	// and pre-allocate memory usage for worker.
	bc.MemRoot.RefreshConsumption()
	ok := bc.MemRoot.CheckConsume(int64(bc.cfg.TikvImporter.LocalWriterMemCacheSize))
	if !ok {
		return nil, genEngineAllocMemFailedErr(bc.MemRoot, bc.jobID, indexID)
	}

	var info string
	en, exist := bc.Load(indexID)
	if !exist {
		engineCacheSize := int64(bc.cfg.TikvImporter.EngineMemCacheSize)
		ok := bc.MemRoot.CheckConsume(StructSizeEngineInfo + engineCacheSize)
		if !ok {
			return nil, genEngineAllocMemFailedErr(bc.MemRoot, bc.jobID, indexID)
		}

		mgr := backend.MakeEngineManager(bc.backend)
		cfg := generateLocalEngineConfig(jobID, schemaName, tableName)
		openedEn, err := mgr.OpenEngine(bc.ctx, cfg, tableName, int32(indexID))
		if err != nil {
			logutil.BgLogger().Warn(LitErrCreateEngineFail, zap.Int64("job ID", jobID),
				zap.Int64("index ID", indexID), zap.Error(err))
			return nil, errors.Trace(err)
		}
		id := openedEn.GetEngineUUID()
		en = newEngineInfo(bc.ctx, jobID, indexID, cfg, openedEn, id, 1, bc.MemRoot)
		bc.Store(indexID, en)
		bc.MemRoot.Consume(StructSizeEngineInfo)
		bc.MemRoot.ConsumeWithTag(encodeEngineTag(jobID, indexID), engineCacheSize)
		info = LitInfoOpenEngine
	} else {
		if en.writerCount+1 > bc.cfg.TikvImporter.RangeConcurrency {
			logutil.BgLogger().Warn(LitErrExceedConcurrency, zap.Int64("job ID", jobID),
				zap.Int64("index ID", indexID),
				zap.Int("concurrency", bc.cfg.TikvImporter.RangeConcurrency))
			return nil, dbterror.ErrIngestFailed.FastGenByArgs("concurrency quota exceeded")
		}
		en.writerCount++
		info = LitInfoAddWriter
	}
	bc.MemRoot.ConsumeWithTag(encodeEngineTag(jobID, indexID), int64(bc.cfg.TikvImporter.LocalWriterMemCacheSize))
	logutil.BgLogger().Info(info, zap.Int64("job ID", jobID),
		zap.Int64("index ID", indexID),
		zap.Int64("current memory usage", bc.MemRoot.CurrentUsage()),
		zap.Int64("memory limitation", bc.MemRoot.MaxMemoryQuota()),
		zap.Int("current writer count", en.writerCount))
	return en, nil
}

// Unregister delete the engineInfo from the engineManager.
func (bc *litBackendCtx) Unregister(jobID, indexID int64) {
	ei, exist := bc.Load(indexID)
	if !exist {
		return
	}

	ei.Clean()
	bc.Delete(indexID)
	bc.checkpointMgr.Close()
	bc.MemRoot.ReleaseWithTag(encodeEngineTag(jobID, indexID))
	bc.MemRoot.Release(StructSizeWriterCtx * int64(ei.writerCount))
	bc.MemRoot.Release(StructSizeEngineInfo)
}

// ResetWorkers reset the writer count of the engineInfo because
// the goroutines of backfill workers have been terminated.
func (bc *litBackendCtx) ResetWorkers(jobID, indexID int64) {
	ei, exist := bc.Load(indexID)
	if !exist {
		return
	}
	bc.MemRoot.Release(StructSizeWriterCtx * int64(ei.writerCount))
	bc.MemRoot.ReleaseWithTag(encodeEngineTag(jobID, indexID))
	engineCacheSize := int64(bc.cfg.TikvImporter.EngineMemCacheSize)
	bc.MemRoot.ConsumeWithTag(encodeEngineTag(jobID, indexID), engineCacheSize)
	ei.writerCount = 0
}

// unregisterAll delete all engineInfo from the engineManager.
func (bc *litBackendCtx) unregisterAll(jobID int64) {
	for _, idxID := range bc.Keys() {
		bc.Unregister(jobID, idxID)
	}
}

func encodeEngineTag(jobID, indexID int64) string {
	return fmt.Sprintf("%d-%d", jobID, indexID)
}
