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
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// maxWriterCount is the max number of writers that can be created for a single engine.
const maxWriterCount = 16

// Register create a new engineInfo and register it to the backend context.
func (bc *litBackendCtx) Register(jobID, indexID int64, schemaName, tableName string) (Engine, error) {
	// Calculate lightning concurrency degree and set memory usage
	// and pre-allocate memory usage for worker.
	bc.MemRoot.RefreshConsumption()
	ok := bc.MemRoot.CheckConsume(int64(bc.cfg.TikvImporter.LocalWriterMemCacheSize))
	if !ok {
		return nil, genEngineAllocMemFailedErr(bc.ctx, bc.MemRoot, bc.jobID, indexID)
	}

	var info string
	en, exist := bc.Load(indexID)
	if !exist || en.openedEngine == nil {
		if exist && en.closedEngine != nil {
			// Import failed before, try to import again.
			err := en.ImportAndClean()
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		engineCacheSize := int64(bc.cfg.TikvImporter.EngineMemCacheSize)
		ok := bc.MemRoot.CheckConsume(StructSizeEngineInfo + engineCacheSize)
		if !ok {
			return nil, genEngineAllocMemFailedErr(bc.ctx, bc.MemRoot, bc.jobID, indexID)
		}

		mgr := backend.MakeEngineManager(bc.backend)
		ts := uint64(0)
		if c := bc.checkpointMgr; c != nil {
			ts = c.GetTS()
		}
		cfg := generateLocalEngineConfig(jobID, schemaName, tableName, ts)
		openedEn, err := mgr.OpenEngine(bc.ctx, cfg, tableName, int32(indexID))
		if err != nil {
			logutil.Logger(bc.ctx).Warn(LitErrCreateEngineFail, zap.Int64("job ID", jobID),
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
		if en.writerCount+1 > maxWriterCount {
			logutil.Logger(bc.ctx).Warn(LitErrExceedConcurrency, zap.Int64("job ID", jobID),
				zap.Int64("index ID", indexID),
				zap.Int("concurrency", bc.cfg.TikvImporter.RangeConcurrency))
			return nil, dbterror.ErrIngestFailed.FastGenByArgs("concurrency quota exceeded")
		}
		en.writerCount++
		info = LitInfoAddWriter
	}
	bc.MemRoot.ConsumeWithTag(encodeEngineTag(jobID, indexID), int64(bc.cfg.TikvImporter.LocalWriterMemCacheSize))
	logutil.Logger(bc.ctx).Info(info, zap.Int64("job ID", jobID),
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
	bc.MemRoot.ReleaseWithTag(encodeEngineTag(jobID, indexID))
	bc.MemRoot.Release(StructSizeWriterCtx * int64(ei.writerCount))
	bc.MemRoot.Release(StructSizeEngineInfo)
}

// ResetWorkers reset the writer count of the engineInfo because
// the goroutines of backfill workers have been terminated.
func (bc *litBackendCtx) ResetWorkers(jobID int64) {
	for _, indexID := range bc.Keys() {
		ei, exist := bc.Load(indexID)
		if !exist {
			continue
		}
		bc.MemRoot.Release(StructSizeWriterCtx * int64(ei.writerCount))
		bc.MemRoot.ReleaseWithTag(encodeEngineTag(jobID, indexID))
		engineCacheSize := int64(bc.cfg.TikvImporter.EngineMemCacheSize)
		bc.MemRoot.ConsumeWithTag(encodeEngineTag(jobID, indexID), engineCacheSize)
		ei.writerCount = 0
	}
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
