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
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/generic"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type engineManager struct {
	generic.SyncMap[int64, *engineInfo]
	MemRoot  MemRoot
	DiskRoot DiskRoot
}

func (m *engineManager) init(memRoot MemRoot, diskRoot DiskRoot) {
	m.SyncMap = generic.NewSyncMap[int64, *engineInfo](10)
	m.MemRoot = memRoot
	m.DiskRoot = diskRoot
}

// Register create a new engineInfo and register it to the engineManager.
func (m *engineManager) Register(bc *BackendContext, jobID, indexID int64, schemaName, tableName string) (*engineInfo, error) {
	// Calculate lightning concurrency degree and set memory usage
	// and pre-allocate memory usage for worker.
	m.MemRoot.RefreshConsumption()
	ok := m.MemRoot.CheckConsume(int64(bc.cfg.TikvImporter.LocalWriterMemCacheSize))
	if !ok {
		return nil, genEngineAllocMemFailedErr(m.MemRoot, bc.jobID, indexID)
	}

	var info string
	en, exist := m.Load(indexID)
	if !exist {
		engineCacheSize := int64(bc.cfg.TikvImporter.EngineMemCacheSize)
		ok := m.MemRoot.CheckConsume(StructSizeEngineInfo + engineCacheSize)
		if !ok {
			return nil, genEngineAllocMemFailedErr(m.MemRoot, bc.jobID, indexID)
		}

		cfg := generateLocalEngineConfig(jobID, schemaName, tableName)
		openedEn, err := bc.backend.OpenEngine(bc.ctx, cfg, tableName, int32(indexID))
		if err != nil {
			logutil.BgLogger().Warn(LitErrCreateEngineFail, zap.Int64("job ID", jobID),
				zap.Int64("index ID", indexID), zap.Error(err))
			return nil, errors.Trace(err)
		}
		id := openedEn.GetEngineUUID()
		en = NewEngineInfo(bc.ctx, jobID, indexID, cfg, openedEn, id, 1, m.MemRoot, m.DiskRoot)
		m.Store(indexID, en)
		m.MemRoot.Consume(StructSizeEngineInfo)
		m.MemRoot.ConsumeWithTag(encodeEngineTag(jobID, indexID), engineCacheSize)
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
	m.MemRoot.ConsumeWithTag(encodeEngineTag(jobID, indexID), int64(bc.cfg.TikvImporter.LocalWriterMemCacheSize))
	logutil.BgLogger().Info(info, zap.Int64("job ID", jobID),
		zap.Int64("index ID", indexID),
		zap.Int64("current memory usage", m.MemRoot.CurrentUsage()),
		zap.Int64("memory limitation", m.MemRoot.MaxMemoryQuota()),
		zap.Int("current writer count", en.writerCount))
	return en, nil
}

// Unregister delete the engineInfo from the engineManager.
func (m *engineManager) Unregister(jobID, indexID int64) {
	ei, exist := m.Load(indexID)
	if !exist {
		return
	}

	ei.Clean()
	m.Delete(indexID)
	m.MemRoot.ReleaseWithTag(encodeEngineTag(jobID, indexID))
	m.MemRoot.Release(StructSizeWriterCtx * int64(ei.writerCount))
	m.MemRoot.Release(StructSizeEngineInfo)
}

// ResetWorkers reset the writer count of the engineInfo because
// the goroutines of backfill workers have been terminated.
func (m *engineManager) ResetWorkers(bc *BackendContext, jobID, indexID int64) {
	ei, exist := m.Load(indexID)
	if !exist {
		return
	}
	m.MemRoot.Release(StructSizeWriterCtx * int64(ei.writerCount))
	m.MemRoot.ReleaseWithTag(encodeEngineTag(jobID, indexID))
	engineCacheSize := int64(bc.cfg.TikvImporter.EngineMemCacheSize)
	m.MemRoot.ConsumeWithTag(encodeEngineTag(jobID, indexID), engineCacheSize)
	ei.writerCount = 0
}

// UnregisterAll delete all engineInfo from the engineManager.
func (m *engineManager) UnregisterAll(jobID int64) {
	for _, idxID := range m.Keys() {
		m.Unregister(jobID, idxID)
	}
}

func encodeEngineTag(jobID, indexID int64) string {
	return fmt.Sprintf("%d-%d", jobID, indexID)
}
