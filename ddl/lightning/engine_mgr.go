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
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type engineManager struct {
	resourceManager[engineInfo]
	MemRoot MemRoot
}

func (m *engineManager) init(memRoot MemRoot) {
	m.resourceManager.init(10)
	m.MemRoot = memRoot
}

// Register create a new engineInfo and register it to the engineManager.
func (m *engineManager) Register(bc *BackendContext, job *model.Job, indexID int64) error {
	// Calculate lightning concurrency degree and set memory usage
	// and pre-allocate memory usage for worker.
	engineKey := GenEngineInfoKey(job.ID, indexID)

	m.MemRoot.RefreshConsumption()
	ok := m.MemRoot.TestConsume(int64(bc.cfg.TikvImporter.LocalWriterMemCacheSize))
	if !ok {
		return logAllocMemFailed(bc.key, engineKey, m.MemRoot)
	}

	en, exist1 := m.Load(engineKey)
	if !exist1 {
		engineCacheSize := int64(bc.cfg.TikvImporter.EngineMemCacheSize)
		ok := m.MemRoot.TestConsume(StructSizeEngineInfo + engineCacheSize)
		if !ok {
			return logAllocMemFailed(bc.key, engineKey, m.MemRoot)
		}

		// Create one slice for one backend on one stmt, current we share one engine
		// Open one engine under an existing backend.
		cfg := generateLocalEngineConfig(job.ID, job.SchemaName, job.TableName)
		openedEn, err := bc.backend.OpenEngine(bc.ctx, cfg, job.TableName, int32(indexID))
		if err != nil {
			return errors.New(LitErrCreateEngineFail)
		}
		id := openedEn.GetEngineUUID()
		en = NewEngineInfo(indexID, engineKey, cfg, bc, openedEn, job.TableName, id, 1, m.MemRoot)
		m.Store(engineKey, en)
		if err != nil {
			return errors.New(LitErrCreateEngineFail)
		}
		m.MemRoot.Consume(StructSizeEngineInfo)
		m.MemRoot.ConsumeWithTag(engineKey, engineCacheSize)
	} else {
		if en.writerCount+1 > bc.cfg.TikvImporter.RangeConcurrency {
			logutil.BgLogger().Warn(LitErrExceedConcurrency, zap.String("Backend key", bc.key),
				zap.String("Engine key", engineKey),
				zap.Int("Concurrency", bc.cfg.TikvImporter.RangeConcurrency))
			return errors.New(LitErrExceedConcurrency)
		}
		en.writerCount++
	}
	m.MemRoot.ConsumeWithTag(engineKey, int64(bc.cfg.TikvImporter.LocalWriterMemCacheSize))
	logutil.BgLogger().Info(LitInfoOpenEngine, zap.String("backend key", bc.key),
		zap.String("Engine key", engineKey),
		zap.String("Current Memory Usage:", strconv.FormatInt(m.MemRoot.CurrentUsage(), 10)),
		zap.String("Memory limitation:", strconv.FormatInt(m.MemRoot.MaxMemoryQuota(), 10)),
		zap.String("Current Writer Count", strconv.Itoa(en.writerCount)))
	return nil
}

func logAllocMemFailed(bcKey, engineKey string, memRoot MemRoot) error {
	logutil.BgLogger().Warn(LitErrAllocMemFail, zap.String("Backend key", bcKey),
		zap.String("Engine key", engineKey),
		zap.Int64("Current Memory Usage:", memRoot.CurrentUsage()),
		zap.Int64("Memory limitation:", memRoot.MaxMemoryQuota()))
	return errors.New(LitErrOutMaxMem)
}

// Unregister delete the engineInfo from the engineManager.
func (m *engineManager) Unregister(engineKey string) {
	ei, exists := m.Load(engineKey)
	if !exists {
		return
	}

	ei.Clean()
	m.Drop(engineKey)
	m.MemRoot.ReleaseWithTag(engineKey)
	m.MemRoot.Release(StructSizeWorkerCtx * int64(ei.writerCount))
	m.MemRoot.Release(StructSizeEngineInfo)
}

// UnregisterAll delete all engineInfo from the engineManager.
func (m *engineManager) UnregisterAll() {
	for _, key := range m.Keys() {
		m.Unregister(key)
	}
}
