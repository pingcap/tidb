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
func (m *engineManager) Register(bc *BackendContext, job *model.Job, engineKey string, indexID int64, wCnt int) (int, error) {
	var err error
	// Calculate lightning concurrency degree and set memory usage.
	// and pre-allocate memory usage for worker
	newWorkerCount := m.MemRoot.WorkerDegree(wCnt, engineKey, job.ID)
	en, exist1 := m.Load(engineKey)
	if !exist1 {
		// When return workerCount is 0, means there is no memory available for lightning worker.
		if newWorkerCount == int(allocFailed) {
			logutil.BgLogger().Warn(LitErrAllocMemFail, zap.String("Backend key", bc.key),
				zap.String("Engine key", engineKey),
				zap.String("Expected worker count:", strconv.Itoa(wCnt)),
				zap.String("Current alloc worker count:", strconv.Itoa(newWorkerCount)))
			return 0, errors.New(LitErrCleanEngineErr)
		}
		// Firstly, update and check the current memory usage.
		m.MemRoot.RefreshConsumption()
		err = m.MemRoot.TryConsume(StructSizeEngineInfo)
		if err != nil {
			logutil.BgLogger().Warn(LitErrAllocMemFail, zap.String("Backend key", bc.key),
				zap.String("Engine key", engineKey),
				zap.String("Current Memory Usage:", strconv.FormatInt(m.MemRoot.CurrentUsage(), 10)),
				zap.String("Memory limitation:", strconv.FormatInt(m.MemRoot.MaxMemoryQuota(), 10)))
			return 0, err
		}
		// Create one slice for one backend on one stmt, current we share one engine
		// Open one engine under an existing backend.
		cfg := generateLocalEngineConfig(job.ID, job.SchemaName, job.TableName)
		en, err := bc.backend.OpenEngine(bc.ctx, cfg, job.TableName, int32(indexID))
		if err != nil {
			return 0, errors.New(LitErrCreateEngineFail)
		}
		id := en.GetEngineUUID()
		ei := NewEngineInfo(indexID, engineKey, cfg, bc, en, job.TableName, id, wCnt, m.MemRoot)
		m.Store(engineKey, ei)
		if err != nil {
			return 0, errors.New(LitErrCreateEngineFail)
		}
		m.MemRoot.Consume(StructSizeEngineInfo)
	} else {
		// If engine exist, then add newWorkerCount.
		en.writerCount += newWorkerCount
	}
	logutil.BgLogger().Info(LitInfoOpenEngine, zap.String("backend key", bc.key),
		zap.String("Engine key", engineKey),
		zap.String("Current Memory Usage:", strconv.FormatInt(m.MemRoot.CurrentUsage(), 10)),
		zap.String("Memory limitation:", strconv.FormatInt(m.MemRoot.MaxMemoryQuota(), 10)),
		zap.String("Expected Worker Count", strconv.Itoa(wCnt)),
		zap.String("Allocated worker count", strconv.Itoa(newWorkerCount)))
	return newWorkerCount, nil
}

// Unregister delete the engineInfo from the engineManager.
func (m *engineManager) Unregister(engineKey string) {
	ei, exists := m.Load(engineKey)
	if !exists {
		return
	}
	m.Drop(engineKey)
	m.MemRoot.ReleaseWithTag(engineKey)
	m.MemRoot.Release(StructSizeWorkerCtx * int64(ei.writerCount))
	m.MemRoot.Release(StructSizeEngineInfo)
}

// UnregisterAll delete all engineInfo from the engineManager.
func (m *engineManager) UnregisterAll() {
	count := len(m.item)
	for k, en := range m.item {
		m.MemRoot.ReleaseWithTag(k)
		delete(m.item, k)
		m.MemRoot.Release(StructSizeWorkerCtx * int64(en.writerCount))
	}
	m.MemRoot.Release(StructSizeEngineInfo * int64(count))
}
