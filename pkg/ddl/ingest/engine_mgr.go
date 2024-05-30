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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// Register implements BackendCtx.
func (bc *litBackendCtx) Register(indexIDs []int64, tableName string) ([]Engine, error) {
	ret := make([]Engine, 0, len(indexIDs))

	for _, indexID := range indexIDs {
		en, ok := bc.engines[indexID]
		if !ok {
			continue
		}
		ret = append(ret, en)
	}
	if l := len(ret); l > 0 {
		if l != len(indexIDs) {
			return nil, errors.Errorf(
				"engines index ID number mismatch: job ID %d, required number of index IDs: %d, actual number of engines: %d",
				bc.jobID, len(indexIDs), l,
			)
		}
		return ret, nil
	}

	bc.memRoot.RefreshConsumption()
	numIdx := int64(len(indexIDs))
	engineCacheSize := int64(bc.cfg.TikvImporter.EngineMemCacheSize)
	ok := bc.memRoot.CheckConsume(numIdx * (structSizeEngineInfo + engineCacheSize))
	if !ok {
		return nil, genEngineAllocMemFailedErr(bc.ctx, bc.memRoot, bc.jobID, indexIDs)
	}

	mgr := backend.MakeEngineManager(bc.backend)
	ts := uint64(0)
	if c := bc.checkpointMgr; c != nil {
		ts = c.GetTS()
	}
	cfg := generateLocalEngineConfig(ts)

	openedEngines := make(map[int64]*engineInfo, numIdx)

	for _, indexID := range indexIDs {
		openedEngine, err := mgr.OpenEngine(bc.ctx, cfg, tableName, int32(indexID))
		if err != nil {
			logutil.Logger(bc.ctx).Warn(LitErrCreateEngineFail,
				zap.Int64("job ID", bc.jobID),
				zap.Int64("index ID", indexID),
				zap.Error(err))

			for _, e := range openedEngines {
				e.Clean()
			}
			return nil, errors.Trace(err)
		}

		openedEngines[indexID] = newEngineInfo(
			bc.ctx,
			bc.jobID,
			indexID,
			cfg,
			bc.cfg,
			openedEngine,
			openedEngine.GetEngineUUID(),
			bc.memRoot,
		)
	}

	for _, indexID := range indexIDs {
		ei := openedEngines[indexID]
		ret = append(ret, ei)
		bc.engines[indexID] = ei
	}
	bc.memRoot.Consume(numIdx * (structSizeEngineInfo + engineCacheSize))

	logutil.Logger(bc.ctx).Info(LitInfoOpenEngine, zap.Int64("job ID", bc.jobID),
		zap.Int64s("index IDs", indexIDs),
		zap.Int64("current memory usage", bc.memRoot.CurrentUsage()),
		zap.Int64("memory limitation", bc.memRoot.MaxMemoryQuota()))
	return ret, nil
}

// UnregisterEngines implements BackendCtx.
func (bc *litBackendCtx) UnregisterEngines() {
	numIdx := int64(len(bc.engines))
	for _, ei := range bc.engines {
		ei.Clean()
	}
	bc.engines = make(map[int64]*engineInfo, 10)

	engineCacheSize := int64(bc.cfg.TikvImporter.EngineMemCacheSize)
	bc.memRoot.Release(numIdx * (structSizeEngineInfo + engineCacheSize))
}

// ImportStarted implements BackendCtx.
func (bc *litBackendCtx) ImportStarted() bool {
	if len(bc.engines) == 0 {
		return false
	}
	for _, ei := range bc.engines {
		if ei.openedEngine == nil {
			return true
		}
	}
	return false
}
