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

// Register create a new engineInfo and register it to the backend context.
func (bc *litBackendCtx) Register(indexIDs []int64, tableName string) ([]Engine, error) {
	for _, indexID := range indexIDs {
		en, ok := bc.engines.Load(indexID)
		if !ok {
			continue
		}
		if en.closedEngine != nil {
			// Import failed before, try to import again.
			err := en.ImportAndClean()
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		return nil, errors.Errorf(
			"engine already exists: job ID %d, index ID: %d",
			bc.jobID, indexID,
		)
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

	ret := make([]Engine, 0, len(indexIDs))
	for _, indexID := range indexIDs {
		ei := openedEngines[indexID]
		ret = append(ret, ei)
		bc.engines.Store(indexID, ei)
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
	indexIDs := bc.engines.Keys()
	numIdx := int64(len(indexIDs))
	for _, indexID := range indexIDs {
		ei, ok := bc.engines.Load(indexID)
		if !ok {
			logutil.Logger(bc.ctx).Error("engine not found",
				zap.Int64("job ID", bc.jobID),
				zap.Int64("index ID", indexID))
			continue
		}
		ei.Clean()
		bc.engines.Delete(indexID)
	}

	engineCacheSize := int64(bc.cfg.TikvImporter.EngineMemCacheSize)
	bc.memRoot.Release(numIdx * (structSizeEngineInfo + engineCacheSize))
}
