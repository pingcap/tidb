// Copyright 2026 PingCAP, Inc.
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

package logclient

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/restore/ingestrec"
	"go.uber.org/zap"
)

// LoadIngestRecorderItems loads persisted ingest recorder items for a segment.
func (rc *LogClient) LoadIngestRecorderItems(
	ctx context.Context,
	restoredTS uint64,
	logCheckpointMetaManager checkpoint.SegmentedRestoreStorage,
) (map[int64]map[int64]bool, error) {
	tableExists := rc.pitrIDMapTableExists()
	if tableExists {
		payload, found, err := rc.loadPitrIdMapPayloadForSegment(ctx, restoredTS)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if found {
			items, err := PitrIngestItemsFromPayload(payload)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if items == nil {
				items = map[int64]map[int64]bool{}
			}
			log.Info("loaded pitr ingest items",
				zap.Uint64("restored-ts", restoredTS),
				zap.Int("table-count", len(items)),
				zap.Int("index-count", ingestrec.CountItems(items)))
			return items, nil
		}
	}

	if logCheckpointMetaManager == nil {
		if tableExists {
			return nil, nil
		}
		return nil, errors.New("checkpoint meta manager is not initialized")
	}
	if tableExists {
		log.Info("pitr ingest items not found in mysql.tidb_pitr_id_map, fallback to checkpoint storage",
			zap.Uint64("restored-ts", restoredTS))
	}
	clusterID := rc.GetClusterID(ctx)
	items, found, err := logCheckpointMetaManager.LoadPITRIngestItems(ctx, clusterID, restoredTS)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !found {
		return nil, nil
	}
	if items == nil {
		items = map[int64]map[int64]bool{}
	}
	log.Info("loaded pitr ingest items",
		zap.Uint64("restored-ts", restoredTS),
		zap.Int("table-count", len(items)),
		zap.Int("index-count", ingestrec.CountItems(items)))
	return items, nil
}

// SaveIngestRecorderItems persists ingest recorder items for the next segment.
func (rc *LogClient) SaveIngestRecorderItems(
	ctx context.Context,
	restoredTS uint64,
	items map[int64]map[int64]bool,
	logCheckpointMetaManager checkpoint.SegmentedRestoreStorage,
) error {
	if rc.pitrIDMapTableExists() {
		payload, found, err := rc.loadPitrIdMapPayloadForSegment(ctx, restoredTS)
		if err != nil {
			return errors.Trace(err)
		}
		if !found {
			if logManager, ok := logCheckpointMetaManager.(checkpoint.LogMetaManagerT); ok {
				dbMaps, err := rc.loadSchemasMap(ctx, restoredTS, logManager)
				if err != nil {
					return errors.Trace(err)
				}
				if len(dbMaps) > 0 {
					payload = newPitrIdMapPayload(dbMaps)
				}
			}
		}
		if payload == nil {
			log.Warn("pitr id map payload not found when saving ingest items",
				zap.Uint64("restored-ts", restoredTS))
			return errors.New("pitr id map payload not found for ingest items")
		}
		payload.IngestItems = PitrIngestItemsToProto(items)
		log.Info("saving pitr ingest items",
			zap.Uint64("restored-ts", restoredTS),
			zap.Int("table-count", len(items)),
			zap.Int("index-count", ingestrec.CountItems(items)))
		return errors.Trace(rc.savePitrIdMapPayloadToTable(ctx, restoredTS, payload))
	}

	if logCheckpointMetaManager == nil {
		return errors.New("checkpoint meta manager is not initialized")
	}
	clusterID := rc.GetClusterID(ctx)
	log.Info("saving pitr ingest items to checkpoint storage",
		zap.Uint64("restored-ts", restoredTS),
		zap.Int("table-count", len(items)),
		zap.Int("index-count", ingestrec.CountItems(items)))
	return errors.Trace(logCheckpointMetaManager.SavePITRIngestItems(ctx, clusterID, restoredTS, items))
}
