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
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/zap"
)

const pitrIngestItemsDir = "pitr_ingest_items"

type pitrIngestItems struct {
	Items map[int64]map[int64]bool `json:"items"`
}

func PitrIngestItemsFilename(clusterID, restoredTS uint64) string {
	return fmt.Sprintf("%s/pitr_ingest_items.cluster_id:%d.restored_ts:%d", pitrIngestItemsDir, clusterID, restoredTS)
}

func countIngestItems(items map[int64]map[int64]bool) int {
	total := 0
	for _, indexMap := range items {
		total += len(indexMap)
	}
	return total
}

func (rc *LogClient) loadIngestItemsFromStorage(
	ctx context.Context,
	storage storeapi.Storage,
	restoredTS uint64,
) (map[int64]map[int64]bool, bool, error) {
	clusterID := rc.GetClusterID(ctx)
	fileName := PitrIngestItemsFilename(clusterID, restoredTS)
	exists, err := storage.FileExists(ctx, fileName)
	if err != nil {
		return nil, false, errors.Annotatef(err, "failed to check ingest items file %s", fileName)
	}
	if !exists {
		return nil, false, nil
	}

	raw, err := storage.ReadFile(ctx, fileName)
	if err != nil {
		return nil, false, errors.Annotatef(err, "failed to read ingest items file %s", fileName)
	}

	var payload pitrIngestItems
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, false, errors.Annotatef(err, "failed to unmarshal ingest items file %s", fileName)
	}
	if payload.Items == nil {
		payload.Items = map[int64]map[int64]bool{}
	}
	log.Info("loaded pitr ingest items",
		zap.String("file", fileName),
		zap.Int("table-count", len(payload.Items)),
		zap.Int("index-count", countIngestItems(payload.Items)))
	return payload.Items, true, nil
}

func (rc *LogClient) saveIngestItemsToStorage(
	ctx context.Context,
	storage storeapi.Storage,
	restoredTS uint64,
	items map[int64]map[int64]bool,
) error {
	clusterID := rc.GetClusterID(ctx)
	fileName := PitrIngestItemsFilename(clusterID, restoredTS)
	if items == nil {
		items = map[int64]map[int64]bool{}
	}
	payload := pitrIngestItems{Items: items}
	raw, err := json.Marshal(&payload)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("saving pitr ingest items",
		zap.String("file", fileName),
		zap.Int("table-count", len(items)),
		zap.Int("index-count", countIngestItems(items)))
	if err := storage.WriteFile(ctx, fileName, raw); err != nil {
		return errors.Annotatef(err, "failed to save ingest items file %s", fileName)
	}
	return nil
}

func (rc *LogClient) loadIngestItems(
	ctx context.Context,
	restoredTS uint64,
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
) (map[int64]map[int64]bool, error) {
	if checkpointStorage := rc.tryGetCheckpointStorage(logCheckpointMetaManager); checkpointStorage != nil {
		items, found, err := rc.loadIngestItemsFromStorage(ctx, checkpointStorage, restoredTS)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if found {
			return items, nil
		}
	}
	if rc.storage == nil {
		return nil, nil
	}
	items, found, err := rc.loadIngestItemsFromStorage(ctx, rc.storage, restoredTS)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !found {
		return nil, nil
	}
	return items, nil
}

func (rc *LogClient) saveIngestItems(
	ctx context.Context,
	restoredTS uint64,
	items map[int64]map[int64]bool,
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
) error {
	storage := rc.tryGetCheckpointStorage(logCheckpointMetaManager)
	if storage == nil {
		storage = rc.storage
	}
	if storage == nil {
		return errors.New("no storage available for persisting ingest items")
	}
	return errors.Trace(rc.saveIngestItemsToStorage(ctx, storage, restoredTS, items))
}

// LoadIngestRecorderItems loads persisted ingest recorder items for a segment.
func (rc *LogClient) LoadIngestRecorderItems(
	ctx context.Context,
	restoredTS uint64,
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
) (map[int64]map[int64]bool, error) {
	return rc.loadIngestItems(ctx, restoredTS, logCheckpointMetaManager)
}

// SaveIngestRecorderItems persists ingest recorder items for the next segment.
func (rc *LogClient) SaveIngestRecorderItems(
	ctx context.Context,
	restoredTS uint64,
	items map[int64]map[int64]bool,
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
) error {
	return rc.saveIngestItems(ctx, restoredTS, items, logCheckpointMetaManager)
}
