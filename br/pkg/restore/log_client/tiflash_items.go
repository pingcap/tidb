// Copyright 2025 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/zap"
)

const pitrTiFlashItemsDir = "pitr_tiflash_items"

type pitrTiFlashItems struct {
	Items map[int64]model.TiFlashReplicaInfo `json:"items"`
}

func PitrTiFlashItemsFilename(clusterID, restoredTS uint64) string {
	return fmt.Sprintf("%s/pitr_tiflash_items.cluster_id:%d.restored_ts:%d", pitrTiFlashItemsDir, clusterID, restoredTS)
}

func (rc *LogClient) loadTiFlashItemsFromStorage(
	ctx context.Context,
	storage storeapi.Storage,
	restoredTS uint64,
) (map[int64]model.TiFlashReplicaInfo, bool, error) {
	clusterID := rc.GetClusterID(ctx)
	fileName := PitrTiFlashItemsFilename(clusterID, restoredTS)
	exists, err := storage.FileExists(ctx, fileName)
	if err != nil {
		return nil, false, errors.Annotatef(err, "failed to check tiflash items file %s", fileName)
	}
	if !exists {
		return nil, false, nil
	}

	raw, err := storage.ReadFile(ctx, fileName)
	if err != nil {
		return nil, false, errors.Annotatef(err, "failed to read tiflash items file %s", fileName)
	}

	var payload pitrTiFlashItems
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, false, errors.Annotatef(err, "failed to unmarshal tiflash items file %s", fileName)
	}
	if payload.Items == nil {
		payload.Items = map[int64]model.TiFlashReplicaInfo{}
	}
	log.Info("loaded pitr tiflash items", zap.String("file", fileName), zap.Int("item-count", len(payload.Items)))
	return payload.Items, true, nil
}

func (rc *LogClient) saveTiFlashItemsToStorage(
	ctx context.Context,
	storage storeapi.Storage,
	restoredTS uint64,
	items map[int64]model.TiFlashReplicaInfo,
) error {
	clusterID := rc.GetClusterID(ctx)
	fileName := PitrTiFlashItemsFilename(clusterID, restoredTS)
	if items == nil {
		items = map[int64]model.TiFlashReplicaInfo{}
	}
	payload := pitrTiFlashItems{Items: items}
	raw, err := json.Marshal(&payload)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("saving pitr tiflash items", zap.String("file", fileName), zap.Int("item-count", len(items)))
	if err := storage.WriteFile(ctx, fileName, raw); err != nil {
		return errors.Annotatef(err, "failed to save tiflash items file %s", fileName)
	}
	return nil
}

func (rc *LogClient) loadTiFlashItems(
	ctx context.Context,
	restoredTS uint64,
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
) (map[int64]model.TiFlashReplicaInfo, error) {
	if checkpointStorage := rc.tryGetCheckpointStorage(logCheckpointMetaManager); checkpointStorage != nil {
		items, found, err := rc.loadTiFlashItemsFromStorage(ctx, checkpointStorage, restoredTS)
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
	items, found, err := rc.loadTiFlashItemsFromStorage(ctx, rc.storage, restoredTS)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !found {
		return nil, nil
	}
	return items, nil
}

func (rc *LogClient) saveTiFlashItems(
	ctx context.Context,
	restoredTS uint64,
	items map[int64]model.TiFlashReplicaInfo,
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
) error {
	storage := rc.tryGetCheckpointStorage(logCheckpointMetaManager)
	if storage == nil {
		storage = rc.storage
	}
	if storage == nil {
		return errors.New("no storage available for persisting tiflash items")
	}
	return errors.Trace(rc.saveTiFlashItemsToStorage(ctx, storage, restoredTS, items))
}

// LoadTiFlashRecorderItems loads persisted TiFlash recorder items for a segment.
func (rc *LogClient) LoadTiFlashRecorderItems(
	ctx context.Context,
	restoredTS uint64,
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
) (map[int64]model.TiFlashReplicaInfo, error) {
	return rc.loadTiFlashItems(ctx, restoredTS, logCheckpointMetaManager)
}

// SaveTiFlashRecorderItems persists TiFlash recorder items for the next segment.
func (rc *LogClient) SaveTiFlashRecorderItems(
	ctx context.Context,
	restoredTS uint64,
	items map[int64]model.TiFlashReplicaInfo,
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
) error {
	return rc.saveTiFlashItems(ctx, restoredTS, items, logCheckpointMetaManager)
}
