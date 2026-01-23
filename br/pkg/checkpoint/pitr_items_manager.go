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

package checkpoint

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
)

func (manager *TableMetaManager[K, SV, LV, M]) LoadPITRIngestItems(
	ctx context.Context,
	clusterID uint64,
	restoredTS uint64,
) (map[int64]map[int64]bool, bool, error) {
	if !pitrItemsTableExists(manager.dom, pitrIngestItemsTableName) {
		return nil, false, nil
	}
	if manager.se == nil {
		return nil, false, errors.New("checkpoint session is not initialized")
	}
	execCtx := manager.se.GetSessionCtx().GetRestrictedSQLExecutor()
	data, found, err := loadPITRItemsFromTable(ctx, execCtx, LogRestorePITRItemsDatabaseName, pitrIngestItemsTableName, clusterID, restoredTS)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	if !found {
		return nil, false, nil
	}
	items, err := unmarshalPITRIngestItems(data)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	return items, true, nil
}

func (manager *TableMetaManager[K, SV, LV, M]) SavePITRIngestItems(
	ctx context.Context,
	clusterID uint64,
	restoredTS uint64,
	items map[int64]map[int64]bool,
) error {
	if manager.se == nil {
		return errors.New("checkpoint session is not initialized")
	}
	data, err := marshalPITRIngestItems(items)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(savePITRItemsToTable(ctx, manager.se, LogRestorePITRItemsDatabaseName, pitrIngestItemsTableName, clusterID, restoredTS, data))
}

func (manager *TableMetaManager[K, SV, LV, M]) LoadPITRTiFlashItems(
	ctx context.Context,
	clusterID uint64,
	restoredTS uint64,
) (map[int64]model.TiFlashReplicaInfo, bool, error) {
	if !pitrItemsTableExists(manager.dom, pitrTiFlashItemsTableName) {
		return nil, false, nil
	}
	if manager.se == nil {
		return nil, false, errors.New("checkpoint session is not initialized")
	}
	execCtx := manager.se.GetSessionCtx().GetRestrictedSQLExecutor()
	data, found, err := loadPITRItemsFromTable(ctx, execCtx, LogRestorePITRItemsDatabaseName, pitrTiFlashItemsTableName, clusterID, restoredTS)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	if !found {
		return nil, false, nil
	}
	items, err := unmarshalPITRTiFlashItems(data)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	return items, true, nil
}

func (manager *TableMetaManager[K, SV, LV, M]) SavePITRTiFlashItems(
	ctx context.Context,
	clusterID uint64,
	restoredTS uint64,
	items map[int64]model.TiFlashReplicaInfo,
) error {
	if manager.se == nil {
		return errors.New("checkpoint session is not initialized")
	}
	data, err := marshalPITRTiFlashItems(items)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(savePITRItemsToTable(ctx, manager.se, LogRestorePITRItemsDatabaseName, pitrTiFlashItemsTableName, clusterID, restoredTS, data))
}

func (manager *StorageMetaManager[K, SV, LV, M]) LoadPITRIngestItems(
	ctx context.Context,
	clusterID uint64,
	restoredTS uint64,
) (map[int64]map[int64]bool, bool, error) {
	path := pitrIngestItemsPath(clusterID, restoredTS)
	data, found, err := loadPITRItemsFromStorage(ctx, manager.storage, path, "ingest items")
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	if !found {
		return nil, false, nil
	}
	items, err := unmarshalPITRIngestItems(data)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	return items, true, nil
}

func (manager *StorageMetaManager[K, SV, LV, M]) SavePITRIngestItems(
	ctx context.Context,
	clusterID uint64,
	restoredTS uint64,
	items map[int64]map[int64]bool,
) error {
	data, err := marshalPITRIngestItems(items)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(savePITRItemsToStorage(ctx, manager.storage, pitrIngestItemsPath(clusterID, restoredTS), "ingest items", data))
}

func (manager *StorageMetaManager[K, SV, LV, M]) LoadPITRTiFlashItems(
	ctx context.Context,
	clusterID uint64,
	restoredTS uint64,
) (map[int64]model.TiFlashReplicaInfo, bool, error) {
	path := pitrTiFlashItemsPath(clusterID, restoredTS)
	data, found, err := loadPITRItemsFromStorage(ctx, manager.storage, path, "tiflash items")
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	if !found {
		return nil, false, nil
	}
	items, err := unmarshalPITRTiFlashItems(data)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	return items, true, nil
}

func (manager *StorageMetaManager[K, SV, LV, M]) SavePITRTiFlashItems(
	ctx context.Context,
	clusterID uint64,
	restoredTS uint64,
	items map[int64]model.TiFlashReplicaInfo,
) error {
	data, err := marshalPITRTiFlashItems(items)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(savePITRItemsToStorage(ctx, manager.storage, pitrTiFlashItemsPath(clusterID, restoredTS), "tiflash items", data))
}
