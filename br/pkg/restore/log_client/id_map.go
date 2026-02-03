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
	"fmt"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"go.uber.org/zap"
)

// Split the pitr_id_map data into 512 KiB chunks to avoid one kv entry size too large.
const PITRIdMapBlockSize int = 524288

func PitrIDMapsFilename(clusterID, restoredTS uint64) string {
	return fmt.Sprintf("pitr_id_maps/pitr_id_map.cluster_id:%d.restored_ts:%d", clusterID, restoredTS)
}

func (rc *LogClient) pitrIDMapTableExists() bool {
	return rc.dom.InfoSchema().TableExists(ast.NewCIStr("mysql"), ast.NewCIStr("tidb_pitr_id_map"))
}

func (rc *LogClient) pitrIDMapHasRestoreIDColumn() bool {
	return restore.HasRestoreIDColumn(rc.GetDomain())
}

func (rc *LogClient) tryGetCheckpointStorage(
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
) storeapi.Storage {
	if !rc.useCheckpoint {
		return nil
	}
	return logCheckpointMetaManager.TryGetStorage()
}

// saveIDMap saves the id mapping information.
func (rc *LogClient) saveIDMap(
	ctx context.Context,
	manager *stream.TableMappingManager,
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
) error {
	payload := &backuppb.PitrIdMapPayload{DbMaps: manager.ToProto()}
	return rc.savePitrIdMapPayload(ctx, rc.restoreTS, payload, logCheckpointMetaManager)
}

func (rc *LogClient) saveIDMap2Storage(
	ctx context.Context,
	storage storeapi.Storage,
	dbMaps []*backuppb.PitrDBMap,
) error {
	clusterID := rc.GetClusterID(ctx)
	metaFileName := PitrIDMapsFilename(clusterID, rc.restoreTS)
	metaWriter := metautil.NewMetaWriter(storage, metautil.MetaFileSize, false, metaFileName, nil)
	metaWriter.Update(func(m *backuppb.BackupMeta) {
		m.ClusterId = clusterID
		m.DbMaps = dbMaps
	})
	return metaWriter.FlushBackupMeta(ctx)
}

func (rc *LogClient) saveIDMap2Table(ctx context.Context, dbMaps []*backuppb.PitrDBMap) error {
	payload := &backuppb.PitrIdMapPayload{DbMaps: dbMaps}
	if existing, found, err := rc.loadPitrIdMapPayloadFromTable(ctx, rc.restoreTS, rc.restoreID); err != nil {
		return errors.Trace(err)
	} else if found {
		payload.IngestItems = existing.IngestItems
		payload.TiflashItems = existing.TiflashItems
	}
	return errors.Trace(rc.savePitrIdMapPayloadToTable(ctx, rc.restoreTS, payload))
}

func (rc *LogClient) savePitrIdMapPayload(
	ctx context.Context,
	restoredTS uint64,
	payload *backuppb.PitrIdMapPayload,
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
) error {
	if payload == nil {
		return errors.New("pitr id map payload is nil")
	}
	tableExists := rc.pitrIDMapTableExists()
	if tableExists {
		if err := rc.savePitrIdMapPayloadToTable(ctx, restoredTS, payload); err != nil {
			return errors.Trace(err)
		}
	}
	if checkpointStorage := rc.tryGetCheckpointStorage(logCheckpointMetaManager); checkpointStorage != nil {
		log.Info("checkpoint storage is specified, save pitr id map to the checkpoint storage.")
		if err := rc.saveIDMap2Storage(ctx, checkpointStorage, payload.GetDbMaps()); err != nil {
			return errors.Trace(err)
		}
	} else if !tableExists {
		log.Info("the table mysql.tidb_pitr_id_map does not exist, maybe the cluster version is old.")
		if err := rc.saveIDMap2Storage(ctx, rc.storage, payload.GetDbMaps()); err != nil {
			return errors.Trace(err)
		}
	}

	if rc.useCheckpoint {
		log.Info("save checkpoint task info with InLogRestoreAndIdMapPersist status")
		if err := logCheckpointMetaManager.SaveCheckpointProgress(ctx, &checkpoint.CheckpointProgress{
			Progress: checkpoint.InLogRestoreAndIdMapPersisted,
		}); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (rc *LogClient) loadSchemasMap(
	ctx context.Context,
	restoredTS uint64,
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
) ([]*backuppb.PitrDBMap, error) {
	if checkpointStorage := rc.tryGetCheckpointStorage(logCheckpointMetaManager); checkpointStorage != nil {
		log.Info("checkpoint storage is specified, load pitr id map from the checkpoint storage.")
		dbMaps, err := rc.loadSchemasMapFromStorage(ctx, checkpointStorage, restoredTS)
		return dbMaps, errors.Trace(err)
	}
	if rc.pitrIDMapTableExists() {
		dbMaps, err := rc.loadSchemasMapFromTable(ctx, restoredTS)
		return dbMaps, errors.Trace(err)
	}
	log.Info("the table mysql.tidb_pitr_id_map does not exist, maybe the cluster version is old.")
	dbMaps, err := rc.loadSchemasMapFromStorage(ctx, rc.storage, restoredTS)
	return dbMaps, errors.Trace(err)
}

func (rc *LogClient) loadSchemasMapFromLastTask(ctx context.Context, lastRestoredTS uint64) ([]*backuppb.PitrDBMap, error) {
	if !rc.pitrIDMapTableExists() {
		return nil, errors.Annotatef(berrors.ErrPiTRIDMapTableNotFound, "segmented restore is impossible")
	}
	return rc.loadSchemasMapFromTable(ctx, lastRestoredTS)
}

func (rc *LogClient) loadSchemasMapFromStorage(
	ctx context.Context,
	storage storeapi.Storage,
	restoredTS uint64,
) ([]*backuppb.PitrDBMap, error) {
	clusterID := rc.GetClusterID(ctx)
	metaFileName := PitrIDMapsFilename(clusterID, restoredTS)
	exist, err := storage.FileExists(ctx, metaFileName)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to check filename:%s ", metaFileName)
	}
	if !exist {
		log.Info("pitr id map does not exist", zap.String("file", metaFileName), zap.Uint64("restored ts", restoredTS))
		return nil, nil
	}

	metaData, err := storage.ReadFile(ctx, metaFileName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	backupMeta := &backuppb.BackupMeta{}
	if err := backupMeta.Unmarshal(metaData); err != nil {
		return nil, errors.Trace(err)
	}
	return backupMeta.GetDbMaps(), nil
}

func (rc *LogClient) loadSchemasMapFromTable(
	ctx context.Context,
	restoredTS uint64,
) ([]*backuppb.PitrDBMap, error) {
	payload, found, err := rc.loadPitrIdMapPayloadForSegment(ctx, restoredTS)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !found {
		log.Info("pitr id map does not exist", zap.Uint64("restored ts", restoredTS))
		return nil, nil
	}
	return payload.GetDbMaps(), nil
}
