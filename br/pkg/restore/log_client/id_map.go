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

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/pkg/kv"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"go.uber.org/zap"
)

// Split the pitr_id_map data into 512 KiB chunks to avoid one kv entry size too large.
const PITRIdMapBlockSize int = 524288

func pitrIDMapsFilename(clusterID, restoredTS uint64) string {
	return fmt.Sprintf("pitr_id_maps/pitr_id_map.cluster_id:%d.restored_ts:%d", clusterID, restoredTS)
}

func (rc *LogClient) pitrIDMapTableExists() bool {
	return rc.dom.InfoSchema().TableExists(pmodel.NewCIStr("mysql"), pmodel.NewCIStr("tidb_pitr_id_map"))
}

func (rc *LogClient) tryGetCheckpointStorage(
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
) storage.ExternalStorage {
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
	dbmaps := manager.ToProto()
	if checkpointStorage := rc.tryGetCheckpointStorage(logCheckpointMetaManager); checkpointStorage != nil {
		log.Info("checkpoint storage is specified, load pitr id map from the checkpoint storage.")
		if err := rc.saveIDMap2Storage(ctx, checkpointStorage, dbmaps); err != nil {
			return errors.Trace(err)
		}
	} else if rc.pitrIDMapTableExists() {
		if err := rc.saveIDMap2Table(ctx, dbmaps); err != nil {
			return errors.Trace(err)
		}
	} else {
		log.Info("the table mysql.tidb_pitr_id_map does not exist, maybe the cluster version is old.")
		if err := rc.saveIDMap2Storage(ctx, rc.storage, dbmaps); err != nil {
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

func (rc *LogClient) saveIDMap2Storage(
	ctx context.Context,
	storage storage.ExternalStorage,
	dbMaps []*backuppb.PitrDBMap,
) error {
	clusterID := rc.GetClusterID(ctx)
	metaFileName := pitrIDMapsFilename(clusterID, rc.restoreTS)
	metaWriter := metautil.NewMetaWriter(storage, metautil.MetaFileSize, false, metaFileName, nil)
	metaWriter.Update(func(m *backuppb.BackupMeta) {
		m.ClusterId = clusterID
		m.DbMaps = dbMaps
	})
	return metaWriter.FlushBackupMeta(ctx)
}

func (rc *LogClient) saveIDMap2Table(ctx context.Context, dbMaps []*backuppb.PitrDBMap) error {
	backupmeta := &backuppb.BackupMeta{DbMaps: dbMaps}
	data, err := proto.Marshal(backupmeta)
	if err != nil {
		return errors.Trace(err)
	}
	// clean the dirty id map at first
	err = rc.unsafeSession.ExecuteInternal(ctx, "DELETE FROM mysql.tidb_pitr_id_map WHERE restored_ts = %? and upstream_cluster_id = %?;", rc.restoreTS, rc.upstreamClusterID)
	if err != nil {
		return errors.Trace(err)
	}
	replacePitrIDMapSQL := "REPLACE INTO mysql.tidb_pitr_id_map (restored_ts, upstream_cluster_id, segment_id, id_map) VALUES (%?, %?, %?, %?);"
	for startIdx, segmentId := 0, 0; startIdx < len(data); segmentId += 1 {
		endIdx := min(startIdx+PITRIdMapBlockSize, len(data))
		err := rc.unsafeSession.ExecuteInternal(ctx, replacePitrIDMapSQL, rc.restoreTS, rc.upstreamClusterID, segmentId, data[startIdx:endIdx])
		if err != nil {
			return errors.Trace(err)
		}
		startIdx = endIdx
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

func (rc *LogClient) loadSchemasMapFromStorage(
	ctx context.Context,
	storage storage.ExternalStorage,
	restoredTS uint64,
) ([]*backuppb.PitrDBMap, error) {
	clusterID := rc.GetClusterID(ctx)
	metaFileName := pitrIDMapsFilename(clusterID, restoredTS)
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
	getPitrIDMapSQL := "SELECT segment_id, id_map FROM mysql.tidb_pitr_id_map WHERE restored_ts = %? and upstream_cluster_id = %? ORDER BY segment_id;"
	execCtx := rc.unsafeSession.GetSessionCtx().GetRestrictedSQLExecutor()
	rows, _, errSQL := execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		getPitrIDMapSQL,
		restoredTS,
		rc.upstreamClusterID,
	)
	if errSQL != nil {
		return nil, errors.Annotatef(errSQL, "failed to get pitr id map from mysql.tidb_pitr_id_map")
	}
	if len(rows) == 0 {
		log.Info("pitr id map does not exist", zap.Uint64("restored ts", restoredTS))
		return nil, nil
	}
	metaData := make([]byte, 0, len(rows)*PITRIdMapBlockSize)
	for i, row := range rows {
		elementID := row.GetUint64(0)
		if uint64(i) != elementID {
			return nil, errors.Errorf("the part(segment_id = %d) of pitr id map is lost", i)
		}
		d := row.GetBytes(1)
		if len(d) == 0 {
			return nil, errors.Errorf("get the empty part(segment_id = %d) of pitr id map", i)
		}
		metaData = append(metaData, d...)
	}
	backupMeta := &backuppb.BackupMeta{}
	if err := backupMeta.Unmarshal(metaData); err != nil {
		return nil, errors.Trace(err)
	}

	return backupMeta.GetDbMaps(), nil
}
