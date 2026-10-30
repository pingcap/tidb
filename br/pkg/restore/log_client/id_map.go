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
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"go.uber.org/zap"
)

// Split the pitr_id_map data into 512 KiB chunks to avoid one kv entry size too large.
const PITRIdMapBlockSize int = 524288

// PitrIDMapsFilename is the legacy ownerless filename. Such a file cannot be
// safely selected for a new task or used as another task's checkpoint map.
func PitrIDMapsFilename(clusterID, restoredTS uint64) string {
	return fmt.Sprintf("pitr_id_maps/pitr_id_map.cluster_id:%d.restored_ts:%d", clusterID, restoredTS)
}

// PitrIDMapsFilenameForTask isolates storage maps by both upstream identity and
// task owner. A route fingerprint cannot distinguish tasks with equal rules.
func PitrIDMapsFilenameForTask(clusterID, upstreamClusterID, restoredTS, restoreID uint64) string {
	return fmt.Sprintf("%srestore-%d.meta", pitrIDMapsPrefix(clusterID, upstreamClusterID, restoredTS), restoreID)
}

func pitrIDMapsPrefix(clusterID, upstreamClusterID, restoredTS uint64) string {
	return fmt.Sprintf("pitr_id_maps/v2/cluster-%d/upstream-%d/restored-%d/", clusterID, upstreamClusterID, restoredTS)
}

// FindUniquePitrIDMapFile selects a v2 storage map for one upstream/restore
// boundary. It returns an empty path when no map exists and rejects multiple
// owners; callers decide independently whether legacy files are acceptable.
func FindUniquePitrIDMapFile(
	ctx context.Context,
	storage storeapi.Storage,
	clusterID, upstreamClusterID, restoredTS uint64,
) (string, error) {
	prefix := pitrIDMapsPrefix(clusterID, upstreamClusterID, restoredTS)
	var metaFileName string
	err := storage.WalkDir(ctx, &storeapi.WalkOption{SubDir: prefix}, func(path string, _ int64) error {
		var owner uint64
		if n, err := fmt.Sscanf(path, prefix+"restore-%d.meta", &owner); err != nil || n != 1 ||
			path != PitrIDMapsFilenameForTask(clusterID, upstreamClusterID, restoredTS, owner) {
			return errors.Errorf("invalid PiTR ID map owner filename %q", path)
		}
		if metaFileName != "" {
			return errors.Errorf("multiple PiTR ID maps exist at restored TS %d for upstream cluster %d; cannot choose a unique task owner", restoredTS, upstreamClusterID)
		}
		metaFileName = path
		return nil
	})
	if err != nil {
		return "", errors.Trace(err)
	}
	return metaFileName, nil
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
	routeFingerprint string,
) error {
	dbmaps := manager.ToProto()
	if checkpointStorage := rc.tryGetCheckpointStorage(logCheckpointMetaManager); checkpointStorage != nil {
		log.Info("checkpoint storage is specified, load pitr id map from the checkpoint storage.")
		if err := rc.saveIDMap2Storage(ctx, checkpointStorage, dbmaps, routeFingerprint); err != nil {
			return errors.Trace(err)
		}
	} else if rc.pitrIDMapTableExists() {
		if err := rc.saveIDMap2Table(ctx, dbmaps, routeFingerprint); err != nil {
			return errors.Trace(err)
		}
	} else {
		log.Info("the table mysql.tidb_pitr_id_map does not exist, maybe the cluster version is old.")
		if err := rc.saveIDMap2Storage(ctx, rc.storage, dbmaps, routeFingerprint); err != nil {
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
	storage storeapi.Storage,
	dbMaps []*backuppb.PitrDBMap,
	routeFingerprint string,
) error {
	clusterID := rc.GetClusterID(ctx)
	metaFileName := PitrIDMapsFilenameForTask(clusterID, rc.upstreamClusterID, rc.restoreTS, rc.restoreID)
	metaWriter := metautil.NewMetaWriter(storage, metautil.MetaFileSize, false, metaFileName, nil)
	metaWriter.Update(func(m *backuppb.BackupMeta) {
		m.ClusterId = clusterID
		m.DbMaps = dbMaps
		m.PitrIdMapRouteFingerprint = routeFingerprint
	})
	return metaWriter.FlushBackupMeta(ctx)
}

func (rc *LogClient) saveIDMap2Table(
	ctx context.Context,
	dbMaps []*backuppb.PitrDBMap,
	routeFingerprint string,
) error {
	backupmeta := &backuppb.BackupMeta{
		BackupSchemaVersion:       backuppb.BackupSchemaVersion,
		DbMaps:                    dbMaps,
		PitrIdMapRouteFingerprint: routeFingerprint,
	}
	data, err := proto.Marshal(backupmeta)
	if err != nil {
		return errors.Trace(err)
	}

	hasRestoreIDColumn := rc.pitrIDMapHasRestoreIDColumn()

	if hasRestoreIDColumn {
		// new version with restore_id column
		// clean the dirty id map at first
		err = rc.unsafeSession.ExecuteInternal(ctx, "DELETE FROM mysql.tidb_pitr_id_map WHERE restored_ts = %? and upstream_cluster_id = %? and restore_id = %?;",
			rc.restoreTS, rc.upstreamClusterID, rc.restoreID)
		if err != nil {
			return errors.Trace(err)
		}
		replacePitrIDMapSQL := "REPLACE INTO mysql.tidb_pitr_id_map (restore_id, restored_ts, upstream_cluster_id, segment_id, id_map) VALUES (%?, %?, %?, %?, %?);"
		for startIdx, segmentId := 0, 0; startIdx < len(data); segmentId += 1 {
			endIdx := min(startIdx+PITRIdMapBlockSize, len(data))
			err := rc.unsafeSession.ExecuteInternal(ctx, replacePitrIDMapSQL, rc.restoreID, rc.restoreTS, rc.upstreamClusterID, segmentId, data[startIdx:endIdx])
			if err != nil {
				return errors.Trace(err)
			}
			startIdx = endIdx
		}
	} else {
		// old version without restore_id column - use default value 0 for restore_id
		log.Info("mysql.tidb_pitr_id_map table does not have restore_id column, using backward compatible mode")
		// clean the dirty id map at first (without restore_id filter)
		err = rc.unsafeSession.ExecuteInternal(ctx, "DELETE FROM mysql.tidb_pitr_id_map WHERE restored_ts = %? and upstream_cluster_id = %?;",
			rc.restoreTS, rc.upstreamClusterID)
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
	}
	return nil
}

func (rc *LogClient) loadSchemasMap(
	ctx context.Context,
	restoredTS uint64,
	logCheckpointMetaManager checkpoint.LogMetaManagerT,
	loadPreviousTask bool,
) (*backuppb.BackupMeta, error) {
	if checkpointStorage := rc.tryGetCheckpointStorage(logCheckpointMetaManager); checkpointStorage != nil {
		log.Info("checkpoint storage is specified, load pitr id map from the checkpoint storage.")
		backupMeta, err := rc.loadSchemasMapFromStorage(ctx, checkpointStorage, restoredTS, loadPreviousTask)
		return backupMeta, errors.Trace(err)
	}
	if rc.pitrIDMapTableExists() {
		backupMeta, err := rc.loadSchemasMapFromTable(ctx, restoredTS, loadPreviousTask)
		return backupMeta, errors.Trace(err)
	}
	log.Info("the table mysql.tidb_pitr_id_map does not exist, maybe the cluster version is old.")
	backupMeta, err := rc.loadSchemasMapFromStorage(ctx, rc.storage, restoredTS, loadPreviousTask)
	return backupMeta, errors.Trace(err)
}

func (rc *LogClient) loadPITRIDMapBackupMeta(metaData []byte) (*backuppb.BackupMeta, error) {
	backupMeta := &backuppb.BackupMeta{}
	if err := backupMeta.Unmarshal(metaData); err != nil {
		return nil, errors.Trace(err)
	}
	if err := metautil.CheckBackupMetaCompatibilityFromBytes(metaData, backupMeta); err != nil {
		if rc.checkRequirements {
			return nil, errors.Trace(err)
		}
		log.Warn("skip backupmeta compatibility check error", zap.Error(err))
	}
	return backupMeta, nil
}

func (rc *LogClient) loadSchemasMapFromStorage(
	ctx context.Context,
	storage storeapi.Storage,
	restoredTS uint64,
	loadPreviousTask bool,
) (*backuppb.BackupMeta, error) {
	clusterID := rc.GetClusterID(ctx)
	legacyExists, err := storage.FileExists(ctx, PitrIDMapsFilename(clusterID, restoredTS))
	if err != nil {
		return nil, errors.Annotate(err, "failed to check legacy PiTR ID map")
	}
	metaFileName := PitrIDMapsFilenameForTask(clusterID, rc.upstreamClusterID, restoredTS, rc.restoreID)
	if loadPreviousTask {
		if legacyExists {
			return nil, errors.Errorf("legacy PiTR ID map at restored TS %d has no task owner or upstream identity; cannot safely choose a predecessor", restoredTS)
		}
		metaFileName, err = FindUniquePitrIDMapFile(ctx, storage, clusterID, rc.upstreamClusterID, restoredTS)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if metaFileName == "" {
			return nil, nil
		}
	}
	exist, err := storage.FileExists(ctx, metaFileName)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to check filename:%s ", metaFileName)
	}
	if !exist {
		if legacyExists {
			return nil, errors.Errorf("legacy PiTR ID map at restored TS %d has no task owner; cannot safely resume restore ID %d", restoredTS, rc.restoreID)
		}
		log.Info("pitr id map does not exist", zap.String("file", metaFileName), zap.Uint64("restored ts", restoredTS))
		return nil, nil
	}

	metaData, err := storage.ReadFile(ctx, metaFileName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	backupMeta, err := rc.loadPITRIDMapBackupMeta(metaData)
	if err != nil {
		return nil, err
	}
	return backupMeta, nil
}

func (rc *LogClient) loadSchemasMapFromTable(
	ctx context.Context,
	restoredTS uint64,
	loadPreviousTask bool,
) (*backuppb.BackupMeta, error) {
	hasRestoreIDColumn := rc.pitrIDMapHasRestoreIDColumn()

	var getPitrIDMapSQL string
	var args []any

	execCtx := rc.unsafeSession.GetSessionCtx().GetRestrictedSQLExecutor()
	restoreID := rc.restoreID
	if hasRestoreIDColumn && loadPreviousTask {
		// A new task has a different restore ID from its predecessor. Select
		// one owner at the boundary before reading any segments; never combine
		// independent tasks' maps or choose an arbitrary one.
		owners, _, err := execCtx.ExecRestrictedSQL(kv.WithInternalSourceType(ctx, kv.InternalTxnBR), nil,
			"SELECT DISTINCT restore_id FROM mysql.tidb_pitr_id_map WHERE restored_ts = %? AND upstream_cluster_id = %? LIMIT 2;",
			restoredTS, rc.upstreamClusterID)
		if err != nil {
			return nil, errors.Annotate(err, "failed to find the previous PiTR ID map owner")
		}
		if len(owners) == 0 {
			return nil, nil
		}
		if len(owners) > 1 {
			return nil, errors.Errorf("multiple PiTR ID maps exist at restored TS %d for upstream cluster %d; cannot choose a predecessor task",
				restoredTS, rc.upstreamClusterID)
		}
		restoreID = owners[0].GetUint64(0)
	}

	if hasRestoreIDColumn {
		// new version with restore_id column
		getPitrIDMapSQL = "SELECT segment_id, id_map FROM mysql.tidb_pitr_id_map WHERE restore_id = %? and restored_ts = %? and upstream_cluster_id = %? ORDER BY segment_id;"
		args = []any{restoreID, restoredTS, rc.upstreamClusterID}
	} else {
		// old version without restore_id column
		log.Info("mysql.tidb_pitr_id_map table does not have restore_id column, using backward compatible mode")
		getPitrIDMapSQL = "SELECT segment_id, id_map FROM mysql.tidb_pitr_id_map WHERE restored_ts = %? and upstream_cluster_id = %? ORDER BY segment_id;"
		args = []any{restoredTS, rc.upstreamClusterID}
	}

	rows, _, errSQL := execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		getPitrIDMapSQL,
		args...,
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
	backupMeta, err := rc.loadPITRIDMapBackupMeta(metaData)
	if err != nil {
		return nil, err
	}

	return backupMeta, nil
}
