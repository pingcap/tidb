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

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
)

const pitrIdMapPayloadVersion int32 = 1

// DecodePitrIdMapPayload parses the payload and returns db maps for callers outside logclient.
func DecodePitrIdMapPayload(metaData []byte) ([]*backuppb.PitrDBMap, error) {
	payload, err := decodePitrIdMapPayload(metaData)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return payload.GetDbMaps(), nil
}

func PitrIngestItemsFromProto(items []*backuppb.PitrIngestItem) map[int64]map[int64]bool {
	if len(items) == 0 {
		return map[int64]map[int64]bool{}
	}
	result := make(map[int64]map[int64]bool, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		indexes := make(map[int64]bool, len(item.Indexes))
		for _, index := range item.Indexes {
			if index == nil {
				continue
			}
			indexes[index.IndexId] = index.IsPrimary
		}
		result[item.TableId] = indexes
	}
	return result
}

func PitrIngestItemsToProto(items map[int64]map[int64]bool) []*backuppb.PitrIngestItem {
	if len(items) == 0 {
		return nil
	}
	result := make([]*backuppb.PitrIngestItem, 0, len(items))
	for tableID, indexMap := range items {
		indexes := make([]*backuppb.PitrIngestIndex, 0, len(indexMap))
		for indexID, isPrimary := range indexMap {
			indexes = append(indexes, &backuppb.PitrIngestIndex{
				IndexId:   indexID,
				IsPrimary: isPrimary,
			})
		}
		result = append(result, &backuppb.PitrIngestItem{
			TableId: tableID,
			Indexes: indexes,
		})
	}
	return result
}

func PitrTiFlashItemsFromProto(items []*backuppb.PitrTiFlashItem) map[int64]model.TiFlashReplicaInfo {
	if len(items) == 0 {
		return map[int64]model.TiFlashReplicaInfo{}
	}
	result := make(map[int64]model.TiFlashReplicaInfo, len(items))
	for _, item := range items {
		if item == nil || item.Replica == nil {
			continue
		}
		replica := item.Replica
		result[item.TableId] = model.TiFlashReplicaInfo{
			Count:                 replica.Count,
			LocationLabels:        append([]string(nil), replica.LocationLabels...),
			Available:             replica.Available,
			AvailablePartitionIDs: append([]int64(nil), replica.AvailablePartitionIds...),
		}
	}
	return result
}

func PitrTiFlashItemsToProto(items map[int64]model.TiFlashReplicaInfo) []*backuppb.PitrTiFlashItem {
	if len(items) == 0 {
		return nil
	}
	result := make([]*backuppb.PitrTiFlashItem, 0, len(items))
	for tableID, replica := range items {
		result = append(result, &backuppb.PitrTiFlashItem{
			TableId: tableID,
			Replica: &backuppb.PitrTiFlashReplicaInfo{
				Count:                 replica.Count,
				LocationLabels:        append([]string(nil), replica.LocationLabels...),
				Available:             replica.Available,
				AvailablePartitionIds: append([]int64(nil), replica.AvailablePartitionIDs...),
			},
		})
	}
	return result
}

func PitrIngestItemsFromPayload(payload *backuppb.PitrIdMapPayload) (map[int64]map[int64]bool, error) {
	if payload == nil {
		return nil, errors.New("pitr id map payload is nil")
	}
	return PitrIngestItemsFromProto(payload.IngestItems), nil
}

func PitrTiFlashItemsFromPayload(payload *backuppb.PitrIdMapPayload) (map[int64]model.TiFlashReplicaInfo, error) {
	if payload == nil {
		return nil, errors.New("pitr id map payload is nil")
	}
	return PitrTiFlashItemsFromProto(payload.TiflashItems), nil
}

func decodePitrIdMapPayload(metaData []byte) (*backuppb.PitrIdMapPayload, error) {
	payload := &backuppb.PitrIdMapPayload{}
	if err := payload.Unmarshal(metaData); err != nil {
		return nil, errors.Trace(err)
	}
	return payload, nil
}

func newPitrIdMapPayload(dbMaps []*backuppb.PitrDBMap) *backuppb.PitrIdMapPayload {
	return &backuppb.PitrIdMapPayload{
		Version: pitrIdMapPayloadVersion,
		DbMaps:  dbMaps,
	}
}

func NewPitrIdMapPayload(dbMaps []*backuppb.PitrDBMap) *backuppb.PitrIdMapPayload {
	return newPitrIdMapPayload(dbMaps)
}

func (rc *LogClient) loadPitrIdMapDataFromTable(
	ctx context.Context,
	restoredTS uint64,
	restoreID uint64,
) ([]byte, bool, error) {
	var getPitrIDMapSQL string
	var args []any

	if rc.pitrIDMapHasRestoreIDColumn() {
		getPitrIDMapSQL = "SELECT segment_id, id_map FROM mysql.tidb_pitr_id_map WHERE restore_id = %? and restored_ts = %? and upstream_cluster_id = %? ORDER BY segment_id;"
		args = []any{restoreID, restoredTS, rc.upstreamClusterID}
	} else {
		getPitrIDMapSQL = "SELECT segment_id, id_map FROM mysql.tidb_pitr_id_map WHERE restored_ts = %? and upstream_cluster_id = %? ORDER BY segment_id;"
		args = []any{restoredTS, rc.upstreamClusterID}
	}

	execCtx := rc.unsafeSession.GetSessionCtx().GetRestrictedSQLExecutor()
	rows, _, errSQL := execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		getPitrIDMapSQL,
		args...,
	)
	if errSQL != nil {
		return nil, false, errors.Annotatef(errSQL, "failed to get pitr id map from mysql.tidb_pitr_id_map")
	}
	if len(rows) == 0 {
		return nil, false, nil
	}
	metaData := make([]byte, 0, len(rows)*PITRIdMapBlockSize)
	for i, row := range rows {
		elementID := row.GetUint64(0)
		if uint64(i) != elementID {
			return nil, false, errors.Errorf("the part(segment_id = %d) of pitr id map is lost", i)
		}
		d := row.GetBytes(1)
		if len(d) == 0 {
			return nil, false, errors.Errorf("get the empty part(segment_id = %d) of pitr id map", i)
		}
		metaData = append(metaData, d...)
	}
	return metaData, true, nil
}

func (rc *LogClient) loadLatestRestoreIDFromTable(
	ctx context.Context,
	restoredTS uint64,
) (uint64, bool, error) {
	if !rc.pitrIDMapHasRestoreIDColumn() {
		return 0, false, errors.New("restore_id column is not available")
	}
	execCtx := rc.unsafeSession.GetSessionCtx().GetRestrictedSQLExecutor()
	rows, _, errSQL := execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		"SELECT restore_id FROM mysql.tidb_pitr_id_map WHERE restored_ts = %? and upstream_cluster_id = %? ORDER BY restore_id DESC LIMIT 1;",
		restoredTS, rc.upstreamClusterID,
	)
	if errSQL != nil {
		return 0, false, errors.Annotatef(errSQL, "failed to get latest restore_id from mysql.tidb_pitr_id_map")
	}
	if len(rows) == 0 {
		return 0, false, nil
	}
	return rows[0].GetUint64(0), true, nil
}

func (rc *LogClient) resolvePitrIdMapRestoreID(
	ctx context.Context,
	restoredTS uint64,
) (uint64, bool, error) {
	if !rc.pitrIDMapHasRestoreIDColumn() {
		return 0, true, nil
	}
	if restoredTS == rc.restoreTS {
		return rc.restoreID, true, nil
	}
	restoreID, found, err := rc.loadLatestRestoreIDFromTable(ctx, restoredTS)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	if !found {
		return 0, false, nil
	}
	return restoreID, true, nil
}

func (rc *LogClient) normalizePitrIdMapRestoreID(restoreID uint64) uint64 {
	if rc.pitrIDMapHasRestoreIDColumn() {
		return restoreID
	}
	return 0
}

func (rc *LogClient) loadPitrIdMapPayloadForSegment(
	ctx context.Context,
	restoredTS uint64,
) (*backuppb.PitrIdMapPayload, bool, error) {
	restoreID, found, err := rc.resolvePitrIdMapRestoreID(ctx, restoredTS)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	if !found {
		return nil, false, nil
	}
	restoreID = rc.normalizePitrIdMapRestoreID(restoreID)
	return rc.loadPitrIdMapPayloadFromTable(ctx, restoredTS, restoreID)
}

func (rc *LogClient) LoadPitrIdMapPayloadForSegment(
	ctx context.Context,
	restoredTS uint64,
) (*backuppb.PitrIdMapPayload, bool, error) {
	if !rc.pitrIDMapTableExists() {
		return nil, false, nil
	}
	return rc.loadPitrIdMapPayloadForSegment(ctx, restoredTS)
}

func (rc *LogClient) loadPitrIdMapPayloadFromTable(
	ctx context.Context,
	restoredTS uint64,
	restoreID uint64,
) (*backuppb.PitrIdMapPayload, bool, error) {
	restoreID = rc.normalizePitrIdMapRestoreID(restoreID)
	payload, found, err := rc.loadPitrIdMapPayloadFromTableOnce(ctx, restoredTS, restoreID)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	return payload, found, nil
}

func (rc *LogClient) loadPitrIdMapPayloadFromTableOnce(
	ctx context.Context,
	restoredTS uint64,
	restoreID uint64,
) (*backuppb.PitrIdMapPayload, bool, error) {
	metaData, found, err := rc.loadPitrIdMapDataFromTable(ctx, restoredTS, restoreID)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	if !found {
		return nil, false, nil
	}
	payload, err := decodePitrIdMapPayload(metaData)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	return payload, true, nil
}

func (rc *LogClient) savePitrIdMapPayloadToTable(
	ctx context.Context,
	restoredTS uint64,
	payload *backuppb.PitrIdMapPayload,
) error {
	if payload == nil {
		return errors.New("pitr id map payload is nil")
	}
	if payload.Version == 0 {
		payload.Version = pitrIdMapPayloadVersion
	}
	data, err := proto.Marshal(payload)
	if err != nil {
		return errors.Trace(err)
	}

	hasRestoreIDColumn := rc.pitrIDMapHasRestoreIDColumn()
	if hasRestoreIDColumn {
		err = rc.unsafeSession.ExecuteInternal(ctx,
			"DELETE FROM mysql.tidb_pitr_id_map WHERE restored_ts = %? and upstream_cluster_id = %? and restore_id = %?;",
			restoredTS, rc.upstreamClusterID, rc.restoreID)
		if err != nil {
			return errors.Trace(err)
		}
		replacePitrIDMapSQL := "REPLACE INTO mysql.tidb_pitr_id_map (restore_id, restored_ts, upstream_cluster_id, segment_id, id_map) VALUES (%?, %?, %?, %?, %?);"
		for startIdx, segmentID := 0, 0; startIdx < len(data); segmentID += 1 {
			endIdx := min(startIdx+PITRIdMapBlockSize, len(data))
			err := rc.unsafeSession.ExecuteInternal(ctx, replacePitrIDMapSQL, rc.restoreID, restoredTS, rc.upstreamClusterID, segmentID, data[startIdx:endIdx])
			if err != nil {
				return errors.Trace(err)
			}
			startIdx = endIdx
		}
		return nil
	}

	log.Info("mysql.tidb_pitr_id_map table does not have restore_id column, using backward compatible mode")
	err = rc.unsafeSession.ExecuteInternal(ctx,
		"DELETE FROM mysql.tidb_pitr_id_map WHERE restored_ts = %? and upstream_cluster_id = %?;",
		restoredTS, rc.upstreamClusterID)
	if err != nil {
		return errors.Trace(err)
	}
	replacePitrIDMapSQL := "REPLACE INTO mysql.tidb_pitr_id_map (restored_ts, upstream_cluster_id, segment_id, id_map) VALUES (%?, %?, %?, %?);"
	for startIdx, segmentID := 0, 0; startIdx < len(data); segmentID += 1 {
		endIdx := min(startIdx+PITRIdMapBlockSize, len(data))
		err := rc.unsafeSession.ExecuteInternal(ctx, replacePitrIDMapSQL, restoredTS, rc.upstreamClusterID, segmentID, data[startIdx:endIdx])
		if err != nil {
			return errors.Trace(err)
		}
		startIdx = endIdx
	}
	return nil
}
