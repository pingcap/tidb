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
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

const (
	LogRestorePITRItemsDatabaseName = "__TiDB_BR_Temporary_Log_Restore_PiTR_Items"

	pitrIngestItemsTableName  = "pitr_ingest_items"
	pitrTiFlashItemsTableName = "pitr_tiflash_items"

	pitrIngestItemsDir  = "pitr_ingest_items"
	pitrTiFlashItemsDir = "pitr_tiflash_items"

	createPITRItemsTable = `
		CREATE TABLE IF NOT EXISTS %n.%n (
			cluster_id BIGINT NOT NULL,
			restored_ts BIGINT NOT NULL,
			segment_id BIGINT NOT NULL,
			data BLOB(524288) NOT NULL,
			update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY(cluster_id, restored_ts, segment_id));`

	insertPITRItemsSQLTemplate = `
		REPLACE INTO %n.%n (cluster_id, restored_ts, segment_id, data) VALUES (%?, %?, %?, %?);`

	selectPITRItemsSQLTemplate = `
		SELECT segment_id, data FROM %n.%n WHERE cluster_id = %? AND restored_ts = %? ORDER BY segment_id;`

	deletePITRItemsSQLTemplate = `
		DELETE FROM %n.%n WHERE cluster_id = %? AND restored_ts = %?;`
)

type pitrIngestItemsPayload struct {
	Items map[int64]map[int64]bool `json:"items"`
}

type pitrTiFlashItemsPayload struct {
	Items map[int64]model.TiFlashReplicaInfo `json:"items"`
}

func marshalPITRIngestItems(items map[int64]map[int64]bool) ([]byte, error) {
	if items == nil {
		items = map[int64]map[int64]bool{}
	}
	return json.Marshal(&pitrIngestItemsPayload{Items: items})
}

func unmarshalPITRIngestItems(data []byte) (map[int64]map[int64]bool, error) {
	var payload pitrIngestItemsPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, errors.Trace(err)
	}
	if payload.Items == nil {
		payload.Items = map[int64]map[int64]bool{}
	}
	return payload.Items, nil
}

func marshalPITRTiFlashItems(items map[int64]model.TiFlashReplicaInfo) ([]byte, error) {
	if items == nil {
		items = map[int64]model.TiFlashReplicaInfo{}
	}
	return json.Marshal(&pitrTiFlashItemsPayload{Items: items})
}

func unmarshalPITRTiFlashItems(data []byte) (map[int64]model.TiFlashReplicaInfo, error) {
	var payload pitrTiFlashItemsPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, errors.Trace(err)
	}
	if payload.Items == nil {
		payload.Items = map[int64]model.TiFlashReplicaInfo{}
	}
	return payload.Items, nil
}

func pitrItemsFilename(dir, name string, clusterID, restoredTS uint64) string {
	return fmt.Sprintf("%s/%s.cluster_id:%d.restored_ts:%d", dir, name, clusterID, restoredTS)
}

func pitrIngestItemsPath(clusterID, restoredTS uint64) string {
	return pitrItemsFilename(pitrIngestItemsDir, pitrIngestItemsDir, clusterID, restoredTS)
}

func pitrTiFlashItemsPath(clusterID, restoredTS uint64) string {
	return pitrItemsFilename(pitrTiFlashItemsDir, pitrTiFlashItemsDir, clusterID, restoredTS)
}

func loadPITRItemsFromStorage(
	ctx context.Context,
	storage storeapi.Storage,
	path string,
	itemName string,
) ([]byte, bool, error) {
	exists, err := storage.FileExists(ctx, path)
	if err != nil {
		return nil, false, errors.Annotatef(err, "failed to check %s file %s", itemName, path)
	}
	if !exists {
		return nil, false, nil
	}
	raw, err := storage.ReadFile(ctx, path)
	if err != nil {
		return nil, false, errors.Annotatef(err, "failed to read %s file %s", itemName, path)
	}
	return raw, true, nil
}

func savePITRItemsToStorage(
	ctx context.Context,
	storage storeapi.Storage,
	path string,
	itemName string,
	data []byte,
) error {
	if err := storage.WriteFile(ctx, path, data); err != nil {
		return errors.Annotatef(err, "failed to save %s file %s", itemName, path)
	}
	return nil
}

func initPITRItemsTable(ctx context.Context, se glue.Session, dbName string, tableNames []string) error {
	if err := se.ExecuteInternal(ctx, "CREATE DATABASE IF NOT EXISTS %n;", dbName); err != nil {
		return errors.Trace(err)
	}
	for _, tableName := range tableNames {
		if err := se.ExecuteInternal(ctx, createPITRItemsTable, dbName, tableName); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func pitrItemsTableExists(dom *domain.Domain, tableName string) bool {
	if dom == nil {
		return false
	}
	return dom.InfoSchema().
		TableExists(ast.NewCIStr(LogRestorePITRItemsDatabaseName), ast.NewCIStr(tableName))
}

func loadPITRItemsFromTable(
	ctx context.Context,
	execCtx sqlexec.RestrictedSQLExecutor,
	dbName string,
	tableName string,
	clusterID uint64,
	restoredTS uint64,
) ([]byte, bool, error) {
	rows, _, errSQL := execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		selectPITRItemsSQLTemplate,
		dbName, tableName, clusterID, restoredTS,
	)
	if errSQL != nil {
		return nil, false, errors.Annotatef(errSQL, "failed to get pitr items from table %s.%s", dbName, tableName)
	}
	if len(rows) == 0 {
		return nil, false, nil
	}
	data := make([]byte, 0, len(rows)*CheckpointIdMapBlockSize)
	for i, row := range rows {
		segmentID, chunk := row.GetUint64(0), row.GetBytes(1)
		if uint64(i) != segmentID {
			return nil, false, errors.Errorf(
				"pitr items table %s.%s is incomplete at segment %d", dbName, tableName, segmentID)
		}
		data = append(data, chunk...)
	}
	return data, true, nil
}

func savePITRItemsToTable(
	ctx context.Context,
	se glue.Session,
	dbName string,
	tableName string,
	clusterID uint64,
	restoredTS uint64,
	data []byte,
) error {
	if err := initPITRItemsTable(ctx, se, dbName, []string{tableName}); err != nil {
		return errors.Trace(err)
	}
	if err := se.ExecuteInternal(ctx, deletePITRItemsSQLTemplate, dbName, tableName, clusterID, restoredTS); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(chunkInsertCheckpointData(data, func(segmentID uint64, chunk []byte) error {
		return errors.Trace(se.ExecuteInternal(ctx, insertPITRItemsSQLTemplate, dbName, tableName, clusterID, restoredTS, segmentID, chunk))
	}))
}
