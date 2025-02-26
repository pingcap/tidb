// Copyright 2024 PingCAP, Inc.
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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

type checkpointStorage interface {
	flushCheckpointData(ctx context.Context, data []byte) error
	flushCheckpointChecksum(ctx context.Context, data []byte) error

	initialLock(ctx context.Context) error
	updateLock(ctx context.Context) error

	close()
}

// Notice that:
// 1. the checkpoint table only records one task checkpoint.
// 2. BR regards the metadata table as a file so that it is not empty if the table exists.
// 3. BR regards the checkpoint table as a directory which is managed by metadata table.
const (
	LogRestoreCheckpointDatabaseName       string = "__TiDB_BR_Temporary_Log_Restore_Checkpoint"
	SnapshotRestoreCheckpointDatabaseName  string = "__TiDB_BR_Temporary_Snapshot_Restore_Checkpoint"
	CustomSSTRestoreCheckpointDatabaseName string = "__TiDB_BR_Temporary_Custom_SST_Restore_Checkpoint"

	// directory level table
	checkpointDataTableName     string = "cpt_data"
	checkpointChecksumTableName string = "cpt_checksum"
	// file level table
	checkpointMetaTableName     string = "cpt_metadata"
	checkpointProgressTableName string = "cpt_progress"
	checkpointIngestTableName   string = "cpt_ingest"

	// the primary key (uuid: uuid, segment_id:0) records the number of segment
	createCheckpointTable string = `
		CREATE TABLE %n.%n (
			uuid binary(32) NOT NULL,
			segment_id BIGINT NOT NULL,
			data BLOB(524288) NOT NULL,
			update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY(uuid, segment_id));`

	insertCheckpointSQLTemplate string = `
		REPLACE INTO %s.%s
			(uuid, segment_id, data) VALUES (%%?, %%?, %%?);`

	selectCheckpointSQLTemplate string = `
		SELECT uuid, segment_id, data FROM %n.%n ORDER BY uuid, segment_id;`

	createCheckpointMetaTable string = `
		CREATE TABLE %n.%n (
			segment_id BIGINT NOT NULL,
			data BLOB(524288) NOT NULL,
			update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY(segment_id));`

	insertCheckpointMetaSQLTemplate string = `
		REPLACE INTO %n.%n (segment_id, data) VALUES (%?, %?);`

	selectCheckpointMetaSQLTemplate string = `SELECT segment_id, data FROM %n.%n;`
)

// IsCheckpointDB checks whether the dbname is checkpoint database.
func IsCheckpointDB(dbname ast.CIStr) bool {
	return dbname.O == LogRestoreCheckpointDatabaseName ||
		dbname.O == SnapshotRestoreCheckpointDatabaseName ||
		dbname.O == CustomSSTRestoreCheckpointDatabaseName
}

const CheckpointIdMapBlockSize int = 524288

func chunkInsertCheckpointData(data []byte, fn func(segmentId uint64, chunk []byte) error) error {
	for startIdx, segmentId := 0, uint64(0); startIdx < len(data); segmentId += 1 {
		endIdx := startIdx + CheckpointIdMapBlockSize
		if endIdx > len(data) {
			endIdx = len(data)
		}
		if err := fn(segmentId, data[startIdx:endIdx]); err != nil {
			return errors.Trace(err)
		}
		startIdx = endIdx
	}
	return nil
}

func chunkInsertCheckpointSQLs(dbName, tableName string, data []byte) ([]string, [][]any) {
	sqls := make([]string, 0, len(data)/CheckpointIdMapBlockSize+1)
	argss := make([][]any, 0, len(data)/CheckpointIdMapBlockSize+1)
	uuid := uuid.New()
	_ = chunkInsertCheckpointData(data, func(segmentId uint64, chunk []byte) error {
		sqls = append(sqls, fmt.Sprintf(insertCheckpointSQLTemplate, dbName, tableName))
		argss = append(argss, []any{uuid[:], segmentId, chunk})
		return nil
	})
	return sqls, argss
}

type tableCheckpointStorage struct {
	se               glue.Session
	checkpointDBName string
}

func (s *tableCheckpointStorage) close() {
	if s.se != nil {
		s.se.Close()
	}
}

func (s *tableCheckpointStorage) initialLock(ctx context.Context) error {
	log.Fatal("unimplement!")
	return nil
}

func (s *tableCheckpointStorage) updateLock(ctx context.Context) error {
	log.Fatal("unimplement!")
	return nil
}

func (s *tableCheckpointStorage) flushCheckpointData(ctx context.Context, data []byte) error {
	sqls, argss := chunkInsertCheckpointSQLs(s.checkpointDBName, checkpointDataTableName, data)
	for i, sql := range sqls {
		args := argss[i]
		if err := s.se.ExecuteInternal(ctx, sql, args...); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (s *tableCheckpointStorage) flushCheckpointChecksum(ctx context.Context, data []byte) error {
	sqls, argss := chunkInsertCheckpointSQLs(s.checkpointDBName, checkpointChecksumTableName, data)
	for i, sql := range sqls {
		args := argss[i]
		if err := s.se.ExecuteInternal(ctx, sql, args...); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func mergeSelectCheckpoint(
	ctx context.Context,
	execCtx sqlexec.RestrictedSQLExecutor,
	dbName, tableName string,
) ([][]byte, error) {
	rows, _, errSQL := execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		selectCheckpointSQLTemplate,
		dbName, tableName,
	)
	if errSQL != nil {
		return nil, errors.Annotatef(errSQL, "failed to get checkpoint data from table %s.%s", dbName, tableName)
	}

	var (
		retData         [][]byte = make([][]byte, 0, len(rows))
		rowData                  = []byte{}
		lastUUID        []byte   = nil
		lastUUIDInvalid bool     = false
		nextSegmentID   uint64   = 0
	)
	for _, row := range rows {
		uuid, segment_id, data := row.GetBytes(0), row.GetUint64(1), row.GetBytes(2)
		if len(uuid) == 0 {
			log.Warn("get the empty uuid, but just skip it")
			continue
		}
		if !bytes.Equal(uuid, lastUUID) {
			if !lastUUIDInvalid && len(rowData) > 0 {
				retData = append(retData, rowData)
			}
			rowData = make([]byte, 0)
			lastUUIDInvalid = false
			nextSegmentID = 0
			lastUUID = uuid
		}

		if lastUUIDInvalid {
			continue
		}

		if nextSegmentID != segment_id {
			lastUUIDInvalid = true
			continue
		}

		rowData = append(rowData, data...)
		nextSegmentID += 1
	}
	if !lastUUIDInvalid && len(rowData) > 0 {
		retData = append(retData, rowData)
	}
	return retData, nil
}

func selectCheckpointData[K KeyType, V ValueType](
	ctx context.Context,
	execCtx sqlexec.RestrictedSQLExecutor,
	dbName string,
	fn func(groupKey K, value V),
) (time.Duration, error) {
	// records the total time cost in the past executions
	var pastDureTime time.Duration = 0
	checkpointDatas, err := mergeSelectCheckpoint(ctx, execCtx, dbName, checkpointDataTableName)
	if err != nil {
		return pastDureTime, errors.Trace(err)
	}
	for _, content := range checkpointDatas {
		if err := parseCheckpointData(content, &pastDureTime, nil, fn); err != nil {
			return pastDureTime, errors.Trace(err)
		}
	}
	return pastDureTime, nil
}

func selectCheckpointChecksum(
	ctx context.Context,
	execCtx sqlexec.RestrictedSQLExecutor,
	dbName string,
) (map[int64]*ChecksumItem, time.Duration, error) {
	var pastDureTime time.Duration = 0
	checkpointChecksum := make(map[int64]*ChecksumItem)
	checkpointChecksums, err := mergeSelectCheckpoint(ctx, execCtx, dbName, checkpointChecksumTableName)
	if err != nil {
		return checkpointChecksum, pastDureTime, errors.Trace(err)
	}
	for _, content := range checkpointChecksums {
		if err := parseCheckpointChecksum(content, checkpointChecksum, &pastDureTime); err != nil {
			return checkpointChecksum, pastDureTime, errors.Trace(err)
		}
	}
	return checkpointChecksum, pastDureTime, nil
}

func initCheckpointTable(ctx context.Context, se glue.Session, dbName string, checkpointTableNames []string) error {
	if err := se.ExecuteInternal(ctx, "CREATE DATABASE IF NOT EXISTS %n;", dbName); err != nil {
		return errors.Trace(err)
	}
	for _, tableName := range checkpointTableNames {
		if err := se.ExecuteInternal(ctx, createCheckpointTable, dbName, tableName); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func insertCheckpointMeta[T any](ctx context.Context, se glue.Session, dbName string, tableName string, meta *T) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return errors.Trace(err)
	}
	if err := se.ExecuteInternal(ctx, createCheckpointMetaTable, dbName, tableName); err != nil {
		return errors.Trace(err)
	}
	err = chunkInsertCheckpointData(data, func(segmentId uint64, chunk []byte) error {
		err := se.ExecuteInternal(ctx, insertCheckpointMetaSQLTemplate, dbName, tableName, segmentId, chunk)
		return errors.Trace(err)
	})
	return errors.Trace(err)
}

func selectCheckpointMeta(
	ctx context.Context,
	execCtx sqlexec.RestrictedSQLExecutor,
	dbName string, tableName string,
	meta any,
) error {
	rows, _, errSQL := execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		selectCheckpointMetaSQLTemplate,
		dbName, tableName,
	)
	if errSQL != nil {
		return errors.Annotatef(errSQL, "failed to get checkpoint metadata from table %s.%s", dbName, tableName)
	}
	if len(rows) == 0 {
		return errors.Errorf(
			"get the empty checkpoint meta, the checkpoint is incomplete from table %s.%s", dbName, tableName)
	}

	data := make([]byte, 0, len(rows)*CheckpointIdMapBlockSize)
	for i, row := range rows {
		segmentId, chunk := row.GetUint64(0), row.GetBytes(1)
		if uint64(i) != segmentId {
			return errors.Errorf(
				"the checkpoint metadata is incomplete from table %s.%s at segment %d", dbName, tableName, segmentId)
		}
		data = append(data, chunk...)
	}
	err := json.Unmarshal(data, meta)
	return errors.Trace(err)
}

func dropCheckpointTables(
	ctx context.Context,
	dom *domain.Domain,
	se glue.Session,
	dbName string, tableNames []string,
) error {
	for _, tableName := range tableNames {
		if err := se.ExecuteInternal(ctx, "DROP TABLE IF EXISTS %n.%n;", dbName, tableName); err != nil {
			return errors.Trace(err)
		}
	}
	// check if any user table is created in the checkpoint database
	tables, err := dom.InfoSchema().SchemaTableInfos(ctx, ast.NewCIStr(dbName))
	if err != nil {
		return errors.Trace(err)
	}
	if len(tables) > 0 {
		log.Warn("user tables in the checkpoint database, skip drop the database",
			zap.String("db", dbName), zap.String("table", tables[0].Name.L))
		return nil
	}
	if err := se.ExecuteInternal(ctx, "DROP DATABASE %n;", dbName); err != nil {
		return errors.Trace(err)
	}
	return nil
}
