// Copyright 2019 PingCAP, Inc.
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

package checkpoints

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/joho/sqltocsv"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// RemoveCheckpoint implements CheckpointsDB.RemoveCheckpoint.
func (cpdb *MySQLCheckpointsDB) RemoveCheckpoint(ctx context.Context, tableName string) error {
	s := common.SQLWithRetry{
		DB:     cpdb.db,
		Logger: log.Wrap(logutil.Logger(ctx)).With(zap.String("table", tableName)),
	}

	if tableName == allTables {
		return s.Exec(ctx, "remove all checkpoints", common.SprintfWithIdentifiers("DROP SCHEMA %s", cpdb.schema))
	}

	deleteChunkQuery := common.SprintfWithIdentifiers(DeleteCheckpointRecordTemplate, cpdb.schema, CheckpointTableNameChunk)
	deleteEngineQuery := common.SprintfWithIdentifiers(DeleteCheckpointRecordTemplate, cpdb.schema, CheckpointTableNameEngine)
	deleteTableQuery := common.SprintfWithIdentifiers(DeleteCheckpointRecordTemplate, cpdb.schema, CheckpointTableNameTable)

	return s.Transact(ctx, "remove checkpoints", func(c context.Context, tx *sql.Tx) error {
		if _, e := tx.ExecContext(c, deleteChunkQuery, tableName); e != nil {
			return errors.Trace(e)
		}
		if _, e := tx.ExecContext(c, deleteEngineQuery, tableName); e != nil {
			return errors.Trace(e)
		}
		if _, e := tx.ExecContext(c, deleteTableQuery, tableName); e != nil {
			return errors.Trace(e)
		}
		return nil
	})
}

// MoveCheckpoints implements CheckpointsDB.MoveCheckpoints.
func (cpdb *MySQLCheckpointsDB) MoveCheckpoints(ctx context.Context, taskID int64) error {
	newSchema := fmt.Sprintf("%s.%d.bak", cpdb.schema, taskID)
	s := common.SQLWithRetry{
		DB:     cpdb.db,
		Logger: log.Wrap(logutil.Logger(ctx)).With(zap.Int64("taskID", taskID)),
	}

	createSchemaQuery := common.SprintfWithIdentifiers("CREATE SCHEMA IF NOT EXISTS %s", newSchema)
	if e := s.Exec(ctx, "create backup checkpoints schema", createSchemaQuery); e != nil {
		return e
	}
	for _, tbl := range []string{
		CheckpointTableNameChunk, CheckpointTableNameEngine,
		CheckpointTableNameTable, CheckpointTableNameTask,
	} {
		query := common.SprintfWithIdentifiers("RENAME TABLE %[1]s.%[3]s TO %[2]s.%[3]s", cpdb.schema, newSchema, tbl)
		if e := s.Exec(ctx, fmt.Sprintf("move %s checkpoints table", tbl), query); e != nil {
			return e
		}
	}

	return nil
}

// GetLocalStoringTables implements CheckpointsDB.GetLocalStoringTables.
func (cpdb *MySQLCheckpointsDB) GetLocalStoringTables(ctx context.Context) (map[string][]int32, error) {
	var targetTables map[string][]int32

	// lightning didn't check CheckpointStatusMaxInvalid before this function is called, so we skip invalid ones
	// engines should exist if
	// 1. table status is earlier than CheckpointStatusIndexImported, and
	// 2. engine status is earlier than CheckpointStatusImported, and
	// 3. chunk has been read

	query := common.SprintfWithIdentifiers(`
		SELECT DISTINCT t.table_name, c.engine_id
		FROM %s.%s t, %s.%s c, %s.%s e
		WHERE t.table_name = c.table_name AND t.table_name = e.table_name AND c.engine_id = e.engine_id
			AND ? < t.status AND t.status < ?
			AND ? < e.status AND e.status < ?
			AND c.pos > c.offset;`,
		cpdb.schema, CheckpointTableNameTable, cpdb.schema, CheckpointTableNameChunk, cpdb.schema, CheckpointTableNameEngine)

	err := common.Retry("get local storing tables", log.Wrap(logutil.Logger(ctx)), func() error {
		targetTables = make(map[string][]int32)
		rows, err := cpdb.db.QueryContext(ctx, query,
			CheckpointStatusMaxInvalid, CheckpointStatusIndexImported,
			CheckpointStatusMaxInvalid, CheckpointStatusImported)
		if err != nil {
			return errors.Trace(err)
		}
		//nolint: errcheck
		defer rows.Close()
		for rows.Next() {
			var (
				tableName string
				engineID  int32
			)
			if err := rows.Scan(&tableName, &engineID); err != nil {
				return errors.Trace(err)
			}
			targetTables[tableName] = append(targetTables[tableName], engineID)
		}
		if err := rows.Err(); err != nil {
			return errors.Trace(err)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return targetTables, err
}

// IgnoreErrorCheckpoint implements CheckpointsDB.IgnoreErrorCheckpoint.
func (cpdb *MySQLCheckpointsDB) IgnoreErrorCheckpoint(ctx context.Context, tableName string) error {
	var (
		query, query2 string
		args          []any
	)
	if tableName == allTables {
		query = common.SprintfWithIdentifiers("UPDATE %s.%s SET status = ? WHERE status <= ?", cpdb.schema, CheckpointTableNameEngine)
		query2 = common.SprintfWithIdentifiers("UPDATE %s.%s SET status = ? WHERE status <= ?", cpdb.schema, CheckpointTableNameTable)
		args = []any{CheckpointStatusLoaded, CheckpointStatusMaxInvalid}
	} else {
		query = common.SprintfWithIdentifiers("UPDATE %s.%s SET status = ? WHERE table_name = ? AND status <= ?", cpdb.schema, CheckpointTableNameEngine)
		query2 = common.SprintfWithIdentifiers("UPDATE %s.%s SET status = ? WHERE table_name = ? AND status <= ?", cpdb.schema, CheckpointTableNameTable)
		args = []any{CheckpointStatusLoaded, tableName, CheckpointStatusMaxInvalid}
	}

	s := common.SQLWithRetry{
		DB:     cpdb.db,
		Logger: log.Wrap(logutil.Logger(ctx)).With(zap.String("table", tableName)),
	}
	err := s.Transact(ctx, "ignore error checkpoints", func(c context.Context, tx *sql.Tx) error {
		if _, e := tx.ExecContext(c, query, args...); e != nil {
			return errors.Trace(e)
		}
		if _, e := tx.ExecContext(c, query2, args...); e != nil {
			return errors.Trace(e)
		}
		return nil
	})
	return errors.Trace(err)
}

// DestroyErrorCheckpoint implements CheckpointsDB.DestroyErrorCheckpoint.
func (cpdb *MySQLCheckpointsDB) DestroyErrorCheckpoint(ctx context.Context, tableName string) ([]DestroyedTableCheckpoint, error) {
	var (
		selectQuery, deleteChunkQuery, deleteEngineQuery, deleteTableQuery string
		args                                                               []any
	)
	if tableName == allTables {
		selectQuery = common.SprintfWithIdentifiers(`
			SELECT
				t.table_name,
				COALESCE(MIN(e.engine_id), 0),
				COALESCE(MAX(e.engine_id), -1)
			FROM %[1]s.%[2]s t
			LEFT JOIN %[1]s.%[3]s e ON t.table_name = e.table_name
			WHERE t.status <= ?
			GROUP BY t.table_name;
		`, cpdb.schema, CheckpointTableNameTable, CheckpointTableNameEngine)
		deleteChunkQuery = common.SprintfWithIdentifiers(`
			DELETE FROM %[1]s.%[2]s WHERE table_name IN (SELECT table_name FROM %[1]s.%[3]s WHERE status <= ?)
		`, cpdb.schema, CheckpointTableNameChunk, CheckpointTableNameTable)
		deleteEngineQuery = common.SprintfWithIdentifiers(`
			DELETE FROM %[1]s.%[2]s WHERE table_name IN (SELECT table_name FROM %[1]s.%[3]s WHERE status <= ?)
		`, cpdb.schema, CheckpointTableNameEngine, CheckpointTableNameTable)
		deleteTableQuery = common.SprintfWithIdentifiers(`
			DELETE FROM %s.%s WHERE status <= ?
		`, cpdb.schema, CheckpointTableNameTable)
		args = []any{CheckpointStatusMaxInvalid}
	} else {
		selectQuery = common.SprintfWithIdentifiers(`
			SELECT
				t.table_name,
				COALESCE(MIN(e.engine_id), 0),
				COALESCE(MAX(e.engine_id), -1)
			FROM %[1]s.%[2]s t
			LEFT JOIN %[1]s.%[3]s e ON t.table_name = e.table_name
			WHERE t.table_name = ? AND t.status <= ?
			GROUP BY t.table_name;
		`, cpdb.schema, CheckpointTableNameTable, CheckpointTableNameEngine)
		deleteChunkQuery = common.SprintfWithIdentifiers(`
			DELETE FROM %[1]s.%[2]s WHERE table_name IN (SELECT table_name FROM %[1]s.%[3]s WHERE table_name = ? AND status <= ?)
		`, cpdb.schema, CheckpointTableNameChunk, CheckpointTableNameTable)
		deleteEngineQuery = common.SprintfWithIdentifiers(`
			DELETE FROM %[1]s.%[2]s WHERE table_name IN (SELECT table_name FROM %[1]s.%[3]s WHERE table_name = ? AND status <= ?)
		`, cpdb.schema, CheckpointTableNameEngine, CheckpointTableNameTable)
		deleteTableQuery = common.SprintfWithIdentifiers(`
			DELETE FROM %s.%s WHERE table_name = ? AND status <= ?
		`, cpdb.schema, CheckpointTableNameTable)
		args = []any{tableName, CheckpointStatusMaxInvalid}
	}

	var targetTables []DestroyedTableCheckpoint

	s := common.SQLWithRetry{
		DB:     cpdb.db,
		Logger: log.Wrap(logutil.Logger(ctx)).With(zap.String("table", tableName)),
	}
	err := s.Transact(ctx, "destroy error checkpoints", func(c context.Context, tx *sql.Tx) error {
		// Obtain the list of tables
		targetTables = nil
		rows, e := tx.QueryContext(c, selectQuery, args...)
		if e != nil {
			return errors.Trace(e)
		}
		//nolint: errcheck
		defer rows.Close()
		for rows.Next() {
			var dtc DestroyedTableCheckpoint
			if e := rows.Scan(&dtc.TableName, &dtc.MinEngineID, &dtc.MaxEngineID); e != nil {
				return errors.Trace(e)
			}
			targetTables = append(targetTables, dtc)
		}
		if e := rows.Err(); e != nil {
			return errors.Trace(e)
		}

		// Delete the checkpoints
		if _, e := tx.ExecContext(c, deleteChunkQuery, args...); e != nil {
			return errors.Trace(e)
		}
		if _, e := tx.ExecContext(c, deleteEngineQuery, args...); e != nil {
			return errors.Trace(e)
		}
		if _, e := tx.ExecContext(c, deleteTableQuery, args...); e != nil {
			return errors.Trace(e)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return targetTables, nil
}

// DumpTables implements CheckpointsDB.DumpTables.
//
//nolint:rowserrcheck // sqltocsv.Write will check this.
func (cpdb *MySQLCheckpointsDB) DumpTables(ctx context.Context, writer io.Writer) error {
	//nolint: rowserrcheck
	rows, err := cpdb.db.QueryContext(ctx, common.SprintfWithIdentifiers(`
		SELECT
			task_id,
			table_name,
			hex(hash) AS hash,
			status,
			create_time,
			update_time,
			auto_rand_base,
			auto_incr_base,
			auto_row_id_base
		FROM %s.%s;
	`, cpdb.schema, CheckpointTableNameTable))
	if err != nil {
		return errors.Trace(err)
	}
	//nolint: errcheck
	defer rows.Close()

	return errors.Trace(sqltocsv.Write(writer, rows))
}

// DumpEngines implements CheckpointsDB.DumpEngines.
//
//nolint:rowserrcheck // sqltocsv.Write will check this.
func (cpdb *MySQLCheckpointsDB) DumpEngines(ctx context.Context, writer io.Writer) error {
	//nolint: rowserrcheck
	rows, err := cpdb.db.QueryContext(ctx, common.SprintfWithIdentifiers(`
		SELECT
			table_name,
			engine_id,
			status,
			create_time,
			update_time
		FROM %s.%s;
	`, cpdb.schema, CheckpointTableNameEngine))
	if err != nil {
		return errors.Trace(err)
	}
	//nolint: errcheck
	defer rows.Close()

	return errors.Trace(sqltocsv.Write(writer, rows))
}

// DumpChunks implements CheckpointsDB.DumpChunks.
//
//nolint:rowserrcheck // sqltocsv.Write will check this.
func (cpdb *MySQLCheckpointsDB) DumpChunks(ctx context.Context, writer io.Writer) error {
	//nolint: rowserrcheck
	rows, err := cpdb.db.QueryContext(ctx, common.SprintfWithIdentifiers(`
		SELECT
			table_name,
			path,
			offset,
			type,
			compression,
			sort_key,
			file_size,
			columns,
			pos,
			real_pos,
			end_offset,
			prev_rowid_max,
			rowid_max,
			kvc_bytes,
			kvc_kvs,
			kvc_checksum,
			create_time,
			update_time
		FROM %s.%s;
	`, cpdb.schema, CheckpointTableNameChunk))
	if err != nil {
		return errors.Trace(err)
	}
	//nolint: errcheck
	defer rows.Close()

	return errors.Trace(sqltocsv.Write(writer, rows))
}
