// Copyright 2020 PingCAP, Inc.
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
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

type Session interface {
	Close()
	Execute(context.Context, string) ([]sqlexec.RecordSet, error)
	CommitTxn(context.Context) error
	RollbackTxn(context.Context)
	PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*ast.ResultField, err error)
	ExecutePreparedStmt(ctx context.Context, stmtID uint32, param []types.Datum) (sqlexec.RecordSet, error)
	DropPreparedStmt(stmtID uint32) error
}

// GlueCheckpointsDB is almost same with MySQLCheckpointsDB, but it uses TiDB's internal data structure which requires a
// lot to keep same with database/sql.
// TODO: Encapsulate Begin/Commit/Rollback txn, form SQL with args and query/iter/scan TiDB's RecordSet into a interface
// to reuse MySQLCheckpointsDB.
type GlueCheckpointsDB struct {
	// getSessionFunc will get a new session from TiDB
	getSessionFunc func() (Session, error)
	schema         string
}

var _ DB = (*GlueCheckpointsDB)(nil)

// dropPreparedStmt drops the statement and when meet an error,
// print an error message.
func dropPreparedStmt(session Session, stmtID uint32) {
	if err := session.DropPreparedStmt(stmtID); err != nil {
		log.L().Error("failed to drop prepared statement", log.ShortError(err))
	}
}

func NewGlueCheckpointsDB(ctx context.Context, se Session, f func() (Session, error), schemaName string) (*GlueCheckpointsDB, error) {
	var escapedSchemaName strings.Builder
	common.WriteMySQLIdentifier(&escapedSchemaName, schemaName)
	schema := escapedSchemaName.String()
	logger := log.With(zap.String("schema", schemaName))

	sql := fmt.Sprintf(CreateDBTemplate, schema)
	err := common.Retry("create checkpoints database", logger, func() error {
		_, err := se.Execute(ctx, sql)
		return err
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	sql = fmt.Sprintf(CreateTaskTableTemplate, schema, CheckpointTableNameTask)
	err = common.Retry("create task checkpoints table", logger, func() error {
		_, err := se.Execute(ctx, sql)
		return err
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	sql = fmt.Sprintf(CreateTableTableTemplate, schema, CheckpointTableNameTable)
	err = common.Retry("create table checkpoints table", logger, func() error {
		_, err := se.Execute(ctx, sql)
		return err
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	sql = fmt.Sprintf(CreateEngineTableTemplate, schema, CheckpointTableNameEngine)
	err = common.Retry("create engine checkpoints table", logger, func() error {
		_, err := se.Execute(ctx, sql)
		return err
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	sql = fmt.Sprintf(CreateChunkTableTemplate, schema, CheckpointTableNameChunk)
	err = common.Retry("create chunks checkpoints table", logger, func() error {
		_, err := se.Execute(ctx, sql)
		return err
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &GlueCheckpointsDB{
		getSessionFunc: f,
		schema:         schema,
	}, nil
}

func (g GlueCheckpointsDB) Initialize(ctx context.Context, cfg *config.Config, dbInfo map[string]*TidbDBInfo) error {
	logger := log.L()
	se, err := g.getSessionFunc()
	if err != nil {
		return errors.Trace(err)
	}
	defer se.Close()

	err = Transact(ctx, "insert checkpoints", se, logger, func(c context.Context, s Session) error {
		stmtID, _, _, err := s.PrepareStmt(fmt.Sprintf(InitTaskTemplate, g.schema, CheckpointTableNameTask))
		if err != nil {
			return errors.Trace(err)
		}
		defer dropPreparedStmt(s, stmtID)
		_, err = s.ExecutePreparedStmt(c, stmtID, []types.Datum{
			types.NewIntDatum(cfg.TaskID),
			types.NewStringDatum(cfg.Mydumper.SourceDir),
			types.NewStringDatum(cfg.TikvImporter.Backend),
			types.NewStringDatum(cfg.TikvImporter.Addr),
			types.NewStringDatum(cfg.TiDB.Host),
			types.NewIntDatum(int64(cfg.TiDB.Port)),
			types.NewStringDatum(cfg.TiDB.PdAddr),
			types.NewStringDatum(cfg.TikvImporter.SortedKVDir),
			types.NewStringDatum(build.ReleaseVersion),
		})
		if err != nil {
			return errors.Trace(err)
		}

		stmtID2, _, _, err := s.PrepareStmt(fmt.Sprintf(InitTableTemplate, g.schema, CheckpointTableNameTable))
		if err != nil {
			return errors.Trace(err)
		}
		defer dropPreparedStmt(s, stmtID2)

		for _, db := range dbInfo {
			for _, table := range db.Tables {
				tableName := common.UniqueTable(db.Name, table.Name)
				_, err = s.ExecutePreparedStmt(c, stmtID2, []types.Datum{
					types.NewIntDatum(cfg.TaskID),
					types.NewStringDatum(tableName),
					types.NewIntDatum(0),
					types.NewIntDatum(table.ID),
				})
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
		return nil
	})
	return errors.Trace(err)
}

func (g GlueCheckpointsDB) TaskCheckpoint(ctx context.Context) (*TaskCheckpoint, error) {
	logger := log.L()
	sql := fmt.Sprintf(ReadTaskTemplate, g.schema, CheckpointTableNameTask)
	se, err := g.getSessionFunc()
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer se.Close()

	var taskCp *TaskCheckpoint
	err = common.Retry("fetch task checkpoint", logger, func() error {
		rs, err := se.Execute(ctx, sql)
		if err != nil {
			return errors.Trace(err)
		}
		r := rs[0]
		defer r.Close()
		req := r.NewChunk(nil)
		err = r.Next(ctx, req)
		if err != nil {
			return err
		}
		if req.NumRows() == 0 {
			return nil
		}

		row := req.GetRow(0)
		taskCp = &TaskCheckpoint{}
		taskCp.TaskID = row.GetInt64(0)
		taskCp.SourceDir = row.GetString(1)
		taskCp.Backend = row.GetString(2)
		taskCp.ImporterAddr = row.GetString(3)
		taskCp.TiDBHost = row.GetString(4)
		taskCp.TiDBPort = int(row.GetInt64(5))
		taskCp.PdAddr = row.GetString(6)
		taskCp.SortedKVDir = row.GetString(7)
		taskCp.LightningVer = row.GetString(8)
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return taskCp, nil
}

func (g GlueCheckpointsDB) Get(ctx context.Context, tableName string) (*TableCheckpoint, error) {
	cp := &TableCheckpoint{
		Engines: map[int32]*EngineCheckpoint{},
	}
	logger := log.With(zap.String("table", tableName))
	se, err := g.getSessionFunc()
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer se.Close()

	tableName = common.InterpolateMySQLString(tableName)
	err = Transact(ctx, "read checkpoint", se, logger, func(c context.Context, s Session) error {
		// 1. Populate the engines.
		sql := fmt.Sprintf(ReadEngineTemplate, g.schema, CheckpointTableNameEngine)
		sql = strings.ReplaceAll(sql, "?", tableName)
		rs, err := s.Execute(ctx, sql)
		if err != nil {
			return errors.Trace(err)
		}
		r := rs[0]
		req := r.NewChunk(nil)
		it := chunk.NewIterator4Chunk(req)
		for {
			err = r.Next(ctx, req)
			if err != nil {
				r.Close()
				return err
			}
			if req.NumRows() == 0 {
				break
			}

			for row := it.Begin(); row != it.End(); row = it.Next() {
				engineID := int32(row.GetInt64(0))
				status := uint8(row.GetUint64(1))
				cp.Engines[engineID] = &EngineCheckpoint{
					Status: CheckpointStatus(status),
				}
			}
		}
		r.Close()

		// 2. Populate the chunks.
		sql = fmt.Sprintf(ReadChunkTemplate, g.schema, CheckpointTableNameChunk)
		sql = strings.ReplaceAll(sql, "?", tableName)
		rs, err = s.Execute(ctx, sql)
		if err != nil {
			return errors.Trace(err)
		}
		r = rs[0]
		req = r.NewChunk(nil)
		it = chunk.NewIterator4Chunk(req)
		for {
			err = r.Next(ctx, req)
			if err != nil {
				r.Close()
				return err
			}
			if req.NumRows() == 0 {
				break
			}

			for row := it.Begin(); row != it.End(); row = it.Next() {
				value := &ChunkCheckpoint{}
				engineID := int32(row.GetInt64(0))
				value.Key.Path = row.GetString(1)
				value.Key.Offset = row.GetInt64(2)
				value.FileMeta.Type = mydump.SourceType(row.GetInt64(3))
				value.FileMeta.Compression = mydump.Compression(row.GetInt64(4))
				value.FileMeta.SortKey = row.GetString(5)
				value.FileMeta.FileSize = row.GetInt64(6)
				colPerm := row.GetBytes(7)
				value.Chunk.Offset = row.GetInt64(8)
				value.Chunk.EndOffset = row.GetInt64(9)
				value.Chunk.PrevRowIDMax = row.GetInt64(10)
				value.Chunk.RowIDMax = row.GetInt64(11)
				kvcBytes := row.GetUint64(12)
				kvcKVs := row.GetUint64(13)
				kvcChecksum := row.GetUint64(14)
				value.Timestamp = row.GetInt64(15)

				value.FileMeta.Path = value.Key.Path
				value.Checksum = verify.MakeKVChecksum(kvcBytes, kvcKVs, kvcChecksum)
				if err := json.Unmarshal(colPerm, &value.ColumnPermutation); err != nil {
					r.Close()
					return errors.Trace(err)
				}
				cp.Engines[engineID].Chunks = append(cp.Engines[engineID].Chunks, value)
			}
		}
		r.Close()

		// 3. Fill in the remaining table info
		sql = fmt.Sprintf(ReadTableRemainTemplate, g.schema, CheckpointTableNameTable)
		sql = strings.ReplaceAll(sql, "?", tableName)
		rs, err = s.Execute(ctx, sql)
		if err != nil {
			return errors.Trace(err)
		}
		r = rs[0]
		defer r.Close()
		req = r.NewChunk(nil)
		err = r.Next(ctx, req)
		if err != nil {
			return err
		}
		if req.NumRows() == 0 {
			return nil
		}

		row := req.GetRow(0)
		cp.Status = CheckpointStatus(row.GetUint64(0))
		cp.AllocBase = row.GetInt64(1)
		cp.TableID = row.GetInt64(2)
		return nil
	})

	if err != nil {
		return nil, errors.Trace(err)
	}

	return cp, nil
}

func (g GlueCheckpointsDB) Close() error {
	return nil
}

func (g GlueCheckpointsDB) InsertEngineCheckpoints(ctx context.Context, tableName string, checkpointMap map[int32]*EngineCheckpoint) error {
	logger := log.With(zap.String("table", tableName))
	se, err := g.getSessionFunc()
	if err != nil {
		return errors.Trace(err)
	}
	defer se.Close()

	err = Transact(ctx, "update engine checkpoints", se, logger, func(c context.Context, s Session) error {
		engineStmt, _, _, err := s.PrepareStmt(fmt.Sprintf(ReplaceEngineTemplate, g.schema, CheckpointTableNameEngine))
		if err != nil {
			return errors.Trace(err)
		}
		defer dropPreparedStmt(s, engineStmt)

		chunkStmt, _, _, err := s.PrepareStmt(fmt.Sprintf(ReplaceChunkTemplate, g.schema, CheckpointTableNameChunk))
		if err != nil {
			return errors.Trace(err)
		}
		defer dropPreparedStmt(s, chunkStmt)

		for engineID, engine := range checkpointMap {
			_, err := s.ExecutePreparedStmt(c, engineStmt, []types.Datum{
				types.NewStringDatum(tableName),
				types.NewIntDatum(int64(engineID)),
				types.NewUintDatum(uint64(engine.Status)),
			})
			if err != nil {
				return errors.Trace(err)
			}
			for _, value := range engine.Chunks {
				columnPerm, err := json.Marshal(value.ColumnPermutation)
				if err != nil {
					return errors.Trace(err)
				}
				_, err = s.ExecutePreparedStmt(c, chunkStmt, []types.Datum{
					types.NewStringDatum(tableName),
					types.NewIntDatum(int64(engineID)),
					types.NewStringDatum(value.Key.Path),
					types.NewIntDatum(value.Key.Offset),
					types.NewIntDatum(int64(value.FileMeta.Type)),
					types.NewIntDatum(int64(value.FileMeta.Compression)),
					types.NewStringDatum(value.FileMeta.SortKey),
					types.NewIntDatum(value.FileMeta.FileSize),
					types.NewBytesDatum(columnPerm),
					types.NewIntDatum(value.Chunk.Offset),
					types.NewIntDatum(value.Chunk.EndOffset),
					types.NewIntDatum(value.Chunk.PrevRowIDMax),
					types.NewIntDatum(value.Chunk.RowIDMax),
					types.NewIntDatum(value.Timestamp),
				})
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
		return nil
	})
	return errors.Trace(err)
}

func (g GlueCheckpointsDB) Update(checkpointDiffs map[string]*TableCheckpointDiff) error {
	logger := log.L()
	se, err := g.getSessionFunc()
	if err != nil {
		log.L().Error("can't get a session to update GlueCheckpointsDB", zap.Error(errors.Trace(err)))
		return err
	}
	defer se.Close()

	chunkQuery := fmt.Sprintf(UpdateChunkTemplate, g.schema, CheckpointTableNameChunk)
	rebaseQuery := fmt.Sprintf(UpdateTableRebaseTemplate, g.schema, CheckpointTableNameTable)
	tableStatusQuery := fmt.Sprintf(UpdateTableStatusTemplate, g.schema, CheckpointTableNameTable)
	engineStatusQuery := fmt.Sprintf(UpdateEngineTemplate, g.schema, CheckpointTableNameEngine)
	return Transact(context.Background(), "update checkpoints", se, logger, func(c context.Context, s Session) error {
		chunkStmt, _, _, err := s.PrepareStmt(chunkQuery)
		if err != nil {
			return errors.Trace(err)
		}
		defer dropPreparedStmt(s, chunkStmt)
		rebaseStmt, _, _, err := s.PrepareStmt(rebaseQuery)
		if err != nil {
			return errors.Trace(err)
		}
		defer dropPreparedStmt(s, rebaseStmt)
		tableStatusStmt, _, _, err := s.PrepareStmt(tableStatusQuery)
		if err != nil {
			return errors.Trace(err)
		}
		defer dropPreparedStmt(s, tableStatusStmt)
		engineStatusStmt, _, _, err := s.PrepareStmt(engineStatusQuery)
		if err != nil {
			return errors.Trace(err)
		}
		defer dropPreparedStmt(s, engineStatusStmt)

		for tableName, cpd := range checkpointDiffs {
			if cpd.hasStatus {
				_, err := s.ExecutePreparedStmt(c, tableStatusStmt, []types.Datum{
					types.NewUintDatum(uint64(cpd.status)),
					types.NewStringDatum(tableName),
				})
				if err != nil {
					return errors.Trace(err)
				}
			}
			if cpd.hasRebase {
				_, err := s.ExecutePreparedStmt(c, rebaseStmt, []types.Datum{
					types.NewIntDatum(cpd.allocBase),
					types.NewStringDatum(tableName),
				})
				if err != nil {
					return errors.Trace(err)
				}
			}
			for engineID, engineDiff := range cpd.engines {
				if engineDiff.hasStatus {
					_, err := s.ExecutePreparedStmt(c, engineStatusStmt, []types.Datum{
						types.NewUintDatum(uint64(engineDiff.status)),
						types.NewStringDatum(tableName),
						types.NewIntDatum(int64(engineID)),
					})
					if err != nil {
						return errors.Trace(err)
					}
				}
				for key, diff := range engineDiff.chunks {
					columnPerm, err := json.Marshal(diff.columnPermutation)
					if err != nil {
						return errors.Trace(err)
					}
					_, err = s.ExecutePreparedStmt(c, chunkStmt, []types.Datum{
						types.NewIntDatum(diff.pos),
						types.NewIntDatum(diff.rowID),
						types.NewUintDatum(diff.checksum.SumSize()),
						types.NewUintDatum(diff.checksum.SumKVS()),
						types.NewUintDatum(diff.checksum.Sum()),
						types.NewBytesDatum(columnPerm),
						types.NewStringDatum(tableName),
						types.NewIntDatum(int64(engineID)),
						types.NewStringDatum(key.Path),
						types.NewIntDatum(key.Offset),
					})
					if err != nil {
						return errors.Trace(err)
					}
				}
			}
		}
		return nil
	})
}

func (g GlueCheckpointsDB) RemoveCheckpoint(ctx context.Context, tableName string) error {
	logger := log.With(zap.String("table", tableName))
	se, err := g.getSessionFunc()
	if err != nil {
		return errors.Trace(err)
	}
	defer se.Close()

	if tableName == allTables {
		return common.Retry("remove all checkpoints", logger, func() error {
			_, err := se.Execute(ctx, "DROP SCHEMA "+g.schema)
			return err
		})
	}
	tableName = common.InterpolateMySQLString(tableName)
	deleteChunkQuery := fmt.Sprintf(DeleteCheckpointRecordTemplate, g.schema, CheckpointTableNameChunk)
	deleteChunkQuery = strings.ReplaceAll(deleteChunkQuery, "?", tableName)
	deleteEngineQuery := fmt.Sprintf(DeleteCheckpointRecordTemplate, g.schema, CheckpointTableNameEngine)
	deleteEngineQuery = strings.ReplaceAll(deleteEngineQuery, "?", tableName)
	deleteTableQuery := fmt.Sprintf(DeleteCheckpointRecordTemplate, g.schema, CheckpointTableNameTable)
	deleteTableQuery = strings.ReplaceAll(deleteTableQuery, "?", tableName)

	return errors.Trace(Transact(ctx, "remove checkpoints", se, logger, func(c context.Context, s Session) error {
		if _, e := s.Execute(c, deleteChunkQuery); e != nil {
			return e
		}
		if _, e := s.Execute(c, deleteEngineQuery); e != nil {
			return e
		}
		if _, e := s.Execute(c, deleteTableQuery); e != nil {
			return e
		}
		return nil
	}))
}

func (g GlueCheckpointsDB) MoveCheckpoints(ctx context.Context, taskID int64) error {
	newSchema := fmt.Sprintf("`%s.%d.bak`", g.schema[1:len(g.schema)-1], taskID)
	logger := log.With(zap.Int64("taskID", taskID))
	se, err := g.getSessionFunc()
	if err != nil {
		return errors.Trace(err)
	}
	defer se.Close()

	err = common.Retry("create backup checkpoints schema", logger, func() error {
		_, err := se.Execute(ctx, "CREATE SCHEMA IF NOT EXISTS "+newSchema)
		return err
	})
	if err != nil {
		return errors.Trace(err)
	}
	for _, tbl := range []string{
		CheckpointTableNameChunk, CheckpointTableNameEngine,
		CheckpointTableNameTable, CheckpointTableNameTask,
	} {
		query := fmt.Sprintf("RENAME TABLE %[1]s.%[3]s TO %[2]s.%[3]s", g.schema, newSchema, tbl)
		err := common.Retry(fmt.Sprintf("move %s checkpoints table", tbl), logger, func() error {
			_, err := se.Execute(ctx, query)
			return err
		})
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (g GlueCheckpointsDB) GetLocalStoringTables(ctx context.Context) (map[string][]int32, error) {
	se, err := g.getSessionFunc()
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer se.Close()

	var targetTables map[string][]int32

	// lightning didn't check CheckpointStatusMaxInvalid before this function is called, so we skip invalid ones
	// engines should exist if
	// 1. table status is earlier than CheckpointStatusIndexImported, and
	// 2. engine status is earlier than CheckpointStatusImported, and
	// 3. chunk has been read
	query := fmt.Sprintf(`
		SELECT DISTINCT t.table_name, c.engine_id
		FROM %s.%s t, %s.%s c, %s.%s e
		WHERE t.table_name = c.table_name AND t.table_name = e.table_name AND c.engine_id = e.engine_id
			AND %d < t.status AND t.status < %d
			AND %d < e.status AND e.status < %d
			AND c.pos > c.offset;`,
		g.schema, CheckpointTableNameTable, g.schema, CheckpointTableNameChunk, g.schema, CheckpointTableNameEngine,
		CheckpointStatusMaxInvalid, CheckpointStatusIndexImported,
		CheckpointStatusMaxInvalid, CheckpointStatusImported)

	err = common.Retry("get local storing tables", log.L(), func() error {
		targetTables = make(map[string][]int32)
		rs, err := se.Execute(ctx, query)
		if err != nil {
			return errors.Trace(err)
		}
		rows, err := drainFirstRecordSet(ctx, rs)
		if err != nil {
			return errors.Trace(err)
		}

		for _, row := range rows {
			tableName := row.GetString(0)
			engineID := int32(row.GetInt64(1))
			targetTables[tableName] = append(targetTables[tableName], engineID)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return targetTables, err
}

func (g GlueCheckpointsDB) IgnoreErrorCheckpoint(ctx context.Context, tableName string) error {
	logger := log.With(zap.String("table", tableName))
	se, err := g.getSessionFunc()
	if err != nil {
		return errors.Trace(err)
	}
	defer se.Close()

	var colName string
	if tableName == allTables {
		// This will expand to `WHERE 'all' = 'all'` and effectively allowing
		// all tables to be included.
		colName = stringLitAll
	} else {
		colName = columnTableName
	}

	tableName = common.InterpolateMySQLString(tableName)

	engineQuery := fmt.Sprintf(`
		UPDATE %s.%s SET status = %d WHERE %s = %s AND status <= %d;
	`, g.schema, CheckpointTableNameEngine, CheckpointStatusLoaded, colName, tableName, CheckpointStatusMaxInvalid)
	tableQuery := fmt.Sprintf(`
		UPDATE %s.%s SET status = %d WHERE %s = %s AND status <= %d;
	`, g.schema, CheckpointTableNameTable, CheckpointStatusLoaded, colName, tableName, CheckpointStatusMaxInvalid)
	return errors.Trace(Transact(ctx, "ignore error checkpoints", se, logger, func(c context.Context, s Session) error {
		if _, e := s.Execute(c, engineQuery); e != nil {
			return e
		}
		if _, e := s.Execute(c, tableQuery); e != nil {
			return e
		}
		return nil
	}))
}

func (g GlueCheckpointsDB) DestroyErrorCheckpoint(ctx context.Context, tableName string) ([]DestroyedTableCheckpoint, error) {
	logger := log.With(zap.String("table", tableName))
	se, err := g.getSessionFunc()
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer se.Close()

	var colName, aliasedColName string

	if tableName == allTables {
		// These will expand to `WHERE 'all' = 'all'` and effectively allowing
		// all tables to be included.
		colName = stringLitAll
		aliasedColName = stringLitAll
	} else {
		colName = columnTableName
		aliasedColName = "t.table_name"
	}

	tableName = common.InterpolateMySQLString(tableName)

	selectQuery := fmt.Sprintf(`
		SELECT
			t.table_name,
			COALESCE(MIN(e.engine_id), 0),
			COALESCE(MAX(e.engine_id), -1)
		FROM %[1]s.%[4]s t
		LEFT JOIN %[1]s.%[5]s e ON t.table_name = e.table_name
		WHERE %[2]s = %[6]s AND t.status <= %[3]d
		GROUP BY t.table_name;
	`, g.schema, aliasedColName, CheckpointStatusMaxInvalid, CheckpointTableNameTable, CheckpointTableNameEngine, tableName)
	deleteChunkQuery := fmt.Sprintf(`
		DELETE FROM %[1]s.%[4]s WHERE table_name IN (SELECT table_name FROM %[1]s.%[5]s WHERE %[2]s = %[6]s AND status <= %[3]d)
	`, g.schema, colName, CheckpointStatusMaxInvalid, CheckpointTableNameChunk, CheckpointTableNameTable, tableName)
	deleteEngineQuery := fmt.Sprintf(`
		DELETE FROM %[1]s.%[4]s WHERE table_name IN (SELECT table_name FROM %[1]s.%[5]s WHERE %[2]s = %[6]s AND status <= %[3]d)
	`, g.schema, colName, CheckpointStatusMaxInvalid, CheckpointTableNameEngine, CheckpointTableNameTable, tableName)
	deleteTableQuery := fmt.Sprintf(`
		DELETE FROM %s.%s WHERE %s = %s AND status <= %d
	`, g.schema, CheckpointTableNameTable, colName, tableName, CheckpointStatusMaxInvalid)

	var targetTables []DestroyedTableCheckpoint
	err = Transact(ctx, "destroy error checkpoints", se, logger, func(c context.Context, s Session) error {
		// clean because it's in a retry
		targetTables = nil
		rs, err := s.Execute(c, selectQuery)
		if err != nil {
			return errors.Trace(err)
		}
		r := rs[0]
		req := r.NewChunk(nil)
		it := chunk.NewIterator4Chunk(req)
		for {
			err = r.Next(ctx, req)
			if err != nil {
				r.Close()
				return err
			}
			if req.NumRows() == 0 {
				break
			}

			for row := it.Begin(); row != it.End(); row = it.Next() {
				var dtc DestroyedTableCheckpoint
				dtc.TableName = row.GetString(0)
				dtc.MinEngineID = int32(row.GetInt64(1))
				dtc.MaxEngineID = int32(row.GetInt64(2))
				targetTables = append(targetTables, dtc)
			}
		}
		r.Close()

		if _, e := s.Execute(c, deleteChunkQuery); e != nil {
			return errors.Trace(e)
		}
		if _, e := s.Execute(c, deleteEngineQuery); e != nil {
			return errors.Trace(e)
		}
		if _, e := s.Execute(c, deleteTableQuery); e != nil {
			return errors.Trace(e)
		}
		return nil
	})

	if err != nil {
		return nil, errors.Trace(err)
	}

	return targetTables, nil
}

func (g GlueCheckpointsDB) DumpTables(ctx context.Context, csv io.Writer) error {
	return errors.Errorf("dumping glue checkpoint into CSV not unsupported")
}

func (g GlueCheckpointsDB) DumpEngines(ctx context.Context, csv io.Writer) error {
	return errors.Errorf("dumping glue checkpoint into CSV not unsupported")
}

func (g GlueCheckpointsDB) DumpChunks(ctx context.Context, csv io.Writer) error {
	return errors.Errorf("dumping glue checkpoint into CSV not unsupported")
}

func Transact(ctx context.Context, purpose string, s Session, logger log.Logger, action func(context.Context, Session) error) error {
	return common.Retry(purpose, logger, func() error {
		_, err := s.Execute(ctx, "BEGIN")
		if err != nil {
			return errors.Annotate(err, "begin transaction failed")
		}
		err = action(ctx, s)
		if err != nil {
			s.RollbackTxn(ctx)
			return err
		}
		err = s.CommitTxn(ctx)
		if err != nil {
			return errors.Annotate(err, "commit transaction failed")
		}
		return nil
	})
}

// TODO: will use drainFirstRecordSet to reduce repeat in GlueCheckpointsDB later
func drainFirstRecordSet(ctx context.Context, rss []sqlexec.RecordSet) ([]chunk.Row, error) {
	if len(rss) != 1 {
		return nil, errors.New("given result set doesn't have length 1")
	}
	rs := rss[0]
	var rows []chunk.Row
	req := rs.NewChunk(nil)
	for {
		err := rs.Next(ctx, req)
		if err != nil || req.NumRows() == 0 {
			rs.Close()
			return rows, err
		}
		iter := chunk.NewIterator4Chunk(req)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			rows = append(rows, r)
		}
		req = chunk.Renew(req, 1024)
	}
}
