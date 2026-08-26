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

package server

import (
	"context"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	parsertypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

var (
	errDiagnosticPageFull    = errors.New("diagnostic page has enough lookahead records")
	errDiagnosticCursorState = errors.New("diagnostic cursor no longer identifies its source record")
)

type diagnosticPositionedRecord struct {
	record any
	cursor diagnosticCursor
}

type diagnosticPageBuilder struct {
	pageSize int
	items    []diagnosticPositionedRecord
}

func newDiagnosticPageBuilder(pageSize int) *diagnosticPageBuilder {
	return &diagnosticPageBuilder{
		pageSize: pageSize,
		items:    make([]diagnosticPositionedRecord, 0, pageSize+1),
	}
}

func (b *diagnosticPageBuilder) add(record any, cursor diagnosticCursor) error {
	b.items = append(b.items, diagnosticPositionedRecord{record: record, cursor: cursor})
	if len(b.items) > b.pageSize {
		return errDiagnosticPageFull
	}
	return nil
}

func (b *diagnosticPageBuilder) finish(base diagnosticCursor, walkErr error) ([]any, diagnosticCursor, bool, error) {
	hasMore := errors.ErrorEqual(walkErr, errDiagnosticPageFull)
	if walkErr != nil && !hasMore {
		return nil, base, false, errors.Trace(walkErr)
	}
	itemCount := len(b.items)
	if hasMore {
		itemCount = b.pageSize
	}
	records := make([]any, itemCount)
	for i := 0; i < itemCount; i++ {
		records[i] = b.items[i].record
	}
	if !hasMore {
		return records, base, true, nil
	}
	return records, b.items[itemCount-1].cursor, false, nil
}

type diagnosticTableRecord struct {
	SchemaID       int64  `json:"schema_id"`
	SchemaName     string `json:"schema_name"`
	TableID        int64  `json:"table_id"`
	TableName      string `json:"table_name"`
	TableKind      string `json:"table_kind"`
	State          string `json:"state"`
	Charset        string `json:"charset"`
	Collation      string `json:"collation"`
	PKIsHandle     bool   `json:"pk_is_handle"`
	IsCommonHandle bool   `json:"is_common_handle"`
	ShardRowIDBits uint64 `json:"shard_row_id_bits"`
	AutoRandomBits uint64 `json:"auto_random_bits"`
	Partitioned    bool   `json:"partitioned"`
	UpdateTS       uint64 `json:"update_ts"`
}

type diagnosticColumnRecord struct {
	SchemaID        int64  `json:"schema_id"`
	SchemaName      string `json:"schema_name"`
	TableID         int64  `json:"table_id"`
	TableName       string `json:"table_name"`
	ColumnID        int64  `json:"column_id"`
	ColumnName      string `json:"column_name"`
	Ordinal         int    `json:"ordinal"`
	TypeCode        byte   `json:"type_code"`
	TypeName        string `json:"type_name"`
	Flag            uint   `json:"flag"`
	Length          int    `json:"length"`
	Decimal         int    `json:"decimal"`
	Charset         string `json:"charset"`
	Collation       string `json:"collation"`
	State           string `json:"state"`
	Hidden          bool   `json:"hidden"`
	Generated       bool   `json:"generated"`
	GeneratedStored bool   `json:"generated_stored"`
}

type diagnosticIndexColumn struct {
	ColumnID     int64  `json:"column_id"`
	ColumnName   string `json:"column_name"`
	PrefixLength int    `json:"prefix_length"`
}

type diagnosticIndexRecord struct {
	SchemaID    int64                   `json:"schema_id"`
	SchemaName  string                  `json:"schema_name"`
	TableID     int64                   `json:"table_id"`
	TableName   string                  `json:"table_name"`
	IndexID     int64                   `json:"index_id"`
	IndexName   string                  `json:"index_name"`
	State       string                  `json:"state"`
	IndexType   string                  `json:"index_type"`
	Unique      bool                    `json:"unique"`
	Primary     bool                    `json:"primary"`
	Invisible   bool                    `json:"invisible"`
	Global      bool                    `json:"global"`
	MultiValued bool                    `json:"multi_valued"`
	Columns     []diagnosticIndexColumn `json:"columns"`
}

type diagnosticPartitionRecord struct {
	SchemaID      int64  `json:"schema_id"`
	SchemaName    string `json:"schema_name"`
	TableID       int64  `json:"table_id"`
	TableName     string `json:"table_name"`
	PartitionID   int64  `json:"partition_id"`
	PartitionName string `json:"partition_name"`
	Ordinal       int    `json:"ordinal"`
	PartitionType string `json:"partition_type"`
}

type diagnosticBindingRecord struct {
	SQLDigest  string `json:"sql_digest"`
	PlanDigest string `json:"plan_digest"`
	Status     string `json:"status"`
	Source     string `json:"source"`
	CreateTime string `json:"create_time"`
	UpdateTime string `json:"update_time"`
}

type diagnosticStatsHealthRecord struct {
	TableID               int64    `json:"table_id"`
	Version               uint64   `json:"version"`
	ModifyCount           int64    `json:"modify_count"`
	RowCount              uint64   `json:"row_count"`
	Snapshot              uint64   `json:"snapshot"`
	LastHistogramsVersion uint64   `json:"last_histograms_version"`
	ModifyRatio           *float64 `json:"modify_ratio"`
}

func collectDiagnosticTables(
	ctx context.Context,
	reader meta.Reader,
	redactor *diagnosticRedactor,
	cursor diagnosticCursor,
	pageSize int,
) ([]any, diagnosticCursor, bool, error) {
	if cursor.SubID != 0 || cursor.RowID != 0 {
		return nil, cursor, false, errDiagnosticCursorState
	}
	builder := newDiagnosticPageBuilder(pageSize)
	err := walkDiagnosticTables(ctx, reader, cursor, false, func(db *model.DBInfo, table *model.TableInfo, _ int64) error {
		position := cursor
		position.DBID = db.ID
		position.TableID = table.ID
		position.SubID = 0
		record := diagnosticTableRecord{
			SchemaID:       db.ID,
			SchemaName:     redactor.identifier("schema", db.Name.O, db.ID),
			TableID:        table.ID,
			TableName:      redactor.identifier("table", table.Name.O, db.ID, table.ID),
			TableKind:      diagnosticTableKind(table),
			State:          table.State.String(),
			Charset:        table.Charset,
			Collation:      table.Collate,
			PKIsHandle:     table.PKIsHandle,
			IsCommonHandle: table.IsCommonHandle,
			ShardRowIDBits: table.ShardRowIDBits,
			AutoRandomBits: table.AutoRandomBits,
			Partitioned:    table.Partition != nil && table.Partition.Enable,
			UpdateTS:       table.UpdateTS,
		}
		return builder.add(record, position)
	})
	return builder.finish(cursor, err)
}

func collectDiagnosticColumns(
	ctx context.Context,
	reader meta.Reader,
	redactor *diagnosticRedactor,
	cursor diagnosticCursor,
	pageSize int,
) ([]any, diagnosticCursor, bool, error) {
	if cursor.RowID != 0 {
		return nil, cursor, false, errDiagnosticCursorState
	}
	builder := newDiagnosticPageBuilder(pageSize)
	err := walkDiagnosticTables(ctx, reader, cursor, true, func(db *model.DBInfo, table *model.TableInfo, startColumnID int64) error {
		start, err := diagnosticSubObjectStart(table.Columns, startColumnID, func(column *model.ColumnInfo) int64 {
			return column.ID
		})
		if err != nil {
			return err
		}
		schemaName := redactor.identifier("schema", db.Name.O, db.ID)
		tableName := redactor.identifier("table", table.Name.O, db.ID, table.ID)
		for ordinal := start; ordinal < len(table.Columns); ordinal++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			column := table.Columns[ordinal]
			position := cursor
			position.DBID = db.ID
			position.TableID = table.ID
			position.SubID = column.ID
			record := diagnosticColumnRecord{
				SchemaID:        db.ID,
				SchemaName:      schemaName,
				TableID:         table.ID,
				TableName:       tableName,
				ColumnID:        column.ID,
				ColumnName:      redactor.identifier("column", column.Name.O, table.ID, column.ID),
				Ordinal:         ordinal + 1,
				TypeCode:        column.GetType(),
				TypeName:        parsertypes.TypeToStr(column.GetType(), column.GetCharset()),
				Flag:            column.GetFlag(),
				Length:          column.GetFlen(),
				Decimal:         column.GetDecimal(),
				Charset:         column.GetCharset(),
				Collation:       column.GetCollate(),
				State:           column.State.String(),
				Hidden:          column.Hidden,
				Generated:       column.GeneratedExprString != "",
				GeneratedStored: column.GeneratedStored,
			}
			if err := builder.add(record, position); err != nil {
				return err
			}
		}
		return nil
	})
	return builder.finish(cursor, err)
}

func collectDiagnosticIndexes(
	ctx context.Context,
	reader meta.Reader,
	redactor *diagnosticRedactor,
	cursor diagnosticCursor,
	pageSize int,
) ([]any, diagnosticCursor, bool, error) {
	if cursor.RowID != 0 {
		return nil, cursor, false, errDiagnosticCursorState
	}
	builder := newDiagnosticPageBuilder(pageSize)
	err := walkDiagnosticTables(ctx, reader, cursor, true, func(db *model.DBInfo, table *model.TableInfo, startIndexID int64) error {
		start, err := diagnosticSubObjectStart(table.Indices, startIndexID, func(index *model.IndexInfo) int64 {
			return index.ID
		})
		if err != nil {
			return err
		}
		schemaName := redactor.identifier("schema", db.Name.O, db.ID)
		tableName := redactor.identifier("table", table.Name.O, db.ID, table.ID)
		for i := start; i < len(table.Indices); i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			index := table.Indices[i]
			columns := make([]diagnosticIndexColumn, 0, len(index.Columns))
			for _, indexColumn := range index.Columns {
				columnID := int64(0)
				if indexColumn.Offset >= 0 && indexColumn.Offset < len(table.Columns) {
					columnID = table.Columns[indexColumn.Offset].ID
				}
				columns = append(columns, diagnosticIndexColumn{
					ColumnID:     columnID,
					ColumnName:   redactor.identifier("column", indexColumn.Name.O, table.ID, columnID),
					PrefixLength: indexColumn.Length,
				})
			}
			position := cursor
			position.DBID = db.ID
			position.TableID = table.ID
			position.SubID = index.ID
			record := diagnosticIndexRecord{
				SchemaID:    db.ID,
				SchemaName:  schemaName,
				TableID:     table.ID,
				TableName:   tableName,
				IndexID:     index.ID,
				IndexName:   redactor.identifier("index", index.Name.O, table.ID, index.ID),
				State:       index.State.String(),
				IndexType:   index.Tp.String(),
				Unique:      index.Unique,
				Primary:     index.Primary,
				Invisible:   index.Invisible,
				Global:      index.Global,
				MultiValued: index.MVIndex,
				Columns:     columns,
			}
			if err := builder.add(record, position); err != nil {
				return err
			}
		}
		return nil
	})
	return builder.finish(cursor, err)
}

func collectDiagnosticPartitions(
	ctx context.Context,
	reader meta.Reader,
	redactor *diagnosticRedactor,
	cursor diagnosticCursor,
	pageSize int,
) ([]any, diagnosticCursor, bool, error) {
	if cursor.RowID != 0 {
		return nil, cursor, false, errDiagnosticCursorState
	}
	builder := newDiagnosticPageBuilder(pageSize)
	err := walkDiagnosticTables(ctx, reader, cursor, true, func(db *model.DBInfo, table *model.TableInfo, startPartitionID int64) error {
		if table.Partition == nil || !table.Partition.Enable {
			if startPartitionID != 0 {
				return errDiagnosticCursorState
			}
			return nil
		}
		definitions := table.Partition.Definitions
		start, err := diagnosticSubObjectStart(definitions, startPartitionID, func(definition model.PartitionDefinition) int64 {
			return definition.ID
		})
		if err != nil {
			return err
		}
		schemaName := redactor.identifier("schema", db.Name.O, db.ID)
		tableName := redactor.identifier("table", table.Name.O, db.ID, table.ID)
		for i := start; i < len(definitions); i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			definition := definitions[i]
			position := cursor
			position.DBID = db.ID
			position.TableID = table.ID
			position.SubID = definition.ID
			record := diagnosticPartitionRecord{
				SchemaID:      db.ID,
				SchemaName:    schemaName,
				TableID:       table.ID,
				TableName:     tableName,
				PartitionID:   definition.ID,
				PartitionName: redactor.identifier("partition", definition.Name.O, table.ID, definition.ID),
				Ordinal:       i + 1,
				PartitionType: table.Partition.Type.String(),
			}
			if err := builder.add(record, position); err != nil {
				return err
			}
		}
		return nil
	})
	return builder.finish(cursor, err)
}

func (h *diagnosticAPIHandler) collectDiagnosticBindings(
	ctx context.Context,
	cursor diagnosticCursor,
	pageSize int,
) ([]any, diagnosticCursor, bool, error) {
	if cursor.DBID != 0 || cursor.TableID != 0 || cursor.SubID != 0 {
		return nil, cursor, false, errDiagnosticCursorState
	}
	startRowID := cursor.RowID
	if startRowID == 0 {
		startRowID = math.MinInt64
	}
	pool := h.dom.AdvancedSysSessionPool()
	session, err := pool.Get()
	if err != nil {
		return nil, cursor, false, errors.Trace(err)
	}
	defer pool.Put(session)
	var sessionLocation *time.Location
	if err := session.WithSessionContext(func(sctx sessionctx.Context) error {
		sessionLocation = sctx.GetSessionVars().Location()
		return nil
	}); err != nil {
		return nil, cursor, false, errors.Trace(err)
	}
	rows, _, err := session.ExecRestrictedSQL(
		ctx,
		[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionWithSnapshot(cursor.SnapshotTS), sqlexec.ExecOptionUseCurSession},
		`SELECT _tidb_rowid, COALESCE(sql_digest, ''), COALESCE(plan_digest, ''), status, source, create_time, update_time
		 FROM mysql.bind_info
		 WHERE _tidb_rowid > %? AND source <> 'builtin'
		 ORDER BY _tidb_rowid
		 LIMIT %?`,
		startRowID,
		pageSize+1,
	)
	if err != nil {
		return nil, cursor, false, errors.Trace(err)
	}
	hasMore := len(rows) > pageSize
	if hasMore {
		rows = rows[:pageSize]
	}
	records := make([]any, len(rows))
	next := cursor
	for i, row := range rows {
		createTime, err := diagnosticTimeRFC3339(row.GetTime(5), sessionLocation)
		if err != nil {
			return nil, cursor, false, errors.Trace(err)
		}
		updateTime, err := diagnosticTimeRFC3339(row.GetTime(6), sessionLocation)
		if err != nil {
			return nil, cursor, false, errors.Trace(err)
		}
		next.RowID = row.GetInt64(0)
		records[i] = diagnosticBindingRecord{
			SQLDigest:  row.GetString(1),
			PlanDigest: row.GetString(2),
			Status:     row.GetString(3),
			Source:     row.GetString(4),
			CreateTime: createTime,
			UpdateTime: updateTime,
		}
	}
	return records, next, !hasMore, nil
}

func diagnosticTimeRFC3339(value types.Time, location *time.Location) (string, error) {
	goTime, err := value.GoTime(location)
	if err != nil {
		return "", errors.Trace(err)
	}
	return goTime.UTC().Format(time.RFC3339Nano), nil
}

func (h *diagnosticAPIHandler) collectDiagnosticStatsHealth(
	ctx context.Context,
	cursor diagnosticCursor,
	pageSize int,
) ([]any, diagnosticCursor, bool, error) {
	if cursor.DBID != 0 || cursor.SubID != 0 || cursor.RowID != 0 {
		return nil, cursor, false, errDiagnosticCursorState
	}
	startTableID := cursor.TableID
	if startTableID == 0 {
		startTableID = math.MinInt64
	}
	pool := h.dom.AdvancedSysSessionPool()
	session, err := pool.Get()
	if err != nil {
		return nil, cursor, false, errors.Trace(err)
	}
	defer pool.Put(session)
	rows, _, err := session.ExecRestrictedSQL(
		ctx,
		[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionWithSnapshot(cursor.SnapshotTS), sqlexec.ExecOptionUseCurSession},
		`SELECT table_id, version, modify_count, count, snapshot, COALESCE(last_stats_histograms_version, 0)
		 FROM mysql.stats_meta USE INDEX(PRIMARY)
		 WHERE table_id > %?
		 ORDER BY table_id
		 LIMIT %?`,
		startTableID,
		pageSize+1,
	)
	if err != nil {
		return nil, cursor, false, errors.Trace(err)
	}
	hasMore := len(rows) > pageSize
	if hasMore {
		rows = rows[:pageSize]
	}
	records := make([]any, len(rows))
	next := cursor
	for i, row := range rows {
		tableID := row.GetInt64(0)
		rowCount := row.GetUint64(3)
		modifyCount := row.GetInt64(2)
		var modifyRatio *float64
		if rowCount > 0 {
			ratio := float64(modifyCount) / float64(rowCount)
			modifyRatio = &ratio
		}
		next.TableID = tableID
		records[i] = diagnosticStatsHealthRecord{
			TableID:               tableID,
			Version:               row.GetUint64(1),
			ModifyCount:           modifyCount,
			RowCount:              rowCount,
			Snapshot:              row.GetUint64(4),
			LastHistogramsVersion: row.GetUint64(5),
			ModifyRatio:           modifyRatio,
		}
	}
	return records, next, !hasMore, nil
}

func walkDiagnosticTables(
	ctx context.Context,
	reader meta.Reader,
	cursor diagnosticCursor,
	revisitCursorTable bool,
	visit func(db *model.DBInfo, table *model.TableInfo, startSubID int64) error,
) error {
	processDB := func(db *model.DBInfo, startTableID int64) error {
		if isExcludedDiagnosticDatabase(db) {
			return nil
		}
		return reader.IterTablesFrom(db.ID, startTableID, func(table *model.TableInfo) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return visit(db, table, 0)
		})
	}

	if cursor.DBID == 0 {
		if cursor.TableID != 0 || cursor.SubID != 0 {
			return errDiagnosticCursorState
		}
		return reader.IterDatabases(func(db *model.DBInfo) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return processDB(db, 0)
		})
	}
	if cursor.TableID == 0 {
		return errDiagnosticCursorState
	}
	db, err := reader.GetDatabase(cursor.DBID)
	if err != nil {
		return errors.Trace(err)
	}
	if db == nil || isExcludedDiagnosticDatabase(db) {
		return errDiagnosticCursorState
	}
	if revisitCursorTable {
		table, err := reader.GetTable(cursor.DBID, cursor.TableID)
		if err != nil {
			return errors.Trace(err)
		}
		if table == nil {
			return errDiagnosticCursorState
		}
		if err := visit(db, table, cursor.SubID); err != nil {
			return err
		}
	}
	if err := processDB(db, cursor.TableID); err != nil {
		return err
	}
	return reader.IterDatabasesFrom(cursor.DBID, func(nextDB *model.DBInfo) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return processDB(nextDB, 0)
	})
}

func isExcludedDiagnosticDatabase(db *model.DBInfo) bool {
	return metadef.IsMemOrSysDB(db.Name.L) || metadef.IsBRRelatedDB(db.Name.O)
}

func diagnosticTableKind(table *model.TableInfo) string {
	switch {
	case table.View != nil:
		return "view"
	case table.Sequence != nil:
		return "sequence"
	default:
		return "base"
	}
}

func diagnosticSubObjectStart[T any](items []T, exclusiveStartID int64, id func(T) int64) (int, error) {
	if exclusiveStartID == 0 {
		return 0, nil
	}
	for i, item := range items {
		if id(item) == exclusiveStartID {
			return i + 1, nil
		}
	}
	return 0, errDiagnosticCursorState
}
