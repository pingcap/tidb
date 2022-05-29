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

package tidb

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/br/pkg/redact"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var extraHandleTableColumn = &table.Column{
	ColumnInfo:    kv.ExtraHandleColumnInfo,
	GeneratedExpr: nil,
	DefaultExpr:   nil,
}

const (
	writeRowsMaxRetryTimes = 3
)

type tidbRow struct {
	insertStmt string
	path       string
	offset     int64
}

var emptyTiDBRow = tidbRow{
	insertStmt: "",
	path:       "",
	offset:     0,
}

type tidbRows []tidbRow

// MarshalLogArray implements the zapcore.ArrayMarshaler interface
func (rows tidbRows) MarshalLogArray(encoder zapcore.ArrayEncoder) error {
	for _, r := range rows {
		encoder.AppendString(redact.String(r.insertStmt))
	}
	return nil
}

type tidbEncoder struct {
	mode mysql.SQLMode
	tbl  table.Table
	se   sessionctx.Context
	// the index of table columns for each data field.
	// index == len(table.columns) means this field is `_tidb_rowid`
	columnIdx []int
	// the max index used in this chunk, due to the ignore-columns config, we can't
	// directly check the total column count, so we fall back to only check that
	// the there are enough columns.
	columnCnt int
}

type tidbBackend struct {
	db          *sql.DB
	onDuplicate string
	errorMgr    *errormanager.ErrorManager
}

// NewTiDBBackend creates a new TiDB backend using the given database.
//
// The backend does not take ownership of `db`. Caller should close `db`
// manually after the backend expired.
func NewTiDBBackend(db *sql.DB, onDuplicate string, errorMgr *errormanager.ErrorManager) backend.Backend {
	switch onDuplicate {
	case config.ReplaceOnDup, config.IgnoreOnDup, config.ErrorOnDup:
	default:
		log.L().Warn("unsupported action on duplicate, overwrite with `replace`")
		onDuplicate = config.ReplaceOnDup
	}
	return backend.MakeBackend(&tidbBackend{db: db, onDuplicate: onDuplicate, errorMgr: errorMgr})
}

func (row tidbRow) Size() uint64 {
	return uint64(len(row.insertStmt))
}

func (row tidbRow) String() string {
	return row.insertStmt
}

func (row tidbRow) ClassifyAndAppend(data *kv.Rows, checksum *verification.KVChecksum, _ *kv.Rows, _ *verification.KVChecksum) {
	rows := (*data).(tidbRows)
	// Cannot do `rows := data.(*tidbRows); *rows = append(*rows, row)`.
	//nolint:gocritic
	*data = append(rows, row)
	cs := verification.MakeKVChecksum(row.Size(), 1, 0)
	checksum.Add(&cs)
}

func (rows tidbRows) SplitIntoChunks(splitSizeInt int) []kv.Rows {
	if len(rows) == 0 {
		return nil
	}

	res := make([]kv.Rows, 0, 1)
	i := 0
	cumSize := uint64(0)
	splitSize := uint64(splitSizeInt)

	for j, row := range rows {
		if i < j && cumSize+row.Size() > splitSize {
			res = append(res, rows[i:j])
			i = j
			cumSize = 0
		}
		cumSize += row.Size()
	}

	return append(res, rows[i:])
}

func (rows tidbRows) Clear() kv.Rows {
	return rows[:0]
}

func (enc *tidbEncoder) appendSQLBytes(sb *strings.Builder, value []byte) {
	sb.Grow(2 + len(value))
	sb.WriteByte('\'')
	if enc.mode.HasNoBackslashEscapesMode() {
		for _, b := range value {
			if b == '\'' {
				sb.WriteString(`''`)
			} else {
				sb.WriteByte(b)
			}
		}
	} else {
		for _, b := range value {
			switch b {
			case 0:
				sb.WriteString(`\0`)
			case '\b':
				sb.WriteString(`\b`)
			case '\n':
				sb.WriteString(`\n`)
			case '\r':
				sb.WriteString(`\r`)
			case '\t':
				sb.WriteString(`\t`)
			case 26:
				sb.WriteString(`\Z`)
			case '\'':
				sb.WriteString(`''`)
			case '\\':
				sb.WriteString(`\\`)
			default:
				sb.WriteByte(b)
			}
		}
	}
	sb.WriteByte('\'')
}

// appendSQL appends the SQL representation of the Datum into the string builder.
// Note that we cannot use Datum.ToString since it doesn't perform SQL escaping.
func (enc *tidbEncoder) appendSQL(sb *strings.Builder, datum *types.Datum, _ *table.Column) error {
	switch datum.Kind() {
	case types.KindNull:
		sb.WriteString("NULL")

	case types.KindMinNotNull:
		sb.WriteString("MINVALUE")

	case types.KindMaxValue:
		sb.WriteString("MAXVALUE")

	case types.KindInt64:
		// longest int64 = -9223372036854775808 which has 20 characters
		var buffer [20]byte
		value := strconv.AppendInt(buffer[:0], datum.GetInt64(), 10)
		sb.Write(value)

	case types.KindUint64, types.KindMysqlEnum, types.KindMysqlSet:
		// longest uint64 = 18446744073709551615 which has 20 characters
		var buffer [20]byte
		value := strconv.AppendUint(buffer[:0], datum.GetUint64(), 10)
		sb.Write(value)

	case types.KindFloat32, types.KindFloat64:
		// float64 has 16 digits of precision, so a buffer size of 32 is more than enough...
		var buffer [32]byte
		value := strconv.AppendFloat(buffer[:0], datum.GetFloat64(), 'g', -1, 64)
		sb.Write(value)
	case types.KindString:
		// See: https://github.com/pingcap/tidb-lightning/issues/550
		// if enc.mode.HasStrictMode() {
		//	d, err := table.CastValue(enc.se, *datum, col.ToInfo(), false, false)
		//	if err != nil {
		//		return errors.Trace(err)
		//	}
		//	datum = &d
		// }

		enc.appendSQLBytes(sb, datum.GetBytes())
	case types.KindBytes:
		enc.appendSQLBytes(sb, datum.GetBytes())

	case types.KindMysqlJSON:
		value, err := datum.GetMysqlJSON().MarshalJSON()
		if err != nil {
			return err
		}
		enc.appendSQLBytes(sb, value)

	case types.KindBinaryLiteral:
		value := datum.GetBinaryLiteral()
		sb.Grow(3 + 2*len(value))
		sb.WriteString("x'")
		if _, err := hex.NewEncoder(sb).Write(value); err != nil {
			return errors.Trace(err)
		}
		sb.WriteByte('\'')

	case types.KindMysqlBit:
		var buffer [20]byte
		intValue, err := datum.GetBinaryLiteral().ToInt(nil)
		if err != nil {
			return err
		}
		value := strconv.AppendUint(buffer[:0], intValue, 10)
		sb.Write(value)

		// time, duration, decimal
	default:
		value, err := datum.ToString()
		if err != nil {
			return err
		}
		sb.WriteByte('\'')
		sb.WriteString(value)
		sb.WriteByte('\'')
	}

	return nil
}

func (*tidbEncoder) Close() {}

func getColumnByIndex(cols []*table.Column, index int) *table.Column {
	if index == len(cols) {
		return extraHandleTableColumn
	}
	return cols[index]
}

func (enc *tidbEncoder) Encode(logger log.Logger, row []types.Datum, _ int64, columnPermutation []int, path string, offset int64) (kv.Row, error) {
	cols := enc.tbl.Cols()

	if len(enc.columnIdx) == 0 {
		columnMaxIdx := -1
		columnIdx := make([]int, len(columnPermutation))
		for i := 0; i < len(columnPermutation); i++ {
			columnIdx[i] = -1
		}
		for i, idx := range columnPermutation {
			if idx >= 0 {
				columnIdx[idx] = i
				if idx > columnMaxIdx {
					columnMaxIdx = idx
				}
			}
		}
		enc.columnIdx = columnIdx
		enc.columnCnt = columnMaxIdx + 1
	}

	// TODO: since the column count doesn't exactly reflect the real column names, we only check the upper bound currently.
	// See: tests/generated_columns/data/gencol.various_types.0.sql this sql has no columns, so encodeLoop will fill the
	// column permutation with default, thus enc.columnCnt > len(row).
	if len(row) < enc.columnCnt {
		// 1. if len(row) < enc.columnCnt: data in row cannot populate the insert statement, because
		// there are enc.columnCnt elements to insert but fewer columns in row
		logger.Error("column count mismatch", zap.Ints("column_permutation", columnPermutation),
			zap.Array("data", kv.RowArrayMarshaler(row)))
		return emptyTiDBRow, errors.Errorf("column count mismatch, expected %d, got %d", enc.columnCnt, len(row))
	}

	if len(row) > len(enc.columnIdx) {
		// 2. if len(row) > len(columnIdx): raw row data has more columns than those
		// in the table
		logger.Error("column count mismatch", zap.Ints("column_count", enc.columnIdx),
			zap.Array("data", kv.RowArrayMarshaler(row)))
		return emptyTiDBRow, errors.Errorf("column count mismatch, at most %d but got %d", len(enc.columnIdx), len(row))
	}

	var encoded strings.Builder
	encoded.Grow(8 * len(row))
	encoded.WriteByte('(')
	cnt := 0
	for i, field := range row {
		if enc.columnIdx[i] < 0 {
			continue
		}
		if cnt > 0 {
			encoded.WriteByte(',')
		}
		datum := field
		if err := enc.appendSQL(&encoded, &datum, getColumnByIndex(cols, enc.columnIdx[i])); err != nil {
			logger.Error("tidb encode failed",
				zap.Array("original", kv.RowArrayMarshaler(row)),
				zap.Int("originalCol", i),
				log.ShortError(err),
			)
			return nil, err
		}
		cnt++
	}
	encoded.WriteByte(')')
	return tidbRow{
		insertStmt: encoded.String(),
		path:       path,
		offset:     offset,
	}, nil
}

// EncodeRowForRecord encodes a row to a string compatible with INSERT statements.
func EncodeRowForRecord(encTable table.Table, sqlMode mysql.SQLMode, row []types.Datum, columnPermutation []int) string {
	enc := tidbEncoder{
		tbl:  encTable,
		mode: sqlMode,
	}
	resRow, err := enc.Encode(log.L(), row, 0, columnPermutation, "", 0)
	if err != nil {
		// if encode can't succeed, fallback to record the raw input strings
		// ignore the error since it can only happen if the datum type is unknown, this can't happen here.
		datumStr, _ := types.DatumsToString(row, true)
		return datumStr
	}
	return resRow.(tidbRow).insertStmt
}

func (be *tidbBackend) Close() {
	// *Not* going to close `be.db`. The db object is normally borrowed from a
	// TidbManager, so we let the manager to close it.
}

func (be *tidbBackend) MakeEmptyRows() kv.Rows {
	return tidbRows(nil)
}

func (be *tidbBackend) RetryImportDelay() time.Duration {
	return 0
}

func (be *tidbBackend) MaxChunkSize() int {
	failpoint.Inject("FailIfImportedSomeRows", func() {
		failpoint.Return(1)
	})
	return 1048576
}

func (be *tidbBackend) ShouldPostProcess() bool {
	return true
}

func (be *tidbBackend) CheckRequirements(ctx context.Context, _ *backend.CheckCtx) error {
	log.L().Info("skipping check requirements for tidb backend")
	return nil
}

func (be *tidbBackend) NewEncoder(tbl table.Table, options *kv.SessionOptions) (kv.Encoder, error) {
	se := kv.NewSession(options)
	if options.SQLMode.HasStrictMode() {
		se.GetSessionVars().SkipUTF8Check = false
		se.GetSessionVars().SkipASCIICheck = false
	}

	return &tidbEncoder{mode: options.SQLMode, tbl: tbl, se: se}, nil
}

func (be *tidbBackend) OpenEngine(context.Context, *backend.EngineConfig, uuid.UUID) error {
	return nil
}

func (be *tidbBackend) CloseEngine(context.Context, *backend.EngineConfig, uuid.UUID) error {
	return nil
}

func (be *tidbBackend) CleanupEngine(context.Context, uuid.UUID) error {
	return nil
}

func (be *tidbBackend) CollectLocalDuplicateRows(ctx context.Context, tbl table.Table, tableName string, opts *kv.SessionOptions) (bool, error) {
	panic("Unsupported Operation")
}

func (be *tidbBackend) CollectRemoteDuplicateRows(ctx context.Context, tbl table.Table, tableName string, opts *kv.SessionOptions) (bool, error) {
	panic("Unsupported Operation")
}

func (be *tidbBackend) ResolveDuplicateRows(ctx context.Context, tbl table.Table, tableName string, algorithm config.DuplicateResolutionAlgorithm) error {
	return nil
}

func (be *tidbBackend) ImportEngine(context.Context, uuid.UUID, int64, int64) error {
	return nil
}

func (be *tidbBackend) WriteRows(ctx context.Context, tableName string, columnNames []string, rows kv.Rows) error {
	var err error
rowLoop:
	for _, r := range rows.SplitIntoChunks(be.MaxChunkSize()) {
		for i := 0; i < writeRowsMaxRetryTimes; i++ {
			// Write in the batch mode first.
			err = be.WriteBatchRowsToDB(ctx, tableName, columnNames, r)
			switch {
			case err == nil:
				continue rowLoop
			case common.IsRetryableError(err):
				// retry next loop
			case be.errorMgr.TypeErrorsRemain() > 0:
				// WriteBatchRowsToDB failed in the batch mode and can not be retried,
				// we need to redo the writing row-by-row to find where the error locates (and skip it correctly in future).
				if err = be.WriteRowsToDB(ctx, tableName, columnNames, r); err != nil {
					// If the error is not nil, it means we reach the max error count in the non-batch mode.
					// For now, we will treat like maxErrorCount is always 0. So we will just return if any error occurs.
					return errors.Annotatef(err, "[%s] write rows reach max error count %d", tableName, 0)
				}
				continue rowLoop
			default:
				return err
			}
		}
		return errors.Annotatef(err, "[%s] batch write rows reach max retry %d and still failed", tableName, writeRowsMaxRetryTimes)
	}
	return nil
}

type stmtTask struct {
	rows tidbRows
	stmt string
}

// WriteBatchRowsToDB write rows in batch mode, which will insert multiple rows like this:
//   insert into t1 values (111), (222), (333), (444);
func (be *tidbBackend) WriteBatchRowsToDB(ctx context.Context, tableName string, columnNames []string, r kv.Rows) error {
	rows := r.(tidbRows)
	insertStmt := be.checkAndBuildStmt(rows, tableName, columnNames)
	if insertStmt == nil {
		return nil
	}
	// Note: we are not going to do interpolation (prepared statements) to avoid
	// complication arise from data length overflow of BIT and BINARY columns
	stmtTasks := make([]stmtTask, 1)
	for i, row := range rows {
		if i != 0 {
			insertStmt.WriteByte(',')
		}
		insertStmt.WriteString(row.insertStmt)
	}
	stmtTasks[0] = stmtTask{rows, insertStmt.String()}
	return be.execStmts(ctx, stmtTasks, tableName, true)
}

func (be *tidbBackend) checkAndBuildStmt(rows tidbRows, tableName string, columnNames []string) *strings.Builder {
	if len(rows) == 0 {
		return nil
	}
	return be.buildStmt(tableName, columnNames)
}

// WriteRowsToDB write rows in row-by-row mode, which will insert multiple rows like this:
//   insert into t1 values (111);
//   insert into t1 values (222);
//   insert into t1 values (333);
//   insert into t1 values (444);
// See more details in br#1366: https://github.com/pingcap/br/issues/1366
func (be *tidbBackend) WriteRowsToDB(ctx context.Context, tableName string, columnNames []string, r kv.Rows) error {
	rows := r.(tidbRows)
	insertStmt := be.checkAndBuildStmt(rows, tableName, columnNames)
	if insertStmt == nil {
		return nil
	}
	is := insertStmt.String()
	stmtTasks := make([]stmtTask, 0, len(rows))
	for _, row := range rows {
		var finalInsertStmt strings.Builder
		finalInsertStmt.WriteString(is)
		finalInsertStmt.WriteString(row.insertStmt)
		stmtTasks = append(stmtTasks, stmtTask{[]tidbRow{row}, finalInsertStmt.String()})
	}
	return be.execStmts(ctx, stmtTasks, tableName, false)
}

func (be *tidbBackend) buildStmt(tableName string, columnNames []string) *strings.Builder {
	var insertStmt strings.Builder
	switch be.onDuplicate {
	case config.ReplaceOnDup:
		insertStmt.WriteString("REPLACE INTO ")
	case config.IgnoreOnDup:
		insertStmt.WriteString("INSERT IGNORE INTO ")
	case config.ErrorOnDup:
		insertStmt.WriteString("INSERT INTO ")
	}
	insertStmt.WriteString(tableName)
	if len(columnNames) > 0 {
		insertStmt.WriteByte('(')
		for i, colName := range columnNames {
			if i != 0 {
				insertStmt.WriteByte(',')
			}
			common.WriteMySQLIdentifier(&insertStmt, colName)
		}
		insertStmt.WriteByte(')')
	}
	insertStmt.WriteString(" VALUES")
	return &insertStmt
}

func (be *tidbBackend) execStmts(ctx context.Context, stmtTasks []stmtTask, tableName string, batch bool) error {
	for _, stmtTask := range stmtTasks {
		for i := 0; i < writeRowsMaxRetryTimes; i++ {
			stmt := stmtTask.stmt
			_, err := be.db.ExecContext(ctx, stmt)
			if err != nil {
				if !common.IsContextCanceledError(err) {
					log.L().Error("execute statement failed",
						zap.Array("rows", stmtTask.rows), zap.String("stmt", redact.String(stmt)), zap.Error(err))
				}
				// It's batch mode, just return the error.
				if batch {
					return errors.Trace(err)
				}
				// Retry the non-batch insert here if this is not the last retry.
				if common.IsRetryableError(err) && i != writeRowsMaxRetryTimes-1 {
					continue
				}
				firstRow := stmtTask.rows[0]
				err = be.errorMgr.RecordTypeError(ctx, log.L(), tableName, firstRow.path, firstRow.offset, firstRow.insertStmt, err)
				if err == nil {
					// max-error not yet reached (error consumed by errorMgr), proceed to next stmtTask.
					break
				}
				return errors.Trace(err)
			}
			// No error, continue the next stmtTask.
			break
		}
	}
	failpoint.Inject("FailIfImportedSomeRows", func() {
		panic("forcing failure due to FailIfImportedSomeRows, before saving checkpoint")
	})
	return nil
}

//nolint:nakedret // TODO: refactor
func (be *tidbBackend) FetchRemoteTableModels(ctx context.Context, schemaName string) (tables []*model.TableInfo, err error) {
	s := common.SQLWithRetry{
		DB:     be.db,
		Logger: log.L(),
	}

	err = s.Transact(ctx, "fetch table columns", func(c context.Context, tx *sql.Tx) error {
		var versionStr string
		if versionStr, err = version.FetchVersion(ctx, tx); err != nil {
			return err
		}
		serverInfo := version.ParseServerInfo(versionStr)

		rows, e := tx.Query(`
			SELECT table_name, column_name, column_type, generation_expression, extra
			FROM information_schema.columns
			WHERE table_schema = ?
			ORDER BY table_name, ordinal_position;
		`, schemaName)
		if e != nil {
			return e
		}
		defer rows.Close()

		var (
			curTableName string
			curColOffset int
			curTable     *model.TableInfo
		)
		for rows.Next() {
			var tableName, columnName, columnType, generationExpr, columnExtra string
			if e := rows.Scan(&tableName, &columnName, &columnType, &generationExpr, &columnExtra); e != nil {
				return e
			}
			if tableName != curTableName {
				curTable = &model.TableInfo{
					Name:       model.NewCIStr(tableName),
					State:      model.StatePublic,
					PKIsHandle: true,
				}
				tables = append(tables, curTable)
				curTableName = tableName
				curColOffset = 0
			}

			// see: https://github.com/pingcap/parser/blob/3b2fb4b41d73710bc6c4e1f4e8679d8be6a4863e/types/field_type.go#L185-L191
			var flag uint
			if strings.HasSuffix(columnType, "unsigned") {
				flag |= mysql.UnsignedFlag
			}
			if strings.Contains(columnExtra, "auto_increment") {
				flag |= mysql.AutoIncrementFlag
			}

			ft := types.FieldType{}
			ft.SetFlag(flag)
			curTable.Columns = append(curTable.Columns, &model.ColumnInfo{
				Name:                model.NewCIStr(columnName),
				Offset:              curColOffset,
				State:               model.StatePublic,
				FieldType:           ft,
				GeneratedExprString: generationExpr,
			})
			curColOffset++
		}
		if rows.Err() != nil {
			return rows.Err()
		}
		// shard_row_id/auto random is only available after tidb v4.0.0
		// `show table next_row_id` is also not available before tidb v4.0.0
		if serverInfo.ServerType != version.ServerTypeTiDB || serverInfo.ServerVersion.Major < 4 {
			return nil
		}

		// init auto id column for each table
		for _, tbl := range tables {
			tblName := common.UniqueTable(schemaName, tbl.Name.O)
			autoIDInfos, err := FetchTableAutoIDInfos(ctx, tx, tblName)
			if err != nil {
				return errors.Trace(err)
			}
			for _, info := range autoIDInfos {
				for _, col := range tbl.Columns {
					if col.Name.O == info.Column {
						switch info.Type {
						case "AUTO_INCREMENT":
							col.AddFlag(mysql.AutoIncrementFlag)
						case "AUTO_RANDOM":
							col.AddFlag(mysql.PriKeyFlag)
							tbl.PKIsHandle = true
							// set a stub here, since we don't really need the real value
							tbl.AutoRandomBits = 1
						}
					}
				}
			}

		}
		return nil
	})
	return
}

func (be *tidbBackend) EngineFileSizes() []backend.EngineFileSize {
	return nil
}

func (be *tidbBackend) FlushEngine(context.Context, uuid.UUID) error {
	return nil
}

func (be *tidbBackend) FlushAllEngines(context.Context) error {
	return nil
}

func (be *tidbBackend) ResetEngine(context.Context, uuid.UUID) error {
	return errors.New("cannot reset an engine in TiDB backend")
}

func (be *tidbBackend) LocalWriter(
	ctx context.Context,
	cfg *backend.LocalWriterConfig,
	_ uuid.UUID,
) (backend.EngineWriter, error) {
	return &Writer{be: be}, nil
}

type Writer struct {
	be *tidbBackend
}

func (w *Writer) Close(ctx context.Context) (backend.ChunkFlushStatus, error) {
	return nil, nil
}

func (w *Writer) AppendRows(ctx context.Context, tableName string, columnNames []string, rows kv.Rows) error {
	return w.be.WriteRows(ctx, tableName, columnNames, rows)
}

func (w *Writer) IsSynced() bool {
	return true
}

type TableAutoIDInfo struct {
	Column string
	NextID int64
	Type   string
}

func FetchTableAutoIDInfos(ctx context.Context, exec utils.QueryExecutor, tableName string) ([]*TableAutoIDInfo, error) {
	rows, e := exec.QueryContext(ctx, fmt.Sprintf("SHOW TABLE %s NEXT_ROW_ID", tableName))
	if e != nil {
		return nil, errors.Trace(e)
	}
	var autoIDInfos []*TableAutoIDInfo
	for rows.Next() {
		var (
			dbName, tblName, columnName, idType string
			nextID                              int64
		)
		columns, err := rows.Columns()
		if err != nil {
			return nil, errors.Trace(err)
		}

		//+--------------+------------+-------------+--------------------+----------------+
		//| DB_NAME      | TABLE_NAME | COLUMN_NAME | NEXT_GLOBAL_ROW_ID | ID_TYPE        |
		//+--------------+------------+-------------+--------------------+----------------+
		//| testsysbench | t          | _tidb_rowid |                  1 | AUTO_INCREMENT |
		//+--------------+------------+-------------+--------------------+----------------+

		// if columns length is 4, it doesn't contains the last column `ID_TYPE`, and it will always be 'AUTO_INCREMENT'
		// for v4.0.0~v4.0.2 show table t next_row_id only returns 4 columns.
		if len(columns) == 4 {
			err = rows.Scan(&dbName, &tblName, &columnName, &nextID)
			idType = "AUTO_INCREMENT"
		} else {
			err = rows.Scan(&dbName, &tblName, &columnName, &nextID, &idType)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		autoIDInfos = append(autoIDInfos, &TableAutoIDInfo{
			Column: columnName,
			NextID: nextID,
			Type:   idType,
		})
	}
	// Defer in for-loop would be costly, anyway, we don't need those rows after this turn of iteration.
	//nolint:sqlclosecheck
	if err := rows.Close(); err != nil {
		return nil, errors.Trace(err)
	}
	if rows.Err() != nil {
		return nil, errors.Trace(rows.Err())
	}
	return autoIDInfos, nil
}
