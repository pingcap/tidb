package export

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/pingcap/dumpling/v4/log"
)

// rowIter implements the SQLRowIter interface.
// Note: To create a rowIter, please use `newRowIter()` instead of struct literal.
type rowIter struct {
	rows    *sql.Rows
	hasNext bool
	args    []interface{}
}

func newRowIter(rows *sql.Rows, argLen int) *rowIter {
	r := &rowIter{
		rows:    rows,
		hasNext: false,
		args:    make([]interface{}, argLen),
	}
	r.hasNext = r.rows.Next()
	return r
}

func (iter *rowIter) Close() error {
	return iter.rows.Close()
}

func (iter *rowIter) Decode(row RowReceiver) error {
	return decodeFromRows(iter.rows, iter.args, row)
}

func (iter *rowIter) Error() error {
	return iter.rows.Err()
}

func (iter *rowIter) Next() {
	iter.hasNext = iter.rows.Next()
}

func (iter *rowIter) HasNext() bool {
	return iter.hasNext
}

type stringIter struct {
	idx int
	ss  []string
}

func newStringIter(ss ...string) StringIter {
	return &stringIter{
		idx: 0,
		ss:  ss,
	}
}

func (m *stringIter) Next() string {
	if m.idx >= len(m.ss) {
		return ""
	}
	ret := m.ss[m.idx]
	m.idx += 1
	return ret
}

func (m *stringIter) HasNext() bool {
	return m.idx < len(m.ss)
}

type tableData struct {
	database        string
	table           string
	query           string
	chunkIndex      int
	rows            *sql.Rows
	colTypes        []*sql.ColumnType
	selectedField   string
	specCmts        []string
	escapeBackslash bool
	SQLRowIter
}

func (td *tableData) Start(ctx context.Context, conn *sql.Conn) error {
	rows, err := conn.QueryContext(ctx, td.query)
	if err != nil {
		return err
	}
	td.rows = rows
	return nil
}

func (td *tableData) ColumnTypes() []string {
	colTypes := make([]string, len(td.colTypes))
	for i, ct := range td.colTypes {
		colTypes[i] = ct.DatabaseTypeName()
	}
	return colTypes
}

func (td *tableData) ColumnNames() []string {
	colNames := make([]string, len(td.colTypes))
	for i, ct := range td.colTypes {
		colNames[i] = ct.Name()
	}
	return colNames
}

func (td *tableData) DatabaseName() string {
	return td.database
}

func (td *tableData) TableName() string {
	return td.table
}

func (td *tableData) ChunkIndex() int {
	return td.chunkIndex
}

func (td *tableData) ColumnCount() uint {
	return uint(len(td.colTypes))
}

func (td *tableData) Rows() SQLRowIter {
	if td.SQLRowIter == nil {
		td.SQLRowIter = newRowIter(td.rows, len(td.colTypes))
	}
	return td.SQLRowIter
}

func (td *tableData) SelectedField() string {
	if td.selectedField == "*" {
		return ""
	}
	return fmt.Sprintf("(%s)", td.selectedField)
}

func (td *tableData) SpecialComments() StringIter {
	return newStringIter(td.specCmts...)
}

func (td *tableData) EscapeBackSlash() bool {
	return td.escapeBackslash
}

func splitTableDataIntoChunks(
	ctx context.Context,
	tableDataIRCh chan TableDataIR,
	errCh chan error,
	linear chan struct{},
	dbName, tableName string, db *sql.Conn, conf *Config) {
	field, err := pickupPossibleField(dbName, tableName, db, conf)
	if err != nil {
		errCh <- withStack(err)
		return
	}
	if field == "" {
		// skip split chunk logic if not found proper field
		log.Debug("skip concurrent dump due to no proper field", zap.String("field", field))
		linear <- struct{}{}
		return
	}

	query := fmt.Sprintf("SELECT MIN(`%s`),MAX(`%s`) FROM `%s`.`%s` ",
		escapeString(field), escapeString(field), escapeString(dbName), escapeString(tableName))
	if conf.Where != "" {
		query = fmt.Sprintf("%s WHERE %s", query, conf.Where)
	}
	log.Debug("split chunks", zap.String("query", query))

	var smin sql.NullString
	var smax sql.NullString
	row := db.QueryRowContext(ctx, query)
	err = row.Scan(&smin, &smax)
	if err != nil {
		log.Error("split chunks - get max min failed", zap.String("query", query), zap.Error(err))
		errCh <- withStack(err)
		return
	}
	if !smax.Valid || !smin.Valid {
		// found no data
		log.Warn("no data to dump", zap.String("schema", dbName), zap.String("table", tableName))
		close(tableDataIRCh)
		return
	}

	var max uint64
	var min uint64
	if max, err = strconv.ParseUint(smax.String, 10, 64); err != nil {
		errCh <- errors.WithMessagef(err, "fail to convert max value %s in query %s", smax.String, query)
		return
	}
	if min, err = strconv.ParseUint(smin.String, 10, 64); err != nil {
		errCh <- errors.WithMessagef(err, "fail to convert min value %s in query %s", smin.String, query)
		return
	}

	count := estimateCount(dbName, tableName, db, field, conf)
	log.Info("get estimated rows count", zap.Uint64("estimateCount", count))
	if count < conf.Rows {
		// skip chunk logic if estimates are low
		log.Debug("skip concurrent dump due to estimate count < rows",
			zap.Uint64("estimate count", count),
			zap.Uint64("conf.rows", conf.Rows),
		)
		linear <- struct{}{}
		return
	}

	// every chunk would have eventual adjustments
	estimatedChunks := count / conf.Rows
	estimatedStep := (max-min)/estimatedChunks + 1
	cutoff := min

	selectedField, err := buildSelectField(db, dbName, tableName, conf.CompleteInsert)
	if err != nil {
		errCh <- withStack(err)
		return
	}

	colTypes, err := GetColumnTypes(db, selectedField, dbName, tableName)
	if err != nil {
		errCh <- withStack(err)
		return
	}
	orderByClause, err := buildOrderByClause(conf, db, dbName, tableName)
	if err != nil {
		errCh <- withStack(err)
		return
	}

	chunkIndex := 0
	nullValueCondition := fmt.Sprintf("`%s` IS NULL OR ", escapeString(field))
LOOP:
	for cutoff <= max {
		chunkIndex += 1
		where := fmt.Sprintf("%s(`%s` >= %d AND `%s` < %d)", nullValueCondition, escapeString(field), cutoff, escapeString(field), cutoff+estimatedStep)
		query = buildSelectQuery(dbName, tableName, selectedField, buildWhereCondition(conf, where), orderByClause)
		if len(nullValueCondition) > 0 {
			nullValueCondition = ""
		}

		td := &tableData{
			database:        dbName,
			table:           tableName,
			query:           query,
			chunkIndex:      chunkIndex,
			colTypes:        colTypes,
			selectedField:   selectedField,
			escapeBackslash: conf.EscapeBackslash,
			specCmts: []string{
				"/*!40101 SET NAMES binary*/;",
			},
		}
		cutoff += estimatedStep
		select {
		case <-ctx.Done():
			break LOOP
		case tableDataIRCh <- td:
		}
	}
	close(tableDataIRCh)
}

type metaData struct {
	target   string
	metaSQL  string
	specCmts []string
}

func (m *metaData) SpecialComments() StringIter {
	return newStringIter(m.specCmts...)
}

func (m *metaData) TargetName() string {
	return m.target
}

func (m *metaData) MetaSQL() string {
	return m.metaSQL
}
