// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"database/sql"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/version"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"go.uber.org/zap"
)

// rowIter implements the SQLRowIter interface.
// Note: To create a rowIter, please use `newRowIter()` instead of struct literal.
type rowIter struct {
	rows    *sql.Rows
	hasNext bool
	args    []any
}

func newRowIter(rows *sql.Rows, argLen int) *rowIter {
	r := &rowIter{
		rows:    rows,
		hasNext: false,
		args:    make([]any, argLen),
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
	return errors.Trace(iter.rows.Err())
}

func (iter *rowIter) Next() {
	iter.hasNext = iter.rows.Next()
}

func (iter *rowIter) HasNext() bool {
	return iter.hasNext
}

// multiQueriesChunkIter implements the SQLRowIter interface.
// Note: To create a rowIter, please use `newRowIter()` instead of struct literal.
type multiQueriesChunkIter struct {
	tctx    *tcontext.Context
	conn    *sql.Conn
	rows    *sql.Rows
	hasNext bool
	id      int
	queries []string
	args    []any
	err     error
}

func newMultiQueryChunkIter(tctx *tcontext.Context, conn *sql.Conn, queries []string, argLen int) *multiQueriesChunkIter {
	r := &multiQueriesChunkIter{
		tctx:    tctx,
		conn:    conn,
		queries: queries,
		id:      0,
		args:    make([]any, argLen),
	}
	r.nextRows()
	return r
}

func (iter *multiQueriesChunkIter) nextRows() {
	if iter.id >= len(iter.queries) {
		iter.hasNext = false
		return
	}
	var err error
	defer func() {
		if err != nil {
			iter.hasNext = false
			iter.err = errors.Trace(err)
		}
	}()
	tctx, conn := iter.tctx, iter.conn
	// avoid the empty chunk
	for iter.id < len(iter.queries) {
		rows := iter.rows
		if rows != nil {
			if err = rows.Close(); err != nil {
				return
			}
			if err = rows.Err(); err != nil {
				return
			}
		}
		tctx.L().Debug("try to start nextRows", zap.String("query", iter.queries[iter.id]))
		rows, err = conn.QueryContext(tctx, iter.queries[iter.id])
		if err != nil {
			return
		}
		if err = rows.Err(); err != nil {
			return
		}
		iter.id++
		iter.rows = rows
		iter.hasNext = iter.rows.Next()
		if iter.hasNext {
			return
		}
	}
}

func (iter *multiQueriesChunkIter) Close() error {
	if iter.err != nil {
		return iter.err
	}
	if iter.rows != nil {
		return iter.rows.Close()
	}
	return nil
}

func (iter *multiQueriesChunkIter) Decode(row RowReceiver) error {
	if iter.err != nil {
		return iter.err
	}
	if iter.rows == nil {
		return errors.Errorf("no valid rows found, id: %d", iter.id)
	}
	return decodeFromRows(iter.rows, iter.args, row)
}

func (iter *multiQueriesChunkIter) Error() error {
	if iter.err != nil {
		return iter.err
	}
	if iter.rows != nil {
		return errors.Trace(iter.rows.Err())
	}
	return nil
}

func (iter *multiQueriesChunkIter) Next() {
	if iter.err == nil {
		iter.hasNext = iter.rows.Next()
		if !iter.hasNext {
			iter.nextRows()
		}
	}
}

func (iter *multiQueriesChunkIter) HasNext() bool {
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
	m.idx++
	return ret
}

func (m *stringIter) HasNext() bool {
	return m.idx < len(m.ss)
}

type tableData struct {
	query        string
	rows         *sql.Rows
	colLen       int
	needColTypes bool
	colTypes     []string
	SQLRowIter
}

func newTableData(query string, colLength int, needColTypes bool) *tableData {
	return &tableData{
		query:        query,
		colLen:       colLength,
		needColTypes: needColTypes,
	}
}

func (td *tableData) Start(tctx *tcontext.Context, conn *sql.Conn) error {
	tctx.L().Debug("try to start tableData", zap.String("query", td.query))
	rows, err := conn.QueryContext(tctx, td.query)
	if err != nil {
		return errors.Annotatef(err, "sql: %s", td.query)
	}
	if err = rows.Err(); err != nil {
		return errors.Annotatef(err, "sql: %s", td.query)
	}
	td.SQLRowIter = nil
	td.rows = rows
	if td.needColTypes {
		ns, err := rows.Columns()
		if err != nil {
			return errors.Trace(err)
		}
		td.colLen = len(ns)
		td.colTypes = make([]string, 0, td.colLen)
		colTps, err := rows.ColumnTypes()
		if err != nil {
			return errors.Trace(err)
		}
		for _, c := range colTps {
			td.colTypes = append(td.colTypes, c.DatabaseTypeName())
		}
	}

	return nil
}

func (td *tableData) Rows() SQLRowIter {
	// should be initialized lazily since it calls rows.Next() which might close the rows when
	// there's nothing to read, causes code which relies on rows not closed to fail.
	if td.SQLRowIter == nil {
		td.SQLRowIter = newRowIter(td.rows, td.colLen)
	}
	return td.SQLRowIter
}

func (td *tableData) Close() error {
	if td.SQLRowIter != nil {
		// will close td.rows internally
		return td.SQLRowIter.Close()
	} else if td.rows != nil {
		return td.rows.Close()
	}
	return nil
}

func (td *tableData) RawRows() *sql.Rows {
	return td.rows
}

type tableMeta struct {
	database         string
	table            string
	colTypes         []*sql.ColumnType
	selectedField    string
	selectedLen      int
	specCmts         []string
	showCreateTable  string
	showCreateView   string
	avgRowLength     uint64
	hasImplicitRowID bool
}

func (tm *tableMeta) ColumnTypes() []string {
	colTypes := make([]string, len(tm.colTypes))
	for i, ct := range tm.colTypes {
		colTypes[i] = ct.DatabaseTypeName()
	}
	return colTypes
}

func (tm *tableMeta) ColumnNames() []string {
	colNames := make([]string, len(tm.colTypes))
	for i, ct := range tm.colTypes {
		colNames[i] = ct.Name()
	}
	return colNames
}

func (tm *tableMeta) DatabaseName() string {
	return tm.database
}

func (tm *tableMeta) TableName() string {
	return tm.table
}

func (tm *tableMeta) ColumnCount() uint {
	return uint(len(tm.colTypes))
}

func (tm *tableMeta) SelectedField() string {
	return tm.selectedField
}

func (tm *tableMeta) SelectedLen() int {
	return tm.selectedLen
}

func (tm *tableMeta) SpecialComments() StringIter {
	return newStringIter(tm.specCmts...)
}

func (tm *tableMeta) ShowCreateTable() string {
	return tm.showCreateTable
}

func (tm *tableMeta) ShowCreateView() string {
	return tm.showCreateView
}

func (tm *tableMeta) AvgRowLength() uint64 {
	return tm.avgRowLength
}

func (tm *tableMeta) HasImplicitRowID() bool {
	return tm.hasImplicitRowID
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
	if !strings.HasSuffix(m.metaSQL, ";\n") {
		m.metaSQL += ";\n"
	}
	return m.metaSQL
}

type multiQueriesChunk struct {
	tctx    *tcontext.Context
	conn    *sql.Conn
	queries []string
	colLen  int
	SQLRowIter
}

func newMultiQueriesChunk(queries []string, colLength int) *multiQueriesChunk {
	return &multiQueriesChunk{
		queries: queries,
		colLen:  colLength,
	}
}

func (td *multiQueriesChunk) Start(tctx *tcontext.Context, conn *sql.Conn) error {
	td.tctx = tctx
	td.conn = conn
	td.SQLRowIter = nil
	return nil
}

func (td *multiQueriesChunk) Rows() SQLRowIter {
	if td.SQLRowIter == nil {
		td.SQLRowIter = newMultiQueryChunkIter(td.tctx, td.conn, td.queries, td.colLen)
	}
	return td.SQLRowIter
}

func (td *multiQueriesChunk) Close() error {
	if td.SQLRowIter != nil {
		return td.SQLRowIter.Close()
	}
	return nil
}

func (*multiQueriesChunk) RawRows() *sql.Rows {
	return nil
}

var serverSpecialComments = map[version.ServerType][]string{
	version.ServerTypeMySQL: {
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;",
		"/*!40101 SET NAMES binary*/;",
	},
	version.ServerTypeTiDB: {
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;",
		"/*!40101 SET NAMES binary*/;",
	},
	version.ServerTypeMariaDB: {
		"/*!40101 SET NAMES binary*/;",
		"SET FOREIGN_KEY_CHECKS=0;",
	},
}

func getSpecialComments(serverType version.ServerType) []string {
	return serverSpecialComments[serverType]
}
