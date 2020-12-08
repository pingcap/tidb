// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
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
	m.idx++
	return ret
}

func (m *stringIter) HasNext() bool {
	return m.idx < len(m.ss)
}

type tableData struct {
	query  string
	rows   *sql.Rows
	colLen int
	SQLRowIter
}

func newTableData(query string, colLen int) *tableData {
	return &tableData{
		query:  query,
		colLen: colLen,
	}
}

func (td *tableData) Start(ctx context.Context, conn *sql.Conn) error {
	rows, err := conn.QueryContext(ctx, td.query)
	if err != nil {
		return errors.Trace(err)
	}
	if err = rows.Err(); err != nil {
		return errors.Trace(err)
	}
	td.SQLRowIter = nil
	td.rows = rows
	return nil
}

func (td *tableData) Rows() SQLRowIter {
	if td.SQLRowIter == nil {
		td.SQLRowIter = newRowIter(td.rows, td.colLen)
	}
	return td.SQLRowIter
}

func (td *tableData) Close() error {
	return td.rows.Close()
}

type tableMeta struct {
	database        string
	table           string
	colTypes        []*sql.ColumnType
	selectedField   string
	specCmts        []string
	showCreateTable string
	showCreateView  string
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
	if tm.selectedField == "*" || tm.selectedField == "" {
		return tm.selectedField
	}
	return fmt.Sprintf("(%s)", tm.selectedField)
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
