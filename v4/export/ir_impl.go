package export

import (
	"database/sql"
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

func (iter *rowIter) Next(row RowReceiver) error {
	err := decodeFromRows(iter.rows, iter.args, row)
	iter.hasNext = iter.rows.Next()
	return err
}

func (iter *rowIter) HasNext() bool {
	return iter.hasNext
}

type sizedRowIter struct {
	rowIter     SQLRowIter
	sizeLimit   uint64
	currentSize uint64
}

func (s *sizedRowIter) Next(row RowReceiver) error {
	err := s.rowIter.Next(row)
	if err != nil {
		return err
	}
	s.currentSize += row.ReportSize()
	return nil
}

func (s *sizedRowIter) HasNext() bool {
	if s.currentSize >= s.sizeLimit {
		return false
	}
	return s.rowIter.HasNext()
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
	database string
	table    string
	rows     *sql.Rows
	colTypes []*sql.ColumnType
	specCmts []string
}

func (td *tableData) ColumnTypes() []string {
	colTypes := make([]string, len(td.colTypes))
	for i, ct := range td.colTypes {
		colTypes[i] = ct.DatabaseTypeName()
	}
	return colTypes
}

func (td *tableData) DatabaseName() string {
	return td.database
}

func (td *tableData) TableName() string {
	return td.table
}

func (td *tableData) ColumnCount() uint {
	return uint(len(td.colTypes))
}

func (td *tableData) Rows() SQLRowIter {
	return newRowIter(td.rows, len(td.colTypes))
}

func (td *tableData) SpecialComments() StringIter {
	return newStringIter(td.specCmts...)
}

type tableDataChunks struct {
	TableDataIR
	rows      SQLRowIter
	sizeLimit uint64
}

func (t *tableDataChunks) Rows() SQLRowIter {
	if t.rows == nil {
		t.rows = t.TableDataIR.Rows()
	}
	return &sizedRowIter{
		rowIter:   t.rows,
		sizeLimit: t.sizeLimit,
	}
}

func splitTableDataIntoChunks(td TableDataIR, chunkSize uint64) *tableDataChunks {
	return &tableDataChunks{
		TableDataIR: td,
		sizeLimit:   chunkSize,
	}
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
