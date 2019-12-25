package export

import (
	"database/sql"
	"fmt"
)

type mockStringWriter struct {
	buf string
}

func (m *mockStringWriter) WriteString(s string) (int, error) {
	if s == "poison" {
		return 0, fmt.Errorf("poison_error")
	}
	m.buf = s
	return len(s), nil
}

type mockStringCollector struct {
	buf string
}

func (m *mockStringCollector) WriteString(s string) (int, error) {
	m.buf += s
	return len(s), nil
}

type mockSQLRowIterator struct {
	idx  int
	data [][]sql.NullString
}

func (m *mockSQLRowIterator) Next(sp RowReceiver) error {
	args := make([]interface{}, len(m.data[m.idx]))
	sp.BindAddress(args)
	for i := range args {
		*(args[i]).(*sql.NullString) = m.data[m.idx][i]
	}
	m.idx += 1
	return nil
}

func (m *mockSQLRowIterator) HasNext() bool {
	return m.idx < len(m.data)
}

type mockMetaIR struct {
	tarName string
	meta    string
	specCmt []string
}

func (m *mockMetaIR) SpecialComments() StringIter {
	return newStringIter(m.specCmt...)
}

func (m *mockMetaIR) TargetName() string {
	return m.tarName
}

func (m *mockMetaIR) MetaSQL() string {
	return m.meta
}

func newMockMetaIR(targetName string, meta string, specialComments []string) MetaIR {
	return &mockMetaIR{
		tarName: targetName,
		meta:    meta,
		specCmt: specialComments,
	}
}

func makeNullString(ss []string) []sql.NullString {
	var ns []sql.NullString
	for _, s := range ss {
		if len(s) != 0 {
			ns = append(ns, sql.NullString{String: s, Valid: true})
		} else {
			ns = append(ns, sql.NullString{Valid: false})
		}
	}
	return ns
}

type mockTableDataIR struct {
	dbName   string
	tblName  string
	data     [][]sql.NullString
	specCmt  []string
	colTypes []string
}

func (m *mockTableDataIR) ColumnTypes() []string {
	return m.colTypes
}

func newMockTableDataIR(databaseName, tableName string, data [][]string, specialComments []string, colTypes []string) TableDataIR {
	var nData [][]sql.NullString
	for _, ss := range data {
		nData = append(nData, makeNullString(ss))
	}

	return &mockTableDataIR{
		dbName:   databaseName,
		tblName:  tableName,
		data:     nData,
		specCmt:  specialComments,
		colTypes: colTypes,
	}
}

func (m *mockTableDataIR) DatabaseName() string {
	return m.dbName
}

func (m *mockTableDataIR) TableName() string {
	return "employee"
}

func (m *mockTableDataIR) ColumnCount() uint {
	return 5
}

func (m *mockTableDataIR) SpecialComments() StringIter {
	return newStringIter(m.specCmt...)
}

func (m *mockTableDataIR) Rows() SQLRowIter {
	return &mockSQLRowIterator{
		idx:  0,
		data: m.data,
	}
}

type DummyLogger struct{}

func (d *DummyLogger) Debug(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	fmt.Println()
}

func (d *DummyLogger) Info(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	fmt.Println()
}

func (d *DummyLogger) Warn(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	fmt.Println()
}

func (d *DummyLogger) Error(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	fmt.Println()
}
