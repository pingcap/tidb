package export

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/DATA-DOG/go-sqlmock"
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

type mockTableIR struct {
	dbName          string
	tblName         string
	chunIndex       int
	data            [][]driver.Value
	selectedField   string
	specCmt         []string
	colTypes        []string
	colNames        []string
	escapeBackSlash bool
	rowErr          error
	SQLRowIter
}

func (m *mockTableIR) Start(ctx context.Context, conn *sql.Conn) error {
	return nil
}

func (m *mockTableIR) DatabaseName() string {
	return m.dbName
}

func (m *mockTableIR) TableName() string {
	return m.tblName
}

func (m *mockTableIR) ChunkIndex() int {
	return m.chunIndex
}

func (m *mockTableIR) ColumnCount() uint {
	return uint(len(m.colTypes))
}

func (m *mockTableIR) ColumnTypes() []string {
	return m.colTypes
}

func (m *mockTableIR) ColumnNames() []string {
	return m.colNames
}

func (m *mockTableIR) SelectedField() string {
	if m.selectedField == "*" {
		return ""
	}
	return m.selectedField
}

func (m *mockTableIR) SpecialComments() StringIter {
	return newStringIter(m.specCmt...)
}

func (m *mockTableIR) Rows() SQLRowIter {
	if m.SQLRowIter == nil {
		mockRows := sqlmock.NewRows(m.colTypes)
		for _, datum := range m.data {
			mockRows.AddRow(datum...)
		}
		db, mock, err := sqlmock.New()
		if err != nil {
			panic(fmt.Sprintf("sqlmock.New return error: %v", err))
		}
		defer db.Close()
		mock.ExpectQuery("select 1").WillReturnRows(mockRows)
		if m.rowErr != nil {
			mockRows.RowError(len(m.data)-1, m.rowErr)
		}
		rows, err := db.Query("select 1")
		if err != nil {
			panic(fmt.Sprintf("sqlmock.New return error: %v", err))
		}
		m.SQLRowIter = newRowIter(rows, len(m.colTypes))
	}
	return m.SQLRowIter
}

func (m *mockTableIR) EscapeBackSlash() bool {
	return m.escapeBackSlash
}

func newMockTableIR(databaseName, tableName string, data [][]driver.Value, specialComments, colTypes []string) *mockTableIR {
	return &mockTableIR{
		dbName:        databaseName,
		tblName:       tableName,
		data:          data,
		specCmt:       specialComments,
		selectedField: "*",
		colTypes:      colTypes,
		SQLRowIter:    nil,
	}
}
