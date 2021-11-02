// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/DATA-DOG/go-sqlmock"

	tcontext "github.com/pingcap/tidb/dumpling/context"
)

type mockPoisonWriter struct {
	buf string
}

func (m *mockPoisonWriter) Write(_ context.Context, p []byte) (int, error) {
	s := string(p)
	if s == "poison" {
		return 0, fmt.Errorf("poison_error")
	}
	m.buf = s
	return len(s), nil
}

func (m *mockPoisonWriter) Close(_ context.Context) error {
	// noop
	return nil
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
	dbName           string
	tblName          string
	chunIndex        int
	data             [][]driver.Value
	selectedField    string
	selectedLen      int
	specCmt          []string
	colTypes         []string
	colNames         []string
	escapeBackSlash  bool
	hasImplicitRowID bool
	rowErr           error
	rows             *sql.Rows
	SQLRowIter
}

func (m *mockTableIR) RawRows() *sql.Rows {
	return m.rows
}

func (m *mockTableIR) ShowCreateTable() string {
	return ""
}

func (m *mockTableIR) ShowCreateView() string {
	return ""
}

func (m *mockTableIR) AvgRowLength() uint64 {
	return 0
}

func (m *mockTableIR) HasImplicitRowID() bool {
	return m.hasImplicitRowID
}

func (m *mockTableIR) Start(_ *tcontext.Context, conn *sql.Conn) error {
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
	return m.selectedField
}

func (m *mockTableIR) SelectedLen() int {
	return m.selectedLen
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
		m.rows = rows
	}
	return m.SQLRowIter
}

func (m *mockTableIR) Close() error {
	return nil
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
		selectedLen:   len(colTypes),
		colTypes:      colTypes,
		SQLRowIter:    nil,
	}
}
