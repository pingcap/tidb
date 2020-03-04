package export

import (
	"context"
	"fmt"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
)

var _ = Suite(&testDumpSuite{})

type testDumpSuite struct{}

type mockWriter struct {
	databaseMeta map[string]string
	tableMeta    map[string]string
	tableData    []TableDataIR
}

func newMockWriter() *mockWriter {
	return &mockWriter{
		databaseMeta: map[string]string{},
		tableMeta:    map[string]string{},
		tableData:    nil,
	}
}

func (m *mockWriter) WriteDatabaseMeta(ctx context.Context, db, createSQL string) error {
	m.databaseMeta[db] = createSQL
	return nil
}

func (m *mockWriter) WriteTableMeta(ctx context.Context, db, table, createSQL string) error {
	m.tableMeta[fmt.Sprintf("%s.%s", db, table)] = createSQL
	return nil
}

func (m *mockWriter) WriteTableData(ctx context.Context, ir TableDataIR) error {
	m.tableData = append(m.tableData, ir)
	return nil
}

func (s *testDumpSuite) TestDumpDatabase(c *C) {
	mockConfig := DefaultConfig()
	mockConfig.SortByPk = false
	mockConfig.Database = "test"
	mockConfig.Tables = NewDatabaseTables().AppendTables("test", "t")
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	showCreateDatabase := "CREATE DATABASE `test`"
	rows := mock.NewRows([]string{"Database", "Create Database"}).AddRow("test", showCreateDatabase)
	mock.ExpectQuery("SHOW CREATE DATABASE test").WillReturnRows(rows)
	showCreateTableResult := "CREATE TABLE t (a INT)"
	rows = mock.NewRows([]string{"Table", "Create Table"}).AddRow("t", showCreateTableResult)
	mock.ExpectQuery("SHOW CREATE TABLE test.t").WillReturnRows(rows)
	rows = mock.NewRows([]string{"a"}).AddRow(1)
	mock.ExpectQuery("SELECT (.) FROM test.t LIMIT 1").WillReturnRows(rows)
	rows = mock.NewRows([]string{"a"}).AddRow(1).AddRow(2)
	mock.ExpectQuery("SELECT (.) FROM test.t").WillReturnRows(rows)

	mockWriter := newMockWriter()
	err = dumpDatabases(context.Background(), mockConfig, db, mockWriter)
	c.Assert(err, IsNil)

	c.Assert(len(mockWriter.databaseMeta), Equals, 1)
	c.Assert(mockWriter.databaseMeta["test"], Equals, "CREATE DATABASE `test`")
	c.Assert(mockWriter.tableMeta["test.t"], Equals, showCreateTableResult)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testDumpSuite) TestDumpTable(c *C) {
	mockConfig := DefaultConfig()
	mockConfig.SortByPk = false
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	showCreateTableResult := "CREATE TABLE t (a INT)"
	rows := mock.NewRows([]string{"Table", "Create Table"}).AddRow("t", showCreateTableResult)
	mock.ExpectQuery("SHOW CREATE TABLE test.t").WillReturnRows(rows)
	rows = mock.NewRows([]string{"a"}).AddRow(1)
	mock.ExpectQuery("SELECT (.) FROM test.t LIMIT 1").WillReturnRows(rows)
	rows = mock.NewRows([]string{"a"}).AddRow(1).AddRow(2)
	mock.ExpectQuery("SELECT (.) FROM test.t").WillReturnRows(rows)

	mockWriter := newMockWriter()
	err = dumpTable(context.Background(), mockConfig, db, "test", &TableInfo{Name: "t"}, mockWriter)
	c.Assert(err, IsNil)

	c.Assert(mockWriter.tableMeta["test.t"], Equals, showCreateTableResult)
	c.Assert(len(mockWriter.tableData), Equals, 1)
	tbDataRes := mockWriter.tableData[0]
	c.Assert(tbDataRes.DatabaseName(), Equals, "test")
	c.Assert(tbDataRes.TableName(), Equals, "t")
	c.Assert(tbDataRes.ColumnCount(), Equals, uint(1))
	c.Assert(tbDataRes.SpecialComments().HasNext(), Equals, false)
	c.Assert(tbDataRes.Rows().HasNext(), Equals, true)
	receiver := newSimpleRowReceiver(1)
	c.Assert(tbDataRes.Rows().Next(receiver), IsNil)
	c.Assert(receiver.data[0], Equals, "2")
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}
