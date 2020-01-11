package export

import (
	"fmt"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
)

var _ = Suite(&testPrepareSuite{})

type testPrepareSuite struct{}

func (s *testPrepareSuite) TestPrepareDumpingDatabases(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	conf := DefaultConfig()
	conf.Database = "db1,db2,db3"
	result, err := prepareDumpingDatabases(conf, db)
	c.Assert(err, IsNil)
	c.Assert(result, DeepEquals, []string{"db1", "db2", "db3"})

	conf.Database = ""
	rows := sqlmock.NewRows([]string{"Database"}).
		AddRow("db1").
		AddRow("db2")
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(rows)
	result, err = prepareDumpingDatabases(conf, db)
	c.Assert(err, IsNil)
	c.Assert(result, DeepEquals, []string{"db1", "db2"})

	mock.ExpectQuery("SHOW DATABASES").WillReturnError(fmt.Errorf("err"))
	_, err = prepareDumpingDatabases(conf, db)
	c.Assert(err, NotNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testPrepareSuite) TestListAllTables(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	resultOk := sqlmock.NewResult(0, 0)
	data := map[databaseName][]tableName{
		"db1": {"t1", "t2"},
		"db2": {"t3", "t4", "t5"},
	}

	var dbNames []databaseName
	for dbName, tbNames := range data {
		dbNames = append(dbNames, dbName)
		mock.ExpectExec("USE .").WillReturnResult(resultOk)
		rows := sqlmock.NewRows([]string{"Tables_in_xx"})
		for _, name := range tbNames {
			rows.AddRow(name)
		}
		mock.ExpectQuery("SHOW TABLES").WillReturnRows(rows)
	}

	tables, err := listAllTables(db, dbNames)
	c.Assert(err, IsNil)

	for d, t := range tables {
		expectedTbs, ok := data[d]
		c.Assert(ok, IsTrue)
		c.Assert(t, DeepEquals, expectedTbs)
	}
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}
