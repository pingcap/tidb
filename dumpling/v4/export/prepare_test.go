package export

import (
	"context"
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
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)

	rows := sqlmock.NewRows([]string{"Database"}).
		AddRow("db1").
		AddRow("db2").
		AddRow("db3").
		AddRow("db5")
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(rows)
	conf := DefaultConfig()
	conf.Databases = []string{"db1", "db2", "db3"}
	result, err := prepareDumpingDatabases(conf, conn)
	c.Assert(err, IsNil)
	c.Assert(result, DeepEquals, []string{"db1", "db2", "db3"})

	conf.Databases = nil
	rows = sqlmock.NewRows([]string{"Database"}).
		AddRow("db1").
		AddRow("db2")
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(rows)
	result, err = prepareDumpingDatabases(conf, conn)
	c.Assert(err, IsNil)
	c.Assert(result, DeepEquals, []string{"db1", "db2"})

	mock.ExpectQuery("SHOW DATABASES").WillReturnError(fmt.Errorf("err"))
	_, err = prepareDumpingDatabases(conf, conn)
	c.Assert(err, NotNil)

	rows = sqlmock.NewRows([]string{"Database"}).
		AddRow("db1").
		AddRow("db2").
		AddRow("db3").
		AddRow("db5")
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(rows)
	conf.Databases = []string{"db1", "db2", "db4", "db6"}
	_, err = prepareDumpingDatabases(conf, conn)
	c.Assert(err, ErrorMatches, `Unknown databases \[db4,db6\]`)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testPrepareSuite) TestListAllTables(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)

	data := NewDatabaseTables().
		AppendTables("db1", "t1", "t2").
		AppendTables("db2", "t3", "t4", "t5").
		AppendViews("db3", "t6", "t7", "t8")

	var dbNames []databaseName
	rows := sqlmock.NewRows([]string{"table_schema", "table_name"})
	for dbName, tableInfos := range data {
		dbNames = append(dbNames, dbName)

		for _, tbInfo := range tableInfos {
			if tbInfo.Type == TableTypeView {
				continue
			}
			rows.AddRow(dbName, tbInfo.Name)
		}
	}
	query := "SELECT table_schema,table_name FROM information_schema.tables WHERE table_type = (.*)"
	mock.ExpectQuery(query).WillReturnRows(rows)

	tables, err := listAllTables(conn, dbNames)
	c.Assert(err, IsNil)

	for d, t := range tables {
		expectedTbs, ok := data[d]
		c.Assert(ok, IsTrue)
		for i := 0; i < len(t); i++ {
			cmt := Commentf("%v mismatch: %v", t[i], expectedTbs[i])
			c.Assert(t[i].Equals(expectedTbs[i]), IsTrue, cmt)
		}
	}

	// Test list all tables and not skipping views.
	data = NewDatabaseTables().
		AppendTables("db", "t1").
		AppendViews("db", "t2")
	query = "SELECT table_schema,table_name FROM information_schema.tables WHERE table_type = (.*)"
	mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"table_schema", "table_name"}).AddRow("db", "t2"))
	tables, err = listAllViews(conn, []string{"db"})
	c.Assert(err, IsNil)
	c.Assert(len(tables), Equals, 1)
	c.Assert(len(tables["db"]), Equals, 1)
	c.Assert(tables["db"][0].Equals(data["db"][1]), IsTrue, Commentf("%v mismatch %v", tables["db"][0], data["db"][1]))

	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testPrepareSuite) TestAdjustConfig(c *C) {
	conf := DefaultConfig()
	conf.Where = "id < 5"
	conf.Sql = "select * from t where id > 3"
	c.Assert(adjustConfig(nil, conf), ErrorMatches, "can't specify both --sql and --where at the same time. Please try to combine them into --sql")
	conf.Where = ""
	c.Assert(adjustConfig(nil, conf), IsNil)
	conf.Sql = ""
	conf.Rows = 5000
	conf.FileSize = 5000
	c.Assert(adjustConfig(nil, conf), IsNil)
	c.Assert(conf.FileSize, Equals, uint64(5000))
}
