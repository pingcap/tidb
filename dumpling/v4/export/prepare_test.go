// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"fmt"
	"strings"

	tcontext "github.com/pingcap/dumpling/v4/context"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
)

var _ = Suite(&testPrepareSuite{})

type testPrepareSuite struct{}

func (s *testPrepareSuite) TestPrepareDumpingDatabases(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	tctx := tcontext.Background().WithLogger(appLogger)
	conn, err := db.Conn(tctx)
	c.Assert(err, IsNil)

	rows := sqlmock.NewRows([]string{"Database"}).
		AddRow("db1").
		AddRow("db2").
		AddRow("db3").
		AddRow("db5")
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(rows)
	conf := defaultConfigForTest(c)
	conf.Databases = []string{"db1", "db2", "db3"}
	result, err := prepareDumpingDatabases(tctx, conf, conn)
	c.Assert(err, IsNil)
	c.Assert(result, DeepEquals, []string{"db1", "db2", "db3"})

	conf.Databases = nil
	rows = sqlmock.NewRows([]string{"Database"}).
		AddRow("db1").
		AddRow("db2")
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(rows)
	result, err = prepareDumpingDatabases(tctx, conf, conn)
	c.Assert(err, IsNil)
	c.Assert(result, DeepEquals, []string{"db1", "db2"})

	mock.ExpectQuery("SHOW DATABASES").WillReturnError(fmt.Errorf("err"))
	_, err = prepareDumpingDatabases(tctx, conf, conn)
	c.Assert(err, NotNil)

	rows = sqlmock.NewRows([]string{"Database"}).
		AddRow("db1").
		AddRow("db2").
		AddRow("db3").
		AddRow("db5")
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(rows)
	conf.Databases = []string{"db1", "db2", "db4", "db6"}
	_, err = prepareDumpingDatabases(tctx, conf, conn)
	c.Assert(err, ErrorMatches, `Unknown databases \[db4,db6\]`)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testPrepareSuite) TestListAllTables(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	tctx := tcontext.Background().WithLogger(appLogger)

	// Test list all tables and skipping views.
	data := NewDatabaseTables().
		AppendTables("db1", []string{"t1", "t2"}, []uint64{1, 2}).
		AppendTables("db2", []string{"t3", "t4", "t5"}, []uint64{3, 4, 5}).
		AppendViews("db3", "t6", "t7", "t8")

	dbNames := make([]databaseName, 0, len(data))
	rows := sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "AVG_ROW_LENGTH"})
	for dbName, tableInfos := range data {
		dbNames = append(dbNames, dbName)

		for _, tbInfo := range tableInfos {
			if tbInfo.Type == TableTypeView {
				continue
			}
			rows.AddRow(dbName, tbInfo.Name, tbInfo.Type.String(), tbInfo.AvgRowLength)
		}
	}
	query := "SELECT TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE,AVG_ROW_LENGTH FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'"
	mock.ExpectQuery(query).WillReturnRows(rows)

	tables, err := ListAllDatabasesTables(tctx, conn, dbNames, listTableByInfoSchema, TableTypeBase)
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
		AppendTables("db", []string{"t1"}, []uint64{1}).
		AppendViews("db", "t2")
	query = "SELECT TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE,AVG_ROW_LENGTH FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' OR TABLE_TYPE='VIEW'"
	mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "AVG_ROW_LENGTH"}).
		AddRow("db", "t1", TableTypeBaseStr, 1).AddRow("db", "t2", TableTypeViewStr, nil))
	tables, err = ListAllDatabasesTables(tctx, conn, []string{"db"}, listTableByInfoSchema, TableTypeBase, TableTypeView)
	c.Assert(err, IsNil)
	c.Assert(len(tables), Equals, 1)
	c.Assert(len(tables["db"]), Equals, 2)
	for i := 0; i < len(tables["db"]); i++ {
		cmt := Commentf("%v mismatch: %v", tables["db"][i], data["db"][i])
		c.Assert(tables["db"][i].Equals(data["db"][i]), IsTrue, cmt)
	}

	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testPrepareSuite) TestListAllTablesByTableStatus(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	tctx := tcontext.Background().WithLogger(appLogger)

	// Test list all tables and skipping views.
	data := NewDatabaseTables().
		AppendTables("db1", []string{"t1", "t2"}, []uint64{1, 2}).
		AppendTables("db2", []string{"t3", "t4", "t5"}, []uint64{3, 4, 5}).
		AppendViews("db3", "t6", "t7", "t8")

	query := "SHOW TABLE STATUS FROM `%s`"
	showTableStatusColumnNames := []string{"Name", "Engine", "Version", "Row_format", "Rows", "Avg_row_length", "Data_length", "Max_data_length", "Index_length", "Data_free", "Auto_increment", "Create_time", "Update_time", "Check_time", "Collation", "Checksum", "Create_options", "Comment"}
	dbNames := make([]databaseName, 0, len(data))
	for dbName, tableInfos := range data {
		dbNames = append(dbNames, dbName)
		rows := sqlmock.NewRows(showTableStatusColumnNames)

		for _, tbInfo := range tableInfos {
			if tbInfo.Type == TableTypeBase {
				rows.AddRow(tbInfo.Name, "InnoDB", 10, "Dynamic", 0, 0, 16384, 0, 0, 0, nil, "2021-07-08 03:04:07", nil, nil, "latin1_swedish_ci", nil, "", "")
			} else {
				rows.AddRow(tbInfo.Name, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, TableTypeView.String())
			}
		}
		mock.ExpectQuery(fmt.Sprintf(query, dbName)).WillReturnRows(rows)
	}

	tables, err := ListAllDatabasesTables(tctx, conn, dbNames, listTableByShowTableStatus, TableTypeBase)
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
		AppendTables("db", []string{"t1"}, []uint64{1}).
		AppendViews("db", "t2")
	mock.ExpectQuery(fmt.Sprintf(query, "db")).WillReturnRows(sqlmock.NewRows(showTableStatusColumnNames).
		AddRow("t1", "InnoDB", 10, "Dynamic", 0, 1, 16384, 0, 0, 0, nil, "2021-07-08 03:04:07", nil, nil, "latin1_swedish_ci", nil, "", "").
		AddRow("t2", nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, TableTypeView.String()))
	tables, err = ListAllDatabasesTables(tctx, conn, []string{"db"}, listTableByShowTableStatus, TableTypeBase, TableTypeView)
	c.Assert(err, IsNil)
	c.Assert(len(tables), Equals, 1)
	c.Assert(len(tables["db"]), Equals, 2)
	for i := 0; i < len(tables["db"]); i++ {
		cmt := Commentf("%v mismatch: %v", tables["db"][i], data["db"][i])
		c.Assert(tables["db"][i].Equals(data["db"][i]), IsTrue, cmt)
	}

	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testPrepareSuite) TestListAllTablesByShowFullTables(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	tctx := tcontext.Background().WithLogger(appLogger)

	// Test list all tables and skipping views.
	data := NewDatabaseTables().
		AppendTables("db1", []string{"t1", "t2"}, []uint64{1, 2}).
		AppendTables("db2", []string{"t3", "t4", "t5"}, []uint64{3, 4, 5}).
		AppendViews("db3", "t6", "t7", "t8")

	query := "SHOW FULL TABLES FROM `%s` WHERE TABLE_TYPE='BASE TABLE'"
	dbNames := make([]databaseName, 0, len(data))
	for dbName, tableInfos := range data {
		dbNames = append(dbNames, dbName)
		columnNames := []string{strings.ToUpper(fmt.Sprintf("Tables_in_%s", dbName)), "TABLE_TYPE"}
		rows := sqlmock.NewRows(columnNames)
		for _, tbInfo := range tableInfos {
			if tbInfo.Type == TableTypeBase {
				rows.AddRow(tbInfo.Name, TableTypeBase.String())
			} else {
				rows.AddRow(tbInfo.Name, TableTypeView.String())
			}
		}
		mock.ExpectQuery(fmt.Sprintf(query, dbName)).WillReturnRows(rows)
	}

	tables, err := ListAllDatabasesTables(tctx, conn, dbNames, listTableByShowFullTables, TableTypeBase)
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
	query = "SHOW FULL TABLES FROM `%s` WHERE TABLE_TYPE='BASE TABLE' OR TABLE_TYPE='VIEW'"
	data = NewDatabaseTables().
		AppendTables("db", []string{"t1"}, []uint64{1}).
		AppendViews("db", "t2")
	for dbName, tableInfos := range data {
		columnNames := []string{strings.ToUpper(fmt.Sprintf("Tables_in_%s", dbName)), "TABLE_TYPE"}
		rows := sqlmock.NewRows(columnNames)
		for _, tbInfo := range tableInfos {
			if tbInfo.Type == TableTypeBase {
				rows.AddRow(tbInfo.Name, TableTypeBase.String())
			} else {
				rows.AddRow(tbInfo.Name, TableTypeView.String())
			}
		}
		mock.ExpectQuery(fmt.Sprintf(query, dbName)).WillReturnRows(rows)
	}
	tables, err = ListAllDatabasesTables(tctx, conn, []string{"db"}, listTableByShowFullTables, TableTypeBase, TableTypeView)
	c.Assert(err, IsNil)
	c.Assert(len(tables), Equals, 1)
	c.Assert(len(tables["db"]), Equals, 2)
	for i := 0; i < len(tables["db"]); i++ {
		cmt := Commentf("%v mismatch: %v", tables["db"][i], data["db"][i])
		c.Assert(tables["db"][i].Equals(data["db"][i]), IsTrue, cmt)
	}

	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testPrepareSuite) TestConfigValidation(c *C) {
	conf := defaultConfigForTest(c)
	conf.Where = "id < 5"
	conf.SQL = "select * from t where id > 3"
	c.Assert(validateSpecifiedSQL(conf), ErrorMatches, "can't specify both --sql and --where at the same time. Please try to combine them into --sql")
	conf.Where = ""
	c.Assert(validateSpecifiedSQL(conf), IsNil)

	conf.FileType = FileFormatSQLTextString
	c.Assert(adjustFileFormat(conf), ErrorMatches, ".*please unset --filetype or set it to 'csv'.*")
	conf.FileType = FileFormatCSVString
	c.Assert(adjustFileFormat(conf), IsNil)
	conf.FileType = ""
	c.Assert(adjustFileFormat(conf), IsNil)
	c.Assert(conf.FileType, Equals, FileFormatCSVString)
	conf.SQL = ""
	conf.FileType = FileFormatSQLTextString
	c.Assert(adjustFileFormat(conf), IsNil)
	conf.FileType = ""
	c.Assert(adjustFileFormat(conf), IsNil)
	c.Assert(conf.FileType, Equals, FileFormatSQLTextString)

	conf.FileType = "rand_str"
	c.Assert(adjustFileFormat(conf), ErrorMatches, "unknown config.FileType 'rand_str'")
}
