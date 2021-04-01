// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"database/sql"
	"errors"

	tcontext "github.com/pingcap/dumpling/v4/context"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/coreos/go-semver/semver"
	. "github.com/pingcap/check"
)

var _ = Suite(&testSQLSuite{})

type testSQLSuite struct{}

func (s *testSQLSuite) TestDetectServerInfo(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	mkVer := makeVersion
	data := [][]interface{}{
		{1, "8.0.18", ServerTypeMySQL, mkVer(8, 0, 18, "")},
		{2, "10.4.10-MariaDB-1:10.4.10+maria~bionic", ServerTypeMariaDB, mkVer(10, 4, 10, "MariaDB-1")},
		{3, "5.7.25-TiDB-v4.0.0-alpha-1263-g635f2e1af", ServerTypeTiDB, mkVer(4, 0, 0, "alpha-1263-g635f2e1af")},
		{4, "5.7.25-TiDB-v3.0.7-58-g6adce2367", ServerTypeTiDB, mkVer(3, 0, 7, "58-g6adce2367")},
		{5, "5.7.25-TiDB-3.0.6", ServerTypeTiDB, mkVer(3, 0, 6, "")},
		{6, "invalid version", ServerTypeUnknown, (*semver.Version)(nil)},
	}
	dec := func(d []interface{}) (tag int, verStr string, tp ServerType, v *semver.Version) {
		return d[0].(int), d[1].(string), ServerType(d[2].(int)), d[3].(*semver.Version)
	}

	for _, datum := range data {
		tag, r, serverTp, expectVer := dec(datum)
		cmt := Commentf("test case number: %d", tag)

		rows := sqlmock.NewRows([]string{"version"}).AddRow(r)
		mock.ExpectQuery("SELECT version()").WillReturnRows(rows)

		verStr, err := SelectVersion(db)
		c.Assert(err, IsNil, cmt)
		info := ParseServerInfo(tcontext.Background(), verStr)
		c.Assert(info.ServerType, Equals, serverTp, cmt)
		c.Assert(info.ServerVersion == nil, Equals, expectVer == nil, cmt)
		if info.ServerVersion == nil {
			c.Assert(expectVer, IsNil, cmt)
		} else {
			c.Assert(info.ServerVersion.Equal(*expectVer), IsTrue)
		}
		c.Assert(mock.ExpectationsWereMet(), IsNil, cmt)
	}
}

func (s *testSQLSuite) TestBuildSelectAllQuery(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)

	mockConf := defaultConfigForTest(c)
	mockConf.SortByPk = true

	// Test TiDB server.
	mockConf.ServerInfo.ServerType = ServerTypeTiDB

	// _tidb_rowid is available.
	mock.ExpectExec("SELECT _tidb_rowid from `test`.`t`").
		WillReturnResult(sqlmock.NewResult(0, 0))

	orderByClause, err := buildOrderByClause(mockConf, conn, "test", "t")
	c.Assert(err, IsNil)

	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "extra"}).AddRow("id", ""))

	selectedField, _, err := buildSelectField(conn, "test", "t", false)
	c.Assert(err, IsNil)
	q := buildSelectQuery("test", "t", selectedField, "", orderByClause)
	c.Assert(q, Equals, "SELECT * FROM `test`.`t` ORDER BY `_tidb_rowid`")

	// _tidb_rowid is unavailable, or PKIsHandle.
	mock.ExpectExec("SELECT _tidb_rowid from `test`.`t`").
		WillReturnError(errors.New(`1054, "Unknown column '_tidb_rowid' in 'field list'"`))

	mock.ExpectQuery("SELECT column_name FROM information_schema.KEY_COLUMN_USAGE").
		WithArgs("test", "t").
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow("id"))

	orderByClause, err = buildOrderByClause(mockConf, conn, "test", "t")
	c.Assert(err, IsNil)

	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "extra"}).AddRow("id", ""))

	selectedField, _, err = buildSelectField(conn, "test", "t", false)
	c.Assert(err, IsNil)
	q = buildSelectQuery("test", "t", selectedField, "", orderByClause)
	c.Assert(q, Equals, "SELECT * FROM `test`.`t` ORDER BY `id`")
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	// Test other servers.
	otherServers := []ServerType{ServerTypeUnknown, ServerTypeMySQL, ServerTypeMariaDB}

	// Test table with primary key.
	for _, serverTp := range otherServers {
		mockConf.ServerInfo.ServerType = serverTp
		cmt := Commentf("server type: %s", serverTp)
		mock.ExpectQuery("SELECT column_name FROM information_schema.KEY_COLUMN_USAGE").
			WithArgs("test", "t").
			WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow("id"))
		orderByClause, err := buildOrderByClause(mockConf, conn, "test", "t")
		c.Assert(err, IsNil, cmt)

		mock.ExpectQuery("SELECT COLUMN_NAME").
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"column_name", "extra"}).AddRow("id", ""))

		selectedField, _, err = buildSelectField(conn, "test", "t", false)
		c.Assert(err, IsNil)
		q = buildSelectQuery("test", "t", selectedField, "", orderByClause)
		c.Assert(q, Equals, "SELECT * FROM `test`.`t` ORDER BY `id`", cmt)
		err = mock.ExpectationsWereMet()
		c.Assert(err, IsNil, cmt)
		c.Assert(mock.ExpectationsWereMet(), IsNil, cmt)
	}

	// Test table without primary key.
	for _, serverTp := range otherServers {
		mockConf.ServerInfo.ServerType = serverTp
		cmt := Commentf("server type: %s", serverTp)
		mock.ExpectQuery("SELECT column_name FROM information_schema.KEY_COLUMN_USAGE").
			WithArgs("test", "t").
			WillReturnRows(sqlmock.NewRows([]string{"column_name"}))

		orderByClause, err := buildOrderByClause(mockConf, conn, "test", "t")
		c.Assert(err, IsNil, cmt)

		mock.ExpectQuery("SELECT COLUMN_NAME").
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"column_name", "extra"}).AddRow("id", ""))

		selectedField, _, err = buildSelectField(conn, "test", "t", false)
		c.Assert(err, IsNil)
		q := buildSelectQuery("test", "t", selectedField, "", orderByClause)
		c.Assert(q, Equals, "SELECT * FROM `test`.`t`", cmt)
		err = mock.ExpectationsWereMet()
		c.Assert(err, IsNil, cmt)
		c.Assert(mock.ExpectationsWereMet(), IsNil)
	}

	// Test when config.SortByPk is disabled.
	mockConf.SortByPk = false
	for tp := ServerTypeUnknown; tp < ServerTypeAll; tp++ {
		mockConf.ServerInfo.ServerType = ServerType(tp)
		cmt := Commentf("current server type: ", tp)

		mock.ExpectQuery("SELECT COLUMN_NAME").
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"column_name", "extra"}).AddRow("id", ""))

		selectedField, _, err := buildSelectField(conn, "test", "t", false)
		c.Assert(err, IsNil)
		q := buildSelectQuery("test", "t", selectedField, "", "")
		c.Assert(q, Equals, "SELECT * FROM `test`.`t`", cmt)
		c.Assert(mock.ExpectationsWereMet(), IsNil, cmt)
	}
}

func (s *testSQLSuite) TestBuildOrderByClause(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)

	mockConf := defaultConfigForTest(c)
	mockConf.SortByPk = true

	// Test TiDB server.
	mockConf.ServerInfo.ServerType = ServerTypeTiDB

	// _tidb_rowid is available.
	mock.ExpectExec("SELECT _tidb_rowid from `test`.`t`").
		WillReturnResult(sqlmock.NewResult(0, 0))

	orderByClause, err := buildOrderByClause(mockConf, conn, "test", "t")
	c.Assert(err, IsNil)
	c.Assert(orderByClause, Equals, "ORDER BY `_tidb_rowid`")

	// _tidb_rowid is unavailable, or PKIsHandle.
	mock.ExpectExec("SELECT _tidb_rowid from `test`.`t`").
		WillReturnError(errors.New(`1054, "Unknown column '_tidb_rowid' in 'field list'"`))

	mock.ExpectQuery("SELECT column_name FROM information_schema.KEY_COLUMN_USAGE").
		WithArgs("test", "t").
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow("id"))

	orderByClause, err = buildOrderByClause(mockConf, conn, "test", "t")
	c.Assert(err, IsNil)
	c.Assert(orderByClause, Equals, "ORDER BY `id`")

	// Test other servers.
	otherServers := []ServerType{ServerTypeUnknown, ServerTypeMySQL, ServerTypeMariaDB}

	// Test table with primary key.
	for _, serverTp := range otherServers {
		mockConf.ServerInfo.ServerType = serverTp
		cmt := Commentf("server type: %s", serverTp)
		mock.ExpectQuery("SELECT column_name FROM information_schema.KEY_COLUMN_USAGE").
			WithArgs("test", "t").
			WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow("id"))
		orderByClause, err := buildOrderByClause(mockConf, conn, "test", "t")
		c.Assert(err, IsNil, cmt)
		c.Assert(orderByClause, Equals, "ORDER BY `id`", cmt)
	}

	// Test table with joint primary key.
	for _, serverTp := range otherServers {
		mockConf.ServerInfo.ServerType = serverTp
		cmt := Commentf("server type: %s", serverTp)
		mock.ExpectQuery("SELECT column_name FROM information_schema.KEY_COLUMN_USAGE").
			WithArgs("test", "t").
			WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow("id").AddRow("name"))
		orderByClause, err := buildOrderByClause(mockConf, conn, "test", "t")
		c.Assert(err, IsNil, cmt)
		c.Assert(orderByClause, Equals, "ORDER BY `id`,`name`", cmt)
	}

	// Test table without primary key.
	for _, serverTp := range otherServers {
		mockConf.ServerInfo.ServerType = serverTp
		cmt := Commentf("server type: %s", serverTp)
		mock.ExpectQuery("SELECT column_name FROM information_schema.KEY_COLUMN_USAGE").
			WithArgs("test", "t").
			WillReturnRows(sqlmock.NewRows([]string{"column_name"}))

		orderByClause, err := buildOrderByClause(mockConf, conn, "test", "t")
		c.Assert(err, IsNil, cmt)
		c.Assert(orderByClause, Equals, "", cmt)
	}

	// Test when config.SortByPk is disabled.
	mockConf.SortByPk = false
	for tp := ServerTypeUnknown; tp < ServerTypeAll; tp++ {
		mockConf.ServerInfo.ServerType = ServerType(tp)
		cmt := Commentf("current server type: ", tp)

		orderByClause, err := buildOrderByClause(mockConf, conn, "test", "t")
		c.Assert(err, IsNil, cmt)
		c.Assert(orderByClause, Equals, "", cmt)
	}
}

func (s *testSQLSuite) TestBuildSelectField(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)

	// generate columns not found
	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "extra"}).AddRow("id", ""))

	selectedField, _, err := buildSelectField(conn, "test", "t", false)
	c.Assert(selectedField, Equals, "*")
	c.Assert(err, IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	// user assigns completeInsert
	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "extra"}).AddRow("id", "").
			AddRow("name", "").AddRow("quo`te", ""))

	selectedField, _, err = buildSelectField(conn, "test", "t", true)
	c.Assert(selectedField, Equals, "`id`,`name`,`quo``te`")
	c.Assert(err, IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	// found generate columns, rest columns is `id`,`name`
	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "extra"}).
			AddRow("id", "").AddRow("name", "").AddRow("quo`te", "").AddRow("generated", "VIRTUAL GENERATED"))

	selectedField, _, err = buildSelectField(conn, "test", "t", false)
	c.Assert(selectedField, Equals, "`id`,`name`,`quo``te`")
	c.Assert(err, IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testSQLSuite) TestParseSnapshotToTSO(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	snapshot := "2020/07/18 20:31:50"
	var unixTimeStamp uint64 = 1595075510
	// generate columns valid snapshot
	mock.ExpectQuery(`SELECT unix_timestamp(?)`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{`unix_timestamp("2020/07/18 20:31:50")`}).AddRow(1595075510))
	tso, err := parseSnapshotToTSO(db, snapshot)
	c.Assert(err, IsNil)
	c.Assert(tso, Equals, (unixTimeStamp<<18)*1000+1)
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	// generate columns not valid snapshot
	mock.ExpectQuery(`SELECT unix_timestamp(?)`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{`unix_timestamp("XXYYZZ")`}).AddRow(nil))
	tso, err = parseSnapshotToTSO(db, "XXYYZZ")
	c.Assert(err, ErrorMatches, "snapshot XXYYZZ format not supported. please use tso or '2006-01-02 15:04:05' format time")
	c.Assert(tso, Equals, uint64(0))
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testSQLSuite) TestShowCreateView(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)

	mock.ExpectQuery("SHOW FIELDS FROM `test`.`v`").
		WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
			AddRow("a", "int(11)", "YES", nil, "NULL", nil))

	mock.ExpectQuery("SHOW CREATE VIEW `test`.`v`").
		WillReturnRows(sqlmock.NewRows([]string{"View", "Create View", "character_set_client", "collation_connection"}).
			AddRow("v", "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `v` (`a`) AS SELECT `t`.`a` AS `a` FROM `test`.`t`", "utf8", "utf8_general_ci"))

	createTableSQL, createViewSQL, err := ShowCreateView(conn, "test", "v")
	c.Assert(err, IsNil)
	c.Assert(createTableSQL, Equals, "CREATE TABLE `v`(\n`a` int\n)ENGINE=MyISAM;\n")
	c.Assert(createViewSQL, Equals, "DROP TABLE IF EXISTS `v`;\nDROP VIEW IF EXISTS `v`;\nSET @PREV_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;\nSET @PREV_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS;\nSET @PREV_COLLATION_CONNECTION=@@COLLATION_CONNECTION;\nSET character_set_client = utf8;\nSET character_set_results = utf8;\nSET collation_connection = utf8_general_ci;\nCREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `v` (`a`) AS SELECT `t`.`a` AS `a` FROM `test`.`t`;\nSET character_set_client = @PREV_CHARACTER_SET_CLIENT;\nSET character_set_results = @PREV_CHARACTER_SET_RESULTS;\nSET collation_connection = @PREV_COLLATION_CONNECTION;\n")
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testSQLSuite) TestGetSuitableRows(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	const query = "select AVG_ROW_LENGTH from INFORMATION_SCHEMA.TABLES where table_schema=\\? and table_name=\\?;"
	tctx, cancel := tcontext.Background().WithCancel()
	defer cancel()
	database := "foo"
	table := "bar"

	testCases := []struct {
		avgRowLength uint64
		expectedRows uint64
		returnErr    error
	}{
		{
			32,
			200000,
			sql.ErrNoRows,
		},
		{
			0,
			200000,
			nil,
		},
		{
			32,
			1000000,
			nil,
		},
		{
			1024,
			131072,
			nil,
		},
		{
			4096,
			32768,
			nil,
		},
	}
	for _, testCase := range testCases {
		if testCase.returnErr == nil {
			mock.ExpectQuery(query).WithArgs(database, table).
				WillReturnRows(sqlmock.NewRows([]string{"AVG_ROW_LENGTH"}).
					AddRow(testCase.avgRowLength))
		} else {
			mock.ExpectQuery(query).WithArgs(database, table).
				WillReturnError(testCase.returnErr)
		}
		rows := GetSuitableRows(tctx, conn, database, table)
		c.Assert(rows, Equals, testCase.expectedRows)
	}
}

func (s *testSQLSuite) TestBuildWhereClauses(c *C) {
	testCases := []struct {
		handleColNames       []string
		handleVals           [][]string
		expectedWhereClauses []string
	}{
		{
			[]string{},
			[][]string{},
			nil,
		},
		{
			[]string{"a"},
			[][]string{{"1"}},
			[]string{"`a`<1", "`a`>=1"},
		},
		{
			[]string{"a"},
			[][]string{
				{"1"},
				{"2"},
				{"3"},
			},
			[]string{"`a`<1", "`a`>=1 and `a`<2", "`a`>=2 and `a`<3", "`a`>=3"},
		},
		{
			[]string{"a", "b"},
			[][]string{{"1", "2"}},
			[]string{"`a`<1 or(`a`=1 and `b`<2)", "`a`>1 or(`a`=1 and `b`>=2)"},
		},
		{
			[]string{"a", "b"},
			[][]string{
				{"1", "2"},
				{"3", "4"},
				{"5", "6"},
			},
			[]string{
				"`a`<1 or(`a`=1 and `b`<2)",
				"(`a`>1 and `a`<3)or(`a`=1 and(`b`>=2))or(`a`=3 and(`b`<4))",
				"(`a`>3 and `a`<5)or(`a`=3 and(`b`>=4))or(`a`=5 and(`b`<6))",
				"`a`>5 or(`a`=5 and `b`>=6)",
			},
		},
		{
			[]string{"a", "b", "c"},
			[][]string{
				{"1", "2", "3"},
				{"4", "5", "6"},
			},
			[]string{
				"`a`<1 or(`a`=1 and `b`<2)or(`a`=1 and `b`=2 and `c`<3)",
				"(`a`>1 and `a`<4)or(`a`=1 and(`b`>2 or(`b`=2 and `c`>=3)))or(`a`=4 and(`b`<5 or(`b`=5 and `c`<6)))",
				"`a`>4 or(`a`=4 and `b`>5)or(`a`=4 and `b`=5 and `c`>=6)",
			},
		},
		{
			[]string{"a", "b", "c"},
			[][]string{
				{"1", "2", "3"},
				{"1", "4", "5"},
			},
			[]string{
				"`a`<1 or(`a`=1 and `b`<2)or(`a`=1 and `b`=2 and `c`<3)",
				"`a`=1 and((`b`>2 and `b`<4)or(`b`=2 and(`c`>=3))or(`b`=4 and(`c`<5)))",
				"`a`>1 or(`a`=1 and `b`>4)or(`a`=1 and `b`=4 and `c`>=5)",
			},
		},
		{
			[]string{"a", "b", "c"},
			[][]string{
				{"1", "2", "3"},
				{"1", "2", "8"},
			},
			[]string{
				"`a`<1 or(`a`=1 and `b`<2)or(`a`=1 and `b`=2 and `c`<3)",
				"`a`=1 and `b`=2 and(`c`>=3 and `c`<8)",
				"`a`>1 or(`a`=1 and `b`>2)or(`a`=1 and `b`=2 and `c`>=8)",
			},
		},
		// special case: avoid return same samples
		{
			[]string{"a", "b", "c"},
			[][]string{
				{"1", "2", "3"},
				{"1", "2", "3"},
			},
			[]string{
				"`a`<1 or(`a`=1 and `b`<2)or(`a`=1 and `b`=2 and `c`<3)",
				"false",
				"`a`>1 or(`a`=1 and `b`>2)or(`a`=1 and `b`=2 and `c`>=3)",
			},
		},
		// test string fields
		{
			[]string{"a", "b", "c"},
			[][]string{
				{"1", "2", "\"3\""},
				{"1", "4", "\"5\""},
			},
			[]string{
				"`a`<1 or(`a`=1 and `b`<2)or(`a`=1 and `b`=2 and `c`<\"3\")",
				"`a`=1 and((`b`>2 and `b`<4)or(`b`=2 and(`c`>=\"3\"))or(`b`=4 and(`c`<\"5\")))",
				"`a`>1 or(`a`=1 and `b`>4)or(`a`=1 and `b`=4 and `c`>=\"5\")",
			},
		},
		{
			[]string{"a", "b", "c", "d"},
			[][]string{
				{"1", "2", "3", "4"},
				{"5", "6", "7", "8"},
			},
			[]string{
				"`a`<1 or(`a`=1 and `b`<2)or(`a`=1 and `b`=2 and `c`<3)or(`a`=1 and `b`=2 and `c`=3 and `d`<4)",
				"(`a`>1 and `a`<5)or(`a`=1 and(`b`>2 or(`b`=2 and `c`>3)or(`b`=2 and `c`=3 and `d`>=4)))or(`a`=5 and(`b`<6 or(`b`=6 and `c`<7)or(`b`=6 and `c`=7 and `d`<8)))",
				"`a`>5 or(`a`=5 and `b`>6)or(`a`=5 and `b`=6 and `c`>7)or(`a`=5 and `b`=6 and `c`=7 and `d`>=8)",
			},
		},
	}
	for _, testCase := range testCases {
		whereClauses := buildWhereClauses(testCase.handleColNames, testCase.handleVals)
		c.Assert(whereClauses, DeepEquals, testCase.expectedWhereClauses)
	}
}

func makeVersion(major, minor, patch int64, preRelease string) *semver.Version {
	return &semver.Version{
		Major:      major,
		Minor:      minor,
		Patch:      patch,
		PreRelease: semver.PreRelease(preRelease),
		Metadata:   "",
	}
}
