// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tidb/br/pkg/version"
	dbconfig "github.com/pingcap/tidb/config"
	tcontext "github.com/pingcap/tidb/dumpling/context"
)

var showIndexHeaders = []string{
	"Table",
	"Non_unique",
	"Key_name",
	"Seq_in_index",
	"Column_name",
	"Collation",
	"Cardinality",
	"Sub_part",
	"Packed",
	"Null",
	"Index_type",
	"Comment",
	"Index_comment",
}

const (
	database = "foo"
	table    = "bar"
)

func TestBuildSelectAllQuery(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	mockConf := defaultConfigForTest(t)
	mockConf.SortByPk = true

	// Test TiDB server.
	mockConf.ServerInfo.ServerType = version.ServerTypeTiDB
	tctx := tcontext.Background().WithLogger(appLogger)
	baseConn := newBaseConn(conn, true, nil)

	orderByClause, err := buildOrderByClause(tctx, mockConf, baseConn, database, table, true)
	require.NoError(t, err)

	mock.ExpectQuery("SHOW COLUMNS FROM").
		WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
			AddRow("id", "int(11)", "NO", "PRI", nil, ""))

	selectedField, _, err := buildSelectField(tctx, baseConn, database, table, false)
	require.NoError(t, err)

	q := buildSelectQuery(database, table, selectedField, "", "", orderByClause)
	require.Equal(t, fmt.Sprintf("SELECT * FROM `%s`.`%s` ORDER BY `_tidb_rowid`", database, table), q)

	mock.ExpectQuery(fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", database, table)).
		WillReturnRows(sqlmock.NewRows(showIndexHeaders).
			AddRow(table, 0, "PRIMARY", 1, "id", "A", 0, nil, nil, "", "BTREE", "", ""))

	orderByClause, err = buildOrderByClause(tctx, mockConf, baseConn, database, table, false)
	require.NoError(t, err)

	mock.ExpectQuery("SHOW COLUMNS FROM").
		WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
			AddRow("id", "int(11)", "NO", "PRI", nil, ""))

	selectedField, _, err = buildSelectField(tctx, baseConn, database, table, false)
	require.NoError(t, err)

	q = buildSelectQuery(database, table, selectedField, "", "", orderByClause)
	require.Equal(t, fmt.Sprintf("SELECT * FROM `%s`.`%s` ORDER BY `id`", database, table), q)
	require.NoError(t, mock.ExpectationsWereMet())

	// Test other servers.
	otherServers := []version.ServerType{version.ServerTypeUnknown, version.ServerTypeMySQL, version.ServerTypeMariaDB}

	// Test table with primary key.
	for _, serverTp := range otherServers {
		mockConf.ServerInfo.ServerType = serverTp
		comment := fmt.Sprintf("server type: %s", serverTp)

		mock.ExpectQuery(fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", database, table)).
			WillReturnRows(sqlmock.NewRows(showIndexHeaders).
				AddRow(table, 0, "PRIMARY", 1, "id", "A", 0, nil, nil, "", "BTREE", "", ""))
		orderByClause, err := buildOrderByClause(tctx, mockConf, baseConn, database, table, false)
		require.NoError(t, err, comment)

		mock.ExpectQuery("SHOW COLUMNS FROM").
			WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
				AddRow("id", "int(11)", "NO", "PRI", nil, ""))

		selectedField, _, err = buildSelectField(tctx, baseConn, database, table, false)
		require.NoError(t, err, comment)

		q = buildSelectQuery(database, table, selectedField, "", "", orderByClause)
		require.Equal(t, fmt.Sprintf("SELECT * FROM `%s`.`%s` ORDER BY `id`", database, table), q, comment)

		err = mock.ExpectationsWereMet()
		require.NoError(t, err, comment)
		require.NoError(t, mock.ExpectationsWereMet(), comment)
	}

	// Test table without primary key.
	for _, serverTp := range otherServers {
		mockConf.ServerInfo.ServerType = serverTp
		comment := fmt.Sprintf("server type: %s", serverTp)

		mock.ExpectQuery(fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", database, table)).
			WillReturnRows(sqlmock.NewRows(showIndexHeaders))

		orderByClause, err := buildOrderByClause(tctx, mockConf, baseConn, database, table, false)
		require.NoError(t, err, comment)

		mock.ExpectQuery("SHOW COLUMNS FROM").
			WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
				AddRow("id", "int(11)", "NO", "PRI", nil, ""))

		selectedField, _, err = buildSelectField(tctx, baseConn, "test", "t", false)
		require.NoError(t, err, comment)

		q := buildSelectQuery(database, table, selectedField, "", "", orderByClause)
		require.Equal(t, fmt.Sprintf("SELECT * FROM `%s`.`%s`", database, table), q, comment)

		err = mock.ExpectationsWereMet()
		require.NoError(t, err, comment)
		require.NoError(t, mock.ExpectationsWereMet(), comment)
	}

	// Test when config.SortByPk is disabled.
	mockConf.SortByPk = false
	for tp := version.ServerTypeUnknown; tp < version.ServerTypeAll; tp++ {
		mockConf.ServerInfo.ServerType = version.ServerType(tp)
		comment := fmt.Sprintf("current server type: %v", tp)

		mock.ExpectQuery("SHOW COLUMNS FROM").
			WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
				AddRow("id", "int(11)", "NO", "PRI", nil, ""))

		selectedField, _, err := buildSelectField(tctx, baseConn, "test", "t", false)
		require.NoError(t, err, comment)

		q := buildSelectQuery(database, table, selectedField, "", "", "")
		require.Equal(t, fmt.Sprintf("SELECT * FROM `%s`.`%s`", database, table), q, comment)
		require.NoError(t, mock.ExpectationsWereMet(), comment)
	}
}

func TestBuildOrderByClause(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	tctx := tcontext.Background().WithLogger(appLogger)
	baseConn := newBaseConn(conn, true, nil)

	mockConf := defaultConfigForTest(t)
	mockConf.SortByPk = true

	// Test TiDB server.
	mockConf.ServerInfo.ServerType = version.ServerTypeTiDB

	orderByClause, err := buildOrderByClause(tctx, mockConf, baseConn, database, table, true)
	require.NoError(t, err)
	require.Equal(t, orderByTiDBRowID, orderByClause)

	mock.ExpectQuery(fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", database, table)).
		WillReturnRows(sqlmock.NewRows(showIndexHeaders).AddRow(table, 0, "PRIMARY", 1, "id", "A", 0, nil, nil, "", "BTREE", "", ""))

	orderByClause, err = buildOrderByClause(tctx, mockConf, baseConn, database, table, false)
	require.NoError(t, err)
	require.Equal(t, "ORDER BY `id`", orderByClause)

	// Test table with primary key.
	mock.ExpectQuery(fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", database, table)).
		WillReturnRows(sqlmock.NewRows(showIndexHeaders).AddRow(table, 0, "PRIMARY", 1, "id", "A", 0, nil, nil, "", "BTREE", "", ""))
	orderByClause, err = buildOrderByClause(tctx, mockConf, baseConn, database, table, false)
	require.NoError(t, err)
	require.Equal(t, "ORDER BY `id`", orderByClause)

	// Test table with joint primary key.
	mock.ExpectQuery(fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", database, table)).
		WillReturnRows(sqlmock.NewRows(showIndexHeaders).
			AddRow(table, 0, "PRIMARY", 1, "id", "A", 0, nil, nil, "", "BTREE", "", "").
			AddRow(table, 0, "PRIMARY", 2, "name", "A", 0, nil, nil, "", "BTREE", "", ""))
	orderByClause, err = buildOrderByClause(tctx, mockConf, baseConn, database, table, false)
	require.NoError(t, err)
	require.Equal(t, "ORDER BY `id`,`name`", orderByClause)

	// Test table without primary key.
	mock.ExpectQuery(fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", database, table)).
		WillReturnRows(sqlmock.NewRows(showIndexHeaders))

	orderByClause, err = buildOrderByClause(tctx, mockConf, baseConn, database, table, false)
	require.NoError(t, err)
	require.Equal(t, "", orderByClause)

	// Test when config.SortByPk is disabled.
	mockConf.SortByPk = false
	for _, hasImplicitRowID := range []bool{false, true} {
		comment := fmt.Sprintf("current hasImplicitRowID: %v", hasImplicitRowID)

		orderByClause, err := buildOrderByClause(tctx, mockConf, baseConn, database, table, hasImplicitRowID)
		require.NoError(t, err, comment)
		require.Equal(t, "", orderByClause, comment)
	}

	// Test build OrderByClause with retry
	baseConn = newBaseConn(conn, true, func(conn *sql.Conn, b bool) (*sql.Conn, error) {
		return conn, nil
	})
	query := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", database, table)
	mock.ExpectQuery(query).WillReturnError(errors.New("invalid connection"))
	mock.ExpectQuery(query).WillReturnError(errors.New("invalid connection"))
	mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(showIndexHeaders).AddRow(table, 0, "PRIMARY", 1, "id", "A", 0, nil, nil, "", "BTREE", "", ""))
	mockConf.SortByPk = true
	orderByClause, err = buildOrderByClause(tctx, mockConf, baseConn, database, table, false)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	require.Equal(t, "ORDER BY `id`", orderByClause)
}

func TestBuildSelectField(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	tctx := tcontext.Background().WithLogger(appLogger)
	baseConn := newBaseConn(conn, true, nil)

	// generate columns not found
	mock.ExpectQuery("SHOW COLUMNS FROM").
		WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
			AddRow("id", "int(11)", "NO", "PRI", nil, ""))

	selectedField, _, err := buildSelectField(tctx, baseConn, "test", "t", false)
	require.Equal(t, "*", selectedField)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())

	// user assigns completeInsert
	mock.ExpectQuery("SHOW COLUMNS FROM").
		WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
			AddRow("id", "int(11)", "NO", "PRI", nil, "").
			AddRow("name", "varchar(12)", "NO", "", nil, "").
			AddRow("quo`te", "varchar(12)", "NO", "UNI", nil, ""))

	selectedField, _, err = buildSelectField(tctx, baseConn, "test", "t", true)
	require.Equal(t, "`id`,`name`,`quo``te`", selectedField)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())

	// found generate columns, rest columns is `id`,`name`
	mock.ExpectQuery("SHOW COLUMNS FROM").
		WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
			AddRow("id", "int(11)", "NO", "PRI", nil, "").
			AddRow("name", "varchar(12)", "NO", "", nil, "").
			AddRow("quo`te", "varchar(12)", "NO", "UNI", nil, "").
			AddRow("generated", "varchar(12)", "NO", "", nil, "VIRTUAL GENERATED"))

	selectedField, _, err = buildSelectField(tctx, baseConn, "test", "t", false)
	require.Equal(t, "`id`,`name`,`quo``te`", selectedField)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())

	// Test build SelectField with retry
	baseConn = newBaseConn(conn, true, func(conn *sql.Conn, b bool) (*sql.Conn, error) {
		return conn, nil
	})
	mock.ExpectQuery("SHOW COLUMNS FROM").WillReturnError(errors.New("invalid connection"))
	mock.ExpectQuery("SHOW COLUMNS FROM").WillReturnError(errors.New("invalid connection"))
	mock.ExpectQuery("SHOW COLUMNS FROM").
		WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
			AddRow("id", "int(11)", "NO", "PRI", nil, ""))

	selectedField, _, err = buildSelectField(tctx, baseConn, "test", "t", false)
	require.Equal(t, "*", selectedField)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestParseSnapshotToTSO(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	snapshot := "2020/07/18 20:31:50"
	var unixTimeStamp uint64 = 1595075510
	// generate columns valid snapshot
	mock.ExpectQuery(`SELECT unix_timestamp(?)`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{`unix_timestamp("2020/07/18 20:31:50")`}).AddRow(1595075510))
	tso, err := parseSnapshotToTSO(db, snapshot)
	require.NoError(t, err)
	require.Equal(t, (unixTimeStamp<<18)*1000, tso)
	require.NoError(t, mock.ExpectationsWereMet())

	// generate columns not valid snapshot
	mock.ExpectQuery(`SELECT unix_timestamp(?)`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{`unix_timestamp("XXYYZZ")`}).AddRow(nil))
	tso, err = parseSnapshotToTSO(db, "XXYYZZ")
	require.EqualError(t, err, "snapshot XXYYZZ format not supported. please use tso or '2006-01-02 15:04:05' format time")
	require.Equal(t, uint64(0), tso)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestShowCreateView(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	tctx := tcontext.Background().WithLogger(appLogger)
	baseConn := newBaseConn(conn, true, nil)

	mock.ExpectQuery("SHOW FIELDS FROM `test`.`v`").
		WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
			AddRow("a", "int(11)", "YES", nil, "NULL", nil))

	mock.ExpectQuery("SHOW CREATE VIEW `test`.`v`").
		WillReturnRows(sqlmock.NewRows([]string{"View", "Create View", "character_set_client", "collation_connection"}).
			AddRow("v", "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `v` (`a`) AS SELECT `t`.`a` AS `a` FROM `test`.`t`", "utf8", "utf8_general_ci"))

	createTableSQL, createViewSQL, err := ShowCreateView(tctx, baseConn, "test", "v")
	require.NoError(t, err)
	require.Equal(t, "CREATE TABLE `v`(\n`a` int\n)ENGINE=MyISAM;\n", createTableSQL)
	require.Equal(t, "DROP TABLE IF EXISTS `v`;\nDROP VIEW IF EXISTS `v`;\nSET @PREV_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;\nSET @PREV_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS;\nSET @PREV_COLLATION_CONNECTION=@@COLLATION_CONNECTION;\nSET character_set_client = utf8;\nSET character_set_results = utf8;\nSET collation_connection = utf8_general_ci;\nCREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `v` (`a`) AS SELECT `t`.`a` AS `a` FROM `test`.`t`;\nSET character_set_client = @PREV_CHARACTER_SET_CLIENT;\nSET character_set_results = @PREV_CHARACTER_SET_RESULTS;\nSET collation_connection = @PREV_COLLATION_CONNECTION;\n", createViewSQL)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestShowCreateSequence(t *testing.T) {
	conf := defaultConfigForTest(t)
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	tctx := tcontext.Background().WithLogger(appLogger)
	baseConn := newBaseConn(conn, true, nil)

	conf.ServerInfo.ServerType = version.ServerTypeTiDB
	mock.ExpectQuery("SHOW CREATE SEQUENCE `test`.`s`").
		WillReturnRows(sqlmock.NewRows([]string{"Sequence", "Create Sequence"}).
			AddRow("s", "CREATE SEQUENCE `s` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))
	mock.ExpectQuery("SHOW TABLE `test`.`s` NEXT_ROW_ID").
		WillReturnRows(sqlmock.NewRows([]string{"DB_NAME", "TABLE_NAME", "COLUMN_NAME", "NEXT_GLOBAL_ROW_ID", "ID_TYPE"}).
			AddRow("test", "s", nil, 1001, "SEQUENCE"))

	createSequenceSQL, err := ShowCreateSequence(tctx, baseConn, "test", "s", conf)
	require.NoError(t, err)
	require.Equal(t, "CREATE SEQUENCE `s` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB;\nSELECT SETVAL(`s`,1001);\n", createSequenceSQL)
	require.NoError(t, mock.ExpectationsWereMet())

	conf.ServerInfo.ServerType = version.ServerTypeMariaDB
	mock.ExpectQuery("SHOW CREATE SEQUENCE `test`.`s`").
		WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("s", "CREATE SEQUENCE `s` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))
	mock.ExpectQuery("SELECT NEXT_NOT_CACHED_VALUE FROM `test`.`s`").
		WillReturnRows(sqlmock.NewRows([]string{"next_not_cached_value"}).
			AddRow(1001))

	createSequenceSQL, err = ShowCreateSequence(tctx, baseConn, "test", "s", conf)
	require.NoError(t, err)
	require.Equal(t, "CREATE SEQUENCE `s` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB;\nSELECT SETVAL(`s`,1001);\n", createSequenceSQL)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestShowCreatePolicy(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	tctx := tcontext.Background().WithLogger(appLogger)
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	baseConn := newBaseConn(conn, true, nil)

	mock.ExpectQuery("SHOW CREATE PLACEMENT POLICY `policy_x`").
		WillReturnRows(sqlmock.NewRows([]string{"Policy", "Create Policy"}).
			AddRow("policy_x", "CREATE PLACEMENT POLICY `policy_x` LEARNERS=1"))

	createPolicySQL, err := ShowCreatePlacementPolicy(tctx, baseConn, "policy_x")
	require.NoError(t, err)
	require.Equal(t, "CREATE PLACEMENT POLICY `policy_x` LEARNERS=1", createPolicySQL)
	require.NoError(t, mock.ExpectationsWereMet())

}

func TestListPolicyNames(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	tctx := tcontext.Background().WithLogger(appLogger)
	conn, err := db.Conn(context.Background())
	baseConn := newBaseConn(conn, true, nil)
	require.NoError(t, err)

	mock.ExpectQuery("select distinct policy_name from information_schema.placement_policies where policy_name is not null;").
		WillReturnRows(sqlmock.NewRows([]string{"policy_name"}).
			AddRow("policy_x"))
	policies, err := ListAllPlacementPolicyNames(tctx, baseConn)
	require.NoError(t, err)
	require.Equal(t, []string{"policy_x"}, policies)
	require.NoError(t, mock.ExpectationsWereMet())

	// some old tidb version doesn't support placement rules returns error
	expectedErr := &mysql.MySQLError{Number: ErrNoSuchTable, Message: "Table 'information_schema.placement_policies' doesn't exist"}
	mock.ExpectExec("select distinct policy_name from information_schema.placement_policies where policy_name is not null;").
		WillReturnError(expectedErr)
	_, err = ListAllPlacementPolicyNames(tctx, baseConn)
	if mysqlErr, ok := err.(*mysql.MySQLError); ok {
		require.Equal(t, mysqlErr.Number, ErrNoSuchTable)
	}
}

func TestGetSuitableRows(t *testing.T) {
	testCases := []struct {
		avgRowLength uint64
		expectedRows uint64
	}{
		{
			0,
			200000,
		},
		{
			32,
			1000000,
		},
		{
			1024,
			131072,
		},
		{
			4096,
			32768,
		},
	}
	for _, testCase := range testCases {
		rows := GetSuitableRows(testCase.avgRowLength)
		require.Equal(t, testCase.expectedRows, rows)
	}
}

func TestSelectTiDBRowID(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	tctx := tcontext.Background().WithLogger(appLogger)
	baseConn := newBaseConn(conn, true, nil)

	database, table := "test", "t"

	// _tidb_rowid is unavailable, or PKIsHandle.
	mock.ExpectExec("SELECT _tidb_rowid from `test`.`t`").
		WillReturnError(errors.New(`1054, "Unknown column '_tidb_rowid' in 'field list'"`))
	hasImplicitRowID, err := SelectTiDBRowID(tctx, baseConn, database, table)
	require.NoError(t, err)
	require.False(t, hasImplicitRowID)

	// _tidb_rowid is available.
	mock.ExpectExec("SELECT _tidb_rowid from `test`.`t`").
		WillReturnResult(sqlmock.NewResult(0, 0))
	hasImplicitRowID, err = SelectTiDBRowID(tctx, baseConn, database, table)
	require.NoError(t, err)
	require.True(t, hasImplicitRowID)

	// _tidb_rowid returns error
	expectedErr := errors.New("mock error")
	mock.ExpectExec("SELECT _tidb_rowid from `test`.`t`").
		WillReturnError(expectedErr)
	hasImplicitRowID, err = SelectTiDBRowID(tctx, baseConn, database, table)
	require.ErrorIs(t, errors.Cause(err), expectedErr)
	require.False(t, hasImplicitRowID)
}

func TestBuildTableSampleQueries(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	baseConn := newBaseConn(conn, true, nil)
	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()

	d := &Dumper{
		tctx:                      tctx,
		conf:                      DefaultConfig(),
		cancelCtx:                 cancel,
		selectTiDBTableRegionFunc: selectTiDBTableRegion,
	}
	d.conf.ServerInfo = version.ServerInfo{
		HasTiKV:       true,
		ServerType:    version.ServerTypeTiDB,
		ServerVersion: tableSampleVersion,
	}

	testCases := []struct {
		handleColNames       []string
		handleColTypes       []string
		handleVals           [][]driver.Value
		expectedWhereClauses []string
		hasTiDBRowID         bool
	}{
		{
			[]string{},
			[]string{},
			[][]driver.Value{},
			nil,
			false,
		},
		{
			[]string{"a"},
			[]string{"BIGINT"},
			[][]driver.Value{{1}},
			[]string{"`a`<1", "`a`>=1"},
			false,
		},
		// check whether dumpling can turn to dump whole table
		{
			[]string{"a"},
			[]string{"BIGINT"},
			[][]driver.Value{},
			nil,
			false,
		},
		// check whether dumpling can turn to dump whole table
		{
			[]string{"_tidb_rowid"},
			[]string{"BIGINT"},
			[][]driver.Value{},
			nil,
			true,
		},
		{
			[]string{"_tidb_rowid"},
			[]string{"BIGINT"},
			[][]driver.Value{{1}},
			[]string{"`_tidb_rowid`<1", "`_tidb_rowid`>=1"},
			true,
		},
		{
			[]string{"a"},
			[]string{"BIGINT"},
			[][]driver.Value{
				{1},
				{2},
				{3},
			},
			[]string{"`a`<1", "`a`>=1 and `a`<2", "`a`>=2 and `a`<3", "`a`>=3"},
			false,
		},
		{
			[]string{"a", "b"},
			[]string{"BIGINT", "BIGINT"},
			[][]driver.Value{{1, 2}},
			[]string{"`a`<1 or(`a`=1 and `b`<2)", "`a`>1 or(`a`=1 and `b`>=2)"},
			false,
		},
		{
			[]string{"a", "b"},
			[]string{"BIGINT", "BIGINT"},
			[][]driver.Value{
				{1, 2},
				{3, 4},
				{5, 6},
			},
			[]string{
				"`a`<1 or(`a`=1 and `b`<2)",
				"(`a`>1 and `a`<3)or(`a`=1 and(`b`>=2))or(`a`=3 and(`b`<4))",
				"(`a`>3 and `a`<5)or(`a`=3 and(`b`>=4))or(`a`=5 and(`b`<6))",
				"`a`>5 or(`a`=5 and `b`>=6)",
			},
			false,
		},
		{
			[]string{"a", "b", "c"},
			[]string{"BIGINT", "BIGINT", "BIGINT"},
			[][]driver.Value{
				{1, 2, 3},
				{4, 5, 6},
			},
			[]string{
				"`a`<1 or(`a`=1 and `b`<2)or(`a`=1 and `b`=2 and `c`<3)",
				"(`a`>1 and `a`<4)or(`a`=1 and(`b`>2 or(`b`=2 and `c`>=3)))or(`a`=4 and(`b`<5 or(`b`=5 and `c`<6)))",
				"`a`>4 or(`a`=4 and `b`>5)or(`a`=4 and `b`=5 and `c`>=6)",
			},
			false,
		},
		{
			[]string{"a", "b", "c"},
			[]string{"BIGINT", "BIGINT", "BIGINT"},
			[][]driver.Value{
				{1, 2, 3},
				{1, 4, 5},
			},
			[]string{
				"`a`<1 or(`a`=1 and `b`<2)or(`a`=1 and `b`=2 and `c`<3)",
				"`a`=1 and((`b`>2 and `b`<4)or(`b`=2 and(`c`>=3))or(`b`=4 and(`c`<5)))",
				"`a`>1 or(`a`=1 and `b`>4)or(`a`=1 and `b`=4 and `c`>=5)",
			},
			false,
		},
		{
			[]string{"a", "b", "c"},
			[]string{"BIGINT", "BIGINT", "BIGINT"},
			[][]driver.Value{
				{1, 2, 3},
				{1, 2, 8},
			},
			[]string{
				"`a`<1 or(`a`=1 and `b`<2)or(`a`=1 and `b`=2 and `c`<3)",
				"`a`=1 and `b`=2 and(`c`>=3 and `c`<8)",
				"`a`>1 or(`a`=1 and `b`>2)or(`a`=1 and `b`=2 and `c`>=8)",
			},
			false,
		},
		// special case: avoid return same samples
		{
			[]string{"a", "b", "c"},
			[]string{"BIGINT", "BIGINT", "BIGINT"},
			[][]driver.Value{
				{1, 2, 3},
				{1, 2, 3},
			},
			[]string{
				"`a`<1 or(`a`=1 and `b`<2)or(`a`=1 and `b`=2 and `c`<3)",
				"false",
				"`a`>1 or(`a`=1 and `b`>2)or(`a`=1 and `b`=2 and `c`>=3)",
			},
			false,
		},
		// special case: numbers has bigger lexicographically order but lower number
		{
			[]string{"a", "b", "c"},
			[]string{"BIGINT", "BIGINT", "BIGINT"},
			[][]driver.Value{
				{12, 2, 3},
				{111, 4, 5},
			},
			[]string{
				"`a`<12 or(`a`=12 and `b`<2)or(`a`=12 and `b`=2 and `c`<3)",
				"(`a`>12 and `a`<111)or(`a`=12 and(`b`>2 or(`b`=2 and `c`>=3)))or(`a`=111 and(`b`<4 or(`b`=4 and `c`<5)))", // should return sql correctly
				"`a`>111 or(`a`=111 and `b`>4)or(`a`=111 and `b`=4 and `c`>=5)",
			},
			false,
		},
		// test string fields
		{
			[]string{"a", "b", "c"},
			[]string{"BIGINT", "BIGINT", "varchar"},
			[][]driver.Value{
				{1, 2, "3"},
				{1, 4, "5"},
			},
			[]string{
				"`a`<1 or(`a`=1 and `b`<2)or(`a`=1 and `b`=2 and `c`<'3')",
				"`a`=1 and((`b`>2 and `b`<4)or(`b`=2 and(`c`>='3'))or(`b`=4 and(`c`<'5')))",
				"`a`>1 or(`a`=1 and `b`>4)or(`a`=1 and `b`=4 and `c`>='5')",
			},
			false,
		},
		{
			[]string{"a", "b", "c", "d"},
			[]string{"BIGINT", "BIGINT", "BIGINT", "BIGINT"},
			[][]driver.Value{
				{1, 2, 3, 4},
				{5, 6, 7, 8},
			},
			[]string{
				"`a`<1 or(`a`=1 and `b`<2)or(`a`=1 and `b`=2 and `c`<3)or(`a`=1 and `b`=2 and `c`=3 and `d`<4)",
				"(`a`>1 and `a`<5)or(`a`=1 and(`b`>2 or(`b`=2 and `c`>3)or(`b`=2 and `c`=3 and `d`>=4)))or(`a`=5 and(`b`<6 or(`b`=6 and `c`<7)or(`b`=6 and `c`=7 and `d`<8)))",
				"`a`>5 or(`a`=5 and `b`>6)or(`a`=5 and `b`=6 and `c`>7)or(`a`=5 and `b`=6 and `c`=7 and `d`>=8)",
			},
			false,
		},
	}
	transferHandleValStrings := func(handleColTypes []string, handleVals [][]driver.Value) [][]string {
		handleValStrings := make([][]string, 0, len(handleVals))
		for _, handleVal := range handleVals {
			handleValString := make([]string, 0, len(handleVal))
			for i, val := range handleVal {
				rec := colTypeRowReceiverMap[strings.ToUpper(handleColTypes[i])]()
				var valStr string
				switch rec.(type) {
				case *SQLTypeString:
					valStr = fmt.Sprintf("'%s'", val)
				case *SQLTypeBytes:
					valStr = fmt.Sprintf("x'%x'", val)
				case *SQLTypeNumber:
					valStr = fmt.Sprintf("%d", val)
				}
				handleValString = append(handleValString, valStr)
			}
			handleValStrings = append(handleValStrings, handleValString)
		}
		return handleValStrings
	}

	for caseID, testCase := range testCases {
		t.Logf("case #%d", caseID)
		handleColNames := testCase.handleColNames
		handleColTypes := testCase.handleColTypes
		handleVals := testCase.handleVals
		handleValStrings := transferHandleValStrings(handleColTypes, handleVals)

		// Test build whereClauses
		whereClauses := buildWhereClauses(handleColNames, handleValStrings)
		require.Equal(t, testCase.expectedWhereClauses, whereClauses)

		// Test build tasks through table sample
		if len(handleColNames) > 0 {
			taskChan := make(chan Task, 128)
			quotaCols := make([]string, 0, len(handleColNames))
			for _, col := range handleColNames {
				quotaCols = append(quotaCols, wrapBackTicks(col))
			}
			selectFields := strings.Join(quotaCols, ",")
			meta := &mockTableIR{
				dbName:           database,
				tblName:          table,
				selectedField:    selectFields,
				hasImplicitRowID: testCase.hasTiDBRowID,
				colTypes:         handleColTypes,
				colNames:         handleColNames,
				specCmt: []string{
					"/*!40101 SET NAMES binary*/;",
				},
			}

			if !testCase.hasTiDBRowID {
				rows := sqlmock.NewRows(showIndexHeaders)
				for i, handleColName := range handleColNames {
					rows.AddRow(table, 0, "PRIMARY", i, handleColName, "A", 0, nil, nil, "", "BTREE", "", "")
				}
				mock.ExpectQuery(fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", database, table)).WillReturnRows(rows)
			}

			rows := sqlmock.NewRows(handleColNames)
			for _, handleVal := range handleVals {
				rows.AddRow(handleVal...)
			}
			mock.ExpectQuery(fmt.Sprintf("SELECT .* FROM `%s`.`%s` TABLESAMPLE REGIONS", database, table)).WillReturnRows(rows)
			// special case, no enough value to split chunks
			if len(handleVals) == 0 {
				if !testCase.hasTiDBRowID {
					rows = sqlmock.NewRows(showIndexHeaders)
					for i, handleColName := range handleColNames {
						rows.AddRow(table, 0, "PRIMARY", i, handleColName, "A", 0, nil, nil, "", "BTREE", "", "")
					}
					mock.ExpectQuery(fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", database, table)).WillReturnRows(rows)
					mock.ExpectQuery("SHOW INDEX FROM").WillReturnRows(sqlmock.NewRows(showIndexHeaders))
				} else {
					d.conf.Rows = 200000
					mock.ExpectQuery("EXPLAIN SELECT `_tidb_rowid`").
						WillReturnRows(sqlmock.NewRows([]string{"id", "count", "task", "operator info"}).
							AddRow("IndexReader_5", "0.00", "root", "index:IndexScan_4"))
				}
			}

			require.NoError(t, d.concurrentDumpTable(tctx, baseConn, meta, taskChan))
			require.NoError(t, mock.ExpectationsWereMet())
			orderByClause := buildOrderByClauseString(handleColNames)

			checkQuery := func(i int, query string) {
				task := <-taskChan
				taskTableData, ok := task.(*TaskTableData)
				require.True(t, ok)
				require.Equal(t, i, taskTableData.ChunkIndex)

				data, ok := taskTableData.Data.(*tableData)
				require.True(t, ok)
				require.Equal(t, query, data.query)
			}

			// special case, no value found
			if len(handleVals) == 0 {
				query := buildSelectQuery(database, table, selectFields, "", "", orderByClause)
				checkQuery(0, query)
				continue
			}

			for i, w := range testCase.expectedWhereClauses {
				query := buildSelectQuery(database, table, selectFields, "", buildWhereCondition(d.conf, w), orderByClause)
				checkQuery(i, query)
			}
		}
	}
}

func TestBuildPartitionClauses(t *testing.T) {
	const (
		dbName        = "test"
		tbName        = "t"
		fields        = "*"
		partition     = "p0"
		where         = "WHERE a > 10"
		orderByClause = "ORDER BY a"
	)
	testCases := []struct {
		partition     string
		where         string
		orderByClause string
		expectedQuery string
	}{
		{
			"",
			"",
			"",
			"SELECT * FROM `test`.`t`",
		},
		{
			partition,
			"",
			"",
			"SELECT * FROM `test`.`t` PARTITION(`p0`)",
		},
		{
			partition,
			where,
			"",
			"SELECT * FROM `test`.`t` PARTITION(`p0`) WHERE a > 10",
		},
		{
			partition,
			"",
			orderByClause,
			"SELECT * FROM `test`.`t` PARTITION(`p0`) ORDER BY a",
		},
		{
			partition,
			where,
			orderByClause,
			"SELECT * FROM `test`.`t` PARTITION(`p0`) WHERE a > 10 ORDER BY a",
		},
		{
			"",
			where,
			orderByClause,
			"SELECT * FROM `test`.`t` WHERE a > 10 ORDER BY a",
		},
	}
	for _, testCase := range testCases {
		query := buildSelectQuery(dbName, tbName, fields, testCase.partition, testCase.where, testCase.orderByClause)
		require.Equal(t, testCase.expectedQuery, query)
	}
}

func TestBuildWhereCondition(t *testing.T) {
	conf := DefaultConfig()
	testCases := []struct {
		confWhere     string
		chunkWhere    string
		expectedWhere string
	}{
		{
			"",
			"",
			"",
		},
		{
			"a >= 1000000 and a <= 2000000",
			"",
			"WHERE a >= 1000000 and a <= 2000000 ",
		},
		{
			"",
			"(`a`>1 and `a`<3)or(`a`=1 and(`b`>=2))or(`a`=3 and(`b`<4))",
			"WHERE (`a`>1 and `a`<3)or(`a`=1 and(`b`>=2))or(`a`=3 and(`b`<4)) ",
		},
		{
			"a >= 1000000 and a <= 2000000",
			"(`a`>1 and `a`<3)or(`a`=1 and(`b`>=2))or(`a`=3 and(`b`<4))",
			"WHERE (a >= 1000000 and a <= 2000000) AND ((`a`>1 and `a`<3)or(`a`=1 and(`b`>=2))or(`a`=3 and(`b`<4))) ",
		},
	}
	for _, testCase := range testCases {
		conf.Where = testCase.confWhere
		where := buildWhereCondition(conf, testCase.chunkWhere)
		require.Equal(t, testCase.expectedWhere, where)
	}
}

func TestBuildRegionQueriesWithoutPartition(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	baseConn := newBaseConn(conn, true, nil)
	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()

	d := &Dumper{
		tctx:                      tctx,
		conf:                      DefaultConfig(),
		cancelCtx:                 cancel,
		selectTiDBTableRegionFunc: selectTiDBTableRegion,
	}
	d.conf.ServerInfo = version.ServerInfo{
		HasTiKV:       true,
		ServerType:    version.ServerTypeTiDB,
		ServerVersion: gcSafePointVersion,
	}
	d.conf.Rows = 200000
	database := "foo"
	table := "bar"

	testCases := []struct {
		regionResults        [][]driver.Value
		handleColNames       []string
		handleColTypes       []string
		expectedWhereClauses []string
		hasTiDBRowID         bool
	}{
		{
			[][]driver.Value{
				{"7480000000000000FF3300000000000000F8", "7480000000000000FF3300000000000000F8"},
			},
			[]string{"a"},
			[]string{"BIGINT"},
			[]string{
				"",
			},
			false,
		},
		{
			[][]driver.Value{
				{"7480000000000000FF3300000000000000F8", "7480000000000000FF3300000000000000F8"},
			},
			[]string{"_tidb_rowid"},
			[]string{"BIGINT"},
			[]string{
				"",
			},
			true,
		},
		{
			[][]driver.Value{
				{"7480000000000000FF3300000000000000F8", "7480000000000000FF3300000000000000F8"},
				{"7480000000000000FF335F728000000000FF0EA6010000000000FA", "tableID=51, _tidb_rowid=960001"},
				{"7480000000000000FF335F728000000000FF1D4C010000000000FA", "tableID=51, _tidb_rowid=1920001"},
				{"7480000000000000FF335F728000000000FF2BF2010000000000FA", "tableID=51, _tidb_rowid=2880001"},
			},
			[]string{"a"},
			[]string{"BIGINT"},
			[]string{
				"`a`<960001",
				"`a`>=960001 and `a`<1920001",
				"`a`>=1920001 and `a`<2880001",
				"`a`>=2880001",
			},
			false,
		},
		{
			[][]driver.Value{
				{"7480000000000000FF3300000000000000F8", "7480000000000000FF3300000000000000F8"},
				{"7480000000000000FF335F728000000000FF0EA6010000000000FA", "tableID=51, _tidb_rowid=960001"},
				// one invalid key
				{"7520000000000000FF335F728000000000FF0EA6010000000000FA", "7520000000000000FF335F728000000000FF0EA6010000000000FA"},
				{"7480000000000000FF335F728000000000FF1D4C010000000000FA", "tableID=51, _tidb_rowid=1920001"},
				{"7480000000000000FF335F728000000000FF2BF2010000000000FA", "tableID=51, _tidb_rowid=2880001"},
			},
			[]string{"_tidb_rowid"},
			[]string{"BIGINT"},
			[]string{
				"`_tidb_rowid`<960001",
				"`_tidb_rowid`>=960001 and `_tidb_rowid`<1920001",
				"`_tidb_rowid`>=1920001 and `_tidb_rowid`<2880001",
				"`_tidb_rowid`>=2880001",
			},
			true,
		},
	}

	for i, testCase := range testCases {
		t.Logf("case #%d", i)
		handleColNames := testCase.handleColNames
		handleColTypes := testCase.handleColTypes
		regionResults := testCase.regionResults

		// Test build tasks through table region
		taskChan := make(chan Task, 128)
		meta := &mockTableIR{
			dbName:           database,
			tblName:          table,
			selectedField:    "*",
			selectedLen:      len(handleColNames),
			hasImplicitRowID: testCase.hasTiDBRowID,
			colTypes:         handleColTypes,
			colNames:         handleColNames,
			specCmt: []string{
				"/*!40101 SET NAMES binary*/;",
			},
		}

		mock.ExpectQuery("SELECT PARTITION_NAME from INFORMATION_SCHEMA.PARTITIONS").
			WithArgs(database, table).WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).AddRow(nil))

		if !testCase.hasTiDBRowID {
			rows := sqlmock.NewRows(showIndexHeaders)
			for i, handleColName := range handleColNames {
				rows.AddRow(table, 0, "PRIMARY", i, handleColName, "A", 0, nil, nil, "", "BTREE", "", "")
			}
			mock.ExpectQuery(fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", database, table)).WillReturnRows(rows)
		}

		rows := sqlmock.NewRows([]string{"START_KEY", "tidb_decode_key(START_KEY)"})
		for _, regionResult := range regionResults {
			rows.AddRow(regionResult...)
		}
		mock.ExpectQuery("SELECT START_KEY,tidb_decode_key\\(START_KEY\\) from INFORMATION_SCHEMA.TIKV_REGION_STATUS").
			WithArgs(database, table).WillReturnRows(rows)

		orderByClause := buildOrderByClauseString(handleColNames)
		// special case, no enough value to split chunks
		if !testCase.hasTiDBRowID && len(regionResults) <= 1 {
			rows = sqlmock.NewRows(showIndexHeaders)
			for i, handleColName := range handleColNames {
				rows.AddRow(table, 0, "PRIMARY", i, handleColName, "A", 0, nil, nil, "", "BTREE", "", "")
			}
			mock.ExpectQuery(fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", database, table)).WillReturnRows(rows)
			mock.ExpectQuery("SHOW INDEX FROM").WillReturnRows(sqlmock.NewRows(showIndexHeaders))
		}
		require.NoError(t, d.concurrentDumpTable(tctx, baseConn, meta, taskChan))
		require.NoError(t, mock.ExpectationsWereMet())

		for i, w := range testCase.expectedWhereClauses {
			query := buildSelectQuery(database, table, "*", "", buildWhereCondition(d.conf, w), orderByClause)
			task := <-taskChan
			taskTableData, ok := task.(*TaskTableData)
			require.True(t, ok)
			require.Equal(t, i, taskTableData.ChunkIndex)
			data, ok := taskTableData.Data.(*tableData)
			require.True(t, ok)
			require.Equal(t, query, data.query)
		}
	}
}

func TestBuildRegionQueriesWithPartitions(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	baseConn := newBaseConn(conn, true, nil)
	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()

	d := &Dumper{
		tctx:                      tctx,
		conf:                      DefaultConfig(),
		cancelCtx:                 cancel,
		selectTiDBTableRegionFunc: selectTiDBTableRegion,
	}
	d.conf.ServerInfo = version.ServerInfo{
		HasTiKV:       true,
		ServerType:    version.ServerTypeTiDB,
		ServerVersion: gcSafePointVersion,
	}
	partitions := []string{"p0", "p1", "p2"}

	testCases := []struct {
		regionResults        [][][]driver.Value
		handleColNames       []string
		handleColTypes       []string
		expectedWhereClauses [][]string
		hasTiDBRowID         bool
		dumpWholeTable       bool
	}{
		{
			[][][]driver.Value{
				{
					{6009, "t_121_i_1_0380000000000ea6010380000000000ea601", "t_121_", 6010, 1, 6010, 0, 0, 0, 74, 1052002},
					{6011, "t_121_", "t_121_i_1_0380000000000ea6010380000000000ea601", 6012, 1, 6012, 0, 0, 0, 68, 972177},
				},
				{
					{6015, "t_122_i_1_0380000000002d2a810380000000002d2a81", "t_122_", 6016, 1, 6016, 0, 0, 0, 77, 1092962},
					{6017, "t_122_", "t_122_i_1_0380000000002d2a810380000000002d2a81", 6018, 1, 6018, 0, 0, 0, 66, 939975},
				},
				{
					{6021, "t_123_i_1_0380000000004baf010380000000004baf01", "t_123_", 6022, 1, 6022, 0, 0, 0, 85, 1206726},
					{6023, "t_123_", "t_123_i_1_0380000000004baf010380000000004baf01", 6024, 1, 6024, 0, 0, 0, 65, 927576},
				},
			},
			[]string{"_tidb_rowid"},
			[]string{"BIGINT"},
			[][]string{
				{""}, {""}, {""},
			},
			true,
			true,
		},
		{
			[][][]driver.Value{
				{
					{6009, "t_121_i_1_0380000000000ea6010380000000000ea601", "t_121_r_10001", 6010, 1, 6010, 0, 0, 0, 74, 1052002},
					{6013, "t_121_r_10001", "t_121_r_970001", 6014, 1, 6014, 0, 0, 0, 75, 975908},
					{6003, "t_121_r_970001", "t_122_", 6004, 1, 6004, 0, 0, 0, 79, 1022285},
					{6011, "t_121_", "t_121_i_1_0380000000000ea6010380000000000ea601", 6012, 1, 6012, 0, 0, 0, 68, 972177},
				},
				{
					{6015, "t_122_i_1_0380000000002d2a810380000000002d2a81", "t_122_r_2070760", 6016, 1, 6016, 0, 0, 0, 77, 1092962},
					{6019, "t_122_r_2070760", "t_122_r_3047115", 6020, 1, 6020, 0, 0, 0, 75, 959650},
					{6005, "t_122_r_3047115", "t_123_", 6006, 1, 6006, 0, 0, 0, 77, 992339},
					{6017, "t_122_", "t_122_i_1_0380000000002d2a810380000000002d2a81", 6018, 1, 6018, 0, 0, 0, 66, 939975},
				},
				{
					{6021, "t_123_i_1_0380000000004baf010380000000004baf01", "t_123_r_4186953", 6022, 1, 6022, 0, 0, 0, 85, 1206726},
					{6025, "t_123_r_4186953", "t_123_r_5165682", 6026, 1, 6026, 0, 0, 0, 74, 951379},
					{6007, "t_123_r_5165682", "t_124_", 6008, 1, 6008, 0, 0, 0, 71, 918488},
					{6023, "t_123_", "t_123_i_1_0380000000004baf010380000000004baf01", 6024, 1, 6024, 0, 0, 0, 65, 927576},
				},
			},
			[]string{"_tidb_rowid"},
			[]string{"BIGINT"},
			[][]string{
				{
					"`_tidb_rowid`<10001",
					"`_tidb_rowid`>=10001 and `_tidb_rowid`<970001",
					"`_tidb_rowid`>=970001",
				},
				{
					"`_tidb_rowid`<2070760",
					"`_tidb_rowid`>=2070760 and `_tidb_rowid`<3047115",
					"`_tidb_rowid`>=3047115",
				},
				{
					"`_tidb_rowid`<4186953",
					"`_tidb_rowid`>=4186953 and `_tidb_rowid`<5165682",
					"`_tidb_rowid`>=5165682",
				},
			},
			true,
			false,
		},
		{
			[][][]driver.Value{
				{
					{6041, "t_134_", "t_134_r_960001", 6042, 1, 6042, 0, 0, 0, 69, 964987},
					{6035, "t_134_r_960001", "t_135_", 6036, 1, 6036, 0, 0, 0, 75, 1052130},
				},
				{
					{6043, "t_135_", "t_135_r_2960001", 6044, 1, 6044, 0, 0, 0, 69, 969576},
					{6037, "t_135_r_2960001", "t_136_", 6038, 1, 6038, 0, 0, 0, 72, 1014464},
				},
				{
					{6045, "t_136_", "t_136_r_4960001", 6046, 1, 6046, 0, 0, 0, 68, 957557},
					{6039, "t_136_r_4960001", "t_137_", 6040, 1, 6040, 0, 0, 0, 75, 1051579},
				},
			},
			[]string{"a"},
			[]string{"BIGINT"},
			[][]string{

				{
					"`a`<960001",
					"`a`>=960001",
				},
				{
					"`a`<2960001",
					"`a`>=2960001",
				},
				{
					"`a`<4960001",
					"`a`>=4960001",
				},
			},
			false,
			false,
		},
	}

	for i, testCase := range testCases {
		t.Logf("case #%d", i)
		handleColNames := testCase.handleColNames
		handleColTypes := testCase.handleColTypes
		regionResults := testCase.regionResults

		// Test build tasks through table region
		taskChan := make(chan Task, 128)
		meta := &mockTableIR{
			dbName:           database,
			tblName:          table,
			selectedField:    "*",
			selectedLen:      len(handleColNames),
			hasImplicitRowID: testCase.hasTiDBRowID,
			colTypes:         handleColTypes,
			colNames:         handleColNames,
			specCmt: []string{
				"/*!40101 SET NAMES binary*/;",
			},
		}

		rows := sqlmock.NewRows([]string{"PARTITION_NAME"})
		for _, partition := range partitions {
			rows.AddRow(partition)
		}
		mock.ExpectQuery("SELECT PARTITION_NAME from INFORMATION_SCHEMA.PARTITIONS").
			WithArgs(database, table).WillReturnRows(rows)

		if !testCase.hasTiDBRowID {
			rows = sqlmock.NewRows(showIndexHeaders)
			for i, handleColName := range handleColNames {
				rows.AddRow(table, 0, "PRIMARY", i, handleColName, "A", 0, nil, nil, "", "BTREE", "", "")
			}
			mock.ExpectQuery(fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", database, table)).WillReturnRows(rows)
		}

		for i, partition := range partitions {
			rows = sqlmock.NewRows([]string{"REGION_ID", "START_KEY", "END_KEY", "LEADER_ID", "LEADER_STORE_ID", "PEERS", "SCATTERING", "WRITTEN_BYTES", "READ_BYTES", "APPROXIMATE_SIZE(MB)", "APPROXIMATE_KEYS"})
			for _, regionResult := range regionResults[i] {
				rows.AddRow(regionResult...)
			}
			mock.ExpectQuery(fmt.Sprintf("SHOW TABLE `%s`.`%s` PARTITION\\(`%s`\\) REGIONS", escapeString(database), escapeString(table), escapeString(partition))).
				WillReturnRows(rows)
		}

		orderByClause := buildOrderByClauseString(handleColNames)
		require.NoError(t, d.concurrentDumpTable(tctx, baseConn, meta, taskChan))
		require.NoError(t, mock.ExpectationsWereMet())

		chunkIdx := 0
		for i, partition := range partitions {
			for _, w := range testCase.expectedWhereClauses[i] {
				query := buildSelectQuery(database, table, "*", partition, buildWhereCondition(d.conf, w), orderByClause)
				task := <-taskChan
				taskTableData, ok := task.(*TaskTableData)
				require.True(t, ok)
				require.Equal(t, chunkIdx, taskTableData.ChunkIndex)
				data, ok := taskTableData.Data.(*tableData)
				require.True(t, ok)
				require.Equal(t, query, data.query)
				chunkIdx++
			}
		}
	}
}

func buildMockNewRows(mock sqlmock.Sqlmock, columns []string, driverValues [][]driver.Value) *sqlmock.Rows {
	rows := mock.NewRows(columns)
	for _, driverValue := range driverValues {
		rows.AddRow(driverValue...)
	}
	return rows
}

func readRegionCsvDriverValues(t *testing.T) [][]driver.Value {
	csvFilename := "region_results.csv"
	file, err := os.Open(csvFilename)
	require.NoError(t, err)
	csvReader := csv.NewReader(file)
	values := make([][]driver.Value, 0, 990)
	for {
		results, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if len(results) != 3 {
			continue
		}
		regionID, err := strconv.Atoi(results[0])
		require.NoError(t, err)
		startKey, endKey := results[1], results[2]
		values = append(values, []driver.Value{regionID, startKey, endKey})
	}
	return values
}

func TestBuildVersion3RegionQueries(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	baseConn := newBaseConn(conn, true, nil)
	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()
	oldOpenFunc := openDBFunc
	defer func() {
		openDBFunc = oldOpenFunc
	}()
	openDBFunc = func(_, _ string) (*sql.DB, error) {
		return db, nil
	}

	conf := DefaultConfig()
	conf.ServerInfo = version.ServerInfo{
		HasTiKV:       true,
		ServerType:    version.ServerTypeTiDB,
		ServerVersion: decodeRegionVersion,
	}
	database := "test"
	conf.Tables = DatabaseTables{
		database: []*TableInfo{
			{"t1", 0, TableTypeBase},
			{"t2", 0, TableTypeBase},
			{"t3", 0, TableTypeBase},
			{"t4", 0, TableTypeBase},
		},
	}
	d := &Dumper{
		tctx:                      tctx,
		conf:                      conf,
		cancelCtx:                 cancel,
		selectTiDBTableRegionFunc: selectTiDBTableRegion,
	}
	showStatsHistograms := buildMockNewRows(mock, []string{"Db_name", "Table_name", "Partition_name", "Column_name", "Is_index", "Update_time", "Distinct_count", "Null_count", "Avg_col_size", "Correlation"},
		[][]driver.Value{
			{"test", "t2", "p0", "a", 0, "2021-06-27 17:43:51", 1999999, 0, 8, 0},
			{"test", "t2", "p1", "a", 0, "2021-06-22 20:30:16", 1260000, 0, 8, 0},
			{"test", "t2", "p2", "a", 0, "2021-06-22 20:32:16", 1230000, 0, 8, 0},
			{"test", "t2", "p3", "a", 0, "2021-06-22 20:36:19", 2000000, 0, 8, 0},
			{"test", "t1", "", "a", 0, "2021-04-22 15:23:58", 7100000, 0, 8, 0},
			{"test", "t3", "", "PRIMARY", 1, "2021-06-27 22:08:43", 4980000, 0, 0, 0},
			{"test", "t4", "p0", "PRIMARY", 1, "2021-06-28 10:54:06", 2000000, 0, 0, 0},
			{"test", "t4", "p1", "PRIMARY", 1, "2021-06-28 10:55:04", 1300000, 0, 0, 0},
			{"test", "t4", "p2", "PRIMARY", 1, "2021-06-28 10:57:05", 1830000, 0, 0, 0},
			{"test", "t4", "p3", "PRIMARY", 1, "2021-06-28 10:59:04", 2000000, 0, 0, 0},
			{"mysql", "global_priv", "", "PRIMARY", 1, "2021-06-04 20:39:44", 0, 0, 0, 0},
		})
	selectMySQLStatsHistograms := buildMockNewRows(mock, []string{"TABLE_ID", "VERSION", "DISTINCT_COUNT"},
		[][]driver.Value{
			{15, "1970-01-01 08:00:00", 0},
			{15, "1970-01-01 08:00:00", 0},
			{15, "1970-01-01 08:00:00", 0},
			{41, "2021-04-22 15:23:58", 7100000},
			{41, "2021-04-22 15:23:59", 7100000},
			{41, "2021-04-22 15:23:59", 7100000},
			{41, "2021-04-22 15:23:59", 7100000},
			{27, "1970-01-01 08:00:00", 0},
			{27, "1970-01-01 08:00:00", 0},
			{25, "1970-01-01 08:00:00", 0},
			{25, "1970-01-01 08:00:00", 0},
			{2098, "2021-06-04 20:39:41", 0},
			{2101, "2021-06-04 20:39:44", 0},
			{2101, "2021-06-04 20:39:44", 0},
			{2101, "2021-06-04 20:39:44", 0},
			{2101, "2021-06-04 20:39:44", 0},
			{2128, "2021-06-22 20:29:19", 1991680},
			{2128, "2021-06-22 20:29:19", 1991680},
			{2128, "2021-06-22 20:29:19", 1991680},
			{2129, "2021-06-22 20:30:16", 1260000},
			{2129, "2021-06-22 20:30:16", 1237120},
			{2129, "2021-06-22 20:30:16", 1237120},
			{2129, "2021-06-22 20:30:16", 1237120},
			{2130, "2021-06-22 20:32:16", 1230000},
			{2130, "2021-06-22 20:32:16", 1216128},
			{2130, "2021-06-22 20:32:17", 1216128},
			{2130, "2021-06-22 20:32:17", 1216128},
			{2131, "2021-06-22 20:36:19", 2000000},
			{2131, "2021-06-22 20:36:19", 1959424},
			{2131, "2021-06-22 20:36:19", 1959424},
			{2131, "2021-06-22 20:36:19", 1959424},
			{2128, "2021-06-27 17:43:51", 1999999},
			{2136, "2021-06-27 22:08:38", 4860000},
			{2136, "2021-06-27 22:08:38", 4860000},
			{2136, "2021-06-27 22:08:38", 4860000},
			{2136, "2021-06-27 22:08:38", 4860000},
			{2136, "2021-06-27 22:08:43", 4980000},
			{2139, "2021-06-28 10:54:05", 1991680},
			{2139, "2021-06-28 10:54:05", 1991680},
			{2139, "2021-06-28 10:54:05", 1991680},
			{2139, "2021-06-28 10:54:05", 1991680},
			{2139, "2021-06-28 10:54:06", 2000000},
			{2140, "2021-06-28 10:55:02", 1246336},
			{2140, "2021-06-28 10:55:02", 1246336},
			{2140, "2021-06-28 10:55:02", 1246336},
			{2140, "2021-06-28 10:55:03", 1246336},
			{2140, "2021-06-28 10:55:04", 1300000},
			{2141, "2021-06-28 10:57:03", 1780000},
			{2141, "2021-06-28 10:57:03", 1780000},
			{2141, "2021-06-28 10:57:03", 1780000},
			{2141, "2021-06-28 10:57:03", 1780000},
			{2141, "2021-06-28 10:57:05", 1830000},
			{2142, "2021-06-28 10:59:03", 1959424},
			{2142, "2021-06-28 10:59:03", 1959424},
			{2142, "2021-06-28 10:59:03", 1959424},
			{2142, "2021-06-28 10:59:03", 1959424},
			{2142, "2021-06-28 10:59:04", 2000000},
		})
	selectRegionStatusHistograms := buildMockNewRows(mock, []string{"REGION_ID", "START_KEY", "END_KEY"}, readRegionCsvDriverValues(t))
	selectInformationSchemaTables := buildMockNewRows(mock, []string{"TABLE_SCHEMA", "TABLE_NAME", "TIDB_TABLE_ID"},
		[][]driver.Value{
			{"mysql", "expr_pushdown_blacklist", 39},
			{"mysql", "user", 5},
			{"mysql", "db", 7},
			{"mysql", "tables_priv", 9},
			{"mysql", "stats_top_n", 37},
			{"mysql", "columns_priv", 11},
			{"mysql", "bind_info", 35},
			{"mysql", "default_roles", 33},
			{"mysql", "role_edges", 31},
			{"mysql", "stats_feedback", 29},
			{"mysql", "gc_delete_range_done", 27},
			{"mysql", "gc_delete_range", 25},
			{"mysql", "help_topic", 17},
			{"mysql", "global_priv", 2101},
			{"mysql", "stats_histograms", 21},
			{"mysql", "opt_rule_blacklist", 2098},
			{"mysql", "stats_meta", 19},
			{"mysql", "stats_buckets", 23},
			{"mysql", "tidb", 15},
			{"mysql", "GLOBAL_VARIABLES", 13},
			{"test", "t2", 2127},
			{"test", "t1", 41},
			{"test", "t3", 2136},
			{"test", "t4", 2138},
		})
	mock.ExpectQuery("SHOW STATS_HISTOGRAMS").
		WillReturnRows(showStatsHistograms)
	mock.ExpectQuery("SELECT TABLE_ID,FROM_UNIXTIME").
		WillReturnRows(selectMySQLStatsHistograms)
	mock.ExpectQuery("SELECT TABLE_SCHEMA,TABLE_NAME,TIDB_TABLE_ID FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_SCHEMA").
		WillReturnRows(selectInformationSchemaTables)
	mock.ExpectQuery("SELECT REGION_ID,START_KEY,END_KEY FROM INFORMATION_SCHEMA.TIKV_REGION_STATUS ORDER BY START_KEY;").
		WillReturnRows(selectRegionStatusHistograms)

	require.NoError(t, d.renewSelectTableRegionFuncForLowerTiDB(tctx))
	require.NoError(t, mock.ExpectationsWereMet())

	testCases := []struct {
		tableName            string
		handleColNames       []string
		handleColTypes       []string
		expectedWhereClauses []string
		hasTiDBRowID         bool
	}{
		{
			"t1",
			[]string{"a"},
			[]string{"INT"},
			[]string{
				"`a`<960001",
				"`a`>=960001 and `a`<1920001",
				"`a`>=1920001 and `a`<2880001",
				"`a`>=2880001 and `a`<3840001",
				"`a`>=3840001 and `a`<4800001",
				"`a`>=4800001 and `a`<5760001",
				"`a`>=5760001 and `a`<6720001",
				"`a`>=6720001",
			},
			false,
		},
		{
			"t2",
			[]string{"a"},
			[]string{"INT"},
			[]string{
				"`a`<960001",
				"`a`>=960001 and `a`<2960001",
				"`a`>=2960001 and `a`<4960001",
				"`a`>=4960001 and `a`<6960001",
				"`a`>=6960001",
			},
			false,
		},
		{
			"t3",
			[]string{"_tidb_rowid"},
			[]string{"BIGINT"},
			[]string{
				"`_tidb_rowid`<81584",
				"`_tidb_rowid`>=81584 and `_tidb_rowid`<1041584",
				"`_tidb_rowid`>=1041584 and `_tidb_rowid`<2001584",
				"`_tidb_rowid`>=2001584 and `_tidb_rowid`<2961584",
				"`_tidb_rowid`>=2961584 and `_tidb_rowid`<3921584",
				"`_tidb_rowid`>=3921584 and `_tidb_rowid`<4881584",
				"`_tidb_rowid`>=4881584 and `_tidb_rowid`<5841584",
				"`_tidb_rowid`>=5841584 and `_tidb_rowid`<6801584",
				"`_tidb_rowid`>=6801584",
			},
			true,
		},
		{
			"t4",
			[]string{"_tidb_rowid"},
			[]string{"BIGINT"},
			[]string{
				"`_tidb_rowid`<180001",
				"`_tidb_rowid`>=180001 and `_tidb_rowid`<1140001",
				"`_tidb_rowid`>=1140001 and `_tidb_rowid`<2200001",
				"`_tidb_rowid`>=2200001 and `_tidb_rowid`<3160001",
				"`_tidb_rowid`>=3160001 and `_tidb_rowid`<4160001",
				"`_tidb_rowid`>=4160001 and `_tidb_rowid`<5120001",
				"`_tidb_rowid`>=5120001 and `_tidb_rowid`<6170001",
				"`_tidb_rowid`>=6170001 and `_tidb_rowid`<7130001",
				"`_tidb_rowid`>=7130001",
			},
			true,
		},
	}

	for i, testCase := range testCases {
		t.Logf("case #%d", i)
		table := testCase.tableName
		handleColNames := testCase.handleColNames
		handleColTypes := testCase.handleColTypes

		// Test build tasks through table region
		taskChan := make(chan Task, 128)
		meta := &mockTableIR{
			dbName:           database,
			tblName:          table,
			selectedField:    "*",
			hasImplicitRowID: testCase.hasTiDBRowID,
			colNames:         handleColNames,
			colTypes:         handleColTypes,
			specCmt: []string{
				"/*!40101 SET NAMES binary*/;",
			},
		}

		if !testCase.hasTiDBRowID {
			rows := sqlmock.NewRows(showIndexHeaders)
			for i, handleColName := range handleColNames {
				rows.AddRow(table, 0, "PRIMARY", i, handleColName, "A", 0, nil, nil, "", "BTREE", "", "")
			}
			mock.ExpectQuery(fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", database, table)).WillReturnRows(rows)
		}

		orderByClause := buildOrderByClauseString(handleColNames)
		require.NoError(t, d.concurrentDumpTable(tctx, baseConn, meta, taskChan))
		require.NoError(t, mock.ExpectationsWereMet())

		chunkIdx := 0
		for _, w := range testCase.expectedWhereClauses {
			query := buildSelectQuery(database, table, "*", "", buildWhereCondition(d.conf, w), orderByClause)
			task := <-taskChan
			taskTableData, ok := task.(*TaskTableData)
			require.True(t, ok)
			require.Equal(t, chunkIdx, taskTableData.ChunkIndex)

			data, ok := taskTableData.Data.(*tableData)
			require.True(t, ok)
			require.Equal(t, query, data.query)

			chunkIdx++
		}
	}
}

func TestCheckTiDBWithTiKV(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	tidbConf := dbconfig.NewConfig()
	stores := []string{"unistore", "mocktikv", "tikv"}
	for _, store := range stores {
		tidbConf.Store = store
		tidbConfBytes, err := json.Marshal(tidbConf)
		require.NoError(t, err)
		mock.ExpectQuery("SELECT @@tidb_config").WillReturnRows(
			sqlmock.NewRows([]string{"@@tidb_config"}).AddRow(string(tidbConfBytes)))
		hasTiKV, err := CheckTiDBWithTiKV(db)
		require.NoError(t, err)
		if store == "tikv" {
			require.True(t, hasTiKV)
		} else {
			require.False(t, hasTiKV)
		}
		require.NoError(t, mock.ExpectationsWereMet())
	}

	errLackPrivilege := errors.New("ERROR 1142 (42000): SELECT command denied to user 'test'@'%' for table 'tidb'")
	expectedResults := []interface{}{errLackPrivilege, 1, 0}
	for i, res := range expectedResults {
		t.Logf("case #%d", i)
		mock.ExpectQuery("SELECT @@tidb_config").WillReturnError(errLackPrivilege)
		expectedErr, ok := res.(error)
		if ok {
			mock.ExpectQuery("SELECT COUNT").WillReturnError(expectedErr)
			hasTiKV, err := CheckTiDBWithTiKV(db)
			require.ErrorIs(t, err, expectedErr)
			require.True(t, hasTiKV)
		} else if cnt, ok := res.(int); ok {
			mock.ExpectQuery("SELECT COUNT").WillReturnRows(
				sqlmock.NewRows([]string{"c"}).AddRow(cnt))
			hasTiKV, err := CheckTiDBWithTiKV(db)
			require.NoError(t, err)
			require.Equal(t, cnt > 0, hasTiKV)
		}
		require.NoError(t, mock.ExpectationsWereMet())
	}
}

func TestPickupPossibleField(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	tctx := tcontext.Background().WithLogger(appLogger)
	baseConn := newBaseConn(conn, true, nil)

	meta := &mockTableIR{
		dbName:   database,
		tblName:  table,
		colNames: []string{"string1", "int1", "int2", "float1", "bin1", "int3", "bool1", "int4"},
		colTypes: []string{"VARCHAR", "INT", "BIGINT", "FLOAT", "BINARY", "MEDIUMINT", "BOOL", "TINYINT"},
		specCmt: []string{
			"/*!40101 SET NAMES binary*/;",
		},
	}

	testCases := []struct {
		expectedErr      error
		expectedField    string
		hasImplicitRowID bool
		showIndexResults [][]driver.Value
	}{
		{
			errors.New("show index error"),
			"",
			false,
			nil,
		}, {
			nil,
			"_tidb_rowid",
			true,
			nil,
		}, // both primary and unique key columns are integers, use primary key first
		{
			nil,
			"int1",
			false,
			[][]driver.Value{
				{table, 0, "PRIMARY", 1, "int1", "A", 2, nil, nil, "", "BTREE", "", ""},
				{table, 0, "PRIMARY", 2, "float1", "A", 2, nil, nil, "", "BTREE", "", ""},
				{table, 0, "int2", 1, "int2", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 1, "string1", 1, "string1", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 1, "int3", 1, "int3", "A", 20, nil, nil, "YES", "BTREE", "", ""},
			},
		}, // primary key doesn't have integer at seq 1, use unique key with integer
		{
			nil,
			"int2",
			false,
			[][]driver.Value{
				{table, 0, "PRIMARY", 1, "float1", "A", 2, nil, nil, "", "BTREE", "", ""},
				{table, 0, "PRIMARY", 2, "int1", "A", 2, nil, nil, "", "BTREE", "", ""},
				{table, 0, "int2", 1, "int2", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 1, "string1", 1, "string1", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 1, "int3", 1, "int3", "A", 20, nil, nil, "YES", "BTREE", "", ""},
			},
		}, // several unique keys, use unique key who has a integer in seq 1
		{
			nil,
			"int1",
			false,
			[][]driver.Value{
				{table, 0, "u1", 1, "int1", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 0, "u1", 2, "string1", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 0, "u1", 3, "bin1", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 0, "u2", 1, "float1", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 0, "u2", 2, "int2", "A", 2, nil, nil, "YES", "BTREE", "", ""},
			},
		}, // several unique keys and ordinary keys, use unique key who has a integer in seq 1
		{
			nil,
			"int1",
			false,
			[][]driver.Value{
				{table, 0, "u1", 1, "float1", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 0, "u1", 2, "int2", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 0, "u2", 1, "int1", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 0, "u2", 2, "string1", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 0, "u2", 3, "bin1", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 1, "int3", 1, "int3", "A", 2, nil, nil, "YES", "BTREE", "", ""},
			},
		}, // several unique keys and ordinary keys, use unique key who has less columns
		{
			nil,
			"int2",
			false,
			[][]driver.Value{
				{table, 0, "u1", 1, "int1", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 0, "u1", 2, "string1", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 0, "u1", 3, "bin1", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 0, "u2", 1, "int2", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 0, "u2", 2, "string1", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 1, "int3", 1, "int3", "A", 20, nil, nil, "YES", "BTREE", "", ""},
			},
		}, // several unique keys and ordinary keys, use key who has max cardinality
		{
			nil,
			"int2",
			false,
			[][]driver.Value{
				{table, 0, "PRIMARY", 1, "string1", "A", 2, nil, nil, "", "BTREE", "", ""},
				{table, 0, "u1", 1, "float1", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 0, "u1", 2, "int3", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 1, "i1", 1, "int1", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 1, "i2", 1, "int2", "A", 5, nil, nil, "YES", "BTREE", "", ""},
				{table, 1, "i2", 2, "bool1", "A", 2, nil, nil, "YES", "BTREE", "", ""},
				{table, 1, "i3", 1, "bin1", "A", 10, nil, nil, "YES", "BTREE", "", ""},
				{table, 1, "i3", 2, "int4", "A", 10, nil, nil, "YES", "BTREE", "", ""},
			},
		},
	}

	query := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", database, table)
	for i, testCase := range testCases {
		t.Logf("case #%d", i)

		meta.hasImplicitRowID = testCase.hasImplicitRowID
		expectedErr := testCase.expectedErr
		if expectedErr != nil {
			mock.ExpectQuery(query).WillReturnError(expectedErr)
		} else if !testCase.hasImplicitRowID {
			rows := sqlmock.NewRows(showIndexHeaders)
			for _, showIndexResult := range testCase.showIndexResults {
				rows.AddRow(showIndexResult...)
			}
			mock.ExpectQuery(query).WillReturnRows(rows)
		}

		field, err := pickupPossibleField(tctx, meta, baseConn)
		if expectedErr != nil {
			require.ErrorIs(t, err, expectedErr)
		} else {
			require.NoError(t, err)
			require.Equal(t, testCase.expectedField, field)
		}
		require.NoError(t, mock.ExpectationsWereMet())
	}
}

func TestCheckIfSeqExists(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	mock.ExpectQuery("SELECT COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).
			AddRow("1"))

	exists, err := CheckIfSeqExists(conn)
	require.NoError(t, err)
	require.Equal(t, true, exists)

	mock.ExpectQuery("SELECT COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).
			AddRow("0"))

	exists, err = CheckIfSeqExists(conn)
	require.NoError(t, err)
	require.Equal(t, false, exists)
}

func TestGetCharsetAndDefaultCollation(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)

	mock.ExpectQuery("SHOW CHARACTER SET").
		WillReturnRows(sqlmock.NewRows([]string{"Charset", "Description", "Default collation", "Maxlen"}).
			AddRow("utf8mb4", "UTF-8 Unicode", "utf8mb4_0900_ai_ci", 4).
			AddRow("latin1", "cp1252 West European", "latin1_swedish_ci", 1))

	charsetAndDefaultCollation, err := GetCharsetAndDefaultCollation(ctx, conn)
	require.NoError(t, err)
	require.Equal(t, "utf8mb4_0900_ai_ci", charsetAndDefaultCollation["utf8mb4"])
	require.Equal(t, "latin1_swedish_ci", charsetAndDefaultCollation["latin1"])
}
