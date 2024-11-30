// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/coreos/go-semver/semver"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/version"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/promutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestDumpExit(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	mock.ExpectQuery(fmt.Sprintf("SHOW CREATE DATABASE `%s`", escapeString(database))).
		WillDelayFor(time.Second).
		WillReturnRows(sqlmock.NewRows([]string{"Database", "Create Database"}).
			AddRow("test", "CREATE DATABASE `test` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))
	mock.ExpectQuery(fmt.Sprintf("SELECT DEFAULT_COLLATION_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '%s'", escapeString(database))).
		WillReturnRows(sqlmock.NewRows([]string{"DEFAULT_COLLATION_NAME"}).
			AddRow("utf8mb4_bin"))

	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()
	defer cancel()
	conn, err := db.Conn(tctx)
	require.NoError(t, err)
	baseConn := newBaseConn(conn, true, nil)

	d := &Dumper{
		tctx:      tctx,
		conf:      DefaultConfig(),
		cancelCtx: cancel,
	}
	wg, writingCtx := errgroup.WithContext(tctx)
	writerErr := errors.New("writer error")

	wg.Go(func() error {
		return errors.Trace(writerErr)
	})
	wg.Go(func() error {
		time.Sleep(time.Second)
		return context.Canceled
	})

	writerCtx := tctx.WithContext(writingCtx)
	taskChan := make(chan Task, 1)
	taskChan <- &TaskDatabaseMeta{}
	d.conf.Tables = DatabaseTables{}.AppendTable(database, nil)
	d.conf.ServerInfo.ServerType = version.ServerTypeMySQL
	require.ErrorIs(t, wg.Wait(), writerErr)
	// if writerCtx is canceled , QuerySQL in `dumpDatabases` will return sqlmock.ErrCancelled
	require.ErrorIs(t, d.dumpDatabases(writerCtx, baseConn, taskChan), sqlmock.ErrCancelled)
}

func TestDumpTableMeta(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()
	defer cancel()
	conn, err := db.Conn(tctx)
	require.NoError(t, err)
	baseConn := newBaseConn(conn, true, nil)

	conf := DefaultConfig()
	conf.NoSchemas = true

	for serverType := version.ServerTypeUnknown; serverType < version.ServerTypeAll; serverType++ {
		conf.ServerInfo.ServerType = version.ServerType(serverType)
		hasImplicitRowID := false
		mock.ExpectQuery("SHOW COLUMNS FROM").
			WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
				AddRow("id", "int(11)", "NO", "PRI", nil, ""))
		if serverType == version.ServerTypeTiDB {
			mock.ExpectExec("SELECT _tidb_rowid from").
				WillReturnResult(sqlmock.NewResult(0, 0))
			hasImplicitRowID = true
		}
		mock.ExpectQuery(fmt.Sprintf("SELECT \\* FROM `%s`.`%s`", database, table)).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
		meta, err := dumpTableMeta(tctx, conf, baseConn, database, &TableInfo{Type: TableTypeBase, Name: table})
		require.NoError(t, err)
		require.Equal(t, database, meta.DatabaseName())
		require.Equal(t, table, meta.TableName())
		require.Equal(t, "*", meta.SelectedField())
		require.Equal(t, 1, meta.SelectedLen())
		require.Equal(t, "", meta.ShowCreateTable())
		require.Equal(t, hasImplicitRowID, meta.HasImplicitRowID())
	}
}

func TestGetListTableTypeByConf(t *testing.T) {
	conf := defaultConfigForTest(t)
	cases := []struct {
		serverInfo  version.ServerInfo
		consistency string
		expected    listTableType
	}{
		{version.ParseServerInfo("5.7.25-TiDB-3.0.6"), ConsistencyTypeSnapshot, listTableByShowTableStatus},
		// no bug version
		{version.ParseServerInfo("8.0.2"), ConsistencyTypeLock, listTableByInfoSchema},
		{version.ParseServerInfo("8.0.2"), ConsistencyTypeFlush, listTableByShowTableStatus},
		{version.ParseServerInfo("8.0.23"), ConsistencyTypeNone, listTableByShowTableStatus},

		// bug version
		{version.ParseServerInfo("8.0.3"), ConsistencyTypeLock, listTableByInfoSchema},
		{version.ParseServerInfo("8.0.3"), ConsistencyTypeFlush, listTableByShowFullTables},
		{version.ParseServerInfo("8.0.3"), ConsistencyTypeNone, listTableByShowTableStatus},
	}

	for _, x := range cases {
		conf.Consistency = x.consistency
		conf.ServerInfo = x.serverInfo
		require.Equalf(t, x.expected, getListTableTypeByConf(conf), "server info: %s, consistency: %s", x.serverInfo, x.consistency)
	}
}

func TestAdjustDatabaseCollation(t *testing.T) {
	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()
	defer cancel()
	parser1 := parser.New()

	originSQLs := []string{
		"create database `test` CHARACTER SET=utf8mb4 COLLATE=utf8mb4_general_ci",
		"create database `test` CHARACTER SET=utf8mb4",
	}

	expectedSQLs := []string{
		"create database `test` CHARACTER SET=utf8mb4 COLLATE=utf8mb4_general_ci",
		"CREATE DATABASE `test` CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci",
	}
	charsetAndDefaultCollationMap := map[string]string{"utf8mb4": "utf8mb4_general_ci"}

	for _, originSQL := range originSQLs {
		newSQL, err := adjustDatabaseCollation(tctx, LooseCollationCompatible, parser1, originSQL, charsetAndDefaultCollationMap)
		require.NoError(t, err)
		require.Equal(t, originSQL, newSQL)
	}

	for i, originSQL := range originSQLs {
		newSQL, err := adjustDatabaseCollation(tctx, StrictCollationCompatible, parser1, originSQL, charsetAndDefaultCollationMap)
		require.NoError(t, err)
		require.Equal(t, expectedSQLs[i], newSQL)
	}
}

func TestAdjustTableCollation(t *testing.T) {
	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()
	defer cancel()

	parser1 := parser.New()

	originSQLs := []string{
		"create table `test`.`t1` (id int) CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
		"create table `test`.`t1` (id int) CHARSET=utf8mb4",
		"create table `test`.`t1` (id int, name varchar(20) CHARACTER SET utf8mb4, work varchar(20)) CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ",
		"create table `test`.`t1` (id int, name varchar(20), work varchar(20)) CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
		"create table `test`.`t1` (id int, name varchar(20) COLLATE utf8mb4_general_ci, work varchar(20)) CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
		"create table `test`.`t1` (id int, name varchar(20) COLLATE utf8mb4_general_ci, work varchar(20) CHARACTER SET utf8mb4) CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
		"create table `test`.`t1` (id int, name varchar(20) CHARACTER SET utf8mb4, work varchar(20)) CHARSET=utf8mb4 ",
		"create table `test`.`t1` (id int, name varchar(20), work varchar(20)) CHARSET=utf8mb4",
		"create table `test`.`t1` (id int, name varchar(20) COLLATE utf8mb4_general_ci, work varchar(20)) CHARSET=utf8mb4",
		"create table `test`.`t1` (id int, name varchar(20) COLLATE utf8mb4_general_ci, work varchar(20) CHARACTER SET utf8mb4) CHARSET=utf8mb4",
		"create table `test`.`t1` (name varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin) CHARSET=latin1 COLLATE=latin1_bin",
	}

	expectedSQLs := []string{
		"CREATE TABLE `test`.`t1` (`id` INT) DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_GENERAL_CI",
		"CREATE TABLE `test`.`t1` (`id` INT) DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_GENERAL_CI",
		"CREATE TABLE `test`.`t1` (`id` INT,`name` VARCHAR(20) CHARACTER SET UTF8MB4 COLLATE utf8mb4_general_ci,`work` VARCHAR(20)) DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_GENERAL_CI",
		"CREATE TABLE `test`.`t1` (`id` INT,`name` VARCHAR(20),`work` VARCHAR(20)) DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_GENERAL_CI",
		"CREATE TABLE `test`.`t1` (`id` INT,`name` VARCHAR(20) COLLATE utf8mb4_general_ci,`work` VARCHAR(20)) DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_GENERAL_CI",
		"CREATE TABLE `test`.`t1` (`id` INT,`name` VARCHAR(20) COLLATE utf8mb4_general_ci,`work` VARCHAR(20) CHARACTER SET UTF8MB4 COLLATE utf8mb4_general_ci) DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_GENERAL_CI",
		"CREATE TABLE `test`.`t1` (`id` INT,`name` VARCHAR(20) CHARACTER SET UTF8MB4 COLLATE utf8mb4_general_ci,`work` VARCHAR(20)) DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_GENERAL_CI",
		"CREATE TABLE `test`.`t1` (`id` INT,`name` VARCHAR(20),`work` VARCHAR(20)) DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_GENERAL_CI",
		"CREATE TABLE `test`.`t1` (`id` INT,`name` VARCHAR(20) COLLATE utf8mb4_general_ci,`work` VARCHAR(20)) DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_GENERAL_CI",
		"CREATE TABLE `test`.`t1` (`id` INT,`name` VARCHAR(20) COLLATE utf8mb4_general_ci,`work` VARCHAR(20) CHARACTER SET UTF8MB4 COLLATE utf8mb4_general_ci) DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_GENERAL_CI",
		"CREATE TABLE `test`.`t1` (`name` VARCHAR(20) CHARACTER SET UTF8MB4 COLLATE utf8mb4_bin) DEFAULT CHARACTER SET = LATIN1 DEFAULT COLLATE = LATIN1_BIN",
	}

	charsetAndDefaultCollationMap := map[string]string{"utf8mb4": "utf8mb4_general_ci"}

	for _, originSQL := range originSQLs {
		newSQL, err := adjustTableCollation(tctx, LooseCollationCompatible, parser1, originSQL, charsetAndDefaultCollationMap)
		require.NoError(t, err)
		require.Equal(t, originSQL, newSQL)
	}

	for i, originSQL := range originSQLs {
		newSQL, err := adjustTableCollation(tctx, StrictCollationCompatible, parser1, originSQL, charsetAndDefaultCollationMap)
		require.NoError(t, err)
		require.Equal(t, expectedSQLs[i], newSQL)
	}
}

func TestUnregisterMetrics(t *testing.T) {
	ctx := context.Background()
	conf := &Config{
		SQL:          "not empty",
		Where:        "not empty",
		PromFactory:  promutil.NewDefaultFactory(),
		PromRegistry: promutil.NewDefaultRegistry(),
	}

	_, err := NewDumper(ctx, conf)
	require.Error(t, err)
	_, err = NewDumper(ctx, conf)
	// should not panic
	require.Error(t, err)
}

func TestSetDefaultSessionParams(t *testing.T) {
	testCases := []struct {
		si             version.ServerInfo
		sessionParams  map[string]any
		expectedParams map[string]any
	}{
		{
			si: version.ServerInfo{
				ServerType:    version.ServerTypeTiDB,
				HasTiKV:       true,
				ServerVersion: semver.New("6.1.0"),
			},
			sessionParams: map[string]any{
				"tidb_snapshot": "2020-01-01 00:00:00",
			},
			expectedParams: map[string]any{
				"tidb_snapshot": "2020-01-01 00:00:00",
			},
		},
		{
			si: version.ServerInfo{
				ServerType:    version.ServerTypeTiDB,
				HasTiKV:       true,
				ServerVersion: semver.New("6.2.0"),
			},
			sessionParams: map[string]any{
				"tidb_snapshot": "2020-01-01 00:00:00",
			},
			expectedParams: map[string]any{
				"tidb_enable_paging": "ON",
				"tidb_snapshot":      "2020-01-01 00:00:00",
			},
		},
		{
			si: version.ServerInfo{
				ServerType:    version.ServerTypeTiDB,
				HasTiKV:       true,
				ServerVersion: semver.New("6.2.0"),
			},
			sessionParams: map[string]any{
				"tidb_enable_paging": "OFF",
				"tidb_snapshot":      "2020-01-01 00:00:00",
			},
			expectedParams: map[string]any{
				"tidb_enable_paging": "OFF",
				"tidb_snapshot":      "2020-01-01 00:00:00",
			},
		},
		{
			si: version.ServerInfo{
				ServerType:    version.ServerTypeMySQL,
				ServerVersion: semver.New("8.0.32"),
			},
			sessionParams:  map[string]any{},
			expectedParams: map[string]any{},
		},
	}

	for _, testCase := range testCases {
		setDefaultSessionParams(testCase.si, testCase.sessionParams)
		require.Equal(t, testCase.expectedParams, testCase.sessionParams)
	}
}

func TestSetSessionParams(t *testing.T) {
	// case 1: fail to set tidb_snapshot, should return error with hint
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	mock.ExpectQuery("SELECT @@tidb_config").
		WillReturnError(errors.New("mock error"))
	mock.ExpectQuery("SELECT COUNT\\(1\\) as c FROM MYSQL.TiDB WHERE VARIABLE_NAME='tikv_gc_safe_point'").
		WillReturnError(errors.New("mock error"))
	tikvErr := &mysql.MySQLError{
		Number:  1105,
		Message: "can not get 'tikv_gc_safe_point'",
	}
	mock.ExpectExec("SET SESSION tidb_snapshot").
		WillReturnError(tikvErr)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/dumpling/export/SkipResetDB", "return(true)"))
	defer failpoint.Disable("github.com/pingcap/tidb/dumpling/export/SkipResetDB=return(true)")

	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()
	defer cancel()

	conf := DefaultConfig()
	conf.ServerInfo = version.ServerInfo{
		ServerType: version.ServerTypeTiDB,
		HasTiKV:    false,
	}
	conf.Snapshot = "439153276059648000"
	conf.Consistency = ConsistencyTypeSnapshot
	d := &Dumper{
		tctx:      tctx,
		conf:      conf,
		cancelCtx: cancel,
		dbHandle:  db,
	}
	err = setSessionParam(d)
	require.ErrorContains(t, err, "consistency=none")

	// case 2: fail to set other
	conf.ServerInfo = version.ServerInfo{
		ServerType: version.ServerTypeMySQL,
		HasTiKV:    false,
	}
	conf.Snapshot = ""
	conf.Consistency = ConsistencyTypeFlush
	conf.SessionParams = map[string]any{
		"mock": "UTC",
	}
	d.dbHandle = db
	mock.ExpectExec("SET SESSION mock").
		WillReturnError(errors.New("Unknown system variable mock"))
	mock.ExpectClose()
	mock.ExpectClose()

	err = setSessionParam(d)
	require.NoError(t, err)
}
