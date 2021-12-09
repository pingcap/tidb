// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	tcontext "github.com/pingcap/tidb/dumpling/context"
)

func TestDumpBlock(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	mock.ExpectQuery(fmt.Sprintf("SHOW CREATE DATABASE `%s`", escapeString(database))).
		WillReturnRows(sqlmock.NewRows([]string{"Database", "Create Database"}).
			AddRow("test", "CREATE DATABASE `test` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))

	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()
	defer cancel()
	conn, err := db.Conn(tctx)
	require.NoError(t, err)

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
	// simulate taskChan is full
	taskChan := make(chan Task, 1)
	taskChan <- &TaskDatabaseMeta{}
	d.conf.Tables = DatabaseTables{}.AppendTable(database, nil)
	require.ErrorIs(t, d.dumpDatabases(writerCtx, conn, taskChan), context.Canceled)
	require.ErrorIs(t, wg.Wait(), writerErr)
}

func TestDumpTableMeta(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()
	defer cancel()
	conn, err := db.Conn(tctx)
	require.NoError(t, err)

	conf := DefaultConfig()
	conf.NoSchemas = true

	for serverType := ServerTypeUnknown; serverType < ServerTypeAll; serverType++ {
		conf.ServerInfo.ServerType = ServerType(serverType)
		hasImplicitRowID := false
		mock.ExpectQuery("SHOW COLUMNS FROM").
			WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
				AddRow("id", "int(11)", "NO", "PRI", nil, ""))
		if serverType == ServerTypeTiDB {
			mock.ExpectExec("SELECT _tidb_rowid from").
				WillReturnResult(sqlmock.NewResult(0, 0))
			hasImplicitRowID = true
		}
		mock.ExpectQuery(fmt.Sprintf("SELECT \\* FROM `%s`.`%s`", database, table)).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
		meta, err := dumpTableMeta(conf, conn, database, &TableInfo{Type: TableTypeBase, Name: table})
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
	t.Parallel()

	conf := defaultConfigForTest(t)
	tctx := tcontext.Background().WithLogger(appLogger)
	cases := []struct {
		serverInfo  ServerInfo
		consistency string
		expected    listTableType
	}{
		{ParseServerInfo(tctx, "5.7.25-TiDB-3.0.6"), consistencyTypeSnapshot, listTableByShowTableStatus},
		// no bug version
		{ParseServerInfo(tctx, "8.0.2"), consistencyTypeLock, listTableByInfoSchema},
		{ParseServerInfo(tctx, "8.0.2"), consistencyTypeFlush, listTableByShowTableStatus},
		{ParseServerInfo(tctx, "8.0.23"), consistencyTypeNone, listTableByShowTableStatus},

		// bug version
		{ParseServerInfo(tctx, "8.0.3"), consistencyTypeLock, listTableByInfoSchema},
		{ParseServerInfo(tctx, "8.0.3"), consistencyTypeFlush, listTableByShowFullTables},
		{ParseServerInfo(tctx, "8.0.3"), consistencyTypeNone, listTableByShowTableStatus},
	}

	for _, x := range cases {
		conf.Consistency = x.consistency
		conf.ServerInfo = x.serverInfo
		require.Equalf(t, x.expected, getListTableTypeByConf(conf), "server info: %s, consistency: %s", x.serverInfo, x.consistency)
	}
}
<<<<<<< HEAD
=======

func TestAdjustDatabaseCollation(t *testing.T) {
	t.Parallel()

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
	for i, originSQL := range originSQLs {
		newSQL, err := adjustDatabaseCollation(tctx, parser1, originSQL, charsetAndDefaultCollationMap)
		require.NoError(t, err)
		require.Equal(t, expectedSQLs[i], newSQL)
	}
}

func TestAdjustTableCollation(t *testing.T) {
	t.Parallel()

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
	}

	charsetAndDefaultCollationMap := map[string]string{"utf8mb4": "utf8mb4_general_ci"}
	for i, originSQL := range originSQLs {
		newSQL, err := adjustTableCollation(tctx, parser1, originSQL, charsetAndDefaultCollationMap)
		require.NoError(t, err)
		require.Equal(t, expectedSQLs[i], newSQL)
	}
}
>>>>>>> 9f744cdf8... dumpling: fix default column collation with upstream when dump table (#30531)
