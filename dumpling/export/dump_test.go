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

	"github.com/pingcap/tidb/br/pkg/version"
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
	cases := []struct {
		serverInfo  version.ServerInfo
		consistency string
		expected    listTableType
	}{
		{version.ParseServerInfo("5.7.25-TiDB-3.0.6"), consistencyTypeSnapshot, listTableByShowTableStatus},
		// no bug version
		{version.ParseServerInfo("8.0.2"), consistencyTypeLock, listTableByInfoSchema},
		{version.ParseServerInfo("8.0.2"), consistencyTypeFlush, listTableByShowTableStatus},
		{version.ParseServerInfo("8.0.23"), consistencyTypeNone, listTableByShowTableStatus},

		// bug version
		{version.ParseServerInfo("8.0.3"), consistencyTypeLock, listTableByInfoSchema},
		{version.ParseServerInfo("8.0.3"), consistencyTypeFlush, listTableByShowFullTables},
		{version.ParseServerInfo("8.0.3"), consistencyTypeNone, listTableByShowTableStatus},
	}

	for _, x := range cases {
		conf.Consistency = x.consistency
		conf.ServerInfo = x.serverInfo
		require.Equalf(t, x.expected, getListTableTypeByConf(conf), "server info: %s, consistency: %s", x.serverInfo, x.consistency)
	}
}
