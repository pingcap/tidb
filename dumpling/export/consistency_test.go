// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/br/pkg/version"
	dbconfig "github.com/pingcap/tidb/config"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/errno"
	"github.com/stretchr/testify/require"
)

func TestConsistencyController(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tctx := tcontext.Background().WithContext(ctx).WithLogger(appLogger)
	conf := defaultConfigForTest(t)
	resultOk := sqlmock.NewResult(0, 1)

	conf.Consistency = ConsistencyTypeNone
	ctrl, _ := NewConsistencyController(ctx, conf, db)
	_, ok := ctrl.(*ConsistencyNone)
	require.True(t, ok)
	require.NoError(t, ctrl.Setup(tctx))
	require.NoError(t, ctrl.TearDown(tctx))

	conf.Consistency = ConsistencyTypeFlush
	mock.ExpectExec("FLUSH TABLES WITH READ LOCK").WillReturnResult(resultOk)
	mock.ExpectExec("UNLOCK TABLES").WillReturnResult(resultOk)
	ctrl, _ = NewConsistencyController(ctx, conf, db)
	_, ok = ctrl.(*ConsistencyFlushTableWithReadLock)
	require.True(t, ok)
	require.NoError(t, ctrl.Setup(tctx))
	require.NoError(t, ctrl.TearDown(tctx))
	require.NoError(t, mock.ExpectationsWereMet())

	conf.Consistency = ConsistencyTypeSnapshot
	conf.ServerInfo.ServerType = version.ServerTypeTiDB
	ctrl, _ = NewConsistencyController(ctx, conf, db)
	_, ok = ctrl.(*ConsistencyNone)
	require.True(t, ok)
	require.NoError(t, ctrl.Setup(tctx))
	require.NoError(t, ctrl.TearDown(tctx))

	conf.ServerInfo.ServerType = version.ServerTypeMySQL
	conf.Consistency = ConsistencyTypeLock
	conf.Tables = NewDatabaseTables().
		AppendTables("db1", []string{"t1", "t2", "t3"}, []uint64{1, 2, 3}).
		AppendViews("db2", "t4")
	mock.ExpectExec("LOCK TABLES `db1`.`t1` READ,`db1`.`t2` READ,`db1`.`t3` READ").WillReturnResult(resultOk)
	mock.ExpectExec("UNLOCK TABLES").WillReturnResult(resultOk)
	ctrl, _ = NewConsistencyController(ctx, conf, db)
	_, ok = ctrl.(*ConsistencyLockDumpingTables)
	require.True(t, ok)
	require.NoError(t, ctrl.Setup(tctx))
	require.NoError(t, ctrl.TearDown(tctx))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestConsistencyLockControllerRetry(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tctx := tcontext.Background().WithContext(ctx)
	conf := defaultConfigForTest(t)
	resultOk := sqlmock.NewResult(0, 1)

	conf.ServerInfo.ServerType = version.ServerTypeMySQL
	conf.Consistency = ConsistencyTypeLock
	conf.Tables = NewDatabaseTables().
		AppendTables("db1", []string{"t1", "t2", "t3"}, []uint64{1, 2, 3}).
		AppendViews("db2", "t4")
	mock.ExpectExec("LOCK TABLES `db1`.`t1` READ,`db1`.`t2` READ,`db1`.`t3` READ").
		WillReturnError(&mysql.MySQLError{Number: ErrNoSuchTable, Message: "Table 'db1.t3' doesn't exist"})
	mock.ExpectExec("LOCK TABLES `db1`.`t1` READ,`db1`.`t2` READ").WillReturnResult(resultOk)
	mock.ExpectExec("UNLOCK TABLES").WillReturnResult(resultOk)
	ctrl, _ := NewConsistencyController(ctx, conf, db)
	_, ok := ctrl.(*ConsistencyLockDumpingTables)
	require.True(t, ok)
	require.NoError(t, ctrl.Setup(tctx))
	require.NoError(t, ctrl.TearDown(tctx))

	// should remove table db1.t3 in tables to dump
	expectedDumpTables := NewDatabaseTables().
		AppendTables("db1", []string{"t1", "t2"}, []uint64{1, 2}).
		AppendViews("db2", "t4")
	require.Equal(t, expectedDumpTables, conf.Tables)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestConsistencyLockControllerEmpty(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tctx := tcontext.Background().WithContext(ctx)
	conf := defaultConfigForTest(t)

	conf.ServerInfo.ServerType = version.ServerTypeMySQL
	conf.Consistency = ConsistencyTypeLock
	conf.Tables = NewDatabaseTables().
		AppendTables("db1", []string{"t1"}, []uint64{1}).
		AppendViews("db2", "t4")
	mock.ExpectExec("LOCK TABLES `db1`.`t1` READ").
		WillReturnError(&mysql.MySQLError{Number: ErrNoSuchTable, Message: "Table 'db1.t1' doesn't exist"})
	ctrl, _ := NewConsistencyController(ctx, conf, db)
	_, ok := ctrl.(*ConsistencyLockDumpingTables)
	require.True(t, ok)
	require.NoError(t, ctrl.Setup(tctx))
	require.NoError(t, ctrl.TearDown(tctx))

	// should remove table db1.t1 in tables to dump
	expectedDumpTables := NewDatabaseTables().
		AppendViews("db2", "t4")
	expectedDumpTables["db1"] = make([]*TableInfo, 0)
	require.Equal(t, expectedDumpTables, conf.Tables)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestResolveAutoConsistency(t *testing.T) {
	conf := defaultConfigForTest(t)
	cases := []struct {
		serverTp            version.ServerType
		resolvedConsistency string
	}{
		{version.ServerTypeTiDB, ConsistencyTypeSnapshot},
		{version.ServerTypeUnknown, ConsistencyTypeNone},
	}

	for _, x := range cases {
		conf.Consistency = ConsistencyTypeAuto
		conf.ServerInfo.ServerType = x.serverTp
		d := &Dumper{conf: conf}
		require.NoError(t, resolveAutoConsistency(d))
		require.Equalf(t, x.resolvedConsistency, conf.Consistency, "server type: %s", x.serverTp.String())
	}

	cases = []struct {
		serverTp            version.ServerType
		resolvedConsistency string
	}{
		{version.ServerTypeMySQL, ConsistencyTypeFlush},
		{version.ServerTypeMariaDB, ConsistencyTypeFlush},
	}

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	tctx := tcontext.Background()
	resultOk := sqlmock.NewResult(0, 1)

	for _, x := range cases {
		conf.Consistency = ConsistencyTypeAuto
		conf.ServerInfo.ServerType = x.serverTp
		d := &Dumper{tctx: tctx, conf: conf, dbHandle: db}
		mock.ExpectExec("FLUSH TABLES WITH READ LOCK").WillReturnResult(resultOk)
		require.NoError(t, resolveAutoConsistency(d))
		require.Equalf(t, x.resolvedConsistency, conf.Consistency, "server type: %s", x.serverTp.String())
	}

	// test no SUPER privilege will fallback to ConsistencyTypeLock
	for _, x := range cases {
		conf.Consistency = ConsistencyTypeAuto
		conf.ServerInfo.ServerType = x.serverTp
		d := &Dumper{tctx: tctx, conf: conf, dbHandle: db}
		mockErr := &mysql.MySQLError{
			Number:  errno.ErrAccessDenied,
			Message: "Couldn't execute 'FLUSH TABLES WITH READ LOCK': Access denied for user 'root'@'%' (using password: YES)",
		}
		mock.ExpectExec("FLUSH TABLES WITH READ LOCK").WillReturnError(mockErr)
		require.NoError(t, resolveAutoConsistency(d))
		require.Equalf(t, ConsistencyTypeLock, conf.Consistency, "server type: %s", x.serverTp.String())
	}
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestConsistencyControllerError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tctx := tcontext.Background().WithContext(ctx).WithLogger(appLogger)
	conf := defaultConfigForTest(t)

	conf.Consistency = "invalid_str"
	_, err = NewConsistencyController(ctx, conf, db)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid consistency option")

	// snapshot consistency is only available in TiDB
	conf.Consistency = ConsistencyTypeSnapshot
	conf.ServerInfo.ServerType = version.ServerTypeUnknown
	_, err = NewConsistencyController(ctx, conf, db)
	require.Error(t, err)

	// flush consistency is unavailable in TiDB
	conf.Consistency = ConsistencyTypeFlush
	conf.ServerInfo.ServerType = version.ServerTypeTiDB
	ctrl, _ := NewConsistencyController(ctx, conf, db)
	err = ctrl.Setup(tctx)
	require.Error(t, err)

	// lock table fail
	conf.Consistency = ConsistencyTypeLock
	conf.Tables = NewDatabaseTables().AppendTables("db", []string{"t"}, []uint64{1})
	mock.ExpectExec("LOCK TABLE").WillReturnError(errors.New(""))
	ctrl, _ = NewConsistencyController(ctx, conf, db)
	err = ctrl.Setup(tctx)
	require.Error(t, err)
}

func TestConsistencyLockTiDBCheck(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tctx := tcontext.Background().WithContext(ctx).WithLogger(appLogger)
	conf := defaultConfigForTest(t)
	resultOk := sqlmock.NewResult(0, 1)

	conf.ServerInfo.ServerType = version.ServerTypeTiDB
	conf.Consistency = ConsistencyTypeLock
	conf.Tables = NewDatabaseTables().
		AppendTables("db1", []string{"t1"}, []uint64{1})
	ctrl, err := NewConsistencyController(ctx, conf, db)
	require.NoError(t, err)

	// no tidb_config found, don't allow to lock tables
	unknownSysVarErr := errors.New("ERROR 1193 (HY000): Unknown system variable 'tidb_config'")
	mock.ExpectQuery("SELECT @@tidb_config").WillReturnError(unknownSysVarErr)
	err = ctrl.Setup(tctx)
	require.ErrorIs(t, err, unknownSysVarErr)
	require.NoError(t, mock.ExpectationsWereMet())

	// enable-table-lock is false, don't allow to lock tables
	tidbConf := dbconfig.NewConfig()
	tidbConf.EnableTableLock = false
	tidbConfBytes, err := json.Marshal(tidbConf)
	require.NoError(t, err)
	mock.ExpectQuery("SELECT @@tidb_config").WillReturnRows(
		sqlmock.NewRows([]string{"@@tidb_config"}).AddRow(string(tidbConfBytes)))
	err = ctrl.Setup(tctx)
	require.ErrorIs(t, err, errTiDBDisableTableLock)
	require.NoError(t, mock.ExpectationsWereMet())

	// enable-table-lock is true, allow to lock tables
	tidbConf.EnableTableLock = true
	tidbConfBytes, err = json.Marshal(tidbConf)
	require.NoError(t, err)
	mock.ExpectQuery("SELECT @@tidb_config").WillReturnRows(
		sqlmock.NewRows([]string{"@@tidb_config"}).AddRow(string(tidbConfBytes)))
	mock.ExpectExec("LOCK TABLES `db1`.`t1` READ").WillReturnResult(resultOk)
	mock.ExpectExec("UNLOCK TABLES").WillReturnResult(resultOk)
	err = ctrl.Setup(tctx)
	require.NoError(t, err)
	require.NoError(t, ctrl.TearDown(tctx))
	require.NoError(t, mock.ExpectationsWereMet())
}
