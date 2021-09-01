// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"errors"
	"testing"

	tcontext "github.com/pingcap/dumpling/v4/context"
	"github.com/stretchr/testify/require"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

func TestConsistencyController(t *testing.T) {
	t.Parallel()

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

	conf.Consistency = consistencyTypeNone
	ctrl, _ := NewConsistencyController(ctx, conf, db)
	_, ok := ctrl.(*ConsistencyNone)
	require.True(t, ok)
	require.NoError(t, ctrl.Setup(tctx))
	require.NoError(t, ctrl.TearDown(tctx))

	conf.Consistency = consistencyTypeFlush
	mock.ExpectExec("FLUSH TABLES WITH READ LOCK").WillReturnResult(resultOk)
	mock.ExpectExec("UNLOCK TABLES").WillReturnResult(resultOk)
	ctrl, _ = NewConsistencyController(ctx, conf, db)
	_, ok = ctrl.(*ConsistencyFlushTableWithReadLock)
	require.True(t, ok)
	require.NoError(t, ctrl.Setup(tctx))
	require.NoError(t, ctrl.TearDown(tctx))
	require.NoError(t, mock.ExpectationsWereMet())

	conf.Consistency = consistencyTypeSnapshot
	conf.ServerInfo.ServerType = ServerTypeTiDB
	ctrl, _ = NewConsistencyController(ctx, conf, db)
	_, ok = ctrl.(*ConsistencyNone)
	require.True(t, ok)
	require.NoError(t, ctrl.Setup(tctx))
	require.NoError(t, ctrl.TearDown(tctx))

	conf.Consistency = consistencyTypeLock
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
	t.Parallel()

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

	conf.Consistency = consistencyTypeLock
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

func TestResolveAutoConsistency(t *testing.T) {
	t.Parallel()

	conf := defaultConfigForTest(t)
	cases := []struct {
		serverTp            ServerType
		resolvedConsistency string
	}{
		{ServerTypeTiDB, consistencyTypeSnapshot},
		{ServerTypeMySQL, consistencyTypeFlush},
		{ServerTypeMariaDB, consistencyTypeFlush},
		{ServerTypeUnknown, consistencyTypeNone},
	}

	for _, x := range cases {
		conf.Consistency = consistencyTypeAuto
		conf.ServerInfo.ServerType = x.serverTp
		d := &Dumper{conf: conf}
		require.NoError(t, resolveAutoConsistency(d))
		require.Equalf(t, x.resolvedConsistency, conf.Consistency, "server type: %s", x.serverTp.String())
	}
}

func TestConsistencyControllerError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tctx := tcontext.Background().WithContext(ctx)
	conf := defaultConfigForTest(t)

	conf.Consistency = "invalid_str"
	_, err = NewConsistencyController(ctx, conf, db)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid consistency option")

	// snapshot consistency is only available in TiDB
	conf.Consistency = consistencyTypeSnapshot
	conf.ServerInfo.ServerType = ServerTypeUnknown
	_, err = NewConsistencyController(ctx, conf, db)
	require.Error(t, err)

	// flush consistency is unavailable in TiDB
	conf.Consistency = consistencyTypeFlush
	conf.ServerInfo.ServerType = ServerTypeTiDB
	ctrl, _ := NewConsistencyController(ctx, conf, db)
	err = ctrl.Setup(tctx)
	require.Error(t, err)

	// lock table fail
	conf.Consistency = consistencyTypeLock
	conf.Tables = NewDatabaseTables().AppendTables("db", []string{"t"}, []uint64{1})
	mock.ExpectExec("LOCK TABLE").WillReturnError(errors.New(""))
	ctrl, _ = NewConsistencyController(ctx, conf, db)
	err = ctrl.Setup(tctx)
	require.Error(t, err)
}
