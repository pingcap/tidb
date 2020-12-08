// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"errors"
	"strings"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
)

var _ = Suite(&testConsistencySuite{})

type testConsistencySuite struct{}

func (s *testConsistencySuite) assertNil(err error, c *C) {
	if err != nil {
		c.Fatal(err.Error())
	}
}

func (s *testConsistencySuite) assertLifetimeErrNil(ctx context.Context, ctrl ConsistencyController, c *C) {
	s.assertNil(ctrl.Setup(ctx), c)
	s.assertNil(ctrl.TearDown(ctx), c)
}

func (s *testConsistencySuite) TestConsistencyController(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conf := DefaultConfig()
	resultOk := sqlmock.NewResult(0, 1)

	conf.Consistency = consistencyTypeNone
	ctrl, _ := NewConsistencyController(ctx, conf, db)
	_, ok := ctrl.(*ConsistencyNone)
	c.Assert(ok, IsTrue)
	s.assertLifetimeErrNil(ctx, ctrl, c)

	conf.Consistency = consistencyTypeFlush
	mock.ExpectExec("FLUSH TABLES WITH READ LOCK").WillReturnResult(resultOk)
	mock.ExpectExec("UNLOCK TABLES").WillReturnResult(resultOk)
	ctrl, _ = NewConsistencyController(ctx, conf, db)
	_, ok = ctrl.(*ConsistencyFlushTableWithReadLock)
	c.Assert(ok, IsTrue)
	s.assertLifetimeErrNil(ctx, ctrl, c)
	if err = mock.ExpectationsWereMet(); err != nil {
		c.Fatal(err.Error())
	}

	conf.Consistency = consistencyTypeSnapshot
	conf.ServerInfo.ServerType = ServerTypeTiDB
	ctrl, _ = NewConsistencyController(ctx, conf, db)
	_, ok = ctrl.(*ConsistencyNone)
	c.Assert(ok, IsTrue)
	s.assertLifetimeErrNil(ctx, ctrl, c)

	conf.Consistency = consistencyTypeLock
	conf.Tables = NewDatabaseTables().
		AppendTables("db1", "t1", "t2", "t3").
		AppendViews("db2", "t4")
	mock.ExpectExec("LOCK TABLES `db1`.`t1` READ,`db1`.`t2` READ,`db1`.`t3` READ").WillReturnResult(resultOk)
	mock.ExpectExec("UNLOCK TABLES").WillReturnResult(resultOk)
	ctrl, _ = NewConsistencyController(ctx, conf, db)
	_, ok = ctrl.(*ConsistencyLockDumpingTables)
	c.Assert(ok, IsTrue)
	s.assertLifetimeErrNil(ctx, ctrl, c)
	if err = mock.ExpectationsWereMet(); err != nil {
		c.Fatal(err.Error())
	}
}

func (s *testConsistencySuite) TestConsistencyLockControllerRetry(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conf := DefaultConfig()
	resultOk := sqlmock.NewResult(0, 1)

	conf.Consistency = consistencyTypeLock
	conf.Tables = NewDatabaseTables().
		AppendTables("db1", "t1", "t2", "t3").
		AppendViews("db2", "t4")
	mock.ExpectExec("LOCK TABLES `db1`.`t1` READ,`db1`.`t2` READ,`db1`.`t3` READ").
		WillReturnError(&mysql.MySQLError{Number: ErrNoSuchTable, Message: "Table 'db1.t3' doesn't exist"})
	mock.ExpectExec("LOCK TABLES `db1`.`t1` READ,`db1`.`t2` READ").WillReturnResult(resultOk)
	mock.ExpectExec("UNLOCK TABLES").WillReturnResult(resultOk)
	ctrl, _ := NewConsistencyController(ctx, conf, db)
	_, ok := ctrl.(*ConsistencyLockDumpingTables)
	c.Assert(ok, IsTrue)
	s.assertLifetimeErrNil(ctx, ctrl, c)
	if err = mock.ExpectationsWereMet(); err != nil {
		c.Fatal(err.Error())
	}
}

func (s *testConsistencySuite) TestResolveAutoConsistency(c *C) {
	conf := DefaultConfig()
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
		c.Assert(resolveAutoConsistency(d), IsNil)
		cmt := Commentf("server type %s", x.serverTp.String())
		c.Assert(conf.Consistency, Equals, x.resolvedConsistency, cmt)
	}
}

func (s *testConsistencySuite) TestConsistencyControllerError(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conf := DefaultConfig()

	conf.Consistency = "invalid_str"
	_, err = NewConsistencyController(ctx, conf, db)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "invalid consistency option"), IsTrue)

	// snapshot consistency is only available in TiDB
	conf.Consistency = consistencyTypeSnapshot
	conf.ServerInfo.ServerType = ServerTypeUnknown
	_, err = NewConsistencyController(ctx, conf, db)
	c.Assert(err, NotNil)

	// flush consistency is unavailable in TiDB
	conf.Consistency = consistencyTypeFlush
	conf.ServerInfo.ServerType = ServerTypeTiDB
	ctrl, _ := NewConsistencyController(ctx, conf, db)
	err = ctrl.Setup(ctx)
	c.Assert(err, NotNil)

	// lock table fail
	conf.Consistency = consistencyTypeLock
	conf.Tables = NewDatabaseTables().AppendTables("db", "t")
	mock.ExpectExec("LOCK TABLE").WillReturnError(errors.New(""))
	ctrl, _ = NewConsistencyController(ctx, conf, db)
	err = ctrl.Setup(ctx)
	c.Assert(err, NotNil)
}
