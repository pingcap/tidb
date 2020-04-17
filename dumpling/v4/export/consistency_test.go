package export

import (
	"errors"
	"strings"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
)

var _ = Suite(&testConsistencySuite{})

type testConsistencySuite struct{}

func (s *testConsistencySuite) assertNil(err error, c *C) {
	if err != nil {
		c.Fatalf(err.Error())
	}
}

func (s *testConsistencySuite) assertLifetimeErrNil(ctrl ConsistencyController, c *C) {
	s.assertNil(ctrl.Setup(), c)
	s.assertNil(ctrl.TearDown(), c)
}

func (s *testConsistencySuite) TestConsistencyController(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conf := DefaultConfig()
	resultOk := sqlmock.NewResult(0, 1)

	conf.Consistency = "none"
	ctrl, _ := NewConsistencyController(conf, db)
	_, ok := ctrl.(*ConsistencyNone)
	c.Assert(ok, IsTrue)
	s.assertLifetimeErrNil(ctrl, c)

	conf.Consistency = "flush"
	mock.ExpectExec("FLUSH TABLES WITH READ LOCK").WillReturnResult(resultOk)
	mock.ExpectExec("UNLOCK TABLES").WillReturnResult(resultOk)
	ctrl, _ = NewConsistencyController(conf, db)
	_, ok = ctrl.(*ConsistencyFlushTableWithReadLock)
	c.Assert(ok, IsTrue)
	s.assertLifetimeErrNil(ctrl, c)
	if err = mock.ExpectationsWereMet(); err != nil {
		c.Fatalf(err.Error())
	}

	conf.Consistency = "snapshot"
	conf.ServerInfo.ServerType = ServerTypeTiDB
	conf.Snapshot = "" // let dumpling detect the TSO
	rows := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"})
	rows.AddRow("tidb-binlog", "413802961528946688", "", "", "")
	tidbRows := sqlmock.NewRows([]string{"c"})
	tidbRows.AddRow(1)
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)
	mock.ExpectQuery("SELECT COUNT\\(1\\) as c FROM MYSQL.TiDB WHERE VARIABLE_NAME='tikv_gc_safe_point'").
		WillReturnRows(tidbRows)
	mock.ExpectExec("SET SESSION tidb_snapshot").
		WillReturnResult(sqlmock.NewResult(0, 1))
	ctrl, _ = NewConsistencyController(conf, db)
	_, ok = ctrl.(*ConsistencySnapshot)
	c.Assert(ok, IsTrue)
	s.assertLifetimeErrNil(ctrl, c)
	if err = mock.ExpectationsWereMet(); err != nil {
		c.Fatalf(err.Error())
	}

	conf.Consistency = "lock"
	conf.Tables = NewDatabaseTables().
		AppendTables("db1", "t1", "t2", "t3").
		AppendViews("db2", "t4")
	for i := 0; i < 4; i++ {
		mock.ExpectExec("LOCK TABLES").WillReturnResult(resultOk)
	}
	mock.ExpectExec("UNLOCK TABLES").WillReturnResult(resultOk)
	ctrl, _ = NewConsistencyController(conf, db)
	_, ok = ctrl.(*ConsistencyLockDumpingTables)
	c.Assert(ok, IsTrue)
	s.assertLifetimeErrNil(ctrl, c)
	if err = mock.ExpectationsWereMet(); err != nil {
		c.Fatalf(err.Error())
	}
}

func (s *testConsistencySuite) TestResolveAutoConsistency(c *C) {
	conf := DefaultConfig()
	cases := []struct {
		serverTp            ServerType
		resolvedConsistency string
	}{
		{ServerTypeTiDB, "snapshot"},
		{ServerTypeMySQL, "flush"},
		{ServerTypeMariaDB, "flush"},
		{ServerTypeUnknown, "none"},
	}

	for _, x := range cases {
		conf.Consistency = "auto"
		conf.ServerInfo.ServerType = x.serverTp
		resolveAutoConsistency(conf)
		cmt := Commentf("server type %s", x.serverTp.String())
		c.Assert(conf.Consistency, Equals, x.resolvedConsistency, cmt)
	}
}

func (s *testConsistencySuite) TestConsistencyControllerError(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conf := DefaultConfig()

	conf.Consistency = "invalid_str"
	_, err = NewConsistencyController(conf, db)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "invalid consistency option"), IsTrue)

	// snapshot consistency is only available in TiDB
	conf.Consistency = "snapshot"
	conf.ServerInfo.ServerType = ServerTypeUnknown
	ctrl, _ := NewConsistencyController(conf, db)
	err = ctrl.Setup()
	c.Assert(err, NotNil)

	// flush consistency is unavailable in TiDB
	conf.Consistency = "flush"
	conf.ServerInfo.ServerType = ServerTypeTiDB
	ctrl, _ = NewConsistencyController(conf, db)
	err = ctrl.Setup()
	c.Assert(err, NotNil)

	// lock table fail
	conf.Consistency = "lock"
	conf.Tables = NewDatabaseTables().AppendTables("db", "t")
	mock.ExpectExec("LOCK TABLE").WillReturnError(errors.New(""))
	ctrl, _ = NewConsistencyController(conf, db)
	err = ctrl.Setup()
	c.Assert(err, NotNil)
}
