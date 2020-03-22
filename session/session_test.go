// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package session_test

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tipb/go-binlog"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

var (
	withTiKV        = flag.Bool("with-tikv", false, "run tests with TiKV cluster started. (not use the mock server)")
	pdAddrs         = flag.String("pd-addrs", "127.0.0.1:2379", "pd addrs")
	pdAddrChan      chan string
	initPdAddrsOnce sync.Once
)

var _ = Suite(&testSessionSuite{})
var _ = Suite(&testSessionSuite2{})
var _ = Suite(&testSessionSuite3{})
var _ = Suite(&testSchemaSuite{})
var _ = Suite(&testIsolationSuite{})
var _ = SerialSuites(&testSchemaSerialSuite{})
var _ = SerialSuites(&testSessionSerialSuite{})

type testSessionSuiteBase struct {
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	store     kv.Storage
	dom       *domain.Domain
	pdAddr    string
}

type testSessionSuite struct {
	testSessionSuiteBase
}

type testSessionSuite2 struct {
	testSessionSuiteBase
}

type testSessionSuite3 struct {
	testSessionSuiteBase
}

type testSessionSerialSuite struct {
	testSessionSuiteBase
}

func clearStorage(store kv.Storage) error {
	txn, err := store.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	iter, err := txn.Iter(nil, nil)
	if err != nil {
		return errors.Trace(err)
	}
	for iter.Valid() {
		txn.Delete(iter.Key())
		if err := iter.Next(); err != nil {
			return errors.Trace(err)
		}
	}
	return txn.Commit(context.Background())
}

func clearETCD(ebd tikv.EtcdBackend) error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:        ebd.EtcdAddrs(),
		AutoSyncInterval: 30 * time.Second,
		DialTimeout:      5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithBackoffMaxDelay(time.Second * 3),
		},
		TLS: ebd.TLSConfig(),
	})
	if err != nil {
		return errors.Trace(err)
	}
	defer cli.Close()

	leases, err := cli.Leases(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	for _, resp := range leases.Leases {
		if _, err := cli.Revoke(context.Background(), resp.ID); err != nil {
			return errors.Trace(err)
		}
	}
	_, err = cli.Delete(context.Background(), "/tidb", clientv3.WithPrefix())
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func initPdAddrs() {
	initPdAddrsOnce.Do(func() {
		addrs := strings.Split(*pdAddrs, ",")
		pdAddrChan = make(chan string, len(addrs))
		for _, addr := range addrs {
			addr = strings.TrimSpace(addr)
			if addr != "" {
				pdAddrChan <- addr
			}
		}
	})
}

func (s *testSessionSuiteBase) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.cluster = mocktikv.NewCluster()

	if *withTiKV {
		initPdAddrs()
		s.pdAddr = <-pdAddrChan
		var d tikv.Driver
		config.GetGlobalConfig().TxnLocalLatches.Enabled = false
		store, err := d.Open(fmt.Sprintf("tikv://%s", s.pdAddr))
		c.Assert(err, IsNil)
		err = clearStorage(store)
		c.Assert(err, IsNil)
		err = clearETCD(store.(tikv.EtcdBackend))
		c.Assert(err, IsNil)
		session.ResetStoreForWithTiKVTest(store)
		s.store = store
	} else {
		mocktikv.BootstrapWithSingleStore(s.cluster)
		s.mvccStore = mocktikv.MustNewMVCCStore()
		store, err := mockstore.NewMockTikvStore(
			mockstore.WithCluster(s.cluster),
			mockstore.WithMVCCStore(s.mvccStore),
		)
		c.Assert(err, IsNil)
		s.store = store
		session.DisableStats4Test()
	}

	var err error
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.dom.GetGlobalVarsCache().Disable()
}

func (s *testSessionSuiteBase) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
	if *withTiKV {
		pdAddrChan <- s.pdAddr
	}
}

func (s *testSessionSuiteBase) TearDownTest(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	r := tk.MustQuery("show full tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tableType := tb[1]
		if tableType == "VIEW" {
			tk.MustExec(fmt.Sprintf("drop view %v", tableName))
		} else if tableType == "BASE TABLE" {
			tk.MustExec(fmt.Sprintf("drop table %v", tableName))
		} else {
			panic(fmt.Sprintf("Unexpected table '%s' with type '%s'.", tableName, tableType))
		}
	}
}

type mockBinlogPump struct {
}

var _ binlog.PumpClient = &mockBinlogPump{}

func (p *mockBinlogPump) WriteBinlog(ctx context.Context, in *binlog.WriteBinlogReq, opts ...grpc.CallOption) (*binlog.WriteBinlogResp, error) {
	return &binlog.WriteBinlogResp{}, nil
}

type mockPumpPullBinlogsClient struct {
	grpc.ClientStream
}

func (m mockPumpPullBinlogsClient) Recv() (*binlog.PullBinlogResp, error) {
	return nil, nil
}

func (p *mockBinlogPump) PullBinlogs(ctx context.Context, in *binlog.PullBinlogReq, opts ...grpc.CallOption) (binlog.Pump_PullBinlogsClient, error) {
	return mockPumpPullBinlogsClient{mocktikv.MockGRPCClientStream()}, nil
}

func (s *testSessionSuite) TestForCoverage(c *C) {
	// Just for test coverage.
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int auto_increment, v int, index (id))")
	tk.MustExec("insert t values ()")
	tk.MustExec("insert t values ()")
	tk.MustExec("insert t values ()")

	// Normal request will not cover txn.Seek.
	tk.MustExec("admin check table t")

	// Cover dirty table operations in StateTxn.
	tk.Se.GetSessionVars().BinlogClient = binloginfo.MockPumpsClient(&mockBinlogPump{})
	tk.MustExec("begin")
	tk.MustExec("truncate table t")
	tk.MustExec("insert t values ()")
	tk.MustExec("delete from t where id = 2")
	tk.MustExec("update t set v = 5 where id = 2")
	tk.MustExec("insert t values ()")
	tk.MustExec("rollback")

	c.Check(tk.Se.SetCollation(mysql.DefaultCollationID), IsNil)

	tk.MustExec("show processlist")
	_, err := tk.Se.FieldList("t")
	c.Check(err, IsNil)
}

func (s *testSessionSuite2) TestErrorRollback(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t_rollback")
	tk.MustExec("create table t_rollback (c1 int, c2 int, primary key(c1))")
	tk.MustExec("insert into t_rollback values (0, 0)")

	var wg sync.WaitGroup
	cnt := 4
	wg.Add(cnt)
	num := 20

	for i := 0; i < cnt; i++ {
		go func() {
			defer wg.Done()
			localTk := testkit.NewTestKitWithInit(c, s.store)
			localTk.MustExec("set @@session.tidb_retry_limit = 100")
			for j := 0; j < num; j++ {
				localTk.Exec("insert into t_rollback values (1, 1)")
				localTk.MustExec("update t_rollback set c2 = c2 + 1 where c1 = 0")
			}
		}()
	}

	wg.Wait()
	tk.MustQuery("select c2 from t_rollback where c1 = 0").Check(testkit.Rows(fmt.Sprint(cnt * num)))
}

func (s *testSessionSuite) TestQueryString(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("create table mutil1 (a int);create table multi2 (a int)")
	queryStr := tk.Se.Value(sessionctx.QueryString)
	c.Assert(queryStr, Equals, "create table multi2 (a int)")

	// Test execution of DDL through the "ExecutePreparedStmt" interface.
	_, err := tk.Se.Execute(context.Background(), "use test;")
	c.Assert(err, IsNil)
	_, err = tk.Se.Execute(context.Background(), "CREATE TABLE t (id bigint PRIMARY KEY, age int)")
	c.Assert(err, IsNil)
	_, err = tk.Se.Execute(context.Background(), "show create table t")
	c.Assert(err, IsNil)
	id, _, _, err := tk.Se.PrepareStmt("CREATE TABLE t2(id bigint PRIMARY KEY, age int)")
	c.Assert(err, IsNil)
	params := []types.Datum{}
	_, err = tk.Se.ExecutePreparedStmt(context.Background(), id, params)
	c.Assert(err, IsNil)
	qs := tk.Se.Value(sessionctx.QueryString)
	c.Assert(qs.(string), Equals, "CREATE TABLE t2(id bigint PRIMARY KEY, age int)")
}

func (s *testSessionSuite) TestAffectedRows(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id TEXT)")
	tk.MustExec(`INSERT INTO t VALUES ("a");`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 1)
	tk.MustExec(`INSERT INTO t VALUES ("b");`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 1)
	tk.MustExec(`UPDATE t set id = 'c' where id = 'a';`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 1)
	tk.MustExec(`UPDATE t set id = 'a' where id = 'a';`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 0)
	tk.MustQuery(`SELECT * from t`).Check(testkit.Rows("c", "b"))
	c.Assert(int(tk.Se.AffectedRows()), Equals, 0)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, data int)")
	tk.MustExec(`INSERT INTO t VALUES (1, 0), (0, 0), (1, 1);`)
	tk.MustExec(`UPDATE t set id = 1 where data = 0;`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 1)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, c1 timestamp);")
	tk.MustExec(`insert t values(1, 0);`)
	tk.MustExec(`UPDATE t set id = 1 where id = 1;`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 0)

	// With ON DUPLICATE KEY UPDATE, the affected-rows value per row is 1 if the row is inserted as a new row,
	// 2 if an existing row is updated, and 0 if an existing row is set to its current values.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int PRIMARY KEY, c2 int);")
	tk.MustExec(`insert t values(1, 1);`)
	tk.MustExec(`insert into t values (1, 1) on duplicate key update c2=2;`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 2)
	tk.MustExec(`insert into t values (1, 1) on duplicate key update c2=2;`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 0)
	tk.MustExec("drop table if exists test")
	createSQL := `CREATE TABLE test (
	  id        VARCHAR(36) PRIMARY KEY NOT NULL,
	  factor    INTEGER                 NOT NULL                   DEFAULT 2);`
	tk.MustExec(createSQL)
	insertSQL := `INSERT INTO test(id) VALUES('id') ON DUPLICATE KEY UPDATE factor=factor+3;`
	tk.MustExec(insertSQL)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 1)
	tk.MustExec(insertSQL)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 2)
	tk.MustExec(insertSQL)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 2)

	tk.Se.SetClientCapability(mysql.ClientFoundRows)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, data int)")
	tk.MustExec(`INSERT INTO t VALUES (1, 0), (0, 0), (1, 1);`)
	tk.MustExec(`UPDATE t set id = 1 where data = 0;`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 2)
}

func (s *testSessionSuite3) TestLastMessage(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id TEXT)")

	// Insert
	tk.MustExec(`INSERT INTO t VALUES ("a");`)
	tk.CheckLastMessage("")
	tk.MustExec(`INSERT INTO t VALUES ("b"), ("c");`)
	tk.CheckLastMessage("Records: 2  Duplicates: 0  Warnings: 0")

	// Update
	tk.MustExec(`UPDATE t set id = 'c' where id = 'a';`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 1)
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	tk.MustExec(`UPDATE t set id = 'a' where id = 'a';`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 0)
	tk.CheckLastMessage("Rows matched: 0  Changed: 0  Warnings: 0")

	// Replace
	tk.MustExec(`drop table if exists t, t1;
        create table t (c1 int PRIMARY KEY, c2 int);
        create table t1 (a1 int, a2 int);`)
	tk.MustExec(`INSERT INTO t VALUES (1,1)`)
	tk.MustExec(`REPLACE INTO t VALUES (2,2)`)
	tk.CheckLastMessage("")
	tk.MustExec(`INSERT INTO t1 VALUES (1,10), (3,30);`)
	tk.CheckLastMessage("Records: 2  Duplicates: 0  Warnings: 0")
	tk.MustExec(`REPLACE INTO t SELECT * from t1`)
	tk.CheckLastMessage("Records: 2  Duplicates: 1  Warnings: 0")

	// Check insert with CLIENT_FOUND_ROWS is set
	tk.Se.SetClientCapability(mysql.ClientFoundRows)
	tk.MustExec(`drop table if exists t, t1;
        create table t (c1 int PRIMARY KEY, c2 int);
        create table t1 (a1 int, a2 int);`)
	tk.MustExec(`INSERT INTO t1 VALUES (1, 10), (2, 2), (3, 30);`)
	tk.MustExec(`INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30);`)
	tk.MustExec(`INSERT INTO t SELECT * FROM t1 ON DUPLICATE KEY UPDATE c2=a2;`)
	tk.CheckLastMessage("Records: 6  Duplicates: 3  Warnings: 0")
}

// TestRowLock . See http://dev.mysql.com/doc/refman/5.7/en/commit.html.
func (s *testSessionSuite) TestRowLock(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	txn, err := tk.Se.Txn(true)
	c.Assert(kv.ErrInvalidTxn.Equal(err), IsTrue)
	c.Assert(txn.Valid(), IsFalse)
	tk.MustExec("create table t (c1 int, c2 int, c3 int)")
	tk.MustExec("insert t values (11, 2, 3)")
	tk.MustExec("insert t values (12, 2, 3)")
	tk.MustExec("insert t values (13, 2, 3)")

	tk1.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk1.MustExec("begin")
	tk1.MustExec("update t set c2=21 where c1=11")

	tk2.MustExec("begin")
	tk2.MustExec("update t set c2=211 where c1=11")
	tk2.MustExec("commit")

	// tk1 will retry and the final value is 21
	tk1.MustExec("commit")

	// Check the result is correct
	tk.MustQuery("select c2 from t where c1=11").Check(testkit.Rows("21"))

	tk1.MustExec("begin")
	tk1.MustExec("update t set c2=21 where c1=11")

	tk2.MustExec("begin")
	tk2.MustExec("update t set c2=22 where c1=12")
	tk2.MustExec("commit")

	tk1.MustExec("commit")
}

// TestAutocommit . See https://dev.mysql.com/doc/internals/en/status-flags.html
func (s *testSessionSuite) TestAutocommit(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t;")
	c.Assert(int(tk.Se.Status()&mysql.ServerStatusAutocommit), Greater, 0)
	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	c.Assert(int(tk.Se.Status()&mysql.ServerStatusAutocommit), Greater, 0)
	tk.MustExec("insert t values ()")
	c.Assert(int(tk.Se.Status()&mysql.ServerStatusAutocommit), Greater, 0)
	tk.MustExec("begin")
	c.Assert(int(tk.Se.Status()&mysql.ServerStatusAutocommit), Greater, 0)
	tk.MustExec("insert t values ()")
	c.Assert(int(tk.Se.Status()&mysql.ServerStatusAutocommit), Greater, 0)
	tk.MustExec("drop table if exists t")
	c.Assert(int(tk.Se.Status()&mysql.ServerStatusAutocommit), Greater, 0)

	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	c.Assert(int(tk.Se.Status()&mysql.ServerStatusAutocommit), Greater, 0)
	tk.MustExec("set autocommit=0")
	c.Assert(int(tk.Se.Status()&mysql.ServerStatusAutocommit), Equals, 0)
	tk.MustExec("insert t values ()")
	c.Assert(int(tk.Se.Status()&mysql.ServerStatusAutocommit), Equals, 0)
	tk.MustExec("commit")
	c.Assert(int(tk.Se.Status()&mysql.ServerStatusAutocommit), Equals, 0)
	tk.MustExec("drop table if exists t")
	c.Assert(int(tk.Se.Status()&mysql.ServerStatusAutocommit), Equals, 0)
	tk.MustExec("set autocommit='On'")
	c.Assert(int(tk.Se.Status()&mysql.ServerStatusAutocommit), Greater, 0)

	// When autocommit is 0, transaction start ts should be the first *valid*
	// statement, rather than *any* statement.
	tk.MustExec("create table t (id int)")
	tk.MustExec("set @@autocommit = 0")
	tk.MustExec("rollback")
	tk.MustExec("set @@autocommit = 0")
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustExec("insert into t select 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))

	// TODO: MySQL compatibility for setting global variable.
	// tk.MustExec("begin")
	// tk.MustExec("insert into t values (42)")
	// tk.MustExec("set @@global.autocommit = 1")
	// tk.MustExec("rollback")
	// tk.MustQuery("select count(*) from t where id = 42").Check(testkit.Rows("0"))
	// Even the transaction is rollbacked, the set statement succeed.
	// tk.MustQuery("select @@global.autocommit").Rows("1")
}

// TestTxnLazyInitialize tests that when autocommit = 0, not all statement starts
// a new transaction.
func (s *testSessionSuite) TestTxnLazyInitialize(c *C) {
	testTxnLazyInitialize(s, c, false)
	testTxnLazyInitialize(s, c, true)
}

func testTxnLazyInitialize(s *testSessionSuite, c *C, isPessimistic bool) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int)")
	if isPessimistic {
		tk.MustExec("set tidb_txn_mode = 'pessimistic'")
	}

	tk.MustExec("set @@autocommit = 0")
	txn, err := tk.Se.Txn(true)
	c.Assert(kv.ErrInvalidTxn.Equal(err), IsTrue)
	txn, err = tk.Se.Txn(false)
	c.Assert(err, IsNil)
	c.Assert(txn.Valid(), IsFalse)
	tk.MustQuery("select @@tidb_current_ts").Check(testkit.Rows("0"))
	tk.MustQuery("select @@tidb_current_ts").Check(testkit.Rows("0"))

	// Those statement should not start a new transaction automacally.
	tk.MustQuery("select 1")
	tk.MustQuery("select @@tidb_current_ts").Check(testkit.Rows("0"))

	tk.MustExec("set @@tidb_general_log = 0")
	tk.MustQuery("select @@tidb_current_ts").Check(testkit.Rows("0"))

	tk.MustQuery("explain select * from t")
	tk.MustQuery("select @@tidb_current_ts").Check(testkit.Rows("0"))

	// Begin statement should start a new transaction.
	tk.MustExec("begin")
	txn, err = tk.Se.Txn(false)
	c.Assert(err, IsNil)
	c.Assert(txn.Valid(), IsTrue)
	tk.MustExec("rollback")

	tk.MustExec("select * from t")
	txn, err = tk.Se.Txn(false)
	c.Assert(err, IsNil)
	c.Assert(txn.Valid(), IsTrue)
	tk.MustExec("rollback")

	tk.MustExec("insert into t values (1)")
	txn, err = tk.Se.Txn(false)
	c.Assert(err, IsNil)
	c.Assert(txn.Valid(), IsTrue)
	tk.MustExec("rollback")
}

func (s *testSessionSuite) TestGlobalVarAccessor(c *C) {
	varName := "max_allowed_packet"
	varValue := "67108864" // This is the default value for max_allowed_packet
	varValue1 := "4194305"
	varValue2 := "4194306"

	tk := testkit.NewTestKitWithInit(c, s.store)
	se := tk.Se.(variable.GlobalVarAccessor)
	// Get globalSysVar twice and get the same value
	v, err := se.GetGlobalSysVar(varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, varValue)
	v, err = se.GetGlobalSysVar(varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, varValue)
	// Set global var to another value
	err = se.SetGlobalSysVar(varName, varValue1)
	c.Assert(err, IsNil)
	v, err = se.GetGlobalSysVar(varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, varValue1)
	c.Assert(tk.Se.CommitTxn(context.TODO()), IsNil)

	tk1 := testkit.NewTestKitWithInit(c, s.store)
	se1 := tk1.Se.(variable.GlobalVarAccessor)
	v, err = se1.GetGlobalSysVar(varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, varValue1)
	err = se1.SetGlobalSysVar(varName, varValue2)
	c.Assert(err, IsNil)
	v, err = se1.GetGlobalSysVar(varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, varValue2)
	c.Assert(tk1.Se.CommitTxn(context.TODO()), IsNil)

	// Make sure the change is visible to any client that accesses that global variable.
	v, err = se.GetGlobalSysVar(varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, varValue2)

	// For issue 10955, make sure the new session load `max_execution_time` into sessionVars.
	s.dom.GetGlobalVarsCache().Disable()
	tk1.MustExec("set @@global.max_execution_time = 100")
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	c.Assert(tk2.Se.GetSessionVars().MaxExecutionTime, Equals, uint64(100))
	tk1.MustExec("set @@global.max_execution_time = 0")

	result := tk.MustQuery("show global variables  where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 18446744073709551615"))
	result = tk.MustQuery("show session variables  where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 18446744073709551615"))
	tk.MustExec("set session sql_select_limit=100000000000;")
	result = tk.MustQuery("show global variables where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 18446744073709551615"))
	result = tk.MustQuery("show session variables where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 100000000000"))

	result = tk.MustQuery("select @@global.autocommit;")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select @@autocommit;")
	result.Check(testkit.Rows("1"))
	tk.MustExec("set @@global.autocommit = 0;")
	result = tk.MustQuery("select @@global.autocommit;")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select @@autocommit;")
	result.Check(testkit.Rows("1"))
	tk.MustExec("set @@global.autocommit=1")

	_, err = tk.Exec("set global time_zone = 'timezone'")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, variable.ErrUnknownTimeZone), IsTrue)
}

func (s *testSessionSuite) TestGetSysVariables(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	// Test ScopeSession
	tk.MustExec("select @@warning_count")
	tk.MustExec("select @@session.warning_count")
	tk.MustExec("select @@local.warning_count")
	_, err := tk.Exec("select @@global.warning_count")
	c.Assert(terror.ErrorEqual(err, variable.ErrIncorrectScope), IsTrue, Commentf("err %v", err))

	// Test ScopeGlobal
	tk.MustExec("select @@max_connections")
	tk.MustExec("select @@global.max_connections")
	_, err = tk.Exec("select @@session.max_connections")
	c.Assert(terror.ErrorEqual(err, variable.ErrIncorrectScope), IsTrue, Commentf("err %v", err))
	_, err = tk.Exec("select @@local.max_connections")
	c.Assert(terror.ErrorEqual(err, variable.ErrIncorrectScope), IsTrue, Commentf("err %v", err))

	// Test ScopeNone
	tk.MustExec("select @@performance_schema_max_mutex_classes")
	tk.MustExec("select @@global.performance_schema_max_mutex_classes")
	_, err = tk.Exec("select @@session.performance_schema_max_mutex_classes")
	c.Assert(terror.ErrorEqual(err, variable.ErrIncorrectScope), IsTrue, Commentf("err %v", err))
	_, err = tk.Exec("select @@local.performance_schema_max_mutex_classes")
	c.Assert(terror.ErrorEqual(err, variable.ErrIncorrectScope), IsTrue, Commentf("err %v", err))
}

func (s *testSessionSuite) TestRetryResetStmtCtx(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table retrytxn (a int unique, b int)")
	tk.MustExec("insert retrytxn values (1, 1)")
	tk.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk.MustExec("begin")
	tk.MustExec("update retrytxn set b = b + 1 where a = 1")

	// Make retryable error.
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustExec("update retrytxn set b = b + 1 where a = 1")

	err := tk.Se.CommitTxn(context.TODO())
	c.Assert(err, IsNil)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
}

func (s *testSessionSuite) TestRetryCleanTxn(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table retrytxn (a int unique, b int)")
	tk.MustExec("insert retrytxn values (1, 1)")
	tk.MustExec("begin")
	tk.MustExec("update retrytxn set b = b + 1 where a = 1")

	// Make retryable error.
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustExec("update retrytxn set b = b + 1 where a = 1")

	// Hijack retry history, add a statement that returns error.
	history := session.GetHistory(tk.Se)
	stmtNode, err := parser.New().ParseOneStmt("insert retrytxn values (2, 'a')", "", "")
	c.Assert(err, IsNil)
	stmt, _ := session.Compile(context.TODO(), tk.Se, stmtNode)
	executor.ResetContextOfStmt(tk.Se, stmtNode)
	history.Add(stmt, tk.Se.GetSessionVars().StmtCtx)
	_, err = tk.Exec("commit")
	c.Assert(err, NotNil)
	txn, err := tk.Se.Txn(false)
	c.Assert(err, IsNil)
	c.Assert(txn.Valid(), IsFalse)
	c.Assert(tk.Se.GetSessionVars().InTxn(), IsFalse)
}

func (s *testSessionSuite) TestReadOnlyNotInHistory(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table history (a int)")
	tk.MustExec("insert history values (1), (2), (3)")
	tk.MustExec("set @@autocommit = 0")
	tk.MustExec("set tidb_disable_txn_auto_retry = 0")
	tk.MustQuery("select * from history")
	history := session.GetHistory(tk.Se)
	c.Assert(history.Count(), Equals, 0)

	tk.MustExec("insert history values (4)")
	tk.MustExec("insert history values (5)")
	c.Assert(history.Count(), Equals, 2)
	tk.MustExec("commit")
	tk.MustQuery("select * from history")
	history = session.GetHistory(tk.Se)
	c.Assert(history.Count(), Equals, 0)
}

func (s *testSessionSuite) TestRetryUnion(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table history (a int)")
	tk.MustExec("insert history values (1), (2), (3)")
	tk.MustExec("set @@autocommit = 0")
	tk.MustExec("set tidb_disable_txn_auto_retry = 0")
	// UNION should't be in retry history.
	tk.MustQuery("(select * from history) union (select * from history)")
	history := session.GetHistory(tk.Se)
	c.Assert(history.Count(), Equals, 0)
	tk.MustQuery("(select * from history for update) union (select * from history)")
	tk.MustExec("update history set a = a + 1")
	history = session.GetHistory(tk.Se)
	c.Assert(history.Count(), Equals, 2)

	// Make retryable error.
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustExec("update history set a = a + 1")

	_, err := tk.Exec("commit")
	c.Assert(err, ErrorMatches, ".*can not retry select for update statement")
}

func (s *testSessionSuite) TestRetryShow(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("set @@autocommit = 0")
	tk.MustExec("set tidb_disable_txn_auto_retry = 0")
	// UNION should't be in retry history.
	tk.MustQuery("show variables")
	tk.MustQuery("show databases")
	history := session.GetHistory(tk.Se)
	c.Assert(history.Count(), Equals, 0)
}

func (s *testSessionSuite) TestNoRetryForCurrentTxn(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table history (a int)")
	tk.MustExec("insert history values (1)")

	// Firstly, disable retry.
	tk.MustExec("set tidb_disable_txn_auto_retry = 1")
	tk.MustExec("begin")
	tk.MustExec("update history set a = 2")
	// Enable retry now.
	tk.MustExec("set tidb_disable_txn_auto_retry = 0")

	tk1.MustExec("update history set a = 3")
	c.Assert(tk.ExecToErr("commit"), NotNil)
}

func (s *testSessionSuite) TestRetryForCurrentTxn(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table history (a int)")
	tk.MustExec("insert history values (1)")

	// Firstly, enable retry.
	tk.MustExec("set tidb_disable_txn_auto_retry = 0")
	tk.MustExec("begin")
	tk.MustExec("update history set a = 2")
	// Disable retry now.
	tk.MustExec("set tidb_disable_txn_auto_retry = 1")

	tk1.MustExec("update history set a = 3")
	tk.MustExec("commit")
	tk.MustQuery("select * from history").Check(testkit.Rows("2"))
}

// TestTruncateAlloc tests that the auto_increment ID does not reuse the old table's allocator.
func (s *testSessionSuite) TestTruncateAlloc(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table truncate_id (a int primary key auto_increment)")
	tk.MustExec("insert truncate_id values (), (), (), (), (), (), (), (), (), ()")
	tk.MustExec("truncate table truncate_id")
	tk.MustExec("insert truncate_id values (), (), (), (), (), (), (), (), (), ()")
	tk.MustQuery("select a from truncate_id where a > 11").Check(testkit.Rows())
}

func (s *testSessionSuite) TestString(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("select 1")
	// here to check the panic bug in String() when txn is nil after committed.
	c.Log(tk.Se.String())
}

func (s *testSessionSuite) TestDatabase(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	// Test database.
	tk.MustExec("create database xxx")
	tk.MustExec("drop database xxx")

	tk.MustExec("drop database if exists xxx")
	tk.MustExec("create database xxx")
	tk.MustExec("create database if not exists xxx")
	tk.MustExec("drop database if exists xxx")

	// Test schema.
	tk.MustExec("create schema xxx")
	tk.MustExec("drop schema xxx")

	tk.MustExec("drop schema if exists xxx")
	tk.MustExec("create schema xxx")
	tk.MustExec("create schema if not exists xxx")
	tk.MustExec("drop schema if exists xxx")
}

func (s *testSessionSuite) TestExecRestrictedSQL(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	r, _, err := tk.Se.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL("select 1;")
	c.Assert(err, IsNil)
	c.Assert(len(r), Equals, 1)
}

// TestInTrans . See https://dev.mysql.com/doc/internals/en/status-flags.html
func (s *testSessionSuite) TestInTrans(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	tk.MustExec("insert t values ()")
	tk.MustExec("begin")
	txn, err := tk.Se.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Valid(), IsTrue)
	tk.MustExec("insert t values ()")
	c.Assert(txn.Valid(), IsTrue)
	tk.MustExec("drop table if exists t;")
	c.Assert(txn.Valid(), IsFalse)
	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	c.Assert(txn.Valid(), IsFalse)
	tk.MustExec("insert t values ()")
	c.Assert(txn.Valid(), IsFalse)
	tk.MustExec("commit")
	tk.MustExec("insert t values ()")

	tk.MustExec("set autocommit=0")
	tk.MustExec("begin")
	c.Assert(txn.Valid(), IsTrue)
	tk.MustExec("insert t values ()")
	c.Assert(txn.Valid(), IsTrue)
	tk.MustExec("commit")
	c.Assert(txn.Valid(), IsFalse)
	tk.MustExec("insert t values ()")
	c.Assert(txn.Valid(), IsTrue)
	tk.MustExec("commit")
	c.Assert(txn.Valid(), IsFalse)

	tk.MustExec("set autocommit=1")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	tk.MustExec("begin")
	c.Assert(txn.Valid(), IsTrue)
	tk.MustExec("insert t values ()")
	c.Assert(txn.Valid(), IsTrue)
	tk.MustExec("rollback")
	c.Assert(txn.Valid(), IsFalse)
}

func (s *testSessionSuite) TestRetryPreparedStmt(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	txn, err := tk.Se.Txn(true)
	c.Assert(kv.ErrInvalidTxn.Equal(err), IsTrue)
	c.Assert(txn.Valid(), IsFalse)
	tk.MustExec("create table t (c1 int, c2 int, c3 int)")
	tk.MustExec("insert t values (11, 2, 3)")

	tk1.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk1.MustExec("begin")
	tk1.MustExec("update t set c2=? where c1=11;", 21)

	tk2.MustExec("begin")
	tk2.MustExec("update t set c2=? where c1=11", 22)
	tk2.MustExec("commit")

	tk1.MustExec("commit")

	tk.MustQuery("select c2 from t where c1=11").Check(testkit.Rows("21"))
}

func (s *testSessionSuite) TestSession(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("ROLLBACK;")
	tk.Se.Close()
}

func (s *testSessionSuite) TestSessionAuth(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "Any not exist username with zero password!", Hostname: "anyhost"}, []byte(""), []byte("")), IsFalse)
}

func (s *testSessionSerialSuite) TestSkipWithGrant(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	save2 := privileges.SkipWithGrant

	privileges.SkipWithGrant = false
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "user_not_exist"}, []byte("yyy"), []byte("zzz")), IsFalse)

	privileges.SkipWithGrant = true
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "xxx", Hostname: `%`}, []byte("yyy"), []byte("zzz")), IsTrue)
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: `%`}, []byte(""), []byte("")), IsTrue)
	tk.MustExec("create table t (id int)")
	tk.MustExec("create role r_1")
	tk.MustExec("grant r_1 to root")
	tk.MustExec("set role all")
	tk.MustExec("show grants for root")
	privileges.SkipWithGrant = save2
}

func (s *testSessionSuite) TestLastInsertID(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	// insert
	tk.MustExec("create table t (c1 int not null auto_increment, c2 int, PRIMARY KEY (c1))")
	tk.MustExec("insert into t set c2 = 11")
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows("1"))

	tk.MustExec("insert into t (c2) values (22), (33), (44)")
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows("2"))

	tk.MustExec("insert into t (c1, c2) values (10, 55)")
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows("2"))

	// replace
	tk.MustExec("replace t (c2) values(66)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 11", "2 22", "3 33", "4 44", "10 55", "11 66"))
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows("11"))

	// update
	tk.MustExec("update t set c1=last_insert_id(c1 + 100)")
	tk.MustQuery("select * from t").Check(testkit.Rows("101 11", "102 22", "103 33", "104 44", "110 55", "111 66"))
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows("111"))
	tk.MustExec("insert into t (c2) values (77)")
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows("112"))

	// drop
	tk.MustExec("drop table t")
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows("112"))

	tk.MustExec("create table t (c2 int, c3 int, c1 int not null auto_increment, PRIMARY KEY (c1))")
	tk.MustExec("insert into t set c2 = 30")

	// insert values
	lastInsertID := tk.Se.LastInsertID()
	tk.MustExec("prepare stmt1 from 'insert into t (c2) values (?)'")
	tk.MustExec("set @v1=10")
	tk.MustExec("set @v2=20")
	tk.MustExec("execute stmt1 using @v1")
	tk.MustExec("execute stmt1 using @v2")
	tk.MustExec("deallocate prepare stmt1")
	currLastInsertID := tk.Se.GetSessionVars().StmtCtx.PrevLastInsertID
	tk.MustQuery("select c1 from t where c2 = 20").Check(testkit.Rows(fmt.Sprint(currLastInsertID)))
	c.Assert(lastInsertID+2, Equals, currLastInsertID)
}

func (s *testSessionSuite) TestPrepareZero(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(v timestamp)")
	tk.MustExec("prepare s1 from 'insert into t (v) values (?)'")
	tk.MustExec("set @v1='0'")
	_, rs := tk.Exec("execute s1 using @v1")
	c.Assert(rs, NotNil)
	tk.MustExec("set @v2='" + types.ZeroDatetimeStr + "'")
	tk.MustExec("execute s1 using @v2")
	tk.MustQuery("select v from t").Check(testkit.Rows("0000-00-00 00:00:00"))
}

func (s *testSessionSuite) TestPrimaryKeyAutoIncrement(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL, name varchar(255) UNIQUE NOT NULL, status int)")
	tk.MustExec("insert t (name) values (?)", "abc")
	id := tk.Se.LastInsertID()
	c.Check(id != 0, IsTrue)

	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustQuery("select * from t").Check(testkit.Rows(fmt.Sprintf("%d abc <nil>", id)))

	tk.MustExec("update t set name = 'abc', status = 1 where id = ?", id)
	tk1.MustQuery("select * from t").Check(testkit.Rows(fmt.Sprintf("%d abc 1", id)))

	// Check for pass bool param to tidb prepared statement
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id tinyint)")
	tk.MustExec("insert t values (?)", true)
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
}

func (s *testSessionSuite) TestAutoIncrementID(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	tk.MustExec("insert t values ()")
	tk.MustExec("insert t values ()")
	tk.MustExec("insert t values ()")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	tk.MustExec("insert t values ()")
	lastID := tk.Se.LastInsertID()
	c.Assert(lastID, Less, uint64(4))
	tk.MustExec("insert t () values ()")
	c.Assert(tk.Se.LastInsertID(), Greater, lastID)
	lastID = tk.Se.LastInsertID()
	tk.MustExec("insert t values (100)")
	c.Assert(tk.Se.LastInsertID(), Equals, uint64(100))

	// If the auto_increment column value is given, it uses the value of the latest row.
	tk.MustExec("insert t values (120), (112)")
	c.Assert(tk.Se.LastInsertID(), Equals, uint64(112))

	// The last_insert_id function only use last auto-generated id.
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows(fmt.Sprint(lastID)))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (i tinyint unsigned not null auto_increment, primary key (i));")
	tk.MustExec("insert into t set i = 254;")
	tk.MustExec("insert t values ()")

	// The last insert ID doesn't care about primary key, it is set even if its a normal index column.
	tk.MustExec("create table autoid (id int auto_increment, index (id))")
	tk.MustExec("insert autoid values ()")
	c.Assert(tk.Se.LastInsertID(), Greater, uint64(0))
	tk.MustExec("insert autoid values (100)")
	c.Assert(tk.Se.LastInsertID(), Equals, uint64(100))

	tk.MustQuery("select last_insert_id(20)").Check(testkit.Rows(fmt.Sprint(20)))
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows(fmt.Sprint(20)))

	// Corner cases for unsigned bigint auto_increment Columns.
	tk.MustExec("drop table if exists autoid")
	tk.MustExec("create table autoid(`auto_inc_id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,UNIQUE KEY `auto_inc_id` (`auto_inc_id`))")
	tk.MustExec("insert into autoid values(9223372036854775808);")
	tk.MustExec("insert into autoid values();")
	tk.MustExec("insert into autoid values();")
	tk.MustQuery("select * from autoid").Check(testkit.Rows("9223372036854775808", "9223372036854775810", "9223372036854775812"))
	// In TiDB : _tidb_rowid will also consume the autoID when the auto_increment column is not the primary key.
	// Using the MaxUint64 and MaxInt64 as the autoID upper limit like MySQL will cause _tidb_rowid allocation fail here.
	_, err := tk.Exec("insert into autoid values(18446744073709551614)")
	c.Assert(terror.ErrorEqual(err, autoid.ErrAutoincReadFailed), IsTrue)
	_, err = tk.Exec("insert into autoid values()")
	c.Assert(terror.ErrorEqual(err, autoid.ErrAutoincReadFailed), IsTrue)
	// FixMe: MySQL works fine with the this sql.
	_, err = tk.Exec("insert into autoid values(18446744073709551615)")
	c.Assert(terror.ErrorEqual(err, autoid.ErrAutoincReadFailed), IsTrue)

	tk.MustExec("drop table if exists autoid")
	tk.MustExec("create table autoid(`auto_inc_id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,UNIQUE KEY `auto_inc_id` (`auto_inc_id`))")
	tk.MustExec("insert into autoid values()")
	tk.MustQuery("select * from autoid").Check(testkit.Rows("1"))
	tk.MustExec("insert into autoid values(5000)")
	tk.MustQuery("select * from autoid").Check(testkit.Rows("1", "5000"))
	_, err = tk.Exec("update autoid set auto_inc_id = 8000")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	tk.MustQuery("select * from autoid use index()").Check(testkit.Rows("1", "5000"))
	tk.MustExec("update autoid set auto_inc_id = 9000 where auto_inc_id=1")
	tk.MustQuery("select * from autoid use index()").Check(testkit.Rows("9000", "5000"))
	tk.MustExec("insert into autoid values()")
	tk.MustQuery("select * from autoid use index()").Check(testkit.Rows("9000", "5000", "9001"))

	// Corner cases for signed bigint auto_increment Columns.
	tk.MustExec("drop table if exists autoid")
	tk.MustExec("create table autoid(`auto_inc_id` bigint(20) NOT NULL AUTO_INCREMENT,UNIQUE KEY `auto_inc_id` (`auto_inc_id`))")
	// In TiDB : _tidb_rowid will also consume the autoID when the auto_increment column is not the primary key.
	// Using the MaxUint64 and MaxInt64 as autoID upper limit like MySQL will cause insert fail if the values is
	// 9223372036854775806. Because _tidb_rowid will be allocated 9223372036854775807 at same time.
	tk.MustExec("insert into autoid values(9223372036854775805);")
	tk.MustQuery("select auto_inc_id, _tidb_rowid from autoid use index()").Check(testkit.Rows("9223372036854775805 9223372036854775806"))
	_, err = tk.Exec("insert into autoid values();")
	c.Assert(terror.ErrorEqual(err, autoid.ErrAutoincReadFailed), IsTrue)
	tk.MustQuery("select auto_inc_id, _tidb_rowid from autoid use index()").Check(testkit.Rows("9223372036854775805 9223372036854775806"))
	tk.MustQuery("select auto_inc_id, _tidb_rowid from autoid use index(auto_inc_id)").Check(testkit.Rows("9223372036854775805 9223372036854775806"))

	tk.MustExec("drop table if exists autoid")
	tk.MustExec("create table autoid(`auto_inc_id` bigint(20) NOT NULL AUTO_INCREMENT,UNIQUE KEY `auto_inc_id` (`auto_inc_id`))")
	tk.MustExec("insert into autoid values()")
	tk.MustQuery("select * from autoid use index()").Check(testkit.Rows("1"))
	tk.MustExec("insert into autoid values(5000)")
	tk.MustQuery("select * from autoid use index()").Check(testkit.Rows("1", "5000"))
	_, err = tk.Exec("update autoid set auto_inc_id = 8000")
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	tk.MustQuery("select * from autoid use index()").Check(testkit.Rows("1", "5000"))
	tk.MustExec("update autoid set auto_inc_id = 9000 where auto_inc_id=1")
	tk.MustQuery("select * from autoid use index()").Check(testkit.Rows("9000", "5000"))
	tk.MustExec("insert into autoid values()")
	tk.MustQuery("select * from autoid use index()").Check(testkit.Rows("9000", "5000", "9001"))
}

func (s *testSessionSuite) TestAutoIncrementWithRetry(c *C) {
	// test for https://github.com/pingcap/tidb/issues/827

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk1 := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk.MustExec("create table t (c2 int, c1 int not null auto_increment, PRIMARY KEY (c1))")
	tk.MustExec("insert into t (c2) values (1), (2), (3), (4), (5)")

	// insert values
	lastInsertID := tk.Se.LastInsertID()
	tk.MustExec("begin")
	tk.MustExec("insert into t (c2) values (11), (12), (13)")
	tk.MustQuery("select c1 from t where c2 = 11").Check(testkit.Rows("6"))
	tk.MustExec("update t set c2 = 33 where c2 = 1")

	tk1.MustExec("update t set c2 = 22 where c2 = 1")

	tk.MustExec("commit")

	tk.MustQuery("select c1 from t where c2 = 11").Check(testkit.Rows("6"))
	currLastInsertID := tk.Se.GetSessionVars().StmtCtx.PrevLastInsertID
	c.Assert(lastInsertID+5, Equals, currLastInsertID)

	// insert set
	lastInsertID = currLastInsertID
	tk.MustExec("begin")
	tk.MustExec("insert into t set c2 = 31")
	tk.MustQuery("select c1 from t where c2 = 31").Check(testkit.Rows("9"))
	tk.MustExec("update t set c2 = 44 where c2 = 2")

	tk1.MustExec("update t set c2 = 55 where c2 = 2")

	tk.MustExec("commit")

	tk.MustQuery("select c1 from t where c2 = 31").Check(testkit.Rows("9"))
	currLastInsertID = tk.Se.GetSessionVars().StmtCtx.PrevLastInsertID
	c.Assert(lastInsertID+3, Equals, currLastInsertID)

	// replace
	lastInsertID = currLastInsertID
	tk.MustExec("begin")
	tk.MustExec("insert into t (c2) values (21), (22), (23)")
	tk.MustQuery("select c1 from t where c2 = 21").Check(testkit.Rows("10"))
	tk.MustExec("update t set c2 = 66 where c2 = 3")

	tk1.MustExec("update t set c2 = 77 where c2 = 3")

	tk.MustExec("commit")

	tk.MustQuery("select c1 from t where c2 = 21").Check(testkit.Rows("10"))
	currLastInsertID = tk.Se.GetSessionVars().StmtCtx.PrevLastInsertID
	c.Assert(lastInsertID+1, Equals, currLastInsertID)

	// update
	lastInsertID = currLastInsertID
	tk.MustExec("begin")
	tk.MustExec("insert into t set c2 = 41")
	tk.MustExec("update t set c1 = 0 where c2 = 41")
	tk.MustQuery("select c1 from t where c2 = 41").Check(testkit.Rows("0"))
	tk.MustExec("update t set c2 = 88 where c2 = 4")

	tk1.MustExec("update t set c2 = 99 where c2 = 4")

	tk.MustExec("commit")

	tk.MustQuery("select c1 from t where c2 = 41").Check(testkit.Rows("0"))
	currLastInsertID = tk.Se.GetSessionVars().StmtCtx.PrevLastInsertID
	c.Assert(lastInsertID+3, Equals, currLastInsertID)

	// prepare
	lastInsertID = currLastInsertID
	tk.MustExec("begin")
	tk.MustExec("prepare stmt from 'insert into t (c2) values (?)'")
	tk.MustExec("set @v1=100")
	tk.MustExec("set @v2=200")
	tk.MustExec("set @v3=300")
	tk.MustExec("execute stmt using @v1")
	tk.MustExec("execute stmt using @v2")
	tk.MustExec("execute stmt using @v3")
	tk.MustExec("deallocate prepare stmt")
	tk.MustQuery("select c1 from t where c2 = 12").Check(testkit.Rows("7"))
	tk.MustExec("update t set c2 = 111 where c2 = 5")

	tk1.MustExec("update t set c2 = 222 where c2 = 5")

	tk.MustExec("commit")

	tk.MustQuery("select c1 from t where c2 = 12").Check(testkit.Rows("7"))
	currLastInsertID = tk.Se.GetSessionVars().StmtCtx.PrevLastInsertID
	c.Assert(lastInsertID+3, Equals, currLastInsertID)
}

func (s *testSessionSuite) TestBinaryReadOnly(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (i int key)")
	id, _, _, err := tk.Se.PrepareStmt("select i from t where i = ?")
	c.Assert(err, IsNil)
	id2, _, _, err := tk.Se.PrepareStmt("insert into t values (?)")
	c.Assert(err, IsNil)
	tk.MustExec("set autocommit = 0")
	tk.MustExec("set tidb_disable_txn_auto_retry = 0")
	_, err = tk.Se.ExecutePreparedStmt(context.Background(), id, []types.Datum{types.NewDatum(1)})
	c.Assert(err, IsNil)
	c.Assert(session.GetHistory(tk.Se).Count(), Equals, 0)
	tk.MustExec("insert into t values (1)")
	c.Assert(session.GetHistory(tk.Se).Count(), Equals, 1)
	_, err = tk.Se.ExecutePreparedStmt(context.Background(), id2, []types.Datum{types.NewDatum(2)})
	c.Assert(err, IsNil)
	c.Assert(session.GetHistory(tk.Se).Count(), Equals, 2)
	tk.MustExec("commit")
}

func (s *testSessionSuite) TestPrepare(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t(id TEXT)")
	tk.MustExec(`INSERT INTO t VALUES ("id");`)
	id, ps, _, err := tk.Se.PrepareStmt("select id+? from t")
	ctx := context.Background()
	c.Assert(err, IsNil)
	c.Assert(id, Equals, uint32(1))
	c.Assert(ps, Equals, 1)
	tk.MustExec(`set @a=1`)
	_, err = tk.Se.ExecutePreparedStmt(ctx, id, []types.Datum{types.NewDatum("1")})
	c.Assert(err, IsNil)
	err = tk.Se.DropPreparedStmt(id)
	c.Assert(err, IsNil)

	tk.MustExec("prepare stmt from 'select 1+?'")
	tk.MustExec("set @v1=100")
	tk.MustQuery("execute stmt using @v1").Check(testkit.Rows("101"))

	tk.MustExec("set @v2=200")
	tk.MustQuery("execute stmt using @v2").Check(testkit.Rows("201"))

	tk.MustExec("set @v3=300")
	tk.MustQuery("execute stmt using @v3").Check(testkit.Rows("301"))
	tk.MustExec("deallocate prepare stmt")

	// Execute prepared statements for more than one time.
	tk.MustExec("create table multiexec (a int, b int)")
	tk.MustExec("insert multiexec values (1, 1), (2, 2)")
	id, _, _, err = tk.Se.PrepareStmt("select a from multiexec where b = ? order by b")
	c.Assert(err, IsNil)
	rs, err := tk.Se.ExecutePreparedStmt(ctx, id, []types.Datum{types.NewDatum(1)})
	c.Assert(err, IsNil)
	rs.Close()
	rs, err = tk.Se.ExecutePreparedStmt(ctx, id, []types.Datum{types.NewDatum(2)})
	rs.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite2) TestSpecifyIndexPrefixLength(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	_, err := tk.Exec("create table t (c1 char, index(c1(3)));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	c.Assert(err, NotNil)

	_, err = tk.Exec("create table t (c1 int, index(c1(3)));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	c.Assert(err, NotNil)

	_, err = tk.Exec("create table t (c1 bit(10), index(c1(3)));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	c.Assert(err, NotNil)

	tk.MustExec("create table t (c1 char, c2 int, c3 bit(10));")

	_, err = tk.Exec("create index idx_c1 on t (c1(3));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	c.Assert(err, NotNil)

	_, err = tk.Exec("create index idx_c1 on t (c2(3));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	c.Assert(err, NotNil)

	_, err = tk.Exec("create index idx_c1 on t (c3(3));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	c.Assert(err, NotNil)

	tk.MustExec("drop table if exists t;")

	_, err = tk.Exec("create table t (c1 int, c2 blob, c3 varchar(64), index(c2));")
	// ERROR 1170 (42000): BLOB/TEXT column 'c2' used in key specification without a key length
	c.Assert(err, NotNil)

	tk.MustExec("create table t (c1 int, c2 blob, c3 varchar(64));")
	_, err = tk.Exec("create index idx_c1 on t (c2);")
	// ERROR 1170 (42000): BLOB/TEXT column 'c2' used in key specification without a key length
	c.Assert(err, NotNil)

	_, err = tk.Exec("create index idx_c1 on t (c2(555555));")
	// ERROR 1071 (42000): Specified key was too long; max key length is 3072 bytes
	c.Assert(err, NotNil)

	_, err = tk.Exec("create index idx_c1 on t (c1(5))")
	// ERROR 1089 (HY000): Incorrect prefix key;
	// the used key part isn't a string, the used length is longer than the key part,
	// or the storage engine doesn't support unique prefix keys
	c.Assert(err, NotNil)

	tk.MustExec("create index idx_c1 on t (c1);")
	tk.MustExec("create index idx_c2 on t (c2(3));")
	tk.MustExec("create unique index idx_c3 on t (c3(5));")

	tk.MustExec("insert into t values (3, 'abc', 'def');")
	tk.MustQuery("select c2 from t where c2 = 'abc';").Check(testkit.Rows("abc"))

	tk.MustExec("insert into t values (4, 'abcd', 'xxx');")
	tk.MustExec("insert into t values (4, 'abcf', 'yyy');")
	tk.MustQuery("select c2 from t where c2 = 'abcf';").Check(testkit.Rows("abcf"))
	tk.MustQuery("select c2 from t where c2 = 'abcd';").Check(testkit.Rows("abcd"))

	tk.MustExec("insert into t values (4, 'ignore', 'abcdeXXX');")
	_, err = tk.Exec("insert into t values (5, 'ignore', 'abcdeYYY');")
	// ERROR 1062 (23000): Duplicate entry 'abcde' for key 'idx_c3'
	c.Assert(err, NotNil)
	tk.MustQuery("select c3 from t where c3 = 'abcde';").Check(testkit.Rows())

	tk.MustExec("delete from t where c3 = 'abcdeXXX';")
	tk.MustExec("delete from t where c2 = 'abc';")

	tk.MustQuery("select c2 from t where c2 > 'abcd';").Check(testkit.Rows("abcf"))
	tk.MustQuery("select c2 from t where c2 < 'abcf';").Check(testkit.Rows("abcd"))
	tk.MustQuery("select c2 from t where c2 >= 'abcd';").Check(testkit.Rows("abcd", "abcf"))
	tk.MustQuery("select c2 from t where c2 <= 'abcf';").Check(testkit.Rows("abcd", "abcf"))
	tk.MustQuery("select c2 from t where c2 != 'abc';").Check(testkit.Rows("abcd", "abcf"))
	tk.MustQuery("select c2 from t where c2 != 'abcd';").Check(testkit.Rows("abcf"))

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int, b char(255), key(a, b(20)));")
	tk.MustExec("insert into t1 values (0, '1');")
	tk.MustExec("update t1 set b = b + 1 where a = 0;")
	tk.MustQuery("select b from t1 where a = 0;").Check(testkit.Rows("2"))

	// test union index.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a text, b text, c int, index (a(3), b(3), c));")
	tk.MustExec("insert into t values ('abc', 'abcd', 1);")
	tk.MustExec("insert into t values ('abcx', 'abcf', 2);")
	tk.MustExec("insert into t values ('abcy', 'abcf', 3);")
	tk.MustExec("insert into t values ('bbc', 'abcd', 4);")
	tk.MustExec("insert into t values ('bbcz', 'abcd', 5);")
	tk.MustExec("insert into t values ('cbck', 'abd', 6);")
	tk.MustQuery("select c from t where a = 'abc' and b <= 'abc';").Check(testkit.Rows())
	tk.MustQuery("select c from t where a = 'abc' and b <= 'abd';").Check(testkit.Rows("1"))
	tk.MustQuery("select c from t where a < 'cbc' and b > 'abcd';").Check(testkit.Rows("2", "3"))
	tk.MustQuery("select c from t where a <= 'abd' and b > 'abc';").Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("select c from t where a < 'bbcc' and b = 'abcd';").Check(testkit.Rows("1", "4"))
	tk.MustQuery("select c from t where a > 'bbcf';").Check(testkit.Rows("5", "6"))
}

func (s *testSessionSuite) TestResultField(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (id int);")

	tk.MustExec(`INSERT INTO t VALUES (1);`)
	tk.MustExec(`INSERT INTO t VALUES (2);`)
	r, err := tk.Exec(`SELECT count(*) from t;`)
	c.Assert(err, IsNil)
	fields := r.Fields()
	c.Assert(err, IsNil)
	c.Assert(len(fields), Equals, 1)
	field := fields[0].Column
	c.Assert(field.Tp, Equals, mysql.TypeLonglong)
	c.Assert(field.Flen, Equals, 21)
}

func (s *testSessionSuite) TestResultType(c *C) {
	// Testcase for https://github.com/pingcap/tidb/issues/325
	tk := testkit.NewTestKitWithInit(c, s.store)
	rs, err := tk.Exec(`select cast(null as char(30))`)
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	err = rs.Next(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(req.GetRow(0).IsNull(0), IsTrue)
	c.Assert(rs.Fields()[0].Column.FieldType.Tp, Equals, mysql.TypeVarString)
}

func (s *testSessionSuite) TestFieldText(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (a int)")
	tests := []struct {
		sql   string
		field string
	}{
		{"select distinct(a) from t", "a"},
		{"select (1)", "1"},
		{"select (1+1)", "(1+1)"},
		{"select a from t", "a"},
		{"select        ((a+1))     from t", "((a+1))"},
		{"select 1 /*!32301 +1 */;", "1  +1 "},
		{"select /*!32301 1  +1 */;", "1  +1 "},
		{"/*!32301 select 1  +1 */;", "1  +1 "},
		{"select 1 + /*!32301 1 +1 */;", "1 +  1 +1 "},
		{"select 1 /*!32301 + 1, 1 */;", "1  + 1"},
		{"select /*!32301 1, 1 +1 */;", "1"},
		{"select /*!32301 1 + 1, */ +1;", "1 + 1"},
	}
	for _, tt := range tests {
		result, err := tk.Exec(tt.sql)
		c.Assert(err, IsNil)
		c.Assert(result.Fields()[0].ColumnAsName.O, Equals, tt.field)
	}
}

func (s *testSessionSuite3) TestIndexMaxLength(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create database test_index_max_length")
	tk.MustExec("use test_index_max_length")

	// create simple index at table creation
	tk.MustGetErrCode("create table t (c1 varchar(3073), index(c1)) charset = ascii;", mysql.ErrTooLongKey)

	// create simple index after table creation
	tk.MustExec("create table t (c1 varchar(3073)) charset = ascii;")
	tk.MustGetErrCode("create index idx_c1 on t(c1) ", mysql.ErrTooLongKey)
	tk.MustExec("drop table t;")

	// create compound index at table creation
	tk.MustGetErrCode("create table t (c1 varchar(3072), c2 varchar(1), index(c1, c2)) charset = ascii;", mysql.ErrTooLongKey)
	tk.MustGetErrCode("create table t (c1 varchar(3072), c2 char(1), index(c1, c2)) charset = ascii;", mysql.ErrTooLongKey)
	tk.MustGetErrCode("create table t (c1 varchar(3072), c2 char, index(c1, c2)) charset = ascii;", mysql.ErrTooLongKey)
	tk.MustGetErrCode("create table t (c1 varchar(3072), c2 date, index(c1, c2)) charset = ascii;", mysql.ErrTooLongKey)
	tk.MustGetErrCode("create table t (c1 varchar(3069), c2 timestamp(1), index(c1, c2)) charset = ascii;", mysql.ErrTooLongKey)

	tk.MustExec("create table t (c1 varchar(3068), c2 bit(26), index(c1, c2)) charset = ascii;") // 26 bit = 4 bytes
	tk.MustExec("drop table t;")
	tk.MustExec("create table t (c1 varchar(3068), c2 bit(32), index(c1, c2)) charset = ascii;") // 32 bit = 4 bytes
	tk.MustExec("drop table t;")
	tk.MustGetErrCode("create table t (c1 varchar(3068), c2 bit(33), index(c1, c2)) charset = ascii;", mysql.ErrTooLongKey)

	// create compound index after table creation
	tk.MustExec("create table t (c1 varchar(3072), c2 varchar(1)) charset = ascii;")
	tk.MustGetErrCode("create index idx_c1_c2 on t(c1, c2);", mysql.ErrTooLongKey)
	tk.MustExec("drop table t;")

	tk.MustExec("create table t (c1 varchar(3072), c2 char(1)) charset = ascii;")
	tk.MustGetErrCode("create index idx_c1_c2 on t(c1, c2);", mysql.ErrTooLongKey)
	tk.MustExec("drop table t;")

	tk.MustExec("create table t (c1 varchar(3072), c2 char) charset = ascii;")
	tk.MustGetErrCode("create index idx_c1_c2 on t(c1, c2);", mysql.ErrTooLongKey)
	tk.MustExec("drop table t;")

	tk.MustExec("create table t (c1 varchar(3072), c2 date) charset = ascii;")
	tk.MustGetErrCode("create index idx_c1_c2 on t(c1, c2);", mysql.ErrTooLongKey)
	tk.MustExec("drop table t;")

	tk.MustExec("create table t (c1 varchar(3069), c2 timestamp(1)) charset = ascii;")
	tk.MustGetErrCode("create index idx_c1_c2 on t(c1, c2);", mysql.ErrTooLongKey)
	tk.MustExec("drop table t;")

	// Test charsets other than `ascii`.
	assertCharsetLimit := func(charset string, bytesPerChar int) {
		base := 3072 / bytesPerChar
		tk.MustGetErrCode(fmt.Sprintf("create table t (a varchar(%d) primary key) charset=%s", base+1, charset), mysql.ErrTooLongKey)
		tk.MustExec(fmt.Sprintf("create table t (a varchar(%d) primary key) charset=%s", base, charset))
		tk.MustExec("drop table if exists t")
	}
	assertCharsetLimit("binary", 1)
	assertCharsetLimit("latin1", 1)
	assertCharsetLimit("utf8", 3)
	assertCharsetLimit("utf8mb4", 4)

	// Test types bit length limit.
	assertTypeLimit := func(tp string, limitBitLength int) {
		base := 3072 - limitBitLength
		tk.MustGetErrCode(fmt.Sprintf("create table t (a blob(10000), b %s, index idx(a(%d), b))", tp, base+1), mysql.ErrTooLongKey)
		tk.MustExec(fmt.Sprintf("create table t (a blob(10000), b %s, index idx(a(%d), b))", tp, base))
		tk.MustExec("drop table if exists t")
	}

	assertTypeLimit("tinyint", 1)
	assertTypeLimit("smallint", 2)
	assertTypeLimit("mediumint", 3)
	assertTypeLimit("int", 4)
	assertTypeLimit("integer", 4)
	assertTypeLimit("bigint", 8)
	assertTypeLimit("float", 4)
	assertTypeLimit("float(24)", 4)
	assertTypeLimit("float(25)", 8)
	assertTypeLimit("decimal(9)", 4)
	assertTypeLimit("decimal(10)", 5)
	assertTypeLimit("decimal(17)", 8)
	assertTypeLimit("year", 1)
	assertTypeLimit("date", 3)
	assertTypeLimit("time", 3)
	assertTypeLimit("datetime", 8)
	assertTypeLimit("timestamp", 4)
}

func (s *testSessionSuite2) TestIndexColumnLength(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (c1 int, c2 blob);")
	tk.MustExec("create index idx_c1 on t(c1);")
	tk.MustExec("create index idx_c2 on t(c2(6));")

	is := s.dom.InfoSchema()
	tab, err2 := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err2, Equals, nil)

	idxC1Cols := tables.FindIndexByColName(tab, "c1").Meta().Columns
	c.Assert(idxC1Cols[0].Length, Equals, types.UnspecifiedLength)

	idxC2Cols := tables.FindIndexByColName(tab, "c2").Meta().Columns
	c.Assert(idxC2Cols[0].Length, Equals, 6)
}

func (s *testSessionSuite2) TestIgnoreForeignKey(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	sqlText := `CREATE TABLE address (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		user_id bigint(20) NOT NULL,
		PRIMARY KEY (id),
		CONSTRAINT FK_7rod8a71yep5vxasb0ms3osbg FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id),
		INDEX FK_7rod8a71yep5vxasb0ms3osbg (user_id) comment ''
		) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;`
	tk.MustExec(sqlText)
}

// TestISColumns tests information_schema.columns.
func (s *testSessionSuite3) TestISColumns(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("select ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS;")
	tk.MustQuery("SELECT CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.CHARACTER_SETS WHERE CHARACTER_SET_NAME = 'utf8mb4'").Check(testkit.Rows("utf8mb4"))
}

func (s *testSessionSuite2) TestRetry(c *C) {
	// For https://github.com/pingcap/tidb/issues/571
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("begin")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int)")
	tk.MustExec("insert t values (1), (2), (3)")
	tk.MustExec("commit")

	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk3 := testkit.NewTestKitWithInit(c, s.store)
	tk3.MustExec("SET SESSION autocommit=0;")
	tk1.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk2.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk3.MustExec("set @@tidb_disable_txn_auto_retry = 0")

	var wg sync.WaitGroup
	wg.Add(3)
	f1 := func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			tk1.MustExec("update t set c = 1;")
		}
	}
	f2 := func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			tk2.MustExec("update t set c = ?;", 1)
		}
	}
	f3 := func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			tk3.MustExec("begin")
			tk3.MustExec("update t set c = 1;")
			tk3.MustExec("commit")
		}
	}
	go f1()
	go f2()
	go f3()
	wg.Wait()
}

func (s *testSessionSuite3) TestMultiStmts(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t1; create table t1(id int ); insert into t1 values (1);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1"))
}

func (s *testSessionSuite2) TestLastExecuteDDLFlag(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(id int)")
	c.Assert(tk.Se.Value(sessionctx.LastExecuteDDL), NotNil)
	tk.MustExec("insert into t1 values (1)")
	c.Assert(tk.Se.Value(sessionctx.LastExecuteDDL), IsNil)
}

func (s *testSessionSuite3) TestDecimal(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a decimal unique);")
	tk.MustExec("insert t values ('100');")
	_, err := tk.Exec("insert t values ('1e2');")
	c.Check(err, NotNil)
}

func (s *testSessionSuite2) TestParser(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	// test for https://github.com/pingcap/tidb/pull/177
	tk.MustExec("CREATE TABLE `t1` ( `a` char(3) NOT NULL default '', `b` char(3) NOT NULL default '', `c` char(3) NOT NULL default '', PRIMARY KEY  (`a`,`b`,`c`)) ENGINE=InnoDB;")
	tk.MustExec("CREATE TABLE `t2` ( `a` char(3) NOT NULL default '', `b` char(3) NOT NULL default '', `c` char(3) NOT NULL default '', PRIMARY KEY  (`a`,`b`,`c`)) ENGINE=InnoDB;")
	tk.MustExec(`INSERT INTO t1 VALUES (1,1,1);`)
	tk.MustExec(`INSERT INTO t2 VALUES (1,1,1);`)
	tk.MustExec(`PREPARE my_stmt FROM "SELECT t1.b, count(*) FROM t1 group by t1.b having count(*) > ALL (SELECT COUNT(*) FROM t2 WHERE t2.a=1 GROUP By t2.b)";`)
	tk.MustExec(`EXECUTE my_stmt;`)
	tk.MustExec(`EXECUTE my_stmt;`)
	tk.MustExec(`deallocate prepare my_stmt;`)
	tk.MustExec(`drop table t1,t2;`)
}

func (s *testSessionSuite3) TestOnDuplicate(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	// test for https://github.com/pingcap/tidb/pull/454
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int, c2 int, c3 int);")
	tk.MustExec("insert into t1 set c1=1, c2=2, c3=1;")
	tk.MustExec("create table t (c1 int, c2 int, c3 int, primary key (c1));")
	tk.MustExec("insert into t set c1=1, c2=4;")
	tk.MustExec("insert into t select * from t1 limit 1 on duplicate key update c3=3333;")
}

func (s *testSessionSuite2) TestReplace(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	// test for https://github.com/pingcap/tidb/pull/456
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int, c2 int, c3 int);")
	tk.MustExec("replace into t1 set c1=1, c2=2, c3=1;")
	tk.MustExec("create table t (c1 int, c2 int, c3 int, primary key (c1));")
	tk.MustExec("replace into t set c1=1, c2=4;")
	tk.MustExec("replace into t select * from t1 limit 1;")
}

func (s *testSessionSuite3) TestDelete(c *C) {
	// test for https://github.com/pingcap/tidb/pull/1135

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("create database test1")
	tk1.MustExec("use test1")
	tk1.MustExec("create table t (F1 VARCHAR(30));")
	tk1.MustExec("insert into t (F1) values ('1'), ('4');")

	tk.MustExec("create table t (F1 VARCHAR(30));")
	tk.MustExec("insert into t (F1) values ('1'), ('2');")
	tk.MustExec("delete m1 from t m2,t m1 where m1.F1>1;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (F1 VARCHAR(30));")
	tk.MustExec("insert into t (F1) values ('1'), ('2');")
	tk.MustExec("delete m1 from t m1,t m2 where true and m1.F1<2;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("2"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (F1 VARCHAR(30));")
	tk.MustExec("insert into t (F1) values ('1'), ('2');")
	tk.MustExec("delete m1 from t m1,t m2 where false;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1", "2"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (F1 VARCHAR(30));")
	tk.MustExec("insert into t (F1) values ('1'), ('2');")
	tk.MustExec("delete m1, m2 from t m1,t m2 where m1.F1>m2.F1;")
	tk.MustQuery("select * from t;").Check(testkit.Rows())

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (F1 VARCHAR(30));")
	tk.MustExec("insert into t (F1) values ('1'), ('2');")
	tk.MustExec("delete test1.t from test1.t inner join test.t where test1.t.F1 > test.t.F1")
	tk1.MustQuery("select * from t;").Check(testkit.Rows("1"))
}

func (s *testSessionSuite2) TestResetCtx(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk1 := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("create table t (i int auto_increment not null key);")
	tk.MustExec("insert into t values (1);")
	tk.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk.MustExec("begin;")
	tk.MustExec("insert into t values (10);")
	tk.MustExec("update t set i = i + row_count();")
	tk.MustQuery("select * from t;").Check(testkit.Rows("2", "11"))

	tk1.MustExec("update t set i = 0 where i = 1;")
	tk1.MustQuery("select * from t;").Check(testkit.Rows("0"))

	tk.MustExec("commit;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1", "11"))

	tk.MustExec("delete from t where i = 11;")
	tk.MustExec("begin;")
	tk.MustExec("insert into t values ();")
	tk.MustExec("update t set i = i + last_insert_id() + 1;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("14", "25"))

	tk1.MustExec("update t set i = 0 where i = 1;")
	tk1.MustQuery("select * from t;").Check(testkit.Rows("0"))

	tk.MustExec("commit;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("13", "25"))
}

func (s *testSessionSuite3) TestUnique(c *C) {
	// test for https://github.com/pingcap/tidb/pull/461

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk1.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk.MustExec(`CREATE TABLE test ( id int(11) UNSIGNED NOT NULL AUTO_INCREMENT, val int UNIQUE, PRIMARY KEY (id)); `)
	tk.MustExec("begin;")
	tk.MustExec("insert into test(id, val) values(1, 1);")
	tk1.MustExec("begin;")
	tk1.MustExec("insert into test(id, val) values(2, 2);")
	tk2.MustExec("begin;")
	tk2.MustExec("insert into test(id, val) values(1, 2);")
	tk2.MustExec("commit;")
	_, err := tk.Exec("commit")
	c.Assert(err, NotNil)
	// Check error type and error message
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue, Commentf("err %v", err))
	c.Assert(err.Error(), Equals, "previous statement: insert into test(id, val) values(1, 1);: [kv:1062]Duplicate entry '1' for key 'PRIMARY'")

	_, err = tk1.Exec("commit")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue, Commentf("err %v", err))
	c.Assert(err.Error(), Equals, "previous statement: insert into test(id, val) values(2, 2);: [kv:1062]Duplicate entry '2' for key 'val'")

	// Test for https://github.com/pingcap/tidb/issues/463
	tk.MustExec("drop table test;")
	tk.MustExec(`CREATE TABLE test (
			id int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
			val int UNIQUE,
			PRIMARY KEY (id)
		);`)
	tk.MustExec("insert into test(id, val) values(1, 1);")
	_, err = tk.Exec("insert into test(id, val) values(2, 1);")
	c.Assert(err, NotNil)
	tk.MustExec("insert into test(id, val) values(2, 2);")

	tk.MustExec("begin;")
	tk.MustExec("insert into test(id, val) values(3, 3);")
	_, err = tk.Exec("insert into test(id, val) values(4, 3);")
	c.Assert(err, NotNil)
	tk.MustExec("insert into test(id, val) values(4, 4);")
	tk.MustExec("commit;")

	tk1.MustExec("begin;")
	tk1.MustExec("insert into test(id, val) values(5, 6);")
	tk.MustExec("begin;")
	tk.MustExec("insert into test(id, val) values(20, 6);")
	tk.MustExec("commit;")
	tk1.Exec("commit")
	tk1.MustExec("insert into test(id, val) values(5, 5);")

	tk.MustExec("drop table test;")
	tk.MustExec(`CREATE TABLE test (
			id int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
			val1 int UNIQUE,
			val2 int UNIQUE,
			PRIMARY KEY (id)
		);`)
	tk.MustExec("insert into test(id, val1, val2) values(1, 1, 1);")
	tk.MustExec("insert into test(id, val1, val2) values(2, 2, 2);")
	tk.Exec("update test set val1 = 3, val2 = 2 where id = 1;")
	tk.MustExec("insert into test(id, val1, val2) values(3, 3, 3);")
}

func (s *testSessionSuite2) TestSet(c *C) {
	// Test for https://github.com/pingcap/tidb/issues/1114

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("set @tmp = 0")
	tk.MustExec("set @tmp := @tmp + 1")
	tk.MustQuery("select @tmp").Check(testkit.Rows("1"))
	tk.MustQuery("select @tmp1 = 1, @tmp2 := 2").Check(testkit.Rows("<nil> 2"))
	tk.MustQuery("select @tmp1 := 11, @tmp2").Check(testkit.Rows("11 2"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int);")
	tk.MustExec("insert into t values (1),(2);")
	tk.MustExec("update t set c = 3 WHERE c = @var:= 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("3", "2"))
	tk.MustQuery("select @tmp := count(*) from t").Check(testkit.Rows("2"))
	tk.MustQuery("select @tmp := c-2 from t where c=3").Check(testkit.Rows("1"))
}

func (s *testSessionSuite3) TestMySQLTypes(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustQuery(`select 0x01 + 1, x'4D7953514C' = "MySQL"`).Check(testkit.Rows("2 1"))
	tk.MustQuery(`select 0b01 + 1, 0b01000001 = "A"`).Check(testkit.Rows("2 1"))
}

func (s *testSessionSuite2) TestIssue986(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	sqlText := `CREATE TABLE address (
 		id bigint(20) NOT NULL AUTO_INCREMENT,
 		PRIMARY KEY (id));`
	tk.MustExec(sqlText)
	tk.MustExec(`insert into address values ('10')`)
}

func (s *testSessionSuite3) TestCast(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustQuery("select cast(0.5 as unsigned)")
	tk.MustQuery("select cast(-0.5 as signed)")
	tk.MustQuery("select hex(cast(0x10 as binary(2)))").Check(testkit.Rows("1000"))
}

func (s *testSessionSuite2) TestTableInfoMeta(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	checkResult := func(affectedRows uint64, insertID uint64) {
		gotRows := tk.Se.AffectedRows()
		c.Assert(gotRows, Equals, affectedRows)

		gotID := tk.Se.LastInsertID()
		c.Assert(gotID, Equals, insertID)
	}

	// create table
	tk.MustExec("CREATE TABLE tbl_test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")

	// insert data
	tk.MustExec(`INSERT INTO tbl_test VALUES (1, "hello");`)
	checkResult(1, 0)

	tk.MustExec(`INSERT INTO tbl_test VALUES (2, "hello");`)
	checkResult(1, 0)

	tk.MustExec(`UPDATE tbl_test SET name = "abc" where id = 2;`)
	checkResult(1, 0)

	tk.MustExec(`DELETE from tbl_test where id = 2;`)
	checkResult(1, 0)

	// select data
	tk.MustQuery("select * from tbl_test").Check(testkit.Rows("1 hello"))
}

func (s *testSessionSuite3) TestCaseInsensitive(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("create table T (a text, B int)")
	tk.MustExec("insert t (A, b) values ('aaa', 1)")
	rs, _ := tk.Exec("select * from t")
	fields := rs.Fields()
	c.Assert(fields[0].ColumnAsName.O, Equals, "a")
	c.Assert(fields[1].ColumnAsName.O, Equals, "B")
	rs, _ = tk.Exec("select A, b from t")
	fields = rs.Fields()
	c.Assert(fields[0].ColumnAsName.O, Equals, "A")
	c.Assert(fields[1].ColumnAsName.O, Equals, "b")
	rs, _ = tk.Exec("select a as A from t where A > 0")
	fields = rs.Fields()
	c.Assert(fields[0].ColumnAsName.O, Equals, "A")
	tk.MustExec("update T set b = B + 1")
	tk.MustExec("update T set B = b + 1")
	tk.MustQuery("select b from T").Check(testkit.Rows("3"))
}

// TestDeletePanic is for delete panic
func (s *testSessionSuite2) TestDeletePanic(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (c int)")
	tk.MustExec("insert into t values (1), (2), (3)")
	tk.MustExec("delete from `t` where `c` = ?", 1)
	tk.MustExec("delete from `t` where `c` = ?", 2)
}

func (s *testSessionSuite2) TestInformationSchemaCreateTime(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (c int)")
	ret := tk.MustQuery("select create_time from information_schema.tables where table_name='t';")
	// Make sure t1 is greater than t.
	time.Sleep(time.Second)
	tk.MustExec("alter table t modify c int default 11")
	ret1 := tk.MustQuery("select create_time from information_schema.tables where table_name='t';")
	t, err := types.ParseDatetime(nil, ret.Rows()[0][0].(string))
	c.Assert(err, IsNil)
	t1, err := types.ParseDatetime(nil, ret1.Rows()[0][0].(string))
	c.Assert(err, IsNil)
	r := t1.Compare(t)
	c.Assert(r, Equals, 1)
}

type testSchemaSuiteBase struct {
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	store     kv.Storage
	dom       *domain.Domain
}

type testSchemaSuite struct {
	testSchemaSuiteBase
}

type testSchemaSerialSuite struct {
	testSchemaSuiteBase
}

func (s *testSchemaSuiteBase) TearDownTest(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testSchemaSuiteBase) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(s.cluster)
	s.mvccStore = mocktikv.MustNewMVCCStore()
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(s.cluster),
		mockstore.WithMVCCStore(s.mvccStore),
	)
	c.Assert(err, IsNil)
	s.store = store
	session.DisableStats4Test()
	dom, err := session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.dom = dom
}

func (s *testSchemaSuiteBase) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *testSchemaSerialSuite) TestLoadSchemaFailed(c *C) {
	atomic.StoreInt32(&domain.SchemaOutOfDateRetryTimes, int32(3))
	atomic.StoreInt64(&domain.SchemaOutOfDateRetryInterval, int64(20*time.Millisecond))
	defer func() {
		atomic.StoreInt32(&domain.SchemaOutOfDateRetryTimes, 10)
		atomic.StoreInt64(&domain.SchemaOutOfDateRetryInterval, int64(500*time.Millisecond))
	}()

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("create table t (a int);")
	tk.MustExec("create table t1 (a int);")
	tk.MustExec("create table t2 (a int);")

	tk1.MustExec("begin")
	tk2.MustExec("begin")

	// Make sure loading information schema is failed and server is invalid.
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/domain/ErrorMockReloadFailed", `return(true)`), IsNil)
	err := domain.GetDomain(tk.Se).Reload()
	c.Assert(err, NotNil)

	lease := domain.GetDomain(tk.Se).DDL().GetLease()
	time.Sleep(lease * 2)

	// Make sure executing insert statement is failed when server is invalid.
	_, err = tk.Exec("insert t values (100);")
	c.Check(err, NotNil)

	tk1.MustExec("insert t1 values (100);")
	tk2.MustExec("insert t2 values (100);")

	_, err = tk1.Exec("commit")
	c.Check(err, NotNil)

	ver, err := s.store.CurrentVersion()
	c.Assert(err, IsNil)
	c.Assert(ver, NotNil)

	failpoint.Disable("github.com/pingcap/tidb/domain/ErrorMockReloadFailed")
	time.Sleep(lease * 2)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert t values (100);")
	// Make sure insert to table t2 transaction executes.
	tk2.MustExec("commit")
}

func (s *testSchemaSerialSuite) TestSchemaCheckerSQL(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk1 := testkit.NewTestKitWithInit(c, s.store)

	// create table
	tk.MustExec(`create table t (id int, c int);`)
	tk.MustExec(`create table t1 (id int, c int);`)
	// insert data
	tk.MustExec(`insert into t values(1, 1);`)

	// The schema version is out of date in the first transaction, but the SQL can be retried.
	tk.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk.MustExec(`begin;`)
	tk1.MustExec(`alter table t add index idx(c);`)
	tk.MustExec(`insert into t values(2, 2);`)
	tk.MustExec(`commit;`)

	// The schema version is out of date in the first transaction, and the SQL can't be retried.
	atomic.StoreUint32(&session.SchemaChangedWithoutRetry, 1)
	defer func() {
		atomic.StoreUint32(&session.SchemaChangedWithoutRetry, 0)
	}()
	tk.MustExec(`begin;`)
	tk1.MustExec(`alter table t modify column c bigint;`)
	tk.MustExec(`insert into t values(3, 3);`)
	_, err := tk.Exec(`commit;`)
	c.Assert(terror.ErrorEqual(err, domain.ErrInfoSchemaChanged), IsTrue, Commentf("err %v", err))

	// But the transaction related table IDs aren't in the updated table IDs.
	tk.MustExec(`begin;`)
	tk1.MustExec(`alter table t add index idx2(c);`)
	tk.MustExec(`insert into t1 values(4, 4);`)
	tk.MustExec(`commit;`)

	// Test for "select for update".
	tk.MustExec(`begin;`)
	tk1.MustExec(`alter table t add index idx3(c);`)
	tk.MustQuery(`select * from t for update`)
	_, err = tk.Exec(`commit;`)
	c.Assert(err, NotNil)
}

func (s *testSchemaSuite) TestPrepareStmtCommitWhenSchemaChanged(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk1 := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("create table t (a int, b int)")
	tk1.MustExec("prepare stmt from 'insert into t values (?, ?)'")
	tk1.MustExec("set @a = 1")

	// Commit find unrelated schema change.
	tk1.MustExec("begin")
	tk.MustExec("create table t1 (id int)")
	tk1.MustExec("execute stmt using @a, @a")
	tk1.MustExec("commit")

	tk1.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk1.MustExec("begin")
	tk.MustExec("alter table t drop column b")
	tk1.MustExec("execute stmt using @a, @a")
	_, err := tk1.Exec("commit")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrWrongValueCountOnRow), IsTrue, Commentf("err %v", err))
}

func (s *testSchemaSuite) TestCommitWhenSchemaChanged(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (a int, b int)")

	tk1.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk1.MustExec("begin")
	tk1.MustExec("insert into t values (1, 1)")

	tk.MustExec("alter table t drop column b")

	// When tk1 commit, it will find schema already changed.
	tk1.MustExec("insert into t values (4, 4)")
	_, err := tk1.Exec("commit")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrWrongValueCountOnRow), IsTrue, Commentf("err %v", err))
}

func (s *testSchemaSuite) TestRetrySchemaChangeForEmptyChange(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (i int)")
	tk.MustExec("create table t1 (i int)")
	tk.MustExec("begin")
	tk1.MustExec("alter table t add j int")
	tk.MustExec("delete from t")
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("commit")
}

func (s *testSchemaSuite) TestRetrySchemaChange(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values (1, 1)")

	tk1.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk1.MustExec("begin")
	tk1.MustExec("update t set b = 5 where a = 1")

	tk.MustExec("alter table t add index b_i (b)")

	run := false
	hook := func() {
		if !run {
			tk.MustExec("update t set b = 3 where a = 1")
			run = true
		}
	}

	// In order to cover a bug that statement history is not updated during retry.
	// See https://github.com/pingcap/tidb/pull/5202
	// Step1: when tk1 commit, it find schema changed and retry().
	// Step2: during retry, hook() is called, tk update primary key.
	// Step3: tk1 continue commit in retry() meet a retryable error(write conflict), retry again.
	// Step4: tk1 retry() success, if it use the stale statement, data and index will inconsistent.
	fpName := "github.com/pingcap/tidb/session/preCommitHook"
	c.Assert(failpoint.Enable(fpName, "return"), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	ctx := context.WithValue(context.Background(), "__preCommitHook", hook)
	err := tk1.Se.CommitTxn(ctx)
	c.Assert(err, IsNil)
	tk.MustQuery("select * from t where t.b = 5").Check(testkit.Rows("1 5"))
}

func (s *testSchemaSuite) TestRetryMissingUnionScan(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (a int primary key, b int unique, c int)")
	tk.MustExec("insert into t values (1, 1, 1)")

	tk1.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk1.MustExec("begin")
	tk1.MustExec("update t set b = 1, c = 2 where b = 2")
	tk1.MustExec("update t set b = 1, c = 2 where a = 1")

	// Create a conflict to reproduces the bug that the second update statement in retry
	// has a dirty table but doesn't use UnionScan.
	tk.MustExec("update t set b = 2 where a = 1")

	tk1.MustExec("commit")
}

func (s *testSchemaSuite) TestTableReaderChunk(c *C) {
	// Since normally a single region mock tikv only returns one partial result we need to manually split the
	// table to test multiple chunks.
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table chk (a int)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert chk values (%d)", i))
	}
	tbl, err := domain.GetDomain(tk.Se).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("chk"))
	c.Assert(err, IsNil)
	s.cluster.SplitTable(s.mvccStore, tbl.Meta().ID, 10)

	tk.Se.GetSessionVars().DistSQLScanConcurrency = 1
	tk.MustExec("set tidb_init_chunk_size = 2")
	defer func() {
		tk.MustExec(fmt.Sprintf("set tidb_init_chunk_size = %d", variable.DefInitChunkSize))
	}()
	rs, err := tk.Exec("select * from chk")
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	var count int
	var numChunks int
	for {
		err = rs.Next(context.TODO(), req)
		c.Assert(err, IsNil)
		numRows := req.NumRows()
		if numRows == 0 {
			break
		}
		for i := 0; i < numRows; i++ {
			c.Assert(req.GetRow(i).GetInt64(0), Equals, int64(count))
			count++
		}
		numChunks++
	}
	c.Assert(count, Equals, 100)
	// FIXME: revert this result to new group value after distsql can handle initChunkSize.
	c.Assert(numChunks, Equals, 1)
	rs.Close()
}

func (s *testSchemaSuite) TestInsertExecChunk(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table test1(a int)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert test1 values (%d)", i))
	}
	tk.MustExec("create table test2(a int)")

	tk.Se.GetSessionVars().DistSQLScanConcurrency = 1
	tk.MustExec("insert into test2(a) select a from test1;")

	rs, err := tk.Exec("select * from test2")
	c.Assert(err, IsNil)
	var idx int
	for {
		req := rs.NewChunk()
		err = rs.Next(context.TODO(), req)
		c.Assert(err, IsNil)
		if req.NumRows() == 0 {
			break
		}

		for rowIdx := 0; rowIdx < req.NumRows(); rowIdx++ {
			row := req.GetRow(rowIdx)
			c.Assert(row.GetInt64(0), Equals, int64(idx))
			idx++
		}
	}

	c.Assert(idx, Equals, 100)
	rs.Close()
}

func (s *testSchemaSuite) TestUpdateExecChunk(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table chk(a int)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert chk values (%d)", i))
	}

	tk.Se.GetSessionVars().DistSQLScanConcurrency = 1
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("update chk set a = a + 100 where a = %d", i))
	}

	rs, err := tk.Exec("select * from chk")
	c.Assert(err, IsNil)
	var idx int
	for {
		req := rs.NewChunk()
		err = rs.Next(context.TODO(), req)
		c.Assert(err, IsNil)
		if req.NumRows() == 0 {
			break
		}

		for rowIdx := 0; rowIdx < req.NumRows(); rowIdx++ {
			row := req.GetRow(rowIdx)
			c.Assert(row.GetInt64(0), Equals, int64(idx+100))
			idx++
		}
	}

	c.Assert(idx, Equals, 100)
	rs.Close()
}

func (s *testSchemaSuite) TestDeleteExecChunk(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table chk(a int)")

	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert chk values (%d)", i))
	}

	tk.Se.GetSessionVars().DistSQLScanConcurrency = 1

	for i := 0; i < 99; i++ {
		tk.MustExec(fmt.Sprintf("delete from chk where a = %d", i))
	}

	rs, err := tk.Exec("select * from chk")
	c.Assert(err, IsNil)

	req := rs.NewChunk()
	err = rs.Next(context.TODO(), req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows(), Equals, 1)

	row := req.GetRow(0)
	c.Assert(row.GetInt64(0), Equals, int64(99))
	rs.Close()
}

func (s *testSchemaSuite) TestDeleteMultiTableExecChunk(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table chk1(a int)")
	tk.MustExec("create table chk2(a int)")

	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert chk1 values (%d)", i))
	}

	for i := 0; i < 50; i++ {
		tk.MustExec(fmt.Sprintf("insert chk2 values (%d)", i))
	}

	tk.Se.GetSessionVars().DistSQLScanConcurrency = 1

	tk.MustExec("delete chk1, chk2 from chk1 inner join chk2 where chk1.a = chk2.a")

	rs, err := tk.Exec("select * from chk1")
	c.Assert(err, IsNil)

	var idx int
	for {
		req := rs.NewChunk()
		err = rs.Next(context.TODO(), req)
		c.Assert(err, IsNil)

		if req.NumRows() == 0 {
			break
		}

		for i := 0; i < req.NumRows(); i++ {
			row := req.GetRow(i)
			c.Assert(row.GetInt64(0), Equals, int64(idx+50))
			idx++
		}
	}
	c.Assert(idx, Equals, 50)
	rs.Close()

	rs, err = tk.Exec("select * from chk2")
	c.Assert(err, IsNil)

	req := rs.NewChunk()
	err = rs.Next(context.TODO(), req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows(), Equals, 0)
	rs.Close()
}

func (s *testSchemaSuite) TestIndexLookUpReaderChunk(c *C) {
	// Since normally a single region mock tikv only returns one partial result we need to manually split the
	// table to test multiple chunks.
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists chk")
	tk.MustExec("create table chk (k int unique, c int)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert chk values (%d, %d)", i, i))
	}
	tbl, err := domain.GetDomain(tk.Se).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("chk"))
	c.Assert(err, IsNil)
	s.cluster.SplitIndex(s.mvccStore, tbl.Meta().ID, tbl.Indices()[0].Meta().ID, 10)

	tk.Se.GetSessionVars().IndexLookupSize = 10
	rs, err := tk.Exec("select * from chk order by k")
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	var count int
	for {
		err = rs.Next(context.TODO(), req)
		c.Assert(err, IsNil)
		numRows := req.NumRows()
		if numRows == 0 {
			break
		}
		for i := 0; i < numRows; i++ {
			c.Assert(req.GetRow(i).GetInt64(0), Equals, int64(count))
			c.Assert(req.GetRow(i).GetInt64(1), Equals, int64(count))
			count++
		}
	}
	c.Assert(count, Equals, 100)
	rs.Close()

	rs, err = tk.Exec("select k from chk where c < 90 order by k")
	c.Assert(err, IsNil)
	req = rs.NewChunk()
	count = 0
	for {
		err = rs.Next(context.TODO(), req)
		c.Assert(err, IsNil)
		numRows := req.NumRows()
		if numRows == 0 {
			break
		}
		for i := 0; i < numRows; i++ {
			c.Assert(req.GetRow(i).GetInt64(0), Equals, int64(count))
			count++
		}
	}
	c.Assert(count, Equals, 90)
	rs.Close()
}

func (s *testSessionSuite2) TestStatementErrorInTransaction(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table statement_side_effect (c int primary key)")
	tk.MustExec("begin")
	tk.MustExec("insert into statement_side_effect values (1)")
	_, err := tk.Exec("insert into statement_side_effect value (2),(3),(4),(1)")
	c.Assert(err, NotNil)
	tk.MustQuery(`select * from statement_side_effect`).Check(testkit.Rows("1"))
	tk.MustExec("commit")
	tk.MustQuery(`select * from statement_side_effect`).Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists test;")
	tk.MustExec(`create table test (
 		  a int(11) DEFAULT NULL,
 		  b int(11) DEFAULT NULL
 	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;`)
	tk.MustExec("insert into test values (1, 2), (1, 2), (1, 1), (1, 1);")

	tk.MustExec("start transaction;")
	// In the transaction, statement error should not rollback the transaction.
	_, err = tk.Exec("update tset set b=11 where a=1 and b=2;")
	c.Assert(err, NotNil)
	// Test for a bug that last line rollback and exit transaction, this line autocommit.
	tk.MustExec("update test set b = 11 where a = 1 and b = 2;")
	tk.MustExec("rollback")
	tk.MustQuery("select * from test where a = 1 and b = 11").Check(testkit.Rows())
}

func (s *testSessionSerialSuite) TestStatementCountLimit(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table stmt_count_limit (id int)")
	saved := config.GetGlobalConfig().Performance.StmtCountLimit
	config.GetGlobalConfig().Performance.StmtCountLimit = 3
	defer func() {
		config.GetGlobalConfig().Performance.StmtCountLimit = saved
	}()
	tk.MustExec("set tidb_disable_txn_auto_retry = 0")
	tk.MustExec("begin")
	tk.MustExec("insert into stmt_count_limit values (1)")
	tk.MustExec("insert into stmt_count_limit values (2)")
	_, err := tk.Exec("insert into stmt_count_limit values (3)")
	c.Assert(err, NotNil)

	// begin is counted into history but this one is not.
	tk.MustExec("SET SESSION autocommit = false")
	tk.MustExec("insert into stmt_count_limit values (1)")
	tk.MustExec("insert into stmt_count_limit values (2)")
	tk.MustExec("insert into stmt_count_limit values (3)")
	_, err = tk.Exec("insert into stmt_count_limit values (4)")
	c.Assert(err, NotNil)
}

func (s *testSessionSerialSuite) TestBatchCommit(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("set tidb_batch_commit = 1")
	tk.MustExec("set tidb_disable_txn_auto_retry = 0")
	tk.MustExec("create table t (id int)")
	saved := config.GetGlobalConfig().Performance
	config.GetGlobalConfig().Performance.StmtCountLimit = 3
	defer func() {
		config.GetGlobalConfig().Performance = saved
	}()
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("SET SESSION autocommit = 1")
	tk.MustExec("begin")
	tk.MustExec("insert into t values (1)")
	tk1.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("insert into t values (2)")
	tk1.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("rollback")
	tk1.MustQuery("select * from t").Check(testkit.Rows())

	// The above rollback will not make the session in transaction.
	tk.MustExec("insert into t values (1)")
	tk1.MustQuery("select * from t").Check(testkit.Rows("1"))
	tk.MustExec("delete from t")

	tk.MustExec("begin")
	tk.MustExec("insert into t values (5)")
	tk1.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("insert into t values (6)")
	tk1.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("insert into t values (7)")
	tk1.MustQuery("select * from t").Check(testkit.Rows("5", "6", "7"))

	// The session is still in transaction.
	tk.MustExec("insert into t values (8)")
	tk1.MustQuery("select * from t").Check(testkit.Rows("5", "6", "7"))
	tk.MustExec("insert into t values (9)")
	tk1.MustQuery("select * from t").Check(testkit.Rows("5", "6", "7"))
	tk.MustExec("insert into t values (10)")
	tk1.MustQuery("select * from t").Check(testkit.Rows("5", "6", "7"))
	tk.MustExec("commit")
	tk1.MustQuery("select * from t").Check(testkit.Rows("5", "6", "7", "8", "9", "10"))

	// The above commit will not make the session in transaction.
	tk.MustExec("insert into t values (11)")
	tk1.MustQuery("select * from t").Check(testkit.Rows("5", "6", "7", "8", "9", "10", "11"))

	tk.MustExec("delete from t")
	tk.MustExec("SET SESSION autocommit = 0")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t values (2)")
	tk.MustExec("insert into t values (3)")
	tk.MustExec("rollback")
	tk1.MustExec("insert into t values (4)")
	tk1.MustExec("insert into t values (5)")
	tk.MustQuery("select * from t").Check(testkit.Rows("4", "5"))
}

func (s *testSessionSuite3) TestCastTimeToDate(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("set time_zone = '-8:00'")
	date := time.Now().In(time.FixedZone("", -8*int(time.Hour/time.Second)))
	tk.MustQuery("select cast(time('12:23:34') as date)").Check(testkit.Rows(date.Format("2006-01-02")))

	tk.MustExec("set time_zone = '+08:00'")
	date = time.Now().In(time.FixedZone("", 8*int(time.Hour/time.Second)))
	tk.MustQuery("select cast(time('12:23:34') as date)").Check(testkit.Rows(date.Format("2006-01-02")))
}

func (s *testSessionSuite) TestSetGlobalTZ(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("set time_zone = '+08:00'")
	tk.MustQuery("show variables like 'time_zone'").Check(testkit.Rows("time_zone +08:00"))

	tk.MustExec("set global time_zone = '+00:00'")

	tk.MustQuery("show variables like 'time_zone'").Check(testkit.Rows("time_zone +08:00"))

	// Disable global variable cache, so load global session variable take effect immediate.
	s.dom.GetGlobalVarsCache().Disable()
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustQuery("show variables like 'time_zone'").Check(testkit.Rows("time_zone +00:00"))
}

func (s *testSessionSuite2) TestRollbackOnCompileError(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert t values (1)")

	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk2.MustQuery("select * from t").Check(testkit.Rows("1"))

	tk.MustExec("rename table t to t2")

	var meetErr bool
	for i := 0; i < 100; i++ {
		_, err := tk2.Exec("insert t values (1)")
		if err != nil {
			meetErr = true
			break
		}
	}
	c.Assert(meetErr, IsTrue)
	tk.MustExec("rename table t2 to t")
	var recoverErr bool
	for i := 0; i < 100; i++ {
		_, err := tk2.Exec("insert t values (1)")
		if err == nil {
			recoverErr = true
			break
		}
	}
	c.Assert(recoverErr, IsTrue)
}

func (s *testSessionSuite3) TestSetTransactionIsolationOneShot(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (k int, v int)")
	tk.MustExec("insert t values (1, 42)")
	tk.MustExec("set tx_isolation = 'read-committed'")
	tk.MustQuery("select @@tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustExec("set tx_isolation = 'repeatable-read'")
	tk.MustExec("set transaction isolation level read committed")
	tk.MustQuery("select @@tx_isolation_one_shot").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@tx_isolation").Check(testkit.Rows("REPEATABLE-READ"))

	// Check isolation level is set to read committed.
	ctx := context.WithValue(context.Background(), "CheckSelectRequestHook", func(req *kv.Request) {
		c.Assert(req.IsolationLevel, Equals, kv.SI)
	})
	tk.Se.Execute(ctx, "select * from t where k = 1")

	// Check it just take effect for one time.
	ctx = context.WithValue(context.Background(), "CheckSelectRequestHook", func(req *kv.Request) {
		c.Assert(req.IsolationLevel, Equals, kv.SI)
	})
	tk.Se.Execute(ctx, "select * from t where k = 1")

	// Can't change isolation level when it's inside a transaction.
	tk.MustExec("begin")
	_, err := tk.Se.Execute(ctx, "set transaction isolation level read committed")
	c.Assert(err, NotNil)
}

func (s *testSessionSuite2) TestDBUserNameLength(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table if not exists t (a int)")
	// Test user name length can be longer than 16.
	tk.MustExec(`CREATE USER 'abcddfjakldfjaldddds'@'%' identified by ''`)
	tk.MustExec(`grant all privileges on test.* to 'abcddfjakldfjaldddds'@'%'`)
	tk.MustExec(`grant all privileges on test.t to 'abcddfjakldfjaldddds'@'%'`)
}

func (s *testSessionSerialSuite) TestKVVars(c *C) {
	c.Skip("there is no backoff here in the large txn, so this test is stale")
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table kvvars (a int, b int)")
	tk.MustExec("insert kvvars values (1, 1)")
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk2.MustExec("set @@tidb_backoff_lock_fast = 1")
	tk2.MustExec("set @@tidb_backoff_weight = 100")
	backoffVal := new(int64)
	backOffWeightVal := new(int32)
	tk2.Se.GetSessionVars().KVVars.Hook = func(name string, vars *kv.Variables) {
		atomic.StoreInt64(backoffVal, int64(vars.BackoffLockFast))
		atomic.StoreInt32(backOffWeightVal, int32(vars.BackOffWeight))
	}
	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		for {
			tk2.MustQuery("select * from kvvars")
			if atomic.LoadInt64(backoffVal) != 0 {
				break
			}
		}
		wg.Done()
	}()

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/mockSleepBetween2PC", "return"), IsNil)
	go func() {
		for {
			tk.MustExec("update kvvars set b = b + 1 where a = 1")
			if atomic.LoadInt64(backoffVal) != 0 {
				break
			}
		}
		wg.Done()
	}()
	wg.Wait()
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/mockSleepBetween2PC"), IsNil)

	for {
		tk2.MustQuery("select * from kvvars")
		if atomic.LoadInt32(backOffWeightVal) != 0 {
			break
		}
	}
	c.Assert(atomic.LoadInt64(backoffVal), Equals, int64(1))
	c.Assert(atomic.LoadInt32(backOffWeightVal), Equals, int32(100))
}

func (s *testSessionSuite2) TestCommitRetryCount(c *C) {
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustExec("create table no_retry (id int)")
	tk1.MustExec("insert into no_retry values (1)")
	tk1.MustExec("set @@tidb_retry_limit = 0")

	tk1.MustExec("begin")
	tk1.MustExec("update no_retry set id = 2")

	tk2.MustExec("begin")
	tk2.MustExec("update no_retry set id = 3")
	tk2.MustExec("commit")

	// No auto retry because retry limit is set to 0.
	_, err := tk1.Se.Execute(context.Background(), "commit")
	c.Assert(err, NotNil)
}

func (s *testSessionSuite3) TestEnablePartition(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("set tidb_enable_table_partition=off")
	tk.MustQuery("show variables like 'tidb_enable_table_partition'").Check(testkit.Rows("tidb_enable_table_partition off"))

	tk.MustExec("set global tidb_enable_table_partition = on")

	tk.MustQuery("show variables like 'tidb_enable_table_partition'").Check(testkit.Rows("tidb_enable_table_partition off"))
	tk.MustQuery("show global variables like 'tidb_enable_table_partition'").Check(testkit.Rows("tidb_enable_table_partition on"))

	// Disable global variable cache, so load global session variable take effect immediate.
	s.dom.GetGlobalVarsCache().Disable()
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustQuery("show variables like 'tidb_enable_table_partition'").Check(testkit.Rows("tidb_enable_table_partition on"))
}

func (s *testSessionSerialSuite) TestTxnRetryErrMsg(c *C) {
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustExec("create table no_retry (id int)")
	tk1.MustExec("insert into no_retry values (1)")
	tk1.MustExec("begin")
	tk2.MustExec("update no_retry set id = id + 1")
	tk1.MustExec("update no_retry set id = id + 1")
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/ErrMockRetryableOnly", `return(true)`), IsNil)
	_, err := tk1.Se.Execute(context.Background(), "commit")
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/ErrMockRetryableOnly"), IsNil)
	c.Assert(err, NotNil)
	c.Assert(kv.ErrTxnRetryable.Equal(err), IsTrue, Commentf("error: %s", err))
	c.Assert(strings.Contains(err.Error(), "mock retryable error"), IsTrue, Commentf("error: %s", err))
	c.Assert(strings.Contains(err.Error(), kv.TxnRetryableMark), IsTrue, Commentf("error: %s", err))
}

func (s *testSchemaSuite) TestDisableTxnAutoRetry(c *C) {
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustExec("create table no_retry (id int)")
	tk1.MustExec("insert into no_retry values (1)")
	tk1.MustExec("set @@tidb_disable_txn_auto_retry = 1")

	tk1.MustExec("begin")
	tk1.MustExec("update no_retry set id = 2")

	tk2.MustExec("begin")
	tk2.MustExec("update no_retry set id = 3")
	tk2.MustExec("commit")

	// No auto retry because tidb_disable_txn_auto_retry is set to 1.
	_, err := tk1.Se.Execute(context.Background(), "commit")
	c.Assert(err, NotNil)

	// session 1 starts a transaction early.
	// execute a select statement to clear retry history.
	tk1.MustExec("select 1")
	tk1.Se.PrepareTxnCtx(context.Background())
	// session 2 update the value.
	tk2.MustExec("update no_retry set id = 4")
	// AutoCommit update will retry, so it would not fail.
	tk1.MustExec("update no_retry set id = 5")

	// RestrictedSQL should retry.
	tk1.Se.GetSessionVars().InRestrictedSQL = true
	tk1.MustExec("begin")

	tk2.MustExec("update no_retry set id = 6")

	tk1.MustExec("update no_retry set id = 7")
	tk1.MustExec("commit")

	// test for disable transaction local latch
	tk1.Se.GetSessionVars().InRestrictedSQL = false
	orgCfg := config.GetGlobalConfig()
	disableLatchCfg := *orgCfg
	disableLatchCfg.TxnLocalLatches.Enabled = false
	config.StoreGlobalConfig(&disableLatchCfg)
	defer config.StoreGlobalConfig(orgCfg)
	tk1.MustExec("begin")
	tk1.MustExec("update no_retry set id = 9")

	tk2.MustExec("update no_retry set id = 8")

	_, err = tk1.Se.Execute(context.Background(), "commit")
	c.Assert(err, NotNil)
	c.Assert(kv.ErrWriteConflict.Equal(err), IsTrue, Commentf("error: %s", err))
	c.Assert(strings.Contains(err.Error(), kv.TxnRetryableMark), IsTrue, Commentf("error: %s", err))
	tk1.MustExec("rollback")

	enableLatchCfg := *orgCfg
	enableLatchCfg.TxnLocalLatches.Enabled = true
	config.StoreGlobalConfig(&enableLatchCfg)
	tk1.MustExec("begin")
	tk2.MustExec("alter table no_retry add index idx(id)")
	tk2.MustQuery("select * from no_retry").Check(testkit.Rows("8"))
	tk1.MustExec("update no_retry set id = 10")
	_, err = tk1.Se.Execute(context.Background(), "commit")
	c.Assert(err, NotNil)

	// set autocommit to begin and commit
	tk1.MustExec("set autocommit = 0")
	tk1.MustQuery("select * from no_retry").Check(testkit.Rows("8"))
	tk2.MustExec("update no_retry set id = 11")
	tk1.MustExec("update no_retry set id = 12")
	_, err = tk1.Se.Execute(context.Background(), "set autocommit = 1")
	c.Assert(err, NotNil)
	c.Assert(kv.ErrWriteConflict.Equal(err), IsTrue, Commentf("error: %s", err))
	c.Assert(strings.Contains(err.Error(), kv.TxnRetryableMark), IsTrue, Commentf("error: %s", err))
	tk1.MustExec("rollback")
	tk2.MustQuery("select * from no_retry").Check(testkit.Rows("11"))

	tk1.MustExec("set autocommit = 0")
	tk1.MustQuery("select * from no_retry").Check(testkit.Rows("11"))
	tk2.MustExec("update no_retry set id = 13")
	tk1.MustExec("update no_retry set id = 14")
	_, err = tk1.Se.Execute(context.Background(), "commit")
	c.Assert(err, NotNil)
	c.Assert(kv.ErrWriteConflict.Equal(err), IsTrue, Commentf("error: %s", err))
	c.Assert(strings.Contains(err.Error(), kv.TxnRetryableMark), IsTrue, Commentf("error: %s", err))
	tk1.MustExec("rollback")
	tk2.MustQuery("select * from no_retry").Check(testkit.Rows("13"))
}

// TestSetGroupConcatMaxLen is for issue #7034
func (s *testSessionSuite2) TestSetGroupConcatMaxLen(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	// Normal case
	tk.MustExec("set global group_concat_max_len = 100")
	tk.MustExec("set @@session.group_concat_max_len = 50")
	result := tk.MustQuery("show global variables  where variable_name='group_concat_max_len';")
	result.Check(testkit.Rows("group_concat_max_len 100"))

	result = tk.MustQuery("show session variables  where variable_name='group_concat_max_len';")
	result.Check(testkit.Rows("group_concat_max_len 50"))

	result = tk.MustQuery("select @@group_concat_max_len;")
	result.Check(testkit.Rows("50"))

	result = tk.MustQuery("select @@global.group_concat_max_len;")
	result.Check(testkit.Rows("100"))

	result = tk.MustQuery("select @@session.group_concat_max_len;")
	result.Check(testkit.Rows("50"))

	tk.MustExec("set @@group_concat_max_len = 1024")

	result = tk.MustQuery("select @@group_concat_max_len;")
	result.Check(testkit.Rows("1024"))

	result = tk.MustQuery("select @@global.group_concat_max_len;")
	result.Check(testkit.Rows("100"))

	result = tk.MustQuery("select @@session.group_concat_max_len;")
	result.Check(testkit.Rows("1024"))

	// Test value out of range
	tk.MustExec("set @@group_concat_max_len=1")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect group_concat_max_len value: '1'"))
	result = tk.MustQuery("select @@group_concat_max_len;")
	result.Check(testkit.Rows("4"))

	_, err := tk.Exec("set @@group_concat_max_len = 18446744073709551616")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))

	// Test illegal type
	_, err = tk.Exec("set @@group_concat_max_len='hello'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))
}

func (s *testSessionSuite2) TestUpdatePrivilege(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (id int);")
	tk.MustExec("create table t2 (id int);")
	tk.MustExec("insert into t1 values (1);")
	tk.MustExec("insert into t2 values (2);")
	tk.MustExec("create user xxx;")
	tk.MustExec("grant all on test.t1 to xxx;")
	tk.MustExec("grant select on test.t2 to xxx;")
	tk.MustExec("flush privileges;")

	tk1 := testkit.NewTestKitWithInit(c, s.store)
	c.Assert(tk1.Se.Auth(&auth.UserIdentity{Username: "xxx", Hostname: "localhost"},
		[]byte(""),
		[]byte("")), IsTrue)

	_, err := tk1.Exec("update t2 set id = 666 where id = 1;")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "privilege check fail"), IsTrue)

	// Cover a bug that t1 and t2 both require update privilege.
	// In fact, the privlege check for t1 should be update, and for t2 should be select.
	_, err = tk1.Exec("update t1,t2 set t1.id = t2.id;")
	c.Assert(err, IsNil)

	// Fix issue 8911
	tk.MustExec("create database weperk")
	tk.MustExec("use weperk")
	tk.MustExec("create table tb_wehub_server (id int, active_count int, used_count int)")
	tk.MustExec("create user 'weperk'")
	tk.MustExec("grant all privileges on weperk.* to 'weperk'@'%'")
	c.Assert(tk1.Se.Auth(&auth.UserIdentity{Username: "weperk", Hostname: "%"},
		[]byte(""), []byte("")), IsTrue)
	tk1.MustExec("use weperk")
	tk1.MustExec("update tb_wehub_server a set a.active_count=a.active_count+1,a.used_count=a.used_count+1 where id=1")

	tk.MustExec("create database service")
	tk.MustExec("create database report")
	tk.MustExec(`CREATE TABLE service.t1 (
  id int(11) DEFAULT NULL,
  a bigint(20) NOT NULL,
  b text DEFAULT NULL,
  PRIMARY KEY (a)
)`)
	tk.MustExec(`CREATE TABLE report.t2 (
  a bigint(20) DEFAULT NULL,
  c bigint(20) NOT NULL
)`)
	tk.MustExec("grant all privileges on service.* to weperk")
	tk.MustExec("grant all privileges on report.* to weperk")
	tk1.Se.GetSessionVars().CurrentDB = ""
	tk1.MustExec(`update service.t1 s,
report.t2 t
set s.a = t.a
WHERE
s.a = t.a
and t.c >=  1 and t.c <= 10000
and s.b !='xx';`)

	// Fix issue 10028
	tk.MustExec("create database ap")
	tk.MustExec("create database tp")
	tk.MustExec("grant all privileges on ap.* to xxx")
	tk.MustExec("grant select on tp.* to xxx")
	tk.MustExec("flush privileges")
	tk.MustExec("create table tp.record( id int,name varchar(128),age int)")
	tk.MustExec("insert into tp.record (id,name,age) values (1,'john',18),(2,'lary',19),(3,'lily',18)")
	tk.MustExec("create table ap.record( id int,name varchar(128),age int)")
	tk.MustExec("insert into ap.record(id) values(1)")
	c.Assert(tk1.Se.Auth(&auth.UserIdentity{Username: "xxx", Hostname: "localhost"},
		[]byte(""),
		[]byte("")), IsTrue)
	_, err2 := tk1.Exec("update ap.record t inner join tp.record tt on t.id=tt.id  set t.name=tt.name")
	c.Assert(err2, IsNil)
}

func (s *testSessionSuite2) TestTxnGoString(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists gostr;")
	tk.MustExec("create table gostr (id int);")
	txn, err := tk.Se.Txn(false)
	c.Assert(err, IsNil)
	str1 := fmt.Sprintf("%#v", txn)
	c.Assert(str1, Equals, "Txn{state=invalid}")
	tk.MustExec("begin")
	txn, err = tk.Se.Txn(false)
	c.Assert(err, IsNil)
	c.Assert(fmt.Sprintf("%#v", txn), Equals, fmt.Sprintf("Txn{state=valid, txnStartTS=%d}", txn.StartTS()))

	tk.MustExec("insert into gostr values (1)")
	c.Assert(fmt.Sprintf("%#v", txn), Equals, fmt.Sprintf("Txn{state=valid, txnStartTS=%d}", txn.StartTS()))

	tk.MustExec("rollback")
	c.Assert(fmt.Sprintf("%#v", txn), Equals, "Txn{state=invalid}")
}

func (s *testSessionSuite3) TestMaxExeucteTime(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("create table MaxExecTime( id int,name varchar(128),age int);")
	tk.MustExec("begin")
	tk.MustExec("insert into MaxExecTime (id,name,age) values (1,'john',18),(2,'lary',19),(3,'lily',18);")

	tk.MustQuery("select @@MAX_EXECUTION_TIME;").Check(testkit.Rows("0"))
	tk.MustQuery("select @@global.MAX_EXECUTION_TIME;").Check(testkit.Rows("0"))
	tk.MustQuery("select /*+ MAX_EXECUTION_TIME(1000) */ * FROM MaxExecTime;")

	tk.MustExec("set @@global.MAX_EXECUTION_TIME = 300;")
	tk.MustQuery("select * FROM MaxExecTime;")

	tk.MustExec("set @@MAX_EXECUTION_TIME = 150;")
	tk.MustQuery("select * FROM MaxExecTime;")

	tk.MustQuery("select @@global.MAX_EXECUTION_TIME;").Check(testkit.Rows("300"))
	tk.MustQuery("select @@MAX_EXECUTION_TIME;").Check(testkit.Rows("150"))

	tk.MustExec("set @@global.MAX_EXECUTION_TIME = 0;")
	tk.MustExec("set @@MAX_EXECUTION_TIME = 0;")
	tk.MustExec("commit")
	tk.MustExec("drop table if exists MaxExecTime;")
}

func (s *testSessionSuite2) TestGrantViewRelated(c *C) {
	tkRoot := testkit.NewTestKitWithInit(c, s.store)
	tkUser := testkit.NewTestKitWithInit(c, s.store)

	tkRoot.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	tkRoot.MustExec("create table if not exists t (a int)")
	tkRoot.MustExec("create view v_version29 as select * from t")
	tkRoot.MustExec("create user 'u_version29'@'%'")
	tkRoot.MustExec("grant select on t to u_version29@'%'")

	tkUser.Se.Auth(&auth.UserIdentity{Username: "u_version29", Hostname: "localhost", CurrentUser: true, AuthUsername: "u_version29", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	tkUser.MustQuery("select current_user();").Check(testkit.Rows("u_version29@%"))
	err := tkUser.ExecToErr("select * from test.v_version29;")
	c.Assert(err, NotNil)
	tkUser.MustQuery("select current_user();").Check(testkit.Rows("u_version29@%"))
	err = tkUser.ExecToErr("create view v_version29_c as select * from t;")
	c.Assert(err, NotNil)

	tkRoot.MustExec(`grant show view on v_version29 to 'u_version29'@'%'`)
	tkRoot.MustQuery("select table_priv from mysql.tables_priv where host='%' and db='test' and user='u_version29' and table_name='v_version29'").Check(testkit.Rows("Show View"))

	tkUser.MustQuery("select current_user();").Check(testkit.Rows("u_version29@%"))
	tkUser.MustQuery("show create view v_version29;")
	err = tkUser.ExecToErr("create view v_version29_c as select * from v_version29;")
	c.Assert(err, NotNil)

	tkRoot.MustExec("create view v_version29_c as select * from v_version29;")
	tkRoot.MustExec(`grant create view on v_version29_c to 'u_version29'@'%'`) // Can't grant privilege on a non-exist table/view.
	tkRoot.MustQuery("select table_priv from mysql.tables_priv where host='%' and db='test' and user='u_version29' and table_name='v_version29_c'").Check(testkit.Rows("Create View"))
	tkRoot.MustExec("drop view v_version29_c")

	tkRoot.MustExec(`grant select on v_version29 to 'u_version29'@'%'`)
	tkUser.MustQuery("select current_user();").Check(testkit.Rows("u_version29@%"))
	tkUser.MustExec("create view v_version29_c as select * from v_version29;")
}

func (s *testSessionSuite3) TestLoadClientInteractive(c *C) {
	var (
		err          error
		connectionID uint64
	)
	tk := testkit.NewTestKit(c, s.store)
	tk.Se, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	id := atomic.AddUint64(&connectionID, 1)
	tk.Se.SetConnectionID(id)
	tk.Se.GetSessionVars().ClientCapability = tk.Se.GetSessionVars().ClientCapability | mysql.ClientInteractive
	tk.MustQuery("select @@wait_timeout").Check(testkit.Rows("28800"))
}

func (s *testSessionSuite2) TestReplicaRead(c *C) {
	var err error
	tk := testkit.NewTestKit(c, s.store)
	tk.Se, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	c.Assert(tk.Se.GetSessionVars().GetReplicaRead(), Equals, kv.ReplicaReadLeader)
	tk.MustExec("set @@tidb_replica_read = 'follower';")
	c.Assert(tk.Se.GetSessionVars().GetReplicaRead(), Equals, kv.ReplicaReadFollower)
	tk.MustExec("set @@tidb_replica_read = 'leader';")
	c.Assert(tk.Se.GetSessionVars().GetReplicaRead(), Equals, kv.ReplicaReadLeader)
}

func (s *testSessionSuite3) TestIsolationRead(c *C) {
	var err error
	tk := testkit.NewTestKit(c, s.store)
	tk.Se, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	c.Assert(len(tk.Se.GetSessionVars().GetIsolationReadEngines()), Equals, 3)
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash';")
	engines := tk.Se.GetSessionVars().GetIsolationReadEngines()
	c.Assert(len(engines), Equals, 1)
	_, hasTiFlash := engines[kv.TiFlash]
	_, hasTiKV := engines[kv.TiKV]
	c.Assert(hasTiFlash, Equals, true)
	c.Assert(hasTiKV, Equals, false)
}

func (s *testSessionSuite2) TestStmtHints(c *C) {
	var err error
	tk := testkit.NewTestKit(c, s.store)
	tk.Se, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)

	// Test MEMORY_QUOTA hint
	tk.MustExec("select /*+ MEMORY_QUOTA(1 MB) */ 1;")
	val := int64(1) * 1024 * 1024
	c.Assert(tk.Se.GetSessionVars().StmtCtx.MemTracker.CheckBytesLimit(val), IsTrue)
	tk.MustExec("select /*+ MEMORY_QUOTA(1 GB) */ 1;")
	val = int64(1) * 1024 * 1024 * 1024
	c.Assert(tk.Se.GetSessionVars().StmtCtx.MemTracker.CheckBytesLimit(val), IsTrue)
	tk.MustExec("select /*+ MEMORY_QUOTA(1 GB), MEMORY_QUOTA(1 MB) */ 1;")
	val = int64(1) * 1024 * 1024
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.MemTracker.CheckBytesLimit(val), IsTrue)
	tk.MustExec("select /*+ MEMORY_QUOTA(0 GB) */ 1;")
	val = int64(0)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.MemTracker.CheckBytesLimit(val), IsTrue)

	// Test NO_INDEX_MERGE hint
	tk.Se.GetSessionVars().SetEnableIndexMerge(true)
	tk.MustExec("select /*+ NO_INDEX_MERGE() */ 1;")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.NoIndexMergeHint, IsTrue)
	tk.MustExec("select /*+ NO_INDEX_MERGE(), NO_INDEX_MERGE() */ 1;")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetSessionVars().GetEnableIndexMerge(), IsTrue)

	// Test USE_TOJA hint
	tk.Se.GetSessionVars().SetAllowInSubqToJoinAndAgg(true)
	tk.MustExec("select /*+ USE_TOJA(false) */ 1;")
	c.Assert(tk.Se.GetSessionVars().GetAllowInSubqToJoinAndAgg(), IsFalse)
	tk.Se.GetSessionVars().SetAllowInSubqToJoinAndAgg(false)
	tk.MustExec("select /*+ USE_TOJA(true) */ 1;")
	c.Assert(tk.Se.GetSessionVars().GetAllowInSubqToJoinAndAgg(), IsTrue)
	tk.MustExec("select /*+ USE_TOJA(false), USE_TOJA(true) */ 1;")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetSessionVars().GetAllowInSubqToJoinAndAgg(), IsTrue)

	// Test USE_CASCADES hint
	tk.Se.GetSessionVars().SetEnableCascadesPlanner(true)
	tk.MustExec("select /*+ USE_CASCADES(false) */ 1;")
	c.Assert(tk.Se.GetSessionVars().GetEnableCascadesPlanner(), IsFalse)
	tk.Se.GetSessionVars().SetEnableCascadesPlanner(false)
	tk.MustExec("select /*+ USE_CASCADES(true) */ 1;")
	c.Assert(tk.Se.GetSessionVars().GetEnableCascadesPlanner(), IsTrue)
	tk.MustExec("select /*+ USE_CASCADES(false), USE_CASCADES(true) */ 1;")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings()[0].Err.Error(), Equals, "USE_CASCADES() is defined more than once, only the last definition takes effect: USE_CASCADES(true)")
	c.Assert(tk.Se.GetSessionVars().GetEnableCascadesPlanner(), IsTrue)

	// Test READ_CONSISTENT_REPLICA hint
	tk.Se.GetSessionVars().SetReplicaRead(kv.ReplicaReadLeader)
	tk.MustExec("select /*+ READ_CONSISTENT_REPLICA() */ 1;")
	c.Assert(tk.Se.GetSessionVars().GetReplicaRead(), Equals, kv.ReplicaReadFollower)
	tk.MustExec("select /*+ READ_CONSISTENT_REPLICA(), READ_CONSISTENT_REPLICA() */ 1;")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetSessionVars().GetReplicaRead(), Equals, kv.ReplicaReadFollower)
}

func (s *testSessionSuite3) TestPessimisticLockOnPartition(c *C) {
	// This test checks that 'select ... for update' locks the partition instead of the table.
	// Cover a bug that table ID is used to encode the lock key mistakenly.
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`create table if not exists forupdate_on_partition (
  age int not null primary key,
  nickname varchar(20) not null,
  gender int not null default 0,
  first_name varchar(30) not null default '',
  last_name varchar(20) not null default '',
  full_name varchar(60) as (concat(first_name, ' ', last_name)),
  index idx_nickname (nickname)
) partition by range (age) (
  partition child values less than (18),
  partition young values less than (30),
  partition middle values less than (50),
  partition old values less than (123)
);`)
	tk.MustExec("insert into forupdate_on_partition (`age`, `nickname`) values (25, 'cosven');")

	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("use test")

	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from forupdate_on_partition where age=25 for update").Check(testkit.Rows("25 cosven 0    "))
	tk1.MustExec("begin pessimistic")

	ch := make(chan int32, 5)
	go func() {
		tk1.MustExec("update forupdate_on_partition set first_name='sw' where age=25")
		ch <- 0
		tk1.MustExec("commit")
	}()

	// Leave 50ms for tk1 to run, tk1 should be blocked at the update operation.
	time.Sleep(50 * time.Millisecond)
	ch <- 1

	tk.MustExec("commit")
	// tk1 should be blocked until tk commit, check the order.
	c.Assert(<-ch, Equals, int32(1))
	c.Assert(<-ch, Equals, int32(0))

	// Once again...
	// This time, test for the update-update conflict.
	tk.MustExec("begin pessimistic")
	tk.MustExec("update forupdate_on_partition set first_name='sw' where age=25")
	tk1.MustExec("begin pessimistic")

	go func() {
		tk1.MustExec("update forupdate_on_partition set first_name = 'xxx' where age=25")
		ch <- 0
		tk1.MustExec("commit")
	}()

	// Leave 50ms for tk1 to run, tk1 should be blocked at the update operation.
	time.Sleep(50 * time.Millisecond)
	ch <- 1

	tk.MustExec("commit")
	// tk1 should be blocked until tk commit, check the order.
	c.Assert(<-ch, Equals, int32(1))
	c.Assert(<-ch, Equals, int32(0))
}

func (s *testSchemaSuite) TestTxnSize(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists txn_size")
	tk.MustExec("create table txn_size (k int , v varchar(64))")
	tk.MustExec("begin")
	tk.MustExec("insert txn_size values (1, 'dfaasdfsdf')")
	tk.MustExec("insert txn_size values (2, 'dsdfaasdfsdf')")
	tk.MustExec("insert txn_size values (3, 'abcdefghijkl')")
	txn, err := tk.Se.Txn(false)
	c.Assert(err, IsNil)
	c.Assert(txn.Size() > 0, IsTrue)
}
