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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package session_test

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"path"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/go-units"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/session/txninfo"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/copr"
	"github.com/pingcap/tidb/store/driver"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mockcopr"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	newTestkit "github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

var (
	pdAddrs         = flag.String("pd-addrs", "127.0.0.1:2379", "pd addrs")
	withTiKV        = flag.Bool("with-tikv", false, "run tests with TiKV cluster started. (not use the mock server)")
	pdAddrChan      chan string
	initPdAddrsOnce sync.Once
)

var _ = Suite(&testSessionSuite{})
var _ = Suite(&testSessionSuite2{})
var _ = Suite(&testSessionSuite3{})
var _ = Suite(&testSchemaSuite{})
var _ = SerialSuites(&testSchemaSerialSuite{})
var _ = SerialSuites(&testSessionSerialSuite{})
var _ = SerialSuites(&testBackupRestoreSuite{})
var _ = SerialSuites(&testTxnStateSerialSuite{})
var _ = SerialSuites(&testStatisticsSuite{})
var _ = SerialSuites(&testTiDBAsLibrary{})

type testSessionSuiteBase struct {
	cluster testutils.Cluster
	store   kv.Storage
	dom     *domain.Domain
	pdAddr  string
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

type testBackupRestoreSuite struct {
	testSessionSuiteBase
}

// testStatisticsSuite contains test about statistics which need running with real TiKV.
// Only tests under /session will be run with real TiKV so we put them here instead of /statistics.
type testStatisticsSuite struct {
	testSessionSuiteBase
}

type testTiDBAsLibrary struct{}

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

func clearETCD(ebd kv.EtcdBackend) error {
	endpoints, err := ebd.EtcdAddrs()
	if err != nil {
		return err
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:        endpoints,
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

	resp, err := cli.Get(context.Background(), "/tidb", clientv3.WithPrefix())
	if err != nil {
		return errors.Trace(err)
	}
	for _, kv := range resp.Kvs {
		if kv.Lease != 0 {
			if _, err := cli.Revoke(context.Background(), clientv3.LeaseID(kv.Lease)); err != nil {
				return errors.Trace(err)
			}
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

	if *withTiKV {
		initPdAddrs()
		s.pdAddr = <-pdAddrChan
		var d driver.TiKVDriver
		config.UpdateGlobal(func(conf *config.Config) {
			conf.TxnLocalLatches.Enabled = false
		})
		store, err := d.Open(fmt.Sprintf("tikv://%s?disableGC=true", s.pdAddr))
		c.Assert(err, IsNil)
		err = clearStorage(store)
		c.Assert(err, IsNil)
		err = clearETCD(store.(kv.EtcdBackend))
		c.Assert(err, IsNil)
		session.ResetStoreForWithTiKVTest(store)
		s.store = store
	} else {
		store, err := mockstore.NewMockStore(
			mockstore.WithClusterInspector(func(c testutils.Cluster) {
				mockstore.BootstrapWithSingleStore(c)
				s.cluster = c
			}),
		)
		c.Assert(err, IsNil)
		s.store = store
		session.DisableStats4Test()
	}

	var err error
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
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
		switch tableType {
		case "VIEW":
			tk.MustExec(fmt.Sprintf("drop view %v", tableName))
		case "BASE TABLE":
			tk.MustExec(fmt.Sprintf("drop table %v", tableName))
		default:
			panic(fmt.Sprintf("Unexpected table '%s' with type '%s'.", tableName, tableType))
		}
	}
}

func createStorage(t *testing.T) (kv.Storage, func()) {
	if *withTiKV {
		initPdAddrs()
		pdAddr := <-pdAddrChan
		var d driver.TiKVDriver
		config.UpdateGlobal(func(conf *config.Config) {
			conf.TxnLocalLatches.Enabled = false
		})
		store, err := d.Open(fmt.Sprintf("tikv://%s?disableGC=true", pdAddr))
		require.NoError(t, err)
		require.NoError(t, clearStorage(store))
		require.NoError(t, clearETCD(store.(kv.EtcdBackend)))
		session.ResetStoreForWithTiKVTest(store)
		dom, err := session.BootstrapSession(store)
		require.NoError(t, err)

		return store, func() {
			dom.Close()
			require.NoError(t, store.Close())
			pdAddrChan <- pdAddr
		}
	}
	return newTestkit.CreateMockStore(t)
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
	return mockPumpPullBinlogsClient{mockcopr.MockGRPCClientStream()}, nil
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

	// Test execution of DDL through the "Execute" interface.
	_, err = tk.Se.Execute(context.Background(), "use test;")
	c.Assert(err, IsNil)
	_, err = tk.Se.Execute(context.Background(), "drop table t2")
	c.Assert(err, IsNil)
	_, err = tk.Se.Execute(context.Background(), "prepare stmt from 'CREATE TABLE t2(id bigint PRIMARY KEY, age int)'")
	c.Assert(err, IsNil)
	_, err = tk.Se.Execute(context.Background(), "execute stmt")
	c.Assert(err, IsNil)
	qs = tk.Se.Value(sessionctx.QueryString)
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
	tk.MustExec(`insert t(id) values(1);`)
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
	_, err := tk.Se.Txn(true)
	c.Assert(kv.ErrInvalidTxn.Equal(err), IsTrue)
	txn, err := tk.Se.Txn(false)
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
	tk.MustExec("set @@global.sql_select_limit = 1")
	result = tk.MustQuery("show global variables where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 1"))
	tk.MustExec("set @@global.sql_select_limit = default")
	result = tk.MustQuery("show global variables where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 18446744073709551615"))

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

func (s *testSessionSuite) TestUpgradeSysvars(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	se := tk.Se.(variable.GlobalVarAccessor)

	// Set the global var to a non canonical form of the value
	// i.e. implying that it was set from an earlier version of TiDB.

	tk.MustExec(`REPLACE INTO mysql.global_variables (variable_name, variable_value) VALUES ('tidb_enable_noop_functions', '0')`)
	domain.GetDomain(tk.Se).NotifyUpdateSysVarCache() // update cache
	v, err := se.GetGlobalSysVar("tidb_enable_noop_functions")
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "OFF")

	// Set the global var to ""  which is the invalid version of this from TiDB 4.0.16
	// the err is quashed by the GetGlobalSysVar, and the default value is restored.
	// This helps callers of GetGlobalSysVar(), which can't individually be expected
	// to handle upgrade/downgrade issues correctly.

	tk.MustExec(`REPLACE INTO mysql.global_variables (variable_name, variable_value) VALUES ('rpl_semi_sync_slave_enabled', '')`)
	domain.GetDomain(tk.Se).NotifyUpdateSysVarCache() // update cache
	v, err = se.GetGlobalSysVar("rpl_semi_sync_slave_enabled")
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "OFF") // the default value is restored.
	result := tk.MustQuery("SHOW VARIABLES LIKE 'rpl_semi_sync_slave_enabled'")
	result.Check(testkit.Rows("rpl_semi_sync_slave_enabled OFF"))

	// Ensure variable out of range is converted to in range after upgrade.
	// This further helps for https://github.com/pingcap/tidb/pull/28842

	tk.MustExec(`REPLACE INTO mysql.global_variables (variable_name, variable_value) VALUES ('tidb_executor_concurrency', '999')`)
	domain.GetDomain(tk.Se).NotifyUpdateSysVarCache() // update cache
	v, err = se.GetGlobalSysVar("tidb_executor_concurrency")
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "256") // the max value is restored.

	// Handle the case of a completely bogus value from an earlier version of TiDB.
	// This could be the case if an ENUM sysvar removes a value.

	tk.MustExec(`REPLACE INTO mysql.global_variables (variable_name, variable_value) VALUES ('tidb_enable_noop_functions', 'SOMEVAL')`)
	domain.GetDomain(tk.Se).NotifyUpdateSysVarCache() // update cache
	v, err = se.GetGlobalSysVar("tidb_enable_noop_functions")
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "OFF") // the default value is restored.
}

func (s *testSessionSuite) TestSetInstanceSysvarBySetGlobalSysVar(c *C) {
	varName := "tidb_general_log"
	defaultValue := "OFF" // This is the default value for tidb_general_log

	tk := testkit.NewTestKitWithInit(c, s.store)
	se := tk.Se.(variable.GlobalVarAccessor)

	// Get globalSysVar twice and get the same default value
	v, err := se.GetGlobalSysVar(varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, defaultValue)
	v, err = se.GetGlobalSysVar(varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, defaultValue)

	// session.GetGlobalSysVar would not get the value which session.SetGlobalSysVar writes,
	// because SetGlobalSysVar calls SetGlobalFromHook, which uses TiDBGeneralLog's SetGlobal,
	// but GetGlobalSysVar could not access TiDBGeneralLog's GetGlobal.

	// set to "1"
	err = se.SetGlobalSysVar(varName, "ON")
	c.Assert(err, IsNil)
	v, err = se.GetGlobalSysVar(varName)
	tk.MustQuery("select @@global.tidb_general_log").Check(testkit.Rows("1"))
	c.Assert(err, IsNil)
	c.Assert(v, Equals, defaultValue)

	// set back to "0"
	err = se.SetGlobalSysVar(varName, defaultValue)
	c.Assert(err, IsNil)
	v, err = se.GetGlobalSysVar(varName)
	tk.MustQuery("select @@global.tidb_general_log").Check(testkit.Rows("0"))
	c.Assert(err, IsNil)
	c.Assert(v, Equals, defaultValue)
}

func (s *testSessionSuite) TestMatchIdentity(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("CREATE USER `useridentity`@`%`")
	tk.MustExec("CREATE USER `useridentity`@`localhost`")
	tk.MustExec("CREATE USER `useridentity`@`192.168.1.1`")
	tk.MustExec("CREATE USER `useridentity`@`example.com`")

	// The MySQL matching rule is most specific to least specific.
	// So if I log in from 192.168.1.1 I should match that entry always.
	identity, err := tk.Se.MatchIdentity("useridentity", "192.168.1.1")
	c.Assert(err, IsNil)
	c.Assert(identity.Username, Equals, "useridentity")
	c.Assert(identity.Hostname, Equals, "192.168.1.1")

	// If I log in from localhost, I should match localhost
	identity, err = tk.Se.MatchIdentity("useridentity", "localhost")
	c.Assert(err, IsNil)
	c.Assert(identity.Username, Equals, "useridentity")
	c.Assert(identity.Hostname, Equals, "localhost")

	// If I log in from 192.168.1.2 I should match wildcard.
	identity, err = tk.Se.MatchIdentity("useridentity", "192.168.1.2")
	c.Assert(err, IsNil)
	c.Assert(identity.Username, Equals, "useridentity")
	c.Assert(identity.Hostname, Equals, "%")

	identity, err = tk.Se.MatchIdentity("useridentity", "127.0.0.1")
	c.Assert(err, IsNil)
	c.Assert(identity.Username, Equals, "useridentity")
	c.Assert(identity.Hostname, Equals, "localhost")

	// This uses the lookup of example.com to get an IP address.
	// We then login with that IP address, but expect it to match the example.com
	// entry in the privileges table (by reverse lookup).
	ips, err := net.LookupHost("example.com")
	c.Assert(err, IsNil)
	identity, err = tk.Se.MatchIdentity("useridentity", ips[0])
	c.Assert(err, IsNil)
	c.Assert(identity.Username, Equals, "useridentity")
	// FIXME: we *should* match example.com instead
	// as long as skip-name-resolve is not set (DEFAULT)
	c.Assert(identity.Hostname, Equals, "%")
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
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[variable:1238]Variable 'max_connections' is a GLOBAL variable")
	_, err = tk.Exec("select @@local.max_connections")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[variable:1238]Variable 'max_connections' is a GLOBAL variable")

	// Test ScopeNone
	tk.MustExec("select @@performance_schema_max_mutex_classes")
	tk.MustExec("select @@global.performance_schema_max_mutex_classes")
	// For issue 19524, test
	tk.MustExec("select @@session.performance_schema_max_mutex_classes")
	tk.MustExec("select @@local.performance_schema_max_mutex_classes")

	_, err = tk.Exec("select @@global.last_insert_id")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[variable:1238]Variable 'last_insert_id' is a SESSION variable")
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
	compiler := executor.Compiler{Ctx: tk.Se}
	stmt, _ := compiler.Compile(context.TODO(), stmtNode)
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

func (s *testSessionSuite) TestRetryGlobalTempTable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists normal_table")
	tk.MustExec("create table normal_table(a int primary key, b int)")
	defer tk.MustExec("drop table if exists normal_table")
	tk.MustExec("drop table if exists temp_table")
	tk.MustExec("create global temporary table temp_table(a int primary key, b int) on commit delete rows")
	defer tk.MustExec("drop table if exists temp_table")

	// insert select
	tk.MustExec("set tidb_disable_txn_auto_retry = 0")
	tk.MustExec("insert normal_table value(100, 100)")
	tk.MustExec("set @@autocommit = 0")
	// used to make conflicts
	tk.MustExec("update normal_table set b=b+1 where a=100")
	tk.MustExec("insert temp_table value(1, 1)")
	tk.MustExec("insert normal_table select * from temp_table")
	c.Assert(session.GetHistory(tk.Se).Count(), Equals, 3)

	// try to conflict with tk
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustExec("update normal_table set b=b+1 where a=100")

	// It will retry internally.
	tk.MustExec("commit")
	tk.MustQuery("select a, b from normal_table order by a").Check(testkit.Rows("1 1", "100 102"))
	tk.MustQuery("select a, b from temp_table order by a").Check(testkit.Rows())

	// update multi-tables
	tk.MustExec("update normal_table set b=b+1 where a=100")
	tk.MustExec("insert temp_table value(1, 2)")
	// before update: normal_table=(1 1) (100 102), temp_table=(1 2)
	tk.MustExec("update normal_table, temp_table set normal_table.b=temp_table.b where normal_table.a=temp_table.a")
	c.Assert(session.GetHistory(tk.Se).Count(), Equals, 3)

	// try to conflict with tk
	tk1.MustExec("update normal_table set b=b+1 where a=100")

	// It will retry internally.
	tk.MustExec("commit")
	tk.MustQuery("select a, b from normal_table order by a").Check(testkit.Rows("1 2", "100 104"))
}

func (s *testSessionSuite) TestRetryLocalTempTable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists normal_table")
	tk.MustExec("create table normal_table(a int primary key, b int)")
	defer tk.MustExec("drop table if exists normal_table")
	tk.MustExec("drop table if exists temp_table")
	tk.MustExec("create temporary table l_temp_table(a int primary key, b int)")
	defer tk.MustExec("drop table if exists l_temp_table")

	// insert select
	tk.MustExec("set tidb_disable_txn_auto_retry = 0")
	tk.MustExec("insert normal_table value(100, 100)")
	tk.MustExec("set @@autocommit = 0")
	// used to make conflicts
	tk.MustExec("update normal_table set b=b+1 where a=100")
	tk.MustExec("insert l_temp_table value(1, 2)")
	tk.MustExec("insert normal_table select * from l_temp_table")
	c.Assert(session.GetHistory(tk.Se).Count(), Equals, 3)

	// try to conflict with tk
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustExec("update normal_table set b=b+1 where a=100")

	// It will retry internally.
	tk.MustExec("commit")
	tk.MustQuery("select a, b from normal_table order by a").Check(testkit.Rows("1 2", "100 102"))
	tk.MustQuery("select a, b from l_temp_table order by a").Check(testkit.Rows("1 2"))

	// update multi-tables
	tk.MustExec("update normal_table set b=b+1 where a=100")
	tk.MustExec("insert l_temp_table value(3, 4)")
	// before update: normal_table=(1 1) (100 102), temp_table=(1 2)
	tk.MustExec("update normal_table, l_temp_table set normal_table.b=l_temp_table.b where normal_table.a=l_temp_table.a")
	c.Assert(session.GetHistory(tk.Se).Count(), Equals, 3)

	// try to conflict with tk
	tk1.MustExec("update normal_table set b=b+1 where a=100")

	// It will retry internally.
	tk.MustExec("commit")
	tk.MustQuery("select a, b from normal_table order by a").Check(testkit.Rows("1 2", "100 104"))
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
	tk.MustExec("set @orig_sql_mode=@@sql_mode; set @@sql_mode='';")
	tk.MustExec("execute s1 using @v2")
	tk.MustQuery("select v from t").Check(testkit.Rows("0000-00-00 00:00:00"))
	tk.MustExec("set @@sql_mode=@orig_sql_mode;")
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
	rs, err := tk.Se.ExecutePreparedStmt(ctx, id, []types.Datum{types.NewDatum("1")})
	c.Assert(err, IsNil)
	rs.Close()
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
	rs, err = tk.Se.ExecutePreparedStmt(ctx, id, []types.Datum{types.NewDatum(1)})
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
	req := rs.NewChunk(nil)
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

	var wg util.WaitGroupWrapper
	wg.Run(func() {
		for i := 0; i < 30; i++ {
			tk1.MustExec("update t set c = 1;")
		}
	})
	wg.Run(func() {
		for i := 0; i < 30; i++ {
			tk2.MustExec("update t set c = ?;", 1)
		}
	})
	wg.Run(func() {
		for i := 0; i < 30; i++ {
			tk3.MustExec("begin")
			tk3.MustExec("update t set c = 1;")
			tk3.MustExec("commit")
		}
	})
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
	rs, err := tk.Exec("select * from t")
	c.Assert(err, IsNil)
	fields := rs.Fields()
	c.Assert(fields[0].ColumnAsName.O, Equals, "a")
	c.Assert(fields[1].ColumnAsName.O, Equals, "B")
	rs.Close()

	rs, err = tk.Exec("select A, b from t")
	c.Assert(err, IsNil)
	fields = rs.Fields()
	c.Assert(fields[0].ColumnAsName.O, Equals, "A")
	c.Assert(fields[1].ColumnAsName.O, Equals, "b")
	rs.Close()

	rs, err = tk.Exec("select a as A from t where A > 0")
	c.Assert(err, IsNil)
	fields = rs.Fields()
	c.Assert(fields[0].ColumnAsName.O, Equals, "A")
	rs.Close()

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
	tk.MustExec(`set @@time_zone = 'Asia/Shanghai'`)
	ret := tk.MustQuery("select create_time from information_schema.tables where table_name='t';")
	// Make sure t1 is greater than t.
	time.Sleep(time.Second)
	tk.MustExec("alter table t modify c int default 11")
	ret1 := tk.MustQuery("select create_time from information_schema.tables where table_name='t';")
	ret2 := tk.MustQuery("show table status like 't'")
	c.Assert(ret1.Rows()[0][0].(string), Equals, ret2.Rows()[0][11].(string))
	t, err := types.ParseDatetime(nil, ret.Rows()[0][0].(string))
	c.Assert(err, IsNil)
	t1, err := types.ParseDatetime(nil, ret1.Rows()[0][0].(string))
	c.Assert(err, IsNil)
	r := t1.Compare(t)
	c.Assert(r, Equals, 1)
	// Check that time_zone changes makes the create_time different
	tk.MustExec(`set @@time_zone = 'Europe/Amsterdam'`)
	ret = tk.MustQuery(`select create_time from information_schema.tables where table_name='t'`)
	ret2 = tk.MustQuery(`show table status like 't'`)
	c.Assert(ret.Rows()[0][0].(string), Equals, ret2.Rows()[0][11].(string))
	t, err = types.ParseDatetime(nil, ret.Rows()[0][0].(string))
	c.Assert(err, IsNil)
	// Asia/Shanghai 2022-02-17 17:40:05 > Europe/Amsterdam 2022-02-17 10:40:05
	r = t1.Compare(t)
	c.Assert(r, Equals, 1)
}

type testSchemaSuiteBase struct {
	cluster testutils.Cluster
	store   kv.Storage
	dom     *domain.Domain
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
	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
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
	originalRetryTime := domain.SchemaOutOfDateRetryTimes.Load()
	originalRetryInterval := domain.SchemaOutOfDateRetryInterval.Load()
	domain.SchemaOutOfDateRetryTimes.Store(3)
	domain.SchemaOutOfDateRetryInterval.Store(20 * time.Millisecond)
	defer func() {
		domain.SchemaOutOfDateRetryTimes.Store(originalRetryTime)
		domain.SchemaOutOfDateRetryInterval.Store(originalRetryInterval)
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

	ver, err := s.store.CurrentVersion(kv.GlobalTxnScope)
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

func (s *testSchemaSerialSuite) TestValidationRecursion(c *C) {
	// We have to expect that validation functions will call GlobalVarsAccessor.GetGlobalSysVar().
	// This tests for a regression where GetGlobalSysVar() can not safely call the validation
	// function because it might cause infinite recursion.
	// See: https://github.com/pingcap/tidb/issues/30255
	sv := variable.SysVar{Scope: variable.ScopeGlobal, Name: "mynewsysvar", Value: "test", Validation: func(vars *variable.SessionVars, normalizedValue string, originalValue string, scope variable.ScopeFlag) (string, error) {
		return vars.GlobalVarsAccessor.GetGlobalSysVar("mynewsysvar")
	}}
	variable.RegisterSysVar(&sv)

	tk := testkit.NewTestKitWithInit(c, s.store)
	val, err := sv.Validate(tk.Se.GetSessionVars(), "test2", variable.ScopeGlobal)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "test")
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

	// Repeated tests for partitioned table
	tk.MustExec(`create table pt (id int, c int) partition by hash (id) partitions 3`)
	tk.MustExec(`insert into pt values(1, 1);`)
	// The schema version is out of date in the first transaction, and the SQL can't be retried.
	tk.MustExec(`begin;`)
	tk1.MustExec(`alter table pt modify column c bigint;`)
	tk.MustExec(`insert into pt values(3, 3);`)
	_, err = tk.Exec(`commit;`)
	c.Assert(terror.ErrorEqual(err, domain.ErrInfoSchemaChanged), IsTrue, Commentf("err %v", err))

	// But the transaction related table IDs aren't in the updated table IDs.
	tk.MustExec(`begin;`)
	tk1.MustExec(`alter table pt add index idx2(c);`)
	tk.MustExec(`insert into t1 values(4, 4);`)
	tk.MustExec(`commit;`)

	// Test for "select for update".
	tk.MustExec(`begin;`)
	tk1.MustExec(`alter table pt add index idx3(c);`)
	tk.MustQuery(`select * from pt for update`)
	_, err = tk.Exec(`commit;`)
	c.Assert(err, NotNil)

	// Test for "select for update".
	tk.MustExec(`begin;`)
	tk1.MustExec(`alter table pt add index idx4(c);`)
	tk.MustQuery(`select * from pt partition (p1) for update`)
	_, err = tk.Exec(`commit;`)
	c.Assert(err, NotNil)
}

func (s *testSchemaSerialSuite) TestSchemaCheckerTempTable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk1 := testkit.NewTestKitWithInit(c, s.store)

	// create table
	tk.MustExec(`drop table if exists normal_table`)
	tk.MustExec(`create table normal_table (id int, c int);`)
	defer tk.MustExec(`drop table if exists normal_table`)
	tk.MustExec(`drop table if exists temp_table`)
	tk.MustExec(`create global temporary table temp_table (id int primary key, c int) on commit delete rows;`)
	defer tk.MustExec(`drop table if exists temp_table`)

	// The schema version is out of date in the first transaction, and the SQL can't be retried.
	atomic.StoreUint32(&session.SchemaChangedWithoutRetry, 1)
	defer func() {
		atomic.StoreUint32(&session.SchemaChangedWithoutRetry, 0)
	}()

	// It's fine to change the schema of temporary tables.
	tk.MustExec(`begin;`)
	tk1.MustExec(`alter table temp_table modify column c tinyint;`)
	tk.MustExec(`insert into temp_table values(3, 3);`)
	tk.MustExec(`commit;`)

	tk.MustExec("begin pessimistic")
	tk1.MustExec(`alter table temp_table modify column c int;`)
	tk.MustQuery(`select * from temp_table for update;`).Check(testkit.Rows())
	tk.MustExec(`commit;`)

	tk.MustExec("begin pessimistic")
	tk1.MustExec(`alter table temp_table modify column c smallint;`)
	tk.MustExec(`insert into temp_table values(3, 4);`)
	tk.MustQuery(`select * from temp_table for update;`).Check(testkit.Rows("3 4"))
	tk.MustExec(`commit;`)

	tk.MustExec("begin pessimistic")
	tk1.MustExec(`alter table temp_table modify column c bigint;`)
	tk.MustQuery(`select * from temp_table where id=1 for update;`).Check(testkit.Rows())
	tk.MustExec(`commit;`)

	tk.MustExec("begin pessimistic")
	tk1.MustExec(`alter table temp_table modify column c smallint;`)
	tk.MustExec("insert into temp_table values (1, 2), (2, 3), (4, 5)")
	tk.MustQuery(`select * from temp_table where id=1 for update;`).Check(testkit.Rows("1 2"))
	tk.MustExec(`commit;`)

	tk.MustExec("begin pessimistic")
	tk1.MustExec(`alter table temp_table modify column c int;`)
	tk.MustQuery(`select * from temp_table where id=1 for update;`).Check(testkit.Rows())
	tk.MustExec(`commit;`)

	tk.MustExec("begin pessimistic")
	tk1.MustExec(`alter table temp_table modify column c bigint;`)
	tk.MustQuery(`select * from temp_table where id in (1, 2, 3) for update;`).Check(testkit.Rows())
	tk.MustExec(`commit;`)

	tk.MustExec("begin pessimistic")
	tk1.MustExec(`alter table temp_table modify column c int;`)
	tk.MustExec("insert into temp_table values (1, 2), (2, 3), (4, 5)")
	tk.MustQuery(`select * from temp_table where id in (1, 2, 3) for update;`).Check(testkit.Rows("1 2", "2 3"))
	tk.MustExec(`commit;`)

	tk.MustExec("insert into normal_table values(1, 2)")
	tk.MustExec("begin pessimistic")
	tk1.MustExec(`alter table temp_table modify column c int;`)
	tk.MustExec(`insert into temp_table values(1, 5);`)
	tk.MustQuery(`select * from temp_table, normal_table where temp_table.id = normal_table.id for update;`).Check(testkit.Rows("1 5 1 2"))
	tk.MustExec(`commit;`)

	tk.MustExec("begin pessimistic")
	tk1.MustExec(`alter table normal_table modify column c bigint;`)
	tk.MustQuery(`select * from temp_table, normal_table where temp_table.id = normal_table.id for update;`).Check(testkit.Rows())
	tk.MustExec(`commit;`)

	// Truncate will modify table ID.
	tk.MustExec(`begin;`)
	tk1.MustExec(`truncate table temp_table;`)
	tk.MustExec(`insert into temp_table values(3, 3);`)
	tk.MustExec(`commit;`)

	// It reports error when also changing the schema of a normal table.
	tk.MustExec(`begin;`)
	tk1.MustExec(`alter table normal_table modify column c bigint;`)
	tk.MustExec(`insert into temp_table values(3, 3);`)
	tk.MustExec(`insert into normal_table values(3, 3);`)
	_, err := tk.Exec(`commit;`)
	c.Assert(terror.ErrorEqual(err, domain.ErrInfoSchemaChanged), IsTrue, Commentf("err %v", err))

	tk.MustExec("begin pessimistic")
	tk1.MustExec(`alter table normal_table modify column c int;`)
	tk.MustExec(`insert into temp_table values(1, 6);`)
	tk.MustQuery(`select * from temp_table, normal_table where temp_table.id = normal_table.id for update;`).Check(testkit.Rows("1 6 1 2"))
	_, err = tk.Exec(`commit;`)
	c.Assert(terror.ErrorEqual(err, domain.ErrInfoSchemaChanged), IsTrue, Commentf("err %v", err))
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
	tk.MustExec("select * from t for update")
	tk.MustExec("update t set i = -i")
	tk.MustExec("delete from t")
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("commit")

	// TODO remove this enable after fixing table delta map.
	tk.MustExec("set tidb_enable_amend_pessimistic_txn = 1")
	tk.MustExec("begin pessimistic")
	tk1.MustExec("alter table t add k int")
	tk.MustExec("select * from t for update")
	tk.MustExec("update t set i = -i")
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
	tableStart := tablecodec.GenTableRecordPrefix(tbl.Meta().ID)
	s.cluster.SplitKeys(tableStart, tableStart.PrefixNext(), 10)

	tk.Se.GetSessionVars().SetDistSQLScanConcurrency(1)
	tk.MustExec("set tidb_init_chunk_size = 2")
	defer func() {
		tk.MustExec(fmt.Sprintf("set tidb_init_chunk_size = %d", variable.DefInitChunkSize))
	}()
	rs, err := tk.Exec("select * from chk")
	c.Assert(err, IsNil)
	req := rs.NewChunk(nil)
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

	tk.Se.GetSessionVars().SetDistSQLScanConcurrency(1)
	tk.MustExec("insert into test2(a) select a from test1;")

	rs, err := tk.Exec("select * from test2")
	c.Assert(err, IsNil)
	var idx int
	for {
		req := rs.NewChunk(nil)
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

	tk.Se.GetSessionVars().SetDistSQLScanConcurrency(1)
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("update chk set a = a + 100 where a = %d", i))
	}

	rs, err := tk.Exec("select * from chk")
	c.Assert(err, IsNil)
	var idx int
	for {
		req := rs.NewChunk(nil)
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

	tk.Se.GetSessionVars().SetDistSQLScanConcurrency(1)

	for i := 0; i < 99; i++ {
		tk.MustExec(fmt.Sprintf("delete from chk where a = %d", i))
	}

	rs, err := tk.Exec("select * from chk")
	c.Assert(err, IsNil)

	req := rs.NewChunk(nil)
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

	tk.Se.GetSessionVars().SetDistSQLScanConcurrency(1)

	tk.MustExec("delete chk1, chk2 from chk1 inner join chk2 where chk1.a = chk2.a")

	rs, err := tk.Exec("select * from chk1")
	c.Assert(err, IsNil)

	var idx int
	for {
		req := rs.NewChunk(nil)
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

	req := rs.NewChunk(nil)
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
	indexStart := tablecodec.EncodeTableIndexPrefix(tbl.Meta().ID, tbl.Indices()[0].Meta().ID)
	s.cluster.SplitKeys(indexStart, indexStart.PrefixNext(), 10)

	tk.Se.GetSessionVars().IndexLookupSize = 10
	rs, err := tk.Exec("select * from chk order by k")
	c.Assert(err, IsNil)
	req := rs.NewChunk(nil)
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
	req = rs.NewChunk(nil)
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
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.StmtCountLimit = 3
	})
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
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.StmtCountLimit = 3
	})
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

func (s *testSessionSuite2) TestHostLengthMax(c *C) {
	host1 := strings.Repeat("a", 65)
	host2 := strings.Repeat("a", 256)

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec(fmt.Sprintf(`CREATE USER 'abcddfjakldfjaldddds'@'%s'`, host1))

	err := tk.ExecToErr(fmt.Sprintf(`CREATE USER 'abcddfjakldfjaldddds'@'%s'`, host2))
	c.Assert(err.Error(), Equals, "[types:1406]Data too long for column 'Host' at row 1")
}

func (s *testSessionSerialSuite) TestKVVars(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("set @@tidb_backoff_lock_fast = 1")
	tk.MustExec("set @@tidb_backoff_weight = 100")
	tk.MustExec("create table if not exists kvvars (a int key)")
	tk.MustExec("insert into kvvars values (1)")
	tk.MustExec("begin")
	txn, err := tk.Se.Txn(false)
	c.Assert(err, IsNil)
	vars := txn.GetVars().(*tikv.Variables)
	c.Assert(vars.BackoffLockFast, Equals, 1)
	c.Assert(vars.BackOffWeight, Equals, 100)
	tk.MustExec("rollback")
	tk.MustExec("set @@tidb_backoff_weight = 50")
	tk.MustExec("set @@autocommit = 0")
	tk.MustExec("select * from kvvars")
	c.Assert(tk.Se.GetSessionVars().InTxn(), IsTrue)
	txn, err = tk.Se.Txn(false)
	c.Assert(err, IsNil)
	vars = txn.GetVars().(*tikv.Variables)
	c.Assert(vars.BackOffWeight, Equals, 50)

	tk.MustExec("set @@autocommit = 1")
	c.Assert(failpoint.Enable("tikvclient/probeSetVars", `return(true)`), IsNil)
	tk.MustExec("select * from kvvars where a = 1")
	c.Assert(failpoint.Disable("tikvclient/probeSetVars"), IsNil)
	c.Assert(transaction.SetSuccess, IsTrue)
	transaction.SetSuccess = false
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
	tk.MustQuery("show variables like 'tidb_enable_table_partition'").Check(testkit.Rows("tidb_enable_table_partition OFF"))

	tk.MustExec("set global tidb_enable_table_partition = on")

	tk.MustQuery("show variables like 'tidb_enable_table_partition'").Check(testkit.Rows("tidb_enable_table_partition OFF"))
	tk.MustQuery("show global variables like 'tidb_enable_table_partition'").Check(testkit.Rows("tidb_enable_table_partition ON"))

	tk.MustExec("set tidb_enable_list_partition=off")
	tk.MustQuery("show variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition OFF"))
	tk.MustExec("set global tidb_enable_list_partition=on")
	tk.MustQuery("show global variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition ON"))
	tk.MustQuery("show variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition OFF"))

	tk.MustExec("set tidb_enable_list_partition=1")
	tk.MustQuery("show variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition ON"))

	tk.MustExec("set tidb_enable_list_partition=on")
	tk.MustQuery("show variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition ON"))

	tk.MustQuery("show global variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition ON"))
	tk.MustExec("set global tidb_enable_list_partition=off")
	tk.MustQuery("show global variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition OFF"))
	tk.MustQuery("show variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition ON"))
	tk.MustExec("set tidb_enable_list_partition=off")
	tk.MustQuery("show variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition OFF"))

	tk.MustExec("set global tidb_enable_list_partition=on")
	tk.MustQuery("show global variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition ON"))
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustQuery("show variables like 'tidb_enable_table_partition'").Check(testkit.Rows("tidb_enable_table_partition ON"))
	tk1.MustQuery("show variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition ON"))
}

func (s *testSessionSerialSuite) TestTxnRetryErrMsg(c *C) {
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustExec("create table no_retry (id int)")
	tk1.MustExec("insert into no_retry values (1)")
	tk1.MustExec("begin")
	tk2.MustExec("update no_retry set id = id + 1")
	tk1.MustExec("update no_retry set id = id + 1")
	c.Assert(failpoint.Enable("tikvclient/mockRetryableErrorResp", `return(true)`), IsNil)
	_, err := tk1.Se.Execute(context.Background(), "commit")
	failpoint.Disable("tikvclient/mockRetryableErrorResp")
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
	err = tk1.Se.PrepareTxnCtx(context.Background())
	c.Assert(err, IsNil)
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
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TxnLocalLatches.Enabled = false
	})
	tk1.MustExec("begin")
	tk1.MustExec("update no_retry set id = 9")

	tk2.MustExec("update no_retry set id = 8")

	_, err = tk1.Se.Execute(context.Background(), "commit")
	c.Assert(err, NotNil)
	c.Assert(kv.ErrWriteConflict.Equal(err), IsTrue, Commentf("error: %s", err))
	c.Assert(strings.Contains(err.Error(), kv.TxnRetryableMark), IsTrue, Commentf("error: %s", err))
	tk1.MustExec("rollback")

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TxnLocalLatches.Enabled = true
	})
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

	tk1 := testkit.NewTestKitWithInit(c, s.store)
	c.Assert(tk1.Se.Auth(&auth.UserIdentity{Username: "xxx", Hostname: "localhost"},
		[]byte(""),
		[]byte("")), IsTrue)

	_, err := tk1.Exec("update t2 set id = 666 where id = 1;")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "privilege check"), IsTrue)

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
	var err error
	tk := testkit.NewTestKit(c, s.store)
	tk.Se, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("create table MaxExecTime( id int,name varchar(128),age int);")
	tk.MustExec("begin")
	tk.MustExec("insert into MaxExecTime (id,name,age) values (1,'john',18),(2,'lary',19),(3,'lily',18);")

	tk.MustQuery("select /*+ MAX_EXECUTION_TIME(1000) MAX_EXECUTION_TIME(500) */ * FROM MaxExecTime;")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings()[0].Err.Error(), Equals, "MAX_EXECUTION_TIME() is defined more than once, only the last definition takes effect: MAX_EXECUTION_TIME(500)")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.HasMaxExecutionTime, Equals, true)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.MaxExecutionTime, Equals, uint64(500))

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

	tkRoot.MustExec(`grant show view, select on v_version29 to 'u_version29'@'%'`)
	tkRoot.MustQuery("select table_priv from mysql.tables_priv where host='%' and db='test' and user='u_version29' and table_name='v_version29'").Check(testkit.Rows("Select,Show View"))

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
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings()[0].Err.Error(), Equals, "Setting the MEMORY_QUOTA to 0 means no memory limit")

	tk.MustExec("use test")
	tk.MustExec("create table t1(a int);")
	tk.MustExec("insert /*+ MEMORY_QUOTA(1 MB) */ into t1 (a) values (1);")
	val = int64(1) * 1024 * 1024
	c.Assert(tk.Se.GetSessionVars().StmtCtx.MemTracker.CheckBytesLimit(val), IsTrue)

	tk.MustExec("insert /*+ MEMORY_QUOTA(1 MB) */  into t1 select /*+ MEMORY_QUOTA(3 MB) */ * from t1;")
	val = int64(1) * 1024 * 1024
	c.Assert(tk.Se.GetSessionVars().StmtCtx.MemTracker.CheckBytesLimit(val), IsTrue)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings()[0].Err.Error(), Equals, "[util:3126]Hint MEMORY_QUOTA(`3145728`) is ignored as conflicting/duplicated.")

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
		ch <- 0
	}()

	// Leave 50ms for tk1 to run, tk1 should be blocked at the update operation.
	time.Sleep(50 * time.Millisecond)
	ch <- 1

	tk.MustExec("commit")
	// tk1 should be blocked until tk commit, check the order.
	c.Assert(<-ch, Equals, int32(1))
	c.Assert(<-ch, Equals, int32(0))
	<-ch // wait for goroutine to quit.

	// Once again...
	// This time, test for the update-update conflict.
	tk.MustExec("begin pessimistic")
	tk.MustExec("update forupdate_on_partition set first_name='sw' where age=25")
	tk1.MustExec("begin pessimistic")

	go func() {
		tk1.MustExec("update forupdate_on_partition set first_name = 'xxx' where age=25")
		ch <- 0
		tk1.MustExec("commit")
		ch <- 0
	}()

	// Leave 50ms for tk1 to run, tk1 should be blocked at the update operation.
	time.Sleep(50 * time.Millisecond)
	ch <- 1

	tk.MustExec("commit")
	// tk1 should be blocked until tk commit, check the order.
	c.Assert(<-ch, Equals, int32(1))
	c.Assert(<-ch, Equals, int32(0))
	<-ch // wait for goroutine to quit.
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

func (s *testSessionSuite2) TestPerStmtTaskID(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table task_id (v int)")

	tk.MustExec("begin")
	tk.MustExec("select * from task_id where v > 10")
	taskID1 := tk.Se.GetSessionVars().StmtCtx.TaskID
	tk.MustExec("select * from task_id where v < 5")
	taskID2 := tk.Se.GetSessionVars().StmtCtx.TaskID
	tk.MustExec("commit")

	c.Assert(taskID1 != taskID2, IsTrue)
}

func (s *testSessionSerialSuite) TestSetTxnScope(c *C) {
	// Check the default value of @@tidb_enable_local_txn and @@txn_scope whitout configuring the zone label.
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustQuery("select @@global.tidb_enable_local_txn;").Check(testkit.Rows("0"))
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	c.Assert(tk.Se.GetSessionVars().CheckAndGetTxnScope(), Equals, kv.GlobalTxnScope)
	// Check the default value of @@tidb_enable_local_txn and @@txn_scope with configuring the zone label.
	failpoint.Enable("tikvclient/injectTxnScope", `return("bj")`)
	tk = testkit.NewTestKitWithInit(c, s.store)
	tk.MustQuery("select @@global.tidb_enable_local_txn;").Check(testkit.Rows("0"))
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	c.Assert(tk.Se.GetSessionVars().CheckAndGetTxnScope(), Equals, kv.GlobalTxnScope)
	failpoint.Disable("tikvclient/injectTxnScope")

	// @@tidb_enable_local_txn is off without configuring the zone label.
	tk = testkit.NewTestKitWithInit(c, s.store)
	tk.MustQuery("select @@global.tidb_enable_local_txn;").Check(testkit.Rows("0"))
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	c.Assert(tk.Se.GetSessionVars().CheckAndGetTxnScope(), Equals, kv.GlobalTxnScope)
	// Set @@txn_scope to local.
	err := tk.ExecToErr("set @@txn_scope = 'local';")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, `.*txn_scope can not be set to local when tidb_enable_local_txn is off.*`)
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	c.Assert(tk.Se.GetSessionVars().CheckAndGetTxnScope(), Equals, kv.GlobalTxnScope)
	// Set @@txn_scope to global.
	tk.MustExec("set @@txn_scope = 'global';")
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	c.Assert(tk.Se.GetSessionVars().CheckAndGetTxnScope(), Equals, kv.GlobalTxnScope)

	// @@tidb_enable_local_txn is off with configuring the zone label.
	failpoint.Enable("tikvclient/injectTxnScope", `return("bj")`)
	tk = testkit.NewTestKitWithInit(c, s.store)
	tk.MustQuery("select @@global.tidb_enable_local_txn;").Check(testkit.Rows("0"))
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	c.Assert(tk.Se.GetSessionVars().CheckAndGetTxnScope(), Equals, kv.GlobalTxnScope)
	// Set @@txn_scope to local.
	err = tk.ExecToErr("set @@txn_scope = 'local';")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, `.*txn_scope can not be set to local when tidb_enable_local_txn is off.*`)
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	c.Assert(tk.Se.GetSessionVars().CheckAndGetTxnScope(), Equals, kv.GlobalTxnScope)
	// Set @@txn_scope to global.
	tk.MustExec("set @@txn_scope = 'global';")
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	c.Assert(tk.Se.GetSessionVars().CheckAndGetTxnScope(), Equals, kv.GlobalTxnScope)
	failpoint.Disable("tikvclient/injectTxnScope")

	// @@tidb_enable_local_txn is on without configuring the zone label.
	tk = testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("set global tidb_enable_local_txn = on;")
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	c.Assert(tk.Se.GetSessionVars().CheckAndGetTxnScope(), Equals, kv.GlobalTxnScope)
	// Set @@txn_scope to local.
	err = tk.ExecToErr("set @@txn_scope = 'local';")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, `.*txn_scope can not be set to local when zone label is empty or "global".*`)
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	c.Assert(tk.Se.GetSessionVars().CheckAndGetTxnScope(), Equals, kv.GlobalTxnScope)
	// Set @@txn_scope to global.
	tk.MustExec("set @@txn_scope = 'global';")
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	c.Assert(tk.Se.GetSessionVars().CheckAndGetTxnScope(), Equals, kv.GlobalTxnScope)

	// @@tidb_enable_local_txn is on with configuring the zone label.
	failpoint.Enable("tikvclient/injectTxnScope", `return("bj")`)
	tk = testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("set global tidb_enable_local_txn = on;")
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.LocalTxnScope))
	c.Assert(tk.Se.GetSessionVars().CheckAndGetTxnScope(), Equals, "bj")
	// Set @@txn_scope to global.
	tk.MustExec("set @@txn_scope = 'global';")
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	c.Assert(tk.Se.GetSessionVars().CheckAndGetTxnScope(), Equals, kv.GlobalTxnScope)
	// Set @@txn_scope to local.
	tk.MustExec("set @@txn_scope = 'local';")
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.LocalTxnScope))
	c.Assert(tk.Se.GetSessionVars().CheckAndGetTxnScope(), Equals, "bj")
	// Try to set @@txn_scope to an invalid value.
	err = tk.ExecToErr("set @@txn_scope='foo'")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, `.*txn_scope value should be global or local.*`)
	failpoint.Disable("tikvclient/injectTxnScope")
}

func (s *testSessionSerialSuite) TestGlobalAndLocalTxn(c *C) {
	// Because the PD config of check_dev_2 test is not compatible with local/global txn yet,
	// so we will skip this test for now.
	if *withTiKV {
		return
	}
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("set global tidb_enable_local_txn = on;")
	tk.MustExec("drop table if exists t1;")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec(`create table t1 (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (100),
	PARTITION p1 VALUES LESS THAN (200)
);`)
	// Config the Placement Rules
	is := s.dom.InfoSchema()
	tb, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	setBundle := func(parName, dc string) {
		pid, err := tables.FindPartitionByName(tb.Meta(), parName)
		c.Assert(err, IsNil)
		groupID := placement.GroupID(pid)
		is.SetBundle(&placement.Bundle{
			ID: groupID,
			Rules: []*placement.Rule{
				{
					GroupID: groupID,
					Role:    placement.Leader,
					Count:   1,
					Constraints: []placement.Constraint{
						{
							Key:    placement.DCLabelKey,
							Op:     placement.In,
							Values: []string{dc},
						},
						{
							Key:    placement.EngineLabelKey,
							Op:     placement.NotIn,
							Values: []string{placement.EngineLabelTiFlash},
						},
					},
				},
			},
		})
	}
	setBundle("p0", "dc-1")
	setBundle("p1", "dc-2")

	// set txn_scope to global
	tk.MustExec(fmt.Sprintf("set @@session.txn_scope = '%s';", kv.GlobalTxnScope))
	result := tk.MustQuery("select @@txn_scope;")
	result.Check(testkit.Rows(kv.GlobalTxnScope))

	// test global txn auto commit
	tk.MustExec("insert into t1 (c) values (1)") // write dc-1 with global scope
	result = tk.MustQuery("select * from t1")    // read dc-1 and dc-2 with global scope
	c.Assert(len(result.Rows()), Equals, 1)

	// begin and commit with global txn scope
	tk.MustExec("begin")
	txn, err := tk.Se.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(tk.Se.GetSessionVars().TxnCtx.TxnScope, Equals, kv.GlobalTxnScope)
	c.Assert(txn.Valid(), IsTrue)
	tk.MustExec("insert into t1 (c) values (1)") // write dc-1 with global scope
	result = tk.MustQuery("select * from t1")    // read dc-1 and dc-2 with global scope
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(txn.Valid(), IsTrue)
	tk.MustExec("commit")
	result = tk.MustQuery("select * from t1")
	c.Assert(len(result.Rows()), Equals, 2)

	// begin and rollback with global txn scope
	tk.MustExec("begin")
	txn, err = tk.Se.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(tk.Se.GetSessionVars().TxnCtx.TxnScope, Equals, kv.GlobalTxnScope)
	c.Assert(txn.Valid(), IsTrue)
	tk.MustExec("insert into t1 (c) values (101)") // write dc-2 with global scope
	result = tk.MustQuery("select * from t1")      // read dc-1 and dc-2 with global scope
	c.Assert(len(result.Rows()), Equals, 3)
	c.Assert(txn.Valid(), IsTrue)
	tk.MustExec("rollback")
	result = tk.MustQuery("select * from t1")
	c.Assert(len(result.Rows()), Equals, 2)

	timeBeforeWriting := time.Now()
	tk.MustExec("insert into t1 (c) values (101)") // write dc-2 with global scope
	result = tk.MustQuery("select * from t1")      // read dc-1 and dc-2 with global scope
	c.Assert(len(result.Rows()), Equals, 3)

	failpoint.Enable("tikvclient/injectTxnScope", `return("dc-1")`)
	defer failpoint.Disable("tikvclient/injectTxnScope")
	// set txn_scope to local
	tk.MustExec("set @@session.txn_scope = 'local';")
	result = tk.MustQuery("select @@txn_scope;")
	result.Check(testkit.Rows("local"))

	// test local txn auto commit
	tk.MustExec("insert into t1 (c) values (1)")          // write dc-1 with dc-1 scope
	result = tk.MustQuery("select * from t1 where c = 1") // point get dc-1 with dc-1 scope
	c.Assert(len(result.Rows()), Equals, 3)
	result = tk.MustQuery("select * from t1 where c < 100") // read dc-1 with dc-1 scope
	c.Assert(len(result.Rows()), Equals, 3)

	// begin and commit with dc-1 txn scope
	tk.MustExec("begin")
	txn, err = tk.Se.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(tk.Se.GetSessionVars().CheckAndGetTxnScope(), Equals, "dc-1")
	c.Assert(txn.Valid(), IsTrue)
	tk.MustExec("insert into t1 (c) values (1)")            // write dc-1 with dc-1 scope
	result = tk.MustQuery("select * from t1 where c < 100") // read dc-1 with dc-1 scope
	c.Assert(len(result.Rows()), Equals, 4)
	c.Assert(txn.Valid(), IsTrue)
	tk.MustExec("commit")
	result = tk.MustQuery("select * from t1 where c < 100")
	c.Assert(len(result.Rows()), Equals, 4)

	// begin and rollback with dc-1 txn scope
	tk.MustExec("begin")
	txn, err = tk.Se.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(tk.Se.GetSessionVars().CheckAndGetTxnScope(), Equals, "dc-1")
	c.Assert(txn.Valid(), IsTrue)
	tk.MustExec("insert into t1 (c) values (1)")            // write dc-1 with dc-1 scope
	result = tk.MustQuery("select * from t1 where c < 100") // read dc-1 with dc-1 scope
	c.Assert(len(result.Rows()), Equals, 5)
	c.Assert(txn.Valid(), IsTrue)
	tk.MustExec("rollback")
	result = tk.MustQuery("select * from t1 where c < 100")
	c.Assert(len(result.Rows()), Equals, 4)

	// test wrong scope local txn auto commit
	_, err = tk.Exec("insert into t1 (c) values (101)") // write dc-2 with dc-1 scope
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, ".*out of txn_scope.*")
	err = tk.ExecToErr("select * from t1 where c = 101") // point get dc-2 with dc-1 scope
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, ".*can not be read by.*")
	err = tk.ExecToErr("select * from t1 where c > 100") // read dc-2 with dc-1 scope
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, ".*can not be read by.*")
	tk.MustExec("begin")
	err = tk.ExecToErr("select * from t1 where c = 101") // point get dc-2 with dc-1 scope
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, ".*can not be read by.*")
	err = tk.ExecToErr("select * from t1 where c > 100") // read dc-2 with dc-1 scope
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, ".*can not be read by.*")
	tk.MustExec("commit")

	// begin and commit reading & writing the data in dc-2 with dc-1 txn scope
	tk.MustExec("begin")
	txn, err = tk.Se.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(tk.Se.GetSessionVars().CheckAndGetTxnScope(), Equals, "dc-1")
	c.Assert(txn.Valid(), IsTrue)
	tk.MustExec("insert into t1 (c) values (101)")       // write dc-2 with dc-1 scope
	err = tk.ExecToErr("select * from t1 where c > 100") // read dc-2 with dc-1 scope
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, ".*can not be read by.*")
	tk.MustExec("insert into t1 (c) values (99)")           // write dc-1 with dc-1 scope
	result = tk.MustQuery("select * from t1 where c < 100") // read dc-1 with dc-1 scope
	c.Assert(len(result.Rows()), Equals, 5)
	c.Assert(txn.Valid(), IsTrue)
	_, err = tk.Exec("commit")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, ".*out of txn_scope.*")
	// Won't read the value 99 because the previous commit failed
	result = tk.MustQuery("select * from t1 where c < 100") // read dc-1 with dc-1 scope
	c.Assert(len(result.Rows()), Equals, 4)

	// Stale Read will ignore the cross-dc txn scope.
	c.Assert(tk.Se.GetSessionVars().CheckAndGetTxnScope(), Equals, "dc-1")
	result = tk.MustQuery("select @@txn_scope;")
	result.Check(testkit.Rows("local"))
	err = tk.ExecToErr("select * from t1 where c > 100") // read dc-2 with dc-1 scope
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, ".*can not be read by.*")
	// Read dc-2 with Stale Read (in dc-1 scope)
	timestamp := timeBeforeWriting.Format(time.RFC3339Nano)
	// TODO: check the result of Stale Read when we figure out how to make the time precision more accurate.
	tk.MustExec(fmt.Sprintf("select * from t1 AS OF TIMESTAMP '%s' where c = 101", timestamp))
	tk.MustExec(fmt.Sprintf("select * from t1 AS OF TIMESTAMP '%s' where c > 100", timestamp))
	tk.MustExec(fmt.Sprintf("START TRANSACTION READ ONLY AS OF TIMESTAMP '%s'", timestamp))
	tk.MustExec("select * from t1 where c = 101")
	tk.MustExec("select * from t1 where c > 100")
	tk.MustExec("commit")
	tk.MustExec("set @@tidb_replica_read='closest-replicas'")
	tk.MustExec(fmt.Sprintf("select * from t1 AS OF TIMESTAMP '%s' where c > 100", timestamp))
	tk.MustExec(fmt.Sprintf("START TRANSACTION READ ONLY AS OF TIMESTAMP '%s'", timestamp))
	tk.MustExec("select * from t1 where c = 101")
	tk.MustExec("select * from t1 where c > 100")
	tk.MustExec("commit")

	tk.MustExec("set global tidb_enable_local_txn = off;")
}

func (s *testSessionSuite2) TestSetEnableRateLimitAction(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	// assert default value
	result := tk.MustQuery("select @@tidb_enable_rate_limit_action;")
	result.Check(testkit.Rows("1"))

	// assert set sys variable
	tk.MustExec("set global tidb_enable_rate_limit_action= '0';")
	tk.Se.Close()

	se, err := session.CreateSession4Test(s.store)
	c.Check(err, IsNil)
	tk.Se = se
	result = tk.MustQuery("select @@tidb_enable_rate_limit_action;")
	result.Check(testkit.Rows("0"))
}

func (s *testSessionSuite3) TestSetVarHint(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.Se.GetSessionVars().SetSystemVar("sql_mode", mysql.DefaultSQLMode)
	tk.MustQuery("SELECT /*+ SET_VAR(sql_mode=ALLOW_INVALID_DATES) */ @@sql_mode;").Check(testkit.Rows("ALLOW_INVALID_DATES"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@sql_mode;").Check(testkit.Rows(mysql.DefaultSQLMode))

	tk.Se.GetSessionVars().SetSystemVar("tmp_table_size", "16777216")
	tk.MustQuery("SELECT /*+ SET_VAR(tmp_table_size=1024) */ @@tmp_table_size;").Check(testkit.Rows("1024"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@tmp_table_size;").Check(testkit.Rows("16777216"))

	tk.Se.GetSessionVars().SetSystemVar("range_alloc_block_size", "4096")
	tk.MustQuery("SELECT /*+ SET_VAR(range_alloc_block_size=4294967295) */ @@range_alloc_block_size;").Check(testkit.Rows("4294967295"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@range_alloc_block_size;").Check(testkit.Rows("4096"))

	tk.Se.GetSessionVars().SetSystemVar("max_execution_time", "0")
	tk.MustQuery("SELECT /*+ SET_VAR(max_execution_time=1) */ @@max_execution_time;").Check(testkit.Rows("1"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@max_execution_time;").Check(testkit.Rows("0"))

	tk.Se.GetSessionVars().SetSystemVar("time_zone", "SYSTEM")
	tk.MustQuery("SELECT /*+ SET_VAR(time_zone='+12:00') */ @@time_zone;").Check(testkit.Rows("+12:00"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@time_zone;").Check(testkit.Rows("SYSTEM"))

	tk.Se.GetSessionVars().SetSystemVar("join_buffer_size", "262144")
	tk.MustQuery("SELECT /*+ SET_VAR(join_buffer_size=128) */ @@join_buffer_size;").Check(testkit.Rows("128"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@join_buffer_size;").Check(testkit.Rows("262144"))

	tk.Se.GetSessionVars().SetSystemVar("max_length_for_sort_data", "1024")
	tk.MustQuery("SELECT /*+ SET_VAR(max_length_for_sort_data=4) */ @@max_length_for_sort_data;").Check(testkit.Rows("4"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@max_length_for_sort_data;").Check(testkit.Rows("1024"))

	tk.Se.GetSessionVars().SetSystemVar("max_error_count", "64")
	tk.MustQuery("SELECT /*+ SET_VAR(max_error_count=0) */ @@max_error_count;").Check(testkit.Rows("0"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@max_error_count;").Check(testkit.Rows("64"))

	tk.Se.GetSessionVars().SetSystemVar("sql_buffer_result", "OFF")
	tk.MustQuery("SELECT /*+ SET_VAR(sql_buffer_result=ON) */ @@sql_buffer_result;").Check(testkit.Rows("ON"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@sql_buffer_result;").Check(testkit.Rows("OFF"))

	tk.Se.GetSessionVars().SetSystemVar("max_heap_table_size", "16777216")
	tk.MustQuery("SELECT /*+ SET_VAR(max_heap_table_size=16384) */ @@max_heap_table_size;").Check(testkit.Rows("16384"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@max_heap_table_size;").Check(testkit.Rows("16777216"))

	tk.Se.GetSessionVars().SetSystemVar("tmp_table_size", "16777216")
	tk.MustQuery("SELECT /*+ SET_VAR(tmp_table_size=16384) */ @@tmp_table_size;").Check(testkit.Rows("16384"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@tmp_table_size;").Check(testkit.Rows("16777216"))

	tk.Se.GetSessionVars().SetSystemVar("div_precision_increment", "4")
	tk.MustQuery("SELECT /*+ SET_VAR(div_precision_increment=0) */ @@div_precision_increment;").Check(testkit.Rows("0"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@div_precision_increment;").Check(testkit.Rows("4"))

	tk.Se.GetSessionVars().SetSystemVar("sql_auto_is_null", "OFF")
	tk.Se.GetSessionVars().SetSystemVar("tidb_enable_noop_functions", "ON")
	tk.MustQuery("SELECT /*+ SET_VAR(sql_auto_is_null=1) */ @@sql_auto_is_null;").Check(testkit.Rows("1"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.Se.GetSessionVars().SetSystemVar("tidb_enable_noop_functions", "OFF")
	tk.MustQuery("SELECT @@sql_auto_is_null;").Check(testkit.Rows("0"))

	tk.Se.GetSessionVars().SetSystemVar("sort_buffer_size", "262144")
	tk.MustQuery("SELECT /*+ SET_VAR(sort_buffer_size=32768) */ @@sort_buffer_size;").Check(testkit.Rows("32768"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@sort_buffer_size;").Check(testkit.Rows("262144"))

	tk.Se.GetSessionVars().SetSystemVar("max_join_size", "18446744073709551615")
	tk.MustQuery("SELECT /*+ SET_VAR(max_join_size=1) */ @@max_join_size;").Check(testkit.Rows("1"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@max_join_size;").Check(testkit.Rows("18446744073709551615"))

	tk.Se.GetSessionVars().SetSystemVar("max_seeks_for_key", "18446744073709551615")
	tk.MustQuery("SELECT /*+ SET_VAR(max_seeks_for_key=1) */ @@max_seeks_for_key;").Check(testkit.Rows("1"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@max_seeks_for_key;").Check(testkit.Rows("18446744073709551615"))

	tk.Se.GetSessionVars().SetSystemVar("max_sort_length", "1024")
	tk.MustQuery("SELECT /*+ SET_VAR(max_sort_length=4) */ @@max_sort_length;").Check(testkit.Rows("4"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@max_sort_length;").Check(testkit.Rows("1024"))

	tk.Se.GetSessionVars().SetSystemVar("bulk_insert_buffer_size", "8388608")
	tk.MustQuery("SELECT /*+ SET_VAR(bulk_insert_buffer_size=0) */ @@bulk_insert_buffer_size;").Check(testkit.Rows("0"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@bulk_insert_buffer_size;").Check(testkit.Rows("8388608"))

	tk.Se.GetSessionVars().SetSystemVar("sql_big_selects", "1")
	tk.MustQuery("SELECT /*+ SET_VAR(sql_big_selects=0) */ @@sql_big_selects;").Check(testkit.Rows("0"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@sql_big_selects;").Check(testkit.Rows("1"))

	tk.Se.GetSessionVars().SetSystemVar("read_rnd_buffer_size", "262144")
	tk.MustQuery("SELECT /*+ SET_VAR(read_rnd_buffer_size=1) */ @@read_rnd_buffer_size;").Check(testkit.Rows("1"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@read_rnd_buffer_size;").Check(testkit.Rows("262144"))

	tk.Se.GetSessionVars().SetSystemVar("unique_checks", "1")
	tk.MustQuery("SELECT /*+ SET_VAR(unique_checks=0) */ @@unique_checks;").Check(testkit.Rows("0"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@unique_checks;").Check(testkit.Rows("1"))

	tk.Se.GetSessionVars().SetSystemVar("read_buffer_size", "131072")
	tk.MustQuery("SELECT /*+ SET_VAR(read_buffer_size=8192) */ @@read_buffer_size;").Check(testkit.Rows("8192"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@read_buffer_size;").Check(testkit.Rows("131072"))

	tk.Se.GetSessionVars().SetSystemVar("default_tmp_storage_engine", "InnoDB")
	tk.MustQuery("SELECT /*+ SET_VAR(default_tmp_storage_engine='CSV') */ @@default_tmp_storage_engine;").Check(testkit.Rows("CSV"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@default_tmp_storage_engine;").Check(testkit.Rows("InnoDB"))

	tk.Se.GetSessionVars().SetSystemVar("optimizer_search_depth", "62")
	tk.MustQuery("SELECT /*+ SET_VAR(optimizer_search_depth=1) */ @@optimizer_search_depth;").Check(testkit.Rows("1"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@optimizer_search_depth;").Check(testkit.Rows("62"))

	tk.Se.GetSessionVars().SetSystemVar("max_points_in_geometry", "65536")
	tk.MustQuery("SELECT /*+ SET_VAR(max_points_in_geometry=3) */ @@max_points_in_geometry;").Check(testkit.Rows("3"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@max_points_in_geometry;").Check(testkit.Rows("65536"))

	tk.Se.GetSessionVars().SetSystemVar("updatable_views_with_limit", "YES")
	tk.MustQuery("SELECT /*+ SET_VAR(updatable_views_with_limit=0) */ @@updatable_views_with_limit;").Check(testkit.Rows("0"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@updatable_views_with_limit;").Check(testkit.Rows("YES"))

	tk.Se.GetSessionVars().SetSystemVar("optimizer_prune_level", "1")
	tk.MustQuery("SELECT /*+ SET_VAR(optimizer_prune_level=0) */ @@optimizer_prune_level;").Check(testkit.Rows("0"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@optimizer_prune_level;").Check(testkit.Rows("1"))

	tk.Se.GetSessionVars().SetSystemVar("group_concat_max_len", "1024")
	tk.MustQuery("SELECT /*+ SET_VAR(group_concat_max_len=4) */ @@group_concat_max_len;").Check(testkit.Rows("4"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@group_concat_max_len;").Check(testkit.Rows("1024"))

	tk.Se.GetSessionVars().SetSystemVar("eq_range_index_dive_limit", "200")
	tk.MustQuery("SELECT /*+ SET_VAR(eq_range_index_dive_limit=0) */ @@eq_range_index_dive_limit;").Check(testkit.Rows("0"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@eq_range_index_dive_limit;").Check(testkit.Rows("200"))

	tk.Se.GetSessionVars().SetSystemVar("sql_safe_updates", "0")
	tk.MustQuery("SELECT /*+ SET_VAR(sql_safe_updates=1) */ @@sql_safe_updates;").Check(testkit.Rows("1"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@sql_safe_updates;").Check(testkit.Rows("0"))

	tk.Se.GetSessionVars().SetSystemVar("end_markers_in_json", "0")
	tk.MustQuery("SELECT /*+ SET_VAR(end_markers_in_json=1) */ @@end_markers_in_json;").Check(testkit.Rows("1"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@end_markers_in_json;").Check(testkit.Rows("0"))

	tk.Se.GetSessionVars().SetSystemVar("windowing_use_high_precision", "ON")
	tk.MustQuery("SELECT /*+ SET_VAR(windowing_use_high_precision=OFF) */ @@windowing_use_high_precision;").Check(testkit.Rows("0"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
	tk.MustQuery("SELECT @@windowing_use_high_precision;").Check(testkit.Rows("1"))

	tk.MustExec("SELECT /*+ SET_VAR(sql_safe_updates = 1) SET_VAR(max_heap_table_size = 1G) */ 1;")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)

	tk.MustExec("SELECT /*+ SET_VAR(collation_server = 'utf8') */ 1;")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings()[0].Err.Error(), Equals, "[planner:3637]Variable 'collation_server' cannot be set using SET_VAR hint.")

	tk.MustExec("SELECT /*+ SET_VAR(max_size = 1G) */ 1;")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings()[0].Err.Error(), Equals, "[planner:3128]Unresolved name 'max_size' for SET_VAR hint")

	tk.MustExec("SELECT /*+ SET_VAR(group_concat_max_len = 1024) SET_VAR(group_concat_max_len = 2048) */ 1;")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings()[0].Err.Error(), Equals, "[planner:3126]Hint SET_VAR(group_concat_max_len=2048) is ignored as conflicting/duplicated.")
}

func (s *testSessionSerialSuite) TestDoDDLJobQuit(c *C) {
	// test https://github.com/pingcap/tidb/issues/18714, imitate DM's use environment
	// use isolated store, because in below failpoint we will cancel its context
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.MockTiKV))
	c.Assert(err, IsNil)
	defer store.Close()
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer dom.Close()
	se, err := session.CreateSession(store)
	c.Assert(err, IsNil)
	defer se.Close()

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/storeCloseInLoop", `return`), IsNil)
	defer failpoint.Disable("github.com/pingcap/tidb/ddl/storeCloseInLoop")

	// this DDL call will enter deadloop before this fix
	err = dom.DDL().CreateSchema(se, model.NewCIStr("testschema"), nil, nil)
	c.Assert(err.Error(), Equals, "context canceled")
}

func (s *testBackupRestoreSuite) TestBackupAndRestore(c *C) {
	// only run BR SQL integration test with tikv store.
	// TODO move this test to BR integration tests.
	if *withTiKV {
		cfg := config.GetGlobalConfig()
		cfg.Store = "tikv"
		cfg.Path = s.pdAddr
		config.StoreGlobalConfig(cfg)
		tk := testkit.NewTestKitWithInit(c, s.store)
		tk.MustExec("create database if not exists br")
		tk.MustExec("use br")
		tk.MustExec("create table t1(v int)")
		tk.MustExec("insert into t1 values (1)")
		tk.MustExec("insert into t1 values (2)")
		tk.MustExec("insert into t1 values (3)")
		tk.MustQuery("select count(*) from t1").Check(testkit.Rows("3"))

		tk.MustExec("create database if not exists br02")
		tk.MustExec("use br02")
		tk.MustExec("create table t1(v int)")

		tmpDir := path.Join(os.TempDir(), "bk1")
		os.RemoveAll(tmpDir)
		// backup database to tmp dir
		tk.MustQuery("backup database br to 'local://" + tmpDir + "'")

		// remove database for recovery
		tk.MustExec("drop database br")
		tk.MustExec("drop database br02")

		// restore database with backup data
		tk.MustQuery("restore database * from 'local://" + tmpDir + "'")
		tk.MustExec("use br")
		tk.MustQuery("select count(*) from t1").Check(testkit.Rows("3"))
		tk.MustExec("drop database br")
	}
}

func (s *testSessionSuite2) TestIssue19127(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists issue19127")
	tk.MustExec("create table issue19127 (c_int int, c_str varchar(40), primary key (c_int, c_str) ) partition by hash (c_int) partitions 4;")
	tk.MustExec("insert into issue19127 values (9, 'angry williams'), (10, 'thirsty hugle');")
	tk.Exec("update issue19127 set c_int = c_int + 10, c_str = 'adoring stonebraker' where c_int in (10, 9);")
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(2))
}

func (s *testSessionSuite2) TestMemoryUsageAlarmVariable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("set @@session.tidb_memory_usage_alarm_ratio=1")
	tk.MustQuery("select @@session.tidb_memory_usage_alarm_ratio").Check(testkit.Rows("1"))
	tk.MustExec("set @@session.tidb_memory_usage_alarm_ratio=0")
	tk.MustQuery("select @@session.tidb_memory_usage_alarm_ratio").Check(testkit.Rows("0"))
	tk.MustExec("set @@session.tidb_memory_usage_alarm_ratio=0.7")
	tk.MustQuery("select @@session.tidb_memory_usage_alarm_ratio").Check(testkit.Rows("0.7"))
	tk.MustExec("set @@session.tidb_memory_usage_alarm_ratio=1.1")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_memory_usage_alarm_ratio value: '1.1'"))
	tk.MustQuery("select @@session.tidb_memory_usage_alarm_ratio").Check(testkit.Rows("1"))

	tk.MustExec("set @@session.tidb_memory_usage_alarm_ratio=-1")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_memory_usage_alarm_ratio value: '-1'"))
	tk.MustQuery("select @@session.tidb_memory_usage_alarm_ratio").Check(testkit.Rows("0"))

	err := tk.ExecToErr("set @@global.tidb_memory_usage_alarm_ratio=0.8")
	c.Assert(err.Error(), Equals, "[variable:1228]Variable 'tidb_memory_usage_alarm_ratio' is a SESSION variable and can't be used with SET GLOBAL")
}

func (s *testSessionSuite2) TestSelectLockInShare(c *C) {
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustExec("DROP TABLE IF EXISTS t_sel_in_share")
	tk1.MustExec("CREATE TABLE t_sel_in_share (id int DEFAULT NULL)")
	tk1.MustExec("insert into t_sel_in_share values (11)")
	err := tk1.ExecToErr("select * from t_sel_in_share lock in share mode")
	c.Assert(err, NotNil)
	tk1.MustExec("set @@tidb_enable_noop_functions = 1")
	tk1.MustQuery("select * from t_sel_in_share lock in share mode").Check(testkit.Rows("11"))
	tk1.MustExec("DROP TABLE t_sel_in_share")
}

func (s *testSessionSerialSuite) TestCoprocessorOOMAction(c *C) {
	// Assert Coprocessor OOMAction
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`set @@tidb_wait_split_region_finish=1`)
	// create table for non keep-order case
	tk.MustExec("drop table if exists t5")
	tk.MustExec("create table t5(id int)")
	tk.MustQuery(`split table t5 between (0) and (10000) regions 10`).Check(testkit.Rows("9 1"))
	// create table for keep-order case
	tk.MustExec("drop table if exists t6")
	tk.MustExec("create table t6(id int, index(id))")
	tk.MustQuery(`split table t6 between (0) and (10000) regions 10`).Check(testkit.Rows("10 1"))
	tk.MustQuery("split table t6 INDEX id between (0) and (10000) regions 10;").Check(testkit.Rows("10 1"))
	count := 10
	for i := 0; i < count; i++ {
		tk.MustExec(fmt.Sprintf("insert into t5 (id) values (%v)", i))
		tk.MustExec(fmt.Sprintf("insert into t6 (id) values (%v)", i))
	}

	testcases := []struct {
		name string
		sql  string
	}{
		{
			name: "keep Order",
			sql:  "select id from t6 order by id",
		},
		{
			name: "non keep Order",
			sql:  "select id from t5",
		},
	}
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.OOMAction = config.OOMActionCancel
	})
	failpoint.Enable("github.com/pingcap/tidb/store/copr/testRateLimitActionMockConsumeAndAssert", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/store/copr/testRateLimitActionMockConsumeAndAssert")

	enableOOM := func(tk *testkit.TestKit, name, sql string) {
		c.Logf("enable OOM, testcase: %v", name)
		// larger than 4 copResponse, smaller than 5 copResponse
		quota := 5*copr.MockResponseSizeForTest - 100
		tk.MustExec("use test")
		tk.MustExec("set @@tidb_distsql_scan_concurrency = 10")
		tk.MustExec(fmt.Sprintf("set @@tidb_mem_quota_query=%v;", quota))
		var expect []string
		for i := 0; i < count; i++ {
			expect = append(expect, fmt.Sprintf("%v", i))
		}
		tk.MustQuery(sql).Sort().Check(testkit.Rows(expect...))
		// assert oom action worked by max consumed > memory quota
		c.Assert(tk.Se.GetSessionVars().StmtCtx.MemTracker.MaxConsumed(), Greater, int64(quota))
	}

	disableOOM := func(tk *testkit.TestKit, name, sql string) {
		c.Logf("disable OOM, testcase: %v", name)
		quota := 5*copr.MockResponseSizeForTest - 100
		tk.MustExec("use test")
		tk.MustExec("set @@tidb_distsql_scan_concurrency = 10")
		tk.MustExec(fmt.Sprintf("set @@tidb_mem_quota_query=%v;", quota))
		err := tk.QueryToErr(sql)
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Matches, "Out Of Memory Quota.*")
	}

	failpoint.Enable("github.com/pingcap/tidb/store/copr/testRateLimitActionMockWaitMax", `return(true)`)
	// assert oom action and switch
	for _, testcase := range testcases {
		se, err := session.CreateSession4Test(s.store)
		c.Check(err, IsNil)
		tk.Se = se
		enableOOM(tk, testcase.name, testcase.sql)
		tk.MustExec("set @@tidb_enable_rate_limit_action = 0")
		disableOOM(tk, testcase.name, testcase.sql)
		tk.MustExec("set @@tidb_enable_rate_limit_action = 1")
		enableOOM(tk, testcase.name, testcase.sql)
		se.Close()
	}

	globaltk := testkit.NewTestKitWithInit(c, s.store)
	globaltk.MustExec("set global tidb_enable_rate_limit_action= 0")
	for _, testcase := range testcases {
		se, err := session.CreateSession4Test(s.store)
		c.Check(err, IsNil)
		tk.Se = se
		disableOOM(tk, testcase.name, testcase.sql)
		se.Close()
	}
	globaltk.MustExec("set global tidb_enable_rate_limit_action= 1")
	for _, testcase := range testcases {
		se, err := session.CreateSession4Test(s.store)
		c.Check(err, IsNil)
		tk.Se = se
		enableOOM(tk, testcase.name, testcase.sql)
		se.Close()
	}
	failpoint.Disable("github.com/pingcap/tidb/store/copr/testRateLimitActionMockWaitMax")

	// assert oom fallback
	for _, testcase := range testcases {
		c.Log(testcase.name)
		se, err := session.CreateSession4Test(s.store)
		c.Check(err, IsNil)
		tk.Se = se
		tk.MustExec("use test")
		tk.MustExec("set tidb_distsql_scan_concurrency = 1")
		tk.MustExec("set @@tidb_mem_quota_query=1;")
		err = tk.QueryToErr(testcase.sql)
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Matches, "Out Of Memory Quota.*")
		se.Close()
	}
}

// TestDefaultWeekFormat checks for issue #21510.
func (s *testSessionSerialSuite) TestDefaultWeekFormat(c *C) {
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustExec("set @@global.default_week_format = 4;")
	defer tk1.MustExec("set @@global.default_week_format = default;")

	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk2.MustQuery("select week('2020-02-02'), @@default_week_format, week('2020-02-02');").Check(testkit.Rows("6 4 6"))
}

func (s *testSessionSerialSuite) TestIssue21944(c *C) {
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	_, err := tk1.Exec("set @@tidb_current_ts=1;")
	c.Assert(err.Error(), Equals, "[variable:1238]Variable 'tidb_current_ts' is a read only variable")
}

func (s *testSessionSerialSuite) TestIssue21943(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	_, err := tk.Exec("set @@last_plan_from_binding='123';")
	c.Assert(err.Error(), Equals, "[variable:1238]Variable 'last_plan_from_binding' is a read only variable")

	_, err = tk.Exec("set @@last_plan_from_cache='123';")
	c.Assert(err.Error(), Equals, "[variable:1238]Variable 'last_plan_from_cache' is a read only variable")
}

func (s *testSessionSerialSuite) TestRemovedSysVars(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	variable.RegisterSysVar(&variable.SysVar{Scope: variable.ScopeGlobal | variable.ScopeSession, Name: "bogus_var", Value: "acdc"})
	result := tk.MustQuery("SHOW GLOBAL VARIABLES LIKE 'bogus_var'")
	result.Check(testkit.Rows("bogus_var acdc"))
	result = tk.MustQuery("SELECT @@GLOBAL.bogus_var")
	result.Check(testkit.Rows("acdc"))
	tk.MustExec("SET GLOBAL bogus_var = 'newvalue'")

	// unregister
	variable.UnregisterSysVar("bogus_var")

	result = tk.MustQuery("SHOW GLOBAL VARIABLES LIKE 'bogus_var'")
	result.Check(testkit.Rows()) // empty
	_, err := tk.Exec("SET GLOBAL bogus_var = 'newvalue'")
	c.Assert(err.Error(), Equals, "[variable:1193]Unknown system variable 'bogus_var'")
	_, err = tk.Exec("SELECT @@GLOBAL.bogus_var")
	c.Assert(err.Error(), Equals, "[variable:1193]Unknown system variable 'bogus_var'")
}

func (s *testSessionSerialSuite) TestCorrectScopeError(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	variable.RegisterSysVar(&variable.SysVar{Scope: variable.ScopeNone, Name: "sv_none", Value: "acdc"})
	variable.RegisterSysVar(&variable.SysVar{Scope: variable.ScopeGlobal, Name: "sv_global", Value: "acdc"})
	variable.RegisterSysVar(&variable.SysVar{Scope: variable.ScopeSession, Name: "sv_session", Value: "acdc"})
	variable.RegisterSysVar(&variable.SysVar{Scope: variable.ScopeGlobal | variable.ScopeSession, Name: "sv_both", Value: "acdc"})

	// check set behavior

	// none
	_, err := tk.Exec("SET sv_none='acdc'")
	c.Assert(err.Error(), Equals, "[variable:1238]Variable 'sv_none' is a read only variable")
	_, err = tk.Exec("SET GLOBAL sv_none='acdc'")
	c.Assert(err.Error(), Equals, "[variable:1238]Variable 'sv_none' is a read only variable")

	// global
	tk.MustExec("SET GLOBAL sv_global='acdc'")
	_, err = tk.Exec("SET sv_global='acdc'")
	c.Assert(err.Error(), Equals, "[variable:1229]Variable 'sv_global' is a GLOBAL variable and should be set with SET GLOBAL")

	// session
	_, err = tk.Exec("SET GLOBAL sv_session='acdc'")
	c.Assert(err.Error(), Equals, "[variable:1228]Variable 'sv_session' is a SESSION variable and can't be used with SET GLOBAL")
	tk.MustExec("SET sv_session='acdc'")

	// both
	tk.MustExec("SET GLOBAL sv_both='acdc'")
	tk.MustExec("SET sv_both='acdc'")

	// unregister
	variable.UnregisterSysVar("sv_none")
	variable.UnregisterSysVar("sv_global")
	variable.UnregisterSysVar("sv_session")
	variable.UnregisterSysVar("sv_both")
}

func (s *testSessionSerialSuite) TestTiKVSystemVars(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	result := tk.MustQuery("SHOW GLOBAL VARIABLES LIKE 'tidb_gc_enable'") // default is on from the sysvar
	result.Check(testkit.Rows("tidb_gc_enable ON"))
	result = tk.MustQuery("SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_enable'")
	result.Check(testkit.Rows()) // but no value in the table (yet) because the value has not been set and the GC has never been run

	// update will set a value in the table
	tk.MustExec("SET GLOBAL tidb_gc_enable = 1")
	result = tk.MustQuery("SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_enable'")
	result.Check(testkit.Rows("true"))

	tk.MustExec("UPDATE mysql.tidb SET variable_value = 'false' WHERE variable_name='tikv_gc_enable'")
	result = tk.MustQuery("SELECT @@tidb_gc_enable;")
	result.Check(testkit.Rows("0")) // reads from mysql.tidb value and changes to false

	tk.MustExec("SET GLOBAL tidb_gc_concurrency = -1") // sets auto concurrency and concurrency
	result = tk.MustQuery("SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_auto_concurrency'")
	result.Check(testkit.Rows("true"))
	result = tk.MustQuery("SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_concurrency'")
	result.Check(testkit.Rows("-1"))

	tk.MustExec("SET GLOBAL tidb_gc_concurrency = 5") // sets auto concurrency and concurrency
	result = tk.MustQuery("SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_auto_concurrency'")
	result.Check(testkit.Rows("false"))
	result = tk.MustQuery("SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_concurrency'")
	result.Check(testkit.Rows("5"))

	tk.MustExec("UPDATE mysql.tidb SET variable_value = 'true' WHERE variable_name='tikv_gc_auto_concurrency'")
	result = tk.MustQuery("SELECT @@tidb_gc_concurrency;")
	result.Check(testkit.Rows("-1")) // because auto_concurrency is turned on it takes precedence

	tk.MustExec("REPLACE INTO mysql.tidb (variable_value, variable_name) VALUES ('15m', 'tikv_gc_run_interval')")
	result = tk.MustQuery("SELECT @@GLOBAL.tidb_gc_run_interval;")
	result.Check(testkit.Rows("15m0s"))
	result = tk.MustQuery("SHOW GLOBAL VARIABLES LIKE 'tidb_gc_run_interval'")
	result.Check(testkit.Rows("tidb_gc_run_interval 15m0s"))

	tk.MustExec("SET GLOBAL tidb_gc_run_interval = '9m'") // too small
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_gc_run_interval value: '9m'"))
	result = tk.MustQuery("SHOW GLOBAL VARIABLES LIKE 'tidb_gc_run_interval'")
	result.Check(testkit.Rows("tidb_gc_run_interval 10m0s"))

	tk.MustExec("SET GLOBAL tidb_gc_run_interval = '700000000000ns'") // specified in ns, also valid

	_, err := tk.Exec("SET GLOBAL tidb_gc_run_interval = '11mins'")
	c.Assert(err.Error(), Equals, "[variable:1232]Incorrect argument type to variable 'tidb_gc_run_interval'") // wrong format
}

func (s *testSessionSerialSuite) TestGlobalVarCollationServer(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@global.collation_server=utf8mb4_general_ci")
	tk.MustQuery("show global variables like 'collation_server'").Check(testkit.Rows("collation_server utf8mb4_general_ci"))
	tk = testkit.NewTestKit(c, s.store)
	tk.MustQuery("show global variables like 'collation_server'").Check(testkit.Rows("collation_server utf8mb4_general_ci"))
	tk.MustQuery("show variables like 'collation_server'").Check(testkit.Rows("collation_server utf8mb4_general_ci"))
}

func (s *testSessionSerialSuite) TestProcessInfoIssue22068(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	var wg util.WaitGroupWrapper
	wg.Run(func() {
		tk.MustQuery("select 1 from t where a = (select sleep(5));").Check(testkit.Rows())
	})
	time.Sleep(2 * time.Second)
	pi := tk.Se.ShowProcess()
	c.Assert(pi, NotNil)
	c.Assert(pi.Info, Equals, "select 1 from t where a = (select sleep(5));")
	c.Assert(pi.Plan, IsNil)
	wg.Wait()
}

func (s *testSessionSerialSuite) TestParseWithParams(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	se := tk.Se
	exec := se.(sqlexec.RestrictedSQLExecutor)

	// test compatibility with ExcuteInternal
	_, err := exec.ParseWithParams(context.TODO(), "SELECT 4")
	c.Assert(err, IsNil)

	// test charset attack
	stmt, err := exec.ParseWithParams(context.TODO(), "SELECT * FROM test WHERE name = %? LIMIT 1", "\xbf\x27 OR 1=1 /*")
	c.Assert(err, IsNil)

	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.RestoreStringDoubleQuotes, &sb)
	err = stmt.Restore(ctx)
	c.Assert(err, IsNil)
	c.Assert(sb.String(), Equals, "SELECT * FROM test WHERE name=_utf8mb4\"\xbf' OR 1=1 /*\" LIMIT 1")

	// test invalid sql
	_, err = exec.ParseWithParams(context.TODO(), "SELECT")
	c.Assert(err, ErrorMatches, ".*You have an error in your SQL syntax.*")

	// test invalid arguments to escape
	_, err = exec.ParseWithParams(context.TODO(), "SELECT %?, %?", 3)
	c.Assert(err, ErrorMatches, "missing arguments.*")

	// test noescape
	stmt, err = exec.ParseWithParams(context.TODO(), "SELECT 3")
	c.Assert(err, IsNil)

	sb.Reset()
	ctx = format.NewRestoreCtx(0, &sb)
	err = stmt.Restore(ctx)
	c.Assert(err, IsNil)
	c.Assert(sb.String(), Equals, "SELECT 3")
}

func (s *testSessionSuite3) TestGlobalTemporaryTable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create global temporary table g_tmp (a int primary key, b int, c int, index i_b(b)) on commit delete rows")
	tk.MustExec("begin")
	tk.MustExec("insert into g_tmp values (3, 3, 3)")
	tk.MustExec("insert into g_tmp values (4, 7, 9)")

	// Cover table scan.
	tk.MustQuery("select * from g_tmp").Check(testkit.Rows("3 3 3", "4 7 9"))
	// Cover index reader.
	tk.MustQuery("select b from g_tmp where b > 3").Check(testkit.Rows("7"))
	// Cover index lookup.
	tk.MustQuery("select c from g_tmp where b = 3").Check(testkit.Rows("3"))
	// Cover point get.
	tk.MustQuery("select * from g_tmp where a = 3").Check(testkit.Rows("3 3 3"))
	// Cover batch point get.
	tk.MustQuery("select * from g_tmp where a in (2,3,4)").Check(testkit.Rows("3 3 3", "4 7 9"))
	tk.MustExec("commit")

	// The global temporary table data is discard after the transaction commit.
	tk.MustQuery("select * from g_tmp").Check(testkit.Rows())
}

type testTxnStateSerialSuite struct {
	testSessionSuiteBase
}

func (s *testTxnStateSerialSuite) TestBasic(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t(a) values (1);")
	info := tk.Se.TxnInfo()
	c.Assert(info, IsNil)

	tk.MustExec("begin pessimistic;")
	startTSStr := tk.MustQuery("select @@tidb_current_ts;").Rows()[0][0].(string)
	startTS, err := strconv.ParseUint(startTSStr, 10, 64)
	c.Assert(err, IsNil)

	c.Assert(failpoint.Enable("tikvclient/beforePessimisticLock", "pause"), IsNil)
	ch := make(chan interface{})
	go func() {
		tk.MustExec("select * from t for update;")
		ch <- nil
	}()
	time.Sleep(100 * time.Millisecond)
	info = tk.Se.TxnInfo()
	_, expectedDigest := parser.NormalizeDigest("select * from t for update;")
	c.Assert(info.CurrentSQLDigest, Equals, expectedDigest.String())
	c.Assert(info.State, Equals, txninfo.TxnLockWaiting)
	c.Assert(info.BlockStartTime.Valid, IsTrue)
	c.Assert(info.StartTS, Equals, startTS)

	c.Assert(failpoint.Disable("tikvclient/beforePessimisticLock"), IsNil)
	<-ch

	info = tk.Se.TxnInfo()
	c.Assert(info.CurrentSQLDigest, Equals, "")
	c.Assert(info.State, Equals, txninfo.TxnIdle)
	c.Assert(info.BlockStartTime.Valid, IsFalse)
	c.Assert(info.StartTS, Equals, startTS)
	_, beginDigest := parser.NormalizeDigest("begin pessimistic;")
	_, selectTSDigest := parser.NormalizeDigest("select @@tidb_current_ts;")
	c.Assert(info.AllSQLDigests, DeepEquals, []string{beginDigest.String(), selectTSDigest.String(), expectedDigest.String()})

	// len and size will be covered in TestLenAndSize
	c.Assert(info.ConnectionID, Equals, tk.Se.GetSessionVars().ConnectionID)
	c.Assert(info.Username, Equals, "")
	c.Assert(info.CurrentDB, Equals, "test")
	c.Assert(info.StartTS, Equals, startTS)

	c.Assert(failpoint.Enable("tikvclient/beforePrewrite", "pause"), IsNil)
	go func() {
		tk.MustExec("commit;")
		ch <- nil
	}()
	time.Sleep(100 * time.Millisecond)
	_, commitDigest := parser.NormalizeDigest("commit;")
	info = tk.Se.TxnInfo()
	c.Assert(info.CurrentSQLDigest, Equals, commitDigest.String())
	c.Assert(info.State, Equals, txninfo.TxnCommitting)
	c.Assert(info.AllSQLDigests, DeepEquals, []string{beginDigest.String(), selectTSDigest.String(), expectedDigest.String(), commitDigest.String()})

	c.Assert(failpoint.Disable("tikvclient/beforePrewrite"), IsNil)
	<-ch
	info = tk.Se.TxnInfo()
	c.Assert(info, IsNil)

	// Test autocommit transaction
	c.Assert(failpoint.Enable("tikvclient/beforePrewrite", "pause"), IsNil)
	go func() {
		tk.MustExec("insert into t values (2)")
		ch <- nil
	}()
	time.Sleep(100 * time.Millisecond)
	info = tk.Se.TxnInfo()
	_, expectedDigest = parser.NormalizeDigest("insert into t values (2)")
	c.Assert(info.CurrentSQLDigest, Equals, expectedDigest.String())
	c.Assert(info.State, Equals, txninfo.TxnCommitting)
	c.Assert(info.BlockStartTime.Valid, IsFalse)
	c.Assert(info.StartTS, Greater, startTS)
	c.Assert(len(info.AllSQLDigests), Equals, 1)
	c.Assert(info.AllSQLDigests[0], Equals, expectedDigest.String())

	c.Assert(failpoint.Disable("tikvclient/beforePrewrite"), IsNil)
	<-ch
	info = tk.Se.TxnInfo()
	c.Assert(info, IsNil)
}

func (s *testTxnStateSerialSuite) TestEntriesCountAndSize(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t(a int);")
	tk.MustExec("begin pessimistic;")
	tk.MustExec("insert into t(a) values (1);")
	info := tk.Se.TxnInfo()
	c.Assert(info.EntriesCount, Equals, uint64(1))
	c.Assert(info.EntriesSize, Equals, uint64(29))
	tk.MustExec("insert into t(a) values (2);")
	info = tk.Se.TxnInfo()
	c.Assert(info.EntriesCount, Equals, uint64(2))
	c.Assert(info.EntriesSize, Equals, uint64(58))
	tk.MustExec("commit;")
}

func (s *testTxnStateSerialSuite) TestRunning(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t(a) values (1);")
	tk.MustExec("begin pessimistic;")
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/session/mockStmtSlow", "return(200)"), IsNil)
	ch := make(chan struct{})
	go func() {
		tk.MustExec("select * from t for update /* sleep */;")
		tk.MustExec("commit;")
		ch <- struct{}{}
	}()
	time.Sleep(100 * time.Millisecond)
	info := tk.Se.TxnInfo()
	c.Assert(info.State, Equals, txninfo.TxnRunning)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/session/mockStmtSlow"), IsNil)
	<-ch
}

func (s *testTxnStateSerialSuite) TestBlocked(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t(a) values (1);")
	tk.MustExec("begin pessimistic;")
	tk.MustExec("select * from t where a = 1 for update;")
	ch := make(chan struct{})
	go func() {
		tk2.MustExec("begin pessimistic")
		tk2.MustExec("select * from t where a = 1 for update;")
		tk2.MustExec("commit;")
		ch <- struct{}{}
	}()
	time.Sleep(100 * time.Millisecond)
	c.Assert(tk2.Se.TxnInfo().State, Equals, txninfo.TxnLockWaiting)
	c.Assert(tk2.Se.TxnInfo().BlockStartTime, NotNil)
	tk.MustExec("commit;")
	<-ch
}

func (s *testTxnStateSerialSuite) TestCommitting(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t(a) values (1), (2);")
	tk.MustExec("begin pessimistic;")
	tk.MustExec("select * from t where a = 1 for update;")
	ch := make(chan struct{})
	go func() {
		tk2.MustExec("begin pessimistic")
		c.Assert(tk2.Se.TxnInfo(), NotNil)
		tk2.MustExec("select * from t where a = 2 for update;")
		c.Assert(failpoint.Enable("github.com/pingcap/tidb/session/mockSlowCommit", "pause"), IsNil)
		tk2.MustExec("commit;")
		ch <- struct{}{}
	}()
	time.Sleep(100 * time.Millisecond)
	c.Assert(tk2.Se.TxnInfo().State, Equals, txninfo.TxnCommitting)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/session/mockSlowCommit"), IsNil)
	tk.MustExec("commit;")
	<-ch
}

func (s *testTxnStateSerialSuite) TestRollbacking(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t(a) values (1), (2);")
	ch := make(chan struct{})
	go func() {
		tk.MustExec("begin pessimistic")
		tk.MustExec("insert into t(a) values (3);")
		c.Assert(failpoint.Enable("github.com/pingcap/tidb/session/mockSlowRollback", "pause"), IsNil)
		tk.MustExec("rollback;")
		ch <- struct{}{}
	}()
	time.Sleep(100 * time.Millisecond)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/session/mockSlowRollback"), IsNil)
	c.Assert(tk.Se.TxnInfo().State, Equals, txninfo.TxnRollingBack)
	<-ch
}

func (s *testTxnStateSerialSuite) TestTxnInfoWithPreparedStmt(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t(a int)")
	tk.MustExec("prepare s1 from 'insert into t values (?)'")
	tk.MustExec("set @v = 1")

	tk.MustExec("begin pessimistic")
	c.Assert(failpoint.Enable("tikvclient/beforePessimisticLock", "pause"), IsNil)
	ch := make(chan interface{})
	go func() {
		tk.MustExec("execute s1 using @v")
		ch <- nil
	}()
	time.Sleep(100 * time.Millisecond)
	info := tk.Se.TxnInfo()
	_, expectDigest := parser.NormalizeDigest("insert into t values (?)")
	c.Assert(info.CurrentSQLDigest, Equals, expectDigest.String())

	c.Assert(failpoint.Disable("tikvclient/beforePessimisticLock"), IsNil)
	<-ch
	info = tk.Se.TxnInfo()
	c.Assert(info.CurrentSQLDigest, Equals, "")
	_, beginDigest := parser.NormalizeDigest("begin pessimistic")
	c.Assert(info.AllSQLDigests, DeepEquals, []string{beginDigest.String(), expectDigest.String()})

	tk.MustExec("rollback")
}

func (s *testTxnStateSerialSuite) TestTxnInfoWithScalarSubquery(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (1, 10), (2, 1)")

	tk.MustExec("begin pessimistic")
	_, beginDigest := parser.NormalizeDigest("begin pessimistic")
	tk.MustExec("select * from t where a = (select b from t where a = 2)")
	_, s1Digest := parser.NormalizeDigest("select * from t where a = (select b from t where a = 2)")

	c.Assert(failpoint.Enable("tikvclient/beforePessimisticLock", "pause"), IsNil)
	ch := make(chan interface{})
	go func() {
		tk.MustExec("update t set b = b + 1 where a = (select b from t where a = 2)")
		ch <- nil
	}()
	_, s2Digest := parser.NormalizeDigest("update t set b = b + 1 where a = (select b from t where a = 1)")
	time.Sleep(100 * time.Millisecond)
	info := tk.Se.TxnInfo()
	c.Assert(info.CurrentSQLDigest, Equals, s2Digest.String())
	c.Assert(info.AllSQLDigests, DeepEquals, []string{beginDigest.String(), s1Digest.String(), s2Digest.String()})

	c.Assert(failpoint.Disable("tikvclient/beforePessimisticLock"), IsNil)
	<-ch
	tk.MustExec("rollback")
}

func (s *testTxnStateSerialSuite) TestTxnInfoWithPSProtocol(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (a int primary key)")

	// Test autocommit transaction

	idInsert, _, _, err := tk.Se.PrepareStmt("insert into t values (?)")
	c.Assert(err, IsNil)

	c.Assert(failpoint.Enable("tikvclient/beforePrewrite", "pause"), IsNil)
	ch := make(chan interface{})
	go func() {
		_, err := tk.Se.ExecutePreparedStmt(context.Background(), idInsert, types.MakeDatums(1))
		c.Assert(err, IsNil)
		ch <- nil
	}()
	time.Sleep(100 * time.Millisecond)
	_, digest := parser.NormalizeDigest("insert into t values (1)")
	info := tk.Se.TxnInfo()
	c.Assert(info, NotNil)
	c.Assert(info.StartTS, Greater, uint64(0))
	c.Assert(info.State, Equals, txninfo.TxnCommitting)
	c.Assert(info.CurrentSQLDigest, Equals, digest.String())
	c.Assert(info.AllSQLDigests, DeepEquals, []string{digest.String()})

	c.Assert(failpoint.Disable("tikvclient/beforePrewrite"), IsNil)
	<-ch
	info = tk.Se.TxnInfo()
	c.Assert(info, IsNil)

	// Test non-autocommit transaction

	id1, _, _, err := tk.Se.PrepareStmt("select * from t where a = ?")
	c.Assert(err, IsNil)
	_, digest1 := parser.NormalizeDigest("select * from t where a = ?")
	id2, _, _, err := tk.Se.PrepareStmt("update t set a = a + 1 where a = ?")
	c.Assert(err, IsNil)
	_, digest2 := parser.NormalizeDigest("update t set a = a + 1 where a = ?")

	tk.MustExec("begin pessimistic")

	_, err = tk.Se.ExecutePreparedStmt(context.Background(), id1, types.MakeDatums(1))
	c.Assert(err, IsNil)

	c.Assert(failpoint.Enable("tikvclient/beforePessimisticLock", "pause"), IsNil)
	go func() {
		_, err := tk.Se.ExecutePreparedStmt(context.Background(), id2, types.MakeDatums(1))
		c.Assert(err, IsNil)
		ch <- nil
	}()
	time.Sleep(100 * time.Millisecond)
	info = tk.Se.TxnInfo()
	c.Assert(info.StartTS, Greater, uint64(0))
	c.Assert(info.CurrentSQLDigest, Equals, digest2.String())
	c.Assert(info.State, Equals, txninfo.TxnLockWaiting)
	c.Assert(info.BlockStartTime.Valid, IsTrue)
	_, beginDigest := parser.NormalizeDigest("begin pessimistic")
	c.Assert(info.AllSQLDigests, DeepEquals, []string{beginDigest.String(), digest1.String(), digest2.String()})

	c.Assert(failpoint.Disable("tikvclient/beforePessimisticLock"), IsNil)
	<-ch
	tk.MustExec("rollback")
}

func (s *testSessionSuite) TestReadDMLBatchSize(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set global tidb_dml_batch_size=1000")
	se, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	// `select 1` to load the global variables.
	_, _ = se.Execute(context.TODO(), "select 1")
	c.Assert(se.GetSessionVars().DMLBatchSize, Equals, 1000)
}

func (s *testSessionSuite) TestInTxnPSProtoPointGet(c *C) {
	ctx := context.Background()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(c1 int primary key, c2 int, c3 int)")
	tk.MustExec("insert into t1 values(1, 10, 100)")

	// Generate the ps statement and make the prepared plan cached for point get.
	id, _, _, err := tk.Se.PrepareStmt("select c1, c2 from t1 where c1 = ?")
	c.Assert(err, IsNil)
	idForUpdate, _, _, err := tk.Se.PrepareStmt("select c1, c2 from t1 where c1 = ? for update")
	c.Assert(err, IsNil)
	params := []types.Datum{types.NewDatum(1)}
	rs, err := tk.Se.ExecutePreparedStmt(ctx, id, params)
	c.Assert(err, IsNil)
	tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 10"))
	rs, err = tk.Se.ExecutePreparedStmt(ctx, idForUpdate, params)
	c.Assert(err, IsNil)
	tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 10"))

	// Query again the cached plan will be used.
	rs, err = tk.Se.ExecutePreparedStmt(ctx, id, params)
	c.Assert(err, IsNil)
	tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 10"))
	rs, err = tk.Se.ExecutePreparedStmt(ctx, idForUpdate, params)
	c.Assert(err, IsNil)
	tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 10"))

	// Start a transaction, now the in txn flag will be added to the session vars.
	_, err = tk.Se.Execute(ctx, "start transaction")
	c.Assert(err, IsNil)
	rs, err = tk.Se.ExecutePreparedStmt(ctx, id, params)
	c.Assert(err, IsNil)
	tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 10"))
	txn, err := tk.Se.Txn(false)
	c.Assert(err, IsNil)
	c.Assert(txn.Valid(), IsTrue)
	rs, err = tk.Se.ExecutePreparedStmt(ctx, idForUpdate, params)
	c.Assert(err, IsNil)
	tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 10"))
	txn, err = tk.Se.Txn(false)
	c.Assert(err, IsNil)
	c.Assert(txn.Valid(), IsTrue)
	_, err = tk.Se.Execute(ctx, "update t1 set c2 = c2 + 1")
	c.Assert(err, IsNil)
	// Check the read result after in-transaction update.
	rs, err = tk.Se.ExecutePreparedStmt(ctx, id, params)
	c.Assert(err, IsNil)
	tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 11"))
	rs, err = tk.Se.ExecutePreparedStmt(ctx, idForUpdate, params)
	c.Assert(err, IsNil)
	tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 11"))
	txn, err = tk.Se.Txn(false)
	c.Assert(err, IsNil)
	c.Assert(txn.Valid(), IsTrue)
	tk.MustExec("commit")
}

func (s *testSessionSuite) TestTMPTableSize(c *C) {
	// Test the @@tidb_tmp_table_max_size system variable.
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create global temporary table t (c1 int, c2 mediumtext) on commit delete rows")
	tk.MustExec("create temporary table tl (c1 int, c2 mediumtext)")

	tk.MustQuery("select @@global.tidb_tmp_table_max_size").Check(testkit.Rows(strconv.Itoa(variable.DefTiDBTmpTableMaxSize)))
	c.Assert(tk.Se.GetSessionVars().TMPTableSize, Equals, int64(variable.DefTiDBTmpTableMaxSize))

	// Min value 1M, so the result is change to 1M, with a warning.
	tk.MustExec("set @@global.tidb_tmp_table_max_size = 123")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_tmp_table_max_size value: '123'"))

	// Change the session scope value to 2M.
	tk.MustExec("set @@session.tidb_tmp_table_max_size = 2097152")
	c.Assert(tk.Se.GetSessionVars().TMPTableSize, Equals, int64(2097152))

	// Check in another session, change session scope value does not affect the global scope.
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustQuery("select @@global.tidb_tmp_table_max_size").Check(testkit.Rows(strconv.Itoa(1 << 20)))

	// The value is now 1M, check the error when table size exceed it.
	tk.MustExec(fmt.Sprintf("set @@session.tidb_tmp_table_max_size = %d", 1<<20))
	tk.MustExec("begin")
	tk.MustExec("insert into t values (1, repeat('x', 512*1024))")
	tk.MustExec("insert into t values (1, repeat('x', 512*1024))")
	tk.MustGetErrCode("insert into t values (1, repeat('x', 512*1024))", errno.ErrRecordFileFull)
	tk.MustExec("rollback")

	// Check local temporary table
	tk.MustExec("begin")
	tk.MustExec("insert into tl values (1, repeat('x', 512*1024))")
	tk.MustExec("insert into tl values (1, repeat('x', 512*1024))")
	tk.MustGetErrCode("insert into tl values (1, repeat('x', 512*1024))", errno.ErrRecordFileFull)
	tk.MustExec("rollback")

	// Check local temporary table with some data in session
	tk.MustExec("insert into tl values (1, repeat('x', 512*1024))")
	tk.MustExec("begin")
	tk.MustExec("insert into tl values (1, repeat('x', 512*1024))")
	tk.MustGetErrCode("insert into tl values (1, repeat('x', 512*1024))", errno.ErrRecordFileFull)
	tk.MustExec("rollback")
}

func (s *testStatisticsSuite) cleanEnv(c *C, store kv.Storage, do *domain.Domain) {
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
	tk.MustExec("delete from mysql.stats_meta")
	tk.MustExec("delete from mysql.stats_histograms")
	tk.MustExec("delete from mysql.stats_buckets")
	do.StatsHandle().Clear()
}

func (s *testStatisticsSuite) TestNewCollationStatsWithPrefixIndex(c *C) {
	defer s.cleanEnv(c, s.store, s.dom)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(40) collate utf8mb4_general_ci, index ia3(a(3)), index ia10(a(10)), index ia(a))")
	tk.MustExec("insert into t values('aaAAaaaAAAabbc'), ('AaAaAaAaAaAbBC'), ('AAAaabbBBbbb'), ('AAAaabbBBbbbccc'), ('aaa'), ('Aa'), ('A'), ('ab')")
	tk.MustExec("insert into t values('b'), ('bBb'), ('Bb'), ('bA'), ('BBBB'), ('BBBBBDDDDDdd'), ('bbbbBBBBbbBBR'), ('BBbbBBbbBBbbBBRRR')")
	h := s.dom.StatsHandle()
	c.Assert(h.HandleDDLEvent(<-h.DDLEventCh()), IsNil)

	tk.MustExec("set @@session.tidb_analyze_version=1")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	tk.MustExec("analyze table t")
	tk.MustExec("explain select * from t where a = 'aaa'")
	c.Assert(h.LoadNeededHistograms(), IsNil)
	tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Sort().Check(testkit.Rows(
		"test t  a 0 0 1 1 \x00A \x00A 0",
		"test t  a 0 1 2 1 \x00A\x00A \x00A\x00A 0",
		"test t  a 0 10 12 1 \x00B\x00B\x00B \x00B\x00B\x00B 0",
		"test t  a 0 11 13 1 \x00B\x00B\x00B\x00B \x00B\x00B\x00B\x00B 0",
		"test t  a 0 12 14 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R\x00R\x00R \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R\x00R\x00R 0",
		"test t  a 0 13 15 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R 0",
		"test t  a 0 14 16 1 \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D\x00D\x00D \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D\x00D\x00D 0",
		"test t  a 0 2 3 1 \x00A\x00A\x00A \x00A\x00A\x00A 0",
		"test t  a 0 3 5 2 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C 0",
		"test t  a 0 4 6 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B 0",
		"test t  a 0 5 7 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00C\x00C\x00C \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00C\x00C\x00C 0",
		"test t  a 0 6 8 1 \x00A\x00B \x00A\x00B 0",
		"test t  a 0 7 9 1 \x00B \x00B 0",
		"test t  a 0 8 10 1 \x00B\x00A \x00B\x00A 0",
		"test t  a 0 9 11 1 \x00B\x00B \x00B\x00B 0",
		"test t  ia 1 0 1 1 \x00A \x00A 0",
		"test t  ia 1 1 2 1 \x00A\x00A \x00A\x00A 0",
		"test t  ia 1 10 12 1 \x00B\x00B\x00B \x00B\x00B\x00B 0",
		"test t  ia 1 11 13 1 \x00B\x00B\x00B\x00B \x00B\x00B\x00B\x00B 0",
		"test t  ia 1 12 14 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R\x00R\x00R \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R\x00R\x00R 0",
		"test t  ia 1 13 15 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R 0",
		"test t  ia 1 14 16 1 \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D\x00D\x00D \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D\x00D\x00D 0",
		"test t  ia 1 2 3 1 \x00A\x00A\x00A \x00A\x00A\x00A 0",
		"test t  ia 1 3 5 2 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C 0",
		"test t  ia 1 4 6 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B 0",
		"test t  ia 1 5 7 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00C\x00C\x00C \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00C\x00C\x00C 0",
		"test t  ia 1 6 8 1 \x00A\x00B \x00A\x00B 0",
		"test t  ia 1 7 9 1 \x00B \x00B 0",
		"test t  ia 1 8 10 1 \x00B\x00A \x00B\x00A 0",
		"test t  ia 1 9 11 1 \x00B\x00B \x00B\x00B 0",
		"test t  ia10 1 0 1 1 \x00A \x00A 0",
		"test t  ia10 1 1 2 1 \x00A\x00A \x00A\x00A 0",
		"test t  ia10 1 10 13 1 \x00B\x00B\x00B\x00B \x00B\x00B\x00B\x00B 0",
		"test t  ia10 1 11 15 2 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B 0",
		"test t  ia10 1 12 16 1 \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D 0",
		"test t  ia10 1 2 3 1 \x00A\x00A\x00A \x00A\x00A\x00A 0",
		"test t  ia10 1 3 5 2 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A 0",
		"test t  ia10 1 4 7 2 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B 0",
		"test t  ia10 1 5 8 1 \x00A\x00B \x00A\x00B 0",
		"test t  ia10 1 6 9 1 \x00B \x00B 0",
		"test t  ia10 1 7 10 1 \x00B\x00A \x00B\x00A 0",
		"test t  ia10 1 8 11 1 \x00B\x00B \x00B\x00B 0",
		"test t  ia10 1 9 12 1 \x00B\x00B\x00B \x00B\x00B\x00B 0",
		"test t  ia3 1 0 1 1 \x00A \x00A 0",
		"test t  ia3 1 1 2 1 \x00A\x00A \x00A\x00A 0",
		"test t  ia3 1 2 7 5 \x00A\x00A\x00A \x00A\x00A\x00A 0",
		"test t  ia3 1 3 8 1 \x00A\x00B \x00A\x00B 0",
		"test t  ia3 1 4 9 1 \x00B \x00B 0",
		"test t  ia3 1 5 10 1 \x00B\x00A \x00B\x00A 0",
		"test t  ia3 1 6 11 1 \x00B\x00B \x00B\x00B 0",
		"test t  ia3 1 7 16 5 \x00B\x00B\x00B \x00B\x00B\x00B 0",
	))
	tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(testkit.Rows(
		"test t  a 0 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C 2",
	))
	tk.MustQuery("select is_index, hist_id, distinct_count, null_count, stats_ver, correlation from mysql.stats_histograms").Sort().Check(testkit.Rows(
		"0 1 15 0 1 0.8411764705882353",
		"1 1 8 0 1 0",
		"1 2 13 0 1 0",
		"1 3 15 0 1 0",
	))

	tk.MustExec("set @@session.tidb_analyze_version=2")
	h = s.dom.StatsHandle()
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	tk.MustExec("analyze table t")
	tk.MustExec("explain select * from t where a = 'aaa'")
	c.Assert(h.LoadNeededHistograms(), IsNil)
	tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Sort().Check(testkit.Rows())
	tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(testkit.Rows(
		"test t  a 0 \x00A 1",
		"test t  a 0 \x00A\x00A 1",
		"test t  a 0 \x00A\x00A\x00A 1",
		"test t  a 0 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C 2",
		"test t  a 0 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B 1",
		"test t  a 0 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00C\x00C\x00C 1",
		"test t  a 0 \x00A\x00B 1",
		"test t  a 0 \x00B 1",
		"test t  a 0 \x00B\x00A 1",
		"test t  a 0 \x00B\x00B 1",
		"test t  a 0 \x00B\x00B\x00B 1",
		"test t  a 0 \x00B\x00B\x00B\x00B 1",
		"test t  a 0 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R\x00R\x00R 1",
		"test t  a 0 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R 1",
		"test t  a 0 \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D\x00D\x00D 1",
		"test t  ia 1 \x00A 1",
		"test t  ia 1 \x00A\x00A 1",
		"test t  ia 1 \x00A\x00A\x00A 1",
		"test t  ia 1 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C 2",
		"test t  ia 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B 1",
		"test t  ia 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00C\x00C\x00C 1",
		"test t  ia 1 \x00A\x00B 1",
		"test t  ia 1 \x00B 1",
		"test t  ia 1 \x00B\x00A 1",
		"test t  ia 1 \x00B\x00B 1",
		"test t  ia 1 \x00B\x00B\x00B 1",
		"test t  ia 1 \x00B\x00B\x00B\x00B 1",
		"test t  ia 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R\x00R\x00R 1",
		"test t  ia 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R 1",
		"test t  ia 1 \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D\x00D\x00D 1",
		"test t  ia10 1 \x00A 1",
		"test t  ia10 1 \x00A\x00A 1",
		"test t  ia10 1 \x00A\x00A\x00A 1",
		"test t  ia10 1 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A 2",
		"test t  ia10 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B 2",
		"test t  ia10 1 \x00A\x00B 1",
		"test t  ia10 1 \x00B 1",
		"test t  ia10 1 \x00B\x00A 1",
		"test t  ia10 1 \x00B\x00B 1",
		"test t  ia10 1 \x00B\x00B\x00B 1",
		"test t  ia10 1 \x00B\x00B\x00B\x00B 1",
		"test t  ia10 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B 2",
		"test t  ia10 1 \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D 1",
		"test t  ia3 1 \x00A 1",
		"test t  ia3 1 \x00A\x00A 1",
		"test t  ia3 1 \x00A\x00A\x00A 5",
		"test t  ia3 1 \x00A\x00B 1",
		"test t  ia3 1 \x00B 1",
		"test t  ia3 1 \x00B\x00A 1",
		"test t  ia3 1 \x00B\x00B 1",
		"test t  ia3 1 \x00B\x00B\x00B 5",
	))
	tk.MustQuery("select is_index, hist_id, distinct_count, null_count, stats_ver, correlation from mysql.stats_histograms").Sort().Check(testkit.Rows(
		"0 1 15 0 2 0.8411764705882353",
		"1 1 8 0 2 0",
		"1 2 13 0 2 0",
		"1 3 15 0 2 0",
	))
}

func (s *testSessionSuite) TestAuthPluginForUser(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("CREATE USER 'tapfu1' IDENTIFIED WITH mysql_native_password BY 'tapfu1'")
	plugin, err := tk.Se.AuthPluginForUser(&auth.UserIdentity{Username: "tapfu1", Hostname: `%`})
	c.Assert(err, IsNil)
	c.Assert(plugin, Equals, "mysql_native_password")

	tk.MustExec("CREATE USER 'tapfu2' IDENTIFIED WITH mysql_native_password")
	plugin, err = tk.Se.AuthPluginForUser(&auth.UserIdentity{Username: "tapfu2", Hostname: `%`})
	c.Assert(err, IsNil)
	c.Assert(plugin, Equals, "")

	tk.MustExec("CREATE USER 'tapfu3' IDENTIFIED WITH caching_sha2_password BY 'tapfu3'")
	plugin, err = tk.Se.AuthPluginForUser(&auth.UserIdentity{Username: "tapfu3", Hostname: `%`})
	c.Assert(err, IsNil)
	c.Assert(plugin, Equals, "caching_sha2_password")

	tk.MustExec("CREATE USER 'tapfu4' IDENTIFIED WITH caching_sha2_password")
	plugin, err = tk.Se.AuthPluginForUser(&auth.UserIdentity{Username: "tapfu4", Hostname: `%`})
	c.Assert(err, IsNil)
	c.Assert(plugin, Equals, "")
}

func (s *testSessionSuite) TestLocalTemporaryTableInsert(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create temporary table tmp1 (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into tmp1 (u, v) values(11, 101)")
	tk.MustExec("insert into tmp1 (u, v) values(12, 102)")
	tk.MustExec("insert into tmp1 values(3, 13, 102)")

	checkRecordOneTwoThreeAndNonExist := func() {
		tk.MustQuery("select * from tmp1 where id=1").Check(testkit.Rows("1 11 101"))
		tk.MustQuery("select * from tmp1 where id=2").Check(testkit.Rows("2 12 102"))
		tk.MustQuery("select * from tmp1 where id=3").Check(testkit.Rows("3 13 102"))
		tk.MustQuery("select * from tmp1 where id=99").Check(testkit.Rows())
	}

	// inserted records exist
	checkRecordOneTwoThreeAndNonExist()

	// insert dup records out txn must be error
	_, err := tk.Exec("insert into tmp1 values(1, 999, 9999)")
	c.Assert(kv.ErrKeyExists.Equal(err), IsTrue)
	checkRecordOneTwoThreeAndNonExist()

	_, err = tk.Exec("insert into tmp1 values(99, 11, 999)")
	c.Assert(kv.ErrKeyExists.Equal(err), IsTrue)
	checkRecordOneTwoThreeAndNonExist()

	// insert dup records in txn must be error
	tk.MustExec("begin")
	_, err = tk.Exec("insert into tmp1 values(1, 999, 9999)")
	c.Assert(kv.ErrKeyExists.Equal(err), IsTrue)
	checkRecordOneTwoThreeAndNonExist()

	_, err = tk.Exec("insert into tmp1 values(99, 11, 9999)")
	c.Assert(kv.ErrKeyExists.Equal(err), IsTrue)
	checkRecordOneTwoThreeAndNonExist()

	tk.MustExec("insert into tmp1 values(4, 14, 104)")
	tk.MustQuery("select * from tmp1 where id=4").Check(testkit.Rows("4 14 104"))

	_, err = tk.Exec("insert into tmp1 values(4, 999, 9999)")
	c.Assert(kv.ErrKeyExists.Equal(err), IsTrue)

	_, err = tk.Exec("insert into tmp1 values(99, 14, 9999)")
	c.Assert(kv.ErrKeyExists.Equal(err), IsTrue)

	checkRecordOneTwoThreeAndNonExist()
	tk.MustExec("commit")

	// check committed insert works
	checkRecordOneTwoThreeAndNonExist()
	tk.MustQuery("select * from tmp1 where id=4").Check(testkit.Rows("4 14 104"))

	// check rollback works
	tk.MustExec("begin")
	tk.MustExec("insert into tmp1 values(5, 15, 105)")
	tk.MustQuery("select * from tmp1 where id=5").Check(testkit.Rows("5 15 105"))
	tk.MustExec("rollback")
	tk.MustQuery("select * from tmp1 where id=5").Check(testkit.Rows())
}

func (s *testSessionSuite) TestLocalTemporaryTableInsertIgnore(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create temporary table tmp1 (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into tmp1 values(1, 11, 101)")
	tk.MustExec("insert into tmp1 values(2, 12, 102)")

	// test outside transaction
	tk.MustExec("insert ignore into tmp1 values(1, 100, 1000)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 'PRIMARY'"))
	tk.MustQuery("select * from tmp1 where id=1").Check(testkit.Rows("1 11 101"))
	tk.MustExec("insert ignore into tmp1 values(5, 15, 105)")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select * from tmp1 where id=5").Check(testkit.Rows("5 15 105"))

	// test in transaction and rollback
	tk.MustExec("begin")
	tk.MustExec("insert ignore into tmp1 values(1, 100, 1000)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 'PRIMARY'"))
	tk.MustQuery("select * from tmp1 where id=1").Check(testkit.Rows("1 11 101"))
	tk.MustExec("insert ignore into tmp1 values(3, 13, 103)")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select * from tmp1 where id=3").Check(testkit.Rows("3 13 103"))
	tk.MustExec("insert ignore into tmp1 values(3, 100, 1000)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '3' for key 'PRIMARY'"))
	tk.MustQuery("select * from tmp1 where id=3").Check(testkit.Rows("3 13 103"))
	tk.MustExec("rollback")
	tk.MustQuery("select * from tmp1").Check(testkit.Rows("1 11 101", "2 12 102", "5 15 105"))

	// test commit
	tk.MustExec("begin")
	tk.MustExec("insert ignore into tmp1 values(1, 100, 1000)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 'PRIMARY'"))
	tk.MustExec("insert ignore into tmp1 values(3, 13, 103)")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("insert ignore into tmp1 values(3, 100, 1000)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '3' for key 'PRIMARY'"))
	tk.MustExec("commit")
	tk.MustQuery("select * from tmp1").Check(testkit.Rows("1 11 101", "2 12 102", "3 13 103", "5 15 105"))
}

func (s *testSessionSuite) TestLocalTemporaryTableInsertOnDuplicateKeyUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create temporary table tmp1 (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into tmp1 values(1, 11, 101)")
	tk.MustExec("insert into tmp1 values(2, 12, 102)")

	// test outside transaction
	tk.MustExec("insert ignore into tmp1 values(1, 100, 1000) on duplicate key update u=12")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '12' for key 'u'"))
	tk.MustQuery("select * from tmp1 where id=1").Check(testkit.Rows("1 11 101"))
	tk.MustExec("insert into tmp1 values(2, 100, 1000) on duplicate key update v=202")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select * from tmp1 where id=2").Check(testkit.Rows("2 12 202"))
	tk.MustExec("insert into tmp1 values(3, 13, 103) on duplicate key update v=203")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select * from tmp1 where id=3").Check(testkit.Rows("3 13 103"))

	// test in transaction and rollback
	tk.MustExec("begin")
	tk.MustExec("insert ignore into tmp1 values(1, 100, 1000) on duplicate key update u=12")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '12' for key 'u'"))
	tk.MustQuery("select * from tmp1 where id=1").Check(testkit.Rows("1 11 101"))
	tk.MustExec("insert into tmp1 values(2, 100, 1000) on duplicate key update v=302")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select * from tmp1 where id=2").Check(testkit.Rows("2 12 302"))
	tk.MustExec("insert into tmp1 values(4, 14, 104) on duplicate key update v=204")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select * from tmp1 where id=4").Check(testkit.Rows("4 14 104"))
	tk.MustExec("rollback")
	tk.MustQuery("select * from tmp1").Check(testkit.Rows("1 11 101", "2 12 202", "3 13 103"))

	// test commit
	tk.MustExec("begin")
	tk.MustExec("insert ignore into tmp1 values(1, 100, 1000) on duplicate key update u=12")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '12' for key 'u'"))
	tk.MustExec("insert into tmp1 values(2, 100, 1000) on duplicate key update v=302")
	tk.MustExec("insert into tmp1 values(4, 14, 104) on duplicate key update v=204")
	tk.MustExec("commit")
	tk.MustQuery("select * from tmp1").Check(testkit.Rows("1 11 101", "2 12 302", "3 13 103", "4 14 104"))
}

func (s *testSessionSuite) TestLocalTemporaryTableReplace(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create temporary table tmp1 (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into tmp1 values(1, 11, 101)")
	tk.MustExec("insert into tmp1 values(2, 12, 102)")
	tk.MustExec("insert into tmp1 values(3, 13, 103)")

	// out of transaction
	tk.MustExec("replace into tmp1 values(1, 12, 1000)")
	tk.MustQuery("select * from tmp1").Check(testkit.Rows("1 12 1000", "3 13 103"))
	tk.MustExec("replace into tmp1 values(4, 14, 104)")
	tk.MustQuery("select * from tmp1 where id=4").Check(testkit.Rows("4 14 104"))

	// in transaction and rollback
	tk.MustExec("begin")
	tk.MustExec("replace into tmp1 values(1, 13, 999)")
	tk.MustQuery("select * from tmp1").Check(testkit.Rows("1 13 999", "4 14 104"))
	tk.MustExec("replace into tmp1 values(5, 15, 105)")
	tk.MustQuery("select * from tmp1 where id=5").Check(testkit.Rows("5 15 105"))
	tk.MustExec("rollback")
	tk.MustQuery("select * from tmp1").Check(testkit.Rows("1 12 1000", "3 13 103", "4 14 104"))

	// out of transaction
	tk.MustExec("begin")
	tk.MustExec("replace into tmp1 values(1, 13, 999)")
	tk.MustExec("replace into tmp1 values(5, 15, 105)")
	tk.MustExec("commit")
	tk.MustQuery("select * from tmp1").Check(testkit.Rows("1 13 999", "4 14 104", "5 15 105"))
}

func (s *testSessionSuite) TestLocalTemporaryTableDelete(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("create temporary table tmp1 (id int primary key, u int unique, v int)")

	insertRecords := func(idList []int) {
		for _, id := range idList {
			tk.MustExec("insert into tmp1 values (?, ?, ?)", id, id+100, id+1000)
		}
	}

	checkAllExistRecords := func(idList []int) {
		sort.Ints(idList)
		expectedResult := make([]string, 0, len(idList))
		expectedIndexResult := make([]string, 0, len(idList))
		for _, id := range idList {
			expectedResult = append(expectedResult, fmt.Sprintf("%d %d %d", id, id+100, id+1000))
			expectedIndexResult = append(expectedIndexResult, fmt.Sprintf("%d", id+100))
		}
		tk.MustQuery("select * from tmp1 order by id").Check(testkit.Rows(expectedResult...))

		// check index deleted
		tk.MustQuery("select /*+ use_index(tmp1, u) */ u from tmp1 order by u").Check(testkit.Rows(expectedIndexResult...))
		tk.MustQuery("show warnings").Check(testkit.Rows())
	}

	assertDelete := func(sql string, deleted []int) {
		idList := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}

		deletedMap := make(map[int]bool)
		for _, id := range deleted {
			deletedMap[id] = true
		}

		keepList := make([]int, 0)
		for _, id := range idList {
			if _, exist := deletedMap[id]; !exist {
				keepList = append(keepList, id)
			}
		}

		// delete records in txn and records are inserted in txn
		tk.MustExec("begin")
		insertRecords(idList)
		tk.MustExec(sql)
		tk.MustQuery("show warnings").Check(testkit.Rows())
		checkAllExistRecords(keepList)
		tk.MustExec("rollback")
		checkAllExistRecords([]int{})

		// delete records out of txn
		insertRecords(idList)
		tk.MustExec(sql)
		checkAllExistRecords(keepList)

		// delete records in txn
		insertRecords(deleted)
		tk.MustExec("begin")
		tk.MustExec(sql)
		checkAllExistRecords(keepList)

		// test rollback
		tk.MustExec("rollback")
		checkAllExistRecords(idList)

		// test commit
		tk.MustExec("begin")
		tk.MustExec(sql)
		tk.MustExec("commit")
		checkAllExistRecords(keepList)

		tk.MustExec("delete from tmp1")
		checkAllExistRecords([]int{})
	}

	assertDelete("delete from tmp1 where id=1", []int{1})
	assertDelete("delete from tmp1 where id in (1, 3, 5)", []int{1, 3, 5})
	assertDelete("delete from tmp1 where u=102", []int{2})
	assertDelete("delete from tmp1 where u in (103, 107, 108)", []int{3, 7, 8})
	assertDelete("delete from tmp1 where id=10", []int{})
	assertDelete("delete from tmp1 where id in (10, 12)", []int{})
	assertDelete("delete from tmp1 where u=110", []int{})
	assertDelete("delete from tmp1 where u in (111, 112)", []int{})
	assertDelete("delete from tmp1 where id in (1, 11, 5)", []int{1, 5})
	assertDelete("delete from tmp1 where u in (102, 121, 106)", []int{2, 6})
	assertDelete("delete from tmp1 where id<3", []int{1, 2})
	assertDelete("delete from tmp1 where u>107", []int{8, 9})
	assertDelete("delete /*+ use_index(tmp1, u) */ from tmp1 where u>105 and u<107", []int{6})
	assertDelete("delete from tmp1 where v>=1006 or v<=1002", []int{1, 2, 6, 7, 8, 9})
}

func (s *testSessionSuite) TestLocalTemporaryTablePointGet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create temporary table tmp1 (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into tmp1 values(1, 11, 101)")
	tk.MustExec("insert into tmp1 values(2, 12, 102)")
	tk.MustExec("insert into tmp1 values(4, 14, 104)")

	// check point get out transaction
	tk.MustQuery("select * from tmp1 where id=1").Check(testkit.Rows("1 11 101"))
	tk.MustQuery("select * from tmp1 where u=11").Check(testkit.Rows("1 11 101"))
	tk.MustQuery("select * from tmp1 where id=2").Check(testkit.Rows("2 12 102"))
	tk.MustQuery("select * from tmp1 where u=12").Check(testkit.Rows("2 12 102"))

	// check point get in transaction
	tk.MustExec("begin")
	tk.MustQuery("select * from tmp1 where id=1").Check(testkit.Rows("1 11 101"))
	tk.MustQuery("select * from tmp1 where u=11").Check(testkit.Rows("1 11 101"))
	tk.MustQuery("select * from tmp1 where id=2").Check(testkit.Rows("2 12 102"))
	tk.MustQuery("select * from tmp1 where u=12").Check(testkit.Rows("2 12 102"))
	tk.MustExec("insert into tmp1 values(3, 13, 103)")
	tk.MustQuery("select * from tmp1 where id=3").Check(testkit.Rows("3 13 103"))
	tk.MustQuery("select * from tmp1 where u=13").Check(testkit.Rows("3 13 103"))
	tk.MustExec("update tmp1 set v=999 where id=2")
	tk.MustQuery("select * from tmp1 where id=2").Check(testkit.Rows("2 12 999"))
	tk.MustExec("delete from tmp1 where id=4")
	tk.MustQuery("select * from tmp1 where id=4").Check(testkit.Rows())
	tk.MustQuery("select * from tmp1 where u=14").Check(testkit.Rows())
	tk.MustExec("commit")

	// check point get after transaction
	tk.MustQuery("select * from tmp1 where id=3").Check(testkit.Rows("3 13 103"))
	tk.MustQuery("select * from tmp1 where u=13").Check(testkit.Rows("3 13 103"))
	tk.MustQuery("select * from tmp1 where id=2").Check(testkit.Rows("2 12 999"))
	tk.MustQuery("select * from tmp1 where id=4").Check(testkit.Rows())
	tk.MustQuery("select * from tmp1 where u=14").Check(testkit.Rows())
}

func (s *testSessionSuite) TestLocalTemporaryTableBatchPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create temporary table tmp1 (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into tmp1 values(1, 11, 101)")
	tk.MustExec("insert into tmp1 values(2, 12, 102)")
	tk.MustExec("insert into tmp1 values(3, 13, 103)")
	tk.MustExec("insert into tmp1 values(4, 14, 104)")

	// check point get out transaction
	tk.MustQuery("select * from tmp1 where id in (1, 3)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustQuery("select * from tmp1 where u in (11, 13)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustQuery("select * from tmp1 where id in (1, 3, 5)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustQuery("select * from tmp1 where u in (11, 13, 15)").Check(testkit.Rows("1 11 101", "3 13 103"))

	// check point get in transaction
	tk.MustExec("begin")
	tk.MustQuery("select * from tmp1 where id in (1, 3)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustQuery("select * from tmp1 where u in (11, 13)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustQuery("select * from tmp1 where id in (1, 3, 5)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustQuery("select * from tmp1 where u in (11, 13, 15)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustExec("insert into tmp1 values(6, 16, 106)")
	tk.MustQuery("select * from tmp1 where id in (1, 6)").Check(testkit.Rows("1 11 101", "6 16 106"))
	tk.MustQuery("select * from tmp1 where u in (11, 16)").Check(testkit.Rows("1 11 101", "6 16 106"))
	tk.MustExec("update tmp1 set v=999 where id=3")
	tk.MustQuery("select * from tmp1 where id in (1, 3)").Check(testkit.Rows("1 11 101", "3 13 999"))
	tk.MustQuery("select * from tmp1 where u in (11, 13)").Check(testkit.Rows("1 11 101", "3 13 999"))
	tk.MustExec("delete from tmp1 where id=4")
	tk.MustQuery("select * from tmp1 where id in (1, 4)").Check(testkit.Rows("1 11 101"))
	tk.MustQuery("select * from tmp1 where u in (11, 14)").Check(testkit.Rows("1 11 101"))
	tk.MustExec("commit")

	// check point get after transaction
	tk.MustQuery("select * from tmp1 where id in (1, 3, 6)").Check(testkit.Rows("1 11 101", "3 13 999", "6 16 106"))
	tk.MustQuery("select * from tmp1 where u in (11, 13, 16)").Check(testkit.Rows("1 11 101", "3 13 999", "6 16 106"))
	tk.MustQuery("select * from tmp1 where id in (1, 4)").Check(testkit.Rows("1 11 101"))
	tk.MustQuery("select * from tmp1 where u in (11, 14)").Check(testkit.Rows("1 11 101"))
}

func (s *testSessionSuite) TestLocalTemporaryTableScan(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create temporary table tmp1 (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into tmp1 values" +
		"(1, 101, 1001), (3, 113, 1003), (5, 105, 1005), (7, 117, 1007), (9, 109, 1009)," +
		"(10, 110, 1010), (12, 112, 1012), (14, 114, 1014), (16, 116, 1016), (18, 118, 1018)",
	)

	assertSelectAsUnModified := func() {
		// For TableReader
		tk.MustQuery("select * from tmp1 where id>3 order by id").Check(testkit.Rows(
			"5 105 1005", "7 117 1007", "9 109 1009",
			"10 110 1010", "12 112 1012", "14 114 1014", "16 116 1016", "18 118 1018",
		))

		// For IndexLookUpReader
		tk.MustQuery("select /*+ use_index(tmp1, u) */ * from tmp1 where u>101 order by u").Check(testkit.Rows(
			"5 105 1005", "9 109 1009", "10 110 1010",
			"12 112 1012", "3 113 1003", "14 114 1014", "16 116 1016", "7 117 1007", "18 118 1018",
		))
		tk.MustQuery("show warnings").Check(testkit.Rows())

		// For IndexReader
		tk.MustQuery("select /*+ use_index(tmp1, u) */ id,u from tmp1 where u>101 order by id").Check(testkit.Rows(
			"3 113", "5 105", "7 117", "9 109", "10 110",
			"12 112", "14 114", "16 116", "18 118",
		))
		tk.MustQuery("show warnings").Check(testkit.Rows())

		// For IndexMerge, temporary table should not use index merge
		tk.MustQuery("select /*+ use_index_merge(tmp1, primary, u) */ * from tmp1 where id>5 or u>110 order by u").Check(testkit.Rows(
			"9 109 1009", "10 110 1010",
			"12 112 1012", "3 113 1003", "14 114 1014", "16 116 1016", "7 117 1007", "18 118 1018",
		))

		tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 IndexMerge is inapplicable or disabled. Cannot use IndexMerge on temporary table."))
	}

	doModify := func() {
		tk.MustExec("insert into tmp1 values(2, 100, 1002)")
		tk.MustExec("insert into tmp1 values(4, 104, 1004)")
		tk.MustExec("insert into tmp1 values(11, 111, 1011)")
		tk.MustExec("update tmp1 set v=9999 where id=7")
		tk.MustExec("update tmp1 set u=132 where id=12")
		tk.MustExec("delete from tmp1 where id=16")
	}

	assertSelectAsModified := func() {
		// For TableReader
		tk.MustQuery("select * from tmp1 where id>3 order by id").Check(testkit.Rows(
			"4 104 1004", "5 105 1005", "7 117 9999", "9 109 1009",
			"10 110 1010", "11 111 1011", "12 132 1012", "14 114 1014", "18 118 1018",
		))

		// For IndexLookUpReader
		tk.MustQuery("select /*+ use_index(tmp1, u) */ * from tmp1 where u>101 order by u").Check(testkit.Rows(
			"4 104 1004", "5 105 1005", "9 109 1009", "10 110 1010", "11 111 1011",
			"3 113 1003", "14 114 1014", "7 117 9999", "18 118 1018", "12 132 1012",
		))
		tk.MustQuery("show warnings").Check(testkit.Rows())

		// For IndexReader
		tk.MustQuery("select /*+ use_index(tmp1, u) */ id,u from tmp1 where u>101 order by id").Check(testkit.Rows(
			"3 113", "4 104", "5 105", "7 117", "9 109",
			"10 110", "11 111", "12 132", "14 114", "18 118",
		))
		tk.MustQuery("show warnings").Check(testkit.Rows())

		// For IndexMerge, temporary table should not use index merge
		tk.MustQuery("select /*+ use_index_merge(tmp1, primary, u) */ * from tmp1 where id>5 or u>110 order by u").Check(testkit.Rows(
			"9 109 1009", "10 110 1010", "11 111 1011",
			"3 113 1003", "14 114 1014", "7 117 9999", "18 118 1018", "12 132 1012",
		))

		tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 IndexMerge is inapplicable or disabled. Cannot use IndexMerge on temporary table."))
	}

	assertSelectAsUnModified()
	tk.MustExec("begin")
	assertSelectAsUnModified()
	doModify()
	tk.MustExec("rollback")
	assertSelectAsUnModified()
	tk.MustExec("begin")
	doModify()
	assertSelectAsModified()
	tk.MustExec("commit")
	assertSelectAsModified()
}

func (s *testSessionSuite) TestLocalTemporaryTableUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create temporary table tmp1 (id int primary key, u int unique, v int)")

	idList := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	insertRecords := func(idList []int) {
		for _, id := range idList {
			tk.MustExec("insert into tmp1 values (?, ?, ?)", id, id+100, id+1000)
		}
	}

	checkNoChange := func() {
		expect := make([]string, 0)
		for _, id := range idList {
			expect = append(expect, fmt.Sprintf("%d %d %d", id, id+100, id+1000))
		}
		tk.MustQuery("select * from tmp1").Check(testkit.Rows(expect...))
	}

	checkUpdatesAndDeletes := func(updates []string, deletes []int) {
		modifyMap := make(map[int]string)
		for _, m := range updates {
			parts := strings.Split(strings.TrimSpace(m), " ")
			c.Assert(len(parts) != 0, IsTrue)
			id, err := strconv.Atoi(parts[0])
			c.Assert(err, IsNil)
			modifyMap[id] = m
		}

		for _, d := range deletes {
			modifyMap[d] = ""
		}

		expect := make([]string, 0)
		for _, id := range idList {
			modify, exist := modifyMap[id]
			if !exist {
				expect = append(expect, fmt.Sprintf("%d %d %d", id, id+100, id+1000))
				continue
			}

			if modify != "" {
				expect = append(expect, modify)
			}

			delete(modifyMap, id)
		}

		otherIds := make([]int, 0)
		for id := range modifyMap {
			otherIds = append(otherIds, id)
		}

		sort.Ints(otherIds)
		for _, id := range otherIds {
			modify, exist := modifyMap[id]
			c.Assert(exist, IsTrue)
			expect = append(expect, modify)
		}

		tk.MustQuery("select * from tmp1").Check(testkit.Rows(expect...))
	}

	type checkSuccess struct {
		update []string
		delete []int
	}

	type checkError struct {
		err error
	}

	cases := []struct {
		sql             string
		checkResult     interface{}
		additionalCheck func(error)
	}{
		// update with point get for primary key
		{"update tmp1 set v=999 where id=1", checkSuccess{[]string{"1 101 999"}, nil}, nil},
		{"update tmp1 set id=12 where id=1", checkSuccess{[]string{"12 101 1001"}, []int{1}}, nil},
		{"update tmp1 set id=1 where id=1", checkSuccess{nil, nil}, nil},
		{"update tmp1 set u=101 where id=1", checkSuccess{nil, nil}, nil},
		{"update tmp1 set v=999 where id=100", checkSuccess{nil, nil}, nil},
		{"update tmp1 set u=102 where id=100", checkSuccess{nil, nil}, nil},
		{"update tmp1 set u=21 where id=1", checkSuccess{[]string{"1 21 1001"}, nil}, func(_ error) {
			// check index deleted
			tk.MustQuery("select /*+ use_index(tmp1, u) */ * from tmp1 where u=101").Check(testkit.Rows())
			tk.MustQuery("show warnings").Check(testkit.Rows())
		}},
		{"update tmp1 set id=2 where id=1", checkError{kv.ErrKeyExists}, nil},
		{"update tmp1 set u=102 where id=1", checkError{kv.ErrKeyExists}, nil},
		// update with batch point get for primary key
		{"update tmp1 set v=v+1000 where id in (1, 3, 5)", checkSuccess{[]string{"1 101 2001", "3 103 2003", "5 105 2005"}, nil}, nil},
		{"update tmp1 set u=u+1 where id in (9, 100)", checkSuccess{[]string{"9 110 1009"}, nil}, nil},
		{"update tmp1 set u=101 where id in (100, 101)", checkSuccess{nil, nil}, nil},
		{"update tmp1 set id=id+1 where id in (8, 9)", checkError{kv.ErrKeyExists}, nil},
		{"update tmp1 set u=u+1 where id in (8, 9)", checkError{kv.ErrKeyExists}, nil},
		{"update tmp1 set id=id+20 where id in (1, 3, 5)", checkSuccess{[]string{"21 101 1001", "23 103 1003", "25 105 1005"}, []int{1, 3, 5}}, nil},
		{"update tmp1 set u=u+100 where id in (1, 3, 5)", checkSuccess{[]string{"1 201 1001", "3 203 1003", "5 205 1005"}, nil}, func(_ error) {
			// check index deleted
			tk.MustQuery("select /*+ use_index(tmp1, u) */ * from tmp1 where u in (101, 103, 105)").Check(testkit.Rows())
			tk.MustQuery("show warnings").Check(testkit.Rows())
		}},
		// update with point get for unique key
		{"update tmp1 set v=888 where u=101", checkSuccess{[]string{"1 101 888"}, nil}, nil},
		{"update tmp1 set id=21 where u=101", checkSuccess{[]string{"21 101 1001"}, []int{1}}, nil},
		{"update tmp1 set v=888 where u=201", checkSuccess{nil, nil}, nil},
		{"update tmp1 set u=201 where u=101", checkSuccess{[]string{"1 201 1001"}, nil}, nil},
		{"update tmp1 set id=2 where u=101", checkError{kv.ErrKeyExists}, nil},
		{"update tmp1 set u=102 where u=101", checkError{kv.ErrKeyExists}, nil},
		// update with batch point get for unique key
		{"update tmp1 set v=v+1000 where u in (101, 103)", checkSuccess{[]string{"1 101 2001", "3 103 2003"}, nil}, nil},
		{"update tmp1 set v=v+1000 where u in (201, 203)", checkSuccess{nil, nil}, nil},
		{"update tmp1 set v=v+1000 where u in (101, 110)", checkSuccess{[]string{"1 101 2001"}, nil}, nil},
		{"update tmp1 set id=id+1 where u in (108, 109)", checkError{kv.ErrKeyExists}, nil},
		// update with table scan and index scan
		{"update tmp1 set v=v+1000 where id<3", checkSuccess{[]string{"1 101 2001", "2 102 2002"}, nil}, nil},
		{"update /*+ use_index(tmp1, u) */ tmp1 set v=v+1000 where u>107", checkSuccess{[]string{"8 108 2008", "9 109 2009"}, nil}, nil},
		{"update tmp1 set v=v+1000 where v>=1007 or v<=1002", checkSuccess{[]string{"1 101 2001", "2 102 2002", "7 107 2007", "8 108 2008", "9 109 2009"}, nil}, nil},
		{"update tmp1 set v=v+1000 where id>=10", checkSuccess{nil, nil}, nil},
		{"update tmp1 set id=id+1 where id>7", checkError{kv.ErrKeyExists}, nil},
		{"update tmp1 set id=id+1 where id>8", checkSuccess{[]string{"10 109 1009"}, []int{9}}, nil},
		{"update tmp1 set u=u+1 where u>107", checkError{kv.ErrKeyExists}, nil},
		{"update tmp1 set u=u+1 where u>108", checkSuccess{[]string{"9 110 1009"}, nil}, nil},
		{"update /*+ use_index(tmp1, u) */ tmp1 set v=v+1000 where u>108 or u<102", checkSuccess{[]string{"1 101 2001", "9 109 2009"}, nil}, nil},
	}

	executeSQL := func(sql string, checkResult interface{}, additionalCheck func(error)) (err error) {
		switch check := checkResult.(type) {
		case checkSuccess:
			tk.MustExec(sql)
			tk.MustQuery("show warnings").Check(testkit.Rows())
			checkUpdatesAndDeletes(check.update, check.delete)
		case checkError:
			err = tk.ExecToErr(sql)
			c.Assert(err, NotNil)
			expectedErr, _ := check.err.(*terror.Error)
			c.Assert(expectedErr.Equal(err), IsTrue)
			checkNoChange()
		default:
			c.Fail()
		}

		if additionalCheck != nil {
			additionalCheck(err)
		}
		return
	}

	for _, sqlCase := range cases {
		// update records in txn and records are inserted in txn
		tk.MustExec("begin")
		insertRecords(idList)
		_ = executeSQL(sqlCase.sql, sqlCase.checkResult, sqlCase.additionalCheck)
		tk.MustExec("rollback")
		tk.MustQuery("select * from tmp1").Check(testkit.Rows())

		// update records out of txn
		insertRecords(idList)
		_ = executeSQL(sqlCase.sql, sqlCase.checkResult, sqlCase.additionalCheck)
		tk.MustExec("delete from tmp1")

		// update records in txn and rollback
		insertRecords(idList)
		tk.MustExec("begin")
		_ = executeSQL(sqlCase.sql, sqlCase.checkResult, sqlCase.additionalCheck)
		tk.MustExec("rollback")
		// rollback left records unmodified
		checkNoChange()

		// update records in txn and commit
		tk.MustExec("begin")
		err := executeSQL(sqlCase.sql, sqlCase.checkResult, sqlCase.additionalCheck)
		tk.MustExec("commit")
		if err != nil {
			checkNoChange()
		} else {
			r, _ := sqlCase.checkResult.(checkSuccess)
			checkUpdatesAndDeletes(r.update, r.delete)
		}
		if sqlCase.additionalCheck != nil {
			sqlCase.additionalCheck(err)
		}
		tk.MustExec("delete from tmp1")
		tk.MustQuery("select * from tmp1").Check(testkit.Rows())
	}
}

func (s *testSessionSuite) TestTemporaryTableInterceptor(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create temporary table test.tmp1 (id int primary key)")
	tbl, err := tk.Se.GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("tmp1"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().TempTableType, Equals, model.TempTableLocal)
	tblID := tbl.Meta().ID

	// prepare a kv pair for temporary table
	k := append(tablecodec.EncodeTablePrefix(tblID), 1)
	err = tk.Se.GetSessionVars().TemporaryTableData.SetTableKey(tblID, k, []byte("v1"))
	c.Assert(err, IsNil)

	initTxnFuncs := []func() error{
		func() error {
			tk.Se.PrepareTSFuture(context.Background())
			return nil
		},
		func() error {
			return tk.Se.NewTxn(context.Background())
		},
		func() error {
			return tk.Se.NewStaleTxnWithStartTS(context.Background(), 0)
		},
		func() error {
			return tk.Se.InitTxnWithStartTS(0)
		},
	}

	for _, initFunc := range initTxnFuncs {
		err := initFunc()
		c.Assert(err, IsNil)

		txn, err := tk.Se.Txn(true)
		c.Assert(err, IsNil)

		val, err := txn.Get(context.Background(), k)
		c.Assert(err, IsNil)
		c.Assert(val, BytesEquals, []byte("v1"))

		val, err = txn.GetSnapshot().Get(context.Background(), k)
		c.Assert(err, IsNil)
		c.Assert(val, BytesEquals, []byte("v1"))

		tk.Se.RollbackTxn(context.Background())
	}

	// Also check GetSnapshotWithTS
	snap := tk.Se.GetSnapshotWithTS(0)
	val, err := snap.Get(context.Background(), k)
	c.Assert(err, IsNil)
	c.Assert(val, BytesEquals, []byte("v1"))
}

func (s *testTiDBAsLibrary) TestMemoryLeak(c *C) {
	initAndCloseTiDB := func() {
		store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
		c.Assert(err, IsNil)
		defer store.Close()

		dom, err := session.BootstrapSession(store)
		//nolint:staticcheck
		defer dom.Close()
		c.Assert(err, IsNil)
	}

	runtime.GC()
	memStat := runtime.MemStats{}
	runtime.ReadMemStats(&memStat)
	oldHeapInUse := memStat.HeapInuse

	for i := 0; i < 20; i++ {
		initAndCloseTiDB()
	}

	runtime.GC()
	runtime.ReadMemStats(&memStat)
	// before the fix, initAndCloseTiDB for 20 times will cost 900 MB memory, so we test for a quite loose upper bound.
	c.Assert(memStat.HeapInuse-oldHeapInUse, Less, uint64(300*units.MiB))
}

func (s *testSessionSuite) TestTiDBReadStaleness(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@tidb_read_staleness='-5'")
	err := tk.ExecToErr("set @@tidb_read_staleness='-5s'")
	c.Assert(err, NotNil)
	err = tk.ExecToErr("set @@tidb_read_staleness='foo'")
	c.Assert(err, NotNil)
	tk.MustExec("set @@tidb_read_staleness=''")
	tk.MustExec("set @@tidb_read_staleness='0'")
}

func (s *testSessionSuite) TestFixSetTiDBSnapshotTS(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
	tk.MustExec("create database t123")
	time.Sleep(time.Second)
	ts := time.Now().Format("2006-1-2 15:04:05")
	time.Sleep(time.Second)
	tk.MustExec("drop database t123")
	err := tk.ExecToErr("use t123")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, ".*Unknown database.*")
	tk.MustExec(fmt.Sprintf("set @@tidb_snapshot='%s'", ts))
	tk.MustExec("use t123")
	// update any session variable and assert whether infoschema is changed
	tk.MustExec("SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER';")
	tk.MustExec("use t123")
}

func (s *testSessionSuite) TestSetPDClientDynmaicOption(c *C) {
	var err error
	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 0.5;")
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("0.5"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 1;")
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 1.5;")
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("1.5"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 10;")
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("10"))
	err = tk.ExecToErr("set tidb_tso_client_batch_max_wait_time = 0;")
	c.Assert(err, NotNil)
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = -1;")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect tidb_tso_client_batch_max_wait_time value: '-1'"))
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = -0.1;")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect tidb_tso_client_batch_max_wait_time value: '-0.1'"))
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 10.1;")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect tidb_tso_client_batch_max_wait_time value: '10.1'"))
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("10"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 11;")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect tidb_tso_client_batch_max_wait_time value: '11'"))
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("10"))

	tk.MustQuery("select @@tidb_enable_tso_follower_proxy;").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_enable_tso_follower_proxy = on;")
	tk.MustQuery("select @@tidb_enable_tso_follower_proxy;").Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_enable_tso_follower_proxy = off;")
	tk.MustQuery("select @@tidb_enable_tso_follower_proxy;").Check(testkit.Rows("0"))
	err = tk.ExecToErr("set tidb_tso_client_batch_max_wait_time = 0;")
	c.Assert(err, NotNil)
}

func (s *testSessionSuite) TestSameNameObjectWithLocalTemporaryTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop sequence if exists s1")
	tk.MustExec("drop view if exists v1")

	// prepare
	tk.MustExec("create table t1 (a int)")
	defer tk.MustExec("drop table if exists t1")
	tk.MustQuery("show create table t1").Check(testkit.Rows(
		"t1 CREATE TABLE `t1` (\n" +
			"  `a` int(11) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("create view v1 as select 1")
	defer tk.MustExec("drop view if exists v1")
	tk.MustQuery("show create view v1").Check(testkit.Rows("v1 CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `v1` (`1`) AS SELECT 1 AS `1` utf8mb4 utf8mb4_bin"))
	tk.MustQuery("show create table v1").Check(testkit.Rows("v1 CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `v1` (`1`) AS SELECT 1 AS `1` utf8mb4 utf8mb4_bin"))

	tk.MustExec("create sequence s1")
	defer tk.MustExec("drop sequence if exists s1")
	tk.MustQuery("show create sequence s1").Check(testkit.Rows("s1 CREATE SEQUENCE `s1` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))
	tk.MustQuery("show create table s1").Check(testkit.Rows("s1 CREATE SEQUENCE `s1` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))

	// temp table
	tk.MustExec("create temporary table t1 (ct1 int)")
	tk.MustQuery("show create table t1").Check(testkit.Rows(
		"t1 CREATE TEMPORARY TABLE `t1` (\n" +
			"  `ct1` int(11) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("create temporary table v1 (cv1 int)")
	tk.MustQuery("show create view v1").Check(testkit.Rows("v1 CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `v1` (`1`) AS SELECT 1 AS `1` utf8mb4 utf8mb4_bin"))
	tk.MustQuery("show create table v1").Check(testkit.Rows(
		"v1 CREATE TEMPORARY TABLE `v1` (\n" +
			"  `cv1` int(11) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("create temporary table s1 (cs1 int)")
	tk.MustQuery("show create sequence s1").Check(testkit.Rows("s1 CREATE SEQUENCE `s1` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))
	tk.MustQuery("show create table s1").Check(testkit.Rows(
		"s1 CREATE TEMPORARY TABLE `s1` (\n" +
			"  `cs1` int(11) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// drop
	tk.MustExec("drop view v1")
	err := tk.ExecToErr("show create view v1")
	c.Assert(err.Error(), Equals, "[schema:1146]Table 'test.v1' doesn't exist")
	tk.MustQuery("show create table v1").Check(testkit.Rows(
		"v1 CREATE TEMPORARY TABLE `v1` (\n" +
			"  `cv1` int(11) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("drop sequence s1")
	err = tk.ExecToErr("show create sequence s1")
	c.Assert(err.Error(), Equals, "[schema:1146]Table 'test.s1' doesn't exist")
	tk.MustQuery("show create table s1").Check(testkit.Rows(
		"s1 CREATE TEMPORARY TABLE `s1` (\n" +
			"  `cs1` int(11) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func (s *testSessionSuite) TestWriteOnMultipleCachedTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ct1, ct2")
	tk.MustExec("create table ct1 (id int, c int)")
	tk.MustExec("create table ct2 (id int, c int)")
	tk.MustExec("alter table ct1 cache")
	tk.MustExec("alter table ct2 cache")
	tk.MustQuery("select * from ct1").Check(testkit.Rows())
	tk.MustQuery("select * from ct2").Check(testkit.Rows())

	lastReadFromCache := func(tk *testkit.TestKit) bool {
		return tk.Se.GetSessionVars().StmtCtx.ReadFromTableCache
	}

	cached := false
	for i := 0; i < 50; i++ {
		tk.MustQuery("select * from ct1")
		if lastReadFromCache(tk) {
			cached = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.Assert(cached, IsTrue)

	tk.MustExec("begin")
	tk.MustExec("insert into ct1 values (3, 4)")
	tk.MustExec("insert into ct2 values (5, 6)")
	tk.MustExec("commit")

	tk.MustQuery("select * from ct1").Check(testkit.Rows("3 4"))
	tk.MustQuery("select * from ct2").Check(testkit.Rows("5 6"))
}

func (s *testSessionSuite) TestForbidSettingBothTSVariable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20060102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)

	// Set tidb_snapshot and assert tidb_read_staleness
	tk.MustExec("set @@tidb_snapshot = '2007-01-01 15:04:05.999999'")
	_, err := tk.Exec("set @@tidb_read_staleness='-5'")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "tidb_snapshot should be clear before setting tidb_read_staleness")
	tk.MustExec("set @@tidb_snapshot = ''")
	tk.MustExec("set @@tidb_read_staleness='-5'")

	// Set tidb_read_staleness and assert tidb_snapshot
	tk.MustExec("set @@tidb_read_staleness='-5'")
	_, err = tk.Exec("set @@tidb_snapshot = '2007-01-01 15:04:05.999999'")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "tidb_read_staleness should be clear before setting tidb_snapshot")
	tk.MustExec("set @@tidb_read_staleness = ''")
	tk.MustExec("set @@tidb_snapshot = '2007-01-01 15:04:05.999999'")
}

func (s *testSessionSuite) TestSysdateIsNow(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustQuery("show variables like '%tidb_sysdate_is_now%'").Check(testkit.Rows("tidb_sysdate_is_now OFF"))
	c.Assert(tk.Se.GetSessionVars().SysdateIsNow, IsFalse)
	tk.MustExec("set @@tidb_sysdate_is_now=true")
	tk.MustQuery("show variables like '%tidb_sysdate_is_now%'").Check(testkit.Rows("tidb_sysdate_is_now ON"))
	c.Assert(tk.Se.GetSessionVars().SysdateIsNow, IsTrue)
}

func (s *testSessionSuite) TestEnableLegacyInstanceScope(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	// enable 'switching' to SESSION variables
	tk.MustExec("set tidb_enable_legacy_instance_scope = 1")
	tk.MustExec("set tidb_general_log = 1")
	tk.MustQuery(`show warnings`).Check(testkit.Rows(fmt.Sprintf("Warning %d modifying tidb_general_log will require SET GLOBAL in a future version of TiDB", errno.ErrInstanceScope)))
	c.Assert(tk.Se.GetSessionVars().EnableLegacyInstanceScope, IsTrue)

	// disable 'switching' to SESSION variables
	tk.MustExec("set tidb_enable_legacy_instance_scope = 0")
	tk.MustGetErrCode("set tidb_general_log = 1", errno.ErrGlobalVariable)
	c.Assert(tk.Se.GetSessionVars().EnableLegacyInstanceScope, IsFalse)
}
