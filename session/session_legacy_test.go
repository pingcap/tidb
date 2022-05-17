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
	"strconv"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/driver"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/tikv/client-go/v2/testutils"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

var withTiKV = flag.Bool("with-tikv", false, "run tests with TiKV cluster started. (not use the mock server)")

var _ = flag.String("pd-addrs", "127.0.0.1:2379", "workaroundGoCheckFlags: pd-addrs")

var _ = Suite(&testSessionSuite{})

type testSessionSuiteBase struct {
	cluster testutils.Cluster
	store   kv.Storage
	dom     *domain.Domain
}

type testSessionSuite struct {
	testSessionSuiteBase
}

func clearTiKVStorage(store kv.Storage) error {
	txn, err := store.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	iter, err := txn.Iter(nil, nil)
	if err != nil {
		return errors.Trace(err)
	}
	for iter.Valid() {
		if err := txn.Delete(iter.Key()); err != nil {
			return errors.Trace(err)
		}
		if err := iter.Next(); err != nil {
			return errors.Trace(err)
		}
	}
	return txn.Commit(context.Background())
}

func clearEtcdStorage(ebd kv.EtcdBackend) error {
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

func (s *testSessionSuiteBase) SetUpSuite(c *C) {
	testleak.BeforeTest()

	if *withTiKV {
		var d driver.TiKVDriver
		config.UpdateGlobal(func(conf *config.Config) {
			conf.TxnLocalLatches.Enabled = false
		})
		store, err := d.Open("tikv://127.0.0.1:2379?disableGC=true")
		c.Assert(err, IsNil)
		err = clearTiKVStorage(store)
		c.Assert(err, IsNil)
		err = clearEtcdStorage(store.(kv.EtcdBackend))
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
	varValue := strconv.FormatUint(variable.DefMaxAllowedPacket, 10) // This is the default value for max_allowed_packet

	// The value of max_allowed_packet should be a multiple of 1024,
	// so the setting of varValue1 and varValue2 would be truncated to varValue0
	varValue0 := "4194304"
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
	c.Assert(v, Equals, varValue0)
	c.Assert(tk.Se.CommitTxn(context.TODO()), IsNil)

	tk1 := testkit.NewTestKitWithInit(c, s.store)
	se1 := tk1.Se.(variable.GlobalVarAccessor)
	v, err = se1.GetGlobalSysVar(varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, varValue0)
	err = se1.SetGlobalSysVar(varName, varValue2)
	c.Assert(err, IsNil)
	v, err = se1.GetGlobalSysVar(varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, varValue0)
	c.Assert(tk1.Se.CommitTxn(context.TODO()), IsNil)

	// Make sure the change is visible to any client that accesses that global variable.
	v, err = se.GetGlobalSysVar(varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, varValue0)

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

func (s *testSessionSuite) TestSession(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("ROLLBACK;")
	tk.Se.Close()
}

func (s *testSessionSuite) TestSessionAuth(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "Any not exist username with zero password!", Hostname: "anyhost"}, []byte(""), []byte("")), IsFalse)
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
