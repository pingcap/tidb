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
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/store/copr"
	"github.com/pingcap/tidb/store/driver"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

var withTiKV = flag.Bool("with-tikv", false, "run tests with TiKV cluster started. (not use the mock server)")

var _ = flag.String("pd-addrs", "127.0.0.1:2379", "workaroundGoCheckFlags: pd-addrs")

var _ = Suite(&testSessionSuite{})
var _ = Suite(&testSessionSuite2{})
var _ = Suite(&testSessionSuite3{})
var _ = SerialSuites(&testSessionSerialSuite{})

type testSessionSuiteBase struct {
	cluster testutils.Cluster
	store   kv.Storage
	dom     *domain.Domain
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
	c.Assert(err.Error(), Equals, "[ddl:1470]String 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' is too long for host name (should be no longer than 255)")
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
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect group_concat_max_len value: '1'"))
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

	// Test STRAIGHT_JOIN hint
	tk.MustExec("select /*+ straight_join() */ 1;")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.StraightJoinOrder, IsTrue)
	tk.MustExec("select /*+ straight_join(), straight_join() */ 1;")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 1)

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
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")
	tk.MustExec("create placement policy p1 leader_constraints='[+zone=dc-1]'")
	tk.MustExec("create placement policy p2 leader_constraints='[+zone=dc-2]'")
	tk.MustExec(`create table t1 (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (100) placement policy p1,
	PARTITION p1 VALUES LESS THAN (200) placement policy p2
);`)
	defer func() {
		tk.MustExec("drop table if exists t1")
		tk.MustExec("drop placement policy if exists p1")
		tk.MustExec("drop placement policy if exists p2")
	}()

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
	tk.MustExec("use test")
	tk.MustExec("create table tmp123(id int)")
	tk.MustQuery("select * from tmp123;")
	haveRateLimitAction := false
	action := tk.Se.GetSessionVars().StmtCtx.MemTracker.GetFallbackForTest(false)
	for ; action != nil; action = action.GetFallback() {
		if action.GetPriority() == memory.DefRateLimitPriority {
			haveRateLimitAction = true
			break
		}
	}
	c.Assert(haveRateLimitAction, IsTrue)

	// assert set sys variable
	tk.MustExec("set global tidb_enable_rate_limit_action= '0';")
	tk.Se.Close()

	se, err := session.CreateSession4Test(s.store)
	c.Check(err, IsNil)
	tk.Se = se
	result = tk.MustQuery("select @@tidb_enable_rate_limit_action;")
	result.Check(testkit.Rows("0"))

	haveRateLimitAction = false
	action = tk.Se.GetSessionVars().StmtCtx.MemTracker.GetFallbackForTest(false)
	for ; action != nil; action = action.GetFallback() {
		if action.GetPriority() == memory.DefRateLimitPriority {
			haveRateLimitAction = true
			break
		}
	}
	c.Assert(haveRateLimitAction, IsFalse)
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

	tk.MustExec("set @@global.tidb_memory_usage_alarm_ratio=1")
	tk.MustQuery("select @@global.tidb_memory_usage_alarm_ratio").Check(testkit.Rows("1"))
	tk.MustExec("set @@global.tidb_memory_usage_alarm_ratio=0")
	tk.MustQuery("select @@global.tidb_memory_usage_alarm_ratio").Check(testkit.Rows("0"))
	tk.MustExec("set @@global.tidb_memory_usage_alarm_ratio=0.7")
	tk.MustQuery("select @@global.tidb_memory_usage_alarm_ratio").Check(testkit.Rows("0.7"))
	tk.MustExec("set @@global.tidb_memory_usage_alarm_ratio=1.1")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_memory_usage_alarm_ratio value: '1.1'"))
	tk.MustQuery("select @@global.tidb_memory_usage_alarm_ratio").Check(testkit.Rows("1"))

	tk.MustExec("set @@global.tidb_memory_usage_alarm_ratio=-1")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_memory_usage_alarm_ratio value: '-1'"))
	tk.MustQuery("select @@global.tidb_memory_usage_alarm_ratio").Check(testkit.Rows("0"))

	tk.MustExec("set @@session.tidb_memory_usage_alarm_ratio=0.8")
	tk.MustQuery(`show warnings`).Check(testkit.Rows(fmt.Sprintf("Warning %d modifying tidb_memory_usage_alarm_ratio will require SET GLOBAL in a future version of TiDB", errno.ErrInstanceScope)))
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
			return sessiontxn.NewTxn(context.Background(), tk.Se)
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
