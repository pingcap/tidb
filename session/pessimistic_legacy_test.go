// Copyright 2019 PingCAP, Inc.
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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	storeerr "github.com/pingcap/tidb/store/driver/error"
	"github.com/tikv/client-go/v2/txnkv/transaction"

	"github.com/pingcap/tidb/util/testkit"
)

var _ = SerialSuites(&testPessimisticSuite{})

type testPessimisticSuite struct {
	testSessionSuiteBase
}

func (s *testPessimisticSuite) SetUpSuite(c *C) {
	s.testSessionSuiteBase.SetUpSuite(c)
	// Set it to 5s for testing lock resolve.
	atomic.StoreUint64(&transaction.ManagedLockTTL, 5000)
	transaction.PrewriteMaxBackoff = 500
}

func (s *testPessimisticSuite) TearDownSuite(c *C) {
	s.testSessionSuiteBase.TearDownSuite(c)
	transaction.PrewriteMaxBackoff = 20000
}

func (s *testPessimisticSuite) TestAmendForIndexChange(c *C) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
	})
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("set tidb_enable_amend_pessimistic_txn = ON;")
	tk.Se.GetSessionVars().EnableAsyncCommit = false
	tk.Se.GetSessionVars().Enable1PC = false
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop database if exists test_db")
	tk.MustExec("create database test_db")
	tk.MustExec("use test_db")
	tk2.MustExec("use test_db")
	tk2.MustExec("drop table if exists t1")

	// Add some different column types.
	columnNames := []string{"c_int", "c_str", "c_datetime", "c_timestamp", "c_double", "c_decimal", "c_float"}
	columnTypes := []string{"int", "varchar(40)", "datetime", "timestamp", "double", "decimal(12, 6)", "float"}

	addIndexFunc := func(idxName string, part bool, a, b int) string {
		var str string
		str = "alter table t"
		if part {
			str = "alter table t_part"
		}
		str += " add index " + idxName + " ("
		str += strings.Join(columnNames[a:b], ",")
		str += ")"
		return str
	}

	for i := 0; i < len(columnTypes); i++ {
		for j := i + 1; j <= len(columnTypes); j++ {
			// Create table and prepare some data.
			tk2.MustExec("drop table if exists t")
			tk2.MustExec("drop table if exists t_part")
			tk2.MustExec(createTable(false, columnNames, columnTypes))
			tk2.MustExec(createTable(true, columnNames, columnTypes))
			tk2.MustExec(`insert into t values(1, "1", "2000-01-01", "2020-01-01", "1.1", "123.321", 1.1)`)
			tk2.MustExec(`insert into t values(2, "2", "2000-01-02", "2020-01-02", "2.2", "223.322", 2.2)`)
			tk2.MustExec(`insert into t_part values(1, "1", "2000-01-01", "2020-01-01", "1.1", "123.321", 1.1)`)
			tk2.MustExec(`insert into t_part values(2, "2", "2000-01-02", "2020-01-02", "2.2", "223.322", 2.2)`)

			// Start a pessimistic transaction, the amend should succeed for common table.
			tk.MustExec("begin pessimistic")
			tk.MustExec(`insert into t values(5, "555", "2000-01-05", "2020-01-05", "5.5", "555.555", 5.5)`)
			idxName := fmt.Sprintf("index%d%d", i, j)
			tk2.MustExec(addIndexFunc(idxName, false, i, j))
			tk.MustExec("commit")
			tk2.MustExec("admin check table t")

			tk.MustExec("begin pessimistic")
			tk.MustExec(`insert into t values(6, "666", "2000-01-06", "2020-01-06", "6.6", "666.666", 6.6)`)
			tk2.MustExec(fmt.Sprintf(`alter table t drop index %s`, idxName))
			tk.MustExec("commit")
			tk2.MustExec("admin check table t")
			tk2.MustQuery("select count(*) from t").Check(testkit.Rows("4"))

			// Start a pessimistic transaction for partition table, the amend should fail.
			tk.MustExec("begin pessimistic")
			tk.MustExec(`insert into t_part values(5, "555", "2000-01-05", "2020-01-05", "5.5", "555.555", 5.5)`)
			tk2.MustExec(addIndexFunc(idxName, true, i, j))
			c.Assert(tk.ExecToErr("commit"), NotNil)
			tk2.MustExec("admin check table t_part")

			tk.MustExec("begin pessimistic")
			tk.MustExec(`insert into t_part values(6, "666", "2000-01-06", "2020-01-06", "6.6", "666.666", 6.6)`)
			tk2.MustExec(fmt.Sprintf(`alter table t_part drop index %s`, idxName))
			c.Assert(tk.ExecToErr("commit"), NotNil)
			tk2.MustExec("admin check table t_part")
			tk2.MustQuery("select count(*) from t_part").Check(testkit.Rows("2"))
		}
	}

	tk2.MustExec("drop database test_db")
}

func (s *testPessimisticSuite) TestAmendForColumnChange(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("set tidb_enable_amend_pessimistic_txn = ON;")
	tk.MustExec("drop database if exists test_db")
	tk.MustExec("create database test_db")
	tk.MustExec("use test_db")
	tk2.MustExec("use test_db")
	tk2.MustExec("drop table if exists t1")

	// Add some different column types.
	columnNames := []string{"c_int", "c_str", "c_datetime", "c_timestamp", "c_double", "c_decimal", "c_float"}
	columnTypes := []string{"int", "varchar(40)", "datetime", "timestamp", "double", "decimal(12, 6)", "float"}
	colChangeDDLs := []string{
		"alter table %s change column c_int c_int bigint",
		"alter table %s modify column c_str varchar(55)",
		"alter table %s modify column c_datetime datetime",
		"alter table %s modify column c_timestamp timestamp",
		"alter table %s modify column c_double double default NULL",
		"alter table %s modify column c_int bigint(20) default 100",
		"alter table %s change column c_float c_float float",
		"alter table %s modify column c_int bigint(20)",
	}
	amendSucc := []bool{
		true,
		true,
		true,
		true,
		true,
		false,
		true,
		true,
	}
	colChangeFunc := func(part bool, i int) string {
		var sql string
		sql = colChangeDDLs[i]
		if part {
			sql = fmt.Sprintf(sql, "t_part")
		} else {
			sql = fmt.Sprintf(sql, "t")
		}
		return sql
	}

	for i := 0; i < len(colChangeDDLs); i++ {
		// Create table and prepare some data.
		tk2.MustExec("drop table if exists t")
		tk2.MustExec("drop table if exists t_part")
		tk2.MustExec(createTable(false, columnNames, columnTypes))
		tk2.MustExec(createTable(true, columnNames, columnTypes))
		tk2.MustExec(`insert into t values(1, "1", "2000-01-01", "2020-01-01", "1.1", "123.321", 1.1)`)
		tk2.MustExec(`insert into t values(2, "2", "2000-01-02", "2020-01-02", "2.2", "223.322", 2.2)`)
		tk2.MustExec(`insert into t_part values(1, "1", "2000-01-01", "2020-01-01", "1.1", "123.321", 1.1)`)
		tk2.MustExec(`insert into t_part values(2, "2", "2000-01-02", "2020-01-02", "2.2", "223.322", 2.2)`)

		// Start a pessimistic transaction, the amend should succeed for common table.
		tk.MustExec("begin pessimistic")
		tk.MustExec(`insert into t values(5, "555", "2000-01-05", "2020-01-05", "5.5", "555.555", 5.5)`)
		tk2.MustExec(colChangeFunc(false, i))
		if amendSucc[i] {
			tk.MustExec("commit")
		} else {
			c.Assert(tk.ExecToErr("commit"), NotNil)
		}
		tk2.MustExec("admin check table t")
		if amendSucc[i] {
			tk2.MustQuery("select count(*) from t").Check(testkit.Rows("3"))
		} else {
			tk2.MustQuery("select count(*) from t").Check(testkit.Rows("2"))
		}

		// Start a pessimistic transaction for partition table, the amend should fail.
		tk.MustExec("begin pessimistic")
		tk.MustExec(`insert into t_part values(5, "555", "2000-01-05", "2020-01-05", "5.5", "555.555", 5.5)`)
		tk2.MustExec(colChangeFunc(true, i))
		c.Assert(tk.ExecToErr("commit"), NotNil)
		tk2.MustExec("admin check table t_part")
		tk2.MustQuery("select count(*) from t_part").Check(testkit.Rows("2"))
	}

	tk2.MustExec("drop database test_db")
}

func (s *testPessimisticSuite) TestAmendForUniqueIndex(c *C) {
	c.Skip("Skip this unstable test(#25986) and bring it back before 2021-07-29.")
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("set tidb_enable_amend_pessimistic_txn = 1;")
	tk.MustExec("drop database if exists test_db")
	tk.MustExec("create database test_db")
	tk.MustExec("use test_db")
	tk2.MustExec("use test_db")
	tk2.MustExec("drop table if exists t1")
	tk2.MustExec("create table t1(c1 int primary key, c2 int, c3 int, unique key uk(c2));")
	tk2.MustExec("insert into t1 values(1, 1, 1);")
	tk2.MustExec("insert into t1 values(2, 2, 2);")

	// New value has duplicates.
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t1 values(3, 3, 3)")
	tk.MustExec("insert into t1 values(4, 4, 3)")
	tk2.MustExec("alter table t1 add unique index uk1(c3)")
	err := tk.ExecToErr("commit")
	c.Assert(err, NotNil)
	tk2.MustExec("alter table t1 drop index uk1")
	tk2.MustExec("admin check table t1")

	// New values has duplicates with old values.
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t1 values(3, 3, 3)")
	tk.MustExec("insert into t1 values(4, 4, 1)")
	tk2.MustExec("alter table t1 add unique index uk1(c3)")
	err = tk.ExecToErr("commit")
	c.Assert(err, NotNil)
	tk2.MustExec("admin check table t1")

	// Put new values.
	tk2.MustQuery("select * from t1 for update").Check(testkit.Rows("1 1 1", "2 2 2"))
	tk2.MustExec("alter table t1 drop index uk1")
	tk.MustExec("begin pessimistic")
	tk2.MustExec("alter table t1 add unique index uk1(c3)")
	tk.MustExec("insert into t1 values(5, 5, 5)")
	tk.MustExec("commit")
	tk2.MustExec("admin check table t1")

	// Update the old value with same unique key value, should abort.
	tk2.MustExec("drop table if exists t;")
	tk2.MustExec("create table t (id int auto_increment primary key, c int);")
	tk2.MustExec("insert into t (id, c) values (1, 2), (3, 4);")
	tk.MustExec("begin pessimistic")
	tk2.MustExec("alter table t add unique index uk(c);")
	tk.MustExec("update t set c = 2 where id = 3;")
	err = tk.ExecToErr("commit")
	c.Assert(err, NotNil)
	tk2.MustExec("admin check table t")

	// Update the old value with same unique key, but the row key has changed.
	tk2.MustExec("drop table if exists t;")
	tk2.MustExec("create table t (id int auto_increment primary key, c int);")
	tk2.MustExec("insert into t (id, c) values (1, 2), (3, 4);")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t values (3, 2) on duplicate key update id = values(id) and c = values(c)")
	finishCh := make(chan error)
	go func() {
		err := tk2.ExecToErr("alter table t add unique index uk(c);")
		finishCh <- err
	}()
	time.Sleep(300 * time.Millisecond)
	tk.MustExec("commit")
	err = <-finishCh
	c.Assert(err, IsNil)
	tk2.MustExec("admin check table t")

	// Update the old value with same unique key, but the row key has changed.
	/* TODO this case could not pass using unistore because of https://github.com/ngaut/unistore/issues/428.
	// Reopen it after fix the unistore issue.
	tk2.MustExec("drop table if exists t;")
	tk2.MustExec("create table t (id int auto_increment primary key, c int);")
	tk2.MustExec("insert into t (id, c) values (1, 2), (3, 4);")
	tk.MustExec("begin pessimistic")
	tk2.MustExec("alter table t add unique index uk(c);")
	tk.MustExec("insert into t values (3, 2) on duplicate key update id = values(id) and c = values(c)")
	tk.MustExec("commit")
	tk2.MustExec("admin check table t")
	*/

	// Test pessimistic retry for unique index amend.
	tk2.MustExec("drop table if exists t;")
	tk2.MustExec("create table t (id int key, c int);")
	tk2.MustExec("insert into t (id, c) values (1, 1), (2, 2);")
	tk.MustExec("begin pessimistic")
	tk2.MustExec("alter table t add unique index uk(c)")
	tk.MustExec("insert into t values(3, 5)")
	tk.MustExec("update t set c = 4 where c = 2")
	errCh := make(chan error, 1)
	go func() {
		var err error
		err = tk2.ExecToErr("begin pessimistic")
		if err != nil {
			errCh <- err
			return
		}
		err = tk2.ExecToErr("insert into t values(5, 5)")
		if err != nil {
			errCh <- err
			return
		}
		err = tk2.ExecToErr("delete from t where id = 5")
		if err != nil {
			errCh <- err
			return
		}
		// let commit in tk start.
		errCh <- err
		time.Sleep(time.Millisecond * 100)
		err = tk2.ExecToErr("commit")
		errCh <- err
	}()
	err = <-errCh
	c.Assert(err, Equals, nil)
	tk.MustExec("commit")
	tk.MustExec("admin check table t")
	err = <-errCh
	c.Assert(err, Equals, nil)
}

func (s *testPessimisticSuite) TestAmendWithColumnTypeChange(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop database if exists test_db")
	tk.MustExec("create database test_db")
	tk.MustExec("use test_db")
	tk2.MustExec("use test_db")
	tk.MustExec("set tidb_enable_amend_pessimistic_txn = 1;")

	tk2.MustExec("drop table if exists t")
	tk2.MustExec("create table t (id int primary key, v varchar(10));")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t values (1, \"123456789\")")
	tk2.MustExec("alter table t modify column v varchar(5);")
	c.Assert(tk.ExecToErr("commit"), NotNil)
}

func (s *testPessimisticSuite) TestAmendTxnVariable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk3 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop database if exists test_db")
	tk.MustExec("create database test_db")
	tk.MustExec("use test_db")
	tk2.MustExec("use test_db")
	tk2.MustExec("drop table if exists t1")
	tk2.MustExec("create table t1(c1 int primary key, c2 int, c3 int, unique key uk(c2));")
	tk2.MustExec("insert into t1 values(1, 1, 1);")
	tk2.MustExec("insert into t1 values(2, 2, 2);")
	tk3.MustExec("use test_db")

	// Set off the session variable.
	tk3.MustExec("set tidb_enable_amend_pessimistic_txn = 0;")
	tk3.MustExec("begin pessimistic")
	tk3.MustExec("insert into t1 values(3, 3, 3)")
	tk.MustExec("set tidb_enable_amend_pessimistic_txn = 1;")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t1 values(4, 4, 4)")
	tk2.MustExec("alter table t1 add column new_col int")
	err := tk3.ExecToErr("commit")
	c.Assert(err, NotNil)
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 1 1 <nil>", "2 2 2 <nil>", "4 4 4 <nil>"))
	tk.MustExec("set tidb_enable_amend_pessimistic_txn = 0;")

	// Set off the global variable.
	tk2.MustExec("set global tidb_enable_amend_pessimistic_txn = 0;")
	tk4 := testkit.NewTestKitWithInit(c, s.store)
	tk4.MustQuery(`show variables like "tidb_enable_amend_pessimistic_txn"`).Check(testkit.Rows("tidb_enable_amend_pessimistic_txn OFF"))
	tk4.MustExec("use test_db")
	tk4.MustExec("begin pessimistic")
	tk4.MustExec("insert into t1 values(5, 5, 5, 5)")
	tk2.MustExec("alter table t1 drop column new_col")
	err = tk4.ExecToErr("commit")
	c.Assert(err, NotNil)
	tk4.MustExec("set tidb_enable_amend_pessimistic_txn = 1;")
	tk4.MustExec("begin pessimistic")
	tk4.MustExec("insert into t1 values(5, 5, 5)")
	tk2.MustExec("alter table t1 add column new_col2 int")
	tk4.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 1 1 <nil>", "2 2 2 <nil>", "4 4 4 <nil>", "5 5 5 <nil>"))
}

func (s *testPessimisticSuite) TestPessimisticTxnWithDDLChangeColumn(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop database if exists test_db")
	tk.MustExec("create database test_db")
	tk.MustExec("use test_db")
	tk2.MustExec("use test_db")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int primary key, c2 int, c3 varchar(10))")
	tk.MustExec("insert t1 values (1, 77, 'a'), (2, 88, 'b')")

	// Extend column field length is acceptable.
	tk.MustExec("set tidb_enable_amend_pessimistic_txn = 1;")
	tk.MustExec("begin pessimistic")
	tk.MustExec("update t1 set c2 = c1 * 10")
	tk2.MustExec("alter table t1 modify column c2 bigint")
	tk.MustExec("commit")
	tk.MustExec("begin pessimistic")
	tk.MustExec("update t1 set c3 = 'aba'")
	tk2.MustExec("alter table t1 modify column c3 varchar(30)")
	tk.MustExec("commit")
	tk2.MustExec("admin check table t1")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 10 aba", "2 20 aba"))

	// Change column from nullable to not null is not allowed by now.
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t1(c1) values(100)")
	tk2.MustExec("alter table t1 change column c2 cc2 bigint not null")
	err := tk.ExecToErr("commit")
	c.Assert(err, NotNil)

	// Change default value is rejected.
	tk2.MustExec("create table ta(a bigint primary key auto_random(3), b varchar(255) default 'old');")
	tk2.MustExec("insert into ta(b) values('a')")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into ta values()")
	tk2.MustExec("alter table ta modify column b varchar(300) default 'new';")
	err = tk.ExecToErr("commit")
	c.Assert(err, NotNil)
	tk2.MustQuery("select b from ta").Check(testkit.Rows("a"))

	// Change default value with add index. There is a new MultipleKeyFlag flag on the index key, and the column is changed,
	// the flag check will fail.
	tk2.MustExec("insert into ta values()")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into ta(b) values('inserted_value')")
	tk.MustExec("insert into ta values()")
	tk.MustExec("insert into ta values()")
	tk2.MustExec("alter table ta add index i1(b)")
	tk2.MustExec("alter table ta change column b b varchar(301) default 'newest'")
	tk2.MustExec("alter table ta modify column b varchar(301) default 'new'")
	c.Assert(tk.ExecToErr("commit"), NotNil)
	tk2.MustExec("admin check table ta")
	tk2.MustQuery("select count(b) from ta use index(i1) where b = 'new'").Check(testkit.Rows("1"))

	// Change default value to now().
	tk2.MustExec("create table tbl_time(c1 int, c_time timestamp)")
	tk2.MustExec("insert into tbl_time(c1) values(1)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into tbl_time(c1) values(2)")
	tk2.MustExec("alter table tbl_time modify column c_time timestamp default now()")
	tk2.MustExec("insert into tbl_time(c1) values(3)")
	tk2.MustExec("insert into tbl_time(c1) values(4)")
	c.Assert(tk.ExecToErr("commit"), NotNil)
	tk2.MustQuery("select count(1) from tbl_time where c_time is not null").Check(testkit.Rows("2"))

	tk2.MustExec("drop database if exists test_db")
}

func (s *testPessimisticSuite) TestInnodbLockWaitTimeout(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists tk")
	tk.MustExec("create table tk (c1 int primary key, c2 int)")
	tk.MustExec("insert into tk values(1,1),(2,2),(3,3),(4,4),(5,5)")
	// tk set global
	tk.MustExec("set global innodb_lock_wait_timeout = 3")
	tk.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 50"))

	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk2.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 3"))
	tk2.MustExec("set innodb_lock_wait_timeout = 2")
	tk2.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 2"))
	// to check whether it will set to innodb_lock_wait_timeout to max value
	tk2.MustExec("set innodb_lock_wait_timeout = 3602")
	tk2.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 3600"))
	tk2.MustExec("set innodb_lock_wait_timeout = 2")

	tk3 := testkit.NewTestKitWithInit(c, s.store)
	tk3.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 3"))
	tk3.MustExec("set innodb_lock_wait_timeout = 1")
	tk3.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 1"))

	tk2.MustExec("set @@autocommit = 0")
	tk3.MustExec("set @@autocommit = 0")

	tk4 := testkit.NewTestKitWithInit(c, s.store)
	tk4.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 3"))
	tk4.MustExec("set @@autocommit = 0")

	// tk2 lock c1 = 1
	tk2.MustExec("begin pessimistic")
	tk2.MustExec("select * from tk where c1 = 1 for update") // lock succ c1 = 1

	// Parallel the blocking tests to accelerate CI.
	var wg sync.WaitGroup
	wg.Add(2)
	timeoutErrCh := make(chan error, 2)
	go func() {
		defer wg.Done()
		// tk3 try lock c1 = 1 timeout 1sec
		tk3.MustExec("begin pessimistic")
		_, err := tk3.Exec("select * from tk where c1 = 1 for update")
		timeoutErrCh <- err
		tk3.MustExec("commit")
	}()

	go func() {
		defer wg.Done()
		// tk5 try lock c1 = 1 timeout 2sec
		tk5 := testkit.NewTestKitWithInit(c, s.store)
		tk5.MustExec("set innodb_lock_wait_timeout = 2")
		tk5.MustExec("begin pessimistic")
		_, err := tk5.Exec("update tk set c2 = c2 - 1 where c1 = 1")
		timeoutErrCh <- err
		tk5.MustExec("rollback")
	}()

	timeoutErr := <-timeoutErrCh
	c.Assert(timeoutErr, NotNil)
	c.Assert(timeoutErr.Error(), Equals, storeerr.ErrLockWaitTimeout.Error())
	timeoutErr = <-timeoutErrCh
	c.Assert(timeoutErr, NotNil)
	c.Assert(timeoutErr.Error(), Equals, storeerr.ErrLockWaitTimeout.Error())

	// tk4 lock c1 = 2
	tk4.MustExec("begin pessimistic")
	tk4.MustExec("update tk set c2 = c2 + 1 where c1 = 2") // lock succ c1 = 2 by update

	tk2.MustExec("set innodb_lock_wait_timeout = 1")
	tk2.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 1"))

	start := time.Now()
	_, err := tk2.Exec("delete from tk where c1 = 2")
	c.Check(time.Since(start), GreaterEqual, 1000*time.Millisecond)
	c.Check(time.Since(start), Less, 3000*time.Millisecond) // unit test diff should not be too big
	c.Check(err.Error(), Equals, storeerr.ErrLockWaitTimeout.Error())

	tk4.MustExec("commit")

	tk.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 50"))
	tk.MustQuery(`select * from tk where c1 = 2`).Check(testkit.Rows("2 3")) // tk4 update commit work, tk2 delete should be rollbacked

	// test stmtRollBack caused by timeout but not the whole transaction
	tk2.MustExec("update tk set c2 = c2 + 2 where c1 = 2")                    // tk2 lock succ c1 = 2 by update
	tk2.MustQuery(`select * from tk where c1 = 2`).Check(testkit.Rows("2 5")) // tk2 update c2 succ

	tk3.MustExec("begin pessimistic")
	tk3.MustExec("select * from tk where c1 = 3 for update") // tk3  lock c1 = 3 succ

	start = time.Now()
	_, err = tk2.Exec("delete from tk where c1 = 3") // tk2 tries to lock c1 = 3 fail, this delete should be rollback, but previous update should be keeped
	c.Check(time.Since(start), GreaterEqual, 1000*time.Millisecond)
	c.Check(time.Since(start), Less, 3000*time.Millisecond) // unit test diff should not be too big
	c.Check(err.Error(), Equals, storeerr.ErrLockWaitTimeout.Error())

	tk2.MustExec("commit")
	tk3.MustExec("commit")

	tk.MustQuery(`select * from tk where c1 = 1`).Check(testkit.Rows("1 1"))
	tk.MustQuery(`select * from tk where c1 = 2`).Check(testkit.Rows("2 5")) // tk2 update succ
	tk.MustQuery(`select * from tk where c1 = 3`).Check(testkit.Rows("3 3")) // tk2 delete should fail
	tk.MustQuery(`select * from tk where c1 = 4`).Check(testkit.Rows("4 4"))
	tk.MustQuery(`select * from tk where c1 = 5`).Check(testkit.Rows("5 5"))

	// clean
	tk.MustExec("drop table if exists tk")
	tk4.MustExec("commit")

	wg.Wait()
}

func (s *testPessimisticSuite) TestInnodbLockWaitTimeoutWaitStart(c *C) {
	// prepare work
	tk := testkit.NewTestKitWithInit(c, s.store)
	defer tk.MustExec("drop table if exists tk")
	tk.MustExec("drop table if exists tk")
	tk.MustExec("create table tk (c1 int primary key, c2 int)")
	tk.MustExec("insert into tk values(1,1),(2,2),(3,3),(4,4),(5,5)")
	tk.MustExec("set global innodb_lock_wait_timeout = 1")

	// raise pessimistic transaction in tk2 and trigger failpoint returning ErrWriteConflict
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk3 := testkit.NewTestKitWithInit(c, s.store)
	tk2.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 1"))

	// tk3 gets the pessimistic lock
	tk3.MustExec("begin pessimistic")
	tk3.MustQuery("select * from tk where c1 = 1 for update")

	tk2.MustExec("begin pessimistic")
	done := make(chan error)
	c.Assert(failpoint.Enable("tikvclient/PessimisticLockErrWriteConflict", "return"), IsNil)
	var duration time.Duration
	go func() {
		var err error
		start := time.Now()
		defer func() {
			duration = time.Since(start)
			done <- err
		}()
		_, err = tk2.Exec("select * from tk where c1 = 1 for update")
	}()
	time.Sleep(time.Millisecond * 100)
	c.Assert(failpoint.Disable("tikvclient/PessimisticLockErrWriteConflict"), IsNil)
	waitErr := <-done
	c.Assert(waitErr, NotNil)
	c.Check(waitErr.Error(), Equals, storeerr.ErrLockWaitTimeout.Error())
	c.Check(duration, GreaterEqual, 1000*time.Millisecond)
	c.Check(duration, LessEqual, 3000*time.Millisecond)
	tk2.MustExec("rollback")
	tk3.MustExec("commit")
}
