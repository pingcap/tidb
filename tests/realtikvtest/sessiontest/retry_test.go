// Copyright 2022 PingCAP, Inc.
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

package sessiontest

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
)

func TestRetryShow(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@autocommit = 0")
	tk.MustExec("set tidb_disable_txn_auto_retry = 0")
	// UNION should't be in retry history.
	tk.MustQuery("show variables")
	tk.MustQuery("show databases")
	history := session.GetHistory(tk.Session())
	require.Equal(t, 0, history.Count())
}

func TestNoRetryForCurrentTxn(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	tk.MustExec("create table history (a int)")
	tk.MustExec("insert history values (1)")

	// Firstly, disable retry.
	tk.MustExec("set tidb_disable_txn_auto_retry = 1")
	tk.MustExec("begin")
	tk.MustExec("update history set a = 2")
	// Enable retry now.
	tk.MustExec("set tidb_disable_txn_auto_retry = 0")

	tk1.MustExec("update history set a = 3")
	require.Error(t, tk.ExecToErr("commit"))
}

func TestRetryPreparedStmt(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	tk.MustExec("drop table if exists t")
	txn, err := tk.Session().Txn(true)
	require.True(t, kv.ErrInvalidTxn.Equal(err))
	require.False(t, txn.Valid())

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

func TestAutoIncrementID(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	tk.MustExec("insert t values ()")
	tk.MustExec("insert t values ()")
	tk.MustExec("insert t values ()")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	tk.MustExec("insert t values ()")
	lastID := tk.Session().LastInsertID()
	require.Less(t, lastID, uint64(4))

	tk.MustExec("insert t () values ()")
	require.Greater(t, tk.Session().LastInsertID(), lastID)
	lastID = tk.Session().LastInsertID()
	tk.MustExec("insert t values (100)")
	require.Equal(t, uint64(100), tk.Session().LastInsertID())

	// If the auto_increment column value is given, it uses the value of the latest row.
	tk.MustExec("insert t values (120), (112)")
	require.Equal(t, uint64(112), tk.Session().LastInsertID())

	// The last_insert_id function only use last auto-generated id.
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows(fmt.Sprint(lastID)))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (i tinyint unsigned not null auto_increment, primary key (i));")
	tk.MustExec("insert into t set i = 254;")
	tk.MustExec("insert t values ()")

	// The last insert ID doesn't care about primary key, it is set even if its a normal index column.
	tk.MustExec("create table autoid (id int auto_increment, index (id))")
	tk.MustExec("insert autoid values ()")
	require.Greater(t, tk.Session().LastInsertID(), uint64(0))
	tk.MustExec("insert autoid values (100)")
	require.Equal(t, uint64(100), tk.Session().LastInsertID())

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
	require.True(t, terror.ErrorEqual(err, autoid.ErrAutoincReadFailed))
	_, err = tk.Exec("insert into autoid values()")
	require.True(t, terror.ErrorEqual(err, autoid.ErrAutoincReadFailed))
	// FixMe: MySQL works fine with the this sql.
	_, err = tk.Exec("insert into autoid values(18446744073709551615)")
	require.True(t, terror.ErrorEqual(err, autoid.ErrAutoincReadFailed))

	tk.MustExec("drop table if exists autoid")
	tk.MustExec("create table autoid(`auto_inc_id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,UNIQUE KEY `auto_inc_id` (`auto_inc_id`))")
	tk.MustExec("insert into autoid values()")
	tk.MustQuery("select * from autoid").Check(testkit.Rows("1"))
	tk.MustExec("insert into autoid values(5000)")
	tk.MustQuery("select * from autoid").Check(testkit.Rows("1", "5000"))
	_, err = tk.Exec("update autoid set auto_inc_id = 8000")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
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
	require.True(t, terror.ErrorEqual(err, autoid.ErrAutoincReadFailed))
	tk.MustQuery("select auto_inc_id, _tidb_rowid from autoid use index()").Check(testkit.Rows("9223372036854775805 9223372036854775806"))
	tk.MustQuery("select auto_inc_id, _tidb_rowid from autoid use index(auto_inc_id)").Check(testkit.Rows("9223372036854775805 9223372036854775806"))

	tk.MustExec("drop table if exists autoid")
	tk.MustExec("create table autoid(`auto_inc_id` bigint(20) NOT NULL AUTO_INCREMENT,UNIQUE KEY `auto_inc_id` (`auto_inc_id`))")
	tk.MustExec("insert into autoid values()")
	tk.MustQuery("select * from autoid use index()").Check(testkit.Rows("1"))
	tk.MustExec("insert into autoid values(5000)")
	tk.MustQuery("select * from autoid use index()").Check(testkit.Rows("1", "5000"))
	_, err = tk.Exec("update autoid set auto_inc_id = 8000")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	tk.MustQuery("select * from autoid use index()").Check(testkit.Rows("1", "5000"))
	tk.MustExec("update autoid set auto_inc_id = 9000 where auto_inc_id=1")
	tk.MustQuery("select * from autoid use index()").Check(testkit.Rows("9000", "5000"))
	tk.MustExec("insert into autoid values()")
	tk.MustQuery("select * from autoid use index()").Check(testkit.Rows("9000", "5000", "9001"))
}

// test for https://github.com/pingcap/tidb/issues/827
func TestAutoIncrementWithRetry(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	tk.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk.MustExec("create table t (c2 int, c1 int not null auto_increment, PRIMARY KEY (c1))")
	tk.MustExec("insert into t (c2) values (1), (2), (3), (4), (5)")

	// insert values
	lastInsertID := tk.Session().LastInsertID()
	tk.MustExec("begin")
	tk.MustExec("insert into t (c2) values (11), (12), (13)")
	tk.MustQuery("select c1 from t where c2 = 11").Check(testkit.Rows("6"))
	tk.MustExec("update t set c2 = 33 where c2 = 1")

	tk1.MustExec("update t set c2 = 22 where c2 = 1")

	tk.MustExec("commit")

	tk.MustQuery("select c1 from t where c2 = 11").Check(testkit.Rows("6"))
	currLastInsertID := tk.Session().GetSessionVars().StmtCtx.PrevLastInsertID
	require.Equal(t, currLastInsertID, lastInsertID+5)

	// insert set
	lastInsertID = currLastInsertID
	tk.MustExec("begin")
	tk.MustExec("insert into t set c2 = 31")
	tk.MustQuery("select c1 from t where c2 = 31").Check(testkit.Rows("9"))
	tk.MustExec("update t set c2 = 44 where c2 = 2")

	tk1.MustExec("update t set c2 = 55 where c2 = 2")

	tk.MustExec("commit")

	tk.MustQuery("select c1 from t where c2 = 31").Check(testkit.Rows("9"))
	currLastInsertID = tk.Session().GetSessionVars().StmtCtx.PrevLastInsertID
	require.Equal(t, currLastInsertID, lastInsertID+3)

	// replace
	lastInsertID = currLastInsertID
	tk.MustExec("begin")
	tk.MustExec("insert into t (c2) values (21), (22), (23)")
	tk.MustQuery("select c1 from t where c2 = 21").Check(testkit.Rows("10"))
	tk.MustExec("update t set c2 = 66 where c2 = 3")

	tk1.MustExec("update t set c2 = 77 where c2 = 3")

	tk.MustExec("commit")

	tk.MustQuery("select c1 from t where c2 = 21").Check(testkit.Rows("10"))
	currLastInsertID = tk.Session().GetSessionVars().StmtCtx.PrevLastInsertID
	require.Equal(t, currLastInsertID, lastInsertID+1)

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
	currLastInsertID = tk.Session().GetSessionVars().StmtCtx.PrevLastInsertID
	require.Equal(t, currLastInsertID, lastInsertID+3)

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
	currLastInsertID = tk.Session().GetSessionVars().StmtCtx.PrevLastInsertID
	require.Equal(t, currLastInsertID, lastInsertID+3)
}

func TestRetryCleanTxn(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table retrytxn (a int unique, b int)")
	tk.MustExec("insert retrytxn values (1, 1)")
	tk.MustExec("begin")
	tk.MustExec("update retrytxn set b = b + 1 where a = 1")

	// Make retryable error.
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("update retrytxn set b = b + 1 where a = 1")

	// Hijack retry history, add a statement that returns error.
	history := session.GetHistory(tk.Session())
	stmtNode, err := parser.New().ParseOneStmt("insert retrytxn values (2, 'a')", "", "")
	require.NoError(t, err)
	compiler := executor.Compiler{Ctx: tk.Session()}
	stmt, _ := compiler.Compile(context.TODO(), stmtNode)
	_ = executor.ResetContextOfStmt(tk.Session(), stmtNode)
	history.Add(stmt, tk.Session().GetSessionVars().StmtCtx)
	_, err = tk.Exec("commit")
	require.Error(t, err)
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	require.False(t, txn.Valid())
	require.False(t, tk.Session().GetSessionVars().InTxn())
}

func TestRetryUnion(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table history (a int)")
	tk.MustExec("insert history values (1), (2), (3)")
	tk.MustExec("set @@autocommit = 0")
	tk.MustExec("set tidb_disable_txn_auto_retry = 0")
	// UNION shouldn't be in retry history.
	tk.MustQuery("(select * from history) union (select * from history)")
	history := session.GetHistory(tk.Session())
	require.Equal(t, 0, history.Count())
	tk.MustQuery("(select * from history for update) union (select * from history)")
	tk.MustExec("update history set a = a + 1")
	history = session.GetHistory(tk.Session())
	require.Equal(t, 2, history.Count())

	// Make retryable error.
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("update history set a = a + 1")

	tk.MustMatchErrMsg("commit", ".*can not retry select for update statement")
}

func TestRetryResetStmtCtx(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table retrytxn (a int unique, b int)")
	tk.MustExec("insert retrytxn values (1, 1)")
	tk.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk.MustExec("begin")
	tk.MustExec("update retrytxn set b = b + 1 where a = 1")

	// Make retryable error.
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("update retrytxn set b = b + 1 where a = 1")

	require.NoError(t, tk.Session().CommitTxn(context.TODO()))
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
}

func TestReadOnlyNotInHistory(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table history (a int)")
	tk.MustExec("insert history values (1), (2), (3)")
	tk.MustExec("set @@autocommit = 0")
	tk.MustExec("set tidb_disable_txn_auto_retry = 0")
	tk.MustQuery("select * from history")
	history := session.GetHistory(tk.Session())
	require.Equal(t, 0, history.Count())

	tk.MustExec("insert history values (4)")
	tk.MustExec("insert history values (5)")
	require.Equal(t, 2, history.Count())
	tk.MustExec("commit")
	tk.MustQuery("select * from history")
	history = session.GetHistory(tk.Session())
	require.Equal(t, 0, history.Count())
}

// For https://github.com/pingcap/tidb/issues/571
func TestRetry(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("begin")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int)")
	tk.MustExec("insert t values (1), (2), (3)")
	tk.MustExec("commit")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("use test")

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
