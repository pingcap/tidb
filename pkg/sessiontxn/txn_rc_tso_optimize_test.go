// Copyright 2021 PingCAP, Inc.
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

package sessiontxn_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/isolation"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestRcTSOCmdCountForPrepareExecuteNormal(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/requestTsoFromPD", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/tsoUseConstantFuture", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/waitTsoOfOracleFuture", "return"))

	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/requestTsoFromPD"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/tsoUseConstantFuture"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/waitTsoOfOracleFuture"))
	}()
	store := testkit.CreateMockStore(t)

	ctx := context.Background()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set global transaction_isolation = 'READ-COMMITTED'")
	tk.MustExec("set global tx_isolation = 'READ-COMMITTED'")
	tk.RefreshSession()
	sctx := tk.Session()
	tk.MustExec("set session tidb_rc_write_check_ts = on")

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(id1 int, id2 int, id3 int, PRIMARY KEY(id1), UNIQUE KEY udx_id2 (id2))")
	tk.MustExec("create table t2(id1 int, id2 int, id3 int, PRIMARY KEY(id1), UNIQUE KEY udx_id2 (id2))")
	tk.MustExec("insert into t1 values (1, 1, 1)")
	tk.MustExec("insert into t2 values (1, 1, 1)")

	// PointPlanQueries always don't send tso request, the others send tso request at first exeuction
	sqlSelectID, _, _, _ := tk.Session().PrepareStmt("select * from t1 where id1 = ? for update")
	sqlUpdateID, _, _, _ := tk.Session().PrepareStmt("update t1 set id3 = id3 + 10 where id1 = ?")
	sqlUpdateID2, _, _, _ := tk.Session().PrepareStmt("update t2 set id3 = id3 + 10 where id1 = ?")
	sqlSelectID2, _, _, _ := tk.Session().PrepareStmt("select id1+id2 as x from t1 where id1 = ? for update")
	sqlSelectID3, _, _, _ := tk.Session().PrepareStmt("select * from t1 where id1 = ?")
	sqlInsertID, _, _, _ := tk.Session().PrepareStmt("insert into t1 values(?, ?, ?)")
	sqlDeleteID, _, _, _ := tk.Session().PrepareStmt("delete from t1 where id1 = ?")
	sqlSelectID4, _, _, _ := tk.Session().PrepareStmt("select * from t1 where id1 > ?")

	res := tk.MustQuery("show variables like 'transaction_isolation'")
	require.Equal(t, "READ-COMMITTED", res.Rows()[0][1])
	resetAllTsoCounter(sctx)

	for i := 1; i < 100; i++ {
		tk.MustExec("begin pessimistic")

		stmt, err := tk.Session().ExecutePreparedStmt(ctx, sqlSelectID, expression.Args2Expressions4Test(1))
		require.NoError(t, err)
		require.NoError(t, stmt.Close())
		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlUpdateID, expression.Args2Expressions4Test(1))
		require.NoError(t, err)
		require.Nil(t, stmt)
		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlUpdateID2, expression.Args2Expressions4Test(1))
		require.NoError(t, err)
		require.Nil(t, stmt)
		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlSelectID2, expression.Args2Expressions4Test(9))
		require.NoError(t, err)
		require.NoError(t, stmt.Close())
		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlSelectID3, expression.Args2Expressions4Test(1))
		require.NoError(t, err)
		require.NoError(t, stmt.Close())

		val := i * 10
		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlInsertID, expression.Args2Expressions4Test(val, val, val))
		require.NoError(t, err)
		require.Nil(t, stmt)
		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlDeleteID, expression.Args2Expressions4Test(val))
		require.NoError(t, err)
		require.Nil(t, stmt)

		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlSelectID4, expression.Args2Expressions4Test(9))
		require.NoError(t, err)
		require.NoError(t, stmt.Close())

		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle := getAllTsoCounter(sctx)
	require.Equal(t, uint64(398), countTsoRequest.(uint64))
	require.Equal(t, uint64(594), countTsoUseConstant.(uint64))
	require.Equal(t, uint64(198), countWaitTsoOracle.(uint64))

	tk.MustExec("set session tidb_rc_write_check_ts = false")
	tk.MustExec("delete from t1")
	tk.MustExec("delete from t2")
	tk.MustExec("insert into t1 values (1, 1, 1)")
	tk.MustExec("insert into t2 values (1, 1, 1)")
	tk.MustExec("insert into t2 values (5, 5, 5)")
	sctx.SetValue(sessiontxn.TsoRequestCount, 0)
	for i := 1; i < 100; i++ {
		tk.MustExec("begin pessimistic")
		stmt, err := tk.Session().ExecutePreparedStmt(ctx, sqlSelectID, expression.Args2Expressions4Test(1))
		require.NoError(t, err)
		require.NoError(t, stmt.Close())
		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlUpdateID, expression.Args2Expressions4Test(1))
		require.NoError(t, err)
		require.Nil(t, stmt)
		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlUpdateID2, expression.Args2Expressions4Test(1))
		require.NoError(t, err)
		require.Nil(t, stmt)
		val := i * 10
		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlInsertID, expression.Args2Expressions4Test(val, val, val))
		require.NoError(t, err)
		require.Nil(t, stmt)
		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlDeleteID, expression.Args2Expressions4Test(val))
		require.NoError(t, err)
		require.Nil(t, stmt)
		tk.MustExec("commit")
	}
	count := sctx.Value(sessiontxn.TsoRequestCount)
	require.Equal(t, uint64(594), count)
}

func TestRcTSOCmdCountForPrepareExecuteExtra(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/requestTsoFromPD", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/tsoUseConstantFuture", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/waitTsoOfOracleFuture", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/requestTsoFromPD"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/tsoUseConstantFuture"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/waitTsoOfOracleFuture"))
	}()
	store := testkit.CreateMockStore(t)

	ctx := context.Background()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set global transaction_isolation = 'READ-COMMITTED'")
	tk.MustExec("set global tx_isolation = 'READ-COMMITTED'")
	tk.RefreshSession()
	sctx := tk.Session()
	tk.MustExec("set session tidb_rc_write_check_ts = on")

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(id1 int, id2 int, id3 int, PRIMARY KEY(id1), UNIQUE KEY udx_id2 (id2))")
	tk.MustExec("create table t2(id1 int, id2 int, id3 int, PRIMARY KEY(id1), UNIQUE KEY udx_id2 (id2))")
	tk.MustExec("insert into t1 values (1, 1, 1)")
	tk.MustExec("insert into t1 values (10, 10, 10)")
	tk.MustExec("insert into t2 values (1, 1, 1)")
	tk.MustExec("insert into t2 values (10, 10, 10)")
	tk.MustExec("insert into t2 values (20, 20, 20)")

	res := tk.MustQuery("show variables like 'transaction_isolation'")
	require.Equal(t, "READ-COMMITTED", res.Rows()[0][1])

	// union statements with two point-lock-read.
	sqlSelectID1, _, _, _ := tk.Session().PrepareStmt("select * from t1 where id1 = ? for update union select * from t2 where id1 = ? for update")
	sqlSelectID2, _, _, _ := tk.Session().PrepareStmt("select id1*2 from t1 where id1 = ? for update union select id1*2 from t2 where id1 = ? for update")
	sqlSelectID3, _, _, _ := tk.Session().PrepareStmt("select * from t1 where id1 = ? for update union select * from t2 where id1 = ?")
	resetAllTsoCounter(sctx)
	for i := 0; i < 10; i++ {
		tk.MustExec("begin pessimistic")

		stmt, err := tk.Session().ExecutePreparedStmt(ctx, sqlSelectID1, expression.Args2Expressions4Test(1, 2))
		require.NoError(t, err)
		require.NoError(t, stmt.Close())

		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlSelectID2, expression.Args2Expressions4Test(1, 2))
		require.NoError(t, err)
		require.NoError(t, stmt.Close())

		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlSelectID3, expression.Args2Expressions4Test(1, 2))
		require.NoError(t, err)
		require.NoError(t, stmt.Close())

		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle := getAllTsoCounter(sctx)
	require.Equal(t, uint64(32), countTsoRequest.(uint64))
	require.Equal(t, uint64(20), countTsoUseConstant.(uint64))
	require.Equal(t, uint64(10), countWaitTsoOracle.(uint64))

	// Join->SelectLock->PoinGet
	sqlSelectID4, _, _, _ := tk.Session().PrepareStmt("SELECT * FROM t1 JOIN t2 ON t1.id1 = t2.id1 WHERE t1.id1 = ? FOR UPDATE")
	resetAllTsoCounter(sctx)
	for i := 0; i < 10; i++ {
		tk.MustExec("begin pessimistic")
		stmt, err := tk.Session().ExecutePreparedStmt(ctx, sqlSelectID4, expression.Args2Expressions4Test(1))
		require.NoError(t, err)
		require.NoError(t, stmt.Close())
		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle = getAllTsoCounter(sctx)
	require.Equal(t, uint64(21), countTsoRequest.(uint64))
	require.Equal(t, uint64(10), countTsoUseConstant.(uint64))
	require.Equal(t, 0, countWaitTsoOracle.(int))

	// SelectLock_7->UnionScan_8->TableReader_10->TableRangeScan_9
	sqlInsertID1, _, _, _ := tk.Session().PrepareStmt("insert into t2 values(?, ?, ?)")
	sqlSelectID5, _, _, _ := tk.Session().PrepareStmt("SELECT * FROM t1 WHERE id1 = ? or id1 < 2 for update")
	resetAllTsoCounter(sctx)
	for i := 1; i < 6; i++ {
		tk.MustExec("begin pessimistic")
		val := i * 11
		stmt, err := tk.Session().ExecutePreparedStmt(ctx, sqlInsertID1, expression.Args2Expressions4Test(val, val, val))
		require.NoError(t, err)
		require.Nil(t, stmt)

		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlSelectID5, expression.Args2Expressions4Test(val))
		require.NoError(t, err)
		require.NoError(t, stmt.Close())

		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle = getAllTsoCounter(sctx)
	require.Equal(t, uint64(16), countTsoRequest.(uint64))
	require.Equal(t, uint64(5), countTsoUseConstant.(uint64))
	require.Equal(t, uint64(5), countWaitTsoOracle.(uint64))

	// BatchPointGet
	sqlSelectID6, _, _, _ := tk.Session().PrepareStmt("SELECT * FROM t1 WHERE id1 = ? OR id1 = ? FOR UPDATE")
	resetAllTsoCounter(sctx)
	for i := 0; i < 5; i++ {
		tk.MustExec("begin pessimistic")
		stmt, err := tk.Session().ExecutePreparedStmt(ctx, sqlSelectID6, expression.Args2Expressions4Test(1, 2))
		require.NoError(t, err)
		require.NoError(t, stmt.Close())
		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle = getAllTsoCounter(sctx)
	require.Equal(t, uint64(15), countTsoRequest.(uint64))
	require.Equal(t, 0, countTsoUseConstant.(int))
	require.Equal(t, uint64(5), countWaitTsoOracle.(uint64))

	// Subquery has SelectLock + PointGet
	sqlSelectID7, _, _, _ := tk.Session().PrepareStmt("SELECT * FROM t1 WHERE id1 IN (SELECT id1 FROM t2 WHERE id1 = ? FOR UPDATE)")
	sqlSelectID8, _, _, _ := tk.Session().PrepareStmt("SELECT * FROM t1 JOIN (SELECT * FROM t2 WHERE id1 = ? FOR UPDATE ) tt2 ON t1.id1 = tt2.id1")
	sqlSelectID9, _, _, _ := tk.Session().PrepareStmt("SELECT (SELECT id1 * 2 FROM t1 WHERE id1 = ? FOR UPDATE)+id1 FROM t2")
	resetAllTsoCounter(sctx)
	for i := 0; i < 5; i++ {
		tk.MustExec("begin pessimistic")
		stmt, err := tk.Session().ExecutePreparedStmt(ctx, sqlSelectID7, expression.Args2Expressions4Test(10))
		require.NoError(t, err)
		require.NoError(t, stmt.Close())

		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlSelectID8, expression.Args2Expressions4Test(1))
		require.NoError(t, err)
		require.NoError(t, stmt.Close())

		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlSelectID9, expression.Args2Expressions4Test(1))
		require.NoError(t, err)
		require.NoError(t, stmt.Close())

		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle = getAllTsoCounter(sctx)
	require.Equal(t, uint64(25), countTsoRequest.(uint64))
	require.Equal(t, 0, countTsoUseConstant.(int))
	require.Equal(t, uint64(15), countWaitTsoOracle.(uint64))

	// PointUpdate Index and Non-index
	sqlUpdateID1, _, _, _ := tk.Session().PrepareStmt("UPDATE t1 set id2 = id2 + 100 WHERE id1 = ?")
	sqlUpdateID2, _, _, _ := tk.Session().PrepareStmt("UPDATE t2 SET id1 = id1 + 100 WHERE id1 = ?")
	resetAllTsoCounter(sctx)
	for i := 0; i < 5; i++ {
		tk.MustExec("begin pessimistic")
		stmt, err := tk.Session().ExecutePreparedStmt(ctx, sqlUpdateID1, expression.Args2Expressions4Test(1))
		require.NoError(t, err)
		require.Nil(t, stmt)
		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlUpdateID2, expression.Args2Expressions4Test(1))
		require.NoError(t, err)
		require.Nil(t, stmt)
		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle = getAllTsoCounter(sctx)
	require.Equal(t, uint64(10), countTsoRequest.(uint64))
	require.Equal(t, uint64(10), countTsoUseConstant.(uint64))
	require.Equal(t, 0, countWaitTsoOracle.(int))

	// SelectLock has PointGet and other plans
	sqlUpdateID3, _, _, _ := tk.Session().PrepareStmt("UPDATE t1 set id2 = id2 + 100 WHERE id1 IN (SELECT id1 FROM t2 WHERE id1 = ?)")
	resetAllTsoCounter(sctx)
	for i := 0; i < 5; i++ {
		tk.MustExec("begin pessimistic")
		stmt, err := tk.Session().ExecutePreparedStmt(ctx, sqlUpdateID3, expression.Args2Expressions4Test(1))
		require.NoError(t, err)
		require.Nil(t, stmt)
		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle = getAllTsoCounter(sctx)
	require.Equal(t, uint64(15), countTsoRequest.(uint64))
	require.Equal(t, 0, countTsoUseConstant.(int))
	require.Equal(t, uint64(5), countWaitTsoOracle.(uint64))

	// PointUpdate with singlerow subquery. singlerow subquery makes tso wait
	// PointUpdate doesn't make tso request
	sqlUpdateID4, _, _, _ := tk.Session().PrepareStmt("UPDATE t1 set id2 = id2 + 100 WHERE id1 =  (SELECT id1 FROM t2 WHERE id1 = ?)")
	resetAllTsoCounter(sctx)
	for i := 0; i < 20; i++ {
		tk.MustExec("begin pessimistic")
		stmt, err := tk.Session().ExecutePreparedStmt(ctx, sqlUpdateID4, expression.Args2Expressions4Test(11))
		require.NoError(t, err)
		require.Nil(t, stmt)
		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle = getAllTsoCounter(sctx)
	require.Equal(t, uint64(60), countTsoRequest.(uint64))
	require.Equal(t, uint64(20), countTsoUseConstant.(uint64))
	require.Equal(t, uint64(20), countWaitTsoOracle.(uint64))

	// delete
	sqlDeleteID1, _, _, _ := tk.Session().PrepareStmt("DELETE FROM t1 WHERE id1 = ?")
	sqlDeleteID2, _, _, _ := tk.Session().PrepareStmt("DELETE FROM t1 WHERE id1 > ?")
	sqlDeleteID3, _, _, _ := tk.Session().PrepareStmt("DELETE FROM t1 WHERE id1 IN (SELECT id1 FROM t2 WHERE id1 = ?)")
	resetAllTsoCounter(sctx)
	for i := 0; i < 1; i++ {
		tk.MustExec("begin pessimistic")
		stmt, err := tk.Session().ExecutePreparedStmt(ctx, sqlDeleteID1, expression.Args2Expressions4Test(3))
		require.NoError(t, err)
		require.Nil(t, stmt)
		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlDeleteID2, expression.Args2Expressions4Test(4))
		require.NoError(t, err)
		require.Nil(t, stmt)
		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlDeleteID3, expression.Args2Expressions4Test(20))
		require.NoError(t, err)
		require.Nil(t, stmt)
		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle = getAllTsoCounter(sctx)
	require.Equal(t, uint64(4), countTsoRequest.(uint64))
	require.Equal(t, uint64(1), countTsoUseConstant.(uint64))
	require.Equal(t, uint64(2), countWaitTsoOracle.(uint64))

	// insert on duplicate
	sqlInsertID2, _, _, _ := tk.Session().PrepareStmt("INSERT INTO t1 VALUES(?,5,5) ON DUPLICATE KEY UPDATE id3 = id3 + 100")
	sqlInsertID3, _, _, _ := tk.Session().PrepareStmt("INSERT INTO t1 VALUES(?,5,5) ON DUPLICATE KEY UPDATE id2 = id2 + 100")
	sqlInsertID4, _, _, _ := tk.Session().PrepareStmt("INSERT INTO t1 VALUES(8,?,5) ON DUPLICATE KEY UPDATE id3 = id3 + 100")
	resetAllTsoCounter(sctx)
	for i := 0; i < 5; i++ {
		tk.MustExec("begin pessimistic")
		stmt, err := tk.Session().ExecutePreparedStmt(ctx, sqlInsertID2, expression.Args2Expressions4Test(10))
		require.NoError(t, err)
		require.Nil(t, stmt)
		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlInsertID3, expression.Args2Expressions4Test(10))
		require.NoError(t, err)
		require.Nil(t, stmt)
		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlInsertID4, expression.Args2Expressions4Test(10))
		require.NoError(t, err)
		require.Nil(t, stmt)
		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle = getAllTsoCounter(sctx)
	require.Equal(t, uint64(25), countTsoRequest.(uint64))
	require.Equal(t, 0, countTsoUseConstant.(int))
	require.Equal(t, uint64(15), countWaitTsoOracle.(uint64))

	// replace into
	tk.MustExec("truncate table t1")
	tk.MustExec("insert into t1 values (1, 1, 1)")
	tk.MustExec("insert into t1 values (10, 10, 10)")
	sqlReplaceIntot1, _, _, _ := tk.Session().PrepareStmt("REPLACE INTO t1 VALUES(1, ?, ?)")
	sqlReplaceIntot2, _, _, _ := tk.Session().PrepareStmt("REPLACE INTO t1 VALUES(?, ?, 20)")
	resetAllTsoCounter(sctx)
	for i := 0; i < 5; i++ {
		tk.MustExec("begin pessimistic")
		val := i * 11
		stmt, err := tk.Session().ExecutePreparedStmt(ctx, sqlReplaceIntot1, expression.Args2Expressions4Test(val, val))
		require.NoError(t, err)
		require.Nil(t, stmt)
		stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlReplaceIntot2, expression.Args2Expressions4Test(val, val))
		require.NoError(t, err)
		require.Nil(t, stmt)
		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle = getAllTsoCounter(sctx)
	require.Equal(t, uint64(20), countTsoRequest.(uint64))
	require.Equal(t, 0, countTsoUseConstant.(int))
	require.Equal(t, uint64(10), countWaitTsoOracle.(uint64))

	// insert ignore
	tk.MustExec("truncate table t1")
	tk.MustExec("insert into t1 values (1, 1, 1)")
	sqlInsertIgnore, _, _, _ := tk.Session().PrepareStmt("INSERT IGNORE INTO t1 VALUES(?, ?, ?)")
	resetAllTsoCounter(sctx)
	tk.MustExec("begin pessimistic")
	stmt, err := tk.Session().ExecutePreparedStmt(ctx, sqlInsertIgnore, expression.Args2Expressions4Test(1, 2, 2))
	require.NoError(t, err)
	require.Nil(t, stmt)
	stmt, err = tk.Session().ExecutePreparedStmt(ctx, sqlInsertIgnore, expression.Args2Expressions4Test(5, 5, 5))
	require.NoError(t, err)
	require.Nil(t, stmt)
	tk.MustExec("commit")
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle = getAllTsoCounter(sctx)
	require.Equal(t, uint64(3), countTsoRequest.(uint64))
	require.Equal(t, uint64(2), countTsoUseConstant.(uint64))
	require.Equal(t, 0, countWaitTsoOracle.(int))
	tk.MustQuery("SELECT * FROM t1 WHERE id1 = 1").Check(testkit.Rows("1 1 1"))
	tk.MustQuery("SELECT * FROM t1 WHERE id1 = 5").Check(testkit.Rows("5 5 5"))
}

func TestRcTSOCmdCountForTextSQLExecuteNormal(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/requestTsoFromPD", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/requestTsoFromPD"))
	}()
	store := testkit.CreateMockStore(t)

	// ctx := context.Background()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set global transaction_isolation = 'READ-COMMITTED'")
	tk.MustExec("set global tx_isolation = 'READ-COMMITTED'")
	tk.RefreshSession()
	sctx := tk.Session()
	tk.MustExec("set session tidb_rc_write_check_ts = on")

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(id1 int, id2 int, id3 int, PRIMARY KEY(id1), UNIQUE KEY udx_id2 (id2))")
	tk.MustExec("create table t2(id1 int, id2 int, id3 int, PRIMARY KEY(id1), UNIQUE KEY udx_id2 (id2))")
	tk.MustExec("insert into t1 values (1, 1, 1)")
	tk.MustExec("insert into t2 values (1, 1, 1)")
	tk.MustExec("insert into t2 values (5, 5, 5)")
	tk.MustExec("insert into t2 values (8, 8, 8)")

	res := tk.MustQuery("show variables like 'transaction_isolation'")
	require.Equal(t, "READ-COMMITTED", res.Rows()[0][1])
	sctx.SetValue(sessiontxn.TsoRequestCount, 0)

	for i := 1; i < 100; i++ {
		tk.MustExec("begin pessimistic")
		tk.MustExec("select * from t1 where id1 = 1 for update")
		tk.MustExec("update t1 set id3 = id3 + 10 where id1 = 1")
		tk.MustExec("update t2 set id3 = id3 + 10 where id1 = 1")
		tk.MustExec("update t2 set id3 = id3 + 10 where id1 > 3 and id1 < 6")
		tk.MustExec("select id1+id2 as x from t1 where id1 = 9 for update")
		val := i * 10
		tk.MustExec(fmt.Sprintf("insert into t2 values(%v, %v, %v)", val, val, val))
		tk.MustExec(fmt.Sprintf("delete from t2 where id1 = %v", val))
		tk.MustExec("commit")
	}
	count := sctx.Value(sessiontxn.TsoRequestCount)
	require.Equal(t, uint64(495), count)
}

func resetAllTsoCounter(sctx sessionctx.Context) {
	sctx.SetValue(sessiontxn.TsoRequestCount, 0)
	sctx.SetValue(sessiontxn.TsoUseConstantCount, 0)
	sctx.SetValue(sessiontxn.TsoWaitCount, 0)
}

func getAllTsoCounter(sctx sessionctx.Context) (any, any, any) {
	countTsoRequest := sctx.Value(sessiontxn.TsoRequestCount)
	countTsoUseConstant := sctx.Value(sessiontxn.TsoUseConstantCount)
	countWaitTsoOracle := sctx.Value(sessiontxn.TsoWaitCount)
	return countTsoRequest, countTsoUseConstant, countWaitTsoOracle
}

func assertAllTsoCounter(t *testing.T,
	assertPair []uint64) {
	for i := 0; i < len(assertPair); i += 2 {
		require.Equal(t, assertPair[i], assertPair[i+1])
	}
}

func TestRcTSOCmdCountForTextSQLExecuteExtra(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/requestTsoFromPD", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/tsoUseConstantFuture", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/waitTsoOfOracleFuture", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/requestTsoFromPD"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/tsoUseConstantFuture"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/waitTsoOfOracleFuture"))
	}()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set global transaction_isolation = 'READ-COMMITTED'")
	tk.MustExec("set global tx_isolation = 'READ-COMMITTED'")
	tk.RefreshSession()
	sctx := tk.Session()
	tk.MustExec("set session tidb_rc_write_check_ts = on")

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(id1 int, id2 int, id3 int, PRIMARY KEY(id1), UNIQUE KEY udx_id2 (id2))")
	tk.MustExec("create table t2(id1 int, id2 int, id3 int, PRIMARY KEY(id1), UNIQUE KEY udx_id2 (id2))")
	tk.MustExec("insert into t1 values (1, 1, 1)")
	tk.MustExec("insert into t1 values (10, 10, 10)")
	tk.MustExec("insert into t2 values (1, 1, 1)")
	tk.MustExec("insert into t2 values (10, 10, 10)")
	tk.MustExec("insert into t2 values (20, 20, 20)")

	res := tk.MustQuery("show variables like 'transaction_isolation'")
	require.Equal(t, "READ-COMMITTED", res.Rows()[0][1])

	// union statements makes disableAdviseWarmup false,
	// use constant tso when all sub queries of unions are point-lock-read.
	resetAllTsoCounter(sctx)
	for i := 0; i < 10; i++ {
		tk.MustExec("begin pessimistic")
		tk.MustExec("select * from t1 where id1 = 1 for update union select * from t2 where id1 = 2 for update")
		tk.MustExec("select id1*2 from t1 where id1 = 1 for update union select id1*2 from t2 where id1 = 2 for update")
		tk.MustExec("select * from t1 where id1 = 1 for update union select * from t2 where id1 = 2")
		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle := getAllTsoCounter(sctx)
	require.Equal(t, uint64(50), countTsoRequest)
	require.Equal(t, uint64(20), countTsoUseConstant)
	require.Equal(t, uint64(10), countWaitTsoOracle)

	// Join->SelectLock->PoinGet
	resetAllTsoCounter(sctx)
	for i := 0; i < 10; i++ {
		tk.MustExec("begin pessimistic")
		tk.MustExec("SELECT * FROM t1 JOIN t2 ON t1.id1 = t2.id1 WHERE t1.id1 = 1 FOR UPDATE")
		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle = getAllTsoCounter(sctx)
	require.Equal(t, uint64(30), countTsoRequest.(uint64))
	require.Equal(t, uint64(10), countTsoUseConstant.(uint64))
	require.Equal(t, 0, countWaitTsoOracle.(int))

	// SelectLock_7->UnionScan_8->TableReader_10->TableRangeScan_9
	resetAllTsoCounter(sctx)
	for i := 1; i < 6; i++ {
		tk.MustExec("begin pessimistic")
		val := i * 11
		tk.MustExec(fmt.Sprintf("insert into t2 values(%v, %v, %v)", val, val, val))
		tk.MustExec(fmt.Sprintf("SELECT * FROM t1 WHERE id1 = %v or id1 < 2 for update", val))
		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle = getAllTsoCounter(sctx)
	require.Equal(t, uint64(20), countTsoRequest.(uint64))
	require.Equal(t, uint64(5), countTsoUseConstant.(uint64))
	require.Equal(t, uint64(5), countWaitTsoOracle.(uint64))

	// BatchPointGet
	resetAllTsoCounter(sctx)
	for i := 0; i < 5; i++ {
		tk.MustExec("begin pessimistic")
		tk.MustExec("SELECT * FROM t1 WHERE id1 = 1 OR id1 = 2 FOR UPDATE")
		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle = getAllTsoCounter(sctx)
	require.Equal(t, uint64(15), countTsoRequest.(uint64))
	require.Equal(t, 0, countTsoUseConstant.(int))
	require.Equal(t, uint64(5), countWaitTsoOracle.(uint64))

	// Subquery has SelectLock + PointGet
	resetAllTsoCounter(sctx)
	for i := 0; i < 5; i++ {
		tk.MustExec("begin pessimistic")
		tk.MustExec("SELECT * FROM t1 WHERE id1 IN (SELECT id1 FROM t2 WHERE id1 = 1 FOR UPDATE)")
		tk.MustExec("SELECT * FROM t1 JOIN (SELECT * FROM t2 WHERE id1 = 1 FOR UPDATE ) tt2 ON t1.id1 = tt2.id1")
		tk.MustExec("SELECT (SELECT id1 * 2 FROM t1 WHERE id1 = 1 FOR UPDATE)+id1 FROM t2")
		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle = getAllTsoCounter(sctx)
	require.Equal(t, uint64(25), countTsoRequest.(uint64))
	require.Equal(t, 0, countTsoUseConstant.(int))
	require.Equal(t, uint64(15), countWaitTsoOracle.(uint64))

	// PointUpdate Index and Non-index
	resetAllTsoCounter(sctx)
	for i := 0; i < 5; i++ {
		tk.MustExec("begin pessimistic")
		tk.MustExec("UPDATE t1 set id2 = id2 + 100 WHERE id1 = 1")
		tk.MustExec("UPDATE t2 SET id1 = id1 + 100 WHERE id1 = 1")
		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle = getAllTsoCounter(sctx)
	require.Equal(t, uint64(10), countTsoRequest.(uint64))
	require.Equal(t, uint64(10), countTsoUseConstant.(uint64))
	require.Equal(t, 0, countWaitTsoOracle.(int))

	// SelectLock has PointGet and other plans
	resetAllTsoCounter(sctx)
	for i := 0; i < 5; i++ {
		tk.MustExec("begin pessimistic")
		tk.MustExec("UPDATE t1 set id2 = id2 + 100 WHERE id1 IN (SELECT id1 FROM t2 WHERE id1 = 1)")
		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle = getAllTsoCounter(sctx)
	require.Equal(t, uint64(15), countTsoRequest.(uint64))
	require.Equal(t, 0, countTsoUseConstant.(int))
	require.Equal(t, uint64(5), countWaitTsoOracle.(uint64))

	// PointUpdate with singlerow subquery. singlerow subquery makes tso wait
	// PointUpdate doesn't make tso request
	resetAllTsoCounter(sctx)
	for i := 0; i < 20; i++ {
		tk.MustExec("begin pessimistic")
		tk.MustExec("UPDATE t1 set id2 = id2 + 100 WHERE id1 =  (SELECT id1 FROM t2 WHERE id1 = 10)")
		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle = getAllTsoCounter(sctx)
	require.Equal(t, uint64(60), countTsoRequest.(uint64))
	require.Equal(t, uint64(20), countTsoUseConstant.(uint64))
	require.Equal(t, uint64(20), countWaitTsoOracle.(uint64))

	// insert with select
	resetAllTsoCounter(sctx)
	for i := 0; i < 1; i++ {
		tk.MustExec("begin pessimistic")
		tk.MustExec("INSERT INTO t1 VALUES(4,4,4)")
		tk.MustExec("INSERT INTO t1 SELECT * FROM t2 WHERE id1 = 11")
		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle = getAllTsoCounter(sctx)
	require.Equal(t, uint64(4), countTsoRequest.(uint64))
	require.Equal(t, uint64(1), countTsoUseConstant.(uint64))
	require.Equal(t, uint64(1), countWaitTsoOracle.(uint64))

	// delete
	resetAllTsoCounter(sctx)
	for i := 0; i < 1; i++ {
		tk.MustExec("begin pessimistic")
		tk.MustExec("DELETE FROM t1 WHERE id1 = 3")
		tk.MustExec("DELETE FROM t1 WHERE id1 > 4")
		tk.MustExec("DELETE FROM t1 WHERE id1 IN (SELECT id1 FROM t2 WHERE id1 = 20)")
		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle = getAllTsoCounter(sctx)
	require.Equal(t, uint64(4), countTsoRequest.(uint64))
	require.Equal(t, uint64(1), countTsoUseConstant.(uint64))
	require.Equal(t, uint64(2), countWaitTsoOracle.(uint64))

	// insert on duplicate key
	resetAllTsoCounter(sctx)
	for i := 0; i < 5; i++ {
		tk.MustExec("begin pessimistic")
		tk.MustExec("INSERT INTO t1 VALUES(10,5,5) ON DUPLICATE KEY UPDATE id3 = id3 + 100")
		tk.MustExec("INSERT INTO t1 VALUES(10,5,5) ON DUPLICATE KEY UPDATE id2 = id2 + 100")
		tk.MustExec("INSERT INTO t1 VALUES(8,10,5) ON DUPLICATE KEY UPDATE id3 = id3 + 100")
		tk.MustExec("commit")
	}
	countTsoRequest, countTsoUseConstant, countWaitTsoOracle = getAllTsoCounter(sctx)
	require.Equal(t, uint64(25), countTsoRequest.(uint64))
	require.Equal(t, 0, countTsoUseConstant.(int))
	require.Equal(t, uint64(15), countWaitTsoOracle.(uint64))
}

func TestConflictErrorsUseRcWriteCheckTs(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/assertPessimisticLockErr", "return"))
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("rollback")

	se := tk.Session()

	tk2 := testkit.NewTestKit(t, store)
	defer tk2.MustExec("rollback")

	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk.MustExec("set transaction_isolation = 'READ-COMMITTED'")
	tk.MustExec("set tx_isolation = 'READ-COMMITTED'")
	tk.MustExec("set session tidb_rc_write_check_ts = on")
	tk2.MustExec("set session tidb_rc_write_check_ts = on")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(id1 int, id2 int, id3 int, PRIMARY KEY(id1), UNIQUE KEY udx_id2 (id2))")
	tk.MustExec("insert into t1 values (1, 1, 1)")
	tk.MustExec("insert into t1 values (10, 10, 10)")

	se.SetValue(sessiontxn.AssertLockErr, nil)
	tk.MustExec("begin pessimistic")
	tk2.MustExec("update t1 set id3 = id3 + 1 where id1 = 1")
	tk.MustExec("select * from t1 where id1 = 1 for update")
	tk.MustExec("commit")
	records, ok := se.Value(sessiontxn.AssertLockErr).(map[string]int)
	require.Equal(t, true, ok)
	require.Equal(t, records["errWriteConflict"], 1)

	se.SetValue(sessiontxn.AssertLockErr, nil)
	tk.MustExec("begin pessimistic")
	tk2.MustExec("update t1 set id1 = 5 where id1 = 1")
	tk.MustExec("delete from t1 where id1 = 5")
	tk.MustExec("commit")
	records, ok = se.Value(sessiontxn.AssertLockErr).(map[string]int)
	require.True(t, ok)
	tk.MustQuery("select * from t1 where id1 = 5 for update").Check(testkit.Rows())
	require.Equal(t, records["errWriteConflict"], 1)

	se.SetValue(sessiontxn.AssertLockErr, nil)
	tk.MustExec("begin pessimistic")
	tk2.MustExec("insert into t1 values(20,20,20)")
	tk.MustExec("delete from t1 where id1 = 20")
	tk.MustQuery("select * from t1 where id1 = 20").Check(testkit.Rows())
	tk.MustExec("commit")
	records, ok = se.Value(sessiontxn.AssertLockErr).(map[string]int)
	require.True(t, ok)
	require.Equal(t, records["errWriteConflict"], 1)

	se.SetValue(sessiontxn.AssertLockErr, nil)
	tk.MustExec("begin pessimistic")
	tk2.MustExec("insert into t1 values(30,30,30)")
	tk.MustExec("update t1 set id3 = 300 where id1 = 30")
	tk.MustQuery("select * from t1 where id1 = 30").Check(testkit.Rows("30 30 300"))
	tk.MustExec("rollback")
	records, ok = se.Value(sessiontxn.AssertLockErr).(map[string]int)
	require.True(t, ok)
	require.Equal(t, records["errWriteConflict"], 1)

	se.SetValue(sessiontxn.AssertLockErr, nil)
	tk.MustExec("begin pessimistic")
	tk2.MustExec("insert into t1 values(40,40,40)")
	_, err := tk.Exec("insert into t1 values(40,400,400)")
	require.Error(t, err)
	tk.MustExec("rollback")
	records, ok = se.Value(sessiontxn.AssertLockErr).(map[string]int)
	require.True(t, ok)
	require.Equal(t, records["errDuplicateKey"], 1)

	se.SetValue(sessiontxn.AssertLockErr, nil)
	tk.MustExec("begin pessimistic")
	tk2.MustExec("insert into t1 values(50,50,50)")
	_, err = tk.Exec("insert ignore into t1 values(50,400,400)")
	require.NoError(t, err)
	tk.MustExec("rollback")
	records, ok = se.Value(sessiontxn.AssertLockErr).(map[string]int)
	require.True(t, ok)
	require.Equal(t, records["errDuplicateKey"], 0)
	require.Equal(t, records["errWriteConflict"], 1)

	tk.MustExec("begin pessimistic")
	mgr := sessiontxn.GetTxnManager(tk.Session())
	tk.MustExec("update t1 set id3 = id3 + 1 where id1 = 1")
	p := mgr.GetContextProvider().(*isolation.PessimisticRCTxnContextProvider)
	require.Equal(t, p.IsCheckTSInWriteStmtMode(), true)
	tk.MustExec("select * from t1 where id1 = 1")
	require.Equal(t, p.IsCheckTSInWriteStmtMode(), false)
	tk.MustExec("rollback")

	se.SetValue(sessiontxn.AssertLockErr, nil)
	tk.MustExec("begin pessimistic")
	tk2.MustExec("update t1 set id3 = id3 + 1 where id1 = 1")
	tk.MustExec("select * from t1 where id1 = 1 for update")
	tk.MustExec("select * from t1 where id1 = 10")
	tk2.MustExec("insert into t1 values(60, 60, 60)")
	tk.MustQuery("select * from t1 where id1 = 60 for update").Check(testkit.Rows("60 60 60"))
	tk.MustExec("commit")
	records, ok = se.Value(sessiontxn.AssertLockErr).(map[string]int)
	require.Equal(t, true, ok)
	require.Equal(t, records["errWriteConflict"], 1)

	tk.MustExec("set session tidb_rc_write_check_ts = false")
	tk2.MustExec("set session tidb_rc_write_check_ts = false")
	se.SetValue(sessiontxn.AssertLockErr, nil)
	tk.MustExec("begin pessimistic")
	tk2.MustExec("update t1 set id3 = id3 + 1 where id1 = 1")
	tk.MustExec("select * from t1 where id1 = 1 for update")
	tk.MustExec("commit")
	_, ok = se.Value(sessiontxn.AssertLockErr).(map[string]int)
	require.Equal(t, false, ok)

	tk.MustExec("insert into t1 values(20, 20, 20)")
	se.SetValue(sessiontxn.AssertLockErr, nil)
	tk.MustExec("begin pessimistic")
	tk2.MustExec("update t1 set id1 = 200 where id1 = 20")
	tk.MustQuery("select * from t1 where id1 = 200 for update").Check(testkit.Rows("200 20 20"))
	tk.MustExec("commit")
	_, ok = se.Value(sessiontxn.AssertLockErr).(map[string]int)
	require.Equal(t, false, ok)

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/assertPessimisticLockErr"))
}

func TestRcWaitTSInSlowLog(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set global transaction_isolation = 'READ-COMMITTED'")
	tk.RefreshSession()
	sctx := tk.Session()

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(id1 int, id2 int, id3 int, PRIMARY KEY(id1), UNIQUE KEY udx_id2 (id2))")
	tk.MustExec("insert into t1 values (1, 1, 1), (2, 2, 2), (3, 3, 3)")

	res := tk.MustQuery("show variables like 'transaction_isolation'")
	require.Equal(t, "READ-COMMITTED", res.Rows()[0][1])
	sctx.SetValue(sessiontxn.TsoRequestCount, 0)

	tk.MustExec("begin pessimistic")
	waitTs1 := sctx.GetSessionVars().DurationWaitTS
	tk.MustExec("update t1 set id3 = id3 + 10 where id1 = 1")
	waitTs2 := sctx.GetSessionVars().DurationWaitTS
	tk.MustExec("update t1 set id3 = id3 + 10 where id1 > 3 and id1 < 6")
	waitTs3 := sctx.GetSessionVars().DurationWaitTS
	tk.MustExec("commit")
	require.NotEqual(t, waitTs1, waitTs2)
	require.NotEqual(t, waitTs1, waitTs2)
	require.NotEqual(t, waitTs2, waitTs3)
}
