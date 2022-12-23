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

package txntest

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestInTxnPSProtoPointGet(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(c1 int primary key, c2 int, c3 int)")
	tk.MustExec("insert into t1 values(1, 10, 100)")

	ctx := context.Background()

	// Generate the ps statement and make the prepared plan cached for point get.
	id, _, _, err := tk.Session().PrepareStmt("select c1, c2 from t1 where c1 = ?")
	require.NoError(t, err)
	idForUpdate, _, _, err := tk.Session().PrepareStmt("select c1, c2 from t1 where c1 = ? for update")
	require.NoError(t, err)
	params := expression.Args2Expressions4Test(1)
	rs, err := tk.Session().ExecutePreparedStmt(ctx, id, params)
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 10"))
	rs, err = tk.Session().ExecutePreparedStmt(ctx, idForUpdate, params)
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 10"))

	// Query again the cached plan will be used.
	rs, err = tk.Session().ExecutePreparedStmt(ctx, id, params)
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 10"))
	rs, err = tk.Session().ExecutePreparedStmt(ctx, idForUpdate, params)
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 10"))

	// Start a transaction, now the in txn flag will be added to the session vars.
	_, err = tk.Session().Execute(ctx, "start transaction")
	require.NoError(t, err)
	rs, err = tk.Session().ExecutePreparedStmt(ctx, id, params)
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 10"))
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	rs, err = tk.Session().ExecutePreparedStmt(ctx, idForUpdate, params)
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 10"))
	txn, err = tk.Session().Txn(false)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	_, err = tk.Session().Execute(ctx, "update t1 set c2 = c2 + 1")
	require.NoError(t, err)
	// Check the read result after in-transaction update.
	rs, err = tk.Session().ExecutePreparedStmt(ctx, id, params)
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 11"))
	rs, err = tk.Session().ExecutePreparedStmt(ctx, idForUpdate, params)
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 11"))
	txn, err = tk.Session().Txn(false)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	tk.MustExec("commit")
}

func TestTxnGoString(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists gostr;")
	tk.MustExec("create table gostr (id int);")

	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	require.Equal(t, "Txn{state=invalid}", fmt.Sprintf("%#v", txn))

	tk.MustExec("begin")
	txn, err = tk.Session().Txn(false)
	require.NoError(t, err)

	require.Equal(t, fmt.Sprintf("Txn{state=valid, txnStartTS=%d}", txn.StartTS()), fmt.Sprintf("%#v", txn))

	tk.MustExec("insert into gostr values (1)")
	require.Equal(t, fmt.Sprintf("Txn{state=valid, txnStartTS=%d}", txn.StartTS()), fmt.Sprintf("%#v", txn))

	tk.MustExec("rollback")
	require.Equal(t, "Txn{state=invalid}", fmt.Sprintf("%#v", txn))
}

func TestSetTransactionIsolationOneSho(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
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
		require.Equal(t, kv.SI, req.IsolationLevel)
	})
	rs, err := tk.Session().Execute(ctx, "select * from t where k = 1")
	require.NoError(t, err)
	rs[0].Close()

	// Check it just take effect for one time.
	ctx = context.WithValue(context.Background(), "CheckSelectRequestHook", func(req *kv.Request) {
		require.Equal(t, kv.SI, req.IsolationLevel)
	})
	rs, err = tk.Session().Execute(ctx, "select * from t where k = 1")
	require.NoError(t, err)
	rs[0].Close()

	// Can't change isolation level when it's inside a transaction.
	tk.MustExec("begin")
	_, err = tk.Session().Execute(ctx, "set transaction isolation level read committed")
	require.Error(t, err)
}

func TestStatementErrorInTransaction(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table statement_side_effect (c int primary key)")
	tk.MustExec("begin")
	tk.MustExec("insert into statement_side_effect values (1)")
	require.Error(t, tk.ExecToErr("insert into statement_side_effect value (2),(3),(4),(1)"))
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
	require.Error(t, tk.ExecToErr("update tset set b=11 where a=1 and b=2;"))
	// Test for a bug that last line rollback and exit transaction, this line autocommit.
	tk.MustExec("update test set b = 11 where a = 1 and b = 2;")
	tk.MustExec("rollback")
	tk.MustQuery("select * from test where a = 1 and b = 11").Check(testkit.Rows())
}

func TestWriteConflictMessage(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int primary key)")
	tk.MustExec("begin optimistic")
	tk2.MustExec("insert into t values (1)")
	tk.MustExec("insert into t values (1)")
	err := tk.ExecToErr("commit")
	require.Contains(t, err.Error(), "Write conflict")
	require.Contains(t, err.Error(), "tableName=test.t, handle=1}")
	require.Contains(t, err.Error(), "reason=Optimistic")

	tk.MustExec("create table t2 (id varchar(30) primary key clustered)")
	tk.MustExec("begin optimistic")
	tk2.MustExec("insert into t2 values ('hello')")
	tk.MustExec("insert into t2 values ('hello')")
	err = tk.ExecToErr("commit")
	require.Contains(t, err.Error(), "Write conflict")
	require.Contains(t, err.Error(), "tableName=test.t2, handle={hello}")
	require.Contains(t, err.Error(), "reason=Optimistic")
}

func TestDuplicateErrorMessage(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tx_isolation='read-committed'")
	tk2.MustExec("use test")
	tk.MustExec("set @@tidb_constraint_check_in_place_pessimistic=off")
	tk.MustExec("create table t (c int primary key, v int)")
	tk.MustExec("create table t2 (c int primary key, v int)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t values (1, 1)")
	tk2.MustExec("insert into t values (1, 1)")
	tk2.MustExec("insert into t2 values (1, 2)")
	tk.MustContainErrMsg("update t set v = v + 1 where c = 1", "Duplicate entry '1' for key 't.PRIMARY'")

	tk.MustExec("create table t3 (c int, v int, unique key i1(v))")
	tk.MustExec("create table t4 (c int, v int, unique key i1(v))")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t3 values (1, 1)")
	tk2.MustExec("insert into t3 values (1, 1)")
	tk2.MustExec("insert into t4 values (1, 2)")
	tk.MustContainErrMsg("update t3 set c = c + 1 where v = 1", "Duplicate entry '1' for key 't3.i1'")
}

func TestAssertionWhenPessimisticLockLost(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("set @@tidb_constraint_check_in_place_pessimistic=0")
	tk1.MustExec("set @@tidb_txn_assertion_level=strict")
	tk2.MustExec("set @@tidb_constraint_check_in_place_pessimistic=0")
	tk2.MustExec("set @@tidb_txn_assertion_level=strict")
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk1.MustExec("create table t (id int primary key, val text)")
	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t where id = 1 for update")
	tk2.MustExec("begin pessimistic")
	tk2.MustExec("insert into t values (1, 'b')")
	tk2.MustExec("insert into t values (2, 'b')")
	tk2.MustExec("commit")
	tk1.MustExec("select * from t where id = 2 for update")
	tk1.MustExec("insert into t values (1, 'a') on duplicate key update val = concat(val, 'a')")
	err := tk1.ExecToErr("commit")
	require.NotContains(t, err.Error(), "assertion")
}
