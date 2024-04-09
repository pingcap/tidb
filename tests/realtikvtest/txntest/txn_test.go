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
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
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

func TestSelectLockForPartitionTable(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk1.MustExec("use test")
	tk1.MustExec("create table t(a int, b int, c int, key idx(a, b, c)) PARTITION BY HASH (c) PARTITIONS 10")
	tk1.MustExec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3)")
	tk1.MustExec("analyze table t")
	tk1.MustExec("begin")
	tk1.MustHavePlan("select * from t use index(idx) where a = 1 and b = 1 order by a limit 1 for update", "IndexReader")
	tk1.MustExec("select * from t use index(idx) where a = 1 and b = 1 order by a limit 1 for update")
	ch := make(chan bool, 1)
	go func() {
		tk2.MustExec("use test")
		tk2.MustExec("begin")
		ch <- false
		// block here, until tk1 finish
		tk2.MustExec("select * from t use index(idx) where a = 1 and b = 1 order by a limit 1 for update")
		ch <- true
	}()

	res := <-ch
	// Sleep here to make sure SelectLock stmt is executed
	time.Sleep(10 * time.Millisecond)

	select {
	case res = <-ch:
	default:
	}
	require.False(t, res)

	tk1.MustExec("commit")
	// wait until tk2 finished
	res = <-ch
	require.True(t, res)
}

func TestTxnEntrySizeLimit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk1.MustExec("create table t (a int, b longtext)")

	// cannot insert a large entry by default
	tk1.MustContainErrMsg("insert into t values (1, repeat('a', 7340032))", "[kv:8025]entry too large, the max entry size is 6291456")

	// increase the entry size limit allow user write large entries
	tk1.MustExec("set session tidb_txn_entry_size_limit=8388608")
	tk1.MustExec("insert into t values (1, repeat('a', 7340032))")
	tk1.MustContainErrMsg("insert into t values (1, repeat('a', 9427968))", "[kv:8025]entry too large, the max entry size is 8388608")

	// update session var does not affect other sessions
	tk2.MustContainErrMsg("insert into t values (1, repeat('a', 7340032))", "[kv:8025]entry too large, the max entry size is 6291456")
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("use test")
	tk3.MustContainErrMsg("insert into t values (1, repeat('a', 7340032))", "[kv:8025]entry too large, the max entry size is 6291456")

	// update session var does not affect internal session used by ddl backfilling
	tk1.MustContainErrMsg("alter table t modify column a varchar(255)", "[kv:8025]entry too large, the max entry size is 6291456")

	// update global var allows ddl backfilling write large entries
	tk1.MustExec("set global tidb_txn_entry_size_limit=8388608")
	tk1.MustExec("alter table t modify column a varchar(255)")
	tk2.MustExec("alter table t modify column a int")

	// update global var does not affect existing sessions
	tk2.MustContainErrMsg("insert into t values (1, repeat('a', 7340032))", "[kv:8025]entry too large, the max entry size is 6291456")
	tk3.MustContainErrMsg("insert into t values (1, repeat('a', 7340032))", "[kv:8025]entry too large, the max entry size is 6291456")

	// update global var affects new sessions
	tk4 := testkit.NewTestKit(t, store)
	tk4.MustExec("use test")
	tk4.MustExec("insert into t values (2, repeat('b', 7340032))")

	// reset global var to default
	tk1.MustExec("set global tidb_txn_entry_size_limit=0")
	tk1.MustContainErrMsg("alter table t modify column a varchar(255)", "[kv:8025]entry too large, the max entry size is 6291456")
	tk2.MustContainErrMsg("alter table t modify column a varchar(255)", "[kv:8025]entry too large, the max entry size is 6291456")
	tk3.MustContainErrMsg("alter table t modify column a varchar(255)", "[kv:8025]entry too large, the max entry size is 6291456")
	tk4.MustContainErrMsg("alter table t modify column a varchar(255)", "[kv:8025]entry too large, the max entry size is 6291456")

	// reset session var to default
	tk1.MustExec("insert into t values (3, repeat('c', 7340032))")
	tk1.MustExec("set session tidb_txn_entry_size_limit=0")
	tk1.MustContainErrMsg("insert into t values (1, repeat('a', 7340032))", "[kv:8025]entry too large, the max entry size is 6291456")
}

func TestCheckTxnStatusOnOptimisticTxnBreakConsistency(t *testing.T) {
	// This test case overs the issue #51666 (tikv#16620).
	if !*realtikvtest.WithRealTiKV {
		t.Skip("skip due to not supporting mock storage")
	}

	// Allow async commit
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = 500 * time.Millisecond
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
	})

	// A helper function to determine whether a KV RPC request is handled on TiKV without RPC error or region error.
	isRequestHandled := func(resp *tikvrpc.Response, err error) bool {
		if err != nil || resp == nil {
			return false
		}

		regionErr, err := resp.GetRegionError()
		if err != nil || regionErr != nil {
			return false
		}

		return true
	}

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tkPrepare1 := testkit.NewTestKit(t, store)
	tkPrepare2 := testkit.NewTestKit(t, store)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tkPrepare1.MustExec("use test")
	tkPrepare2.MustExec("use test")
	tk1.MustExec("use test")
	tk2.MustExec("use test")

	tk1.MustExec("create table t (id int primary key, v int)")
	tk1.MustExec("insert into t values (1, 10), (2, 20)")
	// Table t2 for revealing the possibility that the issue causing data-index inconsistency.
	tk1.MustExec("create table t2 (id int primary key, v int unique)")
	tk1.MustExec("insert into t2 values (1, 10)")

	tkPrepare1.MustExec("set @@tidb_enable_async_commit = 1")
	tk1.MustExec("set @@tidb_enable_async_commit = 0")

	// Prepare a ts collision (currentTxn.StartTS == lastTxn.CommitTS on the same key).
	// Loop until we successfully prepare one.
	var lastCommitTS uint64
	for constructionIters := 0; ; constructionIters++ {
		// Reset the value which might have been updated in the previous attempt.
		tkPrepare1.MustExec("update t set v = 10 where id = 1")

		// Update row 1 in async commit mode
		require.NoError(t, failpoint.Enable("tikvclient/beforePrewrite", "pause"))
		tkPrepapre1Ch := make(chan struct{})
		go func() {
			tkPrepare1.MustExec("update t set v = v + 1 where id = 1")
			tkPrepapre1Ch <- struct{}{}
		}()

		// tkPrepare2 Updates TiKV's max_ts by reading. Assuming tkPrepare2's reading is just before tk1's BEGIN,
		// we expect that tk1 have startTS == tkPrepare2.startTS + 1 so that the tk1.startTS == TiKV's min_commit_ts.
		tkPrepare2.MustQuery("select * from t where id = 1").Check(testkit.Rows("1 10"))
		tk1.MustExec("begin optimistic")

		require.NoError(t, failpoint.Disable("tikvclient/beforePrewrite"))
		select {
		case <-tkPrepapre1Ch:
		case <-time.After(time.Second):
			require.Fail(t, "tkPrepare1 not resumed after unsetting failpoint")
		}

		var err error
		lastCommitTS, err = strconv.ParseUint(tkPrepare1.MustQuery("select json_extract(@@tidb_last_txn_info, '$.commit_ts')").Rows()[0][0].(string), 10, 64)
		require.NoError(t, err)
		currentStartTS, err := strconv.ParseUint(tk1.MustQuery("select @@tidb_current_ts").Rows()[0][0].(string), 10, 64)
		require.NoError(t, err)
		if currentStartTS == lastCommitTS {
			break
		}
		// Abandon and retry.
		tk1.MustExec("rollback")
		if constructionIters >= 1000 {
			require.Fail(t, "failed to construct the ts collision situation of async commit transaction")
		}
	}

	// Now tk1 is in a transaction whose start ts collides with the commit ts of a previously committed transaction
	// that has written row 1. The ts is in variable `lastCommitTS`.

	tk1.MustExec("update t set v = v + 100 where id = 1")
	tk1.MustExec("update t set v = v + 100 where id = 2")
	tk1.MustExec("update t2 set v = v + 1 where id = 1")

	// We will construct the following committing procedure for transaction in tk1:
	// 1. Successfully prewrites all keys but fail to receive the response of the request that prewrites the primary
	//    (by simulating RPC error);
	// 2. tk2 tries to access keys that were already locked by tk1, and performs resolve-locks. When the issue exists,
	//    the primary may be rolled back without any rollback record.
	// 3. tk1 continues and retries prewriting the primary. In normal cases, it should not succeed as the transaction
	//    should have been rolled back by tk2's resolve-locks operation, but it succeeds in the issue.
	// To simulate the procedure for tk1's commit procedure, we use the onRPCFinishedHook failpoint, and inject a hook
	// when committing that makes the first prewrite on tk1's primary fail, and blocks until signaled by the channel
	// `continueCommittingSignalCh`.

	require.NoError(t, failpoint.Enable("tikvclient/twoPCShortLockTTL", "return"))
	require.NoError(t, failpoint.Enable("tikvclient/doNotKeepAlive", "return"))
	require.NoError(t, failpoint.Enable("tikvclient/twoPCRequestBatchSizeLimit", "return"))
	require.NoError(t, failpoint.Enable("tikvclient/onRPCFinishedHook", "return"))

	defer func() {
		require.NoError(t, failpoint.Disable("tikvclient/twoPCShortLockTTL"))
		require.NoError(t, failpoint.Disable("tikvclient/doNotKeepAlive"))
		require.NoError(t, failpoint.Disable("tikvclient/twoPCRequestBatchSizeLimit"))
		require.NoError(t, failpoint.Disable("tikvclient/onRPCFinishedHook"))
	}()

	continueCommittingSignalCh := make(chan struct{})

	primaryReqCount := 0
	onRPCFinishedHook := func(req *tikvrpc.Request, resp *tikvrpc.Response, err error) (*tikvrpc.Response, error) {
		if req.Type == tikvrpc.CmdPrewrite {
			prewriteReq := req.Prewrite()
			// The failpoint "twoPCRequestBatchSizeLimit" must takes effect
			require.Equal(t, 1, len(prewriteReq.GetMutations()))
			if prewriteReq.GetStartVersion() == lastCommitTS &&
				bytes.Equal(prewriteReq.GetMutations()[0].Key, prewriteReq.PrimaryLock) &&
				isRequestHandled(resp, err) {
				primaryReqCount++
				if primaryReqCount == 1 {
					// Block until signaled
					<-continueCommittingSignalCh
					// Simulate RPC failure (but TiKV successfully handled the request) for the first attempt
					return nil, errors.New("injected rpc error in onRPCFinishedHook")
				}
			}
		}
		return resp, err
	}

	ctxWithHook := context.WithValue(context.Background(), "onRPCFinishedHook", onRPCFinishedHook)

	resCh := make(chan error)
	go func() {
		_, err := tk1.ExecWithContext(ctxWithHook, "commit")
		resCh <- err
	}()
	// tk1 must be blocked by the hook function.
	select {
	case err := <-resCh:
		require.Fail(t, "tk1 not blocked, result: "+fmt.Sprintf("%+q", err))
	case <-time.After(time.Millisecond * 50):
	}

	// tk2 conflicts with tk1 and rolls back tk1 by resolving locks.
	tk2.MustExec("update t set v = v + 1 where id = 2")
	tk2.MustExec("insert into t2 values (2, 11)")

	// tk1 must still be blocked
	select {
	case err := <-resCh:
		require.Fail(t, "tk1 not blocked, result: "+fmt.Sprintf("%+q", err))
	case <-time.After(time.Millisecond * 50):
	}

	// Signal tk1 to continue (retry the prewrite request and continue).
	close(continueCommittingSignalCh)

	var err error
	select {
	case err = <-resCh:
	case <-time.After(time.Second):
		require.Fail(t, "tk1 not resumed")
	}

	require.Error(t, err)
	require.Equal(t, errno.ErrWriteConflict, int(errors.Cause(err).(*errors.Error).Code()))
	tk2.MustQuery("select * from t order by id").Check(testkit.Rows("1 11", "2 21"))
	tk2.MustExec("admin check table t2")
	tk2.MustQuery("select * from t2 order by id").Check(testkit.Rows("1 10", "2 11"))
}
