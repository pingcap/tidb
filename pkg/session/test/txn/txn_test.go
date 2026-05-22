// Copyright 2023 PingCAP, Inc.
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

package txn

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

// TestAutocommit . See https://dev.mysql.com/doc/internals/en/status-flags.html
func TestAutocommit(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t;")
	require.Greater(t, int(tk.Session().Status()&mysql.ServerStatusAutocommit), 0)
	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	require.Greater(t, int(tk.Session().Status()&mysql.ServerStatusAutocommit), 0)
	tk.MustExec("insert t values ()")
	require.Greater(t, int(tk.Session().Status()&mysql.ServerStatusAutocommit), 0)
	tk.MustExec("begin")
	require.Greater(t, int(tk.Session().Status()&mysql.ServerStatusAutocommit), 0)
	tk.MustExec("insert t values ()")
	require.Greater(t, int(tk.Session().Status()&mysql.ServerStatusAutocommit), 0)
	tk.MustExec("drop table if exists t")
	require.Greater(t, int(tk.Session().Status()&mysql.ServerStatusAutocommit), 0)

	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	require.Greater(t, int(tk.Session().Status()&mysql.ServerStatusAutocommit), 0)
	tk.MustExec("set autocommit=0")
	require.Equal(t, 0, int(tk.Session().Status()&mysql.ServerStatusAutocommit))
	tk.MustExec("insert t values ()")
	require.Equal(t, 0, int(tk.Session().Status()&mysql.ServerStatusAutocommit))
	tk.MustExec("commit")
	require.Equal(t, 0, int(tk.Session().Status()&mysql.ServerStatusAutocommit))
	tk.MustExec("drop table if exists t")
	require.Equal(t, 0, int(tk.Session().Status()&mysql.ServerStatusAutocommit))
	tk.MustExec("set autocommit='On'")
	require.Greater(t, int(tk.Session().Status()&mysql.ServerStatusAutocommit), 0)

	// When autocommit is 0, transaction start ts should be the first *valid*
	// statement, rather than *any* statement.
	tk.MustExec("create table t (id int key)")
	tk.MustExec("set @@autocommit = 0")
	tk.MustExec("rollback")
	tk.MustExec("set @@autocommit = 0")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("insert into t select 1")
	//nolint:all_revive,revive
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	tk.MustExec("delete from t")

	// When the transaction is rolled back, the global set statement would succeed.
	tk.MustExec("set @@global.autocommit = 0")
	tk.MustExec("begin")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("set @@global.autocommit = 1")
	tk.MustExec("rollback")
	tk.MustQuery("select count(*) from t where id = 1").Check(testkit.Rows("0"))
	tk.MustQuery("select @@global.autocommit").Check(testkit.Rows("1"))

	// When the transaction is committed because of switching mode, the session set statement should succeed.
	tk.MustExec("set autocommit = 0")
	tk.MustExec("begin")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("set autocommit = 1")
	tk.MustExec("rollback")
	tk.MustQuery("select count(*) from t where id = 1").Check(testkit.Rows("1"))
	tk.MustQuery("select @@autocommit").Check(testkit.Rows("1"))

	tk.MustExec("set autocommit = 0")
	tk.MustExec("insert into t values (2)")
	tk.MustExec("set autocommit = 1")
	tk.MustExec("rollback")
	tk.MustQuery("select count(*) from t where id = 2").Check(testkit.Rows("1"))
	tk.MustQuery("select @@autocommit").Check(testkit.Rows("1"))

	// Set should not take effect if the mode is not changed.
	tk.MustExec("set autocommit = 0")
	tk.MustExec("begin")
	tk.MustExec("insert into t values (3)")
	tk.MustExec("set autocommit = 0")
	tk.MustExec("rollback")
	tk.MustQuery("select count(*) from t where id = 3").Check(testkit.Rows("0"))
	tk.MustQuery("select @@autocommit").Check(testkit.Rows("0"))

	tk.MustExec("set autocommit = 1")
	tk.MustExec("begin")
	tk.MustExec("insert into t values (4)")
	tk.MustExec("set autocommit = 1")
	tk.MustExec("rollback")
	tk.MustQuery("select count(*) from t where id = 4").Check(testkit.Rows("0"))
	tk.MustQuery("select @@autocommit").Check(testkit.Rows("1"))
}

// TestTxnLazyInitialize tests that when autocommit = 0, not all statement starts
// a new transaction.
func TestTxnLazyInitialize(t *testing.T) {
	testTxnLazyInitialize(t, false)
	testTxnLazyInitialize(t, true)
}

func testTxnLazyInitialize(t *testing.T, isPessimistic bool) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int)")
	if isPessimistic {
		tk.MustExec("set tidb_txn_mode = 'pessimistic'")
	}

	tk.MustExec("set @@autocommit = 0")
	_, err := tk.Session().Txn(true)
	require.True(t, kv.ErrInvalidTxn.Equal(err))
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	require.False(t, txn.Valid())
	tk.MustQuery("select @@tidb_current_ts").Check(testkit.Rows("0"))
	tk.MustQuery("select @@tidb_current_ts").Check(testkit.Rows("0"))

	// Those statements should not start a new transaction automatically.
	tk.MustQuery("select 1")
	tk.MustQuery("select @@tidb_current_ts").Check(testkit.Rows("0"))

	tk.MustExec("set @@tidb_general_log = 0")
	tk.MustQuery("select @@tidb_current_ts").Check(testkit.Rows("0"))

	// Explain now also build the query and starts a transaction
	tk.MustQuery("explain select * from t")
	res := tk.MustQuery("select @@tidb_current_ts")
	require.NotEqual(t, "0", res.Rows()[0][0])

	// Begin statement should start a new transaction.
	tk.MustExec("begin")
	txn, err = tk.Session().Txn(false)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	tk.MustExec("rollback")

	tk.MustExec("select * from t")
	txn, err = tk.Session().Txn(false)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	tk.MustExec("rollback")

	tk.MustExec("insert into t values (1)")
	txn, err = tk.Session().Txn(false)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	tk.MustExec("rollback")
}

func TestDisableTxnAutoRetry(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second)

	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk1.MustExec("use test")
	tk2.MustExec("use test")

	tk1.MustExec("create table no_retry (id int)")
	tk1.MustExec("insert into no_retry values (1)")
	tk1.MustExec("set @@tidb_disable_txn_auto_retry = 1")

	tk1.MustExec("begin")
	tk1.MustExec("update no_retry set id = 2")

	tk2.MustExec("begin")
	tk2.MustExec("update no_retry set id = 3")
	tk2.MustExec("commit")

	// No auto retry because tidb_disable_txn_auto_retry is set to 1.
	_, err := tk1.Session().Execute(context.Background(), "commit")
	require.Error(t, err)

	// session 1 starts a transaction early.
	// execute a select statement to clear retry history.
	tk1.MustExec("select 1")
	err = tk1.Session().PrepareTxnCtx(context.Background(), nil)
	require.NoError(t, err)
	// session 2 update the value.
	tk2.MustExec("update no_retry set id = 4")
	// AutoCommit update will retry, so it would not fail.
	tk1.MustExec("update no_retry set id = 5")

	// RestrictedSQL should retry.
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	tk1.Session().ExecuteInternal(ctx, "begin")

	tk2.MustExec("update no_retry set id = 6")

	tk1.Session().ExecuteInternal(ctx, "update no_retry set id = 7")
	tk1.Session().ExecuteInternal(ctx, "commit")

	// test for disable transaction local latch
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TxnLocalLatches.Enabled = false
	})
	tk1.MustExec("begin")
	tk1.MustExec("update no_retry set id = 9")

	tk2.MustExec("update no_retry set id = 8")

	_, err = tk1.Session().Execute(context.Background(), "commit")
	require.Error(t, err)
	require.True(t, kv.ErrWriteConflict.Equal(err), fmt.Sprintf("err %v", err))
	require.Contains(t, err.Error(), kv.TxnRetryableMark)
	tk1.MustExec("rollback")

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TxnLocalLatches.Enabled = true
	})
	tk1.MustExec("begin")
	tk2.MustExec("alter table no_retry add index idx(id)")
	tk2.MustQuery("select * from no_retry").Check(testkit.Rows("8"))
	tk1.MustExec("update no_retry set id = 10")
	_, err = tk1.Session().Execute(context.Background(), "commit")
	require.Error(t, err)

	// set autocommit to begin and commit
	tk1.MustExec("set autocommit = 0")
	tk1.MustQuery("select * from no_retry").Check(testkit.Rows("8"))
	tk2.MustExec("update no_retry set id = 11")
	tk1.MustExec("update no_retry set id = 12")
	_, err = tk1.Session().Execute(context.Background(), "set autocommit = 1")
	require.Error(t, err)
	require.True(t, kv.ErrWriteConflict.Equal(err), fmt.Sprintf("err %v", err))
	require.Contains(t, err.Error(), kv.TxnRetryableMark)
	tk1.MustExec("rollback")
	tk2.MustQuery("select * from no_retry").Check(testkit.Rows("11"))

	tk1.MustExec("set autocommit = 0")
	tk1.MustQuery("select * from no_retry").Check(testkit.Rows("11"))
	tk2.MustExec("update no_retry set id = 13")
	tk1.MustExec("update no_retry set id = 14")
	_, err = tk1.Session().Execute(context.Background(), "commit")
	require.Error(t, err)
	require.True(t, kv.ErrWriteConflict.Equal(err), fmt.Sprintf("err %v", err))
	require.Contains(t, err.Error(), kv.TxnRetryableMark)
	tk1.MustExec("rollback")
	tk2.MustQuery("select * from no_retry").Check(testkit.Rows("13"))
}

// The Read-only flags are checked in the planning stage of queries,
// but this test checks we check them again at commit time.
// The main use case for this is a long-running auto-commit statement.
func TestAutoCommitRespectsReadOnly(t *testing.T) {
	store := testkit.CreateMockStore(t)
	var wg sync.WaitGroup
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))

	tk1.MustExec("create table test.auto_commit_test (a int)")
	wg.Add(1)
	go func() {
		err := tk1.ExecToErr("INSERT INTO test.auto_commit_test VALUES (SLEEP(1))")
		require.True(t, terror.ErrorEqual(err, plannererrors.ErrSQLInReadOnlyMode), fmt.Sprintf("err %v", err))
		wg.Done()
	}()
	tk2.MustExec("SET GLOBAL tidb_restricted_read_only = 1")
	err := tk2.ExecToErr("INSERT INTO test.auto_commit_test VALUES (0)") // should also be an error
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrSQLInReadOnlyMode), fmt.Sprintf("err %v", err))
	// Reset and check with the privilege to ignore the readonly flag and continue to insert.
	wg.Wait()
	tk1.MustExec("SET GLOBAL tidb_restricted_read_only = 0")
	tk1.MustExec("SET GLOBAL tidb_super_read_only = 0")
	tk1.MustExec("GRANT RESTRICTED_REPLICA_WRITER_ADMIN on *.* to 'root'")

	wg.Add(1)
	go func() {
		tk1.MustExec("INSERT INTO test.auto_commit_test VALUES (SLEEP(1))")
		wg.Done()
	}()
	tk2.MustExec("SET GLOBAL tidb_restricted_read_only = 1")
	tk2.MustExec("INSERT INTO test.auto_commit_test VALUES (0)")

	// wait for go routines
	wg.Wait()
	tk1.MustExec("SET GLOBAL tidb_restricted_read_only = 0")
	tk1.MustExec("SET GLOBAL tidb_super_read_only = 0")
}

func TestTxnRetryErrMsg(t *testing.T) {
	store := testkit.CreateMockStore(t)
	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("create table no_retry (id int)")
	tk1.MustExec("insert into no_retry values (1)")
	tk1.MustExec("begin")
	tk2.MustExec("use test")
	tk2.MustExec("update no_retry set id = id + 1")
	tk1.MustExec("update no_retry set id = id + 1")
	require.NoError(t, failpoint.Enable("tikvclient/mockRetryableErrorResp", `return(true)`))
	_, err := tk1.Session().Execute(context.Background(), "commit")
	require.NoError(t, failpoint.Disable("tikvclient/mockRetryableErrorResp"))
	require.Error(t, err)
	require.True(t, kv.ErrTxnRetryable.Equal(err), "error: %s", err)
	require.True(t, strings.Contains(err.Error(), "mock retryable error"), "error: %s", err)
	require.True(t, strings.Contains(err.Error(), kv.TxnRetryableMark), "error: %s", err)
}

func TestErrorRollback(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_rollback")
	tk.MustExec("create table t_rollback (c1 int, c2 int, primary key(c1))")
	tk.MustExec("insert into t_rollback values (0, 0)")

	var wg sync.WaitGroup
	cnt := 4
	wg.Add(cnt)
	num := 20

	for range cnt {
		go func() {
			defer wg.Done()
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("set @@session.tidb_retry_limit = 100")
			for range num {
				_, _ = tk.Exec("insert into t_rollback values (1, 1)")
				tk.MustExec("update t_rollback set c2 = c2 + 1 where c1 = 0")
			}
		}()
	}

	wg.Wait()
	tk.MustQuery("select c2 from t_rollback where c1 = 0").Check(testkit.Rows(fmt.Sprint(cnt * num)))
}

// TestInTrans . See https://dev.mysql.com/doc/internals/en/status-flags.html
func TestInTrans(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	tk.MustExec("insert t values ()")
	tk.MustExec("begin")
	txn, err := tk.Session().Txn(true)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	tk.MustExec("insert t values ()")
	require.True(t, txn.Valid())
	tk.MustExec("drop table if exists t;")
	require.False(t, txn.Valid())
	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	require.False(t, txn.Valid())
	tk.MustExec("insert t values ()")
	require.False(t, txn.Valid())
	tk.MustExec("commit")
	tk.MustExec("insert t values ()")

	tk.MustExec("set autocommit=0")
	tk.MustExec("begin")
	require.True(t, txn.Valid())
	tk.MustExec("insert t values ()")
	require.True(t, txn.Valid())
	tk.MustExec("commit")
	require.False(t, txn.Valid())
	tk.MustExec("insert t values ()")
	require.True(t, txn.Valid())
	tk.MustExec("commit")
	require.False(t, txn.Valid())

	tk.MustExec("set autocommit=1")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	tk.MustExec("begin")
	require.True(t, txn.Valid())
	tk.MustExec("insert t values ()")
	require.True(t, txn.Valid())
	tk.MustExec("rollback")
	require.False(t, txn.Valid())
}

func TestCommitTSOrderCheck(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int)")
	currentTS, err := store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	require.NoError(t, err)
	ts := oracle.GoTimeToTS(oracle.GetTimeFromTS(currentTS).Add(time.Minute))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/mockFutureCommitTS", fmt.Sprintf("return(%d)", ts)))
	tk.MustExec("insert into t values(123)")
	_, err = tk.Exec("select * from t")
	require.Regexp(t, fmt.Sprintf(`start_ts:\d+ is before session last_commit_ts:%d`, ts), err.Error())
}

func TestMemBufferSnapshotRead(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int primary key, b int, index i(b));")

	tk.MustExec("set session tidb_distsql_scan_concurrency = 1;")
	tk.MustExec("set session tidb_index_lookup_join_concurrency = 1;")
	tk.MustExec("set session tidb_projection_concurrency=1;")
	tk.MustExec("set session tidb_init_chunk_size=1;")
	tk.MustExec("set session tidb_max_chunk_size=40;")
	tk.MustExec("set session tidb_index_join_batch_size = 10")

	tk.MustExec("begin;")
	// write (0, 0), (1, 1), ... ,(100, 100) into membuffer
	var sb strings.Builder
	sb.WriteString("insert into t values ")
	for i := 0; i <= 100; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("(%d, %d)", i, i))
	}
	tk.MustExec(sb.String())

	// insert on duplicate key statement should update the table to (0, 100), (1, 99), ... (100, 0)
	// This statement will create UnionScan dynamically during execution, and some UnionScan will see staging data(should be bypassed),
	// so it relies on correct snapshot read to get the expected result.
	tk.MustExec("insert into t (select /*+ INL_JOIN(t1) */ 100 - t1.a as a, t1.b from t t1, (select a, b from t) t2 where t1.b = t2.b) on duplicate key update b = values(b)")

	require.Empty(t, tk.MustQuery("select a, b from t where a + b != 100;").Rows())
	tk.MustExec("commit;")
	require.Empty(t, tk.MustQuery("select a, b from t where a + b != 100;").Rows())

	tk.MustExec("set session tidb_distsql_scan_concurrency = default;")
	tk.MustExec("set session tidb_index_lookup_join_concurrency = default;")
	tk.MustExec("set session tidb_projection_concurrency=default;")
	tk.MustExec("set session tidb_init_chunk_size=default;")
	tk.MustExec("set session tidb_max_chunk_size=default;")
	tk.MustExec("set session tidb_index_join_batch_size = default")
}

func TestMemBufferCleanupMemoryLeak(t *testing.T) {
	// Test if cleanup memory will cause a memory leak.
	// When an in-txn statement fails, TiDB cleans up the mutations from this statement.
	// If there's a memory leak, the memory usage could increase uncontrollably with retries.
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a varchar(255) primary key)")
	key1 := strings.Repeat("a", 255)
	key2 := strings.Repeat("b", 255)
	tk.MustExec(`set global tidb_mem_oom_action='cancel'`)
	tk.MustExec("set session tidb_mem_quota_query=10240")
	tk.MustExec("begin")
	tk.MustExec("insert into t values(?)", key2)
	for range 100 {
		// The insert statement will fail because of the duplicate key error.
		err := tk.ExecToErr("insert into t values(?), (?)", key1, key2)
		require.Error(t, err)
		if strings.Contains(err.Error(), "Duplicate") {
			continue
		}
		require.NoError(t, err)
	}
	tk.MustExec("commit")
}

func TestPanicOnRollbackKilledTxn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t values(1);")
	tk.MustExec("insert into t select * from t")
	tk.MustExec("insert into t select * from t")
	tk.MustExec("insert into t select * from t")
	tk.MustExec("insert into t select * from t")
	tk.MustExec("insert into t select * from t")
	tk.MustExec("insert into t select * from t")
	mockTracker := memory.NewTracker(-1, -1)
	mockTracker.IsRootTrackerOfSess = true
	mockTracker.Killer = &sqlkiller.SQLKiller{}
	tk.Session().GetSessionVars().MemTracker.AttachTo(mockTracker)
	mockTracker.Killer.SendKillSignal(sqlkiller.QueryInterrupted)
	tk.Session().Close()
}
