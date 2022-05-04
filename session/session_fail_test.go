// Copyright 2018 PingCAP, Inc.
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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestFailStatementCommitInRetry(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()

	tk := createTestKit(t, store)
	tk.MustExec("create table t (id int)")

	tk.MustExec("begin")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t values (2),(3),(4),(5)")
	tk.MustExec("insert into t values (6)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/mockCommitError8942", `return(true)`))
	_, err := tk.Exec("commit")
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/session/mockCommitError8942"))

	tk.MustExec("insert into t values (6)")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("6"))
}

func TestGetTSFailDirtyState(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()

	tk := createTestKit(t, store)
	tk.MustExec("create table t (id int)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/mockGetTSFail", "return"))
	ctx := failpoint.WithHook(context.Background(), func(ctx context.Context, fpname string) bool {
		return fpname == "github.com/pingcap/tidb/session/mockGetTSFail"
	})
	_, err := tk.Session().Execute(ctx, "select * from t")
	if config.GetGlobalConfig().Store == "unistore" {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}

	// Fix a bug that active txn fail set TxnState.fail to error, and then the following write
	// affected by this fail flag.
	tk.MustExec("insert into t values (1)")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("1"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/session/mockGetTSFail"))
}

func TestGetTSFailDirtyStateInretry(t *testing.T) {
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/session/mockCommitError"))
		require.NoError(t, failpoint.Disable("tikvclient/mockGetTSErrorInRetry"))
	}()

	store, clean := createStorage(t)
	defer clean()

	tk := createTestKit(t, store)
	tk.MustExec("create table t (id int)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/mockCommitError", `return(true)`))
	// This test will mock a PD timeout error, and recover then.
	// Just make mockGetTSErrorInRetry return true once, and then return false.
	require.NoError(t, failpoint.Enable("tikvclient/mockGetTSErrorInRetry",
		`1*return(true)->return(false)`))
	tk.MustExec("insert into t values (2)")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("2"))
}

func TestKillFlagInBackoff(t *testing.T) {
	// This test checks the `killed` flag is passed down to the backoffer through
	// session.KVVars.
	store, clean := createStorage(t)
	defer clean()

	tk := createTestKit(t, store)
	tk.MustExec("create table kill_backoff (id int)")
	// Inject 1 time timeout. If `Killed` is not successfully passed, it will retry and complete query.
	require.NoError(t, failpoint.Enable("tikvclient/tikvStoreSendReqResult", `return("timeout")->return("")`))
	defer failpoint.Disable("tikvclient/tikvStoreSendReqResult")
	// Set kill flag and check its passed to backoffer.
	tk.Session().GetSessionVars().Killed = 1
	rs, err := tk.Exec("select * from kill_backoff")
	require.NoError(t, err)
	_, err = session.ResultSetToStringSlice(context.TODO(), tk.Session(), rs)
	// `interrupted` is returned when `Killed` is set.
	require.Regexp(t, ".*Query execution was interrupted.*", err.Error())
}

func TestClusterTableSendError(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()

	tk := createTestKit(t, store)
	require.NoError(t, failpoint.Enable("tikvclient/tikvStoreSendReqResult", `return("requestTiDBStoreError")`))
	defer failpoint.Disable("tikvclient/tikvStoreSendReqResult")
	tk.MustQuery("select * from information_schema.cluster_slow_query")
	require.Equal(t, tk.Session().GetSessionVars().StmtCtx.WarningCount(), uint16(1))
	require.Regexp(t, ".*TiDB server timeout, address is.*", tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err.Error())
}

func TestAutoCommitNeedNotLinearizability(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()

	tk := createTestKit(t, store)
	tk.MustExec("drop table if exists t1;")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec(`create table t1 (c int)`)

	require.NoError(t, failpoint.Enable("tikvclient/getMinCommitTSFromTSO", `panic`))
	defer func() {
		require.NoError(t, failpoint.Disable("tikvclient/getMinCommitTSFromTSO"))
	}()

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("tidb_enable_async_commit", "1"))
	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("tidb_guarantee_linearizability", "1"))

	// Auto-commit transactions don't need to get minCommitTS from TSO
	tk.MustExec("INSERT INTO t1 VALUES (1)")

	tk.MustExec("BEGIN")
	tk.MustExec("INSERT INTO t1 VALUES (2)")
	// An explicit transaction needs to get minCommitTS from TSO
	func() {
		defer func() {
			err := recover()
			require.NotNil(t, err)
		}()
		tk.MustExec("COMMIT")
	}()

	tk.MustExec("set autocommit = 0")
	tk.MustExec("INSERT INTO t1 VALUES (3)")
	func() {
		defer func() {
			err := recover()
			require.NotNil(t, err)
		}()
		tk.MustExec("COMMIT")
	}()

	// Same for 1PC
	tk.MustExec("set autocommit = 1")
	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("tidb_enable_1pc", "1"))
	tk.MustExec("INSERT INTO t1 VALUES (4)")

	tk.MustExec("BEGIN")
	tk.MustExec("INSERT INTO t1 VALUES (5)")
	func() {
		defer func() {
			err := recover()
			require.NotNil(t, err)
		}()
		tk.MustExec("COMMIT")
	}()

	tk.MustExec("set autocommit = 0")
	tk.MustExec("INSERT INTO t1 VALUES (6)")
	func() {
		defer func() {
			err := recover()
			require.NotNil(t, err)
		}()
		tk.MustExec("COMMIT")
	}()
}

func TestGlobalAndLocalTxn(t *testing.T) {
	// Because the PD config of check_dev_2 test is not compatible with local/global txn yet,
	// so we will skip this test for now.
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
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
	require.Equal(t, 1, len(result.Rows()))

	// begin and commit with global txn scope
	tk.MustExec("begin")
	txn, err := tk.Session().Txn(true)
	require.NoError(t, err)
	require.Equal(t, kv.GlobalTxnScope, tk.Session().GetSessionVars().TxnCtx.TxnScope)
	require.True(t, txn.Valid())
	tk.MustExec("insert into t1 (c) values (1)") // write dc-1 with global scope
	result = tk.MustQuery("select * from t1")    // read dc-1 and dc-2 with global scope
	require.Equal(t, 2, len(result.Rows()))
	require.True(t, txn.Valid())
	tk.MustExec("commit")
	result = tk.MustQuery("select * from t1")
	require.Equal(t, 2, len(result.Rows()))

	// begin and rollback with global txn scope
	tk.MustExec("begin")
	txn, err = tk.Session().Txn(true)
	require.NoError(t, err)
	require.Equal(t, kv.GlobalTxnScope, tk.Session().GetSessionVars().TxnCtx.TxnScope)
	require.True(t, txn.Valid())
	tk.MustExec("insert into t1 (c) values (101)") // write dc-2 with global scope
	result = tk.MustQuery("select * from t1")      // read dc-1 and dc-2 with global scope
	require.Len(t, result.Rows(), 3)
	require.True(t, txn.Valid())
	tk.MustExec("rollback")
	result = tk.MustQuery("select * from t1")
	require.Len(t, result.Rows(), 2)
	timeBeforeWriting := time.Now()
	tk.MustExec("insert into t1 (c) values (101)") // write dc-2 with global scope
	result = tk.MustQuery("select * from t1")      // read dc-1 and dc-2 with global scope
	require.Len(t, result.Rows(), 3)

	require.NoError(t, failpoint.Enable("tikvclient/injectTxnScope", `return("dc-1")`))
	defer func() {
		require.NoError(t, failpoint.Disable("tikvclient/injectTxnScope"))
	}()
	// set txn_scope to local
	tk.MustExec("set @@session.txn_scope = 'local';")
	result = tk.MustQuery("select @@txn_scope;")
	result.Check(testkit.Rows("local"))

	// test local txn auto commit
	tk.MustExec("insert into t1 (c) values (1)")          // write dc-1 with dc-1 scope
	result = tk.MustQuery("select * from t1 where c = 1") // point get dc-1 with dc-1 scope
	require.Len(t, result.Rows(), 3)
	result = tk.MustQuery("select * from t1 where c < 100") // read dc-1 with dc-1 scope
	require.Len(t, result.Rows(), 3)

	// begin and commit with dc-1 txn scope
	tk.MustExec("begin")
	txn, err = tk.Session().Txn(true)
	require.NoError(t, err)
	require.Equal(t, "dc-1", tk.Session().GetSessionVars().CheckAndGetTxnScope())
	require.True(t, txn.Valid())
	tk.MustExec("insert into t1 (c) values (1)")            // write dc-1 with dc-1 scope
	result = tk.MustQuery("select * from t1 where c < 100") // read dc-1 with dc-1 scope
	require.Len(t, result.Rows(), 4)
	require.True(t, txn.Valid())
	tk.MustExec("commit")
	result = tk.MustQuery("select * from t1 where c < 100")
	require.Len(t, result.Rows(), 4)

	// begin and rollback with dc-1 txn scope
	tk.MustExec("begin")
	txn, err = tk.Session().Txn(true)
	require.NoError(t, err)
	require.Equal(t, "dc-1", tk.Session().GetSessionVars().CheckAndGetTxnScope())
	require.True(t, txn.Valid())
	tk.MustExec("insert into t1 (c) values (1)")            // write dc-1 with dc-1 scope
	result = tk.MustQuery("select * from t1 where c < 100") // read dc-1 with dc-1 scope
	require.Len(t, result.Rows(), 5)
	require.True(t, txn.Valid())
	tk.MustExec("rollback")
	result = tk.MustQuery("select * from t1 where c < 100")
	require.Len(t, result.Rows(), 4)

	// test wrong scope local txn auto commit
	_, err = tk.Exec("insert into t1 (c) values (101)") // write dc-2 with dc-1 scope
	require.Error(t, err)
	require.Regexp(t, ".*out of txn_scope.*", err)
	err = tk.ExecToErr("select * from t1 where c = 101") // point get dc-2 with dc-1 scope
	require.Error(t, err)
	require.Regexp(t, ".*can not be read by.*", err)
	err = tk.ExecToErr("select * from t1 where c > 100") // read dc-2 with dc-1 scope
	require.Error(t, err)
	require.Regexp(t, ".*can not be read by.*", err)
	tk.MustExec("begin")
	err = tk.ExecToErr("select * from t1 where c = 101") // point get dc-2 with dc-1 scope
	require.Error(t, err)
	require.Regexp(t, ".*can not be read by.*", err)
	err = tk.ExecToErr("select * from t1 where c > 100") // read dc-2 with dc-1 scope
	require.Error(t, err)
	require.Regexp(t, ".*can not be read by.*", err)
	tk.MustExec("commit")

	// begin and commit reading & writing the data in dc-2 with dc-1 txn scope
	tk.MustExec("begin")
	txn, err = tk.Session().Txn(true)
	require.NoError(t, err)
	require.Equal(t, "dc-1", tk.Session().GetSessionVars().CheckAndGetTxnScope())
	require.True(t, txn.Valid())
	tk.MustExec("insert into t1 (c) values (101)")       // write dc-2 with dc-1 scope
	err = tk.ExecToErr("select * from t1 where c > 100") // read dc-2 with dc-1 scope
	require.Error(t, err)
	require.Regexp(t, ".*can not be read by.*", err)
	tk.MustExec("insert into t1 (c) values (99)")           // write dc-1 with dc-1 scope
	result = tk.MustQuery("select * from t1 where c < 100") // read dc-1 with dc-1 scope
	require.Equal(t, 5, len(result.Rows()))
	require.True(t, txn.Valid())
	_, err = tk.Exec("commit")
	require.Error(t, err)
	require.Regexp(t, ".*out of txn_scope.*", err)
	// Won't read the value 99 because the previous commit failed
	result = tk.MustQuery("select * from t1 where c < 100") // read dc-1 with dc-1 scope
	require.Len(t, result.Rows(), 4)

	// Stale Read will ignore the cross-dc txn scope.
	require.Equal(t, "dc-1", tk.Session().GetSessionVars().CheckAndGetTxnScope())
	result = tk.MustQuery("select @@txn_scope;")
	result.Check(testkit.Rows("local"))
	err = tk.ExecToErr("select * from t1 where c > 100") // read dc-2 with dc-1 scope
	require.Error(t, err)
	require.Regexp(t, ".*can not be read by.*", err)
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
