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
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/session/txninfo"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestBasicTxnState(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t(a) values (1);")
	info := tk.Session().TxnInfo()
	require.Nil(t, info)

	tk.MustExec("begin pessimistic;")
	startTSStr := tk.MustQuery("select @@tidb_current_ts;").Rows()[0][0].(string)
	startTS, err := strconv.ParseUint(startTSStr, 10, 64)
	require.NoError(t, err)

	require.NoError(t, failpoint.Enable("tikvclient/beforePessimisticLock", "pause"))
	defer func() { require.NoError(t, failpoint.Disable("tikvclient/beforePessimisticLock")) }()
	ch := make(chan interface{})
	go func() {
		tk.MustExec("select * from t for update;")
		ch <- nil
	}()
	time.Sleep(100 * time.Millisecond)

	info = tk.Session().TxnInfo()
	_, expectedDigest := parser.NormalizeDigest("select * from t for update;")
	require.Equal(t, expectedDigest.String(), info.CurrentSQLDigest)
	require.Equal(t, txninfo.TxnLockWaiting, info.State)
	require.True(t, info.BlockStartTime.Valid)
	require.Equal(t, startTS, info.StartTS)

	require.NoError(t, failpoint.Disable("tikvclient/beforePessimisticLock"))
	<-ch

	info = tk.Session().TxnInfo()
	require.Equal(t, "", info.CurrentSQLDigest)
	require.Equal(t, txninfo.TxnIdle, info.State)
	require.False(t, info.BlockStartTime.Valid)
	require.Equal(t, startTS, info.StartTS)
	_, beginDigest := parser.NormalizeDigest("begin pessimistic;")
	_, selectTSDigest := parser.NormalizeDigest("select @@tidb_current_ts;")
	require.Equal(t, []string{beginDigest.String(), selectTSDigest.String(), expectedDigest.String()}, info.AllSQLDigests)

	// len and size will be covered in TestLenAndSize
	require.Equal(t, tk.Session().GetSessionVars().ConnectionID, info.ConnectionID)
	require.Equal(t, "", info.Username)
	require.Equal(t, "test", info.CurrentDB)
	require.Equal(t, startTS, info.StartTS)

	require.NoError(t, failpoint.Enable("tikvclient/beforePrewrite", "pause"))
	go func() {
		tk.MustExec("commit;")
		ch <- nil
	}()
	time.Sleep(100 * time.Millisecond)
	_, commitDigest := parser.NormalizeDigest("commit;")
	info = tk.Session().TxnInfo()
	require.Equal(t, commitDigest.String(), info.CurrentSQLDigest)
	require.Equal(t, txninfo.TxnCommitting, info.State)
	require.Equal(t, []string{beginDigest.String(), selectTSDigest.String(), expectedDigest.String(), commitDigest.String()}, info.AllSQLDigests)

	require.NoError(t, failpoint.Disable("tikvclient/beforePrewrite"))
	<-ch
	info = tk.Session().TxnInfo()
	require.Nil(t, info)

	// Test autocommit transaction
	require.NoError(t, failpoint.Enable("tikvclient/beforePrewrite", "pause"))
	go func() {
		tk.MustExec("insert into t values (2)")
		ch <- nil
	}()
	time.Sleep(100 * time.Millisecond)
	info = tk.Session().TxnInfo()
	_, expectedDigest = parser.NormalizeDigest("insert into t values (2)")
	require.Equal(t, expectedDigest.String(), info.CurrentSQLDigest)
	require.Equal(t, txninfo.TxnCommitting, info.State)
	require.False(t, info.BlockStartTime.Valid)
	require.Greater(t, info.StartTS, startTS)
	require.Equal(t, 1, len(info.AllSQLDigests))
	require.Equal(t, expectedDigest.String(), info.AllSQLDigests[0])

	require.NoError(t, failpoint.Disable("tikvclient/beforePrewrite"))
	<-ch
	info = tk.Session().TxnInfo()
	require.Nil(t, info)
}

func TestEntriesCountAndSize(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int);")
	tk.MustExec("begin pessimistic;")
	tk.MustExec("insert into t(a) values (1);")
	info := tk.Session().TxnInfo()
	require.Equal(t, uint64(1), info.EntriesCount)
	require.Equal(t, uint64(29), info.EntriesSize)
	tk.MustExec("insert into t(a) values (2);")
	info = tk.Session().TxnInfo()
	require.Equal(t, uint64(2), info.EntriesCount)
	require.Equal(t, uint64(58), info.EntriesSize)
	tk.MustExec("commit;")
}

func TestRunning(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t(a) values (1);")
	tk.MustExec("begin pessimistic;")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/mockStmtSlow", "return(200)"))
	ch := make(chan struct{})
	go func() {
		tk.MustExec("select * from t for update /* sleep */;")
		tk.MustExec("commit;")
		ch <- struct{}{}
	}()
	time.Sleep(100 * time.Millisecond)
	info := tk.Session().TxnInfo()
	require.Equal(t, txninfo.TxnRunning, info.State)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/session/mockStmtSlow"))
	<-ch
}

func TestBlocked(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")

	tk1.MustExec("create table t(a int);")
	tk1.MustExec("insert into t(a) values (1);")
	tk1.MustExec("begin pessimistic;")
	tk1.MustExec("select * from t where a = 1 for update;")
	ch := make(chan struct{})
	go func() {
		tk2.MustExec("begin pessimistic")
		tk2.MustExec("select * from t where a = 1 for update;")
		tk2.MustExec("commit;")
		ch <- struct{}{}
	}()
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, txninfo.TxnLockWaiting, tk2.Session().TxnInfo().State)
	require.NotNil(t, tk2.Session().TxnInfo().BlockStartTime)
	tk1.MustExec("commit;")
	<-ch
}

func TestCommitting(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")

	tk1.MustExec("create table t(a int);")
	tk1.MustExec("insert into t(a) values (1), (2);")
	tk1.MustExec("begin pessimistic;")
	tk1.MustExec("select * from t where a = 1 for update;")
	ch := make(chan struct{})
	go func() {
		tk2.MustExec("begin pessimistic")
		require.NotNil(t, tk2.Session().TxnInfo())
		tk2.MustExec("select * from t where a = 2 for update;")
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/mockSlowCommit", "pause"))
		tk2.MustExec("commit;")
		ch <- struct{}{}
	}()
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, txninfo.TxnCommitting, tk2.Session().TxnInfo().State)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/session/mockSlowCommit"))
	tk1.MustExec("commit;")
	<-ch
}

func TestRollbackTxnState(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t(a) values (1), (2);")
	ch := make(chan struct{})
	go func() {
		tk.MustExec("begin pessimistic")
		tk.MustExec("insert into t(a) values (3);")
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/mockSlowRollback", "pause"))
		tk.MustExec("rollback;")
		ch <- struct{}{}
	}()
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/session/mockSlowRollback"))
	require.Equal(t, txninfo.TxnRollingBack, tk.Session().TxnInfo().State)
	<-ch
}

func TestTxnInfoWithPreparedStmt(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("prepare s1 from 'insert into t values (?)'")
	tk.MustExec("set @v = 1")

	tk.MustExec("begin pessimistic")
	require.NoError(t, failpoint.Enable("tikvclient/beforePessimisticLock", "pause"))
	ch := make(chan interface{})
	go func() {
		tk.MustExec("execute s1 using @v")
		ch <- nil
	}()
	time.Sleep(100 * time.Millisecond)
	info := tk.Session().TxnInfo()
	_, expectDigest := parser.NormalizeDigest("insert into t values (?)")
	require.Equal(t, expectDigest.String(), info.CurrentSQLDigest)

	require.NoError(t, failpoint.Disable("tikvclient/beforePessimisticLock"))
	<-ch
	info = tk.Session().TxnInfo()
	require.Equal(t, "", info.CurrentSQLDigest)
	_, beginDigest := parser.NormalizeDigest("begin pessimistic")
	require.Equal(t, []string{beginDigest.String(), expectDigest.String()}, info.AllSQLDigests)

	tk.MustExec("rollback")
}

func TestTxnInfoWithScalarSubquery(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (1, 10), (2, 1)")

	tk.MustExec("begin pessimistic")
	_, beginDigest := parser.NormalizeDigest("begin pessimistic")
	tk.MustExec("select * from t where a = (select b from t where a = 2)")
	_, s1Digest := parser.NormalizeDigest("select * from t where a = (select b from t where a = 2)")

	require.NoError(t, failpoint.Enable("tikvclient/beforePessimisticLock", "pause"))
	ch := make(chan interface{})
	go func() {
		tk.MustExec("update t set b = b + 1 where a = (select b from t where a = 2)")
		ch <- nil
	}()
	_, s2Digest := parser.NormalizeDigest("update t set b = b + 1 where a = (select b from t where a = 1)")
	time.Sleep(100 * time.Millisecond)
	info := tk.Session().TxnInfo()
	require.Equal(t, s2Digest.String(), info.CurrentSQLDigest)
	require.Equal(t, []string{beginDigest.String(), s1Digest.String(), s2Digest.String()}, info.AllSQLDigests)

	require.NoError(t, failpoint.Disable("tikvclient/beforePessimisticLock"))
	<-ch
	tk.MustExec("rollback")
}

func TestTxnInfoWithPSProtocol(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key)")

	// Test autocommit transaction

	idInsert, _, _, err := tk.Session().PrepareStmt("insert into t values (?)")
	require.NoError(t, err)

	require.NoError(t, failpoint.Enable("tikvclient/beforePrewrite", "pause"))
	ch := make(chan interface{})
	go func() {
		_, err := tk.Session().ExecutePreparedStmt(context.Background(), idInsert, types.MakeDatums(1))
		require.NoError(t, err)
		ch <- nil
	}()
	time.Sleep(100 * time.Millisecond)
	_, digest := parser.NormalizeDigest("insert into t values (1)")
	info := tk.Session().TxnInfo()
	require.NotNil(t, info)
	require.Greater(t, info.StartTS, uint64(0))
	require.Equal(t, txninfo.TxnCommitting, info.State)
	require.Equal(t, digest.String(), info.CurrentSQLDigest)
	require.Equal(t, []string{digest.String()}, info.AllSQLDigests)

	require.NoError(t, failpoint.Disable("tikvclient/beforePrewrite"))
	<-ch
	info = tk.Session().TxnInfo()
	require.Nil(t, info)

	// Test non-autocommit transaction

	id1, _, _, err := tk.Session().PrepareStmt("select * from t where a = ?")
	require.NoError(t, err)
	_, digest1 := parser.NormalizeDigest("select * from t where a = ?")
	id2, _, _, err := tk.Session().PrepareStmt("update t set a = a + 1 where a = ?")
	require.NoError(t, err)
	_, digest2 := parser.NormalizeDigest("update t set a = a + 1 where a = ?")

	tk.MustExec("begin pessimistic")

	_, err = tk.Session().ExecutePreparedStmt(context.Background(), id1, types.MakeDatums(1))
	require.NoError(t, err)

	require.NoError(t, failpoint.Enable("tikvclient/beforePessimisticLock", "pause"))
	go func() {
		_, err := tk.Session().ExecutePreparedStmt(context.Background(), id2, types.MakeDatums(1))
		require.NoError(t, err)
		ch <- nil
	}()
	time.Sleep(100 * time.Millisecond)
	info = tk.Session().TxnInfo()
	require.Greater(t, info.StartTS, uint64(0))
	require.Equal(t, digest2.String(), info.CurrentSQLDigest)
	require.Equal(t, txninfo.TxnLockWaiting, info.State)
	require.True(t, info.BlockStartTime.Valid)
	_, beginDigest := parser.NormalizeDigest("begin pessimistic")
	require.Equal(t, []string{beginDigest.String(), digest1.String(), digest2.String()}, info.AllSQLDigests)

	require.NoError(t, failpoint.Disable("tikvclient/beforePessimisticLock"))
	<-ch
	tk.MustExec("rollback")
}
