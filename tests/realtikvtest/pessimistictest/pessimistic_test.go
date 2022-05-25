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

package pessimistictest

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	storeerr "github.com/pingcap/tidb/store/driver/error"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/deadlockhistory"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

func createAsyncCommitTestKit(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.Session().GetSessionVars().EnableAsyncCommit = true
	tk.MustExec("use test")
	return tk
}

// TODO: figure out a stable way to run Test1PCWithSchemaChange
//nolint:unused
func create1PCTestKit(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.Session().GetSessionVars().Enable1PC = true
	tk.MustExec("use test")
	return tk
}

type lockTTL uint64

func setLockTTL(v uint64) lockTTL { return lockTTL(atomic.SwapUint64(&transaction.ManagedLockTTL, v)) }

func (v lockTTL) restore() { atomic.StoreUint64(&transaction.ManagedLockTTL, uint64(v)) }

func TestPessimisticTxn(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Make the name has different indent for easier read.
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	tk.MustExec("drop table if exists pessimistic")
	tk.MustExec("create table pessimistic (k int, v int)")
	tk.MustExec("insert into pessimistic values (1, 1)")

	// t1 lock, t2 update, t1 update and retry statement.
	tk1.MustExec("begin pessimistic")

	tk.MustExec("update pessimistic set v = 2 where v = 1")

	// Update can see the change, so this statement affects 0 rows.
	tk1.MustExec("update pessimistic set v = 3 where v = 1")
	require.Equal(t, uint64(0), tk1.Session().AffectedRows())
	require.Equal(t, 0, session.GetHistory(tk1.Session()).Count())
	// select for update can see the change of another transaction.
	tk1.MustQuery("select * from pessimistic for update").Check(testkit.Rows("1 2"))
	// plain select can not see the change of another transaction.
	tk1.MustQuery("select * from pessimistic").Check(testkit.Rows("1 1"))
	tk1.MustExec("update pessimistic set v = 3 where v = 2")
	require.Equal(t, uint64(1), tk1.Session().AffectedRows())

	// pessimistic lock doesn't block read operation of other transactions.
	tk.MustQuery("select * from pessimistic").Check(testkit.Rows("1 2"))

	tk1.MustExec("commit")
	tk1.MustQuery("select * from pessimistic").Check(testkit.Rows("1 3"))

	// t1 lock, t1 select for update, t2 wait t1.
	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from pessimistic where k = 1 for update")
	finishCh := make(chan struct{})
	go func() {
		tk.MustExec("update pessimistic set v = 5 where k = 1")
		finishCh <- struct{}{}
	}()
	time.Sleep(time.Millisecond * 10)
	tk1.MustExec("update pessimistic set v = 3 where k = 1")
	tk1.MustExec("commit")
	<-finishCh
	tk.MustQuery("select * from pessimistic").Check(testkit.Rows("1 5"))
}

func TestTxnMode(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tests := []struct {
		beginStmt     string
		txnMode       string
		isPessimistic bool
	}{
		{"pessimistic", "pessimistic", true},
		{"pessimistic", "optimistic", true},
		{"pessimistic", "", true},
		{"optimistic", "pessimistic", false},
		{"optimistic", "optimistic", false},
		{"optimistic", "", false},
		{"", "pessimistic", true},
		{"", "optimistic", false},
		{"", "", false},
	}
	for _, tt := range tests {
		tk.MustExec(fmt.Sprintf("set @@tidb_txn_mode = '%s'", tt.txnMode))
		tk.MustExec("begin " + tt.beginStmt)
		require.Equal(t, tt.isPessimistic, tk.Session().GetSessionVars().TxnCtx.IsPessimistic)
		tk.MustExec("rollback")
	}

	tk.MustExec("set @@autocommit = 0")
	tk.MustExec("create table if not exists txn_mode (a int)")
	tests2 := []struct {
		txnMode       string
		isPessimistic bool
	}{
		{"pessimistic", true},
		{"optimistic", false},
		{"", false},
	}
	for _, tt := range tests2 {
		tk.MustExec(fmt.Sprintf("set @@tidb_txn_mode = '%s'", tt.txnMode))
		tk.MustExec("rollback")
		tk.MustExec("insert txn_mode values (1)")
		require.Equal(t, tt.isPessimistic, tk.Session().GetSessionVars().TxnCtx.IsPessimistic)
		tk.MustExec("rollback")
	}
	tk.MustExec("set @@global.tidb_txn_mode = 'pessimistic'")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustQuery("select @@tidb_txn_mode").Check(testkit.Rows("pessimistic"))
	tk1.MustExec("set @@autocommit = 0")
	tk1.MustExec("insert txn_mode values (2)")
	require.True(t, tk1.Session().GetSessionVars().TxnCtx.IsPessimistic)
	tk1.MustExec("set @@tidb_txn_mode = ''")
	tk1.MustExec("rollback")
	tk1.MustExec("insert txn_mode values (2)")
	require.False(t, tk1.Session().GetSessionVars().TxnCtx.IsPessimistic)
	tk1.MustExec("rollback")
}

func TestDeadlock(t *testing.T) {
	deadlockhistory.GlobalDeadlockHistory.Clear()
	deadlockhistory.GlobalDeadlockHistory.Resize(10)

	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")

	// Use the root user so that the statements can be recorded into statements_summary table, which is necessary
	// for fetching
	require.True(t, tk1.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk1.MustExec("drop table if exists deadlock")
	tk1.MustExec("create table deadlock (k int primary key, v int)")
	tk1.MustExec("insert into deadlock values (1, 1), (2, 1)")
	tk1.MustExec("begin pessimistic")
	tk1.MustExec("update deadlock set v = v + 1 where k = 1")
	ts1, err := strconv.ParseUint(tk1.MustQuery("select @@tidb_current_ts").Rows()[0][0].(string), 10, 64)
	require.NoError(t, err)

	require.True(t, tk2.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk2.MustExec("begin pessimistic")
	ts2, err := strconv.ParseUint(tk2.MustQuery("select @@tidb_current_ts").Rows()[0][0].(string), 10, 64)
	require.NoError(t, err)

	syncCh := make(chan error)
	go func() {
		tk2.MustExec("update deadlock set v = v + 1 where k = 2")
		syncCh <- nil
		_, err := tk2.Exec("update deadlock set v = v + 1 where k = 1")
		syncCh <- err

		tk2.MustExec("rollback")
	}()
	<-syncCh
	_, err1 := tk1.Exec("update deadlock set v = v + 1 where k = 2")
	err2 := <-syncCh
	// Either err1 or err2 is deadlock error.
	if err1 != nil {
		require.NoError(t, err2)
		err = err1
	} else {
		err = err2
	}
	e, ok := errors.Cause(err).(*terror.Error)
	require.True(t, ok)
	require.Equal(t, mysql.ErrLockDeadlock, int(e.Code()))

	_, digest := parser.NormalizeDigest("update deadlock set v = v + 1 where k = 1")

	expectedDeadlockInfo := []string{
		fmt.Sprintf("%v/%v/%v/%v", ts1, ts2, digest, "update `deadlock` set `v` = `v` + ? where `k` = ?"),
		fmt.Sprintf("%v/%v/%v/%v", ts2, ts1, digest, "update `deadlock` set `v` = `v` + ? where `k` = ?"),
	}
	// The last one is the transaction that encountered the deadlock error.
	if err1 != nil {
		// Swap the two to match the correct order.
		expectedDeadlockInfo[0], expectedDeadlockInfo[1] = expectedDeadlockInfo[1], expectedDeadlockInfo[0]
	}
	res := tk1.MustQuery("select deadlock_id, try_lock_trx_id, trx_holding_lock, current_sql_digest, current_sql_digest_text from information_schema.deadlocks")
	res.CheckAt([]int{1, 2, 3, 4}, testkit.RowsWithSep("/", expectedDeadlockInfo...))
	require.Equal(t, res.Rows()[1][0], res.Rows()[0][0])
}

func TestSingleStatementRollback(t *testing.T) {
	if *realtikvtest.WithRealTiKV {
		t.Skip("skip with tikv because cluster manipulate is not available")
	}

	var cluster testutils.Cluster
	store, clean := realtikvtest.CreateMockStoreAndSetup(t, mockstore.WithClusterInspector(func(c testutils.Cluster) {
		mockstore.BootstrapWithSingleStore(c)
		cluster = c
	}))
	defer clean()

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")

	tk1.MustExec("drop table if exists pessimistic")
	tk1.MustExec("create table single_statement (id int primary key, v int)")
	tk1.MustExec("insert into single_statement values (1, 1), (2, 1), (3, 1), (4, 1)")

	dom := domain.GetDomain(tk1.Session())
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("single_statement"))
	require.NoError(t, err)
	tblID := tbl.Meta().ID

	tableStart := tablecodec.GenTableRecordPrefix(tblID)
	cluster.SplitKeys(tableStart, tableStart.PrefixNext(), 2)
	region1Key := codec.EncodeBytes(nil, tablecodec.EncodeRowKeyWithHandle(tblID, kv.IntHandle(1)))
	region1, _, _ := cluster.GetRegionByKey(region1Key)
	region1ID := region1.Id
	region2Key := codec.EncodeBytes(nil, tablecodec.EncodeRowKeyWithHandle(tblID, kv.IntHandle(3)))
	region2, _, _ := cluster.GetRegionByKey(region2Key)
	region2ID := region2.Id

	syncCh := make(chan bool)
	require.NoError(t, failpoint.Enable("tikvclient/SingleStmtDeadLockRetrySleep", "return"))
	go func() {
		tk2.MustExec("begin pessimistic")
		<-syncCh
		// tk2 will go first, so tk1 will meet deadlock and retry, tk2 will resolve pessimistic rollback
		// lock on key 3 after lock ttl
		cluster.ScheduleDelay(tk2.Session().GetSessionVars().TxnCtx.StartTS, region2ID, time.Millisecond*3)
		tk2.MustExec("update single_statement set v = v + 1")
		tk2.MustExec("commit")
		<-syncCh
	}()
	tk1.MustExec("begin pessimistic")
	syncCh <- true
	cluster.ScheduleDelay(tk1.Session().GetSessionVars().TxnCtx.StartTS, region1ID, time.Millisecond*10)
	tk1.MustExec("update single_statement set v = v + 1")
	tk1.MustExec("commit")
	require.NoError(t, failpoint.Disable("tikvclient/SingleStmtDeadLockRetrySleep"))
	syncCh <- true
}

func TestFirstStatementFail(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists first")
	tk.MustExec("create table first (k int unique)")
	tk.MustExec("insert first values (1)")
	tk.MustExec("begin pessimistic")
	_, err := tk.Exec("insert first values (1)")
	require.Error(t, err)
	tk.MustExec("insert first values (2)")
	tk.MustExec("commit")
}

func TestKeyExistsCheck(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists chk")
	tk.MustExec("create table chk (k int primary key)")
	tk.MustExec("insert chk values (1)")
	tk.MustExec("delete from chk where k = 1")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert chk values (1)")
	tk.MustExec("commit")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("begin optimistic")
	tk1.MustExec("insert chk values (1), (2), (3)")
	_, err := tk1.Exec("commit")
	require.Error(t, err)

	tk.MustExec("begin pessimistic")
	tk.MustExec("insert chk values (2)")
	tk.MustExec("commit")
}

func TestInsertOnDup(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("drop table if exists dup")
	tk.MustExec("create table dup (id int primary key, c int)")
	tk.MustExec("begin pessimistic")

	tk2.MustExec("insert dup values (1, 1)")
	tk.MustExec("insert dup values (1, 1) on duplicate key update c = c + 1")
	tk.MustExec("commit")
	tk.MustQuery("select * from dup").Check(testkit.Rows("1 2"))
}

func TestPointGetOverflow(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(k tinyint, v int, unique key(k))")
	tk.MustExec("begin pessimistic")
	tk.MustExec("update t set v = 100 where k = -200;")
	tk.MustExec("update t set v = 100 where k in (-200, -400);")
}

func TestPointGetKeyLock(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("drop table if exists point")
	tk.MustExec("create table point (id int primary key, u int unique, c int)")
	syncCh := make(chan struct{})

	tk.MustExec("begin pessimistic")
	tk.MustExec("update point set c = c + 1 where id = 1")
	tk.MustExec("delete from point where u = 2")
	go func() {
		tk2.MustExec("begin pessimistic")
		_, err1 := tk2.Exec("insert point values (1, 1, 1)")
		require.True(t, kv.ErrKeyExists.Equal(err1))
		_, err1 = tk2.Exec("insert point values (2, 2, 2)")
		require.True(t, kv.ErrKeyExists.Equal(err1))
		tk2.MustExec("rollback")
		<-syncCh
	}()
	time.Sleep(time.Millisecond * 10)
	tk.MustExec("insert point values (1, 1, 1)")
	tk.MustExec("insert point values (2, 2, 2)")
	tk.MustExec("commit")
	syncCh <- struct{}{}

	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from point where id = 3 for update")
	tk.MustExec("select * from point where u = 4 for update")
	go func() {
		tk2.MustExec("begin pessimistic")
		_, err1 := tk2.Exec("insert point values (3, 3, 3)")
		require.True(t, kv.ErrKeyExists.Equal(err1))
		_, err1 = tk2.Exec("insert point values (4, 4, 4)")
		require.True(t, kv.ErrKeyExists.Equal(err1))
		tk2.MustExec("rollback")
		<-syncCh
	}()
	time.Sleep(time.Millisecond * 10)
	tk.MustExec("insert point values (3, 3, 3)")
	tk.MustExec("insert point values (4, 4, 4)")
	tk.MustExec("commit")
	syncCh <- struct{}{}
}

func TestBankTransfer(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("drop table if exists accounts")
	tk.MustExec("create table accounts (id int primary key, c int)")
	tk.MustExec("insert accounts values (1, 100), (2, 100), (3, 100)")
	syncCh := make(chan struct{})

	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from accounts where id = 1 for update").Check(testkit.Rows("1 100"))
	go func() {
		tk2.MustExec("begin pessimistic")
		tk2.MustExec("select * from accounts where id = 2 for update")
		<-syncCh
		tk2.MustExec("select * from accounts where id = 3 for update")
		tk2.MustExec("update accounts set c = 50 where id = 2")
		tk2.MustExec("update accounts set c = 150 where id = 3")
		tk2.MustExec("commit")
		<-syncCh
	}()
	syncCh <- struct{}{}
	tk.MustQuery("select * from accounts where id = 2 for update").Check(testkit.Rows("2 50"))
	tk.MustExec("update accounts set c = 50 where id = 1")
	tk.MustExec("update accounts set c = 100 where id = 2")
	tk.MustExec("commit")
	syncCh <- struct{}{}
	tk.MustQuery("select sum(c) from accounts").Check(testkit.Rows("300"))
}

func TestLockUnchangedRowKey(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("drop table if exists unchanged")
	tk.MustExec("create table unchanged (id int primary key, c int)")
	tk.MustExec("insert unchanged values (1, 1), (2, 2)")

	tk.MustExec("begin pessimistic")
	tk.MustExec("update unchanged set c = 1 where id < 2")

	tk2.MustExec("begin pessimistic")
	err := tk2.ExecToErr("select * from unchanged where id = 1 for update nowait")
	require.Error(t, err)

	tk.MustExec("rollback")

	tk2.MustQuery("select * from unchanged where id = 1 for update nowait")

	tk.MustExec("begin pessimistic")
	tk.MustExec("insert unchanged values (2, 2) on duplicate key update c = values(c)")

	err = tk2.ExecToErr("select * from unchanged where id = 2 for update nowait")
	require.Error(t, err)

	tk.MustExec("commit")

	tk2.MustQuery("select * from unchanged where id = 1 for update nowait")
	tk2.MustExec("rollback")
}

func TestOptimisticConflicts(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("drop table if exists conflict")
	tk.MustExec("create table conflict (id int primary key, c int)")
	tk.MustExec("insert conflict values (1, 1)")
	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from conflict where id = 1 for update")
	syncCh := make(chan struct{})
	go func() {
		tk2.MustExec("update conflict set c = 3 where id = 1")
		<-syncCh
	}()
	time.Sleep(time.Millisecond * 10)
	tk.MustExec("update conflict set c = 2 where id = 1")
	tk.MustExec("commit")
	syncCh <- struct{}{}
	tk.MustQuery("select c from conflict where id = 1").Check(testkit.Rows("3"))

	// Check pessimistic lock is not resolved.
	tk.MustExec("begin pessimistic")
	tk.MustExec("update conflict set c = 4 where id = 1")
	tk2.MustExec("begin optimistic")
	tk2.MustExec("update conflict set c = 5 where id = 1")
	require.Error(t, tk2.ExecToErr("commit"))
	tk.MustExec("rollback")

	// Update snapshotTS after a conflict, invalidate snapshot cache.
	tk.MustExec("truncate table conflict")
	tk.MustExec("insert into conflict values (1, 2)")
	tk.MustExec("begin pessimistic")
	// This SQL use BatchGet and cache data in the txn snapshot.
	// It can be changed to other SQLs that use BatchGet.
	tk.MustExec("insert ignore into conflict values (1, 2)")

	tk2.MustExec("update conflict set c = c - 1")

	// Make the txn update its forUpdateTS.
	tk.MustQuery("select * from conflict where id = 1 for update").Check(testkit.Rows("1 1"))
	// Cover a bug that the txn snapshot doesn't invalidate cache after ts change.
	tk.MustExec("insert into conflict values (1, 999) on duplicate key update c = c + 2")
	tk.MustExec("commit")
	tk.MustQuery("select * from conflict").Check(testkit.Rows("1 3"))
}

func TestSelectForUpdateNoWait(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk3.MustExec("use test")

	tk.MustExec("drop table if exists tk")
	tk.MustExec("create table tk (c1 int primary key, c2 int)")
	tk.MustExec("insert into tk values(1,1),(2,2),(3,3),(4,4),(5,5)")

	tk.MustExec("set @@autocommit = 0")
	tk2.MustExec("set @@autocommit = 0")
	tk3.MustExec("set @@autocommit = 0")

	// point get with no autocommit
	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from tk where c1 = 2 for update") // lock succ

	tk2.MustExec("begin pessimistic")
	_, err := tk2.Exec("select * from tk where c1 = 2 for update nowait")
	require.Error(t, err)
	tk.MustExec("commit")
	tk2.MustExec("select * from tk where c1 = 2 for update nowait") // lock succ

	tk3.MustExec("begin pessimistic")
	_, err = tk3.Exec("select * from tk where c1 = 2 for update nowait")
	require.Error(t, err)

	tk2.MustExec("commit")
	tk3.MustExec("select * from tk where c1 = 2 for update")
	tk3.MustExec("commit")
	tk.MustExec("commit")

	tk3.MustExec("begin pessimistic")
	tk3.MustExec("update tk set c2 = c2 + 1 where c1 = 3")
	tk2.MustExec("begin pessimistic")
	_, err = tk2.Exec("select * from tk where c1 = 3 for update nowait")
	require.Error(t, err)
	tk3.MustExec("commit")
	tk2.MustExec("select * from tk where c1 = 3 for update nowait")
	tk2.MustExec("commit")

	tk.MustExec("commit")
	tk2.MustExec("commit")
	tk3.MustExec("commit")

	// scan with no autocommit
	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from tk where c1 >= 2 for update")
	tk2.MustExec("begin pessimistic")
	_, err = tk2.Exec("select * from tk where c1 = 2 for update nowait")
	require.Error(t, err)
	_, err = tk2.Exec("select * from tk where c1 > 3 for update nowait")
	require.Error(t, err)
	tk2.MustExec("select * from tk where c1 = 1 for update nowait")
	tk2.MustExec("commit")
	tk.MustQuery("select * from tk where c1 >= 2 for update").Check(testkit.Rows("2 2", "3 4", "4 4", "5 5"))
	tk.MustExec("commit")
	tk.MustExec("begin pessimistic")
	tk.MustExec("update tk set c2 = c2 + 10 where c1 > 3")
	tk3.MustExec("begin pessimistic")
	_, err = tk3.Exec("select * from tk where c1 = 5 for update nowait")
	require.Error(t, err)
	tk3.MustExec("select * from tk where c1 = 1 for update nowait")
	tk.MustExec("commit")
	tk3.MustQuery("select * from tk where c1 > 3 for update nowait").Check(testkit.Rows("4 14", "5 15"))
	tk3.MustExec("commit")

	// delete
	tk3.MustExec("begin pessimistic")
	tk3.MustExec("delete from tk where c1 <= 2")
	tk.MustExec("begin pessimistic")
	_, err = tk.Exec("select * from tk where c1 = 1 for update nowait")
	require.Error(t, err)
	tk3.MustExec("commit")
	tk.MustQuery("select * from tk where c1 > 1 for update nowait").Check(testkit.Rows("3 4", "4 14", "5 15"))
	tk.MustExec("update tk set c2 = c2 + 1 where c1 = 5")
	tk2.MustExec("begin pessimistic")
	_, err = tk2.Exec("select * from tk where c1 = 5 for update nowait")
	require.Error(t, err)
	tk.MustExec("commit")
	tk2.MustQuery("select * from tk where c1 = 5 for update nowait").Check(testkit.Rows("5 16"))
	tk2.MustExec("update tk set c2 = c2 + 1 where c1 = 5")
	tk2.MustQuery("select * from tk where c1 = 5 for update nowait").Check(testkit.Rows("5 17"))
	tk2.MustExec("commit")
}

func TestAsyncRollBackNoWait(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk3.MustExec("use test")

	tk.MustExec("drop table if exists tk")
	tk.MustExec("create table tk (c1 int primary key, c2 int)")
	tk.MustExec("insert into tk values(1,1),(2,2),(3,3),(4,4),(5,17)")

	tk.MustExec("set @@autocommit = 0")
	tk2.MustExec("set @@autocommit = 0")
	tk3.MustExec("set @@autocommit = 0")

	// test get ts failed for handlePessimisticLockError when using nowait
	// even though async rollback for pessimistic lock may rollback later locked key if get ts failed from pd
	// the txn correctness should be ensured
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/ExecStmtGetTsError", "return"))
	require.NoError(t, failpoint.Enable("tikvclient/beforeAsyncPessimisticRollback", "sleep(100)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/ExecStmtGetTsError"))
		require.NoError(t, failpoint.Disable("tikvclient/beforeAsyncPessimisticRollback"))
	}()
	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from tk where c1 > 0 for update nowait")
	tk2.MustExec("begin pessimistic")
	// The lock rollback of this statement is delayed by failpoint beforeAsyncPessimisticRollback.
	_, err := tk2.Exec("select * from tk where c1 > 0 for update nowait")
	require.Error(t, err)
	tk.MustExec("commit")
	// This statement success for now, but its lock will be rollbacked later by the
	// lingering rollback request, as forUpdateTS doesn't change.
	tk2.MustQuery("select * from tk where c1 > 0 for update nowait").Check(testkit.Rows("1 1", "2 2", "3 3", "4 4", "5 17"))
	tk2.MustQuery("select * from tk where c1 = 5 for update nowait").Check(testkit.Rows("5 17"))
	tk3.MustExec("begin pessimistic")

	// TODO: @coocood skip the following test in https://github.com/pingcap/tidb/pull/13553/
	// Remove this code block and figure out why it's skipped.
	// ----------------------
	tk2.MustExec("rollback")
	tk3.MustExec("rollback")
	// ----------------------

	t.Skip("tk3 is blocking because tk2 didn't rollback itself")
	// tk3 succ because tk2 rollback itself.
	tk3.MustExec("update tk set c2 = 1 where c1 = 5")
	// This will not take effect because the lock of tk2 was gone.
	tk2.MustExec("update tk set c2 = c2 + 100 where c1 > 0")
	_, err = tk2.Exec("commit")
	require.Error(t, err) // txn abort because pessimistic lock not found
	tk3.MustExec("commit")
	tk3.MustExec("begin pessimistic")
	tk3.MustQuery("select * from tk where c1 = 5 for update nowait").Check(testkit.Rows("5 1"))
	tk3.MustQuery("select * from tk where c1 = 4 for update nowait").Check(testkit.Rows("4 4"))
	tk3.MustQuery("select * from tk where c1 = 3 for update nowait").Check(testkit.Rows("3 3"))
	tk3.MustExec("commit")
}

func TestWaitLockKill(t *testing.T) {
	// Test kill command works on waiting pessimistic lock.
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("drop table if exists test_kill")
	tk.MustExec("create table test_kill (id int primary key, c int)")
	tk.MustExec("insert test_kill values (1, 1)")
	tk.MustExec("begin pessimistic")
	tk2.MustExec("set innodb_lock_wait_timeout = 50")
	tk2.MustExec("begin pessimistic")
	tk.MustQuery("select * from test_kill where id = 1 for update")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		time.Sleep(500 * time.Millisecond)
		sessVars := tk2.Session().GetSessionVars()
		succ := atomic.CompareAndSwapUint32(&sessVars.Killed, 0, 1)
		require.True(t, succ)
		wg.Wait()
	}()
	_, err := tk2.Exec("update test_kill set c = c + 1 where id = 1")
	wg.Done()
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, storeerr.ErrQueryInterrupted))
	tk.MustExec("rollback")
}

func TestKillStopTTLManager(t *testing.T) {
	// Test killing an idle pessimistic session stop its ttlManager.
	defer setLockTTL(300).restore()
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("drop table if exists test_kill")
	tk.MustExec("create table test_kill (id int primary key, c int)")
	tk.MustExec("insert test_kill values (1, 1)")
	tk.MustExec("begin pessimistic")
	tk2.MustExec("begin pessimistic")
	tk.MustQuery("select * from test_kill where id = 1 for update")
	sessVars := tk.Session().GetSessionVars()
	succ := atomic.CompareAndSwapUint32(&sessVars.Killed, 0, 1)
	require.True(t, succ)

	// This query should success rather than returning a ResolveLock error.
	tk2.MustExec("update test_kill set c = c + 1 where id = 1")
	succ = atomic.CompareAndSwapUint32(&sessVars.Killed, 1, 0)
	require.True(t, succ)
	tk.MustExec("rollback")
	tk2.MustExec("rollback")
}

func TestConcurrentInsert(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("drop table if exists tk")
	tk.MustExec("create table tk (c1 int primary key, c2 int)")
	tk.MustExec("insert tk values (1, 1)")
	tk.MustExec("create table tk1 (c1 int, c2 int)")

	tk1.MustExec("begin pessimistic")
	forUpdateTsA := tk1.Session().GetSessionVars().TxnCtx.GetForUpdateTS()
	tk1.MustQuery("select * from tk where c1 = 1 for update")
	forUpdateTsB := tk1.Session().GetSessionVars().TxnCtx.GetForUpdateTS()
	require.Equal(t, forUpdateTsB, forUpdateTsA)
	tk1.MustQuery("select * from tk where c1 > 0 for update")
	forUpdateTsC := tk1.Session().GetSessionVars().TxnCtx.GetForUpdateTS()
	require.Greater(t, forUpdateTsC, forUpdateTsB)

	tk2.MustExec("insert tk values (2, 2)")
	tk1.MustQuery("select * from tk for update").Check(testkit.Rows("1 1", "2 2"))
	tk2.MustExec("insert tk values (3, 3)")
	tk1.MustExec("update tk set c2 = c2 + 1")
	require.Equal(t, uint64(3), tk1.Session().AffectedRows())
	tk2.MustExec("insert tk values (4, 4)")
	tk1.MustExec("delete from tk")
	require.Equal(t, uint64(4), tk1.Session().AffectedRows())
	tk2.MustExec("insert tk values (5, 5)")
	tk1.MustExec("insert into tk1 select * from tk")
	require.Equal(t, uint64(1), tk1.Session().AffectedRows())
	tk2.MustExec("insert tk values (6, 6)")
	tk1.MustExec("replace into tk1 select * from tk")
	require.Equal(t, uint64(2), tk1.Session().AffectedRows())
	tk2.MustExec("insert tk values (7, 7)")
	// This test is used to test when the selectPlan is a PointGetPlan, and we didn't update its forUpdateTS.
	tk1.MustExec("insert into tk1 select * from tk where c1 = 7")
	require.Equal(t, uint64(1), tk1.Session().AffectedRows())
	tk1.MustExec("commit")
}

func TestInnodbLockWaitTimeout(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tk")
	tk.MustExec("create table tk (c1 int primary key, c2 int)")
	tk.MustExec("insert into tk values(1,1),(2,2),(3,3),(4,4),(5,5)")
	// tk set global
	tk.MustExec("set global innodb_lock_wait_timeout = 3")
	tk.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 50"))

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 3"))
	tk2.MustExec("set innodb_lock_wait_timeout = 2")
	tk2.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 2"))
	// to check whether it will set to innodb_lock_wait_timeout to max value
	tk2.MustExec("set innodb_lock_wait_timeout = 3602")
	tk2.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 3600"))
	tk2.MustExec("set innodb_lock_wait_timeout = 2")

	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("use test")
	tk3.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 3"))
	tk3.MustExec("set innodb_lock_wait_timeout = 1")
	tk3.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 1"))

	tk2.MustExec("set @@autocommit = 0")
	tk3.MustExec("set @@autocommit = 0")

	tk4 := testkit.NewTestKit(t, store)
	tk4.MustExec("use test")
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
		tk5 := testkit.NewTestKit(t, store)
		tk5.MustExec("use test")
		tk5.MustExec("set innodb_lock_wait_timeout = 2")
		tk5.MustExec("begin pessimistic")
		_, err := tk5.Exec("update tk set c2 = c2 - 1 where c1 = 1")
		timeoutErrCh <- err
		tk5.MustExec("rollback")
	}()

	timeoutErr := <-timeoutErrCh
	require.Error(t, timeoutErr)
	require.Equal(t, storeerr.ErrLockWaitTimeout.Error(), timeoutErr.Error())
	timeoutErr = <-timeoutErrCh
	require.Error(t, timeoutErr)
	require.Equal(t, storeerr.ErrLockWaitTimeout.Error(), timeoutErr.Error())

	// tk4 lock c1 = 2
	tk4.MustExec("begin pessimistic")
	tk4.MustExec("update tk set c2 = c2 + 1 where c1 = 2") // lock succ c1 = 2 by update

	tk2.MustExec("set innodb_lock_wait_timeout = 1")
	tk2.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 1"))

	start := time.Now()
	_, err := tk2.Exec("delete from tk where c1 = 2")
	require.GreaterOrEqual(t, time.Since(start), 1000*time.Millisecond)
	require.Less(t, time.Since(start), 3000*time.Millisecond) // unit test diff should not be too big
	require.Equal(t, storeerr.ErrLockWaitTimeout.Error(), err.Error())

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
	require.GreaterOrEqual(t, time.Since(start), 1000*time.Millisecond)
	require.Less(t, time.Since(start), 3000*time.Millisecond) // unit test diff should not be too big
	require.Equal(t, storeerr.ErrLockWaitTimeout.Error(), err.Error())

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

func TestPushConditionCheckForPessimisticTxn(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	defer tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (i int key)")
	tk.MustExec("insert into t values (1)")

	tk.MustExec("set tidb_txn_mode = 'pessimistic'")
	tk.MustExec("begin")
	tk1.MustExec("delete from t where i = 1")
	tk.MustExec("insert into t values (1) on duplicate key update i = values(i)")
	tk.MustExec("commit")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
}

func TestInnodbLockWaitTimeoutWaitStart(t *testing.T) {
	// prepare work
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	defer tk.MustExec("drop table if exists tk")
	tk.MustExec("drop table if exists tk")
	tk.MustExec("create table tk (c1 int primary key, c2 int)")
	tk.MustExec("insert into tk values(1,1),(2,2),(3,3),(4,4),(5,5)")
	tk.MustExec("set global innodb_lock_wait_timeout = 1")

	// raise pessimistic transaction in tk2 and trigger failpoint returning ErrWriteConflict
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("use test")

	tk2.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 1"))

	// tk3 gets the pessimistic lock
	tk3.MustExec("begin pessimistic")
	tk3.MustQuery("select * from tk where c1 = 1 for update")

	tk2.MustExec("begin pessimistic")
	done := make(chan error)
	require.NoError(t, failpoint.Enable("tikvclient/PessimisticLockErrWriteConflict", "return"))
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
	require.NoError(t, failpoint.Disable("tikvclient/PessimisticLockErrWriteConflict"))
	waitErr := <-done
	require.Error(t, waitErr)
	require.Equal(t, storeerr.ErrLockWaitTimeout.Error(), waitErr.Error())
	require.GreaterOrEqual(t, duration, 1000*time.Millisecond)
	require.LessOrEqual(t, duration, 3000*time.Millisecond)
	tk2.MustExec("rollback")
	tk3.MustExec("commit")
}

func TestBatchPointGetWriteConflict(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (i int primary key, c int);")
	tk.MustExec("insert t values (1, 1), (2, 2), (3, 3)")
	tk1.MustExec("begin pessimistic")
	tk1.MustQuery("select * from t where i = 1").Check(testkit.Rows("1 1"))
	tk.MustExec("update t set c = c + 1")
	tk1.MustQuery("select * from t where i in (1, 3) for update").Check(testkit.Rows("1 2", "3 4"))
	tk1.MustExec("commit")
}

func TestPessimisticSerializable(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	tk.MustExec("set tidb_txn_mode = 'pessimistic'")
	tk1.MustExec("set tidb_txn_mode = 'pessimistic'")

	tk.MustExec("drop table if exists test;")
	tk.MustExec("create table test (id int not null primary key, value int);")
	tk.MustExec("insert into test (id, value) values (1, 10);")
	tk.MustExec("insert into test (id, value) values (2, 20);")

	tk.MustExec("set tidb_skip_isolation_level_check = 1")
	tk1.MustExec("set tidb_skip_isolation_level_check = 1")
	tk.MustExec("set tx_isolation = 'SERIALIZABLE'")
	tk1.MustExec("set tx_isolation = 'SERIALIZABLE'")

	// Predicate-Many-Preceders (PMP)
	tk.MustExec("begin")
	tk1.MustExec("begin")
	tk.MustQuery("select * from test where value = 30;").Check(testkit.Rows())
	tk1.MustExec("insert into test (id, value) values(3, 30);")
	tk1.MustExec("commit")
	tk.MustQuery("select * from test where mod(value, 3) = 0;").Check(testkit.Rows())
	tk.MustExec("commit")

	tk.MustExec("truncate table test;")
	tk.MustExec("insert into test (id, value) values (1, 10);")
	tk.MustExec("insert into test (id, value) values (2, 20);")

	tk.MustExec("begin;")
	tk1.MustExec("begin;")
	tk.MustExec("update test set value = value + 10;")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		_, _ = tk1.Exec("delete from test where value = 20;")
		wg.Done()
	}()
	tk.MustExec("commit;")
	wg.Wait()
	tk1.MustExec("rollback;")

	// Lost Update (P4)
	tk.MustExec("truncate table test;")
	tk.MustExec("insert into test (id, value) values (1, 10);")
	tk.MustExec("insert into test (id, value) values (2, 20);")

	tk.MustExec("begin;")
	tk1.MustExec("begin;")
	tk.MustQuery("select * from test where id = 1;").Check(testkit.Rows("1 10"))
	tk1.MustQuery("select * from test where id = 1;").Check(testkit.Rows("1 10"))
	tk.MustExec("update test set value = 11 where id = 1;")

	wg.Add(1)
	go func() {
		_, _ = tk1.Exec("update test set value = 11 where id = 1;")
		wg.Done()
	}()
	tk.MustExec("commit;")
	wg.Wait()
	tk1.MustExec("rollback;")

	// Read Skew (G-single)
	tk.MustExec("truncate table test;")
	tk.MustExec("insert into test (id, value) values (1, 10);")
	tk.MustExec("insert into test (id, value) values (2, 20);")

	tk.MustExec("begin;")
	tk1.MustExec("begin;")
	tk.MustQuery("select * from test where id = 1;").Check(testkit.Rows("1 10"))
	tk1.MustQuery("select * from test where id = 1;").Check(testkit.Rows("1 10"))
	tk1.MustQuery("select * from test where id = 2;").Check(testkit.Rows("2 20"))
	tk1.MustExec("update test set value = 12 where id = 1;")
	tk1.MustExec("update test set value = 18 where id = 1;")
	tk1.MustExec("commit;")
	tk.MustQuery("select * from test where id = 2;").Check(testkit.Rows("2 20"))
	tk.MustExec("commit;")

	tk.MustExec("truncate table test;")
	tk.MustExec("insert into test (id, value) values (1, 10);")
	tk.MustExec("insert into test (id, value) values (2, 20);")

	tk.MustExec("begin;")
	tk1.MustExec("begin;")
	tk.MustQuery("select * from test where mod(value, 5) = 0;").Check(testkit.Rows("1 10", "2 20"))
	tk1.MustExec("update test set value = 12 where value = 10;")
	tk1.MustExec("commit;")
	tk.MustQuery("select * from test where mod(value, 3) = 0;").Check(testkit.Rows())
	tk.MustExec("commit;")

	tk.MustExec("truncate table test;")
	tk.MustExec("insert into test (id, value) values (1, 10);")
	tk.MustExec("insert into test (id, value) values (2, 20);")

	tk.MustExec("begin;")
	tk1.MustExec("begin;")
	tk.MustQuery("select * from test where id = 1;").Check(testkit.Rows("1 10"))
	tk1.MustQuery("select * from test;").Check(testkit.Rows("1 10", "2 20"))
	tk1.MustExec("update test set value = 12 where id = 1;")
	tk1.MustExec("update test set value = 18 where id = 1;")
	tk1.MustExec("commit;")
	_, _ = tk.Exec("delete from test where value = 20;")
	tk.MustExec("rollback;")

	// Write Skew (G2-item)
	tk.MustExec("truncate table test;")
	tk.MustExec("insert into test (id, value) values (1, 10);")
	tk.MustExec("insert into test (id, value) values (2, 20);")

	tk.MustExec("begin;")
	tk1.MustExec("begin;")
	tk.MustQuery("select * from test where id in (1,2);").Check(testkit.Rows("1 10", "2 20"))
	tk1.MustQuery("select * from test where id in (1,2);").Check(testkit.Rows("1 10", "2 20"))
	tk1.MustExec("update test set value = 11 where id = 1;")
	tk1.MustExec("update test set value = 21 where id = 2;")
	tk.MustExec("commit;")
	tk1.MustExec("commit;")
	tk.MustQuery("select * from test;").Check(testkit.Rows("1 11", "2 21"))

	// Anti-Dependency Cycles (G2)
	tk.MustExec("truncate table test;")
	tk.MustExec("insert into test (id, value) values (1, 10);")
	tk.MustExec("insert into test (id, value) values (2, 20);")

	tk.MustExec("begin;")
	tk1.MustExec("begin;")
	tk.MustQuery("select * from test where mod(value, 3) = 0;").Check(testkit.Rows())
	tk1.MustQuery("select * from test where mod(value, 5) = 0;").Check(testkit.Rows("1 10", "2 20"))
	tk.MustExec("insert into test (id, value) values(3, 30);")
	tk1.MustExec("insert into test (id, value) values(4, 60);")
	tk.MustExec("commit;")
	tk1.MustExec("commit;")
	tk.MustQuery("select * from test where mod(value, 3) = 0;").Check(testkit.Rows("3 30", "4 60"))
}

func TestPessimisticReadCommitted(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	tk.MustExec("set tidb_txn_mode = 'pessimistic'")
	tk1.MustExec("set tidb_txn_mode = 'pessimistic'")

	// test SI
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(i int key);")
	tk.MustExec("insert into t values (1);")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))

	tk.MustExec("begin;")
	tk1.MustExec("begin;")
	tk.MustExec("update t set i = -i;")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		tk1.MustExec("update t set i = -i;")
		wg.Done()
	}()
	tk.MustExec("commit;")
	wg.Wait()

	tk1.MustExec("commit;")

	// test RC
	tk.MustExec("set tx_isolation = 'READ-COMMITTED'")
	tk1.MustExec("set tx_isolation = 'READ-COMMITTED'")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(i int key);")
	tk.MustExec("insert into t values (1);")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))

	tk.MustExec("begin;")
	tk1.MustExec("begin;")
	tk.MustExec("update t set i = -i;")

	wg.Add(1)
	go func() {
		tk1.MustExec("update t set i = -i;")
		wg.Done()
	}()
	tk.MustExec("commit;")
	wg.Wait()

	tk1.MustExec("commit;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(i int key, j int unique key, k int, l int, m int, key (k));")
	tk.MustExec("insert into t values (1, 1, 1, 1, 1);")

	// Set it back to RR to test set transaction statement.
	tk.MustExec("set tx_isolation = 'REPEATABLE-READ'")
	tk1.MustExec("set tx_isolation = 'REPEATABLE-READ'")

	// test one shot and some reads.
	tk.MustExec("set transaction isolation level read committed")
	tk.MustExec("begin;")

	// test table reader
	tk.MustQuery("select l from t where l = 1").Check(testkit.Rows("1"))
	tk1.MustExec("update t set l = l + 1 where l = 1;")
	tk.MustQuery("select l from t where l = 2").Check(testkit.Rows("2"))
	tk1.MustExec("update t set l = l + 1 where l = 2;")
	tk.MustQuery("select l from t where l = 3").Check(testkit.Rows("3"))

	// test index reader
	tk.MustQuery("select k from t where k = 1").Check(testkit.Rows("1"))
	tk1.MustExec("update t set k = k + 1 where k = 1;")
	tk.MustQuery("select k from t where k = 2").Check(testkit.Rows("2"))
	tk1.MustExec("update t set k = k + 1 where k = 2;")
	tk.MustQuery("select k from t where k = 3").Check(testkit.Rows("3"))

	// test double read
	tk.MustQuery("select m from t where k = 3").Check(testkit.Rows("1"))
	tk1.MustExec("update t set m = m + 1 where k = 3;")
	tk.MustQuery("select m from t where k = 3").Check(testkit.Rows("2"))
	tk1.MustExec("update t set m = m + 1 where k = 3;")
	tk.MustQuery("select m from t where k = 3").Check(testkit.Rows("3"))

	// test point get plan
	tk.MustQuery("select m from t where i = 1").Check(testkit.Rows("3"))
	tk1.MustExec("update t set m = m + 1 where i = 1;")
	tk.MustQuery("select m from t where i = 1").Check(testkit.Rows("4"))
	tk1.MustExec("update t set m = m + 1 where j = 1;")
	tk.MustQuery("select m from t where j = 1").Check(testkit.Rows("5"))

	// test batch point get plan
	tk1.MustExec("insert into t values (2, 2, 2, 2, 2);")
	tk.MustQuery("select m from t where i in (1, 2)").Check(testkit.Rows("5", "2"))
	tk1.MustExec("update t set m = m + 1 where i in (1, 2);")
	tk.MustQuery("select m from t where i in (1, 2)").Check(testkit.Rows("6", "3"))
	tk1.MustExec("update t set m = m + 1 where j in (1, 2);")
	tk.MustQuery("select m from t where j in (1, 2)").Check(testkit.Rows("7", "4"))

	tk.MustExec("commit;")

	// test for NewTxn()
	tk.MustExec("set tx_isolation = 'READ-COMMITTED'")
	tk.MustExec("begin optimistic;")
	tk.MustQuery("select m from t where j in (1, 2)").Check(testkit.Rows("7", "4"))
	tk.MustExec("begin pessimistic;")
	tk.MustQuery("select m from t where j in (1, 2)").Check(testkit.Rows("7", "4"))
	tk.MustExec("commit;")
}

func TestPessimisticLockNonExistsKey(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (k int primary key, c int)")
	tk.MustExec("insert t values (1, 1), (3, 3), (5, 5)")

	// verify that select with project and filter on a non exists key still locks the key.
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert t values (8, 8)") // Make the transaction dirty.
	tk.MustQuery("select c + 1 from t where k = 2 and c = 2 for update").Check(testkit.Rows())
	explainStr := tk.MustQuery("explain select c + 1 from t where k = 2 and c = 2 for update").Rows()[0][0].(string)
	require.False(t, strings.Contains(explainStr, "UnionScan"))
	tk.MustQuery("select * from t where k in (4, 5, 7) for update").Check(testkit.Rows("5 5"))

	tk1.MustExec("begin pessimistic")
	err := tk1.ExecToErr("select * from t where k = 2 for update nowait")
	require.True(t, storeerr.ErrLockAcquireFailAndNoWaitSet.Equal(err), fmt.Sprintf("err %v", err))
	err = tk1.ExecToErr("select * from t where k = 4 for update nowait")
	require.True(t, storeerr.ErrLockAcquireFailAndNoWaitSet.Equal(err), fmt.Sprintf("err %v", err))
	err = tk1.ExecToErr("select * from t where k = 7 for update nowait")
	require.True(t, storeerr.ErrLockAcquireFailAndNoWaitSet.Equal(err), fmt.Sprintf("err %v", err))
	tk.MustExec("rollback")
	tk1.MustExec("rollback")

	// verify update and delete non exists keys still locks the key.
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert t values (8, 8)") // Make the transaction dirty.
	tk.MustExec("update t set c = c + 1 where k in (2, 3, 4) and c > 0")
	tk.MustExec("delete from t where k in (5, 6, 7) and c > 0")

	tk1.MustExec("begin pessimistic")
	err = tk1.ExecToErr("select * from t where k = 2 for update nowait")
	require.True(t, storeerr.ErrLockAcquireFailAndNoWaitSet.Equal(err), fmt.Sprintf("err %v", err))
	err = tk1.ExecToErr("select * from t where k = 6 for update nowait")
	require.True(t, storeerr.ErrLockAcquireFailAndNoWaitSet.Equal(err), fmt.Sprintf("err %v", err))
	tk.MustExec("rollback")
	tk1.MustExec("rollback")
}

func TestPessimisticCommitReadLock(t *testing.T) {
	// tk1 lock wait timeout is 2s
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	tk.MustExec("set tidb_txn_mode = 'pessimistic'")
	tk1.MustExec("set tidb_txn_mode = 'pessimistic'")
	tk1.MustExec("set innodb_lock_wait_timeout = 2")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(i int key primary key, j int);")
	tk.MustExec("insert into t values (1, 2);")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))

	// tk lock one row
	tk.MustExec("begin;")
	tk.MustQuery("select * from t for update").Check(testkit.Rows("1 2"))
	tk1.MustExec("begin;")
	done := make(chan error)
	go func() {
		var err error
		defer func() {
			done <- err
		}()
		// let txn not found could be checked by lock wait timeout utility
		err = failpoint.Enable("tikvclient/txnNotFoundRetTTL", "return")
		if err != nil {
			return
		}
		_, err = tk1.Exec("update t set j = j + 1 where i = 1")
		if err != nil {
			return
		}
		err = failpoint.Disable("tikvclient/txnNotFoundRetTTL")
		if err != nil {
			return
		}
		_, err = tk1.Exec("commit")
	}()
	// let the lock be hold for a while
	time.Sleep(time.Millisecond * 50)
	tk.MustExec("commit")
	waitErr := <-done
	require.NoError(t, waitErr)
}

func TestPessimisticLockReadValue(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(i int, j int, k int, unique key uk(j));")
	tk.MustExec("insert into t values (1, 1, 1);")

	// tk1 will left op_lock record
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("begin optimistic")
	tk1.MustQuery("select * from t where j = 1 for update").Check(testkit.Rows("1 1 1"))
	tk1.MustQuery("select * from t where j = 1 for update").Check(testkit.Rows("1 1 1"))
	tk1.MustExec("commit")

	// tk2 pessimistic lock read value
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("begin pessimistic")
	tk2.MustQuery("select * from t where j = 1 for update").Check(testkit.Rows("1 1 1"))
	tk2.MustQuery("select * from t where j = 1 for update").Check(testkit.Rows("1 1 1"))
	tk2.MustExec("commit")
}

func TestRCWaitTSOTwice(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (i int key)")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("set tidb_txn_mode = 'pessimistic'")
	tk.MustExec("set tx_isolation = 'read-committed'")
	tk.MustExec("set autocommit = 0")
	tk.MustQuery("select * from t where i = 1").Check(testkit.Rows("1"))
	tk.MustExec("rollback")
}

func TestNonAutoCommitWithPessimisticMode(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int primary key, c2 int)")
	tk.MustExec("insert into t1 values (1, 1)")
	tk.MustExec("set tidb_txn_mode = 'pessimistic'")
	tk.MustExec("set autocommit = 0")
	tk.MustQuery("select * from t1 where c2 = 1 for update").Check(testkit.Rows("1 1"))
	tk2.MustExec("insert into t1 values(2, 1)")
	tk.MustQuery("select * from t1 where c2 = 1 for update").Check(testkit.Rows("1 1", "2 1"))
	tk.MustExec("commit")
	tk2.MustExec("insert into t1 values(3, 1)")
	tk.MustExec("set tx_isolation = 'read-committed'")
	tk.MustQuery("select * from t1 where c2 = 1 for update").Check(testkit.Rows("1 1", "2 1", "3 1"))
	tk2.MustExec("insert into t1 values(4, 1)")
	tk.MustQuery("select * from t1 where c2 = 1 for update").Check(testkit.Rows("1 1", "2 1", "3 1", "4 1"))
	tk.MustExec("commit")
}

func TestBatchPointGetLockIndex(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int primary key, c2 int, c3 int, unique key uk(c2))")
	tk.MustExec("insert into t1 values (1, 1, 1)")
	tk.MustExec("insert into t1 values (5, 5, 5)")
	tk.MustExec("insert into t1 values (10, 10, 10)")
	tk.MustExec("begin pessimistic")
	// the handle does not exist and the index key should be locked as point get executor did
	tk.MustQuery("select * from t1 where c2 in (2, 3) for update").Check(testkit.Rows())
	tk2.MustExec("set innodb_lock_wait_timeout = 1")
	tk2.MustExec("begin pessimistic")
	err := tk2.ExecToErr("insert into t1 values(2, 2, 2)")
	require.Error(t, err)
	require.True(t, storeerr.ErrLockWaitTimeout.Equal(err))
	err = tk2.ExecToErr("select * from t1 where c2 = 3 for update nowait")
	require.Error(t, err)
	require.True(t, storeerr.ErrLockAcquireFailAndNoWaitSet.Equal(err))
	tk.MustExec("rollback")
	tk2.MustExec("rollback")
}

func TestLockGotKeysInRC(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("set tx_isolation = 'READ-COMMITTED'")
	tk2.MustExec("set tx_isolation = 'READ-COMMITTED'")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int primary key, c2 int, c3 int, unique key uk(c2))")
	tk.MustExec("insert into t1 values (1, 1, 1)")
	tk.MustExec("insert into t1 values (5, 5, 5)")
	tk.MustExec("insert into t1 values (10, 10, 10)")
	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from t1 where c1 in (2, 3) for update").Check(testkit.Rows())
	tk.MustQuery("select * from t1 where c2 in (2, 3) for update").Check(testkit.Rows())
	tk.MustQuery("select * from t1 where c1 = 2 for update").Check(testkit.Rows())
	tk.MustQuery("select * from t1 where c2 = 2 for update").Check(testkit.Rows())
	tk2.MustExec("begin pessimistic")
	tk2.MustExec("insert into t1 values(2, 2, 2)")
	tk2.MustExec("select * from t1 where c1 = 3 for update nowait")
	tk2.MustExec("select * from t1 where c2 = 3 for update nowait")
	tk.MustExec("rollback")
	tk2.MustExec("rollback")
}

func TestBatchPointGetAlreadyLocked(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int, c2 int, c3 int, primary key(c1, c2))")
	tk.MustExec("insert t values (1, 1, 1), (2, 2, 2)")
	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from t where c1 > 1 for update").Check(testkit.Rows("2 2 2"))
	tk.MustQuery("select * from t where (c1, c2) in ((2,2)) for update").Check(testkit.Rows("2 2 2"))
	tk.MustExec("commit")
}

func TestRollbackWakeupBlockedTxn(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int primary key, c2 int, c3 int, unique key uk(c2))")
	tk.MustExec("insert into t1 values (1, 1, 1)")
	tk.MustExec("insert into t1 values (5, 5, 5)")
	tk.MustExec("insert into t1 values (10, 10, 10)")

	require.NoError(t, failpoint.Enable("tikvclient/txnExpireRetTTL", "return"))
	require.NoError(t, failpoint.Enable("tikvclient/getTxnStatusDelay", "return"))
	tk.MustExec("begin pessimistic")
	tk2.MustExec("set innodb_lock_wait_timeout = 1")
	tk2.MustExec("begin pessimistic")
	tk.MustExec("update t1 set c3 = c3 + 1")
	errCh := make(chan error)
	go func() {
		var err error
		defer func() {
			errCh <- err
		}()
		_, err = tk2.Exec("update t1 set c3 = 100 where c1 = 1")
		if err != nil {
			return
		}
	}()
	time.Sleep(time.Millisecond * 30)
	tk.MustExec("rollback")
	err := <-errCh
	require.NoError(t, err)
	tk2.MustExec("rollback")
	require.NoError(t, failpoint.Disable("tikvclient/txnExpireRetTTL"))
	require.NoError(t, failpoint.Disable("tikvclient/getTxnStatusDelay"))
}

func TestRCSubQuery(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table `t` ( `c1` int(11) not null, `c2` int(11) default null, primary key (`c1`) )")
	tk.MustExec("insert into t values(1, 3)")
	tk.MustExec("create table `t1` ( `c1` int(11) not null, `c2` int(11) default null, primary key (`c1`) )")
	tk.MustExec("insert into t1 values(1, 3)")
	tk.MustExec("set transaction isolation level read committed")
	tk.MustExec("begin pessimistic")

	tk2.MustExec("update t1 set c2 = c2 + 1")

	tk.MustQuery("select * from t1 where c1 = (select 1) and 1=1;").Check(testkit.Rows("1 4"))
	tk.MustQuery("select * from t1 where c1 = (select c1 from t where c1 = 1) and 1=1;").Check(testkit.Rows("1 4"))
	tk.MustExec("rollback")
}

func TestRCIndexMerge(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t (id int primary key, v int, a int not null, b int not null,
		index ia (a), index ib (b))`)
	tk.MustExec("insert into t values (1, 10, 1, 1)")

	tk.MustExec("set transaction isolation level read committed")
	tk.MustExec("begin pessimistic")
	tk.MustQuery("select /*+ USE_INDEX_MERGE(t, ia, ib) */ * from t where a > 0 or b > 0").Check(
		testkit.Rows("1 10 1 1"),
	)
	tk.MustQuery("select /*+ NO_INDEX_MERGE() */ * from t where a > 0 or b > 0").Check(
		testkit.Rows("1 10 1 1"),
	)

	tk2.MustExec("update t set v = 11 where id = 1")

	// Make sure index merge plan is used.
	plan := tk.MustQuery("explain select /*+ USE_INDEX_MERGE(t, ia, ib) */ * from t where a > 0 or b > 0").Rows()[0][0].(string)
	require.True(t, strings.Contains(plan, "IndexMerge_"))
	tk.MustQuery("select /*+ USE_INDEX_MERGE(t, ia, ib) */ * from t where a > 0 or b > 0").Check(
		testkit.Rows("1 11 1 1"),
	)
	tk.MustQuery("select /*+ NO_INDEX_MERGE() */ * from t where a > 0 or b > 0").Check(
		testkit.Rows("1 11 1 1"),
	)
}

func TestGenerateColPointGet(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_row_format_version = %d", variable.DefTiDBRowFormatV2))
	}()
	tests2 := []int{variable.DefTiDBRowFormatV1, variable.DefTiDBRowFormatV2}
	for _, rowFormat := range tests2 {
		tk.MustExec(fmt.Sprintf("set global tidb_row_format_version = %d", rowFormat))
		tk.MustExec("drop table if exists tu")
		tk.MustExec("CREATE TABLE `tu`(`x` int, `y` int, `z` int GENERATED ALWAYS AS (x + y) VIRTUAL, PRIMARY KEY (`x`), UNIQUE KEY `idz` (`z`))")
		tk.MustExec("insert into tu(x, y) values(1, 2);")

		// test point get lock
		tk.MustExec("begin pessimistic")
		tk.MustQuery("select * from tu where z = 3 for update").Check(testkit.Rows("1 2 3"))
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")
		tk2.MustExec("begin pessimistic")
		err := tk2.ExecToErr("select * from tu where z = 3 for update nowait")
		require.Error(t, err)
		require.True(t, terror.ErrorEqual(err, storeerr.ErrLockAcquireFailAndNoWaitSet))
		tk.MustExec("begin pessimistic")
		tk.MustExec("insert into tu(x, y) values(2, 2);")
		err = tk2.ExecToErr("select * from tu where z = 4 for update nowait")
		require.Error(t, err)
		require.True(t, terror.ErrorEqual(err, storeerr.ErrLockAcquireFailAndNoWaitSet))

		// test batch point get lock
		tk.MustExec("begin pessimistic")
		tk2.MustExec("begin pessimistic")
		tk.MustQuery("select * from tu where z in (1, 3, 5) for update").Check(testkit.Rows("1 2 3"))
		tk2.MustExec("begin pessimistic")
		err = tk2.ExecToErr("select x from tu where z in (3, 7, 9) for update nowait")
		require.Error(t, err)
		require.True(t, terror.ErrorEqual(err, storeerr.ErrLockAcquireFailAndNoWaitSet))
		tk.MustExec("begin pessimistic")
		tk.MustExec("insert into tu(x, y) values(5, 6);")
		err = tk2.ExecToErr("select * from tu where z = 11 for update nowait")
		require.Error(t, err)
		require.True(t, terror.ErrorEqual(err, storeerr.ErrLockAcquireFailAndNoWaitSet))

		tk.MustExec("commit")
		tk2.MustExec("commit")
	}
}

func TestTxnWithExpiredPessimisticLocks(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int primary key, c2 int, c3 int, unique key uk(c2))")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec("insert into t1 values (1, 1, 1)")
	tk.MustExec("insert into t1 values (5, 5, 5)")

	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from t1 where c1 in(1, 5) for update").Check(testkit.Rows("1 1 1", "5 5 5"))
	atomic.StoreUint32(&tk.Session().GetSessionVars().TxnCtx.LockExpire, 1)
	err := tk.ExecToErr("select * from t1 where c1 in(1, 5)")
	require.True(t, terror.ErrorEqual(err, kv.ErrLockExpire))
	tk.MustExec("commit")

	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from t1 where c1 in(1, 5) for update").Check(testkit.Rows("1 1 1", "5 5 5"))
	atomic.StoreUint32(&tk.Session().GetSessionVars().TxnCtx.LockExpire, 1)
	err = tk.ExecToErr("update t1 set c2 = c2 + 1")
	require.True(t, terror.ErrorEqual(err, kv.ErrLockExpire))
	atomic.StoreUint32(&tk.Session().GetSessionVars().TxnCtx.LockExpire, 0)
	tk.MustExec("update t1 set c2 = c2 + 1")
	tk.MustExec("rollback")
}

func TestKillWaitLockTxn(t *testing.T) {
	// Test kill command works on waiting pessimistic lock.
	defer setLockTTL(300).restore()
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk.MustExec("drop table if exists test_kill")
	tk.MustExec("create table test_kill (id int primary key, c int)")
	tk.MustExec("insert test_kill values (1, 1)")

	tk.MustExec("begin pessimistic")
	tk2.MustExec("begin pessimistic")

	tk.MustQuery("select * from test_kill where id = 1 for update")
	errCh := make(chan error)
	go func() {
		var err error
		defer func() {
			errCh <- err
		}()
		time.Sleep(20 * time.Millisecond)
		_, err = tk2.Exec("update test_kill set c = c + 1 where id = 1")
		if err != nil {
			return
		}
	}()
	time.Sleep(100 * time.Millisecond)
	sessVars := tk.Session().GetSessionVars()
	// lock query in tk is killed, the ttl manager will stop
	succ := atomic.CompareAndSwapUint32(&sessVars.Killed, 0, 1)
	require.True(t, succ)
	err := <-errCh
	require.NoError(t, err)
	_, _ = tk.Exec("rollback")
	// reset kill
	atomic.CompareAndSwapUint32(&sessVars.Killed, 1, 0)
	tk.MustExec("rollback")
	tk2.MustExec("rollback")
}

func TestDupLockInconsistency(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, index b (b))")
	tk.MustExec("insert t (a) values (1), (1)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("update t, (select a from t) s set t.b = s.a")
	tk.MustExec("commit")
	tk.MustExec("admin check table t")
}

func TestUseLockCacheInRCMode(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk3.MustExec("use test")

	tk.MustExec("drop table if exists test_kill")
	tk.MustExec("CREATE TABLE SEQUENCE_VALUE_ITEM(SEQ_NAME varchar(60) NOT NULL, SEQ_ID decimal(18,0) DEFAULT NULL, " +
		"PRIMARY KEY (SEQ_NAME))")
	tk.MustExec("create table t1(c1 int, c2 int, unique key(c1))")
	tk.MustExec(`insert into sequence_value_item values("OSCurrentStep", 0)`)
	tk.MustExec("insert into t1 values(1, 1)")
	tk.MustExec("insert into t1 values(2, 2)")

	// tk2 uses RC isolation level
	tk2.MustExec("set @@tx_isolation='READ-COMMITTED'")
	tk2.MustExec("set autocommit = 0")

	// test point get
	tk2.MustExec("SELECT SEQ_ID FROM SEQUENCE_VALUE_ITEM WHERE SEQ_NAME='OSCurrentStep' FOR UPDATE")
	tk2.MustExec("UPDATE SEQUENCE_VALUE_ITEM SET SEQ_ID=SEQ_ID+100 WHERE SEQ_NAME='OSCurrentStep'")
	tk2.MustExec("rollback")
	tk2.MustQuery("select * from t1 where c1 = 1 for update").Check(testkit.Rows("1 1"))
	tk2.MustExec("update t1 set c2 = c2 + 10 where c1 = 1")
	tk2.MustQuery("select * from t1 where c1 in (1, 2) for update").Check(testkit.Rows("1 11", "2 2"))
	tk2.MustExec("update t1 set c2 = c2 + 10 where c1 in (2)")
	tk2.MustQuery("select * from t1 where c1 in (1, 2) for update").Check(testkit.Rows("1 11", "2 12"))
	tk2.MustExec("commit")

	// tk3 uses begin with RC isolation level
	tk3.MustQuery("select * from SEQUENCE_VALUE_ITEM").Check(testkit.Rows("OSCurrentStep 0"))
	tk3.MustExec("set @@tx_isolation='READ-COMMITTED'")
	tk3.MustExec("begin")
	tk3.MustExec("SELECT SEQ_ID FROM SEQUENCE_VALUE_ITEM WHERE SEQ_NAME='OSCurrentStep' FOR UPDATE")
	tk3.MustExec("UPDATE SEQUENCE_VALUE_ITEM SET SEQ_ID=SEQ_ID+100 WHERE SEQ_NAME='OSCurrentStep'")
	tk3.MustQuery("select * from t1 where c1 = 1 for update").Check(testkit.Rows("1 11"))
	tk3.MustExec("update t1 set c2 = c2 + 10 where c1 = 1")
	tk3.MustQuery("select * from t1 where c1 in (1, 2) for update").Check(testkit.Rows("1 21", "2 12"))
	tk3.MustExec("update t1 set c2 = c2 + 10 where c1 in (2)")
	tk3.MustQuery("select * from t1 where c1 in (1, 2) for update").Check(testkit.Rows("1 21", "2 22"))
	tk3.MustExec("commit")

	// verify
	tk.MustQuery("select * from SEQUENCE_VALUE_ITEM").Check(testkit.Rows("OSCurrentStep 100"))
	tk.MustQuery("select * from SEQUENCE_VALUE_ITEM where SEQ_ID = 100").Check(testkit.Rows("OSCurrentStep 100"))
	tk.MustQuery("select * from t1 where c1 = 2").Check(testkit.Rows("2 22"))
	tk.MustQuery("select * from t1 where c1 in (1, 2, 3)").Check(testkit.Rows("1 21", "2 22"))

	// test batch point get
	tk2.MustExec("set autocommit = 1")
	tk2.MustExec("set autocommit = 0")
	tk2.MustExec("SELECT SEQ_ID FROM SEQUENCE_VALUE_ITEM WHERE SEQ_NAME in ('OSCurrentStep') FOR UPDATE")
	tk2.MustExec("UPDATE SEQUENCE_VALUE_ITEM SET SEQ_ID=SEQ_ID+100 WHERE SEQ_NAME in ('OSCurrentStep')")
	tk2.MustQuery("select * from t1 where c1 in (1, 2, 3, 4, 5) for update").Check(testkit.Rows("1 21", "2 22"))
	tk2.MustExec("update t1 set c2 = c2 + 10 where c1 in (1, 2, 3, 4, 5)")
	tk2.MustQuery("select * from t1 where c1 in (1, 2, 3, 4, 5) for update").Check(testkit.Rows("1 31", "2 32"))
	tk2.MustExec("commit")
	tk2.MustExec("SELECT SEQ_ID FROM SEQUENCE_VALUE_ITEM WHERE SEQ_NAME in ('OSCurrentStep') FOR UPDATE")
	tk2.MustExec("UPDATE SEQUENCE_VALUE_ITEM SET SEQ_ID=SEQ_ID+100 WHERE SEQ_NAME in ('OSCurrentStep')")
	tk2.MustExec("rollback")

	tk.MustQuery("select * from SEQUENCE_VALUE_ITEM").Check(testkit.Rows("OSCurrentStep 200"))
	tk.MustQuery("select * from SEQUENCE_VALUE_ITEM where SEQ_NAME in ('OSCurrentStep')").Check(testkit.Rows("OSCurrentStep 200"))
	tk.MustQuery("select * from t1 where c1 in (1, 2, 3)").Check(testkit.Rows("1 31", "2 32"))
	tk.MustExec("rollback")
	tk2.MustExec("rollback")
	tk3.MustExec("rollback")
}

func TestPointGetWithDeleteInMem(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk.MustExec("drop table if exists uk")
	tk.MustExec("create table uk (c1 int primary key, c2 int, unique key uk(c2))")
	tk.MustExec("insert uk values (1, 77), (2, 88), (3, 99)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("delete from uk where c1 = 1")
	tk.MustQuery("select * from uk where c2 = 77").Check(testkit.Rows())
	tk.MustQuery("select * from uk where c2 in(77, 88, 99)").Check(testkit.Rows("2 88", "3 99"))
	tk.MustQuery("select * from uk").Check(testkit.Rows("2 88", "3 99"))
	tk.MustQuery("select * from uk where c2 = 77 for update").Check(testkit.Rows())
	tk.MustQuery("select * from uk where c2 in(77, 88, 99) for update").Check(testkit.Rows("2 88", "3 99"))
	tk.MustExec("rollback")
	tk2.MustQuery("select * from uk where c1 = 1").Check(testkit.Rows("1 77"))
	tk.MustExec("begin")
	tk.MustExec("update uk set c1 = 10 where c1 = 1")
	tk.MustQuery("select * from uk where c2 = 77").Check(testkit.Rows("10 77"))
	tk.MustQuery("select * from uk where c2 in(77, 88, 99)").Check(testkit.Rows("10 77", "2 88", "3 99"))
	tk.MustExec("commit")
	tk2.MustQuery("select * from uk where c1 = 1").Check(testkit.Rows())
	tk2.MustQuery("select * from uk where c2 = 77").Check(testkit.Rows("10 77"))
	tk2.MustQuery("select * from uk where c1 = 10").Check(testkit.Rows("10 77"))
	tk.MustExec("drop table if exists uk")
}

func TestPessimisticTxnWithDDLAddDropColumn(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int primary key, c2 int)")
	tk.MustExec("insert t1 values (1, 77), (2, 88)")
	tk.MustExec("alter table t1 add index k2(c2)")
	tk.MustExec("alter table t1 drop index k2")

	// tk2 starts a pessimistic transaction and make some changes on table t1.
	// tk executes some ddl statements add/drop column on table t1.
	tk.MustExec("set tidb_enable_amend_pessimistic_txn = 1;")
	tk.MustExec("begin pessimistic")
	tk.MustExec("update t1 set c2 = c1 * 10")
	tk2.MustExec("alter table t1 add column c3 int after c1")
	tk.MustExec("commit")
	tk.MustExec("admin check table t1")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 <nil> 10", "2 <nil> 20"))

	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t1 values(5, 5, 5)")
	tk2.MustExec("alter table t1 drop column c3")
	tk2.MustExec("alter table t1 drop column c2")
	tk.MustExec("commit")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1", "2", "5"))
}

func TestPessimisticTxnWithDDLChangeColumn(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

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
	require.Error(t, tk.ExecToErr("commit"))

	// Change default value is rejected.
	tk2.MustExec("create table ta(a bigint primary key auto_random(3), b varchar(255) default 'old');")
	tk2.MustExec("insert into ta(b) values('a')")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into ta values()")
	tk2.MustExec("alter table ta modify column b varchar(300) default 'new';")
	require.Error(t, tk.ExecToErr("commit"))
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
	require.Error(t, tk.ExecToErr("commit"))
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
	require.Error(t, tk.ExecToErr("commit"))
	tk2.MustQuery("select count(1) from tbl_time where c_time is not null").Check(testkit.Rows("2"))
}

func TestPessimisticUnionForUpdate(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int, v int, k int, primary key (id), key kk(k))")
	tk.MustExec("insert into t select 1, 1, 1")
	tk.MustExec("begin pessimistic")
	tk.MustQuery("(select * from t where id between 0 and 1 for update) union all (select * from t where id between 0 and 1 for update)")
	tk.MustExec("update t set k = 2 where k = 1")
	tk.MustExec("commit")
	tk.MustExec("admin check table t")
}

func TestInsertDupKeyAfterLock(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk2.MustExec("drop table if exists t1")
	tk2.MustExec("create table t1(c1 int primary key, c2 int, c3 int, unique key uk(c2));")
	tk2.MustExec("insert into t1 values(1, 2, 3);")
	tk2.MustExec("insert into t1 values(10, 20, 30);")

	// Test insert after lock.
	tk.MustExec("begin pessimistic")
	err := tk.ExecToErr("update t1 set c2 = 20 where c1 = 1;")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	err = tk.ExecToErr("insert into t1 values(1, 15, 300);")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 30"))

	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from t1 for update")
	err = tk.ExecToErr("insert into t1 values(1, 15, 300);")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 30"))

	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from t1 where c2 = 2 for update")
	err = tk.ExecToErr("insert into t1 values(1, 15, 300);")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 30"))

	// Test insert after insert.
	tk.MustExec("begin pessimistic")
	err = tk.ExecToErr("insert into t1 values(1, 15, 300);")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	tk.MustExec("insert into t1 values(5, 6, 7)")
	err = tk.ExecToErr("insert into t1 values(6, 6, 7);")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "5 6 7", "10 20 30"))

	// Test insert after delete.
	tk.MustExec("begin pessimistic")
	tk.MustExec("delete from t1 where c2 > 2")
	tk.MustExec("insert into t1 values(10, 20, 500);")
	err = tk.ExecToErr("insert into t1 values(20, 20, 30);")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	err = tk.ExecToErr("insert into t1 values(1, 20, 30);")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 500"))

	// Test range.
	tk.MustExec("begin pessimistic")
	err = tk.ExecToErr("update t1 set c2 = 20 where c1 >= 1 and c1 < 5;")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	err = tk.ExecToErr("update t1 set c2 = 20 where c1 >= 1 and c1 < 50;")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	err = tk.ExecToErr("insert into t1 values(1, 15, 300);")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 500"))

	// Test select for update after dml.
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t1 values(5, 6, 7)")
	tk.MustExec("select * from t1 where c1 = 5 for update")
	tk.MustExec("select * from t1 where c1 = 6 for update")
	tk.MustExec("select * from t1 for update")
	err = tk.ExecToErr("insert into t1 values(7, 6, 7)")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	err = tk.ExecToErr("insert into t1 values(5, 8, 6)")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	tk.MustExec("select * from t1 where c1 = 5 for update")
	tk.MustExec("select * from t1 where c2 = 8 for update")
	tk.MustExec("select * from t1 for update")
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "5 6 7", "10 20 500"))

	// Test optimistic for update.
	tk.MustExec("begin optimistic")
	tk.MustQuery("select * from t1 where c1 = 1 for update").Check(testkit.Rows("1 2 3"))
	tk.MustExec("insert into t1 values(10, 10, 10)")
	err = tk.ExecToErr("commit")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
}

func TestInsertDupKeyAfterLockBatchPointGet(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk2.MustExec("drop table if exists t1")
	tk2.MustExec("create table t1(c1 int primary key, c2 int, c3 int, unique key uk(c2));")
	tk2.MustExec("insert into t1 values(1, 2, 3);")
	tk2.MustExec("insert into t1 values(10, 20, 30);")

	// Test insert after lock.
	tk.MustExec("begin pessimistic")
	err := tk.ExecToErr("update t1 set c2 = 20 where c1 in (1);")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	err = tk.ExecToErr("insert into t1 values(1, 15, 300);")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 30"))

	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from t1 for update")
	err = tk.ExecToErr("insert into t1 values(1, 15, 300);")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 30"))

	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from t1 where c2 in (2) for update")
	err = tk.ExecToErr("insert into t1 values(1, 15, 300);")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 30"))

	// Test insert after insert.
	tk.MustExec("begin pessimistic")
	err = tk.ExecToErr("insert into t1 values(1, 15, 300);")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	tk.MustExec("insert into t1 values(5, 6, 7)")
	err = tk.ExecToErr("insert into t1 values(6, 6, 7);")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "5 6 7", "10 20 30"))

	// Test insert after delete.
	tk.MustExec("begin pessimistic")
	tk.MustExec("delete from t1 where c2 > 2")
	tk.MustExec("insert into t1 values(10, 20, 500);")
	err = tk.ExecToErr("insert into t1 values(20, 20, 30);")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	err = tk.ExecToErr("insert into t1 values(1, 20, 30);")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 500"))

	// Test range.
	tk.MustExec("begin pessimistic")
	err = tk.ExecToErr("update t1 set c2 = 20 where c1 >= 1 and c1 < 5;")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	err = tk.ExecToErr("update t1 set c2 = 20 where c1 >= 1 and c1 < 50;")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	err = tk.ExecToErr("insert into t1 values(1, 15, 300);")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 500"))

	// Test select for update after dml.
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t1 values(5, 6, 7)")
	tk.MustExec("select * from t1 where c1 in (5, 6) for update")
	tk.MustExec("select * from t1 where c1 = 6 for update")
	tk.MustExec("select * from t1 for update")
	err = tk.ExecToErr("insert into t1 values(7, 6, 7)")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	err = tk.ExecToErr("insert into t1 values(5, 8, 6)")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
	tk.MustExec("select * from t1 where c2 = 8 for update")
	tk.MustExec("select * from t1 where c1 in (5, 8) for update")
	tk.MustExec("select * from t1 for update")
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "5 6 7", "10 20 500"))

	// Test optimistic for update.
	tk.MustExec("begin optimistic")
	tk.MustQuery("select * from t1 where c1 in (1) for update").Check(testkit.Rows("1 2 3"))
	tk.MustExec("insert into t1 values(10, 10, 10)")
	err = tk.ExecToErr("commit")
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists))
}

func TestAmendTxnVariable(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("use test")

	tk2.MustExec("drop table if exists t1")
	tk2.MustExec("create table t1(c1 int primary key, c2 int, c3 int, unique key uk(c2));")
	tk2.MustExec("insert into t1 values(1, 1, 1);")
	tk2.MustExec("insert into t1 values(2, 2, 2);")

	// Set off the session variable.
	tk3.MustExec("set tidb_enable_amend_pessimistic_txn = 0;")
	tk3.MustExec("begin pessimistic")
	tk3.MustExec("insert into t1 values(3, 3, 3)")
	tk.MustExec("set tidb_enable_amend_pessimistic_txn = 1;")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t1 values(4, 4, 4)")
	tk2.MustExec("alter table t1 add column new_col int")
	require.Error(t, tk3.ExecToErr("commit"))
	tk.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 1 1 <nil>", "2 2 2 <nil>", "4 4 4 <nil>"))
	tk.MustExec("set tidb_enable_amend_pessimistic_txn = 0;")

	// Set off the global variable.
	tk2.MustExec("set global tidb_enable_amend_pessimistic_txn = 0;")

	tk4 := testkit.NewTestKit(t, store)
	tk4.MustExec("use test")

	tk4.MustQuery(`show variables like "tidb_enable_amend_pessimistic_txn"`).Check(testkit.Rows("tidb_enable_amend_pessimistic_txn OFF"))
	tk4.MustExec("begin pessimistic")
	tk4.MustExec("insert into t1 values(5, 5, 5, 5)")
	tk2.MustExec("alter table t1 drop column new_col")
	require.Error(t, tk4.ExecToErr("commit"))
	tk4.MustExec("set tidb_enable_amend_pessimistic_txn = 1;")
	tk4.MustExec("begin pessimistic")
	tk4.MustExec("insert into t1 values(5, 5, 5)")
	tk2.MustExec("alter table t1 add column new_col2 int")
	tk4.MustExec("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 1 1 <nil>", "2 2 2 <nil>", "4 4 4 <nil>", "5 5 5 <nil>"))
}

func TestSelectForUpdateWaitSeconds(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tk")
	tk.MustExec("create table tk (c1 int primary key, c2 int)")
	tk.MustExec("insert into tk values(1,1),(2,2),(3,3),(4,4),(5,5)")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("use test")
	tk4 := testkit.NewTestKit(t, store)
	tk4.MustExec("use test")
	tk5 := testkit.NewTestKit(t, store)
	tk5.MustExec("use test")

	// tk2 lock c1 = 5
	tk2.MustExec("begin pessimistic")
	tk3.MustExec("begin pessimistic")
	tk4.MustExec("begin pessimistic")
	tk5.MustExec("begin pessimistic")
	tk2.MustExec("select * from tk where c1 = 5 or c1 = 1 for update")
	start := time.Now()
	errCh := make(chan error, 3)
	go func() {
		// tk3 try lock c1 = 1 timeout 1sec, the default innodb_lock_wait_timeout value is 50s.
		err := tk3.ExecToErr("select * from tk where c1 = 1 for update wait 1")
		errCh <- err
	}()
	go func() {
		// Lock use selectLockExec.
		err := tk4.ExecToErr("select * from tk where c1 >= 1 for update wait 1")
		errCh <- err
	}()
	go func() {
		// Lock use batchPointGetExec.
		err := tk5.ExecToErr("select c2 from tk where c1 in (1, 5) for update wait 1")
		errCh <- err
	}()
	waitErr := <-errCh
	waitErr2 := <-errCh
	waitErr3 := <-errCh
	require.Error(t, waitErr)
	require.Equal(t, storeerr.ErrLockWaitTimeout.Error(), waitErr.Error())
	require.Error(t, waitErr2)
	require.Equal(t, storeerr.ErrLockWaitTimeout.Error(), waitErr2.Error())
	require.Error(t, waitErr3)
	require.Equal(t, storeerr.ErrLockWaitTimeout.Error(), waitErr3.Error())
	require.Less(t, time.Since(start).Seconds(), 45.0)
	tk2.MustExec("commit")
	tk3.MustExec("rollback")
	tk4.MustExec("rollback")
	tk5.MustExec("rollback")
}

func TestSelectForUpdateConflictRetry(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = 500 * time.Millisecond
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
	})

	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := createAsyncCommitTestKit(t, store)
	tk.MustExec("drop table if exists tk")
	tk.MustExec("create table tk (c1 int primary key, c2 int)")
	tk.MustExec("insert into tk values(1,1),(2,2)")

	tk2 := createAsyncCommitTestKit(t, store)
	tk3 := createAsyncCommitTestKit(t, store)
	tk2.MustExec("begin pessimistic")
	tk3.MustExec("begin pessimistic")
	tk2.MustExec("update tk set c2 = c2 + 1 where c1 = 1")
	tk3.MustExec("update tk set c2 = c2 + 1 where c2 = 2")
	tsCh := make(chan uint64)
	go func() {
		tk3.MustExec("update tk set c2 = c2 + 1 where c1 = 1")
		lastTS, err := store.GetOracle().GetLowResolutionTimestamp(context.Background(), &oracle.Option{TxnScope: kv.GlobalTxnScope})
		require.NoError(t, err)
		tsCh <- lastTS
		tk3.MustExec("commit")
		tsCh <- lastTS
	}()
	// tk2LastTS should be its forUpdateTS
	tk2LastTS, err := store.GetOracle().GetLowResolutionTimestamp(context.Background(), &oracle.Option{TxnScope: kv.GlobalTxnScope})
	require.NoError(t, err)
	tk2.MustExec("commit")

	tk3LastTs := <-tsCh
	// it must get a new ts on pessimistic write conflict so the latest timestamp
	// should increase
	require.Greater(t, tk3LastTs, tk2LastTS)
	// wait until the goroutine exists
	<-tsCh
}

func TestAsyncCommitWithSchemaChange(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("TODO: implement commit_ts calculation in unistore")
	} else {
		t.Skip("This test is unstable as depending on time.Sleep")
	}

	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = time.Second
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
	})
	require.NoError(t, failpoint.Enable("tikvclient/asyncCommitDoNothing", "return"))
	defer func() { require.NoError(t, failpoint.Disable("tikvclient/asyncCommitDoNothing")) }()

	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := createAsyncCommitTestKit(t, store)
	tk.MustExec("drop table if exists tk")
	tk.MustExec("create table tk (c1 int primary key, c2 int, c3 int)")
	tk.MustExec("insert into tk values(1, 1, 1)")
	tk2 := createAsyncCommitTestKit(t, store)
	tk3 := createAsyncCommitTestKit(t, store)
	tk.MustExec("set tidb_enable_amend_pessimistic_txn = 1;")
	tk2.MustExec("set tidb_enable_amend_pessimistic_txn = 1;")
	tk3.MustExec("set tidb_enable_amend_pessimistic_txn = 1;")

	// The txn tk writes something but with failpoint the primary key is not committed.
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into tk values(2, 2, 2)")
	tk.MustExec("update tk set c2 = 10 where c1 = 1")
	ch := make(chan struct{})
	go func() {
		// Add index for c2 before commit
		tk2.MustExec("alter table tk add index k2(c2)")
		ch <- struct{}{}
	}()
	// sleep 100ms to let add index run first
	time.Sleep(100 * time.Millisecond)
	// key for c2 should be amended
	tk.MustExec("commit")
	<-ch
	tk3.MustQuery("select * from tk where c2 = 1").Check(testkit.Rows())
	tk3.MustQuery("select * from tk where c2 = 2").Check(testkit.Rows("2 2 2"))
	tk3.MustQuery("select * from tk where c2 = 10").Check(testkit.Rows("1 10 1"))
	tk3.MustExec("admin check table tk")

	tk.MustExec("begin pessimistic")
	tk.MustExec("update tk set c3 = 20 where c1 = 2")
	tk.MustExec("insert into tk values(3, 3, 3)")
	tk.MustExec("commit")
	// Add index for c3 after commit
	tk2.MustExec("alter table tk add index k3(c3)")
	tk3.MustQuery("select * from tk where c3 = 2").Check(testkit.Rows())
	tk3.MustQuery("select * from tk where c3 = 20").Check(testkit.Rows("2 2 20"))
	tk3.MustQuery("select * from tk where c3 = 3").Check(testkit.Rows("3 3 3"))
	tk3.MustExec("admin check table tk")

	tk.MustExec("drop table if exists tk")
	tk.MustExec("create table tk (c1 int primary key, c2 int)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into tk values(1, 1)")
	require.NoError(t, failpoint.Enable("tikvclient/beforePrewrite", "1*pause"))
	go func() {
		time.Sleep(200 * time.Millisecond)
		tk2.MustExec("alter table tk add index k2(c2)")
		require.NoError(t, failpoint.Disable("tikvclient/beforePrewrite"))
		ch <- struct{}{}
	}()
	tk.MustExec("commit")
	<-ch
	tk.MustQuery("select * from tk where c2 = 1").Check(testkit.Rows("1 1"))
	tk3.MustExec("admin check table tk")
}

func Test1PCWithSchemaChange(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("TODO: implement commit_ts calculation in unistore")
	} else {
		t.Skip("This test is unstable as depending on time.Sleep")
	}

	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = time.Second
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
	})

	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := create1PCTestKit(t, store)
	tk2 := create1PCTestKit(t, store)
	tk3 := create1PCTestKit(t, store)

	tk.MustExec("drop table if exists tk")
	tk.MustExec("create table tk (c1 int primary key, c2 int)")
	tk.MustExec("insert into tk values (1, 1)")
	tk.MustExec("set tidb_enable_amend_pessimistic_txn = 1;")
	tk2.MustExec("set tidb_enable_amend_pessimistic_txn = 1;")
	tk3.MustExec("set tidb_enable_amend_pessimistic_txn = 1;")

	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into tk values(2, 2)")
	tk.MustExec("update tk set c2 = 10 where c1 = 1")
	ch := make(chan struct{})
	go func() {
		// Add index for c2 before commit
		tk2.MustExec("alter table tk add index k2(c2)")
		ch <- struct{}{}
	}()
	// sleep 100ms to let add index run first
	time.Sleep(100 * time.Millisecond)
	// key for c2 should be amended
	tk.MustExec("commit")
	<-ch
	tk3.MustQuery("select * from tk where c2 = 2").Check(testkit.Rows("2 2"))
	tk3.MustQuery("select * from tk where c2 = 1").Check(testkit.Rows())
	tk3.MustQuery("select * from tk where c2 = 10").Check(testkit.Rows("1 10"))
	tk3.MustExec("admin check table tk")

	tk.MustExec("drop table if exists tk")
	tk.MustExec("create table tk (c1 int primary key, c2 int)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into tk values(1, 1)")
	require.NoError(t, failpoint.Enable("tikvclient/beforePrewrite", "1*pause"))
	go func() {
		time.Sleep(200 * time.Millisecond)
		tk2.MustExec("alter table tk add index k2(c2)")
		require.NoError(t, failpoint.Disable("tikvclient/beforePrewrite"))
		ch <- struct{}{}
	}()
	tk.MustExec("commit")
	<-ch
	tk.MustQuery("select * from tk where c2 = 1").Check(testkit.Rows("1 1"))
	tk3.MustExec("admin check table tk")
}

func TestAmendForUniqueIndex(t *testing.T) {
	t.Skip("Skip this unstable test(#25986) and bring it back before 2021-07-29.")
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk.MustExec("set tidb_enable_amend_pessimistic_txn = 1;")

	tk2.MustExec("drop table if exists t1")
	tk2.MustExec("create table t1(c1 int primary key, c2 int, c3 int, unique key uk(c2));")
	tk2.MustExec("insert into t1 values(1, 1, 1);")
	tk2.MustExec("insert into t1 values(2, 2, 2);")

	// New value has duplicates.
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t1 values(3, 3, 3)")
	tk.MustExec("insert into t1 values(4, 4, 3)")
	tk2.MustExec("alter table t1 add unique index uk1(c3)")
	require.Error(t, tk.ExecToErr("commit"))
	tk2.MustExec("alter table t1 drop index uk1")
	tk2.MustExec("admin check table t1")

	// New values has duplicates with old values.
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t1 values(3, 3, 3)")
	tk.MustExec("insert into t1 values(4, 4, 1)")
	tk2.MustExec("alter table t1 add unique index uk1(c3)")
	require.Error(t, tk.ExecToErr("commit"))
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
	require.Error(t, tk.ExecToErr("commit"))
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
	err := <-finishCh
	require.NoError(t, err)
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
	require.Equal(t, nil, err)
	tk.MustExec("commit")
	tk.MustExec("admin check table t")
	err = <-errCh
	require.Equal(t, nil, err)
}

func TestAmendWithColumnTypeChange(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("set tidb_enable_amend_pessimistic_txn = 1;")

	tk2.MustExec("drop table if exists t")
	tk2.MustExec("create table t (id int primary key, v varchar(10));")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t values (1, \"123456789\")")
	tk2.MustExec("alter table t modify column v varchar(5);")
	require.Error(t, tk.ExecToErr("commit"))
}

func TestIssue21498(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk.MustExec("set tidb_enable_amend_pessimistic_txn = 1")

	for _, partition := range []bool{false, true} {
		// RC test
		tk.MustExec("drop table if exists t, t1")
		createTable := "create table t (id int primary key, v int, index iv (v))"
		if partition {
			createTable += " partition by range (id) (partition p0 values less than (0),partition p1 values less than (1),partition p2 values less than (2),partition p3 values less than (3),partition pn values less than MAXVALUE)"
		}
		tk.MustExec(createTable)
		tk.MustExec("insert into t values (1, 10), (2, 20), (3, 30), (4, 40)")
		tk.MustExec("create table t1(id int)")
		tk.MustExec("insert into t1 values(1)")

		tk.MustExec("set tx_isolation = 'READ-COMMITTED'")
		tk.MustExec("begin pessimistic")
		tk.MustQuery("select * from t where v = 10").Check(testkit.Rows("1 10"))

		tk2.MustExec("alter table t drop index iv")
		tk2.MustExec("update t set v = 11 where id = 1")

		tk.MustQuery("select * from t where v = 10").Check(testkit.Rows())
		tk.MustQuery("select * from t where v = 11").Check(testkit.Rows("1 11"))
		tk.MustQuery("select * from t where id = 1").Check(testkit.Rows("1 11"))
		tk.MustExec("admin check table t")
		tk.MustExec("commit")

		tk.MustExec("drop table if exists t")
		createTable = "create table t (id int primary key, v int, index iv (v), v2 int)"
		if partition {
			createTable += " partition by range (id) (partition p0 values less than (0),partition p1 values less than (1),partition p2 values less than (2),partition p3 values less than (3),partition pn values less than MAXVALUE)"
		}
		tk.MustExec(createTable)
		tk.MustExec("insert into t values (1, 10, 100), (2, 20, 200), (3, 30, 300), (4, 40, 400)")

		tk.MustExec("begin pessimistic")
		tk.MustQuery("select * from t use index (iv) where v = 10").Check(testkit.Rows("1 10 100"))
		tk2.MustExec("alter table t drop index iv")
		tk2.MustExec("update t set v = 11 where id = 1")
		err := tk.ExecToErr("select * from t use index (iv) where v = 10")
		require.Equal(t, "[planner:1176]Key 'iv' doesn't exist in table 't'", err.Error())
		tk.MustQuery("select * from t where v = 10").Check(testkit.Rows())
		tk2.MustExec("update t set id = 5 where id = 1")
		err = tk.ExecToErr("select * from t use index (iv) where v = 10") // select with
		require.Equal(t, "[planner:1176]Key 'iv' doesn't exist in table 't'", err.Error())
		tk.MustQuery("select * from t where v = 10").Check(testkit.Rows())
		if !partition {
			// amend transaction does not support partition table
			tk.MustExec("insert into t(id, v, v2) select 6, v + 20, v2 + 200 from t where id = 4") // insert ... select with index unchanged
		}
		err = tk.ExecToErr("insert into t(id, v, v2) select 7, v + 30, v2 + 300 from t use index (iv) where id = 4") // insert ... select with index changed
		require.Equal(t, "[planner:1176]Key 'iv' doesn't exist in table 't'", err.Error())
		tk.MustExec("admin check table t") // check consistency inside txn
		tk.MustExec("commit")
		if !partition {
			tk.MustQuery("select * from t").Check(testkit.Rows("2 20 200", "3 30 300", "4 40 400", "5 11 100", "6 60 600"))
		}
		tk.MustExec("admin check table t") // check consistency out of txn

		// RR test for non partition
		if partition {
			continue
		}

		tk.MustExec("set tx_isolation = 'REPEATABLE-READ'")
		tk2.MustExec("alter table t add unique index iv(v)")
		tk.MustExec("begin pessimistic")
		tk2.MustExec("alter table t drop index iv")
		tk2.MustExec("update t set v = 21 where v = 20")
		tk2.MustExec("update t set v = 31 where v = 30")
		tk.MustExec("update t set v = 22 where v = 21") // fast path
		tk.CheckExecResult(1, 0)
		tk.MustExec("update t set v = 23 where v = 22")
		tk.CheckExecResult(1, 0)
		tk.MustExec("update t set v = 32 where v >= 31 and v < 40") // common path
		tk.CheckExecResult(1, 0)
		tk.MustExec("commit")
		tk.MustQuery("select * from t").Check(testkit.Rows("2 23 200", "3 32 300", "4 40 400", "5 11 100", "6 60 600"))

		tk2.MustExec("alter table t add unique index iv(v)")
		tk.MustExec("begin pessimistic")
		tk2.MustExec("alter table t drop index iv")
		tk2.MustExec("update t set v = 24 where v = 23")
		tk2.MustExec("update t set v = 41 where v = 40")
		// fast path
		tk.MustQuery("select * from t where v = 23").Check(testkit.Rows("2 23 200"))
		tk.MustQuery("select * from t where v = 24").Check(testkit.Rows())
		tk.MustQuery("select * from t where v = 23 for update").Check(testkit.Rows())
		tk.MustQuery("select * from t where v = 24 for update").Check(testkit.Rows("2 24 200"))
		tk.MustQuery("select (select id from t where v = 23), id from t1 for update").Check(testkit.Rows("2 1"))
		tk.MustQuery("select (select id from t where v = 24), id from t1 for update").Check(testkit.Rows("<nil> 1"))
		tk.MustQuery("select (select id from t where v = 23 for update), id from t1").Check(testkit.Rows("<nil> 1"))
		tk.MustQuery("select (select id from t where v = 24 for update), id from t1").Check(testkit.Rows("2 1"))
		tk.MustQuery("select (select id + 1 from t where v = 24 for update), id from t1").Check(testkit.Rows("3 1"))
		// sub queries
		tk.MustQuery("select (select id from (select id from t where v = 24 for update) tmp for update), (select id from t where v = 23), id from t where v = 23").Check(testkit.Rows("2 2 2"))
		tk.MustQuery("select (select id + (select id from t where v = 23) from (select id from t where v = 24 for update) tmp), id from t where v = 23").Check(testkit.Rows("4 2"))
		tk.MustQuery("select (select id + (select id from t where v = 23) from (select id from t where v = 24 for update) tmp for update), id from t where v = 23").Check(testkit.Rows("4 2"))
		tk.MustQuery("select (select id + (select id from t where v = 23 for update) from (select id from t where v = 24 for update) tmp), id from t where v = 23").Check(testkit.Rows("<nil> 2"))
		tk.MustQuery("select (select id + (select id from t where v = 23 for update) from (select id from t where v = 24 for update) tmp for update), id from t where v = 23").Check(testkit.Rows("<nil> 2"))
		tk.MustQuery("select (select id + (select id from t where v = 23) from (select id from t where v = 23) tmp), id from t where v = 24 for update").Check(testkit.Rows("4 2"))
		tk.MustQuery("select (select id + (select id from t where v = 23) from (select id from t where v = 24 for update) tmp), id from t where v = 24 for update").Check(testkit.Rows("4 2"))
		tk.MustQuery("select (select id + (select id from t where v = 24 for update) from (select id from t where v = 23) tmp), id from t where v = 24 for update").Check(testkit.Rows("4 2"))

		// test index look up
		tk.MustQuery("select * from t s, t t1 where s.v = 23 and s.id = t1.id").Check(testkit.Rows("2 23 200 2 23 200"))
		tk.MustQuery("select * from t s, t t1 where s.v = 24 and s.id = t1.id").Check(testkit.Rows())
		tk.MustQuery("select * from t s, t t1 where s.v = 23 and s.id = t1.id for update").Check(testkit.Rows())
		// TODO: Do the same with Partitioned Table!!! Since this query leads to two columns in SelectLocExec.tblID2Handle!!!
		tk.MustQuery("select * from t s, t t1 where s.v = 24 and s.id = t1.id for update").Check(testkit.Rows("2 24 200 2 24 200"))
		tk.MustExec("delete from t where v = 24")
		tk.CheckExecResult(1, 0)
		// common path
		tk.MustQuery("select * from t where v >= 41 and v < 50").Check(testkit.Rows())
		tk.MustQuery("select * from t where v >= 41 and v < 50 for update").Check(testkit.Rows("4 41 400"))
		tk.MustExec("delete from t where v >= 41 and v < 50")
		tk.CheckExecResult(1, 0)
		tk.MustExec("commit")
		tk.MustQuery("select * from t").Check(testkit.Rows("3 32 300", "5 11 100", "6 60 600"))

		tk2.MustExec("alter table t add unique index iv(v)")
		tk.MustExec("begin pessimistic")
		tk2.MustExec("alter table t drop index iv")
		tk2.MustExec("update t set v = 33 where v = 32")
		tk.MustExec("insert into t(id, v, v2) select 3 * id, 3 * v, 3 * v2 from t where v = 33")
		tk.CheckExecResult(1, 0)
		tk.MustExec("insert into t(id, v, v2) select (select 4 * id from t where v = 32) id, 4 * v, 4 * v2 from t where v = 33")
		tk.CheckExecResult(1, 0)
		err = tk.ExecToErr("insert into t(id, v, v2) select (select 4 * id from t where v = 33) id, 4 * v, 4 * v2 from t where v = 33")
		require.Error(t, err)
		require.Equal(t, "[table:1048]Column 'id' cannot be null", err.Error())
		tk.MustExec("commit")
		tk.MustQuery("select * from t").Check(testkit.Rows("3 33 300", "5 11 100", "6 60 600", "9 99 900", "12 132 1200"))

		tk2.MustExec("alter table t add unique index iv(v)")
		tk2.MustExec("drop table if exists t1")
		tk2.MustExec("create table t1(id int primary key, v int, index iv (v), v2 int)")
		tk.MustExec("begin pessimistic")
		tk2.MustExec("alter table t drop index iv")
		tk2.MustExec("update t set v = 34 where v = 33")
		tk2.MustExec("update t set v = 12 where v = 11")
		tk.MustExec("insert into t1(id, v, v2) select * from t where v = 33")
		tk.CheckExecResult(0, 0)
		tk.MustExec("insert into t1(id, v, v2) select * from t where v = 12")
		tk.CheckExecResult(1, 0)
		tk.MustExec("commit")
		tk.MustQuery("select * from t1").Check(testkit.Rows("5 12 100"))
	}
}

func TestPlanCacheSchemaChange(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()
	tmp := testkit.NewTestKit(t, store)
	defer tmp.MustExec("set global tidb_enable_prepared_plan_cache=" + variable.BoolToOnOff(variable.EnablePreparedPlanCache.Load()))
	tmp.MustExec("set global tidb_enable_prepared_plan_cache=ON")

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)

	ctx := context.Background()

	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk3.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key, v int, unique index iv (v), vv int)")
	tk.MustExec("insert into t values(1, 1, 1), (2, 2, 2), (4, 4, 4)")

	tk.MustExec("set tidb_enable_amend_pessimistic_txn = 1")
	tk2.MustExec("set tidb_enable_amend_pessimistic_txn = 1")

	// generate plan cache
	tk.MustExec("prepare update_stmt from 'update t set vv = vv + 1 where v = ?'")
	tk.MustExec("set @v = 1")
	tk.MustExec("execute update_stmt using @v")

	stmtID, _, _, err := tk2.Session().PrepareStmt("update t set vv = vv + 1 where v = ?")
	require.NoError(t, err)
	_, err = tk2.Session().ExecutePreparedStmt(ctx, stmtID, []types.Datum{types.NewDatum(1)})
	require.NoError(t, err)

	tk.MustExec("begin pessimistic")
	tk2.MustExec("begin pessimistic")

	tk3.MustExec("alter table t drop index iv")
	tk3.MustExec("update t set v = 3 where v = 2")
	tk3.MustExec("update t set v = 5 where v = 4")

	tk.MustExec("set @v = 2")
	tk.MustExec("execute update_stmt using @v")
	tk.CheckExecResult(0, 0)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("set @v = 3")
	tk.MustExec("execute update_stmt using @v")
	tk.CheckExecResult(1, 0)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	_, err = tk2.Session().ExecutePreparedStmt(ctx, stmtID, []types.Datum{types.NewDatum(4)})
	require.NoError(t, err)
	tk2.CheckExecResult(0, 0)
	tk2.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	_, err = tk2.Session().ExecutePreparedStmt(ctx, stmtID, []types.Datum{types.NewDatum(5)})
	require.NoError(t, err)
	tk2.CheckExecResult(1, 0)
	tk2.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("commit")
	tk2.MustExec("commit")

	tk.MustQuery("select * from t").Check(testkit.Rows("1 1 3", "2 3 3", "4 5 5"))
}

func TestAsyncCommitCalTSFail(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = time.Second
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
	})

	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := createAsyncCommitTestKit(t, store)
	tk2 := createAsyncCommitTestKit(t, store)

	tk.MustExec("drop table if exists tk")
	tk.MustExec("create table tk (c1 int primary key, c2 int)")
	tk.MustExec("insert into tk values (1, 1)")

	tk.MustExec("set tidb_enable_1pc = true")
	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from tk for update").Check(testkit.Rows("1 1"))
	require.NoError(t, failpoint.Enable("tikvclient/failCheckSchemaValid", "return"))
	require.Error(t, tk.ExecToErr("commit"))
	require.NoError(t, failpoint.Disable("tikvclient/failCheckSchemaValid"))

	// The lock should not be blocked.
	tk2.MustExec("set innodb_lock_wait_timeout = 5")
	tk2.MustExec("begin pessimistic")
	tk2.MustExec("update tk set c2 = c2 + 1")
	tk2.MustExec("commit")
}

func TestChangeLockToPut(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("drop table if exists tk")
	tk.MustExec("create table t1(c1 varchar(20) key, c2 int, c3 int, unique key k1(c2), key k2(c3))")
	tk.MustExec(`insert into t1 values ("1", 1, 1), ("2", 2, 2), ("3", 3, 3)`)

	// Test point get change lock to put.
	for _, mode := range []string{"REPEATABLE-READ", "READ-COMMITTED"} {
		tk.MustExec(fmt.Sprintf(`set tx_isolation = "%s"`, mode))
		tk.MustExec("begin pessimistic")
		tk.MustQuery(`select * from t1 where c1 = "1" for update`).Check(testkit.Rows("1 1 1"))
		tk.MustExec("commit")
		tk.MustExec("begin pessimistic")
		tk.MustQuery(`select * from t1 where c1 = "1" for update`).Check(testkit.Rows("1 1 1"))
		tk.MustExec("commit")
		tk.MustExec("admin check table t1")
		tk2.MustExec("begin")
		tk2.MustQuery(`select * from t1 use index(k1) where c2 = "1" for update`).Check(testkit.Rows("1 1 1"))
		tk2.MustQuery(`select * from t1 use index(k1) where c2 = "3" for update`).Check(testkit.Rows("3 3 3"))
		tk2.MustExec("commit")
		tk2.MustExec("begin")
		tk2.MustQuery(`select * from t1 use index(k2) where c3 = 1`).Check(testkit.Rows("1 1 1"))
		tk2.MustQuery("select * from t1 use index(k2) where c3 > 1").Check(testkit.Rows("2 2 2", "3 3 3"))
		tk2.MustExec("commit")
	}

	// Test batch point get change lock to put.
	for _, mode := range []string{"REPEATABLE-READ", "READ-COMMITTED"} {
		tk.MustExec(fmt.Sprintf(`set tx_isolation = "%s"`, mode))
		tk.MustExec("begin pessimistic")
		tk.MustQuery(`select * from t1 where c1 in ("1", "5", "3") for update`).Check(testkit.Rows("1 1 1", "3 3 3"))
		tk.MustExec("commit")
		tk.MustExec("begin pessimistic")
		tk.MustQuery(`select * from t1 where c1 in ("1", "2", "8") for update`).Check(testkit.Rows("1 1 1", "2 2 2"))
		tk.MustExec("commit")
		tk.MustExec("admin check table t1")
		tk2.MustExec("begin")
		tk2.MustQuery(`select * from t1 use index(k1) where c2 in ("1", "2", "3") for update`).Check(testkit.Rows("1 1 1", "2 2 2", "3 3 3"))
		tk2.MustQuery(`select * from t1 use index(k2) where c2 in ("2") for update`).Check(testkit.Rows("2 2 2"))
		tk2.MustExec("commit")
		tk2.MustExec("begin")
		tk2.MustQuery(`select * from t1 use index(k2) where c3 in (5, 8)`).Check(testkit.Rows())
		tk2.MustQuery(`select * from t1 use index(k2) where c3 in (1, 8) for update`).Check(testkit.Rows("1 1 1"))
		tk2.MustQuery(`select * from t1 use index(k2) where c3 > 1`).Check(testkit.Rows("2 2 2", "3 3 3"))
		tk2.MustExec("commit")
	}

	tk.MustExec("admin check table t1")
}

func createTable(part bool, columnNames []string, columnTypes []string) string {
	var str string
	str = "create table t("
	if part {
		str = "create table t_part("
	}
	first := true
	for i, colName := range columnNames {
		if first {
			first = false
		} else {
			str += ","
		}
		str += fmt.Sprintf("%s %s", colName, columnTypes[i])
	}
	str += ", primary key(c_int, c_str)"
	str += ")"
	if part {
		str += "partition by hash(c_int) partitions 8"
	}
	return str
}

func TestAmendForIndexChange(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
	})
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("set tidb_enable_amend_pessimistic_txn = ON;")
	tk.Session().GetSessionVars().EnableAsyncCommit = false
	tk.Session().GetSessionVars().Enable1PC = false

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
			require.Error(t, tk.ExecToErr("commit"))
			tk2.MustExec("admin check table t_part")

			tk.MustExec("begin pessimistic")
			tk.MustExec(`insert into t_part values(6, "666", "2000-01-06", "2020-01-06", "6.6", "666.666", 6.6)`)
			tk2.MustExec(fmt.Sprintf(`alter table t_part drop index %s`, idxName))
			require.Error(t, tk.ExecToErr("commit"))
			tk2.MustExec("admin check table t_part")
			tk2.MustQuery("select count(*) from t_part").Check(testkit.Rows("2"))
		}
	}
}

func TestAmendForColumnChange(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("set tidb_enable_amend_pessimistic_txn = ON;")
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
			require.Error(t, tk.ExecToErr("commit"))
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
		require.Error(t, tk.ExecToErr("commit"))
		tk2.MustExec("admin check table t_part")
		tk2.MustQuery("select count(*) from t_part").Check(testkit.Rows("2"))
	}
}

func TestPessimisticAutoCommitTxn(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("set tidb_txn_mode = 'pessimistic'")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (i int)")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("set autocommit = on")

	rows := tk.MustQuery("explain update t set i = -i").Rows()
	explain := fmt.Sprintf("%v", rows[1])
	require.NotRegexp(t, ".*SelectLock.*", explain)

	originCfg := config.GetGlobalConfig()
	defer config.StoreGlobalConfig(originCfg)
	newCfg := *originCfg
	newCfg.PessimisticTxn.PessimisticAutoCommit.Store(true)
	config.StoreGlobalConfig(&newCfg)

	rows = tk.MustQuery("explain update t set i = -i").Rows()
	explain = fmt.Sprintf("%v", rows[1])
	require.Regexp(t, ".*SelectLock.*", explain)
}

func TestPessimisticLockOnPartition(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	// This test checks that 'select ... for update' locks the partition instead of the table.
	// Cover a bug that table ID is used to encode the lock key mistakenly.
	tk := testkit.NewTestKit(t, store)
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

	tk1 := testkit.NewTestKit(t, store)
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
	require.Equal(t, int32(1), <-ch)
	require.Equal(t, int32(0), <-ch)
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
	require.Equal(t, int32(1), <-ch)
	require.Equal(t, int32(0), <-ch)
	<-ch // wait for goroutine to quit.
}
