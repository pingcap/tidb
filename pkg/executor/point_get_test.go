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

package executor_test

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestSelectCheckVisibility(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(10) key, b int,index idx(b))")
	tk.MustExec("insert into t values('1',1)")
	tk.MustExec("begin")
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	ts := txn.StartTS()
	sessionStore := tk.Session().GetStore().(tikv.Storage)
	// Update gc safe time for check data visibility.
	sessionStore.UpdateTxnSafePointCache(ts+1, time.Now())
	checkSelectResultError := func(sql string, expectErr *terror.Error) {
		re, err := tk.Exec(sql)
		require.NoError(t, err)
		defer re.Close()
		_, err = session.ResultSetToStringSlice(context.Background(), tk.Session(), re)
		require.Error(t, err)
		require.True(t, expectErr.Equal(err))
	}
	// Test point get.
	checkSelectResultError("select * from t where a='1'", storeerr.ErrTxnAbortedByGC)
	// Test batch point get.
	checkSelectResultError("select * from t where a in ('1','2')", storeerr.ErrTxnAbortedByGC)
	// Test Index look up read.
	checkSelectResultError("select * from t where b > 0 ", storeerr.ErrTxnAbortedByGC)
	// Test Index read.
	checkSelectResultError("select b from t where b > 0 ", storeerr.ErrTxnAbortedByGC)
	// Test table read.
	checkSelectResultError("select * from t", storeerr.ErrTxnAbortedByGC)
}

func TestReturnValues(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.Session().GetSessionVars().EnableClusteredIndex = vardef.ClusteredIndexDefModeIntOnly
	tk.MustExec("create table t (a varchar(64) primary key, b int)")
	tk.MustExec("insert t values ('a', 1), ('b', 2), ('c', 3)")
	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from t where a = 'b' for update").Check(testkit.Rows("b 2"))
	tid := external.GetTableByName(t, tk, "test", "t").Meta().ID
	idxVal, err := codec.EncodeKey(tk.Session().GetSessionVars().StmtCtx.TimeZone(), nil, types.NewStringDatum("b"))
	require.NoError(t, err)
	pk := tablecodec.EncodeIndexSeekKey(tid, 1, idxVal)
	txnCtx := tk.Session().GetSessionVars().TxnCtx
	val, ok := txnCtx.GetKeyInPessimisticLockCache(pk)
	require.True(t, ok)
	handle, err := tablecodec.DecodeHandleInIndexValue(val)
	require.NoError(t, err)
	rowKey := tablecodec.EncodeRowKeyWithHandle(tid, handle)
	_, ok = txnCtx.GetKeyInPessimisticLockCache(rowKey)
	require.True(t, ok)
	tk.MustExec("rollback")
}

func TestBatchPointGetForUpdateUsePessimisticLockCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key, v int)")
	tk.MustExec("insert into t values (1, 1), (2, 2)")

	tblInfo := external.GetTableByName(t, tk, "test", "t")
	tid := tblInfo.Meta().ID

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/unistoreRPCClientSendHook", `return(true)`))
	defer func() {
		unistore.UnistoreRPCClientSendHook.Store(nil)
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/unistoreRPCClientSendHook"))
	}()

	var getCnt, batchGetCnt, lockCnt int64
	hook := func(req *tikvrpc.Request) {
		var key []byte
		switch req.Type {
		case tikvrpc.CmdGet:
			key = req.Get().Key
		case tikvrpc.CmdBatchGet:
			r := req.BatchGet()
			if len(r.Keys) == 0 {
				return
			}
			key = r.Keys[0]
		case tikvrpc.CmdPessimisticLock:
			key = req.PessimisticLock().PrimaryLock
		default:
			return
		}
		if tablecodec.DecodeTableID(key) != tid {
			return
		}
		switch req.Type {
		case tikvrpc.CmdGet:
			atomic.AddInt64(&getCnt, 1)
		case tikvrpc.CmdBatchGet:
			atomic.AddInt64(&batchGetCnt, 1)
		case tikvrpc.CmdPessimisticLock:
			atomic.AddInt64(&lockCnt, 1)
		}
	}
	unistore.UnistoreRPCClientSendHook.Store(&hook)

	tk.MustExec("begin pessimistic")
	defer tk.MustExec("rollback")

	// update the values to make sure the "for update request" needs to fetch the latest value
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("update t set v = v*10")
	tk.MustQuery("select * from t").Sort().Check(testkit.Rows("1 1", "2 2"))

	atomic.StoreInt64(&getCnt, 0)
	atomic.StoreInt64(&batchGetCnt, 0)
	atomic.StoreInt64(&lockCnt, 0)

	sql := "select * from t where id in (1, 2) for update"
	tk.MustHavePlan(sql, "Batch_Point_Get")
	tk.MustQuery(sql).Sort().Check(testkit.Rows("1 10", "2 20"))

	// the first query should lock request to fetch value
	require.GreaterOrEqual(t, atomic.LoadInt64(&lockCnt), int64(1))
	require.Equal(t, int64(0), atomic.LoadInt64(&getCnt)+atomic.LoadInt64(&batchGetCnt))

	// the second query should use pessimistic lock cache, no request sent
	atomic.StoreInt64(&getCnt, 0)
	atomic.StoreInt64(&batchGetCnt, 0)
	atomic.StoreInt64(&lockCnt, 0)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("1 10", "2 20"))
	require.Equal(t, int64(0), atomic.LoadInt64(&getCnt)+atomic.LoadInt64(&batchGetCnt)+atomic.LoadInt64(&lockCnt))

	// if the rows contains _tidb_commit_ts, should not use cache
	sql2 := "select * from t where id in (1, 2) and _tidb_commit_ts > 100 for update"
	tk.MustHavePlan(sql2, "Batch_Point_Get")
	atomic.StoreInt64(&getCnt, 0)
	atomic.StoreInt64(&batchGetCnt, 0)
	atomic.StoreInt64(&lockCnt, 0)
	tk.MustQuery(sql2).Sort().Check(testkit.Rows("1 10", "2 20"))
	require.GreaterOrEqual(t, atomic.LoadInt64(&lockCnt)+atomic.LoadInt64(&getCnt), int64(0))
	require.Equal(t, int64(1), atomic.LoadInt64(&batchGetCnt))
}

func mustExecDDL(tk *testkit.TestKit, t *testing.T, sql string, dom *domain.Domain) {
	tk.MustExec(sql)
	require.NoError(t, dom.Reload())
}

func TestMemCacheReadLock(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableTableLock = true
	})
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.Session().GetSessionVars().EnablePointGetCache = true
	defer func() {
		tk.Session().GetSessionVars().EnablePointGetCache = false
		tk.MustExec("drop table if exists point")
	}()

	tk.MustExec("drop table if exists point")
	tk.MustExec("create table point (id int primary key, c int, d varchar(10), unique c_d (c, d))")
	tk.MustExec("insert point values (1, 1, 'a')")
	tk.MustExec("insert point values (2, 2, 'b')")

	// Simply check the cached results.
	mustExecDDL(tk, t, "lock tables point read", dom)
	tk.MustQuery("select id from point where id = 1").Check(testkit.Rows("1"))
	tk.MustQuery("select id from point where id = 1").Check(testkit.Rows("1"))
	mustExecDDL(tk, t, "unlock tables", dom)

	cases := []struct {
		sql string
		r1  bool
		r2  bool
	}{
		{"explain analyze select * from point where id = 1", false, false},
		{"explain analyze select * from point where id in (1, 2)", false, false},

		// Cases for not exist keys.
		{"explain analyze select * from point where id = 3", true, true},
		{"explain analyze select * from point where id in (1, 3)", true, true},
		{"explain analyze select * from point where id in (3, 4)", true, true},
	}

	for _, ca := range cases {
		mustExecDDL(tk, t, "lock tables point read", dom)

		rows := tk.MustQuery(ca.sql).Rows()
		require.Lenf(t, rows, 1, "%v", ca.sql)
		explain := fmt.Sprintf("%v", rows[0])
		require.Regexp(t, ".*num_rpc.*", explain)

		rows = tk.MustQuery(ca.sql).Rows()
		require.Len(t, rows, 1)
		explain = fmt.Sprintf("%v", rows[0])
		ok := strings.Contains(explain, "num_rpc")
		require.Equalf(t, ok, ca.r1, "%v", ca.sql)
		mustExecDDL(tk, t, "unlock tables", dom)

		rows = tk.MustQuery(ca.sql).Rows()
		require.Len(t, rows, 1)
		explain = fmt.Sprintf("%v", rows[0])
		require.Regexp(t, ".*num_rpc.*", explain)

		// Test cache release after unlocking tables.
		mustExecDDL(tk, t, "lock tables point read", dom)
		rows = tk.MustQuery(ca.sql).Rows()
		require.Len(t, rows, 1)
		explain = fmt.Sprintf("%v", rows[0])
		require.Regexp(t, ".*num_rpc.*", explain)

		rows = tk.MustQuery(ca.sql).Rows()
		require.Len(t, rows, 1)
		explain = fmt.Sprintf("%v", rows[0])
		ok = strings.Contains(explain, "num_rpc")
		require.Equal(t, ok, ca.r2, "%v", ca.sql)

		mustExecDDL(tk, t, "unlock tables", dom)
		mustExecDDL(tk, t, "lock tables point read", dom)

		rows = tk.MustQuery(ca.sql).Rows()
		require.Len(t, rows, 1)
		explain = fmt.Sprintf("%v", rows[0])
		require.Regexp(t, ".*num_rpc.*", explain)

		mustExecDDL(tk, t, "unlock tables", dom)
	}
}

func TestBatchPointGetCommitTSWithTableLockRead(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableTableLock = true
	})
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.Session().GetSessionVars().EnablePointGetCache = true
	defer func() {
		tk.Session().GetSessionVars().EnablePointGetCache = false
		tk.MustExec("drop table if exists point")
	}()

	tk.MustExec("drop table if exists point")
	tk.MustExec("create table point (id int primary key, v int)")
	tk.MustExec("insert point values (1, 1), (2, 2)")

	mustExecDDL(tk, t, "lock tables point read", dom)
	defer mustExecDDL(tk, t, "unlock tables", dom)

	sql := "select _tidb_commit_ts from point where id in (1, 2)"
	tk.MustHavePlan(sql, "Batch_Point_Get")
	rows := tk.MustQuery(sql).Rows()
	require.Len(t, rows, 2)
	for _, r := range rows {
		require.Len(t, r, 1)
		ts, err := strconv.ParseUint(r[0].(string), 10, 64)
		require.NoError(t, err)
		require.NotZero(t, ts)
	}
}

func TestPartitionMemCacheReadLock(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableTableLock = true
	})
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.Session().GetSessionVars().EnablePointGetCache = true
	defer func() {
		tk.Session().GetSessionVars().EnablePointGetCache = false
		tk.MustExec("drop table if exists point")
	}()

	tk.MustExec("drop table if exists point")
	tk.MustExec("create table point (id int unique key, c int, d varchar(10)) partition by hash (id) partitions 4")
	tk.MustExec("insert point values (1, 1, 'a')")
	tk.MustExec("insert point values (2, 2, 'b')")

	// Confirm _tidb_rowid will not be duplicated.
	tk.MustQuery("select distinct(_tidb_rowid) from point order by _tidb_rowid").Check(testkit.Rows("1", "2"))

	mustExecDDL(tk, t, "lock tables point read", dom)

	tk.MustQuery("select _tidb_rowid from point where id = 1").Check(testkit.Rows("1"))
	mustExecDDL(tk, t, "unlock tables", dom)

	tk.MustQuery("select _tidb_rowid from point where id = 1").Check(testkit.Rows("1"))
	tk.MustExec("update point set id = -id")

	// Test cache release after unlocking tables.
	mustExecDDL(tk, t, "lock tables point read", dom)
	tk.MustQuery("select _tidb_rowid from point where id = 1").Check(testkit.Rows())

	tk.MustQuery("select _tidb_rowid from point where id = -1").Check(testkit.Rows("1"))
	tk.MustQuery("select _tidb_rowid from point where id = -1").Check(testkit.Rows("1"))
	tk.MustQuery("select _tidb_rowid from point where id = -2").Check(testkit.Rows("2"))

	mustExecDDL(tk, t, "unlock tables", dom)
}

func TestPointGetLockExistKey(t *testing.T) {
	testLock := func(t *testing.T, rc bool, key string, tableName string) {
		store := testkit.CreateMockStore(t)
		tk1, tk2 := testkit.NewTestKit(t, store), testkit.NewTestKit(t, store)

		tk1.MustExec("use test")
		tk2.MustExec("use test")
		tk1.Session().GetSessionVars().EnableClusteredIndex = vardef.ClusteredIndexDefModeIntOnly

		tk1.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
		tk1.MustExec(fmt.Sprintf("create table %s(id int, v int, k int, %s key0(id, v))", tableName, key))
		tk1.MustExec(fmt.Sprintf("insert into %s values(1, 1, 1)", tableName))

		if rc {
			tk1.MustExec("set tx_isolation = 'READ-COMMITTED'")
			tk2.MustExec("set tx_isolation = 'READ-COMMITTED'")
		}

		// select for update
		tk1.MustExec("begin pessimistic")
		tk2.MustExec("begin pessimistic")
		// lock exist key
		tk1.MustExec(fmt.Sprintf("select * from %s where id = 1 and v = 1 for update", tableName))
		// read committed will not lock non-exist key
		if rc {
			tk1.MustExec(fmt.Sprintf("select * from %s where id = 2 and v = 2 for update", tableName))
		}
		tk2.MustExec(fmt.Sprintf("insert into %s values(2, 2, 2)", tableName))
		var wg3 util.WaitGroupWrapper
		wg3.Run(func() {
			tk2.MustExec(fmt.Sprintf("insert into %s values(1, 1, 10)", tableName))
			// tk2.MustExec(fmt.Sprintf("insert into %s values(1, 1, 10)", tableName))
		})
		time.Sleep(150 * time.Millisecond)
		tk1.MustExec(fmt.Sprintf("update %s set v = 2 where id = 1 and v = 1", tableName))
		tk1.MustExec("commit")
		wg3.Wait()
		tk2.MustExec("commit")
		tk1.MustQuery(fmt.Sprintf("select * from %s", tableName)).Check(testkit.Rows(
			"1 2 1",
			"2 2 2",
			"1 1 10",
		))

		// update
		tk1.MustExec("begin pessimistic")
		tk2.MustExec("begin pessimistic")
		// lock exist key
		tk1.MustExec(fmt.Sprintf("update %s set v = 3 where id = 2 and v = 2", tableName))
		// read committed will not lock non-exist key
		if rc {
			tk1.MustExec(fmt.Sprintf("update %s set v =4 where id = 3 and v = 3", tableName))
		}
		tk2.MustExec(fmt.Sprintf("insert into %s values(3, 3, 3)", tableName))
		var wg2 util.WaitGroupWrapper
		wg2.Run(func() {
			tk2.MustExec(fmt.Sprintf("insert into %s values(2, 2, 20)", tableName))
		})
		time.Sleep(150 * time.Millisecond)
		tk1.MustExec("commit")
		wg2.Wait()
		tk2.MustExec("commit")
		tk1.MustQuery(fmt.Sprintf("select * from %s", tableName)).Check(testkit.Rows(
			"1 2 1",
			"2 3 2",
			"1 1 10",
			"3 3 3",
			"2 2 20",
		))

		// delete
		tk1.MustExec("begin pessimistic")
		tk2.MustExec("begin pessimistic")
		// lock exist key
		tk1.MustExec(fmt.Sprintf("delete from %s where id = 3 and v = 3", tableName))
		// read committed will not lock non-exist key
		if rc {
			tk1.MustExec(fmt.Sprintf("delete from %s where id = 4 and v = 4", tableName))
		}
		tk2.MustExec(fmt.Sprintf("insert into %s values(4, 4, 4)", tableName))
		var wg1 util.WaitGroupWrapper
		wg1.Run(func() {
			tk2.MustExec(fmt.Sprintf("insert into %s values(3, 3, 30)", tableName))
		})
		time.Sleep(50 * time.Millisecond)
		tk1.MustExec("commit")
		wg1.Wait()
		tk2.MustExec("commit")
		tk1.MustQuery(fmt.Sprintf("select * from %s", tableName)).Check(testkit.Rows(
			"1 2 1",
			"2 3 2",
			"1 1 10",
			"2 2 20",
			"4 4 4",
			"3 3 30",
		))
	}

	for i, one := range []struct {
		rc  bool
		key string
	}{
		{rc: false, key: "primary key"},
		{rc: false, key: "unique key"},
		{rc: true, key: "primary key"},
		{rc: true, key: "unique key"},
	} {
		tableName := fmt.Sprintf("t_%d", i)
		t.Run(tableName, func(t *testing.T) {
			func(rc bool, key string, tableName string) {
				testLock(t, rc, key, tableName)
			}(one.rc, one.key, tableName)
		})
	}
}

func TestWithTiDBSnapshot(t *testing.T) {
	// Fix issue https://github.com/pingcap/tidb/issues/22436
	// Point get should not use math.MaxUint64 when variable @@tidb_snapshot is set.
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists xx")
	tk.MustExec(`create table xx (id int key)`)
	tk.MustExec(`insert into xx values (1), (7)`)

	// Unrelated code, to make this test pass in the unit test.
	// The `tikv_gc_safe_point` global variable must be there, otherwise the 'set @@tidb_snapshot' operation fails.
	timeSafe := time.Now().Add(-48 * 60 * 60 * time.Second).Format("20060102-15:04:05 -0700 MST")
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	tk.MustExec(fmt.Sprintf(safePointSQL, timeSafe))

	// Record the current tso.
	tk.MustExec("begin")
	tso := tk.Session().GetSessionVars().TxnCtx.StartTS
	tk.MustExec("rollback")
	require.True(t, tso > 0)

	// Insert data.
	tk.MustExec("insert into xx values (8)")

	// Change the snapshot before the tso, the inserted data should not be seen.
	tk.MustExec(fmt.Sprintf("set @@tidb_snapshot = '%d'", tso))
	tk.MustQuery("select * from xx where id = 8").Check(testkit.Rows())

	tk.MustQuery("select * from xx").Check(testkit.Rows("1", "7"))
}

func TestSoftDeleteForUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists softdelete")
	tk.MustExec(`create table softdelete (id int key, v int) active_active='on' softdelete retention 7 day`)
	tk.MustExec(`insert into softdelete values (1, 1), (2, 2), (3, 3)`)
	tk.MustExec(`set @@tidb_translate_softdelete_sql = 1`)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("begin")
	tk1.MustQuery("select * from softdelete where id = 2 for update").Check(testkit.Rows("2 2"))

	var done atomic.Bool
	go func() {
		// delete this row, it should be blocked because of the for update lock
		tk.MustExec("delete from softdelete where id = 2")
		done.Store(true)
	}()

	// check the goroutine logic is block
	require.Never(t, func() bool { return done.Load() }, 100*time.Millisecond, 5*time.Millisecond)
	tk1.MustExec("commit")
	require.Eventually(t, func() bool { return done.Load() }, 100*time.Millisecond, time.Millisecond)

	// test again, this time, id = 2 is soft deleted, check for update lock's behavior
	tk1.MustExec("begin")
	tk1.MustQuery("select * from softdelete where id = 2 for update").Check(testkit.Rows())

	done.Store(false)
	go func() {
		tk.MustExec("insert into softdelete values (2, 22)")
		done.Store(true)
	}()

	// check the goroutine logic is block
	require.Never(t, func() bool { return done.Load() }, 100*time.Millisecond, 5*time.Millisecond)
	tk1.MustExec("commit")
	require.Eventually(t, func() bool { return done.Load() }, 100*time.Millisecond, time.Millisecond)

	// the third test, record id = 5 does not exist, but for update should lock it
	tk1.MustExec("begin")
	tk1.MustQuery("select * from softdelete where id = 5 for update").Check(testkit.Rows())

	done.Store(false)
	go func() {
		tk.MustExec("insert into softdelete values (5, 5)")
		done.Store(true)
	}()

	// check the goroutine logic is block
	require.Never(t, func() bool { return done.Load() }, 100*time.Millisecond, 5*time.Millisecond)
	tk1.MustExec("commit")
}

func TestSoftDeleteImplicitDeleteRowsMetricsBySQLType(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	sctx := tk.Session().(sessionctx.Context)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key, v int) softdelete retention 7 day")
	tk.MustExec("set @@tidb_translate_softdelete_sql = 1")

	readHist := func(label string) (count uint64, sum float64) {
		obs, err := metrics.SoftDeleteImplicitDeleteRows.GetMetricWithLabelValues(label)
		require.NoError(t, err)
		h, ok := obs.(prometheus.Histogram)
		require.True(t, ok)
		m := &dto.Metric{}
		require.NoError(t, h.Write(m))
		return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
	}

	cases := []struct {
		name  string
		label string
		exec  func()
	}{
		{
			name:  "insert",
			label: "Insert",
			exec: func() {
				tk.MustExec("insert into t values (1, 2), (3, 4), (4, 5)")
			},
		},
		{
			name:  "load data",
			label: "LoadData",
			exec: func() {
				readerBuilder := executor.LoadDataReaderBuilder{
					Build: func(_ string) (io.ReadCloser, error) {
						return mydump.NewStringReader("1 2\n3 4\n4 5\n"), nil
					},
					Wg: &sync.WaitGroup{},
				}
				sctx.SetValue(executor.LoadDataReaderBuilderKey, readerBuilder)
				defer sctx.SetValue(executor.LoadDataReaderBuilderKey, nil)
				tk.MustExec("LOAD DATA LOCAL INFILE '/tmp/nonexistence.csv' INTO TABLE t fields terminated by ' ' lines terminated by '\\n' (id, v)")
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tk.MustExec("set @@tidb_translate_softdelete_sql = 'OFF'")
			tk.MustExec("delete from t")
			tk.MustExec("set @@tidb_translate_softdelete_sql = 1")
			tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
			tk.MustExec("delete from t")

			beforeCount, beforeSum := readHist(c.label)
			c.exec()
			afterCount, afterSum := readHist(c.label)

			require.Equal(t, beforeCount+1, afterCount)
			require.Equal(t, beforeSum+2, afterSum)
			tk.MustQuery("select * from t").Sort().Check(testkit.Rows("1 2", "3 4", "4 5"))
		})
	}
}
