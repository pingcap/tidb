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

package executor_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/deadlockhistory"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

func TestTiDBLastTxnInfoCommitMode(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = time.Second
	})

	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, v int)")
	tk.MustExec("insert into t values (1, 1)")

	tk.MustExec("set @@tidb_enable_async_commit = 1")
	tk.MustExec("set @@tidb_enable_1pc = 0")
	tk.MustExec("update t set v = v + 1 where a = 1")
	rows := tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.txn_commit_mode'), json_extract(@@tidb_last_txn_info, '$.async_commit_fallback'), json_extract(@@tidb_last_txn_info, '$.one_pc_fallback')").Rows()
	t.Log(rows)
	require.Equal(t, `"async_commit"`, rows[0][0])
	require.Equal(t, "false", rows[0][1])
	require.Equal(t, "false", rows[0][2])

	tk.MustExec("set @@tidb_enable_async_commit = 0")
	tk.MustExec("set @@tidb_enable_1pc = 1")
	tk.MustExec("update t set v = v + 1 where a = 1")
	rows = tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.txn_commit_mode'), json_extract(@@tidb_last_txn_info, '$.async_commit_fallback'), json_extract(@@tidb_last_txn_info, '$.one_pc_fallback')").Rows()
	require.Equal(t, `"1pc"`, rows[0][0])
	require.Equal(t, "false", rows[0][1])
	require.Equal(t, "false", rows[0][2])

	tk.MustExec("set @@tidb_enable_async_commit = 0")
	tk.MustExec("set @@tidb_enable_1pc = 0")
	tk.MustExec("update t set v = v + 1 where a = 1")
	rows = tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.txn_commit_mode'), json_extract(@@tidb_last_txn_info, '$.async_commit_fallback'), json_extract(@@tidb_last_txn_info, '$.one_pc_fallback')").Rows()
	require.Equal(t, `"2pc"`, rows[0][0])
	require.Equal(t, "false", rows[0][1])
	require.Equal(t, "false", rows[0][2])

	require.NoError(t, failpoint.Enable("tikvclient/invalidMaxCommitTS", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("tikvclient/invalidMaxCommitTS"))
	}()

	tk.MustExec("set @@tidb_enable_async_commit = 1")
	tk.MustExec("set @@tidb_enable_1pc = 0")
	tk.MustExec("update t set v = v + 1 where a = 1")
	rows = tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.txn_commit_mode'), json_extract(@@tidb_last_txn_info, '$.async_commit_fallback'), json_extract(@@tidb_last_txn_info, '$.one_pc_fallback')").Rows()
	t.Log(rows)
	require.Equal(t, `"2pc"`, rows[0][0])
	require.Equal(t, "true", rows[0][1])
	require.Equal(t, "false", rows[0][2])

	tk.MustExec("set @@tidb_enable_async_commit = 0")
	tk.MustExec("set @@tidb_enable_1pc = 1")
	tk.MustExec("update t set v = v + 1 where a = 1")
	rows = tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.txn_commit_mode'), json_extract(@@tidb_last_txn_info, '$.async_commit_fallback'), json_extract(@@tidb_last_txn_info, '$.one_pc_fallback')").Rows()
	t.Log(rows)
	require.Equal(t, `"2pc"`, rows[0][0])
	require.Equal(t, "false", rows[0][1])
	require.Equal(t, "true", rows[0][2])

	tk.MustExec("set @@tidb_enable_async_commit = 1")
	tk.MustExec("set @@tidb_enable_1pc = 1")
	tk.MustExec("update t set v = v + 1 where a = 1")
	rows = tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.txn_commit_mode'), json_extract(@@tidb_last_txn_info, '$.async_commit_fallback'), json_extract(@@tidb_last_txn_info, '$.one_pc_fallback')").Rows()
	t.Log(rows)
	require.Equal(t, `"2pc"`, rows[0][0])
	require.Equal(t, "true", rows[0][1])
	require.Equal(t, "true", rows[0][2])
}

func TestPointGetRepeatableRead(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec(`create table point_get (a int, b int, c int,
			primary key k_a(a),
			unique key k_b(b))`)
	tk1.MustExec("insert into point_get values (1, 1, 1)")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	var (
		step1 = "github.com/pingcap/tidb/pkg/executor/pointGetRepeatableReadTest-step1"
		step2 = "github.com/pingcap/tidb/pkg/executor/pointGetRepeatableReadTest-step2"
	)

	require.NoError(t, failpoint.Enable(step1, "return"))
	require.NoError(t, failpoint.Enable(step2, "pause"))

	updateWaitCh := make(chan struct{})
	go func() {
		ctx := context.WithValue(context.Background(), "pointGetRepeatableReadTest", updateWaitCh)
		ctx = failpoint.WithHook(ctx, func(ctx context.Context, fpname string) bool {
			return fpname == step1 || fpname == step2
		})
		rs, err := tk1.Session().Execute(ctx, "select c from point_get where b = 1")
		require.NoError(t, err)
		result := tk1.ResultSetToResultWithCtx(ctx, rs[0], "execute sql fail")
		result.Check(testkit.Rows("1"))
	}()

	<-updateWaitCh // Wait `POINT GET` first time `get`
	require.NoError(t, failpoint.Disable(step1))
	tk2.MustExec("update point_get set b = 2, c = 2 where a = 1")
	require.NoError(t, failpoint.Disable(step2))
}

func TestBatchPointGetRepeatableRead(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec(`create table batch_point_get (a int, b int, c int, unique key k_b(a, b, c))`)
	tk1.MustExec("insert into batch_point_get values (1, 1, 1), (2, 3, 4), (3, 4, 5)")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	var (
		step1 = "github.com/pingcap/tidb/pkg/executor/batchPointGetRepeatableReadTest-step1"
		step2 = "github.com/pingcap/tidb/pkg/executor/batchPointGetRepeatableReadTest-step2"
	)

	require.NoError(t, failpoint.Enable(step1, "return"))
	require.NoError(t, failpoint.Enable(step2, "pause"))

	updateWaitCh := make(chan struct{})
	go func() {
		ctx := context.WithValue(context.Background(), "batchPointGetRepeatableReadTest", updateWaitCh)
		ctx = failpoint.WithHook(ctx, func(ctx context.Context, fpname string) bool {
			return fpname == step1 || fpname == step2
		})
		rs, err := tk1.Session().Execute(ctx, "select c from batch_point_get where (a, b, c) in ((1, 1, 1))")
		require.NoError(t, err)
		result := tk1.ResultSetToResultWithCtx(ctx, rs[0], "execute sql fail")
		result.Check(testkit.Rows("1"))
	}()

	<-updateWaitCh // Wait `POINT GET` first time `get`
	require.NoError(t, failpoint.Disable(step1))
	tk2.MustExec("update batch_point_get set b = 2, c = 2 where a = 1")
	require.NoError(t, failpoint.Disable(step2))
}

func TestSplitRegionTimeout(t *testing.T) {
	store := testkit.CreateMockStore(t)

	require.NoError(t, failpoint.Enable("tikvclient/injectLiveness", `return("reachable")`))
	defer func() {
		require.NoError(t, failpoint.Disable("tikvclient/injectLiveness"))
	}()

	require.NoError(t, failpoint.Enable("tikvclient/mockSplitRegionTimeout", `return(true)`))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(100),b int, index idx1(b,a))")
	tk.MustExec(`split table t index idx1 by (10000,"abcd"),(10000000);`)
	tk.MustExec(`set @@tidb_wait_split_region_timeout=1`)
	// result 0 0 means split 0 region and 0 region finish scatter regions before timeout.
	tk.MustQuery(`split table t between (0) and (10000) regions 10`).Check(testkit.Rows("0 0"))
	require.NoError(t, failpoint.Disable("tikvclient/mockSplitRegionTimeout"))

	// Test scatter regions timeout.
	require.NoError(t, failpoint.Enable("tikvclient/mockScatterRegionTimeout", `return(true)`))
	tk.MustQuery(`split table t between (0) and (10000) regions 10`).Check(testkit.Rows("10 1"))
	require.NoError(t, failpoint.Disable("tikvclient/mockScatterRegionTimeout"))

	// Test pre-split with timeout.
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_scatter_region=1;")
	require.NoError(t, failpoint.Enable("tikvclient/mockScatterRegionTimeout", `return(true)`))
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	start := time.Now()
	tk.MustExec("create table t (a int, b int) partition by hash(a) partitions 5;")
	require.Less(t, time.Since(start).Seconds(), 10.0)
	require.NoError(t, failpoint.Disable("tikvclient/mockScatterRegionTimeout"))
}

func TestTSOFail(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a int)`)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/mockGetTSFail", "return"))
	ctx := failpoint.WithHook(context.Background(), func(ctx context.Context, fpname string) bool {
		return fpname == "github.com/pingcap/tidb/pkg/session/mockGetTSFail"
	})
	_, err := tk.Session().Execute(ctx, `select * from t`)
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/mockGetTSFail"))
}

func TestKillTableReader(t *testing.T) {
	var retry = "tikvclient/mockRetrySendReqToRegion"
	defer func() {
		require.NoError(t, failpoint.Disable(retry))
	}()
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t values (1),(2),(3)")
	tk.MustExec("set @@tidb_distsql_scan_concurrency=1")
	tk.Session().GetSessionVars().SQLKiller.Reset()
	require.NoError(t, failpoint.Enable(retry, `return(true)`))
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(300 * time.Millisecond)
		tk.Session().GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
	}()
	err := tk.QueryToErr("select * from t")
	require.Error(t, err)
	require.Equal(t, int(exeerrors.ErrQueryInterrupted.Code()), int(terror.ToSQLError(errors.Cause(err).(*terror.Error)).Code))
	wg.Wait()
}

func TestCollectCopRuntimeStats(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t1 (a int, b int)")
	time.Sleep(1 * time.Second)
	tk.MustExec("set tidb_enable_collect_execution_info=1;")
	require.NoError(t, failpoint.Enable("tikvclient/tikvStoreRespResult", `return(true)`))
	rows := tk.MustQuery("explain analyze select * from t1").Rows()
	require.Len(t, rows, 2)
	explain := fmt.Sprintf("%v", rows[0])
	require.Regexp(t, ".*num_rpc:.*, .*regionMiss:.*", explain)
	require.NoError(t, failpoint.Disable("tikvclient/tikvStoreRespResult"))
}

func TestCoprocessorOOMTiCase(t *testing.T) {
	t.Skip("skip")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set @@tidb_wait_split_region_finish=1`)
	// create table for non keep-order case
	tk.MustExec("drop table if exists t5")
	tk.MustExec("create table t5(id int)")
	tk.MustQuery(`split table t5 between (0) and (10000) regions 10`).Check(testkit.Rows("9 1"))
	// create table for keep-order case
	tk.MustExec("drop table if exists t6")
	tk.MustExec("create table t6(id int, index(id))")
	tk.MustQuery(`split table t6 between (0) and (10000) regions 10`).Check(testkit.Rows("10 1"))
	tk.MustQuery("split table t6 INDEX id between (0) and (10000) regions 10;").Check(testkit.Rows("10 1"))
	count := 10
	for i := 0; i < count; i++ {
		tk.MustExec(fmt.Sprintf("insert into t5 (id) values (%v)", i))
		tk.MustExec(fmt.Sprintf("insert into t6 (id) values (%v)", i))
	}
	defer tk.MustExec("SET GLOBAL tidb_mem_oom_action = DEFAULT")
	tk.MustExec("SET GLOBAL tidb_mem_oom_action='LOG'")
	testcases := []struct {
		name string
		sql  string
	}{
		{
			name: "keep Order",
			sql:  "select id from t6 order by id",
		},
		{
			name: "non keep Order",
			sql:  "select id from t5",
		},
	}

	f := func() {
		for _, testcase := range testcases {
			t.Log(testcase.name)
			// larger than one copResponse, smaller than 2 copResponse
			quota := 2*copr.MockResponseSizeForTest - 100
			se, err := session.CreateSession4Test(store)
			require.NoError(t, err)
			tk.SetSession(se)
			tk.MustExec("use test")
			tk.MustExec(fmt.Sprintf("set @@tidb_mem_quota_query=%v;", quota))
			var expect []string
			for i := 0; i < count; i++ {
				expect = append(expect, fmt.Sprintf("%v", i))
			}
			tk.MustQuery(testcase.sql).Sort().Check(testkit.Rows(expect...))
			// assert oom action worked by max consumed > memory quota
			require.Greater(t, tk.Session().GetSessionVars().StmtCtx.MemTracker.MaxConsumed(), int64(quota))
			se.Close()
		}
	}

	// ticase-4169, trigger oom action twice after workers consuming all the data
	err := failpoint.Enable("github.com/pingcap/tidb/pkg/store/copr/ticase-4169", `return(true)`)
	require.NoError(t, err)
	f()
	err = failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/ticase-4169")
	require.NoError(t, err)
	/*
		// ticase-4170, trigger oom action twice after iterator receiving all the data.
		err = failpoint.Enable("github.com/pingcap/tidb/pkg/store/copr/ticase-4170", `return(true)`)
		require.NoError(t, err)
		f()
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/ticase-4170")
		require.NoError(t, err)
		// ticase-4171, trigger oom before reading or consuming any data
		err = failpoint.Enable("github.com/pingcap/tidb/pkg/store/copr/ticase-4171", `return(true)`)
		require.NoError(t, err)
		f()
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/ticase-4171")
		require.NoError(t, err)

	*/
}

func TestIssue21441(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/union/issue21441", `return`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/union/issue21441"))
	}()

	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec(`insert into t values(1),(2),(3)`)
	tk.Session().GetSessionVars().InitChunkSize = 1
	tk.Session().GetSessionVars().MaxChunkSize = 1
	sql := `
select a from t union all
select a from t union all
select a from t union all
select a from t union all
select a from t union all
select a from t union all
select a from t union all
select a from t`
	tk.MustQuery(sql).Sort().Check(testkit.Rows(
		"1", "1", "1", "1", "1", "1", "1", "1",
		"2", "2", "2", "2", "2", "2", "2", "2",
		"3", "3", "3", "3", "3", "3", "3", "3",
	))

	tk.MustQuery("select a from (" + sql + ") t order by a limit 4").Check(testkit.Rows("1", "1", "1", "1"))
	tk.MustQuery("select a from (" + sql + ") t order by a limit 7, 4").Check(testkit.Rows("1", "2", "2", "2"))

	tk.MustExec("set @@tidb_executor_concurrency = 2")
	require.Equal(t, 2, tk.Session().GetSessionVars().UnionConcurrency())
	tk.MustQuery("select a from (" + sql + ") t order by a limit 4").Check(testkit.Rows("1", "1", "1", "1"))
	tk.MustQuery("select a from (" + sql + ") t order by a limit 7, 4").Check(testkit.Rows("1", "2", "2", "2"))
}

func TestTxnWriteThroughputSLI(t *testing.T) {
	store := testkit.CreateMockStore(t)

	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int key, b int)")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/sli/CheckTxnWriteThroughput", "return(true)"))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/util/sli/CheckTxnWriteThroughput")
		require.NoError(t, err)
	}()

	mustExec := func(sql string) {
		tk.MustExec(sql)
		tk.Session().GetTxnWriteThroughputSLI().FinishExecuteStmt(time.Second, tk.Session().AffectedRows(), tk.Session().GetSessionVars().InTxn())
	}
	errExec := func(sql string) {
		err := tk.ExecToErr(sql)
		require.Error(t, err)
		tk.Session().GetTxnWriteThroughputSLI().FinishExecuteStmt(time.Second, tk.Session().AffectedRows(), tk.Session().GetSessionVars().InTxn())
	}

	// Test insert in small txn
	mustExec("insert into t values (1,3),(2,4)")
	writeSLI := tk.Session().GetTxnWriteThroughputSLI()
	require.False(t, writeSLI.IsInvalid())
	require.True(t, writeSLI.IsSmallTxn())
	require.Equal(t, "invalid: false, affectRow: 2, writeSize: 58, readKeys: 0, writeKeys: 2, writeTime: 1s", tk.Session().GetTxnWriteThroughputSLI().String())
	tk.Session().GetTxnWriteThroughputSLI().Reset()

	// Test insert ... select ... from
	mustExec("insert into t select b, a from t")
	require.True(t, writeSLI.IsInvalid())
	require.True(t, writeSLI.IsSmallTxn())
	require.Equal(t, "invalid: true, affectRow: 2, writeSize: 58, readKeys: 0, writeKeys: 2, writeTime: 1s", tk.Session().GetTxnWriteThroughputSLI().String())
	tk.Session().GetTxnWriteThroughputSLI().Reset()

	// Test for delete
	mustExec("delete from t")
	require.Equal(t, "invalid: false, affectRow: 4, writeSize: 76, readKeys: 0, writeKeys: 4, writeTime: 1s", tk.Session().GetTxnWriteThroughputSLI().String())
	tk.Session().GetTxnWriteThroughputSLI().Reset()

	// Test insert not in small txn
	mustExec("begin")
	for i := 0; i < 20; i++ {
		mustExec(fmt.Sprintf("insert into t values (%v,%v)", i, i))
		require.True(t, writeSLI.IsSmallTxn())
	}
	// The statement which affect rows is 0 shouldn't record into time.
	mustExec("select count(*) from t")
	mustExec("select * from t")
	mustExec("insert into t values (20,20)")
	require.False(t, writeSLI.IsSmallTxn())
	mustExec("commit")
	require.False(t, writeSLI.IsInvalid())
	require.Equal(t, "invalid: false, affectRow: 21, writeSize: 609, readKeys: 0, writeKeys: 21, writeTime: 22s", tk.Session().GetTxnWriteThroughputSLI().String())
	tk.Session().GetTxnWriteThroughputSLI().Reset()

	// Test invalid when transaction has replace ... select ... from ... statement.
	mustExec("delete from t")
	tk.Session().GetTxnWriteThroughputSLI().Reset()
	mustExec("begin")
	mustExec("insert into t values (1,3),(2,4)")
	mustExec("replace into t select b, a from t")
	mustExec("commit")
	require.True(t, writeSLI.IsInvalid())
	require.Equal(t, "invalid: true, affectRow: 4, writeSize: 116, readKeys: 0, writeKeys: 4, writeTime: 3s", tk.Session().GetTxnWriteThroughputSLI().String())
	tk.Session().GetTxnWriteThroughputSLI().Reset()

	// Test clean last failed transaction information.
	err := failpoint.Disable("github.com/pingcap/tidb/pkg/util/sli/CheckTxnWriteThroughput")
	require.NoError(t, err)
	mustExec("begin")
	mustExec("insert into t values (1,3),(2,4)")
	errExec("commit")
	require.Equal(t, "invalid: false, affectRow: 0, writeSize: 0, readKeys: 0, writeKeys: 0, writeTime: 0s", tk.Session().GetTxnWriteThroughputSLI().String())

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/sli/CheckTxnWriteThroughput", "return(true)"))
	mustExec("begin")
	mustExec("insert into t values (5, 6)")
	mustExec("commit")
	require.Equal(t, "invalid: false, affectRow: 1, writeSize: 29, readKeys: 0, writeKeys: 1, writeTime: 2s", tk.Session().GetTxnWriteThroughputSLI().String())

	// Test for reset
	tk.Session().GetTxnWriteThroughputSLI().Reset()
	require.Equal(t, "invalid: false, affectRow: 0, writeSize: 0, readKeys: 0, writeKeys: 0, writeTime: 0s", tk.Session().GetTxnWriteThroughputSLI().String())
}

func TestDeadlocksTable(t *testing.T) {
	deadlockhistory.GlobalDeadlockHistory.Clear()
	deadlockhistory.GlobalDeadlockHistory.Resize(10)

	occurTime := time.Date(2021, 5, 10, 1, 2, 3, 456789000, time.Local)
	rec := &deadlockhistory.DeadlockRecord{
		OccurTime:   occurTime,
		IsRetryable: false,
		WaitChain: []deadlockhistory.WaitChainItem{
			{
				TryLockTxn:     101,
				SQLDigest:      "aabbccdd",
				Key:            []byte("k1"),
				AllSQLDigests:  nil,
				TxnHoldingLock: 102,
			},
			{
				TryLockTxn:     102,
				SQLDigest:      "ddccbbaa",
				Key:            []byte("k2"),
				AllSQLDigests:  []string{"sql1"},
				TxnHoldingLock: 101,
			},
		},
	}
	deadlockhistory.GlobalDeadlockHistory.Push(rec)

	occurTime2 := time.Date(2022, 6, 11, 2, 3, 4, 987654000, time.Local)
	rec2 := &deadlockhistory.DeadlockRecord{
		OccurTime:   occurTime2,
		IsRetryable: true,
		WaitChain: []deadlockhistory.WaitChainItem{
			{
				TryLockTxn:     201,
				AllSQLDigests:  []string{},
				TxnHoldingLock: 202,
			},
			{
				TryLockTxn:     202,
				AllSQLDigests:  []string{"sql1", "sql2, sql3"},
				TxnHoldingLock: 203,
			},
			{
				TryLockTxn:     203,
				TxnHoldingLock: 201,
			},
		},
	}
	deadlockhistory.GlobalDeadlockHistory.Push(rec2)

	// `Push` sets the record's ID, and ID in a single DeadlockHistory is monotonically increasing. We must get it here
	// to know what it is.
	id1 := strconv.FormatUint(rec.ID, 10)
	id2 := strconv.FormatUint(rec2.ID, 10)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/expression/sqlDigestRetrieverSkipRetrieveGlobal", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/expression/sqlDigestRetrieverSkipRetrieveGlobal"))
	}()

	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select * from information_schema.deadlocks").Check(
		testkit.RowsWithSep("/",
			id1+"/2021-05-10 01:02:03.456789/0/101/aabbccdd/<nil>/6B31/<nil>/102",
			id1+"/2021-05-10 01:02:03.456789/0/102/ddccbbaa/<nil>/6B32/<nil>/101",
			id2+"/2022-06-11 02:03:04.987654/1/201/<nil>/<nil>/<nil>/<nil>/202",
			id2+"/2022-06-11 02:03:04.987654/1/202/<nil>/<nil>/<nil>/<nil>/203",
			id2+"/2022-06-11 02:03:04.987654/1/203/<nil>/<nil>/<nil>/<nil>/201",
		))
}

func TestTiKVClientReadTimeout(t *testing.T) {
	if *testkit.WithTiKV != "" {
		t.Skip("skip test since it's only work for unistore")
	}
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b int)")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/unistoreRPCDeadlineExceeded", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/unistoreRPCDeadlineExceeded"))
	}()
	// Test for point_get request
	rows := tk.MustQuery("explain analyze select /*+ set_var(tikv_client_read_timeout=1) */ * from t where a = 1").Rows()
	require.Len(t, rows, 1)
	explain := fmt.Sprintf("%v", rows[0])
	require.Regexp(t, ".*Point_Get.* Get:{num_rpc:2, total_time:.*", explain)

	// Test for batch_point_get request
	rows = tk.MustQuery("explain analyze select /*+ set_var(tikv_client_read_timeout=1) */ * from t where a in (1,2)").Rows()
	require.Len(t, rows, 1)
	explain = fmt.Sprintf("%v", rows[0])
	require.Regexp(t, ".*Batch_Point_Get.* BatchGet:{num_rpc:2, total_time:.*", explain)

	// Test for cop request
	rows = tk.MustQuery("explain analyze select /*+ set_var(tikv_client_read_timeout=1) */ * from t where b > 1").Rows()
	require.Len(t, rows, 3)
	explain = fmt.Sprintf("%v", rows[0])
	require.Regexp(t, ".*TableReader.* root  time:.*, loops:.* cop_task: {num: 1, .*num_rpc:2.*", explain)

	// Test for stale read.
	tk.MustExec("set @a=now(6);")
	tk.MustExec("set @@tidb_replica_read='closest-replicas';")
	rows = tk.MustQuery("explain analyze select /*+ set_var(tikv_client_read_timeout=1) */ * from t as of timestamp(@a) where b > 1").Rows()
	require.Len(t, rows, 3)
	explain = fmt.Sprintf("%v", rows[0])
	require.Regexp(t, ".*TableReader.* root  time:.*, loops:.* cop_task: {num: 1, .*num_rpc:2.*", explain)

	// Test for tikv_client_read_timeout session variable.
	tk.MustExec("set @@tikv_client_read_timeout=1;")
	// Test for point_get request
	rows = tk.MustQuery("explain analyze select * from t where a = 1").Rows()
	require.Len(t, rows, 1)
	explain = fmt.Sprintf("%v", rows[0])
	require.Regexp(t, ".*Point_Get.* Get:{num_rpc:2, total_time:.*", explain)

	// Test for batch_point_get request
	rows = tk.MustQuery("explain analyze select * from t where a in (1,2)").Rows()
	require.Len(t, rows, 1)
	explain = fmt.Sprintf("%v", rows[0])
	require.Regexp(t, ".*Batch_Point_Get.* BatchGet:{num_rpc:2, total_time:.*", explain)

	// Test for cop request
	rows = tk.MustQuery("explain analyze select * from t where b > 1").Rows()
	require.Len(t, rows, 3)
	explain = fmt.Sprintf("%v", rows[0])
	require.Regexp(t, ".*TableReader.* root  time:.*, loops:.* cop_task: {num: 1, .*num_rpc:2.*", explain)

	// Test for stale read.
	tk.MustExec("set @a=now(6);")
	tk.MustExec("set @@tidb_replica_read='closest-replicas';")
	rows = tk.MustQuery("explain analyze select * from t as of timestamp(@a) where b > 1").Rows()
	require.Len(t, rows, 3)
	explain = fmt.Sprintf("%v", rows[0])
	require.Regexp(t, ".*TableReader.* root  time:.*, loops:.* cop_task: {num: 1, .*num_rpc:2.*", explain)
}

func TestGetMvccByEncodedKeyRegionError(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	h := helper.NewHelper(store.(helper.Storage))
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	schemaVersion := tk.Session().GetDomainInfoSchema().SchemaMetaVersion()
	key := m.EncodeSchemaDiffKey(schemaVersion)

	resp, err := h.GetMvccByEncodedKey(key)
	require.NoError(t, err)
	require.NotNil(t, resp.Info)
	require.Equal(t, 1, len(resp.Info.Writes))
	require.Less(t, uint64(0), resp.Info.Writes[0].CommitTs)
	commitTs := resp.Info.Writes[0].CommitTs

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/epochNotMatch", "2*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/epochNotMatch"))
	}()
	resp, err = h.GetMvccByEncodedKey(key)
	require.NoError(t, err)
	require.NotNil(t, resp.Info)
	require.Equal(t, 1, len(resp.Info.Writes))
	require.Equal(t, commitTs, resp.Info.Writes[0].CommitTs)
}

func TestShuffleExit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(i int, j int, k int);")
	tk.MustExec("insert into t1 VALUES (1,1,1),(2,2,2),(3,3,3),(4,4,4);")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/shuffleError", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/shuffleError"))
	}()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/shuffleExecFetchDataAndSplit", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/shuffleExecFetchDataAndSplit"))
	}()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/shuffleWorkerRun", "panic(\"ShufflePanic\")"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/shuffleWorkerRun"))
	}()
	err := tk.QueryToErr("SELECT SUM(i) OVER W FROM t1 WINDOW w AS (PARTITION BY j ORDER BY i) ORDER BY 1+SUM(i) OVER w;")
	require.ErrorContains(t, err, "ShuffleExec.Next error")
}

func TestHandleForeignKeyCascadePanic(t *testing.T) {
	// Test no goroutine leak.
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (id int key, a int, index (a));")
	tk.MustExec("create table t2 (id int key, a int, index (a), constraint fk_1 foreign key (a) references t1(a));")
	tk.MustExec("alter table t2 drop foreign key fk_1;")
	tk.MustExec("alter table t2 add constraint fk_1 foreign key (a) references t1(a) on delete set null;")
	tk.MustExec("replace into t1 values (1, 1);")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/handleForeignKeyCascadeError", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/handleForeignKeyCascadeError"))
	}()
	err := tk.ExecToErr("replace into t1 values (1, 2);")
	require.ErrorContains(t, err, "handleForeignKeyCascadeError")
}

func TestBuildProjectionForIndexJoinPanic(t *testing.T) {
	// Test no goroutine leak.
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1(a int, b varchar(8));")
	tk.MustExec("insert into t1 values(1,'1');")
	tk.MustExec("create table t2(a int , b varchar(8) GENERATED ALWAYS AS (c) VIRTUAL, c varchar(8), PRIMARY KEY (a));")
	tk.MustExec("insert into t2(a) values(1);")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/buildProjectionForIndexJoinPanic", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/buildProjectionForIndexJoinPanic"))
	}()
	err := tk.QueryToErr("select /*+ tidb_inlj(t2) */ t2.b, t1.b from t1 join t2 ON t2.a=t1.a;")
	require.ErrorContains(t, err, "buildProjectionForIndexJoinPanic")
}
