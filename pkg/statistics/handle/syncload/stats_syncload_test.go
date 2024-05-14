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

package syncload_test

import (
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/syncload"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/stretchr/testify/require"
)

func TestSyncLoadSkipUnAnalyzedItems(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(a int)")
	tk.MustExec("create table t1(a int)")
	h := dom.StatsHandle()
	h.SetLease(1)

	// no item would be loaded
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/statistics/handle/syncload/assertSyncLoadItems", `return(0)`))
	tk.MustQuery("trace plan select * from t where a > 10")
	failpoint.Disable("github.com/pingcap/tidb/pkg/statistics/handle/syncload/assertSyncLoadItems")
	tk.MustExec("analyze table t1")
	// one column would be loaded
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/statistics/handle/syncload/assertSyncLoadItems", `return(1)`))
	tk.MustQuery("trace plan select * from t1 where a > 10")
	failpoint.Disable("github.com/pingcap/tidb/pkg/statistics/handle/syncload/assertSyncLoadItems")
}

func TestConcurrentLoadHist(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
	testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3)")

	oriLease := dom.StatsHandle().Lease()
	dom.StatsHandle().SetLease(1)
	defer func() {
		dom.StatsHandle().SetLease(oriLease)
	}()
	testKit.MustExec("analyze table t")

	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := dom.StatsHandle()
	stat := h.GetTableStats(tableInfo)
	col, ok := stat.Columns[tableInfo.Columns[0].ID]
	require.True(t, !ok || col.Histogram.Len()+col.TopN.Num() == 0)
	col, ok = stat.Columns[tableInfo.Columns[2].ID]
	require.True(t, !ok || col.Histogram.Len()+col.TopN.Num() == 0)
	stmtCtx := stmtctx.NewStmtCtx()
	neededColumns := make([]model.StatsLoadItem, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		neededColumns = append(neededColumns, model.StatsLoadItem{TableItemID: model.TableItemID{TableID: tableInfo.ID, ID: col.ID, IsIndex: false}, FullLoad: true})
	}
	timeout := time.Nanosecond * mathutil.MaxInt
	h.SendLoadRequests(stmtCtx, neededColumns, timeout)
	rs := h.SyncWaitStatsLoad(stmtCtx)
	require.Nil(t, rs)
	stat = h.GetTableStats(tableInfo)
	hg := stat.Columns[tableInfo.Columns[2].ID].Histogram
	topn := stat.Columns[tableInfo.Columns[2].ID].TopN
	require.Greater(t, hg.Len()+topn.Num(), 0)
}

func TestConcurrentLoadHistTimeout(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("set @@session.tidb_stats_load_sync_wait =60000")
	testKit.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
	testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3)")

	oriLease := dom.StatsHandle().Lease()
	dom.StatsHandle().SetLease(1)
	defer func() {
		dom.StatsHandle().SetLease(oriLease)
	}()
	testKit.MustExec("analyze table t")

	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := dom.StatsHandle()
	stat := h.GetTableStats(tableInfo)
	// TODO: They may need to be empty. Depending on how we operate newly analyzed tables.
	// require.Nil(t, stat.Columns[tableInfo.Columns[0].ID])
	// require.Nil(t, stat.Columns[tableInfo.Columns[2].ID])
	hg := stat.Columns[tableInfo.Columns[0].ID].Histogram
	topn := stat.Columns[tableInfo.Columns[0].ID].TopN
	require.Equal(t, 0, hg.Len()+topn.Num())
	hg = stat.Columns[tableInfo.Columns[2].ID].Histogram
	topn = stat.Columns[tableInfo.Columns[2].ID].TopN
	require.Equal(t, 0, hg.Len()+topn.Num())
	stmtCtx := stmtctx.NewStmtCtx()
	neededColumns := make([]model.StatsLoadItem, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		neededColumns = append(neededColumns, model.StatsLoadItem{TableItemID: model.TableItemID{TableID: tableInfo.ID, ID: col.ID, IsIndex: false}, FullLoad: true})
	}
	h.SendLoadRequests(stmtCtx, neededColumns, 0) // set timeout to 0 so task will go to timeout channel
	rs := h.SyncWaitStatsLoad(stmtCtx)
	require.Error(t, rs)
	stat = h.GetTableStats(tableInfo)
	// require.Nil(t, stat.Columns[tableInfo.Columns[2].ID])
	require.NotNil(t, stat.Columns[tableInfo.Columns[2].ID])
	require.Equal(t, 0, hg.Len()+topn.Num())
}

func TestConcurrentLoadHistWithPanicAndFail(t *testing.T) {
	originConfig := config.GetGlobalConfig()
	newConfig := config.NewConfig()
	newConfig.Performance.StatsLoadConcurrency = -1 // no worker to consume channel
	config.StoreGlobalConfig(newConfig)
	defer config.StoreGlobalConfig(originConfig)
	store, dom := testkit.CreateMockStoreAndDomain(t)

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
	testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3)")

	oriLease := dom.StatsHandle().Lease()
	dom.StatsHandle().SetLease(1)
	defer func() {
		dom.StatsHandle().SetLease(oriLease)
	}()
	testKit.MustExec("analyze table t")

	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := dom.StatsHandle()

	neededColumns := make([]model.StatsLoadItem, 1)
	neededColumns[0] = model.StatsLoadItem{TableItemID: model.TableItemID{TableID: tableInfo.ID, ID: tableInfo.Columns[2].ID, IsIndex: false}, FullLoad: true}
	timeout := time.Nanosecond * mathutil.MaxInt

	failpoints := []struct {
		failPath string
		inTerms  string
	}{
		{
			failPath: "github.com/pingcap/tidb/pkg/statistics/handle/syncload/mockReadStatsForOnePanic",
			inTerms:  "panic",
		},
		{
			failPath: "github.com/pingcap/tidb/pkg/statistics/handle/syncload/mockReadStatsForOneFail",
			inTerms:  "return(true)",
		},
	}

	for _, fp := range failpoints {
		// clear statsCache
		h.Clear()
		require.NoError(t, dom.StatsHandle().Update(is))

		// no stats at beginning
		stat := h.GetTableStats(tableInfo)
		c, ok := stat.Columns[tableInfo.Columns[2].ID]
		require.True(t, !ok || (c.Histogram.Len()+c.TopN.Num() == 0))

		stmtCtx1 := stmtctx.NewStmtCtx()
		h.SendLoadRequests(stmtCtx1, neededColumns, timeout)
		stmtCtx2 := stmtctx.NewStmtCtx()
		h.SendLoadRequests(stmtCtx2, neededColumns, timeout)

		exitCh := make(chan struct{})
		require.NoError(t, failpoint.Enable(fp.failPath, fp.inTerms))

		task1, err1 := h.HandleOneTask(testKit.Session().(sessionctx.Context), nil, exitCh)
		require.Error(t, err1)
		require.NotNil(t, task1)
		for _, resultCh := range stmtCtx1.StatsLoad.ResultCh {
			select {
			case <-resultCh:
				t.Logf("stmtCtx1.ResultCh should not get anything")
				t.FailNow()
			default:
			}
		}
		for _, resultCh := range stmtCtx2.StatsLoad.ResultCh {
			select {
			case <-resultCh:
				t.Logf("stmtCtx1.ResultCh should not get anything")
				t.FailNow()
			default:
			}
		}
		select {
		case <-task1.ResultCh:
			t.Logf("task1.ResultCh should not get anything")
			t.FailNow()
		default:
		}

		require.NoError(t, failpoint.Disable(fp.failPath))
		task3, err3 := h.HandleOneTask(testKit.Session().(sessionctx.Context), task1, exitCh)
		require.NoError(t, err3)
		require.Nil(t, task3)
		for _, resultCh := range stmtCtx1.StatsLoad.ResultCh {
			rs1, ok1 := <-resultCh
			require.True(t, rs1.Shared)
			require.True(t, ok1)
			require.Equal(t, neededColumns[0].TableItemID, rs1.Val.(stmtctx.StatsLoadResult).Item)
		}
		for _, resultCh := range stmtCtx2.StatsLoad.ResultCh {
			rs1, ok1 := <-resultCh
			require.True(t, rs1.Shared)
			require.True(t, ok1)
			require.Equal(t, neededColumns[0].TableItemID, rs1.Val.(stmtctx.StatsLoadResult).Item)
		}

		stat = h.GetTableStats(tableInfo)
		hg := stat.Columns[tableInfo.Columns[2].ID].Histogram
		topn := stat.Columns[tableInfo.Columns[2].ID].TopN
		require.Greater(t, hg.Len()+topn.Num(), 0)
	}
}

func TestRetry(t *testing.T) {
	originConfig := config.GetGlobalConfig()
	newConfig := config.NewConfig()
	newConfig.Performance.StatsLoadConcurrency = -1 // no worker to consume channel
	config.StoreGlobalConfig(newConfig)
	defer config.StoreGlobalConfig(originConfig)
	store, dom := testkit.CreateMockStoreAndDomain(t)

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
	testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3)")

	oriLease := dom.StatsHandle().Lease()
	dom.StatsHandle().SetLease(1)
	defer func() {
		dom.StatsHandle().SetLease(oriLease)
	}()
	testKit.MustExec("analyze table t")

	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()

	h := dom.StatsHandle()

	neededColumns := make([]model.StatsLoadItem, 1)
	neededColumns[0] = model.StatsLoadItem{TableItemID: model.TableItemID{TableID: tableInfo.ID, ID: tableInfo.Columns[2].ID, IsIndex: false}, FullLoad: true}
	timeout := time.Nanosecond * mathutil.MaxInt

	// clear statsCache
	h.Clear()
	require.NoError(t, dom.StatsHandle().Update(is))

	// no stats at beginning
	stat := h.GetTableStats(tableInfo)
	c, ok := stat.Columns[tableInfo.Columns[2].ID]
	require.True(t, !ok || (c.Histogram.Len()+c.TopN.Num() == 0))

	stmtCtx1 := stmtctx.NewStmtCtx()
	h.SendLoadRequests(stmtCtx1, neededColumns, timeout)
	stmtCtx2 := stmtctx.NewStmtCtx()
	h.SendLoadRequests(stmtCtx2, neededColumns, timeout)

	exitCh := make(chan struct{})
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/statistics/handle/syncload/mockReadStatsForOneFail", "return(true)"))
	var (
		task1 *types.NeededItemTask
		err1  error
	)

	for i := 0; i < syncload.RetryCount; i++ {
		task1, err1 = h.HandleOneTask(testKit.Session().(sessionctx.Context), task1, exitCh)
		require.Error(t, err1)
		require.NotNil(t, task1)
		select {
		case <-task1.ResultCh:
			t.Logf("task1.ResultCh should not get nothing")
			t.FailNow()
		default:
		}
	}
	result, err1 := h.HandleOneTask(testKit.Session().(sessionctx.Context), task1, exitCh)
	require.NoError(t, err1)
	require.Nil(t, result)
	for _, resultCh := range stmtCtx1.StatsLoad.ResultCh {
		rs1, ok1 := <-resultCh
		require.True(t, rs1.Shared)
		require.True(t, ok1)
		require.Error(t, rs1.Val.(stmtctx.StatsLoadResult).Error)
	}
	task1.Retry = 0
	for i := 0; i < syncload.RetryCount*5; i++ {
		task1, err1 = h.HandleOneTask(testKit.Session().(sessionctx.Context), task1, exitCh)
		require.Error(t, err1)
		require.NotNil(t, task1)
		select {
		case <-task1.ResultCh:
			t.Logf("task1.ResultCh should not get nothing")
			t.FailNow()
		default:
		}
		task1.Retry = 0
	}
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/statistics/handle/syncload/mockReadStatsForOneFail"))
}
