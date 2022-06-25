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

package handle_test

import (
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestConcurrentLoadHist(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

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
	hg := stat.Columns[tableInfo.Columns[0].ID].Histogram
	topn := stat.Columns[tableInfo.Columns[0].ID].TopN
	require.Greater(t, hg.Len()+topn.Num(), 0)
	hg = stat.Columns[tableInfo.Columns[2].ID].Histogram
	topn = stat.Columns[tableInfo.Columns[2].ID].TopN
	require.Equal(t, 0, hg.Len()+topn.Num())
	stmtCtx := &stmtctx.StatementContext{}
	neededColumns := make([]model.TableColumnID, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		neededColumns = append(neededColumns, model.TableColumnID{TableID: tableInfo.ID, ColumnID: col.ID})
	}
	timeout := time.Nanosecond * mathutil.MaxInt
	h.SendLoadRequests(stmtCtx, neededColumns, timeout)
	rs := h.SyncWaitStatsLoad(stmtCtx)
	require.True(t, rs)
	stat = h.GetTableStats(tableInfo)
	hg = stat.Columns[tableInfo.Columns[2].ID].Histogram
	topn = stat.Columns[tableInfo.Columns[2].ID].TopN
	require.Greater(t, hg.Len()+topn.Num(), 0)
}

func TestConcurrentLoadHistTimeout(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

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
	hg := stat.Columns[tableInfo.Columns[0].ID].Histogram
	topn := stat.Columns[tableInfo.Columns[0].ID].TopN
	require.Greater(t, hg.Len()+topn.Num(), 0)
	hg = stat.Columns[tableInfo.Columns[2].ID].Histogram
	topn = stat.Columns[tableInfo.Columns[2].ID].TopN
	require.Equal(t, 0, hg.Len()+topn.Num())
	stmtCtx := &stmtctx.StatementContext{}
	neededColumns := make([]model.TableColumnID, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		neededColumns = append(neededColumns, model.TableColumnID{TableID: tableInfo.ID, ColumnID: col.ID})
	}
	h.SendLoadRequests(stmtCtx, neededColumns, 0) // set timeout to 0 so task will go to timeout channel
	rs := h.SyncWaitStatsLoad(stmtCtx)
	require.False(t, rs)
	stat = h.GetTableStats(tableInfo)
	hg = stat.Columns[tableInfo.Columns[2].ID].Histogram
	topn = stat.Columns[tableInfo.Columns[2].ID].TopN
	require.Equal(t, 0, hg.Len()+topn.Num())
	// wait for timeout task to be handled
	oldStat := stat
	for {
		time.Sleep(time.Millisecond * 100)
		stat = h.GetTableStats(tableInfo)
		if stat != oldStat {
			break
		}
	}
	hg = stat.Columns[tableInfo.Columns[2].ID].Histogram
	topn = stat.Columns[tableInfo.Columns[2].ID].TopN
	require.Greater(t, hg.Len()+topn.Num(), 0)
}

func TestConcurrentLoadHistWithPanicAndFail(t *testing.T) {
	originConfig := config.GetGlobalConfig()
	newConfig := config.NewConfig()
	newConfig.Performance.StatsLoadConcurrency = 0 // no worker to consume channel
	config.StoreGlobalConfig(newConfig)
	defer config.StoreGlobalConfig(originConfig)
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

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

	neededColumns := make([]model.TableColumnID, 1)
	neededColumns[0] = model.TableColumnID{TableID: tableInfo.ID, ColumnID: tableInfo.Columns[2].ID}
	timeout := time.Nanosecond * mathutil.MaxInt

	failpoints := []struct {
		failPath string
		inTerms  string
	}{
		{
			failPath: "github.com/pingcap/tidb/statistics/handle/mockFinishWorkingPanic",
			inTerms:  "panic",
		},
		{
			failPath: "github.com/pingcap/tidb/statistics/handle/mockReadStatsForOnePanic",
			inTerms:  "panic",
		},
		{
			failPath: "github.com/pingcap/tidb/statistics/handle/mockReadStatsForOneFail",
			inTerms:  "return(true)",
		},
	}

	for _, fp := range failpoints {
		// clear statsCache
		h.Clear()
		require.NoError(t, dom.StatsHandle().Update(is))

		// no stats at beginning
		stat := h.GetTableStats(tableInfo)
		hg := stat.Columns[tableInfo.Columns[2].ID].Histogram
		topn := stat.Columns[tableInfo.Columns[2].ID].TopN
		require.Equal(t, 0, hg.Len()+topn.Num())

		stmtCtx1 := &stmtctx.StatementContext{}
		h.SendLoadRequests(stmtCtx1, neededColumns, timeout)
		stmtCtx2 := &stmtctx.StatementContext{}
		h.SendLoadRequests(stmtCtx2, neededColumns, timeout)

		readerCtx := &handle.StatsReaderContext{}
		exitCh := make(chan struct{})
		require.NoError(t, failpoint.Enable(fp.failPath, fp.inTerms))

		task1, err1 := h.HandleOneTask(nil, readerCtx, testKit.Session().(sqlexec.RestrictedSQLExecutor), exitCh)
		require.Error(t, err1)
		require.NotNil(t, task1)
		list, ok := h.StatsLoad.WorkingColMap[neededColumns[0]]
		require.True(t, ok)
		require.Len(t, list, 1)
		require.Equal(t, stmtCtx1.StatsLoad.ResultCh, list[0])

		task2, err2 := h.HandleOneTask(nil, readerCtx, testKit.Session().(sqlexec.RestrictedSQLExecutor), exitCh)
		require.Nil(t, err2)
		require.Nil(t, task2)
		list, ok = h.StatsLoad.WorkingColMap[neededColumns[0]]
		require.True(t, ok)
		require.Len(t, list, 2)
		require.Equal(t, stmtCtx2.StatsLoad.ResultCh, list[1])

		require.NoError(t, failpoint.Disable(fp.failPath))
		task3, err3 := h.HandleOneTask(task1, readerCtx, testKit.Session().(sqlexec.RestrictedSQLExecutor), exitCh)
		require.NoError(t, err3)
		require.Nil(t, task3)

		require.Len(t, stmtCtx1.StatsLoad.ResultCh, 1)
		require.Len(t, stmtCtx2.StatsLoad.ResultCh, 1)

		rs1, ok1 := <-stmtCtx1.StatsLoad.ResultCh
		require.True(t, ok1)
		require.Equal(t, neededColumns[0], rs1)
		rs2, ok2 := <-stmtCtx2.StatsLoad.ResultCh
		require.True(t, ok2)
		require.Equal(t, neededColumns[0], rs2)

		stat = h.GetTableStats(tableInfo)
		hg = stat.Columns[tableInfo.Columns[2].ID].Histogram
		topn = stat.Columns[tableInfo.Columns[2].ID].TopN
		require.Greater(t, hg.Len()+topn.Num(), 0)
	}
}
