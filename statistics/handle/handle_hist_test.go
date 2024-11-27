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
	"fmt"
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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/statistics/handle/assertSyncLoadItems", `return(0)`))
	tk.MustQuery("trace plan select * from t where a > 10")
	failpoint.Disable("github.com/pingcap/tidb/statistics/handle/assertSyncLoadItems")
	tk.MustExec("analyze table t1")
	// one column would be loaded
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/statistics/handle/assertSyncLoadItems", `return(1)`))
	tk.MustQuery("trace plan select * from t1 where a > 10")
	failpoint.Disable("github.com/pingcap/tidb/statistics/handle/assertSyncLoadItems")
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
	hg := stat.Columns[tableInfo.Columns[0].ID].Histogram
	topn := stat.Columns[tableInfo.Columns[0].ID].TopN
	require.Greater(t, hg.Len()+topn.Num(), 0)
	hg = stat.Columns[tableInfo.Columns[2].ID].Histogram
	topn = stat.Columns[tableInfo.Columns[2].ID].TopN
	require.Equal(t, 0, hg.Len()+topn.Num())
	stmtCtx := &stmtctx.StatementContext{}
	neededColumns := make([]model.TableItemID, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		neededColumns = append(neededColumns, model.TableItemID{TableID: tableInfo.ID, ID: col.ID, IsIndex: false})
	}
	timeout := time.Nanosecond * mathutil.MaxInt
	h.SendLoadRequests(stmtCtx, neededColumns, timeout)
	rs := h.SyncWaitStatsLoad(stmtCtx)
	require.Nil(t, rs)
	stat = h.GetTableStats(tableInfo)
	hg = stat.Columns[tableInfo.Columns[2].ID].Histogram
	topn = stat.Columns[tableInfo.Columns[2].ID].TopN
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
	hg := stat.Columns[tableInfo.Columns[0].ID].Histogram
	topn := stat.Columns[tableInfo.Columns[0].ID].TopN
	require.Greater(t, hg.Len()+topn.Num(), 0)
	hg = stat.Columns[tableInfo.Columns[2].ID].Histogram
	topn = stat.Columns[tableInfo.Columns[2].ID].TopN
	require.Equal(t, 0, hg.Len()+topn.Num())
	stmtCtx := &stmtctx.StatementContext{}
	neededColumns := make([]model.TableItemID, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		neededColumns = append(neededColumns, model.TableItemID{TableID: tableInfo.ID, ID: col.ID, IsIndex: false})
	}
	h.SendLoadRequests(stmtCtx, neededColumns, 0) // set timeout to 0 so task will go to timeout channel
	rs := h.SyncWaitStatsLoad(stmtCtx)
	require.Error(t, rs)
	stat = h.GetTableStats(tableInfo)
	hg = stat.Columns[tableInfo.Columns[2].ID].Histogram
	topn = stat.Columns[tableInfo.Columns[2].ID].TopN
	require.Equal(t, 0, hg.Len()+topn.Num())
}

func TestConcurrentLoadHistWithPanicAndFail(t *testing.T) {
	originConfig := config.GetGlobalConfig()
	newConfig := config.NewConfig()
	newConfig.Performance.StatsLoadConcurrency = 0 // no worker to consume channel
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

	neededColumns := make([]model.TableItemID, 1)
	neededColumns[0] = model.TableItemID{TableID: tableInfo.ID, ID: tableInfo.Columns[2].ID, IsIndex: false}
	timeout := time.Nanosecond * mathutil.MaxInt

	failpoints := []struct {
		failPath string
		inTerms  string
	}{
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
		task3, err3 := h.HandleOneTask(task1, readerCtx, testKit.Session().(sqlexec.RestrictedSQLExecutor), exitCh)
		require.NoError(t, err3)
		require.Nil(t, task3)
		for _, resultCh := range stmtCtx1.StatsLoad.ResultCh {
			rs1, ok1 := <-resultCh
			require.True(t, rs1.Shared)
			require.True(t, ok1)
			require.Equal(t, neededColumns[0].ID, rs1.Val.(stmtctx.StatsLoadResult).Item.ID)
		}
		for _, resultCh := range stmtCtx2.StatsLoad.ResultCh {
			rs1, ok1 := <-resultCh
			require.True(t, rs1.Shared)
			require.True(t, ok1)
			require.Equal(t, neededColumns[0].ID, rs1.Val.(stmtctx.StatsLoadResult).Item.ID)
		}

		stat = h.GetTableStats(tableInfo)
		hg = stat.Columns[tableInfo.Columns[2].ID].Histogram
		topn = stat.Columns[tableInfo.Columns[2].ID].TopN
		require.Greater(t, hg.Len()+topn.Num(), 0)
	}
}

func TestRetry(t *testing.T) {
	originConfig := config.GetGlobalConfig()
	newConfig := config.NewConfig()
	newConfig.Performance.StatsLoadConcurrency = 0 // no worker to consume channel
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

	neededColumns := make([]model.TableItemID, 1)
	neededColumns[0] = model.TableItemID{TableID: tableInfo.ID, ID: tableInfo.Columns[2].ID, IsIndex: false}
	timeout := time.Nanosecond * mathutil.MaxInt

	// clear statsCache
	h.Clear()
	require.NoError(t, dom.StatsHandle().Update(is))

	// no stats at beginning
	stat := h.GetTableStats(tableInfo)
	c, ok := stat.Columns[tableInfo.Columns[2].ID]
	require.True(t, !ok || (c.Histogram.Len()+c.TopN.Num() == 0))

	stmtCtx1 := &stmtctx.StatementContext{}
	h.SendLoadRequests(stmtCtx1, neededColumns, timeout)
	stmtCtx2 := &stmtctx.StatementContext{}
	h.SendLoadRequests(stmtCtx2, neededColumns, timeout)

	exitCh := make(chan struct{})
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/statistics/handle/mockReadStatsForOneFail", "return(true)"))
	var (
		task1 *handle.NeededItemTask
		err1  error
	)
	readerCtx := &handle.StatsReaderContext{}
	for i := 0; i < handle.RetryCount; i++ {
		task1, err1 = h.HandleOneTask(task1, readerCtx, testKit.Session().(sqlexec.RestrictedSQLExecutor), exitCh)
		require.Error(t, err1)
		require.NotNil(t, task1)
		select {
		case <-task1.ResultCh:
			t.Logf("task1.ResultCh should not get nothing")
			t.FailNow()
		default:
		}
	}
	result, err1 := h.HandleOneTask(task1, readerCtx, testKit.Session().(sqlexec.RestrictedSQLExecutor), exitCh)
	require.NoError(t, err1)
	require.Nil(t, result)
	for _, resultCh := range stmtCtx1.StatsLoad.ResultCh {
		rs1, ok1 := <-resultCh
		require.True(t, rs1.Shared)
		require.True(t, ok1)
		require.Error(t, rs1.Val.(stmtctx.StatsLoadResult).Error)
	}
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/statistics/handle/mockReadStatsForOneFail"))
}

func TestMergeGlobalStatsIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE employees3 (
  emp_id int(11) NOT NULL,
  emp_name varchar(25) NOT NULL,
  salary int(11) NOT NULL,
  dept_id int(11) NOT NULL,
  PRIMARY KEY (emp_id) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY RANGE (emp_id)
(
  PARTITION p0 VALUES LESS THAN (1000),
  PARTITION p1 VALUES LESS THAN (4000),
  PARTITION p2 VALUES LESS THAN (12000),
  PARTITION p3 VALUES LESS THAN (16000),
  PARTITION p4 VALUES LESS THAN (20000),
  PARTITION p5 VALUES LESS THAN (25000),
  PARTITION p6 VALUES LESS THAN (30000),
  PARTITION p7 VALUES LESS THAN (35000),
  PARTITION p8 VALUES LESS THAN (40000),
  PARTITION p9 VALUES LESS THAN (45000),
  PARTITION p10 VALUES LESS THAN (50000),
  PARTITION p11 VALUES LESS THAN (55000),
  PARTITION p12 VALUES LESS THAN (65000),
  PARTITION p13 VALUES LESS THAN (75000),
  PARTITION p14 VALUES LESS THAN (85000),
  PARTITION p15 VALUES LESS THAN (95000),
  PARTITION p16 VALUES LESS THAN (105000),
  PARTITION p17 VALUES LESS THAN (115000),
  PARTITION p18 VALUES LESS THAN (125000),
  PARTITION pmax VALUES LESS THAN (MAXVALUE)
);`)
	tk.MustExec(`
SET cte_max_recursion_depth = 1000000000;
INSERT INTO employees3
WITH RECURSIVE EmployeeGenerator AS (
    SELECT
        101 AS emp_id,
        'Emp00001' AS emp_name,
        FLOOR(RAND() * (150000 - 50000) + 50000) AS salary,
        FLOOR(RAND() * 3 + 1) AS dept_id
    UNION ALL
    SELECT
        emp_id + 1,
        CONCAT('Emp', LPAD(CAST(emp_id - 100 AS CHAR), 5, '0')),
        FLOOR(RAND() * (150000 - 50000) + 50000),
        FLOOR(RAND() * 3 + 1)
    FROM
        EmployeeGenerator
    WHERE
        emp_id < 20100
)
SELECT * FROM EmployeeGenerator;
`)
	tk.MustExec("analyze table employees3")
	for i := 0; i <= 12; i++ {
		tk.MustExec(fmt.Sprintf("analyze table employees3 partition p%d", i))
		tk.MustQuery("show stats_histograms where table_name='employees3' and Column_name='PRIMARY' and Partition_name='global'").CheckContain("19958")
	}
}

func TestSendLoadRequestsWaitTooLong(t *testing.T) {
	originConfig := config.GetGlobalConfig()
	newConfig := config.NewConfig()
	newConfig.Performance.StatsLoadConcurrency = 0 // no worker to consume channel
	newConfig.Performance.StatsLoadQueueSize = 10000
	config.StoreGlobalConfig(newConfig)
	defer config.StoreGlobalConfig(originConfig)
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b,c))")
	tk.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3)")

	oriLease := dom.StatsHandle().Lease()
	dom.StatsHandle().SetLease(1)
	defer func() {
		dom.StatsHandle().SetLease(oriLease)
	}()
	tk.MustExec("analyze table t all columns")
	h := dom.StatsHandle()
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	neededColumns := make([]model.TableItemID, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		neededColumns = append(neededColumns, model.TableItemID{TableID: tableInfo.ID, ID: col.ID, IsIndex: false})
	}
	stmtCtx := &stmtctx.StatementContext{}
	timeout := time.Nanosecond * 100
	require.NoError(t, h.SendLoadRequests(stmtCtx, neededColumns, timeout))
	for _, resultCh := range stmtCtx.StatsLoad.ResultCh {
		rs1 := <-resultCh
		require.Error(t, rs1.Err)
	}
	stmtCtx1 := &stmtctx.StatementContext{}
	require.NoError(t, h.SendLoadRequests(stmtCtx1, neededColumns, timeout))
	for _, resultCh := range stmtCtx1.StatsLoad.ResultCh {
		rs1 := <-resultCh
		require.Error(t, rs1.Err)
	}
}
