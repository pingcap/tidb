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

package executor_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
)

func TestFastAnalyze4GlobalStats(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`create database if not exists test_fast_gstats`)
	tk.MustExec("use test_fast_gstats")
	tk.MustExec("set @@session.tidb_enable_fast_analyze=1")
	tk.MustExec("set @@session.tidb_build_stats_concurrency=1")
	// test fast analyze in dynamic mode
	tk.MustExec("set @@session.tidb_analyze_version = 2;")
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'dynamic';")
	tk.MustExec("drop table if exists test_fast_gstats;")
	tk.MustExec("create table test_fast_gstats(a int, b int) PARTITION BY HASH(a) PARTITIONS 2;")
	tk.MustExec("insert into test_fast_gstats values(1,1),(3,3),(4,4),(2,2),(5,5);")
	err := tk.ExecToErr("analyze table test_fast_gstats;")
	require.EqualError(t, err, "Fast analyze hasn't reached General Availability and only support analyze version 1 currently.")
}

func TestAnalyzeIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (id int, v int, primary key(id), index k(v))")
	tk.MustExec("insert into t1(id, v) values(1, 2), (2, 2), (3, 2), (4, 2), (5, 1), (6, 3), (7, 4)")
	tk.MustExec("set @@tidb_analyze_version=1")
	tk.MustExec("analyze table t1 index k")
	require.Greater(t, len(tk.MustQuery("show stats_buckets where table_name = 't1' and column_name = 'k' and is_index = 1").Rows()), 0)
	tk.MustExec("set @@tidb_analyze_version=default")
	tk.MustExec("analyze table t1")
	require.Greater(t, len(tk.MustQuery("show stats_topn where table_name = 't1' and column_name = 'k' and is_index = 1").Rows()), 0)

	func() {
		defer tk.MustExec("set @@session.tidb_enable_fast_analyze=0")
		tk.MustExec("drop stats t1")
		tk.MustExec("set @@session.tidb_enable_fast_analyze=1")
		tk.MustExec("set @@tidb_analyze_version=1")
		tk.MustExec("analyze table t1 index k")
		require.Greater(t, len(tk.MustQuery("show stats_buckets where table_name = 't1' and column_name = 'k' and is_index = 1").Rows()), 1)
	}()
}

func TestAnalyzeIncremental(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_analyze_version = 1")
	tk.Session().GetSessionVars().EnableStreaming = false
	testAnalyzeIncremental(tk, t, dom)
}

func TestAnalyzeIncrementalStreaming(t *testing.T) {
	t.Skip("unistore hasn't support streaming yet.")
	store, clean := testkit.CreateMockStore(t)
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)

	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableStreaming = true
	testAnalyzeIncremental(tk, t, dom)
}

func testAnalyzeIncremental(tk *testkit.TestKit, t *testing.T, dom *domain.Domain) {
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, primary key(a), index idx(b))")
	tk.MustExec("analyze incremental table t index")
	tk.MustQuery("show stats_buckets").Check(testkit.Rows())
	tk.MustExec("insert into t values (1,1)")
	tk.MustExec("analyze incremental table t index")
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t  a 0 0 1 1 1 1 0", "test t  idx 1 0 1 1 1 1 0"))
	tk.MustExec("insert into t values (2,2)")
	tk.MustExec("analyze incremental table t index")
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t  a 0 0 1 1 1 1 0", "test t  a 0 1 2 1 2 2 0", "test t  idx 1 0 1 1 1 1 0", "test t  idx 1 1 2 1 2 2 0"))
	tk.MustExec("analyze incremental table t index")
	// Result should not change.
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t  a 0 0 1 1 1 1 0", "test t  a 0 1 2 1 2 2 0", "test t  idx 1 0 1 1 1 1 0", "test t  idx 1 1 2 1 2 2 0"))

	// Test analyze incremental with feedback.
	tk.MustExec("insert into t values (3,3)")
	oriProbability := statistics.FeedbackProbability.Load()
	oriMinLogCount := handle.MinLogScanCount.Load()
	defer func() {
		statistics.FeedbackProbability.Store(oriProbability)
		handle.MinLogScanCount.Store(oriMinLogCount)
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount.Store(0)
	is := dom.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := table.Meta()
	tk.MustQuery("select * from t use index(idx) where b = 3")
	tk.MustQuery("select * from t where a > 1")
	h := dom.StatsHandle()
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.DumpStatsFeedbackToKV())
	require.NoError(t, h.HandleUpdateStats(is))
	require.NoError(t, h.Update(is))
	require.NoError(t, h.LoadNeededHistograms())
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t  a 0 0 1 1 1 1 0", "test t  a 0 1 3 0 2 2147483647 0", "test t  idx 1 0 1 1 1 1 0", "test t  idx 1 1 2 1 2 2 0"))
	tblStats := h.GetTableStats(tblInfo)
	val, err := codec.EncodeKey(tk.Session().GetSessionVars().StmtCtx, nil, types.NewIntDatum(3))
	require.NoError(t, err)
	require.Equal(t, uint64(1), tblStats.Indices[tblInfo.Indices[0].ID].QueryBytes(val))
	require.False(t, statistics.IsAnalyzed(tblStats.Indices[tblInfo.Indices[0].ID].Flag))
	require.False(t, statistics.IsAnalyzed(tblStats.Columns[tblInfo.Columns[0].ID].Flag))

	tk.MustExec("analyze incremental table t index")
	require.NoError(t, h.LoadNeededHistograms())
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t  a 0 0 1 1 1 1 0", "test t  a 0 1 2 1 2 2 0", "test t  a 0 2 3 1 3 3 0",
		"test t  idx 1 0 1 1 1 1 0", "test t  idx 1 1 2 1 2 2 0", "test t  idx 1 2 3 1 3 3 0"))
	tblStats = h.GetTableStats(tblInfo)
	require.Equal(t, uint64(1), tblStats.Indices[tblInfo.Indices[0].ID].QueryBytes(val))

	// test analyzeIndexIncremental for global-level stats;
	tk.MustExec("set @@session.tidb_analyze_version = 1;")
	tk.MustQuery("select @@tidb_analyze_version").Check(testkit.Rows("1"))
	tk.MustExec("set @@tidb_partition_prune_mode = 'static';")
	tk.MustExec("drop table if exists t;")
	tk.MustExec(`create table t (a int, b int, primary key(a), index idx(b)) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (20),
		partition p2 values less than (30)
	);`)
	tk.MustExec("analyze incremental table t index")
	require.NoError(t, h.LoadNeededHistograms())
	tk.MustQuery("show stats_buckets").Check(testkit.Rows())
	tk.MustExec("insert into t values (1,1)")
	tk.MustExec("analyze incremental table t index")
	tk.MustQuery("show warnings").Check(testkit.Rows()) // no warning
	require.NoError(t, h.LoadNeededHistograms())
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t p0 a 0 0 1 1 1 1 0", "test t p0 idx 1 0 1 1 1 1 0"))
	tk.MustExec("insert into t values (2,2)")
	tk.MustExec("analyze incremental table t index")
	require.NoError(t, h.LoadNeededHistograms())
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t p0 a 0 0 1 1 1 1 0", "test t p0 a 0 1 2 1 2 2 0", "test t p0 idx 1 0 1 1 1 1 0", "test t p0 idx 1 1 2 1 2 2 0"))
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic';")
	tk.MustExec("insert into t values (11,11)")
	err = tk.ExecToErr("analyze incremental table t index")
	require.Equal(t, "[stats]: global statistics for partitioned tables unavailable in ANALYZE INCREMENTAL", err.Error())
}

func TestIssue27429(t *testing.T) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table test.t(id int, value varchar(20) charset utf8mb4 collate utf8mb4_general_ci, value1 varchar(20) charset utf8mb4 collate utf8mb4_bin)")
	tk.MustExec("insert into test.t values (1, 'abc', 'abc '),(4, 'Abc', 'abc'),(3,'def', 'def ');")

	tk.MustQuery("select upper(group_concat(distinct value order by 1)) from test.t;").Check(testkit.Rows("ABC,DEF"))
	tk.MustQuery("select upper(group_concat(distinct value)) from test.t;").Check(testkit.Rows("ABC,DEF"))
}

func TestIssue20874(t *testing.T) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("delete from mysql.stats_histograms")
	tk.MustExec("create table t (a char(10) collate utf8mb4_unicode_ci not null, b char(20) collate utf8mb4_general_ci not null, key idxa(a), key idxb(b))")
	tk.MustExec("insert into t values ('#', 'C'), ('$', 'c'), ('a', 'a')")
	tk.MustExec("set @@tidb_analyze_version=1")
	tk.MustExec("analyze table t")
	tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Sort().Check(testkit.Rows(
		"test t  a 0 0 1 1 \x02\xd2 \x02\xd2 0",
		"test t  a 0 1 2 1 \x0e\x0f \x0e\x0f 0",
		"test t  a 0 2 3 1 \x0e3 \x0e3 0",
		"test t  b 0 0 1 1 \x00A \x00A 0",
		"test t  b 0 1 3 2 \x00C \x00C 0",
		"test t  idxa 1 0 1 1 \x02\xd2 \x02\xd2 0",
		"test t  idxa 1 1 2 1 \x0e\x0f \x0e\x0f 0",
		"test t  idxa 1 2 3 1 \x0e3 \x0e3 0",
		"test t  idxb 1 0 1 1 \x00A \x00A 0",
		"test t  idxb 1 1 3 2 \x00C \x00C 0",
	))
	tk.MustQuery("select is_index, hist_id, distinct_count, null_count, tot_col_size, stats_ver, correlation from mysql.stats_histograms").Sort().Check(testkit.Rows(
		"0 1 3 0 9 1 1",
		"0 2 2 0 9 1 -0.5",
		"1 1 3 0 0 1 0",
		"1 2 2 0 0 1 0",
	))
	tk.MustExec("set @@tidb_analyze_version=2")
	tk.MustExec("analyze table t")
	tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(testkit.Rows(
		"test t  a 0 \x02\xd2 1",
		"test t  a 0 \x0e\x0f 1",
		"test t  a 0 \x0e3 1",
		"test t  b 0 \x00A 1",
		"test t  b 0 \x00C 2",
		"test t  idxa 1 \x02\xd2 1",
		"test t  idxa 1 \x0e\x0f 1",
		"test t  idxa 1 \x0e3 1",
		"test t  idxb 1 \x00A 1",
		"test t  idxb 1 \x00C 2",
	))
	tk.MustQuery("select is_index, hist_id, distinct_count, null_count, tot_col_size, stats_ver, correlation from mysql.stats_histograms").Sort().Check(testkit.Rows(
		"0 1 3 0 6 2 1",
		"0 2 2 0 6 2 -0.5",
		"1 1 3 0 6 2 0",
		"1 2 2 0 6 2 0",
	))
}

func TestAnalyzeClusteredIndexPrimary(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t0(a varchar(20), primary key(a) clustered)")
	tk.MustExec("create table t1(a varchar(20), primary key(a))")
	tk.MustExec("insert into t0 values('1111')")
	tk.MustExec("insert into t1 values('1111')")
	tk.MustExec("set @@session.tidb_analyze_version = 1")
	tk.MustExec("analyze table t0 index primary")
	tk.MustExec("analyze table t1 index primary")
	tk.MustQuery("show stats_buckets").Check(testkit.Rows(
		"test t0  PRIMARY 1 0 1 1 1111 1111 0",
		"test t1  PRIMARY 1 0 1 1 1111 1111 0"))
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("analyze table t0")
	tk.MustExec("analyze table t1")
	tk.MustQuery("show stats_topn").Sort().Check(testkit.Rows(""+
		"test t0  PRIMARY 1 1111 1",
		"test t0  a 0 1111 1",
		"test t1  PRIMARY 1 1111 1",
		"test t1  a 0 1111 1"))
}

func TestAnalyzeSamplingWorkPanic(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12)")
	tk.MustExec("split table t between (-9223372036854775808) and (9223372036854775807) regions 12")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/mockAnalyzeSamplingBuildWorkerPanic", "return(1)"))
	err := tk.ExecToErr("analyze table t")
	require.NotNil(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/mockAnalyzeSamplingBuildWorkerPanic"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/mockAnalyzeSamplingMergeWorkerPanic", "return(1)"))
	err = tk.ExecToErr("analyze table t")
	require.NotNil(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/mockAnalyzeSamplingMergeWorkerPanic"))
}

func TestAnalyzeFastSample(t *testing.T) {
	var cls testutils.Cluster
	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			cls = c
		}),
	)
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	var dom *domain.Domain
	session.DisableStats4Test()
	session.SetSchemaLease(0)
	dom, err = session.BootstrapSession(store)
	require.NoError(t, err)
	defer dom.Close()
	tk := testkit.NewTestKit(t, store)
	atomic.StoreInt64(&executor.RandSeed, 123)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, index index_b(b))")
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	tid := tblInfo.ID

	// construct 5 regions split by {12, 24, 36, 48}
	splitKeys := generateTableSplitKeyForInt(tid, []int{12, 24, 36, 48})
	manipulateCluster(cls, splitKeys)

	for i := 0; i < 60; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}

	handleCols := core.BuildHandleColsForAnalyze(tk.Session(), tblInfo, true, nil)
	var colsInfo []*model.ColumnInfo
	var indicesInfo []*model.IndexInfo
	for _, col := range tblInfo.Columns {
		if mysql.HasPriKeyFlag(col.Flag) {
			continue
		}
		colsInfo = append(colsInfo, col)
	}
	for _, idx := range tblInfo.Indices {
		if idx.State == model.StatePublic {
			indicesInfo = append(indicesInfo, idx)
		}
	}
	opts := make(map[ast.AnalyzeOptionType]uint64)
	opts[ast.AnalyzeOptNumSamples] = 20
	// Get a start_ts later than the above inserts.
	tk.MustExec("begin")
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	ts := txn.StartTS()
	tk.MustExec("commit")
	mockExec := &executor.AnalyzeTestFastExec{
		Ctx:         tk.Session().(sessionctx.Context),
		HandleCols:  handleCols,
		ColsInfo:    colsInfo,
		IdxsInfo:    indicesInfo,
		Concurrency: 1,
		Snapshot:    ts,
		TableID: statistics.AnalyzeTableID{
			PartitionID: -1,
			TableID:     tbl.(table.PhysicalTable).GetPhysicalID(),
		},
		TblInfo: tblInfo,
		Opts:    opts,
	}
	err = mockExec.TestFastSample()
	require.NoError(t, err)
	require.Len(t, mockExec.Collectors, 3)
	for i := 0; i < 2; i++ {
		samples := mockExec.Collectors[i].Samples
		require.Len(t, samples, 20)
		for j := 1; j < 20; j++ {
			cmp, err := samples[j].Value.Compare(tk.Session().GetSessionVars().StmtCtx, &samples[j-1].Value, collate.GetBinaryCollator())
			require.NoError(t, err)
			require.Greater(t, cmp, 0)
		}
	}
}

func TestFastAnalyze(t *testing.T) {
	t.Parallel()
	t.Skip("Skip this unstable test(#25782) and bring it back before 2021-07-29.")
	var cls testutils.Cluster
	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			cls = c
		}),
	)
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	var dom *domain.Domain
	session.DisableStats4Test()
	session.SetSchemaLease(0)
	dom, err = session.BootstrapSession(store)
	require.NoError(t, err)
	dom.SetStatsUpdating(true)
	defer dom.Close()
	tk := testkit.NewTestKit(t, store)
	atomic.StoreInt64(&executor.RandSeed, 123)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, c char(10), index index_b(b))")
	tk.MustExec("set @@session.tidb_enable_fast_analyze=1")
	tk.MustExec("set @@session.tidb_build_stats_concurrency=1")
	tk.MustExec("set @@tidb_analyze_version = 1")
	// Should not panic.
	tk.MustExec("analyze table t")
	tblInfo, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tid := tblInfo.Meta().ID

	// construct 6 regions split by {10, 20, 30, 40, 50}
	splitKeys := generateTableSplitKeyForInt(tid, []int{10, 20, 30, 40, 50})
	manipulateCluster(cls, splitKeys)

	for i := 0; i < 20; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (%d, %d, "char")`, i*3, i*3))
	}
	tk.MustExec("analyze table t with 5 buckets, 6 samples")

	is := tk.Session().(sessionctx.Context).GetInfoSchema().(infoschema.InfoSchema)
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	tbl := dom.StatsHandle().GetTableStats(tableInfo)
	// TODO(tangenta): add stats_meta.row_count assertion.
	for _, col := range tbl.Columns {
		ok, err := checkHistogram(tk.Session().GetSessionVars().StmtCtx, &col.Histogram)
		require.NoError(t, err)
		require.True(t, ok)
	}
	for _, idx := range tbl.Indices {
		ok, err := checkHistogram(tk.Session().GetSessionVars().StmtCtx, &idx.Histogram)
		require.NoError(t, err)
		require.True(t, ok)
	}

	// Test CM Sketch built from fast analyze.
	tk.MustExec("create table t1(a int, b int, index idx(a, b))")
	// Should not panic.
	tk.MustExec("analyze table t1")
	tk.MustExec("insert into t1 values (1,1),(1,1),(1,2),(1,2)")
	tk.MustExec("analyze table t1")
	tk.MustQuery("explain format = 'brief' select a from t1 where a = 1").Check(testkit.Rows(
		"IndexReader 4.00 root  index:IndexRangeScan",
		"└─IndexRangeScan 4.00 cop[tikv] table:t1, index:idx(a, b) range:[1,1], keep order:false"))
	tk.MustQuery("explain format = 'brief' select a, b from t1 where a = 1 and b = 1").Check(testkit.Rows(
		"IndexReader 2.00 root  index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t1, index:idx(a, b) range:[1 1,1 1], keep order:false"))
	tk.MustQuery("explain format = 'brief' select a, b from t1 where a = 1 and b = 2").Check(testkit.Rows(
		"IndexReader 2.00 root  index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t1, index:idx(a, b) range:[1 2,1 2], keep order:false"))

	tk.MustExec("create table t2 (a bigint unsigned, primary key(a))")
	tk.MustExec("insert into t2 values (0), (18446744073709551615)")
	tk.MustExec("analyze table t2")
	tk.MustQuery("show stats_buckets where table_name = 't2'").Check(testkit.Rows(
		"test t2  a 0 0 1 1 0 0 0",
		"test t2  a 0 1 2 1 18446744073709551615 18446744073709551615 0"))

	tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)
	tk.MustExec(`create table t3 (id int, v int, primary key(id), index k(v)) partition by hash (id) partitions 4`)
	tk.MustExec(`insert into t3 values(1, 1), (2, 2), (5, 1), (9, 3), (13, 3), (17, 5), (3, 0)`)
	tk.MustExec(`analyze table t3`)
	tk.MustQuery(`explain format = 'brief' select v from t3 partition(p1) where v = 3`).Check(testkit.Rows(
		"IndexReader 2.00 root  index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t3, partition:p1, index:k(v) range:[3,3], keep order:false",
	))
	tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Dynamic) + `'`)

	// global-stats depends on stats-ver2, but stats-ver2 is not compatible with fast-analyze, so forbid using global-stats with fast-analyze now.
	// TODO: add more test cases about global-stats with fast-analyze after resolving the compatibility problem.
	/*
		// test fast analyze in dynamic mode
		tk.MustExec("drop table if exists t4;")
		tk.MustExec("create table t4(a int, b int) PARTITION BY HASH(a) PARTITIONS 2;")
		tk.MustExec("insert into t4 values(1,1),(3,3),(4,4),(2,2),(5,5);")
		// Because the statistics of partition p1 are missing, the construction of global-level stats will fail.
		tk.MustExec("analyze table t4 partition p1;")
		tk.MustQuery("show warnings").Check(testkit.Rows("Warning 8131 Build global-level stats failed due to missing partition-level stats"))
		// Although the global-level stats build failed, we build partition-level stats for partition p1 success.
		result := tk.MustQuery("show stats_meta where table_name = 't4'").Sort()
		c.Assert(len(result.Rows()), Equals, 1)
		c.Assert(result.Rows()[0][5], Equals, "3")
		// Now, we have the partition-level stats for partition p0. We need get the stats for partition p1. And build the global-level stats.
		tk.MustExec("analyze table t4 partition p0;")
		tk.MustQuery("show warnings").Check(testkit.Rows())
		result = tk.MustQuery("show stats_meta where table_name = 't4'").Sort()
		c.Assert(len(result.Rows()), Equals, 3)
		c.Assert(result.Rows()[0][5], Equals, "5")
		c.Assert(result.Rows()[1][5], Equals, "2")
		c.Assert(result.Rows()[2][5], Equals, "3")
	*/
}
