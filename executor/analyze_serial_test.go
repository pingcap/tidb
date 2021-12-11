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
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/stretchr/testify/require"
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
