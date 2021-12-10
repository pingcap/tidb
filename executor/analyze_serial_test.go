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

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/stretchr/testify/require"
)

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

func TestAnalyzeIncremental(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_analyze_version = 1")
	tk.Session().GetSessionVars().EnableStreaming = false
	testAnalyzeIncremental(tk, dom, t)
}

func TestAnalyzeIncrementalStreaming(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableStreaming = true
	testAnalyzeIncremental(tk, dom, t)
}

// nolint:unused
func testAnalyzeIncremental(tk *testkit.TestKit, dom *domain.Domain, t *testing.T) {
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
	require.EqualError(t, err, "[stats]: global statistics for partitioned tables unavailable in ANALYZE INCREMENTAL")
}
