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

package statistics_test

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestChangeVerTo2Behavior(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	originalVal1 := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal1))
	}()
	tk.MustExec("set global tidb_persist_analyze_options=false")

	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("set @@session.tidb_analyze_version = 1")
	tk.MustExec("insert into t values(1, 1), (1, 2), (1, 3)")
	tk.MustExec("analyze table t")
	is := dom.InfoSchema()
	tblT, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	h := dom.StatsHandle()
	require.NoError(t, h.Update(is))
	statsTblT := h.GetTableStats(tblT.Meta())
	// Analyze table with version 1 success, all statistics are version 1.
	for _, col := range statsTblT.Columns {
		require.Equal(t, int64(1), col.StatsVer)
	}
	for _, idx := range statsTblT.Indices {
		require.Equal(t, int64(1), idx.StatsVer)
	}
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("analyze table t index idx")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 The analyze version from the session is not compatible with the existing statistics of the table. Use the existing version instead"))
	require.NoError(t, h.Update(is))
	statsTblT = h.GetTableStats(tblT.Meta())
	for _, idx := range statsTblT.Indices {
		require.Equal(t, int64(1), idx.StatsVer)
	}
	tk.MustExec("analyze table t index")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 The analyze version from the session is not compatible with the existing statistics of the table. Use the existing version instead"))
	require.NoError(t, h.Update(is))
	statsTblT = h.GetTableStats(tblT.Meta())
	for _, idx := range statsTblT.Indices {
		require.Equal(t, int64(1), idx.StatsVer)
	}
	tk.MustExec("analyze table t ")
	require.NoError(t, h.Update(is))
	statsTblT = h.GetTableStats(tblT.Meta())
	for _, col := range statsTblT.Columns {
		require.Equal(t, int64(2), col.StatsVer)
	}
	for _, idx := range statsTblT.Indices {
		require.Equal(t, int64(2), idx.StatsVer)
	}
	tk.MustExec("set @@session.tidb_analyze_version = 1")
	tk.MustExec("analyze table t index idx")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 The analyze version from the session is not compatible with the existing statistics of the table. Use the existing version instead",
		"Warning 1105 The version 2 would collect all statistics not only the selected indexes"))
	require.NoError(t, h.Update(is))
	statsTblT = h.GetTableStats(tblT.Meta())
	for _, idx := range statsTblT.Indices {
		require.Equal(t, int64(2), idx.StatsVer)
	}
	tk.MustExec("analyze table t index")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 The analyze version from the session is not compatible with the existing statistics of the table. Use the existing version instead",
		"Warning 1105 The version 2 would collect all statistics not only the selected indexes"))
	require.NoError(t, h.Update(is))
	statsTblT = h.GetTableStats(tblT.Meta())
	for _, idx := range statsTblT.Indices {
		require.Equal(t, int64(2), idx.StatsVer)
	}
	tk.MustExec("analyze table t ")
	require.NoError(t, h.Update(is))
	statsTblT = h.GetTableStats(tblT.Meta())
	for _, col := range statsTblT.Columns {
		require.Equal(t, int64(1), col.StatsVer)
	}
	for _, idx := range statsTblT.Indices {
		require.Equal(t, int64(1), idx.StatsVer)
	}
}

func TestChangeVerTo2BehaviorWithPersistedOptions(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	originalVal1 := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal1))
	}()
	tk.MustExec("set global tidb_persist_analyze_options=true")

	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("set @@session.tidb_analyze_version = 1")
	tk.MustExec("insert into t values(1, 1), (1, 2), (1, 3)")
	tk.MustExec("analyze table t")
	is := dom.InfoSchema()
	tblT, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	h := dom.StatsHandle()
	require.NoError(t, h.Update(is))
	statsTblT := h.GetTableStats(tblT.Meta())
	// Analyze table with version 1 success, all statistics are version 1.
	for _, col := range statsTblT.Columns {
		require.Equal(t, int64(1), col.StatsVer)
	}
	for _, idx := range statsTblT.Indices {
		require.Equal(t, int64(1), idx.StatsVer)
	}
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("analyze table t index idx")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 The analyze version from the session is not compatible with the existing statistics of the table. Use the existing version instead"))
	require.NoError(t, h.Update(is))
	statsTblT = h.GetTableStats(tblT.Meta())
	for _, idx := range statsTblT.Indices {
		require.Equal(t, int64(1), idx.StatsVer)
	}
	tk.MustExec("analyze table t index")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 The analyze version from the session is not compatible with the existing statistics of the table. Use the existing version instead"))
	require.NoError(t, h.Update(is))
	statsTblT = h.GetTableStats(tblT.Meta())
	for _, idx := range statsTblT.Indices {
		require.Equal(t, int64(1), idx.StatsVer)
	}
	tk.MustExec("analyze table t ")
	require.NoError(t, h.Update(is))
	statsTblT = h.GetTableStats(tblT.Meta())
	for _, col := range statsTblT.Columns {
		require.Equal(t, int64(2), col.StatsVer)
	}
	for _, idx := range statsTblT.Indices {
		require.Equal(t, int64(2), idx.StatsVer)
	}
	tk.MustExec("set @@session.tidb_analyze_version = 1")
	tk.MustExec("analyze table t index idx")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 The analyze version from the session is not compatible with the existing statistics of the table. Use the existing version instead",
		"Warning 1105 The version 2 would collect all statistics not only the selected indexes",
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t")) // since fallback to ver2 path, should do samplerate adjustment
	require.NoError(t, h.Update(is))
	statsTblT = h.GetTableStats(tblT.Meta())
	for _, idx := range statsTblT.Indices {
		require.Equal(t, int64(2), idx.StatsVer)
	}
	tk.MustExec("analyze table t index")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 The analyze version from the session is not compatible with the existing statistics of the table. Use the existing version instead",
		"Warning 1105 The version 2 would collect all statistics not only the selected indexes",
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t"))
	require.NoError(t, h.Update(is))
	statsTblT = h.GetTableStats(tblT.Meta())
	for _, idx := range statsTblT.Indices {
		require.Equal(t, int64(2), idx.StatsVer)
	}
	tk.MustExec("analyze table t ")
	require.NoError(t, h.Update(is))
	statsTblT = h.GetTableStats(tblT.Meta())
	for _, col := range statsTblT.Columns {
		require.Equal(t, int64(1), col.StatsVer)
	}
	for _, idx := range statsTblT.Indices {
		require.Equal(t, int64(1), idx.StatsVer)
	}
}

func TestFastAnalyzeOnVer2(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("set @@session.tidb_enable_fast_analyze = 1")
	tk.MustExec("insert into t values(1, 1), (1, 2), (1, 3)")
	_, err := tk.Exec("analyze table t")
	require.Error(t, err)
	require.Equal(t, "Fast analyze hasn't reached General Availability and only support analyze version 1 currently", err.Error())
	tk.MustExec("set @@session.tidb_enable_fast_analyze = 0")
	tk.MustExec("analyze table t")
	is := dom.InfoSchema()
	tblT, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	h := dom.StatsHandle()
	require.NoError(t, h.Update(is))
	statsTblT := h.GetTableStats(tblT.Meta())
	for _, col := range statsTblT.Columns {
		require.Equal(t, int64(2), col.StatsVer)
	}
	for _, idx := range statsTblT.Indices {
		require.Equal(t, int64(2), idx.StatsVer)
	}
	tk.MustExec("set @@session.tidb_enable_fast_analyze = 1")
	err = tk.ExecToErr("analyze table t index idx")
	require.Error(t, err)
	require.Equal(t, "Fast analyze hasn't reached General Availability and only support analyze version 1 currently", err.Error())
	tk.MustExec("set @@session.tidb_analyze_version = 1")
	_, err = tk.Exec("analyze table t index idx")
	require.Error(t, err)
	require.Equal(t, "Fast analyze hasn't reached General Availability and only support analyze version 1 currently. But the existing statistics of the table is not version 1", err.Error())
	_, err = tk.Exec("analyze table t index")
	require.Error(t, err)
	require.Equal(t, "Fast analyze hasn't reached General Availability and only support analyze version 1 currently. But the existing statistics of the table is not version 1", err.Error())
	tk.MustExec("analyze table t")
	require.NoError(t, h.Update(is))
	statsTblT = h.GetTableStats(tblT.Meta())
	for _, col := range statsTblT.Columns {
		require.Equal(t, int64(1), col.StatsVer)
	}
	for _, idx := range statsTblT.Indices {
		require.Equal(t, int64(1), idx.StatsVer)
	}
}

func TestIncAnalyzeOnVer2(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("insert into t values(1, 1), (1, 2)")
	tk.MustExec("analyze table t with 2 topn")
	is := dom.InfoSchema()
	h := dom.StatsHandle()
	require.NoError(t, h.Update(is))
	tk.MustExec("insert into t values(2, 1), (2, 2), (2, 3), (3, 3), (4, 4), (4, 3), (4, 2), (4, 1)")
	require.NoError(t, h.Update(is))
	tk.MustExec("analyze incremental table t index idx with 2 topn")
	// After analyze, there's two val in hist.
	tk.MustQuery("show stats_buckets where table_name = 't' and column_name = 'idx'").Check(testkit.Rows(
		"test t  idx 1 0 2 2 1 1 0",
		"test t  idx 1 1 3 1 3 3 0",
	))
	// Two val in topn.
	tk.MustQuery("show stats_topn where table_name = 't' and column_name = 'idx'").Check(testkit.Rows(
		"test t  idx 1 2 3",
		"test t  idx 1 4 4",
	))
}

func TestExpBackoffEstimation(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table exp_backoff(a int, b int, c int, d int, index idx(a, b, c, d))")
	tk.MustExec("insert into exp_backoff values(1, 1, 1, 1), (1, 1, 1, 2), (1, 1, 2, 3), (1, 2, 2, 4), (1, 2, 3, 5)")
	tk.MustExec("set @@session.tidb_analyze_version=2")
	tk.MustExec("analyze table exp_backoff")
	var (
		input  []string
		output [][]string
	)
	integrationSuiteData := statistics.GetIntegrationSuiteData()
	integrationSuiteData.GetTestCases(t, &input, &output)
	inputLen := len(input)
	// The test cases are:
	// Query a = 1, b = 1, c = 1, d >= 3 and d <= 5 separately. We got 5, 3, 2, 3.
	// And then query and a = 1 and b = 1 and c = 1 and d >= 3 and d <= 5. It's result should follow the exp backoff,
	// which is 2/5 * (3/5)^{1/2} * (3/5)*{1/4} * 1^{1/8} * 5 = 1.3634.
	for i := 0; i < inputLen-1; i++ {
		testdata.OnRecord(func() {
			output[i] = testdata.ConvertRowsToStrings(tk.MustQuery(input[i]).Rows())
		})
		tk.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
	}

	// The last case is that no column is loaded and we get no stats at all.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/statistics/cleanEstResults", `return(true)`))
	testdata.OnRecord(func() {
		output[inputLen-1] = testdata.ConvertRowsToStrings(tk.MustQuery(input[inputLen-1]).Rows())
	})
	tk.MustQuery(input[inputLen-1]).Check(testkit.Rows(output[inputLen-1]...))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/statistics/cleanEstResults"))
}

func TestGlobalStats(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@session.tidb_analyze_version = 2;")
	tk.MustExec(`create table t (a int, key(a)) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (20),
		partition p2 values less than (30)
	);`)
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic';")
	tk.MustExec("insert into t values (1), (5), (null), (11), (15), (21), (25);")
	tk.MustExec("analyze table t;")
	// On the table with global-stats, we use explain to query a multi-partition query.
	// And we should get the result that global-stats is used instead of pseudo-stats.
	tk.MustQuery("explain format = 'brief' select a from t where a > 5").Check(testkit.Rows(
		"IndexReader 4.00 root partition:all index:IndexRangeScan",
		"└─IndexRangeScan 4.00 cop[tikv] table:t, index:a(a) range:(5,+inf], keep order:false"))
	// On the table with global-stats, we use explain to query a single-partition query.
	// And we should get the result that global-stats is used instead of pseudo-stats.
	tk.MustQuery("explain format = 'brief' select * from t partition(p1) where a > 15;").Check(testkit.Rows(
		"IndexReader 2.00 root partition:p1 index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t, index:a(a) range:(15,+inf], keep order:false"))

	// Even if we have global-stats, we will not use it when the switch is set to `static`.
	tk.MustExec("set @@tidb_partition_prune_mode = 'static';")
	tk.MustQuery("explain format = 'brief' select a from t where a > 5").Check(testkit.Rows(
		"PartitionUnion 4.00 root  ",
		"├─IndexReader 0.00 root  index:IndexRangeScan",
		"│ └─IndexRangeScan 0.00 cop[tikv] table:t, partition:p0, index:a(a) range:(5,+inf], keep order:false",
		"├─IndexReader 2.00 root  index:IndexRangeScan",
		"│ └─IndexRangeScan 2.00 cop[tikv] table:t, partition:p1, index:a(a) range:(5,+inf], keep order:false",
		"└─IndexReader 2.00 root  index:IndexRangeScan",
		"  └─IndexRangeScan 2.00 cop[tikv] table:t, partition:p2, index:a(a) range:(5,+inf], keep order:false"))

	tk.MustExec("set @@tidb_partition_prune_mode = 'static';")
	tk.MustExec("drop table t;")
	tk.MustExec("create table t(a int, b int, key(a)) PARTITION BY HASH(a) PARTITIONS 2;")
	tk.MustExec("insert into t values(1,1),(3,3),(4,4),(2,2),(5,5);")
	// When we set the mode to `static`, using analyze will not report an error and will not generate global-stats.
	// In addition, when using explain to view the plan of the related query, it was found that `Union` was used.
	tk.MustExec("analyze table t;")
	result := tk.MustQuery("show stats_meta where table_name = 't'").Sort()
	require.Len(t, result.Rows(), 2)
	require.Equal(t, "2", result.Rows()[0][5])
	require.Equal(t, "3", result.Rows()[1][5])
	tk.MustQuery("explain format = 'brief' select a from t where a > 3;").Check(testkit.Rows(
		"PartitionUnion 2.00 root  ",
		"├─IndexReader 1.00 root  index:IndexRangeScan",
		"│ └─IndexRangeScan 1.00 cop[tikv] table:t, partition:p0, index:a(a) range:(3,+inf], keep order:false",
		"└─IndexReader 1.00 root  index:IndexRangeScan",
		"  └─IndexRangeScan 1.00 cop[tikv] table:t, partition:p1, index:a(a) range:(3,+inf], keep order:false"))

	// When we turned on the switch, we found that pseudo-stats will be used in the plan instead of `Union`.
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic';")
	tk.MustQuery("explain format = 'brief' select a from t where a > 3;").Check(testkit.Rows(
		"IndexReader 3333.33 root partition:all index:IndexRangeScan",
		"└─IndexRangeScan 3333.33 cop[tikv] table:t, index:a(a) range:(3,+inf], keep order:false, stats:pseudo"))

	// Execute analyze again without error and can generate global-stats.
	// And when executing related queries, neither Union nor pseudo-stats are used.
	tk.MustExec("analyze table t;")
	result = tk.MustQuery("show stats_meta where table_name = 't'").Sort()
	require.Len(t, result.Rows(), 3)
	require.Equal(t, "5", result.Rows()[0][5])
	require.Equal(t, "2", result.Rows()[1][5])
	require.Equal(t, "3", result.Rows()[2][5])
	tk.MustQuery("explain format = 'brief' select a from t where a > 3;").Check(testkit.Rows(
		"IndexReader 2.00 root partition:all index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t, index:a(a) range:(3,+inf], keep order:false"))

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int, b int, c int)  PARTITION BY HASH(a) PARTITIONS 2;")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic';")
	tk.MustExec("create index idx_ab on t(a, b);")
	tk.MustExec("insert into t values (1, 1, 1), (5, 5, 5), (11, 11, 11), (15, 15, 15), (21, 21, 21), (25, 25, 25);")
	tk.MustExec("analyze table t;")
	// test the indexScan
	tk.MustQuery("explain format = 'brief' select b from t where a > 5 and b > 10;").Check(testkit.Rows(
		"Projection 2.67 root  test.t.b",
		"└─IndexReader 2.67 root partition:all index:Selection",
		"  └─Selection 2.67 cop[tikv]  gt(test.t.b, 10)",
		"    └─IndexRangeScan 4.00 cop[tikv] table:t, index:idx_ab(a, b) range:(5,+inf], keep order:false"))
	// test the indexLookUp
	tk.MustQuery("explain format = 'brief' select * from t use index(idx_ab) where a > 1;").Check(testkit.Rows(
		"IndexLookUp 5.00 root partition:all ",
		"├─IndexRangeScan(Build) 5.00 cop[tikv] table:t, index:idx_ab(a, b) range:(1,+inf], keep order:false",
		"└─TableRowIDScan(Probe) 5.00 cop[tikv] table:t keep order:false"))
	// test the tableScan
	tk.MustQuery("explain format = 'brief' select * from t;").Check(testkit.Rows(
		"TableReader 6.00 root partition:all data:TableFullScan",
		"└─TableFullScan 6.00 cop[tikv] table:t keep order:false"))
}

func TestNULLOnFullSampling(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@session.tidb_analyze_version = 2;")
	tk.MustExec("create table t(a int, index idx(a))")
	tk.MustExec("insert into t values(1), (1), (1), (2), (2), (3), (4), (null), (null), (null)")
	var (
		input  []string
		output [][]string
	)
	tk.MustExec("analyze table t with 2 topn")
	is := dom.InfoSchema()
	tblT, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	h := dom.StatsHandle()
	require.NoError(t, h.Update(is))
	statsTblT := h.GetTableStats(tblT.Meta())
	// Check the null count is 3.
	for _, col := range statsTblT.Columns {
		require.Equal(t, int64(3), col.NullCount)
	}
	integrationSuiteData := statistics.GetIntegrationSuiteData()
	integrationSuiteData.GetTestCases(t, &input, &output)
	// Check the topn and buckets contains no null values.
	for i := 0; i < len(input); i++ {
		testdata.OnRecord(func() {
			output[i] = testdata.ConvertRowsToStrings(tk.MustQuery(input[i]).Rows())
		})
		tk.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
	}
}

func TestAnalyzeSnapshot(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@session.tidb_analyze_version = 2;")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1), (1), (1)")
	tk.MustExec("analyze table t")
	rows := tk.MustQuery("select count, snapshot from mysql.stats_meta").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "3", rows[0][0])
	s1Str := rows[0][1].(string)
	s1, err := strconv.ParseUint(s1Str, 10, 64)
	require.NoError(t, err)
	require.True(t, s1 < math.MaxUint64)
	tk.MustExec("insert into t values(1), (1), (1)")
	tk.MustExec("analyze table t")
	rows = tk.MustQuery("select count, snapshot from mysql.stats_meta").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "6", rows[0][0])
	s2Str := rows[0][1].(string)
	s2, err := strconv.ParseUint(s2Str, 10, 64)
	require.NoError(t, err)
	require.True(t, s2 < math.MaxUint64)
	require.True(t, s2 > s1)
}

func TestHistogramsWithSameTxnTS(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@session.tidb_analyze_version = 2;")
	tk.MustExec("create table t(a int, index(a))")
	tk.MustExec("insert into t values(1), (1), (1)")
	tk.MustExec("analyze table t")
	rows := tk.MustQuery("select version from mysql.stats_meta").Rows()
	require.Len(t, rows, 1)
	v1 := rows[0][0].(string)
	rows = tk.MustQuery("select version from mysql.stats_histograms").Rows()
	require.Len(t, rows, 2)
	v2 := rows[0][0].(string)
	require.Equal(t, v1, v2)
	v3 := rows[1][0].(string)
	require.Equal(t, v2, v3)
}

func TestAnalyzeLongString(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@session.tidb_analyze_version = 2;")
	tk.MustExec("create table t(a longtext);")
	tk.MustExec("insert into t value(repeat(\"a\",65536));")
	tk.MustExec("insert into t value(repeat(\"b\",65536));")
	tk.MustExec("analyze table t with 0 topn")
}

func TestOutdatedStatsCheck(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	handle.AutoAnalyzeMinCnt = 0
	defer func() {
		handle.AutoAnalyzeMinCnt = 1000
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", oriStart))
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", oriEnd))
	}()
	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")

	h := dom.StatsHandle()
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", 19)) // 20 rows
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	is := dom.InfoSchema()
	require.NoError(t, h.Update(is))
	// To pass the stats.Pseudo check in autoAnalyzeTable
	tk.MustExec("analyze table t")
	tk.MustExec("explain select * from t where a = 1")
	require.NoError(t, h.LoadNeededHistograms())

	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", 13)) // 34 rows
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	require.False(t, hasPseudoStats(tk.MustQuery("explain select * from t where a = 1").Rows()))

	tk.MustExec("insert into t values (1)") // 35 rows
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	require.True(t, hasPseudoStats(tk.MustQuery("explain select * from t where a = 1").Rows()))

	tk.MustExec("analyze table t")

	tk.MustExec("delete from t limit 24") // 11 rows
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	require.False(t, hasPseudoStats(tk.MustQuery("explain select * from t where a = 1").Rows()))

	tk.MustExec("delete from t limit 1") // 10 rows
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	require.True(t, hasPseudoStats(tk.MustQuery("explain select * from t where a = 1").Rows()))
}

func hasPseudoStats(rows [][]interface{}) bool {
	for i := range rows {
		if strings.Contains(rows[i][4].(string), "stats:pseudo") {
			return true
		}
	}
	return false
}

// TestNotLoadedStatsOnAllNULLCol makes sure that stats on a column that only contains NULLs can be used even when it's
// not loaded. This is reasonable because it makes no difference whether it's loaded or not.
func TestNotLoadedStatsOnAllNULLCol(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	h := dom.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1000)
	defer func() {
		h.SetLease(oriLease)
	}()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("create table t2(a int)")
	tk.MustExec("insert into t1 values(null), (null), (null), (null)")
	tk.MustExec("insert into t2 values(null), (null)")
	tk.MustExec("analyze table t1;")
	tk.MustExec("analyze table t2;")

	res := tk.MustQuery("explain format = 'brief' select * from t1 left join t2 on t1.a=t2.a order by t1.a, t2.a")
	res.Check(testkit.Rows(
		"Sort 4.00 root  test.t1.a, test.t2.a",
		"└─HashJoin 4.00 root  left outer join, equal:[eq(test.t1.a, test.t2.a)]",
		"  ├─TableReader(Build) 0.00 root  data:Selection",
		// If we are not using stats on this column (which means we use pseudo estimation), the row count for the Selection will become 2.
		"  │ └─Selection 0.00 cop[tikv]  not(isnull(test.t2.a))",
		"  │   └─TableFullScan 2.00 cop[tikv] table:t2 keep order:false",
		"  └─TableReader(Probe) 4.00 root  data:TableFullScan",
		"    └─TableFullScan 4.00 cop[tikv] table:t1 keep order:false"))

	res = tk.MustQuery("explain format = 'brief' select * from t2 left join t1 on t1.a=t2.a order by t1.a, t2.a")
	res.Check(testkit.Rows(
		"Sort 2.00 root  test.t1.a, test.t2.a",
		"└─HashJoin 2.00 root  left outer join, equal:[eq(test.t2.a, test.t1.a)]",
		// If we are not using stats on this column, the build side will become t2 because of smaller row count.
		"  ├─TableReader(Build) 0.00 root  data:Selection",
		// If we are not using stats on this column, the row count for the Selection will become 4.
		"  │ └─Selection 0.00 cop[tikv]  not(isnull(test.t1.a))",
		"  │   └─TableFullScan 4.00 cop[tikv] table:t1 keep order:false",
		"  └─TableReader(Probe) 2.00 root  data:TableFullScan",
		"    └─TableFullScan 2.00 cop[tikv] table:t2 keep order:false"))

	res = tk.MustQuery("explain format = 'brief' select * from t1 right join t2 on t1.a=t2.a order by t1.a, t2.a")
	res.Check(testkit.Rows(
		"Sort 2.00 root  test.t1.a, test.t2.a",
		"└─HashJoin 2.00 root  right outer join, equal:[eq(test.t1.a, test.t2.a)]",
		"  ├─TableReader(Build) 0.00 root  data:Selection",
		"  │ └─Selection 0.00 cop[tikv]  not(isnull(test.t1.a))",
		"  │   └─TableFullScan 4.00 cop[tikv] table:t1 keep order:false",
		"  └─TableReader(Probe) 2.00 root  data:TableFullScan",
		"    └─TableFullScan 2.00 cop[tikv] table:t2 keep order:false"))

	res = tk.MustQuery("explain format = 'brief' select * from t2 right join t1 on t1.a=t2.a order by t1.a, t2.a")
	res.Check(testkit.Rows(
		"Sort 4.00 root  test.t1.a, test.t2.a",
		"└─HashJoin 4.00 root  right outer join, equal:[eq(test.t2.a, test.t1.a)]",
		"  ├─TableReader(Build) 0.00 root  data:Selection",
		"  │ └─Selection 0.00 cop[tikv]  not(isnull(test.t2.a))",
		"  │   └─TableFullScan 2.00 cop[tikv] table:t2 keep order:false",
		"  └─TableReader(Probe) 4.00 root  data:TableFullScan",
		"    └─TableFullScan 4.00 cop[tikv] table:t1 keep order:false"))
}

func TestCrossValidationSelectivity(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	h := dom.StatsHandle()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@tidb_analyze_version = 1")
	tk.MustExec("create table t (a int, b int, c int, primary key (a, b) clustered)")
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	tk.MustExec("insert into t values (1,2,3), (1,4,5)")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("analyze table t")
	tk.MustQuery("explain format = 'brief' select * from t where a = 1 and b > 0 and b < 1000 and c > 1000").Check(testkit.Rows(
		"TableReader 0.00 root  data:Selection",
		"└─Selection 0.00 cop[tikv]  gt(test.t.c, 1000)",
		"  └─TableRangeScan 2.00 cop[tikv] table:t range:(1 0,1 1000), keep order:false"))
}
