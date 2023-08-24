// Copyright 2023 PingCAP, Inc.
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

package globalstats

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestShowGlobalStats(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 0")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
	tk.MustExec("create table t (a int, key(a)) partition by hash(a) partitions 2")
	tk.MustExec("insert into t values (1), (2), (3), (4)")
	tk.MustExec("analyze table t with 1 buckets")
	require.Len(t, tk.MustQuery("show stats_meta").Rows(), 2)
	require.Len(t, tk.MustQuery("show stats_meta where partition_name='global'").Rows(), 0)
	require.Len(t, tk.MustQuery("show stats_buckets").Rows(), 4) // 2 partitions * (1 for the column_a and 1 for the index_a)
	require.Len(t, tk.MustQuery("show stats_buckets where partition_name='global'").Rows(), 0)
	require.Len(t, tk.MustQuery("show stats_histograms").Rows(), 4)
	require.Len(t, tk.MustQuery("show stats_histograms where partition_name='global'").Rows(), 0)
	require.Len(t, tk.MustQuery("show stats_healthy").Rows(), 2)
	require.Len(t, tk.MustQuery("show stats_healthy where partition_name='global'").Rows(), 0)

	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("analyze table t with 0 topn, 1 buckets")
	require.Len(t, tk.MustQuery("show stats_meta").Rows(), 3)
	require.Len(t, tk.MustQuery("show stats_meta where partition_name='global'").Rows(), 1)
	require.Len(t, tk.MustQuery("show stats_buckets").Rows(), 6)
	require.Len(t, tk.MustQuery("show stats_buckets where partition_name='global'").Rows(), 2)
	require.Len(t, tk.MustQuery("show stats_histograms").Rows(), 6)
	require.Len(t, tk.MustQuery("show stats_histograms where partition_name='global'").Rows(), 2)
	require.Len(t, tk.MustQuery("show stats_healthy").Rows(), 3)
	require.Len(t, tk.MustQuery("show stats_healthy where partition_name='global'").Rows(), 1)
}

func TestBuildGlobalLevelStats(t *testing.T) {
	store := testkit.CreateMockStore(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t, t1;")
	testKit.MustExec("set @@tidb_analyze_version = 2")
	testKit.MustExec("set @@tidb_partition_prune_mode = 'static';")
	testKit.MustExec("create table t(a int, b int, c int) PARTITION BY HASH(a) PARTITIONS 3;")
	testKit.MustExec("create table t1(a int);")
	testKit.MustExec("insert into t values(1,1,1),(3,12,3),(4,20,4),(2,7,2),(5,21,5);")
	testKit.MustExec("insert into t1 values(1),(3),(4),(2),(5);")
	testKit.MustExec("create index idx_t_ab on t(a, b);")
	testKit.MustExec("create index idx_t_b on t(b);")
	testKit.MustExec("analyze table t, t1;")
	result := testKit.MustQuery("show stats_meta where table_name = 't';").Sort()
	require.Len(t, result.Rows(), 3)
	require.Equal(t, "1", result.Rows()[0][5])
	require.Equal(t, "2", result.Rows()[1][5])
	require.Equal(t, "2", result.Rows()[2][5])
	result = testKit.MustQuery("show stats_histograms where table_name = 't';").Sort()
	require.Len(t, result.Rows(), 15)

	result = testKit.MustQuery("show stats_meta where table_name = 't1';").Sort()
	require.Len(t, result.Rows(), 1)
	require.Equal(t, "5", result.Rows()[0][5])
	result = testKit.MustQuery("show stats_histograms where table_name = 't1';").Sort()
	require.Len(t, result.Rows(), 1)

	// Test the 'dynamic' mode
	testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic';")
	testKit.MustExec("analyze table t, t1;")
	result = testKit.MustQuery("show stats_meta where table_name = 't'").Sort()
	require.Len(t, result.Rows(), 4)
	require.Equal(t, "5", result.Rows()[0][5])
	require.Equal(t, "1", result.Rows()[1][5])
	require.Equal(t, "2", result.Rows()[2][5])
	require.Equal(t, "2", result.Rows()[3][5])
	result = testKit.MustQuery("show stats_histograms where table_name = 't';").Sort()
	require.Len(t, result.Rows(), 20)

	result = testKit.MustQuery("show stats_meta where table_name = 't1';").Sort()
	require.Len(t, result.Rows(), 1)
	require.Equal(t, "5", result.Rows()[0][5])
	result = testKit.MustQuery("show stats_histograms where table_name = 't1';").Sort()
	require.Len(t, result.Rows(), 1)

	testKit.MustExec("analyze table t index idx_t_ab, idx_t_b;")
	result = testKit.MustQuery("show stats_meta where table_name = 't'").Sort()
	require.Len(t, result.Rows(), 4)
	require.Equal(t, "5", result.Rows()[0][5])
	require.Equal(t, "1", result.Rows()[1][5])
	require.Equal(t, "2", result.Rows()[2][5])
	require.Equal(t, "2", result.Rows()[3][5])
	result = testKit.MustQuery("show stats_histograms where table_name = 't';").Sort()
	require.Len(t, result.Rows(), 20)
}

func TestGlobalStatsHealthy(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`
create table t (
	a int,
	key(a)
)
partition by range (a) (
	partition p0 values less than (10),
	partition p1 values less than (20)
)`)

	checkModifyAndCount := func(gModify, gCount, p0Modify, p0Count, p1Modify, p1Count int) {
		rs := tk.MustQuery("show stats_meta").Rows()
		require.Equal(t, fmt.Sprintf("%v", gModify), rs[0][4].(string))  // global.modify_count
		require.Equal(t, fmt.Sprintf("%v", gCount), rs[0][5].(string))   // global.row_count
		require.Equal(t, fmt.Sprintf("%v", p0Modify), rs[1][4].(string)) // p0.modify_count
		require.Equal(t, fmt.Sprintf("%v", p0Count), rs[1][5].(string))  // p0.row_count
		require.Equal(t, fmt.Sprintf("%v", p1Modify), rs[2][4].(string)) // p1.modify_count
		require.Equal(t, fmt.Sprintf("%v", p1Count), rs[2][5].(string))  // p1.row_count
	}
	checkHealthy := func(gH, p0H, p1H int) {
		tk.MustQuery("show stats_healthy").Check(testkit.Rows(
			fmt.Sprintf("test t global %v", gH),
			fmt.Sprintf("test t p0 %v", p0H),
			fmt.Sprintf("test t p1 %v", p1H)))
	}

	tk.MustExec("set @@tidb_analyze_version=2")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("analyze table t")
	checkModifyAndCount(0, 0, 0, 0, 0, 0)
	checkHealthy(100, 100, 100)

	tk.MustExec("insert into t values (1), (2)") // update p0
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, dom.StatsHandle().Update(dom.InfoSchema()))
	checkModifyAndCount(2, 2, 2, 2, 0, 0)
	checkHealthy(0, 0, 100)

	tk.MustExec("insert into t values (11), (12), (13), (14)") // update p1
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, dom.StatsHandle().Update(dom.InfoSchema()))
	checkModifyAndCount(6, 6, 2, 2, 4, 4)
	checkHealthy(0, 0, 0)

	tk.MustExec("analyze table t")
	checkModifyAndCount(0, 6, 0, 2, 0, 4)
	checkHealthy(100, 100, 100)

	tk.MustExec("insert into t values (4), (5), (15), (16)") // update p0 and p1 together
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, dom.StatsHandle().Update(dom.InfoSchema()))
	checkModifyAndCount(4, 10, 2, 4, 2, 6)
	checkHealthy(33, 0, 50)
}

func TestGlobalStatsData(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`
create table t (
	a int,
	key(a)
)
partition by range (a) (
	partition p0 values less than (10),
	partition p1 values less than (20)
)`)
	tk.MustExec("set @@tidb_analyze_version=2")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("insert into t values (1), (2), (3), (4), (5), (6), (6), (null), (11), (12), (13), (14), (15), (16), (17), (18), (19), (19)")
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("analyze table t with 0 topn, 2 buckets")

	tk.MustQuery("select modify_count, count from mysql.stats_meta order by table_id asc").Check(
		testkit.Rows("0 18", "0 8", "0 10")) // global row-count = sum(partition row-count)

	// distinct, null_count, tot_col_size should be the sum of their values in partition-stats, and correlation should be 0
	tk.MustQuery("select distinct_count, null_count, tot_col_size, correlation=0 from mysql.stats_histograms where is_index=0 order by table_id asc").Check(
		testkit.Rows("15 1 17 1", "6 1 7 0", "9 0 10 0"))
	tk.MustQuery("select distinct_count, null_count, tot_col_size, correlation=0 from mysql.stats_histograms where is_index=1 order by table_id asc").Check(
		testkit.Rows("15 1 0 1", "6 1 7 1", "9 0 10 1"))

	tk.MustQuery("show stats_buckets where is_index=0").Check(
		// db table partition col is_idx bucket_id count repeats lower upper ndv
		testkit.Rows("test t global a 0 0 7 2 1 6 0",
			"test t global a 0 1 17 2 6 19 0",
			"test t p0 a 0 0 4 1 1 4 0",
			"test t p0 a 0 1 7 2 5 6 0",
			"test t p1 a 0 0 6 1 11 16 0",
			"test t p1 a 0 1 10 2 17 19 0"))
	tk.MustQuery("show stats_buckets where is_index=1").Check(
		testkit.Rows("test t global a 1 0 7 2 1 6 0",
			"test t global a 1 1 17 2 6 19 0",
			"test t p0 a 1 0 4 1 1 4 0",
			"test t p0 a 1 1 7 2 5 6 0",
			"test t p1 a 1 0 6 1 11 16 0",
			"test t p1 a 1 1 10 2 17 19 0"))
}

func TestGlobalStatsData2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("set @@tidb_analyze_version=2")

	// int + (column & index with 1 column)
	tk.MustExec("drop table if exists tint")
	tk.MustExec("create table tint (c int, key(c)) partition by range (c) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec("insert into tint values (1), (2), (3), (4), (4), (5), (5), (5), (null), (11), (12), (13), (14), (15), (16), (16), (16), (16), (17), (17)")
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("analyze table tint with 2 topn, 2 buckets")

	tk.MustQuery("select modify_count, count from mysql.stats_meta order by table_id asc").Check(testkit.Rows(
		"0 20",  // global: g.count = p0.count + p1.count
		"0 9",   // p0
		"0 11")) // p1

	tk.MustQuery("show stats_topn where table_name='tint' and is_index=0").Check(testkit.Rows(
		"test tint global c 0 5 3",
		"test tint global c 0 16 4",
		"test tint p0 c 0 4 2",
		"test tint p0 c 0 5 3",
		"test tint p1 c 0 16 4",
		"test tint p1 c 0 17 2"))

	tk.MustQuery("show stats_topn where table_name='tint' and is_index=1").Check(testkit.Rows(
		"test tint global c 1 5 3",
		"test tint global c 1 16 4",
		"test tint p0 c 1 4 2",
		"test tint p0 c 1 5 3",
		"test tint p1 c 1 16 4",
		"test tint p1 c 1 17 2"))

	tk.MustQuery("show stats_buckets where is_index=0").Check(testkit.Rows(
		// db, tbl, part, col, isIdx, bucketID, count, repeat, lower, upper, ndv
		"test tint global c 0 0 5 2 1 4 0", // bucket.ndv is not maintained for column histograms
		"test tint global c 0 1 12 2 17 17 0",
		"test tint p0 c 0 0 2 1 1 2 0",
		"test tint p0 c 0 1 3 1 3 3 0",
		"test tint p1 c 0 0 3 1 11 13 0",
		"test tint p1 c 0 1 5 1 14 15 0"))

	tk.MustQuery("select distinct_count, null_count, tot_col_size from mysql.stats_histograms where is_index=0 order by table_id asc").Check(
		testkit.Rows("12 1 19", // global, g = p0 + p1
			"5 1 8",   // p0
			"7 0 11")) // p1

	tk.MustQuery("show stats_buckets where is_index=1").Check(testkit.Rows(
		// db, tbl, part, col, isIdx, bucketID, count, repeat, lower, upper, ndv
		"test tint global c 1 0 5 2 1 4 0",    // 4 is popped from p0.TopN, so g.ndv = p0.ndv+1
		"test tint global c 1 1 12 2 17 17 0", // same with the column's
		"test tint p0 c 1 0 2 1 1 2 0",
		"test tint p0 c 1 1 3 1 3 3 0",
		"test tint p1 c 1 0 3 1 11 13 0",
		"test tint p1 c 1 1 5 1 14 15 0"))

	tk.MustQuery("select distinct_count, null_count from mysql.stats_histograms where is_index=1 order by table_id asc").Check(
		testkit.Rows("12 1", // global, g = p0 + p1
			"5 1",  // p0
			"7 0")) // p1

	// double + (column + index with 1 column)
	tk.MustExec("drop table if exists tdouble")
	tk.MustExec(`create table tdouble (a int, c double, key(c)) partition by range (a)` +
		`(partition p0 values less than(10),partition p1 values less than(20))`)
	tk.MustExec(`insert into tdouble values ` +
		`(1, 1), (2, 2), (3, 3), (4, 4), (4, 4), (5, 5), (5, 5), (5, 5), (null, null), ` + // values in p0
		`(11, 11), (12, 12), (13, 13), (14, 14), (15, 15), (16, 16), (16, 16), (16, 16), (16, 16), (17, 17), (17, 17)`) // values in p1
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("analyze table tdouble with 2 topn, 2 buckets")

	rs := tk.MustQuery("show stats_meta where table_name='tdouble'").Rows()
	require.Equal(t, "20", rs[0][5].(string)) // g.count = p0.count + p1.count
	require.Equal(t, "9", rs[1][5].(string))  // p0.count
	require.Equal(t, "11", rs[2][5].(string)) // p1.count

	tk.MustQuery("show stats_topn where table_name='tdouble' and is_index=0 and column_name='c'").Check(testkit.Rows(
		`test tdouble global c 0 5 3`,
		`test tdouble global c 0 16 4`,
		`test tdouble p0 c 0 4 2`,
		`test tdouble p0 c 0 5 3`,
		`test tdouble p1 c 0 16 4`,
		`test tdouble p1 c 0 17 2`))

	tk.MustQuery("show stats_topn where table_name='tdouble' and is_index=1 and column_name='c'").Check(testkit.Rows(
		`test tdouble global c 1 5 3`,
		`test tdouble global c 1 16 4`,
		`test tdouble p0 c 1 4 2`,
		`test tdouble p0 c 1 5 3`,
		`test tdouble p1 c 1 16 4`,
		`test tdouble p1 c 1 17 2`))

	tk.MustQuery("show stats_buckets where table_name='tdouble' and is_index=0 and column_name='c'").Check(testkit.Rows(
		// db, tbl, part, col, isIdx, bucketID, count, repeat, lower, upper, ndv
		"test tdouble global c 0 0 5 2 1 4 0", // bucket.ndv is not maintained for column histograms
		"test tdouble global c 0 1 12 2 17 17 0",
		"test tdouble p0 c 0 0 2 1 1 2 0",
		"test tdouble p0 c 0 1 3 1 3 3 0",
		"test tdouble p1 c 0 0 3 1 11 13 0",
		"test tdouble p1 c 0 1 5 1 14 15 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tdouble' and column_name='c' and is_index=0").Rows()
	require.Equal(t, "12", rs[0][6].(string)) // g.ndv = p0 + p1
	require.Equal(t, "5", rs[1][6].(string))
	require.Equal(t, "7", rs[2][6].(string))
	require.Equal(t, "1", rs[0][7].(string)) // g.null_count = p0 + p1
	require.Equal(t, "1", rs[1][7].(string))
	require.Equal(t, "0", rs[2][7].(string))

	tk.MustQuery("show stats_buckets where table_name='tdouble' and is_index=1 and column_name='c'").Check(testkit.Rows(
		// db, tbl, part, col, isIdx, bucketID, count, repeat, lower, upper, ndv
		"test tdouble global c 1 0 5 2 1 4 0", // 4 is popped from p0.TopN, so g.ndv = p0.ndv+1
		"test tdouble global c 1 1 12 2 17 17 0",
		"test tdouble p0 c 1 0 2 1 1 2 0",
		"test tdouble p0 c 1 1 3 1 3 3 0",
		"test tdouble p1 c 1 0 3 1 11 13 0",
		"test tdouble p1 c 1 1 5 1 14 15 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tdouble' and column_name='c' and is_index=1").Rows()
	require.Equal(t, "12", rs[0][6].(string)) // g.ndv = p0 + p1
	require.Equal(t, "5", rs[1][6].(string))
	require.Equal(t, "7", rs[2][6].(string))
	require.Equal(t, "1", rs[0][7].(string)) // g.null_count = p0 + p1
	require.Equal(t, "1", rs[1][7].(string))
	require.Equal(t, "0", rs[2][7].(string))

	// decimal + (column + index with 1 column)
	tk.MustExec("drop table if exists tdecimal")
	tk.MustExec(`create table tdecimal (a int, c decimal(10, 2), key(c)) partition by range (a)` +
		`(partition p0 values less than(10),partition p1 values less than(20))`)
	tk.MustExec(`insert into tdecimal values ` +
		`(1, 1), (2, 2), (3, 3), (4, 4), (4, 4), (5, 5), (5, 5), (5, 5), (null, null), ` + // values in p0
		`(11, 11), (12, 12), (13, 13), (14, 14), (15, 15), (16, 16), (16, 16), (16, 16), (16, 16), (17, 17), (17, 17)`) // values in p1
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("analyze table tdecimal with 2 topn, 2 buckets")

	rs = tk.MustQuery("show stats_meta where table_name='tdecimal'").Rows()
	require.Equal(t, "20", rs[0][5].(string)) // g.count = p0.count + p1.count
	require.Equal(t, "9", rs[1][5].(string))  // p0.count
	require.Equal(t, "11", rs[2][5].(string)) // p1.count

	tk.MustQuery("show stats_topn where table_name='tdecimal' and is_index=0 and column_name='c'").Check(testkit.Rows(
		`test tdecimal global c 0 5.00 3`,
		`test tdecimal global c 0 16.00 4`,
		`test tdecimal p0 c 0 4.00 2`,
		`test tdecimal p0 c 0 5.00 3`,
		`test tdecimal p1 c 0 16.00 4`,
		`test tdecimal p1 c 0 17.00 2`))

	tk.MustQuery("show stats_topn where table_name='tdecimal' and is_index=1 and column_name='c'").Check(testkit.Rows(
		`test tdecimal global c 1 5.00 3`,
		`test tdecimal global c 1 16.00 4`,
		`test tdecimal p0 c 1 4.00 2`,
		`test tdecimal p0 c 1 5.00 3`,
		`test tdecimal p1 c 1 16.00 4`,
		`test tdecimal p1 c 1 17.00 2`))

	tk.MustQuery("show stats_buckets where table_name='tdecimal' and is_index=0 and column_name='c'").Check(testkit.Rows(
		// db, tbl, part, col, isIdx, bucketID, count, repeat, lower, upper, ndv
		"test tdecimal global c 0 0 5 2 1.00 4.00 0", // bucket.ndv is not maintained for column histograms
		"test tdecimal global c 0 1 12 2 17.00 17.00 0",
		"test tdecimal p0 c 0 0 2 1 1.00 2.00 0",
		"test tdecimal p0 c 0 1 3 1 3.00 3.00 0",
		"test tdecimal p1 c 0 0 3 1 11.00 13.00 0",
		"test tdecimal p1 c 0 1 5 1 14.00 15.00 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tdecimal' and column_name='c' and is_index=0").Rows()
	require.Equal(t, "12", rs[0][6].(string)) // g.ndv = p0 + p1
	require.Equal(t, "5", rs[1][6].(string))
	require.Equal(t, "7", rs[2][6].(string))
	require.Equal(t, "1", rs[0][7].(string)) // g.null_count = p0 + p1
	require.Equal(t, "1", rs[1][7].(string))
	require.Equal(t, "0", rs[2][7].(string))

	tk.MustQuery("show stats_buckets where table_name='tdecimal' and is_index=1 and column_name='c'").Check(testkit.Rows(
		// db, tbl, part, col, isIdx, bucketID, count, repeat, lower, upper, ndv
		"test tdecimal global c 1 0 5 2 1.00 4.00 0", // 4 is popped from p0.TopN, so g.ndv = p0.ndv+1
		"test tdecimal global c 1 1 12 2 17.00 17.00 0",
		"test tdecimal p0 c 1 0 2 1 1.00 2.00 0",
		"test tdecimal p0 c 1 1 3 1 3.00 3.00 0",
		"test tdecimal p1 c 1 0 3 1 11.00 13.00 0",
		"test tdecimal p1 c 1 1 5 1 14.00 15.00 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tdecimal' and column_name='c' and is_index=1").Rows()
	require.Equal(t, "12", rs[0][6].(string)) // g.ndv = p0 + p1
	require.Equal(t, "5", rs[1][6].(string))
	require.Equal(t, "7", rs[2][6].(string))
	require.Equal(t, "1", rs[0][7].(string)) // g.null_count = p0 + p1
	require.Equal(t, "1", rs[1][7].(string))
	require.Equal(t, "0", rs[2][7].(string))

	// datetime + (column + index with 1 column)
	tk.MustExec("drop table if exists tdatetime")
	tk.MustExec(`create table tdatetime (a int, c datetime, key(c)) partition by range (a)` +
		`(partition p0 values less than(10),partition p1 values less than(20))`)
	tk.MustExec(`insert into tdatetime values ` +
		`(1, '2000-01-01'), (2, '2000-01-02'), (3, '2000-01-03'), (4, '2000-01-04'), (4, '2000-01-04'), (5, '2000-01-05'), (5, '2000-01-05'), (5, '2000-01-05'), (null, null), ` + // values in p0
		`(11, '2000-01-11'), (12, '2000-01-12'), (13, '2000-01-13'), (14, '2000-01-14'), (15, '2000-01-15'), (16, '2000-01-16'), (16, '2000-01-16'), (16, '2000-01-16'), (16, '2000-01-16'), (17, '2000-01-17'), (17, '2000-01-17')`) // values in p1
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("analyze table tdatetime with 2 topn, 2 buckets")

	rs = tk.MustQuery("show stats_meta where table_name='tdatetime'").Rows()
	require.Equal(t, "20", rs[0][5].(string)) // g.count = p0.count + p1.count
	require.Equal(t, "9", rs[1][5].(string))  // p0.count
	require.Equal(t, "11", rs[2][5].(string)) // p1.count

	tk.MustQuery("show stats_topn where table_name='tdatetime' and is_index=0 and column_name='c'").Check(testkit.Rows(
		`test tdatetime global c 0 2000-01-05 00:00:00 3`,
		`test tdatetime global c 0 2000-01-16 00:00:00 4`,
		`test tdatetime p0 c 0 2000-01-04 00:00:00 2`,
		`test tdatetime p0 c 0 2000-01-05 00:00:00 3`,
		`test tdatetime p1 c 0 2000-01-16 00:00:00 4`,
		`test tdatetime p1 c 0 2000-01-17 00:00:00 2`))

	tk.MustQuery("show stats_topn where table_name='tdatetime' and is_index=1 and column_name='c'").Check(testkit.Rows(
		`test tdatetime global c 1 2000-01-05 00:00:00 3`,
		`test tdatetime global c 1 2000-01-16 00:00:00 4`,
		`test tdatetime p0 c 1 2000-01-04 00:00:00 2`,
		`test tdatetime p0 c 1 2000-01-05 00:00:00 3`,
		`test tdatetime p1 c 1 2000-01-16 00:00:00 4`,
		`test tdatetime p1 c 1 2000-01-17 00:00:00 2`))

	tk.MustQuery("show stats_buckets where table_name='tdatetime' and is_index=0 and column_name='c'").Check(testkit.Rows(
		// db, tbl, part, col, isIdx, bucketID, count, repeat, lower, upper, ndv
		"test tdatetime global c 0 0 5 2 2000-01-01 00:00:00 2000-01-04 00:00:00 0", // bucket.ndv is not maintained for column histograms
		"test tdatetime global c 0 1 12 2 2000-01-17 00:00:00 2000-01-17 00:00:00 0",
		"test tdatetime p0 c 0 0 2 1 2000-01-01 00:00:00 2000-01-02 00:00:00 0",
		"test tdatetime p0 c 0 1 3 1 2000-01-03 00:00:00 2000-01-03 00:00:00 0",
		"test tdatetime p1 c 0 0 3 1 2000-01-11 00:00:00 2000-01-13 00:00:00 0",
		"test tdatetime p1 c 0 1 5 1 2000-01-14 00:00:00 2000-01-15 00:00:00 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tdatetime' and column_name='c' and is_index=0").Rows()
	require.Equal(t, "12", rs[0][6].(string)) // g.ndv = p0 + p1
	require.Equal(t, "5", rs[1][6].(string))
	require.Equal(t, "7", rs[2][6].(string))
	require.Equal(t, "1", rs[0][7].(string)) // g.null_count = p0 + p1
	require.Equal(t, "1", rs[1][7].(string))
	require.Equal(t, "0", rs[2][7].(string))

	tk.MustQuery("show stats_buckets where table_name='tdatetime' and is_index=1 and column_name='c'").Check(testkit.Rows(
		// db, tbl, part, col, isIdx, bucketID, count, repeat, lower, upper, ndv
		"test tdatetime global c 1 0 5 2 2000-01-01 00:00:00 2000-01-04 00:00:00 0", // 4 is popped from p0.TopN, so g.ndv = p0.ndv+1
		"test tdatetime global c 1 1 12 2 2000-01-17 00:00:00 2000-01-17 00:00:00 0",
		"test tdatetime p0 c 1 0 2 1 2000-01-01 00:00:00 2000-01-02 00:00:00 0",
		"test tdatetime p0 c 1 1 3 1 2000-01-03 00:00:00 2000-01-03 00:00:00 0",
		"test tdatetime p1 c 1 0 3 1 2000-01-11 00:00:00 2000-01-13 00:00:00 0",
		"test tdatetime p1 c 1 1 5 1 2000-01-14 00:00:00 2000-01-15 00:00:00 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tdatetime' and column_name='c' and is_index=1").Rows()
	require.Equal(t, "12", rs[0][6].(string)) // g.ndv = p0 + p1
	require.Equal(t, "5", rs[1][6].(string))
	require.Equal(t, "7", rs[2][6].(string))
	require.Equal(t, "1", rs[0][7].(string)) // g.null_count = p0 + p1
	require.Equal(t, "1", rs[1][7].(string))
	require.Equal(t, "0", rs[2][7].(string))

	// string + (column + index with 1 column)
	tk.MustExec("drop table if exists tstring")
	tk.MustExec(`create table tstring (a int, c varchar(32), key(c)) partition by range (a)` +
		`(partition p0 values less than(10),partition p1 values less than(20))`)
	tk.MustExec(`insert into tstring values ` +
		`(1, 'a1'), (2, 'a2'), (3, 'a3'), (4, 'a4'), (4, 'a4'), (5, 'a5'), (5, 'a5'), (5, 'a5'), (null, null), ` + // values in p0
		`(11, 'b11'), (12, 'b12'), (13, 'b13'), (14, 'b14'), (15, 'b15'), (16, 'b16'), (16, 'b16'), (16, 'b16'), (16, 'b16'), (17, 'b17'), (17, 'b17')`) // values in p1
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("analyze table tstring with 2 topn, 2 buckets")

	rs = tk.MustQuery("show stats_meta where table_name='tstring'").Rows()
	require.Equal(t, "20", rs[0][5].(string)) // g.count = p0.count + p1.count
	require.Equal(t, "9", rs[1][5].(string))  // p0.count
	require.Equal(t, "11", rs[2][5].(string)) // p1.count

	tk.MustQuery("show stats_topn where table_name='tstring' and is_index=0 and column_name='c'").Check(testkit.Rows(
		`test tstring global c 0 a5 3`,
		`test tstring global c 0 b16 4`,
		`test tstring p0 c 0 a4 2`,
		`test tstring p0 c 0 a5 3`,
		`test tstring p1 c 0 b16 4`,
		`test tstring p1 c 0 b17 2`))

	tk.MustQuery("show stats_topn where table_name='tstring' and is_index=1 and column_name='c'").Check(testkit.Rows(
		`test tstring global c 1 a5 3`,
		`test tstring global c 1 b16 4`,
		`test tstring p0 c 1 a4 2`,
		`test tstring p0 c 1 a5 3`,
		`test tstring p1 c 1 b16 4`,
		`test tstring p1 c 1 b17 2`))

	tk.MustQuery("show stats_buckets where table_name='tstring' and is_index=0 and column_name='c'").Check(testkit.Rows(
		// db, tbl, part, col, isIdx, bucketID, count, repeat, lower, upper, ndv
		"test tstring global c 0 0 5 2 a1 a4 0", // bucket.ndv is not maintained for column histograms
		"test tstring global c 0 1 12 2 b17 b17 0",
		"test tstring p0 c 0 0 2 1 a1 a2 0",
		"test tstring p0 c 0 1 3 1 a3 a3 0",
		"test tstring p1 c 0 0 3 1 b11 b13 0",
		"test tstring p1 c 0 1 5 1 b14 b15 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tstring' and column_name='c' and is_index=0").Rows()
	require.Equal(t, "12", rs[0][6].(string)) // g.ndv = p0 + p1
	require.Equal(t, "5", rs[1][6].(string))
	require.Equal(t, "7", rs[2][6].(string))
	require.Equal(t, "1", rs[0][7].(string)) // g.null_count = p0 + p1
	require.Equal(t, "1", rs[1][7].(string))
	require.Equal(t, "0", rs[2][7].(string))

	tk.MustQuery("show stats_buckets where table_name='tstring' and is_index=1 and column_name='c'").Check(testkit.Rows(
		// db, tbl, part, col, isIdx, bucketID, count, repeat, lower, upper, ndv
		"test tstring global c 1 0 5 2 a1 a4 0", // 4 is popped from p0.TopN, so g.ndv = p0.ndv+1
		"test tstring global c 1 1 12 2 b17 b17 0",
		"test tstring p0 c 1 0 2 1 a1 a2 0",
		"test tstring p0 c 1 1 3 1 a3 a3 0",
		"test tstring p1 c 1 0 3 1 b11 b13 0",
		"test tstring p1 c 1 1 5 1 b14 b15 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tstring' and column_name='c' and is_index=1").Rows()
	require.Equal(t, "12", rs[0][6].(string)) // g.ndv = p0 + p1
	require.Equal(t, "5", rs[1][6].(string))
	require.Equal(t, "7", rs[2][6].(string))
	require.Equal(t, "1", rs[0][7].(string)) // g.null_count = p0 + p1
	require.Equal(t, "1", rs[1][7].(string))
	require.Equal(t, "0", rs[2][7].(string))
}

func TestGlobalStatsData3(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("set @@tidb_analyze_version=2")

	// index(int, int)
	tk.MustExec("drop table if exists tintint")
	tk.MustExec("create table tintint (a int, b int, key(a, b)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec(`insert into tintint values ` +
		`(1, 1), (1, 2), (2, 1), (2, 2), (2, 3), (2, 3), (3, 1), (3, 1), (3, 1),` + // values in p0
		`(11, 1), (12, 1), (12, 2), (13, 1), (13, 1), (13, 2), (13, 2), (13, 2)`) // values in p1
	tk.MustExec("analyze table tintint with 2 topn, 2 buckets")

	rs := tk.MustQuery("show stats_meta where table_name='tintint'").Rows()
	require.Equal(t, "17", rs[0][5].(string)) // g.total = p0.total + p1.total
	require.Equal(t, "9", rs[1][5].(string))
	require.Equal(t, "8", rs[2][5].(string))

	tk.MustQuery("show stats_topn where table_name='tintint' and is_index=1").Check(testkit.Rows(
		"test tintint global a 1 (3, 1) 3",
		"test tintint global a 1 (13, 2) 3",
		"test tintint p0 a 1 (2, 3) 2",
		"test tintint p0 a 1 (3, 1) 3",
		"test tintint p1 a 1 (13, 1) 2",
		"test tintint p1 a 1 (13, 2) 3"))

	tk.MustQuery("show stats_buckets where table_name='tintint' and is_index=1").Check(testkit.Rows(
		"test tintint global a 1 0 6 2 (1, 1) (2, 3) 0",    // (2, 3) is popped into it
		"test tintint global a 1 1 11 2 (13, 1) (13, 1) 0", // (13, 1) is popped into it
		"test tintint p0 a 1 0 3 1 (1, 1) (2, 1) 0",
		"test tintint p0 a 1 1 4 1 (2, 2) (2, 2) 0",
		"test tintint p1 a 1 0 2 1 (11, 1) (12, 1) 0",
		"test tintint p1 a 1 1 3 1 (12, 2) (12, 2) 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tintint' and is_index=1").Rows()
	require.Equal(t, "11", rs[0][6].(string)) // g.ndv = p0.ndv + p1.ndv
	require.Equal(t, "6", rs[1][6].(string))
	require.Equal(t, "5", rs[2][6].(string))

	// index(int, string)
	tk.MustExec("drop table if exists tintstr")
	tk.MustExec("create table tintstr (a int, b varchar(32), key(a, b)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec(`insert into tintstr values ` +
		`(1, '1'), (1, '2'), (2, '1'), (2, '2'), (2, '3'), (2, '3'), (3, '1'), (3, '1'), (3, '1'),` + // values in p0
		`(11, '1'), (12, '1'), (12, '2'), (13, '1'), (13, '1'), (13, '2'), (13, '2'), (13, '2')`) // values in p1
	tk.MustExec("analyze table tintstr with 2 topn, 2 buckets")

	rs = tk.MustQuery("show stats_meta where table_name='tintstr'").Rows()
	require.Equal(t, "17", rs[0][5].(string)) // g.total = p0.total + p1.total
	require.Equal(t, "9", rs[1][5].(string))
	require.Equal(t, "8", rs[2][5].(string))

	tk.MustQuery("show stats_topn where table_name='tintstr' and is_index=1").Check(testkit.Rows(
		"test tintstr global a 1 (3, 1) 3",
		"test tintstr global a 1 (13, 2) 3",
		"test tintstr p0 a 1 (2, 3) 2",
		"test tintstr p0 a 1 (3, 1) 3",
		"test tintstr p1 a 1 (13, 1) 2",
		"test tintstr p1 a 1 (13, 2) 3"))

	tk.MustQuery("show stats_buckets where table_name='tintstr' and is_index=1").Check(testkit.Rows(
		"test tintstr global a 1 0 6 2 (1, 1) (2, 3) 0",    // (2, 3) is popped into it
		"test tintstr global a 1 1 11 2 (13, 1) (13, 1) 0", // (13, 1) is popped into it
		"test tintstr p0 a 1 0 3 1 (1, 1) (2, 1) 0",
		"test tintstr p0 a 1 1 4 1 (2, 2) (2, 2) 0",
		"test tintstr p1 a 1 0 2 1 (11, 1) (12, 1) 0",
		"test tintstr p1 a 1 1 3 1 (12, 2) (12, 2) 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tintstr' and is_index=1").Rows()
	require.Equal(t, "11", rs[0][6].(string)) // g.ndv = p0.ndv + p1.ndv
	require.Equal(t, "6", rs[1][6].(string))
	require.Equal(t, "5", rs[2][6].(string))

	// index(int, double)
	tk.MustExec("drop table if exists tintdouble")
	tk.MustExec("create table tintdouble (a int, b double, key(a, b)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec(`insert into tintdouble values ` +
		`(1, 1), (1, 2), (2, 1), (2, 2), (2, 3), (2, 3), (3, 1), (3, 1), (3, 1),` + // values in p0
		`(11, 1), (12, 1), (12, 2), (13, 1), (13, 1), (13, 2), (13, 2), (13, 2)`) // values in p1
	tk.MustExec("analyze table tintdouble with 2 topn, 2 buckets")

	rs = tk.MustQuery("show stats_meta where table_name='tintdouble'").Rows()
	require.Equal(t, "17", rs[0][5].(string)) // g.total = p0.total + p1.total
	require.Equal(t, "9", rs[1][5].(string))
	require.Equal(t, "8", rs[2][5].(string))

	tk.MustQuery("show stats_topn where table_name='tintdouble' and is_index=1").Check(testkit.Rows(
		"test tintdouble global a 1 (3, 1) 3",
		"test tintdouble global a 1 (13, 2) 3",
		"test tintdouble p0 a 1 (2, 3) 2",
		"test tintdouble p0 a 1 (3, 1) 3",
		"test tintdouble p1 a 1 (13, 1) 2",
		"test tintdouble p1 a 1 (13, 2) 3"))

	tk.MustQuery("show stats_buckets where table_name='tintdouble' and is_index=1").Check(testkit.Rows(
		"test tintdouble global a 1 0 6 2 (1, 1) (2, 3) 0",    // (2, 3) is popped into it
		"test tintdouble global a 1 1 11 2 (13, 1) (13, 1) 0", // (13, 1) is popped into it
		"test tintdouble p0 a 1 0 3 1 (1, 1) (2, 1) 0",
		"test tintdouble p0 a 1 1 4 1 (2, 2) (2, 2) 0",
		"test tintdouble p1 a 1 0 2 1 (11, 1) (12, 1) 0",
		"test tintdouble p1 a 1 1 3 1 (12, 2) (12, 2) 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tintdouble' and is_index=1").Rows()
	require.Equal(t, "11", rs[0][6].(string)) // g.ndv = p0.ndv + p1.ndv
	require.Equal(t, "6", rs[1][6].(string))
	require.Equal(t, "5", rs[2][6].(string))

	// index(double, decimal)
	tk.MustExec("drop table if exists tdoubledecimal")
	tk.MustExec("create table tdoubledecimal (a int, b decimal(30, 2), key(a, b)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec(`insert into tdoubledecimal values ` +
		`(1, 1), (1, 2), (2, 1), (2, 2), (2, 3), (2, 3), (3, 1), (3, 1), (3, 1),` + // values in p0
		`(11, 1), (12, 1), (12, 2), (13, 1), (13, 1), (13, 2), (13, 2), (13, 2)`) // values in p1
	tk.MustExec("analyze table tdoubledecimal with 2 topn, 2 buckets")

	rs = tk.MustQuery("show stats_meta where table_name='tdoubledecimal'").Rows()
	require.Equal(t, "17", rs[0][5].(string)) // g.total = p0.total + p1.total
	require.Equal(t, "9", rs[1][5].(string))
	require.Equal(t, "8", rs[2][5].(string))

	tk.MustQuery("show stats_topn where table_name='tdoubledecimal' and is_index=1").Check(testkit.Rows(
		"test tdoubledecimal global a 1 (3, 1.00) 3",
		"test tdoubledecimal global a 1 (13, 2.00) 3",
		"test tdoubledecimal p0 a 1 (2, 3.00) 2",
		"test tdoubledecimal p0 a 1 (3, 1.00) 3",
		"test tdoubledecimal p1 a 1 (13, 1.00) 2",
		"test tdoubledecimal p1 a 1 (13, 2.00) 3"))

	tk.MustQuery("show stats_buckets where table_name='tdoubledecimal' and is_index=1").Check(testkit.Rows(
		"test tdoubledecimal global a 1 0 6 2 (1, 1.00) (2, 3.00) 0",    // (2, 3) is popped into it
		"test tdoubledecimal global a 1 1 11 2 (13, 1.00) (13, 1.00) 0", // (13, 1) is popped into it
		"test tdoubledecimal p0 a 1 0 3 1 (1, 1.00) (2, 1.00) 0",
		"test tdoubledecimal p0 a 1 1 4 1 (2, 2.00) (2, 2.00) 0",
		"test tdoubledecimal p1 a 1 0 2 1 (11, 1.00) (12, 1.00) 0",
		"test tdoubledecimal p1 a 1 1 3 1 (12, 2.00) (12, 2.00) 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tdoubledecimal' and is_index=1").Rows()
	require.Equal(t, "11", rs[0][6].(string)) // g.ndv = p0.ndv + p1.ndv
	require.Equal(t, "6", rs[1][6].(string))
	require.Equal(t, "5", rs[2][6].(string))

	// index(string, datetime)
	tk.MustExec("drop table if exists tstrdt")
	tk.MustExec("create table tstrdt (a int, b datetime, key(a, b)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec(`insert into tstrdt values ` +
		`(1, '2000-01-01'), (1, '2000-01-02'), (2, '2000-01-01'), (2, '2000-01-02'), (2, '2000-01-03'), (2, '2000-01-03'), (3, '2000-01-01'), (3, '2000-01-01'), (3, '2000-01-01'),` + // values in p0
		`(11, '2000-01-01'), (12, '2000-01-01'), (12, '2000-01-02'), (13, '2000-01-01'), (13, '2000-01-01'), (13, '2000-01-02'), (13, '2000-01-02'), (13, '2000-01-02')`) // values in p1
	tk.MustExec("analyze table tstrdt with 2 topn, 2 buckets")

	rs = tk.MustQuery("show stats_meta where table_name='tstrdt'").Rows()
	require.Equal(t, "17", rs[0][5].(string)) // g.total = p0.total + p1.total
	require.Equal(t, "9", rs[1][5].(string))
	require.Equal(t, "8", rs[2][5].(string))

	tk.MustQuery("show stats_topn where table_name='tstrdt' and is_index=1").Check(testkit.Rows(
		"test tstrdt global a 1 (3, 2000-01-01 00:00:00) 3",
		"test tstrdt global a 1 (13, 2000-01-02 00:00:00) 3",
		"test tstrdt p0 a 1 (2, 2000-01-03 00:00:00) 2",
		"test tstrdt p0 a 1 (3, 2000-01-01 00:00:00) 3",
		"test tstrdt p1 a 1 (13, 2000-01-01 00:00:00) 2",
		"test tstrdt p1 a 1 (13, 2000-01-02 00:00:00) 3"))

	tk.MustQuery("show stats_buckets where table_name='tstrdt' and is_index=1").Check(testkit.Rows(
		"test tstrdt global a 1 0 6 2 (1, 2000-01-01 00:00:00) (2, 2000-01-03 00:00:00) 0",    // (2, 3) is popped into it
		"test tstrdt global a 1 1 11 2 (13, 2000-01-01 00:00:00) (13, 2000-01-01 00:00:00) 0", // (13, 1) is popped into it
		"test tstrdt p0 a 1 0 3 1 (1, 2000-01-01 00:00:00) (2, 2000-01-01 00:00:00) 0",
		"test tstrdt p0 a 1 1 4 1 (2, 2000-01-02 00:00:00) (2, 2000-01-02 00:00:00) 0",
		"test tstrdt p1 a 1 0 2 1 (11, 2000-01-01 00:00:00) (12, 2000-01-01 00:00:00) 0",
		"test tstrdt p1 a 1 1 3 1 (12, 2000-01-02 00:00:00) (12, 2000-01-02 00:00:00) 0"))

	rs = tk.MustQuery("show stats_histograms where table_name='tstrdt' and is_index=1").Rows()
	require.Equal(t, "11", rs[0][6].(string)) // g.ndv = p0.ndv + p1.ndv
	require.Equal(t, "6", rs[1][6].(string))
	require.Equal(t, "5", rs[2][6].(string))
}

func TestGlobalStatsVersion(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`
create table t (
	a int
)
partition by range (a) (
	partition p0 values less than (10),
	partition p1 values less than (20)
)`)
	require.NoError(t, dom.StatsHandle().HandleDDLEvent(<-dom.StatsHandle().DDLEventCh()))
	tk.MustExec("insert into t values (1), (5), (null), (11), (15)")
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))

	tk.MustExec("set @@tidb_partition_prune_mode='static'")
	tk.MustExec("set @@session.tidb_analyze_version=1")
	tk.MustExec("analyze table t") // both p0 and p1 are in ver1
	require.Len(t, tk.MustQuery("show stats_meta").Rows(), 2)

	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("set @@session.tidb_analyze_version=1")
	err := tk.ExecToErr("analyze table t") // try to build global-stats on ver1
	require.NoError(t, err)

	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("set @@session.tidb_analyze_version=2")
	err = tk.ExecToErr("analyze table t partition p1") // only analyze p1 to let it in ver2 while p0 is in ver1
	require.NoError(t, err)

	tk.MustExec("analyze table t") // both p0 and p1 are in ver2
	require.Len(t, tk.MustQuery("show stats_meta").Rows(), 3)

	// If we already have global-stats, we can get the latest global-stats by analyzing the newly added partition.
	tk.MustExec("alter table t add partition (partition p2 values less than (30))")
	tk.MustExec("insert t values (13), (14), (22), (23)")
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("analyze table t partition p2") // it will success since p0 and p1 are both in ver2
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))
	do := dom
	is := do.InfoSchema()
	h := do.StatsHandle()
	require.NoError(t, h.Update(is))
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	globalStats := h.GetTableStats(tableInfo)
	// global.count = p0.count(3) + p1.count(4) + p2.count(2)
	// modify count is 2 because we didn't analyze p1 after the second insert
	require.Equal(t, int64(9), globalStats.RealtimeCount)
	require.Equal(t, int64(2), globalStats.ModifyCount)

	tk.MustExec("analyze table t partition p1;")
	globalStats = h.GetTableStats(tableInfo)
	// global.count = p0.count(3) + p1.count(4) + p2.count(4)
	// The value of modify count is 0 now.
	require.Equal(t, int64(9), globalStats.RealtimeCount)
	require.Equal(t, int64(0), globalStats.ModifyCount)

	tk.MustExec("alter table t drop partition p2;")
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("analyze table t;")
	globalStats = h.GetTableStats(tableInfo)
	// global.count = p0.count(3) + p1.count(4)
	require.Equal(t, int64(7), globalStats.RealtimeCount)
}

func TestDDLPartition4GlobalStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@session.tidb_analyze_version=2")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec(`create table t (a int) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (20),
		partition p2 values less than (30),
		partition p3 values less than (40),
		partition p4 values less than (50),
		partition p5 values less than (60)
	)`)
	do := dom
	is := do.InfoSchema()
	h := do.StatsHandle()
	err := h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	require.NoError(t, h.Update(is))
	tk.MustExec("insert into t values (1), (2), (3), (4), (5), " +
		"(11), (21), (31), (41), (51)," +
		"(12), (22), (32), (42), (52);")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	tk.MustExec("analyze table t")
	result := tk.MustQuery("show stats_meta where table_name = 't';").Rows()
	require.Len(t, result, 7)
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	globalStats := h.GetTableStats(tableInfo)
	require.Equal(t, int64(15), globalStats.RealtimeCount)

	tk.MustExec("alter table t drop partition p3, p5;")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	require.NoError(t, h.Update(is))
	result = tk.MustQuery("show stats_meta where table_name = 't';").Rows()
	require.Len(t, result, 5)
	// The value of global.count will be updated automatically after we drop the table partition.
	globalStats = h.GetTableStats(tableInfo)
	require.Equal(t, int64(11), globalStats.RealtimeCount)

	tk.MustExec("alter table t truncate partition p2, p4;")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	require.NoError(t, h.Update(is))
	// The value of global.count will not be updated automatically when we truncate the table partition.
	// Because the partition-stats in the partition table which have been truncated has not been updated.
	globalStats = h.GetTableStats(tableInfo)
	require.Equal(t, int64(11), globalStats.RealtimeCount)

	tk.MustExec("analyze table t;")
	result = tk.MustQuery("show stats_meta where table_name = 't';").Rows()
	// The truncate operation only delete the data from the partition p2 and p4. It will not delete the partition-stats.
	require.Len(t, result, 5)
	// The result for the globalStats.count will be right now
	globalStats = h.GetTableStats(tableInfo)
	require.Equal(t, int64(7), globalStats.RealtimeCount)
}

func TestGlobalStatsNDV(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec(`CREATE TABLE t ( a int, key(a) )
	PARTITION BY RANGE (a) (
    PARTITION p0 VALUES LESS THAN (10),
    PARTITION p1 VALUES LESS THAN (20),
    PARTITION p2 VALUES LESS THAN (30),
    PARTITION p3 VALUES LESS THAN (40))`)

	checkNDV := func(ndvs ...int) { // g, p0, ..., p3
		tk.MustExec("analyze table t")
		rs := tk.MustQuery(`show stats_histograms where is_index=1`).Rows()
		require.Len(t, rs, 5)
		for i, ndv := range ndvs {
			require.Equal(t, fmt.Sprintf("%v", ndv), rs[i][6].(string))
		}
	}

	// all partitions are empty
	checkNDV(0, 0, 0, 0, 0)

	// p0 has data while others are empty
	tk.MustExec("insert into t values (1), (2), (3)")
	checkNDV(3, 3, 0, 0, 0)

	// p0, p1, p2 have data while p3 is empty
	tk.MustExec("insert into t values (11), (12), (13), (21), (22), (23)")
	checkNDV(9, 3, 3, 3, 0)

	// all partitions are not empty
	tk.MustExec("insert into t values (31), (32), (33), (34)")
	checkNDV(13, 3, 3, 3, 4)

	// insert some duplicated records
	tk.MustExec("insert into t values (31), (33), (34)")
	tk.MustExec("insert into t values (1), (2), (3)")
	checkNDV(13, 3, 3, 3, 4)
}

func TestGlobalStatsIndexNDV(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	checkNDV := func(tbl string, g int, ps ...int) { // g, p0, ..., p3
		tk.MustExec("analyze table " + tbl)
		rs := tk.MustQuery(fmt.Sprintf(`show stats_histograms where is_index=1 and table_name='%v'`, tbl)).Rows()
		require.Len(t, rs, 1+len(ps))                             // 1(global) + number of partitions
		require.Equal(t, fmt.Sprintf("%v", g), rs[0][6].(string)) // global
		for i, ndv := range ps {
			require.Equal(t, fmt.Sprintf("%v", ndv), rs[i+1][6].(string))
		}
	}

	// int
	tk.MustExec("drop table if exists tint")
	tk.MustExec(`CREATE TABLE tint ( a int, b int, key(b) )
	PARTITION BY RANGE (a) (
    PARTITION p0 VALUES LESS THAN (10),
    PARTITION p1 VALUES LESS THAN (20))`)
	tk.MustExec("insert into tint values (1, 1), (1, 2), (1, 3)") // p0.b: [1, 2, 3], p1.b: []
	checkNDV("tint", 3, 3, 0)
	tk.MustExec("insert into tint values (11, 1), (11, 2), (11, 3)") // p0.b: [1, 2, 3], p1.b: [1, 2, 3]
	checkNDV("tint", 3, 3, 3)
	tk.MustExec("insert into tint values (11, 4), (11, 5), (11, 6)") // p0.b: [1, 2, 3], p1.b: [1, 2, 3, 4, 5, 6]
	checkNDV("tint", 6, 3, 6)
	tk.MustExec("insert into tint values (1, 4), (1, 5), (1, 6), (1, 7), (1, 8)") // p0.b: [1, 2, 3, 4, 5, 6, 7, 8], p1.b: [1, 2, 3, 4, 5, 6]
	checkNDV("tint", 8, 8, 6)

	// double
	tk.MustExec("drop table if exists tdouble")
	tk.MustExec(`CREATE TABLE tdouble ( a int, b double, key(b) )
	PARTITION BY RANGE (a) (
    PARTITION p0 VALUES LESS THAN (10),
    PARTITION p1 VALUES LESS THAN (20))`)
	tk.MustExec("insert into tdouble values (1, 1.1), (1, 2.2), (1, 3.3)") // p0.b: [1, 2, 3], p1.b: []
	checkNDV("tdouble", 3, 3, 0)
	tk.MustExec("insert into tdouble values (11, 1.1), (11, 2.2), (11, 3.3)") // p0.b: [1, 2, 3], p1.b: [1, 2, 3]
	checkNDV("tdouble", 3, 3, 3)
	tk.MustExec("insert into tdouble values (11, 4.4), (11, 5.5), (11, 6.6)") // p0.b: [1, 2, 3], p1.b: [1, 2, 3, 4, 5, 6]
	checkNDV("tdouble", 6, 3, 6)
	tk.MustExec("insert into tdouble values (1, 4.4), (1, 5.5), (1, 6.6), (1, 7.7), (1, 8.8)") // p0.b: [1, 2, 3, 4, 5, 6, 7, 8], p1.b: [1, 2, 3, 4, 5, 6]
	checkNDV("tdouble", 8, 8, 6)

	// decimal
	tk.MustExec("drop table if exists tdecimal")
	tk.MustExec(`CREATE TABLE tdecimal ( a int, b decimal(30, 15), key(b) )
	PARTITION BY RANGE (a) (
    PARTITION p0 VALUES LESS THAN (10),
    PARTITION p1 VALUES LESS THAN (20))`)
	tk.MustExec("insert into tdecimal values (1, 1.1), (1, 2.2), (1, 3.3)") // p0.b: [1, 2, 3], p1.b: []
	checkNDV("tdecimal", 3, 3, 0)
	tk.MustExec("insert into tdecimal values (11, 1.1), (11, 2.2), (11, 3.3)") // p0.b: [1, 2, 3], p1.b: [1, 2, 3]
	checkNDV("tdecimal", 3, 3, 3)
	tk.MustExec("insert into tdecimal values (11, 4.4), (11, 5.5), (11, 6.6)") // p0.b: [1, 2, 3], p1.b: [1, 2, 3, 4, 5, 6]
	checkNDV("tdecimal", 6, 3, 6)
	tk.MustExec("insert into tdecimal values (1, 4.4), (1, 5.5), (1, 6.6), (1, 7.7), (1, 8.8)") // p0.b: [1, 2, 3, 4, 5, 6, 7, 8], p1.b: [1, 2, 3, 4, 5, 6]
	checkNDV("tdecimal", 8, 8, 6)

	// string
	tk.MustExec("drop table if exists tstring")
	tk.MustExec(`CREATE TABLE tstring ( a int, b varchar(30), key(b) )
	PARTITION BY RANGE (a) (
    PARTITION p0 VALUES LESS THAN (10),
    PARTITION p1 VALUES LESS THAN (20))`)
	tk.MustExec("insert into tstring values (1, '111'), (1, '222'), (1, '333')") // p0.b: [1, 2, 3], p1.b: []
	checkNDV("tstring", 3, 3, 0)
	tk.MustExec("insert into tstring values (11, '111'), (11, '222'), (11, '333')") // p0.b: [1, 2, 3], p1.b: [1, 2, 3]
	checkNDV("tstring", 3, 3, 3)
	tk.MustExec("insert into tstring values (11, '444'), (11, '555'), (11, '666')") // p0.b: [1, 2, 3], p1.b: [1, 2, 3, 4, 5, 6]
	checkNDV("tstring", 6, 3, 6)
	tk.MustExec("insert into tstring values (1, '444'), (1, '555'), (1, '666'), (1, '777'), (1, '888')") // p0.b: [1, 2, 3, 4, 5, 6, 7, 8], p1.b: [1, 2, 3, 4, 5, 6]
	checkNDV("tstring", 8, 8, 6)

	// datetime
	tk.MustExec("drop table if exists tdatetime")
	tk.MustExec(`CREATE TABLE tdatetime ( a int, b datetime, key(b) )
	PARTITION BY RANGE (a) (
    PARTITION p0 VALUES LESS THAN (10),
    PARTITION p1 VALUES LESS THAN (20))`)
	tk.MustExec("insert into tdatetime values (1, '2001-01-01'), (1, '2002-01-01'), (1, '2003-01-01')") // p0.b: [1, 2, 3], p1.b: []
	checkNDV("tdatetime", 3, 3, 0)
	tk.MustExec("insert into tdatetime values (11, '2001-01-01'), (11, '2002-01-01'), (11, '2003-01-01')") // p0.b: [1, 2, 3], p1.b: [1, 2, 3]
	checkNDV("tdatetime", 3, 3, 3)
	tk.MustExec("insert into tdatetime values (11, '2004-01-01'), (11, '2005-01-01'), (11, '2006-01-01')") // p0.b: [1, 2, 3], p1.b: [1, 2, 3, 4, 5, 6]
	checkNDV("tdatetime", 6, 3, 6)
	tk.MustExec("insert into tdatetime values (1, '2004-01-01'), (1, '2005-01-01'), (1, '2006-01-01'), (1, '2007-01-01'), (1, '2008-01-01')") // p0.b: [1, 2, 3, 4, 5, 6, 7, 8], p1.b: [1, 2, 3, 4, 5, 6]
	checkNDV("tdatetime", 8, 8, 6)
}
