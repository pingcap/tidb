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

package globalstats_test

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func testGlobalStats2(t *testing.T, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("set @@tidb_analyze_version=2")

	// int + (column & index with 1 column)
	tk.MustExec("drop table if exists tint")
	tk.MustExec("create table tint (c int, key(c)) partition by range (c) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec("insert into tint values (1), (2), (3), (4), (4), (5), (5), (5), (null), (11), (12), (13), (14), (15), (16), (16), (16), (16), (17), (17)")
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(true))
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
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(true))
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
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(true))
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
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(true))
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
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(true))
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

func testIssues24349(testKit *testkit.TestKit) {
	testKit.MustExec("create table t (a int, b int) partition by hash(a) partitions 3")
	testKit.MustExec("insert into t values (0, 3), (0, 3), (0, 3), (0, 2), (1, 1), (1, 2), (1, 2), (1, 2), (1, 3), (1, 4), (2, 1), (2, 1)")
	testKit.MustExec("analyze table t with 1 topn, 3 buckets")
	testKit.MustQuery("show stats_buckets where partition_name='global'").Check(testkit.Rows(
		"test t global a 0 0 2 2 0 2 0",
		"test t global b 0 0 3 1 1 2 0",
		"test t global b 0 1 10 1 4 4 0",
	))
}

func testGlobalStatsAndSQLBinding(tk *testkit.TestKit) {
	tk.MustExec("use test")
	tk.MustExec("create database test_global_stats")
	tk.MustExec("use test_global_stats")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("set tidb_cost_model_version=2")

	// hash and range and list partition
	tk.MustExec("create table thash(a int, b int, key(a)) partition by hash(a) partitions 4")
	tk.MustExec(`create table trange(a int, b int, key(a)) partition by range(a) (
		partition p0 values less than (200),
		partition p1 values less than (400),
		partition p2 values less than (600),
		partition p3 values less than (800),
		partition p4 values less than (1001))`)
	tk.MustExec(`create table tlist (a int, b int, key(a)) partition by list (a) (
		partition p0 values in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
		partition p1 values in (10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
		partition p2 values in (20, 21, 22, 23, 24, 25, 26, 27, 28, 29),
		partition p3 values in (30, 31, 32, 33, 34, 35, 36, 37, 38, 39),
		partition p4 values in (40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50))`)

	// construct some special data distribution
	vals := make([]string, 0, 1000)
	listVals := make([]string, 0, 1000)
	for i := 0; i < 1000; i++ {
		if i < 10 {
			// for hash and range partition, 1% of records are in [0, 100)
			vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(100), rand.Intn(100)))
			// for list partition, 1% of records are equal to 0
			listVals = append(listVals, "(0, 0)")
		} else {
			vals = append(vals, fmt.Sprintf("(%v, %v)", 100+rand.Intn(900), 100+rand.Intn(900)))
			listVals = append(listVals, fmt.Sprintf("(%v, %v)", 1+rand.Intn(50), 1+rand.Intn(50)))
		}
	}
	tk.MustExec("insert into thash values " + strings.Join(vals, ","))
	tk.MustExec("insert into trange values " + strings.Join(vals, ","))
	tk.MustExec("insert into tlist values " + strings.Join(listVals, ","))

	// before analyzing, the planner will choose TableScan to access the 1% of records
	tk.MustHavePlan("select * from thash where a<100", "TableFullScan")
	tk.MustHavePlan("select * from trange where a<100", "TableFullScan")
	tk.MustHavePlan("select * from tlist where a<1", "TableFullScan")

	tk.MustExec("analyze table thash")
	tk.MustExec("analyze table trange")
	tk.MustExec("analyze table tlist")

	tk.MustHavePlan("select * from thash where a<100", "TableFullScan")
	tk.MustHavePlan("select * from trange where a<100", "TableFullScan")
	tk.MustHavePlan("select * from tlist where a<1", "TableFullScan")

	// create SQL bindings
	tk.MustExec("create session binding for select * from thash where a<100 using select * from thash ignore index(a) where a<100")
	tk.MustExec("create session binding for select * from trange where a<100 using select * from trange ignore index(a) where a<100")
	tk.MustExec("create session binding for select * from tlist where a<100 using select * from tlist ignore index(a) where a<100")

	// use TableScan again since the Index(a) is ignored
	tk.MustHavePlan("select * from thash where a<100", "TableFullScan")
	tk.MustHavePlan("select * from trange where a<100", "TableFullScan")
	tk.MustHavePlan("select * from tlist where a<1", "TableFullScan")

	// drop SQL bindings
	tk.MustExec("drop session binding for select * from thash where a<100")
	tk.MustExec("drop session binding for select * from trange where a<100")
	tk.MustExec("drop session binding for select * from tlist where a<100")

	tk.MustHavePlan("select * from thash where a<100", "TableFullScan")
	tk.MustHavePlan("select * from trange where a<100", "TableFullScan")
	tk.MustHavePlan("select * from tlist where a<1", "TableFullScan")
}
