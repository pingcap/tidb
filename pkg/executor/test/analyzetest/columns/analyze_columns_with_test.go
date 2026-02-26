// Copyright 2026 PingCAP, Inc.
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

package columns

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	statstestutil "github.com/pingcap/tidb/pkg/statistics/handle/ddl/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeColumnsWithPrimaryKey(t *testing.T) {
	for _, val := range []ast.ColumnChoice{ast.ColumnList, ast.PredicateColumns} {
		func(choice ast.ColumnChoice) {
			store, dom := testkit.CreateMockStoreAndDomain(t)

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("create table t (a int, b int, c int primary key)")
			statstestutil.HandleNextDDLEventWithTxn(h)
			tk.MustExec("insert into t values (1,1,1), (1,1,2), (2,2,3), (2,2,4), (3,3,5), (4,3,6), (5,4,7), (6,4,8), (null,null,9)")
			tk.MustExec("flush stats_delta")

			is := dom.InfoSchema()
			tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			tblID := tbl.Meta().ID

			switch choice {
			case ast.ColumnList:
				tk.MustExec("analyze table t columns a with 2 topn, 2 buckets")
				tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
					"Warning 1105 Columns c are missing in ANALYZE but their stats are needed for calculating stats for indexes/primary key/extended stats",
				))
			case ast.PredicateColumns:
				originalVal := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
				defer func() {
					tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal))
				}()
				tk.MustExec("select * from t where a > 1")
				require.NoError(t, h.DumpColStatsUsageToKV())
				rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_used_at is not null").Rows()
				require.Equal(t, 1, len(rows))
				require.Equal(t, "a", rows[0][3])
				tk.MustExec("analyze table t predicate columns with 2 topn, 2 buckets")
			}

			rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_analyzed_at is not null").Sort().Rows()
			require.Equal(t, 2, len(rows))
			require.Equal(t, "a", rows[0][3])
			require.Equal(t, "c", rows[1][3])

			tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 9"))
			tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_idx, value, count
				testkit.Rows("test t  a 0 1 2",
					"test t  a 0 2 2",
					"test t  c 0 1 1",
					"test t  c 0 2 1"))
			tk.MustQuery(fmt.Sprintf("select is_index, hist_id, distinct_count, null_count, tot_col_size, stats_ver, truncate(correlation,2) from mysql.stats_histograms where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 1 6 1 8 2 1",
					"0 2 0 0 0 0 0", // column b is not analyzed
					"0 3 9 0 9 2 1",
				))
			tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_index, bucket_id, count, repeats, lower, upper, ndv
				testkit.Rows("test t  a 0 0 3 1 3 5 0",
					"test t  a 0 1 4 1 6 6 0",
					"test t  c 0 0 4 1 3 6 0",
					"test t  c 0 1 7 1 7 9 0"))
		}(val)
	}
}

func TestAnalyzeColumnsWithIndex(t *testing.T) {
	for _, val := range []ast.ColumnChoice{ast.ColumnList, ast.PredicateColumns} {
		func(choice ast.ColumnChoice) {
			store, dom := testkit.CreateMockStoreAndDomain(t)

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("create table t (a int, b int, c int, d int, index idx_b_d(b, d))")
			statstestutil.HandleNextDDLEventWithTxn(h)
			tk.MustExec("insert into t values (1,1,null,1), (2,1,9,1), (1,1,8,1), (2,2,7,2), (1,3,7,3), (2,4,6,4), (1,4,6,5), (2,4,6,5), (1,5,6,5)")
			tk.MustExec("flush stats_delta")

			is := dom.InfoSchema()
			tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			tblID := tbl.Meta().ID

			switch choice {
			case ast.ColumnList:
				tk.MustExec("analyze table t columns c with 2 topn, 2 buckets")
				tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
					"Warning 1105 Columns b,d are missing in ANALYZE but their stats are needed for calculating stats for indexes/primary key/extended stats",
				))
			case ast.PredicateColumns:
				originalVal := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
				defer func() {
					tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal))
				}()
				tk.MustExec("select * from t where c > 1")
				require.NoError(t, h.DumpColStatsUsageToKV())
				rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_used_at is not null").Rows()
				require.Equal(t, 1, len(rows))
				require.Equal(t, "c", rows[0][3])
				tk.MustExec("analyze table t predicate columns with 2 topn, 2 buckets")
			}

			rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_analyzed_at is not null").Sort().Rows()
			require.Equal(t, 3, len(rows))
			require.Equal(t, "b", rows[0][3])
			require.Equal(t, "c", rows[1][3])
			require.Equal(t, "d", rows[2][3])

			tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 9"))
			tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_idx, value, count
				testkit.Rows("test t  b 0 1 3",
					"test t  b 0 4 3",
					"test t  c 0 6 4",
					"test t  c 0 7 2",
					"test t  d 0 1 3",
					"test t  d 0 5 3",
					"test t  idx_b_d 1 (1, 1) 3",
					"test t  idx_b_d 1 (4, 5) 2"))
			tk.MustQuery(fmt.Sprintf("select is_index, hist_id, distinct_count, null_count, tot_col_size, stats_ver, truncate(correlation,2) from mysql.stats_histograms where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 1 0 0 0 0 0", // column a is not analyzed
					"0 2 5 0 9 2 1",
					"0 3 4 1 8 2 -0.07",
					"0 4 5 0 9 2 1",
					"1 1 6 0 18 2 0"))
			tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_index, bucket_id, count, repeats, lower, upper, ndv
				testkit.Rows("test t  b 0 0 2 1 2 3 0",
					"test t  b 0 1 3 1 5 5 0",
					"test t  c 0 0 2 1 8 9 0",
					"test t  d 0 0 2 1 2 3 0",
					"test t  d 0 1 3 1 4 4 0",
					"test t  idx_b_d 1 0 3 1 (2, 2) (4, 4) 0",
					"test t  idx_b_d 1 1 4 1 (5, 5) (5, 5) 0"))
		}(val)
	}
}

func TestAnalyzeColumnsWithClusteredIndex(t *testing.T) {
	for _, val := range []ast.ColumnChoice{ast.ColumnList, ast.PredicateColumns} {
		func(choice ast.ColumnChoice) {
			store, dom := testkit.CreateMockStoreAndDomain(t)

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("create table t (a int, b int, c int, d int, primary key(b, d) clustered)")
			statstestutil.HandleNextDDLEventWithTxn(h)
			tk.MustExec("insert into t values (1,1,null,1), (2,2,9,2), (1,3,8,3), (2,4,7,4), (1,5,7,5), (2,6,6,6), (1,7,6,7), (2,8,6,8), (1,9,6,9)")
			tk.MustExec("flush stats_delta")

			is := dom.InfoSchema()
			tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			tblID := tbl.Meta().ID

			switch choice {
			case ast.ColumnList:
				tk.MustExec("analyze table t columns c with 2 topn, 2 buckets")
				tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
					"Warning 1105 Columns b,d are missing in ANALYZE but their stats are needed for calculating stats for indexes/primary key/extended stats",
				))
			case ast.PredicateColumns:
				originalVal := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
				defer func() {
					tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal))
				}()
				tk.MustExec("select * from t where c > 1")
				require.NoError(t, h.DumpColStatsUsageToKV())
				rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_used_at is not null").Rows()
				require.Equal(t, 1, len(rows))
				require.Equal(t, "c", rows[0][3])
				tk.MustExec("analyze table t predicate columns with 2 topn, 2 buckets")
			}

			rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_analyzed_at is not null").Sort().Rows()
			require.Equal(t, 3, len(rows))
			require.Equal(t, "b", rows[0][3])
			require.Equal(t, "c", rows[1][3])
			require.Equal(t, "d", rows[2][3])

			tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 9"))
			tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_idx, value, count
				testkit.Rows("test t  PRIMARY 1 (1, 1) 1",
					"test t  PRIMARY 1 (2, 2) 1",
					"test t  b 0 1 1",
					"test t  b 0 2 1",
					"test t  c 0 6 4",
					"test t  c 0 7 2",
					"test t  d 0 1 1",
					"test t  d 0 2 1"))
			tk.MustQuery(fmt.Sprintf("select is_index, hist_id, distinct_count, null_count, tot_col_size, stats_ver, truncate(correlation,2) from mysql.stats_histograms where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 1 0 0 0 0 0", // column a is not analyzed
					"0 2 9 0 9 2 1",
					"0 3 4 1 8 2 -0.07",
					"0 4 9 0 9 2 1",
					"1 1 9 0 18 2 0"))
			tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_index, bucket_id, count, repeats, lower, upper, ndv
				testkit.Rows("test t  PRIMARY 1 0 4 1 (3, 3) (6, 6) 0",
					"test t  PRIMARY 1 1 7 1 (7, 7) (9, 9) 0",
					"test t  b 0 0 4 1 3 6 0",
					"test t  b 0 1 7 1 7 9 0",
					"test t  c 0 0 2 1 8 9 0",
					"test t  d 0 0 4 1 3 6 0",
					"test t  d 0 1 7 1 7 9 0"))
		}(val)
	}
}

func TestAnalyzeColumnsWithDynamicPartitionTable(t *testing.T) {
	for _, val := range []ast.ColumnChoice{ast.ColumnList, ast.PredicateColumns} {
		func(choice ast.ColumnChoice) {
			store, dom := testkit.CreateMockStoreAndDomain(t)

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
			tk.MustExec("create table t (a int, b int, c int, index idx(c)) partition by range (a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
			statstestutil.HandleNextDDLEventWithTxn(h)
			tk.MustExec("insert into t values (1,2,1), (2,4,1), (3,6,1), (4,8,2), (4,8,2), (5,10,3), (5,10,4), (5,10,5), (null,null,6), (11,22,7), (12,24,8), (13,26,9), (14,28,10), (15,30,11), (16,32,12), (16,32,13), (16,32,13), (16,32,14), (17,34,14), (17,34,14)")
			tk.MustExec("flush stats_delta")

			is := dom.InfoSchema()
			tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			tblID := tbl.Meta().ID
			defs := tbl.Meta().Partition.Definitions
			p0ID := defs[0].ID
			p1ID := defs[1].ID

			switch choice {
			case ast.ColumnList:
				tk.MustExec("analyze table t columns a with 2 topn, 2 buckets")
				tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p0, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p1, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
					"Warning 1105 Columns c are missing in ANALYZE but their stats are needed for calculating stats for indexes/primary key/extended stats",
				))
			case ast.PredicateColumns:
				originalVal := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
				defer func() {
					tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal))
				}()
				tk.MustExec("select * from t where a < 1")
				require.NoError(t, h.DumpColStatsUsageToKV())
				rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_used_at is not null").Rows()
				require.Equal(t, 1, len(rows))
				require.Equal(t, []any{"test", "t", "global", "a"}, rows[0][:4])
				tk.MustExec("analyze table t predicate columns with 2 topn, 2 buckets")
			}

			rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_analyzed_at is not null").Sort().Rows()
			require.Equal(t, 6, len(rows))
			require.Equal(t, []any{"test", "t", "global", "a"}, rows[0][:4])
			require.Equal(t, []any{"test", "t", "global", "c"}, rows[1][:4])
			require.Equal(t, []any{"test", "t", "p0", "a"}, rows[2][:4])
			require.Equal(t, []any{"test", "t", "p0", "c"}, rows[3][:4])
			require.Equal(t, []any{"test", "t", "p1", "a"}, rows[4][:4])
			require.Equal(t, []any{"test", "t", "p1", "c"}, rows[5][:4])

			rows = tk.MustQuery("show stats_meta where db_name = 'test' and table_name = 't'").Sort().Rows()
			require.Equal(t, 3, len(rows))
			require.Equal(t, []any{"test", "t", "global", "0", "20"}, append(rows[0][:3], rows[0][4:6]...))
			require.Equal(t, []any{"test", "t", "p0", "0", "9"}, append(rows[1][:3], rows[1][4:6]...))
			require.Equal(t, []any{"test", "t", "p1", "0", "11"}, append(rows[2][:3], rows[2][4:6]...))

			tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't' and is_index = 0").Sort().Check(
				// db, tbl, part, col, is_idx, value, count
				testkit.Rows("test t global a 0 16 4",
					"test t global a 0 5 3",
					"test t global c 0 1 3",
					"test t global c 0 14 3",
					"test t p0 a 0 4 2",
					"test t p0 a 0 5 3",
					"test t p0 c 0 1 3",
					"test t p0 c 0 2 2",
					"test t p1 a 0 16 4",
					"test t p1 a 0 17 2",
					"test t p1 c 0 13 2",
					"test t p1 c 0 14 3"))

			tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't' and is_index = 1").Sort().Check(
				// db, tbl, part, col, is_idx, value, count
				testkit.Rows("test t global idx 1 1 3",
					"test t global idx 1 14 3",
					"test t p0 idx 1 1 3",
					"test t p0 idx 1 2 2",
					"test t p1 idx 1 13 2",
					"test t p1 idx 1 14 3"))

			tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't' and is_index = 0").Sort().Check(
				// db, tbl, part, col, is_index, bucket_id, count, repeats, lower, upper, ndv
				testkit.Rows("test t global a 0 0 5 2 1 4 0",
					"test t global a 0 1 12 2 11 17 0",
					"test t global c 0 0 6 1 2 6 0",
					"test t global c 0 1 14 2 7 13 0",
					"test t p0 a 0 0 2 1 1 2 0",
					"test t p0 a 0 1 3 1 3 3 0",
					"test t p0 c 0 0 3 1 3 5 0",
					"test t p0 c 0 1 4 1 6 6 0",
					"test t p1 a 0 0 3 1 11 13 0",
					"test t p1 a 0 1 5 1 14 15 0",
					"test t p1 c 0 0 4 1 7 10 0",
					"test t p1 c 0 1 6 1 11 12 0"))

			tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't' and is_index = 1").Sort().Check(
				// db, tbl, part, col, is_index, bucket_id, count, repeats, lower, upper, ndv
				testkit.Rows("test t global idx 1 0 6 1 2 6 0",
					"test t global idx 1 1 14 2 7 13 0",
					"test t p0 idx 1 0 3 1 3 5 0",
					"test t p0 idx 1 1 4 1 6 6 0",
					"test t p1 idx 1 0 4 1 7 10 0",
					"test t p1 idx 1 1 6 1 11 12 0"))

			tk.MustQuery("select table_id, is_index, hist_id, distinct_count, null_count, tot_col_size, stats_ver, truncate(correlation,2) from mysql.stats_histograms order by table_id, is_index, hist_id asc").Check(
				testkit.Rows(fmt.Sprintf("%d 0 1 12 1 19 2 0", tblID), // global, aA
					fmt.Sprintf("%d 0 2 0 0 0 0 0", tblID),   // global, b, not analyzed
					fmt.Sprintf("%d 0 3 14 0 20 2 0", tblID), // global, c
					fmt.Sprintf("%d 1 1 14 0 0 2 0", tblID),  // global, idx
					fmt.Sprintf("%d 0 1 5 1 8 2 1", p0ID),    // p0, a
					fmt.Sprintf("%d 0 2 0 0 0 0 0", p0ID),    // p0, b, not analyzed
					fmt.Sprintf("%d 0 3 6 0 9 2 1", p0ID),    // p0, c
					fmt.Sprintf("%d 1 1 6 0 9 2 0", p0ID),    // p0, idx
					fmt.Sprintf("%d 0 1 7 0 11 2 1", p1ID),   // p1, a
					fmt.Sprintf("%d 0 2 0 0 0 0 0", p1ID),    // p1, b, not analyzed
					fmt.Sprintf("%d 0 3 8 0 11 2 1", p1ID),   // p1, c
					fmt.Sprintf("%d 1 1 8 0 11 2 0", p1ID),   // p1, idx
				))
		}(val)
	}
}

func TestAnalyzeColumnsWithStaticPartitionTable(t *testing.T) {
	for _, val := range []ast.ColumnChoice{ast.ColumnList, ast.PredicateColumns} {
		func(choice ast.ColumnChoice) {
			store, dom := testkit.CreateMockStoreAndDomain(t)

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
			tk.MustExec("create table t (a int, b int, c int, index idx(c)) partition by range (a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
			statstestutil.HandleNextDDLEventWithTxn(h)
			tk.MustExec("insert into t values (1,2,1), (2,4,1), (3,6,1), (4,8,2), (4,8,2), (5,10,3), (5,10,4), (5,10,5), (null,null,6), (11,22,7), (12,24,8), (13,26,9), (14,28,10), (15,30,11), (16,32,12), (16,32,13), (16,32,13), (16,32,14), (17,34,14), (17,34,14)")
			tk.MustExec("flush stats_delta")

			is := dom.InfoSchema()
			tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			tblID := tbl.Meta().ID
			defs := tbl.Meta().Partition.Definitions
			p0ID := defs[0].ID
			p1ID := defs[1].ID

			switch choice {
			case ast.ColumnList:
				tk.MustExec("analyze table t columns a with 2 topn, 2 buckets")
				tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p0, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p1, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
					"Warning 1105 Columns c are missing in ANALYZE but their stats are needed for calculating stats for indexes/primary key/extended stats",
				))
			case ast.PredicateColumns:
				originalVal := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
				defer func() {
					tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal))
				}()
				tk.MustExec("select * from t where a < 1")
				require.NoError(t, h.DumpColStatsUsageToKV())
				rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_used_at is not null").Rows()
				require.Equal(t, 1, len(rows))
				require.Equal(t, []any{"test", "t", "global", "a"}, rows[0][:4])
				tk.MustExec("analyze table t predicate columns with 2 topn, 2 buckets")
			}

			rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_analyzed_at is not null").Sort().Rows()
			require.Equal(t, 4, len(rows))
			require.Equal(t, []any{"test", "t", "p0", "a"}, rows[0][:4])
			require.Equal(t, []any{"test", "t", "p0", "c"}, rows[1][:4])
			require.Equal(t, []any{"test", "t", "p1", "a"}, rows[2][:4])
			require.Equal(t, []any{"test", "t", "p1", "c"}, rows[3][:4])

			rows = tk.MustQuery("show stats_meta where db_name = 'test' and table_name = 't'").Sort().Rows()
			require.Equal(t, 2, len(rows))
			require.Equal(t, []any{"test", "t", "p0", "0", "9"}, append(rows[0][:3], rows[0][4:6]...))
			require.Equal(t, []any{"test", "t", "p1", "0", "11"}, append(rows[1][:3], rows[1][4:6]...))

			tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't' and is_index = 0").Sort().Check(
				// db, tbl, part, col, is_idx, value, count
				testkit.Rows("test t p0 a 0 4 2",
					"test t p0 a 0 5 3",
					"test t p0 c 0 1 3",
					"test t p0 c 0 2 2",
					"test t p1 a 0 16 4",
					"test t p1 a 0 17 2",
					"test t p1 c 0 13 2",
					"test t p1 c 0 14 3"))

			tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't' and is_index = 1").Sort().Check(
				// db, tbl, part, col, is_idx, value, count
				testkit.Rows("test t p0 idx 1 1 3",
					"test t p0 idx 1 2 2",
					"test t p1 idx 1 13 2",
					"test t p1 idx 1 14 3"))

			tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't' and is_index = 0").Sort().Check(
				// db, tbl, part, col, is_index, bucket_id, count, repeats, lower, upper, ndv
				testkit.Rows("test t p0 a 0 0 2 1 1 2 0",
					"test t p0 a 0 1 3 1 3 3 0",
					"test t p0 c 0 0 3 1 3 5 0",
					"test t p0 c 0 1 4 1 6 6 0",
					"test t p1 a 0 0 3 1 11 13 0",
					"test t p1 a 0 1 5 1 14 15 0",
					"test t p1 c 0 0 4 1 7 10 0",
					"test t p1 c 0 1 6 1 11 12 0"))

			tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't' and is_index = 1").Sort().Check(
				// db, tbl, part, col, is_index, bucket_id, count, repeats, lower, upper, ndv
				testkit.Rows("test t p0 idx 1 0 3 1 3 5 0",
					"test t p0 idx 1 1 4 1 6 6 0",
					"test t p1 idx 1 0 4 1 7 10 0",
					"test t p1 idx 1 1 6 1 11 12 0"))

			// The single-column non-prefix index idx(c) is consolidated with column c,
			// so no is_index=1 rows exist in stats_histograms.
			tk.MustQuery("select table_id, is_index, hist_id, distinct_count, null_count, tot_col_size, stats_ver, truncate(correlation,2) from mysql.stats_histograms order by table_id, is_index, hist_id asc").Check(
				testkit.Rows(fmt.Sprintf("%d 0 1 0 0 0 0 0", tblID), // global, a, not analyzed
					fmt.Sprintf("%d 0 2 0 0 0 0 0", tblID), // global, b, not analyzed
					fmt.Sprintf("%d 0 3 0 0 0 0 0", tblID), // global, c, not analyzed
					fmt.Sprintf("%d 0 1 5 1 8 2 1", p0ID),  // p0, a
					fmt.Sprintf("%d 0 2 0 0 0 0 0", p0ID),  // p0, b, not analyzed
					fmt.Sprintf("%d 0 3 6 0 9 2 1", p0ID),  // p0, c
					fmt.Sprintf("%d 0 1 7 0 11 2 1", p1ID), // p1, a
					fmt.Sprintf("%d 0 2 0 0 0 0 0", p1ID),  // p1, b, not analyzed
					fmt.Sprintf("%d 0 3 8 0 11 2 1", p1ID), // p1, c
				))
		}(val)
	}
}

func TestAnalyzeColumnsWithExtendedStats(t *testing.T) {
	for _, val := range []ast.ColumnChoice{ast.ColumnList, ast.PredicateColumns} {
		func(choice ast.ColumnChoice) {
			store, dom := testkit.CreateMockStoreAndDomain(t)

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("set @@tidb_enable_extended_stats = on")
			tk.MustExec("create table t (a int, b int, c int)")
			statstestutil.HandleNextDDLEventWithTxn(h)
			tk.MustExec("alter table t add stats_extended s1 correlation(b,c)")
			tk.MustExec("insert into t values (5,1,1), (4,2,2), (3,3,3), (2,4,4), (1,5,5)")
			tk.MustExec("flush stats_delta")

			is := dom.InfoSchema()
			tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			tblID := tbl.Meta().ID

			switch choice {
			case ast.ColumnList:
				tk.MustExec("analyze table t columns b with 2 topn, 2 buckets")
				tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
					"Warning 1105 Columns c are missing in ANALYZE but their stats are needed for calculating stats for indexes/primary key/extended stats",
				))
			case ast.PredicateColumns:
				originalVal := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
				defer func() {
					tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal))
				}()
				tk.MustExec("select * from t where b > 1")
				require.NoError(t, h.DumpColStatsUsageToKV())
				rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_used_at is not null").Rows()
				require.Equal(t, 1, len(rows))
				require.Equal(t, "b", rows[0][3])
				tk.MustExec("analyze table t predicate columns with 2 topn, 2 buckets")
			}
			rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_analyzed_at is not null").Sort().Rows()
			require.Equal(t, 2, len(rows))
			require.Equal(t, "b", rows[0][3])
			require.Equal(t, "c", rows[1][3])

			tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 5"))
			tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_idx, value, count
				testkit.Rows("test t  b 0 1 1",
					"test t  b 0 2 1",
					"test t  c 0 1 1",
					"test t  c 0 2 1"))
			tk.MustQuery(fmt.Sprintf("select is_index, hist_id, distinct_count, null_count, tot_col_size, stats_ver, truncate(correlation,2) from mysql.stats_histograms where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 1 0 0 0 0 0", // column a is not analyzed
					"0 2 5 0 5 2 1",
					"0 3 5 0 5 2 1",
				))
			tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_index, bucket_id, count, repeats, lower, upper, ndv
				testkit.Rows("test t  b 0 0 2 1 3 4 0",
					"test t  b 0 1 3 1 5 5 0",
					"test t  c 0 0 2 1 3 4 0",
					"test t  c 0 1 3 1 5 5 0"))
			rows = tk.MustQuery("show stats_extended where db_name = 'test' and table_name = 't'").Rows()
			require.Equal(t, 1, len(rows))
			require.Equal(t, []any{"test", "t", "s1", "[b,c]", "correlation", "1.000000"}, rows[0][:len(rows[0])-1])
		}(val)
	}
}

func TestAnalyzeColumnsWithVirtualColumnIndex(t *testing.T) {
	for _, val := range []ast.ColumnChoice{ast.ColumnList, ast.PredicateColumns} {
		func(choice ast.ColumnChoice) {
			store, dom := testkit.CreateMockStoreAndDomain(t)

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("create table t (a int, b int, c int as (b+1), index idx(c))")
			statstestutil.HandleNextDDLEventWithTxn(h)
			tk.MustExec("insert into t (a,b) values (1,1), (2,2), (3,3), (4,4), (5,4), (6,5), (7,5), (8,5), (null,null)")
			tk.MustExec("flush stats_delta")

			is := dom.InfoSchema()
			tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			tblID := tbl.Meta().ID

			switch choice {
			case ast.ColumnList:
				tk.MustExec("analyze table t columns b with 2 topn, 2 buckets")
				tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
					"Warning 1105 Columns c are missing in ANALYZE but their stats are needed for calculating stats for indexes/primary key/extended stats",
				))
			case ast.PredicateColumns:
				originalVal := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
				defer func() {
					tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal))
				}()
				tk.MustExec("select * from t where b > 1")
				require.NoError(t, h.DumpColStatsUsageToKV())
				rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_used_at is not null").Rows()
				require.Equal(t, 1, len(rows))
				require.Equal(t, "b", rows[0][3])
				tk.MustExec("analyze table t predicate columns with 2 topn, 2 buckets")
			}
			// virtual column c is skipped when dumping stats into disk, so only the stats of column b are updated
			rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_analyzed_at is not null").Rows()
			require.Equal(t, 1, len(rows))
			require.Equal(t, "b", rows[0][3])

			tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 9"))
			tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_idx, value, count
				testkit.Rows("test t  b 0 4 2",
					"test t  b 0 5 3",
					"test t  idx 1 5 2",
					"test t  idx 1 6 3"))
			tk.MustQuery(fmt.Sprintf("select is_index, hist_id, distinct_count, null_count, stats_ver, truncate(correlation,2) from mysql.stats_histograms where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 1 0 0 0 0", // column a is not analyzed
					"0 2 5 1 2 1",
					"0 3 0 0 0 0", // column c is not analyzed
					"1 1 5 1 2 0"))
			tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_index, bucket_id, count, repeats, lower, upper, ndv
				testkit.Rows("test t  b 0 0 2 1 1 2 0",
					"test t  b 0 1 3 1 3 3 0",
					"test t  idx 1 0 2 1 2 3 0",
					"test t  idx 1 1 3 1 4 4 0"))
		}(val)
	}
}
