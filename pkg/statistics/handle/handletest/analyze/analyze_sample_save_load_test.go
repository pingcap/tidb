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

package analyze

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestSampleBasedGlobalStatsSaveAndLoad verifies that:
//  1. ANALYZE TABLE t (all partitions) persists pruned samples in stats_global_merge_data.
//  2. ANALYZE TABLE t PARTITION pN loads saved samples for other partitions and rebuilds global stats.
//  3. Global stats row count reflects all partitions, not just the re-analyzed one.
func TestSampleBasedGlobalStatsSaveAndLoad(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("set @@tidb_partition_prune_mode = '" + string(variable.Dynamic) + "'")
	tk.MustExec("set @@tidb_enable_sample_based_global_stats = 1")

	// Verify settings took effect.
	require.Equal(t, "1", tk.MustQuery("select @@tidb_enable_sample_based_global_stats").Rows()[0][0].(string))
	require.Equal(t, "dynamic", tk.MustQuery("select @@tidb_partition_prune_mode").Rows()[0][0].(string))

	// Create a 4-partition table with distinct data per partition.
	tk.MustExec(`create table t_sample_save (a int, key(a)) partition by range (a) (
		partition p0 values less than (1000),
		partition p1 values less than (2000),
		partition p2 values less than (3000),
		partition p3 values less than (4000)
	)`)
	for i := 0; i < 4; i++ {
		base := i * 1000
		vals := fmt.Sprintf("insert into t_sample_save values (%d)", base)
		for j := 1; j < 200; j++ {
			vals += fmt.Sprintf(", (%d)", base+j)
		}
		tk.MustExec(vals)
	}

	// Step 1: Analyze entire table — should save 4 sample rows (one per partition).
	tk.MustExec("analyze table t_sample_save")

	// Debug: check all rows in stats_global_merge_data.
	allMergeRows := tk.MustQuery("select table_id, type, hist_id from mysql.stats_global_merge_data").Rows()
	t.Logf("stats_global_merge_data has %d rows after full analyze", len(allMergeRows))
	for _, r := range allMergeRows {
		t.Logf("  table_id=%v type=%v hist_id=%v", r[0], r[1], r[2])
	}

	rows := tk.MustQuery("select count(*) from mysql.stats_global_merge_data where type = 2").Rows()
	require.Equal(t, "4", rows[0][0].(string), "expected 4 saved partition samples after full analyze")

	// Record the global stats row count after full analyze.
	globalCountRows := tk.MustQuery("show stats_meta where table_name = 't_sample_save' and partition_name = 'global'").Rows()
	require.Len(t, globalCountRows, 1)
	globalCountAfterFull, err := strconv.ParseInt(globalCountRows[0][5].(string), 10, 64)
	require.NoError(t, err)
	require.Equal(t, int64(800), globalCountAfterFull)

	// Step 2: Insert more data into p0, then re-analyze only p0.
	tk.MustExec("insert into t_sample_save values (800), (801), (802), (803), (804)")
	tk.MustExec("analyze table t_sample_save partition p0")

	// The saved sample for p0 should be updated; other 3 stay. Still 4 rows total.
	rows = tk.MustQuery("select count(*) from mysql.stats_global_merge_data where type = 2").Rows()
	require.Equal(t, "4", rows[0][0].(string), "expected 4 saved samples after single-partition analyze")

	// Global stats should still reflect all partitions (loaded from saved samples).
	globalCountRows = tk.MustQuery("show stats_meta where table_name = 't_sample_save' and partition_name = 'global'").Rows()
	require.Len(t, globalCountRows, 1)
	globalCountAfterPartial, err := strconv.ParseInt(globalCountRows[0][5].(string), 10, 64)
	require.NoError(t, err)
	// p0 now has 205 rows, other partitions still 200 each → total 805.
	require.Equal(t, int64(805), globalCountAfterPartial)
}

// TestSampleSaveSchemaChange verifies that saved samples are gracefully skipped
// when the schema changes (column add/drop) between save and load.
func TestSampleSaveSchemaChange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("set @@tidb_partition_prune_mode = '" + string(variable.Dynamic) + "'")
	tk.MustExec("set @@tidb_enable_sample_based_global_stats = 1")

	tk.MustExec(`create table t_schema_change (a int, b int, key(a)) partition by range (a) (
		partition p0 values less than (1000),
		partition p1 values less than (2000)
	)`)
	for i := 0; i < 2; i++ {
		base := i * 1000
		vals := fmt.Sprintf("insert into t_schema_change values (%d, %d)", base, base)
		for j := 1; j < 100; j++ {
			vals += fmt.Sprintf(", (%d, %d)", base+j, base+j)
		}
		tk.MustExec(vals)
	}

	// Full analyze → saves samples for both partitions.
	tk.MustExec("analyze table t_schema_change")
	rows := tk.MustQuery("select count(*) from mysql.stats_global_merge_data where type = 2").Rows()
	require.Equal(t, "2", rows[0][0].(string))

	// Add a column → schema changes. The saved samples have a different FM sketch count.
	tk.MustExec("alter table t_schema_change add column c int default 0")

	// Analyze only p0 → should detect schema mismatch for p1's saved samples and
	// fall back to merge-based path. No crash or error expected.
	tk.MustExec("analyze table t_schema_change partition p0")

	// Global stats should still exist (merge-based fallback).
	globalRows := tk.MustQuery("show stats_meta where table_name = 't_schema_change' and partition_name = 'global'").Rows()
	require.Len(t, globalRows, 1)
}

// TestSampleSaveDisabledByDefault verifies that no samples are saved when the
// feature flag is off.
func TestSampleSaveDisabledByDefault(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("set @@tidb_partition_prune_mode = '" + string(variable.Dynamic) + "'")
	// Explicitly disable (default behavior).
	tk.MustExec("set @@tidb_enable_sample_based_global_stats = 0")

	tk.MustExec(`create table t_no_save (a int, key(a)) partition by range (a) (
		partition p0 values less than (1000),
		partition p1 values less than (2000)
	)`)
	tk.MustExec("insert into t_no_save values (1), (2), (3), (1001), (1002)")
	tk.MustExec("analyze table t_no_save")

	rows := tk.MustQuery("select count(*) from mysql.stats_global_merge_data where type = 2").Rows()
	require.Equal(t, "0", rows[0][0].(string), "no samples should be saved when feature is disabled")
}

// TestSampleSaveGCOnDrop verifies that saved samples are cleaned up when a
// partition or table is dropped.
func TestSampleSaveGCOnDrop(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("set @@tidb_partition_prune_mode = '" + string(variable.Dynamic) + "'")
	tk.MustExec("set @@tidb_enable_sample_based_global_stats = 1")

	tk.MustExec(`create table t_gc (a int, key(a)) partition by range (a) (
		partition p0 values less than (1000),
		partition p1 values less than (2000)
	)`)
	tk.MustExec("insert into t_gc values (1), (2), (1001), (1002)")
	tk.MustExec("analyze table t_gc")

	rows := tk.MustQuery("select count(*) from mysql.stats_global_merge_data where type = 2").Rows()
	require.Equal(t, "2", rows[0][0].(string))

	// Drop the table — GC should clean up all saved samples.
	tk.MustExec("drop table t_gc")
	require.NoError(t, dom.StatsHandle().GCStats(dom.InfoSchema(), 0))

	rows = tk.MustQuery("select count(*) from mysql.stats_global_merge_data where type = 2").Rows()
	require.Equal(t, "0", rows[0][0].(string), "samples should be cleaned up after table drop")
}

// BenchmarkAnalyzePartitionGlobalStats benchmarks the time to rebuild global
// stats when analyzing a single partition of a 100-partition table.
// It compares:
//   - "MergeBased": traditional merge-based global stats (feature flag off)
//   - "SampleBased/Cold": sample-based with no saved samples (first full analyze)
//   - "SampleBased/Warm": sample-based with saved samples (single partition re-analyze)
func BenchmarkAnalyzePartitionGlobalStats(b *testing.B) {
	const numPartitions = 100
	const rowsPerPartition = 500

	for _, bc := range []struct {
		name   string
		sample bool
		warmup bool // whether to do a full analyze first to populate saved samples
	}{
		{"MergeBased", false, true},
		{"SampleBased/Cold", true, false},
		{"SampleBased/Warm", true, true},
	} {
		b.Run(bc.name, func(b *testing.B) {
			store, dom := testkit.CreateMockStoreAndDomain(b)
			tk := testkit.NewTestKit(b, store)
			tk.MustExec("use test")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("set @@tidb_partition_prune_mode = '" + string(variable.Dynamic) + "'")
			if bc.sample {
				tk.MustExec("set @@tidb_enable_sample_based_global_stats = 1")
			} else {
				tk.MustExec("set @@tidb_enable_sample_based_global_stats = 0")
			}

			// Build partition DDL.
			ddl := "create table t_bench (a int, b int, key(a)) partition by range (a) ("
			for i := 0; i < numPartitions; i++ {
				if i > 0 {
					ddl += ", "
				}
				ddl += fmt.Sprintf("partition p%d values less than (%d)", i, (i+1)*10000)
			}
			ddl += ")"
			tk.MustExec(ddl)

			// Insert data into all partitions.
			for i := 0; i < numPartitions; i++ {
				base := i * 10000
				vals := fmt.Sprintf("(%d, %d)", base, base)
				for j := 1; j < rowsPerPartition; j++ {
					vals += fmt.Sprintf(", (%d, %d)", base+j, base+j)
				}
				tk.MustExec("insert into t_bench values " + vals)
			}

			if bc.warmup {
				// Full analyze populates all partition stats and saves samples.
				tk.MustExec("analyze table t_bench")
				require.NoError(b, dom.StatsHandle().Update(context.Background(), dom.InfoSchema()))
			}

			// Insert some new data into p0 so re-analyze is meaningful.
			tk.MustExec("insert into t_bench values (9900, 9900), (9901, 9901)")

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tk.MustExec("analyze table t_bench partition p0")
			}
			b.StopTimer()

			// Verify global stats exist.
			globalRows := tk.MustQuery("show stats_meta where table_name = 't_bench' and partition_name = 'global'").Rows()
			require.Len(b, globalRows, 1)
		})
	}
}

// BenchmarkFullAnalyzeGlobalStats benchmarks full table ANALYZE with N
// partitions, comparing merge-based vs sample-based global stats.
func BenchmarkFullAnalyzeGlobalStats(b *testing.B) {
	for _, numPartitions := range []int{10, 50, 100} {
		for _, sample := range []bool{false, true} {
			name := fmt.Sprintf("Partitions%d/", numPartitions)
			if sample {
				name += "SampleBased"
			} else {
				name += "MergeBased"
			}
			b.Run(name, func(b *testing.B) {
				store := testkit.CreateMockStore(b)
				tk := testkit.NewTestKit(b, store)
				tk.MustExec("use test")
				tk.MustExec("set @@tidb_analyze_version = 2")
				tk.MustExec("set @@tidb_partition_prune_mode = '" + string(variable.Dynamic) + "'")
				if sample {
					tk.MustExec("set @@tidb_enable_sample_based_global_stats = 1")
				} else {
					tk.MustExec("set @@tidb_enable_sample_based_global_stats = 0")
				}

				ddl := "create table t_full_bench (a int, key(a)) partition by range (a) ("
				for i := 0; i < numPartitions; i++ {
					if i > 0 {
						ddl += ", "
					}
					ddl += fmt.Sprintf("partition p%d values less than (%d)", i, (i+1)*10000)
				}
				ddl += ")"
				tk.MustExec(ddl)

				for i := 0; i < numPartitions; i++ {
					base := i * 10000
					vals := fmt.Sprintf("(%d)", base)
					for j := 1; j < 200; j++ {
						vals += fmt.Sprintf(", (%d)", base+j)
					}
					tk.MustExec("insert into t_full_bench values " + vals)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					tk.MustExec("analyze table t_full_bench")
				}
			})
		}
	}
}

// BenchmarkSinglePartitionAnalyze is a focused benchmark that measures only
// the single-partition analyze + global stats rebuild for a 100-partition table.
// This is the primary scenario where sample-based save/load provides speedup.
func BenchmarkSinglePartitionAnalyze(b *testing.B) {
	const numPartitions = 100
	const rowsPerPartition = 300

	for _, sample := range []bool{false, true} {
		name := "MergeBased"
		if sample {
			name = "SampleBased"
		}
		b.Run(name, func(b *testing.B) {
			store, dom := testkit.CreateMockStoreAndDomain(b)
			tk := testkit.NewTestKit(b, store)
			tk.MustExec("use test")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("set @@tidb_partition_prune_mode = '" + string(variable.Dynamic) + "'")
			if sample {
				tk.MustExec("set @@tidb_enable_sample_based_global_stats = 1")
			} else {
				tk.MustExec("set @@tidb_enable_sample_based_global_stats = 0")
			}

			ddl := "create table t_single (a int, b varchar(64), key(a)) partition by range (a) ("
			for i := 0; i < numPartitions; i++ {
				if i > 0 {
					ddl += ", "
				}
				ddl += fmt.Sprintf("partition p%d values less than (%d)", i, (i+1)*10000)
			}
			ddl += ")"
			tk.MustExec(ddl)

			for i := 0; i < numPartitions; i++ {
				base := i * 10000
				vals := fmt.Sprintf("(%d, 'val_%d')", base, base)
				for j := 1; j < rowsPerPartition; j++ {
					vals += fmt.Sprintf(", (%d, 'val_%d')", base+j, base+j)
				}
				tk.MustExec("insert into t_single values " + vals)
			}

			// Do a full analyze first to establish baseline stats (and save
			// samples for the sample-based path).
			tk.MustExec("analyze table t_single")
			require.NoError(b, dom.StatsHandle().Update(context.Background(), dom.InfoSchema()))

			// Verify global stats exist as baseline.
			globalRows := tk.MustQuery(
				"show stats_meta where table_name = 't_single' and partition_name = 'global'",
			).Rows()
			require.Len(b, globalRows, 1)
			globalCount, err := strconv.ParseInt(globalRows[0][5].(string), 10, 64)
			require.NoError(b, err)
			require.Equal(b, int64(numPartitions*rowsPerPartition), globalCount)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Re-analyze a single partition (p0). The global stats rebuild
				// is the bottleneck we're measuring.
				tk.MustExec("analyze table t_single partition p0")
			}
		})
	}
}
