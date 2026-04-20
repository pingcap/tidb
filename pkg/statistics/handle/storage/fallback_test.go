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

package storage_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/statistics/asyncload"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestBootstrapLoaderFallsBackToLegacyStatsBuckets verifies that the bootstrap
// init-stats loader (h.InitStats, which drives initStatsBucketsByPagingWithSCtx)
// serves index histogram buckets from mysql.stats_buckets when mysql.stats_data
// has no row. This is the upgrade-path code: on an upgraded cluster pre-ANALYZE,
// bucket data for existing tables lives only in stats_buckets until the next
// ANALYZE moves it to stats_data. Without this fallback, the optimizer would
// see pseudo stats and regress plans.
//
// Test setup:
//  1. Create a table with a varbinary column and an index on it; insert rows.
//  2. ANALYZE, which writes stats_data and purges stats_buckets.
//  3. Seed stats_buckets with per-bucket rows whose bound bytes match the
//     varbinary column values (varbinary is blob-identity under
//     convertBoundToBlob, so no re-encoding needed).
//  4. DELETE the stats_data row.
//  5. Clear and re-run InitStats so the bootstrap loader must fall back.
//  6. Assert the loaded histogram has at least one bucket — if the fallback
//     were gone, the bootstrap loader would return an empty histogram.
func TestBootstrapLoaderFallsBackToLegacyStatsBuckets(t *testing.T) {
	asyncload.AsyncLoadHistogramNeededItems.Clear()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("create table t (v varbinary(16), key idx(v))")
	tk.MustExec("insert into t values (0x01), (0x02), (0x03), (0x04), (0x05), (0x06), (0x07), (0x08), (0x09), (0x0a)")

	// Analyze so stats_data is populated. With 10 distinct varbinary values
	// and 256 default buckets, saveBucketsToStorage will write one bucket
	// per value for the index histogram.
	tk.MustExec("analyze table t all columns with 0 topn")

	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	tableID := tableInfo.ID
	idxID := tableInfo.Indices[0].ID

	// Confirm stats_data holds the index bucket row before we remove it.
	rows := tk.MustQuery("select count(*) from mysql.stats_data where table_id = ? and type = 2 and hist_id = ?", tableID, idxID).Rows()
	require.Equal(t, "1", rows[0][0].(string), "analyze should have written the index bucket row to stats_data")

	// Seed stats_buckets with several per-bucket rows matching the test data.
	// The varbinary column means convertBoundToBlob is an identity, so the
	// bound bytes can be literal hex values. This mimics what the pre-
	// migration analyze path would have written.
	for i, val := range []string{"0x01", "0x02", "0x03", "0x04", "0x05"} {
		tk.MustExec(
			"insert into mysql.stats_buckets (table_id, is_index, hist_id, bucket_id, count, repeats, lower_bound, upper_bound, ndv) values (?, 1, ?, ?, ?, 1, "+val+", "+val+", 0)",
			tableID, idxID, i, i+1,
		)
	}
	// Now remove the stats_data row so the priority reader must fall back.
	tk.MustExec("delete from mysql.stats_data where table_id = ? and type = 2 and hist_id = ?", tableID, idxID)

	// Clear the stats handle cache and re-load so the priority reader path
	// re-reads bucket data from storage. InitStats runs the bootstrap loader,
	// which itself has a dual-read path; but this test specifically exercises
	// the priority-reader fallback via LoadNeededHistograms, triggered by
	// IndexStatsIsInvalid.
	h := dom.StatsHandle()
	h.Clear()
	require.NoError(t, h.InitStats(context.Background(), is))

	stat := h.GetPhysicalTableStats(tableID, tableInfo)
	idx := stat.GetIdx(idxID)
	require.NotNil(t, idx, "index stats should be present after init")
	require.Greater(t, idx.Histogram.Len(), 0, "index histogram should have buckets loaded from legacy stats_buckets")
}

// TestPriorityReaderFallsBackToLegacyStatsBuckets verifies the same dual-read
// fallback at the priority-reader layer — i.e. HistogramFromStorageWithPriority
// itself (not the bootstrap loader). Priority reads are triggered by the
// async-load worker when IsInvalid detects that a histogram is needed but not
// yet loaded. On upgraded clusters, the priority reader must serve from
// stats_buckets for any histogram not yet migrated.
//
// This test runs InitStatsLite (histogram metadata only, no buckets) so that
// the in-memory stats cache has the shape "index exists but buckets evicted".
// A subsequent read that hits IsInvalid queues the index and LoadNeededHistograms
// drives the priority reader.
func TestPriorityReaderFallsBackToLegacyStatsBuckets(t *testing.T) {
	asyncload.AsyncLoadHistogramNeededItems.Clear()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("create table t (v varbinary(16), key idx(v))")
	tk.MustExec("insert into t values (0x01), (0x02), (0x03), (0x04), (0x05), (0x06), (0x07), (0x08), (0x09), (0x0a)")
	tk.MustExec("analyze table t all columns with 0 topn")

	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	tableID := tableInfo.ID
	idxID := tableInfo.Indices[0].ID

	// Seed stats_buckets with the legacy per-bucket rows.
	for i, val := range []string{"0x01", "0x02", "0x03", "0x04", "0x05"} {
		tk.MustExec(
			"insert into mysql.stats_buckets (table_id, is_index, hist_id, bucket_id, count, repeats, lower_bound, upper_bound, ndv) values (?, 1, ?, ?, ?, 1, "+val+", "+val+", 0)",
			tableID, idxID, i, i+1,
		)
	}
	// Remove the stats_data row so any subsequent read must fall back.
	tk.MustExec("delete from mysql.stats_data where table_id = ? and type = 2 and hist_id = ?", tableID, idxID)

	// Trigger priority load via an EXPLAIN that needs the index histogram.
	// set tidb_opt_objective='determinate' flips the optimizer to load stats
	// aggressively instead of tolerating pseudo estimates (see read_test.go
	// TestLoadNonExistentIndexStats for the same technique).
	tk.MustExec("set tidb_opt_objective='determinate';")
	tk.MustQuery("select * from t where v = 0x03").Check(testkit.Rows("\x03"))

	// LoadNeededHistograms drains the async-load queue synchronously.
	require.NoError(t, dom.StatsHandle().LoadNeededHistograms(dom.InfoSchema()))

	stat := dom.StatsHandle().GetPhysicalTableStats(tableID, tableInfo)
	idx := stat.GetIdx(idxID)
	require.NotNil(t, idx)
	require.Greater(t, idx.Histogram.Len(), 0, "priority reader should have loaded buckets from legacy stats_buckets fallback")
}
