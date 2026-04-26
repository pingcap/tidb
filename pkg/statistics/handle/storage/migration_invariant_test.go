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
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// assertStatsBucketsMigrationInvariant asserts the post-migration invariant
// that, for every (table_id, hist_id) tracked in mysql.stats_histograms, the
// histogram's bucket data lives in **exactly one** of mysql.stats_buckets and
// mysql.stats_data — never both. The saveBucketsToStorage write path enforces
// this by always deleting the matching legacy row after a successful
// stats_data upsert, and per-histogram GC deletes from both tables in the
// same session.
//
// The query pair used here relies on the type-to-is_index mapping in
// metadef.StatsDataTypeColBucket (1) / StatsDataTypeIdxBucket (2).
func assertStatsBucketsMigrationInvariant(t *testing.T, tk *testkit.TestKit, tableID int64) {
	t.Helper()
	// Find any (table_id, hist_id, is_index) triple that has rows in BOTH
	// mysql.stats_buckets and mysql.stats_data. The invariant is that this
	// query returns zero rows.
	//
	// For stats_data, the type column maps is_index=0 -> 1 and is_index=1 -> 2,
	// so the joined row must satisfy the inverse of that mapping.
	rows := tk.MustQuery(`
		select b.hist_id, b.is_index
		from (
			select distinct table_id, hist_id, is_index
			from mysql.stats_buckets
			where table_id = ?
		) b
		join (
			select distinct table_id, hist_id,
				case when type = 1 then 0 when type = 2 then 1 else -1 end as is_index
			from mysql.stats_data
			where table_id = ? and type in (1, 2)
		) d
		on b.table_id = d.table_id
		   and b.hist_id = d.hist_id
		   and b.is_index = d.is_index
	`, tableID, tableID).Rows()
	require.Emptyf(t, rows, "stats_buckets/stats_data migration invariant violated for table_id=%d: %v", tableID, rows)
}

// TestStatsBucketsMigrationInvariantAfterAnalyze exercises the invariant
// across several analyze cycles on a table with both column and index
// histograms, to catch any write-path regression that would leave a
// histogram's data in both storage locations simultaneously.
func TestStatsBucketsMigrationInvariantAfterAnalyze(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (c1 int, c2 varchar(16), c3 int, primary key(c1), key idx(c2))")
	// Insert enough rows so analyze produces non-empty histograms for
	// at least the index (saveBucketsToStorage early-returns for Len() == 0,
	// which matches legacy behavior — and would leave stats_data empty).
	values := ""
	for i := 1; i <= 50; i++ {
		if i > 1 {
			values += ","
		}
		values += fmt.Sprintf("(%d, 'v%d', %d)", i, i, i*10)
	}
	tk.MustExec("insert into t values " + values)

	tableID := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema='test' and table_name='t'").Rows()[0][0].(string)
	var tid int64
	_, err := fmtSscan(tableID, &tid)
	require.NoError(t, err)

	// First analyze: writes to stats_data, purges stats_buckets (the table is
	// empty pre-analyze so the purge is a no-op).
	tk.MustExec("analyze table t")
	assertStatsBucketsMigrationInvariant(t, tk, tid)

	// Simulate a pre-migration-era stats_buckets row for an existing
	// histogram. Use hist_id taken from mysql.stats_histograms so the row
	// matches whatever analyze actually produced.
	histIDs := tk.MustQuery("select hist_id, is_index from mysql.stats_histograms where table_id = ? and is_index = 1 order by hist_id", tid).Rows()
	require.NotEmpty(t, histIDs, "analyze should have produced at least one index histogram")
	var idxHistID int64
	_, err = fmtSscan(histIDs[0][0].(string), &idxHistID)
	require.NoError(t, err)

	tk.MustExec("insert into mysql.stats_buckets (table_id, is_index, hist_id, bucket_id, count, repeats, lower_bound, upper_bound, ndv) values (?, 1, ?, 0, 1, 1, '', '', 0)",
		tid, idxHistID)
	// After the manual insert the invariant is violated — confirm by a
	// non-fatal query so we can proceed to assert that analyze restores it.
	badRows := tk.MustQuery(`
		select b.hist_id, b.is_index
		from (
			select distinct table_id, hist_id, is_index from mysql.stats_buckets where table_id = ?
		) b
		join (
			select distinct table_id, hist_id,
				case when type = 1 then 0 when type = 2 then 1 else -1 end as is_index
			from mysql.stats_data where table_id = ? and type in (1, 2)
		) d
		on b.table_id = d.table_id and b.hist_id = d.hist_id and b.is_index = d.is_index
	`, tid, tid).Rows()
	require.NotEmpty(t, badRows, "manual stats_buckets insert should violate the invariant for test setup")

	// A fresh analyze cycle should purge the legacy row and restore the
	// invariant via saveBucketsToStorage's unconditional DELETE.
	tk.MustExec("insert into t values (101, 'v101', 1010)")
	tk.MustExec("analyze table t")
	assertStatsBucketsMigrationInvariant(t, tk, tid)

	// Multiple consecutive analyzes must preserve the invariant.
	for range 3 {
		tk.MustExec("analyze table t")
		assertStatsBucketsMigrationInvariant(t, tk, tid)
	}
}

// fmtSscan is a thin shim around fmt.Sscanf so the test body stays readable.
func fmtSscan(s string, out *int64) (int, error) {
	return fmt.Sscanf(s, "%d", out)
}
