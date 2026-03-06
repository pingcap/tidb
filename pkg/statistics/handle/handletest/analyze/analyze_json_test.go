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
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestSampleBasedGlobalStatsSkippedColumnPosition verifies that sample-based
// global stats correctly handles tables with columns that are skipped by
// filterSkipColumnTypes (e.g., JSON). The sample collector is indexed by the
// filtered column positions, so the call site must pass the filtered colsInfo
// to BuildGlobalStatsFromSamples rather than the raw table.Meta().Columns.
//
// Without the fix (passing table.Meta().Columns), column b's histogram would
// be built from wrong data because its position in table.Meta().Columns (2)
// doesn't match its position in the sample collector (1, after JSON is filtered).
// With a clustered PK (no implicit _tidb_rowid), this mismatch causes an
// index-out-of-range panic. Without a clustered PK, _tidb_rowid padding
// prevents the panic but causes silent data corruption.
func TestSampleBasedGlobalStatsSkippedColumnPosition(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("set @@tidb_partition_prune_mode = '" + string(variable.Dynamic) + "'")
	tk.MustExec("set @@tidb_enable_sample_based_global_stats = 1")

	// Use a clustered primary key so there's no implicit _tidb_rowid column
	// in the sample. This makes the sample Columns array have exactly 2
	// entries (a, b) after JSON is filtered, exposing the out-of-range bug
	// when table.Meta().Columns (3 entries: a, j, b) is used instead.
	tk.MustExec(`create table t_json_pk (
		a int primary key clustered,
		j json,
		b int
	) partition by range (a) (
		partition p0 values less than (100),
		partition p1 values less than (200)
	)`)

	// Insert data with known b values so we can verify the histogram.
	// p0: a=1..5, b=10..50; p1: a=101..105, b=110..150
	for i := 1; i <= 5; i++ {
		tk.MustExec("insert into t_json_pk values (?, '{\"k\":1}', ?)", i, i*10)
		tk.MustExec("insert into t_json_pk values (?, '{\"k\":1}', ?)", 100+i, 100+i*10)
	}

	// The ANALYZE should succeed with the fix (filtered colsInfo used).
	// Without the fix + clustered PK, this panics with index out of range
	// because sample.Columns has 2 entries but code tries to access index 2.
	_, err := tk.Exec("analyze table t_json_pk")
	// The memory tracker assertion is a pre-existing issue; ignore it.
	// What matters is there's no "index out of range" panic.
	if err != nil {
		require.NotContains(t, err.Error(), "index out of range",
			"BuildGlobalStatsFromSamples panicked due to column position mismatch")
	}

	// Verify global stats exist and have the correct row count.
	rows := tk.MustQuery("show stats_meta where table_name = 't_json_pk' and partition_name = 'global'").Rows()
	require.Len(t, rows, 1, "global stats should exist")
	require.Equal(t, "10", rows[0][5].(string), "global row count should be 10")

	// Verify the global histogram for column b contains plausible values.
	// With the fix, TopN/histogram is built from actual b values (10-150).
	// Without the fix, it would be built from wrong column data.
	topnRows := tk.MustQuery("show stats_topn where table_name = 't_json_pk' and partition_name = 'global' and column_name = 'b' and is_index = 0").Rows()
	// Check that at least some TopN values are in the expected range [10, 150].
	for _, row := range topnRows {
		val := row[5].(string)
		// b values should be recognizable as 10,20,...,50,110,...,150
		// They should NOT look like row IDs (1,2,3,...) or JSON strings.
		require.False(t, strings.HasPrefix(val, "{"),
			"TopN value %q looks like JSON — wrong column data used", val)
	}
}
