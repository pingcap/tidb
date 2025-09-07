package handle

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Test cases for genInitStatsHistogramsSQL. Mirrors patterns used elsewhere
// in this package for concise, require-based assertions.
func TestGenInitStatsHistogramsSQLAllRecords(t *testing.T) {
	// Non-paging with no specific table IDs should load all records.
	opts := newGenHistSQLOptionsForTableIDs(nil)
	got := genInitStatsHistogramsSQL(opts)

	expected := "select /*+ ORDER_INDEX(mysql.stats_histograms,tbl) */ HIGH_PRIORITY " +
		"table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, " +
		"tot_col_size, stats_ver, correlation from mysql.stats_histograms order by table_id"

	require.Equal(t, expected, got)
}

func TestGenInitStatsHistogramsSQLPaging(t *testing.T) {
	// Paging mode adds a closed-open [start, end) range filter.
	opts := newGenHistSQLOptionsForPaging([2]int64{100, 200})
	got := genInitStatsHistogramsSQL(opts)

	expected := "select /*+ ORDER_INDEX(mysql.stats_histograms,tbl) */ HIGH_PRIORITY " +
		"table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, " +
		"tot_col_size, stats_ver, correlation from mysql.stats_histograms" +
		" where table_id >= 100 and table_id < 200 order by table_id"

	require.Equal(t, expected, got)
}

func TestGenInitStatsHistogramsSQLTableIDs(t *testing.T) {
	// Non-paging with specific table IDs should produce an IN (...) clause
	// using the provided order.
	ids := []int64{5, 2, 7}
	opts := newGenHistSQLOptionsForTableIDs(ids)
	got := genInitStatsHistogramsSQL(opts)

	expected := "select /*+ ORDER_INDEX(mysql.stats_histograms,tbl) */ HIGH_PRIORITY " +
		"table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, " +
		"tot_col_size, stats_ver, correlation from mysql.stats_histograms" +
		" where table_id in (5,2,7) order by table_id"

	require.Equal(t, expected, got)
}

func TestGenInitStatsMetaSQLAllRecords(t *testing.T) {
	got := genInitStatsMetaSQL()
	expected := "select HIGH_PRIORITY version, table_id, modify_count, count, snapshot, last_stats_histograms_version from mysql.stats_meta"
	require.Equal(t, expected, got)
}

func TestGenInitStatsMetaSQLTableIDs(t *testing.T) {
	got := genInitStatsMetaSQL(5, 2, 7)
	// Note the spaces around parentheses are intentional per implementation.
	expected := "select HIGH_PRIORITY version, table_id, modify_count, count, snapshot, last_stats_histograms_version from mysql.stats_meta" +
		" where table_id in (5,2,7)"
	require.Equal(t, expected, got)
}
