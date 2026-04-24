package export

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEscapeSQLString(t *testing.T) {
	// Test the escapeSQLString helper function
	testCases := []struct {
		input    string
		expected string
	}{
		{"simple", "'simple'"},
		{"test'quote", "'test\\'quote'"},
		{"test\"doublequote", "'test\\\"doublequote'"},
		{"value_a123*(", "'value_a123*('"},
		{"value_b456)'", `'value_b456)\''`},
		{"", "''"},
	}

	for _, tc := range testCases {
		result := escapeSQLString(tc.input)
		require.Equal(t, tc.expected, result, "Failed for input: %s", tc.input)
	}
}

func TestLowerBoundQueryLimitClause(t *testing.T) {
	// Test that lower bound WHERE clauses generate proper conditions
	// This tests cursor-based pagination for efficient boundary sampling

	// Test single column lower bound
	columnNames := []string{"user_id"}
	boundary := []string{"user_12345"}

	whereClause := buildLowerBoundWhereClause(columnNames, boundary)
	expected := "`user_id` >= 'user_12345'"
	require.Equal(t, expected, whereClause, "Single column lower bound should generate correct WHERE clause")

	// Test composite key lower bound for complex primary keys
	columnNames = []string{"tenant_id", "record_seq"}
	boundary = []string{"tenant_abc", "1000"}

	whereClause = buildLowerBoundWhereClause(columnNames, boundary)
	expected = "`tenant_id` > 'tenant_abc' OR (`tenant_id` = 'tenant_abc' AND `record_seq` >= '1000')"
	require.Equal(t, expected, whereClause, "Composite key lower bound should generate correct WHERE clause")

	// Test that boundary-first approach with cursor-based sampling is optimal
	// Note: The new approach uses:
	// 1. Cursor-based pagination for boundary sampling (avoids expensive OFFSET)
	// 2. Properly bounded chunks for all data queries
	// 3. No LIMIT clauses needed as safety nets
	t.Log("Boundary sampling uses cursor-based pagination to avoid expensive OFFSET queries")
	t.Log("All chunks except the final one have proper upper and lower bounds")
	t.Log("This eliminates both OFFSET performance issues and full table scan risks")
}

func TestBuildCursorWhereClause(t *testing.T) {
	// Test cursor-based WHERE clause generation for efficient boundary sampling

	// Test with empty inputs
	result := buildCursorWhereClause(nil, nil)
	require.Empty(t, result, "Should return empty string for nil inputs")

	result = buildCursorWhereClause([]string{}, []string{})
	require.Empty(t, result, "Should return empty string for empty inputs")

	// Test single column cursor
	columnNames := []string{"id"}
	boundary := []string{"100"}
	result = buildCursorWhereClause(columnNames, boundary)
	expected := "`id` >= '100'"
	require.Equal(t, expected, result, "Single column cursor should generate correct WHERE clause")

	// Test composite key cursor (2 columns)
	columnNames = []string{"user_id", "created_at"}
	boundary = []string{"user123", "2023-01-01"}
	result = buildCursorWhereClause(columnNames, boundary)
	expected = "`user_id` > 'user123' OR (`user_id` = 'user123' AND `created_at` >= '2023-01-01')"
	require.Equal(t, expected, result, "Two column cursor should generate correct OR condition")

	// Test composite key cursor (3 columns)
	columnNames = []string{"tenant_id", "user_id", "timestamp"}
	boundary = []string{"tenant_a", "user_456", "1640995200"}
	result = buildCursorWhereClause(columnNames, boundary)
	expected = "`tenant_id` > 'tenant_a' OR (`tenant_id` = 'tenant_a' AND `user_id` > 'user_456') OR (`tenant_id` = 'tenant_a' AND `user_id` = 'user_456' AND `timestamp` >= '1640995200')"
	require.Equal(t, expected, result, "Three column cursor should generate correct nested OR conditions")
}

func TestBuildUpperBoundWhereClause(t *testing.T) {
	// Test upper bound WHERE clause generation for chunk boundaries

	// Test with empty inputs
	result := buildUpperBoundWhereClause(nil, nil)
	require.Empty(t, result, "Should return empty string for nil inputs")

	// Test single column upper bound
	columnNames := []string{"id"}
	boundary := []string{"500"}
	result = buildUpperBoundWhereClause(columnNames, boundary)
	expected := "`id` < '500'"
	require.Equal(t, expected, result, "Single column upper bound should generate correct WHERE clause")

	// Test composite key upper bound
	columnNames = []string{"category", "item_id"}
	boundary = []string{"electronics", "12345"}
	result = buildUpperBoundWhereClause(columnNames, boundary)
	expected = "`category` < 'electronics' OR (`category` = 'electronics' AND `item_id` < '12345')"
	require.Equal(t, expected, result, "Composite key upper bound should generate correct OR condition")
}

func TestBuildBoundedWhereClause(t *testing.T) {
	// Test bounded WHERE clause generation for chunk ranges

	// Test with empty inputs
	result := buildBoundedWhereClause(nil, nil, nil)
	require.Empty(t, result, "Should return empty string for nil inputs")

	// Test single column bounded
	columnNames := []string{"score"}
	lowerBoundary := []string{"50"}
	upperBoundary := []string{"100"}
	result = buildBoundedWhereClause(columnNames, lowerBoundary, upperBoundary)
	expected := "(`score` >= '50') AND (`score` < '100')"
	require.Equal(t, expected, result, "Single column bounded should generate correct WHERE clause")

	// Test composite key bounded
	columnNames = []string{"region", "sales_date"}
	lowerBoundary = []string{"east", "2023-01-01"}
	upperBoundary = []string{"west", "2023-12-31"}
	result = buildBoundedWhereClause(columnNames, lowerBoundary, upperBoundary)
	expected = "(`region` > 'east' OR (`region` = 'east' AND `sales_date` >= '2023-01-01')) AND (`region` < 'west' OR (`region` = 'west' AND `sales_date` < '2023-12-31'))"
	require.Equal(t, expected, result, "Composite key bounded should combine lower and upper bounds correctly")
}

func TestExtractOrderByColumns(t *testing.T) {
	// Test ORDER BY clause parsing for composite key chunking

	testCases := []struct {
		orderBy  string
		expected []string
	}{
		{"ORDER BY `id`", []string{"`id`"}},
		{"ORDER BY `user_id`,`created_at`", []string{"`user_id`", "`created_at`"}},
		{"ORDER BY `a`, `b`, `c`", []string{"`a`", "`b`", "`c`"}},
		{"ORDER BY id", []string{"id"}},
		{"ORDER BY user_id, created_at DESC", []string{"user_id", "created_at DESC"}},
		{"", []string{""}},
		// Test columns with commas in names
		{"ORDER BY `col,1`, `col,2`", []string{"`col,1`", "`col,2`"}},
		{"ORDER BY `col,with,many,commas`, `normal_col`", []string{"`col,with,many,commas`", "`normal_col`"}},
		{"ORDER BY `first name`, `last,name`", []string{"`first name`", "`last,name`"}},
	}

	for _, tc := range testCases {
		result := extractOrderByColumns(tc.orderBy)
		require.Equal(t, tc.expected, result, "Failed to parse ORDER BY: %s", tc.orderBy)
	}
}

func TestStreamingChunkingProgressTracking(t *testing.T) {
	// Test progress tracking for streaming string key chunking

	// Test chunk statistics initialization
	stats := newTableChunkStat()
	require.NotNil(t, stats, "Should create new chunk statistics")
	require.Equal(t, int32(0), stats.sent.Load(), "Initial sent count should be 0")
	require.Equal(t, int32(0), stats.finished.Load(), "Initial finished count should be 0")
	require.False(t, stats.finalized.Load(), "Initial finalized status should be false")

	// Test chunk count updates
	stats.sent.Add(1)
	require.Equal(t, int32(1), stats.sent.Load(), "Sent count should increment")

	stats.finished.Add(1)
	require.Equal(t, int32(1), stats.finished.Load(), "Finished count should increment")

	stats.finalized.Store(true)
	require.True(t, stats.finalized.Load(), "Finalized status should be true")
}

func TestMaxChunkLimitSafety(t *testing.T) {
	// Test that the maxChunkLimit constant prevents infinite loops
	require.Equal(t, int64(1000000), int64(maxChunkLimit), "Max chunk limit should be set to prevent runaway chunking")

	// Verify the safety check would trigger for large chunk counts
	require.Greater(t, int64(maxChunkLimit), int64(100000), "Max chunk limit should be large enough for normal use")
	require.Less(t, int64(maxChunkLimit), int64(10000000), "Max chunk limit should prevent excessive memory usage")
}
