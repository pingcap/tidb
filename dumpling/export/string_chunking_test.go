package export

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildStringWhereClauses(t *testing.T) {
	// Test with no boundaries
	clauses := buildStringWhereClauses([]string{"id"}, [][]string{})
	require.Nil(t, clauses)

	// Test with single boundary
	clauses = buildStringWhereClauses([]string{"id"}, [][]string{{"middle"}})
	expected := []string{
		"`id`<'middle'",
		"`id`>='middle'",
	}
	require.Equal(t, expected, clauses)

	// Test with multiple boundaries
	clauses = buildStringWhereClauses([]string{"name"}, [][]string{{"apple"}, {"banana"}, {"cherry"}})
	expected = []string{
		"`name`<'apple'",
		"`name`>='apple' and `name`<'banana'",
		"`name`>='banana' and `name`<'cherry'",
		"`name`>='cherry'",
	}
	require.Equal(t, expected, clauses)
}

func TestBuildStringWhereClausesWithSpecialCharacters(t *testing.T) {
	// Test with boundaries containing special characters that need SQL escaping
	boundaries := [][]string{
		{"value_a123*("},
		{"value_b456)'"},
		{"test'quote"},
		{"test\"doublequote"},
		{"test\\backslash"},
	}

	clauses := buildStringWhereClauses([]string{"user_id"}, boundaries)
	expected := []string{
		"`user_id`<'value_a123*('",
		"`user_id`>='value_a123*(' and `user_id`<'value_b456)'''",
		"`user_id`>='value_b456)''' and `user_id`<'test''quote'",
		"`user_id`>='test''quote' and `user_id`<'test\"doublequote'",
		"`user_id`>='test\"doublequote' and `user_id`<'test\\backslash'",
		"`user_id`>='test\\backslash'",
	}
	require.Equal(t, expected, clauses)

	// Test that the generated SQL is syntactically valid (no syntax errors like the original bug)
	for _, clause := range clauses {
		// These clauses should not contain unescaped quotes or malformed syntax
		// The original bug had: 'value_b456)''  which is malformed
		// Our fix should produce: 'value_b456)''' which is valid SQL
		require.NotContains(t, clause, ")''  ", "should not contain malformed double quotes")
		require.NotContains(t, clause, ")'  ", "should not contain malformed single quotes")
	}
}

func TestEscapeSQLString(t *testing.T) {
	// Test the escapeSQLString helper function
	testCases := []struct {
		input    string
		expected string
	}{
		{"simple", "'simple'"},
		{"test'quote", "'test''quote'"},
		{"test\"doublequote", "'test\"doublequote'"},
		{"value_a123*(", "'value_a123*('"},
		{"value_b456)'", "'value_b456)'''"},
		{"", "''"},
	}

	for _, tc := range testCases {
		result := escapeSQLString(tc.input)
		require.Equal(t, tc.expected, result, "Failed for input: %s", tc.input)
	}
}

func TestSpecialCharacterEscapingFix(t *testing.T) {
	// Test SQL escaping fix for special characters that can cause syntax errors
	// Original issue: single quotes followed by parentheses could generate malformed SQL
	boundaries := [][]string{{"value_x123*("}, {"value_y456)'"}}

	clauses := buildStringWhereClauses([]string{"record_id"}, boundaries)

	// Expected generated WHERE clauses that should be valid SQL
	expected := []string{
		"`record_id`<'value_x123*('",
		"`record_id`>='value_x123*(' and `record_id`<'value_y456)'''",
		"`record_id`>='value_y456)'''",
	}
	require.Equal(t, expected, clauses)

	// Verify that the problematic boundary is properly escaped
	// The original bug could generate: 'value_y456)''  which is malformed
	// Our fix should generate: 'value_y456)''' which is valid SQL

	problematicClause := clauses[1] // The middle clause that was causing the error
	require.Contains(t, problematicClause, "'value_y456)'''", "should contain properly escaped boundary")
	require.NotContains(t, problematicClause, "'value_y456)''  ", "should not contain malformed escaping")
}

func TestBuildStringWhereClausesCompositeKey(t *testing.T) {
	// Test composite key chunking with multiple columns
	boundaries := [][]string{
		{"apple", "100"},
		{"banana", "200"},
		{"cherry", "300"},
	}

	clauses := buildStringWhereClauses([]string{"product_name", "category_id"}, boundaries)

	// For composite keys, the WHERE clauses should use the existing buildCompareClause logic
	// which generates proper composite key comparisons
	require.Equal(t, 4, len(clauses), "Should generate 4 chunks for 3 boundaries")

	// Verify that all clauses contain both column names
	for _, clause := range clauses {
		require.Contains(t, clause, "`product_name`", "Should contain first column")
		require.Contains(t, clause, "`category_id`", "Should contain second column")
	}

	// Test that boundary values are properly escaped
	require.Contains(t, clauses[0], "'apple'", "Should contain escaped first boundary value")
	require.Contains(t, clauses[0], "'100'", "Should contain escaped first boundary value")
}

func TestGetStringOrNumericIndexDetection(t *testing.T) {
	// This is a unit test for the index detection logic
	// Testing the type detection part with mock data

	// Test string type detection
	_, isString := dataTypeString["VARCHAR"]
	require.True(t, isString, "VARCHAR should be detected as string type")

	_, isString = dataTypeString["CHAR"]
	require.True(t, isString, "CHAR should be detected as string type")

	_, isString = dataTypeString["TEXT"]
	require.True(t, isString, "TEXT should be detected as string type")

	// Test numeric type detection
	_, isNumeric := dataTypeInt["INT"]
	require.True(t, isNumeric, "INT should be detected as numeric type")

	_, isNumeric = dataTypeInt["BIGINT"]
	require.True(t, isNumeric, "BIGINT should be detected as numeric type")

	// Test that string types are not detected as numeric
	_, isNumeric = dataTypeInt["VARCHAR"]
	require.False(t, isNumeric, "VARCHAR should not be detected as numeric type")
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
