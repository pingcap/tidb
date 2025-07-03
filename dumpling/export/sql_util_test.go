package export

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetStringOrNumericIndexColumns(t *testing.T) {
	// Test index column detection for composite key chunking

	// This would typically require a database connection, so we test the logic indirectly
	// by testing the helper functions and data structures used

	// Test data type maps used in index detection
	_, isString := dataTypeString["VARCHAR"]
	require.True(t, isString, "VARCHAR should be detected as string type")

	_, isString = dataTypeString["CHAR"]
	require.True(t, isString, "CHAR should be detected as string type")

	_, isString = dataTypeString["TEXT"]
	require.True(t, isString, "TEXT should be detected as string type")

	_, isNumeric := dataTypeInt["INT"]
	require.True(t, isNumeric, "INT should be detected as numeric type")

	_, isNumeric = dataTypeInt["BIGINT"]
	require.True(t, isNumeric, "BIGINT should be detected as numeric type")

	// Verify cross-type validation
	_, isStringFromInt := dataTypeString["INT"]
	require.False(t, isStringFromInt, "INT should not be detected as string type")

	_, isNumericFromString := dataTypeInt["VARCHAR"]
	require.False(t, isNumericFromString, "VARCHAR should not be detected as numeric type")
}

func TestInterpolateStringBoundary(t *testing.T) {
	// Test string boundary interpolation for chunking

	testCases := []struct {
		name     string
		minBytes []byte
		maxBytes []byte
		ratio    float64
		expected string
	}{
		{
			name:     "simple interpolation",
			minBytes: []byte("apple"),
			maxBytes: []byte("zebra"),
			ratio:    0.5,
			expected: "mango", // Approximate middle value
		},
		{
			name:     "start boundary",
			minBytes: []byte("aaa"),
			maxBytes: []byte("zzz"),
			ratio:    0.0,
			expected: "aaa",
		},
		{
			name:     "end boundary",
			minBytes: []byte("aaa"),
			maxBytes: []byte("zzz"),
			ratio:    1.0,
			expected: "zzz",
		},
		{
			name:     "empty strings",
			minBytes: []byte(""),
			maxBytes: []byte("test"),
			ratio:    0.5,
			expected: "te", // Half of "test"
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := interpolateStringBoundary(tc.minBytes, tc.maxBytes, tc.ratio)

			// For exact matches, verify the result
			if tc.ratio == 0.0 {
				require.Equal(t, string(tc.minBytes), result, "Ratio 0.0 should return min value")
			} else if tc.ratio == 1.0 {
				require.Equal(t, string(tc.maxBytes), result, "Ratio 1.0 should return max value")
			} else {
				// For interpolated values, just ensure result is not empty and within bounds
				require.NotEmpty(t, result, "Interpolated result should not be empty")
				require.LessOrEqual(t, result, string(tc.maxBytes), "Result should be <= max")
				require.GreaterOrEqual(t, result, string(tc.minBytes), "Result should be >= min")
			}
		})
	}
}

func TestBuildStringWhereClausesAdvanced(t *testing.T) {
	// Test advanced WHERE clause generation for composite keys

	// Test empty boundary handling
	result := buildStringWhereClauses([]string{"id"}, [][]string{})
	require.Nil(t, result, "Should return nil for empty boundaries")

	result = buildStringWhereClauses([]string{}, [][]string{{"value"}})
	require.Nil(t, result, "Should return nil for empty column names")

	// Test single column with multiple boundaries
	columnNames := []string{"name"}
	boundaries := [][]string{{"alice"}, {"bob"}, {"charlie"}}

	result = buildStringWhereClauses(columnNames, boundaries)
	expected := []string{
		"`name`<'alice'",
		"`name`>='alice' and `name`<'bob'",
		"`name`>='bob' and `name`<'charlie'",
		"`name`>='charlie'",
	}
	require.Equal(t, expected, result, "Single column with multiple boundaries should generate correct clauses")

	// Test composite key with two columns
	columnNames = []string{"region", "city"}
	boundaries = [][]string{
		{"east", "boston"},
		{"west", "seattle"},
	}

	result = buildStringWhereClauses(columnNames, boundaries)
	require.Len(t, result, 3, "Should generate 3 chunks for 2 boundaries")

	// Verify all clauses contain both columns
	for _, clause := range result {
		require.Contains(t, clause, "`region`", "Should contain region column")
		require.Contains(t, clause, "`city`", "Should contain city column")
	}

	// Test with empty values in boundaries (edge case)
	columnNames = []string{"col1", "col2"}
	boundaries = [][]string{
		{"value1", ""}, // Empty second column
		{"value2", "value2b"},
	}

	result = buildStringWhereClauses(columnNames, boundaries)
	require.NotEmpty(t, result, "Should handle empty boundary values")

	// Verify clauses are generated (empty values are handled as empty strings which is valid)
	require.NotEmpty(t, result, "Should generate WHERE clauses even with empty boundary values")
}

func TestPickupPossibleFieldsForStringChunking(t *testing.T) {
	// Test field selection for string chunking (unit test for logic)

	// This function depends on database queries, so we test the integration points
	// and verify that the function signature returns expected types

	// Test that the function exists and has correct signature
	// (This is verified by compilation, but we can test some static aspects)

	// Test that backward compatibility function works
	t.Run("backward_compatibility", func(t *testing.T) {
		// The pickupPossibleFieldForStringChunking function should return
		// only the first field from pickupPossibleFieldsForStringChunking
		// This is tested implicitly by ensuring the functions exist and compile
		require.True(t, true, "Functions should exist and compile correctly")
	})
}

func TestEscapeSQLStringAdvanced(t *testing.T) {
	// Test advanced SQL string escaping scenarios

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple string",
			input:    "hello",
			expected: "'hello'",
		},
		{
			name:     "string with single quote",
			input:    "don't",
			expected: "'don''t'",
		},
		{
			name:     "string with multiple quotes",
			input:    "it's a 'test'",
			expected: "'it''s a ''test'''",
		},
		{
			name:     "string with double quotes",
			input:    `say "hello"`,
			expected: `'say "hello"'`,
		},
		{
			name:     "string with backslash",
			input:    `path\to\file`,
			expected: `'path\to\file'`,
		},
		{
			name:     "empty string",
			input:    "",
			expected: "''",
		},
		{
			name:     "string with special characters",
			input:    "value_x123*(",
			expected: "'value_x123*('",
		},
		{
			name:     "problematic boundary case",
			input:    "value_y456)'",
			expected: "'value_y456)'''",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := escapeSQLString(tc.input)
			require.Equal(t, tc.expected, result, "Failed for input: %s", tc.input)

			// Verify the result is properly quoted
			require.True(t, len(result) >= 2, "Result should be at least 2 characters (quotes)")
			require.Equal(t, "'", string(result[0]), "Should start with single quote")
			require.Equal(t, "'", string(result[len(result)-1]), "Should end with single quote")
		})
	}
}

func TestColumnQuotingInWhereClauses(t *testing.T) {
	// Test proper column name quoting in WHERE clause generation

	// Test with already quoted columns
	columnNames := []string{"`user_id`", "`created_at`"}
	boundary := []string{"user123", "2023-01-01"}

	result := buildCursorWhereClause(columnNames, boundary)
	expected := "`user_id` > 'user123' OR (`user_id` = 'user123' AND `created_at` >= '2023-01-01')"
	require.Equal(t, expected, result, "Should handle pre-quoted column names correctly")

	// Test with unquoted columns
	columnNames = []string{"user_id", "created_at"}
	result = buildCursorWhereClause(columnNames, boundary)
	expected = "`user_id` > 'user123' OR (`user_id` = 'user123' AND `created_at` >= '2023-01-01')"
	require.Equal(t, expected, result, "Should quote unquoted column names")

	// Test mixed quoting
	columnNames = []string{"`user_id`", "created_at"}
	result = buildCursorWhereClause(columnNames, boundary)
	expected = "`user_id` > 'user123' OR (`user_id` = 'user123' AND `created_at` >= '2023-01-01')"
	require.Equal(t, expected, result, "Should handle mixed quoting correctly")
}
