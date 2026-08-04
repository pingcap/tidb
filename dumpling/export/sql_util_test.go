package export

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
			expected: "'don\\'t'",
		},
		{
			name:     "string with multiple quotes",
			input:    "it's a 'test'",
			expected: "'it\\'s a \\'test\\''",
		},
		{
			name:     "string with double quotes",
			input:    `say "hello"`,
			expected: `'say \"hello\"'`,
		},
		{
			name:     "string with backslash",
			input:    `path\to\file`,
			expected: `'path\\to\\file'`,
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
			expected: `'value_y456)\''`,
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
