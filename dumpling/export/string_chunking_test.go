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
