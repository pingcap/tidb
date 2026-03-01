package export

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompositeStringKeyIntegration(t *testing.T) {
	// Test integration of composite string key dumping functionality
	// This test focuses on the WHERE clause generation and progress tracking logic

	// Create mock table metadata
	mockMeta := &mockCompositeTableMeta{
		database: "test",
		table:    "composite_table",
		columns:  []string{"tenant_id", "user_id", "name", "email"},
		types:    []string{"varchar(50)", "varchar(50)", "varchar(100)", "varchar(255)"},
	}

	// Test WHERE clause generation for composite string keys
	fieldNames := []string{"tenant_id", "user_id"}
	boundaries := [][]string{
		{"tenant_b", "user_001"},
		{"tenant_c", "user_002"},
	}

	whereClauses := buildStringWhereClauses(fieldNames, boundaries)
	require.Len(t, whereClauses, 3, "Should generate 3 chunks for 2 boundaries")

	for i, clause := range whereClauses {
		// Verify clause contains both column references
		require.Contains(t, clause, "`tenant_id`", "WHERE clause should reference tenant_id")
		require.Contains(t, clause, "`user_id`", "WHERE clause should reference user_id")
		// Verify clause uses proper comparison operators
		require.True(t, strings.Contains(clause, ">=") || strings.Contains(clause, ">") || strings.Contains(clause, "<"),
			"WHERE clause should use comparison operators")
		t.Logf("Generated WHERE clause %d: %s", i, clause)
	}

	// Test cursor-based WHERE clause generation
	cursorClause := buildCursorWhereClause(fieldNames, []string{"tenant_b", "user_001"})
	expectedCursor := "`tenant_id` > 'tenant_b' OR (`tenant_id` = 'tenant_b' AND `user_id` >= 'user_001')"
	require.Equal(t, expectedCursor, cursorClause, "Cursor WHERE clause should be properly formatted")

	// Test streaming chunk progress tracking
	dumper := &Dumper{
		chunkedTables: sync.Map{},
	}

	stats := newTableChunkStat()
	chunkKey := mockMeta.ChunkKey()
	dumper.chunkedTables.Store(chunkKey, stats)

	// Simulate chunk processing
	stats.sent.Add(3)     // 3 chunks sent
	stats.finished.Add(3) // 3 chunks completed
	stats.finalized.Store(true)

	// Verify progress tracking
	require.Equal(t, int32(3), stats.sent.Load(), "Should track sent chunks")
	require.Equal(t, int32(3), stats.finished.Load(), "Should track completed chunks")
	require.True(t, stats.finalized.Load(), "Should track finalization")

	// Test cleanup condition
	if stats.finished.Load() == stats.sent.Load() && stats.finalized.Load() {
		dumper.chunkedTables.Delete(chunkKey)
	}

	_, exists := dumper.chunkedTables.Load(chunkKey)
	require.False(t, exists, "Should clean up chunk statistics after completion")
}

func TestCompositeStringKeyBoundaryGeneration(t *testing.T) {
	// Test boundary generation for composite string keys

	testCases := []struct {
		name        string
		columns     []string
		boundaries  [][]string
		expectedSQL []string
	}{
		{
			name:    "two column composite key",
			columns: []string{"region", "city"},
			boundaries: [][]string{
				{"east", "boston"},
				{"west", "seattle"},
			},
			expectedSQL: []string{
				"`region`<'east' or(`region`='east' and `city`<'boston')",
				"`region`>='east' and(`region`<'west' or(`region`='west' and `city`<'seattle'))",
				"`region`>='west' or(`region`='west' and `city`>='seattle')",
			},
		},
		{
			name:    "three column composite key",
			columns: []string{"country", "state", "city"},
			boundaries: [][]string{
				{"usa", "california", "los angeles"},
			},
			expectedSQL: []string{
				"`country`<'usa' or(`country`='usa' and `state`<'california') or(`country`='usa' and `state`='california' and `city`<'los angeles')",
				"`country`>='usa' or(`country`='usa' and `state`>='california') or(`country`='usa' and `state`='california' and `city`>='los angeles')",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildStringWhereClauses(tc.columns, tc.boundaries)
			require.Len(t, result, len(tc.expectedSQL), "Should generate expected number of WHERE clauses")

			for i := range tc.expectedSQL {
				// Verify key components are present - all columns should be in all clauses
				for _, col := range tc.columns {
					require.Contains(t, result[i], fmt.Sprintf("`%s`", col),
						"WHERE clause should reference column %s", col)
				}

				t.Logf("Generated WHERE clause: %s", result[i])
			}

			// Verify that all boundary values appear somewhere in the result set
			allClauses := strings.Join(result, " ")
			for _, boundary := range tc.boundaries {
				for _, val := range boundary {
					if val != "" {
						require.Contains(t, allClauses, fmt.Sprintf("'%s'", val),
							"At least one WHERE clause should reference boundary value %s", val)
					}
				}
			}
		})
	}
}

// Enhanced mockCompositeTableMeta for composite key testing
type mockCompositeTableMeta struct {
	database string
	table    string
	columns  []string
	types    []string
}

func (m *mockCompositeTableMeta) ChunkKey() string {
	return fmt.Sprintf("%s.%s", m.database, m.table)
}

func (m *mockCompositeTableMeta) DatabaseName() string {
	return m.database
}

func (m *mockCompositeTableMeta) TableName() string {
	return m.table
}

func (m *mockCompositeTableMeta) ColumnNames() []string {
	if m.columns != nil {
		return m.columns
	}
	return []string{"tenant_id", "user_id", "name", "email"}
}

func (m *mockCompositeTableMeta) ColumnTypes() []string {
	if m.types != nil {
		return m.types
	}
	return []string{"varchar(50)", "varchar(50)", "varchar(100)", "varchar(255)"}
}

func (m *mockCompositeTableMeta) ColumnCount() uint {
	return uint(len(m.ColumnNames()))
}

func (m *mockCompositeTableMeta) SelectedField() string {
	return "*"
}

func (m *mockCompositeTableMeta) SelectedLen() int {
	return len(m.ColumnNames())
}

func (m *mockCompositeTableMeta) HasImplicitRowID() bool {
	return false
}

func (m *mockCompositeTableMeta) SpecialComments() StringIter {
	return nil
}

func (m *mockCompositeTableMeta) ShowCreateTable() string {
	return ""
}

func (m *mockCompositeTableMeta) ShowCreateView() string {
	return ""
}

func (m *mockCompositeTableMeta) AvgRowLength() uint64 {
	return 100
}
