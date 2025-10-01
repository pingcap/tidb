package parser

import (
	"strings"
	"testing"
)

func TestArenaEdgeCases(t *testing.T) {
	parser := New()

	testCases := []struct {
		name string
		sql  string
		shouldPass bool
	}{
		{"Empty SQL", "", false},
		{"Simple SELECT", "SELECT 1", true},
		{"Unicode support", "SELECT '你好' AS greeting", true},
		{"Comments", "/* comment */ SELECT 1", true},
		{"Large IN list", "SELECT * FROM t WHERE id IN (" + strings.Repeat("1,", 999) + "1)", true},
		{"Complex nested query", "SELECT (SELECT COUNT(*) FROM t1 WHERE t1.id = t2.id) FROM t2", true},
		{"Multiple function calls", "SELECT CONCAT(UPPER(name), LOWER(surname)) FROM users", true},
		{"Binary operations", "SELECT a + b * c - d / e FROM table1", true},
		{"Case expressions", "SELECT CASE WHEN x > 0 THEN 'positive' ELSE 'negative' END FROM t", true},
		{"Subqueries with EXISTS", "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset arena before each test
			parser.Reset()

			_, err := parser.ParseOneStmt(tc.sql, "", "")
			if tc.shouldPass && err != nil {
				t.Errorf("Parse failed for %s: %v", tc.name, err)
			} else if !tc.shouldPass && err == nil {
				t.Errorf("Expected parse failure for %s, but it succeeded", tc.name)
			}

			// Verify arena was used (basic smoke test)
			// The arena should have been reset and potentially used
			if tc.shouldPass && err == nil {
				// Arena should be in a consistent state
				if parser.arena == nil {
					t.Errorf("Arena is nil after successful parse")
				}
			}
		})
	}
}

func TestArenaMemoryReuse(t *testing.T) {
	parser := New()

	// Parse the same SQL multiple times to test arena reuse
	sql := "SELECT f1(a), f2(b), f3(c) FROM table1 WHERE x > 0 AND y < 100"

	for i := 0; i < 10; i++ {
		parser.Reset() // This should reset the arena

		_, err := parser.ParseOneStmt(sql, "", "")
		if err != nil {
			t.Fatalf("Parse failed on iteration %d: %v", i, err)
		}

		// Check arena stats if available
		if parser.arena != nil {
			blocks, bytes, capacity := parser.arena.GetStats()
			t.Logf("Iteration %d: blocks=%d, bytes=%d, capacity=%d", i, blocks, bytes, capacity)

			// Arena should not accumulate blocks indefinitely
			if blocks > 2 {
				t.Errorf("Too many arena blocks allocated: %d", blocks)
			}
		}
	}
}

func TestArenaAllocationTypes(t *testing.T) {
	parser := New()

	// Test SQL that uses various arena allocation types
	testQueries := []struct {
		sql string
		description string
	}{
		{"SELECT func1(a, b)", "FuncCallExpr allocation"},
		{"SELECT a + b - c", "BinaryOperationExpr allocation"},
		{"SELECT -a", "UnaryOperationExpr allocation"},
		{"SELECT (SELECT 1)", "SubqueryExpr allocation"},
		{"SELECT EXISTS(SELECT 1)", "ExistsSubqueryExpr allocation"},
		{"SELECT a BETWEEN 1 AND 10", "BetweenExpr allocation"},
		{"SELECT a IS NULL", "IsNullExpr allocation"},
		{"SELECT a IS TRUE", "IsTruthExpr allocation"},
	}

	for _, tq := range testQueries {
		t.Run(tq.description, func(t *testing.T) {
			parser.Reset()

			_, err := parser.ParseOneStmt(tq.sql, "", "")
			if err != nil {
				t.Errorf("Parse failed for %s: %v", tq.description, err)
			}
		})
	}
}

func TestArenaP0SafetyValidation(t *testing.T) {
	parser := New()

	// Test that P0 fixes don't break normal operation
	t.Run("Normal allocation works", func(t *testing.T) {
		_, err := parser.ParseOneStmt("SELECT 1", "", "")
		if err != nil {
			t.Errorf("Basic parse failed: %v", err)
		}
	})

	t.Run("Arena reset works", func(t *testing.T) {
		parser.Reset()
		_, err := parser.ParseOneStmt("SELECT 2", "", "")
		if err != nil {
			t.Errorf("Parse after reset failed: %v", err)
		}
	})

	t.Run("Multiple resets work", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			parser.Reset()
			_, err := parser.ParseOneStmt("SELECT "+string(rune('0'+i)), "", "")
			if err != nil {
				t.Errorf("Parse failed after reset %d: %v", i, err)
			}
		}
	})
}