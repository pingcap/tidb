package parser

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// TestJoinEdgeCases tests edge cases found during strict review.
func TestJoinEdgeCases(t *testing.T) {
	p := New()

	tests := []struct {
		name        string
		sql         string
		expectError bool
	}{
		// NATURAL JOIN should NOT consume ON from DML
		{"natural_join_on_dup_key", "INSERT INTO t1 SELECT 1, a FROM t2 NATURAL JOIN t3 ON DUPLICATE KEY UPDATE j= a", false},
		// STRAIGHT_JOIN should NOT consume ON from DML (same as NATURAL JOIN)
		{"straight_join_on_dup_key", "INSERT INTO t1 SELECT 1, a FROM t2 STRAIGHT_JOIN t3 ON DUPLICATE KEY UPDATE j= a", false},
		// Stacked ON clauses
		{"stacked_on", "SELECT * FROM t1 LEFT JOIN t2 LEFT JOIN t3 ON t2.a = t3.a ON t1.a = t2.a", false},
		// NATURAL JOIN inside stacked context (in parseJoinRHS recursion)
		{"natural_rhs_stacked", "SELECT * FROM t1 LEFT JOIN t2 NATURAL JOIN t3 ON t1.a = t2.a", false},
		// Multiple chained joins
		{"cross_join_chain", "SELECT * FROM t1 JOIN t2 JOIN t3", false},
		// LEFT JOIN requires ON
		{"left_join_no_on", "SELECT * FROM t1 LEFT JOIN t2", true},
		// Nested right join with stacked ON
		{"nested_right_join", "SELECT * FROM t1 LEFT JOIN t2 RIGHT JOIN t3 ON t2.a = t3.a ON t1.a = t2.a", false},
	}

	for _, tc := range tests {
		_, _, err := p.Parse(tc.sql, "", "")
		if tc.expectError {
			if err == nil {
				t.Errorf("%s: expected error but parsed OK", tc.name)
			}
		} else {
			if err != nil {
				t.Errorf("%s: unexpected error: %s", tc.name, err.Error())
			}
		}
	}
}

// TestNaturalJoinInRHS tests NATURAL JOIN specifically inside parseJoinRHS context.
func TestNaturalJoinInRHS(t *testing.T) {
	p := New()

	// This SQL creates a situation where NATURAL JOIN occurs within parseJoinRHS.
	// The LEFT JOIN causes parseJoinRHS to be called. Inside, it encounters
	// NATURAL JOIN, and the ON after t3 must belong to the LEFT JOIN, NOT the NATURAL JOIN.
	sql := "SELECT * FROM t1 LEFT JOIN t2 NATURAL JOIN t3 ON t1.a = t2.a"
	stmts, _, err := p.Parse(sql, "", "")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 stmt, got %d", len(stmts))
	}
	sel := stmts[0].(*ast.SelectStmt)
	join := sel.From.TableRefs
	if join == nil {
		t.Fatalf("expected Join, got nil")
	}

	// The outer join should be LEFT JOIN with ON clause
	if join.Tp != ast.LeftJoin {
		t.Errorf("expected LeftJoin, got %v", join.Tp)
	}
	if join.On == nil {
		t.Errorf("expected ON clause on LEFT JOIN, got nil")
	}

	// The RHS should be a NATURAL JOIN (t2 NATURAL JOIN t3)
	rhsJoin, ok := join.Right.(*ast.Join)
	if !ok {
		t.Fatalf("expected Join as RHS, got %T", join.Right)
	}
	if !rhsJoin.NaturalJoin {
		t.Errorf("expected NaturalJoin=true on RHS, got false")
	}
	if rhsJoin.On != nil {
		t.Errorf("expected no ON on NATURAL JOIN, but got one")
	}
}
