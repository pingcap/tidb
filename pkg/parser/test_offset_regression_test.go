package parser

import (
	"fmt"
	"testing"
)

// TestYaccOffsetParity verifies that HandParser error offsets match yacc
// by computing the expected column from the source text manually.
func TestYaccOffsetParity(t *testing.T) {
	p := New()

	type testCase struct {
		name     string
		sql      string
		wantCol  int    // expected yacc column
		wantNear string // expected near text prefix
	}

	tests := []testCase{
		// subquery_all: "id not null" → unexpected NULL after NOT
		// yacc: col 42, near "null) from t1, t2"
		{"subquery_all", "select (select * from t3 where id not null) from t1, t2", 42, "null"},
		// foreign_key1: "match full match partial" → 2nd MATCH unexpected
		// yacc: col 51, near "match partial)"
		{"foreign_key1", "create table t_34455 (\na int not null,\nforeign key (a) references t3 (a) match full match partial)", 51, "match partial"},
		// json_type: "cast(...as text)" → TEXT is not valid cast type
		// yacc: col 27, near "text)"
		{"json_type", "select cast('{a:1}' as text)", 27, "text"},
	}

	for _, tc := range tests {
		_, _, err := p.Parse(tc.sql, "", "")
		if err == nil {
			t.Errorf("%s: expected error, got nil", tc.name)
			continue
		}
		errStr := err.Error()
		fmt.Printf("%s:\n  got:  %s\n", tc.name, errStr)

		// Check if column matches
		expected := fmt.Sprintf("column %d", tc.wantCol)
		if !contains(errStr, expected) {
			t.Errorf("%s: column mismatch\n  want substr: %s\n  got: %s", tc.name, expected, errStr)
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 || findSubstr(s, substr))
}

func findSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
