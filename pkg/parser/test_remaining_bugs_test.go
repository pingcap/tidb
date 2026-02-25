package parser

import (
	"fmt"
	"testing"
)

func TestCIFailures(t *testing.T) {
	p := New()

	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// subquery_bugs: STRAIGHT_JOIN ON should work
		{"straight_join_on", `SELECT * FROM t1 STRAIGHT_JOIN t2 ON t1.a = t2.a`, false},
		// partition_column: MAXVALUE in LIST partition should error
		{"partition_maxvalue", "alter table t1 add partition\n(partition p1 values in (maxvalue, maxvalue))", true},
		// delete: DELETE FROM t1 alias USING should reject alias on target
		{"delete_alias_target", "DELETE FROM t1 alias USING t1, t2 alias WHERE t1.a = alias.a", true},
		// delete: DELETE FROM alias USING t1, t2 alias — alias is a bare table name, this is valid
		{"delete_bare_alias", "DELETE FROM alias USING t1, t2 alias WHERE t1.a = alias.a", false},
	}

	for _, tc := range tests {
		_, _, err := p.Parse(tc.sql, "", "")
		if tc.wantErr {
			if err != nil {
				fmt.Printf("  OK (expected error): %s: %s\n", tc.name, err.Error())
			} else {
				fmt.Printf("  FAIL: %s: expected error but parsed OK\n", tc.name)
				t.Errorf("%s: expected error but parsed OK", tc.name)
			}
		} else {
			if err == nil {
				fmt.Printf("  OK: %s\n", tc.name)
			} else {
				fmt.Printf("  FAIL: %s: %s\n", tc.name, err.Error())
				t.Errorf("%s: %s", tc.name, err.Error())
			}
		}
	}
}
