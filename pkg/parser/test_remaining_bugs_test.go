package parser

import (
	"fmt"
	"testing"
)

func TestCheckDev2Bugs(t *testing.T) {
	p := New()

	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// 1. ENGINE_ATTRIBUTE should be accepted
		{"engine_attribute", "create table t (a int) ENGINE_ATTRIBUTE = '{\"key\": \"value\"}'", false},
		// 2. tinyint default 71 not null should work
		{"tinyint_default", "create table t (col_23 tinyint default 71 not null)", false},
		// 3. timestamp default '1971-06-09' not null
		{"timestamp_default", "create table t (col timestamp default '1971-06-09' not null, col1 int default 1, unique key(col1))", false},
		// 4. ANALYZE TABLE INDEX PRIMARY
		{"analyze_index_primary", "ANALYZE TABLE t0 INDEX PRIMARY", false},
		// 5. Union intersect (nil pointer)
		{"union_intersect", "prepare stmt from '(select * from t1 union all select * from t1) intersect select * from t2'", false},
		// 6. SET TRANSACTION AS OF TIMESTAMP
		{"set_txn_as_of", "set transaction read only as of timestamp now(6) - interval 0.1 second", false},
		// 7. DEFAULT CHARSET with unknown charset — should error
		{"default_charset_unknown", "CREATE TABLE `t` (`a` int) DEFAULT CHARSET=abcdefg", true},
		// 8. decimal default 0
		{"decimal_default", "create table t (col_30 decimal default 0)", false},
		// 9. STRAIGHT_JOIN ON
		{"straight_join_on", "SELECT * FROM t1 STRAIGHT_JOIN t2 ON t1.a = t2.a", false},
		// 10. partition MAXVALUE
		{"partition_maxvalue", "alter table t1 add partition\n(partition p1 values in (maxvalue, maxvalue))", true},
		// 11. DELETE FROM alias USING
		{"delete_alias_target", "DELETE FROM t1 alias USING t1, t2 alias WHERE t1.a = alias.a", true},
	}

	for _, tc := range tests {
		_, _, err := p.Parse(tc.sql, "", "")
		if tc.wantErr {
			if err == nil {
				fmt.Printf("  FAIL: %s: expected error but parsed OK\n", tc.name)
				t.Errorf("%s: expected error but parsed OK", tc.name)
			} else {
				fmt.Printf("  OK (err): %s: %s\n", tc.name, err.Error())
			}
		} else {
			if err != nil {
				fmt.Printf("  FAIL: %s: %s\n", tc.name, err.Error())
				t.Errorf("%s: %s", tc.name, err.Error())
			} else {
				fmt.Printf("  OK: %s\n", tc.name)
			}
		}
	}
}
