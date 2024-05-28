package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestIssue53290(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// Create tables and insert data
	tk.MustExec("CREATE TABLE t0(c0 INTEGER UNSIGNED);")
	tk.MustExec("CREATE TABLE t1(c0 BOOL , c1 CHAR, c2 INT);")
	tk.MustExec("insert into t0 values(1), (2), (3);")
	tk.MustExec("insert into t1 values(0, 'a', 1), (1, 'b', 2), (1, 'c', 3);")
	tk.MustExec("INSERT INTO mysql.opt_rule_blacklist VALUES('predicate_push_down');")
	tk.MustExec("ADMIN reload opt_rule_blacklist;")

	sql := "EXPLAIN SELECT t1.c0 FROM  t0 RIGHT JOIN t1 ON (NOT (false));"
	tk.MustExec(sql)
}
