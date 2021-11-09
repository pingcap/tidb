package executor_test

import (
	"fmt"
	"github.com/pingcap/tidb/testkit"
	"testing"
	"time"
)

func TestMyc(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists tmp1")
	tk.MustExec("create global temporary table tmp1 " +
		"(id int not null primary key, code int not null, value int default null, unique key code(code))" +
		"on commit delete rows")

	tk.MustExec("use test")
	tk.MustExec("drop table if exists tmp2")
	tk.MustExec("create temporary table tmp2 (id int not null primary key, code int not null, value int default null, unique key code(code));")

	// sleep 1us to make test stale
	time.Sleep(time.Microsecond)

	// test tablesample return empty for global temporary table
	tk.MustQuery("select * from tmp1 tablesample regions()").Check(testkit.Rows())

	tk.MustExec("begin")
	tk.MustExec("insert into tmp1 values (1, 1, 1)")
	tk.MustQuery("select * from tmp1 tablesample regions()").Check(testkit.Rows())
	tk.MustExec("commit")

	// tablesample for global temporary table should not return error for compatibility of tools like dumpling
	tk.MustExec("set @@tidb_snapshot=NOW(6)")

	tk.MustExec("use test")
	tk.MustExec(`START TRANSACTION READ ONLY AS OF TIMESTAMP NOW(3);`)
	// issue 28104
	/*tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a TEXT)")
	tk.MustExec("insert into t values('abc')")

	tk.MustExec("select * from t where a+1")
	tk.MustExec("update t set a = 'def' where a+1")*/

	/*tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int)")
	tk.MustExec("insert into t values(1, 2, 3)")

	tk.MustExec("select a+b from t where b > 1")*/

	tk.MustExec("select NOW()")
	tk.MustExec("set timestamp = DEFAULT")
	tk.MustExec("create table t(a int, b int, c int)")
	tk.MustExec("create table s(a int , b int, c int)")

	tk.MustExec("select * from t join (select a, count(*), sum(b) from s group by a) s on t.a = s.a where t.a < substring('123', 1, 1)")
}
