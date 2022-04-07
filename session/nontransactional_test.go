package session_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/testkit"
)

func TestNonTransactionalDelete(t *testing.T) {
	if !*withTiKV {
		return
	}
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	tk.MustExec("use mysql")
	tk.MustExec("split on a limit 3 delete from test.t")
	tk.MustQuery("select count(*) from test.t").Check(testkit.Rows("0"))

	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into test.t values ('%d', %d)", i, i*2))
	}
	tk.MustQuery("split on a limit 3 dry run delete from test.t").Check(testkit.Rows("DELETE FROM `test`.`t` WHERE `a` BETWEEN 0 AND 34", "DELETE FROM `test`.`t` WHERE `a` BETWEEN 35 AND 69", "DELETE FROM `test`.`t` WHERE `a` BETWEEN 70 AND 99"))
	tk.MustQuery("split on a limit 3 dry run query delete from test.t").Check(testkit.Rows("select `a` from `test`.`t` where true order by `a`"))
	failpoint.Enable("github.com/pingcap/tidb/session/splitDeleteError", `return`)
	defer failpoint.Disable("github.com/pingcap/tidb/session/splitDeleteError")
	tk.MustQuery("split on a limit 3 delete from test.t").Check(testkit.Rows(
		"job id: 1, job size: 35, range: [KindInt64 0, KindInt64 34] DELETE FROM `test`.`t` WHERE `a` BETWEEN 0 AND 34 injected split delete error",
		"job id: 2, job size: 35, range: [KindInt64 35, KindInt64 69] DELETE FROM `test`.`t` WHERE `a` BETWEEN 35 AND 69 injected split delete error",
		"job id: 3, job size: 30, range: [KindInt64 70, KindInt64 99] DELETE FROM `test`.`t` WHERE `a` BETWEEN 70 AND 99 injected split delete error"))
}
