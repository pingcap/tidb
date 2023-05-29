package executor_test

import (
	"github.com/pingcap/tidb/testkit"
	"testing"
	"time"
)

func TestFairLockDebugCs(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	tk.MustExec("create table t(pk int key, val int)")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustQuery("select @@tidb_pessimistic_txn_fair_locking").Check(testkit.Rows("1"))

	tk.MustExec("begin")

	go func() {
		tk2.MustExec("begin")
		tk2.MustExec("update t set val = val + 1 where pk = 1")
		time.Sleep(time.Second)
		tk2.MustExec("commit")
	}()
	time.Sleep(time.Second)
	tk.MustExec("update t set val = val + 1 where pk = 1")
	tk.MustExec("commit")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 3"))
}
