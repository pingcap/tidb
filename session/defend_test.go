package session_test

import (
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDefend14631(t *testing.T) {
	store, close := testkit.CreateMockStore(t)
	defer close()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`create table t1(c1 decimal(6,4), primary key(c1))`)
	tk.MustExec(`insert into t1 set c1 = 0.1`)
	require.Nil(t, failpoint.Enable("github.com/pingcap/tidb/table/tables/printMutation", "return"))
	tk.MustExec(`insert into t1 set c1 = 0.1 on duplicate key update c1 = 1`)
	require.Nil(t, failpoint.Disable("github.com/pingcap/tidb/table/tables/printMutation"))
	tk.MustExec("admin check table t1")
	//tk.MustQuery(`select * from t1 use index(primary)`).Check(testkit.Rows(`1.0000`))
}

func TestCorrupt(t *testing.T) {
	store, close := testkit.CreateMockStore(t)
	defer close()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec("set global tidb_enable_mutation_checker = true;")
	tk.MustExec("set tidb_enable_mutation_checker = true;")
	tk.MustQuery("select @@tidb_enable_mutation_checker").Check(testkit.Rows("1"))
	tk.MustExec(`CREATE TABLE t1653 (c1 VARCHAR(10), c1377 VARCHAR(10), KEY i1654 (c1, c1377), KEY i1655 (c1377, c1))`)
	failpoint.Enable("github.com/pingcap/tidb/table/tables/corruptMutations", "return(\"missingIndex\")")
	tk.MustExec("begin")
	tk.MustExec(`insert into t1653 set c1 = 'a', c1377 = 'b'`)
	tk.MustExec(`insert into t1653 values('aa', 'bb')`)
	tk.MustExec("commit")
	failpoint.Disable("github.com/pingcap/tidb/table/tables/corruptMutations")
}

func TestCheckInTxn(t *testing.T) {
	store, close := testkit.CreateMockStore(t)
	defer close()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t`)
	tk.MustExec("set global tidb_enable_mutation_checker = true;")
	tk.MustExec("create table t(id int, v varchar(20), unique key i1(id))")
	tk.MustExec("insert into t values (1, 'a')")
	require.Nil(t, failpoint.Enable("github.com/pingcap/tidb/table/tables/printMutation", "return"))
	tk.MustExec("begin")
	tk.MustExec("insert into t values (1, 'd'), (3, 'f') on duplicate key update v='x'")
	tk.MustExec("commit")
	require.Nil(t, failpoint.Disable("github.com/pingcap/tidb/table/tables/printMutation"))
}

func TestOncall4058(t *testing.T) {
	store, close := testkit.CreateMockStore(t)
	defer close()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t`)
	tk.MustExec("set global tidb_enable_mutation_checker = 1")
	tk.MustExec("set tidb_enable_mutation_checker = 1")
	tk.MustExec("set tidb_txn_mode='optimistic';")
	tk.MustExec("set tidb_disable_txn_auto_retry=false;")

	tk.MustExec("create table t(a double key auto_increment, b int);")
	tk.MustExec("insert into t values (146576794, 1);")
	tk.MustExec("begin;")
	tk.MustExec("insert into t(b) select 1; ")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("begin")
	tk2.MustExec("insert into t values (146576795, 1)")
	tk2.MustExec("insert into t values (146576796, 1)")
	tk2.MustExec("commit")

	// prevent commit
	err := tk.ExecToErr("commit")
	require.NotNil(t, err)
	tk.MustExec("select * from t")
}
