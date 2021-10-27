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
