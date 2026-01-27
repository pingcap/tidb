package core_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
)

func TestMVDemoCommitTsColumnGate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")

	tk.MustGetErrCode("explain select _tidb_commit_ts from t", mysql.ErrBadField)

	tk.MustExec("set @@session.tidb_enable_materialized_view_demo = 1")
	tk.MustQuery("explain select _tidb_commit_ts from t")
}
