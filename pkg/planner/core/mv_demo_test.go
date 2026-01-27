package core_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
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

func TestMVDemoCommitTsNoPointGetPlan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, v int)")
	tk.MustExec("set @@session.tidb_enable_materialized_view_demo = 1")

	rows := tk.MustQuery("explain select _tidb_commit_ts from t where id = 1").Rows()
	for _, row := range rows {
		require.NotContains(t, fmt.Sprint(row[0]), "Point_Get")
		require.NotContains(t, fmt.Sprint(row[0]), "Batch_Point_Get")
	}
}

func TestMVDemoCommitTsPlanCacheKeyIncludesSysvar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key)")
	tk.MustExec("set @@session.tidb_enable_prepared_plan_cache = 1")
	tk.MustExec("set @@session.tidb_enable_materialized_view_demo = 1")

	tk.MustExec("prepare stmt from 'select _tidb_commit_ts from t where id = ?'")
	tk.MustExec("set @a = 1")
	tk.MustQuery("execute stmt using @a")

	tk.MustExec("set @@session.tidb_enable_materialized_view_demo = 0")
	tk.MustGetErrCode("execute stmt using @a", mysql.ErrBadField)
}
