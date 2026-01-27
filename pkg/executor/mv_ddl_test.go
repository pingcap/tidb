package executor_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestMVCreateDropMaterializedView(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_materialized_view = 1")
	tk.MustExec("create table t_mv (a int, b int)")
	tk.MustExec("create materialized view log on t_mv(a,b)")

	tk.MustExec("create materialized view mv1 as select a, count(*), sum(b), min(b), max(b) from t_mv where a > 0 group by a refresh fast every 60")

	is := tk.Session().GetInfoSchema().(infoschema.InfoSchema)
	mvTbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv1"))
	require.NoError(t, err)
	mvID := mvTbl.Meta().ID

	logTbl := getMVLogTableName(t, tk, "test", "t_mv")
	logInfo, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(logTbl))
	require.NoError(t, err)

	tk.MustQuery(fmt.Sprintf("select base_table_id, log_table_id, last_refresh_result, last_refresh_tso > 0, next_run_time is not null from mysql.mv_refresh_info where mv_id = %d", mvID)).
		Check(testkit.Rows(fmt.Sprintf("%d %d SUCCESS 1 1", mvTbl.Meta().MaterializedViewInfo.BaseTableID, logInfo.Meta().ID)))

	tk.MustGetErrMsg("drop table mv1", "[executor:1235]drop materialized view mv1 is not supported")
	tk.MustExec("drop materialized view mv1")
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.mv_refresh_info where mv_id = %d", mvID)).Check(testkit.Rows("0"))
}

func TestMVCreateMaterializedViewRejectsWhereSubquery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_materialized_view = 1")
	tk.MustExec("create table t_subq (a int, b int)")
	tk.MustExec("create table t_subq2 (a int)")
	tk.MustExec("create materialized view log on t_subq(a,b)")

	tk.MustGetErrMsg(
		"create materialized view mv_subq as select a, count(*), sum(b), min(b), max(b) from t_subq where a in (select a from t_subq2) group by a refresh fast every 60",
		"materialized view query does not support subquery in WHERE clause",
	)
}
