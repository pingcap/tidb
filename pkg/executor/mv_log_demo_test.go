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

func getMVLogTableName(t *testing.T, tk *testkit.TestKit, schemaName, baseTableName string) string {
	t.Helper()
	is := tk.Session().GetInfoSchema().(infoschema.InfoSchema)
	schema := ast.NewCIStr(schemaName)
	baseTbl, err := is.TableByName(context.Background(), schema, ast.NewCIStr(baseTableName))
	require.NoError(t, err)
	baseID := baseTbl.Meta().ID

	tblInfos, err := is.SchemaTableInfos(context.Background(), schema)
	require.NoError(t, err)
	for _, tbl := range tblInfos {
		if tbl != nil && tbl.IsMaterializedViewLog() && tbl.MaterializedViewLogInfo.BaseTableID == baseID {
			return tbl.Name.O
		}
	}
	require.FailNowf(t, "mv log not found", "schema=%s table=%s base_id=%d", schemaName, baseTableName, baseID)
	return ""
}

func TestMVDemoMVLogWritePathInsertUpdateDelete(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_materialized_view_demo = 1")
	tk.MustExec("create table t (id int primary key, a int, b int)")
	tk.MustExec("create materialized view log on t(a,b)")

	logTbl := getMVLogTableName(t, tk, "test", "t")

	tk.MustExec("insert into t values (1,10,20)")
	tk.MustExec("update t set b = 21 where id = 1")
	tk.MustExec("delete from t where id = 1")

	tk.MustQuery(fmt.Sprintf("select a,b,__mv_dml_type,__mv_old_new from %s order by _tidb_rowid", logTbl)).
		Check(testkit.Rows(
			"10 20 I N",
			"10 20 U O",
			"10 21 U N",
			"10 21 D O",
		))

	tk.MustGetErrMsg(fmt.Sprintf("insert into %s(a,b,__mv_dml_type,__mv_old_new) values (1,2,'I','N')", logTbl), "insert into materialized view log "+logTbl+" is not supported now")
	tk.MustGetErrMsg(fmt.Sprintf("update %s set a = 1", logTbl), "update materialized view log "+logTbl+" is not supported now")
	tk.MustGetErrMsg(fmt.Sprintf("delete from %s", logTbl), "delete materialized view log "+logTbl+" is not supported now")
}

func TestMVDemoMVLogWritePathInsertOnDuplicateKeyUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_materialized_view_demo = 1")
	tk.MustExec("create table t2 (id int primary key, a int, b int)")
	tk.MustExec("create materialized view log on t2(a,b)")

	logTbl := getMVLogTableName(t, tk, "test", "t2")

	tk.MustExec("insert into t2 values (1,10,20)")
	tk.MustExec("insert into t2 values (1,11,22) on duplicate key update a = values(a), b = values(b)")

	tk.MustQuery(fmt.Sprintf("select a,b,__mv_dml_type,__mv_old_new from %s order by _tidb_rowid", logTbl)).
		Check(testkit.Rows(
			"10 20 I N",
			"10 20 U O",
			"11 22 U N",
		))
}
