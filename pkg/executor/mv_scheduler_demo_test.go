//go:build intest

package executor_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestMVDemoSchedulerPurgeLogByMinLastRefreshTSO(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_materialized_view_demo = 1")
	tk.MustExec("create table t (id int primary key, a int, b int)")
	tk.MustExec("create materialized view log on t(a,b)")

	// Existing log entries (before MV build) should become purgeable after all dependent MVs are built.
	tk.MustExec("insert into t values (1,1,10),(2,1,20),(3,2,30)")
	logTbl := getMVLogTableName(t, tk, "test", "t")
	logRows := tk.MustQuery(fmt.Sprintf("select count(*) from %s", logTbl)).Rows()[0][0].(string)
	require.NotEqual(t, "0", logRows)

	tk.MustExec("create materialized view mv1 as select a, count(*), sum(b), min(b), max(b) from t group by a refresh fast every 60")
	tk.MustExec("create materialized view mv2 as select a, count(*), sum(b), min(b), max(b) from t group by a refresh fast every 60")

	dom := domain.GetDomain(tk.Session())
	require.NotNil(t, dom)
	require.NoError(t, dom.RunMaterializedViewDemoSchedulerOnceForTest())

	tk.MustQuery(fmt.Sprintf("select count(*) from %s", logTbl)).Check(testkit.Rows("0"))
}

func TestMVDemoSchedulerRefreshDueMV(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_materialized_view_demo = 1")
	tk.MustExec("create table t (id int primary key, a int, b int)")
	tk.MustExec("create materialized view log on t(a,b)")

	tk.MustExec("insert into t values (1,1,10),(2,1,20)")
	tk.MustExec("create materialized view mv as select a, count(*), sum(b), min(b), max(b) from t where b > 0 group by a refresh fast every 60")

	is := tk.Session().GetInfoSchema().(infoschema.InfoSchema)
	mvTbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
	require.NoError(t, err)
	mvID := mvTbl.Meta().ID

	oldTSOStr := tk.MustQuery(fmt.Sprintf("select last_refresh_tso from mysql.mv_refresh_info where mv_id = %d", mvID)).Rows()[0][0].(string)
	oldTSO, err := strconv.ParseUint(oldTSOStr, 10, 64)
	require.NoError(t, err)

	// Generate incremental changes after build.
	tk.MustExec("insert into t values (3,1,30)")
	tk.MustExec("update mysql.mv_refresh_info set next_run_time = NOW() where mv_id = " + fmt.Sprint(mvID))

	dom := domain.GetDomain(tk.Session())
	require.NotNil(t, dom)
	require.NoError(t, dom.RunMaterializedViewDemoSchedulerOnceForTest())

	newTSOStr := tk.MustQuery(fmt.Sprintf("select last_refresh_tso from mysql.mv_refresh_info where mv_id = %d", mvID)).Rows()[0][0].(string)
	newTSO, err := strconv.ParseUint(newTSOStr, 10, 64)
	require.NoError(t, err)
	require.Greater(t, newTSO, oldTSO)

	mvDemoPrepareSnapshotRead(tk)
	tk.MustExec(fmt.Sprintf("set @@tidb_snapshot = '%s'", newTSOStr))
	baseQuery := `select a, count(*), sum(b), min(b), max(b)
			   from t
			  where b > 0
			  group by a
			  order by a`
	baseRows := tk.MustQuery(baseQuery).Rows()
	tk.MustExec("set @@tidb_snapshot = ''")
	require.Equal(t, baseRows, tk.MustQuery("select * from mv order by a").Rows())
}
