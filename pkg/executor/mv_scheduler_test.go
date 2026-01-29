//go:build intest

package executor_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestMVSchedulerPurgeLogByMinLastRefreshTSO(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_materialized_view = 1")
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
	require.NoError(t, dom.RunMaterializedViewSchedulerOnceForTest())

	tk.MustQuery(fmt.Sprintf("select count(*) from %s", logTbl)).Check(testkit.Rows("0"))
}

func TestMVSchedulerRefreshDueMV(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_materialized_view = 1")
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
	require.NoError(t, dom.RunMaterializedViewSchedulerOnceForTest())

	newTSOStr := tk.MustQuery(fmt.Sprintf("select last_refresh_tso from mysql.mv_refresh_info where mv_id = %d", mvID)).Rows()[0][0].(string)
	newTSO, err := strconv.ParseUint(newTSOStr, 10, 64)
	require.NoError(t, err)
	require.Greater(t, newTSO, oldTSO)

	mvPrepareSnapshotRead(tk)
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

func TestMVSchedulerRefreshFailureDoesNotAdvanceLastRefreshTSO(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_materialized_view = 1")
	tk.MustExec("create table t (id int primary key, a int, b int)")
	tk.MustExec("create materialized view log on t(a,b)")

	tk.MustExec("insert into t values (1,1,10),(2,1,20)")
	tk.MustExec("create materialized view mv as select a, count(*), sum(b), min(b), max(b) from t where b > 0 group by a refresh fast every 60")

	is := tk.Session().GetInfoSchema().(infoschema.InfoSchema)
	mvTbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
	require.NoError(t, err)
	mvID := mvTbl.Meta().ID

	row := tk.MustQuery(fmt.Sprintf("select last_refresh_tso from mysql.mv_refresh_info where mv_id = %d", mvID)).Rows()[0]
	oldTSOStr := row[0].(string)
	oldTSO, err := strconv.ParseUint(oldTSOStr, 10, 64)
	require.NoError(t, err)

	// Generate incremental changes after build and make it due for refresh.
	tk.MustExec("insert into t values (3,1,30)")
	tk.MustExec("update mysql.mv_refresh_info set next_run_time = NOW() where mv_id = " + fmt.Sprint(mvID))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mvFastRefreshFailAfterDeltaQuery", `return(true)`))
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mvFastRefreshFailAfterDeltaQuery")
	}()

	dom := domain.GetDomain(tk.Session())
	require.NotNil(t, dom)
	require.NoError(t, dom.RunMaterializedViewSchedulerOnceForTest())

	// last_refresh_tso should not advance on failure.
	failTSOStr := tk.MustQuery(fmt.Sprintf("select last_refresh_tso from mysql.mv_refresh_info where mv_id = %d", mvID)).Rows()[0][0].(string)
	failTSO, err := strconv.ParseUint(failTSOStr, 10, 64)
	require.NoError(t, err)
	require.Equal(t, oldTSO, failTSO)

	failRow := tk.MustQuery(fmt.Sprintf("select last_refresh_result, last_error from mysql.mv_refresh_info where mv_id = %d", mvID)).Rows()[0]
	require.Equal(t, "FAILED", failRow[0].(string))
	require.Contains(t, failRow[1].(string), "mvFastRefreshFailAfterDeltaQuery")

	// Retry after clearing the failpoint.
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mvFastRefreshFailAfterDeltaQuery"))
	tk.MustExec("update mysql.mv_refresh_info set next_run_time = NOW() where mv_id = " + fmt.Sprint(mvID))
	require.NoError(t, dom.RunMaterializedViewSchedulerOnceForTest())

	newTSOStr := tk.MustQuery(fmt.Sprintf("select last_refresh_tso from mysql.mv_refresh_info where mv_id = %d", mvID)).Rows()[0][0].(string)
	newTSO, err := strconv.ParseUint(newTSOStr, 10, 64)
	require.NoError(t, err)
	require.Greater(t, newTSO, oldTSO)

	okRow := tk.MustQuery(fmt.Sprintf("select last_refresh_result, last_error from mysql.mv_refresh_info where mv_id = %d", mvID)).Rows()[0]
	require.Equal(t, "SUCCESS", okRow[0].(string))
	require.Equal(t, "<nil>", fmt.Sprint(okRow[1]))

	mvPrepareSnapshotRead(tk)
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
