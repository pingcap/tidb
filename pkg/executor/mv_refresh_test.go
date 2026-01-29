package executor_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func mvPrepareSnapshotRead(tk *testkit.TestKit) {
	// The `tikv_gc_safe_point` global variable must be there, otherwise `set @@tidb_snapshot` fails in unit tests.
	timeSafe := time.Now().Add(-48 * time.Hour).Format("20060102-15:04:05 -0700 MST")
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	tk.MustExec(fmt.Sprintf(safePointSQL, timeSafe))
}

func TestMVAdminRefreshMaterializedViewFast(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_materialized_view = 1")
	tk.MustExec("create table t (id int primary key, a int, b int)")
	tk.MustExec("create materialized view log on t(a,b)")

	tk.MustExec("insert into t values (1,1,10),(2,1,20),(3,null,5),(4,2,0)")
	tk.MustExec("create materialized view mv as select a, count(*), sum(b), min(b), max(b) from t where b > 0 group by a refresh fast every 60")

	is := tk.Session().GetInfoSchema().(infoschema.InfoSchema)
	mvTbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
	require.NoError(t, err)
	mvID := mvTbl.Meta().ID
	buildTSO := tk.MustQuery(fmt.Sprintf("select last_refresh_tso from mysql.mv_refresh_info where mv_id = %d", mvID)).Rows()[0][0].(string)
	logTbl := getMVLogTableName(t, tk, "test", "t")
	logRowsBeforeStr := tk.MustQuery(fmt.Sprintf("select count(*) from %s", logTbl)).Rows()[0][0].(string)
	logRowsBefore, err := strconv.Atoi(logRowsBeforeStr)
	require.NoError(t, err)

	// Generate incremental changes after build.
	tk.MustExec("insert into t values (5,1,30)")
	tk.MustExec("update t set b = 0 where id = 2")
	tk.MustExec("delete from t where id = 1")
	tk.MustExec("insert into t values (6,null,7)")
	tk.MustExec("update t set a = 2, b = 9 where id = 4")

	logRowsAfterStr := tk.MustQuery(fmt.Sprintf("select count(*) from %s", logTbl)).Rows()[0][0].(string)
	logRowsAfter, err := strconv.Atoi(logRowsAfterStr)
	require.NoError(t, err)
	require.Greater(t, logRowsAfter, logRowsBefore)

	buildTSOInt, err := strconv.ParseUint(buildTSO, 10, 64)
	require.NoError(t, err)
	logMaxCommitTSStr := tk.MustQuery(fmt.Sprintf("select ifnull(max(_tidb_commit_ts), 0) from %s", logTbl)).Rows()[0][0].(string)
	logMaxCommitTS, err := strconv.ParseUint(logMaxCommitTSStr, 10, 64)
	require.NoError(t, err)
	require.Greater(t, logMaxCommitTS, buildTSOInt)

	tk.MustExec("admin refresh materialized view mv fast")

	lastTSO := tk.MustQuery(fmt.Sprintf("select last_refresh_tso from mysql.mv_refresh_info where mv_id = %d", mvID)).Rows()[0][0].(string)
	mvPrepareSnapshotRead(tk)
	tk.MustExec(fmt.Sprintf("set @@tidb_snapshot = '%s'", lastTSO))
	baseQuery := `select a, count(*), sum(b), min(b), max(b)
		   from t
		  where b > 0
		  group by a
		  order by a`
	mvQuery := "select * from mv order by a"
	baseRows := tk.MustQuery(baseQuery).Rows()
	tk.MustExec("set @@tidb_snapshot = ''")
	require.Equal(t, baseRows, tk.MustQuery(mvQuery).Rows())
}

func TestMVAdminRefreshMaterializedViewComplete(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_materialized_view = 1")
	tk.MustExec("create table t (id int primary key, a int, b int)")
	tk.MustExec("create materialized view log on t(a,b)")

	tk.MustExec("insert into t values (1,1,10),(2,1,20)")
	tk.MustExec("create materialized view mv as select a, count(*), sum(b), min(b), max(b) from t group by a refresh fast every 60")

	is := tk.Session().GetInfoSchema().(infoschema.InfoSchema)
	mvTbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
	require.NoError(t, err)
	mvID := mvTbl.Meta().ID

	tk.MustExec("insert into t values (3,1,30)")
	tk.MustExec("delete from t where id = 2")

	tk.MustExec("admin refresh materialized view mv complete")

	lastTSO := tk.MustQuery(fmt.Sprintf("select last_refresh_tso from mysql.mv_refresh_info where mv_id = %d", mvID)).Rows()[0][0].(string)
	mvPrepareSnapshotRead(tk)
	tk.MustExec(fmt.Sprintf("set @@tidb_snapshot = '%s'", lastTSO))
	baseQuery := `select a, count(*), sum(b), min(b), max(b)
		   from t
		  group by a
		  order by a`
	mvQuery := "select * from mv order by a"
	baseRows := tk.MustQuery(baseQuery).Rows()
	tk.MustExec("set @@tidb_snapshot = ''")
	require.Equal(t, baseRows, tk.MustQuery(mvQuery).Rows())
}

func TestMVAdminRefreshCompleteFailsWhenRefreshInfoMissing(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_materialized_view = 1")
	tk.MustExec("create table t (id int primary key, a int, b int)")
	tk.MustExec("create materialized view log on t(a,b)")

	tk.MustExec("insert into t values (1,1,10)")
	tk.MustExec("create materialized view mv as select a, count(*), sum(b), min(b), max(b) from t group by a refresh fast every 60")

	is := tk.Session().GetInfoSchema().(infoschema.InfoSchema)
	mvTbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
	require.NoError(t, err)
	mvID := mvTbl.Meta().ID

	tk.MustExec(fmt.Sprintf("delete from mysql.mv_refresh_info where mv_id = %d", mvID))
	tk.MustGetErrMsg(
		"admin refresh materialized view mv complete",
		fmt.Sprintf("mv_refresh_info row not found for mv_id=%d", mvID),
	)
}

func TestMVCommitTsColumnReturnsNonZero(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_materialized_view = 1")
	tk.MustExec("create table t (id int primary key, v int)")
	tk.MustExec("insert into t values (1,10),(2,20),(3,30)")

	var sawCommitTSInReq bool
	var capturedScanColIDs []int64
	var capturedReqGoType string
	var capturedCoprTp int64
	var mu sync.Mutex
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/store/copr/onBeforeSendReqCtx", func(req *tikvrpc.Request) {
		var data []byte
		switch r := req.Req.(type) {
		case *coprocessor.Request:
			mu.Lock()
			if capturedReqGoType == "" {
				capturedReqGoType = "coprocessor.Request"
				capturedCoprTp = r.GetTp()
			}
			mu.Unlock()
			data = r.Data
		case *coprocessor.BatchRequest:
			mu.Lock()
			if capturedReqGoType == "" {
				capturedReqGoType = "coprocessor.BatchRequest"
				capturedCoprTp = r.GetTp()
			}
			mu.Unlock()
			data = r.Data
		default:
			return
		}
		var dag tipb.DAGRequest
		if err := proto.Unmarshal(data, &dag); err != nil {
			return
		}

		var cols []*tipb.ColumnInfo
		for _, exec := range dag.Executors {
			if exec == nil {
				continue
			}
			if exec.TblScan != nil {
				cols = exec.TblScan.Columns
				break
			}
			if exec.IdxScan != nil {
				cols = exec.IdxScan.Columns
				break
			}
		}
		if cols == nil && dag.RootExecutor != nil {
			var findScanCols func(exec *tipb.Executor) []*tipb.ColumnInfo
			findScanCols = func(exec *tipb.Executor) []*tipb.ColumnInfo {
				if exec == nil {
					return nil
				}
				if exec.TblScan != nil {
					return exec.TblScan.Columns
				}
				if exec.IdxScan != nil {
					return exec.IdxScan.Columns
				}
				switch exec.Tp {
				case tipb.ExecType_TypeTopN:
					return findScanCols(exec.TopN.GetChild())
				case tipb.ExecType_TypeLimit:
					return findScanCols(exec.Limit.GetChild())
				case tipb.ExecType_TypeSelection:
					return findScanCols(exec.Selection.GetChild())
				case tipb.ExecType_TypeAggregation, tipb.ExecType_TypeStreamAgg:
					return findScanCols(exec.Aggregation.GetChild())
				case tipb.ExecType_TypeExchangeSender:
					return findScanCols(exec.ExchangeSender.GetChild())
				default:
					return nil
				}
			}
			cols = findScanCols(dag.RootExecutor)
		}
		if cols == nil {
			return
		}

		mu.Lock()
		if capturedScanColIDs == nil {
			capturedScanColIDs = make([]int64, 0, len(cols))
			for _, col := range cols {
				capturedScanColIDs = append(capturedScanColIDs, col.GetColumnId())
			}
		}
		mu.Unlock()
		for _, col := range cols {
			if col.GetColumnId() == model.ExtraCommitTSID {
				sawCommitTSInReq = true
				return
			}
		}
	})

	t.Logf("store=%T name=%s", store, store.Name())
	t.Logf("plan=%v", tk.MustQuery("explain select ifnull(max(_tidb_commit_ts), 0) from t").Rows())
	t.Logf("plan-select=%v", tk.MustQuery("explain select _tidb_commit_ts from t order by id").Rows())
	t.Logf("plan-select-verbose=%v", tk.MustQuery("explain format='verbose' select _tidb_commit_ts from t order by id").Rows())
	t.Logf("plan-select-json=%v", tk.MustQuery("explain format='tidb_json' select _tidb_commit_ts from t order by id").Rows())
	t.Logf("rows=%v", tk.MustQuery("select _tidb_commit_ts from t order by id").Rows())
	t.Logf("sawCommitTsInCopRequest=%v", sawCommitTSInReq)
	t.Logf("capturedScanColIDs=%v", capturedScanColIDs)
	t.Logf("capturedCoprReqType=%s tp=%d", capturedReqGoType, capturedCoprTp)

	maxCommitTSStr := tk.MustQuery("select ifnull(max(_tidb_commit_ts), 0) from t").Rows()[0][0].(string)
	maxCommitTS, err := strconv.ParseUint(maxCommitTSStr, 10, 64)
	require.NoError(t, err)
	require.Greater(t, maxCommitTS, uint64(0))
}
