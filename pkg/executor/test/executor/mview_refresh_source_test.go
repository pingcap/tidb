// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
)

func newMViewRefreshSourceFixture(t *testing.T) (kv.Storage, *testkit.TestKit, int64, int64) {
	t.Helper()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_source_init (a int not null, b int not null)")
	tk.MustExec("insert into t_source_init values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_source_init (a, b)")
	tk.MustExec("create materialized view mv_source_init (a, s, cnt) refresh fast as select a, sum(b), count(*) from t_source_init group by a")
	mv, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_source_init"))
	require.NoError(t, err)
	base, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t_source_init"))
	require.NoError(t, err)
	return store, tk, mv.Meta().ID, base.Meta().MaterializedViewBase.MLogID
}

func TestMViewRefreshSourceInfoRejectsMissingRecords(t *testing.T) {
	for _, nested := range []bool{false, true} {
		t.Run(fmt.Sprintf("nested=%t", nested), func(t *testing.T) {
			_, tk, _, _ := newMViewRefreshSourceFixture(t)
			name := "mv_source_init"
			if nested {
				tk.MustExec("create materialized view log on mv_source_init (a, s, cnt)")
				tk.MustExec("create materialized view mv_source_child (a, cnt) refresh fast as select a, count(*) from mv_source_init group by a")
				name = "mv_source_child"
			}
			sourceSQL := fmt.Sprintf("select * from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = (select TIDB_TABLE_ID from information_schema.tables where table_schema = 'test' and table_name = '%s')", name)
			tk.MustExec(fmt.Sprintf("delete from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = (select TIDB_TABLE_ID from information_schema.tables where table_schema = 'test' and table_name = '%s')", name))
			beforeRefresh := tk.MustQuery("select * from mysql.tidb_mview_refresh_info order by MVIEW_ID").Rows()
			beforeData := tk.MustQuery("select * from " + name + " order by a").Rows()
			for _, method := range []string{"fast", "complete delta apply", "complete in place", "complete out of place"} {
				t.Run(method, func(t *testing.T) {
					require.ErrorContains(t, tk.ExecToErr("refresh materialized view "+name+" "+method), "input log records are missing")
					tk.MustQuery(sourceSQL).Check(testkit.Rows())
					require.Equal(t, beforeRefresh, tk.MustQuery("select * from mysql.tidb_mview_refresh_info order by MVIEW_ID").Rows())
					require.Equal(t, beforeData, tk.MustQuery("select * from "+name+" order by a").Rows())
				})
			}
		})
	}
}

func TestMViewRefreshSourceInfoCutoverCancelRollsBack(t *testing.T) {
	store, tk, mviewID, mlogID := newMViewRefreshSourceFixture(t)
	before := tk.MustQuery("select * from mysql.tidb_mview_refresh_info").Rows()
	tkInject := testkit.NewTestKit(t, store)
	var injected atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type == model.ActionMViewRefreshOutOfPlaceCutover && job.TableID == mviewID && injected.CompareAndSwap(false, true) {
			// Force a validation error after cutover has staged refresh-info migration.
			tkInject.MustExec(fmt.Sprintf("delete from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = %d", mviewID))
		}
	})
	tk.MustExec("insert into t_source_init values (1, 5)")
	require.ErrorContains(t, tk.ExecToErr("refresh materialized view mv_source_init complete out of place"), "input log records are missing")
	require.True(t, injected.Load())
	tk.MustQuery("select TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA = 'test' and TABLE_NAME = 'mv_source_init'").Check(testkit.Rows(fmt.Sprint(mviewID)))
	require.Equal(t, before, tk.MustQuery("select * from mysql.tidb_mview_refresh_info").Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_fast_refresh_source_info").Check(testkit.Rows("0"))
	tk.MustExec(fmt.Sprintf("insert into mysql.tidb_mview_fast_refresh_source_info (MVIEW_ID, MLOG_ID) values (%d, %d)", mviewID, mlogID))
	tk.MustExec("refresh materialized view mv_source_init complete out of place")
	tk.MustQuery("select * from mv_source_init order by a").Check(testkit.Rows("1 15 2", "2 20 1"))
	tk.MustQuery("select count(*) from mysql.tidb_mview_fast_refresh_source_info").Check(testkit.Rows("1"))
}

func TestMViewRefreshSourceInfoCutoverCancelAfterSQL(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("changed=%t", changed), func(t *testing.T) {
			_, tk, mviewID, _ := newMViewRefreshSourceFixture(t)
			if changed {
				tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_fast_refresh_source_info set MLOG_ID = -1 where MVIEW_ID = %d", mviewID))
			}
			beforeRefresh := tk.MustQuery("select * from mysql.tidb_mview_refresh_info").Rows()
			beforeSources := tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info").Rows()
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockMViewRefreshOutOfPlaceCutoverAfterMigrateRefreshInfoError", "1*return()")
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunAfter", func(job *model.Job) {
				if job.Type == model.ActionMViewRefreshOutOfPlaceCutover && job.TableID == mviewID && job.Error != nil {
					// Exercise terminal cancellation after SQL has released the runtime writes
					// from statement staging into the DDL transaction buffer.
					job.State = model.JobStateCancelled
				}
			})
			tk.MustExec("insert into t_source_init values (1, 5)")
			require.ErrorContains(t, tk.ExecToErr("refresh materialized view mv_source_init complete out of place"), "error after migrating refresh info")
			require.Equal(t, beforeRefresh, tk.MustQuery("select * from mysql.tidb_mview_refresh_info").Rows())
			require.Equal(t, beforeSources, tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info").Rows())
			tk.MustQuery("select TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA = 'test' and TABLE_NAME = 'mv_source_init'").Check(testkit.Rows(fmt.Sprint(mviewID)))
		})
	}
}

func TestMViewRefreshSourceInfoBaselineRecovery(t *testing.T) {
	for _, method := range []string{"complete delta apply", "complete in place", "complete out of place"} {
		t.Run(method, func(t *testing.T) {
			_, tk, mviewID, mlogID := newMViewRefreshSourceFixture(t)
			var err error
			for _, logs := range [][]int64{{-1}, {0}, {mlogID, -1}, {0, mlogID}} {
				tk.MustExec(fmt.Sprintf("delete from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = %d", mviewID))
				for _, logID := range logs {
					tk.MustExec(fmt.Sprintf("insert into mysql.tidb_mview_fast_refresh_source_info values (%d, %d)", mviewID, logID))
				}
				before := tk.MustQuery("select * from mysql.tidb_mview_refresh_info").Rows()
				beforeSources := tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID").Rows()
				beforeData := tk.MustQuery("select * from mv_source_init order by a").Rows()
				tk.MustExec("insert into t_source_init values (1, 5)")
				tk.MustGetErrCode("refresh materialized view mv_source_init fast", int(exeerrors.ErrMVRefreshLogBaselineChanged.Code()))
				require.Equal(t, before, tk.MustQuery("select * from mysql.tidb_mview_refresh_info").Rows())
				require.Equal(t, beforeSources, tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID").Rows())
				require.Equal(t, beforeData, tk.MustQuery("select * from mv_source_init order by a").Rows())
				tk.MustExec("refresh materialized view mv_source_init " + method)
				require.Equal(t, tk.MustQuery("select a, sum(b), count(*) from t_source_init group by a order by a").Rows(),
					tk.MustQuery("select * from mv_source_init order by a").Rows())
				mviewID, err = strconv.ParseInt(fmt.Sprint(tk.MustQuery("select TIDB_TABLE_ID from information_schema.tables where table_schema='test' and table_name='mv_source_init'").Rows()[0][0]), 10, 64)
				require.NoError(t, err)
				tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info").Check(testkit.Rows(fmt.Sprintf("%d %d", mviewID, mlogID)))
				tk.MustExec("insert into t_source_init values (2, 3)")
				tk.MustExec("refresh materialized view mv_source_init fast")
				require.Equal(t, tk.MustQuery("select a, sum(b), count(*) from t_source_init group by a order by a").Rows(),
					tk.MustQuery("select * from mv_source_init order by a").Rows())
			}
		})
	}
	t.Run("unchanged input", func(t *testing.T) {
		for _, method := range []string{"complete delta apply", "complete in place"} {
			t.Run(method, func(t *testing.T) {
				store, tk, mviewID, mlogID := newMViewRefreshSourceFixture(t)
				holder := testkit.NewTestKit(t, store)
				holder.MustExec("begin pessimistic")
				defer holder.MustExec("rollback")
				holder.MustQuery(fmt.Sprintf(
					"select MLOG_ID from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = %d and MLOG_ID = %d for update",
					mviewID, mlogID,
				)).Check(testkit.Rows(fmt.Sprint(mlogID)))
				// Holding the registration lock must not block a COMPLETE whose
				// input baseline is unchanged. A redundant rewrite would time out.
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/mvRefreshInputCaptured", func(sctx sessionctx.Context, _ uint64) {
					require.NoError(t, sctx.GetSessionVars().SetSystemVar(variable.InnodbLockWaitTimeout, "1"))
				})
				beforeTS := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).Rows()
				tk.MustExec("insert into t_source_init values (1, 5)")
				tk.MustExec("refresh materialized view mv_source_init " + method)
				tk.MustQuery("select * from mv_source_init order by a").Check(testkit.Rows("1 15 2", "2 20 1"))
				tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info").Check(testkit.Rows(fmt.Sprintf("%d %d", mviewID, mlogID)))
				afterTS := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).Rows()
				before, err := strconv.ParseUint(fmt.Sprint(beforeTS[0][0]), 10, 64)
				require.NoError(t, err)
				after, err := strconv.ParseUint(fmt.Sprint(afterTS[0][0]), 10, 64)
				require.NoError(t, err)
				require.Greater(t, after, before)
			})
		}
	})
	t.Run("unchanged out-of-place input", func(t *testing.T) {
		_, tk, oldMViewID, mlogID := newMViewRefreshSourceFixture(t)
		// A migration must preserve additional metadata when the log set is unchanged.
		tk.MustExec("alter table mysql.tidb_mview_fast_refresh_source_info add column EXTRA int default 7")
		tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_fast_refresh_source_info set EXTRA = 42 where MVIEW_ID = %d", oldMViewID))
		tk.MustExec("insert into t_source_init values (1, 5)")
		tk.MustExec("refresh materialized view mv_source_init complete out of place")
		newIDRows := tk.MustQuery("select TIDB_TABLE_ID from information_schema.tables where table_schema='test' and table_name='mv_source_init'").Rows()
		require.Len(t, newIDRows, 1)
		newMViewID, err := strconv.ParseInt(fmt.Sprint(newIDRows[0][0]), 10, 64)
		require.NoError(t, err)
		require.NotEqual(t, oldMViewID, newMViewID)
		tk.MustQuery("select MVIEW_ID, MLOG_ID, EXTRA from mysql.tidb_mview_fast_refresh_source_info").
			Check(testkit.Rows(fmt.Sprintf("%d %d 42", newMViewID, mlogID)))
		tk.MustQuery("select * from mv_source_init order by a").Check(testkit.Rows("1 15 2", "2 20 1"))
		tk.MustExec("insert into t_source_init values (2, 3)")
		tk.MustExec("refresh materialized view mv_source_init fast")
		tk.MustQuery("select * from mv_source_init order by a").Check(testkit.Rows("1 15 2", "2 23 2"))
	})
	t.Run("empty input", func(t *testing.T) {
		_, tk, _, mlogID := newMViewRefreshSourceFixture(t)
		tk.MustExec("drop materialized view mv_source_init")
		tk.MustExec("create materialized view mv_source_init (a, s, cnt) refresh fast as select a, sum(b), count(*) from t_source_init where false group by a")
		for _, method := range []string{"complete delta apply", "complete in place", "complete out of place"} {
			tk.MustExec("update mysql.tidb_mview_fast_refresh_source_info set MLOG_ID = 0")
			tk.MustExec("refresh materialized view mv_source_init " + method)
			tk.MustQuery("select * from mv_source_init").Check(testkit.Rows())
			tk.MustQuery("select MLOG_ID from mysql.tidb_mview_fast_refresh_source_info").Check(testkit.Rows(fmt.Sprint(mlogID)))
			tk.MustExec("refresh materialized view mv_source_init fast")
		}
	})
}

func TestMViewRefreshSourceInfoRecoveryAtomicity(t *testing.T) {
	_, tk, mviewID, _ := newMViewRefreshSourceFixture(t)
	tk.MustExec("create materialized view log on mv_source_init (a, s, cnt)")
	tk.MustExec("create materialized view mv_source_child (a, cnt) refresh fast as select a, count(*) from mv_source_init group by a")
	tk.MustExec("create materialized view mv_source_peer (a, s, cnt) refresh fast as select a, sum(b), count(*) from t_source_init group by a")
	tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_fast_refresh_source_info set MLOG_ID = -1 where MVIEW_ID = %d", mviewID))
	before := tk.MustQuery("select * from mysql.tidb_mview_refresh_info order by MVIEW_ID").Rows()
	beforeSources := tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID").Rows()
	tk.MustExec("insert into t_source_init values (1, 5), (3, 7)")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorAfterDataChanges", `return("source recovery rollback")`)
	require.ErrorContains(t, tk.ExecToErr("refresh materialized view mv_source_init complete delta apply"), "source recovery rollback")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorAfterDataChanges"))
	require.Equal(t, before, tk.MustQuery("select * from mysql.tidb_mview_refresh_info order by MVIEW_ID").Rows())
	require.Equal(t, beforeSources, tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID").Rows())
	tk.MustQuery("select * from mv_source_init order by a").Check(testkit.Rows("1 10 1", "2 20 1"))
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewAfterReplaceSourceInfoError", "return()")
	require.ErrorContains(t, tk.ExecToErr("refresh materialized view mv_source_init complete delta apply"), "after replacing source info")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewAfterReplaceSourceInfoError"))
	require.Equal(t, before, tk.MustQuery("select * from mysql.tidb_mview_refresh_info order by MVIEW_ID").Rows())
	require.Equal(t, beforeSources, tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID").Rows())
	tk.MustQuery("select * from mv_source_init order by a").Check(testkit.Rows("1 10 1", "2 20 1"))
	// A successful COMPLETE preserves the output log, so the child still FAST
	// refreshes. The peer's input baseline and watermark are unchanged.
	peerBefore := tk.MustQuery("select * from mysql.tidb_mview_refresh_info where MVIEW_ID = (select TIDB_TABLE_ID from information_schema.tables where table_schema='test' and table_name='mv_source_peer')").Rows()
	var computationTS uint64
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterReplaceSourceInfo", func(ts uint64) { computationTS = ts })
	tk.MustExec("refresh materialized view mv_source_init complete delta apply")
	require.NotZero(t, computationTS)
	tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).Check(testkit.Rows(fmt.Sprint(computationTS)))
	require.Equal(t, peerBefore, tk.MustQuery("select * from mysql.tidb_mview_refresh_info where MVIEW_ID = (select TIDB_TABLE_ID from information_schema.tables where table_schema='test' and table_name='mv_source_peer')").Rows())
	tk.MustExec("refresh materialized view mv_source_child fast")
	tk.MustQuery("select * from mv_source_child order by a").Check(testkit.Rows("1 1", "2 1", "3 1"))
}

// Override immutable metadata to model a parent cutover at a specific snapshot,
// while parent OOP is still guarded by this PR.
type mviewRefreshSourceSnapshot struct {
	infoschema.InfoSchema
	replaced map[int64]table.Table
}

func (is *mviewRefreshSourceSnapshot) TableByID(ctx context.Context, id int64) (table.Table, bool) {
	if tbl, ok := is.replaced[id]; ok {
		return tbl, tbl != nil
	}
	return is.InfoSchema.TableByID(ctx, id)
}

func TestMViewRefreshSourceInfoSnapshotIdentity(t *testing.T) {
	for _, method := range []string{"fast", "complete delta apply", "complete in place", "complete out of place"} {
		for _, change := range []string{"source replaced", "eliminated source replaced", "log replaced", "missing log", "planned input replaced", "snapshot and plan log replaced"} {
			if method != "fast" && (change == "source replaced" || change == "planned input replaced" || change == "snapshot and plan log replaced") {
				continue
			}
			t.Run(method+"/"+change, func(t *testing.T) {
				_, tk, mviewID, mlogID := newMViewRefreshSourceFixture(t)
				if change == "eliminated source replaced" {
					tk.MustExec("drop materialized view mv_source_init")
					tk.MustExec("create materialized view mv_source_init (a, s, cnt) refresh fast as select a, sum(b), count(*) from t_source_init where false group by a")
					id, err := strconv.ParseInt(fmt.Sprint(tk.MustQuery("select TIDB_TABLE_ID from information_schema.tables where table_schema = 'test' and table_name = 'mv_source_init'").Rows()[0][0]), 10, 64)
					require.NoError(t, err)
					mviewID = id
				}
				before := tk.MustQuery("select * from mysql.tidb_mview_refresh_info").Rows()
				beforeSources := tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info").Rows()
				beforeData := tk.MustQuery("select * from mv_source_init order by a").Rows()
				tk.MustExec("insert into t_source_init values (1, 5)")
				var checked bool
				replaceSnapshot := func(plan base.Plan, snapshot *infoschema.InfoSchema) {
					checked = true
					mv, ok := (*snapshot).TableByID(context.Background(), mviewID)
					require.True(t, ok)
					source, ok := (*snapshot).TableByID(context.Background(), mv.Meta().MaterializedView.BaseTableIDs[0])
					require.True(t, ok)
					log, ok := (*snapshot).TableByID(context.Background(), mlogID)
					require.True(t, ok)
					newMV, newSource, newLog := mv.Meta().Clone(), source.Meta().Clone(), log.Meta().Clone()
					replacements := map[int64]table.Table{}
					switch change {
					case "source replaced", "eliminated source replaced":
						newSource.ID += 10000
						newLog.MaterializedViewLog.BaseTableID = newSource.ID
						newMV.MaterializedView.BaseTableIDs = []int64{newSource.ID}
						replacements[source.Meta().ID] = nil
					case "log replaced", "snapshot and plan log replaced":
						newLog.ID += 10000
						newSource.MaterializedViewBase.MLogID = newLog.ID
						replacements[mlogID] = nil
						if change == "snapshot and plan log replaced" {
							plan.(*plannercore.MViewDeltaMerge).MLogTableID = newLog.ID
						}
					case "missing log":
						newSource.MaterializedViewBase.MLogID = 0
					case "planned input replaced":
						plan.(*plannercore.MViewDeltaMerge).MLogTableID += 10000
						return
					}
					replacements[newMV.ID] = tables.MockTableFromMeta(newMV)
					replacements[newSource.ID] = tables.MockTableFromMeta(newSource)
					replacements[newLog.ID] = tables.MockTableFromMeta(newLog)
					*snapshot = &mviewRefreshSourceSnapshot{InfoSchema: *snapshot, replaced: replacements}
				}
				if method == "fast" {
					testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/mvRefreshValidateInputSnapshot", func(_ uint64, plan base.Plan, snapshot *infoschema.InfoSchema) {
						replaceSnapshot(plan, snapshot)
					})
				} else {
					testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/mvRefreshResolveInputSnapshot", func(_ uint64, _ int64, snapshot *infoschema.InfoSchema) {
						replaceSnapshot(nil, snapshot)
					})
				}
				err := tk.ExecToErr("refresh materialized view mv_source_init " + method)
				if (change == "eliminated source replaced" || change == "log replaced" || change == "missing log") && method != "fast" {
					require.NoError(t, err)
					if change == "eliminated source replaced" {
						tk.MustQuery("select * from mv_source_init").Check(testkit.Rows())
					} else {
						tk.MustQuery("select * from mv_source_init order by a").Check(testkit.Rows("1 15 2", "2 20 1"))
					}
					expectedLogID := mlogID
					if change == "log replaced" {
						expectedLogID += 10000
					}
					if change == "missing log" {
						expectedLogID = 0
					}
					tk.MustQuery("select MLOG_ID from mysql.tidb_mview_fast_refresh_source_info").Check(testkit.Rows(fmt.Sprint(expectedLogID)))
				} else {
					require.Error(t, err)
					if method == "fast" && (change == "log replaced" || change == "missing log" || change == "snapshot and plan log replaced") {
						require.True(t, exeerrors.ErrMVRefreshLogBaselineChanged.Equal(err), "%v", err)
					}
					require.Equal(t, before, tk.MustQuery("select * from mysql.tidb_mview_refresh_info").Rows())
					require.Equal(t, beforeSources, tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info").Rows())
					require.Equal(t, beforeData, tk.MustQuery("select * from mv_source_init order by a").Rows())
				}
				require.True(t, checked)
			})
		}
	}
}

func TestMViewRefreshSourceInfoReadTSAfterRetry(t *testing.T) {
	for _, method := range []string{"fast", "complete delta apply"} {
		t.Run(method, func(t *testing.T) {
			_, tk, mviewID, _ := newMViewRefreshSourceFixture(t)
			tk.MustExec("insert into t_source_init values (1, 5)")
			var computationTS, metadataTS uint64
			var refreshSession sessionctx.Context
			var computationPlan base.Plan
			var validatedTS []uint64
			var validatedPlans []base.Plan
			var capturedValidationCount int
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/mvRefreshInputCaptured", func(sctx sessionctx.Context, ts uint64) {
				refreshSession, computationTS = sctx, ts
				computationPlan, _ = sctx.GetSessionVars().StmtCtx.GetPlan().(base.Plan)
				require.NotNil(t, computationPlan)
				require.NotEmpty(t, validatedTS)
				capturedValidationCount = len(validatedTS)
				require.Equal(t, computationTS, validatedTS[capturedValidationCount-1])
				if method == "fast" {
					require.Same(t, computationPlan, validatedPlans[capturedValidationCount-1])
				}
			})
			var retries int
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/beforePessimisticStmtErrorForNextAction", func() { retries++ })
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterInsertRefreshHistRunning", func() {
				testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/pessimisticLockReturnWriteConflict", "1*return(true)")
			})
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterDataChanges", func() {
				require.NotNil(t, refreshSession)
				_, err := refreshSession.GetSQLExecutor().ExecuteInternal(kv.WithInternalSourceType(context.Background(), kv.InternalTxnMVMaintenance),
					"UPDATE mysql.tidb_mview_fast_refresh_source_info SET MLOG_ID = MLOG_ID WHERE MVIEW_ID = %?", mviewID)
				require.NoError(t, err)
				metadataTS = refreshSession.GetSessionVars().TxnCtx.GetForUpdateTS()
			})
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/mvRefreshValidateInputSnapshot", func(ts uint64, plan base.Plan, _ *infoschema.InfoSchema) {
				validatedTS = append(validatedTS, ts)
				validatedPlans = append(validatedPlans, plan)
			})
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/mvRefreshResolveInputSnapshot", func(ts uint64, _ int64, _ *infoschema.InfoSchema) {
				validatedTS = append(validatedTS, ts)
			})
			tk.MustExec("refresh materialized view mv_source_init " + method)
			require.Greater(t, capturedValidationCount, 1)
			require.Len(t, validatedTS, capturedValidationCount)
			require.Positive(t, retries)
			require.Greater(t, metadataTS, computationTS)
			tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).Check(testkit.Rows(fmt.Sprint(computationTS)))
			tk.MustQuery("select * from mv_source_init order by a").Check(testkit.Rows("1 15 2", "2 20 1"))
		})
	}
}
