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

package mviewnested

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
)

func waitMVTaskCancelWatcherRequested(t *testing.T, watchNamePrefix string) <-chan struct{} {
	t.Helper()

	requestedCh := make(chan struct{})
	var requestedChClosed atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/mvTaskCancelWatcherRequested", func(watchName string) {
		if strings.HasPrefix(watchName, watchNamePrefix) && requestedChClosed.CompareAndSwap(false, true) {
			close(requestedCh)
		}
	})
	return requestedCh
}

func TestMaterializedViewRefreshCompleteOutOfPlaceReplacesMLog(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("create placement policy mv_oop_policy followers=2")
	tk.MustExec("alter database test placement policy=mv_oop_policy")
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast as select a, sum(b), count(1) from t group by a")
	tk.MustExec("create materialized view log on mv (a, s, cnt) purge next date_add(now(), interval 1 hour)")
	getInfo := func(name string) *model.TableInfo {
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(name))
		require.NoError(t, err)
		return tbl.Meta()
	}
	oldMV, oldLog := getInfo("mv"), getInfo("$mlog$mv")
	checkLogPlacement := func(logInfo *model.TableInfo) {
		require.NotNil(t, logInfo.PlacementPolicyRef)
		require.Equal(t, "mv_oop_policy", logInfo.PlacementPolicyRef.Name.L)
		bundle, ok := dom.InfoSchema().PlacementBundleByPhysicalTableID(logInfo.ID)
		require.True(t, ok)
		pdBundle, err := infosync.GetRuleBundle(context.Background(), bundle.ID)
		require.NoError(t, err)
		require.Equal(t, bundle, pdBundle)
	}
	checkLogPlacement(oldLog)
	tk.MustExec(fmt.Sprintf("update mysql.tidb_mlog_purge_info set LAST_PURGED_TSO = 1 where MLOG_ID = %d", oldLog.ID))
	for range 2 {
		tk.MustExec("refresh materialized view mv complete out of place")
		newMV, newLog := getInfo("mv"), getInfo("$mlog$mv")
		require.NotEqual(t, oldMV.ID, newMV.ID)
		require.NotEqual(t, oldLog.ID, newLog.ID)
		require.Equal(t, newLog.ID, newMV.MaterializedViewBase.MLogID)
		expectedLog := oldLog.MaterializedViewLog.Clone()
		expectedLog.BaseTableID = newMV.ID
		require.Equal(t, expectedLog, newLog.MaterializedViewLog)
		require.Equal(t, oldLog.Columns, newLog.Columns)
		require.ElementsMatch(t, oldLog.Indices, newLog.Indices)
		checkLogPlacement(newLog)
		tk.MustQuery("select count(*) from `$mlog$mv`").Check(testkit.Rows("0"))
		tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID = %d", oldLog.ID)).Check(testkit.Rows("0"))
		tk.MustQuery(fmt.Sprintf("select NEXT_PURGE_UNIX_SECONDS is not null, LAST_PURGED_TSO is null from mysql.tidb_mlog_purge_info where MLOG_ID = %d", newLog.ID)).Check(testkit.Rows("1 1"))
		_, ok := dom.InfoSchema().TableByID(context.Background(), oldLog.ID)
		require.False(t, ok)
		oldMV, oldLog = newMV, newLog
	}
	tk.MustExec("insert into t values (3, 30)")
	tk.MustExec("refresh materialized view mv fast")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 10 1", "2 20 1", "3 30 1"))
	tk.MustQuery("select count(*) > 0 from `$mlog$mv`").Check(testkit.Rows("1"))
	tk.MustQuery("show tables like '\\_\\_mv\\_shadow\\_%'").Check(testkit.Rows())
}

func TestMaterializedViewRefreshCompleteOutOfPlaceRebindsDependentMaterializedViews(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_oop (a int not null, b int not null)")
	tk.MustExec("insert into t_oop values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_oop (a, b)")
	tk.MustExec("create materialized view mv_oop (a, s, cnt) refresh fast as select a, sum(b), count(*) from t_oop group by a")
	tk.MustExec("create materialized view log on mv_oop (a, s, cnt)")
	for _, name := range []string{"child1", "child2"} {
		tk.MustExec("create materialized view " + name + " (a, s, cnt, cnt_s) refresh fast as select a, sum(s), count(*), count(s) from mv_oop group by a")
	}
	getInfo := func(name string) *model.TableInfo {
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(name))
		require.NoError(t, err)
		return tbl.Meta()
	}
	oldMV, oldLog := getInfo("mv_oop"), getInfo("$mlog$mv_oop")
	child1, child2 := getInfo("child1"), getInfo("child2")
	childIDs := fmt.Sprintf("%d,%d", child1.ID, child2.ID)
	childInfoSQL := "select * from mysql.tidb_mview_refresh_info where MVIEW_ID in (" + childIDs + ") order by MVIEW_ID"
	childSourcesSQL := "select * from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID in (" + childIDs + ") order by MVIEW_ID, MLOG_ID"
	beforeInfo, beforeSources := tk.MustQuery(childInfoSQL).Rows(), tk.MustQuery(childSourcesSQL).Rows()
	tk.MustExec("insert into t_oop values (3, 30)")
	tk.MustExec("refresh materialized view mv_oop complete out of place")
	newMV, newLog := getInfo("mv_oop"), getInfo("$mlog$mv_oop")
	require.NotEqual(t, oldMV.ID, newMV.ID)
	require.NotEqual(t, oldLog.ID, newLog.ID)
	require.ElementsMatch(t, []int64{child1.ID, child2.ID}, newMV.MaterializedViewBase.MViewIDs)
	require.ElementsMatch(t, []int64{child1.ID, child2.ID}, newLog.MaterializedViewLog.DependentMViewIDs)
	for _, name := range []string{"child1", "child2"} {
		require.Equal(t, []int64{newMV.ID}, getInfo(name).MaterializedView.BaseTableIDs)
		tk.MustGetErrCode("refresh materialized view "+name+" fast", int(exeerrors.ErrMVRefreshLogBaselineChanged.Code()))
		tk.MustQuery("select * from " + name + " order by a").Check(testkit.Rows("1 10 1 1", "2 20 1 1"))
	}
	require.Equal(t, beforeInfo, tk.MustQuery(childInfoSQL).Rows())
	require.Equal(t, beforeSources, tk.MustQuery(childSourcesSQL).Rows())
	tk.MustQuery("select count(*) from `$mlog$mv_oop`").Check(testkit.Rows("0"))

	// Old physical objects can be collected without deleting child registrations.
	jobs := tk.MustQuery("admin show ddl jobs where JOB_TYPE='refresh materialized view complete out-of-place cutover'").Rows()
	require.NotEmpty(t, jobs)
	tk.MustQuery(fmt.Sprintf("select count(*) from (select element_id from mysql.gc_delete_range where job_id=%v union all select element_id from mysql.gc_delete_range_done where job_id=%v) g", jobs[0][0], jobs[0][0])).Check(testkit.Rows("2"))

	// A child with an output log can recover its input with DELTA APPLY while
	// preserving the log consumed by the next level.
	tk.MustExec("create materialized view log on child1 (a, s, cnt, cnt_s)")
	tk.MustExec("create materialized view grandchild (a, s, cnt, cnt_s) refresh fast as select a, sum(s), count(*), count(s) from child1 group by a")
	childLogID := getInfo("$mlog$child1").ID
	tk.MustExec("refresh materialized view child1 complete delta apply")
	require.Equal(t, childLogID, getInfo("$mlog$child1").ID)
	tk.MustExec("refresh materialized view grandchild fast")
	tk.MustQuery("select * from grandchild order by a").Check(testkit.Rows("1 10 1 1", "2 20 1 1", "3 30 1 1"))
	tk.MustQuery(fmt.Sprintf("select MLOG_ID from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID=%d", child2.ID)).Check(testkit.Rows(fmt.Sprint(oldLog.ID)))

	// Purge must protect the new log using child2's watermark even though its
	// input registration still names the retired output log.
	tk.MustExec("insert into t_oop values (1, 5), (4, 40)")
	tk.MustExec("update t_oop set b=25 where a=2")
	tk.MustExec("delete from t_oop where a=3")
	tk.MustExec("refresh materialized view mv_oop fast")
	delta := tk.MustQuery("select * from `$mlog$mv_oop` order by _tidb_rowid").Rows()
	require.NotEmpty(t, delta)
	tk.MustExec("purge materialized view log on mv_oop")
	require.Equal(t, delta, tk.MustQuery("select * from `$mlog$mv_oop` order by _tidb_rowid").Rows())
	beforePeer := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID=%d", child2.ID)).Rows()
	tk.MustQuery(fmt.Sprintf("select LAST_PURGED_TSO from mysql.tidb_mlog_purge_info where MLOG_ID=%d", newLog.ID)).Check(beforePeer)
	tk.MustExec("refresh materialized view child1 fast")
	tk.MustExec("refresh materialized view child2 complete in place")
	for _, name := range []string{"child1", "child2"} {
		tk.MustQuery("select * from " + name + " order by a").Check(testkit.Rows("1 15 1 1", "2 25 1 1", "4 40 1 1"))
		tk.MustQuery(fmt.Sprintf("select MLOG_ID from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID=%d", getInfo(name).ID)).Check(testkit.Rows(fmt.Sprint(newLog.ID)))
	}

	// OOP of the intermediate view replaces its own output log, invalidating
	// only the next level. The sibling's recovered input remains valid.
	tk.MustExec("refresh materialized view child1 complete out of place")
	require.NotEqual(t, childLogID, getInfo("$mlog$child1").ID)
	tk.MustGetErrCode("refresh materialized view grandchild fast", int(exeerrors.ErrMVRefreshLogBaselineChanged.Code()))
	tk.MustExec("refresh materialized view grandchild complete delta apply")
	tk.MustExec("insert into t_oop values (5, 50)")
	for _, name := range []string{"mv_oop", "child1", "child2", "grandchild"} {
		tk.MustExec("refresh materialized view " + name + " fast")
	}
	tk.MustQuery("select * from grandchild order by a").Check(testkit.Rows("1 15 1 1", "2 25 1 1", "4 40 1 1", "5 50 1 1"))
	tk.MustExec("drop materialized view grandchild")
	tk.MustExec("drop materialized view log on child1")
	for _, name := range []string{"child1", "child2", "mv_oop"} {
		if name == "mv_oop" {
			tk.MustExec("drop materialized view log on mv_oop")
		}
		tk.MustExec("drop materialized view " + name)
	}
}

func TestMaterializedViewRefreshCompleteOutOfPlaceOutputMLogRollbackAndRetry(t *testing.T) {
	for _, point := range []string{
		"mockMViewRefreshOutOfPlaceCutoverAfterMigrateRefreshInfoError",
		"mockMViewRefreshOutOfPlaceCutoverAfterMetadataError",
	} {
		t.Run(point, func(t *testing.T) {
			_, tk, oldID, _ := newMViewRefreshSourceFixture(t)
			tk.MustExec("create materialized view log on mv_source_init (a, s, cnt)")
			tk.MustExec("create materialized view mv_child (a, s, cnt, cnt_s) refresh fast as select a, sum(s), count(*), count(s) from mv_source_init group by a")
			beforeInfo := tk.MustQuery("select * from mysql.tidb_mview_refresh_info order by MVIEW_ID").Rows()
			beforeSources := tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID").Rows()
			beforePurge := tk.MustQuery("select * from mysql.tidb_mlog_purge_info order by MLOG_ID").Rows()
			beforeTables := tk.MustQuery("select TABLE_NAME, TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' order by TABLE_NAME").Rows()
			tk.MustExec("insert into t_source_init values (3, 30)")
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/"+point, "1*return()")
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", func(job *model.Job) {
				if job.Type == model.ActionMViewRefreshOutOfPlaceCutover && job.TableID == oldID && job.Error != nil {
					job.State = model.JobStateCancelled
				}
			})
			require.Error(t, tk.ExecToErr("refresh materialized view mv_source_init complete out of place"))
			require.Equal(t, beforeInfo, tk.MustQuery("select * from mysql.tidb_mview_refresh_info order by MVIEW_ID").Rows())
			require.Equal(t, beforeSources, tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID").Rows())
			require.Equal(t, beforePurge, tk.MustQuery("select * from mysql.tidb_mlog_purge_info order by MLOG_ID").Rows())
			require.Equal(t, beforeTables, tk.MustQuery("select TABLE_NAME, TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' order by TABLE_NAME").Rows())
			tk.MustQuery("select * from mv_source_init order by a").Check(testkit.Rows("1 10 1", "2 20 1"))
			tk.MustExec("refresh materialized view mv_child fast")
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep"))

			// An internal retry must reuse the log ID allocated in the persisted job.
			var allocated atomic.Int64
			var attempts atomic.Int64
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/"+point, "1*return()")
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
				if job.Type != model.ActionMViewRefreshOutOfPlaceCutover || job.TableID != oldID || job.IsDone() {
					return
				}
				args, err := model.GetRefreshMaterializedViewCompleteOutOfPlaceCutoverArgs(job)
				require.NoError(t, err)
				require.Positive(t, args.NewMLogID)
				allocated.CompareAndSwap(0, args.NewMLogID)
				require.Equal(t, allocated.Load(), args.NewMLogID)
				attempts.Add(1)
			})
			tk.MustExec("refresh materialized view mv_source_init complete out of place")
			require.GreaterOrEqual(t, attempts.Load(), int64(2))
			tk.MustQuery("select TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' and TABLE_NAME='$mlog$mv_source_init'").Check(testkit.Rows(fmt.Sprint(allocated.Load())))
			tk.MustQuery("select * from mv_source_init order by a").Check(testkit.Rows("1 10 1", "2 20 1", "3 30 1"))
		})
	}
}

func TestMaterializedViewRefreshCompleteOutOfPlacePurgeConcurrency(t *testing.T) {
	for _, stage := range []string{"running", "starting", "resolved", "timeout"} {
		t.Run(stage, func(t *testing.T) {
			store, tk, oldMV, _ := newMViewRefreshSourceFixture(t)
			tk.MustExec("create materialized view log on mv_source_init (a, s, cnt)")
			tk.MustExec("insert into t_source_init values (3, 30)")
			tk.MustExec("refresh materialized view mv_source_init fast")
			oldLog := tk.MustQuery("select TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' and TABLE_NAME='$mlog$mv_source_init'").Rows()[0][0]
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval", "return(10)")
			var beforeInfo, beforeSources, beforePurge, beforeTables [][]any
			var cutoverAttempts atomic.Int32
			if stage == "timeout" {
				beforeInfo = tk.MustQuery("select * from mysql.tidb_mview_refresh_info order by MVIEW_ID").Rows()
				beforeSources = tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID").Rows()
				beforePurge = tk.MustQuery("select * from mysql.tidb_mlog_purge_info order by MLOG_ID").Rows()
				beforeTables = tk.MustQuery("select TABLE_NAME, TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' order by TABLE_NAME").Rows()
				testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockMViewOutOfPlacePurgeAdvisoryLockTimeout", "return(1000)")
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", func(job *model.Job) {
					if job.Type == model.ActionMViewRefreshOutOfPlaceCutover && job.TableID == oldMV {
						cutoverAttempts.Add(1)
					}
				})
			}

			entered, release := make(chan struct{}), make(chan struct{})
			running, releaseRunning := make(chan struct{}), make(chan struct{})
			var paused, released, runningPaused, runningReleased atomic.Bool
			unblock := func() {
				if released.CompareAndSwap(false, true) {
					close(release)
				}
			}
			unblockRunning := func() {
				if runningReleased.CompareAndSwap(false, true) {
					close(releaseRunning)
				}
			}
			defer unblock()
			defer unblockRunning()
			waitFor := func(ch <-chan struct{}, what string) {
				t.Helper()
				select {
				case <-ch:
				case <-time.After(10 * time.Second):
					t.Fatalf("timeout waiting for %s", what)
				}
			}
			pause := func(int64, int64) {
				if paused.CompareAndSwap(false, true) {
					close(entered)
					<-release
				}
			}
			switch stage {
			case "starting":
				// The advisory lock is held, but no running history exists yet.
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/mvMaintainMemQuotaAppliedOnPurgeDeleteSession", pause)
			case "resolved":
				// Old metadata/MDL is held before advisory lock acquisition.
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/mvMaintainMemQuotaAppliedOnPurgeSession", pause)
			}
			if stage != "resolved" {
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/purgeMaterializedViewLogDeleteSQL", func(string) {
					if stage == "running" || stage == "timeout" {
						pause(0, 0)
					} else if runningPaused.CompareAndSwap(false, true) {
						close(running)
						<-releaseRunning
					}
				})
			}
			requested := waitMVTaskCancelWatcherRequested(t, "mlog-purge-")
			retried := make(chan struct{})
			var retriedOnce atomic.Bool
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mViewOutOfPlacePurgeAdvisoryLockRetry", func(int64) {
				if retriedOnce.CompareAndSwap(false, true) {
					close(retried)
				}
			})
			var retiring atomic.Bool
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mViewOutOfPlaceBeforeRetireMLog", func(int64) {
				retiring.Store(true)
			})

			purgeDone, cutoverDone := make(chan error, 1), make(chan error, 1)
			purgeFinished, cutoverFinished := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(purgeFinished)
				purger := newMViewTestKit(t, store)
				purger.MustExec("use test")
				purgeDone <- purger.ExecToErr("purge materialized view log on mv_source_init")
			}()
			waitFor(entered, "purge pause point")
			go func() {
				defer close(cutoverFinished)
				refresher := newMViewTestKit(t, store)
				refresher.MustExec("use test")
				cutoverDone <- refresher.ExecToErr("refresh materialized view mv_source_init complete out of place")
			}()
			var otherFinished <-chan struct{}
			defer func() {
				unblock()
				unblockRunning()
				waitFor(purgeFinished, "purge cleanup")
				waitFor(cutoverFinished, "cutover cleanup")
				if otherFinished != nil {
					waitFor(otherFinished, "unrelated DDL cleanup")
				}
			}()

			if stage == "resolved" {
				// Cutover commits, then waits for this statement's old MDL.
				// Purge must fail instead of waiting for the advisory lock.
				require.Eventually(t, func() bool {
					id := tk.MustQueryWithContext(context.Background(), "select TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' and TABLE_NAME='mv_source_init'").Rows()[0][0]
					return fmt.Sprint(id) != fmt.Sprint(oldMV)
				}, 10*time.Second, 10*time.Millisecond)
				unblock()
				waitFor(purgeFinished, "stale purge")
				err := <-purgeDone
				require.True(t, exeerrors.ErrMVAdvisoryLockConflict.Equal(err), "%v", err)
			} else {
				if stage == "starting" {
					waitFor(retried, "cutover retry before running history exists")
					tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID=%v and PURGE_STATUS='running'", oldLog)).Check(testkit.Rows("0"))
					unblock()
					waitFor(running, "purge running history")
				}
				waitFor(requested, "cutover cancellation observed by purge")
				require.False(t, retiring.Load(), "cutover must wait before mutating runtime metadata")
				if stage == "timeout" {
					// Purge still holds the lock after observing cancellation. The
					// total budget must fail the job instead of restarting DDL retries.
					waitFor(cutoverFinished, "cutover purge-lock timeout")
					err := <-cutoverDone
					require.True(t, exeerrors.ErrMVAdvisoryLockConflict.Equal(err), "%v", err)
					require.ErrorContains(t, err, "refresh materialized view complete OUT OF PLACE cutover for test.mv_source_init: timed out waiting for materialized view log purge; retry refresh")
					require.Equal(t, int32(1), cutoverAttempts.Load())
					require.False(t, retiring.Load())
					require.Equal(t, beforeInfo, tk.MustQuery("select * from mysql.tidb_mview_refresh_info order by MVIEW_ID").Rows())
					require.Equal(t, beforeSources, tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID").Rows())
					require.Equal(t, beforePurge, tk.MustQuery("select * from mysql.tidb_mlog_purge_info order by MLOG_ID").Rows())
					require.Equal(t, beforeTables, tk.MustQuery("select TABLE_NAME, TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' order by TABLE_NAME").Rows())
					tk.MustQuery("select * from mv_source_init order by a").Check(testkit.Rows("1 10 1", "2 20 1", "3 30 1"))
					unblock()
					waitFor(purgeFinished, "cancelled purge after cutover timeout")
					require.ErrorContains(t, <-purgeDone, "materialized view task canceled manually")
					require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockMViewOutOfPlacePurgeAdvisoryLockTimeout"))
					tk.MustExec("refresh materialized view mv_source_init complete out of place")
					tk.MustExec("purge materialized view log on mv_source_init")
					return
				}

				// A cancelled purge may still be finishing a DELETE batch.
				// Waiting must not hold the shared schema-version lock.
				otherDone, otherExited := make(chan error, 1), make(chan struct{})
				otherFinished = otherExited
				go func() {
					defer close(otherExited)
					other := newMViewTestKit(t, store)
					other.MustExec("use test")
					otherDone <- other.ExecToErr("create table t_unrelated_to_purge (a int)")
				}()
				waitFor(otherExited, "unrelated DDL while cutover waits")
				require.NoError(t, <-otherDone)
				tk.MustQueryWithContext(context.Background(), "select TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' and TABLE_NAME='mv_source_init'").Check(testkit.Rows(fmt.Sprint(oldMV)))
				unblock()
				unblockRunning()
				waitFor(purgeFinished, "cancelled purge")
				require.ErrorContains(t, <-purgeDone, "materialized view task canceled manually")
			}
			waitFor(cutoverFinished, "cutover after purge exits")
			require.NoError(t, <-cutoverDone)
			tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID=%v", oldLog)).Check(testkit.Rows("0"))
			tk.MustExec("insert into t_source_init values (4, 40)")
			tk.MustExec("refresh materialized view mv_source_init fast")
			tk.MustQuery("select count(*) > 0 from `$mlog$mv_source_init`").Check(testkit.Rows("1"))
			tk.MustExec("purge materialized view log on mv_source_init")
		})
	}
}

func TestMaterializedViewRefreshCompleteOutOfPlaceConcurrentChildFast(t *testing.T) {
	store, tk, oldMV, _ := newMViewRefreshSourceFixture(t)
	tk.MustExec("create materialized view log on mv_source_init (a, s, cnt)")
	tk.MustExec("create materialized view mv_child (a, s, cnt, cnt_s) refresh fast as select a, sum(s), count(*), count(s) from mv_source_init group by a")
	tk.MustExec("insert into t_source_init values (3, 30)")
	before := tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID").Rows()
	entered, release := make(chan struct{}), make(chan struct{})
	var paused, released atomic.Bool
	unblock := func() {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
	}
	defer unblock()
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterInsertRefreshHistRunning", func() {
		if paused.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	})
	childDone := make(chan error, 1)
	go func() {
		child := newMViewTestKit(t, store)
		child.MustExec("use test")
		childDone <- child.ExecToErr("refresh materialized view mv_child fast")
	}()
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("child FAST did not reach its data statement")
	}
	parentDone := make(chan error, 1)
	go func() {
		parent := newMViewTestKit(t, store)
		parent.MustExec("use test")
		parentDone <- parent.ExecToErr("refresh materialized view mv_source_init complete out of place")
	}()
	require.Eventually(t, func() bool {
		id := tk.MustQueryWithContext(context.Background(), "select TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' and TABLE_NAME='mv_source_init'").Rows()[0][0]
		return fmt.Sprint(id) != fmt.Sprint(oldMV)
	}, 30*time.Second, 10*time.Millisecond)
	unblock()
	select {
	case err := <-childDone:
		require.Error(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("child FAST did not finish after parent cutover")
	}
	select {
	case err := <-parentDone:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("parent OOP did not finish after child FAST")
	}
	tk.MustQuery("select * from mv_child order by a").Check(testkit.Rows("1 10 1 1", "2 20 1 1"))
	// The parent's registration moves to its new ID; the child keeps its old
	// input log registration until COMPLETE succeeds.
	tk.MustQuery("select MLOG_ID from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID=(select TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA='test' and TABLE_NAME='mv_child')").Check(testkit.Rows(fmt.Sprint(before[1][1])))
	tk.MustExec("refresh materialized view mv_child complete delta apply")
	tk.MustExec("refresh materialized view mv_child fast")
	tk.MustQuery("select * from mv_child order by a").Check(testkit.Rows("1 10 1 1", "2 20 1 1", "3 30 1 1"))
}
func TestMaterializedViewRefreshCompleteOutOfPlaceCutoverKeepsTiFlashProgressUntilCommit(t *testing.T) {
	const mockTiFlashStoreCountFailpoint = "github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount"
	const cutoverBeforeCommitFailpoint = "github.com/pingcap/tidb/pkg/ddl/mockMViewRefreshOutOfPlaceCutoverBeforeCommitError"
	require.NoError(t, failpoint.Enable(mockTiFlashStoreCountFailpoint, "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable(mockTiFlashStoreCountFailpoint))
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		infosync.SetMockTiFlash(nil)
		tiflash.StatusServer.Close()
	}()
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_tiflash_cutover (a int not null, b int not null)")
	tk.MustExec("insert into t_tiflash_cutover values (1, 10), (2, 7)")
	tk.MustExec("create materialized view log on t_tiflash_cutover (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_tiflash_cutover (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_tiflash_cutover group by a")
	tk.MustExec("alter table mv_tiflash_cutover set tiflash replica 1")
	tk.MustExec("create materialized view log on mv_tiflash_cutover (a, s, cnt) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("alter table `$mlog$mv_tiflash_cutover` set tiflash replica 2 location labels 'zone'")

	mvTable, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_tiflash_cutover"))
	require.NoError(t, err)
	oldMViewID := mvTable.Meta().ID
	oldMLogID := mvTable.Meta().MaterializedViewBase.MLogID
	require.NoError(t, dom.DDLExecutor().UpdateTableReplicaInfo(tk.Session(), oldMLogID, true))
	oldLog, ok := dom.InfoSchema().TableByID(context.Background(), oldMLogID)
	require.True(t, ok)
	oldReplica := *oldLog.Meta().TiFlashReplica
	require.True(t, oldReplica.Available)
	infosync.UpdateTiFlashProgressCache(oldMViewID, 0.75)
	infosync.UpdateTiFlashProgressCache(oldMLogID, 1)
	_, exists := infosync.GetTiFlashProgressFromCache(oldMViewID)
	require.True(t, exists)

	originErrLimit := vardef.GetDDLErrorCountLimit()
	vardef.SetDDLErrorCountLimit(0)
	defer vardef.SetDDLErrorCountLimit(originErrLimit)
	var cancelledJobID, cancelledLogID atomic.Int64
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type == model.ActionMViewRefreshOutOfPlaceCutover && job.TableID == oldMViewID {
			args, err := model.GetRefreshMaterializedViewCompleteOutOfPlaceCutoverArgs(job)
			require.NoError(t, err)
			cancelledJobID.Store(job.ID)
			cancelledLogID.Store(args.NewMLogID)
		}
	})
	require.NoError(t, failpoint.Enable(cutoverBeforeCommitFailpoint, "return"))
	tk.MustExec("insert into t_tiflash_cutover values (3, 4)")
	err = tk.ExecToErr("refresh materialized view mv_tiflash_cutover complete out of place")
	require.ErrorContains(t, err, "error before commit")
	require.NoError(t, failpoint.Disable(cutoverBeforeCommitFailpoint))
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep")
	require.Positive(t, cancelledLogID.Load())
	tk.MustQuery(fmt.Sprintf("select start_key, end_key from mysql.gc_delete_range where job_id = %[1]d union all select start_key, end_key from mysql.gc_delete_range_done where job_id = %[1]d", cancelledJobID.Load())).Check(testkit.Rows(fmt.Sprintf("%x %x", []byte(tablecodec.EncodeTablePrefix(cancelledLogID.Load())), []byte(tablecodec.EncodeTablePrefix(cancelledLogID.Load()+1)))))

	_, exists = infosync.GetTiFlashProgressFromCache(oldMViewID)
	require.True(t, exists)
	_, exists = infosync.GetTiFlashProgressFromCache(oldMLogID)
	require.True(t, exists)
	oldLog, ok = dom.InfoSchema().TableByID(context.Background(), oldMLogID)
	require.True(t, ok)
	require.Equal(t, oldReplica, *oldLog.Meta().TiFlashReplica)

	tk.MustExec("refresh materialized view mv_tiflash_cutover complete out of place")
	_, exists = infosync.GetTiFlashProgressFromCache(oldMViewID)
	require.False(t, exists)
	newLog, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$mv_tiflash_cutover"))
	require.NoError(t, err)
	require.NotEqual(t, oldMLogID, newLog.Meta().ID)
	replica := newLog.Meta().TiFlashReplica
	require.NotNil(t, replica)
	require.Equal(t, oldReplica.Count, replica.Count)
	require.Equal(t, oldReplica.LocationLabels, replica.LocationLabels)
	t.Run("replica state", func(t *testing.T) {
		require.False(t, replica.Available)
		require.Empty(t, replica.AvailablePartitionIDs)
		require.True(t, oldLog.Meta().TiFlashReplica.Available)
	})
	t.Run("placement rule", func(t *testing.T) {
		rule, ok := tiflash.GetPlacementRule(infosync.MakeRuleID(newLog.Meta().ID))
		require.True(t, ok)
		require.Equal(t, int(replica.Count), rule.Count)
		require.Equal(t, replica.LocationLabels, rule.LocationLabels)
	})
	t.Run("old log progress", func(t *testing.T) {
		_, exists := infosync.GetTiFlashProgressFromCache(oldMLogID)
		require.False(t, exists)
	})
}
