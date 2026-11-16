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

package mviewoutputlog

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewRefreshCompleteInPlaceRejectsMLog(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast as select a, sum(b), count(1) from t group by a")
	tk.MustExec("create materialized view log on mv (a, s, cnt)")
	tk.MustExec("insert into t values (3, 30)")

	err := tk.ExecToErr("refresh materialized view mv complete in place")
	require.ErrorContains(t, err, "complete IN PLACE is not supported")
	require.ErrorContains(t, err, "with a materialized view log")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 10 1", "2 20 1"))
}

func TestMaterializedViewRefreshCompleteOutOfPlaceRejectsMLog(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast as select a, sum(b), count(1) from t group by a")
	tk.MustExec("create materialized view log on mv (a, s, cnt)")

	err := tk.ExecToErr("refresh materialized view mv complete out of place")
	require.ErrorContains(t, err, "complete OUT OF PLACE is not supported")
	require.ErrorContains(t, err, "with a materialized view log")
	tk.MustQuery("show tables like '\\_\\_mv\\_shadow\\_%'").Check(testkit.Rows())
}

func TestMaterializedViewRefreshSchemaVersion(t *testing.T) {
	for _, scenario := range []string{"current", "empty_versions", "mlog", "reload_failure"} {
		t.Run(scenario, func(t *testing.T) {
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := newMViewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_schema (a int not null, b int not null)")
			tk.MustExec("insert into t_schema values (1, 10)")
			tk.MustExec("create materialized view log on t_schema (a, b)")
			tk.MustExec("create materialized view mv_schema (a, s, cnt) as select a, sum(b), count(1) from t_schema group by a")
			tk.MustExec("insert into t_schema values (2, 20)")
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/infoschema/issyncer/disableOnTickReload", "return(true)")
			oldIS := dom.InfoSchema()
			if scenario == "empty_versions" {
				// Failed DDL steps can leave multiple reserved versions without diffs.
				ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
				require.NoError(t, kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
					_, err := meta.NewMutator(txn).GenSchemaVersions(2)
					return err
				}))
			} else if scenario != "current" {
				if scenario == "mlog" {
					tk.MustExec("create materialized view log on mv_schema (a, s, cnt)")
				} else {
					tk.MustExec("create table schema_bump (a int)")
				}
				require.Greater(t, dom.InfoSchema().SchemaMetaVersion(), oldIS.SchemaMetaVersion())
				// Simulate a node whose cache has not caught up with committed DDL.
				dom.InfoCache().Upsert(oldIS, 0)
			}
			const reloadFailure = "github.com/pingcap/tidb/pkg/infoschema/issyncer/ErrorMockReloadFailed"
			if scenario != "mlog" {
				testfailpoint.Enable(t, reloadFailure, "return(true)")
			}
			switch scenario {
			case "current", "empty_versions":
				// An up-to-date refresh succeeds even when Reload would fail.
				tk.MustExec("refresh materialized view mv_schema complete in place")
				testfailpoint.Disable(t, reloadFailure)
			case "mlog":
				err := tk.ExecToErr("refresh materialized view mv_schema complete in place")
				require.ErrorContains(t, err, "with a materialized view log")
				require.Greater(t, dom.InfoSchema().SchemaMetaVersion(), oldIS.SchemaMetaVersion())
				tk.MustQuery("select * from mv_schema").Check(testkit.Rows("1 10 1"))
				return
			case "reload_failure":
				err := tk.ExecToErr("refresh materialized view mv_schema complete in place")
				require.ErrorContains(t, err, "mock reload failed")
				testfailpoint.Disable(t, reloadFailure)
				// Both the reload mutex and the MV advisory lock must be released.
				tk.MustExec("refresh materialized view mv_schema complete in place")
				require.Greater(t, dom.InfoSchema().SchemaMetaVersion(), oldIS.SchemaMetaVersion())
			}
			tk.MustQuery("select * from mv_schema order by a").Check(testkit.Rows("1 10 1", "2 20 1"))
		})
	}
}

func TestMaterializedViewRefreshSchemaReloadConcurrent(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_schema (a int not null, b int not null)")
	tk.MustExec("insert into t_schema values (1, 10)")
	tk.MustExec("create materialized view log on t_schema (a, b)")
	const refreshCount = 10
	refreshers := make([]*testkit.TestKit, refreshCount)
	for i := range refreshCount {
		tk.MustExec(fmt.Sprintf("create materialized view mv_schema_%d (a, s, cnt) as select a, sum(b), count(1) from t_schema group by a", i))
		refreshers[i] = newMViewTestKit(t, store)
		refreshers[i].MustExec("use test")
	}
	tk.MustExec("insert into t_schema values (2, 20)")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/infoschema/issyncer/disableOnTickReload", "return(true)")
	oldIS := dom.InfoSchema()
	tk.MustExec("create table schema_bump (a int)")
	requiredVersion := dom.InfoSchema().SchemaMetaVersion()
	require.Greater(t, requiredVersion, oldIS.SchemaMetaVersion())
	dom.InfoCache().Upsert(oldIS, 0)

	waiting := make(chan int64, refreshCount)
	resume := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewBeforeSchemaReloadLock", func(version int64) {
		waiting <- version
		<-resume
	})
	var reloads atomic.Int32
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewBeforeSchemaReload", func(int64) {
		reloads.Add(1)
	})
	done := make(chan error, refreshCount)
	finished := 0
	release := func() {
		select {
		case <-resume:
		default:
			close(resume)
		}
	}
	defer func() {
		release()
		for finished < refreshCount {
			select {
			case <-done:
				finished++
			case <-time.After(15 * time.Second):
				t.Error("timeout cleaning up concurrent refreshes")
				return
			}
		}
	}()
	for i, refresher := range refreshers {
		go func() {
			done <- refresher.ExecToErr(fmt.Sprintf("refresh materialized view mv_schema_%d fast", i))
		}()
	}
	for range refreshCount {
		select {
		case version := <-waiting:
			require.Equal(t, requiredVersion, version)
		case <-time.After(15 * time.Second):
			t.Fatal("timeout waiting for every refresh to observe the stale schema")
		}
	}
	release()
	for finished < refreshCount {
		select {
		case err := <-done:
			finished++
			require.NoError(t, err)
		case <-time.After(15 * time.Second):
			t.Fatal("timeout waiting for concurrent refresh completion")
		}
	}
	require.EqualValues(t, 1, reloads.Load(), "concurrent refreshes must share the first successful schema reload")
	require.GreaterOrEqual(t, dom.InfoSchema().SchemaMetaVersion(), requiredVersion)
	for i := range refreshCount {
		tk.MustQuery(fmt.Sprintf("select * from mv_schema_%d order by a", i)).Check(testkit.Rows("1 10 1", "2 20 1"))
	}
}

func TestMaterializedViewRefreshCreateMLogConflict(t *testing.T) {
	for _, tc := range []struct {
		mode     string
		rollback bool
	}{
		{"fast", false},
		{"complete delta apply", false},
		{"complete in place", false},
		{"complete in place", true},
		{"complete out of place", false},
	} {
		t.Run(fmt.Sprintf("%s/rollback=%t", tc.mode, tc.rollback), func(t *testing.T) {
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := newMViewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_mlog_refresh (a int not null, b int not null)")
			tk.MustExec("insert into t_mlog_refresh values (1, 10)")
			tk.MustExec("create materialized view log on t_mlog_refresh (a, b)")
			tk.MustExec("create materialized view mv_mlog_refresh (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mlog_refresh group by a")
			tk.MustExec("insert into t_mlog_refresh values (2, 20)")

			paused := make(chan struct{})
			resume := make(chan struct{})
			pause := func() { close(paused); <-resume }
			release := func() {
				select {
				case <-resume:
				default:
					close(resume)
				}
			}
			if tc.mode == "complete out of place" {
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceAfterBuildDataLoad", func(uint64) { pause() })
			} else {
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterDataChanges", pause)
			}
			if tc.rollback {
				testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorAfterDataChanges", `return("rollback refresh for lock test")`)
			}
			var createAttempts atomic.Int32
			var cancelled atomic.Bool
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", func(job *model.Job) {
				if job.Type == model.ActionCreateMaterializedViewLog && job.TableName == "$mlog$mv_mlog_refresh" {
					createAttempts.Add(1)
					cancelled.Store(job.IsCancelled())
				}
			})
			tkRefresh := newMViewTestKit(t, store)
			tkRefresh.MustExec("use test")
			refreshDone := make(chan error, 1)
			go func() { refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv_mlog_refresh " + tc.mode) }()
			refreshFinished := false
			defer func() {
				release()
				if !refreshFinished {
					select {
					case <-refreshDone:
					case <-time.After(15 * time.Second):
						t.Error("timeout cleaning up refresh")
					}
				}
			}()
			select {
			case <-paused:
			case <-time.After(15 * time.Second):
				t.Fatal("timeout waiting for refresh")
			}
			ddlDone := make(chan error, 1)
			go func() { ddlDone <- tk.ExecToErr("create materialized view log on mv_mlog_refresh (a, s, cnt)") }()
			ddlFinished := false
			defer func() {
				release()
				if !ddlFinished {
					select {
					case <-ddlDone:
					case <-time.After(15 * time.Second):
						t.Error("timeout cleaning up CREATE MLog")
					}
				}
			}()
			select {
			case err := <-ddlDone:
				ddlFinished = true
				require.ErrorContains(t, err, "materialized view refresh is running")
				require.ErrorContains(t, err, "please retry later")
			case <-time.After(15 * time.Second):
				t.Fatal("CREATE MLog must terminate while refresh still holds its lock")
			}
			require.EqualValues(t, 1, createAttempts.Load(), "lock conflict must not retry the same job")
			require.True(t, cancelled.Load())
			tk.MustQuery("show tables like '$mlog$mv_mlog_refresh'").Check(testkit.Rows())
			release()
			select {
			case err := <-refreshDone:
				refreshFinished = true
				if tc.rollback {
					require.ErrorContains(t, err, "rollback refresh for lock test")
				} else {
					require.NoError(t, err, "OOP cutover must remain runnable after CREATE MLog is cancelled")
				}
			case <-time.After(15 * time.Second):
				t.Fatal("timeout waiting for refresh completion")
			}
			if tc.rollback {
				tk.MustQuery("select * from mv_mlog_refresh order by a").Check(testkit.Rows("1 10 1"))
			} else {
				tk.MustQuery("select * from mv_mlog_refresh order by a").Check(testkit.Rows("1 10 1", "2 20 1"))
			}
			// A new user retry succeeds after refresh releases its lock, including
			// after OOP cutover has replaced the MV's table ID.
			tk.MustExec("create materialized view log on mv_mlog_refresh (a, s, cnt)")
			mv, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_mlog_refresh"))
			require.NoError(t, err)
			mlog, ok := dom.InfoSchema().TableByID(context.Background(), mv.Meta().MaterializedViewBase.MLogID)
			require.True(t, ok)
			require.Equal(t, mv.Meta().ID, mlog.Meta().MaterializedViewLog.BaseTableID)
			tk.MustQuery("show tables like '\\_\\_mv\\_shadow\\_%'").Check(testkit.Rows())
		})
	}
}
