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
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
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

func TestMaterializedViewRefreshSchemaVersion(t *testing.T) {
	for _, scenario := range []string{
		"current", "empty_versions", "unrelated", "unrelated_fast", "unrelated_empty_versions",
		"mlog", "mlog_then_unrelated", "reload_failure", "input_mlog_columns", "output_mlog_columns", "source_oop", "schema_change",
		"schema_cache_miss", "schema_load_failure", "mlog_then_add_column", "related_empty_versions",
	} {
		t.Run(scenario, func(t *testing.T) {
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := newMViewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_schema (a int not null, b int not null)")
			tk.MustExec("insert into t_schema values (1, 10)")
			tk.MustExec("create materialized view log on t_schema (a, b)")
			tk.MustExec("create materialized view mv_schema (a, s, cnt) as select a, sum(b), count(1) from t_schema group by a")
			tk.MustExec("insert into t_schema values (2, 20)")
			refreshSQL := "refresh materialized view mv_schema complete in place"
			if scenario == "input_mlog_columns" {
				tk.MustExec("alter table t_schema add column c int")
			}
			if scenario == "output_mlog_columns" {
				tk.MustExec("create materialized view log on mv_schema (a)")
			}
			if scenario == "source_oop" {
				tk.MustExec("create materialized view log on mv_schema (a, s, cnt)")
				tk.MustExec("create materialized view mv_child (a, s, cnt) as select a, s, count(1) from mv_schema group by a, s")
				refreshSQL = "refresh materialized view mv_child complete in place"
			}
			if scenario == "unrelated_fast" || scenario == "input_mlog_columns" || scenario == "output_mlog_columns" {
				refreshSQL = "refresh materialized view mv_schema fast"
			}
			if scenario == "mlog_then_add_column" {
				refreshSQL = "refresh materialized view mv_schema fast"
			}
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/infoschema/issyncer/disableOnTickReload", "return(true)")
			oldIS := dom.InfoSchema()
			intermediateIS := oldIS
			switch scenario {
			case "unrelated", "unrelated_fast", "unrelated_empty_versions", "schema_cache_miss", "schema_load_failure":
				tk.MustExec("create table schema_bump (a int)")
				tk.MustExec("alter table schema_bump add column b int")
				tk.MustExec("drop table schema_bump")
			case "mlog", "mlog_then_unrelated":
				tk.MustExec("create materialized view log on mv_schema (a, s, cnt)")
				if scenario == "mlog_then_unrelated" {
					// The newest diff alone cannot establish that refresh is unaffected.
					tk.MustExec("create table schema_bump (a int)")
				}
			case "mlog_then_add_column":
				tk.MustExec("create materialized view log on mv_schema (a)")
				intermediateIS = dom.InfoSchema()
				tk.MustExec("alter materialized view log on mv_schema add (s)")
			case "reload_failure", "related_empty_versions":
				tk.MustExec("alter table t_schema add column c int")
			case "input_mlog_columns":
				tk.MustExec("alter materialized view log on t_schema add (c)")
			case "output_mlog_columns":
				tk.MustExec("alter materialized view log on mv_schema add (s)")
			case "source_oop":
				tk.MustExec("refresh materialized view mv_schema complete out of place")
			case "schema_change":
				tk.MustExec("create database schema_bump")
			}
			if scenario == "empty_versions" || scenario == "unrelated_empty_versions" || scenario == "related_empty_versions" {
				// Failed DDL steps can leave multiple reserved versions without diffs.
				ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
				require.NoError(t, kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
					_, err := meta.NewMutator(txn).GenSchemaVersions(2)
					return err
				}))
			}
			if scenario != "current" && scenario != "empty_versions" {
				require.Greater(t, dom.InfoSchema().SchemaMetaVersion(), oldIS.SchemaMetaVersion())
				// Simulate a node whose cache has not caught up with committed DDL.
				dom.InfoCache().Upsert(oldIS, 0)
			}
			const loadFailure = "github.com/pingcap/tidb/pkg/infoschema/mockLoadTableInfoError"
			if scenario == "schema_cache_miss" || scenario == "schema_load_failure" {
				v2, ok := oldIS.(interface {
					EvictTable(ast.CIStr, ast.CIStr)
				})
				if !ok {
					t.Skip("requires InfoSchema V2")
				}
				var injected atomic.Bool
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewBeforeCollectRelatedTables", func() {
					if injected.CompareAndSwap(false, true) {
						v2.EvictTable(ast.NewCIStr("test"), ast.NewCIStr("t_schema"))
						if scenario == "schema_load_failure" {
							testfailpoint.Enable(t, loadFailure, "1*return(true)")
						}
					}
				})
			}
			const reloadFailure = "github.com/pingcap/tidb/pkg/infoschema/issyncer/ErrorMockReloadFailed"
			var reloadLockAttempts atomic.Int32
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewBeforeSchemaReloadLock", func(int64) {
				reloadLockAttempts.Add(1)
				if scenario == "mlog_then_add_column" {
					// Simulate a loader that caught up only to MLog creation. The
					// original dependency set does not contain the new MLog ID.
					dom.InfoCache().Upsert(intermediateIS, 0)
				}
			})
			if scenario != "mlog" && scenario != "mlog_then_unrelated" {
				testfailpoint.Enable(t, reloadFailure, "return(true)")
			}
			switch scenario {
			case "current", "empty_versions", "unrelated", "unrelated_fast", "unrelated_empty_versions", "schema_cache_miss":
				// Unrelated DDL and empty versions do not force a synchronous reload.
				tk.MustExec(refreshSQL)
				require.Zero(t, reloadLockAttempts.Load(), "unaffected refresh must skip the reload mutex")
				testfailpoint.Disable(t, reloadFailure)
			case "mlog", "mlog_then_unrelated":
				err := tk.ExecToErr(refreshSQL)
				require.ErrorContains(t, err, "with a materialized view log")
				require.Greater(t, dom.InfoSchema().SchemaMetaVersion(), oldIS.SchemaMetaVersion())
				tk.MustQuery("select * from mv_schema").Check(testkit.Rows("1 10 1"))
				return
			case "schema_load_failure":
				err := tk.ExecToErr(refreshSQL)
				require.ErrorContains(t, err, "mockLoadTableInfoError")
				require.Zero(t, reloadLockAttempts.Load(), "a table load failure must not trigger a schema reload")
				testfailpoint.Disable(t, loadFailure)
				tk.MustExec(refreshSQL)
				testfailpoint.Disable(t, reloadFailure)
			case "reload_failure", "input_mlog_columns", "output_mlog_columns", "source_oop", "schema_change", "mlog_then_add_column", "related_empty_versions":
				err := tk.ExecToErr(refreshSQL)
				require.ErrorContains(t, err, "mock reload failed")
				testfailpoint.Disable(t, reloadFailure)
				// Both the reload mutex and the MV advisory lock must be released.
				tk.MustExec(refreshSQL)
				require.Greater(t, dom.InfoSchema().SchemaMetaVersion(), oldIS.SchemaMetaVersion())
			}
			tk.MustQuery("select * from mv_schema order by a").Check(testkit.Rows("1 10 1", "2 20 1"))
			if scenario == "source_oop" {
				tk.MustQuery("select * from mv_child order by a").Check(testkit.Rows("1 10 1", "2 20 1"))
			}
			if scenario == "mlog_then_add_column" {
				tk.MustQuery("select a, s from `$mlog$mv_schema`").Check(testkit.Rows("2 20"))
			}
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
	// Every refresh uses this source, so the diff requires a shared reload.
	tk.MustExec("alter table t_schema add column c int")
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
				require.True(t, exeerrors.ErrMVAdvisoryLockConflict.Equal(err), "%v", err)
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
