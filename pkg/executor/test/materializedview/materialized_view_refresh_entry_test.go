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

package materializedview

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewRefreshSQLEntry(t *testing.T) {
	for _, tc := range []struct {
		mode string
		step string
		plan string
	}{
		{"fast", "DATA_CHANGE_FAST_MERGE", "MViewDeltaMerge"},
		{"complete in place", "DATA_CHANGE_COMPLETE_INSERT", "Insert"},
		{"complete delta apply", "DATA_CHANGE_COMPLETE_DELTA_APPLY", "MViewCompleteDeltaApply"},
		{"complete out of place", "OUT_OF_PLACE_LOAD_SHADOW", "Insert"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := newMViewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table src_entry (id int primary key, g int not null, v int not null)")
			tk.MustExec("insert into src_entry values (1,1,10)")
			tk.MustExec("create materialized view log on src_entry (id,g,v)")
			tk.MustExec("create materialized view mv_entry (g,s,cnt) refresh fast as select g,sum(v),count(*) from src_entry group by g")
			tk.MustExec("insert into src_entry values (2,1,20)")
			watermark := tk.MustQuery("select last_success_read_tso from mysql.tidb_mview_refresh_info").Rows()
			id := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema='test' and table_name='mv_entry'").Rows()
			sql := "refresh materialized view mv_entry " + tc.mode

			for range 2 {
				rows := fmt.Sprint(tk.MustQuery(sql + " dry run").Rows())
				require.Contains(t, rows, tc.step)
				require.Contains(t, rows, tc.plan)
				require.Contains(t, rows, "FINALIZE_HIST")
				require.NotContains(t, rows, "status:success")
				tk.MustQuery("select * from mv_entry").Check(testkit.Rows("1 10 1"))
				tk.MustQuery("select last_success_read_tso from mysql.tidb_mview_refresh_info").Check(watermark)
				tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_hist").Check(testkit.Rows("0"))
				tk.MustQuery("select count(*) from information_schema.tables where table_schema='test' and table_name like '__mv_shadow_%'").Check(testkit.Rows("0"))
			}

			rows := fmt.Sprint(tk.MustQuery(sql + " with profile").Rows())
			require.Contains(t, rows, tc.step)
			require.Contains(t, rows, tc.plan)
			require.Contains(t, rows, "status:success, time:")
			require.Contains(t, rows, "FINALIZE_HIST")
			tk.MustQuery("select * from mv_entry").Check(testkit.Rows("1 30 2"))
			tk.MustQuery("select refresh_status,refresh_method from mysql.tidb_mview_refresh_hist").Check(testkit.Rows("success " + tc.mode + " manual"))
			currentID := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema='test' and table_name='mv_entry'").Rows()
			if tc.mode == "complete out of place" {
				require.NotEqual(t, id, currentID)
			} else {
				require.Equal(t, id, currentID)
			}
			tk.MustExec("insert into src_entry values (3,2,40)")
			tk.MustExec(sql)
			require.Positive(t, tk.Session().GetSessionVars().StmtCtx.AffectedRows())
			tk.MustQuery("select * from mv_entry order by g").Check(testkit.Rows("1 30 2", "2 40 1"))
			// Planning again after a real refresh must not reuse the caller's MDL table cache.
			require.Contains(t, fmt.Sprint(tk.MustQuery(sql+" dry run").Rows()), tc.step)
			require.False(t, tk.Session().GetSessionVars().InRestrictedSQL)
			require.False(t, tk.Session().GetSessionVars().InMViewMaintenance)
		})
	}
}

func TestMaterializedViewRefreshEntryValidation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table src_entry (g int not null, v int not null)")
	tk.MustExec("create materialized view log on src_entry (g,v)")
	tk.MustExec("create materialized view mv_entry (g,s,cnt) as select g,sum(v),count(*) from src_entry group by g")
	require.ErrorContains(t, tk.ExecToErr("refresh materialized view mv_entry with async mode complete in place"), "WITH ASYNC MODE is not supported yet")
	for _, suffix := range []string{"", " dry run", " with profile"} {
		tk.MustExec("begin")
		require.ErrorContains(t, tk.ExecToErr("refresh materialized view mv_entry complete in place"+suffix), "cannot run REFRESH MATERIALIZED VIEW in explicit transaction")
		tk.MustExec("rollback")
	}
	tk.MustExec("prepare stmt from 'refresh materialized view mv_entry complete in place'")
	tk.MustExec("begin")
	require.ErrorContains(t, tk.ExecToErr("execute stmt"), "cannot run REFRESH MATERIALIZED VIEW in explicit transaction")
	tk.MustExec("rollback")
	tk.MustExec("deallocate prepare stmt")
	tk.MustExec("set autocommit=0")
	tk.MustExec("refresh materialized view mv_entry complete in place")
	tk.MustExec("insert into src_entry values (1,10)")
	require.ErrorContains(t, tk.ExecToErr("refresh materialized view mv_entry complete in place"), "cannot run REFRESH MATERIALIZED VIEW in explicit transaction")
	tk.MustExec("rollback")
	tk.MustExec("set autocommit=1")
}

func TestMaterializedViewRefreshEntryPrivileges(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table src_entry (g int not null, v int not null)")
	tk.MustExec("create materialized view log on src_entry (g,v)")
	tk.MustExec("create materialized view mv_entry (g,s,cnt) as select g,sum(v),count(*) from src_entry group by g")
	tk.MustExec("create user 'refresh_entry_u'@'%'")
	tkUser := newMViewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "refresh_entry_u", Hostname: "%"}, nil, nil, nil))
	sql := "refresh materialized view test.mv_entry complete delta apply"
	require.ErrorContains(t, tkUser.ExecToErr(sql), "OPERATE VIEW command denied")
	tk.MustExec("grant operate view on test.mv_entry to 'refresh_entry_u'@'%'")
	require.ErrorContains(t, tkUser.ExecToErr(sql), "SELECT command denied")
	for _, suffix := range []string{" dry run", " with profile"} {
		require.ErrorContains(t, tkUser.ExecToErr(sql+suffix), "SHOW VIEW command denied")
	}
	tk.MustExec("grant show view on test.mv_entry to 'refresh_entry_u'@'%'")
	for _, suffix := range []string{" dry run", " with profile"} {
		require.ErrorContains(t, tkUser.QueryToErr(sql+suffix), "SELECT command denied")
	}
	tk.MustExec("grant select on test.src_entry to 'refresh_entry_u'@'%'")
	tkUser.MustExec(sql)
	for _, suffix := range []string{" dry run", " with profile"} {
		require.Contains(t, fmt.Sprint(tkUser.MustQuery(sql+suffix).Rows()), "FINALIZE_HIST")
	}
}

func TestCancelMaterializedViewRefreshJobRequest(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table src_cancel (g int not null, v int not null)")
	tk.MustExec("create materialized view log on src_cancel (g,v)")
	tk.MustExec("create materialized view mv_cancel (g,s,cnt) as select g,sum(v),count(*) from src_cancel group by g")
	mv, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_cancel"))
	require.NoError(t, err)
	tk.MustExec(fmt.Sprintf("insert into mysql.tidb_mview_refresh_hist (refresh_job_id,mview_id,refresh_method,refresh_status) values (1,%d,'fast manual','running'),(2,%d,'fast manual','success')", mv.Meta().ID, mv.Meta().ID))
	tk.MustExec("cancel materialized view refresh job 1")
	tk.MustQuery("select cancel_request_time is not null from mysql.tidb_mview_refresh_hist where refresh_job_id=1").Check(testkit.Rows("1"))
	for _, id := range []int{1, 2, 3} {
		require.ErrorContains(t, tk.ExecToErr(fmt.Sprintf("cancel materialized view refresh job %d", id)), fmt.Sprintf("cannot cancel materialized view refresh job %d", id))
	}
}

func TestCancelMaterializedViewRefreshSQL(t *testing.T) {
	for _, mode := range []string{"fast", "complete in place", "complete delta apply", "complete out of place"} {
		t.Run(mode, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := newMViewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table src_cancel (g int not null, v int not null)")
			tk.MustExec("insert into src_cancel values (1,10)")
			tk.MustExec("create materialized view log on src_cancel (g,v)")
			tk.MustExec("create materialized view mv_cancel (g,s,cnt) refresh fast as select g,sum(v),count(*) from src_cancel group by g")
			tk.MustExec("insert into src_cancel values (1,20)")
			watermark := tk.MustQuery("select last_success_read_tso from mysql.tidb_mview_refresh_info").Rows()
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval", "return(10)")
			pause := "github.com/pingcap/tidb/pkg/executor/pauseRefreshMaterializedViewAfterInsertRefreshHistRunning"
			require.NoError(t, failpoint.Enable(pause, "pause"))
			paused := true
			defer func() {
				if paused {
					require.NoError(t, failpoint.Disable(pause))
				}
			}()
			tkRefresh := newMViewTestKit(t, store)
			refreshDone := make(chan error, 1)
			go func() { refreshDone <- tkRefresh.ExecToErr("refresh materialized view test.mv_cancel " + mode) }()
			require.Eventually(t, func() bool {
				return fmt.Sprint(tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_hist where refresh_status='running'").Rows()[0][0]) == "1"
			}, 10*time.Second, 10*time.Millisecond)
			jobID := fmt.Sprint(tk.MustQuery("select refresh_job_id from mysql.tidb_mview_refresh_hist where refresh_status='running'").Rows()[0][0])
			tk.MustExec("create user 'refresh_cancel_u'@'%'")
			tkCancel := newMViewTestKit(t, store)
			require.NoError(t, tkCancel.Session().Auth(&auth.UserIdentity{Username: "refresh_cancel_u", Hostname: "%"}, nil, nil, nil))
			cancelSQL := "cancel materialized view refresh job " + jobID
			require.ErrorContains(t, tkCancel.ExecToErr(cancelSQL), "OPERATE VIEW command denied")
			tk.MustExec("grant operate view on test.mv_cancel to 'refresh_cancel_u'@'%'")
			requested := waitMVTaskCancelWatcherRequested(t, "refresh-")
			tkCancel.MustExec(cancelSQL)
			select {
			case <-requested:
			case <-time.After(10 * time.Second):
				t.Fatal("timeout waiting for refresh cancellation")
			}
			require.NoError(t, failpoint.Disable(pause))
			paused = false
			select {
			case err := <-refreshDone:
				require.ErrorContains(t, err, "materialized view task canceled manually")
			case <-time.After(10 * time.Second):
				t.Fatal("timeout waiting for refresh to stop")
			}
			tk.MustQuery("select * from mv_cancel").Check(testkit.Rows("1 10 1"))
			tk.MustQuery("select last_success_read_tso from mysql.tidb_mview_refresh_info").Check(watermark)
			tk.MustQuery("select refresh_status,refresh_failed_reason from mysql.tidb_mview_refresh_hist").Check(testkit.Rows("failed cancelled manually by 'refresh_cancel_u'@'%'"))
			tk.MustQuery("select count(*) from information_schema.tables where table_schema='test' and table_name like '__mv_shadow_%'").Check(testkit.Rows("0"))
		})
	}
}
