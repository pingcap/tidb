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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
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

func TestMViewRefreshSourceInfoLazyInit(t *testing.T) {
	for _, nullWatermark := range []bool{false, true} {
		t.Run(fmt.Sprintf("null_watermark=%t", nullWatermark), func(t *testing.T) {
			_, tk, mviewID, mlogID := newMViewRefreshSourceFixture(t)
			tk.MustExec("delete from mysql.tidb_mview_fast_refresh_source_info")
			if nullWatermark {
				tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = NULL where MVIEW_ID = %d", mviewID))
			}
			infoSQL := fmt.Sprintf("select * from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)
			before := tk.MustQuery(infoSQL).Rows()
			// Initialization is independently committed before the advisory lock
			// and refresh body; an ordinary refresh failure must retain it.
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorBeforeInsertHist", `return("stop after initialization")`)
			require.ErrorContains(t, tk.ExecToErr("refresh materialized view mv_source_init fast"), "stop after initialization")
			tk.MustQuery("select MVIEW_ID, MLOG_ID from mysql.tidb_mview_fast_refresh_source_info").Check(testkit.Rows(fmt.Sprintf("%d %d", mviewID, mlogID)))
			require.Equal(t, before, tk.MustQuery(infoSQL).Rows())
			tk.MustQuery("select * from mv_source_init order by a").Check(testkit.Rows("1 10 1", "2 20 1"))
		})
	}
}

func TestMViewRefreshSourceInfoKeepsExistingRecords(t *testing.T) {
	_, tk, mviewID, mlogID := newMViewRefreshSourceFixture(t)
	// Existing records are never silently completed or replaced by lazy init.
	tk.MustExec("delete from mysql.tidb_mview_fast_refresh_source_info")
	tk.MustExec(fmt.Sprintf("insert into mysql.tidb_mview_fast_refresh_source_info values (%d, 0), (%d, %d)", mviewID, mviewID, mlogID+1000))
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorBeforeInsertHist", `return("keep existing baseline")`)
	before := tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info order by MLOG_ID").Rows()
	require.ErrorContains(t, tk.ExecToErr("refresh materialized view mv_source_init fast"), "keep existing baseline")
	require.Equal(t, before, tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info order by MLOG_ID").Rows())
}

func TestMViewRefreshSourceInfoRejectsMissingNestedRecords(t *testing.T) {
	_, tk, _, _ := newMViewRefreshSourceFixture(t)
	tk.MustExec("create materialized view log on mv_source_init (a, s, cnt)")
	tk.MustExec("create materialized view mv_source_child (a, cnt) refresh fast as select a, count(*) from mv_source_init group by a")
	tk.MustExec("delete from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = (select TIDB_TABLE_ID from information_schema.tables where table_schema = 'test' and table_name = 'mv_source_child')")
	require.ErrorContains(t, tk.ExecToErr("refresh materialized view mv_source_child fast"), "input log records are missing for a nested materialized view")
	tk.MustQuery("select count(*) from mysql.tidb_mview_fast_refresh_source_info").Check(testkit.Rows("1"))
}

func TestMViewRefreshSourceInfoConcurrentInit(t *testing.T) {
	store, tk, mviewID, mlogID := newMViewRefreshSourceFixture(t)
	tk.MustExec("delete from mysql.tidb_mview_fast_refresh_source_info")
	entered := make(chan struct{}, 2)
	resume := make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	defer release()
	var attempts atomic.Int32
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/beforeMViewRefreshSourceInfoInitCommit", func(id int64) {
		if id == mviewID && attempts.Add(1) <= 2 {
			entered <- struct{}{}
			<-resume
		}
	})
	done := make(chan error, 2)
	for range 2 {
		se := testkit.NewTestKit(t, store)
		se.MustExec("use test")
		go func() { done <- se.ExecToErr("refresh materialized view mv_source_init fast") }()
	}
	remaining := 2
	defer func() {
		release()
		for remaining > 0 {
			select {
			case <-done:
				remaining--
			case <-time.After(15 * time.Second):
				t.Error("timeout cleaning up concurrent initialization")
				return
			}
		}
	}()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(15 * time.Second):
			t.Fatal("initialization should run before the refresh mutex")
		}
	}
	release()
	successes := 0
	for remaining > 0 {
		select {
		case err := <-done:
			remaining--
			if err == nil {
				successes++
			} else {
				require.ErrorContains(t, err, "another refresh or materialized view log creation is running")
			}
		case <-time.After(15 * time.Second):
			t.Fatal("timeout waiting for concurrent initialization")
		}
	}
	require.Positive(t, successes)
	tk.MustQuery("select MVIEW_ID, MLOG_ID from mysql.tidb_mview_fast_refresh_source_info").Check(testkit.Rows(fmt.Sprintf("%d %d", mviewID, mlogID)))
}

func TestMViewRefreshSourceInfoInitConcurrentDrop(t *testing.T) {
	store, tk, mviewID, _ := newMViewRefreshSourceFixture(t)
	tk.MustExec("delete from mysql.tidb_mview_fast_refresh_source_info")
	entered := make(chan struct{})
	resume := make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	defer release()
	var paused atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/beforeMViewRefreshSourceInfoInitCommit", func(id int64) {
		if id == mviewID && paused.CompareAndSwap(false, true) {
			close(entered)
			<-resume
		}
	})
	se := testkit.NewTestKit(t, store)
	se.MustExec("use test")
	done := make(chan error, 1)
	// OOP's outer statement does not retain target MDL, so DROP can finish
	// while this initialization still has an older transaction snapshot.
	go func() { done <- se.ExecToErr("refresh materialized view mv_source_init complete out of place") }()
	finished := false
	defer func() {
		release()
		if !finished {
			select {
			case <-done:
			case <-time.After(15 * time.Second):
				t.Error("timeout cleaning up initialization")
			}
		}
	}()
	select {
	case <-entered:
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for initialization")
	}
	tk.MustExec("drop materialized view mv_source_init")
	release()
	select {
	case err := <-done:
		finished = true
		require.Error(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for initialization conflict")
	}
	tk.MustQuery("select count(*) from mysql.tidb_mview_fast_refresh_source_info").Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).Check(testkit.Rows("0"))
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
	_, tk, mviewID, _ := newMViewRefreshSourceFixture(t)
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
}
