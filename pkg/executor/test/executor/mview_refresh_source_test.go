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
	"sync/atomic"
	"testing"

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
