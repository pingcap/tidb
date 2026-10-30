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

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/mvservice"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestMaterializedViewServiceAutomaticMaintenance(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mvs (a int primary key, b int not null)")
	tk.MustExec("create materialized view log on t_mvs (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("insert into t_mvs values (1, 10)")
	tk.MustExec("create materialized view mv_service (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mvs group by a")
	tk.MustExec("insert into t_mvs values (2, 20)")

	svc := dom.GetMVService()
	require.NotNil(t, svc)
	require.NoError(t, svc.SetTaskBackpressureConfig(mvservice.TaskBackpressureConfig{}))
	tk.MustExec("set global tidb_mview_task_max=4")
	tk.MustExec("set global tidb_mview_task_refresh_ratio=0.75")
	require.Equal(t, 0.75, svc.GetRefreshTaskConcurrencyRatio())
	tk.MustExec("set global tidb_mview_task_threshold_cpu=0.75")
	tk.MustExec("set global tidb_mview_task_threshold_memory=0.65")
	require.Equal(t, 0.75, svc.GetTaskBackpressureConfig().CPUThreshold)
	require.Equal(t, 0.65, svc.GetTaskBackpressureConfig().MemThreshold)
	tk.MustExec("set global tidb_mview_task_threshold_cpu=0")
	tk.MustExec("set global tidb_mview_task_threshold_memory=0")
	tk.MustExec("set global tidb_mview_refresh_hist_time=24")
	tk.MustExec("set global tidb_mlog_purge_hist_time=24")

	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_service"))
	require.NoError(t, err)
	tk.MustExec("update mysql.tidb_mview_refresh_info set NEXT_REFRESH_UNIX_SECONDS=0 where MVIEW_ID=?", tbl.Meta().ID)
	require.NoError(t, dom.StartMVService())
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		rows := tk.MustQuery("select * from mv_service order by a").Rows()
		assert.Equal(c, [][]any{{"1", "10", "1"}, {"2", "20", "1"}}, rows)
	}, 20*time.Second, 100*time.Millisecond)

	tk.MustExec("update mysql.tidb_mlog_purge_info set NEXT_PURGE_UNIX_SECONDS=0")
	svc.NotifyDDLChange()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		rows := tk.MustQuery("select count(*) from `$mlog$t_mvs`").Rows()
		assert.Equal(c, [][]any{{"0"}}, rows)
	}, 20*time.Second, 100*time.Millisecond)
}

func TestMaterializedViewServiceMiscMaintenance(t *testing.T) {
	t.Run("history", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		svc := dom.GetMVService()
		// Schedule GC soon, then verify the SQL callbacks replace both retention
		// settings without delaying that already scheduled run.
		require.NoError(t, svc.SetMViewRefreshHistRetention(time.Millisecond))
		require.NoError(t, svc.SetMLogPurgeHistRetention(time.Millisecond))
		tk.MustExec("set global tidb_mview_refresh_hist_time=48")
		tk.MustExec("set global tidb_mlog_purge_hist_time=48")

		now := time.Now()
		old := now.Add(-49 * time.Hour)
		stale := now.Add(-36 * time.Hour)
		recent := now.Add(-2 * time.Hour)
		histories := []struct {
			table, prefix, objectIDColumn string
		}{
			{"mysql.tidb_mview_refresh_hist", "REFRESH", "MVIEW_ID"},
			{"mysql.tidb_mlog_purge_hist", "PURGE", "MLOG_ID"},
		}
		for _, history := range histories {
			insertSQL := fmt.Sprintf("insert into %s (%s_JOB_ID, %s, %s_METHOD, %s_STATUS, %s_START_TIME, LAST_HEARTBEAT_TIME, %s_ROWS) values (?, ?, 'manual', ?, ?, ?, 0)",
				history.table, history.prefix, history.objectIDColumn, history.prefix, history.prefix, history.prefix, history.prefix)
			tk.MustExec(insertSQL, oracle.GoTimeToTS(old), 101, "success", old.Format(time.DateTime), nil)
			tk.MustExec(insertSQL, oracle.GoTimeToTS(old)+1, 102, nil, old.Format(time.DateTime), nil)
			tk.MustExec(insertSQL, oracle.GoTimeToTS(recent), 103, "success", recent.Format(time.DateTime), nil)
			tk.MustExec(insertSQL, oracle.GoTimeToTS(old)+2, 104, "running", old.Format(time.DateTime), now.Format(time.DateTime))
			tk.MustExec(insertSQL, oracle.GoTimeToTS(stale), 105, "running", stale.Format(time.DateTime), nil)
		}

		require.NoError(t, dom.StartMVService())
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			for _, history := range histories {
				rows := tk.MustQuery(fmt.Sprintf("select %s, %s_STATUS from %s order by %s", history.objectIDColumn, history.prefix, history.table, history.objectIDColumn)).Rows()
				assert.Equal(c, [][]any{{"103", "success"}, {"104", "running"}, {"105", "orphaned"}}, rows)
			}
		}, 20*time.Second, 100*time.Millisecond)
		for _, history := range histories {
			tk.MustQuery(fmt.Sprintf("select %s_END_TIME is not null, %s_DURATION_SEC is not null, %s_FAILED_REASON is not null from %s where %s=105",
				history.prefix, history.prefix, history.prefix, history.table, history.objectIDColumn)).Check(testkit.Rows("1 1 1"))
		}
	})
	t.Run("freshness_and_accumulation", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := newMViewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_service_alert (a int primary key, b int not null)")
		tk.MustExec("create materialized view log on t_service_alert (a, b) purge next date_add(now(), interval 1 hour) alert rows 1")
		tk.MustExec("insert into t_service_alert values (1, 10)")

		now := time.Now()
		for _, view := range []struct {
			name     string
			age      time.Duration
			disabled bool
		}{
			{"mv_service_warning", 90 * time.Minute, false},
			{"mv_service_overdue", 3 * time.Hour, false},
			{"mv_service_recovered", 0, false},
			{"mv_service_disabled", 0, true},
		} {
			schedule := "next date_add(now(), interval 1 hour)"
			if view.disabled {
				schedule = ""
			}
			tk.MustExec(fmt.Sprintf("create materialized view %s (a, s, cnt) refresh fast %s attributes='mview_alert_warning=3600,mview_alert_overdue=7200,mview_alert_refresh_failed=yes' as select a, sum(b), count(1) from t_service_alert group by a", view.name, schedule))
			tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(view.name))
			require.NoError(t, err)
			tk.MustExec("update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO=? where MVIEW_ID=?", oracle.GoTimeToTS(now.Add(-view.age)), tbl.Meta().ID)
			if view.disabled || view.age == 0 {
				// Clearing a freshness alert must retain the independent failure flag.
				tk.MustExec("insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_SCHEMA, MVIEW_NAME, ALERT_LEVEL, REFRESH_FAILED) values (?, 'test', ?, 'overdue', 'yes')", tbl.Meta().ID, view.name)
			}
		}
		tk.MustExec("insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_NAME, ALERT_LEVEL) values (-1, 'deleted', 'warning')")
		tk.MustExec("insert into t_service_alert values (2, 20), (3, 30)")

		require.NoError(t, dom.StartMVService())
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			rows := tk.MustQuery("select MVIEW_NAME, ALERT_LEVEL, REFRESH_FAILED from mysql.tidb_mview_refresh_alert order by MVIEW_NAME").Rows()
			assert.Equal(c, [][]any{
				{"mv_service_disabled", "<nil>", "yes"},
				{"mv_service_overdue", "overdue", "<nil>"},
				{"mv_service_recovered", "<nil>", "yes"},
				{"mv_service_warning", "warning", "<nil>"},
			}, rows)
			assert.Equal(c, float64(1), testutil.ToFloat64(metrics.MVServiceMVRefreshWarningGauge))
			assert.Equal(c, float64(1), testutil.ToFloat64(metrics.MVServiceMVRefreshOverdueGauge))
			assert.Equal(c, float64(1), testutil.ToFloat64(metrics.MVServiceMVLogAccumulationGauge))
		}, 20*time.Second, 100*time.Millisecond)
	})
}
