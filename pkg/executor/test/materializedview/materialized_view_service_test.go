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
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/mvservice"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
