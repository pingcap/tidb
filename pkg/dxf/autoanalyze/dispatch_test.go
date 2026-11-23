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

package autoanalyze_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	statstestutil "github.com/pingcap/tidb/pkg/statistics/handle/ddl/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestAutoAnalyzeThroughDXF(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, index idx(a))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("set global tidb_enable_dist_auto_analyze = ON")
	defer tk.MustExec("set global tidb_enable_dist_auto_analyze = OFF")
	// The stats owner has a service scope of its own, which the tasks must not follow.
	vardef.ServiceScope.Store("stats-owner")
	defer vardef.ServiceScope.Store("")
	targetScope := ""
	if kerneltype.IsNextGen() {
		targetScope = handle.NextGenTargetScope
	}

	ctx := context.Background()
	h := dom.StatsHandle()
	require.NoError(t, statstestutil.HandleNextDDLEventWithTxn(h))
	tk.MustExec("flush stats_delta *.*")
	require.NoError(t, h.Cache().Update(ctx, dom.InfoSchema()))
	tbl, err := dom.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	const allTasks = "select type, state, concurrency, target_scope from mysql.tidb_global_task union all " +
		"select type, state, concurrency, target_scope from mysql.tidb_global_task_history"
	const finishedJobs = "select count(*) from mysql.analyze_jobs " +
		"where table_name = 't' and job_info like 'auto analyze%' and state = 'finished'"
	succeededTask := fmt.Sprintf("AutoAnalyze succeed 1 %s", targetScope)
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	// The statement can run on another node, which does not update the cache of the stats
	// owner. Dropping the cache after the subtask shows the same to the dispatcher.
	const afterSubtask = "github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/afterRunSubtask"
	testfailpoint.EnableCall(t, afterSubtask, func(taskexecutor.TaskExecutor, *error, context.Context) {
		h.ClearForTest()
	})

	require.True(t, h.AutoAnalyze().HandleAutoAnalyze())
	testfailpoint.Disable(t, afterSubtask)

	// The statement went through DXF and succeeded there, with one slot and
	// outside the scope of the stats owner.
	tk.MustQuery(allTasks).Check(testkit.Rows(succeededTask))
	// It is still recorded as an auto analyze job, and the dispatcher has loaded the
	// new stats into the cache of the stats owner.
	tk.MustQuery(finishedJobs).Check(testkit.Rows("1"))
	require.Equal(t, int64(3), h.GetPhysicalTableStats(tblInfo.ID, tblInfo).RealtimeCount)
}
