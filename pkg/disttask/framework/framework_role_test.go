// Copyright 2024 PingCAP, Inc.
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

package framework_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func checkSubtaskOnNodes(ctx context.Context, t *testing.T, taskID int64, expectedNodes []string) {
	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	nodes, err := storage.GetSubtaskNodesForTest(ctx, mgr, taskID)
	require.NoError(t, err)
	slices.Sort(nodes)
	slices.Sort(expectedNodes)
	require.EqualValues(t, expectedNodes, nodes)
}

func TestRoleBasic(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	tk := testkit.NewTestKit(t, distContext.Store)

	// 1. all "" role.
	submitTaskAndCheckSuccessForBasic(ctx, t, "😁", testContext)

	checkSubtaskOnNodes(ctx, t, 1, []string{":4000", ":4001", ":4002"})
	tk.MustQuery(`select role from mysql.dist_framework_meta where host=":4000"`).Check(testkit.Rows(""))
	tk.MustQuery(`select role from mysql.dist_framework_meta where host=":4001"`).Check(testkit.Rows(""))
	tk.MustQuery(`select role from mysql.dist_framework_meta where host=":4002"`).Check(testkit.Rows(""))

	// 2. one "background" role.
	tk.MustExec("set global tidb_service_scope=background")
	tk.MustQuery("select @@global.tidb_service_scope").Check(testkit.Rows("background"))
	tk.MustQuery("select @@tidb_service_scope").Check(testkit.Rows("background"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/syncRefresh", "1*return()"))
	<-scheduler.TestRefreshedChan
	submitTaskAndCheckSuccessForBasic(ctx, t, "😊", testContext)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/syncRefresh"))

	tk.MustQuery(`select role from mysql.dist_framework_meta where host=":4000"`).Check(testkit.Rows("background"))
	tk.MustQuery(`select role from mysql.dist_framework_meta where host=":4001"`).Check(testkit.Rows(""))
	tk.MustQuery(`select role from mysql.dist_framework_meta where host=":4002"`).Check(testkit.Rows(""))

	checkSubtaskOnNodes(ctx, t, 2, []string{":4000"})

	// 3. 2 "background" role.
	tk.MustExec("update mysql.dist_framework_meta set role = \"background\" where host = \":4001\"")
	time.Sleep(5 * time.Second)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/syncRefresh", "1*return()"))
	<-scheduler.TestRefreshedChan
	submitTaskAndCheckSuccessForBasic(ctx, t, "😆", testContext)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/syncRefresh"))

	checkSubtaskOnNodes(ctx, t, 3, []string{":4000", ":4001"})
	tk.MustQuery(`select role from mysql.dist_framework_meta where host=":4000"`).Check(testkit.Rows("background"))
	tk.MustQuery(`select role from mysql.dist_framework_meta where host=":4001"`).Check(testkit.Rows("background"))
	tk.MustQuery(`select role from mysql.dist_framework_meta where host=":4002"`).Check(testkit.Rows(""))

	distContext.Close()
}

func TestSetRole(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// 1. set wrong sys var.
	tk.MustMatchErrMsg("set global tidb_service_scope=wrong", `incorrect value: .*. tidb_service_scope options: "", background`)
	// 2. set keyspace id.
	tk.MustExec("update mysql.dist_framework_meta set keyspace_id = 16777216 where host = \":4000\"")
	tk.MustQuery("select keyspace_id from mysql.dist_framework_meta where host = \":4000\"").Check(testkit.Rows("16777216"))
}
