// Copyright 2023 PingCAP, Inc.
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

package querywatch_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	mysql "github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestQueryWatch(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("insert into t1 values(1)")
	tk.MustExec("create table t2(a int)")
	tk.MustExec("insert into t2 values(1)")
	tk.MustExec("create table t3(a int)")
	tk.MustExec("insert into t3 values(1)")

	require.Eventually(t, func() bool {
		return dom.RunawayManager().IsSyncerInitialized()
	}, 20*time.Second, 300*time.Millisecond)

	err := tk.QueryToErr("query watch add sql text exact to 'select * from test.t1'")
	require.ErrorContains(t, err, "must set runaway config for resource group `default`")
	err = tk.QueryToErr("query watch add resource group rg2 action DRYRUN sql text exact to 'select * from test.t1'")
	require.ErrorContains(t, err, "the group rg2 does not exist")

	tk.MustExec("alter resource group default QUERY_LIMIT=(EXEC_ELAPSED='50ms' ACTION=DRYRUN)")
	tk.MustQuery("query watch add sql text exact to 'select * from test.t1'").Check(testkit.Rows("1"))
	tk.MustQuery("QUERY WATCH ADD ACTION COOLDOWN SQL TEXT EXACT TO 'select * from test.t2'").Check(testkit.Rows("2"))
	tryInterval := time.Millisecond * 200
	maxWaitDuration := time.Second * 5
	tk.EventuallyMustQueryAndCheck("select SQL_NO_CACHE resource_group_name, watch_text, action, watch from mysql.tidb_runaway_watch", nil,
		testkit.Rows("default select * from test.t1 1 1", "default select * from test.t2 2 1"), maxWaitDuration, tryInterval)

	tk.MustExec("create resource group rg1 RU_PER_SEC=1000 QUERY_LIMIT=(EXEC_ELAPSED='50ms' ACTION=KILL)")
	tk.MustExec("create resource group rg2 RU_PER_SEC=1000 QUERY_LIMIT=(EXEC_ELAPSED='50ms' ACTION=KILL)")

	tk.MustQuery("query watch add resource group rg1 sql text exact to 'select * from test.t1'").Check(testkit.Rows("3"))
	tk.MustQuery("query watch add resource group rg1 sql text similar to 'select * from test.t2'").Check(testkit.Rows("4"))
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	tk.MustQueryWithContext(ctx, "query watch add resource group rg1 action DRYRUN sql text plan to 'select * from test.t3'").Check(testkit.Rows("5"))

	tk.MustQuery("query watch add action KILL SQL DIGEST '4ea0618129ffc6a7effbc0eff4bbcb41a7f5d4c53a6fa0b2e9be81c7010915b0'").Check(testkit.Rows("6"))
	tk.MustQuery("query watch add action KILL PLAN DIGEST 'd08bc323a934c39dc41948b0a073725be3398479b6fa4f6dd1db2a9b115f7f57'").Check(testkit.Rows("7"))

	tk.EventuallyMustQueryAndCheck("select SQL_NO_CACHE resource_group_name, watch_text, action, watch from mysql.tidb_runaway_watch order by id", nil,
		testkit.Rows("default select * from test.t1 1 1",
			"default select * from test.t2 2 1",
			"rg1 select * from test.t1 3 1",
			"rg1 02576c15e1f35a8aa3eb7e3b1f977c9f9f9921a22421b3e9f42bad5ab632b4f6 3 2",
			"rg1 d08bc323a934c39dc41948b0a073725be3398479b6fa4f6dd1db2a9b115f7f57 1 3",
			"default 4ea0618129ffc6a7effbc0eff4bbcb41a7f5d4c53a6fa0b2e9be81c7010915b0 3 2",
			"default d08bc323a934c39dc41948b0a073725be3398479b6fa4f6dd1db2a9b115f7f57 3 3",
		), maxWaitDuration, tryInterval)
	tk.MustQuery("query watch add action COOLDOWN sql text similar to 'select * from test.t1'").Check(testkit.Rows("8"))
	tk.MustQueryWithContext(ctx, "query watch add resource group rg2 action KILL sql text plan to 'select * from test.t3'").Check(testkit.Rows("9"))

	tk.EventuallyMustQueryAndCheck("select SQL_NO_CACHE resource_group_name, watch_text, action, watch from mysql.tidb_runaway_watch order by id", nil,
		testkit.Rows("default select * from test.t1 1 1",
			"default select * from test.t2 2 1",
			"rg1 select * from test.t1 3 1",
			"rg1 02576c15e1f35a8aa3eb7e3b1f977c9f9f9921a22421b3e9f42bad5ab632b4f6 3 2",
			"rg1 d08bc323a934c39dc41948b0a073725be3398479b6fa4f6dd1db2a9b115f7f57 1 3",
			"default d08bc323a934c39dc41948b0a073725be3398479b6fa4f6dd1db2a9b115f7f57 3 3",
			"default 4ea0618129ffc6a7effbc0eff4bbcb41a7f5d4c53a6fa0b2e9be81c7010915b0 2 2",
			"rg2 d08bc323a934c39dc41948b0a073725be3398479b6fa4f6dd1db2a9b115f7f57 3 3",
		), maxWaitDuration, tryInterval)

	tk.EventuallyMustQueryAndCheck("select SQL_NO_CACHE resource_group_name, watch_text, action, watch from information_schema.runaway_watches order by id", nil,
		testkit.Rows("default select * from test.t1 DryRun Exact",
			"default select * from test.t2 CoolDown Exact",
			"rg1 select * from test.t1 Kill Exact",
			"rg1 02576c15e1f35a8aa3eb7e3b1f977c9f9f9921a22421b3e9f42bad5ab632b4f6 Kill Similar",
			"rg1 d08bc323a934c39dc41948b0a073725be3398479b6fa4f6dd1db2a9b115f7f57 DryRun Plan",
			"default d08bc323a934c39dc41948b0a073725be3398479b6fa4f6dd1db2a9b115f7f57 Kill Plan",
			"default 4ea0618129ffc6a7effbc0eff4bbcb41a7f5d4c53a6fa0b2e9be81c7010915b0 CoolDown Similar",
			"rg2 d08bc323a934c39dc41948b0a073725be3398479b6fa4f6dd1db2a9b115f7f57 Kill Plan",
		), maxWaitDuration, tryInterval)

	// avoid the default resource group to be recorded.
	tk.MustExec("alter resource group default QUERY_LIMIT=(EXEC_ELAPSED='1000ms' ACTION=DRYRUN)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/copr/sleepCoprRequest", fmt.Sprintf("return(%d)", 60)))
	err = tk.QueryToErr("select /*+ resource_group(rg1) */ * from t3")
	require.ErrorContains(t, err, "[executor:8253]Query execution was interrupted, identified as runaway query")
	tk.EventuallyMustQueryAndCheck("select SQL_NO_CACHE resource_group_name, original_sql, match_type from mysql.tidb_runaway_queries", nil,
		testkit.Rows(
			"rg1 select /*+ resource_group(rg1) */ * from t3 watch",
			"rg1 select /*+ resource_group(rg1) */ * from t3 identify",
		), maxWaitDuration, tryInterval)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/sleepCoprRequest"))

	tk.MustExec("alter resource group default QUERY_LIMIT=(EXEC_ELAPSED='50ms' ACTION=DRYRUN)")
	tk.MustGetErrCode("select * from t3", mysql.ErrResourceGroupQueryRunawayQuarantine)
	tk.MustQuery("select * from t2").Check(testkit.Rows("1"))
	tk.MustQuery("select /*+ resource_group(rg1) */ * from t1").Check(testkit.Rows("1"))
	tk.MustExec("SET RESOURCE GROUP rg1")
	// hit and schema will affect sql digest
	tk.MustGetErrCode("select * from test.t2", mysql.ErrResourceGroupQueryRunawayQuarantine)
	tk.MustGetErrCode("select /*+ resource_group(rg2) */ * from t3", mysql.ErrResourceGroupQueryRunawayQuarantine)

	tk.MustExec("alter resource group rg1 RU_PER_SEC=1000 QUERY_LIMIT=()")
	tk.EventuallyMustQueryAndCheck("select SQL_NO_CACHE resource_group_name, watch_text, action, watch from information_schema.runaway_watches order by id", nil,
		testkit.Rows("default select * from test.t1 DryRun Exact",
			"default select * from test.t2 CoolDown Exact",
			"rg1 select * from test.t1 Kill Exact",
			"rg1 02576c15e1f35a8aa3eb7e3b1f977c9f9f9921a22421b3e9f42bad5ab632b4f6 Kill Similar",
			"rg1 d08bc323a934c39dc41948b0a073725be3398479b6fa4f6dd1db2a9b115f7f57 DryRun Plan",
			"default d08bc323a934c39dc41948b0a073725be3398479b6fa4f6dd1db2a9b115f7f57 Kill Plan",
			"default 4ea0618129ffc6a7effbc0eff4bbcb41a7f5d4c53a6fa0b2e9be81c7010915b0 CoolDown Similar",
			"rg2 d08bc323a934c39dc41948b0a073725be3398479b6fa4f6dd1db2a9b115f7f57 Kill Plan",
		), maxWaitDuration, tryInterval)

	tk.MustExec("alter resource group rg1 RU_PER_SEC=1000 QUERY_LIMIT=(EXEC_ELAPSED='50ms' ACTION=KILL)")
	tk.EventuallyMustQueryAndCheck("select SQL_NO_CACHE resource_group_name, watch_text, action, watch from information_schema.runaway_watches order by id", nil,
		testkit.Rows("default select * from test.t1 DryRun Exact",
			"default select * from test.t2 CoolDown Exact",
			"rg1 select * from test.t1 Kill Exact",
			"rg1 02576c15e1f35a8aa3eb7e3b1f977c9f9f9921a22421b3e9f42bad5ab632b4f6 Kill Similar",
			"rg1 d08bc323a934c39dc41948b0a073725be3398479b6fa4f6dd1db2a9b115f7f57 DryRun Plan",
			"default d08bc323a934c39dc41948b0a073725be3398479b6fa4f6dd1db2a9b115f7f57 Kill Plan",
			"default 4ea0618129ffc6a7effbc0eff4bbcb41a7f5d4c53a6fa0b2e9be81c7010915b0 CoolDown Similar",
			"rg2 d08bc323a934c39dc41948b0a073725be3398479b6fa4f6dd1db2a9b115f7f57 Kill Plan",
		), maxWaitDuration, tryInterval)

	// test remove
	rs, err := tk.Exec("query watch remove 1")
	require.NoError(t, err)
	require.Nil(t, rs)
	time.Sleep(1 * time.Second)
	tk.MustGetErrCode("select * from test.t1", mysql.ErrResourceGroupQueryRunawayQuarantine)
}
