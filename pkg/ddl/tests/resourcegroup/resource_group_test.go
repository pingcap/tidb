// Copyright 2022 PingCAP, Inc.
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

package resourcegrouptest_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/ddl/resourcegroup"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	mysql "github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestResourceGroupBasic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	re := require.New(t)

	hook := &callback.TestDDLCallback{Do: dom}
	var groupID atomic.Int64
	onJobUpdatedExportedFunc := func(job *model.Job) {
		// job.SchemaID will be assigned when the group is created.
		if (job.SchemaName == "x" || job.SchemaName == "y") && job.Type == model.ActionCreateResourceGroup && job.SchemaID != 0 {
			groupID.Store(job.SchemaID)
			return
		}
	}
	hook.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc)
	dom.DDL().SetHook(hook)

	tk.MustExec("set global tidb_enable_resource_control = 'off'")
	tk.MustGetErrCode("create user usr1 resource group rg1", mysql.ErrResourceGroupSupportDisabled)
	tk.MustExec("create user usr1")
	tk.MustGetErrCode("alter user usr1 resource group rg1", mysql.ErrResourceGroupSupportDisabled)
	tk.MustGetErrCode("create resource group x RU_PER_SEC=1000 ", mysql.ErrResourceGroupSupportDisabled)

	tk.MustExec("set global tidb_enable_resource_control = 'on'")

	// test default resource group.
	tk.MustQuery("select * from information_schema.resource_groups where name = 'default'").Check(testkit.Rows("default UNLIMITED MEDIUM YES <nil> <nil>"))
	tk.MustExec("alter resource group `default` PRIORITY=LOW")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'default'").Check(testkit.Rows("default UNLIMITED LOW YES <nil> <nil>"))
	tk.MustExec("alter resource group `default` ru_per_sec=1000")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'default'").Check(testkit.Rows("default 1000 LOW YES <nil> <nil>"))
	tk.MustContainErrMsg("drop resource group `default`", "can't drop reserved resource group")

	tk.MustExec("create resource group x RU_PER_SEC=1000")
	checkFunc := func(groupInfo *model.ResourceGroupInfo) {
		require.Equal(t, true, groupInfo.ID != 0)
		require.Equal(t, "x", groupInfo.Name.L)
		require.Equal(t, groupID.Load(), groupInfo.ID)
		require.Equal(t, uint64(1000), groupInfo.RURate)
		require.Nil(t, groupInfo.Runaway)
	}
	// Check the group is correctly reloaded in the information schema.
	g := testResourceGroupNameFromIS(t, tk.Session(), "x")
	checkFunc(g)

	// test create if not exists
	tk.MustExec("create resource group if not exists x RU_PER_SEC=10000")
	// Check the resource group is not changed
	g = testResourceGroupNameFromIS(t, tk.Session(), "x")
	checkFunc(g)
	// Check warning message
	res := tk.MustQuery("show warnings")
	res.Check(testkit.Rows("Note 8248 Resource group 'x' already exists"))

	tk.MustExec("set global tidb_enable_resource_control = off")
	tk.MustGetErrCode("alter resource group x RU_PER_SEC=2000 ", mysql.ErrResourceGroupSupportDisabled)
	tk.MustGetErrCode("drop resource group x ", mysql.ErrResourceGroupSupportDisabled)

	tk.MustExec("set global tidb_enable_resource_control = DEFAULT")

	tk.MustGetErrCode("create resource group x RU_PER_SEC=1000 ", mysql.ErrResourceGroupExists)

	tk.MustExec("alter resource group x RU_PER_SEC=2000 BURSTABLE QUERY_LIMIT=(EXEC_ELAPSED='15s' ACTION DRYRUN WATCH SIMILAR DURATION '10m0s')")
	g = testResourceGroupNameFromIS(t, tk.Session(), "x")
	re.Equal(uint64(2000), g.RURate)
	re.Equal(int64(-1), g.BurstLimit)
	re.Equal(uint64(time.Second*15/time.Millisecond), g.Runaway.ExecElapsedTimeMs)
	re.Equal(model.RunawayActionDryRun, g.Runaway.Action)
	re.Equal(model.WatchSimilar, g.Runaway.WatchType)
	re.Equal(int64(time.Minute*10/time.Millisecond), g.Runaway.WatchDurationMs)

	tk.MustExec("alter resource group x QUERY_LIMIT=(EXEC_ELAPSED='20s' ACTION DRYRUN WATCH SIMILAR)")
	g = testResourceGroupNameFromIS(t, tk.Session(), "x")
	re.Equal(uint64(2000), g.RURate)
	re.Equal(int64(-1), g.BurstLimit)
	re.Equal(uint64(time.Second*20/time.Millisecond), g.Runaway.ExecElapsedTimeMs)
	re.Equal(model.RunawayActionDryRun, g.Runaway.Action)
	re.Equal(model.WatchSimilar, g.Runaway.WatchType)
	re.Equal(int64(0), g.Runaway.WatchDurationMs)

	tk.MustQuery("select * from information_schema.resource_groups where name = 'x'").Check(testkit.Rows("x 2000 MEDIUM YES EXEC_ELAPSED='20s', ACTION=DRYRUN, WATCH=SIMILAR DURATION=UNLIMITED <nil>"))

	tk.MustExec("drop resource group x")
	g = testResourceGroupNameFromIS(t, tk.Session(), "x")
	re.Nil(g)

	tk.MustExec("alter resource group if exists not_exists RU_PER_SEC=2000")
	// Check warning message
	res = tk.MustQuery("show warnings")
	res.Check(testkit.Rows("Note 8249 Unknown resource group 'not_exists'"))

	tk.MustExec("create resource group y RU_PER_SEC=4000")
	checkFunc = func(groupInfo *model.ResourceGroupInfo) {
		re.Equal(true, groupInfo.ID != 0)
		re.Equal("y", groupInfo.Name.L)
		re.Equal(groupID.Load(), groupInfo.ID)
		re.Equal(uint64(4000), groupInfo.RURate)
		re.Equal(int64(4000), groupInfo.BurstLimit)
	}
	g = testResourceGroupNameFromIS(t, tk.Session(), "y")
	checkFunc(g)
	tk.MustGetErrCode("alter resource group y PRIORITY=hight", mysql.ErrParse)
	tk.MustExec("alter resource group y PRIORITY=high")
	checkFunc = func(groupInfo *model.ResourceGroupInfo) {
		re.Equal(true, groupInfo.ID != 0)
		re.Equal("y", groupInfo.Name.L)
		re.Equal(groupID.Load(), groupInfo.ID)
		re.Equal(uint64(4000), groupInfo.RURate)
		re.Equal(int64(4000), groupInfo.BurstLimit)
		re.Equal(uint64(16), groupInfo.Priority)
	}
	g = testResourceGroupNameFromIS(t, tk.Session(), "y")
	checkFunc(g)
	tk.MustExec("alter resource group y RU_PER_SEC=6000")
	checkFunc = func(groupInfo *model.ResourceGroupInfo) {
		re.Equal(true, groupInfo.ID != 0)
		re.Equal("y", groupInfo.Name.L)
		re.Equal(groupID.Load(), groupInfo.ID)
		re.Equal(uint64(6000), groupInfo.RURate)
		re.Equal(int64(6000), groupInfo.BurstLimit)
	}
	g = testResourceGroupNameFromIS(t, tk.Session(), "y")
	checkFunc(g)
	tk.MustExec("alter resource group y BURSTABLE RU_PER_SEC=5000 QUERY_LIMIT=(EXEC_ELAPSED='15s' ACTION KILL)")
	checkFunc = func(groupInfo *model.ResourceGroupInfo) {
		re.Equal(true, groupInfo.ID != 0)
		re.Equal("y", groupInfo.Name.L)
		re.Equal(groupID.Load(), groupInfo.ID)
		re.Equal(uint64(5000), groupInfo.RURate)
		re.Equal(int64(-1), groupInfo.BurstLimit)
		re.Equal(uint64(time.Second*15/time.Millisecond), groupInfo.Runaway.ExecElapsedTimeMs)
		re.Equal(model.RunawayActionKill, groupInfo.Runaway.Action)
		re.Equal(int64(0), groupInfo.Runaway.WatchDurationMs)
	}
	g = testResourceGroupNameFromIS(t, tk.Session(), "y")
	checkFunc(g)
	tk.MustExec("alter resource group y RU_PER_SEC=6000 BURSTABLE=false")
	checkFunc = func(groupInfo *model.ResourceGroupInfo) {
		re.Equal(true, groupInfo.ID != 0)
		re.Equal("y", groupInfo.Name.L)
		re.Equal(groupID.Load(), groupInfo.ID)
		re.Equal(uint64(6000), groupInfo.RURate)
		re.Equal(int64(6000), groupInfo.BurstLimit)
	}
	g = testResourceGroupNameFromIS(t, tk.Session(), "y")
	checkFunc(g)
	tk.MustExec("alter resource group y RU_PER_SEC=5000 BURSTABLE")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'y'").Check(testkit.Rows("y 5000 HIGH YES EXEC_ELAPSED='15s', ACTION=KILL <nil>"))
	tk.MustExec("drop resource group y")
	g = testResourceGroupNameFromIS(t, tk.Session(), "y")
	re.Nil(g)

	tk.MustGetErrCode("create resource group x ru_per_sec=1000 ru_per_sec=200", mysql.ErrParse)
	tk.MustContainErrMsg("create resource group x ru_per_sec=1000 ru_per_sec=200, ru_per_sec=300", "Dupliated options specified")
	tk.MustGetErrCode("create resource group x burstable, burstable", mysql.ErrParse)
	tk.MustContainErrMsg("create resource group x burstable, burstable", "Dupliated options specified")
	tk.MustGetErrCode("create resource group x  ru_per_sec=1000, burstable, burstable", mysql.ErrParse)
	tk.MustContainErrMsg("create resource group x  ru_per_sec=1000, burstable, burstable", "Dupliated options specified")
	tk.MustGetErrCode("create resource group x  burstable, ru_per_sec=1000, burstable", mysql.ErrParse)
	tk.MustContainErrMsg("create resource group x burstable, ru_per_sec=1000, burstable", "Dupliated options specified")
	tk.MustContainErrMsg("create resource group x ru_per_sec=1000 burstable QUERY_LIMIT=(EXEC_ELAPSED='15s' action kill action cooldown)", "Dupliated runaway options specified")
	tk.MustContainErrMsg("create resource group x ru_per_sec=1000 QUERY_LIMIT=(EXEC_ELAPSED='15s') burstable priority=Low, QUERY_LIMIT=(EXEC_ELAPSED='15s')", "Dupliated options specified")
	tk.MustContainErrMsg("create resource group x ru_per_sec=1000 QUERY_LIMIT=(EXEC_ELAPSED='15s') QUERY_LIMIT=(EXEC_ELAPSED='15s')", "Dupliated options specified")
	tk.MustContainErrMsg("create resource group x ru_per_sec=1000 QUERY_LIMIT=(action kill)", "invalid exec elapsed time")
	tk.MustGetErrCode("create resource group x ru_per_sec=1000 QUERY_LIMIT=(EXEC_ELAPSED='15s' action kil)", mysql.ErrParse)
	tk.MustContainErrMsg("create resource group x ru_per_sec=1000 QUERY_LIMIT=(EXEC_ELAPSED='15s')", "unknown resource group runaway action")
	tk.MustGetErrCode("create resource group x ru_per_sec=1000 EXEC_ELAPSED='15s' action kill", mysql.ErrParse)
	tk.MustContainErrMsg("create resource group x ru_per_sec=1000 QUERY_LIMIT=(EXEC_ELAPSED='15d' action kill)", "unknown unit \"d\"")
	groups, err := infosync.ListResourceGroups(context.TODO())
	re.Equal(1, len(groups))
	re.NoError(err)

	// Check information schema table information_schema.resource_groups
	tk.MustExec("create resource group x RU_PER_SEC=1000 PRIORITY=LOW")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'x'").Check(testkit.Rows("x 1000 LOW NO <nil> <nil>"))
	tk.MustExec("alter resource group x RU_PER_SEC=2000 BURSTABLE QUERY_LIMIT=(EXEC_ELAPSED='15s' action kill)")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'x'").Check(testkit.Rows("x 2000 LOW YES EXEC_ELAPSED='15s', ACTION=KILL <nil>"))
	tk.MustQuery("show create resource group x").Check(testkit.Rows("x CREATE RESOURCE GROUP `x` RU_PER_SEC=2000, PRIORITY=LOW, BURSTABLE, QUERY_LIMIT=(EXEC_ELAPSED=\"15s\" ACTION=KILL)"))
	tk.MustExec("CREATE RESOURCE GROUP `x_new` RU_PER_SEC=2000 PRIORITY=LOW BURSTABLE=true QUERY_LIMIT=(EXEC_ELAPSED=\"15s\" ACTION=KILL)")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'x_new'").Check(testkit.Rows("x_new 2000 LOW YES EXEC_ELAPSED='15s', ACTION=KILL <nil>"))
	tk.MustExec("alter resource group x BURSTABLE=false RU_PER_SEC=3000")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'x'").Check(testkit.Rows("x 3000 LOW NO EXEC_ELAPSED='15s', ACTION=KILL <nil>"))
	tk.MustQuery("show create resource group x").Check(testkit.Rows("x CREATE RESOURCE GROUP `x` RU_PER_SEC=3000, PRIORITY=LOW, QUERY_LIMIT=(EXEC_ELAPSED=\"15s\" ACTION=KILL)"))

	tk.MustExec("create resource group y BURSTABLE RU_PER_SEC=2000 QUERY_LIMIT=(EXEC_ELAPSED='1s' action COOLDOWN WATCH EXACT duration '1h')")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'y'").Check(testkit.Rows("y 2000 MEDIUM YES EXEC_ELAPSED='1s', ACTION=COOLDOWN, WATCH=EXACT DURATION='1h0m0s' <nil>"))
	tk.MustQuery("show create resource group y").Check(testkit.Rows("y CREATE RESOURCE GROUP `y` RU_PER_SEC=2000, PRIORITY=MEDIUM, BURSTABLE, QUERY_LIMIT=(EXEC_ELAPSED=\"1s\" ACTION=COOLDOWN WATCH=EXACT DURATION=\"1h0m0s\")"))
	tk.MustExec("CREATE RESOURCE GROUP `y_new` RU_PER_SEC=2000 PRIORITY=MEDIUM QUERY_LIMIT=(EXEC_ELAPSED=\"1s\" ACTION=COOLDOWN WATCH EXACT DURATION=\"1h0m0s\")")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'y_new'").Check(testkit.Rows("y_new 2000 MEDIUM NO EXEC_ELAPSED='1s', ACTION=COOLDOWN, WATCH=EXACT DURATION='1h0m0s' <nil>"))
	tk.MustExec("alter resource group y_new RU_PER_SEC=3000")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'y_new'").Check(testkit.Rows("y_new 3000 MEDIUM NO EXEC_ELAPSED='1s', ACTION=COOLDOWN, WATCH=EXACT DURATION='1h0m0s' <nil>"))

	tk.MustExec("CREATE RESOURCE GROUP `z` RU_PER_SEC=2000 PRIORITY=MEDIUM QUERY_LIMIT=(EXEC_ELAPSED=\"1s\" ACTION=COOLDOWN WATCH PLAN DURATION=\"1h0m0s\")")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'z'").Check(testkit.Rows("z 2000 MEDIUM NO EXEC_ELAPSED='1s', ACTION=COOLDOWN, WATCH=PLAN DURATION='1h0m0s' <nil>"))

	tk.MustExec("alter resource group y RU_PER_SEC=4000")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'y'").Check(testkit.Rows("y 4000 MEDIUM YES EXEC_ELAPSED='1s', ACTION=COOLDOWN, WATCH=EXACT DURATION='1h0m0s' <nil>"))
	tk.MustQuery("show create resource group y").Check(testkit.Rows("y CREATE RESOURCE GROUP `y` RU_PER_SEC=4000, PRIORITY=MEDIUM, BURSTABLE, QUERY_LIMIT=(EXEC_ELAPSED=\"1s\" ACTION=COOLDOWN WATCH=EXACT DURATION=\"1h0m0s\")"))

	tk.MustExec("alter resource group y RU_PER_SEC=4000 PRIORITY=HIGH BURSTABLE")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'y'").Check(testkit.Rows("y 4000 HIGH YES EXEC_ELAPSED='1s', ACTION=COOLDOWN, WATCH=EXACT DURATION='1h0m0s' <nil>"))
	tk.MustQuery("show create resource group y").Check(testkit.Rows("y CREATE RESOURCE GROUP `y` RU_PER_SEC=4000, PRIORITY=HIGH, BURSTABLE, QUERY_LIMIT=(EXEC_ELAPSED=\"1s\" ACTION=COOLDOWN WATCH=EXACT DURATION=\"1h0m0s\")"))

	tk.MustQuery("select count(*) from information_schema.resource_groups").Check(testkit.Rows("6"))
	tk.MustGetErrCode("create user usr_fail resource group nil_group", mysql.ErrResourceGroupNotExists)
	tk.MustContainErrMsg("create user usr_fail resource group nil_group", "Unknown resource group 'nil_group'")
	tk.MustExec("create user user2")
	tk.MustGetErrCode("alter user user2 resource group nil_group", mysql.ErrResourceGroupNotExists)
	tk.MustContainErrMsg("alter user user2 resource group nil_group", "Unknown resource group 'nil_group'")

	tk.MustExec("create resource group do_not_delete_rg ru_per_sec=100")
	tk.MustExec("create user usr3 resource group do_not_delete_rg")
	tk.MustQuery("select user_attributes from mysql.user where user = 'usr3'").Check(testkit.Rows(`{"resource_group": "do_not_delete_rg"}`))
	tk.MustContainErrMsg("drop resource group do_not_delete_rg", "user [usr3] depends on the resource group to drop")
	tk.MustExec("alter user usr3 resource group `default`")
	tk.MustExec("alter user usr3 resource group ``")
	tk.MustExec("alter user usr3 resource group `DeFault`")
	tk.MustQuery("select user_attributes from mysql.user where user = 'usr3'").Check(testkit.Rows(`{"resource_group": "default"}`))

	tk.MustExec("alter resource group default ru_per_sec = 1000, priority = medium, background = (task_types = 'lightning, BR');")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'default'").Check(testkit.Rows("default 1000 MEDIUM YES <nil> TASK_TYPES='lightning,br'"))
	tk.MustQuery("show create resource group default").Check(testkit.Rows("default CREATE RESOURCE GROUP `default` RU_PER_SEC=1000, PRIORITY=MEDIUM, BURSTABLE, BACKGROUND=(TASK_TYPES='lightning,br')"))
	g = testResourceGroupNameFromIS(t, tk.Session(), "default")
	require.EqualValues(t, g.Background.JobTypes, []string{"lightning", "br"})

	tk.MustContainErrMsg("create resource group bg ru_per_sec = 1000 background = (task_types = 'lightning')", "unsupported operation")
	tk.MustContainErrMsg("alter resource group x background=(task_types='')", "unsupported operation")
	tk.MustGetErrCode("alter resource group default background=(task_types='a,b,c')", mysql.ErrResourceGroupInvalidBackgroundTaskName)
}

func testResourceGroupNameFromIS(t *testing.T, ctx sessionctx.Context, name string) *model.ResourceGroupInfo {
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	require.NoError(t, err)
	g, _ := dom.InfoSchema().ResourceGroupByName(model.NewCIStr(name))
	return g
}

func TestResourceGroupRunaway(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/FastRunawayGC", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/FastRunawayGC"))
	}()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))

	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")

	tk.MustExec("set global tidb_enable_resource_control='on'")
	tk.MustExec("create resource group rg1 RU_PER_SEC=1000 QUERY_LIMIT=(EXEC_ELAPSED='50ms' ACTION=KILL)")
	tk.MustExec("create resource group rg2 BURSTABLE RU_PER_SEC=2000 QUERY_LIMIT=(EXEC_ELAPSED='50ms' action KILL WATCH EXACT duration '1s')")
	tk.MustExec("create resource group rg3 BURSTABLE RU_PER_SEC=2000 QUERY_LIMIT=(EXEC_ELAPSED='50ms' action KILL WATCH EXACT)")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'rg2'").Check(testkit.Rows("rg2 2000 MEDIUM YES EXEC_ELAPSED='50ms', ACTION=KILL, WATCH=EXACT DURATION='1s' <nil>"))
	tk.MustQuery("select * from information_schema.resource_groups where name = 'rg3'").Check(testkit.Rows("rg3 2000 MEDIUM YES EXEC_ELAPSED='50ms', ACTION=KILL, WATCH=EXACT DURATION=UNLIMITED <nil>"))
	tk.MustQuery("select /*+ resource_group(rg1) */ * from t").Check(testkit.Rows("1"))
	tk.MustQuery("select /*+ resource_group(rg2) */ * from t").Check(testkit.Rows("1"))
	tk.MustQuery("select /*+ resource_group(rg3) */ * from t").Check(testkit.Rows("1"))

	require.Eventually(t, func() bool {
		return dom.RunawayManager().IsSyncerInitialized()
	}, 20*time.Second, 300*time.Millisecond)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/copr/sleepCoprRequest", fmt.Sprintf("return(%d)", 60)))
	err := tk.QueryToErr("select /*+ resource_group(rg1) */ * from t")
	require.ErrorContains(t, err, "[executor:8253]Query execution was interrupted, identified as runaway query")

	tryInterval := time.Millisecond * 100
	maxWaitDuration := time.Second * 5
	tk.EventuallyMustQueryAndCheck("select SQL_NO_CACHE resource_group_name, original_sql, match_type from mysql.tidb_runaway_queries", nil,
		testkit.Rows("rg1 select /*+ resource_group(rg1) */ * from t identify"), maxWaitDuration, tryInterval)
	// require.Len(t, tk.MustQuery("select SQL_NO_CACHE resource_group_name, original_sql, time from mysql.tidb_runaway_queries").Rows(), 0)
	tk.EventuallyMustQueryAndCheck("select SQL_NO_CACHE resource_group_name, original_sql, time from mysql.tidb_runaway_queries", nil,
		nil, maxWaitDuration, tryInterval)
	tk.MustExec("alter resource group rg1 RU_PER_SEC=1000 QUERY_LIMIT=(EXEC_ELAPSED='100ms' ACTION=COOLDOWN)")
	tk.MustQuery("select /*+ resource_group(rg1) */ * from t").Check(testkit.Rows("1"))

	tk.MustExec("alter resource group rg1 RU_PER_SEC=1000 QUERY_LIMIT=(EXEC_ELAPSED='100ms' ACTION=DRYRUN)")
	tk.MustQuery("select /*+ resource_group(rg1) */ * from t").Check(testkit.Rows("1"))

	err = tk.QueryToErr("select /*+ resource_group(rg2) */ * from t")
	require.ErrorContains(t, err, "Query execution was interrupted, identified as runaway query")
	tk.MustGetErrCode("select /*+ resource_group(rg2) */ * from t", mysql.ErrResourceGroupQueryRunawayQuarantine)
	tk.EventuallyMustQueryAndCheck("select SQL_NO_CACHE resource_group_name, original_sql, match_type from mysql.tidb_runaway_queries", nil,
		testkit.Rows("rg2 select /*+ resource_group(rg2) */ * from t identify",
			"rg2 select /*+ resource_group(rg2) */ * from t watch"), maxWaitDuration, tryInterval)
	tk.MustQuery("select SQL_NO_CACHE resource_group_name, watch_text from mysql.tidb_runaway_watch").
		Check(testkit.Rows("rg2 select /*+ resource_group(rg2) */ * from t"))
	// wait for the runaway watch to be cleaned up
	tk.EventuallyMustQueryAndCheck("select SQL_NO_CACHE resource_group_name, watch_text from mysql.tidb_runaway_watch", nil, testkit.Rows(), maxWaitDuration, tryInterval)
	err = tk.QueryToErr("select /*+ resource_group(rg2) */ * from t")
	require.ErrorContains(t, err, "Query execution was interrupted, identified as runaway query")

	tk.EventuallyMustQueryAndCheck("select SQL_NO_CACHE resource_group_name, original_sql, time from mysql.tidb_runaway_queries", nil,
		nil, maxWaitDuration, tryInterval)
	tk.EventuallyMustQueryAndCheck("select SQL_NO_CACHE resource_group_name, watch_text, end_time from mysql.tidb_runaway_watch", nil,
		nil, maxWaitDuration, tryInterval)
	err = tk.QueryToErr("select /*+ resource_group(rg3) */ * from t")
	require.ErrorContains(t, err, "Query execution was interrupted, identified as runaway query")
	tk.MustGetErrCode("select /*+ resource_group(rg3) */ * from t", mysql.ErrResourceGroupQueryRunawayQuarantine)
	tk.EventuallyMustQueryAndCheck("select SQL_NO_CACHE resource_group_name, watch_text from mysql.tidb_runaway_watch", nil,
		testkit.Rows("rg3 select /*+ resource_group(rg3) */ * from t"), maxWaitDuration, tryInterval)

	tk.MustExec("alter resource group rg2 RU_PER_SEC=1000 QUERY_LIMIT=(EXEC_ELAPSED='50ms' ACTION=COOLDOWN)")
	tk.MustQuery("select /*+ resource_group(rg2) */ * from t").Check(testkit.Rows("1"))
	tk.MustExec("alter resource group rg2 RU_PER_SEC=1000 QUERY_LIMIT=(EXEC_ELAPSED='50ms' ACTION=DRYRUN)")
	tk.MustQuery("select /*+ resource_group(rg2) */ * from t").Check(testkit.Rows("1"))
	tk.MustGetErrCode("select /*+ resource_group(rg3) */ * from t", mysql.ErrResourceGroupQueryRunawayQuarantine)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/sleepCoprRequest"))

	tk.MustExec("create resource group rg4 BURSTABLE RU_PER_SEC=2000 QUERY_LIMIT=(EXEC_ELAPSED='50ms' action KILL WATCH EXACT)")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/copr/sleepCoprAfterReq", fmt.Sprintf("return(%d)", 50)))
	tk.MustQuery("select /*+ resource_group(rg4) */ * from t").Check(testkit.Rows("1"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/sleepCoprAfterReq"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/copr/sleepCoprAfterReq", fmt.Sprintf("return(%d)", 60)))
	err = tk.QueryToErr("select /*+ resource_group(rg4) */ * from t")
	require.ErrorContains(t, err, "Query execution was interrupted, identified as runaway query")
	tk.MustGetErrCode("select /*+ resource_group(rg4) */ * from t", mysql.ErrResourceGroupQueryRunawayQuarantine)
	tk.EventuallyMustQueryAndCheck("select SQL_NO_CACHE resource_group_name, watch_text from mysql.tidb_runaway_watch", nil,
		testkit.Rows("rg3 select /*+ resource_group(rg3) */ * from t", "rg4 select /*+ resource_group(rg4) */ * from t"), maxWaitDuration, tryInterval)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/sleepCoprAfterReq"))
}

func TestAlreadyExistsDefaultResourceGroup(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/infosync/managerAlreadyCreateSomeGroups", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/infosync/managerAlreadyCreateSomeGroups"))
	}()
	testkit.CreateMockStoreAndDomain(t)
	groups, _ := infosync.ListResourceGroups(context.TODO())
	require.Equal(t, 2, len(groups))
}

func TestNewResourceGroupFromOptions(t *testing.T) {
	type TestCase struct {
		name      string
		groupName string
		input     *model.ResourceGroupSettings
		output    *rmpb.ResourceGroup
		err       error
	}
	var tests []TestCase
	groupName := "test"
	tests = append(tests, TestCase{
		name:  "empty 1",
		input: &model.ResourceGroupSettings{},
		err:   resourcegroup.ErrUnknownResourceGroupMode,
	})

	tests = append(tests, TestCase{
		name:  "empty 2",
		input: nil,
		err:   resourcegroup.ErrInvalidGroupSettings,
	})

	tests = append(tests, TestCase{
		name: "normal case: ru case 1",
		input: &model.ResourceGroupSettings{
			RURate:   2000,
			Priority: 0,
		},
		output: &rmpb.ResourceGroup{
			Name:     groupName,
			Mode:     rmpb.GroupMode_RUMode,
			Priority: 0,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 2000}},
			},
		},
	})

	tests = append(tests, TestCase{
		name: "normal case: ru case 2",
		input: &model.ResourceGroupSettings{
			RURate:   5000,
			Priority: 8,
		},
		output: &rmpb.ResourceGroup{
			Name:     groupName,
			Priority: 8,
			Mode:     rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 5000}},
			},
		},
	})

	tests = append(tests, TestCase{
		name: "error case: native case 1",
		input: &model.ResourceGroupSettings{
			CPULimiter:       "8",
			IOReadBandwidth:  "3000MB/s",
			IOWriteBandwidth: "3000Mi",
		},
		err: resourcegroup.ErrUnknownResourceGroupMode,
	})

	tests = append(tests, TestCase{
		name: "error case: native case 2",
		input: &model.ResourceGroupSettings{
			CPULimiter:       "8c",
			IOReadBandwidth:  "3000Mi",
			IOWriteBandwidth: "3000Mi",
		},
		err: resourcegroup.ErrUnknownResourceGroupMode,
	})

	tests = append(tests, TestCase{
		name: "error case: native case 3",
		input: &model.ResourceGroupSettings{
			CPULimiter:       "8",
			IOReadBandwidth:  "3000G",
			IOWriteBandwidth: "3000MB",
		},
		err: resourcegroup.ErrUnknownResourceGroupMode,
	})

	tests = append(tests, TestCase{
		name: "error case: duplicated mode",
		input: &model.ResourceGroupSettings{
			CPULimiter:       "8",
			IOReadBandwidth:  "3000Mi",
			IOWriteBandwidth: "3000Mi",
			RURate:           1000,
		},
		err: resourcegroup.ErrInvalidResourceGroupDuplicatedMode,
	})

	tests = append(tests, TestCase{
		name:      "error case: duplicated mode",
		groupName: "test_group_too_looooooooooooooooooooooooooooooooooooooooooooooooong",
		input: &model.ResourceGroupSettings{
			CPULimiter:       "8",
			IOReadBandwidth:  "3000Mi",
			IOWriteBandwidth: "3000Mi",
			RURate:           1000,
		},
		err: resourcegroup.ErrTooLongResourceGroupName,
	})

	for _, test := range tests {
		name := groupName
		if len(test.groupName) > 0 {
			name = test.groupName
		}
		group, err := resourcegroup.NewGroupFromOptions(name, test.input)
		comment := fmt.Sprintf("[%s]\nerr1 %s\nerr2 %s", test.name, err, test.err)
		if test.err != nil {
			require.ErrorIs(t, err, test.err, comment)
		} else {
			require.NoError(t, err, comment)
			require.Equal(t, test.output, group)
		}
	}
}

func TestBindHints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	re := require.New(t)

	tk.MustExec("drop resource group if exists rg1")
	tk.MustExec("create resource group rg1 RU_PER_SEC=1000")

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")

	tk.MustExec("create global binding for select * from t using select /*+ resource_group(rg1) */ * from t")
	tk.MustQuery("select * from t")
	re.Equal("rg1", tk.Session().GetSessionVars().StmtCtx.ResourceGroup)
	re.Equal("rg1", tk.Session().GetSessionVars().StmtCtx.ResourceGroupName)
	re.Equal("default", tk.Session().GetSessionVars().ResourceGroupName)
	tk.MustQuery("select a, b from t")
	re.Equal("", tk.Session().GetSessionVars().StmtCtx.ResourceGroup)
	re.Equal("default", tk.Session().GetSessionVars().StmtCtx.ResourceGroupName)
	re.Equal("default", tk.Session().GetSessionVars().ResourceGroupName)
}
