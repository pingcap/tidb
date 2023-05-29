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

	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/ddl/resourcegroup"
	"github.com/pingcap/tidb/ddl/util/callback"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/testkit"
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
	tk.MustQuery("select * from information_schema.resource_groups where name = 'default'").Check(testkit.Rows("default UNLIMITED MEDIUM YES"))
	tk.MustExec("alter resource group `default` RU_PER_SEC=1000 PRIORITY=LOW")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'default'").Check(testkit.Rows("default 1000 LOW NO"))
	tk.MustContainErrMsg("drop resource group `default`", "can't drop reserved resource group")

	tk.MustExec("create resource group x RU_PER_SEC=1000")
	checkFunc := func(groupInfo *model.ResourceGroupInfo) {
		require.Equal(t, true, groupInfo.ID != 0)
		require.Equal(t, "x", groupInfo.Name.L)
		require.Equal(t, groupID.Load(), groupInfo.ID)
		require.Equal(t, uint64(1000), groupInfo.RURate)
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

	tk.MustExec("alter resource group x RU_PER_SEC=2000 BURSTABLE")
	g = testResourceGroupNameFromIS(t, tk.Session(), "x")
	re.Equal(uint64(2000), g.RURate)
	re.Equal(int64(-1), g.BurstLimit)

	tk.MustQuery("select * from information_schema.resource_groups where name = 'x'").Check(testkit.Rows("x 2000 MEDIUM YES"))

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
	tk.MustExec("alter resource group y BURSTABLE RU_PER_SEC=5000")
	checkFunc = func(groupInfo *model.ResourceGroupInfo) {
		re.Equal(true, groupInfo.ID != 0)
		re.Equal("y", groupInfo.Name.L)
		re.Equal(groupID.Load(), groupInfo.ID)
		re.Equal(uint64(5000), groupInfo.RURate)
		re.Equal(int64(-1), groupInfo.BurstLimit)
	}
	g = testResourceGroupNameFromIS(t, tk.Session(), "y")
	checkFunc(g)
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
	groups, err := infosync.ListResourceGroups(context.TODO())
	re.Equal(1, len(groups))
	re.NoError(err)

	// Check information schema table information_schema.resource_groups
	tk.MustExec("create resource group x RU_PER_SEC=1000 PRIORITY=LOW")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'x'").Check(testkit.Rows("x 1000 LOW NO"))
	tk.MustExec("alter resource group x RU_PER_SEC=2000 BURSTABLE")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'x'").Check(testkit.Rows("x 2000 MEDIUM YES"))
	tk.MustExec("alter resource group x BURSTABLE RU_PER_SEC=3000")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'x'").Check(testkit.Rows("x 3000 MEDIUM YES"))
	tk.MustQuery("show create resource group x").Check(testkit.Rows("x CREATE RESOURCE GROUP `x` RU_PER_SEC=3000 PRIORITY=MEDIUM BURSTABLE"))

	tk.MustExec("create resource group y BURSTABLE RU_PER_SEC=2000")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'y'").Check(testkit.Rows("y 2000 MEDIUM YES"))
	tk.MustQuery("show create resource group y").Check(testkit.Rows("y CREATE RESOURCE GROUP `y` RU_PER_SEC=2000 PRIORITY=MEDIUM BURSTABLE"))

	tk.MustExec("alter resource group y RU_PER_SEC=4000 BURSTABLE")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'y'").Check(testkit.Rows("y 4000 MEDIUM YES"))
	tk.MustQuery("show create resource group y").Check(testkit.Rows("y CREATE RESOURCE GROUP `y` RU_PER_SEC=4000 PRIORITY=MEDIUM BURSTABLE"))

	tk.MustExec("alter resource group y RU_PER_SEC=4000 PRIORITY=HIGH BURSTABLE")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'y'").Check(testkit.Rows("y 4000 HIGH YES"))
	tk.MustQuery("show create resource group y").Check(testkit.Rows("y CREATE RESOURCE GROUP `y` RU_PER_SEC=4000 PRIORITY=HIGH BURSTABLE"))

	tk.MustQuery("select count(*) from information_schema.resource_groups").Check(testkit.Rows("3"))
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
}

func testResourceGroupNameFromIS(t *testing.T, ctx sessionctx.Context, name string) *model.ResourceGroupInfo {
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	require.NoError(t, err)
	g, _ := dom.InfoSchema().ResourceGroupByName(model.NewCIStr(name))
	return g
}

func TestResourceGroupHint(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t1(c1 int)")
	tk.MustExec("insert into t1 values(1)")

	tk.MustExec("set global tidb_enable_resource_control='on'")
	tk.MustExec("create resource group rg1 ru_per_sec=1000")
	tk.MustQuery("select /*+ resource_group(default) */ * from t1")
	tk.MustQuery("select /*+ resource_group(rg1) */ * from t1")
	tk.MustQuery("select /*+ resource_group(rg1) resource_group(default) */ * from t1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 RESOURCE_GROUP() is defined more than once, only the last definition takes effect: RESOURCE_GROUP(default)"))
	tk.MustQuery("select /*+ resource_group(rg1) */ DB, RESOURCE_GROUP from information_schema.processlist").Check(testkit.Rows("test rg1"))
	tk.MustQuery("select DB, RESOURCE_GROUP from information_schema.processlist").Check(testkit.Rows("test "))
	tk.MustExec("set global tidb_enable_resource_control='off'")
	tk.MustQuery("select /*+ resource_group(rg1) */ DB, RESOURCE_GROUP from information_schema.processlist").Check(testkit.Rows("test "))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 8250 Resource control feature is disabled. Run `SET GLOBAL tidb_enable_resource_control='on'` to enable the feature"))
}

func TestAlreadyExistsDefaultResourceGroup(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/infosync/managerAlreadyCreateSomeGroups", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/domain/infosync/managerAlreadyCreateSomeGroups"))
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
	re.Equal("", tk.Session().GetSessionVars().ResourceGroupName)
	tk.MustQuery("select a, b from t")
	re.Equal("", tk.Session().GetSessionVars().StmtCtx.ResourceGroup)
	re.Equal("", tk.Session().GetSessionVars().ResourceGroupName)
}
