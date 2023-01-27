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

package ddl_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/resourcegroup"
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

	hook := &ddl.TestDDLCallback{Do: dom}
	var groupID int64
	onJobUpdatedExportedFunc := func(job *model.Job) {
		// job.SchemaID will be assigned when the group is created.
		if (job.SchemaName == "x" || job.SchemaName == "y") && job.Type == model.ActionCreateResourceGroup && job.SchemaID != 0 {
			groupID = job.SchemaID
			return
		}
	}
	hook.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc)
	dom.DDL().SetHook(hook)

	tk.MustExec("set global tidb_enable_resource_control = 'off'")
	tk.MustGetErrCode("create user usr1 resource group rg1", mysql.ErrResourceGroupSupportDisabled)
	tk.MustExec("create user usr1")
	tk.MustGetErrCode("alter user usr1 resource group rg1", mysql.ErrResourceGroupSupportDisabled)
	tk.MustGetErrCode("create resource group x "+
		"RRU_PER_SEC=1000 "+
		"WRU_PER_SEC=2000", mysql.ErrResourceGroupSupportDisabled)

	tk.MustExec("set global tidb_enable_resource_control = 'on'")

	tk.MustExec("create resource group x " +
		"RRU_PER_SEC=1000 " +
		"WRU_PER_SEC=2000")
	checkFunc := func(groupInfo *model.ResourceGroupInfo) {
		require.Equal(t, true, groupInfo.ID != 0)
		require.Equal(t, "x", groupInfo.Name.L)
		require.Equal(t, groupID, groupInfo.ID)
		require.Equal(t, uint64(1000), groupInfo.RRURate)
		require.Equal(t, uint64(2000), groupInfo.WRURate)
	}
	// Check the group is correctly reloaded in the information schema.
	g := testResourceGroupNameFromIS(t, tk.Session(), "x")
	checkFunc(g)

	tk.MustExec("set global tidb_enable_resource_control = DEFAULT")
	tk.MustGetErrCode("alter resource group x "+
		"RRU_PER_SEC=2000 "+
		"WRU_PER_SEC=3000", mysql.ErrResourceGroupSupportDisabled)
	tk.MustGetErrCode("drop resource group x ", mysql.ErrResourceGroupSupportDisabled)

	tk.MustExec("set global tidb_enable_resource_control = 'on'")

	tk.MustGetErrCode("create resource group x "+
		"RRU_PER_SEC=1000 "+
		"WRU_PER_SEC=2000", mysql.ErrResourceGroupExists)

	tk.MustExec("alter resource group x " +
		"RRU_PER_SEC=2000 " +
		"WRU_PER_SEC=3000")
	g = testResourceGroupNameFromIS(t, tk.Session(), "x")
	re.Equal(uint64(2000), g.RRURate)
	re.Equal(uint64(3000), g.WRURate)

	tk.MustQuery("select * from information_schema.resource_groups where group_name = 'x'").Check(testkit.Rows(strconv.FormatInt(g.ID, 10) + " x 2000 3000"))

	tk.MustExec("drop resource group x")
	g = testResourceGroupNameFromIS(t, tk.Session(), "x")
	re.Nil(g)

	tk.MustExec("create resource group y " +
		"CPU='4000m' " +
		"IO_READ_BANDWIDTH='1G' " +
		"IO_WRITE_BANDWIDTH='300M'")
	checkFunc = func(groupInfo *model.ResourceGroupInfo) {
		require.Equal(t, true, groupInfo.ID != 0)
		require.Equal(t, "y", groupInfo.Name.L)
		require.Equal(t, groupID, groupInfo.ID)
		require.Equal(t, "4000m", groupInfo.CPULimiter)
		require.Equal(t, "1G", groupInfo.IOReadBandwidth)
		require.Equal(t, "300M", groupInfo.IOWriteBandwidth)
	}
	g = testResourceGroupNameFromIS(t, tk.Session(), "y")
	checkFunc(g)
	tk.MustExec("alter resource group y " +
		"CPU='8000m' " +
		"IO_READ_BANDWIDTH='10G' " +
		"IO_WRITE_BANDWIDTH='3000M'")
	checkFunc = func(groupInfo *model.ResourceGroupInfo) {
		require.Equal(t, true, groupInfo.ID != 0)
		require.Equal(t, "y", groupInfo.Name.L)
		require.Equal(t, groupID, groupInfo.ID)
		require.Equal(t, "8000m", groupInfo.CPULimiter)
		require.Equal(t, "10G", groupInfo.IOReadBandwidth)
		require.Equal(t, "3000M", groupInfo.IOWriteBandwidth)
	}
	g = testResourceGroupNameFromIS(t, tk.Session(), "y")
	checkFunc(g)
	tk.MustExec("drop resource group y")
	g = testResourceGroupNameFromIS(t, tk.Session(), "y")
	re.Nil(g)
	tk.MustContainErrMsg("create resource group x RRU_PER_SEC=1000, CPU='8000m';", resourcegroup.ErrInvalidResourceGroupDuplicatedMode.Error())
	groups, err := infosync.GetAllResourceGroups(context.TODO())
	require.Equal(t, 0, len(groups))
	require.NoError(t, err)

	// Check information schema table information_schema.resource_groups
	tk.MustExec("create resource group x " +
		"RRU_PER_SEC=1000 " +
		"WRU_PER_SEC=2000")
	g1 := testResourceGroupNameFromIS(t, tk.Session(), "x")
	tk.MustQuery("select * from information_schema.resource_groups where group_name = 'x'").Check(testkit.Rows(strconv.FormatInt(g1.ID, 10) + " x 1000 2000"))
	tk.MustQuery("show create resource group x").Check(testkit.Rows("x CREATE RESOURCE GROUP `x` RRU_PER_SEC=1000 WRU_PER_SEC=2000"))

	tk.MustExec("create resource group y " +
		"RRU_PER_SEC=2000 " +
		"WRU_PER_SEC=3000")
	g2 := testResourceGroupNameFromIS(t, tk.Session(), "y")
	tk.MustQuery("select * from information_schema.resource_groups where group_name = 'y'").Check(testkit.Rows(strconv.FormatInt(g2.ID, 10) + " y 2000 3000"))
	tk.MustQuery("show create resource group y").Check(testkit.Rows("y CREATE RESOURCE GROUP `y` RRU_PER_SEC=2000 WRU_PER_SEC=3000"))

	tk.MustExec("alter resource group y " +
		"RRU_PER_SEC=4000 " +
		"WRU_PER_SEC=2000")

	g2 = testResourceGroupNameFromIS(t, tk.Session(), "y")
	tk.MustQuery("select * from information_schema.resource_groups where group_name = 'y'").Check(testkit.Rows(strconv.FormatInt(g2.ID, 10) + " y 4000 2000"))
	tk.MustQuery("show create resource group y").Check(testkit.Rows("y CREATE RESOURCE GROUP `y` RRU_PER_SEC=4000 WRU_PER_SEC=2000"))

	tk.MustQuery("select count(*) from information_schema.resource_groups").Check(testkit.Rows("2"))
	tk.MustGetErrCode("create user usr_fail resource group nil_group", mysql.ErrResourceGroupNotExists)
	tk.MustExec("create user user2")
	tk.MustGetErrCode("alter user user2 resource group nil_group", mysql.ErrResourceGroupNotExists)

	tk.MustExec("create resource group do_not_delete_rg rru_per_sec=100 wru_per_sec=200")
	tk.MustExec("create user usr3 resource group do_not_delete_rg")
	tk.MustContainErrMsg("drop resource group do_not_delete_rg", "user [usr3] depends on the resource group to drop")
}

func testResourceGroupNameFromIS(t *testing.T, ctx sessionctx.Context, name string) *model.ResourceGroupInfo {
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	require.NoError(t, err)
	g, _ := dom.InfoSchema().ResourceGroupByName(model.NewCIStr(name))
	return g
}
