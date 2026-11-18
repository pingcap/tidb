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

package resourcegrouptest

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestResourceGroupHintInTxn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create resource group rg1 ru_per_sec=1000")
	tk.MustExec("create resource group rg2 ru_per_sec=1000")
	tk.MustExec("use test;")
	tk.MustExec("create table t (id int primary key, val int)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/kv/TxnResourceGroupChecker", `return("default")`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/kv/TxnResourceGroupChecker"))
	}()
	tk.MustExec("insert into t values (1, 1);")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/kv/TxnResourceGroupChecker", `return("rg1")`))
	tk.MustExec("insert /*+ RESOURCE_GROUP(rg1) */ into t values (2, 2);")
	tk.MustExec("BEGIN;")
	// for pessimistic lock the resource group should be rg1
	tk.MustExec("insert /*+ RESOURCE_GROUP(rg1) */ into t values (3, 3);")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/kv/TxnResourceGroupChecker", `return("rg2")`))
	// for final prewrite/commit the resource group should be rg2
	tk.MustExec("update /*+ RESOURCE_GROUP(rg2) */ t set val = val + 1 where id = 3;")
	tk.MustExec("COMMIT;")

	tk.MustExec("SET @@autocommit=1;")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/kv/TxnResourceGroupChecker", `return("default")`))
	tk.MustExec("insert /*+ RESOURCE_GROUP(not_exist_group) */ into t values (4, 4);")

	tk.MustExec("BEGIN;")
	// for pessimistic lock the resource group should be rg1
	tk.MustExec("insert /*+ RESOURCE_GROUP(unknown_1) */ into t values (5, 5);")
	// for final prewrite/commit the resource group should be rg2
	tk.MustExec("update /*+ RESOURCE_GROUP(unknown_2) */ t set val = val + 1 where id = 5;")
	tk.MustExec("COMMIT;")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/kv/TxnResourceGroupChecker", `return("rg1")`))
	tk.MustExec("insert /*+ RESOURCE_GROUP(RG1) */ into t values (6, 6);")
	require.NoError(t, tk.ExecToErr("select /*+ RESOURCE_GROUP(RG1) */ * from t;"))
}

func TestSessionFallsBackFromDroppedResourceGroup(t *testing.T) {
	store := testkit.CreateMockStore(t)
	root := testkit.NewTestKit(t, store)
	root.MustExec("use test")
	root.MustExec("create table t (id int primary key)")
	root.MustExec("create resource group rg1 ru_per_sec=1000")
	root.MustExec("create resource group rg2 ru_per_sec=1000")
	root.MustExec("create user u1 resource group rg1")
	root.MustExec("grant all on test.* to u1")

	// A session logged in before its user moves to another group falls back
	// to the user's current group once the old group is dropped.
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "%"}, nil, nil, nil))
	tk.MustQuery("select current_resource_group()").Check(testkit.Rows("rg1"))
	root.MustExec("alter user u1 resource group rg2")
	tk.MustQuery("select current_resource_group()").Check(testkit.Rows("rg1"))
	root.MustExec("drop resource group rg1")
	tk.MustQuery("select current_resource_group()").Check(testkit.Rows("rg2"))
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 resource group rg1 no longer exists, the session now uses resource group rg2"))
	tk.MustQuery("select current_resource_group()").Check(testkit.Rows("rg2"))
	tk.MustQuery("show warnings").Check(testkit.Rows())

	// A group set by SET RESOURCE GROUP falls back to default when the user
	// has no group. A transaction started before the drop sends its later
	// requests and its commit in the new group.
	root.MustExec("create resource group rg3 ru_per_sec=1000")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("set resource group rg3")
	// The checker is attached to the transaction when it starts, so enable it
	// only around the start: DDL run later is not checked. The reads before the
	// drop send no lock, prewrite or commit request.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/kv/TxnResourceGroupChecker", `return("default")`))
	tk2.MustExec("begin pessimistic")
	tk2.MustQuery("select * from t").Check(testkit.Rows())
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/kv/TxnResourceGroupChecker"))
	root.MustExec("drop resource group rg3")
	tk2.MustExec("insert into t values (1)")
	tk2.MustQuery("select current_resource_group()").Check(testkit.Rows("default"))
	tk2.MustExec("commit")

	// A group dropped and created again before the next statement is kept.
	root.MustExec("create resource group rg4 ru_per_sec=1000")
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("set resource group rg4")
	root.MustExec("drop resource group rg4")
	root.MustExec("create resource group rg4 ru_per_sec=2000")
	tk3.MustQuery("select current_resource_group()").Check(testkit.Rows("rg4"))
	tk3.MustQuery("show warnings").Check(testkit.Rows())
}
