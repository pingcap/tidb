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

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/kv/TxnResouceGroupChecker", `return("default")`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/kv/TxnResouceGroupChecker"))
	}()
	tk.MustExec("insert into t values (1, 1);")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/kv/TxnResouceGroupChecker", `return("rg1")`))
	tk.MustExec("insert /*+ RESOURCE_GROUP(rg1) */ into t values (2, 2);")
	tk.MustExec("BEGIN;")
	// for pessimistic lock the resource group should be rg1
	tk.MustExec("insert /*+ RESOURCE_GROUP(rg1) */ into t values (3, 3);")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/kv/TxnResouceGroupChecker", `return("rg2")`))
	// for final prewrite/commit the resource group should be rg2
	tk.MustExec("update /*+ RESOURCE_GROUP(rg2) */ t set val = val + 1 where id = 3;")
	tk.MustExec("COMMIT;")

	tk.MustExec("SET @@autocommit=1;")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/kv/TxnResouceGroupChecker", `return("default")`))
	tk.MustExec("insert /*+ RESOURCE_GROUP(not_exist_group) */ into t values (4, 4);")

	tk.MustExec("BEGIN;")
	// for pessimistic lock the resource group should be rg1
	tk.MustExec("insert /*+ RESOURCE_GROUP(unknown_1) */ into t values (5, 5);")
	// for final prewrite/commit the resource group should be rg2
	tk.MustExec("update /*+ RESOURCE_GROUP(unknown_2) */ t set val = val + 1 where id = 5;")
	tk.MustExec("COMMIT;")
}
