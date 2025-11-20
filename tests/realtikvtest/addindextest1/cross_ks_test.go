// Copyright 2025 PingCAP, Inc.
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

package addindextest

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/schemaver"
	"github.com/pingcap/tidb/pkg/keyspace"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestAddIndexOnSystemTable(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("This test is only for nextgen kernel, skip it in classic kernel")
	}

	runtimes := realtikvtest.PrepareForCrossKSTest(t, "keyspace1")
	userStore := runtimes["keyspace1"].Store
	// submit add index sql on user keyspace
	tk := testkit.NewTestKit(t, userStore)
	tk.PrepareDB("crossks")
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t values (1, 2);")
	tk.MustExec("alter table t add index idx_a (a);")
	tk.MustExec("admin check table t;")
	rs := tk.MustQuery("admin show ddl jobs 1;").Rows()
	jobIDStr := rs[0][0].(string)
	jobID, err := strconv.Atoi(jobIDStr)
	require.NoError(t, err)
	builder := ddl.NewTaskKeyBuilder()
	taskKey := builder.Build(int64(jobID))

	// job to user keyspace, task to system keyspace
	sysKSTk := testkit.NewTestKit(t, kvstore.GetSystemStorage())
	taskQuerySQL := fmt.Sprintf(`select sum(c) from (select count(1) c from mysql.tidb_global_task where task_key='%s'
		union select count(1) c from mysql.tidb_global_task_history where task_key='%s') t`, taskKey, taskKey)
	sysKSTk.MustQuery(taskQuerySQL).Check(testkit.Rows("1"))
	// reverse check
	tk.MustQuery(taskQuerySQL).Check(testkit.Rows("0"))
}

func TestCrossKSInfoSchemaSync(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("This test is only for nextgen kernel, skip it in classic kernel")
	}
	runtimes := realtikvtest.PrepareForCrossKSTest(t, "keyspace1", "keyspace2")
	sysDom := runtimes[keyspace.System].Dom
	ks1Store, ks1Dom := runtimes["keyspace1"].Store, runtimes["keyspace1"].Dom
	ks2Store, ks2Dom := runtimes["keyspace2"].Store, runtimes["keyspace2"].Dom

	t.Run("cross keyspace is lazily initialized in SYSTEM ks", func(t *testing.T) {
		require.Empty(t, sysDom.GetCrossKSMgr().GetAllKeyspace())
	})

	t.Run("cross keyspace is eagerly initialized in user ks", func(t *testing.T) {
		require.Len(t, ks1Dom.GetCrossKSMgr().GetAllKeyspace(), 1)
		require.Contains(t, ks1Dom.GetCrossKSMgr().GetAllKeyspace(), keyspace.System)
		require.Len(t, ks2Dom.GetCrossKSMgr().GetAllKeyspace(), 1)
		require.Contains(t, ks2Dom.GetCrossKSMgr().GetAllKeyspace(), keyspace.System)
	})

	t.Run("skip syncing user table of user keyspace in cross keyspace", func(t *testing.T) {
		ks1TK := testkit.NewTestKit(t, ks1Store)
		ks1TK.PrepareDB("crossks")
		ks1TK.MustExec("create table t (a int);")
		ks1TK.MustExec("insert into t values (1);")
		ks1TK.MustExec("alter table t add index idx_a (a);")
		// now initialized cross ks for ks1 in SYSTEM.
		require.Len(t, sysDom.GetCrossKSMgr().GetAllKeyspace(), 1)
		require.Contains(t, sysDom.GetCrossKSMgr().GetAllKeyspace(), "keyspace1")
		var sum *schemaver.SyncSummary
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitVersionSynced", func(inSum *schemaver.SyncSummary) {
			sum = inSum
		})
		ks1TK.MustExec("create table t1 (a int);")
		require.EqualValues(t, 1, sum.ServerCount)
		require.EqualValues(t, 0, sum.AssumedServerCount)
	})

	t.Run("skip syncing user table of SYSTEM keyspace in cross keyspace", func(t *testing.T) {
		sysTK := testkit.NewTestKit(t, kvstore.GetSystemStorage())
		sysTK.PrepareDB("crossks")
		var sum *schemaver.SyncSummary
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitVersionSynced", func(inSum *schemaver.SyncSummary) {
			sum = inSum
		})
		sysTK.MustExec("create table t (a int);")
		require.EqualValues(t, 1, sum.ServerCount)
		require.EqualValues(t, 0, sum.AssumedServerCount)
	})

	t.Run("syncing system tables of user keyspace in cross keyspace", func(t *testing.T) {
		ks1TK := testkit.NewTestKit(t, ks1Store)
		ks1TK.PrepareDB("crossks")
		ks1TK.MustExec("create table t (a int);")
		ks1TK.MustExec("insert into t values (1);")
		ks1TK.MustExec("alter table t add index idx_a (a);")
		// now initialized cross ks for ks1 in SYSTEM.
		require.Len(t, sysDom.GetCrossKSMgr().GetAllKeyspace(), 1)
		require.Contains(t, sysDom.GetCrossKSMgr().GetAllKeyspace(), "keyspace1")
		var sum *schemaver.SyncSummary
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitVersionSynced", func(inSum *schemaver.SyncSummary) {
			sum = inSum
		})
		ks1TK.MustExec("alter table mysql.user add index(file_priv)")
		require.EqualValues(t, 2, sum.ServerCount)
		require.EqualValues(t, 1, sum.AssumedServerCount)
	})

	t.Run("for uninitialized cross ks, system tables of user keyspace is not synced", func(t *testing.T) {
		require.NotContains(t, sysDom.GetCrossKSMgr().GetAllKeyspace(), "keyspace2")
		ks2TK := testkit.NewTestKit(t, ks2Store)
		var sum *schemaver.SyncSummary
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitVersionSynced", func(inSum *schemaver.SyncSummary) {
			sum = inSum
		})
		ks2TK.MustExec("alter table mysql.user add index(file_priv)")
		require.EqualValues(t, 1, sum.ServerCount)
		require.EqualValues(t, 0, sum.AssumedServerCount)
	})

	t.Run("syncing system tables of SYSTEM keyspace in cross keyspace", func(t *testing.T) {
		sysTK := testkit.NewTestKit(t, kvstore.GetSystemStorage())
		var sum *schemaver.SyncSummary
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitVersionSynced", func(inSum *schemaver.SyncSummary) {
			sum = inSum
		})
		sysTK.MustExec("alter table mysql.user add index(file_priv)")
		require.EqualValues(t, 3, sum.ServerCount)
		require.EqualValues(t, 2, sum.AssumedServerCount)
	})
}
