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
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/schemaver"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/meta/model"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/collate"
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

func TestAddIndexOnUserKeyspaceWithDifferentNewCollation(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("This test is only for nextgen kernel, skip it in classic kernel")
	}
	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Experimental.AllowsExpressionIndex = true
	})

	originNewCollationEnabled := collate.NewCollationEnabled()
	t.Cleanup(func() {
		collate.SetNewCollationEnabledForTest(originNewCollationEnabled)
	})

	const userKeyspace = "keyspacecollate"
	runtimes := realtikvtest.PrepareForCrossKSTestWithNewCollation(t, map[string]bool{
		keyspace.System: true,
		userKeyspace:    false,
	}, userKeyspace)
	userStore := runtimes[userKeyspace].Store
	tk := testkit.NewTestKit(t, userStore)
	tk.MustQuery(`select variable_value from mysql.tidb where variable_name = 'new_collation_enabled'`).
		Check(testkit.Rows("False"))
	require.False(t, collate.NewCollationEnabled())

	var backfillInitCnt atomic.Int64
	testfailpoint.Enable(
		t,
		"github.com/pingcap/tidb/pkg/ddl/overrideDefaultUseNewCollateForBackfillStep",
		"return(true)",
	)
	testfailpoint.EnableCall(
		t,
		"github.com/pingcap/tidb/pkg/ddl/afterResolveUserTableNewCollateForBackfillStep",
		func(job *model.Job, defaultUseNewCollate bool, useNewCollate bool) {
			require.False(t, job.ReorgMeta.GetUseNewCollateOrDefault(true))
			require.True(t, defaultUseNewCollate)
			require.False(t, useNewCollate)
			require.False(t, collate.NewCollationEnabled())
			backfillInitCnt.Add(1)
		},
	)

	tk.PrepareDB("crossks_collate")

	cases := []struct {
		name        string
		table       string
		setupSQL    []string
		addIndexSQL []string
		indexes     []string
		dmlSQL      []string
	}{
		{
			name:  "clustered varchar primary key and secondary varchar index",
			table: "t_varchar_pk",
			setupSQL: []string{
				"drop table if exists t_varchar_pk",
				`create table t_varchar_pk (
					id varchar(32) collate utf8mb4_general_ci,
					fk varchar(32) collate utf8mb4_general_ci,
					primary key (id) clustered
				)`,
				"insert into t_varchar_pk values ('aaa', 'abc'), ('bbb', 'bbc'), ('ccc', 'cbc')",
			},
			addIndexSQL: []string{
				"alter table t_varchar_pk add index idx_fk(fk)",
			},
			indexes: []string{"idx_fk"},
			dmlSQL: []string{
				"insert into t_varchar_pk values ('ddd', 'dbc')",
				"update t_varchar_pk set fk = 'updated' where id = 'ddd'",
				"delete from t_varchar_pk where id = 'ddd'",
			},
		},
		{
			name:  "composite clustered primary key with varchar part and secondary int index",
			table: "t_composite_varchar_pk",
			setupSQL: []string{
				"drop table if exists t_composite_varchar_pk",
				`create table t_composite_varchar_pk (
					id1 varchar(32) collate utf8mb4_general_ci,
					id2 int,
					fk int,
					primary key (id1, id2) clustered
				)`,
				"insert into t_composite_varchar_pk values ('ax', 1, 10), ('by', 2, 20), ('cz', 3, 30)",
			},
			addIndexSQL: []string{
				"alter table t_composite_varchar_pk add index idx_fk(fk)",
			},
			indexes: []string{"idx_fk"},
			dmlSQL: []string{
				"insert into t_composite_varchar_pk values ('dw', 4, 40)",
				"update t_composite_varchar_pk set fk = 41 where id1 = 'dw' and id2 = 4",
				"delete from t_composite_varchar_pk where id1 = 'dw' and id2 = 4",
			},
		},
		{
			name:  "generated columns with string transformations",
			table: "t_add_generated_column_index",
			setupSQL: []string{
				"drop table if exists t_add_generated_column_index",
				`create table t_add_generated_column_index (
					id varchar(32) collate utf8mb4_general_ci,
					raw varchar(32) collate utf8mb4_general_ci,
					g_lower varchar(32) generated always as (lower(raw)) virtual,
					g_upper varchar(32) generated always as (upper(raw)) virtual,
					g_concat varchar(80) generated always as (concat(id, ':', raw)) virtual,
					g_substr varchar(32) generated always as (substr(raw, 1, 2)) virtual,
					primary key (id) clustered
				)`,
				"insert into t_add_generated_column_index(id, raw) values ('aaa', 'abc'), ('bbb', 'bbc'), ('ccc', 'cbc')",
			},
			addIndexSQL: []string{
				"alter table t_add_generated_column_index add index idx_g_lower(g_lower)",
				"alter table t_add_generated_column_index add index idx_g_upper(g_upper)",
				"alter table t_add_generated_column_index add index idx_g_concat(g_concat)",
				"alter table t_add_generated_column_index add index idx_g_substr(g_substr)",
			},
			indexes: []string{"idx_g_lower", "idx_g_upper", "idx_g_concat", "idx_g_substr"},
			dmlSQL: []string{
				"insert into t_add_generated_column_index(id, raw) values ('ddd', 'dbc')",
				"update t_add_generated_column_index set raw = 'updated' where id = 'ddd'",
				"delete from t_add_generated_column_index where id = 'ddd'",
			},
		},
		{
			name:  "expression indexes with string transformations",
			table: "t_add_expression_index",
			setupSQL: []string{
				"drop table if exists t_add_expression_index",
				`create table t_add_expression_index (
					id varchar(32) collate utf8mb4_general_ci,
					raw varchar(32) collate utf8mb4_general_ci,
					primary key (id) clustered
				)`,
				"insert into t_add_expression_index values ('aaa', 'abc'), ('bbb', 'bbc'), ('ccc', 'cbc')",
			},
			addIndexSQL: []string{
				"alter table t_add_expression_index add index idx_lower ((lower(raw)))",
				"alter table t_add_expression_index add index idx_upper ((upper(raw)))",
				"alter table t_add_expression_index add index idx_concat ((concat(id, ':', raw)))",
				"alter table t_add_expression_index add index idx_substr ((substr(raw, 1, 2)))",
			},
			indexes: []string{"idx_lower", "idx_upper", "idx_concat", "idx_substr"},
			dmlSQL: []string{
				"insert into t_add_expression_index values ('ddd', 'dbc')",
				"update t_add_expression_index set raw = 'updated' where id = 'ddd'",
				"delete from t_add_expression_index where id = 'ddd'",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			collate.SetNewCollationEnabledForTest(false)
			for _, sql := range tc.setupSQL {
				tk.MustExec(sql)
			}
			for _, sql := range tc.addIndexSQL {
				before := backfillInitCnt.Load()
				collate.SetNewCollationEnabledForTest(false)
				tk.MustExec(sql)
				require.Greater(t, backfillInitCnt.Load(), before)
				collate.SetNewCollationEnabledForTest(false)
			}
			checkTableAndIndexes(tk, tc.table, tc.indexes, "3")
			for _, sql := range tc.dmlSQL {
				tk.MustExec(sql)
			}
			checkTableAndIndexes(tk, tc.table, tc.indexes, "3")
		})
	}
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

func checkTableAndIndexes(tk *testkit.TestKit, tableName string, indexes []string, expectedCount string) {
	tk.MustExec("admin check table " + tableName)
	tk.MustQuery("select count(*) from " + tableName).Check(testkit.Rows(expectedCount))
	for _, indexName := range indexes {
		tk.MustQuery(fmt.Sprintf("select count(*) from %s force index(%s)", tableName, indexName)).
			Check(testkit.Rows(expectedCount))
	}
}
