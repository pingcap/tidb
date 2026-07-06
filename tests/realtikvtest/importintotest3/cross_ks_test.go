// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importintotest

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/objstore"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

var fmap = plannercore.ImportIntoFieldMap

func TestOnUserKeyspace(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("only runs in nextgen kernel")
	}
	bak := vardef.GetStatsLease()
	t.Cleanup(func() {
		vardef.SetStatsLease(bak)
	})
	vardef.SetStatsLease(time.Second)
	runtimes := realtikvtest.PrepareForCrossKSTest(t, "keyspace1")
	userStore := runtimes["keyspace1"].Store
	userTK := testkit.NewTestKit(t, userStore)
	prepareAndUseDB("cross_ks", userTK)
	userTK.MustExec("drop table if exists t;")
	userTK.MustExec("create table t (a bigint, b varchar(100));")
	ctx := context.Background()
	s3Args := "access-key=minioadmin&secret-access-key=minioadmin&endpoint=http%3a%2f%2f0.0.0.0%3a9000"
	objStore, err := objstore.NewFromURL(ctx, fmt.Sprintf("s3://next-gen-test/data?%s", s3Args))
	require.NoError(t, err)
	t.Cleanup(func() {
		objStore.Close()
	})
	var (
		contentSB   strings.Builder
		resultSlice = make([]string, 0, 1000)
	)
	rowCount := 1000
	for i := 0; i < rowCount; i++ {
		contentSB.WriteString(fmt.Sprintf("%d,%d\n", i, i))
		resultSlice = append(resultSlice, fmt.Sprintf("%d %d", i, i))
	}
	require.NoError(t, objStore.WriteFile(ctx, "a.csv", []byte(contentSB.String())))
	importSQL := fmt.Sprintf(`import into t FROM 's3://next-gen-test/data/a.csv?%s'`, s3Args)
	result := userTK.MustQuery(importSQL).Rows()
	require.Len(t, result, 1)
	jobID, err := strconv.Atoi(result[0][0].(string))
	require.NoError(t, err)
	userTK.MustQuery("select * from t").Check(testkit.Rows(resultSlice...))
	taskKey := importinto.TaskKey(int64(jobID))
	tableID, err := strconv.Atoi(result[0][fmap["TableID"]].(string))
	require.NoError(t, err)
	// job to user keyspace, task to system keyspace
	sysKSTk := testkit.NewTestKit(t, kvstore.GetSystemStorage())
	jobQuerySQL := fmt.Sprintf("select count(1) from mysql.tidb_import_jobs where id = %d and table_id=%d and table_schema='%s'", jobID, tableID, "cross_ks")
	taskQuerySQL := fmt.Sprintf(`select id from (select id from mysql.tidb_global_task where task_key='%s'
		union select id from mysql.tidb_global_task_history where task_key='%s') t`, taskKey, taskKey)
	require.Len(t, userTK.MustQuery(jobQuerySQL).Rows(), 1)
	rs := sysKSTk.MustQuery(taskQuerySQL).Rows()
	require.Len(t, rs, 1)

	// when intest, auto analyze is disabled by default, we enable it here.
	bakRunAutoAnalyze := vardef.RunAutoAnalyze.Load()
	userTK.MustExec("set global tidb_enable_auto_analyze=true")
	t.Cleanup(func() {
		userTK.MustExec(fmt.Sprintf("set global tidb_enable_auto_analyze=%t", bakRunAutoAnalyze))
	})
	// check table stats
	require.Eventually(t, func() bool {
		r := userTK.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta where table_id=%d", tableID)).Rows()
		require.Len(t, r, 1)
		modified, err := strconv.Atoi(r[0][0].(string))
		require.NoError(t, err)
		rows, err := strconv.Atoi(r[0][1].(string))
		require.NoError(t, err)
		// import into will update both modify_count and count to rowCount, after
		// auto analyze, modify_count will be set to 0
		return modified == 0 && rows == rowCount
	}, 30*time.Second, 100*time.Millisecond, "stats meta not updated after import into")

	// Check subtask summary from system keyspace is correct.
	taskID := rs[0][0].(string)
	subtaskQuery := fmt.Sprintf(`select summary from (select summary from mysql.tidb_background_subtask where task_key='%s' and step = 1
		union select summary from mysql.tidb_background_subtask_history where task_key='%s' and step = 1) t`, taskID, taskID)
	rs = sysKSTk.MustQuery(subtaskQuery).Rows()
	require.Len(t, rs, 1)
	subtaskSummary := &execute.SubtaskSummary{}
	require.NoError(t, json.Unmarshal([]byte(rs[0][0].(string)), subtaskSummary))
	require.EqualValues(t, rowCount, subtaskSummary.RowCnt.Load())

	// reverse check
	sysKSTk.MustQuery(jobQuerySQL).Check(testkit.Rows("0"))
	require.Len(t, userTK.MustQuery(taskQuerySQL).Rows(), 0)

	// Check the job summary from user keyspace is correct, which is get from subtask summaries.
	rs = userTK.MustQuery(fmt.Sprintf("select summary from mysql.tidb_import_jobs where id = %d", jobID)).Rows()
	require.Len(t, rs, 1)
	summary := &importer.Summary{}
	require.NoError(t, json.Unmarshal([]byte(rs[0][0].(string)), summary))
	require.EqualValues(t, rowCount, summary.ImportedRows)
}

func TestImportIntoOnUserKeyspaceWithDifferentNewCollation(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("only runs in nextgen kernel")
	}

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
	userTK := testkit.NewTestKit(t, userStore)
	userTK.MustQuery(`select variable_value from mysql.tidb where variable_name = 'new_collation_enabled'`).
		Check(testkit.Rows("False"))
	require.False(t, collate.NewCollationEnabled())

	ctx := context.Background()
	s3Args := "access-key=minioadmin&secret-access-key=minioadmin&endpoint=http%3a%2f%2f0.0.0.0%3a9000"
	objStore, err := objstore.NewFromURL(ctx, fmt.Sprintf("s3://next-gen-test/collate-data?%s", s3Args))
	require.NoError(t, err)
	t.Cleanup(func() {
		objStore.Close()
	})

	var taskSubmitCnt atomic.Int64
	var taskRefreshCnt atomic.Int64
	testfailpoint.EnableCall(
		t,
		"github.com/pingcap/tidb/pkg/dxf/framework/storage/beforeSubmitTask",
		func(*int, *proto.ExtraParams) {
			collate.SetNewCollationEnabledForTest(true)
			taskSubmitCnt.Add(1)
		},
	)
	testfailpoint.EnableCall(
		t,
		"github.com/pingcap/tidb/pkg/dxf/framework/scheduler/afterRefreshTask",
		func(task *proto.Task) {
			if task == nil || task.Type != proto.ImportInto || !strings.HasPrefix(task.Key, userKeyspace+"/ImportInto/") {
				return
			}
			var taskMeta importinto.TaskMeta
			require.NoError(t, json.Unmarshal(task.Meta, &taskMeta))
			require.NotNil(t, taskMeta.Plan.UseNewCollate)
			require.False(t, *taskMeta.Plan.UseNewCollate)
			taskRefreshCnt.Add(1)
		},
	)

	prepareAndUseDB("cross_ks_collate", userTK)

	cases := []struct {
		name       string
		table      string
		setupSQL   []string
		fileName   string
		fileData   string
		importSQL  string
		indexes    []string
		postDMLSQL []string
	}{
		{
			name:  "clustered varchar primary key and secondary varchar index",
			table: "trigger_varchar_pk_varchar_idx",
			setupSQL: []string{
				"drop table if exists trigger_varchar_pk_varchar_idx",
				`create table trigger_varchar_pk_varchar_idx (
					id varchar(32) collate utf8mb4_general_ci,
					fk varchar(32) collate utf8mb4_general_ci,
					primary key (id) clustered,
					key idx_fk (fk)
				)`,
			},
			fileName:  "trigger_varchar_pk_varchar_idx.csv",
			fileData:  "x,aaa,abc\nx,bbb,bbc\nx,ccc,cbc\n",
			importSQL: "import into trigger_varchar_pk_varchar_idx(@1,id,fk) from '%s'",
			indexes:   []string{"idx_fk"},
			postDMLSQL: []string{
				"insert into trigger_varchar_pk_varchar_idx values ('ddd', 'dbc')",
				"update trigger_varchar_pk_varchar_idx set fk = 'updated' where id = 'ddd'",
				"delete from trigger_varchar_pk_varchar_idx where id = 'ddd'",
			},
		},
		{
			name:  "clustered varchar primary key and secondary int index",
			table: "trigger_varchar_pk_int_idx",
			setupSQL: []string{
				"drop table if exists trigger_varchar_pk_int_idx",
				`create table trigger_varchar_pk_int_idx (
					id varchar(32) collate utf8mb4_general_ci,
					fk int,
					primary key (id) clustered,
					key idx_fk (fk)
				)`,
			},
			fileName:  "trigger_varchar_pk_int_idx.csv",
			fileData:  "10,aaa,x\n20,bbb,x\n30,ccc,x\n",
			importSQL: "import into trigger_varchar_pk_int_idx(fk,id,@3) from '%s'",
			indexes:   []string{"idx_fk"},
			postDMLSQL: []string{
				"insert into trigger_varchar_pk_int_idx values ('ddd', 40)",
				"update trigger_varchar_pk_int_idx set fk = 41 where id = 'ddd'",
				"delete from trigger_varchar_pk_int_idx where id = 'ddd'",
			},
		},
		{
			name:  "composite clustered primary key with varchar part and secondary int index",
			table: "trigger_composite_varchar_int_pk_int_idx",
			setupSQL: []string{
				"drop table if exists trigger_composite_varchar_int_pk_int_idx",
				`create table trigger_composite_varchar_int_pk_int_idx (
					id1 varchar(32) collate utf8mb4_general_ci,
					id2 int,
					fk int,
					primary key (id1, id2) clustered,
					key idx_fk (fk)
				)`,
			},
			fileName:  "trigger_composite_varchar_int_pk_int_idx.csv",
			fileData:  "1,10,aaa\n2,20,bbb\n3,30,ccc\n",
			importSQL: "import into trigger_composite_varchar_int_pk_int_idx(id2,fk,id1) from '%s'",
			indexes:   []string{"idx_fk"},
			postDMLSQL: []string{
				"insert into trigger_composite_varchar_int_pk_int_idx values ('ddd', 4, 40)",
				"update trigger_composite_varchar_int_pk_int_idx set fk = 41 where id1 = 'ddd' and id2 = 4",
				"delete from trigger_composite_varchar_int_pk_int_idx where id1 = 'ddd' and id2 = 4",
			},
		},
		{
			name:  "composite clustered int primary key and secondary varchar index",
			table: "trigger_composite_int_int_pk_varchar_idx",
			setupSQL: []string{
				"drop table if exists trigger_composite_int_int_pk_varchar_idx",
				`create table trigger_composite_int_int_pk_varchar_idx (
					id1 int,
					id2 int,
					fk varchar(32) collate utf8mb4_general_ci,
					primary key (id1, id2) clustered,
					key idx_fk (fk)
				)`,
			},
			fileName:  "trigger_composite_int_int_pk_varchar_idx.csv",
			fileData:  "1,10,aaa\n2,20,bbb\n3,30,ccc\n",
			importSQL: "import into trigger_composite_int_int_pk_varchar_idx(id1,id2,fk) from '%s'",
			indexes:   []string{"idx_fk"},
			postDMLSQL: []string{
				"insert into trigger_composite_int_int_pk_varchar_idx values (4, 40, 'ddd')",
				"update trigger_composite_int_int_pk_varchar_idx set fk = 'updated' where id1 = 4 and id2 = 40",
				"delete from trigger_composite_int_int_pk_varchar_idx where id1 = 4 and id2 = 40",
			},
		},
		{
			name:  "composite char primary key and secondary varchar index",
			table: "trigger_composite_char_char_pk_varchar_idx",
			setupSQL: []string{
				"drop table if exists trigger_composite_char_char_pk_varchar_idx",
				`create table trigger_composite_char_char_pk_varchar_idx (
					id1 char(32) collate utf8mb4_general_ci,
					id2 char(32) collate utf8mb4_general_ci,
					fk varchar(32) collate utf8mb4_general_ci,
					primary key (id1, id2) clustered,
					key idx_fk (fk)
				)`,
			},
			fileName:  "trigger_composite_char_char_pk_varchar_idx.csv",
			fileData:  "aaa,ax,ay\nbbb,bx,by\nccc,cx,cy\n",
			importSQL: "import into trigger_composite_char_char_pk_varchar_idx(fk,id1,id2) from '%s'",
			indexes:   []string{"idx_fk"},
			postDMLSQL: []string{
				"insert into trigger_composite_char_char_pk_varchar_idx values ('dx', 'dy', 'ddd')",
				"update trigger_composite_char_char_pk_varchar_idx set fk = 'updated' where id1 = 'dx' and id2 = 'dy'",
				"delete from trigger_composite_char_char_pk_varchar_idx where id1 = 'dx' and id2 = 'dy'",
			},
		},
		{
			name:  "clustered varchar primary key and prefix secondary varchar index",
			table: "trigger_varchar_pk_prefix_varchar_idx",
			setupSQL: []string{
				"drop table if exists trigger_varchar_pk_prefix_varchar_idx",
				`create table trigger_varchar_pk_prefix_varchar_idx (
					id varchar(32) collate utf8mb4_general_ci,
					fk varchar(32) collate utf8mb4_general_ci,
					primary key (id) clustered,
					key idx_fk_prefix (fk(2))
				)`,
			},
			fileName:  "trigger_varchar_pk_prefix_varchar_idx.csv",
			fileData:  "x,aaa,abc\nx,bbb,bbc\nx,ccc,cbc\n",
			importSQL: "import into trigger_varchar_pk_prefix_varchar_idx(@1,id,fk) from '%s'",
			indexes:   []string{"idx_fk_prefix"},
			postDMLSQL: []string{
				"insert into trigger_varchar_pk_prefix_varchar_idx values ('ddd', 'dbc')",
				"update trigger_varchar_pk_prefix_varchar_idx set fk = 'updated' where id = 'ddd'",
				"delete from trigger_varchar_pk_prefix_varchar_idx where id = 'ddd'",
			},
		},
		{
			name:  "clustered varchar primary key and secondary varchar index with extra payload",
			table: "trigger_record_ok_index_bad_extra_payload",
			setupSQL: []string{
				"drop table if exists trigger_record_ok_index_bad_extra_payload",
				`create table trigger_record_ok_index_bad_extra_payload (
					id varchar(32) collate utf8mb4_general_ci,
					fk varchar(32) collate utf8mb4_general_ci,
					payload varchar(32) collate utf8mb4_general_ci default 'payload',
					primary key (id) clustered,
					key idx_fk (fk)
				)`,
			},
			fileName:  "trigger_record_ok_index_bad_extra_payload.csv",
			fileData:  "x,aaa,abc\nx,bbb,bbc\nx,ccc,cbc\n",
			importSQL: "import into trigger_record_ok_index_bad_extra_payload(@1,id,fk) from '%s'",
			indexes:   []string{"idx_fk"},
			postDMLSQL: []string{
				"insert into trigger_record_ok_index_bad_extra_payload(id, fk) values ('ddd', 'dbc')",
				"update trigger_record_ok_index_bad_extra_payload set fk = 'updated' where id = 'ddd'",
				"delete from trigger_record_ok_index_bad_extra_payload where id = 'ddd'",
			},
		},
		{
			name:  "generated columns with string transformations",
			table: "t_import_generated",
			setupSQL: []string{
				"drop table if exists t_import_generated",
				`create table t_import_generated (
					id varchar(32) collate utf8mb4_general_ci,
					raw varchar(32) collate utf8mb4_general_ci,
					g_lower varchar(32) generated always as (lower(raw)) stored,
					g_upper varchar(32) generated always as (upper(raw)) stored,
					g_concat varchar(80) generated always as (concat(id, ':', raw)) stored,
					g_substr varchar(32) generated always as (substr(raw, 1, 2)) stored,
					primary key (id) clustered,
					key idx_lower (g_lower),
					key idx_upper (g_upper),
					key idx_concat (g_concat),
					key idx_substr (g_substr)
				)`,
			},
			fileName:  "t_import_generated.csv",
			fileData:  "x,aaa,abc\nx,bbb,bbc\nx,ccc,cbc\n",
			importSQL: "import into t_import_generated(@1,id,raw) from '%s'",
			indexes:   []string{"idx_lower", "idx_upper", "idx_concat", "idx_substr"},
			postDMLSQL: []string{
				"insert into t_import_generated(id, raw) values ('ddd', 'dbc')",
				"update t_import_generated set raw = 'updated' where id = 'ddd'",
				"delete from t_import_generated where id = 'ddd'",
			},
		},
		{
			name:  "assignment expressions with string transformations",
			table: "t_import_assignment",
			setupSQL: []string{
				"drop table if exists t_import_assignment",
				`create table t_import_assignment (
					id varchar(32) collate utf8mb4_general_ci,
					raw varchar(32) collate utf8mb4_general_ci,
					a_lower varchar(32) collate utf8mb4_general_ci,
					a_upper varchar(32) collate utf8mb4_general_ci,
					a_concat varchar(80) collate utf8mb4_general_ci,
					a_substr varchar(32) collate utf8mb4_general_ci,
					primary key (id) clustered,
					key idx_lower (a_lower),
					key idx_upper (a_upper),
					key idx_concat (a_concat),
					key idx_substr (a_substr)
				)`,
			},
			fileName: "t_import_assignment.csv",
			fileData: "x,aaa,abc\nx,bbb,bbc\nx,ccc,cbc\n",
			importSQL: `import into t_import_assignment(@1,@2,@3)
				set id=@2,
				    raw=@3,
				    a_lower=lower(@3),
				    a_upper=upper(@3),
				    a_concat=concat(@2, ':', @3),
				    a_substr=substr(@3, 1, 2)
				from '%s'`,
			indexes: []string{"idx_lower", "idx_upper", "idx_concat", "idx_substr"},
			postDMLSQL: []string{
				`insert into t_import_assignment
					values ('ddd', 'dbc', lower('dbc'), upper('dbc'), concat('ddd', ':', 'dbc'), substr('dbc', 1, 2))`,
				`update t_import_assignment
					set raw = 'updated',
					    a_lower = lower('updated'),
					    a_upper = upper('updated'),
					    a_concat = concat(id, ':', 'updated'),
					    a_substr = substr('updated', 1, 2)
					where id = 'ddd'`,
				"delete from t_import_assignment where id = 'ddd'",
			},
		},
		{
			name:  "control int primary key and varchar secondary index",
			table: "ok_int_pk_varchar_idx",
			setupSQL: []string{
				"drop table if exists ok_int_pk_varchar_idx",
				`create table ok_int_pk_varchar_idx (
					id int,
					fk varchar(32) collate utf8mb4_general_ci,
					primary key (id) clustered,
					key idx_fk (fk)
				)`,
			},
			fileName:  "ok_int_pk_varchar_idx.csv",
			fileData:  "1,abc\n2,bbc\n3,cbc\n",
			importSQL: "import into ok_int_pk_varchar_idx(id,fk) from '%s'",
			indexes:   []string{"idx_fk"},
			postDMLSQL: []string{
				"insert into ok_int_pk_varchar_idx values (4, 'dbc')",
				"update ok_int_pk_varchar_idx set fk = 'updated' where id = 4",
				"delete from ok_int_pk_varchar_idx where id = 4",
			},
		},
		{
			name:  "control varchar primary key without secondary index",
			table: "ok_varchar_pk_no_secondary",
			setupSQL: []string{
				"drop table if exists ok_varchar_pk_no_secondary",
				`create table ok_varchar_pk_no_secondary (
					id varchar(32) collate utf8mb4_general_ci,
					v int,
					primary key (id) clustered
				)`,
			},
			fileName:  "ok_varchar_pk_no_secondary.csv",
			fileData:  "aaa,1\nbbb,2\nccc,3\n",
			importSQL: "import into ok_varchar_pk_no_secondary(id,v) from '%s'",
			postDMLSQL: []string{
				"insert into ok_varchar_pk_no_secondary values ('ddd', 4)",
				"update ok_varchar_pk_no_secondary set v = 5 where id = 'ddd'",
				"delete from ok_varchar_pk_no_secondary where id = 'ddd'",
			},
		},
		{
			name:  "control nonclustered varchar primary key and varchar secondary index",
			table: "ok_nonclustered_varchar_pk_varchar_idx",
			setupSQL: []string{
				"drop table if exists ok_nonclustered_varchar_pk_varchar_idx",
				`create table ok_nonclustered_varchar_pk_varchar_idx (
					id varchar(32) collate utf8mb4_general_ci,
					fk varchar(32) collate utf8mb4_general_ci,
					primary key (id) nonclustered,
					key idx_fk (fk)
				)`,
			},
			fileName:  "ok_nonclustered_varchar_pk_varchar_idx.csv",
			fileData:  "aaa,abc\nbbb,bbc\nccc,cbc\n",
			importSQL: "import into ok_nonclustered_varchar_pk_varchar_idx(id,fk) from '%s'",
			indexes:   []string{"idx_fk"},
			postDMLSQL: []string{
				"insert into ok_nonclustered_varchar_pk_varchar_idx values ('ddd', 'dbc')",
				"update ok_nonclustered_varchar_pk_varchar_idx set fk = 'updated' where id = 'ddd'",
				"delete from ok_nonclustered_varchar_pk_varchar_idx where id = 'ddd'",
			},
		},
		{
			name:  "control char primary key and char secondary index",
			table: "ok_char_pk_char_idx",
			setupSQL: []string{
				"drop table if exists ok_char_pk_char_idx",
				`create table ok_char_pk_char_idx (
					id char(32) collate utf8mb4_general_ci,
					fk char(32) collate utf8mb4_general_ci,
					primary key (id) clustered,
					key idx_fk (fk)
				)`,
			},
			fileName:  "ok_char_pk_char_idx.csv",
			fileData:  "aaa,abc\nbbb,bbc\nccc,cbc\n",
			importSQL: "import into ok_char_pk_char_idx(id,fk) from '%s'",
			indexes:   []string{"idx_fk"},
			postDMLSQL: []string{
				"insert into ok_char_pk_char_idx values ('ddd', 'dbc')",
				"update ok_char_pk_char_idx set fk = 'updated' where id = 'ddd'",
				"delete from ok_char_pk_char_idx where id = 'ddd'",
			},
		},
		{
			name:  "control composite int primary key and int secondary index",
			table: "ok_composite_int_int_pk_int_idx",
			setupSQL: []string{
				"drop table if exists ok_composite_int_int_pk_int_idx",
				`create table ok_composite_int_int_pk_int_idx (
					id1 int,
					id2 int,
					fk int,
					primary key (id1, id2) clustered,
					key idx_fk (fk)
				)`,
			},
			fileName:  "ok_composite_int_int_pk_int_idx.csv",
			fileData:  "1,10,100\n2,20,200\n3,30,300\n",
			importSQL: "import into ok_composite_int_int_pk_int_idx(id1,id2,fk) from '%s'",
			indexes:   []string{"idx_fk"},
			postDMLSQL: []string{
				"insert into ok_composite_int_int_pk_int_idx values (4, 40, 400)",
				"update ok_composite_int_int_pk_int_idx set fk = 401 where id1 = 4 and id2 = 40",
				"delete from ok_composite_int_int_pk_int_idx where id1 = 4 and id2 = 40",
			},
		},
		{
			name:  "control composite varbinary int primary key and int secondary index",
			table: "ok_composite_varbinary_int_pk_int_idx",
			setupSQL: []string{
				"drop table if exists ok_composite_varbinary_int_pk_int_idx",
				`create table ok_composite_varbinary_int_pk_int_idx (
					id1 varbinary(32),
					id2 int,
					fk int,
					primary key (id1, id2) clustered,
					key idx_fk (fk)
				)`,
			},
			fileName:  "ok_composite_varbinary_int_pk_int_idx.csv",
			fileData:  "aaa,1,10\nbbb,2,20\nccc,3,30\n",
			importSQL: "import into ok_composite_varbinary_int_pk_int_idx(id1,id2,fk) from '%s'",
			indexes:   []string{"idx_fk"},
			postDMLSQL: []string{
				"insert into ok_composite_varbinary_int_pk_int_idx values ('ddd', 4, 40)",
				"update ok_composite_varbinary_int_pk_int_idx set fk = 41 where id1 = 'ddd' and id2 = 4",
				"delete from ok_composite_varbinary_int_pk_int_idx where id1 = 'ddd' and id2 = 4",
			},
		},
		{
			name:  "control composite char primary key and int secondary index",
			table: "ok_composite_char_char_pk_int_idx",
			setupSQL: []string{
				"drop table if exists ok_composite_char_char_pk_int_idx",
				`create table ok_composite_char_char_pk_int_idx (
					id1 char(32) collate utf8mb4_general_ci,
					id2 char(32) collate utf8mb4_general_ci,
					fk int,
					primary key (id1, id2) clustered,
					key idx_fk (fk)
				)`,
			},
			fileName:  "ok_composite_char_char_pk_int_idx.csv",
			fileData:  "aaa,ax,10\nbbb,bx,20\nccc,cx,30\n",
			importSQL: "import into ok_composite_char_char_pk_int_idx(id1,id2,fk) from '%s'",
			indexes:   []string{"idx_fk"},
			postDMLSQL: []string{
				"insert into ok_composite_char_char_pk_int_idx values ('ddd', 'dx', 40)",
				"update ok_composite_char_char_pk_int_idx set fk = 41 where id1 = 'ddd' and id2 = 'dx'",
				"delete from ok_composite_char_char_pk_int_idx where id1 = 'ddd' and id2 = 'dx'",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			collate.SetNewCollationEnabledForTest(false)
			for _, sql := range tc.setupSQL {
				userTK.MustExec(sql)
			}
			require.NoError(t, objStore.WriteFile(ctx, tc.fileName, []byte(tc.fileData)))
			beforeSubmit := taskSubmitCnt.Load()
			before := taskRefreshCnt.Load()
			fileURL := fmt.Sprintf("s3://next-gen-test/collate-data/%s?%s", tc.fileName, s3Args)
			result := userTK.MustQuery(fmt.Sprintf(tc.importSQL, fileURL)).Rows()
			require.Len(t, result, 1)
			require.Greater(t, taskSubmitCnt.Load(), beforeSubmit)
			require.Greater(t, taskRefreshCnt.Load(), before)

			collate.SetNewCollationEnabledForTest(false)
			checkImportTableAndIndexes(userTK, tc.table, tc.indexes, "3")
			for _, sql := range tc.postDMLSQL {
				userTK.MustExec(sql)
			}
			checkImportTableAndIndexes(userTK, tc.table, tc.indexes, "3")
		})
	}
}

func checkImportTableAndIndexes(tk *testkit.TestKit, tableName string, indexes []string, expectedCount string) {
	tk.MustExec("admin check table " + tableName)
	tk.MustQuery("select count(*) from " + tableName).Check(testkit.Rows(expectedCount))
	for _, indexName := range indexes {
		tk.MustQuery(fmt.Sprintf("select count(*) from %s force index(%s)", tableName, indexName)).
			Check(testkit.Rows(expectedCount))
	}
}
