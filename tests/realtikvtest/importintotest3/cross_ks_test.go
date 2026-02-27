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
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/objstore"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/testkit"
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
