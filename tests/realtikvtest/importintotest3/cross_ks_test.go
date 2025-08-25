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
	"testing"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestOnUserKeyspace(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("only runs in nextgen kernel")
	}
	runtimes := realtikvtest.PrepareForCrossKSTest(t, "keyspace1")
	userStore := runtimes["keyspace1"].Store
	userTK := testkit.NewTestKit(t, userStore)
	prepareAndUseDB("cross_ks", userTK)
	userTK.MustExec("drop table if exists t;")
	userTK.MustExec("create table t (a bigint, b varchar(100));")
	ctx := context.Background()
	s3Args := "access-key=minioadmin&secret-access-key=minioadmin&endpoint=http%3a%2f%2f0.0.0.0%3a9000"
	objStore, err := storage.NewFromURL(ctx, fmt.Sprintf("s3://next-gen-test/data?%s", s3Args))
	require.NoError(t, err)
	t.Cleanup(func() {
		objStore.Close()
	})
	require.NoError(t, objStore.WriteFile(ctx, "a.csv", []byte("1,1")))
	importSQL := fmt.Sprintf(`import into t FROM 's3://next-gen-test/data/a.csv?%s'`, s3Args)
	result := userTK.MustQuery(importSQL).Rows()
	require.Len(t, result, 1)
	jobID, err := strconv.Atoi(result[0][0].(string))
	require.NoError(t, err)
	userTK.MustQuery("select * from t").Check(testkit.Rows("1 1"))
	taskKey := importinto.TaskKey(int64(jobID))
	// job to user keyspace, task to system keyspace
	sysKSTk := testkit.NewTestKit(t, kvstore.GetSystemStorage())
	jobQuerySQL := fmt.Sprintf("select count(1) from mysql.tidb_import_jobs where id = %d", jobID)
	taskQuerySQL := fmt.Sprintf(`select sum(c) from (select count(1) c from mysql.tidb_global_task where task_key='%s'
		union select count(1) c from mysql.tidb_global_task_history where task_key='%s') t`, taskKey, taskKey)
	userTK.MustQuery(jobQuerySQL).Check(testkit.Rows("1"))
	sysKSTk.MustQuery(taskQuerySQL).Check(testkit.Rows("1"))
	// reverse check
	sysKSTk.MustQuery(jobQuerySQL).Check(testkit.Rows("0"))
	userTK.MustQuery(taskQuerySQL).Check(testkit.Rows("0"))

	// Check the summary from user keyspace is correct, which is get from subtask summaries.
	rs := userTK.MustQuery(fmt.Sprintf("select summary from mysql.tidb_import_jobs where id = %d", jobID)).Rows()
	require.Len(t, rs, 1)
	summary := &importer.Summary{}
	require.NoError(t, json.Unmarshal([]byte(rs[0][0].(string)), summary))
	require.EqualValues(t, 1, summary.ImportedRows)
}
