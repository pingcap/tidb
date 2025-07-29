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
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

// to run this test, you need to start a SYSTEM KS TiDB first.
func TestOnUserKeyspace(t *testing.T) {
	t.Skip("we can only run it manually now, will enable it later")
	if kerneltype.IsClassic() {
		t.Skip("only runs in nextgen kernel")
	}
	userStore := realtikvtest.CreateMockStoreAndSetup(t,
		realtikvtest.WithKeyspaceName("cross_ks"))
	userTK := testkit.NewTestKit(t, userStore)
	prepareAndUseDB("cross_ks", userTK)
	userTK.MustExec("create table t (a bigint, b varchar(100));")
	// currently, we cannot checksum across keyspace, skip it
	// TODO enable it
	// TODO we need upload file the S3, not a fixed file name and S3 address.
	importSQL := `import into t FROM 's3://mybucket/a.csv?access-key=minioadmin&secret-access-key=minioadmin&endpoint=http%3a%2f%2f0.0.0.0%3a9000' with checksum_table='off'`
	result := userTK.MustQuery(importSQL).Rows()
	require.Len(t, result, 1)
	jobID, err := strconv.Atoi(result[0][0].(string))
	require.NoError(t, err)
	taskKey := importinto.TaskKey(int64(jobID))
	// job to user keyspace, task to system keyspace
	sysKSTk := testkit.NewTestKit(t, kvstore.GetSystemStorage())
	userTK.MustQuery("select count(1) from mysql.tidb_import_jobs where id = ?", jobID).Check(testkit.Rows("1"))
	querySQL := `select sum(c) from (select count(1) c from mysql.tidb_global_task where task_key=?
		union select count(1) c from mysql.tidb_global_task_history where task_key=?) t`
	sysKSTk.MustQuery(querySQL, taskKey, taskKey).Check(testkit.Rows("1"))
	// reverse check
	sysKSTk.MustQuery("select count(1) from mysql.tidb_import_jobs where id = ?", jobID).Check(testkit.Rows("0"))
	userTK.MustQuery(querySQL, taskKey, taskKey).Check(testkit.Rows("0"))
}
