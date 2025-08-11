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
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestAddIndexOnSystemTable(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("This test is only for nextgen kernel, skip it in classic kernel")
	}

	// bootstrap SYSTEM and user keyspace
	realtikvtest.CreateMockStoreAndSetup(t)
	userStore := realtikvtest.CreateMockStoreAndSetup(t,
		realtikvtest.WithKeyspaceName("keyspace1"), realtikvtest.WithKeepSystemStore(true))

	// submit add index sql on user keyspace
	tk := testkit.NewTestKit(t, userStore)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t values (1, 2);")
	tk.MustExec("alter table t add index idx_a (a);")
	tk.MustExec("admin check table t;")
	rs := tk.MustQuery("admin show ddl jobs 1;").Rows()
	jobIDStr := rs[0][0].(string)
	jobID, err := strconv.Atoi(jobIDStr)
	require.NoError(t, err)
	taskKey := ddl.TaskKey(int64(jobID))

	// job to user keyspace, task to system keyspace
	sysKSTk := testkit.NewTestKit(t, kvstore.GetSystemStorage())
	taskQuerySQL := fmt.Sprintf(`select sum(c) from (select count(1) c from mysql.tidb_global_task where task_key='%s'
		union select count(1) c from mysql.tidb_global_task_history where task_key='%s') t`, taskKey, taskKey)
	sysKSTk.MustQuery(taskQuerySQL).Check(testkit.Rows("1"))
	// reverse check
	tk.MustQuery(taskQuerySQL).Check(testkit.Rows("0"))
}
