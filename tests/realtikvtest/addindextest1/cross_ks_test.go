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
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestAddIndexOnSystemTable(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("This test is only for nextgen kernel, skip it in classic kernel")
	}

	t.Run("bootstrap system keyspace", func(t *testing.T) {
		realtikvtest.CreateMockStoreAndSetup(t)
	})

	t.Run("submit add index sql on user keyspace", func(t *testing.T) {
		userStore := realtikvtest.CreateMockStoreAndSetup(t,
			realtikvtest.WithKeyspaceName("keyspace1"))
		tk := testkit.NewTestKit(t, userStore)
		tk.MustExec("use test")
		tk.MustExec("create table t (a int, b int);")
		tk.MustExec("insert into t values (1, 2);")

		var ch = make(chan struct{})
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/handle/afterSubmitDXFTask", func() {
			close(ch)
		})
		go func() {
			tk.Exec("alter table t add index idx_a (a);")
		}()
		<-ch
		t.Log("global task submitted")
	})

	t.Run("run add index worker at system keyspace", func(t *testing.T) {
		systemStore := realtikvtest.CreateMockStoreAndSetup(t, realtikvtest.WithRetainData())
		tk := testkit.NewTestKit(t, systemStore)
		tk.MustExec("use test")
		// The table in system keyspace is different from the one in user keyspace.
		tk.MustExec("create table t (a int, b int);")

		rs := tk.MustQuery("select * from mysql.tidb_global_task order by id desc").Rows()
		require.Greater(t, len(rs), 0)

		taskID, err := strconv.Atoi(rs[0][0].(string))
		require.NoError(t, err)
		ctx := kv.WithInternalSourceType(context.Background(), "realtikvtest")
		err = handle.WaitTaskDoneOrPaused(ctx, int64(taskID))
		require.NoError(t, err)
	})

	t.Run("check ddl state at user keyspace", func(t *testing.T) {
		systemStore := realtikvtest.CreateMockStoreAndSetup(t, realtikvtest.WithKeyspaceName("keyspace1"), realtikvtest.WithRetainData())
		tk := testkit.NewTestKit(t, systemStore)
		var jobState string
		require.Eventuallyf(t, func() bool {
			rs := tk.MustQuery("admin show ddl jobs 1;").Rows()
			jobState = rs[0][11].(string)
			return jobState == model.JobStateSynced.String()
		}, 10*time.Second, 200*time.Millisecond, "job state should be done")
		tk.MustExec("use test")
		tk.MustExec("admin check table t;")
	})
}
