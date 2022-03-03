// Copyright 2021 PingCAP, Inc.
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

package ddl_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/stretchr/testify/require"
)

// TestCancelJobMeetError is used to test canceling ddl job failure when convert ddl job to a rolling back job.
func TestCancelAddIndexJobError(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk1 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1.MustExec("use test")

	tk.MustExec("create table t_cancel_add_index (a int)")
	tk.MustExec("insert into t_cancel_add_index values(1),(2),(3)")
	tk.MustExec("set @@global.tidb_ddl_error_count_limit=3")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockConvertAddIdxJob2RollbackJobError", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockConvertAddIdxJob2RollbackJobError"))
	}()

	tbl := tk.GetTableByName("test", "t_cancel_add_index")
	require.NotNil(t, tbl)

	d := dom.DDL()
	hook := &ddl.TestDDLCallback{Do: dom}
	var (
		checkErr error
		jobID    int64
		res      sqlexec.RecordSet
	)
	hook.OnJobUpdatedExported = func(job *model.Job) {
		if job.TableID != tbl.Meta().ID {
			return
		}
		if job.Type != model.ActionAddIndex {
			return
		}
		if job.SchemaState == model.StateDeleteOnly {
			jobID = job.ID
			res, checkErr = tk1.Exec("admin cancel ddl jobs " + strconv.Itoa(int(job.ID)))
			// drain the result set here, otherwise the cancel action won't take effect immediately.
			chk := res.NewChunk(nil)
			if err := res.Next(context.Background(), chk); err != nil {
				checkErr = err
				return
			}
			if err := res.Close(); err != nil {
				checkErr = err
			}
		}
	}
	d.(ddl.DDLForTest).SetHook(hook)

	// This will hang on stateDeleteOnly, and the job will be canceled.
	err := tk.ExecToErr("alter table t_cancel_add_index add index idx(a)")
	require.NoError(t, checkErr)
	require.EqualError(t, err, "[ddl:-1]rollback DDL job error count exceed the limit 3, cancelled it now")

	// Verification of the history job state.
	var job *model.Job
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		var err1 error
		job, err1 = m.GetHistoryDDLJob(jobID)
		return errors.Trace(err1)
	})
	require.NoError(t, err)
	require.Equal(t, int64(4), job.ErrorCount)
	require.EqualError(t, job.Error, "[ddl:-1]rollback DDL job error count exceed the limit 3, cancelled it now")
}
