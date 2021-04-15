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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl_test

import (
	"context"
	"strconv"

	. "github.com/pingcap/check"
	errors2 "github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = SerialSuites(&testRollingBackSuite{&testDBSuite{}})

type testRollingBackSuite struct{ *testDBSuite }

// TestCancelJobMeetError is used to test canceling ddl job failure when convert ddl job to a rollingback job.
func (s *testRollingBackSuite) TestCancelAddIndexJobError(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk1 := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk1.MustExec("use test")

	tk.MustExec("create table t_cancel_add_index (a int)")
	tk.MustExec("insert into t_cancel_add_index values(1),(2),(3)")
	tk.MustExec("set @@global.tidb_ddl_error_count_limit=3")

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/mockConvertAddIdxJob2RollbackJobError", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/mockConvertAddIdxJob2RollbackJobError"), IsNil)
	}()

	tbl := testGetTableByName(c, tk.Se, "test", "t_cancel_add_index")
	c.Assert(tbl, NotNil)

	d := s.dom.DDL()
	hook := &ddl.TestDDLCallback{Do: s.dom}
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
			chk := res.NewChunk()
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
	_, err := tk.Exec("alter table t_cancel_add_index add index idx(a)")
	c.Assert(err, NotNil)
	c.Assert(checkErr, IsNil)
	c.Assert(err.Error(), Equals, "[ddl:-1]rollback DDL job error count exceed the limit 3, cancelled it now")

	// Verification of the history job state.
	var job *model.Job
	err = kv.RunInNewTxn(context.Background(), s.store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		job, err1 = t.GetHistoryDDLJob(jobID)
		return errors2.Trace(err1)
	})
	c.Assert(err, IsNil)
	c.Assert(job.ErrorCount, Equals, int64(4))
	c.Assert(job.Error.Error(), Equals, "[ddl:-1]rollback DDL job error count exceed the limit 3, cancelled it now")
}
