// Copyright 2023 PingCAP, Inc.
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

package adminpause

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestPauseOnWriteConflict(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk1.MustExec("use test")

	tk1.MustExec("create table t(id int)")

	var pauseErr error
	var pauseRS []sqlexec.RecordSet
	hook := &callback.TestDDLCallback{Do: dom}
	d := dom.DDL()
	originalHook := d.GetHook()
	defer d.SetHook(originalHook)

	var adminMutex sync.RWMutex

	jobID := atomic.NewInt64(0)
	// Test when pause cannot be retried and adding index succeeds.
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		adminMutex.Lock()
		if job.Type == model.ActionAddIndex && job.State == model.JobStateRunning &&
			job.SchemaState == model.StateWriteReorganization {
			require.NoError(t, failpoint.Enable(
				"github.com/pingcap/tidb/pkg/ddl/mockFailedCommandOnConcurencyDDL", `return(true)`))
			defer func() {
				require.NoError(t, failpoint.Disable(
					"github.com/pingcap/tidb/pkg/ddl/mockFailedCommandOnConcurencyDDL"))
			}()

			jobID.Store(job.ID)
			stmt := fmt.Sprintf("admin pause ddl jobs %d", jobID.Load())
			pauseRS, pauseErr = tk2.Session().Execute(context.Background(), stmt)
		}
		adminMutex.Unlock()
	}
	d.SetHook(hook.Clone())
	tk1.MustExec("alter table t add index (id)")
	require.EqualError(t, pauseErr, "mock failed admin command on ddl jobs")

	var cancelRS []sqlexec.RecordSet
	var cancelErr error
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		adminMutex.Lock()
		if job.Type == model.ActionAddIndex && job.State == model.JobStateRunning &&
			job.SchemaState == model.StateWriteReorganization {
			jobID.Store(job.ID)
			stmt := fmt.Sprintf("admin pause ddl jobs %d", jobID.Load())
			pauseRS, pauseErr = tk2.Session().Execute(context.Background(), stmt)

			time.Sleep(5 * time.Second)
			stmt = fmt.Sprintf("admin cancel ddl jobs %d", jobID.Load())
			cancelRS, cancelErr = tk2.Session().Execute(context.Background(), stmt)
		}
		adminMutex.Unlock()
	}
	d.SetHook(hook.Clone())

	tk1.MustGetErrCode("alter table t add index (id)", errno.ErrCancelledDDLJob)
	require.NoError(t, pauseErr)
	require.NoError(t, cancelErr)
	result := tk2.ResultSetToResultWithCtx(context.Background(), pauseRS[0], "pause ddl job successfully")
	result.Check(testkit.Rows(fmt.Sprintf("%d successful", jobID.Load())))
	result = tk2.ResultSetToResultWithCtx(context.Background(), cancelRS[0], "cancel ddl job successfully")
	result.Check(testkit.Rows(fmt.Sprintf("%d successful", jobID.Load())))
}

func TestPauseFailedOnCommit(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk1.MustExec("use test")
	tk1.MustExec("create table t(id int)")

	d := dom.DDL()

	jobID := atomic.NewInt64(0)
	var pauseErr error
	var jobErrs []error
	var adminMutex sync.RWMutex

	hook := &callback.TestDDLCallback{Do: dom}
	originalHook := d.GetHook()
	defer d.SetHook(originalHook)
	// Test when pause cannot be retried and adding index succeeds.
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		adminMutex.Lock()
		if job.Type == model.ActionAddIndex && job.State == model.JobStateRunning &&
			job.SchemaState == model.StateWriteReorganization {
			require.NoError(t, failpoint.Enable(
				"github.com/pingcap/tidb/pkg/ddl/mockCommitFailedOnDDLCommand", `return(true)`))
			defer func() {
				require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockCommitFailedOnDDLCommand"))
			}()
			jobID.Store(job.ID)
			jobErrs, pauseErr = ddl.PauseJobs(tk2.Session(), []int64{jobID.Load()})
		}
		adminMutex.Unlock()
	}
	d.SetHook(hook.Clone())

	tk1.MustExec("alter table t add index (id)")
	require.EqualError(t, pauseErr, "mock commit failed on admin command on ddl jobs")
	require.Len(t, jobErrs, 1)
}
