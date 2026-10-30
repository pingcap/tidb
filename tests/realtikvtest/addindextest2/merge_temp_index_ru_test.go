// Copyright 2026 PingCAP, Inc.
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
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	diststorage "github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/dxf/framework/testutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

// TestMergeTempIndexRUAccountingAcrossRetry covers the regression path of the
// temp-index merge RU accounting: a merge subtask commits one range, fails and
// is retried, and after the task succeeds the committed transaction bytes must
// reach the persisted task meta and the finished job's RU.
func TestMergeTempIndexRUAccountingAcrossRetry(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("the temp index merge RU is only accounted in the nextgen kernel")
	}
	testutil.ReduceCheckInterval(t)

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	realtikvtest.SetupNextGenGlobalSort(t, tk)
	tk.MustExec("use test")

	const tableName = "t_merge_ru_retry"
	tk.MustExec("create table " + tableName + " (id int primary key, v int)")
	// A non-empty table is required, otherwise the reorg work is skipped.
	tk.MustExec("insert into " + tableName + " values (0, 0)")

	// Populate the temporary index right after the main index backfill finishes.
	// The index is in ready-to-merge state then, so DML double writes into both
	// the origin and the temporary index and the merge step has ranges to merge.
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	var (
		inserted  atomic.Bool
		insertMu  sync.Mutex
		insertErr error
	)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterBackfillStateRunningDone", func(job *model.Job) {
		if job.Type != model.ActionAddIndex || job.TableName != tableName {
			return
		}
		if !inserted.CompareAndSwap(false, true) {
			return
		}
		// More rows than the default reorg batch size (256), so the merge commits
		// several ranges and the failpoint below leaves ranges unmerged.
		vals := make([]string, 0, 2048)
		for i := 1; i <= 2048; i++ {
			vals = append(vals, "("+strconv.Itoa(i)+","+strconv.Itoa(i)+")")
		}
		_, err := tk1.Exec("insert into " + tableName + " values " + strings.Join(vals, ","))
		insertMu.Lock()
		insertErr = err
		insertMu.Unlock()
	})

	// Fail the merge step once after it has committed its first range, so the
	// subtask is retried with a reset summary and only the remaining ranges are
	// accounted by the successful attempt.
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockMergeTempIndexFailAfterRange", "1*return()")

	// Count the failed merge subtask runs to make sure the retry path is really
	// exercised, instead of the merge succeeding on the first attempt.
	var mergeFailures atomic.Int64
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/afterRunSubtask",
		func(e *taskexecutor.BaseTaskExecutor, resErr *error, _ context.Context) {
			base := e.GetTaskBase()
			if base.Type == proto.Backfill && base.Step == proto.BackfillStepMergeTempIndex &&
				resErr != nil && *resErr != nil {
				mergeFailures.Add(1)
			}
		})

	var (
		jobMu sync.Mutex
		jobID int64
	)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterUpdateJobToTable", func(job *model.Job, updateErr *error) {
		if job.Type != model.ActionAddIndex || job.TableName != tableName || *updateErr != nil {
			return
		}
		jobMu.Lock()
		jobID = job.ID
		jobMu.Unlock()
	})

	tk.MustExec("alter table " + tableName + " add unique index idx_v(v)")

	require.True(t, inserted.Load(), "the temporary index must be populated before merge")
	insertMu.Lock()
	require.NoError(t, insertErr)
	insertMu.Unlock()
	require.Positive(t, mergeFailures.Load(), "the merge subtask must have failed once and been retried")
	tk.MustExec("admin check table " + tableName)

	jobMu.Lock()
	capturedJobID := jobID
	jobMu.Unlock()
	require.NotZero(t, capturedJobID)

	// The task meta must carry the committed bytes of the ranges merged by the
	// successful attempt, so the workload survives the subtask history transfer.
	taskMgr, err := diststorage.GetTaskManager()
	require.NoError(t, err)
	taskKey := ddl.NewTaskKeyBuilder().SetMergeTempIndex(true).Build(capturedJobID)
	taskCtx := kv.WithInternalSourceType(context.Background(), kv.InternalDistTask)
	task, err := taskMgr.GetTaskByKeyWithHistory(taskCtx, taskKey)
	require.NoError(t, err)
	require.NotNil(t, task)
	require.Equal(t, proto.TaskStateSucceed, task.State)
	var taskMeta ddl.BackfillTaskMeta
	require.NoError(t, json.Unmarshal(task.Meta, &taskMeta))
	require.NotNil(t, taskMeta.Summary)
	require.Positive(t, taskMeta.Summary.MergeTempIndexTxnKVSize,
		"merge temp index transaction bytes must be persisted into the task meta")

	// And the accounted bytes must reach the finished job as RU v2.
	historyJob, err := ddl.GetHistoryJobByID(tk.Session(), capturedJobID)
	require.NoError(t, err)
	require.NotNil(t, historyJob)
	weight := config.GetGlobalConfig().RUV2.DDLWeights.TxnKVBytes
	require.Positive(t, historyJob.RU)
	require.GreaterOrEqual(t, historyJob.RU, float64(taskMeta.Summary.MergeTempIndexTxnKVSize)*weight)
}
