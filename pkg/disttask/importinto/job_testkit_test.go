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

package importinto_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestGetTaskImportedRows(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctx := context.WithValue(context.Background(), "etcd", true)
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)

	mgr := storage.NewTaskManager(pool)
	storage.SetTaskManager(mgr)
	manager, err := storage.GetTaskManager()
	require.NoError(t, err)

	// local sort
	taskMeta := importinto.TaskMeta{
		Plan: importer.Plan{},
	}
	bytes, err := json.Marshal(taskMeta)
	require.NoError(t, err)
	taskID, err := manager.CreateTask(ctx, importinto.TaskKey(111), proto.ImportInto, 1, "", 0, proto.ExtraParams{}, bytes)
	require.NoError(t, err)
	importStepSummaries := []*execute.SubtaskSummary{
		{
			OutputRowCnt: 1,
		},
		{
			OutputRowCnt: 2,
		},
	}
	for _, m := range importStepSummaries {
		testutil.CreateSubTaskWithSummary(t, manager, taskID, proto.ImportStepImport,
			"", nil, m, proto.SubtaskStatePending, proto.ImportInto, 11)
	}
	runInfo, err := importinto.GetRuntimeInfoForJob(ctx, 111)
	require.NoError(t, err)
	require.Equal(t, uint64(3), runInfo.ImportRows)

	// global sort
	taskMeta = importinto.TaskMeta{
		Plan: importer.Plan{
			CloudStorageURI: "s3://test-bucket/test-path",
		},
	}
	bytes, err = json.Marshal(taskMeta)
	require.NoError(t, err)
	taskID, err = manager.CreateTask(ctx, importinto.TaskKey(222), proto.ImportInto, 1, "", 0, proto.ExtraParams{}, bytes)
	require.NoError(t, err)
	ingestStepSummaries := []*execute.SubtaskSummary{
		{
			OutputRowCnt: 11,
		},
		{
			OutputRowCnt: 22,
		},
	}
	for _, m := range ingestStepSummaries {
		testutil.CreateSubTaskWithSummary(t, manager, taskID, proto.ImportStepWriteAndIngest,
			"", bytes, m, proto.SubtaskStatePending, proto.ImportInto, 11)
	}
	runInfo, err = importinto.GetRuntimeInfoForJob(ctx, 222)
	require.NoError(t, err)
	require.Equal(t, uint64(33), runInfo.ImportRows)
}

func TestShowImportProgress(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctx := context.WithValue(context.Background(), "etcd", true)
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)

	mgr := storage.NewTaskManager(pool)
	storage.SetTaskManager(mgr)
	manager, err := storage.GetTaskManager()
	require.NoError(t, err)

	// global sort
	taskMeta := importinto.TaskMeta{
		Plan: importer.Plan{
			CloudStorageURI: "s3://test-bucket/test-path",
		},
	}

	taskSummary := &importer.Summary{
		EncodeSummary:      importer.StepSummary{Bytes: 1000, RowCnt: 100},
		MergeSummary:       importer.StepSummary{Bytes: 0, RowCnt: 0},
		IngestSummary:      importer.StepSummary{Bytes: 1000, RowCnt: 100},
		PostProcessSummary: importer.StepSummary{Bytes: 1000, RowCnt: 100},
	}

	taskMeta.TaskResult, err = json.Marshal(taskSummary)
	require.NoError(t, err)
	bytes, err := json.Marshal(taskMeta)
	require.NoError(t, err)

	conn := tk.Session().GetSQLExecutor()
	jobID, err := importer.CreateJob(ctx, conn, "test", "t", 1,
		"root", &importer.ImportParameters{}, 1000)
	require.NoError(t, err)

	taskID, err := manager.CreateTask(ctx, importinto.TaskKey(jobID), proto.ImportInto, 1, "", 0, proto.ExtraParams{}, bytes)
	require.NoError(t, err)

	subtasks := []struct {
		summary execute.SubtaskSummary
		state   proto.SubtaskState
	}{
		{
			execute.SubtaskSummary{InputRowCnt: 20, InputBytes: 200, OutputRowCnt: 20},
			proto.SubtaskStateRunning,
		},
		{
			execute.SubtaskSummary{OutputRowCnt: 20},
			proto.SubtaskStateRunning,
		},
		{
			execute.SubtaskSummary{InputRowCnt: 30, InputBytes: 300, OutputRowCnt: 20},
			proto.SubtaskStateSucceed,
		},
	}

	checkShowInfo := func(expected string, imported int64) {
		rs := tk.MustQuery(fmt.Sprintf("show import job %d", jobID)).Rows()
		require.Contains(t, rs[0][8], expected)
		importedRows, err := strconv.Atoi(rs[0][7].(string))
		require.NoError(t, err)
		require.EqualValues(t, importedRows, imported)
	}

	// Init step
	require.NoError(t, importer.StartJob(ctx, conn, jobID, importer.JobStepGlobalSorting))
	checkShowInfo("[init] N/A", 0)

	// Encode step
	task, err := manager.GetTaskByID(ctx, taskID)
	require.NoError(t, err)
	require.NoError(t, manager.SwitchTaskStep(ctx, task, proto.TaskStateRunning, proto.ImportStepEncodeAndSort, nil))
	for _, s := range subtasks {
		testutil.CreateSubTaskWithSummary(t, manager, taskID, proto.ImportStepEncodeAndSort,
			"", bytes, &s.summary, s.state, proto.ImportInto, 11)
	}

	runInfo, err := importinto.GetRuntimeInfoForJob(ctx, jobID)
	require.NoError(t, err)
	require.EqualValues(t, 1, runInfo.FinishedSubtaskCnt)
	require.EqualValues(t, 3, runInfo.TotalSubtaskCnt)
	require.EqualValues(t, 1000, runInfo.Total)
	require.EqualValues(t, 500, runInfo.Processed)
	checkShowInfo("[encode] subtasks: 1/3, progress: 50.00", 0)

	// Merge step
	task, err = manager.GetTaskByID(ctx, taskID)
	require.NoError(t, err)
	require.NoError(t, manager.SwitchTaskStep(ctx, task, proto.TaskStateRunning, proto.ImportStepMergeSort, nil))

	runInfo, err = importinto.GetRuntimeInfoForJob(ctx, jobID)
	require.NoError(t, err)
	require.EqualValues(t, 0, runInfo.TotalSubtaskCnt)
	require.EqualValues(t, 0, runInfo.Total)
	require.EqualValues(t, 0, runInfo.Processed)
	checkShowInfo("[merge-sort] subtasks: 0/0, progress: 0.00%, speed: 0B/s, elapsed: 0s, ETA: N/A", 0)

	// Ingest step
	for _, s := range subtasks {
		testutil.CreateSubTaskWithSummary(t, manager, taskID, proto.ImportStepWriteAndIngest,
			"", bytes, &s.summary, s.state, proto.ImportInto, 11)
	}

	task, err = manager.GetTaskByID(ctx, taskID)
	require.NoError(t, err)
	require.NoError(t, manager.SwitchTaskStep(ctx, task, proto.TaskStateRunning, proto.ImportStepWriteAndIngest, nil))
	checkShowInfo("[ingest] subtasks: 1/3, progress: 50.00", 60)

	// Post-process step
	task, err = manager.GetTaskByID(ctx, taskID)
	require.NoError(t, err)
	require.NoError(t, manager.SwitchTaskStep(ctx, task, proto.TaskStateRunning, proto.ImportStepPostProcess, nil))
	checkShowInfo("[post-process] N/A", 100)

	require.NoError(t, importer.FinishJob(ctx, conn, jobID, taskSummary))
	checkShowInfo("[Not Running]", 100)
}
