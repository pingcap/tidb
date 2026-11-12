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

package importer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/dxf/framework/testutil"
	"github.com/pingcap/tidb/pkg/dxf/importinto/taskkey"
	"github.com/pingcap/tidb/pkg/importer"
	"github.com/pingcap/tidb/pkg/kv"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/atomic"
)

func switchTaskStep(
	ctx context.Context, t *testing.T,
	manager *storage.TaskManager, taskID int64, step proto.Step,
) {
	task, err := manager.GetTaskByID(ctx, taskID)
	require.NoError(t, err)
	require.NoError(t, manager.SwitchTaskStep(ctx, task, proto.TaskStateRunning, step, nil))
}

func TestGetTaskImportedRows(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)")

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctx := util.WithInternalSourceType(context.Background(), kv.InternalDistTask)

	manager := storage.NewTaskManager(pool)
	storage.SetTaskManager(manager)
	require.NoError(t, manager.InitMeta(ctx, ":4000", ""))

	// local sort
	taskMeta := importer.TaskMeta{
		Plan: importer.Plan{},
		Summary: importer.Summary{
			EncodeSummary: importer.StepSummary{
				Bytes:  10000,
				RowCnt: 1000,
			},
			IngestSummary: importer.StepSummary{
				Bytes:  10000,
				RowCnt: 1000,
			},
		},
	}

	bytes, err := json.Marshal(taskMeta)
	require.NoError(t, err)
	taskID, err := manager.CreateTask(ctx, taskkey.ForJob(111), proto.ImportInto, "", 1, "", 0, proto.ExtraParams{}, bytes)
	require.NoError(t, err)
	importStepSummaries := []*execute.SubtaskSummary{
		{
			RowCnt:    *atomic.NewInt64(300),
			Processed: *atomic.NewInt64(4000),
		},
		{
			RowCnt:    *atomic.NewInt64(400),
			Processed: *atomic.NewInt64(4000),
		},
	}
	for _, m := range importStepSummaries {
		testutil.CreateSubTaskWithSummary(t, manager, taskID, proto.ImportStepImport,
			"", nil, m, proto.SubtaskStatePending, proto.ImportInto, 11)
	}

	switchTaskStep(ctx, t, manager, taskID, proto.ImportStepImport)

	loc := tk.Session().GetSessionVars().Location()

	runInfo, err := importer.GetJobRuntimeInfo(ctx, loc, 111)
	require.NoError(t, err)
	require.EqualValues(t, 700, runInfo.ImportRows)
	require.Equal(t, "80", runInfo.Percent())

	// global sort
	taskMeta = importer.TaskMeta{
		Plan: importer.Plan{
			CloudStorageURI: "s3://test-bucket/test-path",
		},
		Summary: importer.Summary{
			IngestSummary: importer.StepSummary{
				Bytes:  10000,
				RowCnt: 1000,
			},
		},
	}

	bytes, err = json.Marshal(taskMeta)
	require.NoError(t, err)
	taskID, err = manager.CreateTask(ctx, taskkey.ForJob(222), proto.ImportInto, "", 1, "", 0, proto.ExtraParams{}, bytes)
	require.NoError(t, err)
	ingestStepSummaries := []*execute.SubtaskSummary{
		{
			RowCnt:    *atomic.NewInt64(100),
			Processed: *atomic.NewInt64(1000),
		},
		{
			RowCnt:    *atomic.NewInt64(200),
			Processed: *atomic.NewInt64(2000),
		},
	}
	for _, m := range ingestStepSummaries {
		testutil.CreateSubTaskWithSummary(t, manager, taskID, proto.ImportStepWriteAndIngest,
			"", bytes, m, proto.SubtaskStatePending, proto.ImportInto, 11)
	}

	switchTaskStep(ctx, t, manager, taskID, proto.ImportStepWriteAndIngest)
	runInfo, err = importer.GetJobRuntimeInfo(ctx, tk.Session().GetSessionVars().Location(), 222)
	require.NoError(t, err)
	require.EqualValues(t, 300, runInfo.ImportRows)
	require.Equal(t, "30", runInfo.Percent())
}

func TestGetJobLastUpdateTime(t *testing.T) {
	_, manager, ctx := testutil.InitTableTest(t)
	require.NoError(t, manager.InitMeta(ctx, ":4000", ""))

	const (
		jobID             = int64(9527)
		adjacentTaskID    = int64(9007199254740992)
		targetTaskID      = int64(9007199254740993)
		targetHistoryTime = int64(1000)
		adjacentHistTime  = int64(2000)
		targetActiveTime  = int64(1500)
		adjacentLiveTime  = int64(2500)
	)
	_, err := manager.ExecuteSQLWithNewSession(ctx,
		"alter table mysql.tidb_global_task auto_increment = 9007199254740992")
	require.NoError(t, err)

	gotAdjacentTaskID, err := manager.CreateTask(ctx, taskkey.ForJob(jobID+1), proto.ImportInto,
		"", 1, "", 0, proto.ExtraParams{}, nil)
	require.NoError(t, err)
	require.Equal(t, adjacentTaskID, gotAdjacentTaskID)
	gotTargetTaskID, err := manager.CreateTask(ctx, taskkey.ForJob(jobID), proto.ImportInto,
		"", 1, "", 0, proto.ExtraParams{}, nil)
	require.NoError(t, err)
	require.Equal(t, targetTaskID, gotTargetTaskID)

	updateSubtaskTime := func(id, updateTime int64) {
		_, err = manager.ExecuteSQLWithNewSession(ctx, `
			update mysql.tidb_background_subtask
			set state_update_time = %?
			where id = %?`, updateTime, id)
		require.NoError(t, err)
	}
	targetHistoryID := testutil.InsertSubtask(t, manager, targetTaskID, proto.ImportStepEncodeAndSort,
		"tidb-1", nil, proto.SubtaskStateSucceed, proto.ImportInto, 1)
	adjacentHistoryID := testutil.InsertSubtask(t, manager, adjacentTaskID, proto.ImportStepEncodeAndSort,
		"tidb-1", nil, proto.SubtaskStateSucceed, proto.ImportInto, 1)
	updateSubtaskTime(targetHistoryID, targetHistoryTime)
	updateSubtaskTime(adjacentHistoryID, adjacentHistTime)

	targetTask, err := manager.GetTaskByID(ctx, targetTaskID)
	require.NoError(t, err)
	adjacentTask, err := manager.GetTaskByID(ctx, adjacentTaskID)
	require.NoError(t, err)
	require.NoError(t, manager.TransferTasks2History(ctx, []*proto.Task{targetTask, adjacentTask}))

	targetActiveID := testutil.InsertSubtask(t, manager, targetTaskID, proto.ImportStepWriteAndIngest,
		"tidb-1", nil, proto.SubtaskStateRunning, proto.ImportInto, 1)
	adjacentActiveID := testutil.InsertSubtask(t, manager, adjacentTaskID, proto.ImportStepWriteAndIngest,
		"tidb-1", nil, proto.SubtaskStateRunning, proto.ImportInto, 1)
	updateSubtaskTime(targetActiveID, targetActiveTime)
	updateSubtaskTime(adjacentActiveID, adjacentLiveTime)

	expectedRows, err := manager.ExecuteSQLWithNewSession(ctx, "select from_unixtime(%?)", targetActiveTime)
	require.NoError(t, err)
	require.Len(t, expectedRows, 1)

	lastUpdateTime, err := importer.GetJobLastUpdateTime(ctx, jobID)
	require.NoError(t, err)
	require.Equal(t, expectedRows[0].GetTime(0), lastUpdateTime)
}

func TestShowImportProgress(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)")

	fmap := plannercore.ImportIntoFieldMap

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctx := util.WithInternalSourceType(context.Background(), kv.InternalDistTask)

	manager := storage.NewTaskManager(pool)
	storage.SetTaskManager(manager)
	require.NoError(t, manager.InitMeta(ctx, ":4000", ""))

	// global sort
	taskMeta := importer.TaskMeta{
		Plan: importer.Plan{
			CloudStorageURI: "s3://test-bucket/test-path",
		},
		Summary: importer.Summary{
			EncodeSummary:           importer.StepSummary{Bytes: 1000, RowCnt: 100},
			MergeSummary:            importer.StepSummary{Bytes: 0, RowCnt: 0},
			IngestSummary:           importer.StepSummary{Bytes: 1000, RowCnt: 100},
			CollectConflictsSummary: importer.StepSummary{RowCnt: 1000},
			ResolveConflictsSummary: importer.StepSummary{RowCnt: 500},
			ImportedRows:            100,
		},
	}

	bytes, err := json.Marshal(taskMeta)
	require.NoError(t, err)

	conn := tk.Session().GetSQLExecutor()
	jobID, err := importer.CreateJob(ctx, conn, "test", "t", 1,
		"root", "", &importer.ImportParameters{}, 1000)
	require.NoError(t, err)

	taskID, err := manager.CreateTask(ctx, taskkey.ForJob(jobID), proto.ImportInto, "", 1, "", 0, proto.ExtraParams{}, bytes)
	require.NoError(t, err)

	subtasks := []struct {
		summary execute.SubtaskSummary
		state   proto.SubtaskState
	}{
		{
			execute.SubtaskSummary{
				RowCnt:    *atomic.NewInt64(20),
				Processed: *atomic.NewInt64(200),
				Progresses: []execute.Progress{
					{RowCnt: 0, Processed: 0, UpdateTime: time.Unix(1001, 0)},
					{RowCnt: 20, Processed: 200, UpdateTime: time.Unix(1002, 0)},
				},
			},
			proto.SubtaskStateRunning,
		},
		{
			execute.SubtaskSummary{
				RowCnt:    *atomic.NewInt64(30),
				Processed: *atomic.NewInt64(300),
				Progresses: []execute.Progress{
					{RowCnt: 0, Processed: 0, UpdateTime: time.Unix(1000, 0)},
					{RowCnt: 15, Processed: 150, UpdateTime: time.Unix(1001, 0)},
					{RowCnt: 30, Processed: 300, UpdateTime: time.Unix(1002, 0)},
				},
			},
			proto.SubtaskStateSucceed,
		},
		{
			execute.SubtaskSummary{
				RowCnt:    *atomic.NewInt64(0),
				Processed: *atomic.NewInt64(0),
			},
			proto.SubtaskStateSucceed,
		},
	}

	checkShowInfo := func(step, processed, total, percent, speed, eta string, imported int64) {
		rs := tk.MustQuery(fmt.Sprintf("show import job %d", jobID)).Rows()
		require.Equal(t, rs[0][fmap["CurStep"]], step)
		require.Equal(t, rs[0][fmap["CurStepProcessedSize"]], processed)
		require.Equal(t, rs[0][fmap["CurStepTotalSize"]], total)
		require.Equal(t, rs[0][fmap["CurStepProgressPct"]], percent)
		require.Equal(t, rs[0][fmap["CurStepSpeed"]], speed)
		require.Equal(t, rs[0][fmap["CurStepETA"]], eta)

		importedRows, err := strconv.Atoi(rs[0][fmap["ImportedRows"]].(string))
		require.NoError(t, err)
		require.EqualValues(t, importedRows, imported)
	}

	// Init step
	require.NoError(t, importer.StartJob(ctx, conn, jobID, importer.JobStepGlobalSorting))
	checkShowInfo("init", "0B", "0B", "N/A", "0B/s", "N/A", 0)

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/importer/mockSpeedDuration", "return(5000)")

	// Encode step
	switchTaskStep(ctx, t, manager, taskID, proto.ImportStepEncodeAndSort)
	for _, s := range subtasks {
		testutil.CreateSubTaskWithSummary(t, manager, taskID, proto.ImportStepEncodeAndSort,
			"", bytes, &s.summary, s.state, proto.ImportInto, 11)
	}

	loc := tk.Session().GetSessionVars().Location()

	runInfo, err := importer.GetJobRuntimeInfo(ctx, loc, jobID)
	require.NoError(t, err)
	require.EqualValues(t, 1000, runInfo.Total)
	require.EqualValues(t, 500, runInfo.Processed)
	checkShowInfo("encode", "500B", "1000B", "50", "100B/s", "00:00:05", 0)

	// Query totals cannot be known before the SELECT has finished.
	switchTaskStep(ctx, t, manager, taskID, proto.ImportStepQuery)
	checkShowInfo("query", "N/A", "N/A", "N/A", "N/A", "N/A", 0)

	// Merge step
	switchTaskStep(ctx, t, manager, taskID, proto.ImportStepMergeSort)

	runInfo, err = importer.GetJobRuntimeInfo(ctx, loc, jobID)
	require.NoError(t, err)
	require.EqualValues(t, 0, runInfo.Total)
	require.EqualValues(t, 0, runInfo.Processed)
	checkShowInfo("merge-sort", "0B", "0B", "0", "0B/s", "N/A", 0)

	// Ingest step
	for _, s := range subtasks {
		testutil.CreateSubTaskWithSummary(t, manager, taskID, proto.ImportStepWriteAndIngest,
			"", bytes, &s.summary, s.state, proto.ImportInto, 11)
	}

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/importer/mockSpeedDuration", "return(10000)")
	switchTaskStep(ctx, t, manager, taskID, proto.ImportStepWriteAndIngest)
	checkShowInfo("ingest", "500B", "1000B", "50", "50B/s", "00:00:10", 50)

	// collect-conflicts step
	switchTaskStep(ctx, t, manager, taskID, proto.ImportStepCollectConflicts)
	for _, s := range subtasks {
		testutil.CreateSubTaskWithSummary(t, manager, taskID, proto.ImportStepCollectConflicts,
			"", bytes, &s.summary, s.state, proto.ImportInto, 11)
	}
	checkShowInfo("collect-conflicts", "500 conflicts", "1000 conflicts", "50", "50 conflicts/s", "00:00:10", 0)

	// conflict-resolution step
	switchTaskStep(ctx, t, manager, taskID, proto.ImportStepConflictResolution)
	for _, s := range subtasks {
		testutil.CreateSubTaskWithSummary(t, manager, taskID, proto.ImportStepConflictResolution,
			"", bytes, &s.summary, s.state, proto.ImportInto, 11)
	}
	checkShowInfo("conflict-resolution", "500 conflicts", "500 conflicts", "100", "50 conflicts/s", "00:00:00", 0)

	// Post-process step
	switchTaskStep(ctx, t, manager, taskID, proto.ImportStepPostProcess)
	checkShowInfo("post-process", "0B", "0B", "N/A", "0B/s", "N/A", 100)
}

func TestShowImportGroup(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)")

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctx := util.WithInternalSourceType(context.Background(), kv.InternalDistTask)

	manager := storage.NewTaskManager(pool)
	storage.SetTaskManager(manager)
	require.NoError(t, manager.InitMeta(ctx, ":4000", ""))

	conn := tk.Session().GetSQLExecutor()

	// No groups at start
	rs := tk.MustQuery(`show import group "group2"`).Rows()
	require.Len(t, rs, 0)
	rs = tk.MustQuery(`show import groups`).Rows()
	require.Len(t, rs, 0)

	importJobs := []struct {
		SchemaName string
		TableName  string
		TableID    int64
		GroupKey   string
	}{
		{"test", "t1", 1, "group1"},
		{"test", "t2", 2, "group1"},
		{"test", "t3", 3, "group2"},
		{"test", "t4", 4, ""}, // not displayed in show import groups
	}

	for _, job := range importJobs {
		jobID, err := importer.CreateJob(ctx, conn, job.SchemaName, job.TableName, job.TableID,
			"root", job.GroupKey, &importer.ImportParameters{}, 1000)
		require.NoError(t, err)

		taskID, err := manager.CreateTask(ctx, taskkey.ForJob(jobID), proto.ImportInto, "", 1, "", 0, proto.ExtraParams{}, nil)
		require.NoError(t, err)

		switchTaskStep(ctx, t, manager, taskID, proto.ImportStepEncodeAndSort)
		testutil.CreateSubTask(t, manager, taskID, proto.ImportStepEncodeAndSort,
			"", nil, proto.ImportInto, 11)
	}

	rs = tk.MustQuery("show import groups").Sort().Rows()
	for _, r := range rs {
		// create time should never be null
		require.NotEqual(t, "<nil>", r[7])
	}
	require.Len(t, rs, 2)
	require.Equal(t, "group1", rs[0][0])
	require.Equal(t, "2", rs[0][1])
	require.Equal(t, "group2", rs[1][0])
	require.Equal(t, "1", rs[1][1])

	rs = tk.MustQuery(`show import group "nonexist"`).Rows()
	require.Len(t, rs, 0)

	rs = tk.MustQuery(`show import group "group2"`).Rows()
	require.Len(t, rs, 1)
	require.Equal(t, "group2", rs[0][0])
	require.Equal(t, "1", rs[0][1])
	require.NotEqual(t, "<nil>", rs[0][7])
}

func TestFormatTime(t *testing.T) {
	require.Equal(t, "1 d 00:00:00", importer.FormatSecondAsTime(86400))
	require.Equal(t, "2 d 00:00:01", importer.FormatSecondAsTime(172801))
	require.Equal(t, "23:59:59", importer.FormatSecondAsTime(86399))
	require.Equal(t, "00:59:59", importer.FormatSecondAsTime(3599))
	require.Equal(t, "08:00:00", importer.FormatSecondAsTime(28800))
}
