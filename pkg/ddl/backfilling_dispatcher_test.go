// Copyright 2023 PingCAP, Inc.
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

package ddl_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/ngaut/pools"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestBackfillingDispatcherLocalMode(t *testing.T) {
	/// test str
	require.Equal(t, "init", ddl.StepStr(proto.StepInit))
	require.Equal(t, "read-index", ddl.StepStr(ddl.StepReadIndex))
	require.Equal(t, "merge-sort", ddl.StepStr(ddl.StepMergeSort))
	require.Equal(t, "write&ingest", ddl.StepStr(ddl.StepWriteAndIngest))
	require.Equal(t, "done", ddl.StepStr(proto.StepDone))
	require.Equal(t, "unknown", ddl.StepStr(111))

	store, dom := testkit.CreateMockStoreAndDomain(t)
	dsp, err := ddl.NewBackfillingDispatcherExt(dom.DDL())
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	/// 1. test partition table.
	tk.MustExec("create table tp1(id int primary key, v int) PARTITION BY RANGE (id) (\n    " +
		"PARTITION p0 VALUES LESS THAN (10),\n" +
		"PARTITION p1 VALUES LESS THAN (100),\n" +
		"PARTITION p2 VALUES LESS THAN (1000),\n" +
		"PARTITION p3 VALUES LESS THAN MAXVALUE\n);")
	gTask := createAddIndexGlobalTask(t, dom, "test", "tp1", proto.Backfill, false)
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tp1"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()

	// 1.1 OnNextSubtasksBatch
	gTask.Step = dsp.GetNextStep(gTask)
	require.Equal(t, ddl.StepReadIndex, gTask.Step)
	serverInfos, _, err := dsp.GetEligibleInstances(context.Background(), gTask)
	require.NoError(t, err)
	metas, err := dsp.OnNextSubtasksBatch(context.Background(), nil, gTask, serverInfos, gTask.Step)
	require.NoError(t, err)
	require.Equal(t, len(tblInfo.Partition.Definitions), len(metas))
	for i, par := range tblInfo.Partition.Definitions {
		var subTask ddl.BackfillSubTaskMeta
		require.NoError(t, json.Unmarshal(metas[i], &subTask))
		require.Equal(t, par.ID, subTask.PhysicalTableID)
	}

	// 1.2 test partition table OnNextSubtasksBatch after StepReadIndex
	gTask.State = proto.TaskStateRunning
	gTask.Step = dsp.GetNextStep(gTask)
	require.Equal(t, proto.StepDone, gTask.Step)
	metas, err = dsp.OnNextSubtasksBatch(context.Background(), nil, gTask, serverInfos, gTask.Step)
	require.NoError(t, err)
	require.Len(t, metas, 0)

	// 1.3 test partition table OnDone.
	err = dsp.OnDone(context.Background(), nil, gTask)
	require.NoError(t, err)

	/// 2. test non partition table.
	// 2.1 empty table
	tk.MustExec("create table t1(id int primary key, v int)")
	gTask = createAddIndexGlobalTask(t, dom, "test", "t1", proto.Backfill, false)
	metas, err = dsp.OnNextSubtasksBatch(context.Background(), nil, gTask, serverInfos, gTask.Step)
	require.NoError(t, err)
	require.Equal(t, 0, len(metas))
	// 2.2 non empty table.
	tk.MustExec("create table t2(id bigint auto_random primary key)")
	tk.MustExec("insert into t2 values (), (), (), (), (), ()")
	tk.MustExec("insert into t2 values (), (), (), (), (), ()")
	tk.MustExec("insert into t2 values (), (), (), (), (), ()")
	tk.MustExec("insert into t2 values (), (), (), (), (), ()")
	gTask = createAddIndexGlobalTask(t, dom, "test", "t2", proto.Backfill, false)
	// 2.2.1 stepInit
	gTask.Step = dsp.GetNextStep(gTask)
	metas, err = dsp.OnNextSubtasksBatch(context.Background(), nil, gTask, serverInfos, gTask.Step)
	require.NoError(t, err)
	require.Equal(t, 1, len(metas))
	require.Equal(t, ddl.StepReadIndex, gTask.Step)
	// 2.2.2 StepReadIndex
	gTask.State = proto.TaskStateRunning
	gTask.Step = dsp.GetNextStep(gTask)
	require.Equal(t, proto.StepDone, gTask.Step)
	metas, err = dsp.OnNextSubtasksBatch(context.Background(), nil, gTask, serverInfos, gTask.Step)
	require.NoError(t, err)
	require.Equal(t, 0, len(metas))
}

func TestCalculateRegionBatch(t *testing.T) {
	// Test calculate in cloud storage.
	batchCnt := ddl.CalculateRegionBatchForTest(100, 8, false)
	require.Equal(t, 12, batchCnt)
	batchCnt = ddl.CalculateRegionBatchForTest(2, 8, false)
	require.Equal(t, 1, batchCnt)
	batchCnt = ddl.CalculateRegionBatchForTest(8, 8, false)
	require.Equal(t, 1, batchCnt)

	// Test calculate in local storage.
	variable.DDLDiskQuota.Store(96 * units.MiB * 1000)
	batchCnt = ddl.CalculateRegionBatchForTest(100, 8, true)
	require.Equal(t, 12, batchCnt)
	batchCnt = ddl.CalculateRegionBatchForTest(2, 8, true)
	require.Equal(t, 1, batchCnt)
	variable.DDLDiskQuota.Store(96 * units.MiB * 2)
	batchCnt = ddl.CalculateRegionBatchForTest(24, 8, true)
	require.Equal(t, 2, batchCnt)
}

func TestBackfillingDispatcherGlobalSortMode(t *testing.T) {
	// init test env.
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctx := context.WithValue(context.Background(), "etcd", true)
	ctx = util.WithInternalSourceType(ctx, "handle")
	mgr := storage.NewTaskManager(pool)
	storage.SetTaskManager(mgr)
	dspManager, err := dispatcher.NewManager(util.WithInternalSourceType(ctx, "dispatcher"), mgr, "host:port")
	require.NoError(t, err)

	tk.MustExec("use test")
	tk.MustExec("create table t1(id bigint auto_random primary key)")
	tk.MustExec("insert into t1 values (), (), (), (), (), ()")
	tk.MustExec("insert into t1 values (), (), (), (), (), ()")
	tk.MustExec("insert into t1 values (), (), (), (), (), ()")
	tk.MustExec("insert into t1 values (), (), (), (), (), ()")
	task := createAddIndexGlobalTask(t, dom, "test", "t1", proto.Backfill, true)

	dsp := dspManager.MockDispatcher(task)
	ext, err := ddl.NewBackfillingDispatcherExt(dom.DDL())
	require.NoError(t, err)
	ext.(*ddl.BackfillingDispatcherExt).GlobalSort = true
	dsp.Extension = ext

	taskID, err := mgr.AddNewGlobalTask(ctx, task.Key, proto.Backfill, 1, task.Meta)
	require.NoError(t, err)
	task.ID = taskID
	serverInfos, _, err := dsp.GetEligibleInstances(context.Background(), task)
	require.NoError(t, err)

	// 1. to read-index stage
	subtaskMetas, err := dsp.OnNextSubtasksBatch(ctx, dsp, task, serverInfos, dsp.GetNextStep(task))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 1)
	task.Step = ext.GetNextStep(task)
	require.Equal(t, ddl.StepReadIndex, task.Step)
	// update task/subtask, and finish subtask, so we can go to next stage
	subtasks := make([]*proto.Subtask, 0, len(subtaskMetas))
	for _, m := range subtaskMetas {
		subtasks = append(subtasks, proto.NewSubtask(task.Step, task.ID, task.Type, "", m))
	}
	_, err = mgr.UpdateGlobalTaskAndAddSubTasks(ctx, task, subtasks, proto.TaskStatePending)
	require.NoError(t, err)
	gotSubtasks, err := mgr.GetSubtasksForImportInto(ctx, taskID, ddl.StepReadIndex)
	require.NoError(t, err)

	// update meta, same as import into.
	sortStepMeta := &ddl.BackfillSubTaskMeta{
		SortedKVMeta: external.SortedKVMeta{
			StartKey:    []byte("ta"),
			EndKey:      []byte("tc"),
			TotalKVSize: 12,
			MultipleFilesStats: []external.MultipleFilesStat{
				{
					Filenames: [][2]string{
						{"gs://sort-bucket/data/1", "gs://sort-bucket/data/1.stat"},
					},
				},
			},
		},
	}
	sortStepMetaBytes, err := json.Marshal(sortStepMeta)
	require.NoError(t, err)
	for _, s := range gotSubtasks {
		require.NoError(t, mgr.FinishSubtask(ctx, s.SchedulerID, s.ID, sortStepMetaBytes))
	}
	// 2. to merge-sort stage.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/forceMergeSort", `return()`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/forceMergeSort"))
	})
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, dsp, task, serverInfos, ext.GetNextStep(task))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 1)
	task.Step = ext.GetNextStep(task)
	require.Equal(t, ddl.StepMergeSort, task.Step)

	// update meta, same as import into.
	subtasks = make([]*proto.Subtask, 0, len(subtaskMetas))
	for _, m := range subtaskMetas {
		subtasks = append(subtasks, proto.NewSubtask(task.Step, task.ID, task.Type, "", m))
	}
	_, err = mgr.UpdateGlobalTaskAndAddSubTasks(ctx, task, subtasks, proto.TaskStatePending)
	require.NoError(t, err)
	gotSubtasks, err = mgr.GetSubtasksForImportInto(ctx, taskID, task.Step)
	require.NoError(t, err)
	mergeSortStepMeta := &ddl.BackfillSubTaskMeta{
		SortedKVMeta: external.SortedKVMeta{
			StartKey:    []byte("ta"),
			EndKey:      []byte("tc"),
			TotalKVSize: 12,
			MultipleFilesStats: []external.MultipleFilesStat{
				{
					Filenames: [][2]string{
						{"gs://sort-bucket/data/1", "gs://sort-bucket/data/1.stat"},
					},
				},
			},
		},
	}
	mergeSortStepMetaBytes, err := json.Marshal(mergeSortStepMeta)
	require.NoError(t, err)
	for _, s := range gotSubtasks {
		require.NoError(t, mgr.FinishSubtask(ctx, s.SchedulerID, s.ID, mergeSortStepMetaBytes))
	}
	// 3. to write&ingest stage.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockWriteIngest", "return(true)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockWriteIngest"))
	})
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, dsp, task, serverInfos, ext.GetNextStep(task))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 1)
	task.Step = ext.GetNextStep(task)
	require.Equal(t, ddl.StepWriteAndIngest, task.Step)
	// 4. to done stage.
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, dsp, task, serverInfos, ext.GetNextStep(task))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 0)
	task.Step = ext.GetNextStep(task)
	require.Equal(t, proto.StepDone, task.Step)
}

func TestGetNextStep(t *testing.T) {
	task := &proto.Task{
		Step: proto.StepInit,
	}
	ext := &ddl.BackfillingDispatcherExt{}

	// 1. local mode
	for _, nextStep := range []proto.Step{ddl.StepReadIndex, proto.StepDone} {
		require.Equal(t, nextStep, ext.GetNextStep(task))
		task.Step = nextStep
	}
	// 2. global sort mode
	ext = &ddl.BackfillingDispatcherExt{GlobalSort: true}
	task.Step = proto.StepInit
	for _, nextStep := range []proto.Step{ddl.StepReadIndex, ddl.StepMergeSort, ddl.StepWriteAndIngest} {
		require.Equal(t, nextStep, ext.GetNextStep(task))
		task.Step = nextStep
	}
}

func createAddIndexGlobalTask(t *testing.T,
	dom *domain.Domain,
	dbName,
	tblName string,
	taskType proto.TaskType,
	useGlobalSort bool) *proto.Task {
	db, ok := dom.InfoSchema().SchemaByName(model.NewCIStr(dbName))
	require.True(t, ok)
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr(dbName), model.NewCIStr(tblName))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	defaultSQLMode, err := mysql.GetSQLMode(mysql.DefaultSQLMode)
	require.NoError(t, err)

	taskMeta := &ddl.BackfillGlobalMeta{
		Job: model.Job{
			ID:       time.Now().UnixNano(),
			SchemaID: db.ID,
			TableID:  tblInfo.ID,
			ReorgMeta: &model.DDLReorgMeta{
				SQLMode:     defaultSQLMode,
				Location:    &model.TimeZoneLocation{Name: time.UTC.String(), Offset: 0},
				ReorgTp:     model.ReorgTypeLitMerge,
				IsDistReorg: true,
			},
		},
		EleIDs:     []int64{10},
		EleTypeKey: meta.IndexElementKey,
	}
	if useGlobalSort {
		taskMeta.CloudStorageURI = "gs://sort-bucket"
	}

	gTaskMetaBytes, err := json.Marshal(taskMeta)
	require.NoError(t, err)

	gTask := &proto.Task{
		ID:              time.Now().UnixMicro(),
		Type:            taskType,
		Step:            proto.StepInit,
		State:           proto.TaskStatePending,
		Meta:            gTaskMetaBytes,
		StartTime:       time.Now(),
		StateUpdateTime: time.Now(),
	}

	return gTask
}
