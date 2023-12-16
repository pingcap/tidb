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
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestSchedulerExtLocalSort(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctx := context.WithValue(context.Background(), "etcd", true)
	ctx = util.WithInternalSourceType(ctx, "taskManager")
	mgr := storage.NewTaskManager(pool)
	storage.SetTaskManager(mgr)
	sch, err := scheduler.NewManager(util.WithInternalSourceType(ctx, "scheduler"), mgr, "host:port")
	require.NoError(t, err)

	// create job
	conn := tk.Session().(sqlexec.SQLExecutor)
	jobID, err := importer.CreateJob(ctx, conn, "test", "t", 1,
		"root", &importer.ImportParameters{}, 123)
	require.NoError(t, err)
	gotJobInfo, err := importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "pending", gotJobInfo.Status)
	logicalPlan := &importinto.LogicalPlan{
		JobID: jobID,
		Plan: importer.Plan{
			DBName: "test",
			TableInfo: &model.TableInfo{
				Name: model.NewCIStr("t"),
			},
			DisableTiKVImportMode: true,
		},
		Stmt:              `IMPORT INTO db.tb FROM 'gs://test-load/*.csv?endpoint=xxx'`,
		EligibleInstances: []*infosync.ServerInfo{{ID: "1"}},
		ChunkMap:          map[int32][]importinto.Chunk{1: {{Path: "gs://test-load/1.csv"}}},
	}
	bs, err := logicalPlan.ToTaskMeta()
	require.NoError(t, err)
	task := &proto.Task{
		Type:            proto.TaskTypeExample,
		Meta:            bs,
		Step:            proto.StepInit,
		State:           proto.TaskStatePending,
		StateUpdateTime: time.Now(),
	}
	manager, err := storage.GetTaskManager()
	require.NoError(t, err)
	taskMeta, err := json.Marshal(task)
	require.NoError(t, err)
	taskID, err := manager.CreateTask(ctx, importinto.TaskKey(jobID), proto.ImportInto, 1, taskMeta)
	require.NoError(t, err)
	task.ID = taskID

	// to import stage, job should be running
	d := sch.MockScheduler(task)
	ext := importinto.ImportSchedulerExt{}
	serverInfos, _, err := ext.GetEligibleInstances(context.Background(), task)
	require.NoError(t, err)
	subtaskMetas, err := ext.OnNextSubtasksBatch(ctx, d, task, serverInfos, ext.GetNextStep(task))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 1)
	task.Step = ext.GetNextStep(task)
	require.Equal(t, importinto.StepImport, task.Step)
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "running", gotJobInfo.Status)
	// update task/subtask, and finish subtask, so we can go to next stage
	subtasks := make([]*proto.Subtask, 0, len(subtaskMetas))
	for _, m := range subtaskMetas {
		subtasks = append(subtasks, proto.NewSubtask(task.Step, task.ID, task.Type, "", 1, m, 0))
	}
	_, err = manager.UpdateTaskAndAddSubTasks(ctx, task, subtasks, proto.TaskStatePending)
	require.NoError(t, err)
	gotSubtasks, err := manager.GetSubtasksForImportInto(ctx, taskID, importinto.StepImport)
	require.NoError(t, err)
	for _, s := range gotSubtasks {
		require.NoError(t, manager.FinishSubtask(ctx, s.ExecID, s.ID, []byte("{}")))
	}
	// to post-process stage, job should be running and in validating step
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, d, task, serverInfos, ext.GetNextStep(task))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 1)
	task.Step = ext.GetNextStep(task)
	require.Equal(t, importinto.StepPostProcess, task.Step)
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "running", gotJobInfo.Status)
	require.Equal(t, "validating", gotJobInfo.Step)
	// on next stage, job should be finished
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, d, task, serverInfos, ext.GetNextStep(task))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 0)
	task.Step = ext.GetNextStep(task)
	require.Equal(t, proto.StepDone, task.Step)
	require.NoError(t, ext.OnDone(ctx, d, task))
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "finished", gotJobInfo.Status)

	// create another job, start it, and fail it.
	jobID, err = importer.CreateJob(ctx, conn, "test", "t", 1,
		"root", &importer.ImportParameters{}, 123)
	require.NoError(t, err)
	logicalPlan.JobID = jobID
	bs, err = logicalPlan.ToTaskMeta()
	require.NoError(t, err)
	task.Meta = bs
	require.NoError(t, importer.StartJob(ctx, conn, jobID, importer.JobStepImporting))
	task.Error = errors.New("met error")
	require.NoError(t, ext.OnDone(ctx, d, task))
	require.NoError(t, err)
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "failed", gotJobInfo.Status)

	// create another job, start it, and cancel it.
	jobID, err = importer.CreateJob(ctx, conn, "test", "t", 1,
		"root", &importer.ImportParameters{}, 123)
	require.NoError(t, err)
	logicalPlan.JobID = jobID
	bs, err = logicalPlan.ToTaskMeta()
	require.NoError(t, err)
	task.Meta = bs
	require.NoError(t, importer.StartJob(ctx, conn, jobID, importer.JobStepImporting))
	task.Error = errors.New("cancelled by user")
	require.NoError(t, ext.OnDone(ctx, d, task))
	require.NoError(t, err)
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "cancelled", gotJobInfo.Status)
}

func TestSchedulerExtGlobalSort(t *testing.T) {
	// Domain start scheduler manager automatically, we need to disable it as
	// we test import task management in this case.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/disableSchedulerManager", "return(true)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/disableSchedulerManager"))
	})
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctx := context.WithValue(context.Background(), "etcd", true)
	ctx = util.WithInternalSourceType(ctx, "taskManager")
	mgr := storage.NewTaskManager(pool)
	storage.SetTaskManager(mgr)
	sch, err := scheduler.NewManager(util.WithInternalSourceType(ctx, "scheduler"), mgr, "host:port")
	require.NoError(t, err)

	// create job
	conn := tk.Session().(sqlexec.SQLExecutor)
	jobID, err := importer.CreateJob(ctx, conn, "test", "t", 1,
		"root", &importer.ImportParameters{}, 123)
	require.NoError(t, err)
	gotJobInfo, err := importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "pending", gotJobInfo.Status)
	logicalPlan := &importinto.LogicalPlan{
		JobID: jobID,
		Plan: importer.Plan{
			Path:   "gs://test-load/*.csv",
			Format: "csv",
			DBName: "test",
			TableInfo: &model.TableInfo{
				Name:  model.NewCIStr("t"),
				State: model.StatePublic,
			},
			DisableTiKVImportMode: true,
			CloudStorageURI:       "gs://sort-bucket",
			InImportInto:          true,
		},
		Stmt:              `IMPORT INTO db.tb FROM 'gs://test-load/*.csv?endpoint=xxx'`,
		EligibleInstances: []*infosync.ServerInfo{{ID: "1"}},
		ChunkMap: map[int32][]importinto.Chunk{
			1: {{Path: "gs://test-load/1.csv"}},
			2: {{Path: "gs://test-load/2.csv"}},
		},
	}
	bs, err := logicalPlan.ToTaskMeta()
	require.NoError(t, err)
	task := &proto.Task{
		Type:            proto.ImportInto,
		Meta:            bs,
		Step:            proto.StepInit,
		State:           proto.TaskStatePending,
		StateUpdateTime: time.Now(),
	}
	manager, err := storage.GetTaskManager()
	require.NoError(t, err)
	taskMeta, err := json.Marshal(task)
	require.NoError(t, err)
	taskID, err := manager.CreateTask(ctx, importinto.TaskKey(jobID), proto.ImportInto, 1, taskMeta)
	require.NoError(t, err)
	task.ID = taskID

	// to encode-sort stage, job should be running
	d := sch.MockScheduler(task)
	ext := importinto.ImportSchedulerExt{
		GlobalSort: true,
	}
	serverInfos, _, err := ext.GetEligibleInstances(context.Background(), task)
	require.NoError(t, err)
	subtaskMetas, err := ext.OnNextSubtasksBatch(ctx, d, task, serverInfos, ext.GetNextStep(task))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 2)
	task.Step = ext.GetNextStep(task)
	require.Equal(t, importinto.StepEncodeAndSort, task.Step)
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "running", gotJobInfo.Status)
	require.Equal(t, "global-sorting", gotJobInfo.Step)
	// update task/subtask, and finish subtask, so we can go to next stage
	subtasks := make([]*proto.Subtask, 0, len(subtaskMetas))
	for _, m := range subtaskMetas {
		subtasks = append(subtasks, proto.NewSubtask(task.Step, task.ID, task.Type, "", 1, m, 0))
	}
	_, err = manager.UpdateTaskAndAddSubTasks(ctx, task, subtasks, proto.TaskStatePending)
	require.NoError(t, err)
	gotSubtasks, err := manager.GetSubtasksForImportInto(ctx, taskID, task.Step)
	require.NoError(t, err)
	sortStepMeta := &importinto.ImportStepMeta{
		SortedDataMeta: &external.SortedKVMeta{
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
		SortedIndexMetas: map[int64]*external.SortedKVMeta{
			1: {
				StartKey:    []byte("ia"),
				EndKey:      []byte("ic"),
				TotalKVSize: 12,
				MultipleFilesStats: []external.MultipleFilesStat{
					{
						Filenames: [][2]string{
							{"gs://sort-bucket/index/1", "gs://sort-bucket/index/1.stat"},
						},
					},
				},
			},
		},
	}
	sortStepMetaBytes, err := json.Marshal(sortStepMeta)
	require.NoError(t, err)
	for _, s := range gotSubtasks {
		require.NoError(t, manager.FinishSubtask(ctx, s.ExecID, s.ID, sortStepMetaBytes))
	}

	// to merge-sort stage
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/importinto/forceMergeSort", `return("data")`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/importinto/forceMergeSort"))
	})
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, d, task, serverInfos, ext.GetNextStep(task))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 1)
	task.Step = ext.GetNextStep(task)
	require.Equal(t, importinto.StepMergeSort, task.Step)
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "running", gotJobInfo.Status)
	require.Equal(t, "global-sorting", gotJobInfo.Step)
	// update task/subtask, and finish subtask, so we can go to next stage
	subtasks = make([]*proto.Subtask, 0, len(subtaskMetas))
	for _, m := range subtaskMetas {
		subtasks = append(subtasks, proto.NewSubtask(task.Step, task.ID, task.Type, "", 1, m, 0))
	}
	_, err = manager.UpdateTaskAndAddSubTasks(ctx, task, subtasks, proto.TaskStatePending)
	require.NoError(t, err)
	gotSubtasks, err = manager.GetSubtasksForImportInto(ctx, taskID, task.Step)
	require.NoError(t, err)
	mergeSortStepMeta := &importinto.MergeSortStepMeta{
		KVGroup: "data",
		SortedKVMeta: external.SortedKVMeta{
			StartKey:    []byte("ta"),
			EndKey:      []byte("tc"),
			TotalKVSize: 12,
		},
		DataFiles: []string{"gs://sort-bucket/data/1"},
	}
	mergeSortStepMetaBytes, err := json.Marshal(mergeSortStepMeta)
	require.NoError(t, err)
	for _, s := range gotSubtasks {
		require.NoError(t, manager.FinishSubtask(ctx, s.ExecID, s.ID, mergeSortStepMetaBytes))
	}

	// to write-and-ingest stage
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/importinto/mockWriteIngestSpecs", "return(true)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/importinto/mockWriteIngestSpecs"))
	})
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, d, task, serverInfos, ext.GetNextStep(task))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 2)
	task.Step = ext.GetNextStep(task)
	require.Equal(t, importinto.StepWriteAndIngest, task.Step)
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "running", gotJobInfo.Status)
	require.Equal(t, "importing", gotJobInfo.Step)
	// on next stage, to post-process stage
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, d, task, serverInfos, ext.GetNextStep(task))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 1)
	task.Step = ext.GetNextStep(task)
	require.Equal(t, importinto.StepPostProcess, task.Step)
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "running", gotJobInfo.Status)
	require.Equal(t, "validating", gotJobInfo.Step)
	// next stage, done
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, d, task, serverInfos, ext.GetNextStep(task))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 0)
	task.Step = ext.GetNextStep(task)
	require.Equal(t, proto.StepDone, task.Step)
}
