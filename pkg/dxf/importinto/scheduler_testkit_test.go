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
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
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
	sch := scheduler.NewManager(util.WithInternalSourceType(ctx, "scheduler"), store, mgr, "host:port", proto.NodeResourceForTest)

	// create job
	conn := tk.Session().GetSQLExecutor()
	jobID, err := importer.CreateJob(ctx, conn, "test", "t", 1,
		"root", "", &importer.ImportParameters{}, 123)
	require.NoError(t, err)
	gotJobInfo, err := importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "pending", gotJobInfo.Status)
	logicalPlan := &importinto.LogicalPlan{
		JobID: jobID,
		Plan: importer.Plan{
			DBName: "test",
			TableInfo: &model.TableInfo{
				Name: ast.NewCIStr("t"),
			},
			DisableTiKVImportMode: true,
		},
		Stmt:              `IMPORT INTO db.tb FROM 'gs://test-load/*.csv?endpoint=xxx'`,
		EligibleInstances: []*serverinfo.ServerInfo{{StaticInfo: serverinfo.StaticInfo{ID: "1"}}},
		ChunkMap:          map[int32][]importer.Chunk{1: {{Path: "gs://test-load/1.csv"}}},
	}
	bs, err := logicalPlan.ToTaskMeta()
	require.NoError(t, err)
	task := &proto.Task{
		TaskBase: proto.TaskBase{
			Type:  proto.TaskTypeExample,
			Step:  proto.StepInit,
			State: proto.TaskStatePending,
		},
		Meta:            bs,
		StateUpdateTime: time.Now(),
	}
	manager, err := storage.GetTaskManager()
	require.NoError(t, err)
	taskID, err := manager.CreateTask(ctx, importinto.TaskKey(jobID), proto.ImportInto, "", 1, "", 1, proto.ExtraParams{}, bs)
	require.NoError(t, err)
	task.ID = taskID

	// to import stage, job should be running
	d := sch.MockScheduler(task)
	ext := importinto.NewImportSchedulerForTest(false, task, scheduler.NewParamForTest(manager, store))
	subtaskMetas, err := ext.OnNextSubtasksBatch(ctx, d, task, []string{":4000"}, ext.GetNextStep(&task.TaskBase))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 1)
	nextStep := ext.GetNextStep(&task.TaskBase)
	require.Equal(t, proto.ImportStepImport, nextStep)
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "running", gotJobInfo.Status)
	// update task/subtask, and finish subtask, so we can go to next stage
	subtasks := make([]*proto.Subtask, 0, len(subtaskMetas))
	for i, m := range subtaskMetas {
		subtasks = append(subtasks, proto.NewSubtask(nextStep, task.ID, task.Type, "", 1, m, i+1))
	}
	err = manager.SwitchTaskStep(ctx, task, proto.TaskStateRunning, nextStep, subtasks)
	require.NoError(t, err)
	task.Step = nextStep
	gotSubtasks, err := manager.GetSubtasksWithHistory(ctx, taskID, proto.ImportStepImport)
	require.NoError(t, err)
	for _, s := range gotSubtasks {
		require.NoError(t, manager.FinishSubtask(ctx, s.ExecID, s.ID, []byte("{}")))
	}
	// to post-process stage, job should be running and in validating step
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, d, task, []string{":4000"}, ext.GetNextStep(&task.TaskBase))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 1)
	task.Step = ext.GetNextStep(&task.TaskBase)
	require.Equal(t, proto.ImportStepPostProcess, task.Step)
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "running", gotJobInfo.Status)
	require.Equal(t, "validating", gotJobInfo.Step)
	// on next stage, job should be finished
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, d, task, []string{":4000"}, ext.GetNextStep(&task.TaskBase))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 0)
	task.Step = ext.GetNextStep(&task.TaskBase)
	require.Equal(t, proto.StepDone, task.Step)
	require.NoError(t, ext.OnDone(ctx, d, task))
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "finished", gotJobInfo.Status)

	// create another job, start it, and fail it.
	jobID, err = importer.CreateJob(ctx, conn, "test", "t", 1,
		"root", "", &importer.ImportParameters{}, 123)
	require.NoError(t, err)
	logicalPlan.JobID = jobID
	bs, err = logicalPlan.ToTaskMeta()
	require.NoError(t, err)
	task.Meta = bs
	require.NoError(t, importer.StartJob(ctx, conn, jobID, importer.JobStepImporting))
	task.State = proto.TaskStateReverting
	task.Error = errors.New("met error")
	require.NoError(t, ext.OnDone(ctx, d, task))
	require.NoError(t, err)
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "failed", gotJobInfo.Status)

	// create another job, start it, and cancel it.
	jobID, err = importer.CreateJob(ctx, conn, "test", "t", 1,
		"root", "", &importer.ImportParameters{}, 123)
	require.NoError(t, err)
	logicalPlan.JobID = jobID
	bs, err = logicalPlan.ToTaskMeta()
	require.NoError(t, err)
	task.Meta = bs
	require.NoError(t, importer.StartJob(ctx, conn, jobID, importer.JobStepImporting))
	task.State = proto.TaskStateReverting
	task.Error = errors.New("cancelled by user")
	require.NoError(t, ext.OnDone(ctx, d, task))
	require.NoError(t, err)
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "cancelled", gotJobInfo.Status)
}

func TestSchedulerExtGlobalSort(t *testing.T) {
	host := "127.0.0.1"
	port := uint16(4448)
	opt := fakestorage.Options{
		Scheme:     "http",
		Host:       host,
		Port:       port,
		PublicHost: host,
	}
	gcsEndpoint := fmt.Sprintf("http://%s:%d/storage/v1/", host, port)
	sortStorageURI := fmt.Sprintf("gs://sort-bucket/import?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", gcsEndpoint)
	server, err := fakestorage.NewServerWithOptions(opt)
	defer server.Stop()
	require.NoError(t, err)
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sort-bucket"})
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "test-load"})

	// Domain start scheduler manager automatically, we need to disable it as
	// we test import task management in this case.
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)")
	store := testkit.CreateMockStore(t)
	keyspace := store.GetKeyspace()
	scope := handle.GetTargetScope()
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctx := context.WithValue(context.Background(), "etcd", true)
	ctx = util.WithInternalSourceType(ctx, "taskManager")
	mgr := storage.NewTaskManager(pool)
	storage.SetTaskManager(mgr)
	sch := scheduler.NewManager(util.WithInternalSourceType(ctx, "scheduler"), store, mgr, "host:port", proto.NodeResourceForTest)
	require.NoError(t, mgr.InitMeta(ctx, ":4000", scope))

	// create job
	conn := tk.Session().GetSQLExecutor()
	jobID, err := importer.CreateJob(ctx, conn, "test", "t", 1,
		"root", "", &importer.ImportParameters{}, 123)
	require.NoError(t, err)
	gotJobInfo, err := importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "pending", gotJobInfo.Status)
	logicalPlan := &importinto.LogicalPlan{
		JobID: jobID,
		Plan: importer.Plan{
			Path:   fmt.Sprintf("gs://test-load/*.csv?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", gcsEndpoint),
			Format: "csv",
			DBName: "test",
			TableInfo: &model.TableInfo{
				Name:  ast.NewCIStr("t"),
				State: model.StatePublic,
			},
			DisableTiKVImportMode: true,
			CloudStorageURI:       sortStorageURI,
			InImportInto:          true,
		},
		Stmt:              `IMPORT INTO db.tb FROM 'gs://test-load/*.csv?endpoint=xxx'`,
		EligibleInstances: []*serverinfo.ServerInfo{{StaticInfo: serverinfo.StaticInfo{ID: "1"}}},
		ChunkMap: map[int32][]importer.Chunk{
			1: {{Path: "gs://test-load/1.csv"}},
			2: {{Path: "gs://test-load/2.csv"}},
		},
	}
	bs, err := logicalPlan.ToTaskMeta()
	require.NoError(t, err)
	task := &proto.Task{
		TaskBase: proto.TaskBase{
			Type:          proto.ImportInto,
			Step:          proto.StepInit,
			State:         proto.TaskStatePending,
			RequiredSlots: 16,
		},
		Meta:            bs,
		StateUpdateTime: time.Now(),
	}
	manager, err := storage.GetTaskManager()
	require.NoError(t, err)
	taskMeta, err := json.Marshal(task)
	require.NoError(t, err)
	taskID, err := manager.CreateTask(ctx, importinto.TaskKey(jobID), proto.ImportInto, keyspace, 1, scope, 1, proto.ExtraParams{}, taskMeta)
	require.NoError(t, err)
	task.ID = taskID

	// to encode-sort stage, job should be running
	d := sch.MockScheduler(task)
	ext := importinto.NewImportSchedulerForTest(true, task, scheduler.NewParamForTest(manager, store))
	subtaskMetas, err := ext.OnNextSubtasksBatch(ctx, d, task, []string{":4000"}, ext.GetNextStep(&task.TaskBase))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 2)
	nextStep := ext.GetNextStep(&task.TaskBase)
	require.Equal(t, proto.ImportStepEncodeAndSort, nextStep)
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "running", gotJobInfo.Status)
	require.Equal(t, "global-sorting", gotJobInfo.Step)
	// update task/subtask, and finish subtask, so we can go to next stage
	subtasks := make([]*proto.Subtask, 0, len(subtaskMetas))
	for i, m := range subtaskMetas {
		subtasks = append(subtasks, proto.NewSubtask(nextStep, task.ID, task.Type, "", 1, m, i+1))
	}
	err = manager.SwitchTaskStep(ctx, task, proto.TaskStatePending, nextStep, subtasks)
	task.Step = nextStep
	require.NoError(t, err)
	gotSubtasks, err := manager.GetSubtasksWithHistory(ctx, taskID, task.Step)
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
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/dxf/importinto/forceMergeSort", `return("data")`)
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, d, task, []string{":4000"}, ext.GetNextStep(&task.TaskBase))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 1)
	nextStep = ext.GetNextStep(&task.TaskBase)
	require.Equal(t, proto.ImportStepMergeSort, nextStep)
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "running", gotJobInfo.Status)
	require.Equal(t, "global-sorting", gotJobInfo.Step)
	// update task/subtask, and finish subtask, so we can go to next stage
	subtasks = make([]*proto.Subtask, 0, len(subtaskMetas))
	for i, m := range subtaskMetas {
		subtasks = append(subtasks, proto.NewSubtask(nextStep, task.ID, task.Type, "", 1, m, i+1))
	}
	err = manager.SwitchTaskStep(ctx, task, proto.TaskStatePending, nextStep, subtasks)
	require.NoError(t, err)
	task.Step = nextStep
	gotSubtasks, err = manager.GetSubtasksWithHistory(ctx, taskID, task.Step)
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
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/dxf/importinto/mockWriteIngestSpecs", "return(true)")
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, d, task, []string{":4000"}, ext.GetNextStep(&task.TaskBase))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 2)
	task.Step = ext.GetNextStep(&task.TaskBase)
	require.Equal(t, proto.ImportStepWriteAndIngest, task.Step)
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "running", gotJobInfo.Status)
	require.Equal(t, "importing", gotJobInfo.Step)
	// to collect-conflicts state
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, d, task, []string{":4000"}, ext.GetNextStep(&task.TaskBase))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 0)
	task.Step = ext.GetNextStep(&task.TaskBase)
	require.Equal(t, proto.ImportStepCollectConflicts, task.Step)
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "running", gotJobInfo.Status)
	require.Equal(t, "resolving-conflicts", gotJobInfo.Step)
	// to conflict-resolution state
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, d, task, []string{":4000"}, ext.GetNextStep(&task.TaskBase))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 0)
	task.Step = ext.GetNextStep(&task.TaskBase)
	require.Equal(t, proto.ImportStepConflictResolution, task.Step)
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "running", gotJobInfo.Status)
	require.Equal(t, "resolving-conflicts", gotJobInfo.Step)
	// on next stage, to post-process stage
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, d, task, []string{":4000"}, ext.GetNextStep(&task.TaskBase))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 1)
	task.Step = ext.GetNextStep(&task.TaskBase)
	require.Equal(t, proto.ImportStepPostProcess, task.Step)
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "running", gotJobInfo.Status)
	require.Equal(t, "validating", gotJobInfo.Step)
	// next stage, done
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, d, task, []string{":4000"}, ext.GetNextStep(&task.TaskBase))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 0)
	task.Step = ext.GetNextStep(&task.TaskBase)
	require.Equal(t, proto.StepDone, task.Step)
}
