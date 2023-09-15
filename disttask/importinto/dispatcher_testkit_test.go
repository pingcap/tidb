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
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/disttask/importinto"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestDispatcherExt(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctx := context.WithValue(context.Background(), "etcd", true)
	mgr := storage.NewTaskManager(util.WithInternalSourceType(ctx, "taskManager"), pool)
	storage.SetTaskManager(mgr)
	dsp, err := dispatcher.NewManager(util.WithInternalSourceType(ctx, "dispatcher"), mgr, "host:port")
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
	taskID, err := manager.AddNewGlobalTask(importinto.TaskKey(jobID), proto.ImportInto, 1, taskMeta)
	require.NoError(t, err)
	task.ID = taskID

	// to import stage, job should be running
	d := dsp.MockDispatcher(task)
	ext := importinto.ImportDispatcherExt{}
	subtaskMetas, err := ext.OnNextSubtasksBatch(ctx, d, task)
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
		subtasks = append(subtasks, proto.NewSubtask(task.Step, task.ID, task.Type, "", m))
	}
	_, err = manager.UpdateGlobalTaskAndAddSubTasks(task, subtasks, proto.TaskStatePending)
	require.NoError(t, err)
	gotSubtasks, err := manager.GetSubtasksForImportInto(taskID, importinto.StepImport)
	require.NoError(t, err)
	for _, s := range gotSubtasks {
		require.NoError(t, manager.FinishSubtask(s.ID, []byte("{}")))
	}
	// to post-process stage, job should be running and in validating step
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, d, task)
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 1)
	task.Step = ext.GetNextStep(task)
	require.Equal(t, importinto.StepPostProcess, task.Step)
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "running", gotJobInfo.Status)
	require.Equal(t, "validating", gotJobInfo.Step)
	// on next stage, job should be finished
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, d, task)
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 0)
	task.Step = ext.GetNextStep(task)
	require.Equal(t, proto.StepDone, task.Step)
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
	// Set step to StepPostProcess to skip the rollback sql.
	task.Step = importinto.StepPostProcess
	require.NoError(t, importer.StartJob(ctx, conn, jobID))
	_, err = ext.OnErrStage(ctx, d, task, []error{errors.New("test")})
	require.NoError(t, err)
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID, "root", true)
	require.NoError(t, err)
	require.Equal(t, "failed", gotJobInfo.Status)
}
