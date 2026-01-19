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
	"fmt"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/ngaut/pools"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestBackfillingSchedulerLocalMode(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sch, err := ddl.NewBackfillingSchedulerForTest(dom.DDL())
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockWriterMemSizeInKB", "return(1048576)"))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockWriterMemSizeInKB")
	/// 1. test partition table.
	tk.MustExec("create table tp1(id int primary key, v int, key idx (id)) PARTITION BY RANGE (id) (\n    " +
		"PARTITION p0 VALUES LESS THAN (10),\n" +
		"PARTITION p1 VALUES LESS THAN (100),\n" +
		"PARTITION p2 VALUES LESS THAN (1000),\n" +
		"PARTITION p3 VALUES LESS THAN MAXVALUE\n);")
	tk.MustExec("insert into tp1 values (1, 0), (11, 0), (101, 0), (1001, 0);")
	task, server := createAddIndexTask(t, dom, "test", "tp1", proto.Backfill, false)
	require.Nil(t, server)
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("tp1"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()

	// 1.1 OnNextSubtasksBatch
	task.Step = sch.GetNextStep(&task.TaskBase)
	require.Equal(t, proto.BackfillStepReadIndex, task.Step)
	execIDs := []string{":4000"}
	ctx := util.WithInternalSourceType(context.Background(), "backfill")
	metas, err := sch.OnNextSubtasksBatch(ctx, nil, task, execIDs, task.Step)
	require.NoError(t, err)
	require.Equal(t, len(tblInfo.Partition.Definitions), len(metas))
	for i, par := range tblInfo.Partition.Definitions {
		var subTask ddl.BackfillSubTaskMeta
		require.NoError(t, json.Unmarshal(metas[i], &subTask))
		require.Equal(t, par.ID, subTask.PhysicalTableID)
	}

	// 1.2 test partition table OnNextSubtasksBatch after BackfillStepReadIndex
	task.State = proto.TaskStateRunning
	task.Step = sch.GetNextStep(&task.TaskBase)
	require.Equal(t, proto.StepDone, task.Step)
	metas, err = sch.OnNextSubtasksBatch(ctx, nil, task, execIDs, task.Step)
	require.NoError(t, err)
	require.Len(t, metas, 0)

	// 1.3 test partition table OnDone.
	err = sch.OnDone(context.Background(), nil, task)
	require.NoError(t, err)

	/// 2. test non partition table.
	// 2.1 empty table
	tk.MustExec("create table t1(id int primary key, v int)")
	task, server = createAddIndexTask(t, dom, "test", "t1", proto.Backfill, false)
	require.Nil(t, server)
	metas, err = sch.OnNextSubtasksBatch(ctx, nil, task, execIDs, task.Step)
	require.NoError(t, err)
	require.Equal(t, 0, len(metas))
	// 2.2 non empty table.
	tk.MustExec("create table t2(id bigint auto_random primary key)")
	tk.MustExec("insert into t2 values (), (), (), (), (), ()")
	tk.MustExec("insert into t2 values (), (), (), (), (), ()")
	tk.MustExec("insert into t2 values (), (), (), (), (), ()")
	tk.MustExec("insert into t2 values (), (), (), (), (), ()")
	task, server = createAddIndexTask(t, dom, "test", "t2", proto.Backfill, false)
	require.Nil(t, server)
	// 2.2.1 stepInit
	task.Step = sch.GetNextStep(&task.TaskBase)
	metas, err = sch.OnNextSubtasksBatch(ctx, nil, task, execIDs, task.Step)
	require.NoError(t, err)
	require.Equal(t, 1, len(metas))
	require.Equal(t, proto.BackfillStepReadIndex, task.Step)
	// 2.2.2 BackfillStepReadIndex
	task.State = proto.TaskStateRunning
	task.Step = sch.GetNextStep(&task.TaskBase)
	require.Equal(t, proto.StepDone, task.Step)
	metas, err = sch.OnNextSubtasksBatch(ctx, nil, task, execIDs, task.Step)
	require.NoError(t, err)
	require.Equal(t, 0, len(metas))
}

func TestCalculateRegionBatch(t *testing.T) {
	// Test calculate in cloud storage.
	batchCnt := ddl.CalculateRegionBatch(100, 8, false)
	require.Equal(t, 13, batchCnt)
	batchCnt = ddl.CalculateRegionBatch(2, 8, false)
	require.Equal(t, 1, batchCnt)
	batchCnt = ddl.CalculateRegionBatch(8, 8, false)
	require.Equal(t, 1, batchCnt)

	// Test calculate in local storage.
	batchCnt = ddl.CalculateRegionBatch(100, 8, true)
	require.Equal(t, 100, batchCnt)
	batchCnt = ddl.CalculateRegionBatch(2, 8, true)
	require.Equal(t, 2, batchCnt)
	batchCnt = ddl.CalculateRegionBatch(24, 8, true)
	require.Equal(t, 24, batchCnt)
	batchCnt = ddl.CalculateRegionBatch(1000, 8, true)
	require.Equal(t, 334, batchCnt)
	batchCnt = ddl.CalculateRegionBatch(1000, 2, true)
	require.Equal(t, 500, batchCnt)
	batchCnt = ddl.CalculateRegionBatch(200, 3, true)
	require.Equal(t, 100, batchCnt)
}

func TestBackfillingSchedulerGlobalSortMode(t *testing.T) {
	// init test env.
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	baseCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ctx := context.WithValue(baseCtx, "etcd", true)
	ctx = util.WithInternalSourceType(ctx, "handle")
	mgr := storage.NewTaskManager(pool)
	storage.SetTaskManager(mgr)
	schManager := scheduler.NewManager(util.WithInternalSourceType(ctx, "scheduler"), store, mgr, "host:port", proto.NodeResourceForTest)

	tk.MustExec("use test")
	tk.MustExec("create table t1(id bigint auto_random primary key)")
	tk.MustExec("insert into t1 values (), (), (), (), (), ()")
	tk.MustExec("insert into t1 values (), (), (), (), (), ()")
	tk.MustExec("insert into t1 values (), (), (), (), (), ()")
	tk.MustExec("insert into t1 values (), (), (), (), (), ()")
	task, server := createAddIndexTask(t, dom, "test", "t1", proto.Backfill, true)
	require.NotNil(t, server)

	sch := schManager.MockScheduler(task)
	ext, err := ddl.NewBackfillingSchedulerForTest(dom.DDL())
	require.NoError(t, err)
	ext.(*ddl.LitBackfillScheduler).GlobalSort = true
	sch.Extension = ext

	taskID, err := mgr.CreateTask(ctx, task.Key, proto.Backfill, "", 1, "", 0, proto.ExtraParams{}, task.Meta)
	require.NoError(t, err)
	task.ID = taskID
	execIDs := []string{":4000"}

	// 1. to read-index stage
	subtaskMetas, err := sch.OnNextSubtasksBatch(ctx, sch, task, execIDs, sch.GetNextStep(&task.TaskBase))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 1)
	nextStep := ext.GetNextStep(&task.TaskBase)
	require.Equal(t, proto.BackfillStepReadIndex, nextStep)
	// update task/subtask, and finish subtask, so we can go to next stage
	subtasks := make([]*proto.Subtask, 0, len(subtaskMetas))
	for i, m := range subtaskMetas {
		subtasks = append(subtasks, proto.NewSubtask(nextStep, task.ID, task.Type, "", 1, m, i+1))
	}
	err = mgr.SwitchTaskStep(ctx, task, proto.TaskStatePending, nextStep, subtasks)
	require.NoError(t, err)
	task.Step = nextStep
	gotSubtasks, err := mgr.GetSubtasksWithHistory(ctx, taskID, proto.BackfillStepReadIndex)
	require.NoError(t, err)

	// update meta, same as import into.
	sortStepMeta := &ddl.BackfillSubTaskMeta{
		MetaGroups: []*external.SortedKVMeta{{
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
		}},
	}
	sortStepMetaBytes, err := json.Marshal(sortStepMeta)
	require.NoError(t, err)
	for _, s := range gotSubtasks {
		require.NoError(t, mgr.FinishSubtask(ctx, s.ExecID, s.ID, sortStepMetaBytes))
	}
	// 2. to merge-sort stage.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/forceMergeSort", `return()`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/forceMergeSort"))
	})
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, sch, task, execIDs, ext.GetNextStep(&task.TaskBase))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 1)
	nextStep = ext.GetNextStep(&task.TaskBase)
	require.Equal(t, proto.BackfillStepMergeSort, nextStep)

	// update meta, same as import into.
	subtasks = make([]*proto.Subtask, 0, len(subtaskMetas))
	for i, m := range subtaskMetas {
		subtasks = append(subtasks, proto.NewSubtask(nextStep, task.ID, task.Type, "", 1, m, i+1))
	}
	err = mgr.SwitchTaskStepInBatch(ctx, task, proto.TaskStatePending, nextStep, subtasks)
	require.NoError(t, err)
	task.Step = nextStep
	gotSubtasks, err = mgr.GetSubtasksWithHistory(ctx, taskID, task.Step)
	require.NoError(t, err)
	mergeSortStepMeta := &ddl.BackfillSubTaskMeta{
		MetaGroups: []*external.SortedKVMeta{{
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
		}},
	}
	mergeSortStepMetaBytes, err := json.Marshal(mergeSortStepMeta)
	require.NoError(t, err)
	for _, s := range gotSubtasks {
		require.NoError(t, mgr.FinishSubtask(ctx, s.ExecID, s.ID, mergeSortStepMetaBytes))
	}
	// 3. to write&ingest stage.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockWriteIngest", "return(true)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockWriteIngest"))
	})
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, sch, task, execIDs, ext.GetNextStep(&task.TaskBase))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 1)
	task.Step = ext.GetNextStep(&task.TaskBase)
	require.Equal(t, proto.BackfillStepWriteAndIngest, task.Step)
	// 4. to done stage.
	subtaskMetas, err = ext.OnNextSubtasksBatch(ctx, sch, task, execIDs, ext.GetNextStep(&task.TaskBase))
	require.NoError(t, err)
	require.Len(t, subtaskMetas, 0)
	task.Step = ext.GetNextStep(&task.TaskBase)
	require.Equal(t, proto.StepDone, task.Step)
}

func TestGetNextStep(t *testing.T) {
	task := &proto.Task{
		TaskBase: proto.TaskBase{Step: proto.StepInit},
	}
	ext := &ddl.LitBackfillScheduler{}

	// 1. local mode
	for _, nextStep := range []proto.Step{proto.BackfillStepReadIndex, proto.StepDone} {
		require.Equal(t, nextStep, ext.GetNextStep(&task.TaskBase))
		task.Step = nextStep
	}
	// 2. global sort mode
	ext = &ddl.LitBackfillScheduler{GlobalSort: true}
	task.Step = proto.StepInit
	for _, nextStep := range []proto.Step{proto.BackfillStepReadIndex, proto.BackfillStepMergeSort, proto.BackfillStepWriteAndIngest} {
		require.Equal(t, nextStep, ext.GetNextStep(&task.TaskBase))
		task.Step = nextStep
	}

	// 3. merge temp index
	task = &proto.Task{
		TaskBase: proto.TaskBase{Step: proto.StepInit},
	}
	ext = &ddl.LitBackfillScheduler{MergeTempIndex: true}
	for _, nextStep := range []proto.Step{proto.BackfillStepMergeTempIndex, proto.StepDone} {
		require.Equal(t, nextStep, ext.GetNextStep(&task.TaskBase))
		task.Step = nextStep
	}
}

func createAddIndexTask(t *testing.T,
	dom *domain.Domain,
	dbName,
	tblName string,
	taskType proto.TaskType,
	useGlobalSort bool) (*proto.Task, *fakestorage.Server) {
	var (
		gcsHost = "127.0.0.1"
		gcsPort = uint16(4447)
		// for fake gcs server, we must use this endpoint format
		// NOTE: must end with '/'
		gcsEndpointFormat = "http://%s:%d/storage/v1/"
		gcsEndpoint       = fmt.Sprintf(gcsEndpointFormat, gcsHost, gcsPort)
		server            *fakestorage.Server
	)

	db, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr(dbName))
	require.True(t, ok)
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr(dbName), ast.NewCIStr(tblName))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	defaultSQLMode, err := mysql.GetSQLMode(mysql.DefaultSQLMode)
	require.NoError(t, err)

	taskMeta := &ddl.BackfillTaskMeta{
		Job: model.Job{
			ID:       time.Now().UnixNano(),
			SchemaID: db.ID,
			TableID:  tblInfo.ID,
			ReorgMeta: &model.DDLReorgMeta{
				SQLMode:     defaultSQLMode,
				Location:    &model.TimeZoneLocation{Name: time.UTC.String(), Offset: 0},
				ReorgTp:     model.ReorgTypeIngest,
				IsDistReorg: true,
			},
			Version: model.JobVersion2,
		},
		EleIDs:     []int64{10},
		EleTypeKey: meta.IndexElementKey,
	}
	args := &model.ModifyIndexArgs{
		IndexArgs: []*model.IndexArg{{
			IndexName: ast.NewCIStr("idx"),
			Global:    false,
		}},
		OpType: model.OpAddIndex,
	}
	taskMeta.Job.FillArgs(args)
	_, err = taskMeta.Job.Encode(true)
	require.NoError(t, err)
	if useGlobalSort {
		var err error
		opt := fakestorage.Options{
			Scheme:     "http",
			Host:       gcsHost,
			Port:       gcsPort,
			PublicHost: gcsHost,
		}
		server, err = fakestorage.NewServerWithOptions(opt)
		t.Cleanup(func() {
			server.Stop()
		})
		require.NoError(t, err)
		server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})
		taskMeta.CloudStorageURI = fmt.Sprintf("gs://sorted/addindex?endpoint=%s&access-key=xxxxxx&secret-access-key=xxxxxx", gcsEndpoint)
	}

	taskMetaBytes, err := json.Marshal(taskMeta)
	require.NoError(t, err)
	ks := ""
	if kerneltype.IsNextGen() {
		ks = keyspace.System
	}

	task := &proto.Task{
		TaskBase: proto.TaskBase{
			ID:            time.Now().UnixMicro(),
			Type:          taskType,
			Step:          proto.StepInit,
			State:         proto.TaskStatePending,
			RequiredSlots: 16,
			Keyspace:      ks,
		},
		Meta:            taskMetaBytes,
		StartTime:       time.Now(),
		StateUpdateTime: time.Now(),
	}

	return task, server
}

func TestBackfillTaskMetaVersion(t *testing.T) {
	// Test the default version.
	meta := &ddl.BackfillTaskMeta{}
	require.Equal(t, ddl.BackfillTaskMetaVersion0, meta.Version)

	// Test the new version.
	meta = &ddl.BackfillTaskMeta{
		Version: ddl.BackfillTaskMetaVersion1,
	}
	require.Equal(t, ddl.BackfillTaskMetaVersion1, meta.Version)
}
