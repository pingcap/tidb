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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestBackfillingDispatcher(t *testing.T) {
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
	gTask := createAddIndexGlobalTask(t, dom, "test", "tp1", proto.Backfill)
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tp1"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()

	// 1.1 OnNextSubtasksBatch
	gTask.Step = dsp.GetNextStep(nil, gTask)
	require.Equal(t, proto.StepOne, gTask.Step)
	metas, err := dsp.OnNextSubtasksBatch(context.Background(), nil, gTask, gTask.Step)
	require.NoError(t, err)
	require.Equal(t, len(tblInfo.Partition.Definitions), len(metas))
	for i, par := range tblInfo.Partition.Definitions {
		var subTask ddl.BackfillSubTaskMeta
		require.NoError(t, json.Unmarshal(metas[i], &subTask))
		require.Equal(t, par.ID, subTask.PhysicalTableID)
	}

	// 1.2 test partition table OnNextSubtasksBatch after StepInit finished.
	gTask.State = proto.TaskStateRunning
	gTask.Step = dsp.GetNextStep(nil, gTask)
	require.Equal(t, proto.StepThree, gTask.Step)
	// for partition table, we will not generate subtask for StepThree.
	metas, err = dsp.OnNextSubtasksBatch(context.Background(), nil, gTask, gTask.Step)
	require.NoError(t, err)
	require.Len(t, metas, 0)
	gTask.Step = dsp.GetNextStep(nil, gTask)
	require.Equal(t, proto.StepDone, gTask.Step)
	metas, err = dsp.OnNextSubtasksBatch(context.Background(), nil, gTask, gTask.Step)
	require.NoError(t, err)
	require.Len(t, metas, 0)

	// 1.3 test partition table OnErrStage.
	errMeta, err := dsp.OnErrStage(context.Background(), nil, gTask, []error{errors.New("mockErr")})
	require.NoError(t, err)
	require.Nil(t, errMeta)

	errMeta, err = dsp.OnErrStage(context.Background(), nil, gTask, []error{errors.New("mockErr")})
	require.NoError(t, err)
	require.Nil(t, errMeta)

	/// 2. test non partition table.
	// 2.1 empty table
	tk.MustExec("create table t1(id int primary key, v int)")
	gTask = createAddIndexGlobalTask(t, dom, "test", "t1", proto.Backfill)
	metas, err = dsp.OnNextSubtasksBatch(context.Background(), nil, gTask, gTask.Step)
	require.NoError(t, err)
	require.Equal(t, 0, len(metas))
	// 2.2 non empty table.
	tk.MustExec("create table t2(id bigint auto_random primary key)")
	tk.MustExec("insert into t2 values (), (), (), (), (), ()")
	tk.MustExec("insert into t2 values (), (), (), (), (), ()")
	tk.MustExec("insert into t2 values (), (), (), (), (), ()")
	tk.MustExec("insert into t2 values (), (), (), (), (), ()")
	gTask = createAddIndexGlobalTask(t, dom, "test", "t2", proto.Backfill)
	// 2.2.1 stepInit
	gTask.Step = dsp.GetNextStep(nil, gTask)
	metas, err = dsp.OnNextSubtasksBatch(context.Background(), nil, gTask, gTask.Step)
	require.NoError(t, err)
	require.Equal(t, 1, len(metas))
	require.Equal(t, proto.StepOne, gTask.Step)
	// 2.2.2 stepOne
	gTask.State = proto.TaskStateRunning
	gTask.Step = dsp.GetNextStep(nil, gTask)
	require.Equal(t, proto.StepThree, gTask.Step)
	metas, err = dsp.OnNextSubtasksBatch(context.Background(), nil, gTask, gTask.Step)
	require.NoError(t, err)
	require.Equal(t, 1, len(metas))
	gTask.Step = dsp.GetNextStep(nil, gTask)
	require.Equal(t, proto.StepDone, gTask.Step)
	metas, err = dsp.OnNextSubtasksBatch(context.Background(), nil, gTask, gTask.Step)
	require.NoError(t, err)
	require.Equal(t, 0, len(metas))
}

func createAddIndexGlobalTask(t *testing.T, dom *domain.Domain, dbName, tblName string, taskType proto.TaskType) *proto.Task {
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
