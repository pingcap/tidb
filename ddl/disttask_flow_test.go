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
	"encoding/json"
	"testing"
	"time"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestBackfillFlowHandle(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	handler := ddl.NewLitBackfillFlowHandle(func() ddl.DDL {
		return dom.DDL()
	})

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// test normal table ProcessNormalFlow
	tk.MustExec("create table t1(id int primary key, v int)")
	gTask, gTaskMeta := createAddIndexGlobalTask(t, dom, "test", "t1", false)
	metas, err := handler.ProcessNormalFlow(nil, gTask)
	require.NoError(t, err)
	require.Equal(t, proto.StepOne, gTask.Step)
	require.Equal(t, 1, len(metas))
	var subTask ddl.LitBackfillSubTaskMeta
	require.NoError(t, json.Unmarshal(metas[0], &subTask))
	require.Equal(t, gTaskMeta.Job.TableID, subTask.PhysicalTableID)
	require.Equal(t, false, subTask.IsMergingIdx)

	// test normal table ProcessNormalFlow after step1 finished
	gTask.State = proto.TaskStateRunning
	metas, err = handler.ProcessNormalFlow(nil, gTask)
	require.NoError(t, err)
	require.Equal(t, 0, len(metas))

	// test normal table ProcessErrFlow
	errMeta, err := handler.ProcessErrFlow(nil, gTask, "mockErr")
	require.NoError(t, err)
	var rollbackMeta ddl.LitBackfillSubTaskRollbackMeta
	err = json.Unmarshal(errMeta, &rollbackMeta)
	require.NoError(t, err)
	require.Equal(t, false, rollbackMeta.IsMergingIdx)

	// test partition table ProcessNormalFlow
	tk.MustExec("create table tp1(id int primary key, v int) PARTITION BY RANGE (id) (\n    " +
		"PARTITION p0 VALUES LESS THAN (10),\n" +
		"PARTITION p1 VALUES LESS THAN (100),\n" +
		"PARTITION p2 VALUES LESS THAN (1000),\n" +
		"PARTITION p3 VALUES LESS THAN MAXVALUE\n);")
	gTask, gTaskMeta = createAddIndexGlobalTask(t, dom, "test", "tp1", false)
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tp1"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	metas, err = handler.ProcessNormalFlow(nil, gTask)
	require.NoError(t, err)
	require.Equal(t, proto.StepOne, gTask.Step)
	require.Equal(t, len(tblInfo.Partition.Definitions), len(metas))
	for i, par := range tblInfo.Partition.Definitions {
		var subTask ddl.LitBackfillSubTaskMeta
		require.NoError(t, json.Unmarshal(metas[i], &subTask))
		require.Equal(t, par.ID, subTask.PhysicalTableID)
		require.Equal(t, false, subTask.IsMergingIdx)
	}

	// test partition table ProcessNormalFlow after step1 finished
	gTask.State = proto.TaskStateRunning
	metas, err = handler.ProcessNormalFlow(nil, gTask)
	require.NoError(t, err)
	require.Equal(t, 0, len(metas))

	// test partition table ProcessErrFlow
	errMeta, err = handler.ProcessErrFlow(nil, gTask, "mockErr")
	require.NoError(t, err)
	rollbackMeta = ddl.LitBackfillSubTaskRollbackMeta{}
	err = json.Unmarshal(errMeta, &rollbackMeta)
	require.NoError(t, err)
	require.Equal(t, false, rollbackMeta.IsMergingIdx)

	// test step2 merging index
	gTask, gTaskMeta = createAddIndexGlobalTask(t, dom, "test", "t1", true)
	metas, err = handler.ProcessNormalFlow(nil, gTask)
	require.NoError(t, err)
	require.Equal(t, proto.StepTwo, gTask.Step)
	require.Equal(t, 1, len(metas))
	subTask = ddl.LitBackfillSubTaskMeta{}
	require.NoError(t, json.Unmarshal(metas[0], &subTask))
	require.Equal(t, gTaskMeta.Job.TableID, subTask.PhysicalTableID)
	require.Equal(t, true, subTask.IsMergingIdx)

	errMeta, err = handler.ProcessErrFlow(nil, gTask, "mockErr")
	require.NoError(t, err)
	rollbackMeta = ddl.LitBackfillSubTaskRollbackMeta{}
	err = json.Unmarshal(errMeta, &rollbackMeta)
	require.NoError(t, err)
	require.Equal(t, true, rollbackMeta.IsMergingIdx)
}

func createAddIndexGlobalTask(t *testing.T, dom *domain.Domain, dbName, tblName string, isMerging bool) (*proto.Task, *ddl.LitBackfillGlobalTaskMeta) {
	db, ok := dom.InfoSchema().SchemaByName(model.NewCIStr(dbName))
	require.True(t, ok)
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr(dbName), model.NewCIStr(tblName))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	defaultSQLMode, err := mysql.GetSQLMode(mysql.DefaultSQLMode)
	require.NoError(t, err)

	taskMeta := &ddl.LitBackfillGlobalTaskMeta{
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
		EleID:        10,
		EleTypeKey:   meta.IndexElementKey,
		IsMergingIdx: isMerging,
	}

	gTaskMetaBytes, err := json.Marshal(taskMeta)
	require.NoError(t, err)

	gTask := &proto.Task{
		ID:              time.Now().UnixMicro(),
		Type:            ddl.FlowHandleLitBackfillType,
		Step:            proto.StepInit,
		State:           proto.TaskStatePending,
		Meta:            gTaskMetaBytes,
		StartTime:       time.Now(),
		StateUpdateTime: time.Now(),
	}

	return gTask, taskMeta
}
