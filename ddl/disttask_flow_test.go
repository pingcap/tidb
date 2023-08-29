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
	handler, err := ddl.NewLitBackfillFlowHandle(dom.DDL())
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// test partition table ProcessNormalFlow
	tk.MustExec("create table tp1(id int primary key, v int) PARTITION BY RANGE (id) (\n    " +
		"PARTITION p0 VALUES LESS THAN (10),\n" +
		"PARTITION p1 VALUES LESS THAN (100),\n" +
		"PARTITION p2 VALUES LESS THAN (1000),\n" +
		"PARTITION p3 VALUES LESS THAN MAXVALUE\n);")
	gTask := createAddIndexGlobalTask(t, dom, "test", "tp1", ddl.BackfillTaskType)
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tp1"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	metas, err := handler.ProcessNormalFlow(context.Background(), nil, gTask)
	require.NoError(t, err)
	require.Equal(t, proto.StepOne, gTask.Step)
	require.Equal(t, len(tblInfo.Partition.Definitions), len(metas))
	for i, par := range tblInfo.Partition.Definitions {
		var subTask ddl.BackfillSubTaskMeta
		require.NoError(t, json.Unmarshal(metas[i], &subTask))
		require.Equal(t, par.ID, subTask.PhysicalTableID)
	}

	// test partition table ProcessNormalFlow after step1 finished
	gTask.State = proto.TaskStateRunning
	metas, err = handler.ProcessNormalFlow(context.Background(), nil, gTask)
	require.NoError(t, err)
	require.Equal(t, 0, len(metas))

	// test partition table ProcessErrFlow
	errMeta, err := handler.ProcessErrFlow(context.Background(), nil, gTask, []error{errors.New("mockErr")})
	require.NoError(t, err)
	require.Nil(t, errMeta)

	errMeta, err = handler.ProcessErrFlow(context.Background(), nil, gTask, []error{errors.New("mockErr")})
	require.NoError(t, err)
	require.Nil(t, errMeta)

	tk.MustExec("create table t1(id int primary key, v int)")
	gTask = createAddIndexGlobalTask(t, dom, "test", "t1", ddl.BackfillTaskType)
	_, err = handler.ProcessNormalFlow(context.Background(), nil, gTask)
	require.NoError(t, err)
}

func createAddIndexGlobalTask(t *testing.T, dom *domain.Domain, dbName, tblName string, taskType string) *proto.Task {
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
		EleID:      10,
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
