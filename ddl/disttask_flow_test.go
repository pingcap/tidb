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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
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
	handler := ddl.NewLitBackfillFlowHandle(dom.DDL())

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table tp1(id int primary key, v int) PARTITION BY RANGE (id) (\n    " +
		"PARTITION p0 VALUES LESS THAN (10),\n" +
		"PARTITION p1 VALUES LESS THAN (100),\n" +
		"PARTITION p2 VALUES LESS THAN (1000),\n" +
		"PARTITION p3 VALUES LESS THAN MAXVALUE\n);")
	gTask := createAddIndexGlobalTask(t, dom, "test", "tp1", ddl.BackfillTaskType)
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tp1"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()

	// 1. test partition table ProcessNormalFlow
	processAndCheck(t, handler, gTask, tblInfo, func(t *testing.T, metas [][]byte, gTask *proto.Task, tblInfo *model.TableInfo) {
		require.Equal(t, proto.StepOne, gTask.Step)
		require.Equal(t, len(tblInfo.Partition.Definitions), len(metas))
		for i, par := range tblInfo.Partition.Definitions {
			var subTask ddl.BackfillSubTaskMeta
			require.NoError(t, json.Unmarshal(metas[i], &subTask))
			require.Equal(t, par.ID, subTask.PhysicalTableID)
		}
	}, nil)

	// 2. test partition table ProcessNormalFlow after step1 finished
	gTask.Step++
	processAndCheck(t, handler, gTask, tblInfo, func(t *testing.T, metas [][]byte, gTask *proto.Task, tblInfo *model.TableInfo) {
		require.NoError(t, err)
		require.Equal(t, 0, len(metas))
	}, nil)

	// 3. test partition table ProcessErrFlow

	processAndCheck(t, handler, gTask, tblInfo, func(t *testing.T, metas [][]byte, gTask *proto.Task, tblInfo *model.TableInfo) {
		require.Nil(t, metas)
	}, []error{errors.New("mockErr")})
	// check idempotent.
	processAndCheck(t, handler, gTask, tblInfo, func(t *testing.T, metas [][]byte, gTask *proto.Task, tblInfo *model.TableInfo) {
		require.Nil(t, metas)
	}, []error{errors.New("mockErr")})

	// 4. test non-partition-table.
	tk.MustExec("create table t1(id int primary key, v int)")
	gTask = createAddIndexGlobalTask(t, dom, "test", "t1", ddl.BackfillTaskType)
	tbl, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	tblInfo = tbl.Meta()
	processAndCheck(t, handler, gTask, tblInfo, func(t *testing.T, metas [][]byte, gTask *proto.Task, tblInfo *model.TableInfo) {
		require.Equal(t, proto.StepOne, gTask.Step)
		// TODO: check meta
	}, nil)
	gTask.Step++
	processAndCheck(t, handler, gTask, tblInfo, func(t *testing.T, metas [][]byte, gTask *proto.Task, tblInfo *model.TableInfo) {
		for _, meta := range metas {
			var subTask ddl.BackfillSubTaskMeta
			require.NoError(t, json.Unmarshal(meta, &subTask))
			require.Equal(t, []uint8([]byte(nil)), subTask.StartKey)
			require.Equal(t, []uint8([]byte(nil)), subTask.EndKey)
		}
	}, nil)
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
		Step:            proto.StepOne,
		State:           proto.TaskStateRunning,
		Meta:            gTaskMetaBytes,
		StartTime:       time.Now(),
		StateUpdateTime: time.Now(),
	}

	return gTask
}

func processAndCheck(t *testing.T,
	handler dispatcher.TaskFlowHandle,
	gTask *proto.Task, tblInfo *model.TableInfo,
	checkGTaskAndMeta func(t *testing.T, metas [][]byte, gTask *proto.Task, tblInfo *model.TableInfo),
	receiverErr []error) {

	metasChan := make(chan [][]byte)
	errChan := make(chan error)
	doneChan := make(chan bool)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if receiverErr == nil {
			handler.ProcessNormalFlow(context.Background(), nil, gTask, metasChan, errChan, doneChan)
		} else {
			handler.ProcessErrFlow(context.Background(), nil, gTask, receiverErr, metasChan, errChan, doneChan)
		}
		wg.Done()
	}()

	doneDispatch := false
	// TODO: receive metas array
	var metas [][]byte
	for !doneDispatch {
		select {
		case metas = <-metasChan:
		case <-errChan:
		case <-doneChan:
			if !doneDispatch {
				doneDispatch = true
				close(metasChan)
				close(doneChan)
				close(errChan)
			}
		}
	}

	checkGTaskAndMeta(t, metas, gTask, tblInfo)
	wg.Wait()
}
