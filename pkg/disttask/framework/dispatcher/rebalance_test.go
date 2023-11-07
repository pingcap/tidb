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

package dispatcher_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type scaleTestCase struct {
	subtasks          []*proto.Subtask
	liveNodes         []*infosync.ServerInfo
	taskNodes         []string
	cleanedNodes      []string
	expectedTaskNodes []string
	expectedSubtasks  []*proto.Subtask
}

func scaleTest(t *testing.T,
	mockTaskMgr *mock.MockTaskManager,
	testCase scaleTestCase) {
	ctx := context.Background()
	mockTaskMgr.EXPECT().GetSubtasksByStepExceptStates(int64(0), proto.StepInit, proto.TaskStateSucceed).Return(
		testCase.subtasks,
		nil)
	mockTaskMgr.EXPECT().UpdateSubtasksSchedulerIDs(int64(0), testCase.subtasks).Return(nil)
	mockTaskMgr.EXPECT().CleanUpMeta(testCase.cleanedNodes).Return(nil)
	dsp := dispatcher.NewBaseDispatcher(ctx, mockTaskMgr, "server", &proto.Task{Step: proto.StepInit})
	dsp.LiveNodes = testCase.liveNodes
	dsp.TaskNodes = testCase.taskNodes
	require.NoError(t, dsp.RebalanceSubtasksImpl())
	slices.SortFunc(dsp.TaskNodes, func(i, j string) int {
		return strings.Compare(i, j)
	})
	slices.SortFunc(testCase.subtasks, func(i, j *proto.Subtask) int {
		return strings.Compare(i.SchedulerID, j.SchedulerID)
	})
	require.Equal(t, testCase.expectedTaskNodes, dsp.TaskNodes)
	require.Equal(t, testCase.expectedSubtasks, testCase.subtasks)
}

func TestScaleOutNodes(t *testing.T) {
	store := testkit.CreateMockStore(t)
	gtk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return gtk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockTaskMgr := mock.NewMockTaskManager(ctrl)
	testCases := []scaleTestCase{
		// 1. scale out from 1 node to 2 nodes.
		{
			/// 1.1 4 subtasks.
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.1", Port: 4000}, {IP: "1.1.1.2", Port: 4000}},
			[]string{"1.1.1.1:4000"},
			[]string{},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"}},
		},
		{
			/// 1.2 3 subtasks.
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.1", Port: 4000}, {IP: "1.1.1.2", Port: 4000}},
			[]string{"1.1.1.1:4000"},
			[]string{},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"}},
		},
		// 2. scale out from 2 nodes to 4 nodes.
		{
			/// 2.1 4 subtasks
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.1", Port: 4000}, {IP: "1.1.1.2", Port: 4000}, {IP: "1.1.1.3", Port: 4000}, {IP: "1.1.1.4", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000", "1.1.1.3:4000", "1.1.1.4:4000"},
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.4:4000"}},
		},
		{
			/// 2.2 9 subtasks.
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.1", Port: 4000}, {IP: "1.1.1.2", Port: 4000}, {IP: "1.1.1.3", Port: 4000}, {IP: "1.1.1.4", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000", "1.1.1.3:4000", "1.1.1.4:4000"},
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.4:4000"},
				{SchedulerID: "1.1.1.4:4000"}},
		},
		// 3. scale out from 2 nodes to 3 nodes.
		{
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.1", Port: 4000}, {IP: "1.1.1.2", Port: 4000}, {IP: "1.1.1.3", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000", "1.1.1.3:4000"},
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.3:4000"}},
		},
		// 4. scale out from 1 node to another 2 node.
		{
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.2", Port: 4000}, {IP: "1.1.1.3", Port: 4000}},
			[]string{"1.1.1.1:4000"},
			[]string{"1.1.1.1:4000"},
			[]string{"1.1.1.2:4000", "1.1.1.3:4000"},
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.3:4000"}},
		},
		// 5. scale out from tidb1, tidb2 to tidb2, tidb3.
		{
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.2", Port: 4000}, {IP: "1.1.1.3", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{"1.1.1.1:4000"},
			[]string{"1.1.1.2:4000", "1.1.1.3:4000"},
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.3:4000"}},
		},
		// 6. scale out from tidb1, tidb2 to tidb2, tidb3, tidb4.
		{
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.2", Port: 4000}, {IP: "1.1.1.3", Port: 4000}, {IP: "1.1.1.4", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{"1.1.1.1:4000"},
			[]string{"1.1.1.2:4000", "1.1.1.3:4000", "1.1.1.4:4000"},
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.4:4000"}},
		},
		{
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.1:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.2", Port: 4000}, {IP: "1.1.1.3", Port: 4000}, {IP: "1.1.1.4", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{"1.1.1.1:4000"},
			[]string{"1.1.1.2:4000", "1.1.1.3:4000", "1.1.1.4:4000"},
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.4:4000"},
				{SchedulerID: "1.1.1.4:4000"}},
		},
	}
	for _, testCase := range testCases {
		scaleTest(t, mockTaskMgr, testCase)
	}
}

func TestScaleInNodes(t *testing.T) {
	store := testkit.CreateMockStore(t)
	gtk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return gtk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockTaskMgr := mock.NewMockTaskManager(ctrl)
	testCases := []scaleTestCase{
		// 1. scale in from tidb1, tidb2 to tidb1.
		{
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.1", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{"1.1.1.2:4000"},
			[]string{"1.1.1.1:4000"},
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"}},
		},
		// 2. scale in from tidb1, tidb2 to tidb3.
		{
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.3", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{"1.1.1.3:4000"},
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.3:4000"}},
		},
		// 3. scale in from tidb1, tidb2 to tidb3, tidb4.
		{
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.3", Port: 4000}, {IP: "1.1.1.4", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{"1.1.1.3:4000", "1.1.1.4:4000"},
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.4:4000"},
				{SchedulerID: "1.1.1.4:4000"}},
		},
		// 4. scale in from tidb1, tidb2 to tidb2, tidb3.
		{
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.2", Port: 4000}, {IP: "1.1.1.3", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{"1.1.1.1:4000"},
			[]string{"1.1.1.2:4000", "1.1.1.3:4000"},
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.3:4000"}},
		},
		// 5. scale in from 10 nodes to 2 nodes.
		{
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.1:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.4:4000"},
				{SchedulerID: "1.1.1.5:4000"},
				{SchedulerID: "1.1.1.6:4000"},
				{SchedulerID: "1.1.1.7:4000"},
				{SchedulerID: "1.1.1.8:4000"},
				{SchedulerID: "1.1.1.9:4000"},
				{SchedulerID: "1.1.1.10:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.2", Port: 4000}, {IP: "1.1.1.3", Port: 4000}},
			[]string{
				"1.1.1.1:4000",
				"1.1.1.2:4000",
				"1.1.1.3:4000",
				"1.1.1.4:4000",
				"1.1.1.5:4000",
				"1.1.1.6:4000",
				"1.1.1.7:4000",
				"1.1.1.8:4000",
				"1.1.1.9:4000",
				"1.1.1.10:4000"},
			[]string{
				"1.1.1.1:4000",
				"1.1.1.4:4000",
				"1.1.1.5:4000",
				"1.1.1.6:4000",
				"1.1.1.7:4000",
				"1.1.1.8:4000",
				"1.1.1.9:4000",
				"1.1.1.10:4000"},
			[]string{"1.1.1.2:4000", "1.1.1.3:4000"},
			[]*proto.Subtask{
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.2:4000"},
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.3:4000"},
				{SchedulerID: "1.1.1.3:4000"},
			},
		},
	}
	for _, testCase := range testCases {
		scaleTest(t, mockTaskMgr, testCase)
	}
}

// // decide whether add the tests
// func mockDispatcherWithTaskMgr(t *testing.T, pool *pools.ResourcePool) (*dispatcher.BaseDispatcher, *storage.TaskManager) {
// 	ctx := context.Background()
// 	mgr := storage.NewTaskManager(util.WithInternalSourceType(ctx, "taskManager"), pool)
// 	storage.SetTaskManager(mgr)

// 	handle.SubmitGlobalTask("test", proto.TaskTypeExample, 1, nil)
// 	task, err := mgr.GetGlobalTaskByKey("test")
// 	require.NoError(t, err)
// 	mockDispatcher := dispatcher.NewBaseDispatcher(ctx, mgr, "server", task)
// 	mockDispatcher.Extension = &testDispatcherExt{}
// 	return mockDispatcher, mgr
// }

// func TestScaleWithTable(t *testing.T) {
// 	store := testkit.CreateMockStore(t)
// 	gtk := testkit.NewTestKit(t, store)
// 	pool := pools.NewResourcePool(func() (pools.Resource, error) {
// 		return gtk.Session(), nil
// 	}, 1, 1, time.Second)
// 	defer pool.Close()
// 	ctrl := gomock.NewController(t)
// 	defer ctrl.Finish()

// 	dsp, _ := mockDispatcherWithTaskMgr(t, pool)
// 	dsp.OnPending()
// }
