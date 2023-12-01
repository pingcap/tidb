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

type balanceTestCase struct {
	subtasks         []*proto.Subtask
	liveNodes        []*infosync.ServerInfo
	taskNodes        []string
	expectedSubtasks []*proto.Subtask
}

func scaleTest(t *testing.T,
	mockTaskMgr *mock.MockTaskManager,
	testCase scaleTestCase,
	id int) {
	ctx := context.Background()
	mockTaskMgr.EXPECT().GetSubtasksByStepAndState(ctx, int64(id), proto.StepInit, proto.TaskStatePending).Return(
		testCase.subtasks,
		nil)
	mockTaskMgr.EXPECT().UpdateSubtasksSchedulerIDs(ctx, int64(id), testCase.subtasks).Return(nil).AnyTimes()
	mockTaskMgr.EXPECT().CleanUpMeta(ctx, testCase.cleanedNodes).Return(nil).AnyTimes()
	if len(testCase.cleanedNodes) > 0 {
		mockTaskMgr.EXPECT().GetSubtasksByExecIdsAndStepAndState(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	}
	dsp := dispatcher.NewBaseDispatcher(ctx, mockTaskMgr, "server", &proto.Task{Step: proto.StepInit, ID: int64(id)})
	dsp.LiveNodes = testCase.liveNodes
	dsp.TaskNodes = testCase.taskNodes
	require.NoError(t, dsp.ReDispatchSubtasks())
	slices.SortFunc(dsp.TaskNodes, func(i, j string) int {
		return strings.Compare(i, j)
	})
	slices.SortFunc(testCase.subtasks, func(i, j *proto.Subtask) int {
		return strings.Compare(i.ExecID, j.ExecID)
	})
	require.Equal(t, testCase.expectedTaskNodes, dsp.TaskNodes)
	require.Equal(t, testCase.expectedSubtasks, testCase.subtasks)
}

func balanceTest(t *testing.T,
	mockTaskMgr *mock.MockTaskManager,
	testCase balanceTestCase,
	id int) {
	ctx := context.Background()
	mockTaskMgr.EXPECT().GetSubtasksByStepAndState(ctx, int64(id), proto.StepInit, proto.TaskStatePending).Return(
		testCase.subtasks,
		nil)
	mockTaskMgr.EXPECT().CleanUpMeta(ctx, gomock.Any()).Return(nil).AnyTimes()

	mockTaskMgr.EXPECT().UpdateSubtasksSchedulerIDs(ctx, int64(id), testCase.subtasks).Return(nil).AnyTimes()
	dsp := dispatcher.NewBaseDispatcher(ctx, mockTaskMgr, "server", &proto.Task{Step: proto.StepInit, ID: int64(id)})
	dsp.LiveNodes = testCase.liveNodes
	dsp.TaskNodes = testCase.taskNodes
	require.NoError(t, dsp.ReDispatchSubtasks())
	slices.SortFunc(dsp.TaskNodes, func(i, j string) int {
		return strings.Compare(i, j)
	})
	slices.SortFunc(testCase.subtasks, func(i, j *proto.Subtask) int {
		return strings.Compare(i.ExecID, j.ExecID)
	})
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
		// 1. scale out from 1 node to 2 nodes. 4 subtasks.
		{
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.1", Port: 4000}, {IP: "1.1.1.2", Port: 4000}},
			[]string{"1.1.1.1:4000"},
			[]string{},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"}},
		},
		// 2. scale out from 1 node to 2 nodes. 3 subtasks.
		{
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.1", Port: 4000}, {IP: "1.1.1.2", Port: 4000}},
			[]string{"1.1.1.1:4000"},
			[]string{},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"}},
		},
		// 3. scale out from 2 nodes to 4 nodes. 4 subtasks.
		{
			[]*proto.Subtask{
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.1", Port: 4000}, {IP: "1.1.1.2", Port: 4000}, {IP: "1.1.1.3", Port: 4000}, {IP: "1.1.1.4", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000", "1.1.1.3:4000", "1.1.1.4:4000"},
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.4:4000"}},
		},
		// 4. scale out from 2 nodes to 4 nodes. 9 subtasks.
		{
			[]*proto.Subtask{
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.1", Port: 4000}, {IP: "1.1.1.2", Port: 4000}, {IP: "1.1.1.3", Port: 4000}, {IP: "1.1.1.4", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000", "1.1.1.3:4000", "1.1.1.4:4000"},
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.4:4000"},
				{ExecID: "1.1.1.4:4000"}},
		},
		// 5. scale out from 2 nodes to 3 nodes.
		{
			[]*proto.Subtask{
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.1", Port: 4000}, {IP: "1.1.1.2", Port: 4000}, {IP: "1.1.1.3", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000", "1.1.1.3:4000"},
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.3:4000"}},
		},
		// 6. scale out from 1 node to another 2 node.
		{
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.2", Port: 4000}, {IP: "1.1.1.3", Port: 4000}},
			[]string{"1.1.1.1:4000"},
			[]string{"1.1.1.1:4000"},
			[]string{"1.1.1.2:4000", "1.1.1.3:4000"},
			[]*proto.Subtask{
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.3:4000"}},
		},
		// 7. scale out from tidb1, tidb2 to tidb2, tidb3.
		{
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.2", Port: 4000}, {IP: "1.1.1.3", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{"1.1.1.1:4000"},
			[]string{"1.1.1.2:4000", "1.1.1.3:4000"},
			[]*proto.Subtask{
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.3:4000"}},
		},
		// 8. scale from tidb1, tidb2 to tidb3, tidb4.
		{
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.3", Port: 4000}, {IP: "1.1.1.4", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{"1.1.1.3:4000", "1.1.1.4:4000"},
			[]*proto.Subtask{
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.4:4000"},
				{ExecID: "1.1.1.4:4000"}},
		},
		// 9. scale from tidb1, tidb2 to tidb2, tidb3, tidb4.
		{
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.2", Port: 4000}, {IP: "1.1.1.3", Port: 4000}, {IP: "1.1.1.4", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{"1.1.1.1:4000"},
			[]string{"1.1.1.2:4000", "1.1.1.3:4000", "1.1.1.4:4000"},
			[]*proto.Subtask{
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.4:4000"}},
		},
		// 10. scale form tidb1, tidb2 to tidb2, tidb3, tidb4.
		{

			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.1:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.2", Port: 4000}, {IP: "1.1.1.3", Port: 4000}, {IP: "1.1.1.4", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{"1.1.1.1:4000"},
			[]string{"1.1.1.2:4000", "1.1.1.3:4000", "1.1.1.4:4000"},
			[]*proto.Subtask{
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.4:4000"}},
		},
		// 11. scale from tidb1(2 subtasks), tidb2(3 subtasks), tidb3(0 subtasks) to tidb1, tidb3, tidb4, tidb5, tidb6.
		{
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.1", Port: 4000}, {IP: "1.1.1.3", Port: 4000}, {IP: "1.1.1.4", Port: 4000}, {IP: "1.1.1.5", Port: 4000}, {IP: "1.1.1.6", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{"1.1.1.2:4000"},
			[]string{"1.1.1.1:4000", "1.1.1.3:4000", "1.1.1.4:4000", "1.1.1.5:4000", "1.1.1.6:4000"},
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.4:4000"},
				{ExecID: "1.1.1.5:4000"},
				{ExecID: "1.1.1.6:4000"}},
		},
	}
	for i, testCase := range testCases {
		scaleTest(t, mockTaskMgr, testCase, i+1)
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
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.1", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{"1.1.1.2:4000"},
			[]string{"1.1.1.1:4000"},
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"}},
		},
		// 2. scale in from tidb1, tidb2 to tidb3.
		{
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.3", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{"1.1.1.3:4000"},
			[]*proto.Subtask{
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.3:4000"}},
		},
		// 5. scale in from 10 nodes to 2 nodes.
		{
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.4:4000"},
				{ExecID: "1.1.1.5:4000"},
				{ExecID: "1.1.1.6:4000"},
				{ExecID: "1.1.1.7:4000"},
				{ExecID: "1.1.1.8:4000"},
				{ExecID: "1.1.1.9:4000"},
				{ExecID: "1.1.1.10:4000"}},
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
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.3:4000"},
			},
		},
		// 6. scale in from 1 node with 10 subtasks, 1 node with 1 subtasks to 1 node.
		{
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.1", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]string{"1.1.1.2:4000"},
			[]string{"1.1.1.1:4000"},
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"}},
		},
	}
	for i, testCase := range testCases {
		scaleTest(t, mockTaskMgr, testCase, i+1)
	}
}

func TestRebalanceWithoutScale(t *testing.T) {
	store := testkit.CreateMockStore(t)
	gtk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return gtk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockTaskMgr := mock.NewMockTaskManager(ctrl)
	testCases := []balanceTestCase{
		// 1. from tidb1:1, tidb2:3 to tidb1:2, tidb2:2
		{
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.1", Port: 4000}, {IP: "1.1.1.2", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"}},
		},
		// 2. from tidb1:3, tidb2:2, tidb3:1 to tidb1:2, tidb2:2, tidb3:2
		{
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.3:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.1", Port: 4000}, {IP: "1.1.1.2", Port: 4000}, {IP: "1.1.1.3", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000", "1.1.1.3:4000"},
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.3:4000"}},
		},
		// 3. from tidb1: 0, tidb2: 5 to tidb1: 3, tidb2: 2
		{
			[]*proto.Subtask{
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.1", Port: 4000}, {IP: "1.1.1.2", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"}},
		},
		// 4. from tidb1:5, tidb2:0, tidb3:0, tidb4:0, tidb5:0, tidb6:0 to 1,1,1,1,1,0.
		{
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.1", Port: 4000}, {IP: "1.1.1.2", Port: 4000},
				{IP: "1.1.1.3", Port: 4000}, {IP: "1.1.1.4", Port: 4000},
				{IP: "1.1.1.5", Port: 4000}, {IP: "1.1.1.6", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000", "1.1.1.3:4000", "1.1.1.4:4000", "1.1.1.5:4000", "1.1.1.6:4000"},
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.4:4000"},
				{ExecID: "1.1.1.5:4000"}},
		},
		// 5. no balance needed. tidb1:2, tidb2:3
		{
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.1", Port: 4000}, {IP: "1.1.1.2", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"}},
		},
		// 6. no balance needed. tidb1:2, tidb2:2
		{
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"}},
			[]*infosync.ServerInfo{{IP: "1.1.1.1", Port: 4000}, {IP: "1.1.1.2", Port: 4000}},
			[]string{"1.1.1.1:4000", "1.1.1.2:4000"},
			[]*proto.Subtask{
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.1:4000"},
				{ExecID: "1.1.1.2:4000"},
				{ExecID: "1.1.1.2:4000"}},
		},
	}
	for i, testCase := range testCases {
		balanceTest(t, mockTaskMgr, testCase, i+1)
	}
}
