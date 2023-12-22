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

package scheduler

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type scaleTestCase struct {
	subtasks          []*proto.Subtask
	liveNodes         []string
	taskNodes         []string
	cleanedNodes      []string
	expectedTaskNodes []string
	expectedSubtasks  []*proto.Subtask
}

func scaleTest(t *testing.T,
	mockTaskMgr *mock.MockTaskManager,
	testCase scaleTestCase,
	id int) {
	//ctx := context.Background()
	//mockTaskMgr.EXPECT().GetSubtasksByStepAndState(ctx, int64(id), proto.StepInit, proto.TaskStatePending).Return(
	//	testCase.subtasks,
	//	nil)
	//mockTaskMgr.EXPECT().UpdateSubtasksExecIDs(ctx, int64(id), testCase.subtasks).Return(nil).AnyTimes()
	//mockTaskMgr.EXPECT().DeleteDeadNodes(ctx, testCase.cleanedNodes).Return(nil).AnyTimes()
	//if len(testCase.cleanedNodes) > 0 {
	//	mockTaskMgr.EXPECT().GetSubtasksByExecIdsAndStepAndState(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	//}
	//sch := scheduler.NewBaseScheduler(ctx, &proto.Task{Step: proto.StepInit, ID: int64(id)},
	//	scheduler.NewParam(mockTaskMgr, scheduler.NewNodeManager(), scheduler.NewSlotManager()))
	//sch.TaskNodes = testCase.taskNodes
	//require.NoError(t, sch.DoBalanceSubtasks(testCase.liveNodes))
	//slices.SortFunc(sch.TaskNodes, func(i, j string) int {
	//	return strings.Compare(i, j)
	//})
	//slices.SortFunc(testCase.subtasks, func(i, j *proto.Subtask) int {
	//	return strings.Compare(i.ExecID, j.ExecID)
	//})
	//require.Equal(t, testCase.expectedTaskNodes, sch.TaskNodes)
	//require.Equal(t, testCase.expectedSubtasks, testCase.subtasks)
}

func TestScaleOutNodes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockTaskMgr := mock.NewMockTaskManager(ctrl)
	testCases := []scaleTestCase{
		// 1. scale out from 1 node to 2 nodes. 4 subtasks.
		{
			[]*proto.Subtask{
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb1"}},
			[]string{"tidb1", "tidb2"},
			[]string{"tidb1"},
			[]string{},
			[]string{"tidb1", "tidb2"},
			[]*proto.Subtask{
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"}},
		},
		// 2. scale out from 1 node to 2 nodes. 3 subtasks.
		{
			[]*proto.Subtask{
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb1"}},
			[]string{"tidb1", "tidb2"},
			[]string{"tidb1"},
			[]string{},
			[]string{"tidb1", "tidb2"},
			[]*proto.Subtask{
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb2"}},
		},
		// 3. scale out from 2 nodes to 4 nodes. 4 subtasks.
		{
			[]*proto.Subtask{
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "tidb1"},
				{ExecID: "tidb1"}},
			[]string{"tidb1", "tidb2", "1.1.1.3:4000", "1.1.1.4:4000"},
			[]string{"tidb1", "tidb2"},
			[]string{},
			[]string{"tidb1", "tidb2", "1.1.1.3:4000", "1.1.1.4:4000"},
			[]*proto.Subtask{
				{ExecID: "tidb1"},
				{ExecID: "tidb2"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.4:4000"}},
		},
		// 4. scale out from 2 nodes to 4 nodes. 9 subtasks.
		{
			[]*proto.Subtask{
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb1"}},
			[]string{"tidb1", "tidb2", "1.1.1.3:4000", "1.1.1.4:4000"},
			[]string{"tidb1", "tidb2"},
			[]string{},
			[]string{"tidb1", "tidb2", "1.1.1.3:4000", "1.1.1.4:4000"},
			[]*proto.Subtask{
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.4:4000"},
				{ExecID: "1.1.1.4:4000"}},
		},
		// 5. scale out from 2 nodes to 3 nodes.
		{
			[]*proto.Subtask{
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "tidb1"},
				{ExecID: "tidb1"}},
			[]string{"tidb1", "tidb2", "1.1.1.3:4000"},
			[]string{"tidb1", "tidb2"},
			[]string{},
			[]string{"tidb1", "tidb2", "1.1.1.3:4000"},
			[]*proto.Subtask{
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb2"},
				{ExecID: "1.1.1.3:4000"}},
		},
		// 6. scale out from 1 node to another 2 node.
		{
			[]*proto.Subtask{
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb1"}},
			[]string{"tidb2", "1.1.1.3:4000"},
			[]string{"tidb1"},
			[]string{"tidb1"},
			[]string{"tidb2", "1.1.1.3:4000"},
			[]*proto.Subtask{
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.3:4000"}},
		},
		// 7. scale out from tidb1, tidb2 to tidb2, tidb3.
		{
			[]*proto.Subtask{
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"}},
			[]string{"tidb2", "1.1.1.3:4000"},
			[]string{"tidb1", "tidb2"},
			[]string{"tidb1"},
			[]string{"tidb2", "1.1.1.3:4000"},
			[]*proto.Subtask{
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.3:4000"}},
		},
		// 8. scale from tidb1, tidb2 to tidb3, tidb4.
		{
			[]*proto.Subtask{
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"}},
			[]string{"1.1.1.3:4000", "1.1.1.4:4000"},
			[]string{"tidb1", "tidb2"},
			[]string{"tidb1", "tidb2"},
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
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"}},
			[]string{"tidb2", "1.1.1.3:4000", "1.1.1.4:4000"},
			[]string{"tidb1", "tidb2"},
			[]string{"tidb1"},
			[]string{"tidb2", "1.1.1.3:4000", "1.1.1.4:4000"},
			[]*proto.Subtask{
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.4:4000"}},
		},
		// 10. scale form tidb1, tidb2 to tidb2, tidb3, tidb4.
		{

			[]*proto.Subtask{
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "tidb1"}},
			[]string{"tidb2", "1.1.1.3:4000", "1.1.1.4:4000"},
			[]string{"tidb1", "tidb2"},
			[]string{"tidb1"},
			[]string{"tidb2", "1.1.1.3:4000", "1.1.1.4:4000"},
			[]*proto.Subtask{
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.4:4000"}},
		},
		// 11. scale from tidb1(2 subtasks), tidb2(3 subtasks), tidb3(0 subtasks) to tidb1, tidb3, tidb4, tidb5, tidb6.
		{
			[]*proto.Subtask{
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"}},
			[]string{"tidb1", "1.1.1.3:4000", "1.1.1.4:4000", "1.1.1.5:4000", "1.1.1.6:4000"},
			[]string{"tidb1", "tidb2"},
			[]string{"tidb2"},
			[]string{"tidb1", "1.1.1.3:4000", "1.1.1.4:4000", "1.1.1.5:4000", "1.1.1.6:4000"},
			[]*proto.Subtask{
				{ExecID: "tidb1"},
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockTaskMgr := mock.NewMockTaskManager(ctrl)
	testCases := []scaleTestCase{
		// 1. scale in from tidb1, tidb2 to tidb1.
		{
			[]*proto.Subtask{
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"}},
			[]string{"tidb1"},
			[]string{"tidb1", "tidb2"},
			[]string{"tidb2"},
			[]string{"tidb1"},
			[]*proto.Subtask{
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb1"}},
		},
		// 2. scale in from tidb1, tidb2 to tidb3.
		{
			[]*proto.Subtask{
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"}},
			[]string{"1.1.1.3:4000"},
			[]string{"tidb1", "tidb2"},
			[]string{"tidb1", "tidb2"},
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
				{ExecID: "tidb1"},
				{ExecID: "tidb2"},
				{ExecID: "1.1.1.3:4000"},
				{ExecID: "1.1.1.4:4000"},
				{ExecID: "1.1.1.5:4000"},
				{ExecID: "1.1.1.6:4000"},
				{ExecID: "1.1.1.7:4000"},
				{ExecID: "1.1.1.8:4000"},
				{ExecID: "1.1.1.9:4000"},
				{ExecID: "1.1.1.10:4000"}},
			[]string{"tidb2", "1.1.1.3:4000"},
			[]string{
				"tidb1",
				"tidb2",
				"1.1.1.3:4000",
				"1.1.1.4:4000",
				"1.1.1.5:4000",
				"1.1.1.6:4000",
				"1.1.1.7:4000",
				"1.1.1.8:4000",
				"1.1.1.9:4000",
				"1.1.1.10:4000"},
			[]string{
				"tidb1",
				"1.1.1.4:4000",
				"1.1.1.5:4000",
				"1.1.1.6:4000",
				"1.1.1.7:4000",
				"1.1.1.8:4000",
				"1.1.1.9:4000",
				"1.1.1.10:4000"},
			[]string{"tidb2", "1.1.1.3:4000"},
			[]*proto.Subtask{
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
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
				{ExecID: "tidb1"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"},
				{ExecID: "tidb2"}},
			[]string{"tidb1"},
			[]string{"tidb1", "tidb2"},
			[]string{"tidb2"},
			[]string{"tidb1"},
			[]*proto.Subtask{
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb1"},
				{ExecID: "tidb1"}},
		},
	}
	for i, testCase := range testCases {
		scaleTest(t, mockTaskMgr, testCase, i+1)
	}
}

type balanceTestCase struct {
	subtasks          []*proto.Subtask
	eligibleNodes     []string
	expectedSubtasks  []*proto.Subtask
	expectedUsedSlots map[string]int
}

func TestBalanceOneTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	testCases := []balanceTestCase{
		// no subtasks to balance, no need to do anything.
		{
			subtasks:          []*proto.Subtask{},
			eligibleNodes:     []string{"tidb1"},
			expectedSubtasks:  []*proto.Subtask{},
			expectedUsedSlots: map[string]int{"tidb1": 0},
		},
		// balanced, no need to do anything.
		{
			subtasks: []*proto.Subtask{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.TaskStateRunning},
				{ID: 2, ExecID: "tidb2", Concurrency: 16, State: proto.TaskStateRunning},
				{ID: 3, ExecID: "tidb2", Concurrency: 16, State: proto.TaskStatePending},
			},
			eligibleNodes: []string{"tidb1", "tidb2"},
			expectedSubtasks: []*proto.Subtask{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.TaskStateRunning},
				{ID: 2, ExecID: "tidb2", Concurrency: 16, State: proto.TaskStateRunning},
				{ID: 3, ExecID: "tidb2", Concurrency: 16, State: proto.TaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb1": 16, "tidb2": 16},
		},
		// no eligible nodes, no need to do anything.
		{
			subtasks: []*proto.Subtask{
				{ID: 1, ExecID: "tidb1", Concurrency: 32, State: proto.TaskStateRunning},
			},
			eligibleNodes: []string{"tidb1", "tidb2"},
			expectedSubtasks: []*proto.Subtask{
				{ID: 1, ExecID: "tidb1", Concurrency: 32, State: proto.TaskStateRunning},
			},
			expectedUsedSlots: map[string]int{"tidb1": 32, "tidb2": 0},
		},
		// balance from 1:4 to 2:3
		{
			subtasks: []*proto.Subtask{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.TaskStateRunning},
				{ID: 2, ExecID: "tidb2", Concurrency: 16, State: proto.TaskStateRunning},
				{ID: 3, ExecID: "tidb2", Concurrency: 16, State: proto.TaskStatePending},
				{ID: 4, ExecID: "tidb2", Concurrency: 16, State: proto.TaskStatePending},
				{ID: 5, ExecID: "tidb2", Concurrency: 16, State: proto.TaskStatePending},
			},
			eligibleNodes: []string{"tidb1", "tidb2"},
			expectedSubtasks: []*proto.Subtask{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.TaskStateRunning},
				{ID: 2, ExecID: "tidb2", Concurrency: 16, State: proto.TaskStateRunning},
				{ID: 3, ExecID: "tidb2", Concurrency: 16, State: proto.TaskStatePending},
				{ID: 4, ExecID: "tidb2", Concurrency: 16, State: proto.TaskStatePending},
				{ID: 5, ExecID: "tidb1", Concurrency: 16, State: proto.TaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb1": 16, "tidb2": 16},
		},
		// scale out, balance from 5 to 2:2:1
		{
			subtasks: []*proto.Subtask{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.TaskStateRunning},
				{ID: 2, ExecID: "tidb1", Concurrency: 16, State: proto.TaskStatePending},
				{ID: 3, ExecID: "tidb1", Concurrency: 16, State: proto.TaskStatePending},
				{ID: 4, ExecID: "tidb1", Concurrency: 16, State: proto.TaskStatePending},
				{ID: 5, ExecID: "tidb1", Concurrency: 16, State: proto.TaskStatePending},
			},
			eligibleNodes: []string{"tidb1", "tidb2", "tidb3"},
			expectedSubtasks: []*proto.Subtask{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.TaskStateRunning},
				{ID: 2, ExecID: "tidb1", Concurrency: 16, State: proto.TaskStatePending},
				{ID: 3, ExecID: "tidb2", Concurrency: 16, State: proto.TaskStatePending},
				{ID: 4, ExecID: "tidb2", Concurrency: 16, State: proto.TaskStatePending},
				{ID: 5, ExecID: "tidb3", Concurrency: 16, State: proto.TaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb1": 16, "tidb2": 16, "tidb3": 16},
		},
		// scale in, balance from 1:3:1 to 3:2
		{
			subtasks: []*proto.Subtask{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.TaskStateRunning},
				{ID: 2, ExecID: "tidb2", Concurrency: 16, State: proto.TaskStateRunning},
				{ID: 3, ExecID: "tidb2", Concurrency: 16, State: proto.TaskStatePending},
				{ID: 4, ExecID: "tidb2", Concurrency: 16, State: proto.TaskStatePending},
				{ID: 5, ExecID: "tidb3", Concurrency: 16, State: proto.TaskStateRunning},
			},
			eligibleNodes: []string{"tidb1", "tidb3"},
			expectedSubtasks: []*proto.Subtask{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.TaskStateRunning},
				{ID: 2, ExecID: "tidb1", Concurrency: 16, State: proto.TaskStateRunning},
				{ID: 3, ExecID: "tidb1", Concurrency: 16, State: proto.TaskStatePending},
				{ID: 4, ExecID: "tidb3", Concurrency: 16, State: proto.TaskStatePending},
				{ID: 5, ExecID: "tidb3", Concurrency: 16, State: proto.TaskStateRunning},
			},
			expectedUsedSlots: map[string]int{"tidb1": 16, "tidb3": 16},
		},
	}

	ctx := context.Background()
	for i, c := range testCases {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			mockTaskMgr := mock.NewMockTaskManager(ctrl)
			mockTaskMgr.EXPECT().GetActiveSubtasks(gomock.Any(), gomock.Any()).Return(c.subtasks, nil)
			if !assert.ObjectsAreEqual(c.subtasks, c.expectedSubtasks) {
				mockTaskMgr.EXPECT().UpdateSubtasksExecIDs(gomock.Any(), gomock.Any()).Return(nil)
			}
			mockScheduler := mock.NewMockScheduler(ctrl)
			mockScheduler.EXPECT().GetTask().Return(&proto.Task{ID: 1}).Times(2)
			mockScheduler.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(c.eligibleNodes, nil)
			currUsedSlots := make(map[string]int, len(c.eligibleNodes))
			for _, node := range c.eligibleNodes {
				currUsedSlots[node] = 0
			}

			slotMgr := newSlotManager()
			slotMgr.capacity = 16
			b := newBalancer(Param{
				taskMgr: mockTaskMgr,
				nodeMgr: newNodeManager(),
				slotMgr: slotMgr,
			})
			b.currUsedSlots = currUsedSlots
			require.NoError(t, b.balanceSubtasks(ctx, mockScheduler))
			require.Equal(t, c.expectedUsedSlots, b.currUsedSlots)
			// c.subtasks is updated in-place
			require.Equal(t, c.expectedSubtasks, c.subtasks)
			require.True(t, ctrl.Satisfied())
		})
	}

	t.Run("scheduler err or no instance", func(t *testing.T) {
		mockTaskMgr := mock.NewMockTaskManager(ctrl)
		mockScheduler := mock.NewMockScheduler(ctrl)
		mockScheduler.EXPECT().GetTask().Return(&proto.Task{ID: 1}).Times(2)
		mockScheduler.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock error"))
		slotMgr := newSlotManager()
		slotMgr.capacity = 16
		b := newBalancer(Param{
			taskMgr: mockTaskMgr,
			nodeMgr: newNodeManager(),
			slotMgr: slotMgr,
		})
		require.ErrorContains(t, b.balanceSubtasks(ctx, mockScheduler), "mock error")
		require.True(t, ctrl.Satisfied())

		mockScheduler.EXPECT().GetTask().Return(&proto.Task{ID: 1}).Times(2)
		mockScheduler.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil)
		require.ErrorContains(t, b.balanceSubtasks(ctx, mockScheduler), "no eligible nodes to balance subtasks")
		require.True(t, ctrl.Satisfied())
	})

	t.Run("task mgr failed", func(t *testing.T) {
		mockTaskMgr := mock.NewMockTaskManager(ctrl)
		mockTaskMgr.EXPECT().GetActiveSubtasks(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock error"))
		mockScheduler := mock.NewMockScheduler(ctrl)
		mockScheduler.EXPECT().GetTask().Return(&proto.Task{ID: 1}).Times(2)
		mockScheduler.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return([]string{"tidb1"}, nil)

		slotMgr := newSlotManager()
		slotMgr.capacity = 16
		b := newBalancer(Param{
			taskMgr: mockTaskMgr,
			nodeMgr: newNodeManager(),
			slotMgr: slotMgr,
		})
		require.ErrorContains(t, b.balanceSubtasks(ctx, mockScheduler), "mock error")
		require.True(t, ctrl.Satisfied())

		b.currUsedSlots = map[string]int{"tidb1": 0, "tidb2": 0}
		mockTaskMgr.EXPECT().GetActiveSubtasks(gomock.Any(), gomock.Any()).Return(
			[]*proto.Subtask{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.TaskStateRunning},
				{ID: 2, ExecID: "tidb1", Concurrency: 16, State: proto.TaskStatePending},
			}, nil)
		mockTaskMgr.EXPECT().UpdateSubtasksExecIDs(gomock.Any(), gomock.Any()).Return(errors.New("mock error2"))
		mockScheduler.EXPECT().GetTask().Return(&proto.Task{ID: 1}).Times(2)
		mockScheduler.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return([]string{"tidb1", "tidb2"}, nil)
		require.ErrorContains(t, b.balanceSubtasks(ctx, mockScheduler), "mock error2")
		// not updated
		require.Equal(t, map[string]int{"tidb1": 0, "tidb2": 0}, b.currUsedSlots)
		require.True(t, ctrl.Satisfied())
	})
}

func TestBalancerUpdateUsedNodes(t *testing.T) {
	b := newBalancer(Param{})
	b.updateUsedNodes([]*proto.Subtask{
		{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.TaskStateRunning},
		{ID: 2, ExecID: "tidb1", Concurrency: 16, State: proto.TaskStatePending},
	})
	require.Equal(t, map[string]int{"tidb1": 16}, b.currUsedSlots)
	b.updateUsedNodes([]*proto.Subtask{
		{ID: 3, ExecID: "tidb1", Concurrency: 4, State: proto.TaskStateRunning},
		{ID: 4, ExecID: "tidb2", Concurrency: 8, State: proto.TaskStatePending},
		{ID: 5, ExecID: "tidb3", Concurrency: 12, State: proto.TaskStatePending},
	})
	require.Equal(t, map[string]int{"tidb1": 20, "tidb2": 8, "tidb3": 12}, b.currUsedSlots)
}
