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

type balanceTestCase struct {
	subtasks          []*proto.SubtaskBase
	eligibleNodes     []string
	initUsedSlots     map[string]int
	expectedSubtasks  []*proto.SubtaskBase
	expectedUsedSlots map[string]int
}

func TestBalanceOneTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	testCases := []balanceTestCase{
		// no subtasks to balance, no need to do anything.
		{
			subtasks:          []*proto.SubtaskBase{},
			eligibleNodes:     []string{"tidb1"},
			initUsedSlots:     map[string]int{"tidb1": 0},
			expectedSubtasks:  []*proto.SubtaskBase{},
			expectedUsedSlots: map[string]int{"tidb1": 0},
		},
		// balanced, no need to do anything.
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 3, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStatePending},
			},
			eligibleNodes: []string{"tidb1", "tidb2"},
			initUsedSlots: map[string]int{"tidb1": 0, "tidb2": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 3, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb1": 16, "tidb2": 16},
		},
		// balanced case 2, make sure the remainder calculate part is right, so we don't
		// balance subtasks to 2:2:0
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 3, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb3", Concurrency: 16, State: proto.SubtaskStatePending},
			},
			eligibleNodes: []string{"tidb1", "tidb2", "tidb3"},
			initUsedSlots: map[string]int{"tidb1": 0, "tidb2": 0, "tidb3": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 3, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb3", Concurrency: 16, State: proto.SubtaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb1": 16, "tidb2": 16, "tidb3": 16},
		},
		// no eligible nodes to run those subtasks, leave it unbalanced.
		// used slots will not be changed.
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStatePending},
			},
			eligibleNodes: []string{"tidb1", "tidb2"},
			initUsedSlots: map[string]int{"tidb1": 8, "tidb2": 8},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb1": 8, "tidb2": 8},
		},
		// balance subtasks to eligible nodes, tidb1 has 8 used slots cannot run target subtasks.
		// all subtasks will be balanced to tidb2.
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStatePending},
			},
			eligibleNodes: []string{"tidb1", "tidb2"},
			initUsedSlots: map[string]int{"tidb1": 8, "tidb2": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb1": 8, "tidb2": 16},
		},
		// running subtasks are not re-scheduled if the node is eligible, we leave it un-balanced.
		// task executor should mark those subtasks as pending, then we can balance them.
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 3, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStateRunning},
			},
			eligibleNodes: []string{"tidb1", "tidb2"},
			initUsedSlots: map[string]int{"tidb1": 0, "tidb2": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 3, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStateRunning},
			},
			expectedUsedSlots: map[string]int{"tidb1": 16, "tidb2": 0},
		},
		// balance from 1:4 to 2:3
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 3, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 5, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStatePending},
			},
			eligibleNodes: []string{"tidb1", "tidb2"},
			initUsedSlots: map[string]int{"tidb1": 0, "tidb2": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 3, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 5, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb1": 16, "tidb2": 16},
		},
		// scale out, balance from 5 to 2:2:1
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 3, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 5, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStatePending},
			},
			eligibleNodes: []string{"tidb1", "tidb2", "tidb3"},
			initUsedSlots: map[string]int{"tidb1": 0, "tidb2": 0, "tidb3": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 3, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 5, ExecID: "tidb3", Concurrency: 16, State: proto.SubtaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb1": 16, "tidb2": 16, "tidb3": 16},
		},
		// scale out case 2: balance from 4 to 2:1:1
		// this case checks the remainder part is right, so we don't balance it as 2:2:0.
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 3, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStatePending},
			},
			eligibleNodes: []string{"tidb1", "tidb2", "tidb3"},
			initUsedSlots: map[string]int{"tidb1": 0, "tidb2": 0, "tidb3": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 3, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb3", Concurrency: 16, State: proto.SubtaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb1": 16, "tidb2": 16, "tidb3": 16},
		},
		// scale in, balance from 1:3:1 to 3:2
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 3, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 5, ExecID: "tidb3", Concurrency: 16, State: proto.SubtaskStateRunning},
			},
			eligibleNodes: []string{"tidb1", "tidb3"},
			initUsedSlots: map[string]int{"tidb1": 0, "tidb3": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 3, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb3", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 5, ExecID: "tidb3", Concurrency: 16, State: proto.SubtaskStateRunning},
			},
			expectedUsedSlots: map[string]int{"tidb1": 16, "tidb3": 16},
		},
		// scale in and out at the same time, balance from 2:1 to 2:1
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 3, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStatePending},
			},
			eligibleNodes: []string{"tidb2", "tidb3"},
			initUsedSlots: map[string]int{"tidb2": 0, "tidb3": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb3", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 3, ExecID: "tidb2", Concurrency: 16, State: proto.SubtaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb2": 16, "tidb3": 16},
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
			mockScheduler.EXPECT().GetTask().Return(&proto.Task{TaskBase: proto.TaskBase{ID: 1}}).Times(2)
			mockScheduler.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil)

			slotMgr := newSlotManager()
			slotMgr.updateCapacity(16)
			b := newBalancer(Param{
				taskMgr: mockTaskMgr,
				nodeMgr: newNodeManager(""),
				slotMgr: slotMgr,
			})
			b.currUsedSlots = c.initUsedSlots
			require.NoError(t, b.balanceSubtasks(ctx, mockScheduler, c.eligibleNodes))
			require.Equal(t, c.expectedUsedSlots, b.currUsedSlots)
			// c.subtasks is updated in-place
			require.Equal(t, c.expectedSubtasks, c.subtasks)
			require.True(t, ctrl.Satisfied())
		})
	}

	t.Run("scheduler err or no instance", func(t *testing.T) {
		mockTaskMgr := mock.NewMockTaskManager(ctrl)
		mockScheduler := mock.NewMockScheduler(ctrl)
		mockScheduler.EXPECT().GetTask().Return(&proto.Task{TaskBase: proto.TaskBase{ID: 1}}).Times(2)
		mockScheduler.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock error"))
		slotMgr := newSlotManager()
		slotMgr.updateCapacity(16)
		b := newBalancer(Param{
			taskMgr: mockTaskMgr,
			nodeMgr: newNodeManager(""),
			slotMgr: slotMgr,
		})
		require.ErrorContains(t, b.balanceSubtasks(ctx, mockScheduler, []string{"tidb1"}), "mock error")
		require.True(t, ctrl.Satisfied())

		mockScheduler.EXPECT().GetTask().Return(&proto.Task{TaskBase: proto.TaskBase{ID: 1}}).Times(2)
		mockScheduler.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil)
		require.ErrorContains(t, b.balanceSubtasks(ctx, mockScheduler, nil), "no eligible nodes to balance subtasks")
		require.True(t, ctrl.Satisfied())
	})

	t.Run("task mgr failed", func(t *testing.T) {
		mockTaskMgr := mock.NewMockTaskManager(ctrl)
		mockTaskMgr.EXPECT().GetActiveSubtasks(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock error"))
		mockScheduler := mock.NewMockScheduler(ctrl)
		mockScheduler.EXPECT().GetTask().Return(&proto.Task{TaskBase: proto.TaskBase{ID: 1}}).Times(2)
		mockScheduler.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return([]string{"tidb1"}, nil)

		slotMgr := newSlotManager()
		slotMgr.updateCapacity(16)
		b := newBalancer(Param{
			taskMgr: mockTaskMgr,
			nodeMgr: newNodeManager(""),
			slotMgr: slotMgr,
		})
		require.ErrorContains(t, b.balanceSubtasks(ctx, mockScheduler, []string{"tidb1"}), "mock error")
		require.True(t, ctrl.Satisfied())

		b.currUsedSlots = map[string]int{"tidb1": 0, "tidb2": 0}
		mockTaskMgr.EXPECT().GetActiveSubtasks(gomock.Any(), gomock.Any()).Return(
			[]*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStatePending},
			}, nil)
		mockTaskMgr.EXPECT().UpdateSubtasksExecIDs(gomock.Any(), gomock.Any()).Return(errors.New("mock error2"))
		mockScheduler.EXPECT().GetTask().Return(&proto.Task{TaskBase: proto.TaskBase{ID: 1}}).Times(2)
		mockScheduler.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil)
		require.ErrorContains(t, b.balanceSubtasks(ctx, mockScheduler, []string{"tidb1", "tidb2"}), "mock error2")
		// not updated
		require.Equal(t, map[string]int{"tidb1": 0, "tidb2": 0}, b.currUsedSlots)
		require.True(t, ctrl.Satisfied())
	})
}

func TestBalanceMultipleTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockTaskMgr := mock.NewMockTaskManager(ctrl)

	taskCases := []struct {
		subtasks, expectedSubtasks []*proto.SubtaskBase
	}{
		// task 1 is balanced
		// used slots will be {tidb1: 8, tidb2: 8, tidb3: 0}
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", Concurrency: 8, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", Concurrency: 8, State: proto.SubtaskStateRunning},
			},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", Concurrency: 8, State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", Concurrency: 8, State: proto.SubtaskStateRunning},
			},
		},
		// task 2 require balance
		// used slots will be {tidb1: 16, tidb2: 16, tidb3: 8}
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 3, ExecID: "tidb1", Concurrency: 8, State: proto.SubtaskStateRunning},
				{ID: 4, ExecID: "tidb4", Concurrency: 8, State: proto.SubtaskStateRunning},
				{ID: 5, ExecID: "tidb4", Concurrency: 8, State: proto.SubtaskStatePending},
			},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 3, ExecID: "tidb1", Concurrency: 8, State: proto.SubtaskStateRunning},
				{ID: 4, ExecID: "tidb2", Concurrency: 8, State: proto.SubtaskStateRunning},
				{ID: 5, ExecID: "tidb3", Concurrency: 8, State: proto.SubtaskStatePending},
			},
		},
		// task 3 require balance, but no eligible node, so it's not balanced, and
		// used slots are not updated
		// used slots will be {tidb1: 16, tidb2: 16, tidb3: 8}
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 6, ExecID: "tidb4", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 7, ExecID: "tidb4", Concurrency: 16, State: proto.SubtaskStatePending},
			},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 6, ExecID: "tidb4", Concurrency: 16, State: proto.SubtaskStatePending},
				{ID: 7, ExecID: "tidb4", Concurrency: 16, State: proto.SubtaskStatePending},
			},
		},
		// task 4 require balance
		// used slots will be {tidb1: 16, tidb2: 16, tidb3: 16}
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 8, ExecID: "tidb1", Concurrency: 8, State: proto.SubtaskStatePending},
			},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 8, ExecID: "tidb3", Concurrency: 8, State: proto.SubtaskStatePending},
			},
		},
	}
	ctx := context.Background()

	manager := NewManager(ctx, mockTaskMgr, "1")
	manager.slotMgr.updateCapacity(16)
	manager.nodeMgr.nodes.Store(&[]proto.ManagedNode{{ID: "tidb1", Role: ""}, {ID: "tidb2", Role: ""}, {ID: "tidb3", Role: ""}})
	b := newBalancer(Param{
		taskMgr: manager.taskMgr,
		nodeMgr: manager.nodeMgr,
		slotMgr: manager.slotMgr,
	})
	for i := range taskCases {
		taskID := int64(i + 1)
		sch := mock.NewMockScheduler(ctrl)
		sch.EXPECT().GetTask().Return(&proto.Task{TaskBase: proto.TaskBase{ID: taskID}}).AnyTimes()
		manager.addScheduler(taskID, sch)
	}
	require.Len(t, manager.getSchedulers(), 4)

	// fail fast if balance failed on some task
	manager.getSchedulers()[0].(*mock.MockScheduler).EXPECT().
		GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock error"))
	b.balance(ctx, manager)
	require.True(t, ctrl.Satisfied())

	// balance multiple tasks
	for i, c := range taskCases {
		taskID := int64(i + 1)
		if !assert.ObjectsAreEqual(c.subtasks, c.expectedSubtasks) {
			gomock.InOrder(
				manager.getSchedulers()[i].(*mock.MockScheduler).EXPECT().
					GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil),
				mockTaskMgr.EXPECT().GetActiveSubtasks(gomock.Any(), taskID).Return(c.subtasks, nil),
				mockTaskMgr.EXPECT().UpdateSubtasksExecIDs(gomock.Any(), gomock.Any()).Return(nil),
			)
		} else {
			gomock.InOrder(
				manager.getSchedulers()[i].(*mock.MockScheduler).EXPECT().
					GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil),
				mockTaskMgr.EXPECT().GetActiveSubtasks(gomock.Any(), taskID).Return(c.subtasks, nil),
			)
		}
	}
	b.balance(ctx, manager)
	require.Equal(t, map[string]int{"tidb1": 16, "tidb2": 16, "tidb3": 16}, b.currUsedSlots)
	require.True(t, ctrl.Satisfied())
	for _, c := range taskCases {
		require.Equal(t, c.expectedSubtasks, c.subtasks)
	}
}

func TestBalancerUpdateUsedNodes(t *testing.T) {
	b := newBalancer(Param{})
	b.updateUsedNodes([]*proto.SubtaskBase{
		{ID: 1, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStateRunning},
		{ID: 2, ExecID: "tidb1", Concurrency: 16, State: proto.SubtaskStatePending},
	})
	require.Equal(t, map[string]int{"tidb1": 16}, b.currUsedSlots)
	b.updateUsedNodes([]*proto.SubtaskBase{
		{ID: 3, ExecID: "tidb1", Concurrency: 4, State: proto.SubtaskStateRunning},
		{ID: 4, ExecID: "tidb2", Concurrency: 8, State: proto.SubtaskStatePending},
		{ID: 5, ExecID: "tidb3", Concurrency: 12, State: proto.SubtaskStatePending},
	})
	require.Equal(t, map[string]int{"tidb1": 20, "tidb2": 8, "tidb3": 12}, b.currUsedSlots)
}
