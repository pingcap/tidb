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
	"github.com/pingcap/tidb/pkg/dxf/framework/mock"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type balanceTestCase struct {
	subtasks          []*proto.SubtaskBase
	eligibleNodes     []string
	maxNodeCount      int
	nonPreemptible    bool
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
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", State: proto.SubtaskStateRunning},
				{ID: 3, ExecID: "tidb2", State: proto.SubtaskStatePending},
			},
			eligibleNodes: []string{"tidb1", "tidb2"},
			initUsedSlots: map[string]int{"tidb1": 0, "tidb2": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", State: proto.SubtaskStateRunning},
				{ID: 3, ExecID: "tidb2", State: proto.SubtaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb1": 16, "tidb2": 16},
		},
		// balanced case 2, make sure the remainder calculate part is right, so we don't
		// balance subtasks to 2:2:0
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb2", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", State: proto.SubtaskStatePending},
				{ID: 3, ExecID: "tidb1", State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb3", State: proto.SubtaskStatePending},
			},
			eligibleNodes: []string{"tidb1", "tidb2", "tidb3"},
			initUsedSlots: map[string]int{"tidb1": 0, "tidb2": 0, "tidb3": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb2", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", State: proto.SubtaskStatePending},
				{ID: 3, ExecID: "tidb1", State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb3", State: proto.SubtaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb1": 16, "tidb2": 16, "tidb3": 16},
		},
		// no eligible nodes to run those subtasks, leave it unbalanced.
		// used slots will not be changed.
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb1", State: proto.SubtaskStatePending},
			},
			eligibleNodes: []string{"tidb1", "tidb2"},
			initUsedSlots: map[string]int{"tidb1": 8, "tidb2": 8},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb1", State: proto.SubtaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb1": 8, "tidb2": 8},
		},
		// balance subtasks to eligible nodes, tidb1 has 8 used slots cannot run target subtasks.
		// all subtasks will be balanced to tidb2.
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb1", State: proto.SubtaskStatePending},
			},
			eligibleNodes: []string{"tidb1", "tidb2"},
			initUsedSlots: map[string]int{"tidb1": 8, "tidb2": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb2", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", State: proto.SubtaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb1": 8, "tidb2": 16},
		},
		// for a non-preemptible task, the running subtask stays on tidb1 that has no
		// enough slots, and the pending one moves. tidb4 is alive but not eligible,
		// e.g. after a background node joins, so the same applies. tidb3 is dead, so
		// its running subtask moves too.
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb1", State: proto.SubtaskStatePending},
				{ID: 3, ExecID: "tidb3", State: proto.SubtaskStateRunning},
				{ID: 4, ExecID: "tidb4", State: proto.SubtaskStateRunning},
				{ID: 5, ExecID: "tidb4", State: proto.SubtaskStatePending},
			},
			eligibleNodes:  []string{"tidb1", "tidb2"},
			nonPreemptible: true,
			initUsedSlots:  map[string]int{"tidb1": 8, "tidb2": 0, "tidb4": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", State: proto.SubtaskStatePending},
				{ID: 3, ExecID: "tidb2", State: proto.SubtaskStateRunning},
				{ID: 4, ExecID: "tidb4", State: proto.SubtaskStateRunning},
				{ID: 5, ExecID: "tidb2", State: proto.SubtaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb1": 24, "tidb2": 16, "tidb4": 16},
		},
		// After the node limit is reduced to two, tidb1 and tidb2 still run protected
		// subtasks. Only tidb2 can take pending work without exceeding the new limit.
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", State: proto.SubtaskStateRunning},
				{ID: 3, ExecID: "tidb3", State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb3", State: proto.SubtaskStatePending},
			},
			eligibleNodes:  []string{"tidb3", "tidb2"},
			maxNodeCount:   2,
			nonPreemptible: true,
			initUsedSlots:  map[string]int{"tidb1": 0, "tidb2": 0, "tidb3": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", State: proto.SubtaskStateRunning},
				{ID: 3, ExecID: "tidb2", State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb2", State: proto.SubtaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb1": 16, "tidb2": 16, "tidb3": 0},
		},
		// running subtasks are not re-scheduled if the node is eligible, we leave it un-balanced.
		// task executor should mark those subtasks as pending, then we can balance them.
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 3, ExecID: "tidb1", State: proto.SubtaskStateRunning},
			},
			eligibleNodes: []string{"tidb1", "tidb2"},
			initUsedSlots: map[string]int{"tidb1": 0, "tidb2": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 3, ExecID: "tidb1", State: proto.SubtaskStateRunning},
			},
			expectedUsedSlots: map[string]int{"tidb1": 16, "tidb2": 0},
		},
		// balance from 1:4 to 2:3
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", State: proto.SubtaskStateRunning},
				{ID: 3, ExecID: "tidb2", State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb2", State: proto.SubtaskStatePending},
				{ID: 5, ExecID: "tidb2", State: proto.SubtaskStatePending},
			},
			eligibleNodes: []string{"tidb1", "tidb2"},
			initUsedSlots: map[string]int{"tidb1": 0, "tidb2": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", State: proto.SubtaskStateRunning},
				{ID: 3, ExecID: "tidb2", State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb2", State: proto.SubtaskStatePending},
				{ID: 5, ExecID: "tidb1", State: proto.SubtaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb1": 16, "tidb2": 16},
		},
		// scale out, balance from 5 to 2:2:1
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb1", State: proto.SubtaskStatePending},
				{ID: 3, ExecID: "tidb1", State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb1", State: proto.SubtaskStatePending},
				{ID: 5, ExecID: "tidb1", State: proto.SubtaskStatePending},
			},
			eligibleNodes: []string{"tidb1", "tidb2", "tidb3"},
			initUsedSlots: map[string]int{"tidb1": 0, "tidb2": 0, "tidb3": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb1", State: proto.SubtaskStatePending},
				{ID: 3, ExecID: "tidb2", State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb2", State: proto.SubtaskStatePending},
				{ID: 5, ExecID: "tidb3", State: proto.SubtaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb1": 16, "tidb2": 16, "tidb3": 16},
		},
		// scale out case 2: balance from 4 to 2:1:1
		// this case checks the remainder part is right, so we don't balance it as 2:2:0.
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb2", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", State: proto.SubtaskStatePending},
				{ID: 3, ExecID: "tidb2", State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb2", State: proto.SubtaskStatePending},
			},
			eligibleNodes: []string{"tidb1", "tidb2", "tidb3"},
			initUsedSlots: map[string]int{"tidb1": 0, "tidb2": 0, "tidb3": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb2", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", State: proto.SubtaskStatePending},
				{ID: 3, ExecID: "tidb1", State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb3", State: proto.SubtaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb1": 16, "tidb2": 16, "tidb3": 16},
		},
		// scale in, balance from 1:3:1 to 3:2
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", State: proto.SubtaskStateRunning},
				{ID: 3, ExecID: "tidb2", State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb2", State: proto.SubtaskStatePending},
				{ID: 5, ExecID: "tidb3", State: proto.SubtaskStateRunning},
			},
			eligibleNodes: []string{"tidb1", "tidb3"},
			initUsedSlots: map[string]int{"tidb1": 0, "tidb3": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 3, ExecID: "tidb1", State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb3", State: proto.SubtaskStatePending},
				{ID: 5, ExecID: "tidb3", State: proto.SubtaskStateRunning},
			},
			expectedUsedSlots: map[string]int{"tidb1": 16, "tidb3": 16},
		},
		// scale in and out at the same time, balance from 2:1 to 2:1
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb1", State: proto.SubtaskStatePending},
				{ID: 3, ExecID: "tidb2", State: proto.SubtaskStatePending},
			},
			eligibleNodes: []string{"tidb2", "tidb3"},
			initUsedSlots: map[string]int{"tidb2": 0, "tidb3": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb2", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb3", State: proto.SubtaskStatePending},
				{ID: 3, ExecID: "tidb2", State: proto.SubtaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb2": 16, "tidb3": 16},
		},
		// balanced, but max node count is limited.
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", State: proto.SubtaskStateRunning},
				{ID: 3, ExecID: "tidb2", State: proto.SubtaskStatePending},
			},
			maxNodeCount:  1,
			eligibleNodes: []string{"tidb1", "tidb2"},
			initUsedSlots: map[string]int{"tidb1": 0, "tidb2": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb2", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", State: proto.SubtaskStateRunning},
				{ID: 3, ExecID: "tidb2", State: proto.SubtaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb1": 0, "tidb2": 16},
		},
		// scale out, but max node count is limited.
		{
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb3", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb3", State: proto.SubtaskStatePending},
				{ID: 3, ExecID: "tidb3", State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb3", State: proto.SubtaskStatePending},
				{ID: 5, ExecID: "tidb3", State: proto.SubtaskStatePending},
			},
			eligibleNodes: []string{"tidb1", "tidb2", "tidb3"},
			maxNodeCount:  2,
			initUsedSlots: map[string]int{"tidb1": 0, "tidb2": 0, "tidb3": 0},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb3", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb3", State: proto.SubtaskStatePending},
				{ID: 3, ExecID: "tidb3", State: proto.SubtaskStatePending},
				{ID: 4, ExecID: "tidb1", State: proto.SubtaskStatePending},
				{ID: 5, ExecID: "tidb1", State: proto.SubtaskStatePending},
			},
			expectedUsedSlots: map[string]int{"tidb1": 16, "tidb2": 0, "tidb3": 16},
		},
	}

	ctx := context.Background()
	for i, c := range testCases {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			mockTaskMgr := mock.NewMockTaskManager(ctrl)
			mockTaskMgr.EXPECT().GetActiveSubtasks(gomock.Any(), gomock.Any()).Return(c.subtasks, nil)
			mockTaskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(map[string]int{}, nil).AnyTimes()
			if !assert.ObjectsAreEqual(c.subtasks, c.expectedSubtasks) {
				mockTaskMgr.EXPECT().UpdateSubtasksExecIDs(gomock.Any(), gomock.Any()).Return(nil)
			}
			mockScheduler := mock.NewMockScheduler(ctrl)
			mockScheduler.EXPECT().GetTask().Return(&proto.Task{TaskBase: proto.TaskBase{ID: 1, RequiredSlots: 16, MaxNodeCount: c.maxNodeCount,
				ExtraParams: proto.ExtraParams{NonPreemptible: c.nonPreemptible}}}).Times(2)
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

	t.Run("non-preemptible node limit after scope change", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		taskMgr := mock.NewMockTaskManager(ctrl)
		manager := NewManager(ctx, nil, taskMgr, "1", proto.NodeResourceForTest)
		manager.slotMgr.updateCapacity(16)
		manager.nodeMgr.nodes.Store(&[]proto.ManagedNode{{ID: "tidb1"}, {ID: "tidb2", Role: "background"}})
		sch := mock.NewMockScheduler(ctrl)
		sch.EXPECT().GetTask().Return(&proto.Task{TaskBase: proto.TaskBase{ID: 1, RequiredSlots: 16, MaxNodeCount: 1,
			ExtraParams: proto.ExtraParams{NonPreemptible: true}}}).AnyTimes()
		sch.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil).Times(2)
		manager.addScheduler(1, sch)

		running := &proto.SubtaskBase{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning}
		pending := &proto.SubtaskBase{ID: 2, ExecID: "tidb1", State: proto.SubtaskStatePending}
		taskMgr.EXPECT().GetUsedSlotsOnNodes(ctx).Return(map[string]int{"tidb1": 16}, nil).AnyTimes()
		taskMgr.EXPECT().GetActiveSubtasks(ctx, int64(1)).Return([]*proto.SubtaskBase{running, pending}, nil)
		manager.balancer.balance(ctx, manager)
		require.Equal(t, "tidb1", pending.ExecID)
		require.Equal(t, map[string]int{"tidb1": 16, "tidb2": 0}, manager.balancer.currUsedSlots)

		// Once the running subtask finishes, the background node can take the pending one.
		taskMgr.EXPECT().GetActiveSubtasks(ctx, int64(1)).Return([]*proto.SubtaskBase{pending}, nil)
		taskMgr.EXPECT().UpdateSubtasksExecIDs(ctx, []*proto.SubtaskBase{pending}).Return(nil)
		manager.balancer.balance(ctx, manager)
		require.Equal(t, "tidb2", pending.ExecID)
		require.Equal(t, map[string]int{"tidb1": 0, "tidb2": 16}, manager.balancer.currUsedSlots)
	})

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
		mockScheduler.EXPECT().GetTask().Return(&proto.Task{TaskBase: proto.TaskBase{ID: 1, RequiredSlots: 16}}).Times(2)
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
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb1", State: proto.SubtaskStatePending},
			}, nil)
		mockTaskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(map[string]int{}, nil)
		mockTaskMgr.EXPECT().UpdateSubtasksExecIDs(gomock.Any(), gomock.Any()).Return(errors.New("mock error2"))
		mockScheduler.EXPECT().GetTask().Return(&proto.Task{TaskBase: proto.TaskBase{ID: 1, RequiredSlots: 16}}).Times(2)
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
		requiredSlots              int
		subtasks, expectedSubtasks []*proto.SubtaskBase
	}{
		// task 1 is balanced
		// used slots will be {tidb1: 8, tidb2: 8, tidb3: 0}
		{
			requiredSlots: 8,
			subtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", State: proto.SubtaskStateRunning},
			},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 2, ExecID: "tidb2", State: proto.SubtaskStateRunning},
			},
		},
		// task 2 require balance
		// used slots will be {tidb1: 16, tidb2: 16, tidb3: 8}
		{
			requiredSlots: 8,
			subtasks: []*proto.SubtaskBase{
				{ID: 3, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 4, ExecID: "tidb4", State: proto.SubtaskStateRunning},
				{ID: 5, ExecID: "tidb4", State: proto.SubtaskStatePending},
			},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 3, ExecID: "tidb1", State: proto.SubtaskStateRunning},
				{ID: 4, ExecID: "tidb2", State: proto.SubtaskStateRunning},
				{ID: 5, ExecID: "tidb3", State: proto.SubtaskStatePending},
			},
		},
		// task 3 require balance, but no eligible node, so it's not balanced, and
		// used slots are not updated
		// used slots will be {tidb1: 16, tidb2: 16, tidb3: 8}
		{
			requiredSlots: 16,
			subtasks: []*proto.SubtaskBase{
				{ID: 6, ExecID: "tidb4", State: proto.SubtaskStatePending},
				{ID: 7, ExecID: "tidb4", State: proto.SubtaskStatePending},
			},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 6, ExecID: "tidb4", State: proto.SubtaskStatePending},
				{ID: 7, ExecID: "tidb4", State: proto.SubtaskStatePending},
			},
		},
		// task 4 require balance
		// used slots will be {tidb1: 16, tidb2: 16, tidb3: 16}
		{
			requiredSlots: 8,
			subtasks: []*proto.SubtaskBase{
				{ID: 8, ExecID: "tidb1", State: proto.SubtaskStatePending},
			},
			expectedSubtasks: []*proto.SubtaskBase{
				{ID: 8, ExecID: "tidb3", State: proto.SubtaskStatePending},
			},
		},
	}
	ctx := context.Background()

	manager := NewManager(ctx, nil, mockTaskMgr, "1", proto.NodeResourceForTest)
	manager.slotMgr.updateCapacity(16)
	manager.nodeMgr.nodes.Store(&[]proto.ManagedNode{{ID: "tidb1", Role: ""}, {ID: "tidb2", Role: ""}, {ID: "tidb3", Role: ""}})
	b := newBalancer(Param{
		taskMgr: manager.taskMgr,
		nodeMgr: manager.nodeMgr,
		slotMgr: manager.slotMgr,
	})
	for i, tc := range taskCases {
		taskID := int64(i + 1)
		sch := mock.NewMockScheduler(ctrl)
		sch.EXPECT().GetTask().Return(&proto.Task{TaskBase: proto.TaskBase{ID: taskID, RequiredSlots: tc.requiredSlots}}).AnyTimes()
		manager.addScheduler(taskID, sch)
	}
	require.Len(t, manager.getSchedulers(), 4)

	// fail fast if balance failed on some task
	manager.getSchedulers()[0].(*mock.MockScheduler).EXPECT().
		GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock error"))
	b.balance(ctx, manager)
	require.True(t, ctrl.Satisfied())

	// A failed slot query stops this round without updating assignments.
	manager.getSchedulers()[0].(*mock.MockScheduler).EXPECT().
		GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil)
	mockTaskMgr.EXPECT().GetActiveSubtasks(gomock.Any(), int64(1)).Return(taskCases[0].subtasks, nil)
	mockTaskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(nil, errors.New("mock slot query error"))
	b.balance(ctx, manager)
	require.True(t, ctrl.Satisfied())
	require.Equal(t, map[string]int{"tidb1": 0, "tidb2": 0, "tidb3": 0}, b.currUsedSlots)

	// The next round reloads slots and balances all tasks.
	mockTaskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(map[string]int{}, nil)
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

	// task 10 runs before the non-preemptible task 11, but it cannot take the slots
	// of task 11 on tidb1, so its subtask there moves to tidb2.
	manager = NewManager(ctx, nil, mockTaskMgr, "1", proto.NodeResourceForTest)
	manager.slotMgr.updateCapacity(16)
	manager.nodeMgr.nodes.Store(&[]proto.ManagedNode{{ID: "tidb1", Role: ""}, {ID: "tidb2", Role: ""}})
	b = newBalancer(Param{
		taskMgr: manager.taskMgr,
		nodeMgr: manager.nodeMgr,
		slotMgr: manager.slotMgr,
	})
	sch10, sch11 := mock.NewMockScheduler(ctrl), mock.NewMockScheduler(ctrl)
	sch10.EXPECT().GetTask().Return(&proto.Task{TaskBase: proto.TaskBase{ID: 10, RequiredSlots: 8}}).AnyTimes()
	sch11.EXPECT().GetTask().Return(&proto.Task{TaskBase: proto.TaskBase{ID: 11, RequiredSlots: 16,
		ExtraParams: proto.ExtraParams{NonPreemptible: true}}}).AnyTimes()
	manager.addScheduler(10, sch10)
	manager.addScheduler(11, sch11)
	subtasks10 := []*proto.SubtaskBase{
		{ID: 1, ExecID: "tidb1", State: proto.SubtaskStatePending},
		{ID: 2, ExecID: "tidb2", State: proto.SubtaskStatePending},
	}
	subtasks11 := []*proto.SubtaskBase{{ID: 3, ExecID: "tidb1", State: proto.SubtaskStateRunning}}
	gomock.InOrder(
		mockTaskMgr.EXPECT().GetActiveSubtasks(gomock.Any(), int64(11)).Return(subtasks11, nil),
		sch10.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil),
		mockTaskMgr.EXPECT().GetActiveSubtasks(gomock.Any(), int64(10)).Return(subtasks10, nil),
		mockTaskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(map[string]int{"tidb1": 16}, nil),
		mockTaskMgr.EXPECT().UpdateSubtasksExecIDs(gomock.Any(), gomock.Any()).Return(nil),
		sch11.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil),
	)
	b.balance(ctx, manager)
	require.True(t, ctrl.Satisfied())
	require.Equal(t, []*proto.SubtaskBase{
		{ID: 1, ExecID: "tidb2", State: proto.SubtaskStatePending},
		{ID: 2, ExecID: "tidb2", State: proto.SubtaskStatePending},
	}, subtasks10)
	require.Equal(t, "tidb1", subtasks11[0].ExecID)
	require.Equal(t, map[string]int{"tidb1": 16, "tidb2": 8}, b.currUsedSlots)

	// task 20 has its subtask on tidb3 that is dead, and the non-preemptible tasks
	// 21 and 22 of lower rank hold slots on all live nodes. The subtask still moves
	// to a live node, where task 20 waits for them, and they keep running.
	manager = NewManager(ctx, nil, mockTaskMgr, "1", proto.NodeResourceForTest)
	manager.slotMgr.updateCapacity(16)
	manager.nodeMgr.nodes.Store(&[]proto.ManagedNode{{ID: "tidb1", Role: ""}, {ID: "tidb2", Role: ""}})
	b = newBalancer(Param{
		taskMgr: manager.taskMgr,
		nodeMgr: manager.nodeMgr,
		slotMgr: manager.slotMgr,
	})
	nonPreemptible := func(id int64) *mock.MockScheduler {
		sch := mock.NewMockScheduler(ctrl)
		sch.EXPECT().GetTask().Return(&proto.Task{TaskBase: proto.TaskBase{ID: id, RequiredSlots: 8,
			ExtraParams: proto.ExtraParams{NonPreemptible: true}}}).AnyTimes()
		return sch
	}
	sch20, sch21, sch22 := mock.NewMockScheduler(ctrl), nonPreemptible(21), nonPreemptible(22)
	sch20.EXPECT().GetTask().Return(&proto.Task{TaskBase: proto.TaskBase{ID: 20, RequiredSlots: 16}}).AnyTimes()
	manager.addScheduler(20, sch20)
	manager.addScheduler(21, sch21)
	manager.addScheduler(22, sch22)
	subtasks20 := []*proto.SubtaskBase{{ID: 1, ExecID: "tidb3", State: proto.SubtaskStatePending}}
	subtasks21 := []*proto.SubtaskBase{{ID: 2, ExecID: "tidb1", State: proto.SubtaskStateRunning}}
	subtasks22 := []*proto.SubtaskBase{{ID: 3, ExecID: "tidb2", State: proto.SubtaskStateRunning}}
	gomock.InOrder(
		mockTaskMgr.EXPECT().GetActiveSubtasks(gomock.Any(), int64(21)).Return(subtasks21, nil),
		mockTaskMgr.EXPECT().GetActiveSubtasks(gomock.Any(), int64(22)).Return(subtasks22, nil),
		sch20.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil),
		mockTaskMgr.EXPECT().GetActiveSubtasks(gomock.Any(), int64(20)).Return(subtasks20, nil),
		mockTaskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(map[string]int{"tidb1": 8, "tidb2": 8}, nil),
		mockTaskMgr.EXPECT().UpdateSubtasksExecIDs(gomock.Any(), gomock.Any()).Return(nil),
		sch21.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil),
		sch22.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil),
	)
	b.balance(ctx, manager)
	require.True(t, ctrl.Satisfied())
	require.Equal(t, "tidb1", subtasks20[0].ExecID)
	require.Equal(t, "tidb1", subtasks21[0].ExecID)
	require.Equal(t, "tidb2", subtasks22[0].ExecID)
	require.Equal(t, map[string]int{"tidb1": 24, "tidb2": 8}, b.currUsedSlots)

	// task 31 is modified from 8 to 16 slots after its slots are reserved: the 8
	// reserved slots are released, and the placement charges 16.
	manager = NewManager(ctx, nil, mockTaskMgr, "1", proto.NodeResourceForTest)
	manager.slotMgr.updateCapacity(16)
	manager.nodeMgr.nodes.Store(&[]proto.ManagedNode{{ID: "tidb1", Role: ""}, {ID: "tidb2", Role: ""}})
	b = newBalancer(Param{
		taskMgr: manager.taskMgr,
		nodeMgr: manager.nodeMgr,
		slotMgr: manager.slotMgr,
	})
	task31 := func(slots int) *proto.Task {
		return &proto.Task{TaskBase: proto.TaskBase{ID: 31, RequiredSlots: slots,
			ExtraParams: proto.ExtraParams{NonPreemptible: true}}}
	}
	sch31 := mock.NewMockScheduler(ctrl)
	sch31.EXPECT().GetTask().Return(task31(8)).Times(1)
	sch31.EXPECT().GetTask().Return(task31(16)).AnyTimes()
	manager.addScheduler(31, sch31)
	subtasks31 := []*proto.SubtaskBase{{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning}}
	gomock.InOrder(
		mockTaskMgr.EXPECT().GetActiveSubtasks(gomock.Any(), int64(31)).Return(subtasks31, nil),
		sch31.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil),
		mockTaskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(map[string]int{"tidb1": 8}, nil),
	)
	b.balance(ctx, manager)
	require.True(t, ctrl.Satisfied())
	require.Equal(t, "tidb1", subtasks31[0].ExecID)
	require.Equal(t, map[string]int{"tidb1": 16, "tidb2": 0}, b.currUsedSlots)
	require.Equal(t, map[string]int{"tidb1": 0}, b.reservedSlots)
}

func TestBalancerUpdateUsedNodes(t *testing.T) {
	b := newBalancer(Param{})
	b.updateUsedNodes(&proto.TaskBase{RequiredSlots: 16}, []*proto.SubtaskBase{
		{ID: 1, ExecID: "tidb1", State: proto.SubtaskStateRunning},
		{ID: 2, ExecID: "tidb1", State: proto.SubtaskStatePending},
	})
	require.Equal(t, map[string]int{"tidb1": 16}, b.currUsedSlots)
	b.updateUsedNodes(&proto.TaskBase{RequiredSlots: 12}, []*proto.SubtaskBase{
		{ID: 3, ExecID: "tidb1", State: proto.SubtaskStateRunning},
		{ID: 4, ExecID: "tidb2", State: proto.SubtaskStatePending},
		{ID: 5, ExecID: "tidb3", State: proto.SubtaskStatePending},
	})
	require.Equal(t, map[string]int{"tidb1": 28, "tidb2": 12, "tidb3": 12}, b.currUsedSlots)
}

func TestBalanceAvoidPreemptingRunningSubtasks(t *testing.T) {
	ctx := context.Background()
	type balanceTask struct {
		requiredSlots int
		maxNodeCount  int
		subtasks      []*proto.SubtaskBase
	}
	runBalanceWithTasks := func(t *testing.T, capacity int, nodeIDs []string,
		actualUsedSlots map[string]int, tasks []balanceTask) *balancer {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockTaskMgr := mock.NewMockTaskManager(ctrl)
		manager := NewManager(ctx, nil, mockTaskMgr, "1", proto.NodeResourceForTest)
		manager.slotMgr.updateCapacity(capacity)
		nodes := make([]proto.ManagedNode, 0, len(nodeIDs))
		for _, id := range nodeIDs {
			nodes = append(nodes, proto.ManagedNode{ID: id})
		}
		manager.nodeMgr.nodes.Store(&nodes)
		b := newBalancer(Param{
			taskMgr: manager.taskMgr,
			nodeMgr: manager.nodeMgr,
			slotMgr: manager.slotMgr,
		})
		for i, task := range tasks {
			taskID := int64(i + 1)
			sch := mock.NewMockScheduler(ctrl)
			sch.EXPECT().GetTask().Return(&proto.Task{TaskBase: proto.TaskBase{
				ID:            taskID,
				RequiredSlots: task.requiredSlots,
				MaxNodeCount:  task.maxNodeCount,
			}}).AnyTimes()
			sch.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil)
			manager.addScheduler(taskID, sch)
			mockTaskMgr.EXPECT().GetActiveSubtasks(gomock.Any(), taskID).Return(task.subtasks, nil)
		}
		mockTaskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(actualUsedSlots, nil)
		mockTaskMgr.EXPECT().UpdateSubtasksExecIDs(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		b.balance(ctx, manager)
		require.True(t, ctrl.Satisfied())
		return b
	}
	runBalance := func(t *testing.T, nodeIDs []string, actualUsedSlots map[string]int,
		taskSubtasks [][]*proto.SubtaskBase) *balancer {
		tasks := make([]balanceTask, len(taskSubtasks))
		for i, sts := range taskSubtasks {
			tasks[i] = balanceTask{requiredSlots: 3, maxNodeCount: 1, subtasks: sts}
		}
		return runBalanceWithTasks(t, 3, nodeIDs, actualUsedSlots, tasks)
	}

	t.Run("move to idle node", func(t *testing.T) {
		// n1 is full, n2 and n3 are running subtasks of lower ranking tasks, n4
		// and n5 are idle.
		taskSubtasks := [][]*proto.SubtaskBase{
			{{ID: 1, ExecID: "n1", State: proto.SubtaskStateRunning}},
			{{ID: 2, ExecID: "n1", State: proto.SubtaskStatePending}},
			{{ID: 3, ExecID: "n2", State: proto.SubtaskStateRunning}},
			{{ID: 4, ExecID: "n3", State: proto.SubtaskStateRunning}},
		}
		b := runBalance(t, []string{"n1", "n2", "n3", "n4", "n5"},
			map[string]int{"n1": 6, "n2": 3, "n3": 3}, taskSubtasks)
		require.Equal(t, "n1", taskSubtasks[0][0].ExecID)
		require.Equal(t, "n4", taskSubtasks[1][0].ExecID)
		require.Equal(t, "n2", taskSubtasks[2][0].ExecID)
		require.Equal(t, "n3", taskSubtasks[3][0].ExecID)
		require.Equal(t, map[string]int{"n1": 3, "n2": 3, "n3": 3, "n4": 3}, b.actualUsedSlots)
	})

	t.Run("retain existing pending subtask", func(t *testing.T) {
		taskSubtasks := [][]*proto.SubtaskBase{
			{{ID: 1, ExecID: "n1", State: proto.SubtaskStatePending}},
			{{ID: 2, ExecID: "n1", State: proto.SubtaskStateRunning}},
		}
		b := runBalance(t, []string{"n1", "n2"}, map[string]int{"n1": 6}, taskSubtasks)
		require.Equal(t, "n1", taskSubtasks[0][0].ExecID)
		require.Equal(t, "n2", taskSubtasks[1][0].ExecID)
		require.Equal(t, map[string]int{"n1": 3, "n2": 3}, b.actualUsedSlots)
	})

	t.Run("preempt lower ranking task if no idle node", func(t *testing.T) {
		taskSubtasks := [][]*proto.SubtaskBase{
			{{ID: 1, ExecID: "n1", State: proto.SubtaskStateRunning}},
			{{ID: 2, ExecID: "n1", State: proto.SubtaskStatePending}},
			{{ID: 3, ExecID: "n2", State: proto.SubtaskStateRunning}},
		}
		b := runBalance(t, []string{"n1", "n2"}, map[string]int{"n1": 6, "n2": 3}, taskSubtasks)
		require.Equal(t, "n1", taskSubtasks[0][0].ExecID)
		require.Equal(t, "n2", taskSubtasks[1][0].ExecID)
		require.Equal(t, map[string]int{"n1": 3, "n2": 6}, b.actualUsedSlots)
		// no node has enough slots for task 3, it's not balanced.
		require.Equal(t, "n2", taskSubtasks[2][0].ExecID)
	})

	t.Run("partial slots update actualUsedSlots across relocations", func(t *testing.T) {
		// Capacity 8: task 1 (8 slots) fills n1, task 4 (4 slots) runs on n2.
		// Task 2 (4 slots) packs onto n2 without preempting task 4, raising
		// actualUsedSlots[n2] to 8. Task 3 (4 slots) still sees currUsedSlots[n2]=4,
		// so updated actualUsedSlots[n2]=8 steers task 3 onto idle n3 instead.
		tasks := []balanceTask{
			{requiredSlots: 8, maxNodeCount: 1, subtasks: []*proto.SubtaskBase{{ID: 1, ExecID: "n1", State: proto.SubtaskStateRunning}}},
			{requiredSlots: 4, maxNodeCount: 1, subtasks: []*proto.SubtaskBase{{ID: 2, ExecID: "n1", State: proto.SubtaskStatePending}}},
			{requiredSlots: 4, maxNodeCount: 1, subtasks: []*proto.SubtaskBase{{ID: 3, ExecID: "n1", State: proto.SubtaskStatePending}}},
			{requiredSlots: 4, maxNodeCount: 1, subtasks: []*proto.SubtaskBase{{ID: 4, ExecID: "n2", State: proto.SubtaskStateRunning}}},
		}
		b := runBalanceWithTasks(t, 8, []string{"n1", "n2", "n3", "n4"},
			map[string]int{"n1": 16, "n2": 4}, tasks)
		require.Equal(t, "n1", tasks[0].subtasks[0].ExecID)
		require.Equal(t, "n2", tasks[1].subtasks[0].ExecID)
		require.Equal(t, "n3", tasks[2].subtasks[0].ExecID)
		require.Equal(t, "n2", tasks[3].subtasks[0].ExecID)
		require.Equal(t, map[string]int{"n1": 8, "n2": 8, "n3": 4}, b.actualUsedSlots)
	})

	t.Run("clamp stale actualUsedSlots on relocation", func(t *testing.T) {
		// Simulate a stale snapshot that missed task 2's newly inserted subtask on n1.
		taskSubtasks := [][]*proto.SubtaskBase{
			{{ID: 1, ExecID: "n1", State: proto.SubtaskStateRunning}},
			{{ID: 2, ExecID: "n1", State: proto.SubtaskStatePending}},
		}
		b := runBalance(t, []string{"n1", "n2"}, map[string]int{}, taskSubtasks)
		require.Equal(t, "n1", taskSubtasks[0][0].ExecID)
		require.Equal(t, "n2", taskSubtasks[1][0].ExecID)
		require.Equal(t, map[string]int{"n1": 0, "n2": 3}, b.actualUsedSlots)
	})

	t.Run("unbounded max node count avoids busy node", func(t *testing.T) {
		tasks := []balanceTask{
			{
				requiredSlots: 3,
				maxNodeCount:  0,
				subtasks: []*proto.SubtaskBase{
					{ID: 1, ExecID: "n1", State: proto.SubtaskStateRunning},
					{ID: 2, ExecID: "n1", State: proto.SubtaskStatePending},
				},
			},
			{
				requiredSlots: 3,
				maxNodeCount:  1,
				subtasks: []*proto.SubtaskBase{
					{ID: 3, ExecID: "n2", State: proto.SubtaskStateRunning},
				},
			},
		}
		b := runBalanceWithTasks(t, 3, []string{"n1", "n2", "n3"},
			map[string]int{"n1": 3, "n2": 3}, tasks)
		require.Equal(t, "n1", tasks[0].subtasks[0].ExecID)
		require.Equal(t, "n3", tasks[0].subtasks[1].ExecID)
		require.Equal(t, "n2", tasks[1].subtasks[0].ExecID)
		require.Equal(t, map[string]int{"n1": 3, "n2": 3, "n3": 3}, b.actualUsedSlots)
	})

	t.Run("max node count 2 keeps existing node and picks idle node", func(t *testing.T) {
		tasks := []balanceTask{
			{
				requiredSlots: 3,
				maxNodeCount:  2,
				subtasks: []*proto.SubtaskBase{
					{ID: 1, ExecID: "n1", State: proto.SubtaskStateRunning},
					{ID: 2, ExecID: "n1", State: proto.SubtaskStatePending},
				},
			},
			{
				requiredSlots: 3,
				maxNodeCount:  1,
				subtasks: []*proto.SubtaskBase{
					{ID: 3, ExecID: "n2", State: proto.SubtaskStateRunning},
				},
			},
		}
		b := runBalanceWithTasks(t, 3, []string{"n1", "n2", "n3", "n4"},
			map[string]int{"n1": 3, "n2": 3}, tasks)
		require.Equal(t, "n1", tasks[0].subtasks[0].ExecID)
		require.Equal(t, "n3", tasks[0].subtasks[1].ExecID)
		require.Equal(t, "n2", tasks[1].subtasks[0].ExecID)
		require.Equal(t, map[string]int{"n1": 3, "n2": 3, "n3": 3}, b.actualUsedSlots)
	})
}
