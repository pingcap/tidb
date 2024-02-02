// Copyright 2024 PingCAP, Inc.
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

package integrationtests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	mockDispatch "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type resourceCtrlCaseContext struct {
	*testutil.TestDXFContext

	taskWG util.WaitGroupWrapper

	mu sync.RWMutex
	// task-id -> subtask-id -> channel
	channelMap        map[int64]map[int64]chan error
	totalSubtaskCount atomic.Int32
}

func newResourceCtrlCaseContext(t *testing.T, nodeCnt int, subtaskCntMap map[int64]map[proto.Step]int) *resourceCtrlCaseContext {
	c := &resourceCtrlCaseContext{
		TestDXFContext: testutil.NewTestDXFContext(t, nodeCnt),
		channelMap:     make(map[int64]map[int64]chan error),
	}
	c.init(subtaskCntMap)
	return c
}

func (c *resourceCtrlCaseContext) init(subtaskCntMap map[int64]map[proto.Step]int) {
	stepTransition := map[proto.Step]proto.Step{
		proto.StepInit: proto.StepOne,
		proto.StepOne:  proto.StepTwo,
		proto.StepTwo:  proto.StepDone,
	}
	schedulerExt := mockDispatch.NewMockExtension(c.MockCtrl)
	schedulerExt.EXPECT().OnTick(gomock.Any(), gomock.Any()).Return().AnyTimes()
	schedulerExt.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	schedulerExt.EXPECT().IsRetryableErr(gomock.Any()).Return(false).AnyTimes()
	schedulerExt.EXPECT().GetNextStep(gomock.Any()).DoAndReturn(
		func(task *proto.Task) proto.Step {
			return stepTransition[task.Step]
		},
	).AnyTimes()
	schedulerExt.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ storage.TaskHandle, task *proto.Task, _ []string, nextStep proto.Step) (metas [][]byte, err error) {
			cnt := subtaskCntMap[task.ID][nextStep]
			res := make([][]byte, cnt)
			for i := 0; i < cnt; i++ {
				res[i] = []byte(fmt.Sprintf("subtask-%d", i))
			}
			return res, nil
		},
	).AnyTimes()
	schedulerExt.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	testutil.RegisterTaskMetaWithDXFCtx(c.TestDXFContext, schedulerExt, func(ctx context.Context, subtask *proto.Subtask) error {
		ch := c.enterSubtask(subtask)
		defer c.leaveSubtask(subtask)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-ch:
			return err
		}
	})
}

func (c *resourceCtrlCaseContext) runTaskAsync(prefix string, concurrencyes []int) {
	for i, concurrency := range concurrencyes {
		taskKey := fmt.Sprintf("%s-%d", prefix, i)
		c.taskWG.RunWithLog(func() {
			task := testutil.SubmitAndWaitTask(c.Ctx, c.T, taskKey, concurrency)
			require.Equal(c.T, proto.TaskStateSucceed, task.State)
		})
	}
}

func (c *resourceCtrlCaseContext) enterSubtask(subtask *proto.Subtask) chan error {
	c.mu.Lock()
	defer c.mu.Unlock()
	m, ok := c.channelMap[subtask.TaskID]
	if !ok {
		m = make(map[int64]chan error)
		c.channelMap[subtask.TaskID] = m
	}
	m[subtask.ID] = make(chan error)
	c.totalSubtaskCount.Add(1)
	return m[subtask.ID]
}

func (c *resourceCtrlCaseContext) leaveSubtask(subtask *proto.Subtask) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.channelMap[subtask.TaskID], subtask.ID)
	c.totalSubtaskCount.Add(-1)
}

func (c *resourceCtrlCaseContext) waitTotalSubtaskCount(target int) {
	require.Eventually(c.T, func() bool {
		return int(c.totalSubtaskCount.Load()) == target
	}, 10*time.Second, 200*time.Millisecond)
}

func (c *resourceCtrlCaseContext) waitSubtaskCntByTask(taskID int64, subtaskCnt int) {
	require.Eventually(c.T, func() bool {
		c.mu.RLock()
		defer c.mu.RUnlock()
		return len(c.channelMap[taskID]) == subtaskCnt
	}, 10*time.Second, 200*time.Millisecond)
}

func (c *resourceCtrlCaseContext) continueAllSubtasks() {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, m := range c.channelMap {
		for _, ch := range m {
			ch <- nil
		}
	}
}

func TestResourceControlFullyUtilized(t *testing.T) {
	t.Run("fully-utilized", func(t *testing.T) {
		// we have 4 16c nodes, and we have 4 tasks of concurrency 4, each task have 4 subtasks,
		// so the resource can run all subtasks concurrently.
		c := newResourceCtrlCaseContext(t, 4, map[int64]map[proto.Step]int{
			1: {proto.StepOne: 4},
			2: {proto.StepOne: 4},
			3: {proto.StepOne: 4},
			4: {proto.StepOne: 4},
		})
		c.runTaskAsync("a", []int{4, 4, 4, 4})
		c.waitTotalSubtaskCount(16)
		c.continueAllSubtasks()
		c.taskWG.Wait()
	})

	t.Run("fully-utilized-after-scale-out", func(t *testing.T) {
		// we have 2 16c nodes, and we have 4 tasks of concurrency 4, each task have 4 subtasks,
		// so only 8 subtask can be run at first, and all task can be run at the same time
		// after scale out 2 nodes, all subtasks can be run
		c := newResourceCtrlCaseContext(t, 2, map[int64]map[proto.Step]int{
			1: {proto.StepOne: 4},
			2: {proto.StepOne: 4},
			3: {proto.StepOne: 4},
			4: {proto.StepOne: 4},
		})
		c.runTaskAsync("a", []int{4, 4, 4, 4})
		c.waitTotalSubtaskCount(8)
		for taskID := int64(1); taskID <= 4; taskID++ {
			c.waitSubtaskCntByTask(taskID, 2)
		}
		c.ScaleOut(2)
		c.waitTotalSubtaskCount(16)
		c.continueAllSubtasks()
		c.taskWG.Wait()
	})
}
