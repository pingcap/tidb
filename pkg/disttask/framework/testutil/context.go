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

package testutil

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/mock/gomock"
)

// TestContext defines shared variables for disttask tests.
type TestContext struct {
	sync.RWMutex
	// taskID/step -> subtask map.
	subtasksHasRun map[string]map[int64]struct{}
	// for rollback tests.
	RollbackCnt atomic.Int32
	// for plan err handling tests.
	CallTime int
}

// InitTestContext inits test context for disttask tests.
func InitTestContext(t *testing.T, nodeNum int) (context.Context, *gomock.Controller, *TestContext, *testkit.DistExecutionContext) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "dispatcher")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(8)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu"))
	})

	executionContext := testkit.NewDistExecutionContext(t, nodeNum)
	// wait until some node is registered.
	require.Eventually(t, func() bool {
		taskMgr, err := storage.GetTaskManager()
		require.NoError(t, err)
		nodes, err := taskMgr.GetAllNodes(ctx)
		require.NoError(t, err)
		return len(nodes) > 0
	}, 5*time.Second, 100*time.Millisecond)
	testCtx := &TestContext{
		subtasksHasRun: make(map[string]map[int64]struct{}),
	}
	return ctx, ctrl, testCtx, executionContext
}

// CollectSubtask collects subtask info
func (c *TestContext) CollectSubtask(subtask *proto.Subtask) {
	key := getTaskStepKey(subtask.TaskID, subtask.Step)
	c.Lock()
	defer c.Unlock()
	m, ok := c.subtasksHasRun[key]
	if !ok {
		m = make(map[int64]struct{})
		c.subtasksHasRun[key] = m
	}
	m[subtask.ID] = struct{}{}
}

// CollectedSubtaskCnt returns the collected subtask count.
func (c *TestContext) CollectedSubtaskCnt(taskID int64, step proto.Step) int {
	key := getTaskStepKey(taskID, step)
	c.RLock()
	defer c.RUnlock()
	return len(c.subtasksHasRun[key])
}

// getTaskStepKey returns the key of a task step.
func getTaskStepKey(id int64, step proto.Step) string {
	return fmt.Sprintf("%d/%d", id, step)
}
