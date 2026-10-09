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

package mpp

import (
	"testing"

<<<<<<< HEAD
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
=======
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
>>>>>>> 3bd6b6d4d63 (executor: fix race between MPP task dispatch and cancellation (#70690))
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestDispatchCancelRace(t *testing.T) {
	t.Run("cancel wins", func(t *testing.T) {
		coordinator := &localMppCoordinator{}
		task := &kv.MPPDispatchRequest{State: kv.MppTaskReady}

		// Hold the coordinator lock so the dispatch goroutine cannot inspect the
		// task until cancellation has changed its state.
		coordinator.mu.Lock()
		dispatchStarted := make(chan struct{})
		dispatchResult := make(chan bool, 1)
		go func() {
			close(dispatchStarted)
			dispatchResult <- coordinator.tryStartDispatch(task)
		}()
		<-dispatchStarted
		task.State = kv.MppTaskCancelled
		coordinator.mu.Unlock()

		require.False(t, <-dispatchResult)
		require.Equal(t, kv.MppTaskCancelled, task.State)
	})

	t.Run("dispatch wins", func(t *testing.T) {
		coordinator := &localMppCoordinator{}
		task := &kv.MPPDispatchRequest{State: kv.MppTaskReady}

		require.True(t, coordinator.tryStartDispatch(task))

		// cancelMppTasks takes the same lock and includes stores for tasks in the
		// running state before marking all tasks as cancelled.
		coordinator.mu.Lock()
		stateWhenCancelStarted := task.State
		task.State = kv.MppTaskCancelled
		coordinator.mu.Unlock()
		require.Equal(t, kv.MppTaskRunning, stateWhenCancelStarted)
	})
}

func TestNeedReportExecutionSummary(t *testing.T) {
	tableScan := &plannercore.PhysicalTableScan{}
	limit := &plannercore.PhysicalLimit{}
	passSender := &plannercore.PhysicalExchangeSender{
		ExchangeType: tipb.ExchangeType_PassThrough,
	}

	passSender.SetChildren(limit)
	limit.SetChildren(tableScan)
	require.True(t, needReportExecutionSummary(passSender))

	passSender.SetChildren(tableScan)
	require.False(t, needReportExecutionSummary(passSender))
}
