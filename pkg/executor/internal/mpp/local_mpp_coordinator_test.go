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
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/tidb/pkg/kv"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
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

	for _, cancelFirst := range []bool{true, false} {
		name := "dispatchAll before cancel"
		if cancelFirst {
			name = "cancel before dispatchAll"
		}
		t.Run(name, func(t *testing.T) {
			client := &dispatchCancelMPPClient{}
			task := &kv.MPPDispatchRequest{
				Meta:  &mpp.TaskMeta{Address: "tiflash:3930"},
				State: kv.MppTaskReady,
			}
			coordinator := &localMppCoordinator{
				sessionCtx: &dispatchCancelSession{client: client},
				mppReqs:    []*kv.MPPDispatchRequest{task},
				wgDoneChan: make(chan struct{}),
				respChan:   make(chan *mppResponse),
			}
			if cancelFirst {
				coordinator.cancelMppTasks()
			} else {
				// Cancel while the dispatch RPC is in flight, after dispatchAll
				// has marked the task running and before the RPC returns.
				client.onDispatch = coordinator.cancelMppTasks
			}
			coordinator.dispatchAll(context.Background())

			require.Len(t, client.cancelRequests, 1)
			require.Equal(t, []*kv.MPPDispatchRequest{task}, client.cancelRequests[0].Reqs)
			require.Equal(t, kv.MppTaskCancelled, task.State)
			if cancelFirst {
				require.Empty(t, client.dispatchRequests)
				require.Empty(t, client.cancelRequests[0].StoreAddr)
			} else {
				require.Len(t, client.dispatchRequests, 1)
				require.Same(t, task, client.dispatchRequests[0])
				require.Equal(t, map[string]bool{task.Meta.GetAddress(): true}, client.cancelRequests[0].StoreAddr)
			}
		})
	}
}

type dispatchCancelSession struct {
	sessionctx.Context
	client kv.MPPClient
}

func (s *dispatchCancelSession) GetMPPClient() kv.MPPClient {
	return s.client
}

type dispatchCancelMPPClient struct {
	kv.MPPClient
	onDispatch func()
	// The tests dispatch a single task and read the records after dispatchAll
	// waits for its dispatch goroutine to finish.
	dispatchRequests []*kv.MPPDispatchRequest
	cancelRequests   []kv.CancelMPPTasksParam
}

func (c *dispatchCancelMPPClient) DispatchMPPTask(param kv.DispatchMPPTaskParam) (*mpp.DispatchTaskResponse, bool, error) {
	c.dispatchRequests = append(c.dispatchRequests, param.Req)
	if c.onDispatch != nil {
		c.onDispatch()
	}
	return &mpp.DispatchTaskResponse{}, false, nil
}

func (c *dispatchCancelMPPClient) CancelMPPTasks(param kv.CancelMPPTasksParam) {
	c.cancelRequests = append(c.cancelRequests, param)
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
