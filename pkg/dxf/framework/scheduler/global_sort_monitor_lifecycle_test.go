// Copyright 2026 PingCAP, Inc.
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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestGlobalSortMonitorRequestNextGen(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("global sort residual monitoring is only enabled in NextGen")
	}
	t.Cleanup(func() {
		metrics.GlobalSortResidualDataSize.Set(0)
	})

	t.Run("single flight stops with manager", func(t *testing.T) {
		mgr, taskMgr, _ := newGlobalSortMonitorTestManager(t)
		walkStarted := make(chan struct{})
		releaseWalk := make(chan struct{})
		t.Cleanup(func() {
			select {
			case <-releaseWalk:
			default:
				close(releaseWalk)
			}
		})
		var factoryCalls atomic.Int32
		var walkCalls atomic.Int32
		store := &globalSortMonitorStorage{
			Storage: objstore.NewMemStorage(),
			walkFn: func(ctx context.Context) error {
				walkCalls.Add(1)
				close(walkStarted)
				<-releaseWalk
				return ctx.Err()
			},
		}
		mgr.globalSortURIResolver = func(context.Context, kv.Storage) string {
			return "memstore:///residual"
		}
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			factoryCalls.Add(1)
			return store, nil
		}
		taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil)

		requestReturned := make(chan struct{})
		go func() {
			mgr.requestGlobalSortMonitor()
			close(requestReturned)
		}()
		select {
		case <-walkStarted:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "global sort residual scan did not start")
		}
		select {
		case <-requestReturned:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "global sort residual monitor request did not return asynchronously")
		}
		mgr.requestGlobalSortMonitor()

		require.Equal(t, int32(1), factoryCalls.Load())
		require.Equal(t, int32(1), walkCalls.Load())
		metrics.GlobalSortResidualDataSize.Set(37)
		stopDone := make(chan struct{})
		go func() {
			mgr.Stop()
			close(stopDone)
		}()
		select {
		case <-mgr.ctx.Done():
		case <-time.After(5 * time.Second):
			require.FailNow(t, "manager was not canceled")
		}
		select {
		case <-stopDone:
			require.FailNow(t, "manager stopped before the residual scan exited")
		case <-time.After(100 * time.Millisecond):
		}
		close(releaseWalk)
		select {
		case <-stopDone:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "manager did not stop after the residual scan exited")
		}

		require.Equal(t, int32(1), factoryCalls.Load())
		require.Equal(t, int32(1), walkCalls.Load())
		requireGlobalSortGauge(t, 0)
	})

	t.Run("stop clears gauge without a request", func(t *testing.T) {
		mgr, _, _ := newGlobalSortMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(73)

		mgr.Stop()

		requireGlobalSortGauge(t, 0)
	})
}

func TestGlobalSortMonitorRequestStopRaceNextGen(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("global sort residual monitoring is only enabled in NextGen")
	}
	t.Cleanup(func() {
		metrics.GlobalSortResidualDataSize.Set(0)
	})

	mgr, taskMgr, _ := newGlobalSortMonitorTestManager(t)
	mgr.globalSortURIResolver = func(context.Context, kv.Storage) string {
		return ""
	}
	taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil).AnyTimes()
	admissionEntered := make(chan struct{}, 1)
	releaseAdmission := make(chan struct{})
	workerStarted := make(chan struct{}, 1)
	releaseWorker := make(chan struct{})
	var releaseAdmissionOnce sync.Once
	var releaseWorkerOnce sync.Once
	t.Cleanup(func() {
		releaseAdmissionOnce.Do(func() { close(releaseAdmission) })
		releaseWorkerOnce.Do(func() { close(releaseWorker) })
	})

	var admissionCalls atomic.Int32
	testfailpoint.EnableCall(t,
		"github.com/pingcap/tidb/pkg/dxf/framework/scheduler/beforeGlobalSortResidualMonitorRun",
		func() {
			admissionCalls.Add(1)
			select {
			case admissionEntered <- struct{}{}:
			default:
			}
			<-releaseAdmission
		},
	)
	var workerCalls atomic.Int32
	testfailpoint.EnableCall(t,
		"github.com/pingcap/tidb/pkg/dxf/framework/scheduler/globalSortResidualMonitorWorker",
		func() {
			workerCalls.Add(1)
			select {
			case workerStarted <- struct{}{}:
			default:
			}
			<-releaseWorker
		},
	)

	requestDone := make(chan struct{})
	go func() {
		mgr.requestGlobalSortMonitor()
		close(requestDone)
	}()
	select {
	case <-admissionEntered:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "request did not reach monitor worker admission")
	}

	metrics.GlobalSortResidualDataSize.Set(37)
	stopDone := make(chan struct{})
	go func() {
		mgr.Stop()
		close(stopDone)
	}()
	select {
	case <-mgr.ctx.Done():
	case <-time.After(5 * time.Second):
		require.FailNow(t, "manager was not canceled")
	}
	select {
	case <-stopDone:
		require.FailNow(t, "manager stopped before the admitted worker was registered")
	case <-time.After(100 * time.Millisecond):
	}

	requestDuringStopDone := make(chan struct{})
	go func() {
		mgr.requestGlobalSortMonitor()
		close(requestDuringStopDone)
	}()
	releaseAdmissionOnce.Do(func() { close(releaseAdmission) })
	select {
	case <-requestDone:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "admitted request did not return")
	}
	select {
	case <-workerStarted:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "admitted monitor worker did not start")
	}
	select {
	case <-requestDuringStopDone:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "request during Stop did not return")
	}
	select {
	case <-stopDone:
		require.FailNow(t, "manager stopped before the admitted monitor worker exited")
	case <-time.After(100 * time.Millisecond):
	}
	require.Equal(t, int32(1), admissionCalls.Load())
	require.Equal(t, int32(1), workerCalls.Load())

	releaseWorkerOnce.Do(func() { close(releaseWorker) })
	select {
	case <-stopDone:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "manager did not stop after the admitted monitor worker exited")
	}
	requireGlobalSortGauge(t, 0)

	mgr.requestGlobalSortMonitor()
	require.Equal(t, int32(1), admissionCalls.Load())
	require.Equal(t, int32(1), workerCalls.Load())
}

func TestCleanupLoopRequestsGlobalSortMonitor(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("global sort residual monitoring is only enabled in NextGen")
	}

	originalCleanUpInterval := DefaultCleanUpInterval
	DefaultCleanUpInterval = 500 * time.Millisecond
	t.Cleanup(func() {
		DefaultCleanUpInterval = originalCleanUpInterval
		metrics.GlobalSortResidualDataSize.Set(0)
	})

	mgr, taskMgr, _ := newGlobalSortMonitorTestManager(t)
	cleanupStarted := []chan struct{}{
		make(chan struct{}, 1),
		make(chan struct{}, 1),
		make(chan struct{}, 1),
	}
	releaseCleanup := []chan struct{}{
		make(chan struct{}),
		make(chan struct{}),
		make(chan struct{}),
	}
	var releaseCleanupOnce [3]sync.Once
	releaseCleanupCall := func(index int) {
		releaseCleanupOnce[index].Do(func() { close(releaseCleanup[index]) })
	}
	var cleanupCalls atomic.Int32
	unexpectedCleanupCall := make(chan int32, 1)
	taskMgr.EXPECT().GetCleanupTasks(gomock.Any()).DoAndReturn(
		func(context.Context) ([]*proto.Task, error) {
			callNumber := cleanupCalls.Add(1)
			call := int(callNumber) - 1
			if call >= len(cleanupStarted) {
				select {
				case unexpectedCleanupCall <- callNumber:
				default:
				}
				return nil, nil
			}
			cleanupStarted[call] <- struct{}{}
			<-releaseCleanup[call]
			return nil, nil
		},
	).AnyTimes()

	monitorStarted := make(chan struct{}, 1)
	var getAllTasksCalls atomic.Int32
	taskMgr.EXPECT().GetAllTasks(gomock.Any()).DoAndReturn(
		func(context.Context) ([]*proto.TaskBase, error) {
			call := getAllTasksCalls.Add(1)
			if call == 1 {
				monitorStarted <- struct{}{}
			}
			return nil, nil
		},
	).Times(2)

	scanStarted := make(chan struct{}, 1)
	store := &globalSortMonitorStorage{
		Storage: objstore.NewMemStorage(),
		walkFn: func(context.Context) error {
			scanStarted <- struct{}{}
			return nil
		},
	}
	var resolverCalls atomic.Int32
	mgr.globalSortURIResolver = func(context.Context, kv.Storage) string {
		resolverCalls.Add(1)
		return "memstore:///residual"
	}
	var factoryCalls atomic.Int32
	mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
		factoryCalls.Add(1)
		return store, nil
	}

	requestStarted := make(chan int32, 3)
	releaseStartupRequest := make(chan struct{})
	var releaseStartupRequestOnce sync.Once
	releaseStartupMonitorRequest := func() {
		releaseStartupRequestOnce.Do(func() { close(releaseStartupRequest) })
	}
	var requestCalls atomic.Int32
	testfailpoint.EnableCall(t,
		"github.com/pingcap/tidb/pkg/dxf/framework/scheduler/beforeGlobalSortResidualMonitorRun",
		func() {
			call := requestCalls.Add(1)
			requestStarted <- call
			switch call {
			case 1:
				<-releaseStartupRequest
			case 2:
				mgr.Cancel()
			}
		},
	)

	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		mgr.cleanTaskLoop()
	}()
	t.Cleanup(func() {
		for i := range releaseCleanup {
			releaseCleanupCall(i)
		}
		releaseStartupMonitorRequest()
		mgr.Cancel()
		select {
		case <-loopDone:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "cleanup task loop did not stop")
		}
		mgr.wg.Wait()
	})

	select {
	case <-cleanupStarted[0]:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "startup cleanup did not start")
	}
	mgr.finishCh <- struct{}{}
	require.Zero(t, requestCalls.Load())
	releaseCleanupCall(0)
	select {
	case call := <-requestStarted:
		require.Equal(t, int32(1), call)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "startup cleanup did not request the residual monitor")
	}
	releaseStartupMonitorRequest()

	select {
	case <-monitorStarted:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "startup residual monitor did not start")
	}
	select {
	case <-cleanupStarted[1]:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "finish signal did not drain cleanup tasks")
	}
	mgr.wg.Wait()
	select {
	case <-scanStarted:
	default:
		require.FailNow(t, "startup residual monitor did not scan storage")
	}
	require.Equal(t, int32(2), getAllTasksCalls.Load())
	require.Equal(t, int32(1), resolverCalls.Load())
	require.Equal(t, int32(1), factoryCalls.Load())
	require.Equal(t, 1, store.closeCount)

	releaseCleanupCall(1)
	select {
	case call := <-requestStarted:
		require.FailNow(t, "finish signal requested the residual monitor", "request %d", call)
	case <-cleanupStarted[2]:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "periodic cleanup did not start")
	}
	require.Equal(t, int32(1), requestCalls.Load())
	require.Equal(t, int32(2), getAllTasksCalls.Load())

	releaseCleanupCall(2)
	select {
	case call := <-requestStarted:
		require.Equal(t, int32(2), call)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "periodic cleanup did not request the residual monitor")
	}
	select {
	case <-loopDone:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "cleanup task loop did not stop")
	}
	mgr.wg.Wait()
	select {
	case call := <-unexpectedCleanupCall:
		require.FailNow(t, "cleanup task loop ran after the periodic monitor request", "call %d", call)
	default:
	}
	require.Equal(t, int32(3), cleanupCalls.Load())
	require.Equal(t, int32(2), requestCalls.Load())
	require.Equal(t, int32(2), getAllTasksCalls.Load())
	require.Equal(t, int32(1), resolverCalls.Load())
	require.Equal(t, int32(1), factoryCalls.Load())
	require.Equal(t, 1, store.closeCount)
}

func TestGlobalSortMonitorRequestClassic(t *testing.T) {
	if !kerneltype.IsClassic() {
		t.Skip("classic-only assertion")
	}

	mgr, _, _ := newGlobalSortMonitorTestManager(t)
	var factoryCalls atomic.Int32
	mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
		factoryCalls.Add(1)
		return nil, errors.New("unexpected factory call")
	}

	mgr.requestGlobalSortMonitor()
	mgr.Stop()

	require.Zero(t, factoryCalls.Load())
}
