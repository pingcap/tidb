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

package residual

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestMonitorRequest(t *testing.T) {
	t.Cleanup(func() {
		metrics.GlobalSortResidualDataSize.Set(0)
	})

	t.Run("single flight stops with monitor", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
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
		store := &monitorStorage{
			Storage: objstore.NewMemStorage(),
			walkFn: func(ctx context.Context) error {
				walkCalls.Add(1)
				close(walkStarted)
				<-releaseWalk
				return ctx.Err()
			},
		}
		m := newTestMonitor(ctx, t, Config{
			TaskCount: func(context.Context) (int, error) { return 0, nil },
			StorageURI: func(context.Context) string {
				return "memstore:///residual"
			},
		})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
			factoryCalls.Add(1)
			return store, nil
		}

		requestReturned := make(chan struct{})
		go func() {
			m.Request()
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
		m.Request()

		require.Equal(t, int32(1), factoryCalls.Load())
		require.Equal(t, int32(1), walkCalls.Load())
		metrics.GlobalSortResidualDataSize.Set(37)
		stopDone := make(chan struct{})
		go func() {
			cancel()
			m.Stop()
			close(stopDone)
		}()
		select {
		case <-stopDone:
			require.FailNow(t, "monitor stopped before the residual scan exited")
		case <-time.After(100 * time.Millisecond):
		}
		close(releaseWalk)
		select {
		case <-stopDone:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "monitor did not stop after the residual scan exited")
		}

		require.Equal(t, int32(1), factoryCalls.Load())
		require.Equal(t, int32(1), walkCalls.Load())
		requireGauge(t, 0)
	})

	t.Run("stop clears gauge without a request", func(t *testing.T) {
		m := newTestMonitor(context.Background(), t, Config{})
		metrics.GlobalSortResidualDataSize.Set(73)

		m.Stop()

		requireGauge(t, 0)
	})
}

func TestMonitorRequestStopRace(t *testing.T) {
	t.Cleanup(func() {
		metrics.GlobalSortResidualDataSize.Set(0)
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m := newTestMonitor(ctx, t, Config{
		TaskCount:  func(context.Context) (int, error) { return 0, nil },
		StorageURI: func(context.Context) string { return "" },
	})
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
		"github.com/pingcap/tidb/pkg/ingestor/globalsort/residual/beforeGlobalSortResidualMonitorRun",
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
		"github.com/pingcap/tidb/pkg/ingestor/globalsort/residual/globalSortResidualMonitorWorker",
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
		m.Request()
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
		cancel()
		m.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
		require.FailNow(t, "monitor stopped before the admitted worker was registered")
	case <-time.After(100 * time.Millisecond):
	}

	requestDuringStopDone := make(chan struct{})
	go func() {
		m.Request()
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
		require.FailNow(t, "monitor stopped before the admitted monitor worker exited")
	case <-time.After(100 * time.Millisecond):
	}
	require.Equal(t, int32(1), admissionCalls.Load())
	require.Equal(t, int32(1), workerCalls.Load())

	releaseWorkerOnce.Do(func() { close(releaseWorker) })
	select {
	case <-stopDone:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "monitor did not stop after the admitted monitor worker exited")
	}
	requireGauge(t, 0)

	m.Request()
	require.Equal(t, int32(1), admissionCalls.Load())
	require.Equal(t, int32(1), workerCalls.Load())
}

func TestMonitorRequestDisabled(t *testing.T) {
	var factoryCalls atomic.Int32
	core, _ := observer.New(zap.DebugLevel)
	m := NewMonitor(context.Background(), Config{
		Enabled: false,
		Logger:  zap.New(core),
		TaskCount: func(context.Context) (int, error) {
			t.Fatal("unexpected task count")
			return 0, nil
		},
	})
	m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
		factoryCalls.Add(1)
		return nil, errors.New("unexpected factory call")
	}

	m.Request()
	m.Stop()

	require.Zero(t, factoryCalls.Load())
}
