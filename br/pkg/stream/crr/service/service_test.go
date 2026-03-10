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

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/stream/crr/internal/checkpoint"
	"github.com/pingcap/tidb/br/pkg/stream/crr/internal/testutil"
	streamhelperconfig "github.com/pingcap/tidb/br/pkg/streamhelper/config"
	"github.com/stretchr/testify/require"
)

func TestServiceTracksSuccessfulCheckpoint(t *testing.T) {
	ctx := context.Background()
	h := newServiceHarness(t, ctx)
	initialCheckpoint := h.requireInitialCheckpointByTick()

	svc, err := New(
		Deps{
			PD:         h.PDSim,
			Upstream:   h.Upstream,
			Downstream: h.Downstream,
		},
		CalculatorConfig{
			TaskName:     "drr_test_task",
			PollInterval: 5 * time.Millisecond,
		},
		Config{RetryInterval: 5 * time.Millisecond},
	)
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(runCtx)
	}()

	upstreamCheckpoint := h.flushRoundsAndGetCheckpoint([]uint64{1}, 1)
	h.requireCheckpointAdvancedByTick(initialCheckpoint, upstreamCheckpoint)
	h.requireReplicateAllPending()

	require.Eventually(t, func() bool {
		snapshot := svc.Status()
		return snapshot.SafeCheckpoint == upstreamCheckpoint &&
			snapshot.SyncedTS > 0 &&
			snapshot.ConsecutiveFailures == 0 &&
			!snapshot.LastSuccessTime.IsZero()
	}, 5*time.Second, 20*time.Millisecond)

	cancel()
	require.NoError(t, <-done)

	snapshot := svc.Status()
	require.Equal(t, stateStopped, snapshot.State)
	require.False(t, snapshot.Live)
	require.False(t, snapshot.Ready)
}

func TestServiceTracksFailures(t *testing.T) {
	ctx := context.Background()
	h := newServiceHarness(t, ctx)
	initialCheckpoint := h.requireInitialCheckpointByTick()

	svc, err := New(
		Deps{
			PD:         h.PDSim,
			Upstream:   h.Upstream,
			Downstream: failingDownstreamChecker{},
		},
		CalculatorConfig{
			TaskName:     "drr_test_task",
			PollInterval: 5 * time.Millisecond,
		},
		Config{RetryInterval: 5 * time.Millisecond},
	)
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(runCtx)
	}()

	upstreamCheckpoint := h.flushRoundsAndGetCheckpoint([]uint64{1}, 1)
	h.requireCheckpointAdvancedByTick(initialCheckpoint, upstreamCheckpoint)

	require.Eventually(t, func() bool {
		snapshot := svc.Status()
		return snapshot.State == stateDegraded &&
			snapshot.ConsecutiveFailures > 0 &&
			snapshot.LastError != ""
	}, 5*time.Second, 20*time.Millisecond)

	cancel()
	require.NoError(t, <-done)
}

func TestServiceStatusEndpoints(t *testing.T) {
	status := newStatusStore("task")
	svc := &Service{status: status}
	mux := http.NewServeMux()
	svc.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/livez", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)

	status.start()
	status.applyEvent(checkpoint.CheckpointEvent{
		Type:               checkpoint.EventCheckpointAdvanced,
		Time:               time.Now(),
		UpstreamCheckpoint: 42,
		SafeCheckpoint:     42,
		SyncedTS:           42,
	})

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	var snapshot StatusSnapshot
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &snapshot))
	require.Equal(t, uint64(42), snapshot.SafeCheckpoint)
	require.Equal(t, uint64(42), snapshot.SyncedTS)
}

func TestServiceRegisterPanicsOnNilMux(t *testing.T) {
	svc := &Service{}
	require.PanicsWithValue(t, "service: nil mux", func() {
		svc.Register(nil)
	})
}

type serviceHarness struct {
	t   *testing.T
	ctx context.Context
	*testutil.TestHarness
}

func newServiceHarness(t *testing.T, ctx context.Context) *serviceHarness {
	t.Helper()

	boundaries, err := testutil.BuildRegionLayout(
		testutil.AddRoundRobinRegions(1, 1),
	)
	require.NoError(t, err)

	h, err := testutil.NewLocalTestHarnessWithTestContext(ctx, testutil.NewTestContext(t), boundaries)
	require.NoError(t, err)
	h.Advancer.UpdateConfig(&streamhelperconfig.CommandConfig{
		BackoffTime:            streamhelperconfig.DefaultBackOffTime,
		TickDuration:           streamhelperconfig.DefaultTickInterval,
		TryAdvanceThreshold:    10 * 365 * 24 * time.Hour,
		CheckPointLagLimit:     streamhelperconfig.DefaultCheckPointLagLimit,
		OwnershipCycleInterval: streamhelperconfig.DefaultOwnershipCycleInterval,
	})
	return &serviceHarness{
		t:           t,
		ctx:         ctx,
		TestHarness: h,
	}
}

func (h *serviceHarness) requireInitialCheckpointByTick() uint64 {
	_, err := h.Tick(h.ctx)
	require.ErrorContains(h.t, err, "GetLastFlushTSOfRegion is legacy and disabled in DRR harness")
	require.True(h.t, h.Advancer.HasSubscriptions())

	checkpoint := h.PDSim.GlobalCheckpoint()
	require.Greater(h.t, checkpoint, uint64(0))
	return checkpoint
}

func (h *serviceHarness) flushRoundsAndGetCheckpoint(stores []uint64, rounds int) uint64 {
	var roundCheckpoint uint64
	for range rounds {
		roundCheckpoint = ^uint64(0)
		for _, storeID := range stores {
			record, err := h.FlushSim.FlushStore(h.ctx, storeID)
			require.NoError(h.t, err)
			if record.FlushTS < roundCheckpoint {
				roundCheckpoint = record.FlushTS
			}
		}
	}
	require.NotEqual(h.t, ^uint64(0), roundCheckpoint)
	return roundCheckpoint
}

func (h *serviceHarness) requireCheckpointAdvancedByTick(before uint64, expected uint64) {
	require.Greater(h.t, expected, before)

	var checkpoint uint64
	var tickErr error
	require.Eventually(h.t, func() bool {
		checkpoint, tickErr = h.Tick(h.ctx)
		return tickErr == nil && checkpoint == expected
	}, time.Second, 10*time.Millisecond)
	require.NoError(h.t, tickErr)
	require.Greater(h.t, checkpoint, before)
	require.Equal(h.t, expected, checkpoint)
	require.Equal(h.t, expected, h.PDSim.GlobalCheckpoint())
}

func (h *serviceHarness) requireReplicateAllPending() {
	pulled := h.PullMessages(0)
	require.Greater(h.t, pulled, 0)
	replicated, err := h.Replicate(h.ctx, 0)
	require.NoError(h.t, err)
	require.Greater(h.t, replicated, 0)
}

type failingDownstreamChecker struct{}

func (failingDownstreamChecker) FileExists(ctx context.Context, name string) (bool, error) {
	return false, fmt.Errorf("boom for %s", name)
}
