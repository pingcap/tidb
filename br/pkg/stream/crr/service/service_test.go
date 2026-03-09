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
	"github.com/stretchr/testify/require"
)

func TestServiceTracksSuccessfulCheckpoint(t *testing.T) {
	ctx := context.Background()
	boundaries, err := testutil.NewRegionLayoutBuilder().
		AddRoundRobinRegions(1, 1).
		Build()
	require.NoError(t, err)

	h, err := testutil.NewLocalTestHarnessWithTestContext(ctx, testutil.NewTestContext(t), boundaries)
	require.NoError(t, err)

	svc, err := New(
		checkpoint.CalculatorDeps{
			PD:         h.PDSim,
			Upstream:   h.Upstream,
			Downstream: h.Downstream,
		},
		checkpoint.CheckpointCalculatorConfig{
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

	_, err = h.FlushSim.FlushStore(ctx, 1)
	require.NoError(t, err)
	upstreamCheckpoint := h.PDSim.CurrentTSO()
	require.NoError(t, h.UploadGlobalCheckpoint(ctx, upstreamCheckpoint))
	require.Greater(t, h.PullMessages(0), 0)
	_, err = h.Replicate(ctx, 0)
	require.NoError(t, err)

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
	boundaries, err := testutil.NewRegionLayoutBuilder().
		AddRoundRobinRegions(1, 1).
		Build()
	require.NoError(t, err)

	h, err := testutil.NewLocalTestHarnessWithTestContext(ctx, testutil.NewTestContext(t), boundaries)
	require.NoError(t, err)

	svc, err := New(
		checkpoint.CalculatorDeps{
			PD:         h.PDSim,
			Upstream:   h.Upstream,
			Downstream: failingDownstreamChecker{},
		},
		checkpoint.CheckpointCalculatorConfig{
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

	_, err = h.FlushSim.FlushStore(ctx, 1)
	require.NoError(t, err)
	require.NoError(t, h.UploadGlobalCheckpoint(ctx, h.PDSim.CurrentTSO()))

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

type failingDownstreamChecker struct{}

func (failingDownstreamChecker) FileExists(ctx context.Context, name string) (bool, error) {
	return false, fmt.Errorf("boom for %s", name)
}
