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

package checkpoint_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/pingcap/tidb/br/pkg/stream/crr/internal/checkpoint"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/utiltest/crr"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

func TestCheckpointCalculatorRejectsUnsupportedMetaScanStorage(t *testing.T) {
	ctx := context.Background()
	boundaries, err := testutil.BuildRegionLayout(
		testutil.AddRoundRobinRegions(1, 1),
	)
	require.NoError(t, err)

	tc := testutil.NewTestContext(t)
	h, err := testutil.NewLocalTestHarnessWithTestContext(ctx, tc, boundaries)
	require.NoError(t, err)

	_, err = checkpoint.NewCalculator(
		checkpoint.CalculatorDeps{
			PD: h.PDSim,
			Upstream: &recordingUpstreamStorage{
				inner: h.Upstream,
				uri:   "azure://bucket/prefix/",
			},
			Downstream: h.Downstream,
		},
		checkpoint.CheckpointCalculatorConfig{TaskName: "drr_test_task"},
		nil,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "StartAfter-capable upstream storage")
}

func TestCheckpointCalculatorDoesNotAdvanceSyncedTSWhenNewAliveStoreHasNoFlush(t *testing.T) {
	ctx := context.Background()
	boundaries, err := testutil.BuildRegionLayout(
		testutil.AddRoundRobinRegions(1, 1),
	)
	require.NoError(t, err)

	tc := testutil.NewTestContext(t)
	h, err := testutil.NewLocalTestHarnessWithTestContext(ctx, tc, boundaries)
	require.NoError(t, err)

	pd := &fakePDMetaReader{}
	calculator, err := checkpoint.NewCalculator(
		checkpoint.CalculatorDeps{
			PD:         pd,
			Upstream:   h.Upstream,
			Downstream: h.Downstream,
		},
		checkpoint.CheckpointCalculatorConfig{TaskName: "drr_test_task"},
		nil,
	)
	require.NoError(t, err)

	firstFlush, err := h.FlushSim.FlushStore(ctx, 1)
	require.NoError(t, err)
	pd.Set(firstFlush.FlushTS, 1)
	require.Greater(t, h.PullMessages(0), 0)
	_, err = h.Replicate(ctx, 0)
	require.NoError(t, err)

	firstCheckpoint, err := calculator.ComputeNextCheckpoint(ctx)
	require.NoError(t, err)
	require.Equal(t, firstFlush.FlushTS, firstCheckpoint)
	require.Equal(t, firstFlush.FlushTS, calculator.SyncedTS())

	secondFlush, err := h.FlushSim.FlushStore(ctx, 1)
	require.NoError(t, err)
	require.Greater(t, secondFlush.FlushTS, firstFlush.FlushTS)
	pd.Set(secondFlush.FlushTS, 1, 2)
	require.Greater(t, h.PullMessages(0), 0)
	_, err = h.Replicate(ctx, 0)
	require.NoError(t, err)

	secondCheckpoint, err := calculator.ComputeNextCheckpoint(ctx)
	require.NoError(t, err)
	require.Equal(t, secondFlush.FlushTS, secondCheckpoint)
	require.Equal(t, firstFlush.FlushTS, calculator.SyncedTS())
}

func TestCheckpointCalculatorObserverSeesFailure(t *testing.T) {
	ctx := context.Background()
	boundaries, err := testutil.BuildRegionLayout(
		testutil.AddRoundRobinRegions(1, 1),
	)
	require.NoError(t, err)

	tc := testutil.NewTestContext(t)
	h, err := testutil.NewLocalTestHarnessWithTestContext(ctx, tc, boundaries)
	require.NoError(t, err)

	record, err := h.FlushSim.FlushStore(ctx, 1)
	require.NoError(t, err)

	pd := &fakePDMetaReader{}
	pd.Set(record.FlushTS, 1)
	observer := &recordingObserver{}
	calculator, err := checkpoint.NewCalculator(
		checkpoint.CalculatorDeps{
			PD:         pd,
			Upstream:   h.Upstream,
			Downstream: failingDownstreamChecker{},
		},
		checkpoint.CheckpointCalculatorConfig{TaskName: "drr_test_task"},
		observer,
	)
	require.NoError(t, err)

	_, err = calculator.ComputeNextCheckpoint(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "check downstream file")

	events := observer.Events()
	require.Len(t, events, 3)
	require.Equal(t, checkpoint.EventUpstreamAdvanced, events[0].Type)
	require.Equal(t, checkpoint.EventRoundPlanned, events[1].Type)
	require.Equal(t, checkpoint.EventCalculationFailed, events[2].Type)
	require.Error(t, events[2].Err)
	require.Contains(t, events[2].Err.Error(), "boom")
}

type fakePDMetaReader struct {
	mu         sync.Mutex
	checkpoint uint64
	stores     []streamhelper.Store
}

func (f *fakePDMetaReader) GetGlobalCheckpointForTask(ctx context.Context, taskName string) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.checkpoint, nil
}

func (f *fakePDMetaReader) Stores(ctx context.Context) ([]streamhelper.Store, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	stores := make([]streamhelper.Store, len(f.stores))
	copy(stores, f.stores)
	return stores, nil
}

func (f *fakePDMetaReader) Set(checkpoint uint64, storeIDs ...uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checkpoint = checkpoint
	f.stores = f.stores[:0]
	for _, storeID := range storeIDs {
		f.stores = append(f.stores, streamhelper.Store{ID: storeID, BootAt: 1})
	}
}

type recordingUpstreamStorage struct {
	inner interface {
		WalkDir(ctx context.Context, opt *storeapi.WalkOption, fn func(path string, size int64) error) error
		ReadFile(ctx context.Context, name string) ([]byte, error)
		URI() string
	}
	uri      string
	walkOpts []storeapi.WalkOption
}

func (s *recordingUpstreamStorage) WalkDir(
	ctx context.Context,
	opt *storeapi.WalkOption,
	fn func(path string, size int64) error,
) error {
	if opt == nil {
		s.walkOpts = append(s.walkOpts, storeapi.WalkOption{})
	} else {
		s.walkOpts = append(s.walkOpts, *opt)
	}
	return s.inner.WalkDir(ctx, opt, fn)
}

func (s *recordingUpstreamStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	return s.inner.ReadFile(ctx, name)
}

func (s *recordingUpstreamStorage) URI() string {
	if s.uri != "" {
		return s.uri
	}
	return s.inner.URI()
}

type recordingObserver struct {
	mu     sync.Mutex
	events []checkpoint.CheckpointEvent
}

func (o *recordingObserver) OnCheckpointEvent(event checkpoint.CheckpointEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, event)
}

func (o *recordingObserver) Events() []checkpoint.CheckpointEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	events := make([]checkpoint.CheckpointEvent, len(o.events))
	copy(events, o.events)
	return events
}

type failingDownstreamChecker struct{}

func (failingDownstreamChecker) FileExists(ctx context.Context, name string) (bool, error) {
	return false, fmt.Errorf("boom for %s", name)
}
