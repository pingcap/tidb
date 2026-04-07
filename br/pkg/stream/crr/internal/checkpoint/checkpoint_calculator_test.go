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
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/stream/crr/internal/checkpoint"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	testutil "github.com/pingcap/tidb/br/pkg/utiltest/crr"
	"github.com/pingcap/tidb/pkg/objstore"
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
			Sync: checkpoint.NewExistenceSyncChecker(h.Downstream),
		},
		checkpoint.CheckpointCalculatorConfig{TaskName: "drr_test_task"},
		nil,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "StartAfter-capable upstream storage")
}

func TestCheckpointCalculatorRequiresObjectSyncChecker(t *testing.T) {
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
			PD:       h.PDSim,
			Upstream: h.Upstream,
		},
		checkpoint.CheckpointCalculatorConfig{TaskName: "drr_test_task"},
		nil,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "object sync checker must not be nil")
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
			PD:       pd,
			Upstream: h.Upstream,
			Sync:     checkpoint.NewExistenceSyncChecker(h.Downstream),
		},
		checkpoint.CheckpointCalculatorConfig{TaskName: "drr_test_task"},
		nil,
	)
	require.NoError(t, err)

	firstFlush, err := h.FlushSim.FlushStore(ctx, 1)
	require.NoError(t, err)
	require.Less(t, firstFlush.CheckpointTS, firstFlush.FlushTS)
	pd.Set(firstFlush.CheckpointTS, 1)
	require.Greater(t, h.PullMessages(0), 0)
	_, err = h.Replicate(ctx, 0)
	require.NoError(t, err)

	firstCheckpoint, err := calculator.ComputeNextCheckpoint(ctx)
	require.NoError(t, err)
	require.Equal(t, firstFlush.CheckpointTS, firstCheckpoint)
	require.Equal(t, firstFlush.FlushTS, calculator.SyncedTS())

	secondFlush, err := h.FlushSim.FlushStore(ctx, 1)
	require.NoError(t, err)
	require.Greater(t, secondFlush.FlushTS, firstFlush.FlushTS)
	require.Greater(t, secondFlush.CheckpointTS, firstFlush.CheckpointTS)
	pd.Set(secondFlush.CheckpointTS, 1, 2)
	require.Greater(t, h.PullMessages(0), 0)
	_, err = h.Replicate(ctx, 0)
	require.NoError(t, err)

	secondCheckpoint, err := calculator.ComputeNextCheckpoint(ctx)
	require.NoError(t, err)
	require.Equal(t, secondFlush.CheckpointTS, secondCheckpoint)
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
	pd.Set(record.CheckpointTS, 1)
	observer := &recordingObserver{}
	calculator, err := checkpoint.NewCalculator(
		checkpoint.CalculatorDeps{
			PD:       pd,
			Upstream: h.Upstream,
			Sync:     failingObjectSyncChecker{},
		},
		checkpoint.CheckpointCalculatorConfig{TaskName: "drr_test_task"},
		observer,
	)
	require.NoError(t, err)

	_, err = calculator.ComputeNextCheckpoint(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "check sync status")

	events := observer.Events()
	require.Len(t, events, 3)
	require.Equal(t, checkpoint.EventUpstreamAdvanced, events[0].Type)
	require.Equal(t, checkpoint.EventRoundPlanned, events[1].Type)
	require.Equal(t, checkpoint.EventCalculationFailed, events[2].Type)
	require.Error(t, events[2].Err)
	require.Contains(t, events[2].Err.Error(), "boom")
}

func TestCheckpointCalculatorUsesProvidedObjectSyncChecker(t *testing.T) {
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
	pd.Set(record.CheckpointTS, 1)

	syncStates := fileSyncMap{
		record.MetadataPath: true,
	}
	for _, logPath := range record.LogPaths {
		syncStates[logPath] = true
	}

	calculator, err := checkpoint.NewCalculator(
		checkpoint.CalculatorDeps{
			PD:       pd,
			Upstream: h.Upstream,
			Sync:     syncStates,
		},
		checkpoint.CheckpointCalculatorConfig{TaskName: "drr_test_task"},
		nil,
	)
	require.NoError(t, err)

	checkpointTS, err := calculator.ComputeNextCheckpoint(ctx)
	require.NoError(t, err)
	require.Equal(t, record.CheckpointTS, checkpointTS)
	require.Equal(t, record.FlushTS, calculator.SyncedTS())
}

func TestCheckpointCalculatorFailsOnObjectSyncError(t *testing.T) {
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
	pd.Set(record.CheckpointTS, 1)

	syncStates := fileSyncResultMap{
		record.MetadataPath: {synced: true},
	}
	for _, logPath := range record.LogPaths {
		syncStates[logPath] = fileSyncResult{err: fmt.Errorf("boom")}
	}

	calculator, err := checkpoint.NewCalculator(
		checkpoint.CalculatorDeps{
			PD:       pd,
			Upstream: h.Upstream,
			Sync:     syncStates,
		},
		checkpoint.CheckpointCalculatorConfig{TaskName: "drr_test_task"},
		nil,
	)
	require.NoError(t, err)

	_, err = calculator.ComputeNextCheckpoint(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "boom")
}

func TestCheckpointCalculatorWaitsForRemovedStoreFiles(t *testing.T) {
	ctx := context.Background()
	upstream, err := objstore.NewLocalStorage(t.TempDir())
	require.NoError(t, err)

	meta1Path, log1Path := writeCheckpointTestMeta(ctx, t, upstream, 10, 1)
	meta2Path, log2Path := writeCheckpointTestMeta(ctx, t, upstream, 20, 2)

	pd := &fakePDMetaReader{}
	pd.Set(20, 2)

	calculator, err := checkpoint.NewCalculator(
		checkpoint.CalculatorDeps{
			PD:       pd,
			Upstream: upstream,
			Sync: checkpoint.NewExistenceSyncChecker(fileExistenceMap{
				meta1Path: true,
				meta2Path: true,
				log2Path:  true,
			}),
		},
		checkpoint.CheckpointCalculatorConfig{
			TaskName:     "drr_test_task",
			PollInterval: time.Millisecond,
		},
		nil,
	)
	require.NoError(t, err)

	computeCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()

	checkpointTS, err := calculator.ComputeNextCheckpoint(computeCtx)
	require.Zero(t, checkpointTS)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotEmpty(t, log1Path)
}

func TestCheckpointCalculatorObservedRemovedStoreStillBoundsSyncedTS(t *testing.T) {
	ctx := context.Background()
	upstream, err := objstore.NewLocalStorage(t.TempDir())
	require.NoError(t, err)

	meta1Path, log1Path := writeCheckpointTestMeta(ctx, t, upstream, 10, 1)
	meta2Path, log2Path := writeCheckpointTestMeta(ctx, t, upstream, 20, 2)

	pd := &fakePDMetaReader{}
	pd.Set(20, 2)

	calculator, err := checkpoint.NewCalculator(
		checkpoint.CalculatorDeps{
			PD:       pd,
			Upstream: upstream,
			Sync: checkpoint.NewExistenceSyncChecker(fileExistenceMap{
				meta1Path: true,
				log1Path:  true,
				meta2Path: true,
				log2Path:  true,
			}),
		},
		checkpoint.CheckpointCalculatorConfig{TaskName: "drr_test_task"},
		nil,
	)
	require.NoError(t, err)

	checkpointTS, err := calculator.ComputeNextCheckpoint(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(20), checkpointTS)
	require.Equal(t, uint64(10), calculator.SyncedTS())
	require.Equal(
		t,
		map[uint64]uint64{
			1: 10,
			2: 20,
		},
		calculator.StateSnapshot().SyncedByStore,
	)
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

type failingObjectSyncChecker struct{}

func (failingObjectSyncChecker) FileSynced(ctx context.Context, name string) (bool, error) {
	return false, fmt.Errorf("boom for %s", name)
}

type fileExistenceMap map[string]bool

func (m fileExistenceMap) FileExists(ctx context.Context, name string) (bool, error) {
	return m[name], nil
}

type fileSyncMap map[string]bool

func (m fileSyncMap) FileSynced(ctx context.Context, name string) (bool, error) {
	return m[name], nil
}

type fileSyncResult struct {
	synced bool
	err    error
}

type fileSyncResultMap map[string]fileSyncResult

func (m fileSyncResultMap) FileSynced(ctx context.Context, name string) (bool, error) {
	result := m[name]
	return result.synced, result.err
}

func writeCheckpointTestMeta(
	ctx context.Context,
	t *testing.T,
	storage storeapi.Storage,
	flushTS uint64,
	storeID uint64,
) (string, string) {
	t.Helper()

	logPath := fmt.Sprintf("v1/log/store-%d/flush-%016x.log", storeID, flushTS)
	metaPath := fmt.Sprintf(
		"%s/%016X-%016X-%016X-%016X.meta",
		stream.GetStreamBackupMetaPrefix(),
		flushTS,
		flushTS,
		flushTS,
		flushTS,
	)
	meta := &backuppb.Metadata{
		StoreId: int64(storeID),
		FileGroups: []*backuppb.DataFileGroup{
			{
				Path: logPath,
				DataFilesInfo: []*backuppb.DataFileInfo{
					{Path: logPath, MinTs: flushTS, MaxTs: flushTS},
				},
			},
		},
	}
	payload, err := meta.Marshal()
	require.NoError(t, err)
	require.NoError(t, storage.WriteFile(ctx, metaPath, payload))
	return metaPath, logPath
}
