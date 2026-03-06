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

package crr_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/stream/crr"
	"github.com/pingcap/tidb/br/pkg/stream/crr/internal/testutil"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

func TestCheckpointCalculatorUsesStartAfterFromSyncedTS(t *testing.T) {
	ctx := context.Background()
	boundaries, err := testutil.NewRegionLayoutBuilder().
		AddRoundRobinRegions(1, 1).
		Build()
	require.NoError(t, err)

	h, err := testutil.NewLocalTestHarness(ctx, t.TempDir(), boundaries)
	require.NoError(t, err)
	t.Cleanup(h.Close)

	upstream := &recordingUpstreamStorage{inner: h.Upstream}
	calculator, err := crr.NewCheckpointCalculator(
		h.PDSim,
		upstream,
		h.Downstream,
		crr.CheckpointCalculatorConfig{
			TaskName:     "drr_test_task",
			PollInterval: 5 * time.Millisecond,
		},
	)
	require.NoError(t, err)

	firstFlush, err := h.FlushSim.FlushStore(ctx, 1)
	require.NoError(t, err)
	require.NoError(t, h.UploadGlobalCheckpoint(ctx, h.PDSim.CurrentTSO()))
	require.Greater(t, h.PullMessages(0), 0)
	_, err = h.Replicate(ctx, 0)
	require.NoError(t, err)

	firstCheckpoint, err := calculator.ComputeNextCheckpoint(ctx)
	require.NoError(t, err)
	require.Greater(t, firstCheckpoint, uint64(0))
	require.Len(t, upstream.walkOpts, 1)
	require.Empty(t, upstream.walkOpts[0].StartAfter)

	secondFlush, err := h.FlushSim.FlushStore(ctx, 1)
	require.NoError(t, err)
	require.Greater(t, secondFlush.FlushTS, firstFlush.FlushTS)
	require.NoError(t, h.UploadGlobalCheckpoint(ctx, h.PDSim.CurrentTSO()))
	require.Greater(t, h.PullMessages(0), 0)
	_, err = h.Replicate(ctx, 0)
	require.NoError(t, err)

	secondCheckpoint, err := calculator.ComputeNextCheckpoint(ctx)
	require.NoError(t, err)
	require.Greater(t, secondCheckpoint, firstCheckpoint)
	require.Len(t, upstream.walkOpts, 2)
	require.Equal(
		t,
		fmt.Sprintf("%s/%016x%s", stream.GetStreamBackupMetaPrefix(), firstFlush.FlushTS, "ffffffffffffffff~"),
		upstream.walkOpts[1].StartAfter,
	)
}

func TestCheckpointCalculatorReadsMetaFilesInParallelWithinLimit(t *testing.T) {
	ctx := context.Background()
	stores := testutil.StoreIDRange(1, 4)
	boundaries, err := testutil.NewRegionLayoutBuilder().
		AddRoundRobinRegions(8, stores...).
		Build()
	require.NoError(t, err)

	h, err := testutil.NewLocalTestHarness(ctx, t.TempDir(), boundaries)
	require.NoError(t, err)
	t.Cleanup(h.Close)

	for _, storeID := range stores {
		_, err := h.FlushSim.FlushStore(ctx, storeID)
		require.NoError(t, err)
	}
	require.NoError(t, h.UploadGlobalCheckpoint(ctx, h.PDSim.CurrentTSO()))
	require.Greater(t, h.PullMessages(0), 0)
	_, err = h.Replicate(ctx, 0)
	require.NoError(t, err)

	upstream := &blockingUpstreamStorage{
		inner:      h.Upstream,
		metaDelay:  30 * time.Millisecond,
		readRecord: &concurrencyRecorder{},
	}
	calculator, err := crr.NewCheckpointCalculator(
		h.PDSim,
		upstream,
		h.Downstream,
		crr.CheckpointCalculatorConfig{
			TaskName:            "drr_test_task",
			PollInterval:        5 * time.Millisecond,
			MetaReadConcurrency: 2,
		},
	)
	require.NoError(t, err)

	checkpoint, err := calculator.ComputeNextCheckpoint(ctx)
	require.NoError(t, err)
	require.Greater(t, checkpoint, uint64(0))
	require.Equal(t, int32(2), upstream.readRecord.maxSeen.Load())
}

func TestCheckpointCalculatorRejectsUnsupportedMetaScanStorage(t *testing.T) {
	ctx := context.Background()
	boundaries, err := testutil.NewRegionLayoutBuilder().
		AddRoundRobinRegions(1, 1).
		Build()
	require.NoError(t, err)

	h, err := testutil.NewLocalTestHarness(ctx, t.TempDir(), boundaries)
	require.NoError(t, err)
	t.Cleanup(h.Close)

	_, err = crr.NewCheckpointCalculator(
		h.PDSim,
		&recordingUpstreamStorage{
			inner: h.Upstream,
			uri:   "azure://bucket/prefix/",
		},
		h.Downstream,
		crr.CheckpointCalculatorConfig{TaskName: "drr_test_task"},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "StartAfter-capable upstream storage")
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

type blockingUpstreamStorage struct {
	inner interface {
		WalkDir(ctx context.Context, opt *storeapi.WalkOption, fn func(path string, size int64) error) error
		ReadFile(ctx context.Context, name string) ([]byte, error)
		URI() string
	}
	metaDelay  time.Duration
	readRecord *concurrencyRecorder
}

func (s *blockingUpstreamStorage) WalkDir(
	ctx context.Context,
	opt *storeapi.WalkOption,
	fn func(path string, size int64) error,
) error {
	return s.inner.WalkDir(ctx, opt, fn)
}

func (s *blockingUpstreamStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	if strings.HasSuffix(name, ".meta") {
		active := s.readRecord.active.Add(1)
		s.readRecord.observe(active)
		defer s.readRecord.active.Add(-1)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(s.metaDelay):
		}
	}
	return s.inner.ReadFile(ctx, name)
}

func (s *blockingUpstreamStorage) URI() string {
	return s.inner.URI()
}

type concurrencyRecorder struct {
	active  atomic.Int32
	maxSeen atomic.Int32
}

func (r *concurrencyRecorder) observe(active int32) {
	for {
		maxSeen := r.maxSeen.Load()
		if active <= maxSeen {
			return
		}
		if r.maxSeen.CompareAndSwap(maxSeen, active) {
			return
		}
	}
}
