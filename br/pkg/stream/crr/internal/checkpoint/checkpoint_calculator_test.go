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
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/stream/crr/internal/checkpoint"
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
	calculator, err := checkpoint.NewCalculator(
		checkpoint.CalculatorDeps{
			PD:         h.PDSim,
			Upstream:   upstream,
			Downstream: h.Downstream,
		},
		checkpoint.CheckpointCalculatorConfig{
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

	script := testutil.NewSyncScript(t, "github.com/pingcap/tidb/br/pkg/stream/crr/internal/checkpoint")
	var started atomic.Int32
	firstTwoStarted := make(chan struct{})
	releaseFirstTwo := make(chan struct{})
	var closeStarted sync.Once
	script.On("before-read-meta", func(ctx testutil.InjectContext, _ string) {
		current := started.Add(1)
		if current == 2 {
			closeStarted.Do(func() { close(firstTwoStarted) })
		}
		if current <= 2 {
			<-releaseFirstTwo
		}
	})

	calculator, err := checkpoint.NewCalculator(
		checkpoint.CalculatorDeps{
			PD:         h.PDSim,
			Upstream:   h.Upstream,
			Downstream: h.Downstream,
		},
		checkpoint.CheckpointCalculatorConfig{
			TaskName:            "drr_test_task",
			PollInterval:        5 * time.Millisecond,
			MetaReadConcurrency: 2,
		},
	)
	require.NoError(t, err)

	type calcResult struct {
		checkpoint uint64
		err        error
	}
	resultCh := make(chan calcResult, 1)
	go func() {
		checkpoint, err := calculator.ComputeNextCheckpoint(ctx)
		resultCh <- calcResult{checkpoint: checkpoint, err: err}
	}()

	<-firstTwoStarted
	require.Equal(t, int32(2), started.Load())

	select {
	case result := <-resultCh:
		require.FailNowf(t, "checkpoint should still wait for blocked meta readers", "unexpected result: %+v", result)
	default:
	}

	close(releaseFirstTwo)

	result := <-resultCh
	require.NoError(t, result.err)
	require.Greater(t, result.checkpoint, uint64(0))
	require.GreaterOrEqual(t, started.Load(), int32(2))
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
