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
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/stream/crr/internal/checkpoint"
	"github.com/pingcap/tidb/br/pkg/stream/crr/internal/testutil"
	"github.com/pingcap/tidb/br/pkg/stream/crr/internal/testutil/syncpoint"
	streamhelperconfig "github.com/pingcap/tidb/br/pkg/streamhelper/config"
	"github.com/stretchr/testify/require"
)

const (
	checkpointSyncPath = "github.com/pingcap/tidb/br/pkg/stream/crr/internal/checkpoint"
	testutilSyncPath   = "github.com/pingcap/tidb/br/pkg/stream/crr/internal/testutil"
)

func TestPartialCRRReplicationFailsRestoreValidationEvenIfCheckpointMatches(t *testing.T) {
	ctx := context.Background()
	stores := testutil.StoreIDRange(1, 6)
	boundaries, err := testutil.BuildRegionLayout(
		testutil.AddRoundRobinRegions(10, stores...),
	)
	require.NoError(t, err)

	h := newIntegrationHarness(t, ctx, boundaries)
	initialCheckpoint := h.requireInitialCheckpointByTick()
	upstreamCheckpoint := h.flushRoundsAndGetCheckpoint(stores, 3)
	h.requireCheckpointAdvancedByTick(initialCheckpoint, upstreamCheckpoint)

	pulled := h.PullMessages(0)
	require.Greater(t, pulled, 0)

	replicated, err := h.Replicate(ctx, pulled/3)
	require.NoError(t, err)
	require.Greater(t, replicated, 0)
	require.Less(t, replicated, pulled)

	// A naive checkpoint-only check can still pass.
	downstreamCheckpoint := upstreamCheckpoint
	err = h.AssertDownstreamCanRestoreTo(ctx, downstreamCheckpoint)
	require.Error(t, err)
	require.Contains(t, err.Error(), "is not readable")
}

func TestCheckpointCalculatorWaitsUntilRoundFullySynced(t *testing.T) {
	ctx := context.Background()
	stores := testutil.StoreIDRange(1, 6)
	boundaries, err := testutil.BuildRegionLayout(
		testutil.AddRoundRobinRegions(10, stores...),
	)
	require.NoError(t, err)

	h := newIntegrationHarness(t, ctx, boundaries)
	initialCheckpoint := h.requireInitialCheckpointByTick()
	upstreamCheckpoint := h.flushRoundsAndGetCheckpoint(stores, 3)
	h.requireCheckpointAdvancedByTick(initialCheckpoint, upstreamCheckpoint)

	pulled := h.PullMessages(0)
	require.Greater(t, pulled, 0)

	replicated, err := h.Replicate(ctx, pulled/3)
	require.NoError(t, err)
	require.Greater(t, replicated, 0)
	require.Less(t, replicated, pulled)

	calculator, err := checkpoint.NewCalculator(
		checkpoint.CalculatorDeps{
			PD:         h.PDSim,
			Upstream:   h.Upstream,
			Downstream: h.Downstream,
		},
		checkpoint.CheckpointCalculatorConfig{
			TaskName:     "drr_test_task",
			PollInterval: 5 * time.Millisecond,
		},
		nil,
	)
	require.NoError(t, err)

	type calcResult struct {
		checkpoint uint64
		err        error
	}
	calcResultCh := make(chan calcResult, 1)

	calcCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	go func() {
		checkpoint, err := calculator.ComputeNextCheckpoint(calcCtx)
		calcResultCh <- calcResult{checkpoint: checkpoint, err: err}
	}()

	select {
	case result := <-calcResultCh:
		require.FailNowf(t, "checkpoint should wait for full sync", "unexpected early result: %+v", result)
	case <-time.After(80 * time.Millisecond):
	}

	restReplicated, err := h.Replicate(ctx, 0)
	require.NoError(t, err)
	require.Greater(t, restReplicated, 0)

	result := <-calcResultCh
	require.NoError(t, result.err)
	require.Equal(t, upstreamCheckpoint, result.checkpoint)
	require.NoError(t, h.AssertDownstreamCanRestoreTo(ctx, result.checkpoint))
}

func TestCheckpointCalculatorConcurrentFlushInterleavings(t *testing.T) {
	ctx := context.Background()
	h := newSingleStoreIntegrationHarness(t, ctx)

	initialCheckpoint := h.requireInitialCheckpointByTick()
	stableCheckpoint := h.computeStableCheckpoint(initialCheckpoint)

	rounds := []concurrentRound{
		{
			name: "list-before-future-meta-write",
			steps: []syncpoint.StepDecl{
				checkpointStep("begin-calculate-checkpoint", func() {}),
				checkpointStep("before-list-meta", func() {}),
				flushStep("begin-flush-store", func() {}),
				checkpointStep("before-read-meta", func(_ string) {}),
				flushStep("before-write-flush-meta", func() {}),
				checkpointStep("flush-meta", func(_ string, _ uint64, _ uint64) {}),
				flushStep("after-write-flush-meta", func() {}),
				flushStep("after-flush-regions", func() {}),
			},
		},
		{
			name: "flush-starts-before-list",
			steps: []syncpoint.StepDecl{
				checkpointStep("begin-calculate-checkpoint", func() {}),
				flushStep("begin-flush-store", func() {}),
				checkpointStep("before-list-meta", func() {}),
				flushStep("before-write-flush-meta", func() {}),
				checkpointStep("before-read-meta", func(_ string) {}),
				flushStep("after-write-flush-meta", func() {}),
				checkpointStep("flush-meta", func(_ string, _ uint64, _ uint64) {}),
				flushStep("after-flush-regions", func() {}),
			},
		},
		{
			name: "flush-begins-before-calculation",
			steps: []syncpoint.StepDecl{
				flushStep("begin-flush-store", func() {}),
				checkpointStep("begin-calculate-checkpoint", func() {}),
				checkpointStep("before-list-meta", func() {}),
				flushStep("before-write-flush-meta", func() {}),
				checkpointStep("before-read-meta", func(_ string) {}),
				checkpointStep("flush-meta", func(_ string, _ uint64, _ uint64) {}),
				flushStep("after-write-flush-meta", func() {}),
				flushStep("after-flush-regions", func() {}),
			},
		},
	}

	script := syncpoint.New(t)

	for _, round := range rounds {
		t.Run(round.name, func(t *testing.T) {
			currentCheckpoint := h.flushRoundsAndGetCheckpoint([]uint64{1}, 1)
			h.requireCheckpointAdvancedByTick(stableCheckpoint, currentCheckpoint)
			h.requireReplicateAllPending()

			seqCtx, cancel := context.WithTimeout(h.ctx, 5*time.Second)
			defer cancel()
			script.BeginSeq(seqCtx, round.steps...)
			futureFlushTS := h.runConcurrentFlushRound(currentCheckpoint)
			script.EndSeq()
			stableCheckpoint = h.advanceToStableCheckpoint(currentCheckpoint, futureFlushTS)
			require.Equal(t, futureFlushTS, stableCheckpoint)
		})
	}
}

type calcResult struct {
	checkpoint uint64
	err        error
}

type flushResult struct {
	record testutil.FlushRecord
	err    error
}

type concurrentRound struct {
	name  string
	steps []syncpoint.StepDecl
}

type integrationHarness struct {
	t   *testing.T
	ctx context.Context
	*testutil.TestHarness
	calculator *checkpoint.Calculator
}

func newIntegrationHarness(
	t *testing.T,
	ctx context.Context,
	boundaries []testutil.RegionBoundary,
) *integrationHarness {
	t.Helper()

	h, err := testutil.NewLocalTestHarnessWithTestContext(ctx, testutil.NewTestContext(t), boundaries)
	require.NoError(t, err)
	configureAdvancerForSubscriptionDrivenTick(h)
	return &integrationHarness{
		t:           t,
		ctx:         ctx,
		TestHarness: h,
	}
}

func newSingleStoreIntegrationHarness(t *testing.T, ctx context.Context) *integrationHarness {
	t.Helper()

	boundaries, err := testutil.BuildRegionLayout(
		testutil.AddRoundRobinRegions(1, 1),
	)
	require.NoError(t, err)

	h := newIntegrationHarness(t, ctx, boundaries)
	h.calculator, err = checkpoint.NewCalculator(
		checkpoint.CalculatorDeps{
			PD:         h.PDSim,
			Upstream:   h.Upstream,
			Downstream: h.Downstream,
		},
		checkpoint.CheckpointCalculatorConfig{
			TaskName:     "drr_test_task",
			PollInterval: 5 * time.Millisecond,
		},
	)
	require.NoError(t, err)
	return h
}

func checkpointStep(name string, fn any) syncpoint.StepDecl {
	return syncpoint.Step(checkpointSyncPath+"/"+name, fn)
}

func flushStep(name string, fn any) syncpoint.StepDecl {
	return syncpoint.Step(testutilSyncPath+"/"+name, fn)
}

func (h *integrationHarness) runConcurrentFlushRound(currentCheckpoint uint64) uint64 {
	futureFlushCh := make(chan flushResult, 1)
	go func() {
		record, err := h.FlushSim.FlushStore(h.ctx, 1)
		futureFlushCh <- flushResult{record: record, err: err}
	}()

	resultCh := make(chan calcResult, 1)
	calcCtx, cancel := context.WithTimeout(h.ctx, 5*time.Second)
	defer cancel()
	go func() {
		checkpoint, err := h.calculator.ComputeNextCheckpoint(calcCtx)
		resultCh <- calcResult{checkpoint: checkpoint, err: err}
	}()

	future := h.requireFlushResult(futureFlushCh)
	require.Greater(h.t, future.record.FlushTS, currentCheckpoint)

	h.requireReplicateAllPending()

	result := h.requireCalcResult(resultCh)
	require.NoError(h.t, result.err)
	require.Equal(h.t, currentCheckpoint, result.checkpoint)
	require.NoError(h.t, h.AssertDownstreamCanRestoreTo(h.ctx, result.checkpoint))
	return future.record.FlushTS
}

func (h *integrationHarness) computeStableCheckpoint(lastCheckpoint uint64) uint64 {
	nextCheckpoint := h.flushRoundsAndGetCheckpoint([]uint64{1}, 1)
	h.requireCheckpointAdvancedByTick(lastCheckpoint, nextCheckpoint)
	h.requireReplicateAllPending()

	result, err := h.calculator.ComputeNextCheckpoint(h.ctx)
	require.NoError(h.t, err)
	require.Equal(h.t, nextCheckpoint, result)
	require.NoError(h.t, h.AssertDownstreamCanRestoreTo(h.ctx, result))
	return result
}

func (h *integrationHarness) advanceToStableCheckpoint(lastCheckpoint uint64, nextCheckpoint uint64) uint64 {
	h.requireCheckpointAdvancedByTick(lastCheckpoint, nextCheckpoint)

	result, err := h.calculator.ComputeNextCheckpoint(h.ctx)
	require.NoError(h.t, err)
	require.Equal(h.t, nextCheckpoint, result)
	require.NoError(h.t, h.AssertDownstreamCanRestoreTo(h.ctx, result))
	return result
}

func (h *integrationHarness) requireReplicateAllPending() {
	pulled := h.PullMessages(0)
	require.Greater(h.t, pulled, 0)
	replicated, err := h.Replicate(h.ctx, 0)
	require.NoError(h.t, err)
	require.Greater(h.t, replicated, 0)
}

func (h *integrationHarness) requireFlushResult(ch <-chan flushResult) flushResult {
	select {
	case result := <-ch:
		require.NoError(h.t, result.err)
		return result
	case <-time.After(5 * time.Second):
		require.FailNow(h.t, "timed out waiting for flush result")
		return flushResult{}
	}
}

func (h *integrationHarness) requireCalcResult(ch <-chan calcResult) calcResult {
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		require.FailNow(h.t, "timed out waiting for checkpoint result")
		return calcResult{}
	}
}

func (h *integrationHarness) requireInitialCheckpointByTick() uint64 {
	_, err := h.Tick(h.ctx)
	require.ErrorContains(h.t, err, "GetLastFlushTSOfRegion is legacy and disabled in DRR harness")
	require.True(h.t, h.Advancer.HasSubscriptions())

	checkpoint := h.PDSim.GlobalCheckpoint()
	require.Greater(h.t, checkpoint, uint64(0))
	return checkpoint
}

func configureAdvancerForSubscriptionDrivenTick(h *testutil.TestHarness) {
	h.Advancer.UpdateConfig(&streamhelperconfig.CommandConfig{
		BackoffTime:            streamhelperconfig.DefaultBackOffTime,
		TickDuration:           streamhelperconfig.DefaultTickInterval,
		TryAdvanceThreshold:    10 * 365 * 24 * time.Hour,
		CheckPointLagLimit:     streamhelperconfig.DefaultCheckPointLagLimit,
		OwnershipCycleInterval: streamhelperconfig.DefaultOwnershipCycleInterval,
	})
}

func (h *integrationHarness) flushRoundsAndGetCheckpoint(stores []uint64, rounds int) uint64 {
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

func (h *integrationHarness) requireCheckpointAdvancedByTick(before uint64, expected uint64) {
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
