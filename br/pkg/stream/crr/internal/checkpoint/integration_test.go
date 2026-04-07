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
	streamhelperconfig "github.com/pingcap/tidb/br/pkg/streamhelper/config"
	testutil "github.com/pingcap/tidb/br/pkg/utiltest/crr"
	"github.com/pingcap/tidb/br/pkg/utiltest/syncpoint"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

const (
	checkpointSyncPath = "github.com/pingcap/tidb/br/pkg/stream/crr/internal/checkpoint"
	testutilSyncPath   = "github.com/pingcap/tidb/br/pkg/utiltest/crr"
)

func TestPartialCRRReplicationFailsRestoreValidationEvenIfCheckpointMatches(t *testing.T) {
	ctx := context.Background()
	stores := testutil.StoreIDRange(1, 6)
	boundaries, err := testutil.BuildRegionLayout(
		testutil.AddRoundRobinRegions(10, stores...),
	)
	require.NoError(t, err)

	h := newIntegrationHarness(ctx, t, boundaries)
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

	h := newIntegrationHarness(ctx, t, boundaries)
	initialCheckpoint := h.requireInitialCheckpointByTick()
	upstreamCheckpoint := h.flushRoundsAndGetCheckpoint(stores, 3)
	h.requireCheckpointAdvancedByTick(initialCheckpoint, upstreamCheckpoint)

	pulled := h.PullMessages(0)
	require.Greater(t, pulled, 0)

	replicated, err := h.Replicate(ctx, pulled/3)
	require.NoError(t, err)
	require.Greater(t, replicated, 0)
	require.Less(t, replicated, pulled)

	calculator := h.newCalculator()

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

func TestCheckpointCalculatorUsesStartAfterFromSyncedTS(t *testing.T) {
	ctx := context.Background()
	h := newSingleStoreIntegrationHarness(ctx, t)

	initialCheckpoint := h.requireInitialCheckpointByTick()
	upstream := &recordingUpstreamStorage{inner: h.Upstream}
	calculator := h.newCalculator(
		withUpstream(upstream),
	)

	firstRecord, err := h.FlushSim.FlushStore(ctx, 1)
	require.NoError(t, err)
	require.Less(t, firstRecord.CheckpointTS, firstRecord.FlushTS)
	firstCheckpoint := firstRecord.CheckpointTS
	h.requireCheckpointAdvancedByTick(initialCheckpoint, firstCheckpoint)
	h.requireReplicateAllPending()

	computedCheckpoint, err := calculator.ComputeNextCheckpoint(ctx)
	require.NoError(t, err)
	require.Equal(t, firstCheckpoint, computedCheckpoint)
	require.Len(t, upstream.walkOpts, 1)
	require.Empty(t, upstream.walkOpts[0].StartAfter)

	secondRecord, err := h.FlushSim.FlushStore(ctx, 1)
	require.NoError(t, err)
	secondCheckpoint := secondRecord.CheckpointTS
	h.requireCheckpointAdvancedByTick(firstCheckpoint, secondCheckpoint)
	h.requireReplicateAllPending()

	computedCheckpoint, err = calculator.ComputeNextCheckpoint(ctx)
	require.NoError(t, err)
	require.Equal(t, secondCheckpoint, computedCheckpoint)
	require.Len(t, upstream.walkOpts, 2)
	require.Equal(
		t,
		fmt.Sprintf("%s/%016X%s", stream.GetStreamBackupMetaPrefix(), firstRecord.FlushTS, "FFFFFFFFFFFFFFFF~"),
		upstream.walkOpts[1].StartAfter,
	)
}

func TestCheckpointCalculatorRestoresPersistentState(t *testing.T) {
	ctx := context.Background()
	h := newSingleStoreIntegrationHarness(ctx, t)

	initialCheckpoint := h.requireInitialCheckpointByTick()
	firstRecord, err := h.FlushSim.FlushStore(ctx, 1)
	require.NoError(t, err)
	require.Less(t, firstRecord.CheckpointTS, firstRecord.FlushTS)
	secondRecord, err := h.FlushSim.FlushStore(ctx, 1)
	require.NoError(t, err)
	secondCheckpoint := secondRecord.CheckpointTS
	require.Greater(t, secondCheckpoint, firstRecord.CheckpointTS)
	h.requireCheckpointAdvancedByTick(initialCheckpoint, secondCheckpoint)
	h.requireReplicateAllPending()

	upstream := &recordingUpstreamStorage{inner: h.Upstream}
	calculator := h.newCalculator(
		withPersistentState(checkpoint.PersistentState{SyncedTS: firstRecord.FlushTS}),
		withUpstream(upstream),
	)

	computedCheckpoint, err := calculator.ComputeNextCheckpoint(ctx)
	require.NoError(t, err)
	require.Equal(t, secondCheckpoint, computedCheckpoint)
	require.Len(t, upstream.walkOpts, 1)
	require.Equal(
		t,
		fmt.Sprintf("%s/%016X%s", stream.GetStreamBackupMetaPrefix(), firstRecord.FlushTS, "FFFFFFFFFFFFFFFF~"),
		upstream.walkOpts[0].StartAfter,
	)
}

func TestCheckpointCalculatorRestoredCheckpointSkipsUnchangedUpstream(t *testing.T) {
	ctx := context.Background()
	h := newSingleStoreIntegrationHarness(ctx, t)

	initialCheckpoint := h.requireInitialCheckpointByTick()
	upstreamCheckpoint := h.flushRoundsAndGetCheckpoint([]uint64{1}, 1)
	h.requireCheckpointAdvancedByTick(initialCheckpoint, upstreamCheckpoint)
	h.requireReplicateAllPending()

	upstream := &recordingUpstreamStorage{inner: h.Upstream}
	calculator := h.newCalculator(
		withPersistentState(checkpoint.PersistentState{
			LastCheckpoint: upstreamCheckpoint,
			SyncedTS:       h.calculator.SyncedTS(),
			SyncedByStore:  map[uint64]uint64{1: h.calculator.SyncedTS()},
		}),
		withUpstream(upstream),
	)

	result, err := calculator.ComputeNextCheckpoint(ctx)
	require.NoError(t, err)
	require.Equal(t, upstreamCheckpoint, result)
	require.Empty(t, upstream.walkOpts)
}

func TestCheckpointCalculatorReadsMetaFilesInParallelWithinLimit(t *testing.T) {
	ctx := context.Background()
	stores := testutil.StoreIDRange(1, 4)
	boundaries, err := testutil.BuildRegionLayout(
		testutil.AddRoundRobinRegions(8, stores...),
	)
	require.NoError(t, err)

	h := newIntegrationHarness(ctx, t, boundaries)
	initialCheckpoint := h.requireInitialCheckpointByTick()
	upstreamCheckpoint := h.flushRoundsAndGetCheckpoint(stores, 1)
	h.requireCheckpointAdvancedByTick(initialCheckpoint, upstreamCheckpoint)
	h.requireReplicateAllPending()

	var started atomic.Int32
	firstTwoStarted := make(chan struct{})
	releaseFirstTwo := make(chan struct{})
	var closeStarted sync.Once
	testfailpoint.EnableCall(t, checkpointSyncPath+"/before-read-meta", func(_ string) {
		current := started.Add(1)
		if current == 2 {
			closeStarted.Do(func() { close(firstTwoStarted) })
		}
		if current <= 2 {
			<-releaseFirstTwo
		}
	})

	calculator := h.newCalculator(
		withCalculatorConfig(checkpoint.CheckpointCalculatorConfig{MetaReadConcurrency: 2}),
	)

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

	result := h.requireCalcResult(resultCh)
	require.NoError(t, result.err)
	require.Equal(t, upstreamCheckpoint, result.checkpoint)
	require.GreaterOrEqual(t, started.Load(), int32(2))
}

func TestCheckpointCalculatorReturnsCurrentCheckpointWhenUpstreamUnchanged(t *testing.T) {
	ctx := context.Background()
	h := newSingleStoreIntegrationHarness(ctx, t)

	initialCheckpoint := h.requireInitialCheckpointByTick()
	nextCheckpoint := h.flushRoundsAndGetCheckpoint([]uint64{1}, 1)
	h.requireCheckpointAdvancedByTick(initialCheckpoint, nextCheckpoint)
	h.requireReplicateAllPending()

	result, err := h.calculator.ComputeNextCheckpoint(ctx)
	require.NoError(t, err)
	require.Equal(t, nextCheckpoint, result)

	unchangedCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	result, err = h.calculator.ComputeNextCheckpoint(unchangedCtx)
	require.NoError(t, err)
	require.Equal(t, nextCheckpoint, result)
}

func TestCheckpointCalculatorObserverSeesSuccessLifecycle(t *testing.T) {
	ctx := context.Background()
	h := newSingleStoreIntegrationHarness(ctx, t)

	initialCheckpoint := h.requireInitialCheckpointByTick()
	upstreamCheckpoint := h.flushRoundsAndGetCheckpoint([]uint64{1}, 1)
	h.requireCheckpointAdvancedByTick(initialCheckpoint, upstreamCheckpoint)
	h.requireReplicateAllPending()

	observer := &recordingObserver{}
	calculator := h.newCalculator(
		withObserver(observer),
	)

	safeCheckpoint, err := calculator.ComputeNextCheckpoint(ctx)
	require.NoError(t, err)
	require.Equal(t, upstreamCheckpoint, safeCheckpoint)

	events := observer.Events()
	require.Len(t, events, 3)
	require.Equal(t, checkpoint.EventUpstreamAdvanced, events[0].Type)
	require.Equal(t, upstreamCheckpoint, events[0].UpstreamCheckpoint)
	require.Equal(t, checkpoint.EventRoundPlanned, events[1].Type)
	require.Equal(t, 1, events[1].AliveStoreCount)
	require.Greater(t, events[1].PendingFileCount, 0)
	require.NotNil(t, events[1].Statistic)
	require.Equal(t, 1, events[1].Statistic.UpstreamReadMetaFileCount)
	require.Equal(t, 1, events[1].Statistic.EstimatedSyncLogFileCount)
	require.Equal(t, map[string]int{".log": 1, ".meta": 1}, events[1].Statistic.PlannedFileSuffixCounts)
	require.Equal(t, checkpoint.EventCheckpointAdvanced, events[2].Type)
	require.Equal(t, upstreamCheckpoint, events[2].SafeCheckpoint)
	require.Equal(t, calculator.SyncedTS(), events[2].SyncedTS)
	require.NotNil(t, events[2].Statistic)
	require.Equal(t, 2, events[2].Statistic.DownstreamCheckFileCount)
	require.Equal(t, map[string]int{".log": 1, ".meta": 1}, events[2].Statistic.DownstreamCheckFileSuffixCounts)
}

func TestCheckpointCalculatorConcurrentFlushInterleavings(t *testing.T) {
	ctx := context.Background()
	h := newSingleStoreIntegrationHarness(ctx, t)

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

			seqCtx, cancel := context.WithTimeout(h.ctx, 5*time.Second)
			defer cancel()
			script.BeginSeq(seqCtx, round.steps...)
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
			require.Greater(h.t, future.record.CheckpointTS, currentCheckpoint)
			require.Greater(h.t, future.record.FlushTS, currentCheckpoint)

			h.requireReplicateAllPending()

			result := h.requireCalcResult(resultCh)
			require.NoError(h.t, result.err)
			require.Equal(h.t, currentCheckpoint, result.checkpoint)
			require.NoError(h.t, h.AssertDownstreamCanRestoreTo(h.ctx, result.checkpoint))
			futureCheckpoint := future.record.CheckpointTS
			script.EndSeq()
			stableCheckpoint = h.advanceToStableCheckpoint(currentCheckpoint, futureCheckpoint)
			require.Equal(t, futureCheckpoint, stableCheckpoint)
			h.AssertDownstreamCanRestoreTo(ctx, stableCheckpoint)
		})
	}
}

func TestCheckpointCalculatorWaitsForFutureFlushMetaNeededByCheckpoint(t *testing.T) {
	ctx := context.Background()
	h := newSingleStoreIntegrationHarness(ctx, t)

	initialCheckpoint := h.requireInitialCheckpointByTick()
	record, err := h.FlushSim.FlushStore(ctx, 1)
	require.NoError(t, err)
	require.Less(t, record.CheckpointTS, record.FlushTS)
	h.requireCheckpointAdvancedByTick(initialCheckpoint, record.CheckpointTS)

	pulled := h.PullMessages(0)
	require.Greater(t, pulled, 0)

	replicated, err := h.Replicate(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, 1, replicated)

	err = h.AssertDownstreamCanRestoreTo(ctx, record.CheckpointTS)
	require.Error(t, err)
	require.Contains(t, err.Error(), "references unreadable log file")

	computeCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()

	checkpointTS, err := h.calculator.ComputeNextCheckpoint(computeCtx)
	require.Zero(t, checkpointTS)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	h.requireReplicateAllPending()

	checkpointTS, err = h.calculator.ComputeNextCheckpoint(ctx)
	require.NoError(t, err)
	require.Equal(t, record.CheckpointTS, checkpointTS)
	require.NoError(t, h.AssertDownstreamCanRestoreTo(ctx, checkpointTS))
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
	ctx context.Context,
	t *testing.T,
	boundaries []testutil.RegionBoundary,
) *integrationHarness {
	t.Helper()

	return newIntegrationHarnessWithTestContext(ctx, t, testutil.NewTestContext(t), boundaries)
}

func newIntegrationHarnessWithTestContext(
	ctx context.Context,
	t *testing.T,
	tc *testutil.TestContext,
	boundaries []testutil.RegionBoundary,
) *integrationHarness {
	t.Helper()

	h, err := testutil.NewLocalTestHarnessWithTestContext(ctx, tc, boundaries)
	require.NoError(t, err)
	configureAdvancerForSubscriptionDrivenTick(h)
	return &integrationHarness{
		t:           t,
		ctx:         ctx,
		TestHarness: h,
	}
}

func newSingleStoreIntegrationHarness(ctx context.Context, t *testing.T) *integrationHarness {
	t.Helper()

	boundaries, err := testutil.BuildRegionLayout(
		testutil.AddRoundRobinRegions(1, 1),
	)
	require.NoError(t, err)

	h := newIntegrationHarness(ctx, t, boundaries)
	h.calculator = h.newCalculator()
	return h
}

func checkpointStep(name string, fn any) syncpoint.StepDecl {
	return syncpoint.Step(checkpointSyncPath+"/"+name, fn)
}

func flushStep(name string, fn any) syncpoint.StepDecl {
	return syncpoint.Step(testutilSyncPath+"/"+name, fn)
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
	h.PullMessages(0)
	buffered := len(h.CRRWorker.BufferedMessages())
	require.Greater(h.t, buffered, 0)
	replicated, err := h.Replicate(h.ctx, 0)
	require.NoError(h.t, err)
	require.Equal(h.t, buffered, replicated)
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

type calculatorParams struct {
	deps     checkpoint.CalculatorDeps
	cfg      checkpoint.CheckpointCalculatorConfig
	observer checkpoint.Observer
	state    *checkpoint.PersistentState
}

type calculatorOption func(*calculatorParams)

func withCalculatorConfig(cfg checkpoint.CheckpointCalculatorConfig) calculatorOption {
	return func(params *calculatorParams) {
		params.cfg = cfg
	}
}

func withUpstream(upstream checkpoint.UpstreamStorageReader) calculatorOption {
	return func(params *calculatorParams) {
		params.deps.Upstream = upstream
	}
}

func withPersistentState(state checkpoint.PersistentState) calculatorOption {
	return func(params *calculatorParams) {
		params.state = &state
	}
}

func withObserver(observer checkpoint.Observer) calculatorOption {
	return func(params *calculatorParams) {
		params.observer = observer
	}
}

func (h *integrationHarness) newCalculator(
	opts ...calculatorOption,
) *checkpoint.Calculator {
	params := calculatorParams{
		deps: checkpoint.CalculatorDeps{
			PD:       h.PDSim,
			Upstream: h.Upstream,
			Sync:     checkpoint.NewExistenceSyncChecker(h.Downstream),
		},
	}
	for _, opt := range opts {
		opt(&params)
	}
	if params.cfg.TaskName == "" {
		params.cfg.TaskName = "drr_test_task"
	}
	if params.cfg.PollInterval == 0 {
		params.cfg.PollInterval = 5 * time.Millisecond
	}
	calculator, err := checkpoint.NewCalculator(params.deps, params.cfg, params.observer)
	require.NoError(h.t, err)
	if params.state != nil {
		require.NoError(h.t, calculator.RestorePersistentState(*params.state))
	}
	return calculator
}

func (h *integrationHarness) flushRoundsAndGetCheckpoint(stores []uint64, rounds int) uint64 {
	var roundCheckpoint uint64
	for range rounds {
		roundCheckpoint = ^uint64(0)
		for _, storeID := range stores {
			record, err := h.FlushSim.FlushStore(h.ctx, storeID)
			require.NoError(h.t, err)
			if record.CheckpointTS < roundCheckpoint {
				roundCheckpoint = record.CheckpointTS
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
