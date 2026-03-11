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
	"math"
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

	firstCheckpoint := h.flushRoundsAndGetCheckpoint([]uint64{1}, 1)
	h.requireCheckpointAdvancedByTick(initialCheckpoint, firstCheckpoint)
	h.requireReplicateAllPending()

	computedCheckpoint, err := calculator.ComputeNextCheckpoint(ctx)
	require.NoError(t, err)
	require.Equal(t, firstCheckpoint, computedCheckpoint)
	require.Len(t, upstream.walkOpts, 1)
	require.Empty(t, upstream.walkOpts[0].StartAfter)

	secondCheckpoint := h.flushRoundsAndGetCheckpoint([]uint64{1}, 1)
	h.requireCheckpointAdvancedByTick(firstCheckpoint, secondCheckpoint)
	h.requireReplicateAllPending()

	computedCheckpoint, err = calculator.ComputeNextCheckpoint(ctx)
	require.NoError(t, err)
	require.Equal(t, secondCheckpoint, computedCheckpoint)
	require.Len(t, upstream.walkOpts, 2)
	require.Equal(
		t,
		fmt.Sprintf("%s/%016x%s", stream.GetStreamBackupMetaPrefix(), firstCheckpoint, "ffffffffffffffff~"),
		upstream.walkOpts[1].StartAfter,
	)
}

func TestCheckpointCalculatorUsesConfiguredInitialSyncedTS(t *testing.T) {
	ctx := context.Background()
	h := newSingleStoreIntegrationHarness(ctx, t)

	initialCheckpoint := h.requireInitialCheckpointByTick()
	firstFlushTS := h.flushRoundsAndGetCheckpoint([]uint64{1}, 1)
	secondCheckpoint := h.flushRoundsAndGetCheckpoint([]uint64{1}, 1)
	require.Greater(t, secondCheckpoint, firstFlushTS)
	h.requireCheckpointAdvancedByTick(initialCheckpoint, secondCheckpoint)
	h.requireReplicateAllPending()

	upstream := &recordingUpstreamStorage{inner: h.Upstream}
	calculator := h.newCalculator(
		withCalculatorConfig(checkpoint.CheckpointCalculatorConfig{InitialSyncedTS: firstFlushTS}),
		withUpstream(upstream),
	)

	computedCheckpoint, err := calculator.ComputeNextCheckpoint(ctx)
	require.NoError(t, err)
	require.Equal(t, secondCheckpoint, computedCheckpoint)
	require.Len(t, upstream.walkOpts, 1)
	require.Equal(
		t,
		fmt.Sprintf("%s/%016x%s", stream.GetStreamBackupMetaPrefix(), firstFlushTS, "ffffffffffffffff~"),
		upstream.walkOpts[0].StartAfter,
	)
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
	require.Equal(t, checkpoint.EventCheckpointAdvanced, events[2].Type)
	require.Equal(t, upstreamCheckpoint, events[2].SafeCheckpoint)
	require.Equal(t, calculator.SyncedTS(), events[2].SyncedTS)
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
			require.Greater(h.t, future.record.FlushTS, currentCheckpoint)

			h.requireReplicateAllPending()

			result := h.requireCalcResult(resultCh)
			require.NoError(h.t, result.err)
			require.Equal(h.t, currentCheckpoint, result.checkpoint)
			require.NoError(h.t, h.AssertDownstreamCanRestoreTo(h.ctx, result.checkpoint))
			futureFlushTS := future.record.FlushTS
			script.EndSeq()
			stableCheckpoint = h.advanceToStableCheckpoint(currentCheckpoint, futureFlushTS)
			require.Equal(t, futureFlushTS, stableCheckpoint)
			h.AssertDownstreamCanRestoreTo(ctx, stableCheckpoint)
		})
	}
}

func TestCheckpointCalculatorRandomizedCRRSimulation(t *testing.T) {
	ctx := context.Background()
	tc := testutil.NewTestContext(t)
	cfg := randomizedCRRSimulationConfig{
		Iterations:                     1000,
		InitialStores:                  3,
		MaxStores:                      12,
		RegionCount:                    12,
		MaxNonFlushStoresPerRound:      2,
		StoreNoFlushChancePercent:      10,
		AddStoreChancePercent:          12,
		MaxAddStoresPerRound:           1,
		RemoveStoreChancePercent:       10,
		MaxRemoveStoresPerRound:        1,
		ScatterChancePercent:           3,
		MaxScatterRegionsPerRound:      3,
		ReplicateChancePercent:         90,
		MaxReplicateBatchPerRound:      12,
		RestartCalculatorChancePercent: 8,
		GlobalProgressCheckEvery:       3,
		CatchUpEvery:                   50,
		ComputeTimeout:                 2 * time.Millisecond,
		CatchUpTimeout:                 100 * time.Millisecond,
		CalculatorPollInterval:         time.Millisecond,
	}
	if testing.Verbose() {
		t.Logf("randomized crr seed=%d config=%+v", tc.Seed(), cfg)
	}

	stores := testutil.StoreIDRange(1, cfg.InitialStores)
	boundaries, err := testutil.BuildRegionLayout(
		testutil.AddRoundRobinRegions(cfg.RegionCount, stores...),
	)
	require.NoError(t, err)

	h := newIntegrationHarnessWithTestContext(ctx, t, tc, boundaries)
	h.calculator = h.newCalculator(
		withCalculatorConfig(checkpoint.CheckpointCalculatorConfig{PollInterval: cfg.CalculatorPollInterval}),
	)

	sim := newRandomizedCRRSimulation(h, tc.RNG("checkpoint-randomized-simulation"), cfg)
	lastSafeCheckpoint := h.requireInitialCheckpointByTick()
	lastValidatedCheckpoint := lastSafeCheckpoint
	lastGlobalCheckpoint := lastSafeCheckpoint

	for round := 1; round <= cfg.Iterations; round++ {
		sim.runRound(round, &lastSafeCheckpoint, &lastValidatedCheckpoint)

		if round%cfg.GlobalProgressCheckEvery == 0 {
			lastGlobalCheckpoint = sim.requireGlobalCheckpointProgress(lastGlobalCheckpoint)
			sim.logf("round=%d global-progress checkpoint=%d state=%s", round, lastGlobalCheckpoint, sim.describeState())
		}

		if round%cfg.CatchUpEvery == 0 {
			sim.replicateAllPending()

			checkpoint, advanced := sim.tryComputeCheckpoint(cfg.CatchUpTimeout)
			require.Truef(t, advanced, "calculator failed to catch up after draining pending files, state=%s", sim.describeState())
			require.GreaterOrEqual(t, checkpoint, lastSafeCheckpoint)
			sim.requireCheckpointRangeSafe(lastValidatedCheckpoint, checkpoint)
			lastSafeCheckpoint = checkpoint
			lastValidatedCheckpoint = checkpoint
			sim.logf("round=%d catch-up checkpoint=%d state=%s", round, checkpoint, sim.describeState())
		}
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

type randomizedCRRSimulationConfig struct {
	Iterations                     int
	InitialStores                  int
	MaxStores                      int
	RegionCount                    int
	StoreNoFlushChancePercent      int
	MaxNonFlushStoresPerRound      int
	AddStoreChancePercent          int
	MaxAddStoresPerRound           int
	RemoveStoreChancePercent       int
	MaxRemoveStoresPerRound        int
	ScatterChancePercent           int
	MaxScatterRegionsPerRound      int
	ReplicateChancePercent         int
	MaxReplicateBatchPerRound      int
	RestartCalculatorChancePercent int
	GlobalProgressCheckEvery       int
	CatchUpEvery                   int
	ComputeTimeout                 time.Duration
	CatchUpTimeout                 time.Duration
	CalculatorPollInterval         time.Duration
}

type randomizedCRRSimulation struct {
	h   *integrationHarness
	rng interface {
		IntN(int) int
	}
	cfg           randomizedCRRSimulationConfig
	nextStoreID   uint64
	readyStoreIDs []uint64
}

type randomizedCRRRoundLog struct {
	flushedStores       []uint64
	replicatedFiles     int
	restartedCalculator bool
	addedStores         []uint64
	removedStores       []uint64
	scatteredRegions    []uint64
	checkpoint          uint64
	advanced            bool
}

func newRandomizedCRRSimulation(
	h *integrationHarness,
	rng interface {
		IntN(int) int
	},
	cfg randomizedCRRSimulationConfig,
) *randomizedCRRSimulation {
	nextStoreID := uint64(1)
	for _, storeID := range h.PDSim.StoreIDs() {
		if storeID >= nextStoreID {
			nextStoreID = storeID + 1
		}
	}
	return &randomizedCRRSimulation{
		h:             h,
		rng:           rng,
		cfg:           cfg,
		nextStoreID:   nextStoreID,
		readyStoreIDs: append([]uint64(nil), h.PDSim.StoreIDs()...),
	}
}

func (s *randomizedCRRSimulation) runRound(
	round int,
	lastSafeCheckpoint *uint64,
	lastValidatedCheckpoint *uint64,
) {
	roundLog := randomizedCRRRoundLog{}
	roundLog.flushedStores = s.flushRandomStores()
	roundLog.replicatedFiles = s.replicateRandomBufferedFiles()
	roundLog.restartedCalculator = s.restartCalculatorIfNeeded()

	checkpoint, advanced := s.tryComputeCheckpoint(s.cfg.ComputeTimeout)
	roundLog.checkpoint = checkpoint
	roundLog.advanced = advanced
	if advanced {
		require.GreaterOrEqualf(
			s.h.t,
			checkpoint,
			*lastSafeCheckpoint,
			"calculator checkpoint regressed at round %d, state=%s",
			round,
			s.describeState(),
		)
		s.requireCheckpointRangeSafe(*lastValidatedCheckpoint, checkpoint)
		*lastSafeCheckpoint = checkpoint
		*lastValidatedCheckpoint = checkpoint
	} else {
		require.GreaterOrEqualf(
			s.h.t,
			*lastValidatedCheckpoint,
			*lastSafeCheckpoint,
			"validated checkpoint fell behind safe checkpoint at round %d, state=%s",
			round,
			s.describeState(),
		)
	}

	roundLog.addedStores = s.addRandomStores()
	roundLog.removedStores = s.removeRandomStores()
	roundLog.scatteredRegions = s.scatterRandomRegions()
	s.logf(
		"round=%d flush=%v replicate=%d restart=%t add=%v remove=%v scatter=%v advanced=%t checkpoint=%d safe=%d validated=%d state=%s",
		round,
		roundLog.flushedStores,
		roundLog.replicatedFiles,
		roundLog.restartedCalculator,
		roundLog.addedStores,
		roundLog.removedStores,
		roundLog.scatteredRegions,
		roundLog.advanced,
		roundLog.checkpoint,
		*lastSafeCheckpoint,
		*lastValidatedCheckpoint,
		s.describeState(),
	)
}

func (s *randomizedCRRSimulation) logf(format string, args ...any) {
	if testing.Verbose() {
		s.h.t.Logf(format, args...)
	}
}

func (s *randomizedCRRSimulation) flushRandomStores() []uint64 {
	storesWithRegions := s.storesWithRegions()
	if len(storesWithRegions) == 0 {
		return nil
	}
	count := s.sampleOptionalCount(s.cfg.StoreNoFlushChancePercent, s.cfg.MaxNonFlushStoresPerRound, math.MaxInt)
	selected := s.pickStoreSubset(storesWithRegions, len(storesWithRegions)-count)
	start := make(chan struct{})
	resultCh := make(chan flushResult, len(selected))
	for _, storeID := range selected {
		storeID := storeID
		go func() {
			<-start
			record, err := s.h.FlushSim.FlushStore(s.h.ctx, storeID)
			resultCh <- flushResult{record: record, err: err}
		}()
	}
	close(start)
	for range selected {
		result := <-resultCh
		require.NoError(s.h.t, result.err)
	}
	return selected
}

func (s *randomizedCRRSimulation) addRandomStores() []uint64 {
	limit := s.cfg.MaxStores - len(s.h.PDSim.StoreIDs())
	count := s.sampleOptionalCount(s.cfg.AddStoreChancePercent, s.cfg.MaxAddStoresPerRound, limit)
	added := make([]uint64, 0, count)
	for range count {
		storeID := s.nextStoreID
		store := s.h.PDSim.EnsureStore(storeID, s.h.PDSim.CurrentTSO())
		store.SetSupportFlushSub(true)
		store.LegacyRegionCheckpointRPCEnabled = false
		store.FlushTaskName = "drr"
		added = append(added, storeID)
		s.nextStoreID++
	}
	return added
}

func (s *randomizedCRRSimulation) removeRandomStores() []uint64 {
	storeIDs := s.removableStoreIDs()
	remaining := s.sampleOptionalCount(s.cfg.RemoveStoreChancePercent, s.cfg.MaxRemoveStoresPerRound, len(storeIDs))
	if remaining == 0 {
		return nil
	}
	removed := make([]uint64, 0, remaining)
	for remaining > 0 {
		candidates := s.removableStoreIDs()
		if len(candidates) == 0 {
			return removed
		}
		storeID := candidates[s.rng.IntN(len(candidates))]
		remainingStores := excludeStoreID(s.currentReadyStoreIDs(), storeID)
		for _, regionID := range s.regionIDsOnStore(storeID) {
			if len(remainingStores) == 0 {
				break
			}
			destination := remainingStores[s.rng.IntN(len(remainingStores))]
			s.h.PDSim.TransferRegionTo(regionID, []uint64{destination})
			s.h.PDSim.SetRegionLeader(regionID, destination)
			s.h.PDSim.BumpRegionEpoch(regionID)
			s.h.PDSim.SetRegionCheckpoint(regionID, s.h.PDSim.GlobalCheckpoint())
		}
		s.h.PDSim.RemoveStore(storeID)
		removed = append(removed, storeID)
		remaining--
	}
	return removed
}

func (s *randomizedCRRSimulation) scatterRandomRegions() []uint64 {
	storeIDs := s.currentReadyStoreIDs()
	if len(storeIDs) <= 1 {
		return nil
	}
	regionIDs := s.h.PDSim.RegionIDs()
	count := s.sampleOptionalCount(s.cfg.ScatterChancePercent, s.cfg.MaxScatterRegionsPerRound, len(regionIDs))
	selected := s.pickRegionSubset(regionIDs, count)
	scattered := make([]uint64, 0, len(selected))
	for _, regionID := range selected {
		state, ok := s.h.PDSim.RegionSnapshot(regionID)
		require.True(s.h.t, ok)

		candidates := excludeStoreID(storeIDs, state.StoreID)
		if len(candidates) == 0 {
			continue
		}
		destination := candidates[s.rng.IntN(len(candidates))]
		s.h.PDSim.TransferRegionTo(regionID, []uint64{destination})
		s.h.PDSim.SetRegionLeader(regionID, destination)
		s.h.PDSim.BumpRegionEpoch(regionID)
		s.h.PDSim.SetRegionCheckpoint(regionID, s.h.PDSim.GlobalCheckpoint())
		scattered = append(scattered, regionID)
	}
	return scattered
}

func (s *randomizedCRRSimulation) replicateRandomBufferedFiles() int {
	s.h.PullMessages(0)
	buffered := len(s.h.CRRWorker.BufferedMessages())
	count := s.samplePartialReplicateCount(buffered)
	if count == 0 {
		return 0
	}
	replicated, err := s.h.Replicate(s.h.ctx, count)
	require.NoError(s.h.t, err)
	require.Equal(s.h.t, count, replicated)
	return replicated
}

func (s *randomizedCRRSimulation) restartCalculatorIfNeeded() bool {
	if !s.rollPercent(s.cfg.RestartCalculatorChancePercent) {
		return false
	}
	s.h.calculator = s.h.newCalculator(
		withCalculatorConfig(checkpoint.CheckpointCalculatorConfig{PollInterval: s.cfg.CalculatorPollInterval}),
	)
	return true
}

func (s *randomizedCRRSimulation) tryComputeCheckpoint(timeout time.Duration) (uint64, bool) {
	ctx, cancel := context.WithTimeout(s.h.ctx, timeout)
	defer cancel()

	checkpoint, err := s.h.calculator.ComputeNextCheckpoint(ctx)
	if err == nil {
		return checkpoint, true
	}
	require.ErrorIs(s.h.t, err, context.DeadlineExceeded)
	return 0, false
}

func (s *randomizedCRRSimulation) replicateAllPending() {
	for {
		pulled := s.h.PullMessages(0)
		buffered := len(s.h.CRRWorker.BufferedMessages())
		if buffered == 0 {
			require.Zero(s.h.t, pulled)
			return
		}
		replicated, err := s.h.Replicate(s.h.ctx, 0)
		require.NoError(s.h.t, err)
		require.Equal(s.h.t, buffered, replicated)
	}
}

func (s *randomizedCRRSimulation) requireGlobalCheckpointProgress(previous uint64) uint64 {
	expected := s.flushAllStoresAndGetCheckpoint()
	s.h.requireCheckpointAdvancedByTick(previous, expected)
	s.readyStoreIDs = append(s.readyStoreIDs[:0], s.h.PDSim.StoreIDs()...)
	return expected
}

func (s *randomizedCRRSimulation) requireCheckpointRangeSafe(previous, current uint64) {
	if current <= previous {
		return
	}
	for _, record := range s.h.FlushSim.RecordsUpTo(current) {
		if record.FlushTS <= previous {
			continue
		}
		_, err := s.h.Downstream.ReadFile(s.h.ctx, record.MetadataPath)
		require.NoErrorf(s.h.t, err, "metadata %s should be readable", record.MetadataPath)
		for _, logPath := range record.LogPaths {
			_, err := s.h.Downstream.ReadFile(s.h.ctx, logPath)
			require.NoErrorf(s.h.t, err, "log %s should be readable", logPath)
		}
	}
}

func (s *randomizedCRRSimulation) describeState() string {
	return fmt.Sprintf(
		"stores=%v buffered=%d global=%d synced=%d",
		s.h.PDSim.StoreIDs(),
		len(s.h.CRRWorker.BufferedMessages()),
		s.h.PDSim.GlobalCheckpoint(),
		s.h.calculator.SyncedTS(),
	)
}

func (s *randomizedCRRSimulation) storesWithRegions() []uint64 {
	stores := make([]uint64, 0)
	for _, storeID := range s.h.PDSim.StoreIDs() {
		if len(s.regionIDsOnStore(storeID)) > 0 {
			stores = append(stores, storeID)
		}
	}
	return stores
}

func (s *randomizedCRRSimulation) flushAllStoresAndGetCheckpoint() uint64 {
	stores := s.storesWithRegions()
	require.NotEmpty(s.h.t, stores)

	checkpoint := ^uint64(0)
	for _, storeID := range stores {
		record, err := s.h.FlushSim.FlushStore(s.h.ctx, storeID)
		require.NoError(s.h.t, err)
		if record.FlushTS < checkpoint {
			checkpoint = record.FlushTS
		}
	}
	require.NotEqual(s.h.t, ^uint64(0), checkpoint)
	return checkpoint
}

func (s *randomizedCRRSimulation) removableStoreIDs() []uint64 {
	storeIDs := s.h.PDSim.StoreIDs()
	readyStores := s.currentReadyStoreIDs()
	result := make([]uint64, 0, len(storeIDs))
	for _, storeID := range storeIDs {
		if len(storeIDs) <= 1 {
			break
		}
		if len(s.regionIDsOnStore(storeID)) == 0 {
			result = append(result, storeID)
			continue
		}
		if len(excludeStoreID(readyStores, storeID)) > 0 {
			result = append(result, storeID)
		}
	}
	return result
}

func (s *randomizedCRRSimulation) currentReadyStoreIDs() []uint64 {
	currentStoreSet := make(map[uint64]struct{}, len(s.h.PDSim.StoreIDs()))
	for _, storeID := range s.h.PDSim.StoreIDs() {
		currentStoreSet[storeID] = struct{}{}
	}
	readyStores := make([]uint64, 0, len(s.readyStoreIDs))
	for _, storeID := range s.readyStoreIDs {
		if _, ok := currentStoreSet[storeID]; ok {
			readyStores = append(readyStores, storeID)
		}
	}
	return readyStores
}

func (s *randomizedCRRSimulation) regionIDsOnStore(storeID uint64) []uint64 {
	states, err := s.h.PDSim.RegionSnapshotsOnStore(storeID)
	require.NoError(s.h.t, err)
	regionIDs := make([]uint64, 0, len(states))
	for _, state := range states {
		regionIDs = append(regionIDs, state.ID)
	}
	return regionIDs
}

func (s *randomizedCRRSimulation) pickStoreSubset(storeIDs []uint64, count int) []uint64 {
	return pickUint64Subset(storeIDs, count, s.rng.IntN)
}

func (s *randomizedCRRSimulation) pickRegionSubset(regionIDs []uint64, count int) []uint64 {
	return pickUint64Subset(regionIDs, count, s.rng.IntN)
}

func (s *randomizedCRRSimulation) sampleOptionalCount(chancePercent, maxCount, limit int) int {
	if limit <= 0 || maxCount <= 0 || !s.rollPercent(chancePercent) {
		return 0
	}
	return 1 + s.rng.IntN(min(limit, maxCount))
}

func (s *randomizedCRRSimulation) samplePartialReplicateCount(buffered int) int {
	if buffered <= 0 || !s.rollPercent(s.cfg.ReplicateChancePercent) {
		return 0
	}
	limit := min(buffered, s.cfg.MaxReplicateBatchPerRound)
	if limit <= 1 {
		return limit
	}
	return 1 + s.rng.IntN(limit-1)
}

func (s *randomizedCRRSimulation) rollPercent(chancePercent int) bool {
	if chancePercent <= 0 {
		return false
	}
	if chancePercent >= 100 {
		return true
	}
	return s.rng.IntN(100) < chancePercent
}

func pickUint64Subset(items []uint64, count int, intN func(int) int) []uint64 {
	if count <= 0 || len(items) == 0 {
		return nil
	}
	pool := append([]uint64(nil), items...)
	for i := len(pool) - 1; i > 0; i-- {
		j := intN(i + 1)
		pool[i], pool[j] = pool[j], pool[i]
	}
	if count > len(pool) {
		count = len(pool)
	}
	return pool[:count]
}

func excludeStoreID(storeIDs []uint64, excluded uint64) []uint64 {
	result := make([]uint64, 0, len(storeIDs))
	for _, storeID := range storeIDs {
		if storeID != excluded {
			result = append(result, storeID)
		}
	}
	return result
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

type calculatorParams struct {
	deps     checkpoint.CalculatorDeps
	cfg      checkpoint.CheckpointCalculatorConfig
	observer checkpoint.Observer
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

func withDownstream(downstream checkpoint.DownstreamObjectChecker) calculatorOption {
	return func(params *calculatorParams) {
		params.deps.Downstream = downstream
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
			PD:         h.PDSim,
			Upstream:   h.Upstream,
			Downstream: h.Downstream,
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
	return calculator
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
