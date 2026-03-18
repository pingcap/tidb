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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/stream/crr/internal/checkpoint"
	testutil "github.com/pingcap/tidb/br/pkg/utiltest/crr"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestCheckpointCalculatorRandomizedCRRSimulation(t *testing.T) {
	ctx := context.Background()
	tc := testutil.NewTestContext(t)
	cfg := randomizedCRRSimulationConfig{
		Iterations:                        1000,
		InitialStores:                     3,
		MaxStores:                         12,
		RegionCount:                       12,
		MaxNonFlushStoresPerRound:         2,
		StoreNoFlushChancePercent:         10,
		AddStoreChancePercent:             12,
		MaxAddStoresPerRound:              1,
		RemoveStoreChancePercent:          10,
		MaxRemoveStoresPerRound:           1,
		ScatterChancePercent:              3,
		MaxScatterRegionsPerRound:         3,
		ReplicateChancePercent:            90,
		MaxReplicateBatchPerRound:         20,
		MaxBufferedFiles:                  32,
		RestartCalculatorChancePercent:    10,
		RestartCarrySyncedTSChancePercent: 100,
		PruneEmptyStoreAfterRounds:        3,
		PruneEmptyStoreChancePercent:      85,
		GlobalProgressCheckEvery:          13,
		CatchUpEvery:                      299,
		ComputeTimeout:                    2 * time.Millisecond,
		CatchUpTimeout:                    100 * time.Millisecond,
		CalculatorPollInterval:            time.Millisecond,
	}
	log.Info(
		"randomized crr simulation seed",
		zap.Int64("seed", tc.Seed()),
		zap.Reflect("config", cfg),
	)

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
			sim.log(
				"randomized crr global progress",
				zap.Int("round", round),
				zap.Uint64("checkpoint", lastGlobalCheckpoint),
				zap.String("state", sim.describeState()),
			)
		}

		if round%cfg.CatchUpEvery == 0 {
			sim.pruneEmptyStores(true)
			sim.replicateAllPending()

			checkpoint, advanced := sim.tryComputeCheckpoint(cfg.CatchUpTimeout)
			require.Truef(t, advanced, "calculator failed to catch up after draining pending files, state=%s", sim.describeState())
			require.GreaterOrEqual(t, checkpoint, lastSafeCheckpoint)
			sim.requireCheckpointRangeSafe(lastValidatedCheckpoint, checkpoint)
			lastSafeCheckpoint = checkpoint
			lastValidatedCheckpoint = checkpoint
			sim.log(
				"randomized crr catch up",
				zap.Int("round", round),
				zap.Uint64("checkpoint", checkpoint),
				zap.String("state", sim.describeState()),
			)
		}
	}

	require.Greater(t, sim.lastState.SyncedTS, uint64(0), "synced ts should advance during randomized simulation")
}

type randomizedCRRSimulationConfig struct {
	Iterations                        int
	InitialStores                     int
	MaxStores                         int
	RegionCount                       int
	StoreNoFlushChancePercent         int
	MaxNonFlushStoresPerRound         int
	AddStoreChancePercent             int
	MaxAddStoresPerRound              int
	RemoveStoreChancePercent          int
	MaxRemoveStoresPerRound           int
	ScatterChancePercent              int
	MaxScatterRegionsPerRound         int
	ReplicateChancePercent            int
	MaxReplicateBatchPerRound         int
	MaxBufferedFiles                  int
	RestartCalculatorChancePercent    int
	RestartCarrySyncedTSChancePercent int
	PruneEmptyStoreAfterRounds        int
	PruneEmptyStoreChancePercent      int
	GlobalProgressCheckEvery          int
	CatchUpEvery                      int
	ComputeTimeout                    time.Duration
	CatchUpTimeout                    time.Duration
	CalculatorPollInterval            time.Duration
}

type randomizedCRRSimulation struct {
	h                *integrationHarness
	rng              intNSource
	cfg              randomizedCRRSimulationConfig
	nextStoreID      uint64
	pendingStoreIDs  []uint64
	readyStoreIDs    []uint64
	lastState        checkpoint.PersistentState
	emptyStoreRounds map[uint64]int
}

type randomizedCRRRoundLog struct {
	flushedStores       []uint64
	replicatedFiles     int
	restartedCalculator bool
	addedStores         []uint64
	removedStores       []uint64
	scatteredRegions    []uint64
	actionOrder         []string
	checkpoint          uint64
	advanced            bool
}

type intNSource interface {
	IntN(int) int
}

type randomizedCRRRoundAction struct {
	name string
	run  func(*randomizedCRRRoundLog)
}

type roundRNG struct {
	rng *rand.Rand
}

func (r *roundRNG) IntN(n int) int {
	return r.rng.Intn(n)
}

func newRandomizedCRRSimulation(
	h *integrationHarness,
	rng intNSource,
	cfg randomizedCRRSimulationConfig,
) *randomizedCRRSimulation {
	nextStoreID := uint64(1)
	for _, storeID := range h.PDSim.StoreIDs() {
		if storeID >= nextStoreID {
			nextStoreID = storeID + 1
		}
	}
	return &randomizedCRRSimulation{
		h:                h,
		rng:              rng,
		cfg:              cfg,
		nextStoreID:      nextStoreID,
		readyStoreIDs:    append([]uint64(nil), h.PDSim.StoreIDs()...),
		lastState:        h.calculator.StateSnapshot(),
		emptyStoreRounds: make(map[uint64]int),
	}
}

func (s *randomizedCRRSimulation) runRound(
	round int,
	lastSafeCheckpoint *uint64,
	lastValidatedCheckpoint *uint64,
) {
	roundLog := randomizedCRRRoundLog{}
	// The replicate and compute branches stay concurrent, but they must not
	// share one RNG or same-seed runs will diverge with goroutine scheduling.
	replicateRNG := s.newConcurrentRNG()
	computeRNG := s.newConcurrentRNG()

	for _, action := range s.shuffleRoundActions([]randomizedCRRRoundAction{
		{
			name: "replicate-and-compute",
			run: func(log *randomizedCRRRoundLog) {
				wg := new(sync.WaitGroup)
				wg.Go(func() {
					log.replicatedFiles = s.replicateRandomBufferedFiles(replicateRNG)
				})
				wg.Go(func() {
					log.restartedCalculator = s.restartCalculatorIfNeeded(computeRNG)
					log.checkpoint, log.advanced = s.tryComputeCheckpoint(s.cfg.ComputeTimeout)
				})
				wg.Wait()
			},
		},
		{
			name: "add-stores",
			run: func(log *randomizedCRRRoundLog) {
				log.addedStores = s.addRandomStores()
			},
		},
		{
			name: "remove-stores",
			run: func(log *randomizedCRRRoundLog) {
				log.removedStores = s.removeRandomStores()
			},
		},
		{
			name: "scatter-regions",
			run: func(log *randomizedCRRRoundLog) {
				log.scatteredRegions = s.scatterRandomRegions()
			},
		},
		{
			name: "prune-empty-stores",
			run: func(log *randomizedCRRRoundLog) {
				log.removedStores = append(log.removedStores, s.pruneEmptyStores(false)...)
			},
		},
		{
			name: "flush-stores",
			run: func(log *randomizedCRRRoundLog) {
				log.flushedStores = s.flushRandomStores()
			},
		},
	}) {
		roundLog.actionOrder = append(roundLog.actionOrder, action.name)
		action.run(&roundLog)
	}

	s.rememberSyncedTS()

	s.log(
		"randomized crr round",
		zap.Int("round", round),
		zap.Uint64s("flushed-stores", roundLog.flushedStores),
		zap.Int("replicated-files", roundLog.replicatedFiles),
		zap.Bool("restarted-calculator", roundLog.restartedCalculator),
		zap.Uint64s("added-stores", roundLog.addedStores),
		zap.Uint64s("removed-stores", roundLog.removedStores),
		zap.Uint64s("scattered-regions", roundLog.scatteredRegions),
		zap.Strings("action-order", roundLog.actionOrder),
		zap.Bool("advanced", roundLog.advanced),
		zap.Uint64("checkpoint", roundLog.checkpoint),
		zap.Uint64("safe-checkpoint", *lastSafeCheckpoint),
		zap.Uint64("validated-checkpoint", *lastValidatedCheckpoint),
		zap.String("state", s.describeState()),
	)

	if roundLog.advanced {
		require.GreaterOrEqualf(
			s.h.t,
			roundLog.checkpoint,
			*lastSafeCheckpoint,
			"calculator checkpoint regressed at round %d, state=%s",
			round,
			s.describeState(),
		)
		s.requireCheckpointRangeSafe(*lastValidatedCheckpoint, roundLog.checkpoint)
		*lastSafeCheckpoint = roundLog.checkpoint
		*lastValidatedCheckpoint = roundLog.checkpoint
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
}

func (s *randomizedCRRSimulation) log(msg string, fields ...zap.Field) {
	log.Info(msg, fields...)
}

func (s *randomizedCRRSimulation) shuffleRoundActions(
	actions []randomizedCRRRoundAction,
) []randomizedCRRRoundAction {
	for i := len(actions) - 1; i > 0; i-- {
		j := s.rng.IntN(i + 1)
		actions[i], actions[j] = actions[j], actions[i]
	}
	return actions
}

func (s *randomizedCRRSimulation) flushRandomStores() []uint64 {
	storesWithRegions := s.storesWithRegions()
	if len(storesWithRegions) == 0 {
		return nil
	}
	count := s.sampleOptionalCount(s.cfg.StoreNoFlushChancePercent, s.cfg.MaxNonFlushStoresPerRound, math.MaxInt)
	selected := s.pickStoreSubset(storesWithRegions, len(storesWithRegions)-count)
	for _, storeID := range selected {
		_, err := s.h.FlushSim.FlushStore(s.h.ctx, storeID)
		require.NoError(s.h.t, err)
	}
	return selected
}

func (s *randomizedCRRSimulation) addRandomStores() []uint64 {
	limit := s.cfg.MaxStores - len(s.h.PDSim.StoreIDs()) - len(s.pendingStoreIDs)
	count := s.sampleOptionalCount(s.cfg.AddStoreChancePercent, s.cfg.MaxAddStoresPerRound, limit)
	added := make([]uint64, 0, count)
	for range count {
		storeID := s.nextStoreID
		s.pendingStoreIDs = append(s.pendingStoreIDs, storeID)
		added = append(added, storeID)
		s.nextStoreID++
	}
	return added
}

func (s *randomizedCRRSimulation) activatePendingStores() bool {
	if len(s.pendingStoreIDs) == 0 {
		return false
	}

	for _, storeID := range s.pendingStoreIDs {
		store := s.h.PDSim.EnsureStore(storeID, s.h.PDSim.CurrentTSO())
		store.SetSupportFlushSub(true)
		store.LegacyRegionCheckpointRPCEnabled = false
		store.FlushTaskName = "drr"
		s.readyStoreIDs = append(s.readyStoreIDs, storeID)
		s.bootstrapAddedStore(storeID)
	}
	s.pendingStoreIDs = s.pendingStoreIDs[:0]
	return true
}

func (s *randomizedCRRSimulation) bootstrapAddedStore(storeID uint64) {
	donorStores := excludeStoreID(s.currentReadyStoreIDs(), storeID)
	if len(donorStores) == 0 {
		return
	}
	donorStoreID := donorStores[s.rng.IntN(len(donorStores))]
	regionIDs := s.regionIDsOnStore(donorStoreID)
	if len(regionIDs) == 0 {
		return
	}

	// Bootstrap the new store with one region so it can be flushed in the same
	// round after topology changes settle.
	regionID := regionIDs[s.rng.IntN(len(regionIDs))]
	s.h.PDSim.TransferRegionTo(regionID, []uint64{storeID})
	s.h.PDSim.SetRegionLeader(regionID, storeID)
	s.h.PDSim.BumpRegionEpoch(regionID)
	s.h.PDSim.SetRegionCheckpoint(regionID, s.h.PDSim.GlobalCheckpoint())
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

func (s *randomizedCRRSimulation) pruneEmptyStores(force bool) []uint64 {
	storeIDs := s.h.PDSim.StoreIDs()
	removed := make([]uint64, 0)
	currentStores := make(map[uint64]struct{}, len(storeIDs))
	for _, storeID := range storeIDs {
		currentStores[storeID] = struct{}{}
		if len(s.regionIDsOnStore(storeID)) != 0 {
			delete(s.emptyStoreRounds, storeID)
			continue
		}
		s.emptyStoreRounds[storeID]++
		if !force && s.emptyStoreRounds[storeID] < s.cfg.PruneEmptyStoreAfterRounds {
			continue
		}
		if !force && !s.rollPercent(s.cfg.PruneEmptyStoreChancePercent) {
			continue
		}
		s.h.PDSim.RemoveStore(storeID)
		delete(s.emptyStoreRounds, storeID)
		removed = append(removed, storeID)
	}
	for storeID := range s.emptyStoreRounds {
		if _, ok := currentStores[storeID]; !ok {
			delete(s.emptyStoreRounds, storeID)
		}
	}
	return removed
}

func (s *randomizedCRRSimulation) replicateRandomBufferedFiles(rng intNSource) int {
	s.h.PullMessages(0)
	buffered := len(s.h.CRRWorker.BufferedMessages())
	count := s.samplePartialReplicateCount(buffered, rng)
	if count == 0 {
		return 0
	}
	replicated, err := s.h.CRRWorker.ReplicateBufferedRandom(s.h.ctx, count, rng.IntN)
	require.NoError(s.h.t, err)
	require.Equal(s.h.t, count, replicated)
	return replicated
}

func (s *randomizedCRRSimulation) restartCalculatorIfNeeded(rng intNSource) bool {
	s.rememberSyncedTS()
	if !rollPercent(rng, s.cfg.RestartCalculatorChancePercent) {
		return false
	}
	cfg := checkpoint.CheckpointCalculatorConfig{PollInterval: s.cfg.CalculatorPollInterval}
	opts := []calculatorOption{withCalculatorConfig(cfg)}
	if s.lastState.SyncedTS > 0 && rollPercent(rng, s.cfg.RestartCarrySyncedTSChancePercent) {
		opts = append(opts, withPersistentState(s.lastState))
	}
	s.h.calculator = s.h.newCalculator(
		opts...,
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
		replicated, err := s.h.CRRWorker.ReplicateBufferedRandom(s.h.ctx, 0, s.rng.IntN)
		require.NoError(s.h.t, err)
		require.Equal(s.h.t, buffered, replicated)
	}
}

func (s *randomizedCRRSimulation) requireGlobalCheckpointProgress(previous uint64) uint64 {
	if s.activatePendingStores() {
		// Let the advancer observe subscription topology changes before using the
		// newly activated stores in the expected global checkpoint.
		_, err := s.h.Tick(s.h.ctx)
		require.NoError(s.h.t, err)
	}
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
		if record.CheckpointTS <= previous {
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
		if record.CheckpointTS < checkpoint {
			checkpoint = record.CheckpointTS
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

func (s *randomizedCRRSimulation) samplePartialReplicateCount(
	buffered int,
	rng intNSource,
) int {
	if buffered <= 0 {
		return 0
	}
	if s.cfg.MaxBufferedFiles > 0 && buffered > s.cfg.MaxBufferedFiles {
		return buffered - s.cfg.MaxBufferedFiles
	}
	if !rollPercent(rng, s.cfg.ReplicateChancePercent) {
		return 0
	}
	limit := min(buffered, s.cfg.MaxReplicateBatchPerRound)
	if limit <= 1 {
		return limit
	}
	return 1 + rng.IntN(limit-1)
}

func (s *randomizedCRRSimulation) rollPercent(chancePercent int) bool {
	return rollPercent(s.rng, chancePercent)
}

func (s *randomizedCRRSimulation) rememberSyncedTS() {
	state := s.h.calculator.StateSnapshot()
	if state.SyncedTS > s.lastState.SyncedTS || state.LastCheckpoint > s.lastState.LastCheckpoint {
		s.lastState = state
	}
}

func (s *randomizedCRRSimulation) newConcurrentRNG() intNSource {
	seed := int64(s.rng.IntN(math.MaxInt))
	return &roundRNG{rng: rand.New(rand.NewSource(seed + 1))}
}

func rollPercent(rng intNSource, chancePercent int) bool {
	if chancePercent <= 0 {
		return false
	}
	if chancePercent >= 100 {
		return true
	}
	return rng.IntN(100) < chancePercent
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
