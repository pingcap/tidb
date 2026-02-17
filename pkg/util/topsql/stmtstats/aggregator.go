// Copyright 2021 PingCAP, Inc.
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

package stmtstats

import (
	"context"
	"sync"
	"time"

	reporter_metrics "github.com/pingcap/tidb/pkg/util/topsql/reporter/metrics"
	"github.com/pingcap/tidb/pkg/util/topsql/state"
	"go.uber.org/atomic"
)

const maxStmtStatsSize = 1000000

// maxRUKeysPerAggregate is the hard cap on distinct RU keys per aggregation cycle.
// This implements Session-level backpressure (Phase 2 Decision E).
// Excess keys are dropped early to protect hot paths.
const maxRUKeysPerAggregate = 10000

// globalAggregator is global *aggregator.
var globalAggregator = newAggregator()

// aggregator is used to collect and aggregate data from all StatementStats.
// It is responsible for collecting data from all StatementStats, aggregating
// them together, uploading them and regularly cleaning up the closed StatementStats.
type aggregator struct {
	ctx          context.Context
	cancel       context.CancelFunc
	running      *atomic.Bool
	statsSet     sync.Map // map[*StatementStats]struct{}
	collectors   sync.Map // map[Collector]struct{}
	ruCollectors sync.Map // map[RUCollector]struct{}
	wg           sync.WaitGroup
	statsLen     atomic.Uint32
}

// newAggregator creates an empty aggregator.
func newAggregator() *aggregator {
	return &aggregator{running: atomic.NewBool(false)}
}

func (m *aggregator) start() {
	if m.running.Load() {
		return
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.running.Store(true)
	m.wg.Add(1)
	go m.run()
}

// run will block the current goroutine and execute the main loop of aggregator.
func (m *aggregator) run() {
	// Run order matters: aggregateRU() must run before aggregate() to avoid
	// dropping RU increments from finished sessions.
	tick := time.NewTicker(time.Second)
	defer func() {
		tick.Stop()
		m.wg.Done()
	}()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-tick.C:
			m.aggregateRU()
			m.aggregate()
		}
	}
}

// aggregate collects TopSQL data (CPU + stmt stats) from all associated StatementStats.
// If StatementStats has been closed, collect will remove it from the map.
func (m *aggregator) aggregate() {
	total := StatementStatsMap{}
	m.statsSet.Range(func(statsR, _ any) bool {
		stats := statsR.(*StatementStats)
		if stats.Finished() {
			m.unregister(stats)
		}
		total.Merge(stats.Take())
		return true
	})
	// If TopSQL is not enabled, just drop them.
	if len(total) > 0 && state.TopSQLEnabled() {
		m.collectors.Range(func(c, _ any) bool {
			c.(Collector).CollectStmtStatsMap(total)
			return true
		})
	}
}

// aggregateRU collects RU increment data from all associated StatementStats.
//
// Behavior:
//  1. Iterates all registered StatementStats
//  2. Calls MergeRUInto() to drain RU increments from each session
//  3. Merges all increments into single RUIncrementMap
//  4. Applies hard cap on distinct keys (Phase 2 Decision E - backpressure)
//  5. Pushes to all registered RUCollectors (gated on TopRUEnabled())
func (m *aggregator) aggregateRU() {
	topRUEnabled := state.TopRUEnabled()
	// Always drain RU increments to avoid keeping stale data when TopRU is disabled.
	// Disabled means no RU output (no-op on reporting), while housekeeping drain is still allowed.
	total := RUIncrementMap{}
	var droppedKeys int64
	var droppedRU float64
	m.statsSet.Range(func(statsAny, _ any) bool {
		stats := statsAny.(*StatementStats)
		// No need to check Finished() again - already checked in aggregate()
		sessionRU := stats.MergeRUInto()
		// Phase 2 Decision E: Apply hard cap on distinct RU keys.
		// When approaching the limit, stop merging new keys to protect hot paths.
		for key, incr := range sessionRU {
			// Under capacity - normal merge
			if existing, ok := total[key]; ok {
				existing.Merge(incr)
				continue
			}

			if len(total) >= maxRUKeysPerAggregate {
				// At capacity - only merge into existing keys
				droppedKeys++
				droppedRU += incr.TotalRU
			} else {
				total[key] = incr
			}
		}
		return true
	})

	// Record backpressure metrics outside the hot loop.
	if droppedKeys > 0 {
		reporter_metrics.IgnoreExceedRUKeysCounter.Add(float64(droppedKeys))
		reporter_metrics.IgnoreExceedRUAmountCounter.Add(droppedRU)
	}

	if topRUEnabled && len(total) > 0 {
		m.ruCollectors.Range(func(c, _ any) bool {
			c.(RUCollector).CollectRUIncrements(total)
			return true
		})
	}
}

// register binds StatementStats to aggregator.
// register is thread-safe.
func (m *aggregator) register(stats *StatementStats) {
	if m.statsLen.Load() > maxStmtStatsSize {
		return
	}
	m.statsLen.Inc()
	m.statsSet.Store(stats, struct{}{})
}

// unregister removes StatementStats from aggregator.
// unregister is thread-safe.
func (m *aggregator) unregister(stats *StatementStats) {
	m.statsSet.Delete(stats)
	m.statsLen.Dec()
}

// registerCollector binds a Collector to aggregator.
// registerCollector is thread-safe.
func (m *aggregator) registerCollector(collector Collector) {
	m.collectors.Store(collector, struct{}{})
}

// unregisterCollector removes Collector from aggregator.
// unregisterCollector is thread-safe.
func (m *aggregator) unregisterCollector(collector Collector) {
	m.collectors.Delete(collector)
}

// registerRUCollector binds an RUCollector to aggregator.
// registerRUCollector is thread-safe.
func (m *aggregator) registerRUCollector(collector RUCollector) {
	m.ruCollectors.Store(collector, struct{}{})
}

// unregisterRUCollector removes RUCollector from aggregator.
// unregisterRUCollector is thread-safe.
func (m *aggregator) unregisterRUCollector(collector RUCollector) {
	m.ruCollectors.Delete(collector)
}

// close ends the execution of the current aggregator.
func (m *aggregator) close() {
	if !m.running.Load() {
		return
	}
	if m.cancel != nil {
		m.cancel()
	}
	m.running.Store(false)
	m.wg.Wait()
}

// closed returns whether the aggregator has been closed.
func (m *aggregator) closed() bool {
	return !m.running.Load()
}

// SetupAggregator is used to initialize the background aggregator goroutine of the stmtstats module.
// SetupAggregator is **not** thread-safe.
func SetupAggregator() {
	globalAggregator.start()
}

// CloseAggregator is used to stop the background aggregator goroutine of the stmtstats module.
// SetupAggregator is **not** thread-safe.
func CloseAggregator() {
	globalAggregator.close()
}

// RegisterCollector binds a Collector to globalAggregator.
// RegisterCollector is thread-safe.
func RegisterCollector(collector Collector) {
	globalAggregator.registerCollector(collector)
}

// UnregisterCollector removes Collector from globalAggregator.
// UnregisterCollector is thread-safe.
func UnregisterCollector(collector Collector) {
	globalAggregator.unregisterCollector(collector)
}

// RegisterRUCollector binds an RUCollector to globalAggregator.
// Called at TopSQL startup to wire reporter into RU data flow.
// RegisterRUCollector is thread-safe.
func RegisterRUCollector(collector RUCollector) {
	globalAggregator.registerRUCollector(collector)
}

// UnregisterRUCollector removes RUCollector from globalAggregator.
// UnregisterRUCollector is thread-safe.
func UnregisterRUCollector(collector RUCollector) {
	globalAggregator.unregisterRUCollector(collector)
}

// Collector is used to collect StatementStatsMap.
type Collector interface {
	// CollectStmtStatsMap is used to collect StatementStatsMap.
	CollectStmtStatsMap(StatementStatsMap)
}

// RUCollector is used to collect RU increment data.
// This interface is parallel to Collector but handles TopRU data flow.
//
// Design Rationale:
//   - Separate interface from Collector to maintain TopSQL/TopRU independence
//   - Single method design matches Collector pattern
//   - Reporter implements this interface to receive aggregated RU data
//
// Data Flow:
//
//	aggregator (1s tick) -> RUCollector.CollectRUIncrements()
//	-> Reporter.collectRUIncrementsChan -> collectWorker -> ruCollecting
//
// Phase 2: Reporter applies Hybrid TopN filtering (200 users × 200 SQLs) in ruCollecting.
type RUCollector interface {
	// CollectRUIncrements is called by aggregator every 1s with merged RU deltas
	// from all sessions, aggregated by (user, sql_digest, plan_digest).
	CollectRUIncrements(RUIncrementMap)
}
