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
			m.aggregateAll()
		}
	}
}

// aggregateAll performs a single tick of data collection. It calls drainAndPushRU
// first, then drainAndPushStmtStats. The ordering matters: RU must be drained
// before stmt stats to avoid losing RU deltas from sessions that become Finished()
// and get unregistered during the stmt stats phase.
func (m *aggregator) aggregateAll() {
	m.drainAndPushRU()
	m.drainAndPushStmtStats()
}

// drainAndPushStmtStats collects TopSQL data (CPU + stmt stats) from all
// associated StatementStats. Finished sessions are unregistered here.
func (m *aggregator) drainAndPushStmtStats() {
	total := StatementStatsMap{}
	m.statsSet.Range(func(statsR, _ any) bool {
		stats := statsR.(*StatementStats)
		if stats.Finished() {
			m.unregister(stats)
		}
		total.Merge(stats.Take())
		return true
	})
	if len(total) > 0 && state.TopSQLEnabled() {
		m.collectors.Range(func(c, _ any) bool {
			c.(Collector).CollectStmtStatsMap(total)
			return true
		})
	}
}

// drainAndPushRU drains RU increments from all sessions, applies key caps, and
// pushes merged data to RUCollectors when TopRU is enabled.
func (m *aggregator) drainAndPushRU() {
	total := RUIncrementMap{}
	var droppedKeys int64
	var droppedRU float64
	m.statsSet.Range(func(statsAny, _ any) bool {
		stats := statsAny.(*StatementStats)
		sessionRU := stats.MergeRUInto()
		for key, incr := range sessionRU {
			if existing, ok := total[key]; ok {
				existing.Merge(incr)
				continue
			}
			if len(total) >= maxRUKeysPerAggregate {
				droppedKeys++
				droppedRU += incr.TotalRU
			} else {
				total[key] = incr
			}
		}
		return true
	})

	if droppedKeys > 0 {
		reporter_metrics.IgnoreExceedRUKeysCounter.Add(float64(droppedKeys))
		reporter_metrics.IgnoreExceedRUTotalCounter.Add(droppedRU)
	}

	if state.TopRUEnabled() && len(total) > 0 {
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

// Collector is used to collect StatementStatsMap.
type Collector interface {
	// CollectStmtStatsMap is used to collect StatementStatsMap.
	CollectStmtStatsMap(StatementStatsMap)
}

// RUCollector collects RU increments for the TopRU pipeline.
// It is separate from Collector to keep TopSQL and TopRU decoupled.
type RUCollector interface {
	// CollectRUIncrements is called by aggregator every 1s with merged RU deltas
	// from all sessions, aggregated by (user, sql_digest, plan_digest).
	CollectRUIncrements(RUIncrementMap)
}
