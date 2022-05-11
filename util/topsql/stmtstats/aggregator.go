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

	"github.com/pingcap/tidb/util/topsql/state"
	"go.uber.org/atomic"
)

const maxStmtStatsSize = 1000000

// globalAggregator is global *aggregator.
var globalAggregator = newAggregator()

// aggregator is used to collect and aggregate data from all StatementStats.
// It is responsible for collecting data from all StatementStats, aggregating
// them together, uploading them and regularly cleaning up the closed StatementStats.
type aggregator struct {
	ctx        context.Context
	cancel     context.CancelFunc
	statsLen   atomic.Uint32
	statsSet   sync.Map // map[*StatementStats]struct{}
	collectors sync.Map // map[Collector]struct{}
	running    *atomic.Bool
	wg         sync.WaitGroup
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
			m.aggregate()
		}
	}
}

// aggregate data from all associated StatementStats.
// If StatementStats has been closed, collect will remove it from the map.
func (m *aggregator) aggregate() {
	total := StatementStatsMap{}
	m.statsSet.Range(func(statsR, _ interface{}) bool {
		stats := statsR.(*StatementStats)
		if stats.Finished() {
			m.unregister(stats)
		}
		total.Merge(stats.Take())
		return true
	})
	// If TopSQL is not enabled, just drop them.
	if len(total) > 0 && state.TopSQLEnabled() {
		m.collectors.Range(func(c, _ interface{}) bool {
			c.(Collector).CollectStmtStatsMap(total)
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
