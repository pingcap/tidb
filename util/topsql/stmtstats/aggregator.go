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
	"sync/atomic"
	"time"
)

// globalAggregator is global *aggregator.
var globalAggregator atomic.Value

// StatementStatsRecord is the merged StatementStatsMap with timestamp.
type StatementStatsRecord struct {
	Timestamp int64
	Data      StatementStatsMap
}

// aggregator is used to collect and aggregate data from all StatementStats.
// It is responsible for collecting data from all StatementStats, aggregating
// them together, uploading them and regularly cleaning up the closed StatementStats.
type aggregator struct {
	ctx         context.Context
	cancel      context.CancelFunc
	statsSet    sync.Map // map[*StatementStats]struct{}
	collectors  sync.Map // map[uint64]Collector
	collectorId uint64
	running     int32
}

// newAggregator creates an empty aggregator.
func newAggregator() *aggregator {
	return &aggregator{}
}

// run will block the current goroutine and execute the main loop of aggregator.
func (m *aggregator) run() {
	m.ctx, m.cancel = context.WithCancel(context.Background())
	atomic.StoreInt32(&m.running, 1)
	defer func() {
		atomic.StoreInt32(&m.running, 0)
	}()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
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
	r := StatementStatsRecord{
		Timestamp: time.Now().Unix(),
		Data:      StatementStatsMap{},
	}
	m.statsSet.Range(func(statsR, _ interface{}) bool {
		stats := statsR.(*StatementStats)
		if stats.Finished() {
			m.unregister(stats)
		}
		r.Data.Merge(stats.Take())
		return true
	})
	m.collectors.Range(func(_, c interface{}) bool {
		c.(Collector).CollectStmtStatsRecords([]StatementStatsRecord{r})
		return true
	})
}

// register binds StatementStats to aggregator.
// register is thread-safe.
func (m *aggregator) register(stats *StatementStats) {
	m.statsSet.Store(stats, struct{}{})
}

// unregister removes StatementStats from aggregator.
// unregister is thread-safe.
func (m *aggregator) unregister(stats *StatementStats) {
	m.statsSet.Delete(stats)
}

// registerCollector binds a Collector to aggregator, collector ID will be returned.
func (m *aggregator) registerCollector(collector Collector) uint64 {
	id := atomic.AddUint64(&m.collectorId, 1)
	m.collectors.Store(id, collector)
	return id
}

// unregisterCollector removes Collector from aggregator by collector ID.
func (m *aggregator) unregisterCollector(id uint64) {
	m.collectors.Delete(id)
}

// close ends the execution of the current aggregator.
func (m *aggregator) close() {
	m.cancel()
}

// closed returns whether the aggregator has been closed.
func (m *aggregator) closed() bool {
	return atomic.LoadInt32(&m.running) == 0
}

// SetupAggregator is used to initialize the background aggregator goroutine of the stmtstats module.
func SetupAggregator() {
	if v := globalAggregator.Load(); v != nil {
		if c, ok := v.(*aggregator); ok && c != nil {
			if c.closed() {
				go c.run()
			}
			return
		}
	}
	c := newAggregator()
	go c.run()
	globalAggregator.Store(c)
}

// CloseAggregator is used to stop the background aggregator goroutine of the stmtstats module.
func CloseAggregator() {
	if v := globalAggregator.Load(); v != nil {
		if c, ok := v.(*aggregator); ok && c != nil && !c.closed() {
			c.close()
		}
	}
}

// RegisterCollector binds a Collector to globalAggregator, collector ID will be returned.
// RegisterCollector is thread-safe.
func RegisterCollector(collector Collector) uint64 {
	if v := globalAggregator.Load(); v != nil {
		if c, ok := v.(*aggregator); ok && c != nil {
			return c.registerCollector(collector)
		}
	}
	return 0
}

// UnregisterCollector removes Collector from globalAggregator by collector ID.
// UnregisterCollector is thread-safe.
func UnregisterCollector(id uint64) {
	if v := globalAggregator.Load(); v != nil {
		if c, ok := v.(*aggregator); ok && c != nil {
			c.unregisterCollector(id)
		}
	}
}

// Collector is used to collect StatementStatsRecord.
type Collector interface {
	// CollectStmtStatsRecords is used to collect list of StatementStatsRecord.
	CollectStmtStatsRecords([]StatementStatsRecord)
}

// CollectorFunc implements Collector, in order to facilitate the user to
// write callback function directly.
type CollectorFunc func([]StatementStatsRecord)

// CollectStmtStatsRecords implements Collector.CollectStmtStatsRecords.
func (f CollectorFunc) CollectStmtStatsRecords(records []StatementStatsRecord) {
	f(records)
}
