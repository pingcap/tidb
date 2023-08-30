// Copyright 2023 PingCAP, Inc.
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

package lfu

import (
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/ristretto"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle/cache/internal"
	"github.com/pingcap/tidb/statistics/handle/cache/internal/metrics"
	"github.com/pingcap/tidb/util/intest"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/memory"
	"go.uber.org/zap"
)

// LFU is a LFU based on the ristretto.Cache
type LFU struct {
	cache        *ristretto.Cache
	resultKeySet *keySetShard
	cost         atomic.Int64
	closed       atomic.Bool
	closeOnce    sync.Once
}

var testMode = false

// NewLFU creates a new LFU cache.
func NewLFU(totalMemCost int64) (*LFU, error) {
	if totalMemCost == 0 {
		if intest.InTest {
			totalMemCost = 5000000
		} else {
			memTotal, err := memory.MemTotal()
			if err != nil {
				return nil, err
			}
			totalMemCost = int64(memTotal / 2)
		}
	}
	metrics.CapacityGauge.Set(float64(totalMemCost))
	result := &LFU{}
	bufferItems := int64(64)

	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters:        mathutil.Max(totalMemCost/128*2, 10), // assume the cost per table stats is 128
		MaxCost:            totalMemCost,
		BufferItems:        bufferItems,
		OnEvict:            result.onEvict,
		OnExit:             result.onExit,
		OnReject:           result.onReject,
		IgnoreInternalCost: testMode,
		Metrics:            testMode,
	})
	if err != nil {
		return nil, err
	}
	result.cache = cache
	result.resultKeySet = newKeySetShard()
	return result, err
}

// Get implements statsCacheInner
func (s *LFU) Get(tid int64, _ bool) (*statistics.Table, bool) {
	result, ok := s.cache.Get(tid)
	if !ok {
		return s.resultKeySet.Get(tid)
	}
	return result.(*statistics.Table), ok
}

// Put implements statsCacheInner
func (s *LFU) Put(tblID int64, tbl *statistics.Table) bool {
	cost := tbl.MemoryUsage().TotalTrackingMemUsage()
	s.resultKeySet.AddKeyValue(tblID, tbl)
	s.addCost(cost)
	return s.cache.Set(tblID, tbl, cost)
}

// Del implements statsCacheInner
func (s *LFU) Del(tblID int64) {
	s.cache.Del(tblID)
	s.resultKeySet.Remove(tblID)
}

// Cost implements statsCacheInner
func (s *LFU) Cost() int64 {
	return s.cost.Load()
}

// Values implements statsCacheInner
func (s *LFU) Values() []*statistics.Table {
	result := make([]*statistics.Table, 0, 512)
	for _, k := range s.resultKeySet.Keys() {
		if value, ok := s.resultKeySet.Get(k); ok {
			result = append(result, value)
		}
	}
	return result
}

// DropEvicted drop stats for table column/index
func DropEvicted(item statistics.TableCacheItem) {
	if !item.IsStatsInitialized() ||
		item.GetEvictedStatus() == statistics.AllEvicted {
		return
	}
	item.DropUnnecessaryData()
}

func (s *LFU) onReject(item *ristretto.Item) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Warn("panic in onReject", zap.Any("error", r), zap.Stack("stack"))
		}
	}()
	s.dropMemory(item)
	metrics.RejectCounter.Inc()
}

func (s *LFU) onEvict(item *ristretto.Item) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Warn("panic in onEvict", zap.Any("error", r), zap.Stack("stack"))
		}
	}()
	s.dropMemory(item)
	metrics.EvictCounter.Inc()
}

func (s *LFU) dropMemory(item *ristretto.Item) {
	if item.Value == nil {
		// Sometimes the same key may be passed to the "onEvict/onExit"
		// function twice, and in the second invocation, the value is empty,
		// so it should not be processed.
		return
	}
	if s.closed.Load() {
		return
	}
	// We do not need to calculate the cost during onEvict,
	// because the onexit function is also called when the evict event occurs.
	// TODO(hawkingrei): not copy the useless part.
	table := item.Value.(*statistics.Table).Copy()
	for _, column := range table.Columns {
		DropEvicted(column)
	}
	for _, indix := range table.Indices {
		DropEvicted(indix)
	}
	s.resultKeySet.AddKeyValue(int64(item.Key), table)
	after := table.MemoryUsage().TotalTrackingMemUsage()
	// why add before again? because the cost will be subtracted in onExit.
	// in fact, it is after - before
	s.addCost(after)
}

func (s *LFU) onExit(val any) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Warn("panic in onExit", zap.Any("error", r), zap.Stack("stack"))
		}
	}()
	if val == nil {
		// Sometimes the same key may be passed to the "onEvict/onExit" function twice,
		// and in the second invocation, the value is empty, so it should not be processed.
		return
	}
	if s.closed.Load() {
		return
	}
	s.addCost(
		-1 * val.(*statistics.Table).MemoryUsage().TotalTrackingMemUsage())
}

// Len implements statsCacheInner
func (s *LFU) Len() int {
	return s.resultKeySet.Len()
}

// Copy implements statsCacheInner
func (s *LFU) Copy() internal.StatsCacheInner {
	return s
}

// SetCapacity implements statsCacheInner
func (s *LFU) SetCapacity(maxCost int64) {
	s.cache.UpdateMaxCost(maxCost)
	metrics.CapacityGauge.Set(float64(maxCost))
}

// wait blocks until all buffered writes have been applied. This ensures a call to Set()
// will be visible to future calls to Get(). it is only used for test.
func (s *LFU) wait() {
	s.cache.Wait()
}

func (s *LFU) metrics() *ristretto.Metrics {
	return s.cache.Metrics
}

// Close implements statsCacheInner
func (s *LFU) Close() {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.Clear()
		s.cache.Close()
		s.cache.Wait()
	})
}

// Clear implements statsCacheInner
func (s *LFU) Clear() {
	s.cache.Clear()
	s.resultKeySet.Clear()
}

func (s *LFU) addCost(v int64) {
	newv := s.cost.Add(v)
	metrics.CostGauge.Set(float64(newv))
}
