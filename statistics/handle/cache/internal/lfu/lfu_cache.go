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
	"sync/atomic"

	"github.com/dgraph-io/ristretto"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle/cache/internal"
	"github.com/pingcap/tidb/statistics/handle/cache/internal/metrics"
	"github.com/pingcap/tidb/util/intest"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/memory"
)

// LFU is a LFU based on the ristretto.Cache
type LFU struct {
	l0cache *ristretto.Cache
	l1cache *ristretto.Cache
	l2cache *ristretto.Cache

	resultKeySet *keySetShard
	cost         atomic.Int64
}

// NewLFU creates a new LFU cache.
func NewLFU(totalMemCost int64) (*LFU, error) {
	if totalMemCost == 0 {
		memTotal, err := memory.MemTotal()
		if err != nil {
			return nil, err
		}
		totalMemCost = int64(memTotal / 2)
	}
	metrics.CapacityGauge.Set(float64(totalMemCost))
	result := &LFU{}
	bufferItems := int64(64)
	if intest.InTest {
		bufferItems = 1
	}

	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters:        mathutil.Max(totalMemCost/128*2, 10), // assume the cost per table stats is 128
		MaxCost:            totalMemCost,
		BufferItems:        bufferItems,
		OnEvict:            result.onEvict,
		OnExit:             result.onExit,
		IgnoreInternalCost: intest.InTest,
		Metrics:            intest.InTest,
	})
	if err != nil {
		return nil, err
	}
	result.l0cache = cache
	l1cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters:        mathutil.Max(totalMemCost/128*2, 10), // assume the cost per table stats is 128
		MaxCost:            totalMemCost,
		BufferItems:        bufferItems,
		OnEvict:            result.onEvict,
		OnExit:             result.onExit,
		IgnoreInternalCost: intest.InTest,
		Metrics:            intest.InTest,
	})
	if err != nil {
		return nil, err
	}
	result.l1cache = l1cache
	l2cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters:        mathutil.Max(totalMemCost/128*2, 10), // assume the cost per table stats is 128
		MaxCost:            totalMemCost,
		BufferItems:        bufferItems,
		OnEvict:            result.onEvict,
		OnExit:             result.onExit,
		IgnoreInternalCost: intest.InTest,
		Metrics:            intest.InTest,
	})
	if err != nil {
		return nil, err
	}
	result.l2cache = l2cache
	result.resultKeySet = newKeySetShard()
	return result, err
}

// Get implements statsCacheInner
func (s *LFU) Get(tid int64, _ bool) (*statistics.Table, bool) {
	result, ok := s.l0cache.Get(tid)
	if !ok {
		return nil, ok
	}
	return result.(*statistics.Table), ok
}

// Put implements statsCacheInner
func (s *LFU) Put(tblID int64, tbl *statistics.Table) bool {
	ok := s.l0cache.Set(tblID, tbl, tbl.MemoryUsage().TotalTrackingMemUsage())
	if ok { // NOTE: `s.cache` and `s.resultKeySet` may be inconsistent since the update operation is not atomic, but it's acceptable for our scenario
		s.resultKeySet.Add(tblID)
		s.cost.Add(tbl.MemoryUsage().TotalTrackingMemUsage())
		metrics.CostGauge.Set(float64(s.cost.Load()))
	}
	return ok
}

// Del implements statsCacheInner
func (s *LFU) Del(tblID int64) {
	s.l0cache.Del(tblID)
	s.l1cache.Del(tblID)
	s.l2cache.Del(tblID)
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
		if value, ok := s.l0cache.Get(k); ok {
			result = append(result, value.(*statistics.Table))
		}
	}
	return result
}

func (s *LFU) onEvict(item *ristretto.Item) {
	// We do not need to calculate the cost during onEvict, because the onexit function
	// is also called when the evict event occurs.
	metrics.EvictCounter.Inc()
	table := item.Value.(*statistics.Table)
	var getEvictedStatus int
	for _, column := range table.Columns {
		statistics.DropEvicted(column)
		getEvictedStatus = column.GetEvictedStatus()
	}
	for _, indix := range table.Indices {
		statistics.DropEvicted(indix)
		getEvictedStatus = indix.GetEvictedStatus()
	}
	switch getEvictedStatus {
	case statistics.AllLoaded:
		panic("unreachable")
	case statistics.OnlyCmsEvicted:
		s.l1cache.Set(item.Key, item.Value, item.Cost)
	case statistics.OnlyHistRemained:
		s.l2cache.Set(item.Key, item.Value, item.Cost)
	case statistics.AllEvicted:
		s.resultKeySet.AddKeyValue(int64(item.Key), item.Value.(*statistics.Table))
		s.cost.Add(item.Value.(*statistics.Table).MemoryUsage().TotalTrackingMemUsage())
	}
}

func (s *LFU) onExit(val interface{}) {
	s.cost.Add(-1 * val.(*statistics.Table).MemoryUsage().TotalTrackingMemUsage())
	metrics.CostGauge.Set(float64(s.cost.Load()))
}

// Len implements statsCacheInner
func (s *LFU) Len() int {
	return s.resultKeySet.Len()
}

// Front implements statsCacheInner
func (*LFU) Front() int64 {
	return 0
}

// Copy implements statsCacheInner
func (s *LFU) Copy() internal.StatsCacheInner {
	return s
}

// SetCapacity implements statsCacheInner
func (s *LFU) SetCapacity(maxCost int64) {
	s.l0cache.UpdateMaxCost(maxCost)
	metrics.CapacityGauge.Set(float64(maxCost))
}

// wait blocks until all buffered writes have been applied. This ensures a call to Set()
// will be visible to future calls to Get(). it is only used for test.
func (s *LFU) wait() {
	s.l0cache.Wait()
	s.l1cache.Wait()
	s.l2cache.Wait()
}

func (s *LFU) metrics() *ristretto.Metrics {
	return s.l0cache.Metrics
}

// Close implements statsCacheInner
func (s *LFU) Close() {
	s.l0cache.Close()
	s.l1cache.Close()
	s.l2cache.Close()
	s.l0cache.Wait()
	s.l1cache.Wait()
	s.l2cache.Wait()
}
