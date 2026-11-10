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
	"math/rand/v2"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/ristretto"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache/internal"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache/metrics"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

// LFU is a LFU based on the ristretto.Cache.
//
// NOTE: In Ristretto, updates to keys already resident in the primary store become visible
// immediately, but keys not currently resident go through a buffered admission path and are
// applied by a background worker. So a successful Set does not guarantee immediate Get
// visibility for a non-resident key.
type LFU struct {
	cache *ristretto.Cache
	// This is a secondary cache layer used to store all tables,
	// including those that have been evicted from the primary cache.
	resultKeySet *keySetShard
	cost         atomic.Int64
	closed       atomic.Bool
	closeOnce    sync.Once
}

// NewLFU creates a new LFU cache.
func NewLFU(totalMemCost int64) (*LFU, error) {
	cost, err := adjustMemCost(totalMemCost)
	if err != nil {
		return nil, err
	}
	if intest.InTest && totalMemCost == 0 {
		// In test, we set the cost to 5MB to avoid using too many memory in the LFU's CM sketch.
		cost = 5000000
	}
	metrics.CapacityGauge.Set(float64(cost))
	result := &LFU{}
	bufferItems := int64(64)

	cache, err := ristretto.NewCache(
		&ristretto.Config{
			NumCounters:        max(min(cost/128, 1_000_000), 10), // assume the cost per table stats is 128
			MaxCost:            cost,
			BufferItems:        bufferItems,
			OnEvict:            result.onEvict,
			OnExit:             result.onExit,
			OnReject:           result.onReject,
			IgnoreInternalCost: intest.InTest,
			Metrics:            intest.InTest,
		},
	)
	if err != nil {
		return nil, err
	}
	result.cache = cache
	result.resultKeySet = newKeySetShard()
	return result, err
}

// adjustMemCost adjusts the memory cost according to the total memory cost.
// When the total memory cost is 0, the memory cost is set to 20% of the total memory.
func adjustMemCost(totalMemCost int64) (result int64, err error) {
	if totalMemCost == 0 {
		memTotal, err := memory.MemTotal()
		if err != nil {
			return 0, err
		}
		return int64(memTotal * 20 / 100), nil
	}
	return totalMemCost, nil
}

// Get reads the primary Ristretto cache first and falls back to resultKeySet.
//
// NOTE: We keep this order to preserve Ristretto's admission/frequency behavior.
// That means a resident primary-cache entry wins even if resultKeySet has already been updated to
// a newer table snapshot; until the buffered Ristretto write catches up, Get can still observe
// that older snapshot. We are aware of this stale-read window, but it is very rare in practice, so
// we currently document it instead of changing the read order and taking on different concurrency
// trade-offs.
//
// Extreme example (the old flaky pattern from TestNonLiteInitStatsWithTableIDs):
//  1. InitStats(tbl1) publishes tbl1 to resultKeySet, but the new primary-cache entry is still in
//     Ristretto's buffered admission path.
//  2. A later targeted InitStats call that includes tbl1 again reads tbl1 from resultKeySet,
//     fills in more index state, and republishes a newer tbl1 snapshot there.
//  3. The older tbl1 can still reach Ristretto first, so topn/buckets sees that resident older
//     snapshot, misses the index entry, and Put() can write that stale tbl1 back into the main
//     cache.
//
// For phase-by-phase or chunk-by-chunk cache construction, callers that read after writes should
// call WaitForAsyncUpdates before the next dependent read. InitStats does this between load phases
// and chunks so a temporary LFU cache cannot carry an older table snapshot into later loading.
func (s *LFU) Get(tid int64) (*statistics.Table, bool) {
	result, ok := s.cache.Get(tid)
	if !ok {
		return s.resultKeySet.Get(tid)
	}
	return result.(*statistics.Table), ok
}

// Put publishes to resultKeySet first, then hands the item to Ristretto.
//
// Resident-key updates become visible there immediately; non-resident keys still go through the
// buffered admission path.
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
	table := item.Value.(*statistics.Table).CopyAs(statistics.AllDataWritable)
	table.DropEvicted()
	s.resultKeySet.AddKeyValue(int64(item.Key), table)
	after := table.MemoryUsage().TotalTrackingMemUsage()
	// why add before again? because the cost will be subtracted in onExit.
	// in fact, it is after - before
	s.addCost(after)
	s.triggerEvict()
}

func (s *LFU) triggerEvict() {
	// When the memory usage of the cache exceeds the maximum value, Many item need to evict. But
	// ristretto'c cache execute the evict operation when to write the cache. for we can evict as soon as possible,
	// we will write some fake item to the cache. fake item have a negative key, and the value is nil.
	if s.Cost() > s.cache.MaxCost() {
		//nolint: gosec
		s.cache.Set(-rand.Int(), nil, 0)
	}
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
	s.triggerEvict()
	// Subtract the memory usage of the table from the total memory usage.
	s.addCost(-val.(*statistics.Table).MemoryUsage().TotalTrackingMemUsage())
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
	cost, err := adjustMemCost(maxCost)
	if err != nil {
		logutil.BgLogger().Warn("adjustMemCost failed", zap.Error(err))
		return
	}
	s.cache.UpdateMaxCost(cost)
	s.triggerEvict()
	metrics.CapacityGauge.Set(float64(cost))
	metrics.CostGauge.Set(float64(s.Cost()))
}

// WaitForAsyncUpdates blocks until all buffered writes have been applied. This ensures a call to
// Put is visible to future calls to Get.
func (s *LFU) WaitForAsyncUpdates() {
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

// TriggerEvict implements statsCacheInner
func (s *LFU) TriggerEvict() {
	s.triggerEvict()
}
