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
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/exp/rand"
)

// LFU is a LFU based on the ristretto.Cache
type LFU[k Key, v Value] struct {
	cache *ristretto.Cache
	// This is a secondary cache layer used to store all tables,
	// including those that have been evicted from the primary cache.
	resultKeySet *keySetShard[k, v]
	cost         atomic.Int64
	closed       atomic.Bool
	closeOnce    sync.Once

	// dropEvicted is to evict useless part of the table when to evict.
	dropEvicted func(any)

	// missCounter is the counter of missing cache.
	missCounter prometheus.Counter
	// hitCounter is the counter of hitting cache.
	hitCounter prometheus.Counter
	// updateCounter is the counter of updating cache.
	updateCounter prometheus.Counter
	// delCounter is the counter of deleting cache.
	delCounter prometheus.Counter
	// evictCounter is the counter of evicting cache.
	evictCounter prometheus.Counter
	// rejectCounter is the counter of reject cache.
	rejectCounter prometheus.Counter
	// costGauge is the gauge of cost time.
	costGauge prometheus.Gauge
	// capacityGauge is the gauge of capacity.
	capacityGauge prometheus.Gauge
}

// NewLFU creates a new LFU cache.
func NewLFU[k Key, v Value](totalMemCost int64, dropEvicted func(any), capacityGauge prometheus.Gauge) (*LFU[k, v], error) {
	cost, err := adjustMemCost(totalMemCost)
	if err != nil {
		return nil, err
	}
	if intest.InTest && totalMemCost == 0 {
		// In test, we set the cost to 5MB to avoid using too many memory in the LFU's CM sketch.
		cost = 5000000
	}
	capacityGauge.Set(float64(cost))
	result := &LFU[k, v]{}
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
	result.dropEvicted = dropEvicted
	result.capacityGauge = capacityGauge
	result.resultKeySet = newKeySetShard[k, v]()
	return result, err
}

// adjustMemCost adjusts the memory cost according to the total memory cost.
// When the total memory cost is 0, the memory cost is set to half of the total memory.
func adjustMemCost(totalMemCost int64) (result int64, err error) {
	if totalMemCost == 0 {
		memTotal, err := memory.MemTotal()
		if err != nil {
			return 0, err
		}
		return int64(memTotal / 2), nil
	}
	return totalMemCost, nil
}

// Get implements statsCacheInner
func (s *LFU[K, V]) Get(tid K) (V, bool) {
	result, ok := s.cache.Get(tid)
	if !ok {
		return s.resultKeySet.Get(tid)
	}
	return result.(V), ok
}

// Put implements statsCacheInner
func (s *LFU[K, V]) Put(tblID K, tbl V) bool {
	cost := tbl.TotalTrackingMemUsage()
	s.resultKeySet.AddKeyValue(tblID, tbl)
	s.addCost(cost)
	return s.cache.Set(tblID, tbl, cost)
}

// Del implements statsCacheInner
func (s *LFU[K, V]) Del(tblID K) {
	s.cache.Del(tblID)
	s.resultKeySet.Remove(tblID)
}

// Cost implements statsCacheInner
func (s *LFU[K, V]) Cost() int64 {
	return s.cost.Load()
}

// Values implements statsCacheInner
func (s *LFU[K, V]) Values() []V {
	result := make([]V, 0, 512)
	for _, k := range s.resultKeySet.Keys() {
		if value, ok := s.resultKeySet.Get(k); ok {
			result = append(result, value)
		}
	}
	return result
}

func (s *LFU[K, V]) onReject(item *ristretto.Item) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Warn("panic in onReject", zap.Any("error", r), zap.Stack("stack"))
		}
	}()
	s.dropMemory(item)
	s.rejectCounter.Inc()
}

func (s *LFU[K, V]) onEvict(item *ristretto.Item) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Warn("panic in onEvict", zap.Any("error", r), zap.Stack("stack"))
		}
	}()
	s.dropMemory(item)
	s.evictCounter.Inc()
}

func (s *LFU[K, V]) dropMemory(item *ristretto.Item) {
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
	table := item.Value.(V).DeepCopy().(V)
	s.dropEvicted(table)
	s.resultKeySet.AddKeyValue(K(item.Key), table)
	after := table.TotalTrackingMemUsage()
	// why add before again? because the cost will be subtracted in onExit.
	// in fact, it is after - before
	s.addCost(after)
	s.triggerEvict()
}

func (s *LFU[K, V]) triggerEvict() {
	// When the memory usage of the cache exceeds the maximum value, Many item need to evict. But
	// ristretto'c cache execute the evict operation when to write the cache. for we can evict as soon as possible,
	// we will write some fake item to the cache. fake item have a negative key, and the value is nil.
	if s.Cost() > s.cache.MaxCost() {
		//nolint: gosec
		s.cache.Set(-rand.Int(), nil, 0)
	}
}

func (s *LFU[K, V]) onExit(val any) {
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
	// Subtract the memory usage of the table from the total memory usage.
	s.addCost(-val.(V).TotalTrackingMemUsage())
}

// Len implements statsCacheInner
func (s *LFU[K, V]) Len() int {
	return s.resultKeySet.Len()
}

// Copy implements statsCacheInner
func (s *LFU[K, V]) Copy() *LFU[K, V] {
	return s
}

// SetCapacity implements statsCacheInner
func (s *LFU[K, V]) SetCapacity(maxCost int64) {
	cost, err := adjustMemCost(maxCost)
	if err != nil {
		logutil.BgLogger().Warn("adjustMemCost failed", zap.Error(err))
		return
	}
	s.cache.UpdateMaxCost(cost)
	s.triggerEvict()
	s.capacityGauge.Set(float64(cost))
	s.costGauge.Set(float64(s.Cost()))
}

// Wait blocks until all buffered writes have been applied. This ensures a call to Set()
// will be visible to future calls to Get(). it is only used for test.
func (s *LFU[K, V]) Wait() {
	s.cache.Wait()
}

// Metrics is to get metrics. It is only used for test.
func (s *LFU[K, V]) Metrics() *ristretto.Metrics {
	return s.cache.Metrics
}

// Close implements statsCacheInner
func (s *LFU[K, V]) Close() {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.Clear()
		s.cache.Close()
		s.cache.Wait()
	})
}

// Clear implements statsCacheInner
func (s *LFU[K, V]) Clear() {
	s.cache.Clear()
	s.resultKeySet.Clear()
}

func (s *LFU[K, V]) addCost(v int64) {
	newv := s.cost.Add(v)
	s.costGauge.Set(float64(newv))
}

// RegisterMissCounter register MissCounter
func (s *LFU[K, V]) RegisterMissCounter(c prometheus.Counter) {
	s.missCounter = c
}

// RegisterHitCounter register HitCounter
func (s *LFU[K, V]) RegisterHitCounter(c prometheus.Counter) {
	s.hitCounter = c
}

// RegisterUpdateCounter register UpdateCounter
func (s *LFU[K, V]) RegisterUpdateCounter(c prometheus.Counter) {
	s.updateCounter = c
}

// RegisterDelCounter register DelCounter
func (s *LFU[K, V]) RegisterDelCounter(c prometheus.Counter) {
	s.delCounter = c
}

// RegisterEvictCounter register EvictCounter
func (s *LFU[K, V]) RegisterEvictCounter(c prometheus.Counter) {
	s.evictCounter = c
}

// RegisterRejectCounter register RejectCounter
func (s *LFU[K, V]) RegisterRejectCounter(c prometheus.Counter) {
	s.rejectCounter = c
}

// RegisterCostGauge register CostGauge
func (s *LFU[K, V]) RegisterCostGauge(g prometheus.Gauge) {
	s.costGauge = g
}
