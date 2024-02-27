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
	"github.com/dgraph-io/ristretto"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache/internal"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache/internal/metrics"
	"github.com/pingcap/tidb/pkg/util/lfu"
)

// LFU is a LFU based on the ristretto.Cache
type LFU struct {
	cache *lfu.LFU[int64, *statistics.Table]
}

// NewLFU creates a new LFU cache.
func NewLFU(totalMemCost int64) (*LFU, error) {
	cache, err := lfu.NewLFU[int64, *statistics.Table](totalMemCost, DropEvicted, metrics.CapacityGauge)
	if err != nil {
		return nil, err
	}
	cache.RegisterMissCounter(metrics.MissCounter)
	cache.RegisterHitCounter(metrics.HitCounter)
	cache.RegisterUpdateCounter(metrics.UpdateCounter)
	cache.RegisterDelCounter(metrics.DelCounter)
	cache.RegisterEvictCounter(metrics.EvictCounter)
	cache.RegisterRejectCounter(metrics.RejectCounter)
	cache.RegisterCostGauge(metrics.CostGauge)
	return &LFU{
		cache: cache,
	}, nil
}

// Get implements statsCacheInner
func (s *LFU) Get(tid int64) (*statistics.Table, bool) {
	return s.cache.Get(tid)
}

// Put implements statsCacheInner
func (s *LFU) Put(tblID int64, tbl *statistics.Table) bool {
	return s.cache.Put(tblID, tbl)
}

// Del implements statsCacheInner
func (s *LFU) Del(tblID int64) {
	s.cache.Del(tblID)
}

// Cost implements statsCacheInner
func (s *LFU) Cost() int64 {
	return s.cache.Cost()
}

// Values implements statsCacheInner
func (s *LFU) Values() []*statistics.Table {
	return s.Values()
}

// DropEvicted drop stats for table column/index
func DropEvicted(table any) {
	t := table.(*statistics.Table)
	for _, column := range t.Columns {
		dropEvicted(column)
	}
	for _, indix := range t.Indices {
		dropEvicted(indix)
	}

}

// dropEvicted drop stats for table column/index
func dropEvicted(item statistics.TableCacheItem) {
	if !item.IsStatsInitialized() ||
		item.GetEvictedStatus() == statistics.AllEvicted {
		return
	}
	item.DropUnnecessaryData()
}

// Len implements statsCacheInner
func (s *LFU) Len() int {
	return s.cache.Len()
}

// Copy implements statsCacheInner
func (s *LFU) Copy() internal.StatsCacheInner {
	cache := s.cache.Copy()
	return &LFU{cache: cache}
}

// SetCapacity implements statsCacheInner
func (s *LFU) SetCapacity(maxCost int64) {
	s.cache.SetCapacity(maxCost)
}

// wait blocks until all buffered writes have been applied. This ensures a call to Set()
// will be visible to future calls to Get(). it is only used for test.
func (s *LFU) wait() {
	s.cache.Wait()
}

func (s *LFU) metrics() *ristretto.Metrics {
	return s.cache.Metrics()
}

// Close implements statsCacheInner
func (s *LFU) Close() {
	s.cache.Close()
}

// Clear implements statsCacheInner
func (s *LFU) Clear() {
	s.cache.Clear()
}
