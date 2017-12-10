// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedule

import (
	"time"

	//log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	//"github.com/pingcap/kvproto/pkg/metapb"
	//"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/cache"
	"github.com/pingcap/pd/server/core"
)

var (
	// HotRegionLowThreshold is the low threadshold of hot region
	HotRegionLowThreshold = 3
)

const (
	// RegionHeartBeatReportInterval is the heartbeat report interval of a region
	RegionHeartBeatReportInterval = 60

	statCacheMaxLen              = 1000
	hotWriteRegionMinFlowRate    = 16 * 1024
	hotReadRegionMinFlowRate     = 128 * 1024
	storeHeartBeatReportInterval = 10
	minHotRegionReportInterval   = 3
	hotRegionAntiCount           = 1
)

// BasicCluster provides basic data member and interface for a tikv cluster.
type BasicCluster struct {
	Stores          *core.StoresInfo
	Regions         *core.RegionsInfo
	WriteStatistics cache.Cache
	ReadStatistics  cache.Cache
}

// NewOpInfluence creates a OpInfluence.
func NewOpInfluence(operators []*Operator, cluster Cluster) OpInfluence {
	m := make(map[uint64]*StoreInfluence)

	for _, op := range operators {
		if !op.IsTimeout() && !op.IsFinish() {
			op.Influence(m, cluster.GetRegion(op.RegionID()))
		}
	}

	return m
}

// OpInfluence is a map of StoreInfluence.
type OpInfluence map[uint64]*StoreInfluence

// GetStoreInfluence get storeInfluence of specific store.
func (m OpInfluence) GetStoreInfluence(id uint64) *StoreInfluence {
	storeInfluence, ok := m[id]
	if !ok {
		storeInfluence = &StoreInfluence{}
		m[id] = storeInfluence
	}
	return storeInfluence
}

// StoreInfluence records influences that pending operators will make.
type StoreInfluence struct {
	RegionSize  int
	RegionCount int
	LeaderSize  int
	LeaderCount int
}

// NewBasicCluster creates a BasicCluster.
func NewBasicCluster() *BasicCluster {
	return &BasicCluster{
		Stores:          core.NewStoresInfo(),
		Regions:         core.NewRegionsInfo(),
		ReadStatistics:  cache.NewCache(statCacheMaxLen, cache.TwoQueueCache),
		WriteStatistics: cache.NewCache(statCacheMaxLen, cache.TwoQueueCache),
	}
}

// GetStores returns all Stores in the cluster.
func (bc *BasicCluster) GetStores() []*core.StoreInfo {
	return bc.Stores.GetStores()
}

// GetStore searches for a store by ID.
func (bc *BasicCluster) GetStore(storeID uint64) *core.StoreInfo {
	return bc.Stores.GetStore(storeID)
}

// GetRegion searches for a region by ID.
func (bc *BasicCluster) GetRegion(regionID uint64) *core.RegionInfo {
	return bc.Regions.GetRegion(regionID)
}

// GetRegionStores returns all Stores that contains the region's peer.
func (bc *BasicCluster) GetRegionStores(region *core.RegionInfo) []*core.StoreInfo {
	var Stores []*core.StoreInfo
	for id := range region.GetStoreIds() {
		if store := bc.Stores.GetStore(id); store != nil {
			Stores = append(Stores, store)
		}
	}
	return Stores
}

// GetFollowerStores returns all Stores that contains the region's follower peer.
func (bc *BasicCluster) GetFollowerStores(region *core.RegionInfo) []*core.StoreInfo {
	var Stores []*core.StoreInfo
	for id := range region.GetFollowers() {
		if store := bc.Stores.GetStore(id); store != nil {
			Stores = append(Stores, store)
		}
	}
	return Stores
}

// GetLeaderStore returns all Stores that contains the region's leader peer.
func (bc *BasicCluster) GetLeaderStore(region *core.RegionInfo) *core.StoreInfo {
	return bc.Stores.GetStore(region.Leader.GetStoreId())
}

// GetStoresAverageScore returns the total resource score of all StoreInfo
func (bc *BasicCluster) GetStoresAverageScore(kind core.ResourceKind) float64 {
	return bc.Stores.AverageResourceScore(kind)
}

// BlockStore stops balancer from selecting the store.
func (bc *BasicCluster) BlockStore(storeID uint64) error {
	return errors.Trace(bc.Stores.BlockStore(storeID))
}

// UnblockStore allows balancer to select the store.
func (bc *BasicCluster) UnblockStore(storeID uint64) {
	bc.Stores.UnblockStore(storeID)
}

// RandFollowerRegion returns a random region that has a follower on the store.
func (bc *BasicCluster) RandFollowerRegion(storeID uint64) *core.RegionInfo {
	return bc.Regions.RandFollowerRegion(storeID)
}

// RandLeaderRegion returns a random region that has leader on the store.
func (bc *BasicCluster) RandLeaderRegion(storeID uint64) *core.RegionInfo {
	return bc.Regions.RandLeaderRegion(storeID)
}

// IsRegionHot checks if a region is in hot state.
func (bc *BasicCluster) IsRegionHot(id uint64) bool {
	if stat, ok := bc.WriteStatistics.Peek(id); ok {
		return stat.(*core.RegionStat).HotDegree >= HotRegionLowThreshold
	}
	return false
}

// RegionWriteStats returns hot region's write stats.
func (bc *BasicCluster) RegionWriteStats() []*core.RegionStat {
	elements := bc.WriteStatistics.Elems()
	stats := make([]*core.RegionStat, len(elements))
	for i := range elements {
		stats[i] = elements[i].Value.(*core.RegionStat)
	}
	return stats
}

// RegionReadStats returns hot region's read stats.
func (bc *BasicCluster) RegionReadStats() []*core.RegionStat {
	elements := bc.ReadStatistics.Elems()
	stats := make([]*core.RegionStat, len(elements))
	for i := range elements {
		stats[i] = elements[i].Value.(*core.RegionStat)
	}
	return stats
}

// PutStore put a store
func (bc *BasicCluster) PutStore(store *core.StoreInfo) error {
	bc.Stores.SetStore(store)
	return nil
}

// PutRegion put a region
func (bc *BasicCluster) PutRegion(region *core.RegionInfo) error {
	bc.Regions.SetRegion(region)
	return nil
}

// UpdateWriteStatus update the write status
func (bc *BasicCluster) UpdateWriteStatus(region *core.RegionInfo) {
	var WrittenBytesPerSec uint64
	v, isExist := bc.WriteStatistics.Peek(region.GetId())
	if isExist {
		interval := time.Since(v.(*core.RegionStat).LastUpdateTime).Seconds()
		if interval < minHotRegionReportInterval {
			return
		}
		WrittenBytesPerSec = uint64(float64(region.WrittenBytes) / interval)
	} else {
		WrittenBytesPerSec = uint64(float64(region.WrittenBytes) / float64(RegionHeartBeatReportInterval))
	}
	region.WrittenBytes = WrittenBytesPerSec

	// hotRegionThreshold is use to pick hot region
	// suppose the number of the hot Regions is statCacheMaxLen
	// and we use total written Bytes past storeHeartBeatReportInterval seconds to divide the number of hot Regions
	// divide 2 because the store reports data about two times than the region record write to rocksdb
	divisor := float64(statCacheMaxLen) * 2 * storeHeartBeatReportInterval
	hotRegionThreshold := uint64(float64(bc.Stores.TotalWrittenBytes()) / divisor)

	if hotRegionThreshold < hotWriteRegionMinFlowRate {
		hotRegionThreshold = hotWriteRegionMinFlowRate
	}
	bc.UpdateWriteStatCache(region, hotRegionThreshold)
}

// UpdateWriteStatCache updates statistic for a region if it's hot, or remove it from statistics if it cools down
func (bc *BasicCluster) UpdateWriteStatCache(region *core.RegionInfo, hotRegionThreshold uint64) {
	var v *core.RegionStat
	key := region.GetId()
	value, isExist := bc.WriteStatistics.Peek(key)
	newItem := &core.RegionStat{
		RegionID:       region.GetId(),
		FlowBytes:      region.WrittenBytes,
		LastUpdateTime: time.Now(),
		StoreID:        region.Leader.GetStoreId(),
		Version:        region.GetRegionEpoch().GetVersion(),
		AntiCount:      hotRegionAntiCount,
	}

	if isExist {
		v = value.(*core.RegionStat)
		newItem.HotDegree = v.HotDegree + 1
	}

	if region.WrittenBytes < hotRegionThreshold {
		if !isExist {
			return
		}
		if v.AntiCount <= 0 {
			bc.WriteStatistics.Remove(key)
			return
		}
		// eliminate some noise
		newItem.HotDegree = v.HotDegree - 1
		newItem.AntiCount = v.AntiCount - 1
		newItem.FlowBytes = v.FlowBytes
	}
	bc.WriteStatistics.Put(key, newItem)
}

// UpdateReadStatus update the read status
func (bc *BasicCluster) UpdateReadStatus(region *core.RegionInfo) {
	var ReadBytesPerSec uint64
	v, isExist := bc.ReadStatistics.Peek(region.GetId())
	if isExist {
		interval := time.Now().Sub(v.(*core.RegionStat).LastUpdateTime).Seconds()
		if interval < minHotRegionReportInterval {
			return
		}
		ReadBytesPerSec = uint64(float64(region.ReadBytes) / interval)
	} else {
		ReadBytesPerSec = uint64(float64(region.ReadBytes) / float64(RegionHeartBeatReportInterval))
	}
	region.ReadBytes = ReadBytesPerSec

	// hotRegionThreshold is use to pick hot region
	// suppose the number of the hot Regions is statLRUMaxLen
	// and we use total written Bytes past storeHeartBeatReportInterval seconds to divide the number of hot Regions
	divisor := float64(statCacheMaxLen) * storeHeartBeatReportInterval
	hotRegionThreshold := uint64(float64(bc.Stores.TotalReadBytes()) / divisor)

	if hotRegionThreshold < hotReadRegionMinFlowRate {
		hotRegionThreshold = hotReadRegionMinFlowRate
	}
	bc.UpdateReadStatCache(region, hotRegionThreshold)
}

// UpdateReadStatCache updates statistic for a region if it's hot, or remove it from statistics if it cools down
func (bc *BasicCluster) UpdateReadStatCache(region *core.RegionInfo, hotRegionThreshold uint64) {
	var v *core.RegionStat
	key := region.GetId()
	value, isExist := bc.ReadStatistics.Peek(key)
	newItem := &core.RegionStat{
		RegionID:       region.GetId(),
		FlowBytes:      region.ReadBytes,
		LastUpdateTime: time.Now(),
		StoreID:        region.Leader.GetStoreId(),
		Version:        region.GetRegionEpoch().GetVersion(),
		AntiCount:      hotRegionAntiCount,
	}

	if isExist {
		v = value.(*core.RegionStat)
		newItem.HotDegree = v.HotDegree + 1
	}

	if region.ReadBytes < hotRegionThreshold {
		if !isExist {
			return
		}
		if v.AntiCount <= 0 {
			bc.ReadStatistics.Remove(key)
			return
		}
		// eliminate some noise
		newItem.HotDegree = v.HotDegree - 1
		newItem.AntiCount = v.AntiCount - 1
		newItem.FlowBytes = v.FlowBytes
	}
	bc.ReadStatistics.Put(key, newItem)
}
