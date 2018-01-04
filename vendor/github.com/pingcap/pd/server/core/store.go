// Copyright 2016 PingCAP, Inc.
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

package core

import (
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	log "github.com/sirupsen/logrus"
)

// StoreInfo contains information about a store.
type StoreInfo struct {
	*metapb.Store
	Stats *pdpb.StoreStats
	// Blocked means that the store is blocked from balance.
	blocked          bool
	LeaderCount      int
	RegionCount      int
	LeaderSize       int64
	RegionSize       int64
	PendingPeerCount int
	LastHeartbeatTS  time.Time
	LeaderWeight     float64
	RegionWeight     float64
}

// NewStoreInfo creates StoreInfo with meta data.
func NewStoreInfo(store *metapb.Store) *StoreInfo {
	return &StoreInfo{
		Store:        store,
		LeaderWeight: 1.0,
		RegionWeight: 1.0,
	}
}

// Clone creates a copy of current StoreInfo.
func (s *StoreInfo) Clone() *StoreInfo {
	return &StoreInfo{
		Store:            proto.Clone(s.Store).(*metapb.Store),
		Stats:            proto.Clone(s.Stats).(*pdpb.StoreStats),
		blocked:          s.blocked,
		LeaderCount:      s.LeaderCount,
		RegionCount:      s.RegionCount,
		LeaderSize:       s.LeaderSize,
		RegionSize:       s.RegionSize,
		PendingPeerCount: s.PendingPeerCount,
		LastHeartbeatTS:  s.LastHeartbeatTS,
		LeaderWeight:     s.LeaderWeight,
		RegionWeight:     s.RegionWeight,
	}
}

// Block stops balancer from selecting the store.
func (s *StoreInfo) Block() {
	s.blocked = true
}

// Unblock allows balancer to select the store.
func (s *StoreInfo) Unblock() {
	s.blocked = false
}

// IsBlocked returns if the store is blocked.
func (s *StoreInfo) IsBlocked() bool {
	return s.blocked
}

// IsUp checks if the store's state is Up.
func (s *StoreInfo) IsUp() bool {
	return s.GetState() == metapb.StoreState_Up
}

// IsOffline checks if the store's state is Offline.
func (s *StoreInfo) IsOffline() bool {
	return s.GetState() == metapb.StoreState_Offline
}

// IsTombstone checks if the store's state is Tombstone.
func (s *StoreInfo) IsTombstone() bool {
	return s.GetState() == metapb.StoreState_Tombstone
}

// DownTime returns the time elapsed since last heartbeat.
func (s *StoreInfo) DownTime() time.Duration {
	return time.Since(s.LastHeartbeatTS)
}

const minWeight = 1e-6

// LeaderScore returns the store's leader score: leaderCount / leaderWeight.
func (s *StoreInfo) LeaderScore() float64 {
	if s.LeaderWeight <= 0 {
		return float64(s.LeaderSize) / minWeight
	}
	return float64(s.LeaderSize) / s.LeaderWeight
}

// RegionScore returns the store's region score: regionSize / regionWeight.
func (s *StoreInfo) RegionScore() float64 {
	if s.RegionWeight <= 0 {
		return float64(s.RegionSize) / minWeight
	}
	return float64(s.RegionSize) / s.RegionWeight
}

// StorageSize returns store's used storage size reported from tikv.
func (s *StoreInfo) StorageSize() uint64 {
	return s.Stats.GetUsedSize()
}

// AvailableRatio is store's freeSpace/capacity.
func (s *StoreInfo) AvailableRatio() float64 {
	if s.Stats.GetCapacity() == 0 {
		return 0
	}
	return float64(s.Stats.GetAvailable()) / float64(s.Stats.GetCapacity())
}

const storeLowSpaceThreshold = 0.2

// IsLowSpace checks if the store is lack of space.
func (s *StoreInfo) IsLowSpace() bool {
	return s.AvailableRatio() < storeLowSpaceThreshold
}

// ResourceCount reutrns count of leader/region in the store.
func (s *StoreInfo) ResourceCount(kind ResourceKind) uint64 {
	switch kind {
	case LeaderKind:
		return uint64(s.LeaderCount)
	case RegionKind:
		return uint64(s.RegionCount)
	default:
		return 0
	}
}

// ResourceSize returns size of leader/region in the store
func (s *StoreInfo) ResourceSize(kind ResourceKind) int64 {
	switch kind {
	case LeaderKind:
		return s.LeaderSize
	case RegionKind:
		return s.RegionSize
	default:
		return 0
	}
}

// ResourceScore reutrns score of leader/region in the store.
func (s *StoreInfo) ResourceScore(kind ResourceKind) float64 {
	switch kind {
	case LeaderKind:
		return s.LeaderScore()
	case RegionKind:
		return s.RegionScore()
	default:
		return 0
	}
}

// ResourceWeight returns weight of leader/region in the score
func (s *StoreInfo) ResourceWeight(kind ResourceKind) float64 {
	switch kind {
	case LeaderKind:
		if s.LeaderWeight <= 0 {
			return minWeight
		}
		return s.LeaderWeight
	case RegionKind:
		if s.RegionWeight <= 0 {
			return minWeight
		}
		return s.RegionWeight
	default:
		return 0
	}
}

// GetStartTS returns the start timestamp.
func (s *StoreInfo) GetStartTS() time.Time {
	return time.Unix(int64(s.Stats.GetStartTime()), 0)
}

// GetUptime returns the uptime.
func (s *StoreInfo) GetUptime() time.Duration {
	uptime := s.LastHeartbeatTS.Sub(s.GetStartTS())
	if uptime > 0 {
		return uptime
	}
	return 0
}

// If a store's last heartbeat is storeDisconnectDuration ago, the store will
// be marked as disconnected state. The value should be greater than tikv's
// store heartbeat interval (default 10s).
var storeDisconnectDuration = 20 * time.Second

// IsDisconnected checks if a store is disconnected, which means PD misses
// tikv's store heartbeat for a short time, maybe caused by process restart or
// temporary network failure.
func (s *StoreInfo) IsDisconnected() bool {
	return s.DownTime() > storeDisconnectDuration
}

// GetLabelValue returns a label's value (if exists).
func (s *StoreInfo) GetLabelValue(key string) string {
	for _, label := range s.GetLabels() {
		if strings.EqualFold(label.GetKey(), key) {
			return label.GetValue()
		}
	}
	return ""
}

// CompareLocation compares 2 stores' labels and returns at which level their
// locations are different. It returns -1 if they are at the same location.
func (s *StoreInfo) CompareLocation(other *StoreInfo, labels []string) int {
	for i, key := range labels {
		v1, v2 := s.GetLabelValue(key), other.GetLabelValue(key)
		// If label is not set, the store is considered at the same location
		// with any other store.
		if v1 != "" && v2 != "" && !strings.EqualFold(v1, v2) {
			return i
		}
	}
	return -1
}

// MergeLabels merges the passed in labels with origins, overriding duplicated
// ones.
func (s *StoreInfo) MergeLabels(labels []*metapb.StoreLabel) {
L:
	for _, newLabel := range labels {
		for _, label := range s.Labels {
			if strings.EqualFold(label.Key, newLabel.Key) {
				label.Value = newLabel.Value
				continue L
			}
		}
		s.Labels = append(s.Labels, newLabel)
	}
}

// StoreHotRegionInfos : used to get human readable description for hot regions.
type StoreHotRegionInfos struct {
	AsPeer   StoreHotRegionsStat `json:"as_peer"`
	AsLeader StoreHotRegionsStat `json:"as_leader"`
}

// StoreHotRegionsStat used to record the hot region statistics group by store
type StoreHotRegionsStat map[uint64]*HotRegionsStat

var (
	// ErrStoreNotFound is for log of store no found
	ErrStoreNotFound = func(storeID uint64) error {
		return errors.Errorf("store %v not found", storeID)
	}
	// ErrStoreIsBlocked is for log of store is blocked
	ErrStoreIsBlocked = func(storeID uint64) error {
		return errors.Errorf("store %v is blocked", storeID)
	}
)

// StoresInfo is a map of storeID to StoreInfo
type StoresInfo struct {
	stores map[uint64]*StoreInfo
}

// NewStoresInfo create a StoresInfo with map of storeID to StoreInfo
func NewStoresInfo() *StoresInfo {
	return &StoresInfo{
		stores: make(map[uint64]*StoreInfo),
	}
}

// GetStore return a StoreInfo with storeID
func (s *StoresInfo) GetStore(storeID uint64) *StoreInfo {
	store, ok := s.stores[storeID]
	if !ok {
		return nil
	}
	return store.Clone()
}

// SetStore set a StoreInfo with storeID
func (s *StoresInfo) SetStore(store *StoreInfo) {
	s.stores[store.GetId()] = store
}

// BlockStore block a StoreInfo with storeID
func (s *StoresInfo) BlockStore(storeID uint64) error {
	store, ok := s.stores[storeID]
	if !ok {
		return ErrStoreNotFound(storeID)
	}
	if store.IsBlocked() {
		return ErrStoreIsBlocked(storeID)
	}
	store.Block()
	return nil
}

// UnblockStore unblock a StoreInfo with storeID
func (s *StoresInfo) UnblockStore(storeID uint64) {
	store, ok := s.stores[storeID]
	if !ok {
		log.Fatalf("store %d is unblocked, but it is not found", storeID)
	}
	store.Unblock()
}

// GetStores get a complete set of StoreInfo
func (s *StoresInfo) GetStores() []*StoreInfo {
	stores := make([]*StoreInfo, 0, len(s.stores))
	for _, store := range s.stores {
		stores = append(stores, store.Clone())
	}
	return stores
}

// GetMetaStores get a complete set of metapb.Store
func (s *StoresInfo) GetMetaStores() []*metapb.Store {
	stores := make([]*metapb.Store, 0, len(s.stores))
	for _, store := range s.stores {
		stores = append(stores, proto.Clone(store.Store).(*metapb.Store))
	}
	return stores
}

// GetStoreCount return the total count of storeInfo
func (s *StoresInfo) GetStoreCount() int {
	return len(s.stores)
}

// SetLeaderCount set the leader count to a storeInfo
func (s *StoresInfo) SetLeaderCount(storeID uint64, leaderCount int) {
	if store, ok := s.stores[storeID]; ok {
		store.LeaderCount = leaderCount
	}
}

// SetRegionCount set the region count to a storeInfo
func (s *StoresInfo) SetRegionCount(storeID uint64, regionCount int) {
	if store, ok := s.stores[storeID]; ok {
		store.RegionCount = regionCount
	}
}

// SetPendingPeerCount sets the pengding count to a storeInfo
func (s *StoresInfo) SetPendingPeerCount(storeID uint64, pendingPeerCount int) {
	if store, ok := s.stores[storeID]; ok {
		store.PendingPeerCount = pendingPeerCount
	}
}

// SetLeaderSize set the leader count to a storeInfo
func (s *StoresInfo) SetLeaderSize(storeID uint64, leaderSize int64) {
	if store, ok := s.stores[storeID]; ok {
		store.LeaderSize = leaderSize
	}
}

// SetRegionSize set the region count to a storeInfo
func (s *StoresInfo) SetRegionSize(storeID uint64, regionSize int64) {
	if store, ok := s.stores[storeID]; ok {
		store.RegionSize = regionSize
	}
}

// AverageResourceScore return the total resource score of all StoreInfo
func (s *StoresInfo) AverageResourceScore(kind ResourceKind) float64 {
	var totalResourceSize int64
	var totalResourceWeight float64
	for _, s := range s.stores {
		if s.IsUp() {
			totalResourceWeight += s.ResourceWeight(kind)
			totalResourceSize += s.ResourceSize(kind)
		}
	}

	if totalResourceWeight == 0 {
		return 0
	}
	return float64(totalResourceSize) / totalResourceWeight
}

// TotalWrittenBytes return the total written bytes of all StoreInfo
func (s *StoresInfo) TotalWrittenBytes() uint64 {
	var totalWrittenBytes uint64
	for _, s := range s.stores {
		if s.IsUp() {
			totalWrittenBytes += s.Stats.GetBytesWritten()
		}
	}
	return totalWrittenBytes
}

// TotalReadBytes return the total read bytes of all StoreInfo
func (s *StoresInfo) TotalReadBytes() uint64 {
	var totalReadBytes uint64
	for _, s := range s.stores {
		if s.IsUp() {
			totalReadBytes += s.Stats.GetBytesRead()
		}
	}
	return totalReadBytes
}

// GetStoresWriteStat return the write stat of all StoreInfo
func (s *StoresInfo) GetStoresWriteStat() map[uint64]uint64 {
	res := make(map[uint64]uint64)
	for _, s := range s.stores {
		res[s.GetId()] = s.Stats.GetBytesWritten()
	}
	return res
}

// GetStoresReadStat return the read stat of all StoreInfo
func (s *StoresInfo) GetStoresReadStat() map[uint64]uint64 {
	res := make(map[uint64]uint64)
	for _, s := range s.stores {
		res[s.GetId()] = s.Stats.GetBytesRead()
	}
	return res
}
