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

package schedulers

import (
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pingcap/pd/server/schedule"
	log "github.com/sirupsen/logrus"
)

type mockCluster struct {
	*schedule.BasicCluster
	id *core.MockIDAllocator
	*MockSchedulerOptions
}

// NewMockCluster creates a new mockCluster
func newMockCluster(opt *MockSchedulerOptions) *mockCluster {
	return &mockCluster{
		BasicCluster:         schedule.NewBasicCluster(),
		id:                   core.NewMockIDAllocator(),
		MockSchedulerOptions: opt,
	}
}

func (mc *mockCluster) allocID() (uint64, error) {
	return mc.id.Alloc()
}

// ScanRegions scan region with start key, until number greater than limit.
func (mc *mockCluster) ScanRegions(startKey []byte, limit int) []*core.RegionInfo {
	return mc.Regions.ScanRange(startKey, limit)
}

// AllocPeer allocs a new peer on a store.
func (mc *mockCluster) AllocPeer(storeID uint64) (*metapb.Peer, error) {
	peerID, err := mc.allocID()
	if err != nil {
		log.Errorf("failed to alloc peer: %v", err)
		return nil, errors.Trace(err)
	}
	peer := &metapb.Peer{
		Id:      peerID,
		StoreId: storeID,
	}
	return peer, nil
}

func (mc *mockCluster) setStoreUp(storeID uint64) {
	store := mc.GetStore(storeID)
	store.State = metapb.StoreState_Up
	store.LastHeartbeatTS = time.Now()
	mc.PutStore(store)
}

func (mc *mockCluster) setStoreDown(storeID uint64) {
	store := mc.GetStore(storeID)
	store.State = metapb.StoreState_Up
	store.LastHeartbeatTS = time.Time{}
	mc.PutStore(store)
}

func (mc *mockCluster) setStoreOffline(storeID uint64) {
	store := mc.GetStore(storeID)
	store.State = metapb.StoreState_Offline
	mc.PutStore(store)
}

func (mc *mockCluster) setStoreBusy(storeID uint64, busy bool) {
	store := mc.GetStore(storeID)
	store.Stats.IsBusy = busy
	store.LastHeartbeatTS = time.Now()
	mc.PutStore(store)
}

func (mc *mockCluster) addLeaderStore(storeID uint64, leaderCount int) {
	store := core.NewStoreInfo(&metapb.Store{Id: storeID})
	store.Stats = &pdpb.StoreStats{}
	store.LastHeartbeatTS = time.Now()
	store.LeaderCount = leaderCount
	store.Stats.Capacity = uint64(1024)
	store.Stats.Available = store.Stats.Capacity
	store.LeaderSize = int64(leaderCount) * 10
	mc.PutStore(store)
}

func (mc *mockCluster) addRegionStore(storeID uint64, regionCount int) {
	store := core.NewStoreInfo(&metapb.Store{Id: storeID})
	store.Stats = &pdpb.StoreStats{}
	store.LastHeartbeatTS = time.Now()
	store.RegionCount = regionCount
	store.RegionSize = int64(regionCount) * 10
	store.Stats.Capacity = uint64(1024)
	store.Stats.Available = store.Stats.Capacity
	mc.PutStore(store)
}

func (mc *mockCluster) updateStoreLeaderWeight(storeID uint64, weight float64) {
	store := mc.GetStore(storeID)
	store.LeaderWeight = weight
	mc.PutStore(store)
}

func (mc *mockCluster) updateStoreRegionWeight(storeID uint64, weight float64) {
	store := mc.GetStore(storeID)
	store.RegionWeight = weight
	mc.PutStore(store)
}

func (mc *mockCluster) updateStoreLeaderSize(storeID uint64, size int64) {
	store := mc.GetStore(storeID)
	store.LeaderSize = size
	mc.PutStore(store)
}

func (mc *mockCluster) updateStoreRegionSize(storeID uint64, size int64) {
	store := mc.GetStore(storeID)
	store.RegionSize = size
	mc.PutStore(store)
}

func (mc *mockCluster) addLabelsStore(storeID uint64, regionCount int, labels map[string]string) {
	mc.addRegionStore(storeID, regionCount)
	store := mc.GetStore(storeID)
	for k, v := range labels {
		store.Labels = append(store.Labels, &metapb.StoreLabel{Key: k, Value: v})
	}
	mc.PutStore(store)
}

func (mc *mockCluster) addLeaderRegion(regionID uint64, leaderID uint64, followerIds ...uint64) {
	region := &metapb.Region{Id: regionID}
	leader, _ := mc.AllocPeer(leaderID)
	region.Peers = []*metapb.Peer{leader}
	for _, id := range followerIds {
		peer, _ := mc.AllocPeer(id)
		region.Peers = append(region.Peers, peer)
	}
	regionInfo := core.NewRegionInfo(region, leader)
	regionInfo.ApproximateSize = 10
	mc.PutRegion(regionInfo)
}

func (mc *mockCluster) addLeaderRegionWithRange(regionID uint64, startKey string, endKey string, leaderID uint64, followerIds ...uint64) {
	region := &metapb.Region{Id: regionID}
	leader, _ := mc.AllocPeer(leaderID)
	region.Peers = []*metapb.Peer{leader}
	for _, id := range followerIds {
		peer, _ := mc.AllocPeer(id)
		region.Peers = append(region.Peers, peer)
	}
	r := core.NewRegionInfo(region, leader)
	r.StartKey = []byte(startKey)
	r.EndKey = []byte(endKey)
	mc.PutRegion(r)
}

func (mc *mockCluster) LoadRegion(regionID uint64, followerIds ...uint64) {
	//  regions load from etcd will have no leader
	region := &metapb.Region{Id: regionID}
	region.Peers = []*metapb.Peer{}
	for _, id := range followerIds {
		peer, _ := mc.AllocPeer(id)
		region.Peers = append(region.Peers, peer)
	}
	mc.PutRegion(core.NewRegionInfo(region, nil))
}

func (mc *mockCluster) addLeaderRegionWithWriteInfo(regionID uint64, leaderID uint64, writtenBytes uint64, followerIds ...uint64) {
	region := &metapb.Region{Id: regionID}
	leader, _ := mc.AllocPeer(leaderID)
	region.Peers = []*metapb.Peer{leader}
	for _, id := range followerIds {
		peer, _ := mc.AllocPeer(id)
		region.Peers = append(region.Peers, peer)
	}
	r := core.NewRegionInfo(region, leader)
	r.WrittenBytes = writtenBytes
	mc.BasicCluster.UpdateWriteStatus(r)
	mc.PutRegion(r)
}

func (mc *mockCluster) updateLeaderCount(storeID uint64, leaderCount int) {
	store := mc.GetStore(storeID)
	store.LeaderCount = leaderCount
	store.LeaderSize = int64(leaderCount) * 10
	mc.PutStore(store)
}

func (mc *mockCluster) updateRegionCount(storeID uint64, regionCount int) {
	store := mc.GetStore(storeID)
	store.RegionCount = regionCount
	store.RegionSize = int64(regionCount) * 10
	mc.PutStore(store)
}

func (mc *mockCluster) updateSnapshotCount(storeID uint64, snapshotCount int) {
	store := mc.GetStore(storeID)
	store.Stats.ApplyingSnapCount = uint32(snapshotCount)
	mc.PutStore(store)
}

func (mc *mockCluster) updateStorageRatio(storeID uint64, usedRatio, availableRatio float64) {
	store := mc.GetStore(storeID)
	store.Stats.Capacity = uint64(1024)
	store.Stats.UsedSize = uint64(float64(store.Stats.Capacity) * usedRatio)
	store.Stats.Available = uint64(float64(store.Stats.Capacity) * availableRatio)
	mc.PutStore(store)
}

func (mc *mockCluster) updateStorageWrittenBytes(storeID uint64, BytesWritten uint64) {
	store := mc.GetStore(storeID)
	store.Stats.BytesWritten = BytesWritten
	mc.PutStore(store)
}
func (mc *mockCluster) updateStorageReadBytes(storeID uint64, BytesRead uint64) {
	store := mc.GetStore(storeID)
	store.Stats.BytesRead = BytesRead
	mc.PutStore(store)
}

func (mc *mockCluster) addLeaderRegionWithReadInfo(regionID uint64, leaderID uint64, readBytes uint64, followerIds ...uint64) {
	region := &metapb.Region{Id: regionID}
	leader, _ := mc.AllocPeer(leaderID)
	region.Peers = []*metapb.Peer{leader}
	for _, id := range followerIds {
		peer, _ := mc.AllocPeer(id)
		region.Peers = append(region.Peers, peer)
	}
	r := core.NewRegionInfo(region, leader)
	r.ReadBytes = readBytes
	mc.BasicCluster.UpdateReadStatus(r)
	mc.PutRegion(r)
}

func (mc *mockCluster) GetOpt() schedule.NamespaceOptions {
	return mc.MockSchedulerOptions
}

func (mc *mockCluster) GetLeaderScheduleLimit() uint64 {
	return mc.MockSchedulerOptions.GetLeaderScheduleLimit(namespace.DefaultNamespace)
}

func (mc *mockCluster) GetRegionScheduleLimit() uint64 {
	return mc.MockSchedulerOptions.GetRegionScheduleLimit(namespace.DefaultNamespace)
}

func (mc *mockCluster) GetReplicaScheduleLimit() uint64 {
	return mc.MockSchedulerOptions.GetReplicaScheduleLimit(namespace.DefaultNamespace)
}

func (mc *mockCluster) GetMaxReplicas() int {
	return mc.MockSchedulerOptions.GetMaxReplicas(namespace.DefaultNamespace)
}

const (
	defaultMaxReplicas          = 3
	defaultMaxSnapshotCount     = 3
	defaultMaxPendingPeerCount  = 16
	defaultMaxStoreDownTime     = time.Hour
	defaultLeaderScheduleLimit  = 64
	defaultRegionScheduleLimit  = 12
	defaultReplicaScheduleLimit = 16
	defaultTolerantSizeRatio    = 2.5
)

// MockSchedulerOptions is a mock of SchedulerOptions
// which implements Options interface
type MockSchedulerOptions struct {
	RegionScheduleLimit   uint64
	LeaderScheduleLimit   uint64
	ReplicaScheduleLimit  uint64
	MaxSnapshotCount      uint64
	MaxPendingPeerCount   uint64
	MaxStoreDownTime      time.Duration
	MaxReplicas           int
	LocationLabels        []string
	HotRegionLowThreshold int
	TolerantSizeRatio     float64
}

func newMockSchedulerOptions() *MockSchedulerOptions {
	mso := &MockSchedulerOptions{}
	mso.RegionScheduleLimit = defaultRegionScheduleLimit
	mso.LeaderScheduleLimit = defaultLeaderScheduleLimit
	mso.ReplicaScheduleLimit = defaultReplicaScheduleLimit
	mso.MaxSnapshotCount = defaultMaxSnapshotCount
	mso.MaxStoreDownTime = defaultMaxStoreDownTime
	mso.MaxReplicas = defaultMaxReplicas
	mso.HotRegionLowThreshold = schedule.HotRegionLowThreshold
	mso.MaxPendingPeerCount = defaultMaxPendingPeerCount
	mso.TolerantSizeRatio = defaultTolerantSizeRatio
	return mso
}

// GetLeaderScheduleLimit mock method
func (mso *MockSchedulerOptions) GetLeaderScheduleLimit(name string) uint64 {
	return mso.LeaderScheduleLimit
}

// GetRegionScheduleLimit mock method
func (mso *MockSchedulerOptions) GetRegionScheduleLimit(name string) uint64 {
	return mso.RegionScheduleLimit
}

// GetReplicaScheduleLimit mock method
func (mso *MockSchedulerOptions) GetReplicaScheduleLimit(name string) uint64 {
	return mso.ReplicaScheduleLimit
}

// GetMaxSnapshotCount mock method
func (mso *MockSchedulerOptions) GetMaxSnapshotCount() uint64 {
	return mso.MaxSnapshotCount
}

// GetMaxPendingPeerCount mock method
func (mso *MockSchedulerOptions) GetMaxPendingPeerCount() uint64 {
	return mso.MaxPendingPeerCount
}

// GetMaxStoreDownTime mock method
func (mso *MockSchedulerOptions) GetMaxStoreDownTime() time.Duration {
	return mso.MaxStoreDownTime
}

// GetMaxReplicas mock method
func (mso *MockSchedulerOptions) GetMaxReplicas(name string) int {
	return mso.MaxReplicas
}

// GetLocationLabels mock method
func (mso *MockSchedulerOptions) GetLocationLabels() []string {
	return mso.LocationLabels
}

// GetHotRegionLowThreshold mock method
func (mso *MockSchedulerOptions) GetHotRegionLowThreshold() int {
	return mso.HotRegionLowThreshold
}

// GetTolerantSizeRatio mock method
func (mso *MockSchedulerOptions) GetTolerantSizeRatio() float64 {
	return mso.TolerantSizeRatio
}

// SetMaxReplicas mock method
func (mso *MockSchedulerOptions) SetMaxReplicas(replicas int) {
	mso.MaxReplicas = replicas
}
