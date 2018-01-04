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
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

// RegionInfo records detail region info.
type RegionInfo struct {
	*metapb.Region
	Leader          *metapb.Peer
	DownPeers       []*pdpb.PeerStats
	PendingPeers    []*metapb.Peer
	WrittenBytes    uint64
	ReadBytes       uint64
	ApproximateSize int64
}

// NewRegionInfo creates RegionInfo with region's meta and leader peer.
func NewRegionInfo(region *metapb.Region, leader *metapb.Peer) *RegionInfo {
	return &RegionInfo{
		Region: region,
		Leader: leader,
	}
}

// EmptyRegionApproximateSize is the region approximate size of an empty region
// (heartbeat size <= 1MB).
const EmptyRegionApproximateSize = 1

// RegionFromHeartbeat constructs a Region from region heartbeat.
func RegionFromHeartbeat(heartbeat *pdpb.RegionHeartbeatRequest) *RegionInfo {
	return &RegionInfo{
		Region:          heartbeat.GetRegion(),
		Leader:          heartbeat.GetLeader(),
		DownPeers:       heartbeat.GetDownPeers(),
		PendingPeers:    heartbeat.GetPendingPeers(),
		WrittenBytes:    heartbeat.GetBytesWritten(),
		ReadBytes:       heartbeat.GetBytesRead(),
		ApproximateSize: int64(math.Ceil(float64(heartbeat.GetApproximateSize()) / 1e6)), // use size of MB as unit
	}
}

// Clone returns a copy of current regionInfo.
func (r *RegionInfo) Clone() *RegionInfo {
	downPeers := make([]*pdpb.PeerStats, 0, len(r.DownPeers))
	for _, peer := range r.DownPeers {
		downPeers = append(downPeers, proto.Clone(peer).(*pdpb.PeerStats))
	}
	pendingPeers := make([]*metapb.Peer, 0, len(r.PendingPeers))
	for _, peer := range r.PendingPeers {
		pendingPeers = append(pendingPeers, proto.Clone(peer).(*metapb.Peer))
	}
	return &RegionInfo{
		Region:          proto.Clone(r.Region).(*metapb.Region),
		Leader:          proto.Clone(r.Leader).(*metapb.Peer),
		DownPeers:       downPeers,
		PendingPeers:    pendingPeers,
		WrittenBytes:    r.WrittenBytes,
		ReadBytes:       r.ReadBytes,
		ApproximateSize: r.ApproximateSize,
	}
}

// GetPeer returns the peer with specified peer id.
func (r *RegionInfo) GetPeer(peerID uint64) *metapb.Peer {
	for _, peer := range r.GetPeers() {
		if peer.GetId() == peerID {
			return peer
		}
	}
	return nil
}

// GetDownPeer returns the down peers with specified peer id.
func (r *RegionInfo) GetDownPeer(peerID uint64) *metapb.Peer {
	for _, down := range r.DownPeers {
		if down.GetPeer().GetId() == peerID {
			return down.GetPeer()
		}
	}
	return nil
}

// GetPendingPeer returns the pending peer with specified peer id.
func (r *RegionInfo) GetPendingPeer(peerID uint64) *metapb.Peer {
	for _, peer := range r.PendingPeers {
		if peer.GetId() == peerID {
			return peer
		}
	}
	return nil
}

// GetStorePeer returns the peer in specified store.
func (r *RegionInfo) GetStorePeer(storeID uint64) *metapb.Peer {
	for _, peer := range r.GetPeers() {
		if peer.GetStoreId() == storeID {
			return peer
		}
	}
	return nil
}

// RemoveStorePeer removes the peer in specified store.
func (r *RegionInfo) RemoveStorePeer(storeID uint64) {
	var peers []*metapb.Peer
	for _, peer := range r.GetPeers() {
		if peer.GetStoreId() != storeID {
			peers = append(peers, peer)
		}
	}
	r.Peers = peers
}

// GetStoreIds returns a map indicate the region distributed.
func (r *RegionInfo) GetStoreIds() map[uint64]struct{} {
	peers := r.GetPeers()
	stores := make(map[uint64]struct{}, len(peers))
	for _, peer := range peers {
		stores[peer.GetStoreId()] = struct{}{}
	}
	return stores
}

// GetFollowers returns a map indicate the follow peers distributed.
func (r *RegionInfo) GetFollowers() map[uint64]*metapb.Peer {
	peers := r.GetPeers()
	followers := make(map[uint64]*metapb.Peer, len(peers))
	for _, peer := range peers {
		if r.Leader == nil || r.Leader.GetId() != peer.GetId() {
			followers[peer.GetStoreId()] = peer
		}
	}
	return followers
}

// GetFollower randomly returns a follow peer.
func (r *RegionInfo) GetFollower() *metapb.Peer {
	for _, peer := range r.GetPeers() {
		if r.Leader == nil || r.Leader.GetId() != peer.GetId() {
			return peer
		}
	}
	return nil
}

// GetDiffFollowers returns the followers which is not located in the same
// store as any other followers of the another specified region.
func (r *RegionInfo) GetDiffFollowers(other *RegionInfo) []*metapb.Peer {
	res := make([]*metapb.Peer, 0, len(r.Peers))
	for _, p := range r.GetFollowers() {
		diff := true
		for _, o := range other.GetFollowers() {
			if p.GetStoreId() == o.GetStoreId() {
				diff = false
				break
			}
		}
		if diff {
			res = append(res, p)
		}
	}
	return res
}

// RegionStat records each hot region's statistics
type RegionStat struct {
	RegionID  uint64 `json:"region_id"`
	FlowBytes uint64 `json:"flow_bytes"`
	// HotDegree records the hot region update times
	HotDegree int `json:"hot_degree"`
	// LastUpdateTime used to calculate average write
	LastUpdateTime time.Time `json:"last_update_time"`
	StoreID        uint64    `json:"-"`
	// AntiCount used to eliminate some noise when remove region in cache
	AntiCount int
	// Version used to check the region split times
	Version uint64
}

// RegionsStat is a list of a group region state type
type RegionsStat []RegionStat

func (m RegionsStat) Len() int           { return len(m) }
func (m RegionsStat) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }
func (m RegionsStat) Less(i, j int) bool { return m[i].FlowBytes < m[j].FlowBytes }

// HotRegionsStat records all hot regions statistics
type HotRegionsStat struct {
	TotalFlowBytes uint64      `json:"total_flow_bytes"`
	RegionsCount   int         `json:"regions_count"`
	RegionsStat    RegionsStat `json:"statistics"`
}

// regionMap wraps a map[uint64]*core.RegionInfo and supports randomly pick a region.
type regionMap struct {
	m         map[uint64]*regionEntry
	ids       []uint64
	totalSize int64
}

type regionEntry struct {
	*RegionInfo
	pos int
}

func newRegionMap() *regionMap {
	return &regionMap{
		m:         make(map[uint64]*regionEntry),
		totalSize: 0,
	}
}

func (rm *regionMap) Len() int {
	if rm == nil {
		return 0
	}
	return len(rm.m)
}

func (rm *regionMap) Get(id uint64) *RegionInfo {
	if rm == nil {
		return nil
	}
	if entry, ok := rm.m[id]; ok {
		return entry.RegionInfo
	}
	return nil
}

func (rm *regionMap) Put(region *RegionInfo) {
	if old, ok := rm.m[region.GetId()]; ok {
		rm.totalSize += region.ApproximateSize - old.ApproximateSize
		old.RegionInfo = region
		return
	}
	rm.m[region.GetId()] = &regionEntry{
		RegionInfo: region,
		pos:        len(rm.ids),
	}
	rm.ids = append(rm.ids, region.GetId())
	rm.totalSize += region.ApproximateSize
}

func (rm *regionMap) RandomRegion() *RegionInfo {
	if rm.Len() == 0 {
		return nil
	}
	return rm.Get(rm.ids[rand.Intn(rm.Len())])
}

func (rm *regionMap) Delete(id uint64) {
	if rm == nil {
		return
	}
	if old, ok := rm.m[id]; ok {
		len := rm.Len()
		last := rm.m[rm.ids[len-1]]
		last.pos = old.pos
		rm.ids[last.pos] = last.GetId()
		delete(rm.m, id)
		rm.ids = rm.ids[:len-1]
		rm.totalSize -= old.ApproximateSize
	}
}

func (rm *regionMap) TotalSize() int64 {
	if rm.Len() == 0 {
		return 0
	}
	return rm.totalSize
}

// RegionsInfo for export
type RegionsInfo struct {
	tree         *regionTree
	regions      *regionMap            // regionID -> regionInfo
	leaders      map[uint64]*regionMap // storeID -> regionID -> regionInfo
	followers    map[uint64]*regionMap // storeID -> regionID -> regionInfo
	pendingPeers map[uint64]*regionMap // storeID -> regionID -> regionInfo
}

// NewRegionsInfo creates RegionsInfo with tree, regions, leaders and followers
func NewRegionsInfo() *RegionsInfo {
	return &RegionsInfo{
		tree:         newRegionTree(),
		regions:      newRegionMap(),
		leaders:      make(map[uint64]*regionMap),
		followers:    make(map[uint64]*regionMap),
		pendingPeers: make(map[uint64]*regionMap),
	}
}

// GetRegion return the RegionInfo with regionID
func (r *RegionsInfo) GetRegion(regionID uint64) *RegionInfo {
	region := r.regions.Get(regionID)
	if region == nil {
		return nil
	}
	return region.Clone()
}

// SetRegion set the RegionInfo with regionID
func (r *RegionsInfo) SetRegion(region *RegionInfo) {
	if origin := r.regions.Get(region.GetId()); origin != nil {
		r.RemoveRegion(origin)
	}
	r.AddRegion(region)
}

// Length return the RegionsInfo length
func (r *RegionsInfo) Length() int {
	return r.regions.Len()
}

// TreeLength return the RegionsInfo tree length(now only used in test)
func (r *RegionsInfo) TreeLength() int {
	return r.tree.length()
}

// AddRegion add RegionInfo to regionTree and regionMap, also update leadres and followers by region peers
func (r *RegionsInfo) AddRegion(region *RegionInfo) {
	// Add to tree and regions.
	r.tree.update(region.Region)
	r.regions.Put(region)

	if region.Leader == nil {
		return
	}

	// Add to leaders and followers.
	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		if peer.GetId() == region.Leader.GetId() {
			// Add leader peer to leaders.
			store, ok := r.leaders[storeID]
			if !ok {
				store = newRegionMap()
				r.leaders[storeID] = store
			}
			store.Put(region)
		} else {
			// Add follower peer to followers.
			store, ok := r.followers[storeID]
			if !ok {
				store = newRegionMap()
				r.followers[storeID] = store
			}
			store.Put(region)
		}
	}

	for _, peer := range region.PendingPeers {
		storeID := peer.GetStoreId()
		store, ok := r.pendingPeers[storeID]
		if !ok {
			store = newRegionMap()
			r.pendingPeers[storeID] = store
		}
		store.Put(region)
	}
}

// RemoveRegion remove RegionInfo from regionTree and regionMap
func (r *RegionsInfo) RemoveRegion(region *RegionInfo) {
	// Remove from tree and regions.
	r.tree.remove(region.Region)
	r.regions.Delete(region.GetId())

	// Remove from leaders and followers.
	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		r.leaders[storeID].Delete(region.GetId())
		r.followers[storeID].Delete(region.GetId())
		r.pendingPeers[storeID].Delete(region.GetId())
	}
}

// SearchRegion search RegionInfo from regionTree
func (r *RegionsInfo) SearchRegion(regionKey []byte) *RegionInfo {
	region := r.tree.search(regionKey)
	if region == nil {
		return nil
	}
	return r.GetRegion(region.GetId())
}

// GetRegions get a set of RegionInfo from regionMap
func (r *RegionsInfo) GetRegions() []*RegionInfo {
	regions := make([]*RegionInfo, 0, r.regions.Len())
	for _, region := range r.regions.m {
		regions = append(regions, region.Clone())
	}
	return regions
}

// GetStoreLeaderRegionSize get total size of store's leader regions
func (r *RegionsInfo) GetStoreLeaderRegionSize(storeID uint64) int64 {
	return r.leaders[storeID].TotalSize()
}

// GetStoreFollowerRegionSize get total size of store's follower regions
func (r *RegionsInfo) GetStoreFollowerRegionSize(storeID uint64) int64 {
	return r.followers[storeID].TotalSize()
}

// GetStoreRegionSize get total size of store's regions
func (r *RegionsInfo) GetStoreRegionSize(storeID uint64) int64 {
	return r.GetStoreLeaderRegionSize(storeID) + r.GetStoreFollowerRegionSize(storeID)
}

// GetMetaRegions get a set of metapb.Region from regionMap
func (r *RegionsInfo) GetMetaRegions() []*metapb.Region {
	regions := make([]*metapb.Region, 0, r.regions.Len())
	for _, region := range r.regions.m {
		regions = append(regions, proto.Clone(region.Region).(*metapb.Region))
	}
	return regions
}

// GetRegionCount get the total count of RegionInfo of regionMap
func (r *RegionsInfo) GetRegionCount() int {
	return r.regions.Len()
}

// GetStoreRegionCount get  the total count of  a store's leader and follower RegionInfo by storeID
func (r *RegionsInfo) GetStoreRegionCount(storeID uint64) int {
	return r.GetStoreLeaderCount(storeID) + r.GetStoreFollowerCount(storeID)
}

// GetStorePendingPeerCount gets the total count of  a store's region that includes pending peer
func (r *RegionsInfo) GetStorePendingPeerCount(storeID uint64) int {
	return r.pendingPeers[storeID].Len()
}

// GetStoreLeaderCount get the total count of a store's leader RegionInfo
func (r *RegionsInfo) GetStoreLeaderCount(storeID uint64) int {
	return r.leaders[storeID].Len()
}

// GetStoreFollowerCount get the total count of a store's follower RegionInfo
func (r *RegionsInfo) GetStoreFollowerCount(storeID uint64) int {
	return r.followers[storeID].Len()
}

// RandRegion get a region by random
func (r *RegionsInfo) RandRegion() *RegionInfo {
	return randRegion(r.regions)
}

// RandLeaderRegion get a store's leader region by random
func (r *RegionsInfo) RandLeaderRegion(storeID uint64) *RegionInfo {
	return randRegion(r.leaders[storeID])
}

// RandFollowerRegion get a store's follower region by random
func (r *RegionsInfo) RandFollowerRegion(storeID uint64) *RegionInfo {
	return randRegion(r.followers[storeID])
}

// GetLeader return leader RegionInfo by storeID and regionID(now only used in test)
func (r *RegionsInfo) GetLeader(storeID uint64, regionID uint64) *RegionInfo {
	return r.leaders[storeID].Get(regionID)
}

// GetFollower return follower RegionInfo by storeID and regionID(now only used in test)
func (r *RegionsInfo) GetFollower(storeID uint64, regionID uint64) *RegionInfo {
	return r.followers[storeID].Get(regionID)
}

// ScanRange scans region with start key, until number greater than limit.
func (r *RegionsInfo) ScanRange(startKey []byte, limit int) []*RegionInfo {
	res := make([]*RegionInfo, 0, limit)
	r.tree.scanRange(startKey, func(region *metapb.Region) bool {
		res = append(res, r.GetRegion(region.GetId()))
		return len(res) < limit
	})
	return res
}

// RegionStats records a list of regions' statistics and distribution status.
type RegionStats struct {
	Count            int              `json:"count"`
	EmptyCount       int              `json:"empty_count"`
	StorageSize      int64            `json:"storage_size"`
	StoreLeaderCount map[uint64]int   `json:"store_leader_count"`
	StorePeerCount   map[uint64]int   `json:"store_peer_count"`
	StoreLeaderSize  map[uint64]int64 `json:"store_leader_size"`
	StorePeerSize    map[uint64]int64 `json:"store_peer_size"`
}

func newRegionStats() *RegionStats {
	return &RegionStats{
		StoreLeaderCount: make(map[uint64]int),
		StorePeerCount:   make(map[uint64]int),
		StoreLeaderSize:  make(map[uint64]int64),
		StorePeerSize:    make(map[uint64]int64),
	}
}

// Observe adds a region's statistics into RegionStats.
func (s *RegionStats) Observe(r *RegionInfo) {
	s.Count++
	if r.ApproximateSize <= EmptyRegionApproximateSize {
		s.EmptyCount++
	}
	s.StorageSize += r.ApproximateSize
	if r.Leader != nil {
		s.StoreLeaderCount[r.Leader.GetStoreId()]++
		s.StoreLeaderSize[r.Leader.GetStoreId()] += r.ApproximateSize
	}
	for _, p := range r.Peers {
		s.StorePeerCount[p.GetStoreId()]++
		s.StorePeerSize[p.GetStoreId()] += r.ApproximateSize
	}
}

// GetRegionStats scans regions that inside range [startKey, endKey) and sums up
// their statistics.
func (r *RegionsInfo) GetRegionStats(startKey, endKey []byte) *RegionStats {
	stats := newRegionStats()
	r.tree.scanRange(startKey, func(meta *metapb.Region) bool {
		if len(endKey) > 0 && (len(meta.EndKey) == 0 || bytes.Compare(meta.EndKey, endKey) >= 0) {
			return false
		}
		if region := r.GetRegion(meta.GetId()); region != nil {
			stats.Observe(region)
		}
		return true
	})
	return stats
}

const randomRegionMaxRetry = 10

func randRegion(regions *regionMap) *RegionInfo {
	for i := 0; i < randomRegionMaxRetry; i++ {
		region := regions.RandomRegion()
		if region == nil {
			return nil
		}
		if len(region.DownPeers) == 0 && len(region.PendingPeers) == 0 {
			return region.Clone()
		}
	}
	return nil
}

// DiffRegionPeersInfo return the difference of peers info  between two RegionInfo
func DiffRegionPeersInfo(origin *RegionInfo, other *RegionInfo) string {
	var ret []string
	for _, a := range origin.Peers {
		both := false
		for _, b := range other.Peers {
			if reflect.DeepEqual(a, b) {
				both = true
				break
			}
		}
		if !both {
			ret = append(ret, fmt.Sprintf("Remove peer:{%v}", a))
		}
	}
	for _, b := range other.Peers {
		both := false
		for _, a := range origin.Peers {
			if reflect.DeepEqual(a, b) {
				both = true
				break
			}
		}
		if !both {
			ret = append(ret, fmt.Sprintf("Add peer:{%v}", b))
		}
	}
	return strings.Join(ret, ",")
}

// DiffRegionKeyInfo return the difference of key info between two RegionInfo
func DiffRegionKeyInfo(origin *RegionInfo, other *RegionInfo) string {
	var ret []string
	if !bytes.Equal(origin.Region.StartKey, other.Region.StartKey) {
		originKey := &metapb.Region{StartKey: origin.Region.StartKey}
		otherKey := &metapb.Region{StartKey: other.Region.StartKey}
		ret = append(ret, fmt.Sprintf("StartKey Changed:{%s} -> {%s}", originKey, otherKey))
	}
	if !bytes.Equal(origin.Region.EndKey, other.Region.EndKey) {
		originKey := &metapb.Region{EndKey: origin.Region.EndKey}
		otherKey := &metapb.Region{EndKey: other.Region.EndKey}
		ret = append(ret, fmt.Sprintf("EndKey Changed:{%s} -> {%s}", originKey, otherKey))
	}

	return strings.Join(ret, ",")
}
