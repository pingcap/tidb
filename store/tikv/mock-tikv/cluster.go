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

package mocktikv

import (
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
)

// Cluster simulates a TiKV cluster. It focuses on management and the change of
// meta data. A Cluster mainly includes following 3 kinds of meta data:
// 1) Region: A Region is a fragment of TiKV's data whose range is [start, end).
//    The data of a Region is duplicated to multiple Peers and distributed in
//    multiple Stores.
// 2) Peer: A Peer is a replica of a Region's data. All peers of a Region form
//    a group, each group elects a Leader to provide services.
// 3) Store: A Store is a storage/service node. Try to think it as a TiKV server
//    process. Only the store with request's Region's leader Peer could respond
//    to client's request.
type Cluster struct {
	mu      sync.RWMutex
	id      uint64
	stores  map[uint64]*Store
	regions map[uint64]*Region
}

// NewCluster creates an empty cluster. It needs to be bootstrapped before
// providing service.
func NewCluster() *Cluster {
	return &Cluster{
		stores:  make(map[uint64]*Store),
		regions: make(map[uint64]*Region),
	}
}

// AllocID creates an unique ID in cluster. The ID could be used as either
// StoreID, RegionID, or PeerID.
func (c *Cluster) AllocID() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.allocID()
}

// AllocIDs creates multiple IDs.
func (c *Cluster) AllocIDs(n int) []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	var ids []uint64
	for len(ids) < n {
		ids = append(ids, c.allocID())
	}
	return ids
}

func (c *Cluster) allocID() uint64 {
	c.id++
	return c.id
}

// GetStore returns a Store's meta.
func (c *Cluster) GetStore(storeID uint64) *metapb.Store {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if store := c.stores[storeID]; store != nil {
		return proto.Clone(store.meta).(*metapb.Store)
	}
	return nil
}

// GetStoreByAddr returns a Store's meta by an addr.
func (c *Cluster) GetStoreByAddr(addr string) *metapb.Store {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, s := range c.stores {
		if s.meta.GetAddress() == addr {
			return proto.Clone(s.meta).(*metapb.Store)
		}
	}
	return nil
}

// AddStore add a new Store to the cluster.
func (c *Cluster) AddStore(storeID uint64, addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stores[storeID] = newStore(storeID, addr)
}

// RemoveStore removes a Store from the cluster.
func (c *Cluster) RemoveStore(storeID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.stores, storeID)
}

// GetRegion returns a Region's meta and leader ID.
func (c *Cluster) GetRegion(regionID uint64) (*metapb.Region, uint64) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	r := c.regions[regionID]
	if r == nil {
		return nil, 0
	}
	return proto.Clone(r.meta).(*metapb.Region), r.leader
}

// GetRegionByKey returns the Region whose range contains the key.
func (c *Cluster) GetRegionByKey(key []byte) *metapb.Region {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, r := range c.regions {
		if regionContains(r.meta.StartKey, r.meta.EndKey, key) {
			return proto.Clone(r.meta).(*metapb.Region)
		}
	}
	return nil
}

// Bootstrap creates the first Region. The Stores should be in the Cluster before
// bootstrap.
func (c *Cluster) Bootstrap(regionID uint64, storeIDs, peerIDs []uint64, leaderStoreID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(storeIDs) != len(peerIDs) {
		panic("len(storeIDs) != len(peerIDs)")
	}
	c.regions[regionID] = newRegion(regionID, storeIDs, peerIDs, leaderStoreID)
}

// AddPeer adds a new Peer for the Region on the Store.
func (c *Cluster) AddPeer(regionID, storeID, peerID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.regions[regionID].addPeer(peerID, storeID)
}

// RemovePeer removes the Peer from the Region. Note that if the Peer is leader,
// the Region will have no leader before calling ChangeLeader().
func (c *Cluster) RemovePeer(regionID, storeID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.regions[regionID].removePeer(storeID)
}

// ChangeLeader sets the Region's leader Peer. Caller should guarantee the Peer
// exists.
func (c *Cluster) ChangeLeader(regionID, leaderStoreID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.regions[regionID].changeLeader(leaderStoreID)
}

// GiveUpLeader sets the Region's leader to 0. The Region will have no leader
// before calling ChangeLeader().
func (c *Cluster) GiveUpLeader(regionID uint64) {
	c.ChangeLeader(regionID, 0)
}

// Split splits a Region at the key and creates new Region.
func (c *Cluster) Split(regionID, newRegionID uint64, key []byte, peerIDs []uint64, leaderPeerID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	newRegion := c.regions[regionID].split(newRegionID, key, peerIDs, leaderPeerID)
	c.regions[newRegionID] = newRegion
}

// Merge merges 2 Regions, their key ranges should be adjacent.
func (c *Cluster) Merge(regionID1, regionID2 uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.regions[regionID1].merge(c.regions[regionID2].meta.GetEndKey())
	delete(c.regions, regionID2)
}

// Region is the Region meta data.
type Region struct {
	meta   *metapb.Region
	leader uint64
}

func newPeerMeta(peerID, storeID uint64) *metapb.Peer {
	return &metapb.Peer{
		Id:      proto.Uint64(peerID),
		StoreId: proto.Uint64(storeID),
	}
}

func newRegion(regionID uint64, storeIDs, peerIDs []uint64, leaderPeerID uint64) *Region {
	if len(storeIDs) != len(peerIDs) {
		panic("len(storeIDs) != len(peerIds)")
	}
	var peers []*metapb.Peer
	for i := range storeIDs {
		peers = append(peers, newPeerMeta(peerIDs[i], storeIDs[i]))
	}
	meta := &metapb.Region{
		Id:    proto.Uint64(regionID),
		Peers: peers,
	}
	return &Region{
		meta:   meta,
		leader: leaderPeerID,
	}
}

func (r *Region) addPeer(peerID, storeID uint64) {
	r.meta.Peers = append(r.meta.Peers, newPeerMeta(peerID, storeID))
	r.incConfVer()
}

func (r *Region) removePeer(peerID uint64) {
	for i, peer := range r.meta.Peers {
		if peer.GetId() == peerID {
			r.meta.Peers = append(r.meta.Peers[:i], r.meta.Peers[i+1:]...)
			break
		}
	}
	if r.leader == peerID {
		r.leader = 0
	}
	r.incConfVer()
}

func (r *Region) changeLeader(leaderStoreID uint64) {
	r.leader = leaderStoreID
}

func (r *Region) split(newRegionID uint64, key []byte, peerIDs []uint64, leaderPeerID uint64) *Region {
	if len(r.meta.Peers) != len(peerIDs) {
		panic("len(r.meta.Peers) != len(peerIDs)")
	}
	var storeIDs []uint64
	for _, peer := range r.meta.Peers {
		storeIDs = append(storeIDs, peer.GetStoreId())
	}
	region := newRegion(newRegionID, storeIDs, peerIDs, leaderPeerID)
	region.updateKeyRange(key, r.meta.EndKey)
	r.updateKeyRange(r.meta.StartKey, key)
	return region
}

func (r *Region) merge(endKey []byte) {
	r.meta.EndKey = endKey
	r.incVersion()
}

func (r *Region) updateKeyRange(start, end []byte) {
	r.meta.StartKey = start
	r.meta.EndKey = end
	r.incVersion()
}

func (r *Region) incConfVer() {
	r.meta.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: proto.Uint64(r.meta.GetRegionEpoch().GetConfVer() + 1),
		Version: proto.Uint64(r.meta.GetRegionEpoch().GetVersion()),
	}
}

func (r *Region) incVersion() {
	r.meta.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: proto.Uint64(r.meta.GetRegionEpoch().GetConfVer()),
		Version: proto.Uint64(r.meta.GetRegionEpoch().GetVersion() + 1),
	}
}

// Store is the Store's meta data.
type Store struct {
	meta *metapb.Store
}

func newStore(storeID uint64, addr string) *Store {
	return &Store{
		meta: &metapb.Store{
			Id:      proto.Uint64(storeID),
			Address: proto.String(addr),
		},
	}
}
