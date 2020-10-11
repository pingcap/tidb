// Copyright 2020 PingCAP, Inc.
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

package unistore

import (
	"fmt"
	"sync"
	"time"

	us "github.com/ngaut/unistore/tikv"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/store/mockstore/cluster"
	"github.com/pingcap/tidb/util/codec"
)

type delayKey struct {
	startTS  uint64
	regionID uint64
}

var _ cluster.Cluster = new(Cluster)

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
	*us.MockRegionManager

	// delayEvents is used to control the execution sequence of rpc requests for test.
	delayEvents map[delayKey]time.Duration
	delayMu     sync.Mutex
}

func newCluster(rm *us.MockRegionManager) *Cluster {
	return &Cluster{
		MockRegionManager: rm,
		delayEvents:       make(map[delayKey]time.Duration),
	}
}

// ScheduleDelay schedules a delay event for a transaction on a region.
func (c *Cluster) ScheduleDelay(startTS, regionID uint64, dur time.Duration) {
	c.delayMu.Lock()
	c.delayEvents[delayKey{startTS: startTS, regionID: regionID}] = dur
	c.delayMu.Unlock()
}

func (c *Cluster) handleDelay(startTS, regionID uint64) {
	key := delayKey{startTS: startTS, regionID: regionID}
	c.delayMu.Lock()
	dur, ok := c.delayEvents[key]
	if ok {
		delete(c.delayEvents, key)
	}
	c.delayMu.Unlock()
	if ok {
		time.Sleep(dur)
	}
}

// SplitRaw splits region for raw KV.
func (c *Cluster) SplitRaw(regionID, newRegionID uint64, rawKey []byte, peerIDs []uint64, leaderPeerID uint64) *metapb.Region {
	encodedKey := codec.EncodeBytes(nil, rawKey)
	return c.MockRegionManager.SplitRaw(regionID, newRegionID, encodedKey, peerIDs, leaderPeerID)
}

// BootstrapWithSingleStore initializes a Cluster with 1 Region and 1 Store.
func BootstrapWithSingleStore(cluster *Cluster) (storeID, peerID, regionID uint64) {
	storeID, regionID, peerID = cluster.AllocID(), cluster.AllocID(), cluster.AllocID()
	store := &metapb.Store{
		Id:      storeID,
		Address: fmt.Sprintf("store%d", storeID),
	}
	region := &metapb.Region{
		Id:          regionID,
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers:       []*metapb.Peer{{Id: peerID, StoreId: storeID}},
	}
	if err := cluster.Bootstrap([]*metapb.Store{store}, region); err != nil {
		panic(err)
	}
	return
}

// BootstrapWithMultiStores initializes a Cluster with 1 Region and n Stores.
func BootstrapWithMultiStores(cluster *Cluster, n int) (storeIDs, peerIDs []uint64, regionID uint64, leaderPeer uint64) {
	storeIDs = cluster.AllocIDs(n)
	peerIDs = cluster.AllocIDs(n)
	leaderPeer = peerIDs[0]
	regionID = cluster.AllocID()
	stores := make([]*metapb.Store, n)
	for i, storeID := range storeIDs {
		stores[i] = &metapb.Store{
			Id:      storeID,
			Address: fmt.Sprintf("store%d", storeID),
		}
	}
	peers := make([]*metapb.Peer, n)
	for i, peerID := range peerIDs {
		peers[i] = &metapb.Peer{
			Id:      peerID,
			StoreId: storeIDs[i],
		}
	}
	region := &metapb.Region{
		Id:          regionID,
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers:       peers,
	}
	if err := cluster.Bootstrap(stores, region); err != nil {
		panic(err)
	}
	return
}

// BootstrapWithMultiRegions initializes a Cluster with multiple Regions and 1
// Store. The number of Regions will be len(splitKeys) + 1.
func BootstrapWithMultiRegions(cluster *Cluster, splitKeys ...[]byte) (storeID uint64, regionIDs, peerIDs []uint64) {
	var firstRegionID, firstPeerID uint64
	storeID, firstPeerID, firstRegionID = BootstrapWithSingleStore(cluster)
	regionIDs = append([]uint64{firstRegionID}, cluster.AllocIDs(len(splitKeys))...)
	peerIDs = append([]uint64{firstPeerID}, cluster.AllocIDs(len(splitKeys))...)
	for i, k := range splitKeys {
		cluster.Split(regionIDs[i], regionIDs[i+1], k, []uint64{peerIDs[i]}, peerIDs[i])
	}
	return
}
