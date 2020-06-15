// Copyright 2018 PingCAP, Inc.
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

package cluster

import (
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/kv"
)

// Cluster simulates a TiKV cluster.
// It can be used to change cluster states in tests.
type Cluster interface {
	// AllocID creates an unique ID in cluster. The ID could be used as either
	// StoreID, RegionID, or PeerID.
	AllocID() uint64
	// GetRegionByKey returns the Region and its leader whose range contains the key.
	GetRegionByKey(key []byte) (*metapb.Region, *metapb.Peer)
	// GetAllStores returns all Stores' meta.
	GetAllStores() []*metapb.Store
	// ScheduleDelay schedules a delay event for a transaction on a region.
	ScheduleDelay(startTS, regionID uint64, dur time.Duration)
	// Split splits a Region at the key (encoded) and creates new Region.
	Split(regionID, newRegionID uint64, key []byte, peerIDs []uint64, leaderPeerID uint64)
	// SplitRaw splits a Region at the key (not encoded) and creates new Region.
	SplitRaw(regionID, newRegionID uint64, rawKey []byte, peerIDs []uint64, leaderPeerID uint64) *metapb.Region
	// SplitTable evenly splits the data in table into count regions.
	SplitTable(tableID int64, count int)
	// SplitIndex evenly splits the data in index into count regions.
	SplitIndex(tableID, indexID int64, count int)
	// SplitKeys evenly splits the start, end key into "count" regions.
	SplitKeys(start, end kv.Key, count int)
	// AddStore adds a new Store to the cluster.
	AddStore(storeID uint64, addr string)
	// RemoveStore removes a Store from the cluster.
	RemoveStore(storeID uint64)
}
