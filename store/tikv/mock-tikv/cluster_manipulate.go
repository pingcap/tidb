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

import "fmt"

// BootstrapWithSingleStore initializes a Cluster with 1 Region and 1 Store.
func BootstrapWithSingleStore(cluster *Cluster) (storeID, regionID uint64) {
	ids := cluster.AllocIDs(2)
	storeID, regionID = ids[0], ids[1]
	cluster.AddStore(storeID, fmt.Sprintf("store%d", storeID))
	cluster.Bootstrap(regionID, []uint64{storeID}, storeID)
	return
}

// BootstrapWithMultiStores initializes a Cluster with 1 Region and n Stores.
func BootstrapWithMultiStores(cluster *Cluster, n int) (storeIDs []uint64, regionID uint64, leaderStore uint64) {
	ids := cluster.AllocIDs(n + 1)
	regionID, leaderStore, storeIDs = ids[0], ids[1], ids[1:]
	for _, storeID := range storeIDs {
		cluster.AddStore(storeID, fmt.Sprintf("store%d", storeID))
	}
	cluster.Bootstrap(regionID, storeIDs, leaderStore)
	return
}

// BoostrapWithMultiRegions initializes a Cluster with multiple Regions and 1
// Store. The number of Regions will be len(splitKeys) + 1.
func BootstrapWithMultiRegions(cluster *Cluster, splitKeys ...[]byte) (storeID uint64, regionIDs []uint64) {
	var firstRegionID uint64
	storeID, firstRegionID = BootstrapWithSingleStore(cluster)
	regionIDs = append([]uint64{firstRegionID}, cluster.AllocIDs(len(splitKeys))...)
	for i, k := range splitKeys {
		cluster.Split(regionIDs[i], regionIDs[i+1], k, storeID)
	}
	return
}
