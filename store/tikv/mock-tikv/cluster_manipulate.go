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
func BootstrapWithSingleStore(cluster *Cluster) {
	ids := cluster.AllocIDs(2)
	storeID, regionID := ids[0], ids[1]
	cluster.AddStore(storeID, fmt.Sprintf("store%d", storeID))
	cluster.Bootstrap(regionID, []uint64{storeID}, storeID)
}
