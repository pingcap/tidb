package mocktikv

import "fmt"

// BootstrapWithSingleStore initializes a Cluster with 1 Region and 1 Store.
func BootstrapWithSingleStore(cluster *Cluster) {
	ids := cluster.AllocIDs(2)
	storeID, regionID := ids[0], ids[1]
	cluster.AddStore(storeID, fmt.Sprintf("store%d", storeID))
	cluster.Bootstrap(regionID, []uint64{storeID}, storeID)
}
