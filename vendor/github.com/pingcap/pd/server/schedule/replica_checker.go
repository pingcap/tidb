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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	log "github.com/sirupsen/logrus"
)

// ReplicaChecker ensures region has the best replicas.
type ReplicaChecker struct {
	cluster    Cluster
	classifier namespace.Classifier
	filters    []Filter
}

// NewReplicaChecker creates a replica checker.
func NewReplicaChecker(cluster Cluster, classifier namespace.Classifier) *ReplicaChecker {
	filters := []Filter{
		NewHealthFilter(),
		NewSnapshotCountFilter(),
	}

	return &ReplicaChecker{
		cluster:    cluster,
		classifier: classifier,
		filters:    filters,
	}
}

// Check verifies a region's replicas, creating an Operator if need.
func (r *ReplicaChecker) Check(region *core.RegionInfo) *Operator {
	if op := r.checkDownPeer(region); op != nil {
		return op
	}
	if op := r.checkOfflinePeer(region); op != nil {
		return op
	}

	if len(region.GetPeers()) < r.cluster.GetMaxReplicas() {
		newPeer := r.SelectBestPeerToAddReplica(region, r.filters...)
		if newPeer == nil {
			return nil
		}
		step := AddPeer{ToStore: newPeer.GetStoreId(), PeerID: newPeer.GetId()}
		return NewOperator("makeUpReplica", region.GetId(), OpReplica|OpRegion, step)
	}

	if len(region.GetPeers()) > r.cluster.GetMaxReplicas() {
		oldPeer, _ := r.selectWorstPeer(region)
		if oldPeer == nil {
			return nil
		}
		return CreateRemovePeerOperator("removeExtraReplica", OpReplica, region, oldPeer.GetStoreId())
	}

	return r.checkBestReplacement(region)
}

// SelectBestPeerToAddReplica returns a new peer that to be used to add a replica.
func (r *ReplicaChecker) SelectBestPeerToAddReplica(region *core.RegionInfo, filters ...Filter) *metapb.Peer {
	storeID, _ := r.SelectBestStoreToAddReplica(region, filters...)
	if storeID == 0 {
		return nil
	}
	newPeer, err := r.cluster.AllocPeer(storeID)
	if err != nil {
		return nil
	}
	return newPeer
}

// SelectBestStoreToAddReplica returns the store to add a replica.
func (r *ReplicaChecker) SelectBestStoreToAddReplica(region *core.RegionInfo, filters ...Filter) (uint64, float64) {
	// Add some must have filters.
	newFilters := []Filter{
		NewStateFilter(),
		NewStorageThresholdFilter(),
		NewExcludedFilter(nil, region.GetStoreIds()),
	}
	filters = append(filters, newFilters...)

	if r.classifier != nil {
		filters = append(filters, NewNamespaceFilter(r.classifier, r.classifier.GetRegionNamespace(region)))
	}

	regionStores := r.cluster.GetRegionStores(region)
	selector := NewReplicaSelector(regionStores, r.cluster.GetLocationLabels(), r.filters...)
	target := selector.SelectTarget(r.cluster, r.cluster.GetStores(), filters...)
	if target == nil {
		return 0, 0
	}
	return target.GetId(), DistinctScore(r.cluster.GetLocationLabels(), regionStores, target)
}

// selectWorstPeer returns the worst peer in the region.
func (r *ReplicaChecker) selectWorstPeer(region *core.RegionInfo) (*metapb.Peer, float64) {
	regionStores := r.cluster.GetRegionStores(region)
	selector := NewReplicaSelector(regionStores, r.cluster.GetLocationLabels(), r.filters...)
	worstStore := selector.SelectSource(r.cluster, regionStores)
	if worstStore == nil {
		return nil, 0
	}
	return region.GetStorePeer(worstStore.GetId()), DistinctScore(r.cluster.GetLocationLabels(), regionStores, worstStore)
}

// selectBestReplacement returns the best store to replace the region peer.
func (r *ReplicaChecker) selectBestReplacement(region *core.RegionInfo, peer *metapb.Peer) (uint64, float64) {
	// Get a new region without the peer we are going to replace.
	newRegion := region.Clone()
	newRegion.RemoveStorePeer(peer.GetStoreId())
	return r.SelectBestStoreToAddReplica(newRegion, NewExcludedFilter(nil, region.GetStoreIds()))
}

func (r *ReplicaChecker) checkDownPeer(region *core.RegionInfo) *Operator {
	for _, stats := range region.DownPeers {
		peer := stats.GetPeer()
		if peer == nil {
			continue
		}
		store := r.cluster.GetStore(peer.GetStoreId())
		if store == nil {
			log.Infof("lost the store %d,maybe you are recovering the PD cluster.", peer.GetStoreId())
			return nil
		}
		if store.DownTime() < r.cluster.GetMaxStoreDownTime() {
			continue
		}
		if stats.GetDownSeconds() < uint64(r.cluster.GetMaxStoreDownTime().Seconds()) {
			continue
		}
		return CreateRemovePeerOperator("removeDownReplica", OpReplica, region, peer.GetStoreId())
	}
	return nil
}

func (r *ReplicaChecker) checkOfflinePeer(region *core.RegionInfo) *Operator {
	for _, peer := range region.GetPeers() {
		store := r.cluster.GetStore(peer.GetStoreId())
		if store == nil {
			log.Infof("lost the store %d, maybe you are recovering the PD cluster.", peer.GetStoreId())
			return nil
		}
		if store.IsUp() {
			continue
		}

		// Check the number of replicas first.
		if len(region.GetPeers()) > r.cluster.GetMaxReplicas() {
			return CreateRemovePeerOperator("removeExtraOfflineReplica", OpReplica, region, peer.GetStoreId())
		}

		// Consider we have 3 peers (A, B, C), we set the store that contains C to
		// offline while C is pending. If we generate an operator that adds a replica
		// D then removes C, D will not be successfully added util C is normal again.
		// So it's better to remove C directly.
		if region.GetPendingPeer(peer.GetId()) != nil {
			return CreateRemovePeerOperator("removePendingOfflineReplica", OpReplica, region, peer.GetStoreId())
		}

		newPeer := r.SelectBestPeerToAddReplica(region)
		if newPeer == nil {
			return nil
		}
		return CreateMovePeerOperator("makeUpOfflineReplica", region, OpReplica, peer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
	}

	return nil
}

func (r *ReplicaChecker) checkBestReplacement(region *core.RegionInfo) *Operator {
	oldPeer, oldScore := r.selectWorstPeer(region)
	if oldPeer == nil {
		return nil
	}
	storeID, newScore := r.selectBestReplacement(region, oldPeer)
	if storeID == 0 {
		return nil
	}
	// Make sure the new peer is better than the old peer.
	if newScore <= oldScore {
		return nil
	}
	newPeer, err := r.cluster.AllocPeer(storeID)
	if err != nil {
		return nil
	}
	return CreateMovePeerOperator("moveToBetterLocation", region, OpReplica, oldPeer.GetStoreId(), storeID, newPeer.GetId())
}
