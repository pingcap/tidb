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
	"math"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	log "github.com/sirupsen/logrus"
)

// scheduleTransferLeader schedules a region to transfer leader to the peer.
func scheduleTransferLeader(cluster schedule.Cluster, schedulerName string, s schedule.Selector, filters ...schedule.Filter) (region *core.RegionInfo, peer *metapb.Peer) {
	stores := cluster.GetStores()
	if len(stores) == 0 {
		schedulerCounter.WithLabelValues(schedulerName, "no_store").Inc()
		return nil, nil
	}

	var averageLeader float64
	for _, s := range stores {
		averageLeader += float64(s.LeaderScore()) / float64(len(stores))
	}

	mostLeaderStore := s.SelectSource(cluster, stores, filters...)
	leastLeaderStore := s.SelectTarget(cluster, stores, filters...)

	var mostLeaderDistance, leastLeaderDistance float64
	if mostLeaderStore != nil {
		mostLeaderDistance = math.Abs(mostLeaderStore.LeaderScore() - averageLeader)
	}
	if leastLeaderStore != nil {
		leastLeaderDistance = math.Abs(leastLeaderStore.LeaderScore() - averageLeader)
	}
	if mostLeaderDistance == 0 && leastLeaderDistance == 0 {
		schedulerCounter.WithLabelValues(schedulerName, "already_balanced").Inc()
		return nil, nil
	}

	if mostLeaderDistance > leastLeaderDistance {
		region, peer = scheduleRemoveLeader(cluster, schedulerName, mostLeaderStore.GetId(), s)
		if region == nil {
			region, peer = scheduleAddLeader(cluster, schedulerName, leastLeaderStore.GetId())
		}
	} else {
		region, peer = scheduleAddLeader(cluster, schedulerName, leastLeaderStore.GetId())
		if region == nil {
			region, peer = scheduleRemoveLeader(cluster, schedulerName, mostLeaderStore.GetId(), s)
		}
	}

	return region, peer
}

// scheduleAddLeader transfers a leader into the store.
func scheduleAddLeader(cluster schedule.Cluster, schedulerName string, storeID uint64) (*core.RegionInfo, *metapb.Peer) {
	region := cluster.RandFollowerRegion(storeID)
	if region == nil {
		schedulerCounter.WithLabelValues(schedulerName, "no_target_peer").Inc()
		return nil, nil
	}
	return region, region.GetStorePeer(storeID)
}

// scheduleRemoveLeader transfers a leader out of the store.
func scheduleRemoveLeader(cluster schedule.Cluster, schedulerName string, storeID uint64, s schedule.Selector) (*core.RegionInfo, *metapb.Peer) {
	region := cluster.RandLeaderRegion(storeID)
	if region == nil {
		schedulerCounter.WithLabelValues(schedulerName, "no_leader_region").Inc()
		return nil, nil
	}
	targetStores := cluster.GetFollowerStores(region)
	target := s.SelectTarget(cluster, targetStores)
	if target == nil {
		schedulerCounter.WithLabelValues(schedulerName, "no_target_store").Inc()
		return nil, nil
	}

	return region, region.GetStorePeer(target.GetId())
}

// scheduleRemovePeer schedules a region to remove the peer.
func scheduleRemovePeer(cluster schedule.Cluster, schedulerName string, s schedule.Selector, filters ...schedule.Filter) (*core.RegionInfo, *metapb.Peer) {
	stores := cluster.GetStores()

	source := s.SelectSource(cluster, stores, filters...)
	if source == nil {
		schedulerCounter.WithLabelValues(schedulerName, "no_store").Inc()
		return nil, nil
	}

	region := cluster.RandFollowerRegion(source.GetId())
	if region == nil {
		region = cluster.RandLeaderRegion(source.GetId())
	}
	if region == nil {
		schedulerCounter.WithLabelValues(schedulerName, "no_region").Inc()
		return nil, nil
	}

	return region, region.GetStorePeer(source.GetId())
}

// scheduleAddPeer schedules a new peer.
func scheduleAddPeer(cluster schedule.Cluster, s schedule.Selector, filters ...schedule.Filter) *metapb.Peer {
	stores := cluster.GetStores()

	target := s.SelectTarget(cluster, stores, filters...)
	if target == nil {
		return nil
	}

	newPeer, err := cluster.AllocPeer(target.GetId())
	if err != nil {
		log.Errorf("failed to allocate peer: %v", err)
		return nil
	}

	return newPeer
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func takeInfluence(store *core.StoreInfo, storeInfluence *schedule.StoreInfluence) {
	store.LeaderCount += storeInfluence.LeaderCount
	store.RegionCount += storeInfluence.RegionCount
	store.LeaderSize += int64(storeInfluence.LeaderSize)
	store.RegionSize += int64(storeInfluence.RegionSize)
}

// shouldBalance returns true if we should balance the source and target store.
// The tolerantRatio provides a buffer to make the cluster stable, so that we
// don't need to schedule very frequently.
// TODO: simplify the arguments
func shouldBalance(source, target *core.StoreInfo, avgScore float64, kind core.ResourceKind, region *core.RegionInfo, opInfluence schedule.OpInfluence, tolerantRatio float64) bool {
	takeInfluence(source, opInfluence.GetStoreInfluence(source.GetId()))
	takeInfluence(target, opInfluence.GetStoreInfluence(target.GetId()))

	sourceScore := source.ResourceScore(kind)
	targetScore := target.ResourceScore(kind)
	if targetScore >= sourceScore {
		return false
	}

	// avgScore is the goal for every store
	// in expection, sourceScore > avgScore > targetScore
	// if not, moving region is not necessary
	// In this case, either sourceSizeDiff or targetSizeDiff will be negative, then obviously return false
	sourceSizeDiff := (sourceScore - avgScore) * source.ResourceWeight(kind)
	targetSizeDiff := (avgScore - targetScore) * target.ResourceWeight(kind)

	return math.Min(sourceSizeDiff, targetSizeDiff) >= float64(region.ApproximateSize)*tolerantRatio
}

func adjustBalanceLimit(cluster schedule.Cluster, kind core.ResourceKind) uint64 {
	stores := cluster.GetStores()
	counts := make([]float64, 0, len(stores))
	for _, s := range stores {
		if s.IsUp() {
			counts = append(counts, float64(s.ResourceCount(kind)))
		}
	}
	limit, _ := stats.StandardDeviation(stats.Float64Data(counts))
	return maxUint64(1, uint64(limit))
}
