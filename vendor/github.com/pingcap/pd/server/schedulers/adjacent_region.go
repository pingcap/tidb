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
	"bytes"
	"strconv"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	log "github.com/sirupsen/logrus"
)

const (
	scanLimit                    = 1000
	defaultAdjacentPeerLimit     = 1
	defaultAdjacentLeaderLimit   = 64
	minAdjacentSchedulerInterval = time.Second
	maxAdjacentSchedulerInterval = 30 * time.Second
)

func init() {
	schedule.RegisterScheduler("adjacent-region", func(limiter *schedule.Limiter, args []string) (schedule.Scheduler, error) {
		if len(args) == 2 {
			leaderLimit, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return nil, errors.Trace(err)
			}
			peerLimit, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return newBalanceAdjacentRegionScheduler(limiter, leaderLimit, peerLimit), nil
		}
		return newBalanceAdjacentRegionScheduler(limiter), nil
	})
}

// balanceAdjacentRegionScheduler will disperse adjacent regions.
// we will scan a part regions order by key, then select the longest
// adjacent regions and disperse them. finally, we will guarantee
// 1. any two adjacent regions' leader will not in the same store
// 2. the two regions' leader will not in the public store of this two regions
type balanceAdjacentRegionScheduler struct {
	*baseScheduler
	selector             schedule.Selector
	leaderLimit          uint64
	peerLimit            uint64
	lastKey              []byte
	cacheRegions         *adjacentState
	adjacentRegionsCount int
}

type adjacentState struct {
	assignedStoreIds []uint64
	regions          []*core.RegionInfo
	head             int
}

func (a *adjacentState) clear() {
	a.assignedStoreIds = a.assignedStoreIds[:0]
	a.regions = a.regions[:0]
	a.head = 0
}

func (a *adjacentState) len() int {
	return len(a.regions) - a.head
}

// newBalanceAdjacentRegionScheduler creates a scheduler that tends to disperse adjacent region
// on each store.
func newBalanceAdjacentRegionScheduler(limiter *schedule.Limiter, args ...uint64) schedule.Scheduler {
	filters := []schedule.Filter{
		schedule.NewBlockFilter(),
		schedule.NewStateFilter(),
		schedule.NewHealthFilter(),
		schedule.NewSnapshotCountFilter(),
		schedule.NewStorageThresholdFilter(),
		schedule.NewPendingPeerCountFilter(),
	}
	base := newBaseScheduler(limiter)
	s := &balanceAdjacentRegionScheduler{
		baseScheduler: base,
		selector:      schedule.NewRandomSelector(filters),
		leaderLimit:   defaultAdjacentLeaderLimit,
		peerLimit:     defaultAdjacentPeerLimit,
		lastKey:       []byte(""),
	}
	if len(args) == 2 {
		s.leaderLimit = args[0]
		s.peerLimit = args[1]
	}
	return s
}

func (l *balanceAdjacentRegionScheduler) GetName() string {
	return "balance-adjacent-region-scheduler"
}

func (l *balanceAdjacentRegionScheduler) GetType() string {
	return "adjacent-region"
}

func (l *balanceAdjacentRegionScheduler) GetMinInterval() time.Duration {
	return minAdjacentSchedulerInterval
}

func (l *balanceAdjacentRegionScheduler) GetNextInterval(interval time.Duration) time.Duration {
	return intervalGrow(interval, maxAdjacentSchedulerInterval, linearGrowth)
}

func (l *balanceAdjacentRegionScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return l.allowBalanceLeader() || l.allowBalanceLeader()
}

func (l *balanceAdjacentRegionScheduler) allowBalanceLeader() bool {
	return l.limiter.OperatorCount(schedule.OpAdjacent|schedule.OpLeader) < l.leaderLimit
}

func (l *balanceAdjacentRegionScheduler) allowBalancePeer() bool {
	return l.limiter.OperatorCount(schedule.OpAdjacent|schedule.OpRegion) < l.peerLimit
}

func (l *balanceAdjacentRegionScheduler) Schedule(cluster schedule.Cluster, opInfluence schedule.OpInfluence) *schedule.Operator {
	if l.cacheRegions == nil {
		l.cacheRegions = &adjacentState{
			assignedStoreIds: make([]uint64, 0, len(cluster.GetStores())),
			regions:          make([]*core.RegionInfo, 0, scanLimit),
			head:             0,
		}
	}
	// we will process cache firstly
	if l.cacheRegions.len() >= 2 {
		return l.process(cluster)
	}

	l.cacheRegions.clear()
	regions := cluster.ScanRegions(l.lastKey, scanLimit)
	// scan to the end
	if len(regions) <= 1 {
		l.adjacentRegionsCount = 0
		schedulerStatus.WithLabelValues(l.GetName(), "adjacent_count").Set(float64(l.adjacentRegionsCount))
		l.lastKey = []byte("")
		return nil
	}

	// calculate max adjacentRegions and record to the cache
	adjacentRegions := make([]*core.RegionInfo, 0, scanLimit)
	adjacentRegions = append(adjacentRegions, regions[0])
	maxLen := 0
	for i, r := range regions[1:] {
		l.lastKey = r.StartKey

		// append if the region are adjacent
		lastRegion := adjacentRegions[len(adjacentRegions)-1]
		if lastRegion.Leader.GetStoreId() == r.Leader.GetStoreId() && bytes.Equal(lastRegion.EndKey, r.StartKey) {
			adjacentRegions = append(adjacentRegions, r)
			if i != len(regions)-2 { // not the last element
				continue
			}
		}

		if len(adjacentRegions) == 1 {
			adjacentRegions[0] = r
		} else {
			// got an max length adjacent regions in this range
			if maxLen < len(adjacentRegions) {
				l.cacheRegions.clear()
				maxLen = len(adjacentRegions)
				l.cacheRegions.regions = append(l.cacheRegions.regions, adjacentRegions...)
				adjacentRegions = adjacentRegions[:0]
				adjacentRegions = append(adjacentRegions, r)
			}
		}
	}

	l.adjacentRegionsCount += maxLen
	return l.process(cluster)
}

func (l *balanceAdjacentRegionScheduler) process(cluster schedule.Cluster) *schedule.Operator {
	if l.cacheRegions.len() < 2 {
		return nil
	}
	head := l.cacheRegions.head
	r1 := l.cacheRegions.regions[head]
	r2 := l.cacheRegions.regions[head+1]

	defer func() {
		if l.cacheRegions.len() < 0 {
			log.Fatalf("[%s]the cache overflow should never happen", l.GetName())
		}
		l.cacheRegions.head = head + 1
		l.lastKey = r2.StartKey
	}()
	if l.unsafeToBalance(cluster, r1) {
		schedulerCounter.WithLabelValues(l.GetName(), "skip").Inc()
		return nil
	}
	op := l.disperseLeader(cluster, r1, r2)
	if op == nil {
		schedulerCounter.WithLabelValues(l.GetName(), "no_leader").Inc()
		op = l.dispersePeer(cluster, r1)
	}
	if op == nil {
		schedulerCounter.WithLabelValues(l.GetName(), "no_peer").Inc()
		l.cacheRegions.assignedStoreIds = l.cacheRegions.assignedStoreIds[:0]
	}
	return op
}

func (l *balanceAdjacentRegionScheduler) unsafeToBalance(cluster schedule.Cluster, region *core.RegionInfo) bool {
	if len(region.GetPeers()) != cluster.GetMaxReplicas() {
		return true
	}
	store := cluster.GetStore(region.Leader.GetStoreId())
	s := l.selector.SelectSource(cluster, []*core.StoreInfo{store})
	if s == nil {
		return true
	}
	// Skip hot regions.
	if cluster.IsRegionHot(region.GetId()) {
		schedulerCounter.WithLabelValues(l.GetName(), "region_hot").Inc()
		return true
	}
	return false
}

func (l *balanceAdjacentRegionScheduler) disperseLeader(cluster schedule.Cluster, before *core.RegionInfo, after *core.RegionInfo) *schedule.Operator {
	if !l.allowBalanceLeader() {
		return nil
	}
	diffPeers := before.GetDiffFollowers(after)
	if len(diffPeers) == 0 {
		return nil
	}
	storesInfo := make([]*core.StoreInfo, 0, len(diffPeers))
	for _, p := range diffPeers {
		storesInfo = append(storesInfo, cluster.GetStore(p.GetStoreId()))
	}
	target := l.selector.SelectTarget(cluster, storesInfo)
	if target == nil {
		return nil
	}
	step := schedule.TransferLeader{FromStore: before.Leader.GetStoreId(), ToStore: target.GetId()}
	op := schedule.NewOperator("balance-adjacent-leader", before.GetId(), schedule.OpAdjacent|schedule.OpLeader, step)
	op.SetPriorityLevel(core.LowPriority)
	schedulerCounter.WithLabelValues(l.GetName(), "adjacent_leader").Inc()
	return op
}

func (l *balanceAdjacentRegionScheduler) dispersePeer(cluster schedule.Cluster, region *core.RegionInfo) *schedule.Operator {
	if !l.allowBalancePeer() {
		return nil
	}
	// scoreGuard guarantees that the distinct score will not decrease.
	leaderStoreID := region.Leader.GetStoreId()
	stores := cluster.GetRegionStores(region)
	source := cluster.GetStore(leaderStoreID)
	scoreGuard := schedule.NewDistinctScoreFilter(cluster.GetLocationLabels(), stores, source)
	excludeStores := region.GetStoreIds()
	for _, storeID := range l.cacheRegions.assignedStoreIds {
		if _, ok := excludeStores[storeID]; !ok {
			excludeStores[storeID] = struct{}{}
		}
	}

	filters := []schedule.Filter{
		schedule.NewExcludedFilter(nil, excludeStores),
		scoreGuard,
	}
	target := l.selector.SelectTarget(cluster, cluster.GetStores(), filters...)
	if target == nil {
		return nil
	}
	newPeer, err := cluster.AllocPeer(target.GetId())
	if err != nil {
		return nil
	}
	if newPeer == nil {
		schedulerCounter.WithLabelValues(l.GetName(), "no_peer").Inc()
		return nil
	}

	// record the store id and exclude it in next time
	l.cacheRegions.assignedStoreIds = append(l.cacheRegions.assignedStoreIds, newPeer.GetStoreId())

	op := schedule.CreateMovePeerOperator("balance-adjacent-peer", region, schedule.OpAdjacent, leaderStoreID, newPeer.GetStoreId(), newPeer.GetId())
	op.SetPriorityLevel(core.LowPriority)
	schedulerCounter.WithLabelValues(l.GetName(), "adjacent_peer").Inc()
	return op
}
