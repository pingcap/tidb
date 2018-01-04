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
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

func init() {
	schedule.RegisterScheduler("balance-leader", func(limiter *schedule.Limiter, args []string) (schedule.Scheduler, error) {
		return newBalanceLeaderScheduler(limiter), nil
	})
}

type balanceLeaderScheduler struct {
	*baseScheduler
	limit    uint64
	selector schedule.Selector
}

// newBalanceLeaderScheduler creates a scheduler that tends to keep leaders on
// each store balanced.
func newBalanceLeaderScheduler(limiter *schedule.Limiter) schedule.Scheduler {
	filters := []schedule.Filter{
		schedule.NewBlockFilter(),
		schedule.NewStateFilter(),
		schedule.NewHealthFilter(),
	}
	base := newBaseScheduler(limiter)
	return &balanceLeaderScheduler{
		baseScheduler: base,
		limit:         1,
		selector:      schedule.NewBalanceSelector(core.LeaderKind, filters),
	}
}

func (l *balanceLeaderScheduler) GetName() string {
	return "balance-leader-scheduler"
}

func (l *balanceLeaderScheduler) GetType() string {
	return "balance-leader"
}

func (l *balanceLeaderScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	limit := minUint64(l.limit, cluster.GetLeaderScheduleLimit())
	return l.limiter.OperatorCount(schedule.OpLeader) < limit
}

func (l *balanceLeaderScheduler) Schedule(cluster schedule.Cluster, opInfluence schedule.OpInfluence) *schedule.Operator {
	schedulerCounter.WithLabelValues(l.GetName(), "schedule").Inc()
	region, newLeader := scheduleTransferLeader(cluster, l.GetName(), l.selector)
	if region == nil {
		return nil
	}

	// Skip hot regions.
	if cluster.IsRegionHot(region.GetId()) {
		schedulerCounter.WithLabelValues(l.GetName(), "region_hot").Inc()
		return nil
	}

	source := cluster.GetStore(region.Leader.GetStoreId())
	target := cluster.GetStore(newLeader.GetStoreId())
	avgScore := cluster.GetStoresAverageScore(core.LeaderKind)
	if !shouldBalance(source, target, avgScore, core.LeaderKind, region, opInfluence, cluster.GetTolerantSizeRatio()) {
		schedulerCounter.WithLabelValues(l.GetName(), "skip").Inc()
		return nil
	}
	l.limit = adjustBalanceLimit(cluster, core.LeaderKind)
	schedulerCounter.WithLabelValues(l.GetName(), "new_operator").Inc()
	step := schedule.TransferLeader{FromStore: region.Leader.GetStoreId(), ToStore: newLeader.GetStoreId()}
	return schedule.NewOperator("balance-leader", region.GetId(), schedule.OpBalance|schedule.OpLeader, step)
}
