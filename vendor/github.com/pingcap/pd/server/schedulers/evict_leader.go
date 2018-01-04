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
	"fmt"
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

func init() {
	schedule.RegisterScheduler("evict-leader", func(limiter *schedule.Limiter, args []string) (schedule.Scheduler, error) {
		if len(args) != 1 {
			return nil, errors.New("evict-leader needs 1 argument")
		}
		id, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return newEvictLeaderScheduler(limiter, id), nil
	})
}

type evictLeaderScheduler struct {
	*baseScheduler
	name     string
	storeID  uint64
	selector schedule.Selector
}

// newEvictLeaderScheduler creates an admin scheduler that transfers all leaders
// out of a store.
func newEvictLeaderScheduler(limiter *schedule.Limiter, storeID uint64) schedule.Scheduler {
	filters := []schedule.Filter{
		schedule.NewStateFilter(),
		schedule.NewHealthFilter(),
	}
	base := newBaseScheduler(limiter)
	return &evictLeaderScheduler{
		baseScheduler: base,
		name:          fmt.Sprintf("evict-leader-scheduler-%d", storeID),
		storeID:       storeID,
		selector:      schedule.NewRandomSelector(filters),
	}
}

func (s *evictLeaderScheduler) GetName() string {
	return s.name
}

func (s *evictLeaderScheduler) GetType() string {
	return "evict-leader"
}

func (s *evictLeaderScheduler) Prepare(cluster schedule.Cluster) error {
	return errors.Trace(cluster.BlockStore(s.storeID))
}

func (s *evictLeaderScheduler) Cleanup(cluster schedule.Cluster) {
	cluster.UnblockStore(s.storeID)
}

func (s *evictLeaderScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return s.limiter.OperatorCount(schedule.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (s *evictLeaderScheduler) Schedule(cluster schedule.Cluster, opInfluence schedule.OpInfluence) *schedule.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	region := cluster.RandLeaderRegion(s.storeID)
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_leader").Inc()
		return nil
	}
	target := s.selector.SelectTarget(cluster, cluster.GetFollowerStores(region))
	if target == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_target_store").Inc()
		return nil
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
	step := schedule.TransferLeader{FromStore: region.Leader.GetStoreId(), ToStore: target.GetId()}
	op := schedule.NewOperator("evict-leader", region.GetId(), schedule.OpLeader, step)
	op.SetPriorityLevel(core.HighPriority)
	return op
}
