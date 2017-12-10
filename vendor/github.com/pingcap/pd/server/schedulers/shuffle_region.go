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
	schedule.RegisterScheduler("shuffle-region", func(opt schedule.Options, limiter *schedule.Limiter, args []string) (schedule.Scheduler, error) {
		return newShuffleRegionScheduler(opt, limiter), nil
	})
}

type shuffleRegionScheduler struct {
	*baseScheduler
	opt      schedule.Options
	selector schedule.Selector
}

// newShuffleRegionScheduler creates an admin scheduler that shuffles regions
// between stores.
func newShuffleRegionScheduler(opt schedule.Options, limiter *schedule.Limiter) schedule.Scheduler {
	filters := []schedule.Filter{
		schedule.NewStateFilter(opt),
		schedule.NewHealthFilter(opt),
	}
	base := newBaseScheduler(limiter)
	return &shuffleRegionScheduler{
		baseScheduler: base,
		opt:           opt,
		selector:      schedule.NewRandomSelector(filters),
	}
}

func (s *shuffleRegionScheduler) GetName() string {
	return "shuffle-region-scheduler"
}

func (s *shuffleRegionScheduler) GetType() string {
	return "shuffle-region"
}

func (s *shuffleRegionScheduler) IsScheduleAllowed() bool {
	return s.limiter.OperatorCount(core.RegionKind) < s.opt.GetRegionScheduleLimit()
}

func (s *shuffleRegionScheduler) Schedule(cluster schedule.Cluster, opInfluence schedule.OpInfluence) *schedule.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	region, oldPeer := scheduleRemovePeer(cluster, s.GetName(), s.selector)
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_region").Inc()
		return nil
	}

	excludedFilter := schedule.NewExcludedFilter(nil, region.GetStoreIds())
	newPeer := scheduleAddPeer(cluster, s.selector, excludedFilter)
	if newPeer == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_new_peer").Inc()
		return nil
	}

	schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
	op := schedule.CreateMovePeerOperator("shuffle-region", region, core.RegionKind, oldPeer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
	op.SetPriorityLevel(core.HighPriority)
	return op
}
