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
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

func init() {
	schedule.RegisterScheduler("grant-leader", func(limiter *schedule.Limiter, args []string) (schedule.Scheduler, error) {
		if len(args) != 1 {
			return nil, errors.New("grant-leader needs 1 argument")
		}
		id, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return newGrantLeaderScheduler(limiter, id), nil
	})
}

const scheduleInterval = time.Millisecond * 10

// grantLeaderScheduler transfers all leaders to peers in the store.
type grantLeaderScheduler struct {
	*baseScheduler
	name    string
	storeID uint64
}

// newGrantLeaderScheduler creates an admin scheduler that transfers all leaders
// to a store.
func newGrantLeaderScheduler(limiter *schedule.Limiter, storeID uint64) schedule.Scheduler {
	base := newBaseScheduler(limiter)
	return &grantLeaderScheduler{
		baseScheduler: base,
		name:          fmt.Sprintf("grant-leader-scheduler-%d", storeID),
		storeID:       storeID,
	}
}

func (s *grantLeaderScheduler) GetName() string {
	return s.name
}

func (s *grantLeaderScheduler) GetType() string {
	return "grant-leader"
}
func (s *grantLeaderScheduler) Prepare(cluster schedule.Cluster) error {
	return errors.Trace(cluster.BlockStore(s.storeID))
}

func (s *grantLeaderScheduler) Cleanup(cluster schedule.Cluster) {
	cluster.UnblockStore(s.storeID)
}

func (s *grantLeaderScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return s.limiter.OperatorCount(schedule.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (s *grantLeaderScheduler) Schedule(cluster schedule.Cluster, opInfluence schedule.OpInfluence) *schedule.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	region := cluster.RandFollowerRegion(s.storeID)
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_follower").Inc()
		return nil
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
	step := schedule.TransferLeader{FromStore: region.Leader.GetStoreId(), ToStore: s.storeID}
	op := schedule.NewOperator("grant-leader", region.GetId(), schedule.OpLeader, step)
	op.SetPriorityLevel(core.HighPriority)
	return op
}
