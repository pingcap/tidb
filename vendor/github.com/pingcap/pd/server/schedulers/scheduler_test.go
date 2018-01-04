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

package schedulers

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server/schedule"
)

var _ = Suite(&testShuffleLeaderSuite{})

type testShuffleLeaderSuite struct{}

func (s *testShuffleLeaderSuite) TestShuffle(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	sl, err := schedule.CreateScheduler("shuffle-leader", schedule.NewLimiter())
	c.Assert(err, IsNil)
	c.Assert(sl.Schedule(tc, schedule.OpInfluence{}), IsNil)

	// Add stores 1,2,3,4
	tc.addLeaderStore(1, 6)
	tc.addLeaderStore(2, 7)
	tc.addLeaderStore(3, 8)
	tc.addLeaderStore(4, 9)
	// Add regions 1,2,3,4 with leaders in stores 1,2,3,4
	tc.addLeaderRegion(1, 1, 2, 3, 4)
	tc.addLeaderRegion(1, 2, 3, 4, 1)
	tc.addLeaderRegion(2, 2, 3, 4, 1)
	tc.addLeaderRegion(2, 3, 4, 1, 2)
	tc.addLeaderRegion(3, 3, 4, 1, 2)
	tc.addLeaderRegion(3, 4, 1, 2, 3)
	tc.addLeaderRegion(4, 4, 1, 2, 3)
	tc.addLeaderRegion(4, 1, 2, 3, 4)

	for i := 0; i < 4; i++ {
		op := sl.Schedule(tc, schedule.NewOpInfluence(nil, tc))
		sourceID := op.Step(0).(schedule.TransferLeader).FromStore
		op = sl.Schedule(tc, schedule.NewOpInfluence(nil, tc))
		targetID := op.Step(0).(schedule.TransferLeader).ToStore
		c.Assert(sourceID, Equals, targetID)
	}
}

var _ = Suite(&testBalanceAdjacentRegionSuite{})

type testBalanceAdjacentRegionSuite struct{}

func (s *testBalanceAdjacentRegionSuite) TestBalance(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	sc, err := schedule.CreateScheduler("adjacent-region", schedule.NewLimiter())
	c.Assert(err, IsNil)
	c.Assert(sc.Schedule(tc, schedule.NewOpInfluence(nil, tc)), IsNil)

	// Add stores 1,2,3,4
	tc.addLeaderStore(1, 5)
	tc.addLeaderStore(2, 0)
	tc.addLeaderStore(3, 0)
	tc.addLeaderStore(4, 0)
	// Add regions
	tc.addLeaderRegionWithRange(1, "", "a", 1, 2, 3)
	tc.addLeaderRegionWithRange(2, "a", "b", 1, 2, 3)
	tc.addLeaderRegionWithRange(3, "b", "c", 1, 3, 4)
	tc.addLeaderRegionWithRange(4, "c", "d", 1, 2, 3)
	tc.addLeaderRegionWithRange(5, "e", "f", 1, 2, 3)
	tc.addLeaderRegionWithRange(6, "f", "g", 1, 2, 3)
	tc.addLeaderRegionWithRange(7, "z", "", 1, 2, 3)

	// check and do operator
	// transfer peer from store 1 to 4 for region 1 because the distribution of
	// the two regions is same, we will transfer the peer, which is leader now,
	// to a new store
	checkTransferPeerWithLeaderTransfer(c, sc.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 1, 4)
	// suppose we add peer in store 4, transfer leader to store 2, remove peer in store 1
	tc.addLeaderRegionWithRange(1, "", "a", 2, 3, 4)

	// transfer leader from store 1 to store 2 for region 2 because we have a different peer location,
	// we can directly transfer leader to peer 2. we priority to transfer leader because less overhead
	CheckTransferLeader(c, sc.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 1, 2)
	tc.addLeaderRegionWithRange(2, "a", "b", 2, 1, 3)

	// transfer leader from store 1 to store 2 for region 3
	CheckTransferLeader(c, sc.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 1, 4)
	tc.addLeaderRegionWithRange(3, "b", "c", 4, 1, 3)

	// transfer peer from store 1 to store 4 for region 5
	// the region 5 just adjacent the region 6
	checkTransferPeerWithLeaderTransfer(c, sc.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 1, 4)
	tc.addLeaderRegionWithRange(5, "e", "f", 2, 3, 4)

	c.Assert(sc.Schedule(tc, schedule.NewOpInfluence(nil, tc)), IsNil)
	c.Assert(sc.Schedule(tc, schedule.NewOpInfluence(nil, tc)), IsNil)
	CheckTransferLeader(c, sc.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 2, 4)
	tc.addLeaderRegionWithRange(1, "", "a", 4, 2, 3)
	for i := 0; i < 10; i++ {
		c.Assert(sc.Schedule(tc, schedule.NewOpInfluence(nil, tc)), IsNil)
	}
}
