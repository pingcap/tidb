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

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pingcap/pd/server/schedule"
)

func newTestScheduleConfig() *MockSchedulerOptions {
	mso := newMockSchedulerOptions()
	return mso
}

func newTestReplication(mso *MockSchedulerOptions, maxReplicas int, locationLabels ...string) {
	mso.MaxReplicas = maxReplicas
	mso.LocationLabels = locationLabels
}

var _ = Suite(&testBalanceSpeedSuite{})

type testBalanceSpeedSuite struct{}

type testBalanceSpeedCase struct {
	sourceCount    uint64
	targetCount    uint64
	avgScore       float64
	regionSize     int64
	diff           int
	expectedResult bool
}

func (s *testBalanceSpeedSuite) TestBalanceSpeed(c *C) {
	testCases := []testBalanceSpeedCase{
		// source > avg > target
		{2, 0, 10, 1, 0, true},
		{2, 0, 10, 1, 10, false},
		{2, 0, 10, 10, 0, false},
		{10, 10, 100, 1, 0, false},
		{20, 10, 150, 20, 0, true},
		{20, 10, 150, 20, 50, false},
		{20, 10, 150, 30, 0, false},
		{1000, 2, 7500, 1, 0, true},
		{1000, 2, 7500, 20, 0, true},
		{1000, 2, 7500, 300, 0, true},
		{1000, 2, 7500, 300, 2000, false},
		{1000, 2, 7500, 1000, 0, true},
		{1000, 2, 7500, 1001, 0, false},
		// source > target > avg
		{5, 2, 10, 1, 0, false},
		{5, 2, 10, 10, 0, false},
		// avg > source > target
		{4, 0, 50, 1, 0, false},
		{4, 0, 50, 10, 0, false},
	}

	s.testBalanceSpeed(c, testCases, 1)
	s.testBalanceSpeed(c, testCases, 10)
	s.testBalanceSpeed(c, testCases, 100)
	s.testBalanceSpeed(c, testCases, 1000)
}

func newTestOpInfluence(source uint64, target uint64, kind core.ResourceKind, countDiff int) schedule.OpInfluence {
	m := make(map[uint64]*schedule.StoreInfluence)
	if kind == core.LeaderKind {
		m[source] = &schedule.StoreInfluence{
			LeaderCount: -countDiff,
			LeaderSize:  -countDiff * 10,
		}
		m[target] = &schedule.StoreInfluence{
			LeaderCount: countDiff,
			LeaderSize:  countDiff * 10,
		}
	} else if kind == core.RegionKind {
		m[source] = &schedule.StoreInfluence{
			RegionCount: -countDiff,
			RegionSize:  -countDiff * 10,
		}
		m[target] = &schedule.StoreInfluence{
			RegionCount: countDiff,
			RegionSize:  countDiff * 10,
		}
	}

	return m
}

func (s *testBalanceSpeedSuite) testBalanceSpeed(c *C, tests []testBalanceSpeedCase, capaGB uint64) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	for _, t := range tests {
		tc.addLeaderStore(1, int(t.sourceCount))
		tc.addLeaderStore(2, int(t.targetCount))
		source := tc.GetStore(1)
		target := tc.GetStore(2)
		region := &core.RegionInfo{ApproximateSize: t.regionSize}
		c.Assert(shouldBalance(source, target, t.avgScore, core.LeaderKind, region, newTestOpInfluence(1, 2, core.LeaderKind, t.diff), defaultTolerantSizeRatio), Equals, t.expectedResult)
	}

	for _, t := range tests {
		tc.addRegionStore(1, int(t.sourceCount))
		tc.addRegionStore(2, int(t.targetCount))
		source := tc.GetStore(1)
		target := tc.GetStore(2)
		region := &core.RegionInfo{ApproximateSize: t.regionSize}
		c.Assert(shouldBalance(source, target, t.avgScore, core.RegionKind, region, newTestOpInfluence(1, 2, core.RegionKind, t.diff), defaultTolerantSizeRatio), Equals, t.expectedResult)
	}
}

func (s *testBalanceSpeedSuite) TestBalanceLimit(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)
	tc.addLeaderStore(1, 10)
	tc.addLeaderStore(2, 20)
	tc.addLeaderStore(3, 30)

	// StandDeviation is sqrt((10^2+0+10^2)/3).
	c.Assert(adjustBalanceLimit(tc, core.LeaderKind), Equals, uint64(math.Sqrt(200.0/3.0)))

	tc.setStoreOffline(1)
	// StandDeviation is sqrt((5^2+5^2)/2).
	c.Assert(adjustBalanceLimit(tc, core.LeaderKind), Equals, uint64(math.Sqrt(50.0/2.0)))
}

var _ = Suite(&testBalanceLeaderSchedulerSuite{})

type testBalanceLeaderSchedulerSuite struct {
	tc *mockCluster
	lb schedule.Scheduler
}

func (s *testBalanceLeaderSchedulerSuite) SetUpTest(c *C) {
	opt := newTestScheduleConfig()
	s.tc = newMockCluster(opt)
	lb, err := schedule.CreateScheduler("balance-leader", schedule.NewLimiter())
	c.Assert(err, IsNil)
	s.lb = lb
}

func (s *testBalanceLeaderSchedulerSuite) schedule(operators []*schedule.Operator) *schedule.Operator {
	return s.lb.Schedule(s.tc, schedule.NewOpInfluence(operators, s.tc))
}

func (s *testBalanceLeaderSchedulerSuite) TestBalanceLimit(c *C) {
	// Stores:     1    2    3    4
	// Leaders:    1    0    0    0
	// Region1:    L    F    F    F
	s.tc.addLeaderStore(1, 1)
	s.tc.addLeaderStore(2, 0)
	s.tc.addLeaderStore(3, 0)
	s.tc.addLeaderStore(4, 0)
	s.tc.addLeaderRegion(1, 1, 2, 3, 4)
	c.Check(s.schedule(nil), IsNil)

	// Stores:     1    2    3    4
	// Leaders:    10   0    0    0
	// Region1:    L    F    F    F
	s.tc.updateLeaderCount(1, 10)
	c.Check(s.schedule(nil), NotNil)

	// Stores:     1    2    3    4
	// Leaders:    7    8    9   10
	// Region1:    F    F    F    L
	s.tc.updateLeaderCount(1, 7)
	s.tc.updateLeaderCount(2, 8)
	s.tc.updateLeaderCount(3, 9)
	s.tc.updateLeaderCount(4, 10)
	s.tc.addLeaderRegion(1, 4, 1, 2, 3)
	c.Check(s.schedule(nil), IsNil)

	// Stores:     1    2    3    4
	// Leaders:    7    8    9   16
	// Region1:    F    F    F    L
	s.tc.updateLeaderCount(4, 16)
	c.Check(s.schedule(nil), NotNil)
}

func (s *testBalanceLeaderSchedulerSuite) TestScheduleWithOpInfluence(c *C) {
	// Stores:     1    2    3    4
	// Leaders:    7    8    9   16
	// Region1:    F    F    F    L
	s.tc.addLeaderStore(1, 7)
	s.tc.addLeaderStore(2, 8)
	s.tc.addLeaderStore(3, 9)
	s.tc.addLeaderStore(4, 16)
	s.tc.addLeaderRegion(1, 4, 1, 2, 3)
	op := s.schedule(nil)
	c.Check(op, NotNil)
	c.Check(s.schedule([]*schedule.Operator{op}), IsNil)

	// Stores:     1    2    3    4
	// Leaders:    8    8    9   15
	// Region1:    F    F    F    L
	s.tc.updateLeaderCount(1, 8)
	s.tc.updateLeaderCount(2, 8)
	s.tc.updateLeaderCount(3, 9)
	s.tc.updateLeaderCount(4, 15)
	s.tc.addLeaderRegion(1, 4, 1, 2, 3)
	c.Check(s.schedule(nil), IsNil)
}

func (s *testBalanceLeaderSchedulerSuite) TestBalanceFilter(c *C) {
	// Stores:     1    2    3    4
	// Leaders:    1    2    3   16
	// Region1:    F    F    F    L
	s.tc.addLeaderStore(1, 1)
	s.tc.addLeaderStore(2, 2)
	s.tc.addLeaderStore(3, 3)
	s.tc.addLeaderStore(4, 16)
	s.tc.addLeaderRegion(1, 4, 1, 2, 3)

	CheckTransferLeader(c, s.schedule(nil), 4, 1)
	// Test stateFilter.
	// If store 1 is down, it will be filtered,
	// store 2 becomes the store with least leaders.
	s.tc.setStoreDown(1)
	CheckTransferLeader(c, s.schedule(nil), 4, 2)

	// Test healthFilter.
	// If store 2 is busy, it will be filtered,
	// store 3 becomes the store with least leaders.
	s.tc.setStoreBusy(2, true)
	CheckTransferLeader(c, s.schedule(nil), 4, 3)
}

func (s *testBalanceLeaderSchedulerSuite) TestLeaderWeight(c *C) {
	// Stores:	1	2	3	4
	// Leaders:    10      10      10      10
	// Weight:    0.5     0.9       1       2
	// Region1:     L       F       F       F

	s.tc.addLeaderStore(1, 10)
	s.tc.addLeaderStore(2, 10)
	s.tc.addLeaderStore(3, 10)
	s.tc.addLeaderStore(4, 10)
	s.tc.updateStoreLeaderWeight(1, 0.5)
	s.tc.updateStoreLeaderWeight(2, 0.9)
	s.tc.updateStoreLeaderWeight(3, 1)
	s.tc.updateStoreLeaderWeight(4, 2)
	s.tc.addLeaderRegion(1, 1, 2, 3, 4)
	CheckTransferLeader(c, s.schedule(nil), 1, 4)
	s.tc.updateLeaderCount(4, 30)
	CheckTransferLeader(c, s.schedule(nil), 1, 3)
}

func (s *testBalanceLeaderSchedulerSuite) TestBalanceSelector(c *C) {
	// Stores:     1    2    3    4
	// Leaders:    1    2    3   16
	// Region1:    -    F    F    L
	// Region2:    F    F    L    -
	s.tc.addLeaderStore(1, 1)
	s.tc.addLeaderStore(2, 2)
	s.tc.addLeaderStore(3, 3)
	s.tc.addLeaderStore(4, 16)
	s.tc.addLeaderRegion(1, 4, 2, 3)
	s.tc.addLeaderRegion(2, 3, 1, 2)
	// Average leader is 5.5. Select store 4 as source.
	CheckTransferLeader(c, s.schedule(nil), 4, 2)

	// Stores:     1    2    3    4
	// Leaders:    1    14   15   16
	// Region1:    -    F    F    L
	// Region2:    F    F    L    -
	s.tc.updateLeaderCount(2, 14)
	s.tc.updateLeaderCount(3, 15)
	// Average leader is 11.5. Select store 1 as target.
	CheckTransferLeader(c, s.schedule(nil), 3, 1)

	// Stores:     1    2    3    4
	// Leaders:    1    2    15   16
	// Region1:    -    F    F    L
	// Region2:    -    F    L    F
	s.tc.addLeaderRegion(2, 3, 2, 4)
	s.tc.addLeaderStore(2, 2)
	// Unable to find a region in store 1. Transfer a leader out of store 4 instead.
	CheckTransferLeader(c, s.schedule(nil), 4, 2)
}

var _ = Suite(&testBalanceRegionSchedulerSuite{})

type testBalanceRegionSchedulerSuite struct{}

func (s *testBalanceRegionSchedulerSuite) TestBalance(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	sb, err := schedule.CreateScheduler("balance-region", schedule.NewLimiter())
	c.Assert(err, IsNil)
	cache := sb.(*balanceRegionScheduler).cache

	opt.SetMaxReplicas(1)

	// Add stores 1,2,3,4.
	tc.addRegionStore(1, 6)
	tc.addRegionStore(2, 8)
	tc.addRegionStore(3, 8)
	tc.addRegionStore(4, 16)
	// Add region 1 with leader in store 4.
	tc.addLeaderRegion(1, 4)
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 4, 1)

	// Test stateFilter.
	tc.setStoreOffline(1)
	tc.updateRegionCount(2, 6)
	cache.Remove(4)
	// When store 1 is offline, it will be filtered,
	// store 2 becomes the store with least regions.
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 4, 2)

	// Test MaxReplicas.
	opt.SetMaxReplicas(3)
	c.Assert(sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), IsNil)
	opt.SetMaxReplicas(1)
	c.Assert(sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), NotNil)
}

func (s *testBalanceRegionSchedulerSuite) TestReplicas3(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	newTestReplication(opt, 3, "zone", "rack", "host")

	sb, err := schedule.CreateScheduler("balance-region", schedule.NewLimiter())
	c.Assert(err, IsNil)
	cache := sb.(*balanceRegionScheduler).cache

	// Store 1 has the largest region score, so the balancer try to replace peer in store 1.
	tc.addLabelsStore(1, 16, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(2, 15, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.addLabelsStore(3, 14, map[string]string{"zone": "z1", "rack": "r2", "host": "h2"})

	tc.addLeaderRegion(1, 1, 2, 3)
	// This schedule try to replace peer in store 1, but we have no other stores,
	// so store 1 will be set in the cache and skipped next schedule.
	c.Assert(sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), IsNil)
	c.Assert(cache.Exists(1), IsTrue)

	// Store 4 has smaller region score than store 2.
	tc.addLabelsStore(4, 2, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 2, 4)

	// Store 5 has smaller region score than store 1.
	tc.addLabelsStore(5, 2, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	cache.Remove(1) // Delete store 1 from cache, or it will be skipped.
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 1, 5)

	// Store 6 has smaller region score than store 5.
	tc.addLabelsStore(6, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 1, 6)

	// Store 7 has the same region score with store 6, but in a different host.
	tc.addLabelsStore(7, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"})
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 1, 7)

	// If store 7 is not available, we wait.
	tc.setStoreDown(7)
	c.Assert(sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), IsNil)
	c.Assert(cache.Exists(1), IsTrue)
	tc.setStoreUp(7)
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 2, 7)
	cache.Remove(1)
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 1, 7)

	// Store 8 has smaller region score than store 7, but the distinct score decrease.
	tc.addLabelsStore(8, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h3"})
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 1, 7)

	// Take down 4,5,6,7
	tc.setStoreDown(4)
	tc.setStoreDown(5)
	tc.setStoreDown(6)
	tc.setStoreDown(7)
	c.Assert(sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), IsNil)
	c.Assert(cache.Exists(1), IsTrue)
	cache.Remove(1)

	// Store 9 has different zone with other stores but larger region score than store 1.
	tc.addLabelsStore(9, 9, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	c.Assert(sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), IsNil)
}

func (s *testBalanceRegionSchedulerSuite) TestReplicas5(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	newTestReplication(opt, 5, "zone", "rack", "host")

	sb, err := schedule.CreateScheduler("balance-region", schedule.NewLimiter())
	c.Assert(err, IsNil)

	tc.addLabelsStore(1, 4, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(2, 5, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(3, 6, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(4, 7, map[string]string{"zone": "z4", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(5, 28, map[string]string{"zone": "z5", "rack": "r1", "host": "h1"})

	tc.addLeaderRegion(1, 1, 2, 3, 4, 5)

	// Store 6 has smaller region score.
	tc.addLabelsStore(6, 1, map[string]string{"zone": "z5", "rack": "r2", "host": "h1"})
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 5, 6)

	// Store 7 has smaller region score and higher distinct score.
	tc.addLabelsStore(7, 5, map[string]string{"zone": "z6", "rack": "r1", "host": "h1"})
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 5, 7)

	// Store 1 has smaller region score and higher distinct score.
	tc.addLeaderRegion(1, 2, 3, 4, 5, 6)
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 5, 1)

	// Store 6 has smaller region score and higher distinct score.
	tc.addLabelsStore(11, 29, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.addLabelsStore(12, 8, map[string]string{"zone": "z2", "rack": "r2", "host": "h1"})
	tc.addLabelsStore(13, 7, map[string]string{"zone": "z3", "rack": "r2", "host": "h1"})
	tc.addLeaderRegion(1, 2, 3, 11, 12, 13)
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 11, 6)
}

func (s *testBalanceRegionSchedulerSuite) TestStoreWeight(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	sb, err := schedule.CreateScheduler("balance-region", schedule.NewLimiter())
	c.Assert(err, IsNil)
	opt.SetMaxReplicas(1)

	tc.addRegionStore(1, 10)
	tc.addRegionStore(2, 10)
	tc.addRegionStore(3, 10)
	tc.addRegionStore(4, 10)
	tc.updateStoreRegionWeight(1, 0.5)
	tc.updateStoreRegionWeight(2, 0.9)
	tc.updateStoreRegionWeight(3, 1.0)
	tc.updateStoreRegionWeight(4, 2.0)

	tc.addLeaderRegion(1, 1)
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 1, 4)

	tc.updateRegionCount(4, 30)
	CheckTransferPeer(c, sb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 1, 3)
}

var _ = Suite(&testReplicaCheckerSuite{})

type testReplicaCheckerSuite struct{}

func (s *testReplicaCheckerSuite) TestBasic(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	rc := schedule.NewReplicaChecker(tc, namespace.DefaultClassifier)

	opt.MaxSnapshotCount = 2

	// Add stores 1,2,3,4.
	tc.addRegionStore(1, 4)
	tc.addRegionStore(2, 3)
	tc.addRegionStore(3, 2)
	tc.addRegionStore(4, 1)
	// Add region 1 with leader in store 1 and follower in store 2.
	tc.addLeaderRegion(1, 1, 2)

	// Region has 2 peers, we need to add a new peer.
	region := tc.GetRegion(1)
	CheckAddPeer(c, rc.Check(region), 4)

	// Test healthFilter.
	// If store 4 is down, we add to store 3.
	tc.setStoreDown(4)
	CheckAddPeer(c, rc.Check(region), 3)
	tc.setStoreUp(4)
	CheckAddPeer(c, rc.Check(region), 4)

	// Test snapshotCountFilter.
	// If snapshotCount > MaxSnapshotCount, we add to store 3.
	tc.updateSnapshotCount(4, 3)
	CheckAddPeer(c, rc.Check(region), 3)
	// If snapshotCount < MaxSnapshotCount, we can add peer again.
	tc.updateSnapshotCount(4, 1)
	CheckAddPeer(c, rc.Check(region), 4)

	// Test storageThresholdFilter.
	// If availableRatio < storageAvailableRatioThreshold(0.2), we can not add peer.
	tc.updateStorageRatio(4, 0.9, 0.1)
	CheckAddPeer(c, rc.Check(region), 3)
	tc.updateStorageRatio(4, 0.5, 0.1)
	CheckAddPeer(c, rc.Check(region), 3)
	// If availableRatio > storageAvailableRatioThreshold(0.2), we can add peer again.
	tc.updateStorageRatio(4, 0.7, 0.3)
	CheckAddPeer(c, rc.Check(region), 4)

	// Add peer in store 4, and we have enough replicas.
	peer4, _ := tc.AllocPeer(4)
	region.Peers = append(region.Peers, peer4)
	c.Assert(rc.Check(region), IsNil)

	// Add peer in store 3, and we have redundant replicas.
	peer3, _ := tc.AllocPeer(3)
	region.Peers = append(region.Peers, peer3)
	checkRemovePeer(c, rc.Check(region), 1)
	region.RemoveStorePeer(1)

	// Peer in store 2 is down, remove it.
	tc.setStoreDown(2)
	downPeer := &pdpb.PeerStats{
		Peer:        region.GetStorePeer(2),
		DownSeconds: 24 * 60 * 60,
	}
	region.DownPeers = append(region.DownPeers, downPeer)
	checkRemovePeer(c, rc.Check(region), 2)
	region.DownPeers = nil
	c.Assert(rc.Check(region), IsNil)

	// Peer in store 3 is offline, transfer peer to store 1.
	tc.setStoreOffline(3)
	CheckTransferPeer(c, rc.Check(region), 3, 1)
}

func (s *testReplicaCheckerSuite) TestLostStore(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	tc.addRegionStore(1, 1)
	tc.addRegionStore(2, 1)

	rc := schedule.NewReplicaChecker(tc, namespace.DefaultClassifier)

	// now region peer in store 1,2,3.but we just have store 1,2
	// This happens only in recovering the PD tc
	// should not panic
	tc.addLeaderRegion(1, 1, 2, 3)
	region := tc.GetRegion(1)
	op := rc.Check(region)
	c.Assert(op, IsNil)
}

func (s *testReplicaCheckerSuite) TestOffline(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	newTestReplication(opt, 3, "zone", "rack", "host")

	rc := schedule.NewReplicaChecker(tc, namespace.DefaultClassifier)

	tc.addLabelsStore(1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(2, 2, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(3, 3, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(4, 4, map[string]string{"zone": "z3", "rack": "r2", "host": "h1"})

	tc.addLeaderRegion(1, 1)
	region := tc.GetRegion(1)

	// Store 2 has different zone and smallest region score.
	CheckAddPeer(c, rc.Check(region), 2)
	peer2, _ := tc.AllocPeer(2)
	region.Peers = append(region.Peers, peer2)

	// Store 3 has different zone and smallest region score.
	CheckAddPeer(c, rc.Check(region), 3)
	peer3, _ := tc.AllocPeer(3)
	region.Peers = append(region.Peers, peer3)

	// Store 4 has the same zone with store 3 and larger region score.
	peer4, _ := tc.AllocPeer(4)
	region.Peers = append(region.Peers, peer4)
	checkRemovePeer(c, rc.Check(region), 4)

	// Test healthFilter.
	tc.setStoreBusy(4, true)
	c.Assert(rc.Check(region), IsNil)
	tc.setStoreBusy(4, false)
	checkRemovePeer(c, rc.Check(region), 4)

	// Test offline
	// the number of region peers more than the maxReplicas
	// remove the peer
	tc.setStoreOffline(3)
	checkRemovePeer(c, rc.Check(region), 3)
	region.RemoveStorePeer(4)
	// the number of region peers equals the maxReplicas
	// Transfer peer to store 4.
	CheckTransferPeer(c, rc.Check(region), 3, 4)

	// Store 5 has a different zone, we can keep it safe.
	tc.addLabelsStore(5, 5, map[string]string{"zone": "z4", "rack": "r1", "host": "h1"})
	CheckTransferPeer(c, rc.Check(region), 3, 5)
	tc.updateSnapshotCount(5, 10)
	c.Assert(rc.Check(region), IsNil)
}

func (s *testReplicaCheckerSuite) TestDistinctScore(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	newTestReplication(opt, 3, "zone", "rack", "host")

	rc := schedule.NewReplicaChecker(tc, namespace.DefaultClassifier)

	tc.addLabelsStore(1, 9, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.addLabelsStore(2, 8, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})

	// We need 3 replicas.
	tc.addLeaderRegion(1, 1)
	region := tc.GetRegion(1)
	CheckAddPeer(c, rc.Check(region), 2)
	peer2, _ := tc.AllocPeer(2)
	region.Peers = append(region.Peers, peer2)

	// Store 1,2,3 have the same zone, rack, and host.
	tc.addLabelsStore(3, 5, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	CheckAddPeer(c, rc.Check(region), 3)

	// Store 4 has smaller region score.
	tc.addLabelsStore(4, 4, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	CheckAddPeer(c, rc.Check(region), 4)

	// Store 5 has a different host.
	tc.addLabelsStore(5, 5, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"})
	CheckAddPeer(c, rc.Check(region), 5)

	// Store 6 has a different rack.
	tc.addLabelsStore(6, 6, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	CheckAddPeer(c, rc.Check(region), 6)

	// Store 7 has a different zone.
	tc.addLabelsStore(7, 7, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	CheckAddPeer(c, rc.Check(region), 7)

	// Test stateFilter.
	tc.setStoreOffline(7)
	CheckAddPeer(c, rc.Check(region), 6)
	tc.setStoreUp(7)
	CheckAddPeer(c, rc.Check(region), 7)

	// Add peer to store 7.
	peer7, _ := tc.AllocPeer(7)
	region.Peers = append(region.Peers, peer7)

	// Replace peer in store 1 with store 6 because it has a different rack.
	CheckTransferPeer(c, rc.Check(region), 1, 6)
	peer6, _ := tc.AllocPeer(6)
	region.Peers = append(region.Peers, peer6)
	checkRemovePeer(c, rc.Check(region), 1)
	region.RemoveStorePeer(1)
	c.Assert(rc.Check(region), IsNil)

	// Store 8 has the same zone and different rack with store 7.
	// Store 1 has the same zone and different rack with store 6.
	// So store 8 and store 1 are equivalent.
	tc.addLabelsStore(8, 1, map[string]string{"zone": "z2", "rack": "r2", "host": "h1"})
	c.Assert(rc.Check(region), IsNil)

	// Store 9 has a different zone, but it is almost full.
	tc.addLabelsStore(9, 1, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	tc.updateStorageRatio(9, 0.9, 0.1)
	c.Assert(rc.Check(region), IsNil)

	// Store 10 has a different zone.
	// Store 2 and 6 have the same distinct score, but store 2 has larger region score.
	// So replace peer in store 2 with store 10.
	tc.addLabelsStore(10, 1, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	CheckTransferPeer(c, rc.Check(region), 2, 10)
	peer10, _ := tc.AllocPeer(10)
	region.Peers = append(region.Peers, peer10)
	checkRemovePeer(c, rc.Check(region), 2)
	region.RemoveStorePeer(2)
	c.Assert(rc.Check(region), IsNil)
}

func (s *testReplicaCheckerSuite) TestDistinctScore2(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	newTestReplication(opt, 5, "zone", "host")

	rc := schedule.NewReplicaChecker(tc, namespace.DefaultClassifier)

	tc.addLabelsStore(1, 1, map[string]string{"zone": "z1", "host": "h1"})
	tc.addLabelsStore(2, 1, map[string]string{"zone": "z1", "host": "h2"})
	tc.addLabelsStore(3, 1, map[string]string{"zone": "z1", "host": "h3"})
	tc.addLabelsStore(4, 1, map[string]string{"zone": "z2", "host": "h1"})
	tc.addLabelsStore(5, 1, map[string]string{"zone": "z2", "host": "h2"})
	tc.addLabelsStore(6, 1, map[string]string{"zone": "z3", "host": "h1"})

	tc.addLeaderRegion(1, 1, 2, 4)
	region := tc.GetRegion(1)

	CheckAddPeer(c, rc.Check(region), 6)
	peer6, _ := tc.AllocPeer(6)
	region.Peers = append(region.Peers, peer6)

	CheckAddPeer(c, rc.Check(region), 5)
	peer5, _ := tc.AllocPeer(5)
	region.Peers = append(region.Peers, peer5)

	c.Assert(rc.Check(region), IsNil)
}

var _ = Suite(&testBalanceHotWriteRegionSchedulerSuite{})

type testBalanceHotWriteRegionSchedulerSuite struct{}

func (s *testBalanceHotWriteRegionSchedulerSuite) TestBalance(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)

	hb, err := schedule.CreateScheduler("hot-write-region", schedule.NewLimiter())
	c.Assert(err, IsNil)

	// Add stores 1, 2, 3, 4, 5 with region counts 3, 2, 2, 2, 0.
	tc.addRegionStore(1, 3)
	tc.addRegionStore(2, 2)
	tc.addRegionStore(3, 2)
	tc.addRegionStore(4, 2)
	tc.addRegionStore(5, 0)

	// Report store written bytes.
	tc.updateStorageWrittenBytes(1, 75*1024*1024)
	tc.updateStorageWrittenBytes(2, 45*1024*1024)
	tc.updateStorageWrittenBytes(3, 45*1024*1024)
	tc.updateStorageWrittenBytes(4, 60*1024*1024)
	tc.updateStorageWrittenBytes(5, 0)

	// Region 1, 2 and 3 are hot regions.
	//| region_id | leader_sotre | follower_store | follower_store | written_bytes |
	//|-----------|--------------|----------------|----------------|---------------|
	//|     1     |       1      |        2       |       3        |      512KB    |
	//|     2     |       1      |        3       |       4        |      512KB    |
	//|     3     |       1      |        2       |       4        |      512KB    |
	tc.addLeaderRegionWithWriteInfo(1, 1, 512*1024*schedule.RegionHeartBeatReportInterval, 2, 3)
	tc.addLeaderRegionWithWriteInfo(2, 1, 512*1024*schedule.RegionHeartBeatReportInterval, 3, 4)
	tc.addLeaderRegionWithWriteInfo(3, 1, 512*1024*schedule.RegionHeartBeatReportInterval, 2, 4)
	opt.HotRegionLowThreshold = 0

	// Will transfer a hot region from store 1 to store 5, because the total count of peers
	// which is hot for store 1 is more larger than other stores.
	checkTransferPeerWithLeaderTransfer(c, hb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 1, 5)

	// After transfer a hot region from store 1 to store 5
	//| region_id | leader_sotre | follower_store | follower_store | written_bytes |
	//|-----------|--------------|----------------|----------------|---------------|
	//|     1     |       1      |        2       |       3        |      512KB    |
	//|     2     |       1      |        3       |       4        |      512KB    |
	//|     3     |       5      |        2       |       4        |      512KB    |
	tc.updateStorageWrittenBytes(1, 60*1024*1024)
	tc.updateStorageWrittenBytes(2, 30*1024*1024)
	tc.updateStorageWrittenBytes(3, 60*1024*1024)
	tc.updateStorageWrittenBytes(4, 30*1024*1024)
	tc.updateStorageWrittenBytes(5, 30*1024*1024)
	tc.addLeaderRegionWithWriteInfo(1, 1, 512*1024*schedule.RegionHeartBeatReportInterval, 2, 3)
	tc.addLeaderRegionWithWriteInfo(2, 1, 512*1024*schedule.RegionHeartBeatReportInterval, 3, 4)
	tc.addLeaderRegionWithWriteInfo(3, 5, 512*1024*schedule.RegionHeartBeatReportInterval, 2, 4)

	// We can find that the leader of all hot regions are on store 1,
	// so one of the leader will transfer to another store.
	checkTransferLeaderFrom(c, hb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 1)
}

var _ = Suite(&testBalanceHotReadRegionSchedulerSuite{})

type testBalanceHotReadRegionSchedulerSuite struct{}

func (s *testBalanceHotReadRegionSchedulerSuite) TestBalance(c *C) {
	opt := newTestScheduleConfig()
	tc := newMockCluster(opt)
	hb, err := schedule.CreateScheduler("hot-read-region", schedule.NewLimiter())
	c.Assert(err, IsNil)

	// Add stores 1, 2, 3, 4, 5 with region counts 3, 2, 2, 2, 0.
	tc.addRegionStore(1, 3)
	tc.addRegionStore(2, 2)
	tc.addRegionStore(3, 2)
	tc.addRegionStore(4, 2)
	tc.addRegionStore(5, 0)

	// Report store read bytes.
	tc.updateStorageReadBytes(1, 75*1024*1024)
	tc.updateStorageReadBytes(2, 45*1024*1024)
	tc.updateStorageReadBytes(3, 45*1024*1024)
	tc.updateStorageReadBytes(4, 60*1024*1024)
	tc.updateStorageReadBytes(5, 0)

	// Region 1, 2 and 3 are hot regions.
	//| region_id | leader_sotre | follower_store | follower_store |   read_bytes  |
	//|-----------|--------------|----------------|----------------|---------------|
	//|     1     |       1      |        2       |       3        |      512KB    |
	//|     2     |       2      |        1       |       3        |      512KB    |
	//|     3     |       1      |        2       |       3        |      512KB    |
	tc.addLeaderRegionWithReadInfo(1, 1, 512*1024*schedule.RegionHeartBeatReportInterval, 2, 3)
	tc.addLeaderRegionWithReadInfo(2, 2, 512*1024*schedule.RegionHeartBeatReportInterval, 1, 3)
	tc.addLeaderRegionWithReadInfo(3, 1, 512*1024*schedule.RegionHeartBeatReportInterval, 2, 3)
	opt.HotRegionLowThreshold = 0

	// Will transfer a hot region leader from store 1 to store 3, because the total count of peers
	// which is hot for store 1 is more larger than other stores.
	CheckTransferLeader(c, hb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 1, 3)
	// assume handle the operator
	tc.addLeaderRegionWithReadInfo(3, 3, 512*1024*schedule.RegionHeartBeatReportInterval, 1, 2)

	// After transfer a hot region leader from store 1 to store 3
	// the tree region leader will be evenly distributed in three stores
	tc.updateStorageReadBytes(1, 60*1024*1024)
	tc.updateStorageReadBytes(2, 30*1024*1024)
	tc.updateStorageReadBytes(3, 60*1024*1024)
	tc.updateStorageReadBytes(4, 30*1024*1024)
	tc.updateStorageReadBytes(5, 30*1024*1024)
	tc.addLeaderRegionWithReadInfo(4, 1, 512*1024*schedule.RegionHeartBeatReportInterval, 2, 3)
	tc.addLeaderRegionWithReadInfo(5, 4, 512*1024*schedule.RegionHeartBeatReportInterval, 2, 5)

	// Now appear two read hot region in store 1 and 4
	// We will Transfer peer from 1 to 5
	checkTransferPeerWithLeaderTransfer(c, hb.Schedule(tc, schedule.NewOpInfluence(nil, tc)), 1, 5)
}

func checkRemovePeer(c *C, op *schedule.Operator, storeID uint64) {
	if op.Len() == 1 {
		c.Assert(op.Step(0).(schedule.RemovePeer).FromStore, Equals, storeID)
	} else {
		c.Assert(op.Len(), Equals, 2)
		c.Assert(op.Step(0).(schedule.TransferLeader).FromStore, Equals, storeID)
		c.Assert(op.Step(1).(schedule.RemovePeer).FromStore, Equals, storeID)
	}
}

func checkTransferPeerWithLeaderTransfer(c *C, op *schedule.Operator, sourceID, targetID uint64) {
	c.Assert(op.Len(), Equals, 3)
	CheckTransferPeer(c, op, sourceID, targetID)
}

func checkTransferLeaderFrom(c *C, op *schedule.Operator, sourceID uint64) {
	c.Assert(op.Len(), Equals, 1)
	c.Assert(op.Step(0).(schedule.TransferLeader).FromStore, Equals, sourceID)
}
