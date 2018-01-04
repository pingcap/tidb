// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedulers"
)

var _ = Suite(&testNamespaceSuite{})

type testNamespaceSuite struct {
	classifier *mapClassifer
	tc         *testClusterInfo
	opt        *scheduleOption
}

func (s *testNamespaceSuite) SetUpTest(c *C) {
	s.classifier = newMapClassifer()
	_, s.opt = newTestScheduleConfig()
	s.tc = newTestClusterInfo(s.opt)
}

func (s *testNamespaceSuite) TestReplica(c *C) {
	// store regionCount namespace
	//     1           0       ns1
	//     2          10       ns1
	//     3           0       ns2
	s.tc.addRegionStore(1, 0)
	s.tc.addRegionStore(2, 10)
	s.tc.addRegionStore(3, 0)
	s.classifier.setStore(1, "ns1")
	s.classifier.setStore(2, "ns1")
	s.classifier.setStore(3, "ns2")

	checker := schedule.NewReplicaChecker(s.tc, s.classifier)

	// Replica should be added to the store with the same namespace.
	s.classifier.setRegion(1, "ns1")
	s.tc.addLeaderRegion(1, 1)
	op := checker.Check(s.tc.GetRegion(1))
	schedulers.CheckAddPeer(c, op, 2)
	s.tc.addLeaderRegion(1, 3)
	op = checker.Check(s.tc.GetRegion(1))
	schedulers.CheckAddPeer(c, op, 1)

	// Stop adding replica if no store in the same namespace.
	s.tc.addLeaderRegion(1, 1, 2)
	op = checker.Check(s.tc.GetRegion(1))
	c.Assert(op, IsNil)
}

func (s *testNamespaceSuite) TestNamespaceChecker(c *C) {
	// store regionCount namespace
	//     1           0       ns1
	//     2          10       ns1
	//     3           0       ns2
	s.tc.addRegionStore(1, 0)
	s.tc.addRegionStore(2, 10)
	s.tc.addRegionStore(3, 0)
	s.classifier.setStore(1, "ns1")
	s.classifier.setStore(2, "ns1")
	s.classifier.setStore(3, "ns2")

	checker := schedule.NewNamespaceChecker(s.tc, s.classifier)

	// Move the region if it was not in the right store.
	s.classifier.setRegion(1, "ns2")
	s.tc.addLeaderRegion(1, 1)
	op := checker.Check(s.tc.GetRegion(1))
	schedulers.CheckTransferPeer(c, op, 1, 3)

	// Only move one region if the one was in the right store while the other was not.
	s.classifier.setRegion(2, "ns1")
	s.tc.addLeaderRegion(2, 1)
	s.classifier.setRegion(3, "ns2")
	s.tc.addLeaderRegion(3, 2)
	op = checker.Check(s.tc.GetRegion(2))
	c.Assert(op, IsNil)
	op = checker.Check(s.tc.GetRegion(3))
	schedulers.CheckTransferPeer(c, op, 2, 3)

	// Do NOT move the region if it was in the right store.
	s.classifier.setRegion(4, "ns2")
	s.tc.addLeaderRegion(4, 3)
	op = checker.Check(s.tc.GetRegion(4))
	c.Assert(op, IsNil)

	// Move the peer with questions to the right store if the region has multiple peers.
	s.classifier.setRegion(5, "ns1")
	s.tc.addLeaderRegion(5, 1, 1, 3)
	op = checker.Check(s.tc.GetRegion(5))
	schedulers.CheckTransferPeer(c, op, 3, 2)
}

func (s *testNamespaceSuite) TestSchedulerBalanceRegion(c *C) {
	// store regionCount namespace
	//     1           0       ns1
	//     2         100       ns1
	//     3         200       ns2
	s.tc.addRegionStore(1, 0)
	s.tc.addRegionStore(2, 100)
	s.tc.addRegionStore(3, 200)
	s.classifier.setStore(1, "ns1")
	s.classifier.setStore(2, "ns1")
	s.classifier.setStore(3, "ns2")
	s.opt.SetMaxReplicas(1)
	sched, _ := schedule.CreateScheduler("balance-region", schedule.NewLimiter())

	// Balance is limited within a namespace.
	s.tc.addLeaderRegion(1, 2)
	s.classifier.setRegion(1, "ns1")
	op := scheduleByNamespace(s.tc, s.classifier, sched, schedule.NewOpInfluence(nil, s.tc))
	schedulers.CheckTransferPeer(c, op, 2, 1)

	// If no more store in the namespace, balance stops.
	s.tc.addLeaderRegion(1, 3)
	s.classifier.setRegion(1, "ns2")
	op = scheduleByNamespace(s.tc, s.classifier, sched, schedule.NewOpInfluence(nil, s.tc))
	c.Assert(op, IsNil)

	// If region is not in the correct namespace, it will not be balanced. The
	// region should be in 'ns1', but its replica is located in 'ns2', neither
	// namespace will select it for balance.
	s.tc.addRegionStore(4, 0)
	s.classifier.setStore(4, "ns2")
	s.tc.addLeaderRegion(1, 3)
	s.classifier.setRegion(1, "ns1")
	op = scheduleByNamespace(s.tc, s.classifier, sched, schedule.NewOpInfluence(nil, s.tc))
	c.Assert(op, IsNil)
}

func (s *testNamespaceSuite) TestSchedulerBalanceLeader(c *C) {
	// store regionCount namespace
	//     1         100       ns1
	//     2         200       ns1
	//     3           0       ns2
	//     4         300       ns2
	s.tc.addLeaderStore(1, 100)
	s.tc.addLeaderStore(2, 200)
	s.tc.addLeaderStore(3, 0)
	s.tc.addLeaderStore(4, 300)
	s.classifier.setStore(1, "ns1")
	s.classifier.setStore(2, "ns1")
	s.classifier.setStore(3, "ns2")
	s.classifier.setStore(4, "ns2")
	sched, _ := schedule.CreateScheduler("balance-leader", schedule.NewLimiter())

	// Balance is limited within a namespace.
	s.tc.addLeaderRegion(1, 2, 1)
	s.classifier.setRegion(1, "ns1")
	op := scheduleByNamespace(s.tc, s.classifier, sched, schedule.NewOpInfluence(nil, s.tc))
	schedulers.CheckTransferLeader(c, op, 2, 1)

	// If region is not in the correct namespace, it will not be balanced.
	s.tc.addLeaderRegion(1, 4, 1)
	s.classifier.setRegion(1, "ns1")
	op = scheduleByNamespace(s.tc, s.classifier, sched, schedule.NewOpInfluence(nil, s.tc))
	c.Assert(op, IsNil)
}

type mapClassifer struct {
	stores  map[uint64]string
	regions map[uint64]string
}

func newMapClassifer() *mapClassifer {
	return &mapClassifer{
		stores:  make(map[uint64]string),
		regions: make(map[uint64]string),
	}
}

func (c *mapClassifer) GetStoreNamespace(store *core.StoreInfo) string {
	if ns, ok := c.stores[store.GetId()]; ok {
		return ns
	}
	return namespace.DefaultNamespace
}

func (c *mapClassifer) GetRegionNamespace(region *core.RegionInfo) string {
	if ns, ok := c.regions[region.GetId()]; ok {
		return ns
	}
	return namespace.DefaultNamespace
}

func (c *mapClassifer) GetAllNamespaces() []string {
	all := make(map[string]struct{})
	for _, ns := range c.stores {
		all[ns] = struct{}{}
	}
	for _, ns := range c.regions {
		all[ns] = struct{}{}
	}

	nss := make([]string, 0, len(all))

	for ns := range all {
		nss = append(nss, ns)
	}
	return nss
}

func (c *mapClassifer) IsNamespaceExist(name string) bool {
	for _, ns := range c.stores {
		if ns == name {
			return true
		}
	}
	for _, ns := range c.regions {
		if ns == name {
			return true
		}
	}
	return false
}

func (c *mapClassifer) setStore(id uint64, namespace string) {
	c.stores[id] = namespace
}

func (c *mapClassifer) setRegion(id uint64, namespace string) {
	c.regions[id] = namespace
}
