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
	"math/rand"

	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pingcap/pd/server/schedule"
)

// namespaceCluster is part of a global cluster that contains stores and reigons
// within a specific namespace.
type namespaceCluster struct {
	schedule.Cluster
	classifier namespace.Classifier
	namespace  string
	stores     map[uint64]*core.StoreInfo
}

func newNamespaceCluster(c schedule.Cluster, classifier namespace.Classifier, namespace string) *namespaceCluster {
	stores := make(map[uint64]*core.StoreInfo)
	for _, s := range c.GetStores() {
		if classifier.GetStoreNamespace(s) == namespace {
			stores[s.GetId()] = s
		}
	}
	return &namespaceCluster{
		Cluster:    c,
		classifier: classifier,
		namespace:  namespace,
		stores:     stores,
	}
}

func (c *namespaceCluster) checkRegion(region *core.RegionInfo) bool {
	if c.classifier.GetRegionNamespace(region) != c.namespace {
		return false
	}
	for _, p := range region.Peers {
		if _, ok := c.stores[p.GetStoreId()]; !ok {
			return false
		}
	}
	return true
}

const randRegionMaxRetry = 10

// RandFollowerRegion returns a random region that has a follower on the store.
func (c *namespaceCluster) RandFollowerRegion(storeID uint64) *core.RegionInfo {
	for i := 0; i < randRegionMaxRetry; i++ {
		r := c.Cluster.RandFollowerRegion(storeID)
		if r == nil {
			return nil
		}
		if c.checkRegion(r) {
			return r
		}
	}
	return nil
}

// RandLeaderRegion returns a random region that has leader on the store.
func (c *namespaceCluster) RandLeaderRegion(storeID uint64) *core.RegionInfo {
	for i := 0; i < randRegionMaxRetry; i++ {
		r := c.Cluster.RandLeaderRegion(storeID)
		if r == nil {
			return nil
		}
		if c.checkRegion(r) {
			return r
		}
	}
	return nil
}

// GetStores returns all stores in the namespace.
func (c *namespaceCluster) GetStores() []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, len(c.stores))
	for _, s := range c.stores {
		stores = append(stores, s)
	}
	return stores
}

// GetStore searches for a store by ID.
func (c *namespaceCluster) GetStore(id uint64) *core.StoreInfo {
	return c.stores[id]
}

// GetRegion searches for a region by ID.
func (c *namespaceCluster) GetRegion(id uint64) *core.RegionInfo {
	r := c.Cluster.GetRegion(id)
	if r == nil || !c.checkRegion(r) {
		return nil
	}
	return r
}

// RegionWriteStats returns hot region's write stats.
func (c *namespaceCluster) RegionWriteStats() []*core.RegionStat {
	allStats := c.Cluster.RegionWriteStats()
	stats := make([]*core.RegionStat, 0, len(allStats))
	for _, s := range allStats {
		if c.GetRegion(s.RegionID) != nil {
			stats = append(stats, s)
		}
	}
	return stats
}

func scheduleByNamespace(cluster schedule.Cluster, classifier namespace.Classifier, scheduler schedule.Scheduler, opInfluence schedule.OpInfluence) *schedule.Operator {
	namespaces := classifier.GetAllNamespaces()
	for _, i := range rand.Perm(len(namespaces)) {
		nc := newNamespaceCluster(cluster, classifier, namespaces[i])
		if op := scheduler.Schedule(nc, opInfluence); op != nil {
			return op
		}
	}
	return nil
}

func (c *namespaceCluster) GetLeaderScheduleLimit() uint64 {
	return c.GetOpt().GetLeaderScheduleLimit(c.namespace)
}

func (c *namespaceCluster) GetRegionScheduleLimit() uint64 {
	return c.GetOpt().GetRegionScheduleLimit(c.namespace)
}

func (c *namespaceCluster) GetReplicaScheduleLimit() uint64 {
	return c.GetOpt().GetReplicaScheduleLimit(c.namespace)
}

func (c *namespaceCluster) GetMaxReplicas() int {
	return c.GetOpt().GetMaxReplicas(c.namespace)
}
