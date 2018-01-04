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

package faketikv

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	log "github.com/sirupsen/logrus"
)

// ClusterInfo records all cluster information.
type ClusterInfo struct {
	*core.RegionsInfo
	Nodes       map[uint64]*Node
	firstRegion *core.RegionInfo
}

// GetBootstrapInfo returns first region and its leader store.
func (c *ClusterInfo) GetBootstrapInfo() (*metapb.Store, *metapb.Region) {
	storeID := c.firstRegion.Leader.GetStoreId()
	store := c.Nodes[storeID]
	return store.Store, c.firstRegion.Region
}

func (c *ClusterInfo) nodeHealth(storeID uint64) bool {
	n, ok := c.Nodes[storeID]
	if !ok {
		return false
	}

	return n.GetState() == Up
}

func (c ClusterInfo) electNewLeader(region *core.RegionInfo) *metapb.Peer {
	var (
		unhealth         int
		newLeaderStoreID uint64
	)
	ids := region.GetStoreIds()
	for id := range ids {
		if c.nodeHealth(id) {
			newLeaderStoreID = id
		} else {
			unhealth++
		}
	}
	if unhealth > len(ids)/2 {
		return nil
	}
	for _, peer := range region.Peers {
		if peer.GetStoreId() == newLeaderStoreID {
			return peer
		}
	}
	return nil
}

func (c *ClusterInfo) stepLeader(region *core.RegionInfo) {
	if region.Leader != nil && c.nodeHealth(region.Leader.GetStoreId()) {
		return
	}
	newLeader := c.electNewLeader(region)
	region.Leader = newLeader
	if newLeader == nil {
		c.SetRegion(region)
		log.Infof("[region %d] no leader", region.GetId())
		return
	}
	log.Info("[region %d] elect new leader: %+v,old leader: %+v", region.GetId(), newLeader, region.Leader)
	c.SetRegion(region)
	c.reportRegionChange(region.GetId())
}

func (c *ClusterInfo) reportRegionChange(regionID uint64) {
	region := c.GetRegion(regionID)
	if n, ok := c.Nodes[region.Leader.GetStoreId()]; ok {
		n.reportRegionChange(region.GetId())
	}
}

func (c *ClusterInfo) stepRegions() {
	regions := c.GetRegions()
	for _, region := range regions {
		c.stepLeader(region)
	}
}

// AddTask adds task in specify node.
func (c *ClusterInfo) AddTask(task Task) {
	storeID := task.TargetStoreID()
	if n, ok := c.Nodes[storeID]; ok {
		n.AddTask(task)
	}
}
