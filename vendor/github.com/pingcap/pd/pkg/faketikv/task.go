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
	"fmt"

	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

// Task running in node.
type Task interface {
	Desc() string
	RegionID() uint64
	Step(cluster *ClusterInfo)
	TargetStoreID() uint64
	IsFinished() bool
}

func responseToTask(resp *pdpb.RegionHeartbeatResponse, clusterInfo *ClusterInfo) Task {
	regionID := resp.GetRegionId()
	region := clusterInfo.GetRegion(regionID)
	epoch := resp.GetRegionEpoch()

	//  change peer
	if resp.GetChangePeer() != nil {
		changePeer := resp.GetChangePeer()
		switch changePeer.GetChangeType() {
		case eraftpb.ConfChangeType_AddNode:
			return &addPeer{
				regionID: regionID,
				size:     region.ApproximateSize,
				speed:    100 * 1000 * 1000,
				epoch:    epoch,
				peer:     changePeer.GetPeer(),
			}
		case eraftpb.ConfChangeType_RemoveNode:
			return &removePeer{
				regionID: regionID,
				size:     region.ApproximateSize,
				speed:    100 * 1000 * 1000,
				epoch:    epoch,
				peer:     changePeer.GetPeer(),
			}
		}
	} else if resp.GetTransferLeader() != nil {
		changePeer := resp.GetTransferLeader().GetPeer()
		fromPeer := region.Leader
		return &transferLeader{
			regionID: regionID,
			epoch:    epoch,
			fromPeer: fromPeer,
			peer:     changePeer,
		}
	}
	return nil
}

type transferLeader struct {
	regionID uint64
	epoch    *metapb.RegionEpoch
	fromPeer *metapb.Peer
	peer     *metapb.Peer
	finished bool
}

func (t *transferLeader) Desc() string {
	return fmt.Sprintf("transfer leader from store %d to store %d", t.fromPeer.GetStoreId(), t.peer.GetStoreId())
}

func (t *transferLeader) Step(cluster *ClusterInfo) {
	if t.finished {
		return
	}
	region := cluster.GetRegion(t.regionID)
	if region.RegionEpoch.Version > t.epoch.Version || region.RegionEpoch.ConfVer > t.epoch.ConfVer {
		t.finished = true
		return
	}
	if region.GetPeer(t.peer.GetId()) != nil {
		region.Leader = t.peer
	}
	t.finished = true
	cluster.SetRegion(region)
}

func (t *transferLeader) TargetStoreID() uint64 {
	return t.fromPeer.GetStoreId()
}

func (t *transferLeader) RegionID() uint64 {
	return t.regionID
}

func (t *transferLeader) IsFinished() bool {
	return t.finished
}

type addPeer struct {
	regionID uint64
	size     int64
	speed    int64
	epoch    *metapb.RegionEpoch
	peer     *metapb.Peer
	finished bool
}

func (a *addPeer) Desc() string {
	return fmt.Sprintf("add peer %+v for region %d", a.peer, a.regionID)
}

func (a *addPeer) Step(cluster *ClusterInfo) {
	if a.finished {
		return
	}
	region := cluster.GetRegion(a.regionID)
	if region.RegionEpoch.Version > a.epoch.Version || region.RegionEpoch.ConfVer > a.epoch.ConfVer {
		a.finished = true
		return
	}

	a.size -= a.speed
	if a.size < 0 {
		if region.GetPeer(a.peer.GetId()) == nil {
			region.Peers = append(region.Peers, a.peer)
			region.RegionEpoch.ConfVer++
			cluster.SetRegion(region)
		}
		a.finished = true
	}
}

func (a *addPeer) TargetStoreID() uint64 {
	return a.peer.GetStoreId()
}

func (a *addPeer) RegionID() uint64 {
	return a.regionID
}

func (a *addPeer) IsFinished() bool {
	return a.finished
}

type removePeer struct {
	regionID uint64
	size     int64
	speed    int64
	epoch    *metapb.RegionEpoch
	peer     *metapb.Peer
	finished bool
}

func (a *removePeer) Desc() string {
	return fmt.Sprintf("remove peer %+v for region %d", a.peer, a.regionID)
}

func (a *removePeer) Step(cluster *ClusterInfo) {
	if a.finished {
		return
	}
	region := cluster.GetRegion(a.regionID)
	if region.RegionEpoch.Version > a.epoch.Version || region.RegionEpoch.ConfVer > a.epoch.ConfVer {
		a.finished = true
		return
	}

	a.size -= a.speed
	if a.size < 0 {
		for i, peer := range region.GetPeers() {
			if peer.GetId() == a.peer.GetId() {
				region.Peers = append(region.Peers[:i], region.Peers[i+1:]...)
				region.RegionEpoch.ConfVer++
				cluster.SetRegion(region)
				break
			}
		}
		a.finished = true
	}
}

func (a *removePeer) TargetStoreID() uint64 {
	return a.peer.GetStoreId()
}

func (a *removePeer) RegionID() uint64 {
	return a.regionID
}

func (a *removePeer) IsFinished() bool {
	return a.finished
}

type writeRegion struct{}

type readRegion struct{}
