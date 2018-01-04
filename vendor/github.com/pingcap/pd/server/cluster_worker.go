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

package server

import (
	"bytes"

	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
	log "github.com/sirupsen/logrus"
)

// HandleRegionHeartbeat processes RegionInfo reports from client.
func (c *RaftCluster) HandleRegionHeartbeat(region *core.RegionInfo) error {
	if err := c.cachedCluster.handleRegionHeartbeat(region); err != nil {
		return errors.Trace(err)
	}

	// If the region peer count is 0, then we should not handle this.
	if len(region.GetPeers()) == 0 {
		log.Warnf("invalid region, zero region peer count - %v", region)
		return errors.Errorf("invalid region, zero region peer count - %v", region)
	}

	c.coordinator.dispatch(region)
	return nil
}

func (c *RaftCluster) handleAskSplit(request *pdpb.AskSplitRequest) (*pdpb.AskSplitResponse, error) {
	reqRegion := request.GetRegion()
	startKey := reqRegion.GetStartKey()
	region, _ := c.GetRegionByKey(startKey)

	// If the request epoch is less than current region epoch, then returns an error.
	reqRegionEpoch := reqRegion.GetRegionEpoch()
	regionEpoch := region.GetRegionEpoch()
	if reqRegionEpoch.GetVersion() < regionEpoch.GetVersion() ||
		reqRegionEpoch.GetConfVer() < regionEpoch.GetConfVer() {
		return nil, errors.Errorf("invalid region epoch, request: %v, currenrt: %v", reqRegionEpoch, regionEpoch)
	}

	newRegionID, err := c.s.idAlloc.Alloc()
	if err != nil {
		return nil, errors.Trace(err)
	}

	peerIDs := make([]uint64, len(request.Region.Peers))
	for i := 0; i < len(peerIDs); i++ {
		if peerIDs[i], err = c.s.idAlloc.Alloc(); err != nil {
			return nil, errors.Trace(err)
		}
	}

	split := &pdpb.AskSplitResponse{
		NewRegionId: newRegionID,
		NewPeerIds:  peerIDs,
	}

	return split, nil
}

func (c *RaftCluster) checkSplitRegion(left *metapb.Region, right *metapb.Region) error {
	if left == nil || right == nil {
		return errors.New("invalid split region")
	}

	if !bytes.Equal(left.GetEndKey(), right.GetStartKey()) {
		return errors.New("invalid split region")
	}

	if len(right.GetEndKey()) == 0 || bytes.Compare(left.GetStartKey(), right.GetEndKey()) < 0 {
		return nil
	}

	return errors.New("invalid split region")
}

func (c *RaftCluster) handleReportSplit(request *pdpb.ReportSplitRequest) (*pdpb.ReportSplitResponse, error) {
	left := request.GetLeft()
	right := request.GetRight()

	err := c.checkSplitRegion(left, right)
	if err != nil {
		log.Warnf("report split region is invalid - %v, %v", request, errors.ErrorStack(err))
		return nil, errors.Trace(err)
	}

	// Build origin region by using left and right.
	originRegion := proto.Clone(right).(*metapb.Region)
	originRegion.RegionEpoch = nil
	originRegion.StartKey = left.GetStartKey()
	log.Infof("[region %d] region split, generate new region: %v", originRegion.GetId(), left)
	return &pdpb.ReportSplitResponse{}, nil
}
