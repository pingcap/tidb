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

package server

import (
	"context"
	"strconv"
	"sync"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
	log "github.com/sirupsen/logrus"
)

type heartbeatStream interface {
	Send(*pdpb.RegionHeartbeatResponse) error
}

type streamUpdate struct {
	storeID uint64
	stream  heartbeatStream
}

type heartbeatStreams struct {
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	clusterID uint64
	streams   map[uint64]heartbeatStream
	msgCh     chan *pdpb.RegionHeartbeatResponse
	streamCh  chan streamUpdate
}

func newHeartbeatStreams(clusterID uint64) *heartbeatStreams {
	ctx, cancel := context.WithCancel(context.Background())
	hs := &heartbeatStreams{
		ctx:       ctx,
		cancel:    cancel,
		clusterID: clusterID,
		streams:   make(map[uint64]heartbeatStream),
		msgCh:     make(chan *pdpb.RegionHeartbeatResponse, regionheartbeatSendChanCap),
		streamCh:  make(chan streamUpdate, 1),
	}
	hs.wg.Add(1)
	go hs.run()
	return hs
}

func (s *heartbeatStreams) run() {
	defer s.wg.Done()
	for {
		select {
		case update := <-s.streamCh:
			s.streams[update.storeID] = update.stream
		case msg := <-s.msgCh:
			storeID := msg.GetTargetPeer().GetStoreId()
			storeLabel := strconv.FormatUint(storeID, 10)
			if stream, ok := s.streams[storeID]; ok {
				if err := stream.Send(msg); err != nil {
					log.Errorf("[region %v] send heartbeat message fail: %v", msg.RegionId, err)
					delete(s.streams, storeID)
					regionHeartbeatCounter.WithLabelValues(storeLabel, "push", "err").Inc()
				} else {
					regionHeartbeatCounter.WithLabelValues(storeLabel, "push", "ok").Inc()
				}
			} else {
				log.Debugf("[region %v] heartbeat stream not found for store %v, skip send message", msg.RegionId, storeID)
				regionHeartbeatCounter.WithLabelValues(storeLabel, "push", "skip").Inc()
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *heartbeatStreams) Close() {
	s.cancel()
	s.wg.Wait()
}

func (s *heartbeatStreams) bindStream(storeID uint64, stream heartbeatStream) {
	update := streamUpdate{
		storeID: storeID,
		stream:  stream,
	}
	select {
	case s.streamCh <- update:
	case <-s.ctx.Done():
	}
}

func (s *heartbeatStreams) sendMsg(region *core.RegionInfo, msg *pdpb.RegionHeartbeatResponse) {
	if region.Leader == nil {
		return
	}

	msg.Header = &pdpb.ResponseHeader{ClusterId: s.clusterID}
	msg.RegionId = region.GetId()
	msg.RegionEpoch = region.GetRegionEpoch()
	msg.TargetPeer = region.Leader

	select {
	case s.msgCh <- msg:
	case <-s.ctx.Done():
	}
}

func (s *heartbeatStreams) sendErr(region *core.RegionInfo, errType pdpb.ErrorType, errMsg string, storeLabel string) {
	regionHeartbeatCounter.WithLabelValues(storeLabel, "report", "err").Inc()

	msg := &pdpb.RegionHeartbeatResponse{
		Header: &pdpb.ResponseHeader{
			ClusterId: s.clusterID,
			Error: &pdpb.Error{
				Type:    errType,
				Message: errMsg,
			},
		},
	}

	select {
	case s.msgCh <- msg:
	case <-s.ctx.Done():
	}
}
