// Copyright 2021 PingCAP, Inc.
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

package mocktikv

import (
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/ddl/placement"
)

// Session stores session scope rpc data.
type Session struct {
	cluster   *Cluster
	mvccStore MVCCStore

	// storeID stores id for current request
	storeID uint64
	// startKey is used for handling normal request.
	startKey []byte
	endKey   []byte
	// rawStartKey is used for handling coprocessor request.
	rawStartKey []byte
	rawEndKey   []byte
	// isolationLevel is used for current request.
	isolationLevel kvrpcpb.IsolationLevel
	resolvedLocks  []uint64
}

func (s *Session) checkRequestContext(ctx *kvrpcpb.Context) *errorpb.Error {
	ctxPeer := ctx.GetPeer()
	if ctxPeer != nil && ctxPeer.GetStoreId() != s.storeID {
		return &errorpb.Error{
			Message:       *proto.String("store not match"),
			StoreNotMatch: &errorpb.StoreNotMatch{},
		}
	}
	region, leaderID := s.cluster.GetRegion(ctx.GetRegionId())
	// No region found.
	if region == nil {
		return &errorpb.Error{
			Message: *proto.String("region not found"),
			RegionNotFound: &errorpb.RegionNotFound{
				RegionId: *proto.Uint64(ctx.GetRegionId()),
			},
		}
	}
	var storePeer, leaderPeer *metapb.Peer
	for _, p := range region.Peers {
		if p.GetStoreId() == s.storeID {
			storePeer = p
		}
		if p.GetId() == leaderID {
			leaderPeer = p
		}
	}
	// The Store does not contain a Peer of the Region.
	if storePeer == nil {
		return &errorpb.Error{
			Message: *proto.String("region not found"),
			RegionNotFound: &errorpb.RegionNotFound{
				RegionId: *proto.Uint64(ctx.GetRegionId()),
			},
		}
	}
	// No leader.
	if leaderPeer == nil {
		return &errorpb.Error{
			Message: *proto.String("no leader"),
			NotLeader: &errorpb.NotLeader{
				RegionId: *proto.Uint64(ctx.GetRegionId()),
			},
		}
	}
	// The Peer on the Store is not leader. If it's tiflash store , we pass this check.
	if storePeer.GetId() != leaderPeer.GetId() && !isTiFlashStore(s.cluster.GetStore(storePeer.GetStoreId())) {
		return &errorpb.Error{
			Message: *proto.String("not leader"),
			NotLeader: &errorpb.NotLeader{
				RegionId: *proto.Uint64(ctx.GetRegionId()),
				Leader:   leaderPeer,
			},
		}
	}
	// Region epoch does not match.
	if !proto.Equal(region.GetRegionEpoch(), ctx.GetRegionEpoch()) {
		nextRegion, _ := s.cluster.GetRegionByKey(region.GetEndKey())
		currentRegions := []*metapb.Region{region}
		if nextRegion != nil {
			currentRegions = append(currentRegions, nextRegion)
		}
		return &errorpb.Error{
			Message: *proto.String("epoch not match"),
			EpochNotMatch: &errorpb.EpochNotMatch{
				CurrentRegions: currentRegions,
			},
		}
	}
	s.startKey, s.endKey = region.StartKey, region.EndKey
	s.isolationLevel = ctx.IsolationLevel
	s.resolvedLocks = ctx.ResolvedLocks
	return nil
}

func (s *Session) checkRequestSize(size int) *errorpb.Error {
	// TiKV has a limitation on raft log size.
	// mocktikv has no raft inside, so we check the request's size instead.
	if size >= requestMaxSize {
		return &errorpb.Error{
			RaftEntryTooLarge: &errorpb.RaftEntryTooLarge{},
		}
	}
	return nil
}

func (s *Session) checkRequest(ctx *kvrpcpb.Context, size int) *errorpb.Error {
	if err := s.checkRequestContext(ctx); err != nil {
		return err
	}
	return s.checkRequestSize(size)
}

func (s *Session) checkKeyInRegion(key []byte) bool {
	return regionContains(s.startKey, s.endKey, NewMvccKey(key))
}

func isTiFlashStore(store *metapb.Store) bool {
	for _, l := range store.GetLabels() {
		if l.GetKey() == placement.EngineLabelKey && l.GetValue() == placement.EngineLabelTiFlash {
			return true
		}
	}
	return false
}
