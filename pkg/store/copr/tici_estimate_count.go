// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package copr

import (
	"context"
	"math"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type ticiEstimateShardGroup struct {
	addr          string
	shardInfos    []*coprocessor.ShardInfo
	uniqueShardID map[uint64]struct{}
}

// EstimateTiCICount picks one TiFlash store, estimates the matching rows on that store,
// and then scales the result by the ratio of total hit shards to sampled-store hit shards.
func (s *Store) EstimateTiCICount(ctx context.Context, req *kv.TiCIEstimateCountRequest, timeout time.Duration) (uint64, error) {
	if req == nil {
		return 0, errors.New("TiCI estimate count request is nil")
	}
	if req.FTSQueryInfo == nil {
		return 0, errors.New("TiCI estimate count request missing FTS query info")
	}
	if req.KeyRanges == nil || req.KeyRanges.TotalRangeNum() == 0 {
		return 0, nil
	}
	cache := s.GetTiCIShardCache()
	if cache == nil {
		return 0, errors.New("TiCI shard cache is unavailable")
	}

	kvRanges := req.KeyRanges.AppendSelfTo(nil)
	locations, err := cache.BatchLocateKeyRanges(ctx, req.TableID, req.IndexID, kvRanges)
	if err != nil {
		return 0, err
	}
	groups, totalUniqueShards := buildTiCIEstimateShardGroups(locations)
	if totalUniqueShards == 0 {
		return 0, nil
	}
	group := chooseTiCIEstimateShardGroup(groups)
	if group == nil || len(group.shardInfos) == 0 {
		return 0, errors.New("tiflash_fts node is unavailable")
	}

	pbReq, err := buildTiCIEstimatePBRequest(req, group.shardInfos)
	if err != nil {
		return 0, err
	}
	clientReq := tikvrpc.NewRequest(tikvrpc.CmdEstimateTiCICount, pbReq)
	clientReq.StoreTp = getEndPointType(kv.TiFlash)
	resp, err := s.GetTiKVClient().SendRequest(ctx, group.addr, clientReq, timeout)
	if err != nil {
		return 0, err
	}
	pbResp, ok := resp.Resp.(*coprocessor.TiCIEstimateCountResponse)
	if !ok {
		return 0, errors.Errorf("unexpected EstimateTiCICount response type %T", resp.Resp)
	}
	if pbResp.OtherError != "" {
		return 0, errors.New(pbResp.OtherError)
	}

	sampledUniqueShards := uint64(len(group.uniqueShardID))
	if sampledUniqueShards == 0 || sampledUniqueShards >= totalUniqueShards {
		return pbResp.EstCount, nil
	}
	return uint64(math.Ceil(float64(pbResp.EstCount) * float64(totalUniqueShards) / float64(sampledUniqueShards))), nil
}

func buildTiCIEstimatePBRequest(req *kv.TiCIEstimateCountRequest, shardInfos []*coprocessor.ShardInfo) (*coprocessor.TiCIEstimateCountRequest, error) {
	ftsQueryInfo, err := proto.Marshal(req.FTSQueryInfo)
	if err != nil {
		return nil, err
	}
	return &coprocessor.TiCIEstimateCountRequest{
		Context:        &kvrpcpb.Context{},
		StartTs:        req.StartTS,
		TableId:        req.TableID,
		FtsQueryInfo:   ftsQueryInfo,
		IndexId:        req.IndexID,
		TimeZoneName:   req.TimeZoneName,
		TimeZoneOffset: req.TimeZoneOffset,
		ShardInfos:     shardInfos,
	}, nil
}

func buildTiCIEstimateShardGroups(shards []*ShardLocation) (map[string]*ticiEstimateShardGroup, uint64) {
	groups := make(map[string]*ticiEstimateShardGroup)
	totalUnique := make(map[uint64]struct{})
	for _, shard := range shards {
		if shard == nil || shard.ShardWithAddr == nil || len(shard.localCacheAddrs) == 0 || shard.Ranges == nil {
			continue
		}
		addr := shard.localCacheAddrs[0]
		group, ok := groups[addr]
		if !ok {
			group = &ticiEstimateShardGroup{
				addr:          addr,
				shardInfos:    make([]*coprocessor.ShardInfo, 0),
				uniqueShardID: make(map[uint64]struct{}),
			}
			groups[addr] = group
		}
		group.shardInfos = append(group.shardInfos, &coprocessor.ShardInfo{
			ShardId:    shard.ShardID,
			ShardEpoch: shard.Epoch,
			Ranges:     shard.Ranges.ToPBRanges(),
		})
		group.uniqueShardID[shard.ShardID] = struct{}{}
		totalUnique[shard.ShardID] = struct{}{}
	}
	return groups, uint64(len(totalUnique))
}

func chooseTiCIEstimateShardGroup(groups map[string]*ticiEstimateShardGroup) *ticiEstimateShardGroup {
	var best *ticiEstimateShardGroup
	for _, group := range groups {
		if best == nil || len(group.uniqueShardID) > len(best.uniqueShardID) || (len(group.uniqueShardID) == len(best.uniqueShardID) && group.addr < best.addr) {
			best = group
		}
	}
	return best
}
