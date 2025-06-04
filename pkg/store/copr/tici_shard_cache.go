// Copyright 2025 PingCAP, Inc.
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
	"fmt"
	"sync"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/tici"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Shard represents a shard of data for tici.
type Shard struct {
	ShardID  uint64
	StartKey string
	EndKey   string
	Epoch    uint64
}

// ShardWithAddr represents a shard of data with local cache addresses.
type ShardWithAddr struct {
	Shard
	localCacheAddrs []string
}

// ShardInfoWithAddr represents a shard of data with local cache addresses.
type ShardInfoWithAddr struct {
	coprocessor.ShardInfo
	localCacheAddrs []string
}

type shardIndexMu struct {
	// shardIndex is a map of region ID to shared index.
	sync.RWMutex
	shareds map[uint64]*ShardWithAddr
}

// Client is the interface for the TiCI shard cache client.
type Client interface {
	ScanRanges(ctx context.Context, tableID int64, indexID int64, keyRanges []kv.KeyRange, limit int) ([]ShardWithAddr, error)
	Close()
}

// TiCIShardCacheClient is a gRPC client for the TiCI shard cache service.
type TiCIShardCacheClient struct {
	c *grpc.ClientConn
}

// NewTiCIShardCacheClient creates a new TiCIShardCacheClient instance.
func NewTiCIShardCacheClient() (*TiCIShardCacheClient, error) {
	c, err := grpc.DialContext(context.Background(), "127.0.0.1:50061", grpc.WithInsecure())
	return &TiCIShardCacheClient{c}, err
}

// ScanRanges sends a request to the TiCI shard cache service to scan ranges for a given table and index.
func (c *TiCIShardCacheClient) ScanRanges(ctx context.Context, tableID int64, indexID int64, keyRanges []kv.KeyRange, limit int) (ret []ShardWithAddr, err error) {
	request := &tici.GetShardLocalCacheRequest{
		TableId:   tableID,
		IndexId:   indexID,
		KeyRanges: nil,
		Limit:     int32(limit),
	}

	response := new(tici.GetShardLocalCacheResponse)
	err = c.c.Invoke(ctx, tici.MetaService_GetShardLocalCacheInfo_FullMethodName, request, response)
	if err != nil {
		return nil, err
	}
	if response.Status != 0 {
		return nil, fmt.Errorf("GetShardLocalCacheInfo failed: %d", response.Status)
	}

	var s = "ShardLocalCacheInfos:["
	for _, info := range response.ShardLocalCacheInfos {
		if info != nil {
			s += fmt.Sprintf("[ShardId: %d, StartKey: %s, EndKey: %s, Epoch: %d, LocalCacheAddrs: %v; ]",
				info.Shard.ShardId, info.Shard.StartKey, info.Shard.EndKey, info.Shard.Epoch, info.LocalCacheAddrs)
		}
	}
	s += "]"
	logutil.BgLogger().Info("GetShardLocalCacheInfo", zap.String("info", s))

	for _, s := range response.ShardLocalCacheInfos {
		if s != nil {
			ret = append(ret, ShardWithAddr{
				Shard{
					ShardID:  s.Shard.ShardId,
					StartKey: s.Shard.StartKey,
					EndKey:   s.Shard.EndKey,
					Epoch:    s.Shard.Epoch,
				},
				s.LocalCacheAddrs,
			})
		}
	}

	return ret, nil
}

// Close closes the gRPC client connection.
func (c *TiCIShardCacheClient) Close() {
	c.c.Close()
}

// TiCIShardCache is a cache for TiCI shard information.
type TiCIShardCache struct {
	client Client
	mu     shardIndexMu
}

// NewTiCIShardCache creates a new TiCIShardCache instance with the provided client.
func NewTiCIShardCache(client Client) *TiCIShardCache {
	return &TiCIShardCache{
		client: client,
		mu: shardIndexMu{
			shareds: make(map[uint64]*ShardWithAddr),
		},
	}
}

// ScanRanges scans the ranges for a given table and index, and returns the shard information with local cache addresses.
func (s *TiCIShardCache) ScanRanges(ctx context.Context, tableID int64, indexID int64, keyRanges []kv.KeyRange, limit int) ([]ShardInfoWithAddr, error) {
	shards, err := s.client.ScanRanges(ctx, tableID, indexID, keyRanges, limit)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, shard := range shards {
		s.mu.shareds[shard.ShardID] = &shard
	}

	// Mock the local cache addresses for testing purposes
	ret := make([]ShardInfoWithAddr, 0, len(shards))
	for _, shard := range shards {
		ret = append(ret, ShardInfoWithAddr{
			ShardInfo: coprocessor.ShardInfo{
				ShardId:    shard.ShardID,
				ShardEpoch: shard.Epoch,
				// Ranges: []kv.KeyRange{},
			},
			localCacheAddrs: shard.localCacheAddrs,
		})
	}

	return ret, nil
}
