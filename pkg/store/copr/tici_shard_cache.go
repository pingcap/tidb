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
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/tici"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	btreeDegree           = 32
	defaultShardsPerBatch = 128
)

// Shard represents a shard of data for tici.
type Shard struct {
	ShardID  uint64
	StartKey []byte
	EndKey   []byte
	Epoch    uint64
}

// Contains checks if the shard contains the key.
func (s *Shard) Contains(key []byte) bool {
	return bytes.Compare(s.StartKey, key) <= 0 &&
		(bytes.Compare(key, s.EndKey) < 0 || len(s.EndKey) == 0)
}

// ContainsByEnd checks if the shard contains the key by its end key.
func (s *Shard) ContainsByEnd(key []byte) bool {
	if len(key) == 0 {
		return len(s.EndKey) == 0
	}
	return bytes.Compare(s.StartKey, key) < 0 &&
		(bytes.Compare(key, s.EndKey) <= 0 || len(s.EndKey) == 0)
}

const (
	expiredTTL    = -1
	shardCacheTTL = 600 // 10min
)

// ShardWithAddr represents a shard of data with local cache addresses.
type ShardWithAddr struct {
	Shard
	localCacheAddrs []string
	ttl             atomic.Int64
}

func (s *ShardWithAddr) invalidate() {
	s.ttl.Store(expiredTTL)
}

// CheckShardCacheTTL checks if the shard cache is still valid based on the provided timestamp.
func (s *ShardWithAddr) CheckShardCacheTTL(ts int64) bool {
	for {
		oldTTL := s.ttl.Load()
		if oldTTL < ts {
			return false
		}
		if s.ttl.CompareAndSwap(oldTTL, ts+shardCacheTTL) {
			return true
		}
	}
}

// ShardLocation represents a shard location with its ranges.
type ShardLocation struct {
	*ShardWithAddr
	Ranges *KeyRanges
}

type shardIndexMu struct {
	sync.RWMutex
	shards map[uint64]*ShardWithAddr
	sorted *SortedShards
}

// Client is the interface for the TiCI shard cache client.
type Client interface {
	ScanRanges(ctx context.Context, tableID int64, indexID int64, keyRanges []kv.KeyRange, limit int) ([]*ShardWithAddr, error)
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
func (c *TiCIShardCacheClient) ScanRanges(ctx context.Context, tableID int64, indexID int64, keyRanges []kv.KeyRange, limit int) (ret []*ShardWithAddr, err error) {
	ticiKeyRanges := make([]*tici.KeyRange, 0, len(keyRanges))
	for _, r := range keyRanges {
		ticiKeyRanges = append(ticiKeyRanges, &tici.KeyRange{
			StartKey: r.StartKey,
			EndKey:   r.EndKey,
		})
	}

	request := &tici.GetShardLocalCacheRequest{
		TableId:   tableID,
		IndexId:   indexID,
		KeyRanges: ticiKeyRanges,
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
			s += fmt.Sprintf("[ShardId: %d, StartKey: %v, EndKey: %v, Epoch: %d, LocalCacheAddrs: %v; ]",
				info.Shard.ShardId, info.Shard.StartKey, info.Shard.EndKey, info.Shard.Epoch, info.LocalCacheAddrs)
		}
	}
	s += "]"
	logutil.BgLogger().Info("GetShardLocalCacheInfo", zap.String("info", s))

	ts := time.Now().Unix()
	for _, s := range response.ShardLocalCacheInfos {
		if s != nil {
			shard := &ShardWithAddr{}
			shard.ShardID = s.Shard.ShardId
			shard.StartKey = s.Shard.StartKey
			shard.EndKey = s.Shard.EndKey
			shard.Epoch = s.Shard.Epoch
			shard.localCacheAddrs = s.LocalCacheAddrs
			shard.ttl.Store(ts + shardCacheTTL)
			ret = append(ret, shard)
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
			shards: make(map[uint64]*ShardWithAddr),
			sorted: NewSortedShards(btreeDegree),
		},
	}
}

// SplitKeyRangesByLocations splits key ranges by shard locations.
func (s *TiCIShardCache) SplitKeyRangesByLocations(
	ctx context.Context,
	tableID int64,
	indexID int64,
	ranges *KeyRanges,
	limit int,
) ([]*ShardLocation, error) {
	if limit == 0 || ranges.Len() <= 0 {
		return nil, nil
	}

	kvRanges := make([]kv.KeyRange, 0, ranges.Len())
	for i := range ranges.Len() {
		kvRanges = append(kvRanges, kv.KeyRange{
			StartKey: ranges.At(i).StartKey,
			EndKey:   ranges.At(i).EndKey,
		})
	}
	locs, err := s.BatchLocateKeyRanges(ctx, tableID, indexID, kvRanges)
	if err != nil {
		return nil, err
	}

	res := make([]*ShardLocation, 0)
	nextLocIndex := 0
	for ranges.Len() > 0 {
		loc := locs[nextLocIndex]
		if nextLocIndex == (len(locs) - 1) {
			res = append(res, &ShardLocation{
				ShardWithAddr: loc.ShardWithAddr,
				Ranges:        ranges,
			})
			break
		}
		nextLocIndex++
		isBreak := false
		res, ranges, isBreak = s.splitKeyRangesByLocations(loc.ShardWithAddr, ranges, res)
		if isBreak {
			break
		}
	}
	return res, nil
}

// BatchLocateKeyRanges locates key ranges in the TiCI shard cache.
func (s *TiCIShardCache) BatchLocateKeyRanges(
	ctx context.Context,
	tableID int64,
	indexID int64,
	keyRanges []kv.KeyRange,
) ([]*ShardLocation, error) {
	var ss = "BatchLocateKeyRanges keyRanges:["
	for _, info := range keyRanges {
		ss += fmt.Sprintf("[ StartKey: %v, EndKey: %v ]",
			[]byte(info.StartKey), []byte(info.EndKey))
	}
	ss += "]"
	logutil.BgLogger().Info("TiCIShardCache BatchLocateKeyRanges",
		zap.String("keyRanges", ss))
	uncachedRanges := make([]kv.KeyRange, 0, len(keyRanges))
	cachedShards := make([]*ShardWithAddr, 0, len(keyRanges))
	// 1. find shards from cache
	var lastShard *ShardWithAddr
	for _, keyRange := range keyRanges {
		if lastShard != nil {
			if lastShard.ContainsByEnd(keyRange.EndKey) {
				continue
			} else if lastShard.Contains(keyRange.StartKey) {
				keyRange.StartKey = kv.Key(lastShard.EndKey)
			}
		}

		shard := s.tryFindShardByKey(keyRange.StartKey, false)
		lastShard = shard
		if shard == nil {
			uncachedRanges = append(uncachedRanges, keyRange)
			continue
		}

		cachedShards = append(cachedShards, shard)
		if shard.ContainsByEnd(keyRange.EndKey) {
			continue
		}

		keyRange.StartKey = kv.Key(shard.EndKey)
		containsAll := false
	outer:
		for {
			batchShardInCache, err := s.scanShardsFromCache(ctx, keyRange.StartKey, keyRange.EndKey, defaultShardsPerBatch)
			if err != nil {
				return nil, err
			}
			for _, shard = range batchShardInCache {
				if !shard.Contains(keyRange.StartKey) { // uncached hole, load the rest shards
					break outer
				}
				cachedShards = append(cachedShards, shard)
				lastShard = shard
				if shard.ContainsByEnd(keyRange.EndKey) {
					// the range is fully hit in the shard cache.
					containsAll = true
					break outer
				}
				keyRange.StartKey = kv.Key(shard.EndKey)
			}
			if len(batchShardInCache) < defaultShardsPerBatch { // shard cache miss, load the rest shards
				break
			}
		}
		if !containsAll {
			uncachedRanges = append(uncachedRanges, keyRange)
		}
	}

	merger := newBatchLocateShardsMerger(cachedShards, len(cachedShards)+len(uncachedRanges))
	retry := 0
	// 2. load remaining shards from tici client
	for len(uncachedRanges) > 0 {
		shards, err := s.BatchLoadShardsWithKeyRanges(ctx, tableID, indexID, uncachedRanges, defaultShardsPerBatch)
		if err != nil {
			return nil, err
		}
		if len(shards) == 0 {
			logutil.BgLogger().Warn("TiCIShardCache BatchLoadShardsWithKeyRanges return empty shards without err")
			break
		}
		for _, shard := range shards {
			merger.appendShard(shard)
		}
		uncachedRanges = rangesAfterKey(uncachedRanges, shards[len(shards)-1].EndKey)
		retry++
		if retry > 10 {
			var s = "uncachedRanges:["
			for _, info := range uncachedRanges {
				s += fmt.Sprintf("[ StartKey: %v, EndKey: %v ]",
					[]byte(info.StartKey), []byte(info.EndKey))
			}
			s += "]"
			logutil.BgLogger().Warn("TiCIShardCache BatchLoadShardsWithKeyRanges retry too many times, may be a bug",
				zap.Int("retry", retry), zap.String("uncachedRanges", s))
			err = errors.New("TiCIShardCache BatchLoadShardsWithKeyRanges retry too many times, may be a bug")
			return nil, err
		}
	}
	return merger.build(), nil
}

func rangesAfterKey(keyRanges []kv.KeyRange, splitKey []byte) []kv.KeyRange {
	if len(keyRanges) == 0 {
		return nil
	}
	if len(splitKey) == 0 || len(keyRanges[len(keyRanges)-1].EndKey) > 0 && bytes.Compare(splitKey, keyRanges[len(keyRanges)-1].EndKey) >= 0 {
		// fast check, if all ranges are loaded from PD, quit the loop.
		return nil
	}

	n := sort.Search(len(keyRanges), func(i int) bool {
		return len(keyRanges[i].EndKey) == 0 || bytes.Compare(keyRanges[i].EndKey, splitKey) > 0
	})

	keyRanges = keyRanges[n:]
	if bytes.Compare(splitKey, keyRanges[0].StartKey) > 0 {
		keyRanges[0].StartKey = splitKey
	}
	return keyRanges
}

// BatchLoadShardsWithKeyRanges loads shards from the TiCI shard cache client based on the provided key ranges.
func (s *TiCIShardCache) BatchLoadShardsWithKeyRanges(
	ctx context.Context,
	tableID int64,
	indexID int64,
	keyRanges []kv.KeyRange,
	limit int,
) (shards []*ShardWithAddr, err error) {
	if len(keyRanges) == 0 {
		return nil, nil
	}
	shards, err = s.client.ScanRanges(ctx, tableID, indexID, keyRanges, limit)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, shard := range shards {
		s.insertShardToCache(shard, true)
	}

	return shards, nil
}

func (s *TiCIShardCache) insertShardToCache(cachedShard *ShardWithAddr, invalidateOldShard bool) bool {
	return s.mu.insertShardToCache(cachedShard, invalidateOldShard)
}

func (s *TiCIShardCache) tryFindShardByKey(key []byte, isEndKey bool) (r *ShardWithAddr) {
	var expired bool
	r, expired = s.searchCachedShardByKey(key, isEndKey)
	if r == nil || expired {
		return nil
	}
	return r
}

func (s *TiCIShardCache) searchCachedShardByKey(key []byte, isEndKey bool) (*ShardWithAddr, bool) {
	s.mu.RLock()
	shard := s.mu.sorted.SearchByKey(key, isEndKey)
	defer s.mu.RUnlock()
	if shard == nil {
		return nil, false
	}
	return shard, false
}

func (s *TiCIShardCache) scanShardsFromCache(ctx context.Context, startKey, endKey []byte, limit int) ([]*ShardWithAddr, error) {
	if limit == 0 {
		return nil, nil
	}
	var shards []*ShardWithAddr
	s.mu.RLock()
	defer s.mu.RUnlock()
	shards = s.mu.sorted.AscendGreaterOrEqual(startKey, endKey, limit)
	return shards, nil
}

func (s *TiCIShardCache) splitKeyRangesByLocations(loc *ShardWithAddr, ranges *KeyRanges, res []*ShardLocation) ([]*ShardLocation, *KeyRanges, bool) {
	var r kv.KeyRange
	var i int
	for ; i < ranges.Len(); i++ {
		r = ranges.At(i)
		if !(loc.Contains(r.EndKey) || bytes.Equal(loc.EndKey, r.EndKey)) {
			break
		}
	}
	if i == ranges.Len() {
		res = append(res, &ShardLocation{ShardWithAddr: loc, Ranges: ranges})
		return res, ranges, true
	}
	if loc.Contains(r.StartKey) {
		taskRanges := ranges.Slice(0, i)
		taskRanges.last = &kv.KeyRange{
			StartKey: r.StartKey,
			EndKey:   loc.EndKey,
		}
		res = append(res, &ShardLocation{ShardWithAddr: loc, Ranges: taskRanges})
		ranges = ranges.Slice(i+1, ranges.Len())
		ranges.first = &kv.KeyRange{
			StartKey: loc.EndKey,
			EndKey:   r.EndKey,
		}
	} else {
		if i > 0 {
			taskRanges := ranges.Slice(0, i)
			res = append(res, &ShardLocation{ShardWithAddr: loc, Ranges: taskRanges})
			ranges = ranges.Slice(i, ranges.Len())
		}
	}
	return res, ranges, false
}

type batchLocateShardsMerger struct {
	lastEndKey      []byte
	cachedIdx       int
	cachedShards    []*ShardWithAddr
	mergedLocations []*ShardLocation
}

func newBatchLocateShardsMerger(cachedShards []*ShardWithAddr, sizeHint int) *batchLocateShardsMerger {
	return &batchLocateShardsMerger{
		lastEndKey:      nil,
		cachedShards:    cachedShards,
		mergedLocations: make([]*ShardLocation, 0, sizeHint),
	}
}

func (m *batchLocateShardsMerger) appendKeyLocation(shard *ShardWithAddr) {
	m.mergedLocations = append(m.mergedLocations, &ShardLocation{
		ShardWithAddr: shard,
		Ranges:        NewKeyRanges([]kv.KeyRange{{StartKey: kv.Key(shard.StartKey), EndKey: kv.Key(shard.EndKey)}}),
	})
}

func (m *batchLocateShardsMerger) appendShard(uncachedShard *ShardWithAddr) {
	defer func() {
		endKey := uncachedShard.EndKey
		if len(endKey) == 0 {
			m.cachedIdx = len(m.cachedShards)
		} else {
			m.lastEndKey = uncachedShard.EndKey
		}
	}()
	if len(uncachedShard.StartKey) == 0 {
		m.appendKeyLocation(uncachedShard)
		return
	}

	if m.lastEndKey != nil && bytes.Compare(m.lastEndKey, kv.Key(uncachedShard.StartKey)) >= 0 {
		m.appendKeyLocation(uncachedShard)
		return
	}

	for ; m.cachedIdx < len(m.cachedShards); m.cachedIdx++ {
		if m.lastEndKey != nil && bytes.Compare(m.lastEndKey, kv.Key(m.cachedShards[m.cachedIdx].EndKey)) >= 0 {
			continue
		}
		if bytes.Compare(kv.Key(m.cachedShards[m.cachedIdx].StartKey), kv.Key(uncachedShard.StartKey)) >= 0 {
			break
		}
		m.appendKeyLocation(m.cachedShards[m.cachedIdx])
	}
	m.appendKeyLocation(uncachedShard)
}

func (m *batchLocateShardsMerger) build() []*ShardLocation {
	for ; m.cachedIdx < len(m.cachedShards); m.cachedIdx++ {
		if m.lastEndKey != nil && bytes.Compare(m.lastEndKey, kv.Key(m.cachedShards[m.cachedIdx].EndKey)) >= 0 {
			continue
		}
		m.appendKeyLocation(m.cachedShards[m.cachedIdx])
	}
	return m.mergedLocations
}

func (mu *shardIndexMu) insertShardToCache(cachedShard *ShardWithAddr, invalidateOldShard bool) bool {
	intersectingShard, _ := mu.sorted.removeIntersecting(cachedShard)
	for _, item := range intersectingShard {
		if invalidateOldShard {
			item.cachedShard.invalidate()
			mu.shards[item.cachedShard.ShardID] = nil
		}
	}
	mu.sorted.ReplaceOrInsert(cachedShard)
	mu.shards[cachedShard.ShardID] = cachedShard
	return true
}

// InvalidateCachedShard invalidates the cached shard with the given shardID.
func (s *TiCIShardCache) InvalidateCachedShard(shardID uint64) {
	logutil.BgLogger().Info("InvalidateCachedShard", zap.Uint64("shardID", shardID))
	cachedShard := s.GetCachedShardWithRLock(shardID)
	if cachedShard == nil {
		return
	}
	cachedShard.invalidate()
}

// GetCachedShardWithRLock retrieves the cached shard with the given shardID using a read lock.
func (s *TiCIShardCache) GetCachedShardWithRLock(shardID uint64) *ShardWithAddr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if shard, ok := s.mu.shards[shardID]; ok {
		return shard
	}
	return nil
}
