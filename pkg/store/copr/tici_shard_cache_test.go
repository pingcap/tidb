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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package copr

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
)

type mockClient struct {
	shards []*ShardWithAddr
}

func newMockClient() *mockClient {
	shards := make([]*ShardWithAddr, 0)
	shards = append(shards, &ShardWithAddr{
		Shard: Shard{
			ShardID:  100,
			Epoch:    0,
			StartKey: []byte(""),
			EndKey:   []byte("a"),
		},
		localCacheAddrs: []string{
			"addr-100",
		},
	})
	for i := range 10 {
		shards = append(shards, &ShardWithAddr{
			Shard: Shard{
				ShardID:  uint64(i),
				Epoch:    uint64(i),
				StartKey: []byte(fmt.Sprintf("%c", 'a'+i*2)),
				EndKey:   []byte(fmt.Sprintf("%c", 'a'+i*2+2)),
			},
			localCacheAddrs: []string{
				fmt.Sprintf("addr-%d", i),
			},
		})
	}

	shards = append(shards, &ShardWithAddr{
		Shard: Shard{
			ShardID:  200,
			Epoch:    0,
			StartKey: []byte("u"),
			EndKey:   []byte(""),
		},
		localCacheAddrs: []string{
			"addr-200",
		},
	})
	return &mockClient{shards: shards}
}

func (m *mockClient) ScanRanges(ctx context.Context, tableID int64, indexID int64, keyRanges []kv.KeyRange, limit int) (ret []*ShardWithAddr, err error) {
	need := make([]bool, len(m.shards))

	for _, keyRange := range keyRanges {
		for i, shard := range m.shards {
			if shard.Contains(keyRange.StartKey) || shard.ContainsByEnd(keyRange.EndKey) ||
				(bytes.Compare(keyRange.StartKey, shard.StartKey) == -1 &&
					bytes.Compare(shard.EndKey, keyRange.EndKey) == -1 && len(shard.StartKey) > 0 && len(shard.EndKey) > 0) {
				need[i] = true
			}
		}
	}

	for i, ok := range need {
		if ok {
			m.shards[i].ttl.Store(time.Now().Unix() + shardCacheTTL)
			ret = append(ret, m.shards[i])
		}
	}
	return ret, nil
}

func (m *mockClient) Close() {
}

func TestShardCache(t *testing.T) {
	client := newMockClient()

	cache := NewTiCIShardCache(client)
	ctx := context.Background()

	ranges := make([]kv.KeyRange, 0)
	ranges = append(ranges, kv.KeyRange{
		StartKey: []byte("b"),
		EndKey:   []byte("f"),
	})

	ranges = append(ranges, kv.KeyRange{
		StartKey: []byte("i"),
		EndKey:   []byte("l"),
	})

	locs, err := cache.BatchLocateKeyRanges(ctx, 0, 0, ranges)
	require.NoError(t, err)
	require.Len(t, locs, 5)
	// [a,c), [c,e), [e,g), [i,k), [k,m)
	require.Equal(t, cache.mu.sorted[0].b.Len(), 5)

	ranges = ranges[:0]
	ranges = append(ranges, kv.KeyRange{
		StartKey: []byte("e"),
		EndKey:   []byte("k"),
	})
	locs, err = cache.BatchLocateKeyRanges(ctx, 0, 0, ranges)
	require.NoError(t, err)
	require.Len(t, locs, 3)
	// [e,g), [g,i), [i,k)
	require.Equal(t, cache.mu.sorted[0].b.Len(), 6) // insert [g,i) into the cache

	ranges = make([]kv.KeyRange, 0)
	ranges = append(ranges, kv.KeyRange{
		StartKey: []byte("b"),
		EndKey:   []byte("f"),
	})

	ranges = append(ranges, kv.KeyRange{
		StartKey: []byte("i"),
		EndKey:   []byte("l"),
	})

	locs, err = cache.BatchLocateKeyRanges(ctx, 0, 1, ranges)
	require.NoError(t, err)
	require.Len(t, locs, 5)
	// test another indexID
	require.Equal(t, cache.mu.sorted[0].b.Len(), 6)
	require.Equal(t, cache.mu.sorted[1].b.Len(), 5)
}

func TestShardCacheRandomRangesNoOverlapOrMissing(t *testing.T) {
	seed := time.Now().Unix()
	randGen := rand.New(rand.NewSource(seed))
	t.Logf("rand seed: %d", seed)
	client := &mockClient{
		shards: []*ShardWithAddr{
			{
				Shard: Shard{
					ShardID:  1,
					Epoch:    0,
					StartKey: []byte("a"),
					EndKey:   []byte("e"),
				},
				localCacheAddrs: []string{"addr-1"},
			},
			{
				Shard: Shard{
					ShardID:  2,
					Epoch:    0,
					StartKey: []byte("e"),
					EndKey:   []byte("g"),
				},
				localCacheAddrs: []string{"addr-2"},
			},
			{
				Shard: Shard{
					ShardID:  1,
					Epoch:    0,
					StartKey: []byte("g"),
					EndKey:   []byte("j"),
				},
				localCacheAddrs: []string{"addr-1"},
			},
			{
				Shard: Shard{
					ShardID:  3,
					Epoch:    0,
					StartKey: []byte("g"),
					EndKey:   []byte("z"),
				},
				localCacheAddrs: []string{"addr-1"},
			},
		},
	}
	cache := NewTiCIShardCache(client)
	ctx := context.Background()

	shardIDs := make([]uint64, 0, len(client.shards))
	for _, s := range client.shards {
		shardIDs = append(shardIDs, s.ShardID)
	}

	randomKey := func(rng *rand.Rand, length int) []byte {
		b := make([]byte, length)
		for i := range b {
			b[i] = byte('a' + rng.Intn(20)) // keep headroom so endKey can advance
		}
		return b
	}

	makeRange := func(rng *rand.Rand, length int) kv.KeyRange {
		start := randomKey(rng, length)
		end := append([]byte(nil), start...)
		delta := 1 + rng.Intn(5)
		end[len(end)-1] = byte(min(int('z'), int(end[len(end)-1])+delta))
		return kv.KeyRange{StartKey: start, EndKey: end}
	}

	makeRanges := func(rng *rand.Rand, count int, length int) []kv.KeyRange {
		rgs := make([]kv.KeyRange, 0, count)
		for i := 0; i < count; i++ {
			rgs = append(rgs, makeRange(rng, length))
		}
		return rgs
	}

	sortByStart := func(rgs []kv.KeyRange) {
		sort.Slice(rgs, func(i, j int) bool {
			cmp := bytes.Compare(rgs[i].StartKey, rgs[j].StartKey)
			if cmp == 0 {
				return bytes.Compare(rgs[i].EndKey, rgs[j].EndKey) < 0
			}
			return cmp < 0
		})
	}

	mergeRanges := func(rgs []kv.KeyRange) []kv.KeyRange {
		if len(rgs) == 0 {
			return nil
		}
		sorted := append([]kv.KeyRange(nil), rgs...)
		sortByStart(sorted)
		merged := []kv.KeyRange{sorted[0]}
		for _, r := range sorted[1:] {
			last := &merged[len(merged)-1]
			if len(last.EndKey) == 0 || bytes.Compare(last.EndKey, r.StartKey) >= 0 {
				if len(last.EndKey) == 0 || bytes.Compare(last.EndKey, r.EndKey) >= 0 {
					continue
				}
				last.EndKey = r.EndKey
				continue
			}
			merged = append(merged, r)
		}
		return merged
	}

	expireRandomShard := func() {
		if len(shardIDs) == 0 {
			return
		}
		cache.InvalidateCachedShard(shardIDs[randGen.Intn(len(shardIDs))])
	}

	mutateRandomShard := func(iter int) {
		if len(client.shards) == 0 {
			return
		}
		idx := randGen.Intn(len(client.shards))
		s := client.shards[idx]
		s.Epoch++
		s.localCacheAddrs = []string{fmt.Sprintf("addr-%d-%d", s.ShardID, iter)}
	}

	assertNoOverlapAndMissing := func(req []kv.KeyRange, locs []*ShardLocation) {
		locatedRanges := make([]kv.KeyRange, 0, len(req)+len(locs))
		for _, loc := range locs {
			loc.Ranges.Do(func(ran *kv.KeyRange) {
				locatedRanges = append(locatedRanges, *ran)
			})
		}
		sortByStart(locatedRanges)
		for i := 1; i < len(locatedRanges); i++ {
			require.LessOrEqual(t, bytes.Compare(locatedRanges[i-1].EndKey, locatedRanges[i].StartKey), 0,
				"overlapping ranges: %q-%q and %q-%q", locatedRanges[i-1].StartKey, locatedRanges[i-1].EndKey,
				locatedRanges[i].StartKey, locatedRanges[i].EndKey)
		}
		expectedUnion := mergeRanges(req)
		locatedUnion := mergeRanges(locatedRanges)
		require.Equal(t, expectedUnion, locatedUnion, "missing coverage")
	}

	for iter := 0; iter < 10; iter++ {
		ranges := makeRanges(randGen, 12, iter+1)
		ranges = mergeRanges(ranges)
		if randGen.Intn(2) == 0 {
			expireRandomShard()
		}
		if randGen.Intn(2) == 0 {
			mutateRandomShard(iter)
		}
		locs, err := cache.BatchLocateKeyRanges(ctx, 0, 0, ranges)
		require.NoError(t, err)
		assertNoOverlapAndMissing(ranges, locs)
	}
}
