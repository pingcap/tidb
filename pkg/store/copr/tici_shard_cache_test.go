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

	// for test purpose only
	randomBound [][]byte
	rng         *rand.Rand
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

func newMockClientWithFullRange(rng *rand.Rand) *mockClient {
	shards := make([]*ShardWithAddr, 0)
	shards = append(shards, &ShardWithAddr{
		Shard: Shard{
			ShardID:  100,
			Epoch:    0,
			StartKey: []byte(""),
			EndKey:   []byte(""),
		},
		localCacheAddrs: []string{
			"addr-100",
		},
	})

	randomKey := func(length int) []byte {
		const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		b := make([]byte, length)
		for i := range b {
			b[i] = letters[rng.Intn(len(letters))]
		}
		return b
	}
	randomBound := make([][]byte, 0, 1000)
	for range 1000 {
		randomBound = append(randomBound, randomKey(rng.Intn(10)+3))
	}
	sort.Slice(randomBound, func(i, j int) bool {
		return bytes.Compare(randomBound[i], randomBound[j]) < 0
	})

	unique := [][]byte{randomBound[0]}
	for i := 1; i < len(randomBound); i++ {
		if !bytes.Equal(randomBound[i], randomBound[i-1]) {
			unique = append(unique, randomBound[i])
		}
	}

	return &mockClient{shards: shards, randomBound: unique, rng: rng}
}

func (m *mockClient) genRandomRangeBetween(start, end []byte) []byte {
	var startIdx, endIdx int
	if len(start) == 0 {
		startIdx = -1
	} else {
		startIdx = sort.Search(len(m.randomBound), func(i int) bool {
			return bytes.Compare(m.randomBound[i], start) > 0
		})
	}
	if len(end) == 0 {
		endIdx = len(m.randomBound)
	} else {
		endIdx = sort.Search(len(m.randomBound), func(i int) bool {
			return bytes.Compare(m.randomBound[i], end) > 0
		})
	}
	if endIdx-startIdx <= 1 {
		return nil
	}
	return m.randomBound[m.rng.Intn(endIdx-startIdx-1)+startIdx+1]
}

func (m *mockClient) randomSplitOne() {
	if len(m.shards) == 0 {
		return
	}
	idx := m.rng.Intn(len(m.shards))
	s := m.shards[idx]
	midKey := m.genRandomRangeBetween(s.StartKey, s.EndKey)
	if midKey == nil {
		return
	}
	shard1 := &ShardWithAddr{
		Shard: Shard{
			ShardID:  s.ShardID,
			Epoch:    s.Epoch + 1,
			StartKey: s.StartKey,
			EndKey:   midKey,
		},
		localCacheAddrs: []string{fmt.Sprintf("addr-%d-1", s.ShardID)},
	}
	shard2 := &ShardWithAddr{
		Shard: Shard{
			ShardID:  uint64(m.rng.Int63n(1000)),
			Epoch:    s.Epoch + 1,
			StartKey: midKey,
			EndKey:   s.EndKey,
		},
		localCacheAddrs: []string{fmt.Sprintf("addr-%d-2", s.ShardID)},
	}
	m.shards = append(m.shards[:idx], append([]*ShardWithAddr{shard1, shard2}, m.shards[idx+1:]...)...)
}

func (m *mockClient) randomMergeOne() {
	if len(m.shards) < 2 {
		return
	}
	idx := m.rng.Intn(len(m.shards) - 1)
	s1 := m.shards[idx]
	s2 := m.shards[idx+1]
	if !bytes.Equal(s1.EndKey, s2.StartKey) {
		panic("Test Fail")
	}
	merged := &ShardWithAddr{
		Shard: Shard{
			ShardID:  s1.ShardID,
			Epoch:    s1.Epoch + 1,
			StartKey: s1.StartKey,
			EndKey:   s2.EndKey,
		},
		localCacheAddrs: []string{fmt.Sprintf("addr-%d-merged", s1.ShardID)},
	}
	m.shards = append(m.shards[:idx], append([]*ShardWithAddr{merged}, m.shards[idx+2:]...)...)
}

func TestShardCacheRandomRangesNoOverlapOrMissing(t *testing.T) {
	var seed int64 = time.Now().Unix()
	t.Logf("test seed: %d", seed)
	rng := rand.New(rand.NewSource(seed))
	client := newMockClientWithFullRange(rng)
	cache := NewTiCIShardCache(client)
	ctx := context.Background()

	sortByStart := func(ranges []kv.KeyRange) {
		sort.Slice(ranges, func(i, j int) bool {
			return bytes.Compare(ranges[i].StartKey, ranges[j].StartKey) < 0
		})
	}
	mergeRanges := func(ranges []kv.KeyRange) []kv.KeyRange {
		if len(ranges) == 0 {
			return nil
		}
		sortByStart(ranges)
		merged := make([]kv.KeyRange, 0, len(ranges))
		cur := ranges[0]
		for i := 1; i < len(ranges); i++ {
			if bytes.Compare(cur.EndKey, ranges[i].StartKey) >= 0 {
				if bytes.Compare(cur.EndKey, ranges[i].EndKey) < 0 {
					cur.EndKey = ranges[i].EndKey
				}
			} else {
				merged = append(merged, cur)
				cur = ranges[i]
			}
		}
		merged = append(merged, cur)
		return merged
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

	const rangeCount = 10
	const testCount = 100
	for range testCount {
		bound := make([][]byte, 0, rangeCount*2)
		ranges := make([]kv.KeyRange, 0)
		for range rangeCount * 2 {
			k := client.genRandomRangeBetween([]byte(""), []byte(""))
			bound = append(bound, k)
		}
		sort.Slice(bound, func(i, j int) bool {
			return bytes.Compare(bound[i], bound[j]) < 0
		})
		for i := range rangeCount {
			ranges = append(ranges, kv.KeyRange{
				StartKey: bound[i*2],
				EndKey:   bound[i*2+1],
			})
		}
		if rng.Intn(2) == 0 {
			for range 20 {
				client.randomSplitOne()
			}
			for range 5 {
				client.randomMergeOne()
			}
		}
		// invalidate the cache
		for id := range cache.mu.shards {
			if id%3 == 0 {
				cache.InvalidateCachedShard(id)
			}
		}

		locs, err := cache.BatchLocateKeyRanges(ctx, 0, 0, ranges)
		require.NoError(t, err)
		assertNoOverlapAndMissing(ranges, locs)
	}
}
