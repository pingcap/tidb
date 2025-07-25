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
	"testing"

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
	require.Equal(t, cache.mu.sorted.b.Len(), 5)

	ranges = ranges[:0]
	ranges = append(ranges, kv.KeyRange{
		StartKey: []byte("e"),
		EndKey:   []byte("k"),
	})
	locs, err = cache.BatchLocateKeyRanges(ctx, 0, 0, ranges)
	require.NoError(t, err)
	require.Len(t, locs, 3)
	// [e,g), [g,i), [i,k)
	require.Equal(t, cache.mu.sorted.b.Len(), 6) // insert [g,i) into the cache
}
