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
	shards map[string][]*ShardWithAddr
}

func newMockClient() *mockClient {
	return &mockClient{shards: make(map[string][]*ShardWithAddr)}
}

func keyspaceIDAndIndexID(keyspaceID uint32, indexID int64) string {
	return fmt.Sprintf("%d_%d", keyspaceID, indexID)
}
func (m *mockClient) init(keyspaceIDAndIndexID string) {
	m.shards[keyspaceIDAndIndexID] = []*ShardWithAddr{
		{
			Shard:           Shard{StartKey: []byte("a"), EndKey: []byte("c"), ShardID: 1},
			localCacheAddrs: []string{"addr1"},
		},
		{
			Shard:           Shard{StartKey: []byte("c"), EndKey: []byte("e"), ShardID: 2},
			localCacheAddrs: []string{"addr2"},
		},
		{
			Shard:           Shard{StartKey: []byte("e"), EndKey: []byte("g"), ShardID: 3},
			localCacheAddrs: []string{"addr3"},
		},
		{
			Shard:           Shard{StartKey: []byte("g"), EndKey: []byte("i"), ShardID: 4},
			localCacheAddrs: []string{"addr4"},
		},
		{
			Shard:           Shard{StartKey: []byte("i"), EndKey: []byte("k"), ShardID: 5},
			localCacheAddrs: []string{"addr5"},
		},
		{
			Shard:           Shard{StartKey: []byte("k"), EndKey: []byte("m"), ShardID: 6},
			localCacheAddrs: []string{"addr6"},
		},
	}
}

func (m *mockClient) ScanRanges(ctx context.Context, keyspaceID uint32, tableID int64, indexID int64, keyRanges []kv.KeyRange, limit int) (ret []*ShardWithAddr, err error) {
	id := keyspaceIDAndIndexID(keyspaceID, indexID)
	if m.shards[id] == nil {
		m.init(id)
	}
	need := make([]bool, len(m.shards[id]))

	for _, keyRange := range keyRanges {
		for i, shard := range m.shards[id] {
			if shard.Contains(keyRange.StartKey) || shard.ContainsByEnd(keyRange.EndKey) ||
				(bytes.Compare(keyRange.StartKey, shard.StartKey) == -1 &&
					bytes.Compare(shard.EndKey, keyRange.EndKey) == -1 && len(shard.StartKey) > 0 && len(shard.EndKey) > 0) {
				need[i] = true
			}
		}
	}

	for i, ok := range need {
		if ok {
			ret = append(ret, m.shards[id][i])
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
	keyspaceid := uint32(1)
	indexid := int64(1)
	tableid := int64(1)

	ranges := make([]kv.KeyRange, 0)
	ranges = append(ranges, kv.KeyRange{
		StartKey: []byte("b"),
		EndKey:   []byte("f"),
	})

	ranges = append(ranges, kv.KeyRange{
		StartKey: []byte("i"),
		EndKey:   []byte("l"),
	})

	locs, err := cache.BatchLocateKeyRanges(ctx, keyspaceid, tableid, indexid, ranges)
	require.NoError(t, err)
	require.Len(t, locs, 5)
	// [a,c), [c,e), [e,g), [i,k), [k,m)
	require.Equal(t, cache.mu.sorted[keyspaceIDAndIndexID(keyspaceid, indexid)].b.Len(), 5)

	ranges = ranges[:0]
	ranges = append(ranges, kv.KeyRange{
		StartKey: []byte("e"),
		EndKey:   []byte("k"),
	})
	locs, err = cache.BatchLocateKeyRanges(ctx, keyspaceid, tableid, indexid, ranges)
	require.NoError(t, err)
	require.Len(t, locs, 3)
	// [e,g), [g,i), [i,k)
	require.Equal(t, cache.mu.sorted[keyspaceIDAndIndexID(keyspaceid, indexid)].b.Len(), 6) // insert [g,i) into the cache

	indexid = int64(2)
	ranges = make([]kv.KeyRange, 0)
	ranges = append(ranges, kv.KeyRange{
		StartKey: []byte("b"),
		EndKey:   []byte("f"),
	})

	ranges = append(ranges, kv.KeyRange{
		StartKey: []byte("i"),
		EndKey:   []byte("l"),
	})

	locs, err = cache.BatchLocateKeyRanges(ctx, keyspaceid, tableid, indexid, ranges)
	require.NoError(t, err)
	require.Len(t, locs, 5)
	// test another indexID
	require.Equal(t, cache.mu.sorted[keyspaceIDAndIndexID(keyspaceid, 1)].b.Len(), 6)
	require.Equal(t, cache.mu.sorted[keyspaceIDAndIndexID(keyspaceid, 2)].b.Len(), 5)

	keyspaceid = uint32(2)
	tableid = 1
	indexid = 1
	ranges = make([]kv.KeyRange, 0)
	ranges = append(ranges, kv.KeyRange{
		StartKey: []byte("b"),
		EndKey:   []byte("f"),
	})

	locs, err = cache.BatchLocateKeyRanges(ctx, keyspaceid, tableid, indexid, ranges)
	require.NoError(t, err)
	require.Len(t, locs, 3)
	// test another keyspaceID
	require.Equal(t, cache.mu.sorted[keyspaceIDAndIndexID(2, 1)].b.Len(), 3)
	require.Equal(t, cache.mu.sorted[keyspaceIDAndIndexID(1, 1)].b.Len(), 6)
	require.Equal(t, cache.mu.sorted[keyspaceIDAndIndexID(1, 2)].b.Len(), 5)
}
