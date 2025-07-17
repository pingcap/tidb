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
	"time"

	"github.com/google/btree"
)

// SortedShards is a sorted shards by start key.
type SortedShards struct {
	b *btree.BTreeG[*btreeItem]
}

type btreeItem struct {
	key         []byte
	cachedShard *ShardWithAddr
}

func newBtreeItem(cachedShard *ShardWithAddr) *btreeItem {
	return &btreeItem{
		key:         cachedShard.StartKey,
		cachedShard: cachedShard,
	}
}

func newBtreeSearchItem(key []byte) *btreeItem {
	return &btreeItem{key: key}
}

func (item *btreeItem) Less(other *btreeItem) bool {
	return bytes.Compare(item.key, other.key) < 0
}

// NewSortedShards creates a new SortedShards with the specified B-tree degree.
func NewSortedShards(btreeDegree int) *SortedShards {
	return &SortedShards{
		b: btree.NewG(btreeDegree, func(a, b *btreeItem) bool { return a.Less(b) }),
	}
}

// ReplaceOrInsert replaces an existing shard with the same start key or inserts a new one.
func (s *SortedShards) ReplaceOrInsert(cachedShard *ShardWithAddr) *ShardWithAddr {
	old, _ := s.b.ReplaceOrInsert(newBtreeItem(cachedShard))
	if old != nil {
		return old.cachedShard
	}
	return nil
}

// SearchByKey searches for a shard that contains the specified key.
func (s *SortedShards) SearchByKey(key []byte, isEndKey bool) (r *ShardWithAddr) {
	s.b.DescendLessOrEqual(newBtreeSearchItem(key), func(item *btreeItem) bool {
		shard := item.cachedShard
		if isEndKey && bytes.Equal(shard.StartKey, key) {
			return true // iterate next item
		}
		if !isEndKey && shard.Contains(key) || isEndKey && shard.ContainsByEnd(key) {
			r = shard
		}
		return false
	})
	return
}

// AscendGreaterOrEqual iterates over shards that start at or after the specified start key,
func (s *SortedShards) AscendGreaterOrEqual(startKey, endKey []byte, limit int) (shards []*ShardWithAddr) {
	now := time.Now().Unix()
	lastStartKey := startKey
	s.b.AscendGreaterOrEqual(newBtreeSearchItem(startKey), func(item *btreeItem) bool {
		shard := item.cachedShard
		if len(endKey) > 0 && bytes.Compare(shard.StartKey, endKey) >= 0 {
			return false
		}
		if !shard.CheckShardCacheTTL(now) {
			return false
		}
		if !shard.Contains(lastStartKey) { // uncached hole
			return false
		}
		lastStartKey = shard.EndKey
		shards = append(shards, shard)
		return len(shards) < limit
	})
	return shards
}

func (s *SortedShards) removeIntersecting(r *ShardWithAddr) ([]*btreeItem, bool) {
	var deleted []*btreeItem
	s.b.AscendGreaterOrEqual(newBtreeSearchItem(r.StartKey), func(item *btreeItem) bool {
		if len(r.EndKey) > 0 && bytes.Compare(item.cachedShard.StartKey, r.EndKey) >= 0 {
			return false
		}
		deleted = append(deleted, item)
		return true
	})
	for _, item := range deleted {
		s.b.Delete(item)
	}
	return deleted, false
}

// Clear removes all shards from the sorted shards.
func (s *SortedShards) Clear() {
	s.b.Clear(false)
}
