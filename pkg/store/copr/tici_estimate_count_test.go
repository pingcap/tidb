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
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestTiCIEstimateCountHelpers(t *testing.T) {
	t.Run("build shard groups", func(t *testing.T) {
		locations := []*ShardLocation{
			{
				ShardWithAddr: &ShardWithAddr{
					Shard:           Shard{ShardID: 1, Epoch: 11},
					localCacheAddrs: []string{"addr-b"},
				},
				Ranges: NewKeyRanges([]kv.KeyRange{{StartKey: []byte("a"), EndKey: []byte("b")}}),
			},
			{
				ShardWithAddr: &ShardWithAddr{
					Shard:           Shard{ShardID: 2, Epoch: 12},
					localCacheAddrs: []string{"addr-a"},
				},
				Ranges: NewKeyRanges([]kv.KeyRange{{StartKey: []byte("b"), EndKey: []byte("c")}}),
			},
			{
				ShardWithAddr: &ShardWithAddr{
					Shard:           Shard{ShardID: 2, Epoch: 12},
					localCacheAddrs: []string{"addr-a"},
				},
				Ranges: NewKeyRanges([]kv.KeyRange{{StartKey: []byte("c"), EndKey: []byte("d")}}),
			},
			{
				ShardWithAddr: &ShardWithAddr{
					Shard:           Shard{ShardID: 3, Epoch: 13},
					localCacheAddrs: []string{"addr-a"},
				},
				Ranges: NewKeyRanges([]kv.KeyRange{{StartKey: []byte("d"), EndKey: []byte("e")}}),
			},
		}

		groups, totalUnique := buildTiCIEstimateShardGroups(locations)
		require.Equal(t, uint64(3), totalUnique)
		require.Len(t, groups, 2)
		require.Len(t, groups["addr-a"].shardInfos, 3)
		require.Len(t, groups["addr-a"].uniqueShardID, 2)
		require.Len(t, groups["addr-b"].uniqueShardID, 1)
		require.Equal(t, uint64(2), groups["addr-a"].shardInfos[0].GetShardId())
	})

	t.Run("choose shard group", func(t *testing.T) {
		groups := map[string]*ticiEstimateShardGroup{
			"addr-b": {addr: "addr-b", uniqueShardID: map[uint64]struct{}{1: {}, 2: {}}},
			"addr-a": {addr: "addr-a", uniqueShardID: map[uint64]struct{}{3: {}, 4: {}}},
			"addr-c": {addr: "addr-c", uniqueShardID: map[uint64]struct{}{5: {}}},
		}

		chosen := chooseTiCIEstimateShardGroup(groups)
		require.NotNil(t, chosen)
		require.Equal(t, "addr-a", chosen.addr)
	})

	t.Run("build pb request", func(t *testing.T) {
		req, err := buildTiCIEstimatePBRequest(&kv.TiCIEstimateCountRequest{
			TableID:        42,
			IndexID:        7,
			FTSQueryInfo:   &tipb.FTSQueryInfo{QueryType: tipb.FTSQueryType_FTSQueryTypeWithScore},
			TimeZoneName:   "Asia/Shanghai",
			TimeZoneOffset: 28800,
		}, nil)
		require.NoError(t, err)
		require.Equal(t, int64(42), req.GetTableId())
		require.Equal(t, int64(7), req.GetIndexId())
		require.Equal(t, "Asia/Shanghai", req.GetTimeZoneName())
	})
}
