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
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
)

// buildTiCIShardInfosByAddrFromLocations pre-counts single-candidate shards as
// fixed load, then assigns shards in loc order. Multi-candidate shards choose the
// candidate store with the lowest current load; ties keep TiCI's candidate order.
func buildTiCIShardInfosByAddrFromLocations(locs []*ShardLocation) (map[string][]*coprocessor.ShardInfo, error) {
	addrLoad := make(map[string]int)
	for _, loc := range locs {
		if len(loc.localCacheAddrs) == 0 {
			return nil, errors.Errorf("tici shard %d has no local cache address", loc.ShardID)
		}
		if loc.Ranges == nil {
			return nil, errors.Errorf("tici shard %d has nil ranges", loc.ShardID)
		}
		if len(loc.localCacheAddrs) == 1 {
			addrLoad[loc.localCacheAddrs[0]]++
		}
	}

	storeShard := make(map[string][]*coprocessor.ShardInfo)
	for _, loc := range locs {
		addr := selectTiCIShardAddr(loc.localCacheAddrs, addrLoad)
		storeShard[addr] = append(storeShard[addr], &coprocessor.ShardInfo{
			ShardId:    loc.ShardID,
			ShardEpoch: loc.Epoch,
			Ranges:     loc.Ranges.ToPBRanges(),
		})
	}
	return storeShard, nil
}

func selectTiCIShardAddr(addrs []string, addrLoad map[string]int) string {
	if len(addrs) == 1 {
		return addrs[0]
	}
	selected := addrs[0]
	selectedLoad := addrLoad[selected]
	for _, addr := range addrs[1:] {
		load := addrLoad[addr]
		if load < selectedLoad {
			selected = addr
			selectedLoad = load
		}
	}
	addrLoad[selected]++
	return selected
}
