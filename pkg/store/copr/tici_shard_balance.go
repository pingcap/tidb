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

func buildTiCIShardInfosByAddrFromLocations(locs []*ShardLocation) (map[string][]*coprocessor.ShardInfo, error) {
	storeShard := make(map[string][]*coprocessor.ShardInfo)
	addrLoad := make(map[string]int)
	for _, loc := range locs {
		if len(loc.localCacheAddrs) == 0 {
			return nil, errors.Errorf("tici shard %d has no local cache address", loc.ShardID)
		}
		if loc.Ranges == nil {
			return nil, errors.Errorf("tici shard %d has nil ranges", loc.ShardID)
		}
		addr := selectTiCIShardAddr(loc.localCacheAddrs, addrLoad)
		if addr == "" {
			continue
		}
		storeShard[addr] = append(storeShard[addr], &coprocessor.ShardInfo{
			ShardId:    loc.ShardID,
			ShardEpoch: loc.Epoch,
			Ranges:     loc.Ranges.ToPBRanges(),
		})
	}
	return storeShard, nil
}

func selectTiCIShardAddr(addrs []string, addrLoad map[string]int) string {
	var selected string
	selectedLoad := 0
	for _, addr := range addrs {
		load := addrLoad[addr]
		if selected == "" || load < selectedLoad {
			selected = addr
			selectedLoad = load
		}
	}
	if selected != "" {
		addrLoad[selected]++
	}
	return selected
}
