// Copyright 2023 PingCAP, Inc.
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

package globalstats

import (
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle/storage"
)

type AsyncGlobalStatsCache struct {
	cache         map[int64]*statistics.Table
	skipPartition map[int64]struct{}
}

func NewAsyncGlobalStatsCache(cache map[int64]*statistics.Table) *AsyncGlobalStatsCache {
	return &AsyncGlobalStatsCache{
		cache:         cache,
		skipPartition: make(map[int64]struct{}),
	}
}

func (c *AsyncGlobalStatsCache) GetTableStats(tableID int64) *statistics.Table {
	return c.cache[tableID]
}

func (c *AsyncGlobalStatsCache) SkipPartiton(sctx sessionctx.Context, partitionID int64, histID int64, isIndex bool) (bool, error) {
	if c.cache != nil {
		partitionStats, ok := c.cache[partitionID]
		if ok {
			return true, nil
		}
		isSkip, analyzed := partitionStats.IsSkipPartition(histID, isIndex)
		if !analyzed {
			return true, nil
		}
		if partitionStats.RealtimeCount > 0 && isSkip {
			return true, nil
		}
		return false, nil
	}
	var index = int64(0)
	if isIndex {
		index = 0
	}
	skip, err := storage.CheckSkipPartition(sctx, partitionID, histID, index)
	if err != nil {
		return true, err
	}
	return skip, nil
}
