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
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
)

type GlobalStatsInfo struct {
	IsIndex int
	// When the `isIndex == 0`, HistIDs will be the column IDs.
	// Otherwise, HistIDs will only contain the index ID.
	HistIDs      []int64
	StatsVersion int
}

func WriteGlobalStatsToStorage(statsHandle statstypes.StatsHandle, globalStats *GlobalStats, info *GlobalStatsInfo, gid int64) error {
	// Dump global-level stats to kv.
	for i := 0; i < globalStats.Num; i++ {
		hg, cms, topN := globalStats.Hg[i], globalStats.Cms[i], globalStats.TopN[i]
		if hg == nil {
			// All partitions have no stats so global stats are not created.
			continue
		}
		// fms for global stats doesn't need to dump to kv.
		err := statsHandle.SaveStatsToStorage(gid,
			globalStats.Count,
			globalStats.ModifyCount,
			info.IsIndex,
			hg,
			cms,
			topN,
			info.StatsVersion,
			1,
			true,
			util.StatsMetaHistorySourceAnalyze,
		)
		if err != nil {
			return err
		}
	}
	return nil
}
