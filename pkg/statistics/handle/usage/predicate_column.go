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

package usage

import (
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/indexusage"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/predicatecolumn"
	utilstats "github.com/pingcap/tidb/pkg/statistics/handle/util"
)

// statsUsageImpl implements statstypes.StatsUsage.
type statsUsageImpl struct {
	statsHandle statstypes.StatsHandle

	// idxUsageCollector contains all the index usage collectors required by session.
	idxUsageCollector *indexusage.Collector

	// SessionStatsList contains all the stats collector required by session.
	*SessionStatsList
}

// NewStatsUsageImpl creates a statstypes.StatsUsage.
func NewStatsUsageImpl(statsHandle statstypes.StatsHandle) statstypes.StatsUsage {
	return &statsUsageImpl{
		statsHandle:       statsHandle,
		idxUsageCollector: indexusage.NewCollector(),
		SessionStatsList:  NewSessionStatsList()}
}

// LoadColumnStatsUsage returns all columns' usage information.
func (u *statsUsageImpl) LoadColumnStatsUsage(loc *time.Location) (colStatsMap map[model.TableItemID]statstypes.ColStatsTimeInfo, err error) {
	err = utilstats.CallWithSCtx(u.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		colStatsMap, err = predicatecolumn.LoadColumnStatsUsage(sctx, loc)
		return err
	})
	return
}

// GetPredicateColumns returns IDs of predicate columns, which are the columns whose stats are used(needed) when generating query plans.
func (u *statsUsageImpl) GetPredicateColumns(tableID int64) (columnIDs []int64, err error) {
	err = utilstats.CallWithSCtx(u.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		columnIDs, err = predicatecolumn.GetPredicateColumns(sctx, tableID)
		return err
	}, utilstats.FlagWrapTxn)
	return
}

// CollectColumnsInExtendedStats returns IDs of the columns involved in extended stats.
func (u *statsUsageImpl) CollectColumnsInExtendedStats(tableID int64) (columnIDs []int64, err error) {
	err = utilstats.CallWithSCtx(u.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		columnIDs, err = predicatecolumn.CollectColumnsInExtendedStats(sctx, tableID)
		return err
	})
	return
}
