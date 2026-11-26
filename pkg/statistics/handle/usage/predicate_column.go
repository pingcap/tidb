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

// predicateColumnsGetter implements statstypes.PredicateColumnsGetter in the sessions of a pool.
type predicateColumnsGetter struct {
	pool utilstats.SessionPool
}

// newPredicateColumnsGetter creates a statstypes.PredicateColumnsGetter that reads in the
// sessions of pool. It needs no stats cache.
func newPredicateColumnsGetter(pool utilstats.SessionPool) *predicateColumnsGetter {
	return &predicateColumnsGetter{pool: pool}
}

// Tracker tracks the table deltas and the usage of column and index stats, and dumps them to KV.
type Tracker struct {
	*predicateColumnsGetter

	pool       utilstats.SessionPool
	lease      utilstats.LeaseGetter
	locks      statstypes.LockedTablesGetter
	tableStats statstypes.NonPseudoPhysicalTableStatsGetter

	// idxUsageCollector contains all the index usage collectors required by session.
	idxUsageCollector *indexusage.Collector

	// sessionStats contains all the stats collector required by session.
	sessionStats *SessionStatsList
}

// NewTracker creates a Tracker that dumps the stats usage in the sessions of pool. It skips the
// tables that locks gives, and reads the table stats from tableStats.
func NewTracker(
	pool utilstats.SessionPool,
	lease utilstats.LeaseGetter,
	locks statstypes.LockedTablesGetter,
	tableStats statstypes.NonPseudoPhysicalTableStatsGetter,
) *Tracker {
	return &Tracker{
		predicateColumnsGetter: newPredicateColumnsGetter(pool),
		pool:                   pool,
		lease:                  lease,
		locks:                  locks,
		tableStats:             tableStats,
		idxUsageCollector:      indexusage.NewCollector(),
		sessionStats:           NewSessionStatsList()}
}

// LoadColumnStatsUsage returns all columns' usage information.
func (u *Tracker) LoadColumnStatsUsage(loc *time.Location) (colStatsMap map[model.TableItemID]statstypes.ColStatsTimeInfo, err error) {
	err = utilstats.CallWithSCtx(u.pool.SPool(), func(sctx sessionctx.Context) error {
		colStatsMap, err = predicatecolumn.LoadColumnStatsUsage(sctx, loc)
		return err
	})
	return
}

// GetPredicateColumns returns IDs of predicate columns, which are the columns whose stats are used(needed) when generating query plans.
func (g *predicateColumnsGetter) GetPredicateColumns(tableID int64) (columnIDs []int64, err error) {
	err = utilstats.CallWithSCtx(g.pool.SPool(), func(sctx sessionctx.Context) error {
		columnIDs, err = predicatecolumn.GetPredicateColumns(sctx, tableID)
		return err
	}, utilstats.FlagWrapTxn)
	return
}
