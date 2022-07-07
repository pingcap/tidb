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

package core

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/mathutil"
)

type collectPredicateColumnsPoint struct{}

func (c collectPredicateColumnsPoint) optimize(ctx context.Context, plan LogicalPlan, op *logicalOptimizeOp) (LogicalPlan, error) {
	if plan.SCtx().GetSessionVars().InRestrictedSQL {
		return plan, nil
	}
	predicateNeeded := variable.EnableColumnTracking.Load()
	syncWait := plan.SCtx().GetSessionVars().StatsLoadSyncWait * time.Millisecond.Nanoseconds()
	histNeeded := syncWait > 0
	predicateColumns, histNeededColumns := CollectColumnStatsUsage(plan, predicateNeeded, histNeeded)
	if len(predicateColumns) > 0 {
		plan.SCtx().UpdateColStatsUsage(predicateColumns)
	}
	histNeededIndices := collectSyncIndices(plan.SCtx(), histNeededColumns)
	histNeededItems := collectHistNeededItems(histNeededColumns, histNeededIndices)
	if histNeeded && len(histNeededItems) > 0 {
		err := RequestLoadStats(plan.SCtx(), histNeededItems, syncWait)
		return plan, err
	}
	return plan, nil
}

func (c collectPredicateColumnsPoint) name() string {
	return "collect_predicate_columns_point"
}

type syncWaitStatsLoadPoint struct{}

func (s syncWaitStatsLoadPoint) optimize(ctx context.Context, plan LogicalPlan, op *logicalOptimizeOp) (LogicalPlan, error) {
	if plan.SCtx().GetSessionVars().InRestrictedSQL {
		return plan, nil
	}
	_, err := SyncWaitStatsLoad(plan)
	return plan, err
}

func (s syncWaitStatsLoadPoint) name() string {
	return "sync_wait_stats_load_point"
}

const maxDuration = 1<<63 - 1

// RequestLoadStats send load column/index stats requests to stats handle
func RequestLoadStats(ctx sessionctx.Context, neededHistItems []model.TableItemID, syncWait int64) error {
	stmtCtx := ctx.GetSessionVars().StmtCtx
	hintMaxExecutionTime := int64(stmtCtx.MaxExecutionTime)
	if hintMaxExecutionTime <= 0 {
		hintMaxExecutionTime = maxDuration
	}
	sessMaxExecutionTime := int64(ctx.GetSessionVars().MaxExecutionTime)
	if sessMaxExecutionTime <= 0 {
		sessMaxExecutionTime = maxDuration
	}
	waitTime := mathutil.Min(syncWait, hintMaxExecutionTime, sessMaxExecutionTime)
	var timeout = time.Duration(waitTime)
	err := domain.GetDomain(ctx).StatsHandle().SendLoadRequests(stmtCtx, neededHistItems, timeout)
	if err != nil {
		return handleTimeout(stmtCtx)
	}
	return nil
}

// SyncWaitStatsLoad sync-wait for stats load until timeout
func SyncWaitStatsLoad(plan LogicalPlan) (bool, error) {
	stmtCtx := plan.SCtx().GetSessionVars().StmtCtx
	if stmtCtx.StatsLoad.Fallback {
		return false, nil
	}
	success := domain.GetDomain(plan.SCtx()).StatsHandle().SyncWaitStatsLoad(stmtCtx)
	if success {
		return true, nil
	}
	err := handleTimeout(stmtCtx)
	return false, err
}

func handleTimeout(stmtCtx *stmtctx.StatementContext) error {
	err := errors.New("Timeout when sync-load full stats for needed columns")
	if variable.StatsLoadPseudoTimeout.Load() {
		stmtCtx.AppendWarning(err)
		stmtCtx.StatsLoad.Fallback = true
		return nil
	}
	return err
}

// collectSyncIndices will collect the indices which includes following conditions:
// 1. the indices contained the any one of histNeededColumns, eg: histNeededColumns contained A,B columns, and idx_a is
// composed up by A column, then we thought the idx_a should be collected
// 2. The stats condition of idx_a can't meet IsFullLoad, which means its stats was evicted previously
func collectSyncIndices(ctx sessionctx.Context, histNeededColumns []model.TableItemID) map[model.TableItemID]struct{} {
	histNeededIndices := make(map[model.TableItemID]struct{})
	stats := domain.GetDomain(ctx).StatsHandle()
	for _, column := range histNeededColumns {
		if column.IsIndex {
			continue
		}
		tbl, ok := ctx.GetDomainInfoSchema().(infoschema.InfoSchema).TableByID(column.TableID)
		if !ok {
			continue
		}
		colName := ""
		for _, col := range tbl.Cols() {
			if col.ID == column.ID {
				colName = col.Name.String()
				break
			}
		}
		if colName == "" {
			continue
		}
		for _, idx := range tbl.Indices() {
			if idx.Meta().State != model.StatePublic {
				continue
			}
			hasCol := false
			for _, idxCol := range idx.Meta().Columns {
				if idxCol.Name.String() == colName {
					hasCol = true
					break
				}
			}
			if hasCol {
				tblStats := stats.GetTableStats(tbl.Meta())
				if tblStats == nil || tblStats.Pseudo {
					continue
				}
				idxStats, ok := tblStats.Indices[idx.Meta().ID]
				if !ok || !idxStats.IsFullLoad() {
					histNeededIndices[model.TableItemID{TableID: column.TableID, ID: idxStats.ID, IsIndex: true}] = struct{}{}
				}
			}
		}
	}
	return histNeededIndices
}

func collectHistNeededItems(histNeededColumns []model.TableItemID, histNeededIndices map[model.TableItemID]struct{}) (histNeededItems []model.TableItemID) {
	for idx := range histNeededIndices {
		histNeededItems = append(histNeededItems, idx)
	}
	histNeededItems = append(histNeededItems, histNeededColumns...)
	return
}
