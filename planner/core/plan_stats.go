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
	if histNeeded && len(histNeededColumns) > 0 {
		err := RequestLoadColumnStats(plan.SCtx(), histNeededColumns, syncWait)
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

// RequestLoadColumnStats send requests to stats handle
func RequestLoadColumnStats(ctx sessionctx.Context, neededColumns []model.TableColumnID, syncWait int64) error {
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
	err := domain.GetDomain(ctx).StatsHandle().SendLoadRequests(stmtCtx, neededColumns, timeout)
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
