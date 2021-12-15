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

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
)

type collectPredicateColumnsPoint struct{}

func (c collectPredicateColumnsPoint) optimize(ctx context.Context, plan LogicalPlan, op *logicalOptimizeOp) (LogicalPlan, error) {
	RequestLoadColumnStats(plan)
	return plan, nil
}

func (c collectPredicateColumnsPoint) name() string {
	return "collect_predicate_columns_point"
}

type syncWaitStatsLoadPoint struct{}

func (s syncWaitStatsLoadPoint) optimize(ctx context.Context, plan LogicalPlan, op *logicalOptimizeOp) (LogicalPlan, error) {
	SyncWaitStatsLoad(plan)
	return plan, nil
}

func (s syncWaitStatsLoadPoint) name() string {
	return "sync_wait_stats_load_point"
}

// RequestLoadColumnStats send requests to stats handle
func RequestLoadColumnStats(plan LogicalPlan) {
	if plan.SCtx().GetSessionVars().InRestrictedSQL {
		return
	}
	syncWait := int64(config.GetGlobalConfig().Stats.SyncLoadWait)
	if syncWait <= 0 {
		return
	}
	stmtCtx := plan.SCtx().GetSessionVars().StmtCtx
	hintMaxExecutionTime := int64(stmtCtx.MaxExecutionTime)
	if hintMaxExecutionTime == 0 {
		hintMaxExecutionTime = mathutil.MaxInt
	}
	sessMaxExecutionTime := int64(plan.SCtx().GetSessionVars().MaxExecutionTime)
	if sessMaxExecutionTime == 0 {
		sessMaxExecutionTime = mathutil.MaxInt
	}
	waitTime := mathutil.MinInt64(syncWait, mathutil.MinInt64(hintMaxExecutionTime, sessMaxExecutionTime))
	var timeout = time.Duration(waitTime) * time.Millisecond
	neededColumns := CollectHistColumns(plan)
	domain.GetDomain(plan.SCtx()).StatsHandle().SendLoadRequests(stmtCtx, neededColumns, timeout)
}

// SyncWaitStatsLoad sync-wait for stats load until timeout
func SyncWaitStatsLoad(plan LogicalPlan) (bool, error) {
	stmtCtx := plan.SCtx().GetSessionVars().StmtCtx
	success := domain.GetDomain(plan.SCtx()).StatsHandle().SyncWaitStatsLoad(stmtCtx)
	if !success && config.GetGlobalConfig().Stats.PseudoForLoadTimeout {
		err := errors.New("Timeout when sync-load full stats for needed columns.")
		stmtCtx.AppendWarning(err)
		stmtCtx.StatsLoad.Fallback = true
		return false, err
	} else {
		return true, nil
	}
}
