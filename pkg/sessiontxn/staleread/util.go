// Copyright 2022 PingCAP, Inc.
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

package staleread

import (
	"context"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	pctx "github.com/pingcap/tidb/pkg/planner/context"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/tikv/client-go/v2/oracle"
)

// CalculateAsOfTsExpr calculates the TsExpr of AsOfClause to get a StartTS.
func CalculateAsOfTsExpr(ctx context.Context, sctx pctx.PlanContext, tsExpr ast.ExprNode) (uint64, error) {
	sctx.GetSessionVars().StmtCtx.SetStaleTSOProvider(func() (uint64, error) {
		failpoint.Inject("mockStaleReadTSO", func(val failpoint.Value) (uint64, error) {
			return uint64(val.(int)), nil
		})
		// this function accepts a context, but we don't need it when there is a valid cached ts.
		// in most cases, the stale read ts can be calculated from `cached ts + time since cache - staleness`,
		// this can be more accurate than `time.Now() - staleness`, because TiDB's local time can drift.
		return sctx.GetStore().GetOracle().GetStaleTimestamp(ctx, oracle.GlobalTxnScope, 0)
	})
	tsVal, err := plannerutil.EvalAstExprWithPlanCtx(sctx, tsExpr)
	if err != nil {
		return 0, err
	}

	if tsVal.IsNull() {
		return 0, plannererrors.ErrAsOf.FastGenWithCause("as of timestamp cannot be NULL")
	}

	toTypeTimestamp := types.NewFieldType(mysql.TypeTimestamp)
	// We need at least the millionsecond here, so set fsp to 3.
	toTypeTimestamp.SetDecimal(3)
	tsTimestamp, err := tsVal.ConvertTo(sctx.GetSessionVars().StmtCtx.TypeCtx(), toTypeTimestamp)
	if err != nil {
		return 0, err
	}
	tsTime, err := tsTimestamp.GetMysqlTime().GoTime(sctx.GetSessionVars().Location())
	if err != nil {
		return 0, err
	}
	return oracle.GoTimeToTS(tsTime), nil
}

// CalculateTsWithReadStaleness calculates the TsExpr for readStaleness duration
func CalculateTsWithReadStaleness(sctx sessionctx.Context, readStaleness time.Duration) (uint64, error) {
	nowVal, err := expression.GetStmtTimestamp(sctx.GetExprCtx().GetEvalCtx())
	if err != nil {
		return 0, err
	}
	tsVal := nowVal.Add(readStaleness)
	sc := sctx.GetSessionVars().StmtCtx
	minTsVal := expression.GetStmtMinSafeTime(sc, sctx.GetStore(), sc.TimeZone())
	return oracle.GoTimeToTS(expression.CalAppropriateTime(tsVal, nowVal, minTsVal)), nil
}

// IsStmtStaleness indicates whether the current statement is staleness or not
func IsStmtStaleness(sctx sessionctx.Context) bool {
	return sctx.GetSessionVars().StmtCtx.IsStaleness
}

// GetExternalTimestamp returns the external timestamp in cache, or get and store it in cache
func GetExternalTimestamp(ctx context.Context, sc *stmtctx.StatementContext) (uint64, error) {
	// Try to get from the stmt cache to make sure this function is deterministic.
	externalTimestamp, err := sc.GetOrEvaluateStmtCache(stmtctx.StmtExternalTSCacheKey, func() (any, error) {
		return variable.GetExternalTimestamp(ctx)
	})

	if err != nil {
		return 0, plannererrors.ErrAsOf.FastGenWithCause(err.Error())
	}
	return externalTimestamp.(uint64), nil
}
