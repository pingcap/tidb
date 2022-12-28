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

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/tikv/client-go/v2/oracle"
)

// CalculateAsOfTsExpr calculates the TsExpr of AsOfClause to get a StartTS.
func CalculateAsOfTsExpr(sctx sessionctx.Context, tsExpr ast.ExprNode) (uint64, error) {
	tsVal, err := expression.EvalAstExpr(sctx, tsExpr)
	if err != nil {
		return 0, err
	}

	if tsVal.IsNull() {
		return 0, errAsOf.FastGenWithCause("as of timestamp cannot be NULL")
	}

	toTypeTimestamp := types.NewFieldType(mysql.TypeTimestamp)
	// We need at least the millionsecond here, so set fsp to 3.
	toTypeTimestamp.SetDecimal(3)
	tsTimestamp, err := tsVal.ConvertTo(sctx.GetSessionVars().StmtCtx, toTypeTimestamp)
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
	nowVal, err := expression.GetStmtTimestamp(sctx)
	if err != nil {
		return 0, err
	}
	tsVal := nowVal.Add(readStaleness)
	minTsVal := expression.GetMinSafeTime(sctx)
	return oracle.GoTimeToTS(expression.CalAppropriateTime(tsVal, nowVal, minTsVal)), nil
}

// IsStmtStaleness indicates whether the current statement is staleness or not
func IsStmtStaleness(sctx sessionctx.Context) bool {
	return sctx.GetSessionVars().StmtCtx.IsStaleness
}

// GetExternalTimestamp returns the external timestamp in cache, or get and store it in cache
func GetExternalTimestamp(ctx context.Context, sctx sessionctx.Context) (uint64, error) {
	// Try to get from the stmt cache to make sure this function is deterministic.
	stmtCtx := sctx.GetSessionVars().StmtCtx
	externalTimestamp, err := stmtCtx.GetOrEvaluateStmtCache(stmtctx.StmtExternalTSCacheKey, func() (interface{}, error) {
		return variable.GetExternalTimestamp(ctx)
	})

	if err != nil {
		return 0, errAsOf.FastGenWithCause(err.Error())
	}
	return externalTimestamp.(uint64), nil
}
