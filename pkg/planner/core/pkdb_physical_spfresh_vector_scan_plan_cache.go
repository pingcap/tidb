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

package core

import (
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// CloneForPlanCache implements the base.Plan interface.
func (p *PhysicalSPFreshVectorScan) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalSPFreshVectorScan)
	*cloned = *p
	producer, ok := p.physicalSchemaProducer.cloneForPlanCacheWithSelf(newCtx, cloned)
	if !ok {
		return nil, false
	}
	cloned.physicalSchemaProducer = *producer
	cloned.QueryVec = p.QueryVec.Clone()
	if p.QueryVecParam != nil {
		cloned.QueryVecParam = p.QueryVecParam.Clone().(*expression.Constant)
	}
	cloned.Filter = p.Filter.Clone()
	return cloned, true
}

// isVectorDistanceOrderByForPlanCache admits the literal vector query shape
// into non-prepared parameterization without widening arbitrary ORDER BY expressions.
func isVectorDistanceOrderByForPlanCache(expr ast.ExprNode) bool {
	fn, ok := expr.(*ast.FuncCallExpr)
	if !ok || len(fn.Args) != 2 {
		return false
	}
	switch fn.FnName.L {
	case ast.VecL1Distance, ast.VecL2Distance, ast.VecCosineDistance, ast.VecNegativeInnerProduct:
	default:
		return false
	}
	for i, arg := range fn.Args {
		if _, ok := arg.(*ast.ColumnNameExpr); !ok {
			continue
		}
		value, ok := fn.Args[1-i].(*driver.ValueExpr)
		return ok && value.Kind() == types.KindString
	}
	return false
}

func (p *PhysicalSPFreshVectorScan) rebuildForPlanCache() error {
	if p.QueryVecParam != nil {
		vec, isNull, err := p.QueryVecParam.EvalVectorFloat32(p.SCtx().GetExprCtx().GetEvalCtx(), chunk.Row{})
		if err != nil {
			return &spfreshVectorParameterError{err}
		}
		if isNull {
			return errors.New("SPFresh query vector is NULL")
		}
		if err := vec.CheckDimsFitColumn(int(p.Index.VectorInfo.Dimension)); err != nil {
			return &spfreshVectorParameterError{err}
		}
		p.QueryVec = vec
	}
	appendSPFreshVectorIndexReadWarnings(p.SCtx())
	return nil
}

type spfreshVectorParameterError struct{ error }

func (e *spfreshVectorParameterError) Unwrap() error { return e.error }

// spfreshPlanCacheWarnings removes warnings that are regenerated from the
// current execution context during plan rebuild, rather than replayed.
// warnings must be an owned copy of the statement warnings.
func spfreshPlanCacheWarnings(warnings []stmtctx.SQLWarn) []stmtctx.SQLWarn {
	if !config.SPFreshEnabled() {
		return warnings
	}
	return slices.DeleteFunc(warnings, func(warn stmtctx.SQLWarn) bool {
		switch warn.Err.Error() {
		case spfreshVectorIndexStaleReadWarning, spfreshVectorIndexExplicitTxnWarning:
			return true
		default:
			return false
		}
	})
}
