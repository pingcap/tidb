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

package mview

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/tikv/client-go/v2/oracle"
)

// SplitMLogCommitTSFilterSelectivity extracts mlog commit-ts filters and estimates their selectivity.
func SplitMLogCommitTSFilterSelectivity(
	sctx planctx.PlanContext,
	mlogTableInfo *model.TableInfo,
	schema *expression.Schema,
	conds expression.CNFExprs,
) (expression.CNFExprs, float64, bool) {
	if sctx == nil || mlogTableInfo == nil || mlogTableInfo.MaterializedViewLog == nil || schema == nil {
		return conds, 0, false
	}
	estimationCtx, ok := sctx.(planctx.MLogCommitTSEstimationContext)
	if !ok {
		return conds, 0, false
	}
	estimation := estimationCtx.GetMLogCommitTSEstimation()
	// The planner may see other mlog tables; the retained window only belongs to the active refresh mlog.
	if estimation == nil || mlogTableInfo.ID != estimation.MLogTableID {
		return conds, 0, false
	}
	commitTSCol := getMLogCommitTSFilterColumn(schema)
	if commitTSCol == nil {
		return conds, 0, false
	}
	commitTSFilter := mlogCommitTSFilterWindow{}
	remainingConds := make(expression.CNFExprs, 0, len(conds))
	evalCtx := sctx.GetExprCtx().GetEvalCtx()
	for _, cond := range conds {
		if commitTSFilter.addCond(evalCtx, commitTSCol, cond) {
			continue
		}
		remainingConds = append(remainingConds, cond)
	}
	if !commitTSFilter.hasBound() {
		return conds, 0, false
	}
	selectivity, ok := estimateMLogCommitTSSelectivity(estimation, mlogTableInfo, commitTSFilter)
	if !ok {
		return conds, 0, false
	}
	return remainingConds, selectivity, true
}

func getMLogCommitTSFilterColumn(schema *expression.Schema) *expression.Column {
	for _, col := range schema.Columns {
		if col.ID == model.ExtraCommitTSID {
			return col
		}
	}
	return nil
}

type mlogCommitTSFilterWindow struct {
	lowerTSO uint64
	hasLower bool
	upperTSO uint64
	hasUpper bool
}

func (w *mlogCommitTSFilterWindow) hasBound() bool {
	return w.hasLower || w.hasUpper
}

func (w *mlogCommitTSFilterWindow) addCond(
	evalCtx expression.EvalContext,
	commitTSCol *expression.Column,
	expr expression.Expression,
) bool {
	sf, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return false
	}
	switch sf.FuncName.L {
	case ast.GT, ast.GE, ast.LT, ast.LE:
	default:
		return false
	}
	args := sf.GetArgs()
	if len(args) != 2 {
		return false
	}
	op, value, ok := extractMLogCommitTSFilterBound(evalCtx, commitTSCol, sf.FuncName.L, args)
	if !ok {
		return false
	}
	switch op {
	case ast.GT, ast.GE:
		if !w.hasLower || value > w.lowerTSO {
			w.lowerTSO = value
			w.hasLower = true
		}
	case ast.LT, ast.LE:
		if !w.hasUpper || value < w.upperTSO {
			w.upperTSO = value
			w.hasUpper = true
		}
	}
	return true
}

func extractMLogCommitTSFilterBound(
	evalCtx expression.EvalContext,
	commitTSCol *expression.Column,
	op string,
	args []expression.Expression,
) (string, uint64, bool) {
	if col, ok := args[0].(*expression.Column); ok && commitTSCol.EqualColumn(col) {
		value, isNull, ok := expression.GetUint64FromConstant(evalCtx, args[1])
		if !ok || isNull {
			return "", 0, false
		}
		return op, value, true
	}
	if col, ok := args[1].(*expression.Column); ok && commitTSCol.EqualColumn(col) {
		value, isNull, ok := expression.GetUint64FromConstant(evalCtx, args[0])
		if !ok || isNull {
			return "", 0, false
		}
		switch op {
		case ast.GT:
			return ast.LT, value, true
		case ast.GE:
			return ast.LE, value, true
		case ast.LT:
			return ast.GT, value, true
		case ast.LE:
			return ast.GE, value, true
		}
	}
	return "", 0, false
}

func estimateMLogCommitTSSelectivity(
	estimation *planctx.MLogCommitTSEstimation,
	mlogTableInfo *model.TableInfo,
	filter mlogCommitTSFilterWindow,
) (float64, bool) {
	if estimation.RetainedUpperTSO == 0 {
		return 0, false
	}
	retainedLowerTSO := estimation.RetainedLowerTSO
	if retainedLowerTSO == 0 && mlogTableInfo != nil {
		retainedLowerTSO = mlogTableInfo.UpdateTS
	}
	if retainedLowerTSO == 0 {
		return 0, false
	}

	// Missing filter bounds are unbounded on that side, so use the retained mlog window boundary.
	filterLowerTSO := retainedLowerTSO
	if filter.hasLower && filter.lowerTSO > filterLowerTSO {
		filterLowerTSO = filter.lowerTSO
	}
	filterUpperTSO := estimation.RetainedUpperTSO
	if filter.hasUpper && filter.upperTSO < filterUpperTSO {
		filterUpperTSO = filter.upperTSO
	}
	if filterLowerTSO >= filterUpperTSO {
		return 0, true
	}

	// This is a physical-time estimate. Logical TSO parts and strict/inclusive bound differences
	// are too fine-grained for this planner-only cardinality hint.
	retainedLowerPhysical := oracle.ExtractPhysical(retainedLowerTSO)
	retainedUpperPhysical := oracle.ExtractPhysical(estimation.RetainedUpperTSO)
	filterLowerPhysical := oracle.ExtractPhysical(filterLowerTSO)
	filterUpperPhysical := oracle.ExtractPhysical(filterUpperTSO)
	retainedDuration := retainedUpperPhysical - retainedLowerPhysical
	filterDuration := filterUpperPhysical - filterLowerPhysical
	if retainedDuration <= 0 || filterDuration <= 0 {
		return 0, false
	}

	selectivity := float64(filterDuration) / float64(retainedDuration)
	if selectivity < 0 {
		selectivity = 0
	} else if selectivity > 1 {
		selectivity = 1
	}
	return selectivity, true
}
