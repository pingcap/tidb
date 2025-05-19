// Copyright 2025 PingCAP, Inc.
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

package ppd

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	corebase "github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/constraint"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
)

var _ rule.Rule = &XFPredicatePushDownDataSource{}

// XFPredicatePushDownDataSource try to convert case when from agg to selection downward.
type XFPredicatePushDownDataSource struct {
	*rule.BaseRule
}

// NewXFPredicatePushDownDataSource creates a new XFDeCorrelateSimpleApply rule.
func NewXFPredicatePushDownDataSource() *XFPredicatePushDownDataSource {
	pa := pattern.NewPattern(pattern.OperandSelection, pattern.EngineTiDBOnly)
	pa.SetChildren(pattern.NewPattern(pattern.OperandDataSource, pattern.EngineTiDBOnly))
	return &XFPredicatePushDownDataSource{
		BaseRule: rule.NewBaseRule(rule.XFPredicatePushDownDataSource, pa),
	}
}

// ID implement the Rule interface.
func (*XFPredicatePushDownDataSource) ID() uint {
	return uint(rule.XFPredicatePushDownDataSource)
}

// PreCheck implements the Rule interface.
func (xf *XFPredicatePushDownDataSource) PreCheck(selGE corebase.LogicalPlan) bool {
	selOp := selGE.GetWrappedLogicalPlan().(*logicalop.LogicalSelection)
	predicates := constraint.DeleteTrueExprs(selOp, selOp.Conditions)
	canBePushDown, _ := logicalop.SplitSetGetVarFunc(predicates)
	if len(canBePushDown) <= 0 {
		return false
	}
	return true
}

// XForm implements the Rule interface.
func (xf *XFPredicatePushDownDataSource) XForm(selGE corebase.LogicalPlan) ([]corebase.LogicalPlan, bool, error) {
	selOp := selGE.GetWrappedLogicalPlan().(*logicalop.LogicalSelection)
	dsOp := selGE.Children()[0].GetWrappedLogicalPlan().(*logicalop.DataSource)
	predicates := constraint.DeleteTrueExprs(selOp, selOp.Conditions)
	// SplitSetGetVarFunc work can be handled by PushDownExprs utility.
	// when arrive here, the len(canBePushDown) is > 0.
	// PropagateConstant can ensure when the new expr is created, it always creates a new slices
	// or function bottom-up, kind of COW, doesn't affect the original expressions.
	predicates = expression.PropagateConstant(dsOp.SCtx().GetExprCtx(), predicates)
	predicates = constraint.DeleteTrueExprs(dsOp, predicates)
	// we don't take care of AddPrefix4ShardIndexes now, maybe future work.
	clonedDS := dsOp.DataSourceShallowRef()
	clonedDS.ReAlloc4Cascades(clonedDS.TP(), clonedDS)
	clonedDS.AllConds = predicates
	if xf.CheckDual(clonedDS, predicates) {
		dual := logicalop.LogicalTableDual{}.Init(clonedDS.SCtx(), clonedDS.QueryBlockOffset())
		dual.SetSchema(clonedDS.Schema())
		return []corebase.LogicalPlan{dual}, true, nil
	}
	clonedDS.PushedDownConds, predicates = expression.PushDownExprs(util.GetPushDownCtx(clonedDS.SCtx()), predicates, kv.UnSpecified)
	return []corebase.LogicalPlan{clonedDS}, true, nil
}

// CheckDual checks if a LogicalTableDual can be generated if cond is constant false or null.
func (xf *XFPredicatePushDownDataSource) CheckDual(clonedDS corebase.LogicalPlan, conds []expression.Expression) bool {
	for _, cond := range conds {
		if expression.IsConstNull(cond) {
			if expression.MaybeOverOptimized4PlanCache(clonedDS.SCtx().GetExprCtx(), conds) {
				return false
			}
			return true
		}
	}
	if len(conds) != 1 {
		return false
	}

	con, ok := conds[0].(*expression.Constant)
	if !ok {
		return false
	}
	sc := clonedDS.SCtx().GetSessionVars().StmtCtx
	if expression.MaybeOverOptimized4PlanCache(clonedDS.SCtx().GetExprCtx(), []expression.Expression{con}) {
		return false
	}
	if isTrue, err := con.Value.ToBool(sc.TypeCtxOrDefault()); (err == nil && isTrue == 0) || con.Value.IsNull() {
		return true
	}
	return false
}
