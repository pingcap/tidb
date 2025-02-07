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

package projection

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	corebase "github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
)

var _ rule.Rule = &XFMergeAdjacentProjection{}

// XFMergeAdjacentProjection pull the correlated expression from projection as child of apply.
type XFMergeAdjacentProjection struct {
	*rule.BaseRule
}

// NewXFMergeAdjacentProjection creates a new XFDeCorrelateSimpleApply rule.
func NewXFMergeAdjacentProjection() *XFMergeAdjacentProjection {
	pa1 := pattern.NewPattern(pattern.OperandProjection, pattern.EngineTiDBOnly)
	pa2 := pattern.NewPattern(pattern.OperandProjection, pattern.EngineTiDBOnly)
	pa2.SetChildren(pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly))
	pa1.SetChildren(pa2)
	return &XFMergeAdjacentProjection{
		BaseRule: rule.NewBaseRule(rule.XFMergeAdjacentProjection, pa1),
	}
}

// ID implement the Rule interface.
func (*XFMergeAdjacentProjection) ID() uint {
	return uint(rule.XFMergeAdjacentProjection)
}

// XForm implements the Rule interface.
func (*XFMergeAdjacentProjection) XForm(projGE corebase.LogicalPlan) ([]corebase.LogicalPlan, bool, error) {
	projOp := projGE.GetWrappedLogicalPlan().(*logicalop.LogicalProjection)
	childProjGE := projGE.Children()[0]
	childProjOp := childProjGE.GetWrappedLogicalPlan().(*logicalop.LogicalProjection)
	if expression.ExprsHasSideEffects(childProjGE.GetWrappedLogicalPlan().(*logicalop.LogicalProjection).Exprs) {
		return nil, false, nil
	}
	replace := make(map[string]*expression.Column)
	for i, col := range childProjOp.Schema().Columns {
		if colOrigin, ok := childProjOp.Exprs[i].(*expression.Column); ok {
			replace[string(col.HashCode())] = colOrigin
		}
	}
	newProjOp := logicalop.LogicalProjection{Exprs: make([]expression.Expression, len(projOp.Exprs))}.Init(projOp.SCtx(), projOp.QueryBlockOffset())
	newProjOp.SetSchema(projGE.Schema()) // share the same schema.
	for i, expr := range projOp.Exprs {
		// expr Clone doesn't clone the fieldType currently.
		newExpr := expr.Clone()
		// eliminate the usage of second proj's schema.
		ruleutil.ResolveExprAndReplace(newExpr, replace)
		newProjOp.Exprs[i] = ruleutil.ReplaceColumnOfExpr(newExpr, childProjOp.Exprs, childProjOp.Schema())
	}
	newProjOp.SetChildren(childProjGE.Children()...)
	return []corebase.LogicalPlan{newProjOp}, false, nil
}
