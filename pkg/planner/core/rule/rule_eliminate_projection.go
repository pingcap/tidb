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

package rule

import (
	"context"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
)

// canProjectionBeEliminatedLoose checks whether a projection can be eliminated,
// returns true if every expression is a single column.
func canProjectionBeEliminatedLoose(p *logicalop.LogicalProjection) bool {
	// project for expand will assign a new col id for col ref, because these column should be
	// data cloned in the execution time and may be filled with null value at the same time.
	// so it's not a REAL column reference. Detect the column ref in projection here and do
	// the elimination here will restore the Expand's grouping sets column back to use the
	// original column ref again. (which is not right)
	if p.Proj4Expand {
		return false
	}
	for _, expr := range p.Exprs {
		_, ok := expr.(*expression.Column)
		if !ok {
			return false
		}
	}
	return true
}

// For select, insert, delete list
// The projection eliminate in logical optimize will optimize the projection under the projection, window, agg
// The projection eliminate in post optimize will optimize other projection

// ProjectionEliminator is for update stmt
// The projection eliminate in logical optimize has been forbidden.
// The projection eliminate in post optimize will optimize the projection under the projection, window, agg (the condition is same as logical optimize)
type ProjectionEliminator struct {
}

// Optimize implements the logicalOptRule interface.
func (pe *ProjectionEliminator) Optimize(_ context.Context, lp base.LogicalPlan) (base.LogicalPlan, bool, error) {
	planChanged := false
	root := pe.eliminate(lp, make(map[string]*expression.Column), false)
	return root, planChanged, nil
}

// eliminate eliminates the redundant projection in a logical plan.
func (pe *ProjectionEliminator) eliminate(p base.LogicalPlan, replace map[string]*expression.Column, canEliminate bool) base.LogicalPlan {
	// LogicalCTE's logical optimization is independent.
	if _, ok := p.(*logicalop.LogicalCTE); ok {
		return p
	}
	proj, isProj := p.(*logicalop.LogicalProjection)
	childFlag := canEliminate
	if _, isUnion := p.(*logicalop.LogicalUnionAll); isUnion {
		childFlag = false
	} else if _, isAgg := p.(*logicalop.LogicalAggregation); isAgg || isProj {
		childFlag = true
	} else if _, isWindow := p.(*logicalop.LogicalWindow); isWindow {
		childFlag = true
	}
	for i, child := range p.Children() {
		p.Children()[i] = pe.eliminate(child, replace, childFlag)
	}

	// Replace all columns in the schema with the replaced columns.
	switch x := p.(type) {
	case *logicalop.LogicalApply:
		x.SetSchema(logicalop.BuildLogicalJoinSchema(x.JoinType, x))
	default:
		for i, dst := range p.Schema().Columns {
			p.Schema().Columns[i] = ruleutil.ResolveColumnAndReplace(dst, replace)
		}
	}

	p.ReplaceExprColumns(replace)

	// eliminate duplicate projection: projection with child projection
	if isProj {
		if child, ok := p.Children()[0].(*logicalop.LogicalProjection); ok && !expression.ExprsHasSideEffects(child.Exprs) {
			ctx := p.SCtx()
			for i := range proj.Exprs {
				proj.Exprs[i] = ruleutil.ReplaceColumnOfExpr(proj.Exprs[i], child.Exprs, child.Schema())
				foldedExpr := expression.FoldConstant(ctx.GetExprCtx(), proj.Exprs[i])
				// the folded expr should have the same null flag with the original expr, especially for the projection under union, so forcing it here.
				foldedExpr.GetType(ctx.GetExprCtx().GetEvalCtx()).SetFlag((foldedExpr.GetType(ctx.GetExprCtx().GetEvalCtx()).GetFlag() & ^mysql.NotNullFlag) | (proj.Exprs[i].GetType(ctx.GetExprCtx().GetEvalCtx()).GetFlag() & mysql.NotNullFlag))
				proj.Exprs[i] = foldedExpr
			}
			p.Children()[0] = child.Children()[0]
		}
	}

	if !(isProj && canEliminate && canProjectionBeEliminatedLoose(proj)) {
		return p
	}
	exprs := proj.Exprs
	for i, col := range proj.Schema().Columns {
		replace[string(col.HashCode())] = exprs[i].(*expression.Column)
	}
	return p.Children()[0]
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*ProjectionEliminator) Name() string {
	return "projection_eliminate"
}
