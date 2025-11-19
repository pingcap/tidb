// Copyright 2016 PingCAP, Inc.
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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
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

// canProjectionBeEliminatedStrict checks whether a projection can be
// eliminated, returns true if the projection just copy its child's output.
func canProjectionBeEliminatedStrict(p *physicalop.PhysicalProjection) bool {
	// This is due to the in-compatibility between TiFlash and TiDB:
	// For TiDB, the output schema of final agg is all the aggregated functions and for
	// TiFlash, the output schema of agg(TiFlash not aware of the aggregation mode) is
	// aggregated functions + group by columns, so to make the things work, for final
	// mode aggregation that need to be running in TiFlash, always add an extra Project
	// the align the output schema. In the future, we can solve this in-compatibility by
	// passing down the aggregation mode to TiFlash.
	if physicalAgg, ok := p.Children()[0].(*physicalop.PhysicalHashAgg); ok {
		if physicalAgg.MppRunMode == physicalop.Mpp1Phase || physicalAgg.MppRunMode == physicalop.Mpp2Phase || physicalAgg.MppRunMode == physicalop.MppScalar {
			if physicalAgg.IsFinalAgg() {
				return false
			}
		}
	}
	if physicalAgg, ok := p.Children()[0].(*physicalop.PhysicalStreamAgg); ok {
		if physicalAgg.MppRunMode == physicalop.Mpp1Phase || physicalAgg.MppRunMode == physicalop.Mpp2Phase || physicalAgg.MppRunMode == physicalop.MppScalar {
			if physicalAgg.IsFinalAgg() {
				return false
			}
		}
	}
	// If this projection is specially added for `DO`, we keep it.
	if p.CalculateNoDelay {
		return false
	}
	if p.Schema().Len() == 0 {
		return true
	}
	child := p.Children()[0]
	if p.Schema().Len() != child.Schema().Len() {
		return false
	}
	for i, expr := range p.Exprs {
		col, ok := expr.(*expression.Column)
		if !ok || !col.EqualColumn(child.Schema().Columns[i]) {
			return false
		}
	}
	return true
}

func doPhysicalProjectionElimination(p base.PhysicalPlan) base.PhysicalPlan {
	for i, child := range p.Children() {
		p.Children()[i] = doPhysicalProjectionElimination(child)
	}

	// eliminate projection in a coprocessor task
	tableReader, isTableReader := p.(*physicalop.PhysicalTableReader)
	if isTableReader && tableReader.StoreType == kv.TiFlash {
		tableReader.TablePlan = eliminatePhysicalProjection(tableReader.TablePlan)
		tableReader.TablePlans = physicalop.FlattenListPushDownPlan(tableReader.TablePlan)
		return p
	}

	proj, isProj := p.(*physicalop.PhysicalProjection)
	if !isProj || !canProjectionBeEliminatedStrict(proj) {
		return p
	}
	child := p.Children()[0]
	if childProj, ok := child.(*physicalop.PhysicalProjection); ok {
		// when current projection is an empty projection(schema pruned by column pruner), no need to reset child's schema
		// TODO: avoid producing empty projection in column pruner.
		if p.Schema().Len() != 0 {
			childProj.SetSchema(p.Schema())
		}
	}
	for i, col := range p.Schema().Columns {
		if p.SCtx().GetSessionVars().StmtCtx.ColRefFromUpdatePlan.Has(int(col.UniqueID)) && !child.Schema().Columns[i].Equal(nil, col) {
			return p
		}
	}
	return child
}

// eliminatePhysicalProjection should be called after physical optimization to
// eliminate the redundant projection left after logical projection elimination.
func eliminatePhysicalProjection(p base.PhysicalPlan) base.PhysicalPlan {
	if val, _err_ := failpoint.Eval(_curpkg_("DisableProjectionPostOptimization")); _err_ == nil {
		if val.(bool) {
			return p
		}
	}

	newRoot := doPhysicalProjectionElimination(p)
	return newRoot
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

	// replace logical plan schema
	switch x := p.(type) {
	case *logicalop.LogicalJoin:
		x.SetSchema(logicalop.BuildLogicalJoinSchema(x.JoinType, x))
	case *logicalop.LogicalApply:
		x.SetSchema(logicalop.BuildLogicalJoinSchema(x.JoinType, x))
	default:
		for i, dst := range p.Schema().Columns {
			p.Schema().Columns[i] = ruleutil.ResolveColumnAndReplace(dst, replace)
		}
	}

	// Filter replace map first to make sure the replaced columns
	// exist in the child output.
	newReplace := make(map[string]*expression.Column, len(replace))
	for code, expr := range replace {
	childloop:
		for _, child := range p.Children() {
			for _, schemaCol := range child.Schema().Columns {
				if schemaCol.Equal(nil, expr) {
					newReplace[code] = expr
					break childloop
				}
			}
		}
	}
	if len(newReplace) > 0 {
		p.ReplaceExprColumns(newReplace)
	}

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

// Name implements the logicalOptRule.<1st> interface.
func (*ProjectionEliminator) Name() string {
	return "projection_eliminate"
}
