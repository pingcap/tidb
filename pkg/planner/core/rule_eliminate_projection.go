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
	"bytes"
	"context"
	"fmt"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
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
func canProjectionBeEliminatedStrict(p *PhysicalProjection) bool {
	// This is due to the in-compatibility between TiFlash and TiDB:
	// For TiDB, the output schema of final agg is all the aggregated functions and for
	// TiFlash, the output schema of agg(TiFlash not aware of the aggregation mode) is
	// aggregated functions + group by columns, so to make the things work, for final
	// mode aggregation that need to be running in TiFlash, always add an extra Project
	// the align the output schema. In the future, we can solve this in-compatibility by
	// passing down the aggregation mode to TiFlash.
	if physicalAgg, ok := p.Children()[0].(*PhysicalHashAgg); ok {
		if physicalAgg.MppRunMode == Mpp1Phase || physicalAgg.MppRunMode == Mpp2Phase || physicalAgg.MppRunMode == MppScalar {
			if physicalAgg.IsFinalAgg() {
				return false
			}
		}
	}
	if physicalAgg, ok := p.Children()[0].(*PhysicalStreamAgg); ok {
		if physicalAgg.MppRunMode == Mpp1Phase || physicalAgg.MppRunMode == Mpp2Phase || physicalAgg.MppRunMode == MppScalar {
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
	tableReader, isTableReader := p.(*PhysicalTableReader)
	if isTableReader && tableReader.StoreType == kv.TiFlash {
		tableReader.tablePlan = eliminatePhysicalProjection(tableReader.tablePlan)
		tableReader.TablePlans = flattenPushDownPlan(tableReader.tablePlan)
		return p
	}

	proj, isProj := p.(*PhysicalProjection)
	if !isProj || !canProjectionBeEliminatedStrict(proj) {
		return p
	}
	child := p.Children()[0]
	if childProj, ok := child.(*PhysicalProjection); ok {
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
	failpoint.Inject("DisableProjectionPostOptimization", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(p)
		}
	})

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
func (pe *ProjectionEliminator) Optimize(_ context.Context, lp base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	root := pe.eliminate(lp, make(map[string]*expression.Column), false, opt)
	return root, planChanged, nil
}

// eliminate eliminates the redundant projection in a logical plan.
func (pe *ProjectionEliminator) eliminate(p base.LogicalPlan, replace map[string]*expression.Column, canEliminate bool, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
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
		p.Children()[i] = pe.eliminate(child, replace, childFlag, opt)
	}

	// replace logical plan schema
	switch x := p.(type) {
	case *logicalop.LogicalJoin:
		x.SetSchema(logicalop.BuildLogicalJoinSchema(x.JoinType, x))
	case *logicalop.LogicalApply:
		x.SetSchema(logicalop.BuildLogicalJoinSchema(x.JoinType, x))
	default:
		for _, dst := range p.Schema().Columns {
			ruleutil.ResolveColumnAndReplace(dst, replace)
		}
	}
	// replace all of exprs in logical plan
	p.ReplaceExprColumns(replace)

	// eliminate duplicate projection: projection with child projection
	if isProj {
		if child, ok := p.Children()[0].(*logicalop.LogicalProjection); ok && !expression.ExprsHasSideEffects(child.Exprs) {
			ctx := p.SCtx()
			for i := range proj.Exprs {
				proj.Exprs[i] = ReplaceColumnOfExpr(proj.Exprs[i], child, child.Schema())
				foldedExpr := expression.FoldConstant(ctx.GetExprCtx(), proj.Exprs[i])
				// the folded expr should have the same null flag with the original expr, especially for the projection under union, so forcing it here.
				foldedExpr.GetType(ctx.GetExprCtx().GetEvalCtx()).SetFlag((foldedExpr.GetType(ctx.GetExprCtx().GetEvalCtx()).GetFlag() & ^mysql.NotNullFlag) | (proj.Exprs[i].GetType(ctx.GetExprCtx().GetEvalCtx()).GetFlag() & mysql.NotNullFlag))
				proj.Exprs[i] = foldedExpr
			}
			p.Children()[0] = child.Children()[0]
			appendDupProjEliminateTraceStep(proj, child, opt)
		}
	}

	if !(isProj && canEliminate && canProjectionBeEliminatedLoose(proj)) {
		return p
	}
	exprs := proj.Exprs
	for i, col := range proj.Schema().Columns {
		replace[string(col.HashCode())] = exprs[i].(*expression.Column)
	}
	appendProjEliminateTraceStep(proj, opt)
	return p.Children()[0]
}

// ReplaceColumnOfExpr replaces column of expression by another LogicalProjection.
func ReplaceColumnOfExpr(expr expression.Expression, proj *logicalop.LogicalProjection, schema *expression.Schema) expression.Expression {
	switch v := expr.(type) {
	case *expression.Column:
		idx := schema.ColumnIndex(v)
		if idx != -1 && idx < len(proj.Exprs) {
			return proj.Exprs[idx]
		}
	case *expression.ScalarFunction:
		for i := range v.GetArgs() {
			v.GetArgs()[i] = ReplaceColumnOfExpr(v.GetArgs()[i], proj, schema)
		}
	}
	return expr
}

// Name implements the logicalOptRule.<1st> interface.
func (*ProjectionEliminator) Name() string {
	return "projection_eliminate"
}

func appendDupProjEliminateTraceStep(parent, child *logicalop.LogicalProjection, opt *optimizetrace.LogicalOptimizeOp) {
	ectx := parent.SCtx().GetExprCtx().GetEvalCtx()
	action := func() string {
		buffer := bytes.NewBufferString(
			fmt.Sprintf("%v_%v is eliminated, %v_%v's expressions changed into[", child.TP(), child.ID(), parent.TP(), parent.ID()))
		for i, expr := range parent.Exprs {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(expr.StringWithCtx(ectx, perrors.RedactLogDisable))
		}
		buffer.WriteString("]")
		return buffer.String()
	}
	reason := func() string {
		return fmt.Sprintf("%v_%v's child %v_%v is redundant", parent.TP(), parent.ID(), child.TP(), child.ID())
	}
	opt.AppendStepToCurrent(child.ID(), child.TP(), reason, action)
}

func appendProjEliminateTraceStep(proj *logicalop.LogicalProjection, opt *optimizetrace.LogicalOptimizeOp) {
	reason := func() string {
		return fmt.Sprintf("%v_%v's Exprs are all Columns", proj.TP(), proj.ID())
	}
	action := func() string {
		return fmt.Sprintf("%v_%v is eliminated", proj.TP(), proj.ID())
	}
	opt.AppendStepToCurrent(proj.ID(), proj.TP(), reason, action)
}
