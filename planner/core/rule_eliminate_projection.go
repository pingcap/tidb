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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
)

// canProjectionBeEliminatedLoose checks whether a projection can be eliminated,
// returns true if every expression is a single column.
func canProjectionBeEliminatedLoose(p *LogicalProjection) bool {
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
			if physicalAgg.isFinalAgg() {
				return false
			}
		}
	}
	if physicalAgg, ok := p.Children()[0].(*PhysicalStreamAgg); ok {
		if physicalAgg.MppRunMode == Mpp1Phase || physicalAgg.MppRunMode == Mpp2Phase || physicalAgg.MppRunMode == MppScalar {
			if physicalAgg.isFinalAgg() {
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
		if !ok || !col.Equal(nil, child.Schema().Columns[i]) {
			return false
		}
	}
	return true
}

func resolveColumnAndReplace(origin *expression.Column, replace map[string]*expression.Column) {
	dst := replace[string(origin.HashCode(nil))]
	if dst != nil {
		retType, inOperand := origin.RetType, origin.InOperand
		*origin = *dst
		origin.RetType, origin.InOperand = retType, inOperand
	}
}

// ResolveExprAndReplace replaces columns fields of expressions by children logical plans.
func ResolveExprAndReplace(origin expression.Expression, replace map[string]*expression.Column) {
	switch expr := origin.(type) {
	case *expression.Column:
		resolveColumnAndReplace(expr, replace)
	case *expression.CorrelatedColumn:
		resolveColumnAndReplace(&expr.Column, replace)
	case *expression.ScalarFunction:
		for _, arg := range expr.GetArgs() {
			ResolveExprAndReplace(arg, replace)
		}
	}
}

func doPhysicalProjectionElimination(p PhysicalPlan) PhysicalPlan {
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
		childProj.SetSchema(p.Schema())
	}
	return child
}

// eliminatePhysicalProjection should be called after physical optimization to
// eliminate the redundant projection left after logical projection elimination.
func eliminatePhysicalProjection(p PhysicalPlan) PhysicalPlan {
	failpoint.Inject("DisableProjectionPostOptimization", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(p)
		}
	})

	oldSchema := p.Schema()
	newRoot := doPhysicalProjectionElimination(p)
	newCols := newRoot.Schema().Columns
	for i, oldCol := range oldSchema.Columns {
		oldCol.Index = newCols[i].Index
		oldCol.ID = newCols[i].ID
		oldCol.UniqueID = newCols[i].UniqueID
		oldCol.VirtualExpr = newCols[i].VirtualExpr
		newRoot.Schema().Columns[i] = oldCol
	}
	return newRoot
}

type projectionEliminator struct {
}

// optimize implements the logicalOptRule interface.
func (pe *projectionEliminator) optimize(ctx context.Context, lp LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	root := pe.eliminate(lp, make(map[string]*expression.Column), false, opt)
	return root, nil
}

// eliminate eliminates the redundant projection in a logical plan.
func (pe *projectionEliminator) eliminate(p LogicalPlan, replace map[string]*expression.Column, canEliminate bool, opt *logicalOptimizeOp) LogicalPlan {
	proj, isProj := p.(*LogicalProjection)
	childFlag := canEliminate
	if _, isUnion := p.(*LogicalUnionAll); isUnion {
		childFlag = false
	} else if _, isAgg := p.(*LogicalAggregation); isAgg || isProj {
		childFlag = true
	} else if _, isWindow := p.(*LogicalWindow); isWindow {
		childFlag = true
	}
	for i, child := range p.Children() {
		p.Children()[i] = pe.eliminate(child, replace, childFlag, opt)
	}

	switch x := p.(type) {
	case *LogicalJoin:
		x.schema = buildLogicalJoinSchema(x.JoinType, x)
	case *LogicalApply:
		x.schema = buildLogicalJoinSchema(x.JoinType, x)
	default:
		for _, dst := range p.Schema().Columns {
			resolveColumnAndReplace(dst, replace)
		}
	}
	p.replaceExprColumns(replace)
	if isProj {
		if child, ok := p.Children()[0].(*LogicalProjection); ok && !ExprsHasSideEffects(child.Exprs) {
			for i := range proj.Exprs {
				proj.Exprs[i] = ReplaceColumnOfExpr(proj.Exprs[i], child, child.Schema())
				foldedExpr := expression.FoldConstant(proj.Exprs[i])
				// the folded expr should have the same null flag with the original expr, especially for the projection under union, so forcing it here.
				foldedExpr.GetType().SetFlag((foldedExpr.GetType().GetFlag() & ^mysql.NotNullFlag) | (proj.Exprs[i].GetType().GetFlag() & mysql.NotNullFlag))
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
		replace[string(col.HashCode(nil))] = exprs[i].(*expression.Column)
	}
	appendProjEliminateTraceStep(proj, opt)
	return p.Children()[0]
}

// ReplaceColumnOfExpr replaces column of expression by another LogicalProjection.
func ReplaceColumnOfExpr(expr expression.Expression, proj *LogicalProjection, schema *expression.Schema) expression.Expression {
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

func (p *LogicalJoin) replaceExprColumns(replace map[string]*expression.Column) {
	for _, equalExpr := range p.EqualConditions {
		ResolveExprAndReplace(equalExpr, replace)
	}
	for _, leftExpr := range p.LeftConditions {
		ResolveExprAndReplace(leftExpr, replace)
	}
	for _, rightExpr := range p.RightConditions {
		ResolveExprAndReplace(rightExpr, replace)
	}
	for _, otherExpr := range p.OtherConditions {
		ResolveExprAndReplace(otherExpr, replace)
	}
}

func (p *LogicalProjection) replaceExprColumns(replace map[string]*expression.Column) {
	for _, expr := range p.Exprs {
		ResolveExprAndReplace(expr, replace)
	}
}

func (la *LogicalAggregation) replaceExprColumns(replace map[string]*expression.Column) {
	for _, agg := range la.AggFuncs {
		for _, aggExpr := range agg.Args {
			ResolveExprAndReplace(aggExpr, replace)
		}
	}
	for _, gbyItem := range la.GroupByItems {
		ResolveExprAndReplace(gbyItem, replace)
	}
}

func (p *LogicalSelection) replaceExprColumns(replace map[string]*expression.Column) {
	for _, expr := range p.Conditions {
		ResolveExprAndReplace(expr, replace)
	}
}

func (la *LogicalApply) replaceExprColumns(replace map[string]*expression.Column) {
	la.LogicalJoin.replaceExprColumns(replace)
	for _, coCol := range la.CorCols {
		dst := replace[string(coCol.Column.HashCode(nil))]
		if dst != nil {
			coCol.Column = *dst
		}
	}
}

func (ls *LogicalSort) replaceExprColumns(replace map[string]*expression.Column) {
	for _, byItem := range ls.ByItems {
		ResolveExprAndReplace(byItem.Expr, replace)
	}
}

func (lt *LogicalTopN) replaceExprColumns(replace map[string]*expression.Column) {
	for _, byItem := range lt.ByItems {
		ResolveExprAndReplace(byItem.Expr, replace)
	}
}

func (p *LogicalWindow) replaceExprColumns(replace map[string]*expression.Column) {
	for _, desc := range p.WindowFuncDescs {
		for _, arg := range desc.Args {
			ResolveExprAndReplace(arg, replace)
		}
	}
	for _, item := range p.PartitionBy {
		resolveColumnAndReplace(item.Col, replace)
	}
	for _, item := range p.OrderBy {
		resolveColumnAndReplace(item.Col, replace)
	}
}

func (*projectionEliminator) name() string {
	return "projection_eliminate"
}

func appendDupProjEliminateTraceStep(parent, child *LogicalProjection, opt *logicalOptimizeOp) {
	action := func() string {
		buffer := bytes.NewBufferString(
			fmt.Sprintf("%v_%v is eliminated, %v_%v's expressions changed into[", child.TP(), child.ID(), parent.TP(), parent.ID()))
		for i, expr := range parent.Exprs {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(expr.String())
		}
		buffer.WriteString("]")
		return buffer.String()
	}
	reason := func() string {
		return fmt.Sprintf("%v_%v's child %v_%v is redundant", parent.TP(), parent.ID(), child.TP(), child.ID())
	}
	opt.appendStepToCurrent(child.ID(), child.TP(), reason, action)
}

func appendProjEliminateTraceStep(proj *LogicalProjection, opt *logicalOptimizeOp) {
	reason := func() string {
		return fmt.Sprintf("%v_%v's Exprs are all Columns", proj.TP(), proj.ID())
	}
	action := func() string {
		return fmt.Sprintf("%v_%v is eliminated", proj.TP(), proj.ID())
	}
	opt.appendStepToCurrent(proj.ID(), proj.TP(), reason, action)
}
