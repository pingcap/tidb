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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// PPDSolver stands for Predicate Push Down.
type PPDSolver struct{}

// exprPrefixAdder is the wrapper struct to add tidb_shard(x) = val for `OrigConds`
// `cols` is the index columns for a unique shard index
type exprPrefixAdder struct {
	sctx      base.PlanContext
	OrigConds []expression.Expression
	cols      []*expression.Column
	lengths   []int
}

// Optimize implements base.LogicalOptRule.<0th> interface.
func (*PPDSolver) Optimize(_ context.Context, lp base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	_, p := lp.PredicatePushDown(nil, opt)
	return p, planChanged, nil
}

func addSelection(p base.LogicalPlan, child base.LogicalPlan, conditions []expression.Expression, chIdx int, opt *optimizetrace.LogicalOptimizeOp) {
	if len(conditions) == 0 {
		p.Children()[chIdx] = child
		return
	}
	conditions = expression.PropagateConstant(p.SCtx().GetExprCtx(), conditions)
	// Return table dual when filter is constant false or null.
	dual := Conds2TableDual(child, conditions)
	if dual != nil {
		p.Children()[chIdx] = dual
		appendTableDualTraceStep(child, dual, conditions, opt)
		return
	}

	conditions = DeleteTrueExprs(p, conditions)
	if len(conditions) == 0 {
		p.Children()[chIdx] = child
		return
	}
	selection := LogicalSelection{Conditions: conditions}.Init(p.SCtx(), p.QueryBlockOffset())
	selection.SetChildren(child)
	p.Children()[chIdx] = selection
	appendAddSelectionTraceStep(p, child, selection, opt)
}

func splitSetGetVarFunc(filters []expression.Expression) ([]expression.Expression, []expression.Expression) {
	canBePushDown := make([]expression.Expression, 0, len(filters))
	canNotBePushDown := make([]expression.Expression, 0, len(filters))
	for _, expr := range filters {
		if expression.HasGetSetVarFunc(expr) {
			canNotBePushDown = append(canNotBePushDown, expr)
		} else {
			canBePushDown = append(canBePushDown, expr)
		}
	}
	return canBePushDown, canNotBePushDown
}

// DeriveOtherConditions given a LogicalJoin, check the OtherConditions to see if we can derive more
// conditions for left/right child pushdown.
func DeriveOtherConditions(
	p *LogicalJoin, leftSchema *expression.Schema, rightSchema *expression.Schema,
	deriveLeft bool, deriveRight bool) (
	leftCond []expression.Expression, rightCond []expression.Expression) {
	isOuterSemi := (p.JoinType == LeftOuterSemiJoin) || (p.JoinType == AntiLeftOuterSemiJoin)
	ctx := p.SCtx()
	exprCtx := ctx.GetExprCtx()
	for _, expr := range p.OtherConditions {
		if deriveLeft {
			leftRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(exprCtx, expr, leftSchema)
			if leftRelaxedCond != nil {
				leftCond = append(leftCond, leftRelaxedCond)
			}
			notNullExpr := deriveNotNullExpr(ctx, expr, leftSchema)
			if notNullExpr != nil {
				leftCond = append(leftCond, notNullExpr)
			}
		}
		if deriveRight {
			rightRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(exprCtx, expr, rightSchema)
			if rightRelaxedCond != nil {
				rightCond = append(rightCond, rightRelaxedCond)
			}
			// For LeftOuterSemiJoin and AntiLeftOuterSemiJoin, we can actually generate
			// `col is not null` according to expressions in `OtherConditions` now, but we
			// are putting column equal condition converted from `in (subq)` into
			// `OtherConditions`(@sa https://github.com/pingcap/tidb/pull/9051), then it would
			// cause wrong results, so we disable this optimization for outer semi joins now.
			// TODO enable this optimization for outer semi joins later by checking whether
			// condition in `OtherConditions` is converted from `in (subq)`.
			if isOuterSemi {
				continue
			}
			notNullExpr := deriveNotNullExpr(ctx, expr, rightSchema)
			if notNullExpr != nil {
				rightCond = append(rightCond, notNullExpr)
			}
		}
	}
	return
}

// deriveNotNullExpr generates a new expression `not(isnull(col))` given `col1 op col2`,
// in which `col` is in specified schema. Caller guarantees that only one of `col1` or
// `col2` is in schema.
func deriveNotNullExpr(ctx base.PlanContext, expr expression.Expression, schema *expression.Schema) expression.Expression {
	binop, ok := expr.(*expression.ScalarFunction)
	if !ok || len(binop.GetArgs()) != 2 {
		return nil
	}
	arg0, lOK := binop.GetArgs()[0].(*expression.Column)
	arg1, rOK := binop.GetArgs()[1].(*expression.Column)
	if !lOK || !rOK {
		return nil
	}
	childCol := schema.RetrieveColumn(arg0)
	if childCol == nil {
		childCol = schema.RetrieveColumn(arg1)
	}
	if util.IsNullRejected(ctx, schema, expr) && !mysql.HasNotNullFlag(childCol.RetType.GetFlag()) {
		return expression.BuildNotNullExpr(ctx.GetExprCtx(), childCol)
	}
	return nil
}

// Conds2TableDual builds a LogicalTableDual if cond is constant false or null.
func Conds2TableDual(p base.LogicalPlan, conds []expression.Expression) base.LogicalPlan {
	if len(conds) != 1 {
		return nil
	}
	con, ok := conds[0].(*expression.Constant)
	if !ok {
		return nil
	}
	sc := p.SCtx().GetSessionVars().StmtCtx
	if expression.MaybeOverOptimized4PlanCache(p.SCtx().GetExprCtx(), []expression.Expression{con}) {
		return nil
	}
	if isTrue, err := con.Value.ToBool(sc.TypeCtxOrDefault()); (err == nil && isTrue == 0) || con.Value.IsNull() {
		dual := logicalop.LogicalTableDual{}.Init(p.SCtx(), p.QueryBlockOffset())
		dual.SetSchema(p.Schema())
		return dual
	}
	return nil
}

// DeleteTrueExprs deletes the surely true expressions
func DeleteTrueExprs(p base.LogicalPlan, conds []expression.Expression) []expression.Expression {
	newConds := make([]expression.Expression, 0, len(conds))
	for _, cond := range conds {
		con, ok := cond.(*expression.Constant)
		if !ok {
			newConds = append(newConds, cond)
			continue
		}
		if expression.MaybeOverOptimized4PlanCache(p.SCtx().GetExprCtx(), []expression.Expression{con}) {
			newConds = append(newConds, cond)
			continue
		}
		sc := p.SCtx().GetSessionVars().StmtCtx
		if isTrue, err := con.Value.ToBool(sc.TypeCtx()); err == nil && isTrue == 1 {
			continue
		}
		newConds = append(newConds, cond)
	}
	return newConds
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*PPDSolver) Name() string {
	return "predicate_push_down"
}

func appendTableDualTraceStep(replaced base.LogicalPlan, dual base.LogicalPlan, conditions []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) {
	action := func() string {
		return fmt.Sprintf("%v_%v is replaced by %v_%v", replaced.TP(), replaced.ID(), dual.TP(), dual.ID())
	}
	ectx := replaced.SCtx().GetExprCtx().GetEvalCtx()
	reason := func() string {
		buffer := bytes.NewBufferString("The conditions[")
		for i, cond := range conditions {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(cond.StringWithCtx(ectx, errors.RedactLogDisable))
		}
		buffer.WriteString("] are constant false or null")
		return buffer.String()
	}
	opt.AppendStepToCurrent(dual.ID(), dual.TP(), reason, action)
}

func appendSelectionPredicatePushDownTraceStep(p *LogicalSelection, conditions []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) {
	action := func() string {
		return fmt.Sprintf("%v_%v is removed", p.TP(), p.ID())
	}
	reason := func() string {
		return ""
	}
	if len(conditions) > 0 {
		evalCtx := p.SCtx().GetExprCtx().GetEvalCtx()
		reason = func() string {
			buffer := bytes.NewBufferString("The conditions[")
			for i, cond := range conditions {
				if i > 0 {
					buffer.WriteString(",")
				}
				buffer.WriteString(cond.StringWithCtx(evalCtx, errors.RedactLogDisable))
			}
			fmt.Fprintf(buffer, "] in %v_%v are pushed down", p.TP(), p.ID())
			return buffer.String()
		}
	}
	opt.AppendStepToCurrent(p.ID(), p.TP(), reason, action)
}

func appendDataSourcePredicatePushDownTraceStep(ds *DataSource, opt *optimizetrace.LogicalOptimizeOp) {
	if len(ds.PushedDownConds) < 1 {
		return
	}
	ectx := ds.SCtx().GetExprCtx().GetEvalCtx()
	reason := func() string {
		return ""
	}
	action := func() string {
		buffer := bytes.NewBufferString("The conditions[")
		for i, cond := range ds.PushedDownConds {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(cond.StringWithCtx(ectx, errors.RedactLogDisable))
		}
		fmt.Fprintf(buffer, "] are pushed down across %v_%v", ds.TP(), ds.ID())
		return buffer.String()
	}
	opt.AppendStepToCurrent(ds.ID(), ds.TP(), reason, action)
}

func appendAddSelectionTraceStep(p base.LogicalPlan, child base.LogicalPlan, sel *LogicalSelection, opt *optimizetrace.LogicalOptimizeOp) {
	reason := func() string {
		return ""
	}
	action := func() string {
		return fmt.Sprintf("add %v_%v to connect %v_%v and %v_%v", sel.TP(), sel.ID(), p.TP(), p.ID(), child.TP(), child.ID())
	}
	opt.AppendStepToCurrent(sel.ID(), sel.TP(), reason, action)
}

func (ds *DataSource) addExprPrefixCond(sc base.PlanContext, path *util.AccessPath,
	conds []expression.Expression) ([]expression.Expression, error) {
	idxCols, idxColLens :=
		expression.IndexInfo2PrefixCols(ds.Columns, ds.Schema().Columns, path.Index)
	if len(idxCols) == 0 {
		return conds, nil
	}

	adder := &exprPrefixAdder{
		sctx:      sc,
		OrigConds: conds,
		cols:      idxCols,
		lengths:   idxColLens,
	}

	return adder.addExprPrefix4ShardIndex()
}

// AddExprPrefix4ShardIndex
// if original condition is a LogicOr expression, such as `WHERE a = 1 OR a = 10`,
// call the function AddExprPrefix4DNFCond to add prefix expression tidb_shard(a) = xxx for shard index.
// Otherwise, if the condition is  `WHERE a = 1`, `WHERE a = 1 AND b = 10`, `WHERE a IN (1, 2, 3)`......,
// call the function AddExprPrefix4CNFCond to add prefix expression for shard index.
func (adder *exprPrefixAdder) addExprPrefix4ShardIndex() ([]expression.Expression, error) {
	if len(adder.OrigConds) == 1 {
		if sf, ok := adder.OrigConds[0].(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicOr {
			return adder.addExprPrefix4DNFCond(sf)
		}
	}
	return adder.addExprPrefix4CNFCond(adder.OrigConds)
}

// AddExprPrefix4CNFCond
// add the prefix expression for CNF condition, e.g. `WHERE a = 1`, `WHERE a = 1 AND b = 10`, ......
// @param[in] conds        the original condtion of the datasoure. e.g. `WHERE t1.a = 1 AND t1.b = 10 AND t2.a = 20`.
//
//	if current datasource is `t1`, conds is {t1.a = 1, t1.b = 10}. if current datasource is
//	`t2`, conds is {t2.a = 20}
//
// @return  -     the new condition after adding expression prefix
func (adder *exprPrefixAdder) addExprPrefix4CNFCond(conds []expression.Expression) ([]expression.Expression, error) {
	newCondtionds, err := ranger.AddExpr4EqAndInCondition(adder.sctx.GetRangerCtx(),
		conds, adder.cols)

	return newCondtionds, err
}

// AddExprPrefix4DNFCond
// add the prefix expression for DNF condition, e.g. `WHERE a = 1 OR a = 10`, ......
// The condition returned is `WHERE (tidb_shard(a) = 214 AND a = 1) OR (tidb_shard(a) = 142 AND a = 10)`
// @param[in] condition    the original condtion of the datasoure. e.g. `WHERE a = 1 OR a = 10`. condtion is `a = 1 OR a = 10`
// @return 	 -          the new condition after adding expression prefix. It's still a LogicOr expression.
func (adder *exprPrefixAdder) addExprPrefix4DNFCond(condition *expression.ScalarFunction) ([]expression.Expression, error) {
	var err error
	dnfItems := expression.FlattenDNFConditions(condition)
	newAccessItems := make([]expression.Expression, 0, len(dnfItems))

	exprCtx := adder.sctx.GetExprCtx()
	for _, item := range dnfItems {
		if sf, ok := item.(*expression.ScalarFunction); ok {
			var accesses []expression.Expression
			if sf.FuncName.L == ast.LogicAnd {
				cnfItems := expression.FlattenCNFConditions(sf)
				accesses, err = adder.addExprPrefix4CNFCond(cnfItems)
				if err != nil {
					return []expression.Expression{condition}, err
				}
				newAccessItems = append(newAccessItems, expression.ComposeCNFCondition(exprCtx, accesses...))
			} else if sf.FuncName.L == ast.EQ || sf.FuncName.L == ast.In {
				// only add prefix expression for EQ or IN function
				accesses, err = adder.addExprPrefix4CNFCond([]expression.Expression{sf})
				if err != nil {
					return []expression.Expression{condition}, err
				}
				newAccessItems = append(newAccessItems, expression.ComposeCNFCondition(exprCtx, accesses...))
			} else {
				newAccessItems = append(newAccessItems, item)
			}
		} else {
			newAccessItems = append(newAccessItems, item)
		}
	}

	return []expression.Expression{expression.ComposeDNFCondition(exprCtx, newAccessItems...)}, nil
}
