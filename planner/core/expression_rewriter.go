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
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/stringutil"
)

// EvalSubqueryFirstRow evaluates incorrelated subqueries once, and get first row.
var EvalSubqueryFirstRow func(ctx context.Context, p PhysicalPlan, is infoschema.InfoSchema, sctx sessionctx.Context) (row []types.Datum, err error)

// evalAstExpr evaluates ast expression directly.
func evalAstExpr(sctx sessionctx.Context, expr ast.ExprNode) (types.Datum, error) {
	if val, ok := expr.(*driver.ValueExpr); ok {
		return val.Datum, nil
	}
	newExpr, err := rewriteAstExpr(sctx, expr, nil, nil)
	if err != nil {
		return types.Datum{}, err
	}
	return newExpr.Eval(chunk.Row{})
}

// rewriteAstExpr rewrites ast expression directly.
func rewriteAstExpr(sctx sessionctx.Context, expr ast.ExprNode, schema *expression.Schema, names types.NameSlice) (expression.Expression, error) {
	var is infoschema.InfoSchema
	if sctx.GetSessionVars().TxnCtx.InfoSchema != nil {
		is = sctx.GetSessionVars().TxnCtx.InfoSchema.(infoschema.InfoSchema)
	}
	b := NewPlanBuilder(sctx, is, &hint.BlockHintProcessor{})
	fakePlan := LogicalTableDual{}.Init(sctx, 0)
	if schema != nil {
		fakePlan.schema = schema
		fakePlan.names = names
	}
	newExpr, _, err := b.rewrite(context.TODO(), expr, fakePlan, nil, true)
	if err != nil {
		return nil, err
	}
	return newExpr, nil
}

func (b *PlanBuilder) rewriteInsertOnDuplicateUpdate(ctx context.Context, exprNode ast.ExprNode, mockPlan LogicalPlan, insertPlan *Insert) (expression.Expression, error) {
	b.rewriterCounter++
	defer func() { b.rewriterCounter-- }()

	rewriter := b.getExpressionRewriter(ctx, mockPlan)
	// The rewriter maybe is obtained from "b.rewriterPool", "rewriter.err" is
	// not nil means certain previous procedure has not handled this error.
	// Here we give us one more chance to make a correct behavior by handling
	// this missed error.
	if rewriter.err != nil {
		return nil, rewriter.err
	}

	rewriter.insertPlan = insertPlan
	rewriter.asScalar = true

	expr, _, err := b.rewriteExprNode(rewriter, exprNode, true)
	return expr, err
}

// rewrite function rewrites ast expr to expression.Expression.
// aggMapper maps ast.AggregateFuncExpr to the columns offset in p's output schema.
// asScalar means whether this expression must be treated as a scalar expression.
// And this function returns a result expression, a new plan that may have apply or semi-join.
func (b *PlanBuilder) rewrite(ctx context.Context, exprNode ast.ExprNode, p LogicalPlan, aggMapper map[*ast.AggregateFuncExpr]int, asScalar bool) (expression.Expression, LogicalPlan, error) {
	expr, resultPlan, err := b.rewriteWithPreprocess(ctx, exprNode, p, aggMapper, nil, asScalar, nil)
	return expr, resultPlan, err
}

// rewriteWithPreprocess is for handling the situation that we need to adjust the input ast tree
// before really using its node in `expressionRewriter.Leave`. In that case, we first call
// er.preprocess(expr), which returns a new expr. Then we use the new expr in `Leave`.
func (b *PlanBuilder) rewriteWithPreprocess(
	ctx context.Context,
	exprNode ast.ExprNode,
	p LogicalPlan, aggMapper map[*ast.AggregateFuncExpr]int,
	windowMapper map[*ast.WindowFuncExpr]int,
	asScalar bool,
	preprocess func(ast.Node) ast.Node,
) (expression.Expression, LogicalPlan, error) {
	b.rewriterCounter++
	defer func() { b.rewriterCounter-- }()

	rewriter := b.getExpressionRewriter(ctx, p)
	// The rewriter maybe is obtained from "b.rewriterPool", "rewriter.err" is
	// not nil means certain previous procedure has not handled this error.
	// Here we give us one more chance to make a correct behavior by handling
	// this missed error.
	if rewriter.err != nil {
		return nil, nil, rewriter.err
	}

	rewriter.aggrMap = aggMapper
	rewriter.windowMap = windowMapper
	rewriter.asScalar = asScalar
	rewriter.preprocess = preprocess

	expr, resultPlan, err := b.rewriteExprNode(rewriter, exprNode, asScalar)
	return expr, resultPlan, err
}

func (b *PlanBuilder) getExpressionRewriter(ctx context.Context, p LogicalPlan) (rewriter *expressionRewriter) {
	defer func() {
		if p != nil {
			rewriter.schema = p.Schema()
			rewriter.names = p.OutputNames()
		}
	}()

	if len(b.rewriterPool) < b.rewriterCounter {
		rewriter = &expressionRewriter{p: p, b: b, sctx: b.ctx, ctx: ctx}
		b.rewriterPool = append(b.rewriterPool, rewriter)
		return
	}

	rewriter = b.rewriterPool[b.rewriterCounter-1]
	rewriter.p = p
	rewriter.asScalar = false
	rewriter.aggrMap = nil
	rewriter.preprocess = nil
	rewriter.insertPlan = nil
	rewriter.disableFoldCounter = 0
	rewriter.ctxStack = rewriter.ctxStack[:0]
	rewriter.ctxNameStk = rewriter.ctxNameStk[:0]
	rewriter.ctx = ctx
	return
}

func (b *PlanBuilder) rewriteExprNode(rewriter *expressionRewriter, exprNode ast.ExprNode, asScalar bool) (expression.Expression, LogicalPlan, error) {
	if rewriter.p != nil {
		curColLen := rewriter.p.Schema().Len()
		defer func() {
			names := rewriter.p.OutputNames().Shallow()[:curColLen]
			for i := curColLen; i < rewriter.p.Schema().Len(); i++ {
				names = append(names, types.EmptyName)
			}
			// After rewriting finished, only old columns are visible.
			// e.g. select * from t where t.a in (select t1.a from t1);
			// The output columns before we enter the subquery are the columns from t.
			// But when we leave the subquery `t.a in (select t1.a from t1)`, we got a Apply operator
			// and the output columns become [t.*, t1.*]. But t1.* is used only inside the subquery. If there's another filter
			// which is also a subquery where t1 is involved. The name resolving will fail if we still expose the column from
			// the previous subquery.
			// So here we just reset the names to empty to avoid this situation.
			// TODO: implement ScalarSubQuery and resolve it during optimizing. In building phase, we will not change the plan's structure.
			rewriter.p.SetOutputNames(names)
		}()
	}
	exprNode.Accept(rewriter)
	if rewriter.err != nil {
		return nil, nil, errors.Trace(rewriter.err)
	}
	if !asScalar && len(rewriter.ctxStack) == 0 {
		return nil, rewriter.p, nil
	}
	if len(rewriter.ctxStack) != 1 {
		return nil, nil, errors.Errorf("context len %v is invalid", len(rewriter.ctxStack))
	}
	rewriter.err = expression.CheckArgsNotMultiColumnRow(rewriter.ctxStack[0])
	if rewriter.err != nil {
		return nil, nil, errors.Trace(rewriter.err)
	}
	return rewriter.ctxStack[0], rewriter.p, nil
}

type expressionRewriter struct {
	ctxStack   []expression.Expression
	ctxNameStk []*types.FieldName
	p          LogicalPlan
	schema     *expression.Schema
	names      []*types.FieldName
	err        error
	aggrMap    map[*ast.AggregateFuncExpr]int
	windowMap  map[*ast.WindowFuncExpr]int
	b          *PlanBuilder
	sctx       sessionctx.Context
	ctx        context.Context

	// asScalar indicates the return value must be a scalar value.
	// NOTE: This value can be changed during expression rewritten.
	asScalar bool

	// preprocess is called for every ast.Node in Leave.
	preprocess func(ast.Node) ast.Node

	// insertPlan is only used to rewrite the expressions inside the assignment
	// of the "INSERT" statement.
	insertPlan *Insert

	// disableFoldCounter controls fold-disabled scope. If > 0, rewriter will NOT do constant folding.
	// Typically, during visiting AST, while entering the scope(disable), the counter will +1; while
	// leaving the scope(enable again), the counter will -1.
	// NOTE: This value can be changed during expression rewritten.
	disableFoldCounter int
}

func (er *expressionRewriter) ctxStackLen() int {
	return len(er.ctxStack)
}

func (er *expressionRewriter) ctxStackPop(num int) {
	l := er.ctxStackLen()
	er.ctxStack = er.ctxStack[:l-num]
	er.ctxNameStk = er.ctxNameStk[:l-num]
}

func (er *expressionRewriter) ctxStackAppend(col expression.Expression, name *types.FieldName) {
	er.ctxStack = append(er.ctxStack, col)
	er.ctxNameStk = append(er.ctxNameStk, name)
}

// constructBinaryOpFunction converts binary operator functions
// 1. If op are EQ or NE or NullEQ, constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to (a0 op b0) and (a1 op b1) and (a2 op b2)
// 2. Else constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to
// `IF( a0 NE b0, a0 op b0,
// 		IF ( isNull(a0 NE b0), Null,
// 			IF ( a1 NE b1, a1 op b1,
// 				IF ( isNull(a1 NE b1), Null, a2 op b2))))`
func (er *expressionRewriter) constructBinaryOpFunction(l expression.Expression, r expression.Expression, op string) (expression.Expression, error) {
	lLen, rLen := expression.GetRowLen(l), expression.GetRowLen(r)
	if lLen == 1 && rLen == 1 {
		return er.newFunction(op, types.NewFieldType(mysql.TypeTiny), l, r)
	} else if rLen != lLen {
		return nil, expression.ErrOperandColumns.GenWithStackByArgs(lLen)
	}
	switch op {
	case ast.EQ, ast.NE, ast.NullEQ:
		funcs := make([]expression.Expression, lLen)
		for i := 0; i < lLen; i++ {
			var err error
			funcs[i], err = er.constructBinaryOpFunction(expression.GetFuncArg(l, i), expression.GetFuncArg(r, i), op)
			if err != nil {
				return nil, err
			}
		}
		if op == ast.NE {
			return expression.ComposeDNFCondition(er.sctx, funcs...), nil
		}
		return expression.ComposeCNFCondition(er.sctx, funcs...), nil
	default:
		larg0, rarg0 := expression.GetFuncArg(l, 0), expression.GetFuncArg(r, 0)
		var expr1, expr2, expr3, expr4, expr5 expression.Expression
		expr1 = expression.NewFunctionInternal(er.sctx, ast.NE, types.NewFieldType(mysql.TypeTiny), larg0, rarg0)
		expr2 = expression.NewFunctionInternal(er.sctx, op, types.NewFieldType(mysql.TypeTiny), larg0, rarg0)
		expr3 = expression.NewFunctionInternal(er.sctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), expr1)
		var err error
		l, err = expression.PopRowFirstArg(er.sctx, l)
		if err != nil {
			return nil, err
		}
		r, err = expression.PopRowFirstArg(er.sctx, r)
		if err != nil {
			return nil, err
		}
		expr4, err = er.constructBinaryOpFunction(l, r, op)
		if err != nil {
			return nil, err
		}
		expr5, err = er.newFunction(ast.If, types.NewFieldType(mysql.TypeTiny), expr3, expression.NewNull(), expr4)
		if err != nil {
			return nil, err
		}
		return er.newFunction(ast.If, types.NewFieldType(mysql.TypeTiny), expr1, expr2, expr5)
	}
}

func (er *expressionRewriter) buildSubquery(ctx context.Context, subq *ast.SubqueryExpr) (LogicalPlan, error) {
	if er.schema != nil {
		outerSchema := er.schema.Clone()
		er.b.outerSchemas = append(er.b.outerSchemas, outerSchema)
		er.b.outerNames = append(er.b.outerNames, er.names)
		defer func() {
			er.b.outerSchemas = er.b.outerSchemas[0 : len(er.b.outerSchemas)-1]
			er.b.outerNames = er.b.outerNames[0 : len(er.b.outerNames)-1]
		}()
	}

	np, err := er.b.buildResultSetNode(ctx, subq.Query)
	if err != nil {
		return nil, err
	}
	// Pop the handle map generated by the subquery.
	er.b.handleHelper.popMap()
	return np, nil
}

// Enter implements Visitor interface.
func (er *expressionRewriter) Enter(inNode ast.Node) (ast.Node, bool) {
	switch v := inNode.(type) {
	case *ast.AggregateFuncExpr:
		index, ok := -1, false
		if er.aggrMap != nil {
			index, ok = er.aggrMap[v]
		}
		if !ok {
			er.err = ErrInvalidGroupFuncUse
			return inNode, true
		}
		er.ctxStackAppend(er.schema.Columns[index], er.names[index])
		return inNode, true
	case *ast.ColumnNameExpr:
		if index, ok := er.b.colMapper[v]; ok {
			er.ctxStackAppend(er.schema.Columns[index], er.names[index])
			return inNode, true
		}
	case *ast.CompareSubqueryExpr:
		return er.handleCompareSubquery(er.ctx, v)
	case *ast.ExistsSubqueryExpr:
		return er.handleExistSubquery(er.ctx, v)
	case *ast.PatternInExpr:
		if v.Sel != nil {
			return er.handleInSubquery(er.ctx, v)
		}
		if len(v.List) != 1 {
			break
		}
		// For 10 in ((select * from t)), the parser won't set v.Sel.
		// So we must process this case here.
		x := v.List[0]
		for {
			switch y := x.(type) {
			case *ast.SubqueryExpr:
				v.Sel = y
				return er.handleInSubquery(er.ctx, v)
			case *ast.ParenthesesExpr:
				x = y.Expr
			default:
				return inNode, false
			}
		}
	case *ast.SubqueryExpr:
		return er.handleScalarSubquery(er.ctx, v)
	case *ast.ParenthesesExpr:
	case *ast.ValuesExpr:
		schema, names := er.schema, er.names
		// NOTE: "er.insertPlan != nil" means that we are rewriting the
		// expressions inside the assignment of "INSERT" statement. we have to
		// use the "tableSchema" of that "insertPlan".
		if er.insertPlan != nil {
			schema = er.insertPlan.tableSchema
			names = er.insertPlan.tableColNames
		}
		idx, err := expression.FindFieldName(names, v.Column.Name)
		if err != nil {
			er.err = err
			return inNode, false
		}
		if idx < 0 {
			er.err = ErrUnknownColumn.GenWithStackByArgs(v.Column.Name.OrigColName(), "field list")
			return inNode, false
		}
		col := schema.Columns[idx]
		er.ctxStackAppend(expression.NewValuesFunc(er.sctx, col.Index, col.RetType), types.EmptyName)
		return inNode, true
	case *ast.WindowFuncExpr:
		index, ok := -1, false
		if er.windowMap != nil {
			index, ok = er.windowMap[v]
		}
		if !ok {
			er.err = ErrWindowInvalidWindowFuncUse.GenWithStackByArgs(strings.ToLower(v.F))
			return inNode, true
		}
		er.ctxStackAppend(er.schema.Columns[index], er.names[index])
		return inNode, true
	case *ast.FuncCallExpr:
		if _, ok := expression.DisableFoldFunctions[v.FnName.L]; ok {
			er.disableFoldCounter++
		}
	case *ast.SetCollationExpr:
		// Do nothing
	default:
		er.asScalar = true
	}
	return inNode, false
}

func (er *expressionRewriter) buildSemiApplyFromEqualSubq(np LogicalPlan, l, r expression.Expression, not bool) {
	var condition expression.Expression
	if rCol, ok := r.(*expression.Column); ok && (er.asScalar || not) {
		rCol.InOperand = true
		// If both input columns of `!= all / = any` expression are not null, we can treat the expression
		// as normal column equal condition.
		if lCol, ok := l.(*expression.Column); ok && mysql.HasNotNullFlag(lCol.GetType().Flag) && mysql.HasNotNullFlag(rCol.GetType().Flag) {
			rCol.InOperand = false
		}
	}
	condition, er.err = er.constructBinaryOpFunction(l, r, ast.EQ)
	if er.err != nil {
		return
	}
	er.p, er.err = er.b.buildSemiApply(er.p, np, []expression.Expression{condition}, er.asScalar, not)
}

func (er *expressionRewriter) handleCompareSubquery(ctx context.Context, v *ast.CompareSubqueryExpr) (ast.Node, bool) {
	v.L.Accept(er)
	if er.err != nil {
		return v, true
	}
	lexpr := er.ctxStack[len(er.ctxStack)-1]
	subq, ok := v.R.(*ast.SubqueryExpr)
	if !ok {
		er.err = errors.Errorf("Unknown compare type %T.", v.R)
		return v, true
	}
	np, err := er.buildSubquery(ctx, subq)
	if err != nil {
		er.err = err
		return v, true
	}
	// Only (a,b,c) = any (...) and (a,b,c) != all (...) can use row expression.
	canMultiCol := (!v.All && v.Op == opcode.EQ) || (v.All && v.Op == opcode.NE)
	if !canMultiCol && (expression.GetRowLen(lexpr) != 1 || np.Schema().Len() != 1) {
		er.err = expression.ErrOperandColumns.GenWithStackByArgs(1)
		return v, true
	}
	lLen := expression.GetRowLen(lexpr)
	if lLen != np.Schema().Len() {
		er.err = expression.ErrOperandColumns.GenWithStackByArgs(lLen)
		return v, true
	}
	var rexpr expression.Expression
	if np.Schema().Len() == 1 {
		rexpr = np.Schema().Columns[0]
	} else {
		args := make([]expression.Expression, 0, np.Schema().Len())
		for _, col := range np.Schema().Columns {
			args = append(args, col)
		}
		rexpr, er.err = er.newFunction(ast.RowFunc, args[0].GetType(), args...)
		if er.err != nil {
			return v, true
		}
	}
	switch v.Op {
	// Only EQ, NE and NullEQ can be composed with and.
	case opcode.EQ, opcode.NE, opcode.NullEQ:
		if v.Op == opcode.EQ {
			if v.All {
				er.handleEQAll(lexpr, rexpr, np)
			} else {
				// `a = any(subq)` will be rewriten as `a in (subq)`.
				er.buildSemiApplyFromEqualSubq(np, lexpr, rexpr, false)
				if er.err != nil {
					return v, true
				}
			}
		} else if v.Op == opcode.NE {
			if v.All {
				// `a != all(subq)` will be rewriten as `a not in (subq)`.
				er.buildSemiApplyFromEqualSubq(np, lexpr, rexpr, true)
				if er.err != nil {
					return v, true
				}
			} else {
				er.handleNEAny(lexpr, rexpr, np)
			}
		} else {
			// TODO: Support this in future.
			er.err = errors.New("We don't support <=> all or <=> any now")
			return v, true
		}
	default:
		// When < all or > any , the agg function should use min.
		useMin := ((v.Op == opcode.LT || v.Op == opcode.LE) && v.All) || ((v.Op == opcode.GT || v.Op == opcode.GE) && !v.All)
		er.handleOtherComparableSubq(lexpr, rexpr, np, useMin, v.Op.String(), v.All)
	}
	if er.asScalar {
		// The parent expression only use the last column in schema, which represents whether the condition is matched.
		er.ctxStack[len(er.ctxStack)-1] = er.p.Schema().Columns[er.p.Schema().Len()-1]
		er.ctxNameStk[len(er.ctxNameStk)-1] = er.p.OutputNames()[er.p.Schema().Len()-1]
	}
	return v, true
}

// handleOtherComparableSubq handles the queries like < any, < max, etc. For example, if the query is t.id < any (select s.id from s),
// it will be rewrote to t.id < (select max(s.id) from s).
func (er *expressionRewriter) handleOtherComparableSubq(lexpr, rexpr expression.Expression, np LogicalPlan, useMin bool, cmpFunc string, all bool) {
	plan4Agg := LogicalAggregation{}.Init(er.sctx, er.b.getSelectOffset())
	if hint := er.b.TableHints(); hint != nil {
		plan4Agg.aggHints = hint.aggHints
	}
	plan4Agg.SetChildren(np)

	// Create a "max" or "min" aggregation.
	funcName := ast.AggFuncMax
	if useMin {
		funcName = ast.AggFuncMin
	}
	funcMaxOrMin, err := aggregation.NewAggFuncDesc(er.sctx, funcName, []expression.Expression{rexpr}, false)
	if err != nil {
		er.err = err
		return
	}

	// Create a column and append it to the schema of that aggregation.
	colMaxOrMin := &expression.Column{
		UniqueID: er.sctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  funcMaxOrMin.RetTp,
	}
	schema := expression.NewSchema(colMaxOrMin)

	plan4Agg.names = append(plan4Agg.names, types.EmptyName)
	plan4Agg.SetSchema(schema)
	plan4Agg.AggFuncs = []*aggregation.AggFuncDesc{funcMaxOrMin}

	cond := expression.NewFunctionInternal(er.sctx, cmpFunc, types.NewFieldType(mysql.TypeTiny), lexpr, colMaxOrMin)
	er.buildQuantifierPlan(plan4Agg, cond, lexpr, rexpr, all)
}

// buildQuantifierPlan adds extra condition for any / all subquery.
func (er *expressionRewriter) buildQuantifierPlan(plan4Agg *LogicalAggregation, cond, lexpr, rexpr expression.Expression, all bool) {
	innerIsNull := expression.NewFunctionInternal(er.sctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), rexpr)
	outerIsNull := expression.NewFunctionInternal(er.sctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), lexpr)

	funcSum, err := aggregation.NewAggFuncDesc(er.sctx, ast.AggFuncSum, []expression.Expression{innerIsNull}, false)
	if err != nil {
		er.err = err
		return
	}
	colSum := &expression.Column{
		UniqueID: er.sctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  funcSum.RetTp,
	}
	plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, funcSum)
	plan4Agg.schema.Append(colSum)
	innerHasNull := expression.NewFunctionInternal(er.sctx, ast.NE, types.NewFieldType(mysql.TypeTiny), colSum, expression.NewZero())

	// Build `count(1)` aggregation to check if subquery is empty.
	funcCount, err := aggregation.NewAggFuncDesc(er.sctx, ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	if err != nil {
		er.err = err
		return
	}
	colCount := &expression.Column{
		UniqueID: er.sctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  funcCount.RetTp,
	}
	plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, funcCount)
	plan4Agg.schema.Append(colCount)

	if all {
		// All of the inner record set should not contain null value. So for t.id < all(select s.id from s), it
		// should be rewrote to t.id < min(s.id) and if(sum(s.id is null) != 0, null, true).
		innerNullChecker := expression.NewFunctionInternal(er.sctx, ast.If, types.NewFieldType(mysql.TypeTiny), innerHasNull, expression.NewNull(), expression.NewOne())
		cond = expression.ComposeCNFCondition(er.sctx, cond, innerNullChecker)
		// If the subquery is empty, it should always return true.
		emptyChecker := expression.NewFunctionInternal(er.sctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), colCount, expression.NewZero())
		// If outer key is null, and subquery is not empty, it should always return null, even when it is `null = all (1, 2)`.
		outerNullChecker := expression.NewFunctionInternal(er.sctx, ast.If, types.NewFieldType(mysql.TypeTiny), outerIsNull, expression.NewNull(), expression.NewZero())
		cond = expression.ComposeDNFCondition(er.sctx, cond, emptyChecker, outerNullChecker)
	} else {
		// For "any" expression, if the subquery has null and the cond returns false, the result should be NULL.
		// Specifically, `t.id < any (select s.id from s)` would be rewrote to `t.id < max(s.id) or if(sum(s.id is null) != 0, null, false)`
		innerNullChecker := expression.NewFunctionInternal(er.sctx, ast.If, types.NewFieldType(mysql.TypeTiny), innerHasNull, expression.NewNull(), expression.NewZero())
		cond = expression.ComposeDNFCondition(er.sctx, cond, innerNullChecker)
		// If the subquery is empty, it should always return false.
		emptyChecker := expression.NewFunctionInternal(er.sctx, ast.NE, types.NewFieldType(mysql.TypeTiny), colCount, expression.NewZero())
		// If outer key is null, and subquery is not empty, it should return null.
		outerNullChecker := expression.NewFunctionInternal(er.sctx, ast.If, types.NewFieldType(mysql.TypeTiny), outerIsNull, expression.NewNull(), expression.NewOne())
		cond = expression.ComposeCNFCondition(er.sctx, cond, emptyChecker, outerNullChecker)
	}

	// TODO: Add a Projection if any argument of aggregate funcs or group by items are scalar functions.
	// plan4Agg.buildProjectionIfNecessary()
	if !er.asScalar {
		// For Semi LogicalApply without aux column, the result is no matter false or null. So we can add it to join predicate.
		er.p, er.err = er.b.buildSemiApply(er.p, plan4Agg, []expression.Expression{cond}, false, false)
		return
	}
	// If we treat the result as a scalar value, we will add a projection with a extra column to output true, false or null.
	outerSchemaLen := er.p.Schema().Len()
	er.p = er.b.buildApplyWithJoinType(er.p, plan4Agg, InnerJoin)
	joinSchema := er.p.Schema()
	proj := LogicalProjection{
		Exprs: expression.Column2Exprs(joinSchema.Clone().Columns[:outerSchemaLen]),
	}.Init(er.sctx, er.b.getSelectOffset())
	proj.names = make([]*types.FieldName, outerSchemaLen, outerSchemaLen+1)
	copy(proj.names, er.p.OutputNames())
	proj.SetSchema(expression.NewSchema(joinSchema.Clone().Columns[:outerSchemaLen]...))
	proj.Exprs = append(proj.Exprs, cond)
	proj.schema.Append(&expression.Column{
		UniqueID: er.sctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  cond.GetType(),
	})
	proj.names = append(proj.names, types.EmptyName)
	proj.SetChildren(er.p)
	er.p = proj
}

// handleNEAny handles the case of != any. For example, if the query is t.id != any (select s.id from s), it will be rewrote to
// t.id != s.id or count(distinct s.id) > 1 or [any checker]. If there are two different values in s.id ,
// there must exist a s.id that doesn't equal to t.id.
func (er *expressionRewriter) handleNEAny(lexpr, rexpr expression.Expression, np LogicalPlan) {
	firstRowFunc, err := aggregation.NewAggFuncDesc(er.sctx, ast.AggFuncFirstRow, []expression.Expression{rexpr}, false)
	if err != nil {
		er.err = err
		return
	}
	countFunc, err := aggregation.NewAggFuncDesc(er.sctx, ast.AggFuncCount, []expression.Expression{rexpr}, true)
	if err != nil {
		er.err = err
		return
	}
	plan4Agg := LogicalAggregation{
		AggFuncs: []*aggregation.AggFuncDesc{firstRowFunc, countFunc},
	}.Init(er.sctx, er.b.getSelectOffset())
	if hint := er.b.TableHints(); hint != nil {
		plan4Agg.aggHints = hint.aggHints
	}
	plan4Agg.SetChildren(np)
	firstRowResultCol := &expression.Column{
		UniqueID: er.sctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  firstRowFunc.RetTp,
	}
	count := &expression.Column{
		UniqueID: er.sctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  countFunc.RetTp,
	}
	plan4Agg.names = append(plan4Agg.names, types.EmptyName, types.EmptyName)
	plan4Agg.SetSchema(expression.NewSchema(firstRowResultCol, count))
	gtFunc := expression.NewFunctionInternal(er.sctx, ast.GT, types.NewFieldType(mysql.TypeTiny), count, expression.NewOne())
	neCond := expression.NewFunctionInternal(er.sctx, ast.NE, types.NewFieldType(mysql.TypeTiny), lexpr, firstRowResultCol)
	cond := expression.ComposeDNFCondition(er.sctx, gtFunc, neCond)
	er.buildQuantifierPlan(plan4Agg, cond, lexpr, rexpr, false)
}

// handleEQAll handles the case of = all. For example, if the query is t.id = all (select s.id from s), it will be rewrote to
// t.id = (select s.id from s having count(distinct s.id) <= 1 and [all checker]).
func (er *expressionRewriter) handleEQAll(lexpr, rexpr expression.Expression, np LogicalPlan) {
	firstRowFunc, err := aggregation.NewAggFuncDesc(er.sctx, ast.AggFuncFirstRow, []expression.Expression{rexpr}, false)
	if err != nil {
		er.err = err
		return
	}
	countFunc, err := aggregation.NewAggFuncDesc(er.sctx, ast.AggFuncCount, []expression.Expression{rexpr}, true)
	if err != nil {
		er.err = err
		return
	}
	plan4Agg := LogicalAggregation{
		AggFuncs: []*aggregation.AggFuncDesc{firstRowFunc, countFunc},
	}.Init(er.sctx, er.b.getSelectOffset())
	if hint := er.b.TableHints(); hint != nil {
		plan4Agg.aggHints = hint.aggHints
	}
	plan4Agg.SetChildren(np)
	plan4Agg.names = append(plan4Agg.names, types.EmptyName)
	firstRowResultCol := &expression.Column{
		UniqueID: er.sctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  firstRowFunc.RetTp,
	}
	plan4Agg.names = append(plan4Agg.names, types.EmptyName)
	count := &expression.Column{
		UniqueID: er.sctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  countFunc.RetTp,
	}
	plan4Agg.SetSchema(expression.NewSchema(firstRowResultCol, count))
	leFunc := expression.NewFunctionInternal(er.sctx, ast.LE, types.NewFieldType(mysql.TypeTiny), count, expression.NewOne())
	eqCond := expression.NewFunctionInternal(er.sctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), lexpr, firstRowResultCol)
	cond := expression.ComposeCNFCondition(er.sctx, leFunc, eqCond)
	er.buildQuantifierPlan(plan4Agg, cond, lexpr, rexpr, true)
}

func (er *expressionRewriter) handleExistSubquery(ctx context.Context, v *ast.ExistsSubqueryExpr) (ast.Node, bool) {
	subq, ok := v.Sel.(*ast.SubqueryExpr)
	if !ok {
		er.err = errors.Errorf("Unknown exists type %T.", v.Sel)
		return v, true
	}
	np, err := er.buildSubquery(ctx, subq)
	if err != nil {
		er.err = err
		return v, true
	}
	np = er.popExistsSubPlan(np)
	if len(ExtractCorrelatedCols4LogicalPlan(np)) > 0 {
		er.p, er.err = er.b.buildSemiApply(er.p, np, nil, er.asScalar, v.Not)
		if er.err != nil || !er.asScalar {
			return v, true
		}
		er.ctxStackAppend(er.p.Schema().Columns[er.p.Schema().Len()-1], er.p.OutputNames()[er.p.Schema().Len()-1])
	} else {
		physicalPlan, _, err := DoOptimize(ctx, er.sctx, er.b.optFlag, np)
		if err != nil {
			er.err = err
			return v, true
		}
		row, err := EvalSubqueryFirstRow(ctx, physicalPlan, er.b.is, er.b.ctx)
		if err != nil {
			er.err = err
			return v, true
		}
		if (row != nil && !v.Not) || (row == nil && v.Not) {
			er.ctxStackAppend(expression.NewOne(), types.EmptyName)
		} else {
			er.ctxStackAppend(expression.NewZero(), types.EmptyName)
		}
	}
	return v, true
}

// popExistsSubPlan will remove the useless plan in exist's child.
// See comments inside the method for more details.
func (er *expressionRewriter) popExistsSubPlan(p LogicalPlan) LogicalPlan {
out:
	for {
		switch plan := p.(type) {
		// This can be removed when in exists clause,
		// e.g. exists(select count(*) from t order by a) is equal to exists t.
		case *LogicalProjection, *LogicalSort:
			p = p.Children()[0]
		case *LogicalAggregation:
			if len(plan.GroupByItems) == 0 {
				p = LogicalTableDual{RowCount: 1}.Init(er.sctx, er.b.getSelectOffset())
				break out
			}
			p = p.Children()[0]
		default:
			break out
		}
	}
	return p
}

func (er *expressionRewriter) handleInSubquery(ctx context.Context, v *ast.PatternInExpr) (ast.Node, bool) {
	asScalar := er.asScalar
	er.asScalar = true
	v.Expr.Accept(er)
	if er.err != nil {
		return v, true
	}
	lexpr := er.ctxStack[len(er.ctxStack)-1]
	subq, ok := v.Sel.(*ast.SubqueryExpr)
	if !ok {
		er.err = errors.Errorf("Unknown compare type %T.", v.Sel)
		return v, true
	}
	np, err := er.buildSubquery(ctx, subq)
	if err != nil {
		er.err = err
		return v, true
	}
	lLen := expression.GetRowLen(lexpr)
	if lLen != np.Schema().Len() {
		er.err = expression.ErrOperandColumns.GenWithStackByArgs(lLen)
		return v, true
	}
	var rexpr expression.Expression
	if np.Schema().Len() == 1 {
		rexpr = np.Schema().Columns[0]
		rCol := rexpr.(*expression.Column)
		// For AntiSemiJoin/LeftOuterSemiJoin/AntiLeftOuterSemiJoin, we cannot treat `in` expression as
		// normal column equal condition, so we specially mark the inner operand here.
		if v.Not || asScalar {
			rCol.InOperand = true
			// If both input columns of `in` expression are not null, we can treat the expression
			// as normal column equal condition instead.
			lCol, ok := lexpr.(*expression.Column)
			if ok && mysql.HasNotNullFlag(lCol.GetType().Flag) && mysql.HasNotNullFlag(rCol.GetType().Flag) {
				rCol.InOperand = false
			}
		}
	} else {
		args := make([]expression.Expression, 0, np.Schema().Len())
		for _, col := range np.Schema().Columns {
			args = append(args, col)
		}
		rexpr, er.err = er.newFunction(ast.RowFunc, args[0].GetType(), args...)
		if er.err != nil {
			return v, true
		}
	}
	checkCondition, err := er.constructBinaryOpFunction(lexpr, rexpr, ast.EQ)
	if err != nil {
		er.err = err
		return v, true
	}

	// If the leftKey and the rightKey have different collations, don't convert the sub-query to an inner-join
	// since when converting we will add a distinct-agg upon the right child and this distinct-agg doesn't have the right collation.
	// To keep it simple, we forbid this converting if they have different collations.
	lt, rt := lexpr.GetType(), rexpr.GetType()
	collFlag := collate.CompatibleCollate(lt.Collate, rt.Collate)

	// If it's not the form of `not in (SUBQUERY)`,
	// and has no correlated column from the current level plan(if the correlated column is from upper level,
	// we can treat it as constant, because the upper LogicalApply cannot be eliminated since current node is a join node),
	// and don't need to append a scalar value, we can rewrite it to inner join.
	if er.sctx.GetSessionVars().GetAllowInSubqToJoinAndAgg() && !v.Not && !asScalar && len(extractCorColumnsBySchema4LogicalPlan(np, er.p.Schema())) == 0 && collFlag {
		// We need to try to eliminate the agg and the projection produced by this operation.
		er.b.optFlag |= flagEliminateAgg
		er.b.optFlag |= flagEliminateProjection
		er.b.optFlag |= flagJoinReOrder
		// Build distinct for the inner query.
		agg, err := er.b.buildDistinct(np, np.Schema().Len())
		if err != nil {
			er.err = err
			return v, true
		}
		// Build inner join above the aggregation.
		join := LogicalJoin{JoinType: InnerJoin}.Init(er.sctx, er.b.getSelectOffset())
		join.SetChildren(er.p, agg)
		join.SetSchema(expression.MergeSchema(er.p.Schema(), agg.schema))
		join.names = make([]*types.FieldName, er.p.Schema().Len()+agg.Schema().Len())
		copy(join.names, er.p.OutputNames())
		copy(join.names[er.p.Schema().Len():], agg.OutputNames())
		join.AttachOnConds(expression.SplitCNFItems(checkCondition))
		// Set join hint for this join.
		if er.b.TableHints() != nil {
			join.setPreferredJoinType(er.b.TableHints())
		}
		er.p = join
	} else {
		er.p, er.err = er.b.buildSemiApply(er.p, np, expression.SplitCNFItems(checkCondition), asScalar, v.Not)
		if er.err != nil {
			return v, true
		}
	}

	er.ctxStackPop(1)
	if asScalar {
		col := er.p.Schema().Columns[er.p.Schema().Len()-1]
		er.ctxStackAppend(col, er.p.OutputNames()[er.p.Schema().Len()-1])
	}
	return v, true
}

func (er *expressionRewriter) handleScalarSubquery(ctx context.Context, v *ast.SubqueryExpr) (ast.Node, bool) {
	np, err := er.buildSubquery(ctx, v)
	if err != nil {
		er.err = err
		return v, true
	}
	np = er.b.buildMaxOneRow(np)
	if len(ExtractCorrelatedCols4LogicalPlan(np)) > 0 {
		er.p = er.b.buildApplyWithJoinType(er.p, np, LeftOuterJoin)
		if np.Schema().Len() > 1 {
			newCols := make([]expression.Expression, 0, np.Schema().Len())
			for _, col := range np.Schema().Columns {
				newCols = append(newCols, col)
			}
			expr, err1 := er.newFunction(ast.RowFunc, newCols[0].GetType(), newCols...)
			if err1 != nil {
				er.err = err1
				return v, true
			}
			er.ctxStackAppend(expr, types.EmptyName)
		} else {
			er.ctxStackAppend(er.p.Schema().Columns[er.p.Schema().Len()-1], er.p.OutputNames()[er.p.Schema().Len()-1])
		}
		return v, true
	}
	physicalPlan, _, err := DoOptimize(ctx, er.sctx, er.b.optFlag, np)
	if err != nil {
		er.err = err
		return v, true
	}
	row, err := EvalSubqueryFirstRow(ctx, physicalPlan, er.b.is, er.b.ctx)
	if err != nil {
		er.err = err
		return v, true
	}
	if np.Schema().Len() > 1 {
		newCols := make([]expression.Expression, 0, np.Schema().Len())
		for i, data := range row {
			newCols = append(newCols, &expression.Constant{
				Value:   data,
				RetType: np.Schema().Columns[i].GetType()})
		}
		expr, err1 := er.newFunction(ast.RowFunc, newCols[0].GetType(), newCols...)
		if err1 != nil {
			er.err = err1
			return v, true
		}
		er.ctxStackAppend(expr, types.EmptyName)
	} else {
		er.ctxStackAppend(&expression.Constant{
			Value:   row[0],
			RetType: np.Schema().Columns[0].GetType(),
		}, types.EmptyName)
	}
	return v, true
}

// Leave implements Visitor interface.
func (er *expressionRewriter) Leave(originInNode ast.Node) (retNode ast.Node, ok bool) {
	if er.err != nil {
		return retNode, false
	}
	var inNode = originInNode
	if er.preprocess != nil {
		inNode = er.preprocess(inNode)
	}
	switch v := inNode.(type) {
	case *ast.AggregateFuncExpr, *ast.ColumnNameExpr, *ast.ParenthesesExpr, *ast.WhenClause,
		*ast.SubqueryExpr, *ast.ExistsSubqueryExpr, *ast.CompareSubqueryExpr, *ast.ValuesExpr, *ast.WindowFuncExpr, *ast.TableNameExpr:
	case *driver.ValueExpr:
		v.Datum.SetValue(v.Datum.GetValue(), &v.Type)
		value := &expression.Constant{Value: v.Datum, RetType: &v.Type}
		er.ctxStackAppend(value, types.EmptyName)
	case *driver.ParamMarkerExpr:
		var value expression.Expression
		value, er.err = expression.ParamMarkerExpression(er.sctx, v)
		if er.err != nil {
			return retNode, false
		}
		er.ctxStackAppend(value, types.EmptyName)
	case *ast.VariableExpr:
		er.rewriteVariable(v)
	case *ast.FuncCallExpr:
		er.funcCallToExpression(v)
		if _, ok := expression.DisableFoldFunctions[v.FnName.L]; ok {
			er.disableFoldCounter--
		}
	case *ast.TableName:
		er.toTable(v)
	case *ast.ColumnName:
		er.toColumn(v)
	case *ast.UnaryOperationExpr:
		er.unaryOpToExpression(v)
	case *ast.BinaryOperationExpr:
		er.binaryOpToExpression(v)
	case *ast.BetweenExpr:
		er.betweenToExpression(v)
	case *ast.CaseExpr:
		er.caseToExpression(v)
	case *ast.FuncCastExpr:
		arg := er.ctxStack[len(er.ctxStack)-1]
		er.err = expression.CheckArgsNotMultiColumnRow(arg)
		if er.err != nil {
			return retNode, false
		}

		// check the decimal precision of "CAST(AS TIME)".
		er.err = er.checkTimePrecision(v.Tp)
		if er.err != nil {
			return retNode, false
		}

		er.ctxStack[len(er.ctxStack)-1] = expression.BuildCastFunction(er.sctx, arg, v.Tp)
		er.ctxNameStk[len(er.ctxNameStk)-1] = types.EmptyName
	case *ast.PatternLikeExpr:
		er.patternLikeToExpression(v)
	case *ast.PatternRegexpExpr:
		er.regexpToScalarFunc(v)
	case *ast.RowExpr:
		er.rowToScalarFunc(v)
	case *ast.PatternInExpr:
		if v.Sel == nil {
			er.inToExpression(len(v.List), v.Not, &v.Type)
		}
	case *ast.PositionExpr:
		er.positionToScalarFunc(v)
	case *ast.IsNullExpr:
		er.isNullToExpression(v)
	case *ast.IsTruthExpr:
		er.isTrueToScalarFunc(v)
	case *ast.DefaultExpr:
		er.evalDefaultExpr(v)
	// TODO: Perhaps we don't need to transcode these back to generic integers/strings
	case *ast.TrimDirectionExpr:
		er.ctxStackAppend(&expression.Constant{
			Value:   types.NewIntDatum(int64(v.Direction)),
			RetType: types.NewFieldType(mysql.TypeTiny),
		}, types.EmptyName)
	case *ast.TimeUnitExpr:
		er.ctxStackAppend(&expression.Constant{
			Value:   types.NewStringDatum(v.Unit.String()),
			RetType: types.NewFieldType(mysql.TypeVarchar),
		}, types.EmptyName)
	case *ast.GetFormatSelectorExpr:
		er.ctxStackAppend(&expression.Constant{
			Value:   types.NewStringDatum(v.Selector.String()),
			RetType: types.NewFieldType(mysql.TypeVarchar),
		}, types.EmptyName)
	case *ast.SetCollationExpr:
		arg := er.ctxStack[len(er.ctxStack)-1]
		if collate.NewCollationEnabled() {
			var collInfo *charset.Collation
			// TODO(bb7133): use charset.ValidCharsetAndCollation when its bug is fixed.
			if collInfo, er.err = collate.GetCollationByName(v.Collate); er.err != nil {
				break
			}
			chs := arg.GetType().Charset
			if chs != "" && collInfo.CharsetName != chs {
				er.err = charset.ErrCollationCharsetMismatch.GenWithStackByArgs(collInfo.Name, chs)
				break
			}
		}
		// SetCollationExpr sets the collation explicitly, even when the evaluation type of the expression is non-string.
		if _, ok := arg.(*expression.Column); ok {
			// Wrap a cast here to avoid changing the original FieldType of the column expression.
			exprType := arg.GetType().Clone()
			exprType.Collate = v.Collate
			casted := expression.BuildCastFunction(er.sctx, arg, exprType)
			er.ctxStackPop(1)
			er.ctxStackAppend(casted, types.EmptyName)
		} else {
			// For constant and scalar function, we can set its collate directly.
			arg.GetType().Collate = v.Collate
		}
		er.ctxStack[len(er.ctxStack)-1].SetCoercibility(expression.CoercibilityExplicit)
		er.ctxStack[len(er.ctxStack)-1].SetCharsetAndCollation(arg.GetType().Charset, arg.GetType().Collate)
	default:
		er.err = errors.Errorf("UnknownType: %T", v)
		return retNode, false
	}

	if er.err != nil {
		return retNode, false
	}
	return originInNode, true
}

// newFunction chooses which expression.NewFunctionImpl() will be used.
func (er *expressionRewriter) newFunction(funcName string, retType *types.FieldType, args ...expression.Expression) (expression.Expression, error) {
	if er.disableFoldCounter > 0 {
		return expression.NewFunctionBase(er.sctx, funcName, retType, args...)
	}
	return expression.NewFunction(er.sctx, funcName, retType, args...)
}

func (er *expressionRewriter) checkTimePrecision(ft *types.FieldType) error {
	if ft.EvalType() == types.ETDuration && ft.Decimal > int(types.MaxFsp) {
		return errTooBigPrecision.GenWithStackByArgs(ft.Decimal, "CAST", types.MaxFsp)
	}
	return nil
}

func (er *expressionRewriter) useCache() bool {
	return er.sctx.GetSessionVars().StmtCtx.UseCache
}

func (er *expressionRewriter) rewriteVariable(v *ast.VariableExpr) {
	stkLen := len(er.ctxStack)
	name := strings.ToLower(v.Name)
	sessionVars := er.b.ctx.GetSessionVars()
	if !v.IsSystem {
		if v.Value != nil {
			er.ctxStack[stkLen-1], er.err = er.newFunction(ast.SetVar,
				er.ctxStack[stkLen-1].GetType(),
				expression.DatumToConstant(types.NewDatum(name), mysql.TypeString),
				er.ctxStack[stkLen-1])
			er.ctxNameStk[stkLen-1] = types.EmptyName
			return
		}
		f, err := er.newFunction(ast.GetVar,
			// TODO: Here is wrong, the sessionVars should store a name -> Datum map. Will fix it later.
			types.NewFieldType(mysql.TypeString),
			expression.DatumToConstant(types.NewStringDatum(name), mysql.TypeString))
		if err != nil {
			er.err = err
			return
		}
		f.SetCoercibility(expression.CoercibilityImplicit)
		er.ctxStackAppend(f, types.EmptyName)
		return
	}
	var val string
	var err error
	if v.ExplicitScope {
		err = variable.ValidateGetSystemVar(name, v.IsGlobal)
		if err != nil {
			er.err = err
			return
		}
	}
	sysVar := variable.SysVars[name]
	if sysVar == nil {
		er.err = variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
		return
	}
	// Variable is @@gobal.variable_name or variable is only global scope variable.
	if v.IsGlobal || sysVar.Scope == variable.ScopeGlobal {
		val, err = variable.GetGlobalSystemVar(sessionVars, name)
	} else {
		val, err = variable.GetSessionSystemVar(sessionVars, name)
	}
	if err != nil {
		er.err = err
		return
	}
	e := expression.DatumToConstant(types.NewStringDatum(val), mysql.TypeVarString)
	e.GetType().Charset, _ = er.sctx.GetSessionVars().GetSystemVar(variable.CharacterSetConnection)
	e.GetType().Collate, _ = er.sctx.GetSessionVars().GetSystemVar(variable.CollationConnection)
	er.ctxStackAppend(e, types.EmptyName)
}

func (er *expressionRewriter) unaryOpToExpression(v *ast.UnaryOperationExpr) {
	stkLen := len(er.ctxStack)
	var op string
	switch v.Op {
	case opcode.Plus:
		// expression (+ a) is equal to a
		return
	case opcode.Minus:
		op = ast.UnaryMinus
	case opcode.BitNeg:
		op = ast.BitNeg
	case opcode.Not:
		op = ast.UnaryNot
	default:
		er.err = errors.Errorf("Unknown Unary Op %T", v.Op)
		return
	}
	if expression.GetRowLen(er.ctxStack[stkLen-1]) != 1 {
		er.err = expression.ErrOperandColumns.GenWithStackByArgs(1)
		return
	}
	er.ctxStack[stkLen-1], er.err = er.newFunction(op, &v.Type, er.ctxStack[stkLen-1])
	er.ctxNameStk[stkLen-1] = types.EmptyName
}

func (er *expressionRewriter) binaryOpToExpression(v *ast.BinaryOperationExpr) {
	stkLen := len(er.ctxStack)
	var function expression.Expression
	switch v.Op {
	case opcode.EQ, opcode.NE, opcode.NullEQ, opcode.GT, opcode.GE, opcode.LT, opcode.LE:
		function, er.err = er.constructBinaryOpFunction(er.ctxStack[stkLen-2], er.ctxStack[stkLen-1],
			v.Op.String())
	default:
		lLen := expression.GetRowLen(er.ctxStack[stkLen-2])
		rLen := expression.GetRowLen(er.ctxStack[stkLen-1])
		if lLen != 1 || rLen != 1 {
			er.err = expression.ErrOperandColumns.GenWithStackByArgs(1)
			return
		}
		function, er.err = er.newFunction(v.Op.String(), types.NewFieldType(mysql.TypeUnspecified), er.ctxStack[stkLen-2:]...)
	}
	if er.err != nil {
		return
	}
	er.ctxStackPop(2)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) notToExpression(hasNot bool, op string, tp *types.FieldType,
	args ...expression.Expression) expression.Expression {
	opFunc, err := er.newFunction(op, tp, args...)
	if err != nil {
		er.err = err
		return nil
	}
	if !hasNot {
		return opFunc
	}

	opFunc, err = er.newFunction(ast.UnaryNot, tp, opFunc)
	if err != nil {
		er.err = err
		return nil
	}
	return opFunc
}

func (er *expressionRewriter) isNullToExpression(v *ast.IsNullExpr) {
	stkLen := len(er.ctxStack)
	if expression.GetRowLen(er.ctxStack[stkLen-1]) != 1 {
		er.err = expression.ErrOperandColumns.GenWithStackByArgs(1)
		return
	}
	function := er.notToExpression(v.Not, ast.IsNull, &v.Type, er.ctxStack[stkLen-1])
	er.ctxStackPop(1)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) positionToScalarFunc(v *ast.PositionExpr) {
	pos := v.N
	str := strconv.Itoa(pos)
	if v.P != nil {
		stkLen := len(er.ctxStack)
		val := er.ctxStack[stkLen-1]
		intNum, isNull, err := expression.GetIntFromConstant(er.sctx, val)
		str = "?"
		if err == nil {
			if isNull {
				return
			}
			pos = intNum
			er.ctxStackPop(1)
		}
		er.err = err
	}
	if er.err == nil && pos > 0 && pos <= er.schema.Len() {
		er.ctxStackAppend(er.schema.Columns[pos-1], er.names[pos-1])
	} else {
		er.err = ErrUnknownColumn.GenWithStackByArgs(str, clauseMsg[er.b.curClause])
	}
}

func (er *expressionRewriter) isTrueToScalarFunc(v *ast.IsTruthExpr) {
	stkLen := len(er.ctxStack)
	op := ast.IsTruth
	if v.True == 0 {
		op = ast.IsFalsity
	}
	if expression.GetRowLen(er.ctxStack[stkLen-1]) != 1 {
		er.err = expression.ErrOperandColumns.GenWithStackByArgs(1)
		return
	}
	function := er.notToExpression(v.Not, op, &v.Type, er.ctxStack[stkLen-1])
	er.ctxStackPop(1)
	er.ctxStackAppend(function, types.EmptyName)
}

// inToExpression converts in expression to a scalar function. The argument lLen means the length of in list.
// The argument not means if the expression is not in. The tp stands for the expression type, which is always bool.
// a in (b, c, d) will be rewritten as `(a = b) or (a = c) or (a = d)`.
func (er *expressionRewriter) inToExpression(lLen int, not bool, tp *types.FieldType) {
	stkLen := len(er.ctxStack)
	l := expression.GetRowLen(er.ctxStack[stkLen-lLen-1])
	for i := 0; i < lLen; i++ {
		if l != expression.GetRowLen(er.ctxStack[stkLen-lLen+i]) {
			er.err = expression.ErrOperandColumns.GenWithStackByArgs(l)
			return
		}
	}
	args := er.ctxStack[stkLen-lLen-1:]
	leftFt := args[0].GetType()
	leftEt, leftIsNull := leftFt.EvalType(), leftFt.Tp == mysql.TypeNull
	if leftIsNull {
		er.ctxStackPop(lLen + 1)
		er.ctxStackAppend(expression.NewNull(), types.EmptyName)
		return
	}
	if leftEt == types.ETInt {
		for i := 1; i < len(args); i++ {
			if c, ok := args[i].(*expression.Constant); ok {
				var isExceptional bool
				args[i], isExceptional = expression.RefineComparedConstant(er.sctx, *leftFt, c, opcode.EQ)
				if isExceptional {
					args[i] = c
				}
			}
		}
	}
	allSameType := true
	for _, arg := range args[1:] {
		if arg.GetType().Tp != mysql.TypeNull && expression.GetAccurateCmpType(args[0], arg) != leftEt {
			allSameType = false
			break
		}
	}
	var function expression.Expression
	if allSameType && l == 1 && lLen > 1 {
		function = er.notToExpression(not, ast.In, tp, er.ctxStack[stkLen-lLen-1:]...)
	} else {
		eqFunctions := make([]expression.Expression, 0, lLen)
		for i := stkLen - lLen; i < stkLen; i++ {
			expr, err := er.constructBinaryOpFunction(args[0], er.ctxStack[i], ast.EQ)
			if err != nil {
				er.err = err
				return
			}
			eqFunctions = append(eqFunctions, expr)
		}
		function = expression.ComposeDNFCondition(er.sctx, eqFunctions...)
		if not {
			var err error
			function, err = er.newFunction(ast.UnaryNot, tp, function)
			if err != nil {
				er.err = err
				return
			}
		}
	}
	er.ctxStackPop(lLen + 1)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) caseToExpression(v *ast.CaseExpr) {
	stkLen := len(er.ctxStack)
	argsLen := 2 * len(v.WhenClauses)
	if v.ElseClause != nil {
		argsLen++
	}
	er.err = expression.CheckArgsNotMultiColumnRow(er.ctxStack[stkLen-argsLen:]...)
	if er.err != nil {
		return
	}

	// value                          -> ctxStack[stkLen-argsLen-1]
	// when clause(condition, result) -> ctxStack[stkLen-argsLen:stkLen-1];
	// else clause                    -> ctxStack[stkLen-1]
	var args []expression.Expression
	if v.Value != nil {
		// args:  eq scalar func(args: value, condition1), result1,
		//        eq scalar func(args: value, condition2), result2,
		//        ...
		//        else clause
		value := er.ctxStack[stkLen-argsLen-1]
		args = make([]expression.Expression, 0, argsLen)
		for i := stkLen - argsLen; i < stkLen-1; i += 2 {
			arg, err := er.newFunction(ast.EQ, types.NewFieldType(mysql.TypeTiny), value, er.ctxStack[i])
			if err != nil {
				er.err = err
				return
			}
			args = append(args, arg)
			args = append(args, er.ctxStack[i+1])
		}
		if v.ElseClause != nil {
			args = append(args, er.ctxStack[stkLen-1])
		}
		argsLen++ // for trimming the value element later
	} else {
		// args:  condition1, result1,
		//        condition2, result2,
		//        ...
		//        else clause
		args = er.ctxStack[stkLen-argsLen:]
	}
	function, err := er.newFunction(ast.Case, &v.Type, args...)
	if err != nil {
		er.err = err
		return
	}
	er.ctxStackPop(argsLen)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) patternLikeToExpression(v *ast.PatternLikeExpr) {
	l := len(er.ctxStack)
	er.err = expression.CheckArgsNotMultiColumnRow(er.ctxStack[l-2:]...)
	if er.err != nil {
		return
	}

	char, col := er.sctx.GetSessionVars().GetCharsetInfo()
	var function expression.Expression
	fieldType := &types.FieldType{}
	isPatternExactMatch := false
	// Treat predicate 'like' the same way as predicate '=' when it is an exact match.
	if patExpression, ok := er.ctxStack[l-1].(*expression.Constant); ok {
		patString, isNull, err := patExpression.EvalString(nil, chunk.Row{})
		if err != nil {
			er.err = err
			return
		}
		if !isNull {
			patValue, patTypes := stringutil.CompilePattern(patString, v.Escape)
			if stringutil.IsExactMatch(patTypes) && er.ctxStack[l-2].GetType().EvalType() == types.ETString {
				op := ast.EQ
				if v.Not {
					op = ast.NE
				}
				types.DefaultTypeForValue(string(patValue), fieldType, char, col)
				function, er.err = er.constructBinaryOpFunction(er.ctxStack[l-2],
					&expression.Constant{Value: types.NewStringDatum(string(patValue)), RetType: fieldType},
					op)
				isPatternExactMatch = true
			}
		}
	}
	if !isPatternExactMatch {
		types.DefaultTypeForValue(int(v.Escape), fieldType, char, col)
		function = er.notToExpression(v.Not, ast.Like, &v.Type,
			er.ctxStack[l-2], er.ctxStack[l-1], &expression.Constant{Value: types.NewIntDatum(int64(v.Escape)), RetType: fieldType})
	}

	er.ctxStackPop(2)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) regexpToScalarFunc(v *ast.PatternRegexpExpr) {
	l := len(er.ctxStack)
	er.err = expression.CheckArgsNotMultiColumnRow(er.ctxStack[l-2:]...)
	if er.err != nil {
		return
	}
	function := er.notToExpression(v.Not, ast.Regexp, &v.Type, er.ctxStack[l-2], er.ctxStack[l-1])
	er.ctxStackPop(2)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) rowToScalarFunc(v *ast.RowExpr) {
	stkLen := len(er.ctxStack)
	length := len(v.Values)
	rows := make([]expression.Expression, 0, length)
	for i := stkLen - length; i < stkLen; i++ {
		rows = append(rows, er.ctxStack[i])
	}
	er.ctxStackPop(length)
	function, err := er.newFunction(ast.RowFunc, rows[0].GetType(), rows...)
	if err != nil {
		er.err = err
		return
	}
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) betweenToExpression(v *ast.BetweenExpr) {
	stkLen := len(er.ctxStack)
	er.err = expression.CheckArgsNotMultiColumnRow(er.ctxStack[stkLen-3:]...)
	if er.err != nil {
		return
	}

	expr, lexp, rexp := er.ctxStack[stkLen-3], er.ctxStack[stkLen-2], er.ctxStack[stkLen-1]

	if expression.GetCmpTp4MinMax([]expression.Expression{expr, lexp, rexp}) == types.ETDatetime {
		expr = expression.WrapWithCastAsTime(er.sctx, expr, types.NewFieldType(mysql.TypeDatetime))
		lexp = expression.WrapWithCastAsTime(er.sctx, lexp, types.NewFieldType(mysql.TypeDatetime))
		rexp = expression.WrapWithCastAsTime(er.sctx, rexp, types.NewFieldType(mysql.TypeDatetime))
	}

	var op string
	var l, r expression.Expression
	l, er.err = er.newFunction(ast.GE, &v.Type, expr, lexp)
	if er.err == nil {
		r, er.err = er.newFunction(ast.LE, &v.Type, expr, rexp)
	}
	op = ast.LogicAnd
	if er.err != nil {
		return
	}
	function, err := er.newFunction(op, &v.Type, l, r)
	if err != nil {
		er.err = err
		return
	}
	if v.Not {
		function, err = er.newFunction(ast.UnaryNot, &v.Type, function)
		if err != nil {
			er.err = err
			return
		}
	}
	er.ctxStackPop(3)
	er.ctxStackAppend(function, types.EmptyName)
}

// rewriteFuncCall handles a FuncCallExpr and generates a customized function.
// It should return true if for the given FuncCallExpr a rewrite is performed so that original behavior is skipped.
// Otherwise it should return false to indicate (the caller) that original behavior needs to be performed.
func (er *expressionRewriter) rewriteFuncCall(v *ast.FuncCallExpr) bool {
	switch v.FnName.L {
	// when column is not null, ifnull on such column is not necessary.
	case ast.Ifnull:
		if len(v.Args) != 2 {
			er.err = expression.ErrIncorrectParameterCount.GenWithStackByArgs(v.FnName.O)
			return true
		}
		stackLen := len(er.ctxStack)
		arg1 := er.ctxStack[stackLen-2]
		col, isColumn := arg1.(*expression.Column)
		// if expr1 is a column and column has not null flag, then we can eliminate ifnull on
		// this column.
		if isColumn && mysql.HasNotNullFlag(col.RetType.Flag) {
			name := er.ctxNameStk[stackLen-2]
			newCol := col.Clone().(*expression.Column)
			er.ctxStackPop(len(v.Args))
			er.ctxStackAppend(newCol, name)
			return true
		}

		return false
	case ast.Nullif:
		if len(v.Args) != 2 {
			er.err = expression.ErrIncorrectParameterCount.GenWithStackByArgs(v.FnName.O)
			return true
		}
		stackLen := len(er.ctxStack)
		param1 := er.ctxStack[stackLen-2]
		param2 := er.ctxStack[stackLen-1]
		// param1 = param2
		funcCompare, err := er.constructBinaryOpFunction(param1, param2, ast.EQ)
		if err != nil {
			er.err = err
			return true
		}
		// NULL
		nullTp := types.NewFieldType(mysql.TypeNull)
		nullTp.Flen, nullTp.Decimal = mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeNull)
		paramNull := &expression.Constant{
			Value:   types.NewDatum(nil),
			RetType: nullTp,
		}
		// if(param1 = param2, NULL, param1)
		funcIf, err := er.newFunction(ast.If, &v.Type, funcCompare, paramNull, param1)
		if err != nil {
			er.err = err
			return true
		}
		er.ctxStackPop(len(v.Args))
		er.ctxStackAppend(funcIf, types.EmptyName)
		return true
	default:
		return false
	}
}

func (er *expressionRewriter) funcCallToExpression(v *ast.FuncCallExpr) {
	stackLen := len(er.ctxStack)
	args := er.ctxStack[stackLen-len(v.Args):]
	er.err = expression.CheckArgsNotMultiColumnRow(args...)
	if er.err != nil {
		return
	}

	if er.rewriteFuncCall(v) {
		return
	}

	var function expression.Expression
	er.ctxStackPop(len(v.Args))
	if _, ok := expression.DeferredFunctions[v.FnName.L]; er.useCache() && ok {
		// When the expression is unix_timestamp and the number of argument is not zero,
		// we deal with it as normal expression.
		if v.FnName.L == ast.UnixTimestamp && len(v.Args) != 0 {
			function, er.err = er.newFunction(v.FnName.L, &v.Type, args...)
			er.ctxStackAppend(function, types.EmptyName)
		} else {
			function, er.err = expression.NewFunctionBase(er.sctx, v.FnName.L, &v.Type, args...)
			c := &expression.Constant{Value: types.NewDatum(nil), RetType: function.GetType().Clone(), DeferredExpr: function}
			er.ctxStackAppend(c, types.EmptyName)
		}
	} else {
		function, er.err = er.newFunction(v.FnName.L, &v.Type, args...)
		er.ctxStackAppend(function, types.EmptyName)
	}
}

// Now TableName in expression only used by sequence function like nextval(seq).
// The function arg should be evaluated as a table name rather than normal column name like mysql does.
func (er *expressionRewriter) toTable(v *ast.TableName) {
	fullName := v.Name.L
	if len(v.Schema.L) != 0 {
		fullName = v.Schema.L + "." + fullName
	}
	val := &expression.Constant{
		Value:   types.NewDatum(fullName),
		RetType: types.NewFieldType(mysql.TypeString),
	}
	er.ctxStackAppend(val, types.EmptyName)
}

func (er *expressionRewriter) toColumn(v *ast.ColumnName) {
	idx, err := expression.FindFieldName(er.names, v)
	if err != nil {
		er.err = ErrAmbiguous.GenWithStackByArgs(v.Name, clauseMsg[fieldList])
		return
	}
	if idx >= 0 {
		column := er.schema.Columns[idx]
		if column.IsHidden {
			er.err = ErrUnknownColumn.GenWithStackByArgs(v.Name, clauseMsg[er.b.curClause])
			return
		}
		er.ctxStackAppend(column, er.names[idx])
		return
	}
	for i := len(er.b.outerSchemas) - 1; i >= 0; i-- {
		outerSchema, outerName := er.b.outerSchemas[i], er.b.outerNames[i]
		idx, err = expression.FindFieldName(outerName, v)
		if idx >= 0 {
			column := outerSchema.Columns[idx]
			er.ctxStackAppend(&expression.CorrelatedColumn{Column: *column, Data: new(types.Datum)}, outerName[idx])
			return
		}
		if err != nil {
			er.err = ErrAmbiguous.GenWithStackByArgs(v.Name, clauseMsg[fieldList])
			return
		}
	}
	if join, ok := er.p.(*LogicalJoin); ok && join.redundantSchema != nil {
		idx, err := expression.FindFieldName(join.redundantNames, v)
		if err != nil {
			er.err = err
			return
		}
		if idx >= 0 {
			er.ctxStackAppend(join.redundantSchema.Columns[idx], join.redundantNames[idx])
			return
		}
	}
	if _, ok := er.p.(*LogicalUnionAll); ok && v.Table.O != "" {
		er.err = ErrTablenameNotAllowedHere.GenWithStackByArgs(v.Table.O, "SELECT", clauseMsg[er.b.curClause])
		return
	}
	if er.b.curClause == globalOrderByClause {
		er.b.curClause = orderByClause
	}
	er.err = ErrUnknownColumn.GenWithStackByArgs(v.String(), clauseMsg[er.b.curClause])
}

func (er *expressionRewriter) evalDefaultExpr(v *ast.DefaultExpr) {
	stkLen := len(er.ctxStack)
	name := er.ctxNameStk[stkLen-1]
	switch er.ctxStack[stkLen-1].(type) {
	case *expression.Column:
	case *expression.CorrelatedColumn:
	default:
		idx, err := expression.FindFieldName(er.names, v.Name)
		if err != nil {
			er.err = err
			return
		}
		if er.err != nil {
			return
		}
		if idx < 0 {
			er.err = ErrUnknownColumn.GenWithStackByArgs(v.Name.OrigColName(), "field_list")
			return
		}
	}
	dbName := name.DBName
	if dbName.O == "" {
		// if database name is not specified, use current database name
		dbName = model.NewCIStr(er.sctx.GetSessionVars().CurrentDB)
	}
	if name.OrigTblName.O == "" {
		// column is evaluated by some expressions, for example:
		// `select default(c) from (select (a+1) as c from t) as t0`
		// in such case, a 'no default' error is returned
		er.err = table.ErrNoDefaultValue.GenWithStackByArgs(name.ColName)
		return
	}
	var tbl table.Table
	tbl, er.err = er.b.is.TableByName(dbName, name.OrigTblName)
	if er.err != nil {
		return
	}
	colName := name.OrigColName.O
	if colName == "" {
		// in some cases, OrigColName is empty, use ColName instead
		colName = name.ColName.O
	}
	col := table.FindCol(tbl.Cols(), colName)
	if col == nil {
		er.err = ErrUnknownColumn.GenWithStackByArgs(v.Name, "field_list")
		return
	}
	isCurrentTimestamp := hasCurrentDatetimeDefault(col)
	var val *expression.Constant
	switch {
	case isCurrentTimestamp && col.Tp == mysql.TypeDatetime:
		// for DATETIME column with current_timestamp, use NULL to be compatible with MySQL 5.7
		val = expression.NewNull()
	case isCurrentTimestamp && col.Tp == mysql.TypeTimestamp:
		// for TIMESTAMP column with current_timestamp, use 0 to be compatible with MySQL 5.7
		zero := types.NewTime(types.ZeroCoreTime, mysql.TypeTimestamp, int8(col.Decimal))
		val = &expression.Constant{
			Value:   types.NewDatum(zero),
			RetType: types.NewFieldType(mysql.TypeTimestamp),
		}
	default:
		// for other columns, just use what it is
		val, er.err = er.b.getDefaultValue(col)
	}
	if er.err != nil {
		return
	}
	er.ctxStackPop(1)
	er.ctxStackAppend(val, types.EmptyName)
}

// hasCurrentDatetimeDefault checks if column has current_timestamp default value
func hasCurrentDatetimeDefault(col *table.Column) bool {
	x, ok := col.DefaultValue.(string)
	if !ok {
		return false
	}
	return strings.ToLower(x) == ast.CurrentTimestamp
}
