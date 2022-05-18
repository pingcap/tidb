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
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/sem"
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
	// in tests, it may be null
	if s, ok := sctx.GetInfoSchema().(infoschema.InfoSchema); ok {
		is = s
	}
	b, savedBlockNames := NewPlanBuilder().Init(sctx, is, &hint.BlockHintProcessor{})
	fakePlan := LogicalTableDual{}.Init(sctx, 0)
	if schema != nil {
		fakePlan.schema = schema
		fakePlan.names = names
	}
	b.curClause = expressionClause
	newExpr, _, err := b.rewrite(context.TODO(), expr, fakePlan, nil, true)
	if err != nil {
		return nil, err
	}
	sctx.GetSessionVars().PlannerSelectBlockAsName = savedBlockNames
	return newExpr, nil
}

func (b *PlanBuilder) rewriteInsertOnDuplicateUpdate(ctx context.Context, exprNode ast.ExprNode, mockPlan LogicalPlan, insertPlan *Insert) (expression.Expression, error) {
	b.rewriterCounter++
	defer func() { b.rewriterCounter-- }()

	b.curClause = fieldList
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
		rewriter.sctx.SetValue(expression.TiDBDecodeKeyFunctionKey, decodeKeyFromString)
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
	rewriter.tryFoldCounter = 0
	rewriter.ctxStack = rewriter.ctxStack[:0]
	rewriter.ctxNameStk = rewriter.ctxNameStk[:0]
	rewriter.ctx = ctx
	rewriter.err = nil
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
	tryFoldCounter     int
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
	outerWindowSpecs := er.b.windowSpecs
	defer func() {
		er.b.windowSpecs = outerWindowSpecs
	}()

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
		if ok {
			// index < 0 indicates this is a correlated aggregate belonging to outer query,
			// for which a correlated column will be created later, so we append a null constant
			// as a temporary result expression.
			if index < 0 {
				er.ctxStackAppend(expression.NewNull(), types.EmptyName)
			} else {
				// index >= 0 indicates this is a regular aggregate column
				er.ctxStackAppend(er.schema.Columns[index], er.names[index])
			}
			return inNode, true
		}
		// replace correlated aggregate in sub-query with its corresponding correlated column
		if col, ok := er.b.correlatedAggMapper[v]; ok {
			er.ctxStackAppend(col, types.EmptyName)
			return inNode, true
		}
		er.err = ErrInvalidGroupFuncUse
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
		er.asScalar = true
		if _, ok := expression.DisableFoldFunctions[v.FnName.L]; ok {
			er.disableFoldCounter++
		}
		if _, ok := expression.TryFoldFunctions[v.FnName.L]; ok {
			er.tryFoldCounter++
		}
	case *ast.CaseExpr:
		er.asScalar = true
		if _, ok := expression.DisableFoldFunctions["case"]; ok {
			er.disableFoldCounter++
		}
		if _, ok := expression.TryFoldFunctions["case"]; ok {
			er.tryFoldCounter++
		}
	case *ast.BinaryOperationExpr:
		er.asScalar = true
		if v.Op == opcode.LogicAnd || v.Op == opcode.LogicOr {
			er.tryFoldCounter++
		}
	case *ast.SetCollationExpr:
		// Do nothing
	default:
		er.asScalar = true
	}
	return inNode, false
}

func (er *expressionRewriter) buildSemiApplyFromEqualSubq(np LogicalPlan, l, r expression.Expression, not bool) {
	if er.asScalar || not {
		if expression.GetRowLen(r) == 1 {
			rCol := r.(*expression.Column)
			// If both input columns of `!= all / = any` expression are not null, we can treat the expression
			// as normal column equal condition.
			if !expression.ExprNotNull(l) || !expression.ExprNotNull(rCol) {
				rColCopy := *rCol
				rColCopy.InOperand = true
				r = &rColCopy
			}
		} else {
			rowFunc := r.(*expression.ScalarFunction)
			rargs := rowFunc.GetArgs()
			args := make([]expression.Expression, 0, len(rargs))
			modified := false
			for i, rarg := range rargs {
				larg := expression.GetFuncArg(l, i)
				if !expression.ExprNotNull(larg) || !expression.ExprNotNull(rarg) {
					rCol := rarg.(*expression.Column)
					rColCopy := *rCol
					rColCopy.InOperand = true
					rarg = &rColCopy
					modified = true
				}
				args = append(args, rarg)
			}
			if modified {
				r, er.err = er.newFunction(ast.RowFunc, args[0].GetType(), args...)
				if er.err != nil {
					return
				}
			}
		}
	}
	var condition expression.Expression
	condition, er.err = er.constructBinaryOpFunction(l, r, ast.EQ)
	if er.err != nil {
		return
	}
	er.p, er.err = er.b.buildSemiApply(er.p, np, []expression.Expression{condition}, er.asScalar, not)
}

func (er *expressionRewriter) handleCompareSubquery(ctx context.Context, v *ast.CompareSubqueryExpr) (ast.Node, bool) {
	ci := er.b.prepareCTECheckForSubQuery()
	defer resetCTECheckForSubQuery(ci)
	v.L.Accept(er)
	if er.err != nil {
		return v, true
	}
	lexpr := er.ctxStack[len(er.ctxStack)-1]
	subq, ok := v.R.(*ast.SubqueryExpr)
	if !ok {
		er.err = errors.Errorf("Unknown compare type %T", v.R)
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

	// Lexpr cannot compare with rexpr by different collate
	opString := new(strings.Builder)
	v.Op.Format(opString)
	_, er.err = expression.CheckAndDeriveCollationFromExprs(er.sctx, opString.String(), types.ETInt, lexpr, rexpr)
	if er.err != nil {
		return v, true
	}

	switch v.Op {
	// Only EQ, NE and NullEQ can be composed with and.
	case opcode.EQ, opcode.NE, opcode.NullEQ:
		if v.Op == opcode.EQ {
			if v.All {
				er.handleEQAll(lexpr, rexpr, np)
			} else {
				// `a = any(subq)` will be rewriten as `a in (subq)`.
				er.asScalar = true
				er.buildSemiApplyFromEqualSubq(np, lexpr, rexpr, false)
				if er.err != nil {
					return v, true
				}
			}
		} else if v.Op == opcode.NE {
			if v.All {
				// `a != all(subq)` will be rewriten as `a not in (subq)`.
				er.asScalar = true
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
	colMaxOrMin.SetCoercibility(rexpr.Coercibility())
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
	// If there is NULL in s.id column, s.id should be the value that isn't null in condition t.id != s.id.
	// So use function max to filter NULL.
	maxFunc, err := aggregation.NewAggFuncDesc(er.sctx, ast.AggFuncMax, []expression.Expression{rexpr}, false)
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
		AggFuncs: []*aggregation.AggFuncDesc{maxFunc, countFunc},
	}.Init(er.sctx, er.b.getSelectOffset())
	if hint := er.b.TableHints(); hint != nil {
		plan4Agg.aggHints = hint.aggHints
	}
	plan4Agg.SetChildren(np)
	maxResultCol := &expression.Column{
		UniqueID: er.sctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  maxFunc.RetTp,
	}
	maxResultCol.SetCoercibility(rexpr.Coercibility())
	count := &expression.Column{
		UniqueID: er.sctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  countFunc.RetTp,
	}
	plan4Agg.names = append(plan4Agg.names, types.EmptyName, types.EmptyName)
	plan4Agg.SetSchema(expression.NewSchema(maxResultCol, count))
	gtFunc := expression.NewFunctionInternal(er.sctx, ast.GT, types.NewFieldType(mysql.TypeTiny), count, expression.NewOne())
	neCond := expression.NewFunctionInternal(er.sctx, ast.NE, types.NewFieldType(mysql.TypeTiny), lexpr, maxResultCol)
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
	firstRowResultCol.SetCoercibility(rexpr.Coercibility())
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
	ci := er.b.prepareCTECheckForSubQuery()
	defer resetCTECheckForSubQuery(ci)
	subq, ok := v.Sel.(*ast.SubqueryExpr)
	if !ok {
		er.err = errors.Errorf("Unknown exists type %T", v.Sel)
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
		// We don't want nth_plan hint to affect separately executed subqueries here, so disable nth_plan temporarily.
		NthPlanBackup := er.sctx.GetSessionVars().StmtCtx.StmtHints.ForceNthPlan
		er.sctx.GetSessionVars().StmtCtx.StmtHints.ForceNthPlan = -1
		physicalPlan, _, err := DoOptimize(ctx, er.sctx, er.b.optFlag, np)
		er.sctx.GetSessionVars().StmtCtx.StmtHints.ForceNthPlan = NthPlanBackup
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
	ci := er.b.prepareCTECheckForSubQuery()
	defer resetCTECheckForSubQuery(ci)
	asScalar := er.asScalar
	er.asScalar = true
	v.Expr.Accept(er)
	if er.err != nil {
		return v, true
	}
	lexpr := er.ctxStack[len(er.ctxStack)-1]
	subq, ok := v.Sel.(*ast.SubqueryExpr)
	if !ok {
		er.err = errors.Errorf("Unknown compare type %T", v.Sel)
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
			// If both input columns of `in` expression are not null, we can treat the expression
			// as normal column equal condition instead.
			if !expression.ExprNotNull(lexpr) || !expression.ExprNotNull(rCol) {
				rColCopy := *rCol
				rColCopy.InOperand = true
				rexpr = &rColCopy
			}
		}
	} else {
		args := make([]expression.Expression, 0, np.Schema().Len())
		for i, col := range np.Schema().Columns {
			if v.Not || asScalar {
				larg := expression.GetFuncArg(lexpr, i)
				if !expression.ExprNotNull(larg) || !expression.ExprNotNull(col) {
					rarg := *col
					rarg.InOperand = true
					col = &rarg
				}
			}
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
	collFlag := collate.CompatibleCollate(lt.GetCollate(), rt.GetCollate())

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
			join.setPreferredJoinTypeAndOrder(er.b.TableHints())
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
	ci := er.b.prepareCTECheckForSubQuery()
	defer resetCTECheckForSubQuery(ci)
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
	// We don't want nth_plan hint to affect separately executed subqueries here, so disable nth_plan temporarily.
	NthPlanBackup := er.sctx.GetSessionVars().StmtCtx.StmtHints.ForceNthPlan
	er.sctx.GetSessionVars().StmtCtx.StmtHints.ForceNthPlan = -1
	physicalPlan, _, err := DoOptimize(ctx, er.sctx, er.b.optFlag, np)
	er.sctx.GetSessionVars().StmtCtx.StmtHints.ForceNthPlan = NthPlanBackup
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
			constant := &expression.Constant{
				Value:   data,
				RetType: np.Schema().Columns[i].GetType()}
			constant.SetCoercibility(np.Schema().Columns[i].Coercibility())
			newCols = append(newCols, constant)
		}
		expr, err1 := er.newFunction(ast.RowFunc, newCols[0].GetType(), newCols...)
		if err1 != nil {
			er.err = err1
			return v, true
		}
		er.ctxStackAppend(expr, types.EmptyName)
	} else {
		constant := &expression.Constant{
			Value:   row[0],
			RetType: np.Schema().Columns[0].GetType(),
		}
		constant.SetCoercibility(np.Schema().Columns[0].Coercibility())
		er.ctxStackAppend(constant, types.EmptyName)
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
		// set right not null flag for constant value
		retType := v.Type.Clone()
		switch v.Datum.Kind() {
		case types.KindNull:
			retType.DelFlag(mysql.NotNullFlag)
		default:
			retType.AddFlag(mysql.NotNullFlag)
		}
		v.Datum.SetValue(v.Datum.GetValue(), retType)
		value := &expression.Constant{Value: v.Datum, RetType: retType}
		value.SetRepertoire(expression.ASCII)
		if retType.EvalType() == types.ETString {
			for _, b := range v.Datum.GetBytes() {
				// if any character in constant is not ascii, set the repertoire to UNICODE.
				if b >= 0x80 {
					value.SetRepertoire(expression.UNICODE)
					break
				}
			}
		}
		er.ctxStackAppend(value, types.EmptyName)
	case *driver.ParamMarkerExpr:
		var value expression.Expression
		value, er.err = expression.ParamMarkerExpression(er.sctx, v, false)
		if er.err != nil {
			return retNode, false
		}
		er.ctxStackAppend(value, types.EmptyName)
	case *ast.VariableExpr:
		er.rewriteVariable(v)
	case *ast.FuncCallExpr:
		if _, ok := expression.TryFoldFunctions[v.FnName.L]; ok {
			er.tryFoldCounter--
		}
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
		if v.Op == opcode.LogicAnd || v.Op == opcode.LogicOr {
			er.tryFoldCounter--
		}
		er.binaryOpToExpression(v)
	case *ast.BetweenExpr:
		er.betweenToExpression(v)
	case *ast.CaseExpr:
		if _, ok := expression.TryFoldFunctions["case"]; ok {
			er.tryFoldCounter--
		}
		er.caseToExpression(v)
		if _, ok := expression.DisableFoldFunctions["case"]; ok {
			er.disableFoldCounter--
		}
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

		castFunction := expression.BuildCastFunction(er.sctx, arg, v.Tp)
		if v.Tp.EvalType() == types.ETString {
			castFunction.SetCoercibility(expression.CoercibilityImplicit)
			if v.Tp.GetCharset() == charset.CharsetASCII {
				castFunction.SetRepertoire(expression.ASCII)
			} else {
				castFunction.SetRepertoire(expression.UNICODE)
			}
		} else {
			castFunction.SetCoercibility(expression.CoercibilityNumeric)
			castFunction.SetRepertoire(expression.ASCII)
		}

		er.ctxStack[len(er.ctxStack)-1] = castFunction
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
			chs := arg.GetType().GetCharset()
			if chs != "" && collInfo.CharsetName != chs {
				er.err = charset.ErrCollationCharsetMismatch.GenWithStackByArgs(collInfo.Name, chs)
				break
			}
		}
		// SetCollationExpr sets the collation explicitly, even when the evaluation type of the expression is non-string.
		if _, ok := arg.(*expression.Column); ok {
			// Wrap a cast here to avoid changing the original FieldType of the column expression.
			exprType := arg.GetType().Clone()
			exprType.SetCollate(v.Collate)
			casted := expression.BuildCastFunction(er.sctx, arg, exprType)
			er.ctxStackPop(1)
			er.ctxStackAppend(casted, types.EmptyName)
		} else {
			// For constant and scalar function, we can set its collate directly.
			arg.GetType().SetCollate(v.Collate)
		}
		er.ctxStack[len(er.ctxStack)-1].SetCoercibility(expression.CoercibilityExplicit)
		er.ctxStack[len(er.ctxStack)-1].SetCharsetAndCollation(arg.GetType().GetCharset(), arg.GetType().GetCollate())
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
func (er *expressionRewriter) newFunction(funcName string, retType *types.FieldType, args ...expression.Expression) (ret expression.Expression, err error) {
	if er.disableFoldCounter > 0 {
		ret, err = expression.NewFunctionBase(er.sctx, funcName, retType, args...)
	} else if er.tryFoldCounter > 0 {
		ret, err = expression.NewFunctionTryFold(er.sctx, funcName, retType, args...)
	} else {
		ret, err = expression.NewFunction(er.sctx, funcName, retType, args...)
	}
	if err != nil {
		return
	}
	if scalarFunc, ok := ret.(*expression.ScalarFunction); ok {
		er.b.ctx.BuiltinFunctionUsageInc(scalarFunc.Function.PbCode().String())
	}
	return
}

func (er *expressionRewriter) checkTimePrecision(ft *types.FieldType) error {
	if ft.EvalType() == types.ETDuration && ft.GetDecimal() > types.MaxFsp {
		return errTooBigPrecision.GenWithStackByArgs(ft.GetDecimal(), "CAST", types.MaxFsp)
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
			tp := er.ctxStack[stkLen-1].GetType()
			er.ctxStack[stkLen-1], er.err = er.newFunction(ast.SetVar, tp,
				expression.DatumToConstant(types.NewDatum(name), mysql.TypeString, 0),
				er.ctxStack[stkLen-1])
			er.ctxNameStk[stkLen-1] = types.EmptyName
			// Store the field type of the variable into SessionVars.UserVarTypes.
			// Normally we can infer the type from SessionVars.User, but we need SessionVars.UserVarTypes when
			// GetVar has not been executed to fill the SessionVars.Users.
			sessionVars.UsersLock.Lock()
			sessionVars.UserVarTypes[name] = tp
			sessionVars.UsersLock.Unlock()
			return
		}
		sessionVars.UsersLock.RLock()
		tp, ok := sessionVars.UserVarTypes[name]
		sessionVars.UsersLock.RUnlock()
		if !ok {
			tp = types.NewFieldType(mysql.TypeVarString)
			tp.SetFlen(mysql.MaxFieldVarCharLength)
		}
		f, err := er.newFunction(ast.GetVar, tp, expression.DatumToConstant(types.NewStringDatum(name), mysql.TypeString, 0))
		if err != nil {
			er.err = err
			return
		}
		f.SetCoercibility(expression.CoercibilityImplicit)
		er.ctxStackAppend(f, types.EmptyName)
		return
	}
	sysVar := variable.GetSysVar(name)
	if sysVar == nil {
		er.err = variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
		if err := variable.CheckSysVarIsRemoved(name); err != nil {
			// Removed vars still return an error, but we customize it from
			// "unknown" to an explanation of why it is not supported.
			// This is important so users at least know they had the name correct.
			er.err = err
		}
		return
	}
	if sem.IsEnabled() && sem.IsInvisibleSysVar(sysVar.Name) {
		err := ErrSpecificAccessDenied.GenWithStackByArgs("RESTRICTED_VARIABLES_ADMIN")
		er.b.visitInfo = appendDynamicVisitInfo(er.b.visitInfo, "RESTRICTED_VARIABLES_ADMIN", false, err)
	}
	if v.ExplicitScope && !sysVar.HasNoneScope() {
		if v.IsGlobal && !(sysVar.HasGlobalScope() || sysVar.HasInstanceScope()) {
			er.err = variable.ErrIncorrectScope.GenWithStackByArgs(name, "SESSION")
			return
		}
		if !v.IsGlobal && !sysVar.HasSessionScope() {
			er.err = variable.ErrIncorrectScope.GenWithStackByArgs(name, "GLOBAL")
			return
		}
	}
	var val string
	var err error
	if sysVar.HasNoneScope() {
		val = sysVar.Value
	} else if v.IsGlobal {
		val, err = variable.GetGlobalSystemVar(sessionVars, name)
	} else {
		val, err = variable.GetSessionOrGlobalSystemVar(sessionVars, name)
	}
	if err != nil {
		er.err = err
		return
	}
	nativeVal, nativeType, nativeFlag := sysVar.GetNativeValType(val)
	e := expression.DatumToConstant(nativeVal, nativeType, nativeFlag)
	charset, _ := sessionVars.GetSystemVar(variable.CharacterSetConnection)
	e.GetType().SetCharset(charset)
	collate, _ := sessionVars.GetSystemVar(variable.CollationConnection)
	e.GetType().SetCollate(collate)
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
	case opcode.Not, opcode.Not2:
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
	if er.err == nil && pos > 0 && pos <= er.schema.Len() && !er.schema.Columns[pos-1].IsHidden {
		er.ctxStackAppend(er.schema.Columns[pos-1], er.names[pos-1])
	} else {
		er.err = ErrUnknownColumn.GenWithStackByArgs(str, clauseMsg[er.b.curClause])
	}
}

func (er *expressionRewriter) isTrueToScalarFunc(v *ast.IsTruthExpr) {
	stkLen := len(er.ctxStack)
	op := ast.IsTruthWithoutNull
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
	leftEt, leftIsNull := leftFt.EvalType(), leftFt.GetType() == mysql.TypeNull
	if leftIsNull {
		er.ctxStackPop(lLen + 1)
		er.ctxStackAppend(expression.NewNull(), types.EmptyName)
		return
	}
	if leftEt == types.ETInt {
		for i := 1; i < len(args); i++ {
			if c, ok := args[i].(*expression.Constant); ok {
				var isExceptional bool
				if expression.MaybeOverOptimized4PlanCache(er.sctx, []expression.Expression{c}) {
					if c.GetType().EvalType() == types.ETString {
						// To keep the result be compatible with MySQL, refine `int non-constant <cmp> str constant`
						// here and skip this refine operation in all other cases for safety.
						er.sctx.GetSessionVars().StmtCtx.SkipPlanCache = true
						expression.RemoveMutableConst(er.sctx, []expression.Expression{c})
					} else {
						continue
					}
				} else if er.sctx.GetSessionVars().StmtCtx.SkipPlanCache {
					// We should remove the mutable constant for correctness, because its value may be changed.
					expression.RemoveMutableConst(er.sctx, []expression.Expression{c})
				}
				args[i], isExceptional = expression.RefineComparedConstant(er.sctx, *leftFt, c, opcode.EQ)
				if isExceptional {
					args[i] = c
				}
			}
		}
	}
	allSameType := true
	for _, arg := range args[1:] {
		if arg.GetType().GetType() != mysql.TypeNull && expression.GetAccurateCmpType(args[0], arg) != leftEt {
			allSameType = false
			break
		}
	}
	var function expression.Expression
	if allSameType && l == 1 && lLen > 1 {
		function = er.notToExpression(not, ast.In, tp, er.ctxStack[stkLen-lLen-1:]...)
	} else {
		// If we rewrite IN to EQ, we need to decide what's the collation EQ uses.
		coll := er.deriveCollationForIn(l, lLen, stkLen, args)
		if er.err != nil {
			return
		}
		er.castCollationForIn(l, lLen, stkLen, coll)
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

// deriveCollationForIn derives collation for in expression.
// We don't handle the cases if the element is a tuple, such as (a, b, c) in ((x1, y1, z1), (x2, y2, z2)).
func (er *expressionRewriter) deriveCollationForIn(colLen int, elemCnt int, stkLen int, args []expression.Expression) *expression.ExprCollation {
	if colLen == 1 {
		// a in (x, y, z) => coll[0]
		coll2, err := expression.CheckAndDeriveCollationFromExprs(er.sctx, "IN", types.ETInt, args...)
		er.err = err
		if er.err != nil {
			return nil
		}
		return coll2
	}
	return nil
}

// castCollationForIn casts collation info for arguments in the `in clause` to make sure the used collation is correct after we
// rewrite it to equal expression.
func (er *expressionRewriter) castCollationForIn(colLen int, elemCnt int, stkLen int, coll *expression.ExprCollation) {
	// We don't handle the cases if the element is a tuple, such as (a, b, c) in ((x1, y1, z1), (x2, y2, z2)).
	if colLen != 1 {
		return
	}
	for i := stkLen - elemCnt; i < stkLen; i++ {
		if er.ctxStack[i].GetType().EvalType() == types.ETString {
			rowFunc, ok := er.ctxStack[i].(*expression.ScalarFunction)
			if ok && rowFunc.FuncName.String() == ast.RowFunc {
				continue
			}
			// Don't convert it if it's charset is binary. So that we don't convert 0x12 to a string.
			if er.ctxStack[i].GetType().GetCollate() == coll.Collation {
				continue
			}
			tp := er.ctxStack[i].GetType().Clone()
			if er.ctxStack[i].GetType().Hybrid() {
				if expression.GetAccurateCmpType(er.ctxStack[stkLen-elemCnt-1], er.ctxStack[i]) == types.ETString {
					tp = types.NewFieldType(mysql.TypeVarString)
				} else {
					continue
				}
			}
			tp.SetCharset(coll.Charset)
			tp.SetCollate(coll.Collation)
			er.ctxStack[i] = expression.BuildCastFunction(er.sctx, er.ctxStack[i], tp)
			er.ctxStack[i].SetCoercibility(expression.CoercibilityExplicit)
		}
	}
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
	// Treat predicate 'like' the same way as predicate '=' when it is an exact match and new collation is not enabled.
	if patExpression, ok := er.ctxStack[l-1].(*expression.Constant); ok && !collate.NewCollationEnabled() {
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

func (er *expressionRewriter) wrapExpWithCast() (expr, lexp, rexp expression.Expression) {
	stkLen := len(er.ctxStack)
	expr, lexp, rexp = er.ctxStack[stkLen-3], er.ctxStack[stkLen-2], er.ctxStack[stkLen-1]
	var castFunc func(sessionctx.Context, expression.Expression) expression.Expression
	switch expression.ResolveType4Between([3]expression.Expression{expr, lexp, rexp}) {
	case types.ETInt:
		castFunc = expression.WrapWithCastAsInt
	case types.ETReal:
		castFunc = expression.WrapWithCastAsReal
	case types.ETDecimal:
		castFunc = expression.WrapWithCastAsDecimal
	case types.ETString:
		castFunc = func(ctx sessionctx.Context, e expression.Expression) expression.Expression {
			// string kind expression do not need cast
			if e.GetType().EvalType().IsStringKind() {
				return e
			}
			return expression.WrapWithCastAsString(ctx, e)
		}
	case types.ETDuration:
		expr = expression.WrapWithCastAsTime(er.sctx, expr, types.NewFieldType(mysql.TypeDuration))
		lexp = expression.WrapWithCastAsTime(er.sctx, lexp, types.NewFieldType(mysql.TypeDuration))
		rexp = expression.WrapWithCastAsTime(er.sctx, rexp, types.NewFieldType(mysql.TypeDuration))
		return
	case types.ETDatetime:
		expr = expression.WrapWithCastAsTime(er.sctx, expr, types.NewFieldType(mysql.TypeDatetime))
		lexp = expression.WrapWithCastAsTime(er.sctx, lexp, types.NewFieldType(mysql.TypeDatetime))
		rexp = expression.WrapWithCastAsTime(er.sctx, rexp, types.NewFieldType(mysql.TypeDatetime))
		return
	default:
		return
	}

	expr = castFunc(er.sctx, expr)
	lexp = castFunc(er.sctx, lexp)
	rexp = castFunc(er.sctx, rexp)
	return
}

func (er *expressionRewriter) betweenToExpression(v *ast.BetweenExpr) {
	stkLen := len(er.ctxStack)
	er.err = expression.CheckArgsNotMultiColumnRow(er.ctxStack[stkLen-3:]...)
	if er.err != nil {
		return
	}

	expr, lexp, rexp := er.wrapExpWithCast()

	coll, err := expression.CheckAndDeriveCollationFromExprs(er.sctx, "BETWEEN", types.ETInt, expr, lexp, rexp)
	er.err = err
	if er.err != nil {
		return
	}

	// Handle enum or set. We need to know their real type to decide whether to cast them.
	lt := expression.GetAccurateCmpType(expr, lexp)
	rt := expression.GetAccurateCmpType(expr, rexp)
	enumOrSetRealTypeIsStr := lt != types.ETInt && rt != types.ETInt

	expr = expression.BuildCastCollationFunction(er.sctx, expr, coll, enumOrSetRealTypeIsStr)
	lexp = expression.BuildCastCollationFunction(er.sctx, lexp, coll, enumOrSetRealTypeIsStr)
	rexp = expression.BuildCastCollationFunction(er.sctx, rexp, coll, enumOrSetRealTypeIsStr)

	var l, r expression.Expression
	l, er.err = expression.NewFunction(er.sctx, ast.GE, &v.Type, expr, lexp)
	if er.err != nil {
		return
	}
	r, er.err = expression.NewFunction(er.sctx, ast.LE, &v.Type, expr, rexp)
	if er.err != nil {
		return
	}
	function, err := er.newFunction(ast.LogicAnd, &v.Type, l, r)
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
		var isEnumSet bool
		if arg1.GetType().GetType() == mysql.TypeEnum || arg1.GetType().GetType() == mysql.TypeSet {
			isEnumSet = true
		}
		// if expr1 is a column and column has not null flag, then we can eliminate ifnull on
		// this column.
		if isColumn && !isEnumSet && mysql.HasNotNullFlag(col.RetType.GetFlag()) {
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
		flen, decimal := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeNull)
		nullTp.SetFlen(flen)
		nullTp.SetDecimal(decimal)
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
	if _, ok := er.p.(*LogicalUnionAll); ok && v.Table.O != "" {
		er.err = ErrTablenameNotAllowedHere.GenWithStackByArgs(v.Table.O, "SELECT", clauseMsg[er.b.curClause])
		return
	}
	col, name, err := findFieldNameFromNaturalUsingJoin(er.p, v)
	if err != nil {
		er.err = err
		return
	} else if col != nil {
		er.ctxStackAppend(col, name)
		return
	}
	if er.b.curClause == globalOrderByClause {
		er.b.curClause = orderByClause
	}
	er.err = ErrUnknownColumn.GenWithStackByArgs(v.String(), clauseMsg[er.b.curClause])
}

func findFieldNameFromNaturalUsingJoin(p LogicalPlan, v *ast.ColumnName) (col *expression.Column, name *types.FieldName, err error) {
	switch x := p.(type) {
	case *LogicalLimit, *LogicalSelection, *LogicalTopN, *LogicalSort, *LogicalMaxOneRow:
		return findFieldNameFromNaturalUsingJoin(p.Children()[0], v)
	case *LogicalJoin:
		if x.fullSchema != nil {
			idx, err := expression.FindFieldName(x.fullNames, v)
			if err != nil {
				return nil, nil, err
			}
			if idx >= 0 {
				return x.fullSchema.Columns[idx], x.fullNames[idx], nil
			}
		}
	}
	return nil, nil, nil
}

func (er *expressionRewriter) evalDefaultExpr(v *ast.DefaultExpr) {
	var name *types.FieldName
	// Here we will find the corresponding column for default function. At the same time, we need to consider the issue
	// of subquery and name space.
	// For example, we have two tables t1(a int default 1, b int) and t2(a int default -1, c int). Consider the following SQL:
	// 		select a from t1 where a > (select default(a) from t2)
	// Refer to the behavior of MySQL, we need to find column a in table t2. If table t2 does not have column a, then find it
	// in table t1. If there are none, return an error message.
	// Based on the above description, we need to look in er.b.allNames from back to front.
	for i := len(er.b.allNames) - 1; i >= 0; i-- {
		idx, err := expression.FindFieldName(er.b.allNames[i], v.Name)
		if err != nil {
			er.err = err
			return
		}
		if idx >= 0 {
			name = er.b.allNames[i][idx]
			break
		}
	}
	if name == nil {
		idx, err := expression.FindFieldName(er.names, v.Name)
		if err != nil {
			er.err = err
			return
		}
		if idx < 0 {
			er.err = ErrUnknownColumn.GenWithStackByArgs(v.Name.OrigColName(), "field list")
			return
		}
		name = er.names[idx]
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
	case isCurrentTimestamp && (col.GetType() == mysql.TypeDatetime || col.GetType() == mysql.TypeTimestamp):
		t, err := expression.GetTimeValue(er.sctx, ast.CurrentTimestamp, col.GetType(), col.GetDecimal())
		if err != nil {
			return
		}
		val = &expression.Constant{
			Value:   t,
			RetType: types.NewFieldType(col.GetType()),
		}
	default:
		// for other columns, just use what it is
		val, er.err = er.b.getDefaultValue(col)
	}
	if er.err != nil {
		return
	}
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

func decodeKeyFromString(ctx sessionctx.Context, s string) string {
	sc := ctx.GetSessionVars().StmtCtx
	key, err := hex.DecodeString(s)
	if err != nil {
		sc.AppendWarning(errors.Errorf("invalid key: %X", key))
		return s
	}
	// Auto decode byte if needed.
	_, bs, err := codec.DecodeBytes(key, nil)
	if err == nil {
		key = bs
	}
	tableID := tablecodec.DecodeTableID(key)
	if tableID == 0 {
		sc.AppendWarning(errors.Errorf("invalid key: %X", key))
		return s
	}
	dm := domain.GetDomain(ctx)
	if dm == nil {
		sc.AppendWarning(errors.Errorf("domain not found when decoding key: %X", key))
		return s
	}
	is := dm.InfoSchema()
	if is == nil {
		sc.AppendWarning(errors.Errorf("infoschema not found when decoding key: %X", key))
		return s
	}
	tbl, _ := is.TableByID(tableID)
	loc := ctx.GetSessionVars().Location()
	if tablecodec.IsRecordKey(key) {
		ret, err := decodeRecordKey(key, tableID, tbl, loc)
		if err != nil {
			sc.AppendWarning(err)
			return s
		}
		return ret
	} else if tablecodec.IsIndexKey(key) {
		ret, err := decodeIndexKey(key, tableID, tbl, loc)
		if err != nil {
			sc.AppendWarning(err)
			return s
		}
		return ret
	} else if tablecodec.IsTableKey(key) {
		ret, err := decodeTableKey(key, tableID)
		if err != nil {
			sc.AppendWarning(err)
			return s
		}
		return ret
	}
	sc.AppendWarning(errors.Errorf("invalid key: %X", key))
	return s
}

func decodeRecordKey(key []byte, tableID int64, tbl table.Table, loc *time.Location) (string, error) {
	_, handle, err := tablecodec.DecodeRecordKey(key)
	if err != nil {
		return "", errors.Trace(err)
	}
	if handle.IsInt() {
		ret := make(map[string]interface{})
		ret["table_id"] = strconv.FormatInt(tableID, 10)
		// When the clustered index is enabled, we should show the PK name.
		if tbl != nil && tbl.Meta().HasClusteredIndex() {
			ret[tbl.Meta().GetPkName().String()] = handle.IntValue()
		} else {
			ret["_tidb_rowid"] = handle.IntValue()
		}
		retStr, err := json.Marshal(ret)
		if err != nil {
			return "", errors.Trace(err)
		}
		return string(retStr), nil
	}
	if tbl != nil {
		tblInfo := tbl.Meta()
		idxInfo := tables.FindPrimaryIndex(tblInfo)
		if idxInfo == nil {
			return "", errors.Trace(errors.Errorf("primary key not found when decoding record key: %X", key))
		}
		cols := make(map[int64]*types.FieldType, len(tblInfo.Columns))
		for _, col := range tblInfo.Columns {
			cols[col.ID] = &col.FieldType
		}
		handleColIDs := make([]int64, 0, len(idxInfo.Columns))
		for _, col := range idxInfo.Columns {
			handleColIDs = append(handleColIDs, tblInfo.Columns[col.Offset].ID)
		}

		if len(handleColIDs) != handle.NumCols() {
			return "", errors.Trace(errors.Errorf("primary key length not match handle columns number in key"))
		}
		datumMap, err := tablecodec.DecodeHandleToDatumMap(handle, handleColIDs, cols, loc, nil)
		if err != nil {
			return "", errors.Trace(err)
		}
		ret := make(map[string]interface{})
		ret["table_id"] = tableID
		handleRet := make(map[string]interface{})
		for colID := range datumMap {
			dt := datumMap[colID]
			dtStr, err := datumToJSONObject(&dt)
			if err != nil {
				return "", errors.Trace(err)
			}
			found := false
			for _, colInfo := range tblInfo.Columns {
				if colInfo.ID == colID {
					found = true
					handleRet[colInfo.Name.L] = dtStr
					break
				}
			}
			if !found {
				return "", errors.Trace(errors.Errorf("column not found when decoding record key: %X", key))
			}
		}
		ret["handle"] = handleRet
		retStr, err := json.Marshal(ret)
		if err != nil {
			return "", errors.Trace(err)
		}
		return string(retStr), nil
	}
	ret := make(map[string]interface{})
	ret["table_id"] = tableID
	ret["handle"] = handle.String()
	retStr, err := json.Marshal(ret)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(retStr), nil
}

func decodeIndexKey(key []byte, tableID int64, tbl table.Table, loc *time.Location) (string, error) {
	if tbl != nil {
		_, indexID, _, err := tablecodec.DecodeKeyHead(key)
		if err != nil {
			return "", errors.Trace(errors.Errorf("invalid record/index key: %X", key))
		}
		tblInfo := tbl.Meta()
		var targetIndex *model.IndexInfo
		for _, idx := range tblInfo.Indices {
			if idx.ID == indexID {
				targetIndex = idx
				break
			}
		}
		if targetIndex == nil {
			return "", errors.Trace(errors.Errorf("index not found when decoding index key: %X", key))
		}
		colInfos := tables.BuildRowcodecColInfoForIndexColumns(targetIndex, tblInfo)
		tps := tables.BuildFieldTypesForIndexColumns(targetIndex, tblInfo)
		values, err := tablecodec.DecodeIndexKV(key, []byte{0}, len(colInfos), tablecodec.HandleNotNeeded, colInfos)
		if err != nil {
			return "", errors.Trace(err)
		}
		ds := make([]types.Datum, 0, len(colInfos))
		for i := 0; i < len(colInfos); i++ {
			d, err := tablecodec.DecodeColumnValue(values[i], tps[i], loc)
			if err != nil {
				return "", errors.Trace(err)
			}
			ds = append(ds, d)
		}
		ret := make(map[string]interface{})
		ret["table_id"] = tableID
		ret["index_id"] = indexID
		idxValMap := make(map[string]interface{}, len(targetIndex.Columns))
		for i := 0; i < len(targetIndex.Columns); i++ {
			dtStr, err := datumToJSONObject(&ds[i])
			if err != nil {
				return "", errors.Trace(err)
			}
			idxValMap[targetIndex.Columns[i].Name.L] = dtStr
		}
		ret["index_vals"] = idxValMap
		retStr, err := json.Marshal(ret)
		if err != nil {
			return "", errors.Trace(err)
		}
		return string(retStr), nil
	}
	_, indexID, indexValues, err := tablecodec.DecodeIndexKey(key)
	if err != nil {
		return "", errors.Trace(errors.Errorf("invalid index key: %X", key))
	}
	ret := make(map[string]interface{})
	ret["table_id"] = tableID
	ret["index_id"] = indexID
	ret["index_vals"] = strings.Join(indexValues, ", ")
	retStr, err := json.Marshal(ret)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(retStr), nil
}

func decodeTableKey(key []byte, tableID int64) (string, error) {
	ret := map[string]int64{"table_id": tableID}
	retStr, err := json.Marshal(ret)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(retStr), nil
}

func datumToJSONObject(d *types.Datum) (interface{}, error) {
	if d.IsNull() {
		return nil, nil
	}
	return d.ToString()
}
