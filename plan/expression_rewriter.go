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

package plan

import (
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/sessionctx/varsutil"
	"github.com/pingcap/tidb/util/types"
)

// EvalSubquery evaluates incorrelated subqueries once.
var EvalSubquery func(p PhysicalPlan, is infoschema.InfoSchema, ctx context.Context) ([][]types.Datum, error)

// evalAstExpr evaluates ast expression directly.
func evalAstExpr(expr ast.ExprNode, ctx context.Context) (types.Datum, error) {
	if val, ok := expr.(*ast.ValueExpr); ok {
		return val.Datum, nil
	}
	b := &planBuilder{
		ctx:       ctx,
		allocator: new(idAllocator),
		colMapper: make(map[*ast.ColumnNameExpr]int),
	}
	if ctx.GetSessionVars().TxnCtx.InfoSchema != nil {
		b.is = ctx.GetSessionVars().TxnCtx.InfoSchema.(infoschema.InfoSchema)
	}
	err := expression.InferType(ctx.GetSessionVars().StmtCtx, expr)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	newExpr, _, err := b.rewrite(expr, nil, nil, true)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return newExpr.Eval(nil)
}

// rewrite function rewrites ast expr to expression.Expression.
// aggMapper maps ast.AggregateFuncExpr to the columns offset in p's output schema.
// asScalar means whether this expression must be treated as a scalar expression.
// And this function returns a result expression, a new plan that may have apply or semi-join.
func (b *planBuilder) rewrite(expr ast.ExprNode, p LogicalPlan, aggMapper map[*ast.AggregateFuncExpr]int, asScalar bool) (
	expression.Expression, LogicalPlan, error) {
	return b.rewriteWithPreprocess(expr, p, aggMapper, asScalar, nil)
}

// rewriteWithPreprocess is for handling the situation that we need to adjust the input ast tree
// before really using its node in `expressionRewriter.Leave`. In that case, we first call
// er.preprocess(expr), which returns a new expr. Then we use the new expr in `Leave`.
func (b *planBuilder) rewriteWithPreprocess(expr ast.ExprNode, p LogicalPlan, aggMapper map[*ast.AggregateFuncExpr]int, asScalar bool, preprocess func(ast.Node) ast.Node) (
	expression.Expression, LogicalPlan, error) {
	er := &expressionRewriter{
		p:          p,
		aggrMap:    aggMapper,
		b:          b,
		asScalar:   asScalar,
		ctx:        b.ctx,
		preprocess: preprocess,
	}
	if p != nil {
		er.schema = p.Schema()
	}
	expr.Accept(er)
	if er.err != nil {
		return nil, nil, errors.Trace(er.err)
	}
	if !asScalar && len(er.ctxStack) == 0 {
		return nil, er.p, nil
	}
	if len(er.ctxStack) != 1 {
		return nil, nil, errors.Errorf("context len %v is invalid", len(er.ctxStack))
	}
	if getRowLen(er.ctxStack[0]) != 1 {
		return nil, nil, ErrOperandColumns.GenByArgs(1)
	}
	return er.ctxStack[0], er.p, nil
}

type expressionRewriter struct {
	ctxStack []expression.Expression
	p        LogicalPlan
	schema   *expression.Schema
	err      error
	aggrMap  map[*ast.AggregateFuncExpr]int
	b        *planBuilder
	ctx      context.Context
	// asScalar means the return value must be a scalar value.
	asScalar bool

	// preprocess is called for every ast.Node in Leave.
	preprocess func(ast.Node) ast.Node
}

func getRowLen(e expression.Expression) int {
	if f, ok := e.(*expression.ScalarFunction); ok && f.FuncName.L == ast.RowFunc {
		return len(f.GetArgs())
	}
	if c, ok := e.(*expression.Constant); ok && c.Value.Kind() == types.KindRow {
		return len(c.Value.GetRow())
	}
	return 1
}

func getRowArg(e expression.Expression, idx int) expression.Expression {
	if f, ok := e.(*expression.ScalarFunction); ok {
		return f.GetArgs()[idx]
	}
	c, _ := e.(*expression.Constant)
	d := c.Value.GetRow()[idx]
	return &expression.Constant{Value: d, RetType: c.GetType()}
}

// popRowArg pops the first element and return the rest of row.
// e.g. After this function (1, 2, 3) becomes (2, 3).
func popRowArg(ctx context.Context, e expression.Expression) (ret expression.Expression, err error) {
	if f, ok := e.(*expression.ScalarFunction); ok {
		args := f.GetArgs()
		if len(args) == 2 {
			return args[1].Clone(), nil
		}
		ret, err = expression.NewFunction(ctx, f.FuncName.L, f.GetType(), args[1:]...)
		return ret, errors.Trace(err)
	}
	c, _ := e.(*expression.Constant)
	ret = &expression.Constant{Value: types.NewDatum(c.Value.GetRow()[1:]), RetType: c.GetType()}
	return
}

// 1. If op are EQ or NE or NullEQ, constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to (a0 op b0) and (a1 op b1) and (a2 op b2)
// 2. If op are LE or GE, constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to
// `IF( (a0 op b0) EQ 0, 0,
//      IF ( (a1 op b1) EQ 0, 0, a2 op b2))`
// 3. If op are LT or GT, constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to
// `IF( a0 NE b0, a0 op b0,
//      IF( a1 NE b1,
//          a1 op b1,
//          a2 op b2)
// )`
func (er *expressionRewriter) constructBinaryOpFunction(l expression.Expression, r expression.Expression, op string) (expression.Expression, error) {
	lLen, rLen := getRowLen(l), getRowLen(r)
	if lLen == 1 && rLen == 1 {
		return expression.NewFunction(er.ctx, op, types.NewFieldType(mysql.TypeTiny), l, r)
	} else if rLen != lLen {
		return nil, ErrOperandColumns.GenByArgs(lLen)
	}
	switch op {
	case ast.EQ, ast.NE, ast.NullEQ:
		funcs := make([]expression.Expression, lLen)
		for i := 0; i < lLen; i++ {
			var err error
			funcs[i], err = er.constructBinaryOpFunction(getRowArg(l, i), getRowArg(r, i), op)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		return expression.ComposeCNFCondition(er.ctx, funcs...), nil
	default:
		larg0, rarg0 := getRowArg(l, 0), getRowArg(r, 0)
		var expr1, expr2, expr3 expression.Expression
		if op == ast.LE || op == ast.GE {
			expr1, _ = expression.NewFunction(er.ctx, op, types.NewFieldType(mysql.TypeTiny), larg0, rarg0)
			expr1, _ = expression.NewFunction(er.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), expr1, expression.Zero)
			expr2 = expression.Zero
		} else if op == ast.LT || op == ast.GT {
			expr1, _ = expression.NewFunction(er.ctx, ast.NE, types.NewFieldType(mysql.TypeTiny), larg0, rarg0)
			expr2, _ = expression.NewFunction(er.ctx, op, types.NewFieldType(mysql.TypeTiny), larg0, rarg0)
		}
		l, err := popRowArg(er.ctx, l)
		if err != nil {
			return nil, errors.Trace(err)
		}
		r, err := popRowArg(er.ctx, r)
		if err != nil {
			return nil, errors.Trace(err)
		}
		expr3, err = er.constructBinaryOpFunction(l, r, op)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return expression.NewFunction(er.ctx, ast.If, types.NewFieldType(mysql.TypeTiny), expr1, expr2, expr3)
	}
}

func (er *expressionRewriter) buildSubquery(subq *ast.SubqueryExpr) LogicalPlan {
	outerSchema := er.schema.Clone()
	er.b.outerSchemas = append(er.b.outerSchemas, outerSchema)
	np := er.b.buildResultSetNode(subq.Query)
	er.b.outerSchemas = er.b.outerSchemas[0 : len(er.b.outerSchemas)-1]
	if er.b.err != nil {
		er.err = errors.Trace(er.b.err)
		return nil
	}
	return np
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
			er.err = errors.New("Can't appear aggrFunctions")
			return inNode, true
		}
		er.ctxStack = append(er.ctxStack, er.schema.Columns[index])
		return inNode, true
	case *ast.ColumnNameExpr:
		if index, ok := er.b.colMapper[v]; ok {
			er.ctxStack = append(er.ctxStack, er.schema.Columns[index])
			return inNode, true
		}
	case *ast.CompareSubqueryExpr:
		return er.handleCompareSubquery(v)
	case *ast.ExistsSubqueryExpr:
		return er.handleExistSubquery(v)
	case *ast.PatternInExpr:
		if v.Sel != nil {
			return er.handleInSubquery(v)
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
				return er.handleInSubquery(v)
			case *ast.ParenthesesExpr:
				x = y.Expr
			default:
				return inNode, false
			}
		}
	case *ast.SubqueryExpr:
		return er.handleScalarSubquery(v)
	case *ast.ParenthesesExpr:
	case *ast.ValuesExpr:
		er.ctxStack = append(er.ctxStack, expression.NewValuesFunc(v.Column.Refer.Column.Offset, &v.Type, er.ctx))
		return inNode, true
	default:
		er.asScalar = true
	}
	return inNode, false
}

func (er *expressionRewriter) handleCompareSubquery(v *ast.CompareSubqueryExpr) (ast.Node, bool) {
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
	np := er.buildSubquery(subq)
	if er.err != nil {
		return v, true
	}
	// Only (a,b,c) = all (...) and (a,b,c) != any () can use row expression.
	canMultiCol := (!v.All && v.Op == opcode.EQ) || (v.All && v.Op == opcode.NE)
	if !canMultiCol && (getRowLen(lexpr) != 1 || np.Schema().Len() != 1) {
		er.err = ErrOperandColumns.GenByArgs(1)
		return v, true
	}
	lLen := getRowLen(lexpr)
	if lLen != np.Schema().Len() {
		er.err = ErrOperandColumns.GenByArgs(lLen)
		return v, true
	}
	var condition expression.Expression
	var rexpr expression.Expression
	if np.Schema().Len() == 1 {
		rexpr = np.Schema().Columns[0].Clone()
	} else {
		args := make([]expression.Expression, 0, np.Schema().Len())
		for _, col := range np.Schema().Columns {
			args = append(args, col.Clone())
		}
		rexpr, er.err = expression.NewFunction(er.ctx, ast.RowFunc, args[0].GetType(), args...)
		if er.err != nil {
			er.err = errors.Trace(er.err)
			return v, true
		}
	}
	switch v.Op {
	// Only EQ, NE and NullEQ can be composed with and.
	case opcode.EQ, opcode.NE, opcode.NullEQ:
		condition, er.err = er.constructBinaryOpFunction(lexpr, rexpr, ast.EQ)
		if er.err != nil {
			er.err = errors.Trace(er.err)
			return v, true
		}
		if v.Op == opcode.EQ {
			if v.All {
				er.handleEQAll(lexpr, rexpr, np)
			} else {
				er.p = er.b.buildSemiApply(er.p, np, []expression.Expression{condition}, er.asScalar, false)
			}
		} else if v.Op == opcode.NE {
			if v.All {
				er.p = er.b.buildSemiApply(er.p, np, []expression.Expression{condition}, er.asScalar, true)
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
	}
	return v, true
}

// handleOtherComparableSubq handles the queries like < any, < max, etc. For example, if the query is t.id < any (select s.id from s),
// it will be rewrote to t.id < (select max(s.id) from s).
func (er *expressionRewriter) handleOtherComparableSubq(lexpr, rexpr expression.Expression, np LogicalPlan, useMin bool, cmpFunc string, all bool) {
	funcName := ast.AggFuncMax
	if useMin {
		funcName = ast.AggFuncMin
	}
	aggFunc := expression.NewAggFunction(funcName, []expression.Expression{rexpr}, false)
	agg := LogicalAggregation{
		AggFuncs: []expression.AggregationFunction{aggFunc},
	}.init(er.b.allocator, er.ctx)
	addChild(agg, np)
	aggCol0 := &expression.Column{
		ColName:  model.NewCIStr("agg_Col_0"),
		FromID:   agg.id,
		Position: 0,
		RetType:  aggFunc.GetType(),
	}
	schema := expression.NewSchema(aggCol0)
	agg.SetSchema(schema)
	cond, _ := expression.NewFunction(er.ctx, cmpFunc, types.NewFieldType(mysql.TypeTiny), lexpr, aggCol0.Clone())
	er.buildQuantifierPlan(agg, cond, rexpr, all)
}

// buildQuantifierPlan adds extra condition for any / all subquery.
func (er *expressionRewriter) buildQuantifierPlan(agg *LogicalAggregation, cond, rexpr expression.Expression, all bool) {
	isNullFunc, _ := expression.NewFunction(er.ctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), rexpr.Clone())
	sumFunc := expression.NewAggFunction(ast.AggFuncSum, []expression.Expression{isNullFunc}, false)
	countFuncNull := expression.NewAggFunction(ast.AggFuncCount, []expression.Expression{isNullFunc.Clone()}, false)
	agg.AggFuncs = append(agg.AggFuncs, sumFunc, countFuncNull)
	posID := agg.schema.Len()
	aggColSum := &expression.Column{
		ColName:  model.NewCIStr("agg_col_sum"),
		FromID:   agg.id,
		Position: posID,
		RetType:  sumFunc.GetType(),
	}
	agg.schema.Append(aggColSum)
	if all {
		aggColCountNull := &expression.Column{
			ColName:  model.NewCIStr("agg_col_cnt"),
			FromID:   agg.id,
			Position: posID + 1,
			RetType:  countFuncNull.GetType(),
		}
		agg.schema.Append(aggColCountNull)
		// All of the inner record set should not contain null value. So for t.id < all(select s.id from s), it
		// should be rewrote to t.id < min(s.id) and if(sum(s.id is null) = 0, true, null).
		hasNotNull, _ := expression.NewFunction(er.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), aggColSum.Clone(), expression.Zero)
		nullChecker, _ := expression.NewFunction(er.ctx, ast.If, types.NewFieldType(mysql.TypeTiny), hasNotNull, expression.One, expression.Null)
		cond = expression.ComposeCNFCondition(er.ctx, cond, nullChecker)
		// If the set is empty, it should always return true.
		checkEmpty, _ := expression.NewFunction(er.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), aggColCountNull.Clone(), expression.Zero)
		cond = expression.ComposeDNFCondition(er.ctx, cond, checkEmpty)
	} else {
		// For "any" expression, if the record set has null and the cond return false, the result should be NULL.
		hasNull, _ := expression.NewFunction(er.ctx, ast.NE, types.NewFieldType(mysql.TypeTiny), aggColSum.Clone(), expression.Zero)
		nullChecker, _ := expression.NewFunction(er.ctx, ast.If, types.NewFieldType(mysql.TypeTiny), hasNull, expression.Null, expression.Zero)
		cond = expression.ComposeDNFCondition(er.ctx, cond, nullChecker)
	}
	if !er.asScalar {
		// For Semi LogicalApply without aux column, the result is no matter false or null. So we can add it to join predicate.
		er.p = er.b.buildSemiApply(er.p, agg, []expression.Expression{cond}, false, false)
		return
	}
	// If we treat the result as a scalar value, we will add a projection with a extra column to output true, false or null.
	outerSchemaLen := er.p.Schema().Len()
	er.p = er.b.buildApplyWithJoinType(er.p, agg, InnerJoin)
	joinSchema := er.p.Schema()
	proj := Projection{
		Exprs: expression.Column2Exprs(joinSchema.Clone().Columns[:outerSchemaLen]),
	}.init(er.b.allocator, er.ctx)
	proj.SetSchema(expression.NewSchema(joinSchema.Clone().Columns[:outerSchemaLen]...))
	proj.Exprs = append(proj.Exprs, cond)
	proj.schema.Append(&expression.Column{
		FromID:      proj.id,
		ColName:     model.NewCIStr("aux_col"),
		Position:    proj.schema.Len(),
		IsAggOrSubq: true,
		RetType:     cond.GetType(),
	})
	addChild(proj, er.p)
	er.p = proj
}

// handleNEAny handles the case of != any. For example, if the query is t.id != any (select s.id from s), it will be rewrote to
// t.id != s.id or count(distinct s.id) > 1 or [any checker]. If there are two different values in s.id ,
// there must exist a s.id that doesn't equal to t.id.
func (er *expressionRewriter) handleNEAny(lexpr, rexpr expression.Expression, np LogicalPlan) {
	firstRowFunc := expression.NewAggFunction(ast.AggFuncFirstRow, []expression.Expression{rexpr}, false)
	countFunc := expression.NewAggFunction(ast.AggFuncCount, []expression.Expression{rexpr.Clone()}, true)
	agg := LogicalAggregation{
		AggFuncs: []expression.AggregationFunction{firstRowFunc, countFunc},
	}.init(er.b.allocator, er.ctx)
	addChild(agg, np)
	firstRowResultCol := &expression.Column{
		ColName:  model.NewCIStr("col_firstRow"),
		FromID:   agg.id,
		Position: 0,
		RetType:  firstRowFunc.GetType(),
	}
	count := &expression.Column{
		ColName:  model.NewCIStr("col_count"),
		FromID:   agg.id,
		Position: 1,
		RetType:  countFunc.GetType(),
	}
	agg.SetSchema(expression.NewSchema(firstRowResultCol, count))
	gtFunc, _ := expression.NewFunction(er.ctx, ast.GT, types.NewFieldType(mysql.TypeTiny), count.Clone(), expression.One)
	neCond, _ := expression.NewFunction(er.ctx, ast.NE, types.NewFieldType(mysql.TypeTiny), lexpr, firstRowResultCol.Clone())
	cond := expression.ComposeDNFCondition(er.ctx, gtFunc, neCond)
	er.buildQuantifierPlan(agg, cond, rexpr, false)
}

// handleEQAll handles the case of = all. For example, if the query is t.id = all (select s.id from s), it will be rewrote to
// t.id = (select s.id from s having count(distinct s.id) <= 1 and [all checker]).
func (er *expressionRewriter) handleEQAll(lexpr, rexpr expression.Expression, np LogicalPlan) {
	firstRowFunc := expression.NewAggFunction(ast.AggFuncFirstRow, []expression.Expression{rexpr}, false)
	countFunc := expression.NewAggFunction(ast.AggFuncCount, []expression.Expression{rexpr.Clone()}, true)
	agg := LogicalAggregation{
		AggFuncs: []expression.AggregationFunction{firstRowFunc, countFunc},
	}.init(er.b.allocator, er.ctx)
	addChild(agg, np)
	firstRowResultCol := &expression.Column{
		ColName:  model.NewCIStr("col_firstRow"),
		FromID:   agg.id,
		Position: 0,
		RetType:  firstRowFunc.GetType(),
	}
	count := &expression.Column{
		ColName:  model.NewCIStr("col_count"),
		FromID:   agg.id,
		Position: 1,
		RetType:  countFunc.GetType(),
	}
	agg.SetSchema(expression.NewSchema(firstRowResultCol, count))
	leFunc, _ := expression.NewFunction(er.ctx, ast.LE, types.NewFieldType(mysql.TypeTiny), count.Clone(), expression.One)
	eqCond, _ := expression.NewFunction(er.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), lexpr, firstRowResultCol.Clone())
	cond := expression.ComposeCNFCondition(er.ctx, leFunc, eqCond)
	er.buildQuantifierPlan(agg, cond, rexpr, true)
}

func (er *expressionRewriter) handleExistSubquery(v *ast.ExistsSubqueryExpr) (ast.Node, bool) {
	subq, ok := v.Sel.(*ast.SubqueryExpr)
	if !ok {
		er.err = errors.Errorf("Unknown exists type %T.", v.Sel)
		return v, true
	}
	np := er.buildSubquery(subq)
	if er.err != nil {
		return v, true
	}
	np = er.b.buildExists(np)
	if len(np.extractCorrelatedCols()) > 0 {
		er.p = er.b.buildSemiApply(er.p, np.Children()[0].(LogicalPlan), nil, er.asScalar, false)
		if !er.asScalar {
			return v, true
		}
		er.ctxStack = append(er.ctxStack, er.p.Schema().Columns[er.p.Schema().Len()-1])
	} else {
		physicalPlan, err := doOptimize(er.b.optFlag, np, er.b.ctx, er.b.allocator)
		rows, err := EvalSubquery(physicalPlan, er.b.is, er.b.ctx)
		if err != nil {
			er.err = errors.Trace(err)
			return v, true
		}
		er.ctxStack = append(er.ctxStack, &expression.Constant{
			Value:   rows[0][0],
			RetType: types.NewFieldType(mysql.TypeTiny)})
	}
	return v, true
}

func (er *expressionRewriter) handleInSubquery(v *ast.PatternInExpr) (ast.Node, bool) {
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
	np := er.buildSubquery(subq)
	if er.err != nil {
		return v, true
	}
	lLen := getRowLen(lexpr)
	if lLen != np.Schema().Len() {
		er.err = ErrOperandColumns.GenByArgs(lLen)
		return v, true
	}
	// Sometimes we can unfold the in subquery. For example, a in (select * from t) can rewrite to `a in (1,2,3,4)`.
	// TODO: Now we cannot add it to CBO framework. Instead, user can set a session variable to open this optimization.
	// We will improve our CBO framework in future.
	if lLen == 1 && er.ctx.GetSessionVars().AllowInSubqueryUnFolding && len(np.extractCorrelatedCols()) == 0 {
		physicalPlan, err := doOptimize(er.b.optFlag, np, er.b.ctx, er.b.allocator)
		if err != nil {
			er.err = errors.Trace(err)
			return v, true
		}
		rows, err := EvalSubquery(physicalPlan, er.b.is, er.b.ctx)
		if err != nil {
			er.err = errors.Trace(err)
			return v, true
		}
		for _, row := range rows {
			con := &expression.Constant{
				Value:   row[0],
				RetType: np.Schema().Columns[0].GetType(),
			}
			er.ctxStack = append(er.ctxStack, con)
		}
		listLen := len(rows)
		if listLen == 0 {
			er.ctxStack[len(er.ctxStack)-1] = &expression.Constant{
				Value:   types.NewDatum(v.Not),
				RetType: types.NewFieldType(mysql.TypeTiny),
			}
		} else {
			er.inToExpression(listLen, v.Not, &v.Type)
		}
		return v, true
	}
	var rexpr expression.Expression
	if np.Schema().Len() == 1 {
		rexpr = np.Schema().Columns[0].Clone()
	} else {
		args := make([]expression.Expression, 0, np.Schema().Len())
		for _, col := range np.Schema().Columns {
			args = append(args, col.Clone())
		}
		rexpr, er.err = expression.NewFunction(er.ctx, ast.RowFunc, args[0].GetType(), args...)
		if er.err != nil {
			er.err = errors.Trace(er.err)
			return v, true
		}
	}
	// a in (subq) will be rewrote as a = any(subq).
	// a not in (subq) will be rewrote as a != all(subq).
	checkCondition, err := er.constructBinaryOpFunction(lexpr, rexpr, ast.EQ)
	if err != nil {
		er.err = errors.Trace(err)
		return v, true
	}
	er.p = er.b.buildSemiApply(er.p, np, expression.SplitCNFItems(checkCondition), asScalar, v.Not)
	if asScalar {
		col := er.p.Schema().Columns[er.p.Schema().Len()-1]
		er.ctxStack[len(er.ctxStack)-1] = col
	} else {
		er.ctxStack = er.ctxStack[:len(er.ctxStack)-1]
	}
	return v, true
}

func (er *expressionRewriter) handleScalarSubquery(v *ast.SubqueryExpr) (ast.Node, bool) {
	np := er.buildSubquery(v)
	if er.err != nil {
		return v, true
	}
	np = er.b.buildMaxOneRow(np)
	if len(np.extractCorrelatedCols()) > 0 {
		er.p = er.b.buildApplyWithJoinType(er.p, np, LeftOuterJoin)
		if np.Schema().Len() > 1 {
			newCols := make([]expression.Expression, 0, np.Schema().Len())
			for _, col := range np.Schema().Columns {
				newCols = append(newCols, col.Clone())
			}
			expr, err := expression.NewFunction(er.ctx, ast.RowFunc, newCols[0].GetType(), newCols...)
			if err != nil {
				er.err = errors.Trace(err)
				return v, true
			}
			er.ctxStack = append(er.ctxStack, expr)
		} else {
			er.ctxStack = append(er.ctxStack, er.p.Schema().Columns[er.p.Schema().Len()-1])
		}
		return v, true
	}
	physicalPlan, err := doOptimize(er.b.optFlag, np, er.b.ctx, er.b.allocator)
	if err != nil {
		er.err = errors.Trace(err)
		return v, true
	}
	rows, err := EvalSubquery(physicalPlan, er.b.is, er.b.ctx)
	if err != nil {
		er.err = errors.Trace(err)
		return v, true
	}
	if np.Schema().Len() > 1 {
		newCols := make([]expression.Expression, 0, np.Schema().Len())
		for i, data := range rows[0] {
			newCols = append(newCols, &expression.Constant{
				Value:   data,
				RetType: np.Schema().Columns[i].GetType()})
		}
		expr, err1 := expression.NewFunction(er.ctx, ast.RowFunc, newCols[0].GetType(), newCols...)
		if err1 != nil {
			er.err = errors.Trace(err1)
			return v, true
		}
		er.ctxStack = append(er.ctxStack, expr)
	} else {
		er.ctxStack = append(er.ctxStack, &expression.Constant{
			Value:   rows[0][0],
			RetType: np.Schema().Columns[0].GetType(),
		})
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
		*ast.SubqueryExpr, *ast.ExistsSubqueryExpr, *ast.CompareSubqueryExpr, *ast.ValuesExpr:
	case *ast.ValueExpr:
		tp := &types.FieldType{}
		types.DefaultTypeForValue(v.GetValue(), tp)
		value := &expression.Constant{Value: v.Datum, RetType: tp}
		er.ctxStack = append(er.ctxStack, value)
	case *ast.ParamMarkerExpr:
		value := &expression.Constant{Value: v.Datum, RetType: &v.Type}
		er.ctxStack = append(er.ctxStack, value)
	case *ast.VariableExpr:
		er.rewriteVariable(v)
	case *ast.FuncCallExpr:
		er.funcCallToExpression(v)
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
		er.checkArgsOneColumn(arg)
		if er.err != nil {
			return retNode, false
		}
		er.ctxStack[len(er.ctxStack)-1] = expression.NewCastFunc(v.Tp, arg, er.ctx)
	case *ast.PatternLikeExpr:
		er.likeToScalarFunc(v)
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
	default:
		er.err = errors.Errorf("UnknownType: %T", v)
		return retNode, false
	}

	if er.err != nil {
		return retNode, false
	}
	return originInNode, true
}

func datumToConstant(d types.Datum, tp byte) *expression.Constant {
	return &expression.Constant{Value: d, RetType: types.NewFieldType(tp)}
}

func (er *expressionRewriter) rewriteVariable(v *ast.VariableExpr) {
	stkLen := len(er.ctxStack)
	name := strings.ToLower(v.Name)
	sessionVars := er.b.ctx.GetSessionVars()
	if !v.IsSystem {
		if v.Value != nil {
			er.ctxStack[stkLen-1], er.err = expression.NewFunction(er.ctx,
				ast.SetVar,
				er.ctxStack[stkLen-1].GetType(),
				datumToConstant(types.NewDatum(name), mysql.TypeString),
				er.ctxStack[stkLen-1])
			return
		}
		f, err := expression.NewFunction(er.ctx,
			ast.GetVar,
			// TODO: Here is wrong, the sessionVars should store a name -> Datum map. Will fix it later.
			types.NewFieldType(mysql.TypeString),
			datumToConstant(types.NewStringDatum(name), mysql.TypeString))
		if err != nil {
			er.err = errors.Trace(err)
			return
		}
		er.ctxStack = append(er.ctxStack, f)
		return
	}
	var val string
	var err error
	if v.IsGlobal {
		val, err = varsutil.GetGlobalSystemVar(sessionVars, name)
	} else {
		val, err = varsutil.GetSessionSystemVar(sessionVars, name)
	}
	if err != nil {
		er.err = errors.Trace(err)
		return
	}
	er.ctxStack = append(er.ctxStack, datumToConstant(types.NewStringDatum(val), mysql.TypeString))
	return
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
	if getRowLen(er.ctxStack[stkLen-1]) != 1 {
		er.err = ErrOperandColumns.GenByArgs(1)
		return
	}
	er.ctxStack[stkLen-1], er.err = expression.NewFunction(er.ctx, op, &v.Type, er.ctxStack[stkLen-1])
}

func (er *expressionRewriter) binaryOpToExpression(v *ast.BinaryOperationExpr) {
	stkLen := len(er.ctxStack)
	var function expression.Expression
	switch v.Op {
	case opcode.EQ, opcode.NE, opcode.NullEQ, opcode.GT, opcode.GE, opcode.LT, opcode.LE:
		function, er.err = er.constructBinaryOpFunction(er.ctxStack[stkLen-2], er.ctxStack[stkLen-1],
			v.Op.String())
	default:
		lLen := getRowLen(er.ctxStack[stkLen-2])
		rLen := getRowLen(er.ctxStack[stkLen-1])
		if lLen != 1 || rLen != 1 {
			er.err = ErrOperandColumns.GenByArgs(1)
			return
		}
		function, er.err = expression.NewFunction(er.ctx, v.Op.String(), &v.Type, er.ctxStack[stkLen-2:]...)
	}
	if er.err != nil {
		er.err = errors.Trace(er.err)
		return
	}
	er.ctxStack = er.ctxStack[:stkLen-2]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) notToExpression(hasNot bool, op string, tp *types.FieldType,
	args ...expression.Expression) expression.Expression {
	opFunc, err := expression.NewFunction(er.ctx, op, tp, args...)
	if err != nil {
		er.err = errors.Trace(err)
		return nil
	}
	if !hasNot {
		return opFunc
	}

	opFunc, err = expression.NewFunction(er.ctx, ast.UnaryNot, tp, opFunc)
	if err != nil {
		er.err = errors.Trace(err)
		return nil
	}
	return opFunc
}

func (er *expressionRewriter) isNullToExpression(v *ast.IsNullExpr) {
	stkLen := len(er.ctxStack)
	if getRowLen(er.ctxStack[stkLen-1]) != 1 {
		er.err = ErrOperandColumns.GenByArgs(1)
		return
	}
	function := er.notToExpression(v.Not, ast.IsNull, &v.Type, er.ctxStack[stkLen-1])
	er.ctxStack = er.ctxStack[:stkLen-1]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) positionToScalarFunc(v *ast.PositionExpr) {
	if v.N > 0 && v.N <= er.schema.Len() {
		er.ctxStack = append(er.ctxStack, er.schema.Columns[v.N-1])
	} else {
		er.err = errors.Errorf("Position %d is out of range", v.N)
	}
}

func (er *expressionRewriter) isTrueToScalarFunc(v *ast.IsTruthExpr) {
	stkLen := len(er.ctxStack)
	op := ast.IsTruth
	if v.True == 0 {
		op = ast.IsFalsity
	}
	if getRowLen(er.ctxStack[stkLen-1]) != 1 {
		er.err = ErrOperandColumns.GenByArgs(1)
		return
	}
	function := er.notToExpression(v.Not, op, &v.Type, er.ctxStack[stkLen-1])
	er.ctxStack = er.ctxStack[:stkLen-1]
	er.ctxStack = append(er.ctxStack, function)
}

// inToExpression convert in expression to a scalar function. The argument lLen means the length of in list.
// The argument not means if the expression is not in. The tp stands for the expression type, which is always bool.
func (er *expressionRewriter) inToExpression(lLen int, not bool, tp *types.FieldType) {
	stkLen := len(er.ctxStack)
	l := getRowLen(er.ctxStack[stkLen-lLen-1])
	for i := 0; i < lLen; i++ {
		if l != getRowLen(er.ctxStack[stkLen-lLen+i]) {
			er.err = ErrOperandColumns.GenByArgs(l)
			return
		}
	}
	function := er.notToExpression(not, ast.In, tp, er.ctxStack[stkLen-lLen-1:]...)
	er.ctxStack = er.ctxStack[:stkLen-lLen-1]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) caseToExpression(v *ast.CaseExpr) {
	stkLen := len(er.ctxStack)
	argsLen := 2 * len(v.WhenClauses)
	if v.ElseClause != nil {
		argsLen++
	}
	er.checkArgsOneColumn(er.ctxStack[stkLen-argsLen:]...)
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
			arg, err := expression.NewFunction(er.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), value.Clone(), er.ctxStack[i])
			if err != nil {
				er.err = errors.Trace(err)
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
	function, err := expression.NewFunction(er.ctx, ast.Case, &v.Type, args...)
	if err != nil {
		er.err = errors.Trace(err)
		return
	}
	er.ctxStack = er.ctxStack[:stkLen-argsLen]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) likeToScalarFunc(v *ast.PatternLikeExpr) {
	l := len(er.ctxStack)
	er.checkArgsOneColumn(er.ctxStack[l-2:]...)
	if er.err != nil {
		return
	}
	escapeTp := &types.FieldType{}
	types.DefaultTypeForValue(int(v.Escape), escapeTp)
	function := er.notToExpression(v.Not, ast.Like, &v.Type,
		er.ctxStack[l-2], er.ctxStack[l-1], &expression.Constant{Value: types.NewIntDatum(int64(v.Escape)), RetType: escapeTp})
	er.ctxStack = er.ctxStack[:l-2]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) regexpToScalarFunc(v *ast.PatternRegexpExpr) {
	l := len(er.ctxStack)
	er.checkArgsOneColumn(er.ctxStack[l-2:]...)
	if er.err != nil {
		return
	}
	function := er.notToExpression(v.Not, ast.Regexp, &v.Type, er.ctxStack[l-2], er.ctxStack[l-1])
	er.ctxStack = er.ctxStack[:l-2]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) rowToScalarFunc(v *ast.RowExpr) {
	stkLen := len(er.ctxStack)
	length := len(v.Values)
	rows := make([]expression.Expression, 0, length)
	for i := stkLen - length; i < stkLen; i++ {
		rows = append(rows, er.ctxStack[i])
	}
	er.ctxStack = er.ctxStack[:stkLen-length]
	function, err := expression.NewFunction(er.ctx, ast.RowFunc, rows[0].GetType(), rows...)
	if err != nil {
		er.err = errors.Trace(err)
		return
	}
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) betweenToExpression(v *ast.BetweenExpr) {
	stkLen := len(er.ctxStack)
	er.checkArgsOneColumn(er.ctxStack[stkLen-3:]...)
	if er.err != nil {
		return
	}
	var op string
	var l, r expression.Expression
	l, er.err = expression.NewFunction(er.ctx, ast.GE, &v.Type, er.ctxStack[stkLen-3], er.ctxStack[stkLen-2])
	if er.err == nil {
		r, er.err = expression.NewFunction(er.ctx, ast.LE, &v.Type, er.ctxStack[stkLen-3].Clone(), er.ctxStack[stkLen-1])
	}
	op = ast.LogicAnd
	if er.err != nil {
		er.err = errors.Trace(er.err)
		return
	}
	function, err := expression.NewFunction(er.ctx, op, &v.Type, l, r)
	if err != nil {
		er.err = errors.Trace(err)
		return
	}
	if v.Not {
		function, err = expression.NewFunction(er.ctx, ast.UnaryNot, &v.Type, function)
		if err != nil {
			er.err = errors.Trace(err)
			return
		}
	}
	er.ctxStack = er.ctxStack[:stkLen-3]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) checkArgsOneColumn(args ...expression.Expression) {
	for _, arg := range args {
		if getRowLen(arg) != 1 {
			er.err = ErrOperandColumns.GenByArgs(1)
			return
		}
	}
}

func (er *expressionRewriter) funcCallToExpression(v *ast.FuncCallExpr) {
	stackLen := len(er.ctxStack)
	args := er.ctxStack[stackLen-len(v.Args):]
	er.checkArgsOneColumn(args...)
	if er.err != nil {
		return
	}
	var function expression.Expression
	function, er.err = expression.NewFunction(er.ctx, v.FnName.L, &v.Type, args...)
	er.ctxStack = er.ctxStack[:stackLen-len(v.Args)]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) toColumn(v *ast.ColumnName) {
	column, err := er.schema.FindColumn(v)
	if err != nil {
		er.err = ErrAmbiguous.GenByArgs(v.Name)
		return
	}
	if column != nil {
		er.ctxStack = append(er.ctxStack, column.Clone())
		return
	}
	for i := len(er.b.outerSchemas) - 1; i >= 0; i-- {
		outerSchema := er.b.outerSchemas[i]
		column, err = outerSchema.FindColumn(v)
		if column != nil {
			er.ctxStack = append(er.ctxStack, &expression.CorrelatedColumn{Column: *column})
			return
		}
		if err != nil {
			er.err = errors.Trace(err)
			return
		}
	}
	if join, ok := er.p.(*LogicalJoin); ok && join.redundantSchema != nil {
		column, err := join.redundantSchema.FindColumn(v)
		if err != nil {
			er.err = errors.Trace(err)
			return
		}
		if column != nil {
			er.ctxStack = append(er.ctxStack, column.Clone())
			return
		}
	}
	er.err = errors.Errorf("Unknown column %s %s %s.", v.Schema.L, v.Table.L, v.Name.L)
}
