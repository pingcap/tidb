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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// EvalSubqueryFirstRow evaluates incorrelated subqueries once, and get first row.
var EvalSubqueryFirstRow func(ctx context.Context, p base.PhysicalPlan, is infoschema.InfoSchema, sctx base.PlanContext) (row []types.Datum, err error)

// evalAstExprWithPlanCtx evaluates ast expression with plan context.
// Different with expression.EvalSimpleAst, it uses planner context and is more powerful to build some special expressions
// like subquery, window function, etc.
func evalAstExprWithPlanCtx(sctx base.PlanContext, expr ast.ExprNode) (types.Datum, error) {
	if val, ok := expr.(*driver.ValueExpr); ok {
		return val.Datum, nil
	}
	newExpr, err := rewriteAstExprWithPlanCtx(sctx, expr, nil, nil, false)
	if err != nil {
		return types.Datum{}, err
	}
	return newExpr.Eval(sctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
}

// evalAstExpr evaluates ast expression directly.
func evalAstExpr(ctx expression.BuildContext, expr ast.ExprNode) (types.Datum, error) {
	if val, ok := expr.(*driver.ValueExpr); ok {
		return val.Datum, nil
	}
	newExpr, err := buildSimpleExpr(ctx, expr)
	if err != nil {
		return types.Datum{}, err
	}
	return newExpr.Eval(ctx.GetEvalCtx(), chunk.Row{})
}

// rewriteAstExprWithPlanCtx rewrites ast expression directly.
// Different with expression.BuildSimpleExpr, it uses planner context and is more powerful to build some special expressions
// like subquery, window function, etc.
func rewriteAstExprWithPlanCtx(sctx base.PlanContext, expr ast.ExprNode, schema *expression.Schema, names types.NameSlice, allowCastArray bool) (expression.Expression, error) {
	var is infoschema.InfoSchema
	// in tests, it may be null
	if s, ok := sctx.GetInfoSchema().(infoschema.InfoSchema); ok {
		is = s
	}
	b, savedBlockNames := NewPlanBuilder().Init(sctx, is, hint.NewQBHintHandler(nil))
	b.allowBuildCastArray = allowCastArray
	fakePlan := logicalop.LogicalTableDual{}.Init(sctx, 0)
	if schema != nil {
		fakePlan.SetSchema(schema)
		fakePlan.SetOutputNames(names)
	}
	b.curClause = expressionClause
	newExpr, _, err := b.rewrite(context.TODO(), expr, fakePlan, nil, true)
	if err != nil {
		return nil, err
	}
	sctx.GetSessionVars().PlannerSelectBlockAsName.Store(&savedBlockNames)
	return newExpr, nil
}

func buildSimpleExpr(ctx expression.BuildContext, node ast.ExprNode, opts ...expression.BuildOption) (expression.Expression, error) {
	intest.AssertNotNil(node)
	if node == nil {
		// This should never happen. Return error to make it easy to debug in case we have some unexpected bugs.
		return nil, errors.New("expression node should be present")
	}

	var options expression.BuildOptions
	for _, opt := range opts {
		opt(&options)
	}

	if options.InputSchema == nil && len(options.InputNames) > 0 {
		return nil, errors.New("InputSchema and InputNames should be specified at the same time")
	}

	if options.InputSchema != nil && len(options.InputSchema.Columns) != len(options.InputNames) {
		return nil, errors.New("InputSchema and InputNames should be the same length")
	}

	// assert all input db names are the same if specified
	intest.AssertFunc(func() bool {
		if len(options.InputNames) == 0 {
			return true
		}

		dbName := options.InputNames[0].DBName
		if options.SourceTableDB.L != "" {
			intest.Assert(dbName.L == options.SourceTableDB.L)
		}

		for _, name := range options.InputNames {
			intest.Assert(name.DBName.L == dbName.L)
		}
		return true
	})

	rewriter := &expressionRewriter{
		ctx:                 context.TODO(),
		sctx:                ctx,
		schema:              options.InputSchema,
		names:               options.InputNames,
		sourceTable:         options.SourceTable,
		allowBuildCastArray: options.AllowCastArray,
		asScalar:            true,
	}

	if tbl := options.SourceTable; tbl != nil && rewriter.schema == nil {
		cols, names, err := expression.ColumnInfos2ColumnsAndNames(ctx, options.SourceTableDB, tbl.Name, tbl.Cols(), tbl)
		if err != nil {
			return nil, err
		}
		intest.Assert(len(cols) == len(names))
		rewriter.schema = expression.NewSchema(cols...)
		rewriter.names = names
	}

	if rewriter.schema == nil {
		rewriter.schema = expression.NewSchema()
	}

	expr, _, err := rewriteExprNode(rewriter, node, rewriter.asScalar)
	if err != nil {
		return nil, err
	}

	if ft := options.TargetFieldType; ft != nil {
		expr = expression.BuildCastFunction(ctx, expr, ft)
	}

	return expr, err
}

func (b *PlanBuilder) rewriteInsertOnDuplicateUpdate(ctx context.Context, exprNode ast.ExprNode, mockPlan base.LogicalPlan, insertPlan *physicalop.Insert) (expression.Expression, error) {
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

	rewriter.planCtx.insertPlan = insertPlan
	rewriter.asScalar = true
	rewriter.allowBuildCastArray = b.allowBuildCastArray

	expr, _, err := rewriteExprNode(rewriter, exprNode, true)
	return expr, err
}

// rewrite function rewrites ast expr to expression.Expression.
// aggMapper maps ast.AggregateFuncExpr to the columns offset in p's output schema.
// asScalar means whether this expression must be treated as a scalar expression.
// And this function returns a result expression, a new plan that may have apply or semi-join.
func (b *PlanBuilder) rewrite(ctx context.Context, exprNode ast.ExprNode, p base.LogicalPlan, aggMapper map[*ast.AggregateFuncExpr]int, asScalar bool) (expression.Expression, base.LogicalPlan, error) {
	expr, resultPlan, err := b.rewriteWithPreprocess(ctx, exprNode, p, aggMapper, nil, asScalar, nil)
	return expr, resultPlan, err
}

// rewriteWithPreprocess is for handling the situation that we need to adjust the input ast tree
// before really using its node in `expressionRewriter.Leave`. In that case, we first call
// er.preprocess(expr), which returns a new expr. Then we use the new expr in `Leave`.
func (b *PlanBuilder) rewriteWithPreprocess(
	ctx context.Context,
	exprNode ast.ExprNode,
	p base.LogicalPlan, aggMapper map[*ast.AggregateFuncExpr]int,
	windowMapper map[*ast.WindowFuncExpr]int,
	asScalar bool,
	preprocess func(ast.Node) ast.Node,
) (expression.Expression, base.LogicalPlan, error) {
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

	rewriter.planCtx.aggrMap = aggMapper
	rewriter.planCtx.windowMap = windowMapper
	rewriter.asScalar = asScalar
	rewriter.allowBuildCastArray = b.allowBuildCastArray
	rewriter.preprocess = preprocess

	expr, resultPlan, err := rewriteExprNode(rewriter, exprNode, asScalar)
	return expr, resultPlan, err
}

func (b *PlanBuilder) getExpressionRewriter(ctx context.Context, p base.LogicalPlan) (rewriter *expressionRewriter) {
	defer func() {
		if p != nil {
			if join, ok := p.(*logicalop.LogicalJoin); ok && join.FullSchema != nil {
				rewriter.schema = join.FullSchema
				rewriter.names = join.FullNames
			} else {
				rewriter.schema = p.Schema()
				rewriter.names = p.OutputNames()
			}
		}
	}()

	if len(b.rewriterPool) < b.rewriterCounter {
		rewriter = &expressionRewriter{
			sctx: b.ctx.GetExprCtx(), ctx: ctx,
			planCtx: &exprRewriterPlanCtx{plan: p, builder: b, curClause: b.curClause, rollExpand: b.currentBlockExpand},
		}
		b.rewriterPool = append(b.rewriterPool, rewriter)
		return
	}

	rewriter = b.rewriterPool[b.rewriterCounter-1]
	rewriter.asScalar = false
	rewriter.preprocess = nil
	rewriter.disableFoldCounter = 0
	rewriter.tryFoldCounter = 0
	rewriter.ctxStack = rewriter.ctxStack[:0]
	rewriter.ctxNameStk = rewriter.ctxNameStk[:0]
	rewriter.ctx = ctx
	rewriter.err = nil
	rewriter.planCtx.plan = p
	rewriter.planCtx.curClause = b.curClause
	rewriter.planCtx.aggrMap = nil
	rewriter.planCtx.insertPlan = nil
	rewriter.planCtx.rollExpand = b.currentBlockExpand
	return
}

func rewriteExprNode(rewriter *expressionRewriter, exprNode ast.ExprNode, asScalar bool) (expression.Expression, base.LogicalPlan, error) {
	planCtx := rewriter.planCtx
	// sourceTable is only used to build simple expression with one table
	// when planCtx is present, sourceTable should be nil.
	intest.Assert(planCtx == nil || rewriter.sourceTable == nil)
	if planCtx != nil && planCtx.plan != nil {
		curColLen := planCtx.plan.Schema().Len()
		defer func() {
			names := planCtx.plan.OutputNames().Shallow()[:curColLen]
			for i := curColLen; i < planCtx.plan.Schema().Len(); i++ {
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
			planCtx.plan.SetOutputNames(names)
		}()
	}
	exprNode.Accept(rewriter)
	if rewriter.err != nil {
		return nil, nil, errors.Trace(rewriter.err)
	}

	var plan base.LogicalPlan
	if planCtx != nil {
		plan = planCtx.plan
	}

	if !asScalar && len(rewriter.ctxStack) == 0 {
		return nil, plan, nil
	}
	if len(rewriter.ctxStack) != 1 {
		return nil, nil, errors.Errorf("context len %v is invalid", len(rewriter.ctxStack))
	}
	rewriter.err = expression.CheckArgsNotMultiColumnRow(rewriter.ctxStack[0])
	if rewriter.err != nil {
		return nil, nil, errors.Trace(rewriter.err)
	}
	return rewriter.ctxStack[0], plan, nil
}

type exprRewriterPlanCtx struct {
	plan    base.LogicalPlan
	builder *PlanBuilder

	// curClause tracks which part of the query is being processed
	curClause clauseCode

	aggrMap   map[*ast.AggregateFuncExpr]int
	windowMap map[*ast.WindowFuncExpr]int

	// insertPlan is only used to rewrite the expressions inside the assignment
	// of the "INSERT" statement.
	insertPlan *physicalop.Insert

	rollExpand *logicalop.LogicalExpand
}

type expressionRewriter struct {
	ctxStack   []expression.Expression
	ctxNameStk []*types.FieldName
	schema     *expression.Schema
	names      []*types.FieldName
	err        error

	sctx expression.BuildContext
	ctx  context.Context

	// asScalar indicates the return value must be a scalar value.
	// NOTE: This value can be changed during expression rewritten.
	asScalar bool
	// allowBuildCastArray indicates whether allow cast(... as ... array).
	allowBuildCastArray bool
	// sourceTable is only used to build simple expression without all columns from a single table
	sourceTable *model.TableInfo

	// preprocess is called for every ast.Node in Leave.
	preprocess func(ast.Node) ast.Node

	// disableFoldCounter controls fold-disabled scope. If > 0, rewriter will NOT do constant folding.
	// Typically, during visiting AST, while entering the scope(disable), the counter will +1; while
	// leaving the scope(enable again), the counter will -1.
	// NOTE: This value can be changed during expression rewritten.
	disableFoldCounter int
	tryFoldCounter     int

	astNodeStack []ast.Node

	planCtx *exprRewriterPlanCtx
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
/*
	The algorithm is as follows:
	1. If the length of the two sides of the expression is 1, return l op r directly.
    2. If the length of the two sides of the expression is not equal, return an error.
	3. If the operator is EQ, NE, or NullEQ, converts (a0,a1,a2) op (b0,b1,b2) to (a0 op b0) and (a1 op b1) and (a2 op b2)
	4. If the operator is not EQ, NE, or NullEQ,
            converts (a0,a1,a2) op (b0,b1,b2) to (a0 > b0) or (a0 = b0 and a1 > b1) or (a0 = b0 and a1 = b1 and a2 op b2)
	   Especially, op is GE or LE, the prefix element will be converted to > or <.
            converts (a0,a1,a2) >= (b0,b1,b2) to (a0 > b0) or (a0 = b0 and a1 > b1) or (a0 = b0 and a1 = b1 and a2 >= b2)
       The only different between >= and > is that >= additional include the (x,y,z) = (a,b,c).
*/
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
		for i := range lLen {
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
		/*
				The algorithm is as follows:
				1. Iterate over i left columns and construct his own CNF for each left column.
			        1.1 Iterate over j (every i-1 columns) to l[j]=r[j]
			        1.2 Build current i column with op to l[i] op r[i]
			        1.3 Combine 1.1 and 1.2 predicates with AND operator
				2. Combine every i CNF with OR operator.
		*/
		resultDNFList := make([]expression.Expression, 0, lLen)
		// Step 1
		for i := range lLen {
			exprList := make([]expression.Expression, 0, i+1)
			// Step 1.1 build prefix equal conditions
			// (l[0], ... , l[i-1], ...) op (r[0], ... , r[i-1], ...) should be convert to
			// l[0] = r[0] and l[1] = r[1] and ... and l[i-1] = r[i-1]
			for j := range i {
				jExpr, err := er.constructBinaryOpFunction(expression.GetFuncArg(l, j), expression.GetFuncArg(r, j), ast.EQ)
				if err != nil {
					return nil, err
				}
				exprList = append(exprList, jExpr)
			}

			// Especially, op is GE or LE, the prefix element will be converted to > or <.
			degeneratedOp := op
			if i < lLen-1 {
				switch op {
				case ast.GE:
					degeneratedOp = ast.GT
				case ast.LE:
					degeneratedOp = ast.LT
				}
			}
			// Step 1.2
			currentIndexExpr, err := er.constructBinaryOpFunction(expression.GetFuncArg(l, i), expression.GetFuncArg(r, i), degeneratedOp)
			if err != nil {
				return nil, err
			}
			exprList = append(exprList, currentIndexExpr)
			// Step 1.3
			currentExpr := expression.ComposeCNFCondition(er.sctx, exprList...)
			resultDNFList = append(resultDNFList, currentExpr)
		}
		// Step 2
		return expression.ComposeDNFCondition(er.sctx, resultDNFList...), nil
	}
}



