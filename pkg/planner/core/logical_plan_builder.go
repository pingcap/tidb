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
	"fmt"
	"math"
	"math/bits"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	core_metrics "github.com/pingcap/tidb/pkg/planner/core/metrics"
	fd "github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/planner/util/tablesampler"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	util2 "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hack"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
)

const (
	// ErrExprInSelect  is in select fields for the error of ErrFieldNotInGroupBy
	ErrExprInSelect = "SELECT list"
	// ErrExprInOrderBy  is in order by items for the error of ErrFieldNotInGroupBy
	ErrExprInOrderBy = "ORDER BY"
)

// aggOrderByResolver is currently resolving expressions of order by clause
// in aggregate function GROUP_CONCAT.
type aggOrderByResolver struct {
	ctx       base.PlanContext
	err       error
	args      []ast.ExprNode
	exprDepth int // exprDepth is the depth of current expression in expression tree.
}

func (a *aggOrderByResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	a.exprDepth++
	if n, ok := inNode.(*driver.ParamMarkerExpr); ok {
		if a.exprDepth == 1 {
			_, isNull, isExpectedType := getUintFromNode(a.ctx, n, false)
			// For constant uint expression in top level, it should be treated as position expression.
			if !isNull && isExpectedType {
				return expression.ConstructPositionExpr(n), true
			}
		}
	}
	return inNode, false
}

func (a *aggOrderByResolver) Leave(inNode ast.Node) (ast.Node, bool) {
	if v, ok := inNode.(*ast.PositionExpr); ok {
		pos, isNull, err := expression.PosFromPositionExpr(a.ctx.GetExprCtx(), a.ctx, v)
		if err != nil {
			a.err = err
		}
		if err != nil || isNull {
			return inNode, false
		}
		if pos < 1 || pos > len(a.args) {
			errPos := strconv.Itoa(pos)
			if v.P != nil {
				errPos = "?"
			}
			a.err = plannererrors.ErrUnknownColumn.FastGenByArgs(errPos, "order clause")
			return inNode, false
		}
		ret := a.args[pos-1]
		return ret, true
	}
	return inNode, true
}

func (b *PlanBuilder) buildExpand(p base.LogicalPlan, gbyItems []expression.Expression) (base.LogicalPlan, []expression.Expression, error) {
	b.optFlag |= flagResolveExpand

	// Rollup syntax require expand OP to do the data expansion, different data replica supply the different grouping layout.
	distinctGbyExprs, gbyExprsRefPos := expression.DeduplicateGbyExpression(gbyItems)
	// build another projection below.
	proj := LogicalProjection{Exprs: make([]expression.Expression, 0, p.Schema().Len()+len(distinctGbyExprs))}.Init(b.ctx, b.getSelectOffset())
	// project: child's output and distinct GbyExprs in advance. (make every group-by item to be a column)
	projSchema := p.Schema().Clone()
	names := p.OutputNames()
	for _, col := range projSchema.Columns {
		proj.Exprs = append(proj.Exprs, col)
	}
	distinctGbyColNames := make(types.NameSlice, 0, len(distinctGbyExprs))
	distinctGbyCols := make([]*expression.Column, 0, len(distinctGbyExprs))
	for _, expr := range distinctGbyExprs {
		// distinct group expr has been resolved in resolveGby.
		proj.Exprs = append(proj.Exprs, expr)

		// add the newly appended names.
		var name *types.FieldName
		if c, ok := expr.(*expression.Column); ok {
			name = buildExpandFieldName(c, names[p.Schema().ColumnIndex(c)], "")
		} else {
			name = buildExpandFieldName(expr, nil, "")
		}
		names = append(names, name)
		distinctGbyColNames = append(distinctGbyColNames, name)

		// since we will change the nullability of source col, proj it with a new col id.
		col := &expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			// clone it rather than using it directly,
			RetType: expr.GetType(b.ctx.GetExprCtx().GetEvalCtx()).Clone(),
		}

		projSchema.Append(col)
		distinctGbyCols = append(distinctGbyCols, col)
	}
	proj.SetSchema(projSchema)
	proj.SetChildren(p)
	// since expand will ref original col and make some change, do the copy in executor rather than ref the same chunk.column.
	proj.AvoidColumnEvaluator = true
	proj.Proj4Expand = true
	newGbyItems := expression.RestoreGbyExpression(distinctGbyCols, gbyExprsRefPos)

	// build expand.
	rollupGroupingSets := expression.RollupGroupingSets(newGbyItems)
	// eg: <a,b,c> with rollup => {},{a},{a,b},{a,b,c}
	// for every grouping set above, we should individually set those not-needed grouping-set col as null value.
	// eg: let's say base schema is <a,b,c,d>, d is unrelated col, keep it real in every grouping set projection.
	// 		for grouping set {a,b,c}, project it as: [a,    b,    c,    d,   gid]
	//      for grouping set {a,b},   project it as: [a,    b,    null, d,   gid]
	//      for grouping set {a},     project it as: [a,    null, null, d,   gid]
	// 		for grouping set {},      project it as: [null, null, null, d,   gid]
	expandSchema := proj.Schema().Clone()
	expression.AdjustNullabilityFromGroupingSets(rollupGroupingSets, expandSchema)
	expand := LogicalExpand{
		rollupGroupingSets:  rollupGroupingSets,
		distinctGroupByCol:  distinctGbyCols,
		distinctGbyColNames: distinctGbyColNames,
		// for resolving grouping function args.
		distinctGbyExprs: distinctGbyExprs,

		// fill the gen col names when building level projections.
	}.Init(b.ctx, b.getSelectOffset())

	// if we want to use bitAnd for the quick computation of grouping function, then the maximum capacity of num of grouping is about 64.
	expand.GroupingMode = tipb.GroupingMode_ModeBitAnd
	if len(expand.rollupGroupingSets) > 64 {
		expand.GroupingMode = tipb.GroupingMode_ModeNumericSet
	}

	expand.distinctSize, expand.rollupGroupingIDs, expand.rollupID2GIDS = expand.rollupGroupingSets.DistinctSize()
	hasDuplicateGroupingSet := len(expand.rollupGroupingSets) != expand.distinctSize
	// append the generated column for logical Expand.
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.SetFlag(mysql.UnsignedFlag | mysql.NotNullFlag)
	gid := &expression.Column{
		UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  tp,
		OrigName: "gid",
	}
	expand.GID = gid
	expandSchema.Append(gid)
	expand.ExtraGroupingColNames = append(expand.ExtraGroupingColNames, gid.OrigName)
	names = append(names, buildExpandFieldName(gid, nil, "gid_"))
	expand.GIDName = names[len(names)-1]
	if hasDuplicateGroupingSet {
		// the last two col of the schema should be gid & gpos
		gpos := &expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  tp.Clone(),
			OrigName: "gpos",
		}
		expand.GPos = gpos
		expandSchema.Append(gpos)
		expand.ExtraGroupingColNames = append(expand.ExtraGroupingColNames, gpos.OrigName)
		names = append(names, buildExpandFieldName(gpos, nil, "gpos_"))
		expand.GPosName = names[len(names)-1]
	}
	expand.SetChildren(proj)
	expand.SetSchema(expandSchema)
	expand.SetOutputNames(names)

	// register current rollup Expand operator in current select block.
	b.currentBlockExpand = expand

	// defer generating level-projection as last logical optimization rule.
	return expand, newGbyItems, nil
}

func (b *PlanBuilder) buildAggregation(ctx context.Context, p base.LogicalPlan, aggFuncList []*ast.AggregateFuncExpr, gbyItems []expression.Expression,
	correlatedAggMap map[*ast.AggregateFuncExpr]int) (base.LogicalPlan, map[int]int, error) {
	b.optFlag |= flagBuildKeyInfo
	b.optFlag |= flagPushDownAgg
	// We may apply aggregation eliminate optimization.
	// So we add the flagMaxMinEliminate to try to convert max/min to topn and flagPushDownTopN to handle the newly added topn operator.
	b.optFlag |= flagMaxMinEliminate
	b.optFlag |= flagPushDownTopN
	// when we eliminate the max and min we may add `is not null` filter.
	b.optFlag |= flagPredicatePushDown
	b.optFlag |= flagEliminateAgg
	b.optFlag |= flagEliminateProjection

	if b.ctx.GetSessionVars().EnableSkewDistinctAgg {
		b.optFlag |= flagSkewDistinctAgg
	}
	// flag it if cte contain aggregation
	if b.buildingCTE {
		b.outerCTEs[len(b.outerCTEs)-1].containAggOrWindow = true
	}
	var rollupExpand *LogicalExpand
	if expand, ok := p.(*LogicalExpand); ok {
		rollupExpand = expand
	}

	plan4Agg := LogicalAggregation{AggFuncs: make([]*aggregation.AggFuncDesc, 0, len(aggFuncList))}.Init(b.ctx, b.getSelectOffset())
	if hintinfo := b.TableHints(); hintinfo != nil {
		plan4Agg.PreferAggType = hintinfo.PreferAggType
		plan4Agg.PreferAggToCop = hintinfo.PreferAggToCop
	}
	schema4Agg := expression.NewSchema(make([]*expression.Column, 0, len(aggFuncList)+p.Schema().Len())...)
	names := make(types.NameSlice, 0, len(aggFuncList)+p.Schema().Len())
	// aggIdxMap maps the old index to new index after applying common aggregation functions elimination.
	aggIndexMap := make(map[int]int)

	allAggsFirstRow := true
	for i, aggFunc := range aggFuncList {
		newArgList := make([]expression.Expression, 0, len(aggFunc.Args))
		for _, arg := range aggFunc.Args {
			newArg, np, err := b.rewrite(ctx, arg, p, nil, true)
			if err != nil {
				return nil, nil, err
			}
			p = np
			newArgList = append(newArgList, newArg)
		}
		newFunc, err := aggregation.NewAggFuncDesc(b.ctx.GetExprCtx(), aggFunc.F, newArgList, aggFunc.Distinct)
		if err != nil {
			return nil, nil, err
		}
		if newFunc.Name != ast.AggFuncFirstRow {
			allAggsFirstRow = false
		}
		if aggFunc.Order != nil {
			trueArgs := aggFunc.Args[:len(aggFunc.Args)-1] // the last argument is SEPARATOR, remote it.
			resolver := &aggOrderByResolver{
				ctx:  b.ctx,
				args: trueArgs,
			}
			for _, byItem := range aggFunc.Order.Items {
				resolver.exprDepth = 0
				resolver.err = nil
				retExpr, _ := byItem.Expr.Accept(resolver)
				if resolver.err != nil {
					return nil, nil, errors.Trace(resolver.err)
				}
				newByItem, np, err := b.rewrite(ctx, retExpr.(ast.ExprNode), p, nil, true)
				if err != nil {
					return nil, nil, err
				}
				p = np
				newFunc.OrderByItems = append(newFunc.OrderByItems, &util.ByItems{Expr: newByItem, Desc: byItem.Desc})
			}
		}
		// combine identical aggregate functions
		combined := false
		for j := 0; j < i; j++ {
			oldFunc := plan4Agg.AggFuncs[aggIndexMap[j]]
			if oldFunc.Equal(b.ctx.GetExprCtx().GetEvalCtx(), newFunc) {
				aggIndexMap[i] = aggIndexMap[j]
				combined = true
				if _, ok := correlatedAggMap[aggFunc]; ok {
					if _, ok = b.correlatedAggMapper[aggFuncList[j]]; !ok {
						b.correlatedAggMapper[aggFuncList[j]] = &expression.CorrelatedColumn{
							Column: *schema4Agg.Columns[aggIndexMap[j]],
							Data:   new(types.Datum),
						}
					}
					b.correlatedAggMapper[aggFunc] = b.correlatedAggMapper[aggFuncList[j]]
				}
				break
			}
		}
		// create new columns for aggregate functions which show up first
		if !combined {
			position := len(plan4Agg.AggFuncs)
			aggIndexMap[i] = position
			plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
			column := expression.Column{
				UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  newFunc.RetTp,
			}
			schema4Agg.Append(&column)
			names = append(names, types.EmptyName)
			if _, ok := correlatedAggMap[aggFunc]; ok {
				b.correlatedAggMapper[aggFunc] = &expression.CorrelatedColumn{
					Column: column,
					Data:   new(types.Datum),
				}
			}
		}
	}
	for i, col := range p.Schema().Columns {
		newFunc, err := aggregation.NewAggFuncDesc(b.ctx.GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{col}, false)
		if err != nil {
			return nil, nil, err
		}
		plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
		newCol, _ := col.Clone().(*expression.Column)
		newCol.RetType = newFunc.RetTp
		schema4Agg.Append(newCol)
		names = append(names, p.OutputNames()[i])
	}
	var (
		join            *LogicalJoin
		isJoin          bool
		isSelectionJoin bool
	)
	join, isJoin = p.(*LogicalJoin)
	selection, isSelection := p.(*LogicalSelection)
	if isSelection {
		join, isSelectionJoin = selection.Children()[0].(*LogicalJoin)
	}
	if (isJoin && join.fullSchema != nil) || (isSelectionJoin && join.fullSchema != nil) {
		for i, col := range join.fullSchema.Columns {
			if p.Schema().Contains(col) {
				continue
			}
			newFunc, err := aggregation.NewAggFuncDesc(b.ctx.GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{col}, false)
			if err != nil {
				return nil, nil, err
			}
			plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
			newCol, _ := col.Clone().(*expression.Column)
			newCol.RetType = newFunc.RetTp
			schema4Agg.Append(newCol)
			names = append(names, join.fullNames[i])
		}
	}
	hasGroupBy := len(gbyItems) > 0
	for i, aggFunc := range plan4Agg.AggFuncs {
		err := aggFunc.UpdateNotNullFlag4RetType(hasGroupBy, allAggsFirstRow)
		if err != nil {
			return nil, nil, err
		}
		schema4Agg.Columns[i].RetType = aggFunc.RetTp
	}
	plan4Agg.names = names
	plan4Agg.SetChildren(p)
	if rollupExpand != nil {
		// append gid and gpos as the group keys if any.
		plan4Agg.GroupByItems = append(gbyItems, rollupExpand.GID)
		if rollupExpand.GPos != nil {
			plan4Agg.GroupByItems = append(plan4Agg.GroupByItems, rollupExpand.GPos)
		}
	} else {
		plan4Agg.GroupByItems = gbyItems
	}
	plan4Agg.SetSchema(schema4Agg)
	return plan4Agg, aggIndexMap, nil
}

func (b *PlanBuilder) buildTableRefs(ctx context.Context, from *ast.TableRefsClause) (p base.LogicalPlan, err error) {
	if from == nil {
		p = b.buildTableDual()
		return
	}
	defer func() {
		// After build the resultSetNode, need to reset it so that it can be referenced by outer level.
		for _, cte := range b.outerCTEs {
			cte.recursiveRef = false
		}
	}()
	return b.buildResultSetNode(ctx, from.TableRefs, false)
}

func (b *PlanBuilder) buildResultSetNode(ctx context.Context, node ast.ResultSetNode, isCTE bool) (p base.LogicalPlan, err error) {
	//If it is building the CTE queries, we will mark them.
	b.isCTE = isCTE
	switch x := node.(type) {
	case *ast.Join:
		return b.buildJoin(ctx, x)
	case *ast.TableSource:
		var isTableName bool
		switch v := x.Source.(type) {
		case *ast.SelectStmt:
			ci := b.prepareCTECheckForSubQuery()
			defer resetCTECheckForSubQuery(ci)
			b.optFlag = b.optFlag | flagConstantPropagation
			p, err = b.buildSelect(ctx, v)
		case *ast.SetOprStmt:
			ci := b.prepareCTECheckForSubQuery()
			defer resetCTECheckForSubQuery(ci)
			p, err = b.buildSetOpr(ctx, v)
		case *ast.TableName:
			p, err = b.buildDataSource(ctx, v, &x.AsName)
			isTableName = true
		default:
			err = plannererrors.ErrUnsupportedType.GenWithStackByArgs(v)
		}
		if err != nil {
			return nil, err
		}

		for _, name := range p.OutputNames() {
			if name.Hidden {
				continue
			}
			if x.AsName.L != "" {
				name.TblName = x.AsName
			}
		}
		// `TableName` is not a select block, so we do not need to handle it.
		var plannerSelectBlockAsName []ast.HintTable
		if p := b.ctx.GetSessionVars().PlannerSelectBlockAsName.Load(); p != nil {
			plannerSelectBlockAsName = *p
		}
		if len(plannerSelectBlockAsName) > 0 && !isTableName {
			plannerSelectBlockAsName[p.QueryBlockOffset()] = ast.HintTable{DBName: p.OutputNames()[0].DBName, TableName: p.OutputNames()[0].TblName}
		}
		// Duplicate column name in one table is not allowed.
		// "select * from (select 1, 1) as a;" is duplicate
		dupNames := make(map[string]struct{}, len(p.Schema().Columns))
		for _, name := range p.OutputNames() {
			colName := name.ColName.O
			if _, ok := dupNames[colName]; ok {
				return nil, plannererrors.ErrDupFieldName.GenWithStackByArgs(colName)
			}
			dupNames[colName] = struct{}{}
		}
		return p, nil
	case *ast.SelectStmt:
		return b.buildSelect(ctx, x)
	case *ast.SetOprStmt:
		return b.buildSetOpr(ctx, x)
	default:
		return nil, plannererrors.ErrUnsupportedType.GenWithStack("Unsupported ast.ResultSetNode(%T) for buildResultSetNode()", x)
	}
}

// pushDownConstExpr checks if the condition is from filter condition, if true, push it down to both
// children of join, whatever the join type is; if false, push it down to inner child of outer join,
// and both children of non-outer-join.
func (p *LogicalJoin) pushDownConstExpr(expr expression.Expression, leftCond []expression.Expression,
	rightCond []expression.Expression, filterCond bool) ([]expression.Expression, []expression.Expression) {
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		if filterCond {
			leftCond = append(leftCond, expr)
			// Append the expr to right join condition instead of `rightCond`, to make it able to be
			// pushed down to children of join.
			p.RightConditions = append(p.RightConditions, expr)
		} else {
			rightCond = append(rightCond, expr)
		}
	case RightOuterJoin:
		if filterCond {
			rightCond = append(rightCond, expr)
			p.LeftConditions = append(p.LeftConditions, expr)
		} else {
			leftCond = append(leftCond, expr)
		}
	case SemiJoin, InnerJoin:
		leftCond = append(leftCond, expr)
		rightCond = append(rightCond, expr)
	case AntiSemiJoin:
		if filterCond {
			leftCond = append(leftCond, expr)
		}
		rightCond = append(rightCond, expr)
	}
	return leftCond, rightCond
}

func (p *LogicalJoin) extractOnCondition(conditions []expression.Expression, deriveLeft bool,
	deriveRight bool) (eqCond []*expression.ScalarFunction, leftCond []expression.Expression,
	rightCond []expression.Expression, otherCond []expression.Expression) {
	return p.ExtractOnCondition(conditions, p.Children()[0].Schema(), p.Children()[1].Schema(), deriveLeft, deriveRight)
}

// ExtractOnCondition divide conditions in CNF of join node into 4 groups.
// These conditions can be where conditions, join conditions, or collection of both.
// If deriveLeft/deriveRight is set, we would try to derive more conditions for left/right plan.
func (p *LogicalJoin) ExtractOnCondition(
	conditions []expression.Expression,
	leftSchema *expression.Schema,
	rightSchema *expression.Schema,
	deriveLeft bool,
	deriveRight bool) (eqCond []*expression.ScalarFunction, leftCond []expression.Expression,
	rightCond []expression.Expression, otherCond []expression.Expression) {
	ctx := p.SCtx()
	for _, expr := range conditions {
		// For queries like `select a in (select a from s where s.b = t.b) from t`,
		// if subquery is empty caused by `s.b = t.b`, the result should always be
		// false even if t.a is null or s.a is null. To make this join "empty aware",
		// we should differentiate `t.a = s.a` from other column equal conditions, so
		// we put it into OtherConditions instead of EqualConditions of join.
		if expression.IsEQCondFromIn(expr) {
			otherCond = append(otherCond, expr)
			continue
		}
		binop, ok := expr.(*expression.ScalarFunction)
		if ok && len(binop.GetArgs()) == 2 {
			arg0, lOK := binop.GetArgs()[0].(*expression.Column)
			arg1, rOK := binop.GetArgs()[1].(*expression.Column)
			if lOK && rOK {
				leftCol := leftSchema.RetrieveColumn(arg0)
				rightCol := rightSchema.RetrieveColumn(arg1)
				if leftCol == nil || rightCol == nil {
					leftCol = leftSchema.RetrieveColumn(arg1)
					rightCol = rightSchema.RetrieveColumn(arg0)
					arg0, arg1 = arg1, arg0
				}
				if leftCol != nil && rightCol != nil {
					if deriveLeft {
						if util.IsNullRejected(ctx, leftSchema, expr) && !mysql.HasNotNullFlag(leftCol.RetType.GetFlag()) {
							notNullExpr := expression.BuildNotNullExpr(ctx.GetExprCtx(), leftCol)
							leftCond = append(leftCond, notNullExpr)
						}
					}
					if deriveRight {
						if util.IsNullRejected(ctx, rightSchema, expr) && !mysql.HasNotNullFlag(rightCol.RetType.GetFlag()) {
							notNullExpr := expression.BuildNotNullExpr(ctx.GetExprCtx(), rightCol)
							rightCond = append(rightCond, notNullExpr)
						}
					}
					if binop.FuncName.L == ast.EQ {
						cond := expression.NewFunctionInternal(ctx.GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), arg0, arg1)
						eqCond = append(eqCond, cond.(*expression.ScalarFunction))
						continue
					}
				}
			}
		}
		columns := expression.ExtractColumns(expr)
		// `columns` may be empty, if the condition is like `correlated_column op constant`, or `constant`,
		// push this kind of constant condition down according to join type.
		if len(columns) == 0 {
			leftCond, rightCond = p.pushDownConstExpr(expr, leftCond, rightCond, deriveLeft || deriveRight)
			continue
		}
		allFromLeft, allFromRight := true, true
		for _, col := range columns {
			if !leftSchema.Contains(col) {
				allFromLeft = false
			}
			if !rightSchema.Contains(col) {
				allFromRight = false
			}
		}
		if allFromRight {
			rightCond = append(rightCond, expr)
		} else if allFromLeft {
			leftCond = append(leftCond, expr)
		} else {
			// Relax expr to two supersets: leftRelaxedCond and rightRelaxedCond, the expression now is
			// `expr AND leftRelaxedCond AND rightRelaxedCond`. Motivation is to push filters down to
			// children as much as possible.
			if deriveLeft {
				leftRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(ctx.GetExprCtx(), expr, leftSchema)
				if leftRelaxedCond != nil {
					leftCond = append(leftCond, leftRelaxedCond)
				}
			}
			if deriveRight {
				rightRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(ctx.GetExprCtx(), expr, rightSchema)
				if rightRelaxedCond != nil {
					rightCond = append(rightCond, rightRelaxedCond)
				}
			}
			otherCond = append(otherCond, expr)
		}
	}
	return
}

// extractTableAlias returns table alias of the base.LogicalPlan's columns.
// It will return nil when there are multiple table alias, because the alias is only used to check if
// the base.LogicalPlan Match some optimizer hints, and hints are not expected to take effect in this case.
func extractTableAlias(p base.Plan, parentOffset int) *h.HintedTable {
	if len(p.OutputNames()) > 0 && p.OutputNames()[0].TblName.L != "" {
		firstName := p.OutputNames()[0]
		for _, name := range p.OutputNames() {
			if name.TblName.L != firstName.TblName.L ||
				(name.DBName.L != "" && firstName.DBName.L != "" && name.DBName.L != firstName.DBName.L) { // DBName can be nil, see #46160
				return nil
			}
		}
		qbOffset := p.QueryBlockOffset()
		var blockAsNames []ast.HintTable
		if p := p.SCtx().GetSessionVars().PlannerSelectBlockAsName.Load(); p != nil {
			blockAsNames = *p
		}
		// For sub-queries like `(select * from t) t1`, t1 should belong to its surrounding select block.
		if qbOffset != parentOffset && blockAsNames != nil && blockAsNames[qbOffset].TableName.L != "" {
			qbOffset = parentOffset
		}
		dbName := firstName.DBName
		if dbName.L == "" {
			dbName = model.NewCIStr(p.SCtx().GetSessionVars().CurrentDB)
		}
		return &h.HintedTable{DBName: dbName, TblName: firstName.TblName, SelectOffset: qbOffset}
	}
	return nil
}

func (p *LogicalJoin) setPreferredJoinTypeAndOrder(hintInfo *h.PlanHints) {
	if hintInfo == nil {
		return
	}

	lhsAlias := extractTableAlias(p.Children()[0], p.QueryBlockOffset())
	rhsAlias := extractTableAlias(p.Children()[1], p.QueryBlockOffset())
	if hintInfo.IfPreferMergeJoin(lhsAlias) {
		p.preferJoinType |= h.PreferMergeJoin
		p.leftPreferJoinType |= h.PreferMergeJoin
	}
	if hintInfo.IfPreferMergeJoin(rhsAlias) {
		p.preferJoinType |= h.PreferMergeJoin
		p.rightPreferJoinType |= h.PreferMergeJoin
	}
	if hintInfo.IfPreferNoMergeJoin(lhsAlias) {
		p.preferJoinType |= h.PreferNoMergeJoin
		p.leftPreferJoinType |= h.PreferNoMergeJoin
	}
	if hintInfo.IfPreferNoMergeJoin(rhsAlias) {
		p.preferJoinType |= h.PreferNoMergeJoin
		p.rightPreferJoinType |= h.PreferNoMergeJoin
	}
	if hintInfo.IfPreferBroadcastJoin(lhsAlias) {
		p.preferJoinType |= h.PreferBCJoin
		p.leftPreferJoinType |= h.PreferBCJoin
	}
	if hintInfo.IfPreferBroadcastJoin(rhsAlias) {
		p.preferJoinType |= h.PreferBCJoin
		p.rightPreferJoinType |= h.PreferBCJoin
	}
	if hintInfo.IfPreferShuffleJoin(lhsAlias) {
		p.preferJoinType |= h.PreferShuffleJoin
		p.leftPreferJoinType |= h.PreferShuffleJoin
	}
	if hintInfo.IfPreferShuffleJoin(rhsAlias) {
		p.preferJoinType |= h.PreferShuffleJoin
		p.rightPreferJoinType |= h.PreferShuffleJoin
	}
	if hintInfo.IfPreferHashJoin(lhsAlias) {
		p.preferJoinType |= h.PreferHashJoin
		p.leftPreferJoinType |= h.PreferHashJoin
	}
	if hintInfo.IfPreferHashJoin(rhsAlias) {
		p.preferJoinType |= h.PreferHashJoin
		p.rightPreferJoinType |= h.PreferHashJoin
	}
	if hintInfo.IfPreferNoHashJoin(lhsAlias) {
		p.preferJoinType |= h.PreferNoHashJoin
		p.leftPreferJoinType |= h.PreferNoHashJoin
	}
	if hintInfo.IfPreferNoHashJoin(rhsAlias) {
		p.preferJoinType |= h.PreferNoHashJoin
		p.rightPreferJoinType |= h.PreferNoHashJoin
	}
	if hintInfo.IfPreferINLJ(lhsAlias) {
		p.preferJoinType |= h.PreferLeftAsINLJInner
		p.leftPreferJoinType |= h.PreferINLJ
	}
	if hintInfo.IfPreferINLJ(rhsAlias) {
		p.preferJoinType |= h.PreferRightAsINLJInner
		p.rightPreferJoinType |= h.PreferINLJ
	}
	if hintInfo.IfPreferINLHJ(lhsAlias) {
		p.preferJoinType |= h.PreferLeftAsINLHJInner
		p.leftPreferJoinType |= h.PreferINLHJ
	}
	if hintInfo.IfPreferINLHJ(rhsAlias) {
		p.preferJoinType |= h.PreferRightAsINLHJInner
		p.rightPreferJoinType |= h.PreferINLHJ
	}
	if hintInfo.IfPreferINLMJ(lhsAlias) {
		p.preferJoinType |= h.PreferLeftAsINLMJInner
		p.leftPreferJoinType |= h.PreferINLMJ
	}
	if hintInfo.IfPreferINLMJ(rhsAlias) {
		p.preferJoinType |= h.PreferRightAsINLMJInner
		p.rightPreferJoinType |= h.PreferINLMJ
	}
	if hintInfo.IfPreferNoIndexJoin(lhsAlias) {
		p.preferJoinType |= h.PreferNoIndexJoin
		p.leftPreferJoinType |= h.PreferNoIndexJoin
	}
	if hintInfo.IfPreferNoIndexJoin(rhsAlias) {
		p.preferJoinType |= h.PreferNoIndexJoin
		p.rightPreferJoinType |= h.PreferNoIndexJoin
	}
	if hintInfo.IfPreferNoIndexHashJoin(lhsAlias) {
		p.preferJoinType |= h.PreferNoIndexHashJoin
		p.leftPreferJoinType |= h.PreferNoIndexHashJoin
	}
	if hintInfo.IfPreferNoIndexHashJoin(rhsAlias) {
		p.preferJoinType |= h.PreferNoIndexHashJoin
		p.rightPreferJoinType |= h.PreferNoIndexHashJoin
	}
	if hintInfo.IfPreferNoIndexMergeJoin(lhsAlias) {
		p.preferJoinType |= h.PreferNoIndexMergeJoin
		p.leftPreferJoinType |= h.PreferNoIndexMergeJoin
	}
	if hintInfo.IfPreferNoIndexMergeJoin(rhsAlias) {
		p.preferJoinType |= h.PreferNoIndexMergeJoin
		p.rightPreferJoinType |= h.PreferNoIndexMergeJoin
	}
	if hintInfo.IfPreferHJBuild(lhsAlias) {
		p.preferJoinType |= h.PreferLeftAsHJBuild
		p.leftPreferJoinType |= h.PreferHJBuild
	}
	if hintInfo.IfPreferHJBuild(rhsAlias) {
		p.preferJoinType |= h.PreferRightAsHJBuild
		p.rightPreferJoinType |= h.PreferHJBuild
	}
	if hintInfo.IfPreferHJProbe(lhsAlias) {
		p.preferJoinType |= h.PreferLeftAsHJProbe
		p.leftPreferJoinType |= h.PreferHJProbe
	}
	if hintInfo.IfPreferHJProbe(rhsAlias) {
		p.preferJoinType |= h.PreferRightAsHJProbe
		p.rightPreferJoinType |= h.PreferHJProbe
	}
	hasConflict := false
	if !p.SCtx().GetSessionVars().EnableAdvancedJoinHint || p.SCtx().GetSessionVars().StmtCtx.StraightJoinOrder {
		if containDifferentJoinTypes(p.preferJoinType) {
			hasConflict = true
		}
	} else if p.SCtx().GetSessionVars().EnableAdvancedJoinHint {
		if containDifferentJoinTypes(p.leftPreferJoinType) || containDifferentJoinTypes(p.rightPreferJoinType) {
			hasConflict = true
		}
	}
	if hasConflict {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
			"Join hints are conflict, you can only specify one type of join")
		p.preferJoinType = 0
	}
	// set the join order
	if hintInfo.LeadingJoinOrder != nil {
		p.preferJoinOrder = hintInfo.MatchTableName([]*h.HintedTable{lhsAlias, rhsAlias}, hintInfo.LeadingJoinOrder)
	}
	// set hintInfo for further usage if this hint info can be used.
	if p.preferJoinType != 0 || p.preferJoinOrder {
		p.hintInfo = hintInfo
	}
}

func setPreferredJoinTypeFromOneSide(preferJoinType uint, isLeft bool) (resJoinType uint) {
	if preferJoinType == 0 {
		return
	}
	if preferJoinType&h.PreferINLJ > 0 {
		preferJoinType &= ^h.PreferINLJ
		if isLeft {
			resJoinType |= h.PreferLeftAsINLJInner
		} else {
			resJoinType |= h.PreferRightAsINLJInner
		}
	}
	if preferJoinType&h.PreferINLHJ > 0 {
		preferJoinType &= ^h.PreferINLHJ
		if isLeft {
			resJoinType |= h.PreferLeftAsINLHJInner
		} else {
			resJoinType |= h.PreferRightAsINLHJInner
		}
	}
	if preferJoinType&h.PreferINLMJ > 0 {
		preferJoinType &= ^h.PreferINLMJ
		if isLeft {
			resJoinType |= h.PreferLeftAsINLMJInner
		} else {
			resJoinType |= h.PreferRightAsINLMJInner
		}
	}
	if preferJoinType&h.PreferHJBuild > 0 {
		preferJoinType &= ^h.PreferHJBuild
		if isLeft {
			resJoinType |= h.PreferLeftAsHJBuild
		} else {
			resJoinType |= h.PreferRightAsHJBuild
		}
	}
	if preferJoinType&h.PreferHJProbe > 0 {
		preferJoinType &= ^h.PreferHJProbe
		if isLeft {
			resJoinType |= h.PreferLeftAsHJProbe
		} else {
			resJoinType |= h.PreferRightAsHJProbe
		}
	}
	resJoinType |= preferJoinType
	return
}

// setPreferredJoinType generates hint information for the logicalJoin based on the hint information of its left and right children.
func (p *LogicalJoin) setPreferredJoinType() {
	if p.leftPreferJoinType == 0 && p.rightPreferJoinType == 0 {
		return
	}
	p.preferJoinType = setPreferredJoinTypeFromOneSide(p.leftPreferJoinType, true) | setPreferredJoinTypeFromOneSide(p.rightPreferJoinType, false)
	if containDifferentJoinTypes(p.preferJoinType) {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
			"Join hints conflict after join reorder phase, you can only specify one type of join")
		p.preferJoinType = 0
	}
}

func (ds *DataSource) setPreferredStoreType(hintInfo *h.PlanHints) {
	if hintInfo == nil {
		return
	}

	var alias *h.HintedTable
	if len(ds.TableAsName.L) != 0 {
		alias = &h.HintedTable{DBName: ds.DBName, TblName: *ds.TableAsName, SelectOffset: ds.QueryBlockOffset()}
	} else {
		alias = &h.HintedTable{DBName: ds.DBName, TblName: ds.tableInfo.Name, SelectOffset: ds.QueryBlockOffset()}
	}
	if hintTbl := hintInfo.IfPreferTiKV(alias); hintTbl != nil {
		for _, path := range ds.possibleAccessPaths {
			if path.StoreType == kv.TiKV {
				ds.preferStoreType |= h.PreferTiKV
				ds.preferPartitions[h.PreferTiKV] = hintTbl.Partitions
				break
			}
		}
		if ds.preferStoreType&h.PreferTiKV == 0 {
			errMsg := fmt.Sprintf("No available path for table %s.%s with the store type %s of the hint /*+ read_from_storage */, "+
				"please check the status of the table replica and variable value of tidb_isolation_read_engines(%v)",
				ds.DBName.O, ds.table.Meta().Name.O, kv.TiKV.Name(), ds.SCtx().GetSessionVars().GetIsolationReadEngines())
			ds.SCtx().GetSessionVars().StmtCtx.SetHintWarning(errMsg)
		} else {
			ds.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because you have set a hint to read table `" + hintTbl.TblName.O + "` from TiKV.")
		}
	}
	if hintTbl := hintInfo.IfPreferTiFlash(alias); hintTbl != nil {
		// `ds.preferStoreType != 0`, which means there's a hint hit the both TiKV value and TiFlash value for table.
		// We can't support read a table from two different storages, even partition table.
		if ds.preferStoreType != 0 {
			ds.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
				fmt.Sprintf("Storage hints are conflict, you can only specify one storage type of table %s.%s",
					alias.DBName.L, alias.TblName.L))
			ds.preferStoreType = 0
			return
		}
		for _, path := range ds.possibleAccessPaths {
			if path.StoreType == kv.TiFlash {
				ds.preferStoreType |= h.PreferTiFlash
				ds.preferPartitions[h.PreferTiFlash] = hintTbl.Partitions
				break
			}
		}
		if ds.preferStoreType&h.PreferTiFlash == 0 {
			errMsg := fmt.Sprintf("No available path for table %s.%s with the store type %s of the hint /*+ read_from_storage */, "+
				"please check the status of the table replica and variable value of tidb_isolation_read_engines(%v)",
				ds.DBName.O, ds.table.Meta().Name.O, kv.TiFlash.Name(), ds.SCtx().GetSessionVars().GetIsolationReadEngines())
			ds.SCtx().GetSessionVars().StmtCtx.SetHintWarning(errMsg)
		}
	}
}

func resetNotNullFlag(schema *expression.Schema, start, end int) {
	for i := start; i < end; i++ {
		col := *schema.Columns[i]
		newFieldType := *col.RetType
		newFieldType.DelFlag(mysql.NotNullFlag)
		col.RetType = &newFieldType
		schema.Columns[i] = &col
	}
}

func (b *PlanBuilder) buildJoin(ctx context.Context, joinNode *ast.Join) (base.LogicalPlan, error) {
	// We will construct a "Join" node for some statements like "INSERT",
	// "DELETE", "UPDATE", "REPLACE". For this scenario "joinNode.Right" is nil
	// and we only build the left "ResultSetNode".
	if joinNode.Right == nil {
		return b.buildResultSetNode(ctx, joinNode.Left, false)
	}

	b.optFlag = b.optFlag | flagPredicatePushDown
	// Add join reorder flag regardless of inner join or outer join.
	b.optFlag = b.optFlag | flagJoinReOrder
	b.optFlag |= flagPredicateSimplification
	b.optFlag |= flagConvertOuterToInnerJoin

	leftPlan, err := b.buildResultSetNode(ctx, joinNode.Left, false)
	if err != nil {
		return nil, err
	}

	rightPlan, err := b.buildResultSetNode(ctx, joinNode.Right, false)
	if err != nil {
		return nil, err
	}

	// The recursive part in CTE must not be on the right side of a LEFT JOIN.
	if lc, ok := rightPlan.(*LogicalCTETable); ok && joinNode.Tp == ast.LeftJoin {
		return nil, plannererrors.ErrCTERecursiveForbiddenJoinOrder.GenWithStackByArgs(lc.name)
	}

	handleMap1 := b.handleHelper.popMap()
	handleMap2 := b.handleHelper.popMap()
	b.handleHelper.mergeAndPush(handleMap1, handleMap2)

	joinPlan := LogicalJoin{StraightJoin: joinNode.StraightJoin || b.inStraightJoin}.Init(b.ctx, b.getSelectOffset())
	joinPlan.SetChildren(leftPlan, rightPlan)
	joinPlan.SetSchema(expression.MergeSchema(leftPlan.Schema(), rightPlan.Schema()))
	joinPlan.names = make([]*types.FieldName, leftPlan.Schema().Len()+rightPlan.Schema().Len())
	copy(joinPlan.names, leftPlan.OutputNames())
	copy(joinPlan.names[leftPlan.Schema().Len():], rightPlan.OutputNames())

	// Set join type.
	switch joinNode.Tp {
	case ast.LeftJoin:
		// left outer join need to be checked elimination
		b.optFlag = b.optFlag | flagEliminateOuterJoin
		joinPlan.JoinType = LeftOuterJoin
		resetNotNullFlag(joinPlan.schema, leftPlan.Schema().Len(), joinPlan.schema.Len())
	case ast.RightJoin:
		// right outer join need to be checked elimination
		b.optFlag = b.optFlag | flagEliminateOuterJoin
		joinPlan.JoinType = RightOuterJoin
		resetNotNullFlag(joinPlan.schema, 0, leftPlan.Schema().Len())
	default:
		joinPlan.JoinType = InnerJoin
	}

	// Merge sub-plan's fullSchema into this join plan.
	// Please read the comment of LogicalJoin.fullSchema for the details.
	var (
		lFullSchema, rFullSchema *expression.Schema
		lFullNames, rFullNames   types.NameSlice
	)
	if left, ok := leftPlan.(*LogicalJoin); ok && left.fullSchema != nil {
		lFullSchema = left.fullSchema
		lFullNames = left.fullNames
	} else {
		lFullSchema = leftPlan.Schema()
		lFullNames = leftPlan.OutputNames()
	}
	if right, ok := rightPlan.(*LogicalJoin); ok && right.fullSchema != nil {
		rFullSchema = right.fullSchema
		rFullNames = right.fullNames
	} else {
		rFullSchema = rightPlan.Schema()
		rFullNames = rightPlan.OutputNames()
	}
	if joinNode.Tp == ast.RightJoin {
		// Make sure lFullSchema means outer full schema and rFullSchema means inner full schema.
		lFullSchema, rFullSchema = rFullSchema, lFullSchema
		lFullNames, rFullNames = rFullNames, lFullNames
	}
	joinPlan.fullSchema = expression.MergeSchema(lFullSchema, rFullSchema)

	// Clear NotNull flag for the inner side schema if it's an outer join.
	if joinNode.Tp == ast.LeftJoin || joinNode.Tp == ast.RightJoin {
		resetNotNullFlag(joinPlan.fullSchema, lFullSchema.Len(), joinPlan.fullSchema.Len())
	}

	// Merge sub-plan's fullNames into this join plan, similar to the fullSchema logic above.
	joinPlan.fullNames = make([]*types.FieldName, 0, len(lFullNames)+len(rFullNames))
	for _, lName := range lFullNames {
		name := *lName
		joinPlan.fullNames = append(joinPlan.fullNames, &name)
	}
	for _, rName := range rFullNames {
		name := *rName
		joinPlan.fullNames = append(joinPlan.fullNames, &name)
	}

	// Set preferred join algorithm if some join hints is specified by user.
	joinPlan.setPreferredJoinTypeAndOrder(b.TableHints())

	// "NATURAL JOIN" doesn't have "ON" or "USING" conditions.
	//
	// The "NATURAL [LEFT] JOIN" of two tables is defined to be semantically
	// equivalent to an "INNER JOIN" or a "LEFT JOIN" with a "USING" clause
	// that names all columns that exist in both tables.
	//
	// See https://dev.mysql.com/doc/refman/5.7/en/join.html for more detail.
	if joinNode.NaturalJoin {
		err = b.buildNaturalJoin(joinPlan, leftPlan, rightPlan, joinNode)
		if err != nil {
			return nil, err
		}
	} else if joinNode.Using != nil {
		err = b.buildUsingClause(joinPlan, leftPlan, rightPlan, joinNode)
		if err != nil {
			return nil, err
		}
	} else if joinNode.On != nil {
		b.curClause = onClause
		onExpr, newPlan, err := b.rewrite(ctx, joinNode.On.Expr, joinPlan, nil, false)
		if err != nil {
			return nil, err
		}
		if newPlan != joinPlan {
			return nil, errors.New("ON condition doesn't support subqueries yet")
		}
		onCondition := expression.SplitCNFItems(onExpr)
		// Keep these expressions as a LogicalSelection upon the inner join, in order to apply
		// possible decorrelate optimizations. The ON clause is actually treated as a WHERE clause now.
		if joinPlan.JoinType == InnerJoin {
			sel := LogicalSelection{Conditions: onCondition}.Init(b.ctx, b.getSelectOffset())
			sel.SetChildren(joinPlan)
			return sel, nil
		}
		joinPlan.AttachOnConds(onCondition)
	} else if joinPlan.JoinType == InnerJoin {
		// If a inner join without "ON" or "USING" clause, it's a cartesian
		// product over the join tables.
		joinPlan.cartesianJoin = true
	}

	return joinPlan, nil
}

// buildUsingClause eliminate the redundant columns and ordering columns based
// on the "USING" clause.
//
// According to the standard SQL, columns are ordered in the following way:
//  1. coalesced common columns of "leftPlan" and "rightPlan", in the order they
//     appears in "leftPlan".
//  2. the rest columns in "leftPlan", in the order they appears in "leftPlan".
//  3. the rest columns in "rightPlan", in the order they appears in "rightPlan".
func (b *PlanBuilder) buildUsingClause(p *LogicalJoin, leftPlan, rightPlan base.LogicalPlan, join *ast.Join) error {
	filter := make(map[string]bool, len(join.Using))
	for _, col := range join.Using {
		filter[col.Name.L] = true
	}
	err := b.coalesceCommonColumns(p, leftPlan, rightPlan, join.Tp, filter)
	if err != nil {
		return err
	}
	// We do not need to coalesce columns for update and delete.
	if b.inUpdateStmt || b.inDeleteStmt {
		p.setSchemaAndNames(expression.MergeSchema(p.Children()[0].Schema(), p.Children()[1].Schema()),
			append(p.Children()[0].OutputNames(), p.Children()[1].OutputNames()...))
	}
	return nil
}

// buildNaturalJoin builds natural join output schema. It finds out all the common columns
// then using the same mechanism as buildUsingClause to eliminate redundant columns and build join conditions.
// According to standard SQL, producing this display order:
//
//	All the common columns
//	Every column in the first (left) table that is not a common column
//	Every column in the second (right) table that is not a common column
func (b *PlanBuilder) buildNaturalJoin(p *LogicalJoin, leftPlan, rightPlan base.LogicalPlan, join *ast.Join) error {
	err := b.coalesceCommonColumns(p, leftPlan, rightPlan, join.Tp, nil)
	if err != nil {
		return err
	}
	// We do not need to coalesce columns for update and delete.
	if b.inUpdateStmt || b.inDeleteStmt {
		p.setSchemaAndNames(expression.MergeSchema(p.Children()[0].Schema(), p.Children()[1].Schema()),
			append(p.Children()[0].OutputNames(), p.Children()[1].OutputNames()...))
	}
	return nil
}

// coalesceCommonColumns is used by buildUsingClause and buildNaturalJoin. The filter is used by buildUsingClause.
func (b *PlanBuilder) coalesceCommonColumns(p *LogicalJoin, leftPlan, rightPlan base.LogicalPlan, joinTp ast.JoinType, filter map[string]bool) error {
	lsc := leftPlan.Schema().Clone()
	rsc := rightPlan.Schema().Clone()
	if joinTp == ast.LeftJoin {
		resetNotNullFlag(rsc, 0, rsc.Len())
	} else if joinTp == ast.RightJoin {
		resetNotNullFlag(lsc, 0, lsc.Len())
	}
	lColumns, rColumns := lsc.Columns, rsc.Columns
	lNames, rNames := leftPlan.OutputNames().Shallow(), rightPlan.OutputNames().Shallow()
	if joinTp == ast.RightJoin {
		leftPlan, rightPlan = rightPlan, leftPlan
		lNames, rNames = rNames, lNames
		lColumns, rColumns = rsc.Columns, lsc.Columns
	}

	// Check using clause with ambiguous columns.
	if filter != nil {
		checkAmbiguous := func(names types.NameSlice) error {
			columnNameInFilter := set.StringSet{}
			for _, name := range names {
				if _, ok := filter[name.ColName.L]; !ok {
					continue
				}
				if columnNameInFilter.Exist(name.ColName.L) {
					return plannererrors.ErrAmbiguous.GenWithStackByArgs(name.ColName.L, "from clause")
				}
				columnNameInFilter.Insert(name.ColName.L)
			}
			return nil
		}
		err := checkAmbiguous(lNames)
		if err != nil {
			return err
		}
		err = checkAmbiguous(rNames)
		if err != nil {
			return err
		}
	} else {
		// Even with no using filter, we still should check the checkAmbiguous name before we try to find the common column from both side.
		// (t3 cross join t4) natural join t1
		//  t1 natural join (t3 cross join t4)
		// t3 and t4 may generate the same name column from cross join.
		// for every common column of natural join, the name from right or left should be exactly one.
		commonNames := make([]string, 0, len(lNames))
		lNameMap := make(map[string]int, len(lNames))
		rNameMap := make(map[string]int, len(rNames))
		for _, name := range lNames {
			// Natural join should ignore _tidb_rowid
			if name.ColName.L == "_tidb_rowid" {
				continue
			}
			// record left map
			if cnt, ok := lNameMap[name.ColName.L]; ok {
				lNameMap[name.ColName.L] = cnt + 1
			} else {
				lNameMap[name.ColName.L] = 1
			}
		}
		for _, name := range rNames {
			// Natural join should ignore _tidb_rowid
			if name.ColName.L == "_tidb_rowid" {
				continue
			}
			// record right map
			if cnt, ok := rNameMap[name.ColName.L]; ok {
				rNameMap[name.ColName.L] = cnt + 1
			} else {
				rNameMap[name.ColName.L] = 1
			}
			// check left map
			if cnt, ok := lNameMap[name.ColName.L]; ok {
				if cnt > 1 {
					return plannererrors.ErrAmbiguous.GenWithStackByArgs(name.ColName.L, "from clause")
				}
				commonNames = append(commonNames, name.ColName.L)
			}
		}
		// check right map
		for _, commonName := range commonNames {
			if rNameMap[commonName] > 1 {
				return plannererrors.ErrAmbiguous.GenWithStackByArgs(commonName, "from clause")
			}
		}
	}

	// Find out all the common columns and put them ahead.
	commonLen := 0
	for i, lName := range lNames {
		// Natural join should ignore _tidb_rowid
		if lName.ColName.L == "_tidb_rowid" {
			continue
		}
		for j := commonLen; j < len(rNames); j++ {
			if lName.ColName.L != rNames[j].ColName.L {
				continue
			}

			if len(filter) > 0 {
				if !filter[lName.ColName.L] {
					break
				}
				// Mark this column exist.
				filter[lName.ColName.L] = false
			}

			col := lColumns[i]
			copy(lColumns[commonLen+1:i+1], lColumns[commonLen:i])
			lColumns[commonLen] = col

			name := lNames[i]
			copy(lNames[commonLen+1:i+1], lNames[commonLen:i])
			lNames[commonLen] = name

			col = rColumns[j]
			copy(rColumns[commonLen+1:j+1], rColumns[commonLen:j])
			rColumns[commonLen] = col

			name = rNames[j]
			copy(rNames[commonLen+1:j+1], rNames[commonLen:j])
			rNames[commonLen] = name

			commonLen++
			break
		}
	}

	if len(filter) > 0 && len(filter) != commonLen {
		for col, notExist := range filter {
			if notExist {
				return plannererrors.ErrUnknownColumn.GenWithStackByArgs(col, "from clause")
			}
		}
	}

	schemaCols := make([]*expression.Column, len(lColumns)+len(rColumns)-commonLen)
	copy(schemaCols[:len(lColumns)], lColumns)
	copy(schemaCols[len(lColumns):], rColumns[commonLen:])
	names := make(types.NameSlice, len(schemaCols))
	copy(names, lNames)
	copy(names[len(lNames):], rNames[commonLen:])

	conds := make([]expression.Expression, 0, commonLen)
	for i := 0; i < commonLen; i++ {
		lc, rc := lsc.Columns[i], rsc.Columns[i]
		cond, err := expression.NewFunction(b.ctx.GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), lc, rc)
		if err != nil {
			return err
		}
		conds = append(conds, cond)
		if p.fullSchema != nil {
			// since fullSchema is derived from left and right schema in upper layer, so rc/lc must be in fullSchema.
			if joinTp == ast.RightJoin {
				p.fullNames[p.fullSchema.ColumnIndex(lc)].Redundant = true
			} else {
				p.fullNames[p.fullSchema.ColumnIndex(rc)].Redundant = true
			}
		}
	}

	p.SetSchema(expression.NewSchema(schemaCols...))
	p.names = names

	p.OtherConditions = append(conds, p.OtherConditions...)

	return nil
}

func (b *PlanBuilder) buildSelection(ctx context.Context, p base.LogicalPlan, where ast.ExprNode, aggMapper map[*ast.AggregateFuncExpr]int) (base.LogicalPlan, error) {
	b.optFlag |= flagPredicatePushDown
	b.optFlag |= flagDeriveTopNFromWindow
	b.optFlag |= flagPredicateSimplification
	if b.curClause != havingClause {
		b.curClause = whereClause
	}

	conditions := splitWhere(where)
	expressions := make([]expression.Expression, 0, len(conditions))
	selection := LogicalSelection{}.Init(b.ctx, b.getSelectOffset())
	for _, cond := range conditions {
		expr, np, err := b.rewrite(ctx, cond, p, aggMapper, false)
		if err != nil {
			return nil, err
		}
		// for case: explain SELECT year+2 as y, SUM(profit) AS profit FROM sales GROUP BY year+2, year+profit WITH ROLLUP having y > 2002;
		// currently, we succeed to resolve y to (year+2), but fail to resolve (year+2) to grouping col, and to base column function: plus(year, 2) instead.
		// which will cause this selection being pushed down through Expand OP itself.
		//
		// In expand, we will additionally project (year+2) out as a new column, let's say grouping_col here, and we wanna it can substitute any upper layer's (year+2)
		expr = b.replaceGroupingFunc(expr)

		p = np
		if expr == nil {
			continue
		}
		expressions = append(expressions, expr)
	}
	cnfExpres := make([]expression.Expression, 0)
	useCache := b.ctx.GetSessionVars().StmtCtx.UseCache()
	for _, expr := range expressions {
		cnfItems := expression.SplitCNFItems(expr)
		for _, item := range cnfItems {
			if con, ok := item.(*expression.Constant); ok && expression.ConstExprConsiderPlanCache(con, useCache) {
				ret, _, err := expression.EvalBool(b.ctx.GetExprCtx().GetEvalCtx(), expression.CNFExprs{con}, chunk.Row{})
				if err != nil {
					return nil, errors.Trace(err)
				}
				if ret {
					continue
				}
				// If there is condition which is always false, return dual plan directly.
				dual := LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
				dual.names = p.OutputNames()
				dual.SetSchema(p.Schema())
				return dual, nil
			}
			cnfExpres = append(cnfExpres, item)
		}
	}
	if len(cnfExpres) == 0 {
		return p, nil
	}
	evalCtx := b.ctx.GetExprCtx().GetEvalCtx()
	// check expr field types.
	for i, expr := range cnfExpres {
		if expr.GetType(evalCtx).EvalType() == types.ETString {
			tp := &types.FieldType{}
			tp.SetType(mysql.TypeDouble)
			tp.SetFlag(expr.GetType(evalCtx).GetFlag())
			tp.SetFlen(mysql.MaxRealWidth)
			tp.SetDecimal(types.UnspecifiedLength)
			types.SetBinChsClnFlag(tp)
			cnfExpres[i] = expression.TryPushCastIntoControlFunctionForHybridType(b.ctx.GetExprCtx(), expr, tp)
		}
	}
	selection.Conditions = cnfExpres
	selection.SetChildren(p)
	return selection, nil
}

// buildProjectionFieldNameFromColumns builds the field name, table name and database name when field expression is a column reference.
func (*PlanBuilder) buildProjectionFieldNameFromColumns(origField *ast.SelectField, colNameField *ast.ColumnNameExpr, name *types.FieldName) (colName, origColName, tblName, origTblName, dbName model.CIStr) {
	origTblName, origColName, dbName = name.OrigTblName, name.OrigColName, name.DBName
	if origField.AsName.L == "" {
		colName = colNameField.Name.Name
	} else {
		colName = origField.AsName
	}
	if tblName.L == "" {
		tblName = name.TblName
	} else {
		tblName = colNameField.Name.Table
	}
	return
}

// buildProjectionFieldNameFromExpressions builds the field name when field expression is a normal expression.
func (b *PlanBuilder) buildProjectionFieldNameFromExpressions(_ context.Context, field *ast.SelectField) (model.CIStr, error) {
	if agg, ok := field.Expr.(*ast.AggregateFuncExpr); ok && agg.F == ast.AggFuncFirstRow {
		// When the query is select t.a from t group by a; The Column Name should be a but not t.a;
		return agg.Args[0].(*ast.ColumnNameExpr).Name.Name, nil
	}

	innerExpr := getInnerFromParenthesesAndUnaryPlus(field.Expr)
	funcCall, isFuncCall := innerExpr.(*ast.FuncCallExpr)
	// When used to produce a result set column, NAME_CONST() causes the column to have the given name.
	// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_name-const for details
	if isFuncCall && funcCall.FnName.L == ast.NameConst {
		if v, err := evalAstExpr(b.ctx.GetExprCtx(), funcCall.Args[0]); err == nil {
			if s, err := v.ToString(); err == nil {
				return model.NewCIStr(s), nil
			}
		}
		return model.NewCIStr(""), plannererrors.ErrWrongArguments.GenWithStackByArgs("NAME_CONST")
	}
	valueExpr, isValueExpr := innerExpr.(*driver.ValueExpr)

	// Non-literal: Output as inputed, except that comments need to be removed.
	if !isValueExpr {
		return model.NewCIStr(parser.SpecFieldPattern.ReplaceAllStringFunc(field.Text(), parser.TrimComment)), nil
	}

	// Literal: Need special processing
	switch valueExpr.Kind() {
	case types.KindString:
		projName := valueExpr.GetString()
		projOffset := valueExpr.GetProjectionOffset()
		if projOffset >= 0 {
			projName = projName[:projOffset]
		}
		// See #3686, #3994:
		// For string literals, string content is used as column name. Non-graph initial characters are trimmed.
		fieldName := strings.TrimLeftFunc(projName, func(r rune) bool {
			return !unicode.IsOneOf(mysql.RangeGraph, r)
		})
		return model.NewCIStr(fieldName), nil
	case types.KindNull:
		// See #4053, #3685
		return model.NewCIStr("NULL"), nil
	case types.KindBinaryLiteral:
		// Don't rewrite BIT literal or HEX literals
		return model.NewCIStr(field.Text()), nil
	case types.KindInt64:
		// See #9683
		// TRUE or FALSE can be a int64
		if mysql.HasIsBooleanFlag(valueExpr.Type.GetFlag()) {
			if i := valueExpr.GetValue().(int64); i == 0 {
				return model.NewCIStr("FALSE"), nil
			}
			return model.NewCIStr("TRUE"), nil
		}
		fallthrough

	default:
		fieldName := field.Text()
		fieldName = strings.TrimLeft(fieldName, "\t\n +(")
		fieldName = strings.TrimRight(fieldName, "\t\n )")
		return model.NewCIStr(fieldName), nil
	}
}

func buildExpandFieldName(expr expression.Expression, name *types.FieldName, genName string) *types.FieldName {
	_, isCol := expr.(*expression.Column)
	var origTblName, origColName, dbName, colName, tblName model.CIStr
	if genName != "" {
		// for case like: gid_, gpos_
		colName = model.NewCIStr(expr.String())
	} else if isCol {
		// col ref to original col, while its nullability may be changed.
		origTblName, origColName, dbName = name.OrigTblName, name.OrigColName, name.DBName
		colName = model.NewCIStr("ex_" + name.ColName.O)
		tblName = model.NewCIStr("ex_" + name.TblName.O)
	} else {
		// Other: complicated expression.
		colName = model.NewCIStr("ex_" + expr.String())
	}
	newName := &types.FieldName{
		TblName:     tblName,
		OrigTblName: origTblName,
		ColName:     colName,
		OrigColName: origColName,
		DBName:      dbName,
	}
	return newName
}

// buildProjectionField builds the field object according to SelectField in projection.
func (b *PlanBuilder) buildProjectionField(ctx context.Context, p base.LogicalPlan, field *ast.SelectField, expr expression.Expression) (*expression.Column, *types.FieldName, error) {
	var origTblName, tblName, origColName, colName, dbName model.CIStr
	innerNode := getInnerFromParenthesesAndUnaryPlus(field.Expr)
	col, isCol := expr.(*expression.Column)
	// Correlated column won't affect the final output names. So we can put it in any of the three logic block.
	// Don't put it into the first block just for simplifying the codes.
	if colNameField, ok := innerNode.(*ast.ColumnNameExpr); ok && isCol {
		// Field is a column reference.
		idx := p.Schema().ColumnIndex(col)
		var name *types.FieldName
		// The column maybe the one from join's redundant part.
		if idx == -1 {
			name = findColFromNaturalUsingJoin(p, col)
		} else {
			name = p.OutputNames()[idx]
		}
		colName, origColName, tblName, origTblName, dbName = b.buildProjectionFieldNameFromColumns(field, colNameField, name)
	} else if field.AsName.L != "" {
		// Field has alias.
		colName = field.AsName
	} else {
		// Other: field is an expression.
		var err error
		if colName, err = b.buildProjectionFieldNameFromExpressions(ctx, field); err != nil {
			return nil, nil, err
		}
	}
	name := &types.FieldName{
		TblName:     tblName,
		OrigTblName: origTblName,
		ColName:     colName,
		OrigColName: origColName,
		DBName:      dbName,
	}
	if isCol {
		return col, name, nil
	}
	if expr == nil {
		return nil, name, nil
	}
	// invalid unique id
	correlatedColUniqueID := int64(0)
	if cc, ok := expr.(*expression.CorrelatedColumn); ok {
		correlatedColUniqueID = cc.UniqueID
	}
	// for expr projection, we should record the map relationship <hashcode, uniqueID> down.
	newCol := &expression.Column{
		UniqueID:              b.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:               expr.GetType(b.ctx.GetExprCtx().GetEvalCtx()),
		CorrelatedColUniqueID: correlatedColUniqueID,
	}
	if b.ctx.GetSessionVars().OptimizerEnableNewOnlyFullGroupByCheck {
		if b.ctx.GetSessionVars().MapHashCode2UniqueID4ExtendedCol == nil {
			b.ctx.GetSessionVars().MapHashCode2UniqueID4ExtendedCol = make(map[string]int, 1)
		}
		b.ctx.GetSessionVars().MapHashCode2UniqueID4ExtendedCol[string(expr.HashCode())] = int(newCol.UniqueID)
	}
	newCol.SetCoercibility(expr.Coercibility())
	return newCol, name, nil
}

type userVarTypeProcessor struct {
	ctx     context.Context
	plan    base.LogicalPlan
	builder *PlanBuilder
	mapper  map[*ast.AggregateFuncExpr]int
	err     error
}

func (p *userVarTypeProcessor) Enter(in ast.Node) (ast.Node, bool) {
	v, ok := in.(*ast.VariableExpr)
	if !ok {
		return in, false
	}
	if v.IsSystem || v.Value == nil {
		return in, true
	}
	_, p.plan, p.err = p.builder.rewrite(p.ctx, v, p.plan, p.mapper, true)
	return in, true
}

func (p *userVarTypeProcessor) Leave(in ast.Node) (ast.Node, bool) {
	return in, p.err == nil
}

func (b *PlanBuilder) preprocessUserVarTypes(ctx context.Context, p base.LogicalPlan, fields []*ast.SelectField, mapper map[*ast.AggregateFuncExpr]int) error {
	aggMapper := make(map[*ast.AggregateFuncExpr]int)
	for agg, i := range mapper {
		aggMapper[agg] = i
	}
	processor := userVarTypeProcessor{
		ctx:     ctx,
		plan:    p,
		builder: b,
		mapper:  aggMapper,
	}
	for _, field := range fields {
		field.Expr.Accept(&processor)
		if processor.err != nil {
			return processor.err
		}
	}
	return nil
}

// findColFromNaturalUsingJoin is used to recursively find the column from the
// underlying natural-using-join.
// e.g. For SQL like `select t2.a from t1 join t2 using(a) where t2.a > 0`, the
// plan will be `join->selection->projection`. The schema of the `selection`
// will be `[t1.a]`, thus we need to recursively retrieve the `t2.a` from the
// underlying join.
func findColFromNaturalUsingJoin(p base.LogicalPlan, col *expression.Column) (name *types.FieldName) {
	switch x := p.(type) {
	case *LogicalLimit, *LogicalSelection, *LogicalTopN, *LogicalSort, *LogicalMaxOneRow:
		return findColFromNaturalUsingJoin(p.Children()[0], col)
	case *LogicalJoin:
		if x.fullSchema != nil {
			idx := x.fullSchema.ColumnIndex(col)
			return x.fullNames[idx]
		}
	}
	return nil
}

type resolveGroupingTraverseAction struct {
	CurrentBlockExpand *LogicalExpand
}

func (r resolveGroupingTraverseAction) Transform(expr expression.Expression) (res expression.Expression) {
	switch x := expr.(type) {
	case *expression.Column:
		// when meeting a column, judge whether it's a relate grouping set col.
		// eg: select a, b from t group by a, c with rollup, here a is, while b is not.
		// in underlying Expand schema (a,b,c,a',c'), a select list should be resolved to a'.
		res, _ = r.CurrentBlockExpand.trySubstituteExprWithGroupingSetCol(x)
	case *expression.CorrelatedColumn:
		// select 1 in (select t2.a from t group by t2.a, b with rollup) from t2;
		// in this case: group by item has correlated column t2.a, and it's select list contains t2.a as well.
		res, _ = r.CurrentBlockExpand.trySubstituteExprWithGroupingSetCol(x)
	case *expression.Constant:
		// constant just keep it real: select 1 from t group by a, b with rollup.
		res = x
	case *expression.ScalarFunction:
		// scalar function just try to resolve itself first, then if not changed, trying resolve its children.
		var substituted bool
		res, substituted = r.CurrentBlockExpand.trySubstituteExprWithGroupingSetCol(x)
		if !substituted {
			// if not changed, try to resolve it children.
			// select a+1, grouping(b) from t group by a+1 (projected as c), b with rollup: in this case, a+1 is resolved as c as a whole.
			// select a+1, grouping(b) from t group by a(projected as a'), b with rollup  : in this case, a+1 is resolved as a'+ 1.
			newArgs := x.GetArgs()
			for i, arg := range newArgs {
				newArgs[i] = r.Transform(arg)
			}
			res = x
		}
	default:
		res = expr
	}
	return res
}

func (b *PlanBuilder) replaceGroupingFunc(expr expression.Expression) expression.Expression {
	// current block doesn't have an expand OP, just return it.
	if b.currentBlockExpand == nil {
		return expr
	}
	// curExpand can supply the distinctGbyExprs and gid col.
	traverseAction := resolveGroupingTraverseAction{CurrentBlockExpand: b.currentBlockExpand}
	return expr.Traverse(traverseAction)
}

func (b *PlanBuilder) implicitProjectGroupingSetCols(projSchema *expression.Schema, projNames []*types.FieldName, projExprs []expression.Expression) (*expression.Schema, []*types.FieldName, []expression.Expression) {
	if b.currentBlockExpand == nil {
		return projSchema, projNames, projExprs
	}
	m := make(map[int64]struct{}, len(b.currentBlockExpand.distinctGroupByCol))
	for _, col := range projSchema.Columns {
		m[col.UniqueID] = struct{}{}
	}
	for idx, gCol := range b.currentBlockExpand.distinctGroupByCol {
		if _, ok := m[gCol.UniqueID]; ok {
			// grouping col has been explicitly projected, not need to reserve it here for later order-by item (a+1)
			// like: select a+1, b from t group by a+1 order by a+1.
			continue
		}
		// project the grouping col out implicitly here. If it's not used by later OP, it will be cleaned in column pruner.
		projSchema.Append(gCol)
		projExprs = append(projExprs, gCol)
		projNames = append(projNames, b.currentBlockExpand.distinctGbyColNames[idx])
	}
	// project GID.
	projSchema.Append(b.currentBlockExpand.GID)
	projExprs = append(projExprs, b.currentBlockExpand.GID)
	projNames = append(projNames, b.currentBlockExpand.GIDName)
	// project GPos if any.
	if b.currentBlockExpand.GPos != nil {
		projSchema.Append(b.currentBlockExpand.GPos)
		projExprs = append(projExprs, b.currentBlockExpand.GPos)
		projNames = append(projNames, b.currentBlockExpand.GPosName)
	}
	return projSchema, projNames, projExprs
}

// buildProjection returns a Projection plan and non-aux columns length.
func (b *PlanBuilder) buildProjection(ctx context.Context, p base.LogicalPlan, fields []*ast.SelectField, mapper map[*ast.AggregateFuncExpr]int,
	windowMapper map[*ast.WindowFuncExpr]int, considerWindow bool, expandGenerateColumn bool) (base.LogicalPlan, []expression.Expression, int, error) {
	err := b.preprocessUserVarTypes(ctx, p, fields, mapper)
	if err != nil {
		return nil, nil, 0, err
	}
	b.optFlag |= flagEliminateProjection
	b.curClause = fieldList
	proj := LogicalProjection{Exprs: make([]expression.Expression, 0, len(fields))}.Init(b.ctx, b.getSelectOffset())
	schema := expression.NewSchema(make([]*expression.Column, 0, len(fields))...)
	oldLen := 0
	newNames := make([]*types.FieldName, 0, len(fields))
	for i, field := range fields {
		if !field.Auxiliary {
			oldLen++
		}

		isWindowFuncField := ast.HasWindowFlag(field.Expr)
		// Although window functions occurs in the select fields, but it has to be processed after having clause.
		// So when we build the projection for select fields, we need to skip the window function.
		// When `considerWindow` is false, we will only build fields for non-window functions, so we add fake placeholders.
		// for window functions. These fake placeholders will be erased in column pruning.
		// When `considerWindow` is true, all the non-window fields have been built, so we just use the schema columns.
		if considerWindow && !isWindowFuncField {
			col := p.Schema().Columns[i]
			proj.Exprs = append(proj.Exprs, col)
			schema.Append(col)
			newNames = append(newNames, p.OutputNames()[i])
			continue
		} else if !considerWindow && isWindowFuncField {
			expr := expression.NewZero()
			proj.Exprs = append(proj.Exprs, expr)
			col, name, err := b.buildProjectionField(ctx, p, field, expr)
			if err != nil {
				return nil, nil, 0, err
			}
			schema.Append(col)
			newNames = append(newNames, name)
			continue
		}
		newExpr, np, err := b.rewriteWithPreprocess(ctx, field.Expr, p, mapper, windowMapper, true, nil)
		if err != nil {
			return nil, nil, 0, err
		}

		// for case: select a+1, b, sum(b), grouping(a) from t group by a, b with rollup.
		// the column inside aggregate (only sum(b) here) should be resolved to original source column,
		// while for others, just use expanded columns if exists: a'+ 1, b', group(gid)
		newExpr = b.replaceGroupingFunc(newExpr)

		// For window functions in the order by clause, we will append an field for it.
		// We need rewrite the window mapper here so order by clause could find the added field.
		if considerWindow && isWindowFuncField && field.Auxiliary {
			if windowExpr, ok := field.Expr.(*ast.WindowFuncExpr); ok {
				windowMapper[windowExpr] = i
			}
		}

		p = np
		proj.Exprs = append(proj.Exprs, newExpr)

		col, name, err := b.buildProjectionField(ctx, p, field, newExpr)
		if err != nil {
			return nil, nil, 0, err
		}
		schema.Append(col)
		newNames = append(newNames, name)
	}
	// implicitly project expand grouping set cols, if not used later, it will being pruned out in logical column pruner.
	schema, newNames, proj.Exprs = b.implicitProjectGroupingSetCols(schema, newNames, proj.Exprs)

	proj.SetSchema(schema)
	proj.names = newNames
	if expandGenerateColumn {
		// Sometimes we need to add some fields to the projection so that we can use generate column substitute
		// optimization. For example: select a+1 from t order by a+1, with a virtual generate column c as (a+1) and
		// an index on c. We need to add c into the projection so that we can replace a+1 with c.
		exprToColumn := make(ExprColumnMap)
		collectGenerateColumn(p, exprToColumn)
		for expr, col := range exprToColumn {
			idx := p.Schema().ColumnIndex(col)
			if idx == -1 {
				continue
			}
			if proj.schema.Contains(col) {
				continue
			}
			proj.schema.Columns = append(proj.schema.Columns, col)
			proj.Exprs = append(proj.Exprs, expr)
			proj.names = append(proj.names, p.OutputNames()[idx])
		}
	}
	proj.SetChildren(p)
	// delay the only-full-group-by-check in create view statement to later query.
	if !b.isCreateView && b.ctx.GetSessionVars().OptimizerEnableNewOnlyFullGroupByCheck && b.ctx.GetSessionVars().SQLMode.HasOnlyFullGroupBy() {
		fds := proj.ExtractFD()
		// Projection -> Children -> ...
		// Let the projection itself to evaluate the whole FD, which will build the connection
		// 1: from select-expr to registered-expr
		// 2: from base-column to select-expr
		// After that
		if fds.HasAggBuilt {
			for offset, expr := range proj.Exprs[:len(fields)] {
				// skip the auxiliary column in agg appended to select fields, which mainly comes from two kind of cases:
				// 1: having agg(t.a), this will append t.a to the select fields, if it isn't here.
				// 2: order by agg(t.a), this will append t.a to the select fields, if it isn't here.
				if fields[offset].AuxiliaryColInAgg {
					continue
				}
				item := intset.NewFastIntSet()
				switch x := expr.(type) {
				case *expression.Column:
					item.Insert(int(x.UniqueID))
				case *expression.ScalarFunction:
					if expression.CheckFuncInExpr(x, ast.AnyValue) {
						continue
					}
					scalarUniqueID, ok := fds.IsHashCodeRegistered(string(hack.String(x.HashCode())))
					if !ok {
						logutil.BgLogger().Warn("Error occurred while maintaining the functional dependency")
						continue
					}
					item.Insert(scalarUniqueID)
				default:
				}
				// Rule #1, if there are no group cols, the col in the order by shouldn't be limited.
				if fds.GroupByCols.Only1Zero() && fields[offset].AuxiliaryColInOrderBy {
					continue
				}

				// Rule #2, if select fields are constant, it's ok.
				if item.SubsetOf(fds.ConstantCols()) {
					continue
				}

				// Rule #3, if select fields are subset of group by items, it's ok.
				if item.SubsetOf(fds.GroupByCols) {
					continue
				}

				// Rule #4, if select fields are dependencies of Strict FD with determinants in group-by items, it's ok.
				// lax FD couldn't be done here, eg: for unique key (b), index key NULL & NULL are different rows with
				// uncertain other column values.
				strictClosure := fds.ClosureOfStrict(fds.GroupByCols)
				if item.SubsetOf(strictClosure) {
					continue
				}
				// locate the base col that are not in (constant list / group by list / strict fd closure) for error show.
				baseCols := expression.ExtractColumns(expr)
				errShowCol := baseCols[0]
				for _, col := range baseCols {
					colSet := intset.NewFastIntSet(int(col.UniqueID))
					if !colSet.SubsetOf(strictClosure) {
						errShowCol = col
						break
					}
				}
				// better use the schema alias name firstly if any.
				name := ""
				for idx, schemaCol := range proj.Schema().Columns {
					if schemaCol.UniqueID == errShowCol.UniqueID {
						name = proj.names[idx].String()
						break
					}
				}
				if name == "" {
					name = errShowCol.OrigName
				}
				// Only1Zero is to judge whether it's no-group-by-items case.
				if !fds.GroupByCols.Only1Zero() {
					return nil, nil, 0, plannererrors.ErrFieldNotInGroupBy.GenWithStackByArgs(offset+1, ErrExprInSelect, name)
				}
				return nil, nil, 0, plannererrors.ErrMixOfGroupFuncAndFields.GenWithStackByArgs(offset+1, name)
			}
			if fds.GroupByCols.Only1Zero() {
				// maxOneRow is delayed from agg's ExtractFD logic since some details listed in it.
				projectionUniqueIDs := intset.NewFastIntSet()
				for _, expr := range proj.Exprs {
					switch x := expr.(type) {
					case *expression.Column:
						projectionUniqueIDs.Insert(int(x.UniqueID))
					case *expression.ScalarFunction:
						scalarUniqueID, ok := fds.IsHashCodeRegistered(string(hack.String(x.HashCode())))
						if !ok {
							logutil.BgLogger().Warn("Error occurred while maintaining the functional dependency")
							continue
						}
						projectionUniqueIDs.Insert(scalarUniqueID)
					}
				}
				fds.MaxOneRow(projectionUniqueIDs)
			}
			// for select * from view (include agg), outer projection don't have to check select list with the inner group-by flag.
			fds.HasAggBuilt = false
		}
	}
	return proj, proj.Exprs, oldLen, nil
}

func (b *PlanBuilder) buildDistinct(child base.LogicalPlan, length int) (*LogicalAggregation, error) {
	b.optFlag = b.optFlag | flagBuildKeyInfo
	b.optFlag = b.optFlag | flagPushDownAgg
	plan4Agg := LogicalAggregation{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, child.Schema().Len()),
		GroupByItems: expression.Column2Exprs(child.Schema().Clone().Columns[:length]),
	}.Init(b.ctx, child.QueryBlockOffset())
	if hintinfo := b.TableHints(); hintinfo != nil {
		plan4Agg.PreferAggType = hintinfo.PreferAggType
		plan4Agg.PreferAggToCop = hintinfo.PreferAggToCop
	}
	for _, col := range child.Schema().Columns {
		aggDesc, err := aggregation.NewAggFuncDesc(b.ctx.GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{col}, false)
		if err != nil {
			return nil, err
		}
		plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, aggDesc)
	}
	plan4Agg.SetChildren(child)
	plan4Agg.SetSchema(child.Schema().Clone())
	plan4Agg.names = child.OutputNames()
	// Distinct will be rewritten as first_row, we reset the type here since the return type
	// of first_row is not always the same as the column arg of first_row.
	for i, col := range plan4Agg.schema.Columns {
		col.RetType = plan4Agg.AggFuncs[i].RetTp
	}
	return plan4Agg, nil
}

// unionJoinFieldType finds the type which can carry the given types in Union.
// Note that unionJoinFieldType doesn't handle charset and collation, caller need to handle it by itself.
func unionJoinFieldType(a, b *types.FieldType) *types.FieldType {
	// We ignore the pure NULL type.
	if a.GetType() == mysql.TypeNull {
		return b
	} else if b.GetType() == mysql.TypeNull {
		return a
	}
	resultTp := types.AggFieldType([]*types.FieldType{a, b})
	// This logic will be intelligible when it is associated with the buildProjection4Union logic.
	if resultTp.GetType() == mysql.TypeNewDecimal {
		// The decimal result type will be unsigned only when all the decimals to be united are unsigned.
		resultTp.AndFlag(b.GetFlag() & mysql.UnsignedFlag)
	} else {
		// Non-decimal results will be unsigned when a,b both unsigned.
		// ref1: https://dev.mysql.com/doc/refman/5.7/en/union.html#union-result-set
		// ref2: https://github.com/pingcap/tidb/issues/24953
		resultTp.AddFlag((a.GetFlag() & mysql.UnsignedFlag) & (b.GetFlag() & mysql.UnsignedFlag))
	}
	resultTp.SetDecimalUnderLimit(max(a.GetDecimal(), b.GetDecimal()))
	// `flen - decimal` is the fraction before '.'
	if a.GetFlen() == -1 || b.GetFlen() == -1 {
		resultTp.SetFlenUnderLimit(-1)
	} else {
		resultTp.SetFlenUnderLimit(max(a.GetFlen()-a.GetDecimal(), b.GetFlen()-b.GetDecimal()) + resultTp.GetDecimal())
	}
	types.TryToFixFlenOfDatetime(resultTp)
	if resultTp.EvalType() != types.ETInt && (a.EvalType() == types.ETInt || b.EvalType() == types.ETInt) && resultTp.GetFlen() < mysql.MaxIntWidth {
		resultTp.SetFlen(mysql.MaxIntWidth)
	}
	expression.SetBinFlagOrBinStr(b, resultTp)
	return resultTp
}

// Set the flen of the union column using the max flen in children.
func (b *PlanBuilder) setUnionFlen(resultTp *types.FieldType, cols []expression.Expression) {
	if resultTp.GetFlen() == -1 {
		return
	}
	isBinary := resultTp.GetCharset() == charset.CharsetBin
	for i := 0; i < len(cols); i++ {
		childTp := cols[i].GetType(b.ctx.GetExprCtx().GetEvalCtx())
		childTpCharLen := 1
		if isBinary {
			if charsetInfo, ok := charset.CharacterSetInfos[childTp.GetCharset()]; ok {
				childTpCharLen = charsetInfo.Maxlen
			}
		}
		resultTp.SetFlen(max(resultTp.GetFlen(), childTpCharLen*childTp.GetFlen()))
	}
}

func (b *PlanBuilder) buildProjection4Union(_ context.Context, u *LogicalUnionAll) error {
	unionCols := make([]*expression.Column, 0, u.Children()[0].Schema().Len())
	names := make([]*types.FieldName, 0, u.Children()[0].Schema().Len())

	// Infer union result types by its children's schema.
	for i, col := range u.Children()[0].Schema().Columns {
		tmpExprs := make([]expression.Expression, 0, len(u.Children()))
		tmpExprs = append(tmpExprs, col)
		resultTp := col.RetType
		for j := 1; j < len(u.Children()); j++ {
			tmpExprs = append(tmpExprs, u.Children()[j].Schema().Columns[i])
			childTp := u.Children()[j].Schema().Columns[i].RetType
			resultTp = unionJoinFieldType(resultTp, childTp)
		}
		collation, err := expression.CheckAndDeriveCollationFromExprs(b.ctx.GetExprCtx(), "UNION", resultTp.EvalType(), tmpExprs...)
		if err != nil || collation.Coer == expression.CoercibilityNone {
			return collate.ErrIllegalMixCollation.GenWithStackByArgs("UNION")
		}
		resultTp.SetCharset(collation.Charset)
		resultTp.SetCollate(collation.Collation)
		b.setUnionFlen(resultTp, tmpExprs)
		names = append(names, &types.FieldName{ColName: u.Children()[0].OutputNames()[i].ColName})
		unionCols = append(unionCols, &expression.Column{
			RetType:  resultTp,
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
		})
	}
	u.schema = expression.NewSchema(unionCols...)
	u.names = names
	// Process each child and add a projection above original child.
	// So the schema of `UnionAll` can be the same with its children's.
	for childID, child := range u.Children() {
		exprs := make([]expression.Expression, len(child.Schema().Columns))
		for i, srcCol := range child.Schema().Columns {
			dstType := unionCols[i].RetType
			srcType := srcCol.RetType
			if !srcType.Equal(dstType) {
				exprs[i] = expression.BuildCastFunction4Union(b.ctx.GetExprCtx(), srcCol, dstType)
			} else {
				exprs[i] = srcCol
			}
		}
		b.optFlag |= flagEliminateProjection
		proj := LogicalProjection{Exprs: exprs, AvoidColumnEvaluator: true}.Init(b.ctx, b.getSelectOffset())
		proj.SetSchema(u.schema.Clone())
		// reset the schema type to make the "not null" flag right.
		for i, expr := range exprs {
			proj.schema.Columns[i].RetType = expr.GetType(b.ctx.GetExprCtx().GetEvalCtx())
		}
		proj.SetChildren(child)
		u.Children()[childID] = proj
	}
	return nil
}

func (b *PlanBuilder) buildSetOpr(ctx context.Context, setOpr *ast.SetOprStmt) (base.LogicalPlan, error) {
	if setOpr.With != nil {
		l := len(b.outerCTEs)
		defer func() {
			b.outerCTEs = b.outerCTEs[:l]
		}()
		_, err := b.buildWith(ctx, setOpr.With)
		if err != nil {
			return nil, err
		}
	}

	// Because INTERSECT has higher precedence than UNION and EXCEPT. We build it first.
	selectPlans := make([]base.LogicalPlan, 0, len(setOpr.SelectList.Selects))
	afterSetOprs := make([]*ast.SetOprType, 0, len(setOpr.SelectList.Selects))
	selects := setOpr.SelectList.Selects
	for i := 0; i < len(selects); i++ {
		intersects := []ast.Node{selects[i]}
		for i+1 < len(selects) {
			breakIteration := false
			switch x := selects[i+1].(type) {
			case *ast.SelectStmt:
				if *x.AfterSetOperator != ast.Intersect && *x.AfterSetOperator != ast.IntersectAll {
					breakIteration = true
				}
			case *ast.SetOprSelectList:
				if *x.AfterSetOperator != ast.Intersect && *x.AfterSetOperator != ast.IntersectAll {
					breakIteration = true
				}
				if x.Limit != nil || x.OrderBy != nil {
					// when SetOprSelectList's limit and order-by is not nil, it means itself is converted from
					// an independent ast.SetOprStmt in parser, its data should be evaluated first, and ordered
					// by given items and conduct a limit on it, then it can only be integrated with other brothers.
					breakIteration = true
				}
			}
			if breakIteration {
				break
			}
			intersects = append(intersects, selects[i+1])
			i++
		}
		selectPlan, afterSetOpr, err := b.buildIntersect(ctx, intersects)
		if err != nil {
			return nil, err
		}
		selectPlans = append(selectPlans, selectPlan)
		afterSetOprs = append(afterSetOprs, afterSetOpr)
	}
	setOprPlan, err := b.buildExcept(ctx, selectPlans, afterSetOprs)
	if err != nil {
		return nil, err
	}

	oldLen := setOprPlan.Schema().Len()

	for i := 0; i < len(setOpr.SelectList.Selects); i++ {
		b.handleHelper.popMap()
	}
	b.handleHelper.pushMap(nil)

	if setOpr.OrderBy != nil {
		setOprPlan, err = b.buildSort(ctx, setOprPlan, setOpr.OrderBy.Items, nil, nil)
		if err != nil {
			return nil, err
		}
	}

	if setOpr.Limit != nil {
		setOprPlan, err = b.buildLimit(setOprPlan, setOpr.Limit)
		if err != nil {
			return nil, err
		}
	}

	// Fix issue #8189 (https://github.com/pingcap/tidb/issues/8189).
	// If there are extra expressions generated from `ORDER BY` clause, generate a `Projection` to remove them.
	if oldLen != setOprPlan.Schema().Len() {
		proj := LogicalProjection{Exprs: expression.Column2Exprs(setOprPlan.Schema().Columns[:oldLen])}.Init(b.ctx, b.getSelectOffset())
		proj.SetChildren(setOprPlan)
		schema := expression.NewSchema(setOprPlan.Schema().Clone().Columns[:oldLen]...)
		for _, col := range schema.Columns {
			col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
		}
		proj.names = setOprPlan.OutputNames()[:oldLen]
		proj.SetSchema(schema)
		return proj, nil
	}
	return setOprPlan, nil
}

func (b *PlanBuilder) buildSemiJoinForSetOperator(
	leftOriginPlan base.LogicalPlan,
	rightPlan base.LogicalPlan,
	joinType JoinType) (leftPlan base.LogicalPlan, err error) {
	leftPlan, err = b.buildDistinct(leftOriginPlan, leftOriginPlan.Schema().Len())
	if err != nil {
		return nil, err
	}
	b.optFlag |= flagConvertOuterToInnerJoin

	joinPlan := LogicalJoin{JoinType: joinType}.Init(b.ctx, b.getSelectOffset())
	joinPlan.SetChildren(leftPlan, rightPlan)
	joinPlan.SetSchema(leftPlan.Schema())
	joinPlan.names = make([]*types.FieldName, leftPlan.Schema().Len())
	copy(joinPlan.names, leftPlan.OutputNames())
	for j := 0; j < len(rightPlan.Schema().Columns); j++ {
		leftCol, rightCol := leftPlan.Schema().Columns[j], rightPlan.Schema().Columns[j]
		eqCond, err := expression.NewFunction(b.ctx.GetExprCtx(), ast.NullEQ, types.NewFieldType(mysql.TypeTiny), leftCol, rightCol)
		if err != nil {
			return nil, err
		}
		_, leftArgIsColumn := eqCond.(*expression.ScalarFunction).GetArgs()[0].(*expression.Column)
		_, rightArgIsColumn := eqCond.(*expression.ScalarFunction).GetArgs()[1].(*expression.Column)
		if leftCol.RetType.GetType() != rightCol.RetType.GetType() || !leftArgIsColumn || !rightArgIsColumn {
			joinPlan.OtherConditions = append(joinPlan.OtherConditions, eqCond)
		} else {
			joinPlan.EqualConditions = append(joinPlan.EqualConditions, eqCond.(*expression.ScalarFunction))
		}
	}
	return joinPlan, nil
}

// buildIntersect build the set operator for 'intersect'. It is called before buildExcept and buildUnion because of its
// higher precedence.
func (b *PlanBuilder) buildIntersect(ctx context.Context, selects []ast.Node) (base.LogicalPlan, *ast.SetOprType, error) {
	var leftPlan base.LogicalPlan
	var err error
	var afterSetOperator *ast.SetOprType
	switch x := selects[0].(type) {
	case *ast.SelectStmt:
		afterSetOperator = x.AfterSetOperator
		leftPlan, err = b.buildSelect(ctx, x)
	case *ast.SetOprSelectList:
		afterSetOperator = x.AfterSetOperator
		leftPlan, err = b.buildSetOpr(ctx, &ast.SetOprStmt{SelectList: x, With: x.With, Limit: x.Limit, OrderBy: x.OrderBy})
	}
	if err != nil {
		return nil, nil, err
	}
	if len(selects) == 1 {
		return leftPlan, afterSetOperator, nil
	}

	columnNums := leftPlan.Schema().Len()
	for i := 1; i < len(selects); i++ {
		var rightPlan base.LogicalPlan
		switch x := selects[i].(type) {
		case *ast.SelectStmt:
			if *x.AfterSetOperator == ast.IntersectAll {
				// TODO: support intersect all
				return nil, nil, errors.Errorf("TiDB do not support intersect all")
			}
			rightPlan, err = b.buildSelect(ctx, x)
		case *ast.SetOprSelectList:
			if *x.AfterSetOperator == ast.IntersectAll {
				// TODO: support intersect all
				return nil, nil, errors.Errorf("TiDB do not support intersect all")
			}
			rightPlan, err = b.buildSetOpr(ctx, &ast.SetOprStmt{SelectList: x, With: x.With, Limit: x.Limit, OrderBy: x.OrderBy})
		}
		if err != nil {
			return nil, nil, err
		}
		if rightPlan.Schema().Len() != columnNums {
			return nil, nil, plannererrors.ErrWrongNumberOfColumnsInSelect.GenWithStackByArgs()
		}
		leftPlan, err = b.buildSemiJoinForSetOperator(leftPlan, rightPlan, SemiJoin)
		if err != nil {
			return nil, nil, err
		}
	}
	return leftPlan, afterSetOperator, nil
}

// buildExcept build the set operators for 'except', and in this function, it calls buildUnion at the same time. Because
// Union and except has the same precedence.
func (b *PlanBuilder) buildExcept(ctx context.Context, selects []base.LogicalPlan, afterSetOpts []*ast.SetOprType) (base.LogicalPlan, error) {
	unionPlans := []base.LogicalPlan{selects[0]}
	tmpAfterSetOpts := []*ast.SetOprType{nil}
	columnNums := selects[0].Schema().Len()
	for i := 1; i < len(selects); i++ {
		rightPlan := selects[i]
		if rightPlan.Schema().Len() != columnNums {
			return nil, plannererrors.ErrWrongNumberOfColumnsInSelect.GenWithStackByArgs()
		}
		if *afterSetOpts[i] == ast.Except {
			leftPlan, err := b.buildUnion(ctx, unionPlans, tmpAfterSetOpts)
			if err != nil {
				return nil, err
			}
			leftPlan, err = b.buildSemiJoinForSetOperator(leftPlan, rightPlan, AntiSemiJoin)
			if err != nil {
				return nil, err
			}
			unionPlans = []base.LogicalPlan{leftPlan}
			tmpAfterSetOpts = []*ast.SetOprType{nil}
		} else if *afterSetOpts[i] == ast.ExceptAll {
			// TODO: support except all.
			return nil, errors.Errorf("TiDB do not support except all")
		} else {
			unionPlans = append(unionPlans, rightPlan)
			tmpAfterSetOpts = append(tmpAfterSetOpts, afterSetOpts[i])
		}
	}
	return b.buildUnion(ctx, unionPlans, tmpAfterSetOpts)
}

func (b *PlanBuilder) buildUnion(ctx context.Context, selects []base.LogicalPlan, afterSetOpts []*ast.SetOprType) (base.LogicalPlan, error) {
	if len(selects) == 1 {
		return selects[0], nil
	}
	distinctSelectPlans, allSelectPlans, err := b.divideUnionSelectPlans(ctx, selects, afterSetOpts)
	if err != nil {
		return nil, err
	}
	unionDistinctPlan, err := b.buildUnionAll(ctx, distinctSelectPlans)
	if err != nil {
		return nil, err
	}
	if unionDistinctPlan != nil {
		unionDistinctPlan, err = b.buildDistinct(unionDistinctPlan, unionDistinctPlan.Schema().Len())
		if err != nil {
			return nil, err
		}
		if len(allSelectPlans) > 0 {
			// Can't change the statements order in order to get the correct column info.
			allSelectPlans = append([]base.LogicalPlan{unionDistinctPlan}, allSelectPlans...)
		}
	}

	unionAllPlan, err := b.buildUnionAll(ctx, allSelectPlans)
	if err != nil {
		return nil, err
	}
	unionPlan := unionDistinctPlan
	if unionAllPlan != nil {
		unionPlan = unionAllPlan
	}

	return unionPlan, nil
}

// divideUnionSelectPlans resolves union's select stmts to logical plans.
// and divide result plans into "union-distinct" and "union-all" parts.
// divide rule ref:
//
//	https://dev.mysql.com/doc/refman/5.7/en/union.html
//
// "Mixed UNION types are treated such that a DISTINCT union overrides any ALL union to its left."
func (*PlanBuilder) divideUnionSelectPlans(_ context.Context, selects []base.LogicalPlan, setOprTypes []*ast.SetOprType) (distinctSelects []base.LogicalPlan, allSelects []base.LogicalPlan, err error) {
	firstUnionAllIdx := 0
	columnNums := selects[0].Schema().Len()
	for i := len(selects) - 1; i > 0; i-- {
		if firstUnionAllIdx == 0 && *setOprTypes[i] != ast.UnionAll {
			firstUnionAllIdx = i + 1
		}
		if selects[i].Schema().Len() != columnNums {
			return nil, nil, plannererrors.ErrWrongNumberOfColumnsInSelect.GenWithStackByArgs()
		}
	}
	return selects[:firstUnionAllIdx], selects[firstUnionAllIdx:], nil
}

func (b *PlanBuilder) buildUnionAll(ctx context.Context, subPlan []base.LogicalPlan) (base.LogicalPlan, error) {
	if len(subPlan) == 0 {
		return nil, nil
	}
	u := LogicalUnionAll{}.Init(b.ctx, b.getSelectOffset())
	u.SetChildren(subPlan...)
	err := b.buildProjection4Union(ctx, u)
	return u, err
}

// itemTransformer transforms ParamMarkerExpr to PositionExpr in the context of ByItem
type itemTransformer struct{}

func (*itemTransformer) Enter(inNode ast.Node) (ast.Node, bool) {
	if n, ok := inNode.(*driver.ParamMarkerExpr); ok {
		newNode := expression.ConstructPositionExpr(n)
		return newNode, true
	}
	return inNode, false
}

func (*itemTransformer) Leave(inNode ast.Node) (ast.Node, bool) {
	return inNode, false
}

func (b *PlanBuilder) buildSort(ctx context.Context, p base.LogicalPlan, byItems []*ast.ByItem, aggMapper map[*ast.AggregateFuncExpr]int, windowMapper map[*ast.WindowFuncExpr]int) (*LogicalSort, error) {
	return b.buildSortWithCheck(ctx, p, byItems, aggMapper, windowMapper, nil, 0, false)
}

func (b *PlanBuilder) buildSortWithCheck(ctx context.Context, p base.LogicalPlan, byItems []*ast.ByItem, aggMapper map[*ast.AggregateFuncExpr]int, windowMapper map[*ast.WindowFuncExpr]int,
	projExprs []expression.Expression, oldLen int, hasDistinct bool) (*LogicalSort, error) {
	if _, isUnion := p.(*LogicalUnionAll); isUnion {
		b.curClause = globalOrderByClause
	} else {
		b.curClause = orderByClause
	}
	sort := LogicalSort{}.Init(b.ctx, b.getSelectOffset())
	exprs := make([]*util.ByItems, 0, len(byItems))
	transformer := &itemTransformer{}
	for i, item := range byItems {
		newExpr, _ := item.Expr.Accept(transformer)
		item.Expr = newExpr.(ast.ExprNode)
		it, np, err := b.rewriteWithPreprocess(ctx, item.Expr, p, aggMapper, windowMapper, true, nil)
		if err != nil {
			return nil, err
		}
		// for case: select a+1, b, sum(b) from t group by a+1, b with rollup order by a+1.
		// currently, we fail to resolve (a+1) in order-by to projection item (a+1), and adding
		// another a' in the select fields instead, leading finally resolved expr is a'+1 here.
		//
		// Anyway, a and a' has the same column unique id, so we can do the replacement work like
		// we did in build projection phase.
		it = b.replaceGroupingFunc(it)

		// check whether ORDER BY items show up in SELECT DISTINCT fields, see #12442
		if hasDistinct && projExprs != nil {
			err = b.checkOrderByInDistinct(item, i, it, p, projExprs, oldLen)
			if err != nil {
				return nil, err
			}
		}

		p = np
		exprs = append(exprs, &util.ByItems{Expr: it, Desc: item.Desc})
	}
	sort.ByItems = exprs
	sort.SetChildren(p)
	return sort, nil
}

// checkOrderByInDistinct checks whether ORDER BY has conflicts with DISTINCT, see #12442
func (b *PlanBuilder) checkOrderByInDistinct(byItem *ast.ByItem, idx int, expr expression.Expression, p base.LogicalPlan, originalExprs []expression.Expression, length int) error {
	// Check if expressions in ORDER BY whole match some fields in DISTINCT.
	// e.g.
	// select distinct count(a) from t group by b order by count(a);  ✔
	// select distinct a+1 from t order by a+1;                       ✔
	// select distinct a+1 from t order by a+2;                       ✗
	evalCtx := b.ctx.GetExprCtx().GetEvalCtx()
	for j := 0; j < length; j++ {
		// both check original expression & as name
		if expr.Equal(evalCtx, originalExprs[j]) || expr.Equal(evalCtx, p.Schema().Columns[j]) {
			return nil
		}
	}

	// Check if referenced columns of expressions in ORDER BY whole match some fields in DISTINCT,
	// both original expression and alias can be referenced.
	// e.g.
	// select distinct a from t order by sin(a);                            ✔
	// select distinct a, b from t order by a+b;                            ✔
	// select distinct count(a), sum(a) from t group by b order by sum(a);  ✔
	cols := expression.ExtractColumns(expr)
CheckReferenced:
	for _, col := range cols {
		for j := 0; j < length; j++ {
			if col.Equal(evalCtx, originalExprs[j]) || col.Equal(evalCtx, p.Schema().Columns[j]) {
				continue CheckReferenced
			}
		}

		// Failed cases
		// e.g.
		// select distinct sin(a) from t order by a;                            ✗
		// select distinct a from t order by a+b;                               ✗
		if _, ok := byItem.Expr.(*ast.AggregateFuncExpr); ok {
			return plannererrors.ErrAggregateInOrderNotSelect.GenWithStackByArgs(idx+1, "DISTINCT")
		}
		// select distinct count(a) from t group by b order by sum(a);          ✗
		return plannererrors.ErrFieldInOrderNotSelect.GenWithStackByArgs(idx+1, col.OrigName, "DISTINCT")
	}
	return nil
}

// getUintFromNode gets uint64 value from ast.Node.
// For ordinary statement, node should be uint64 constant value.
// For prepared statement, node is string. We should convert it to uint64.
func getUintFromNode(ctx base.PlanContext, n ast.Node, mustInt64orUint64 bool) (uVal uint64, isNull bool, isExpectedType bool) {
	var val any
	switch v := n.(type) {
	case *driver.ValueExpr:
		val = v.GetValue()
	case *driver.ParamMarkerExpr:
		if !v.InExecute {
			return 0, false, true
		}
		if mustInt64orUint64 {
			if expected, _ := CheckParamTypeInt64orUint64(v); !expected {
				return 0, false, false
			}
		}
		param, err := expression.ParamMarkerExpression(ctx, v, false)
		if err != nil {
			return 0, false, false
		}
		str, isNull, err := expression.GetStringFromConstant(ctx.GetExprCtx().GetEvalCtx(), param)
		if err != nil {
			return 0, false, false
		}
		if isNull {
			return 0, true, true
		}
		val = str
	default:
		return 0, false, false
	}
	switch v := val.(type) {
	case uint64:
		return v, false, true
	case int64:
		if v >= 0 {
			return uint64(v), false, true
		}
	case string:
		ctx := ctx.GetSessionVars().StmtCtx.TypeCtx()
		uVal, err := types.StrToUint(ctx, v, false)
		if err != nil {
			return 0, false, false
		}
		return uVal, false, true
	}
	return 0, false, false
}

// CheckParamTypeInt64orUint64 check param type for plan cache limit, only allow int64 and uint64 now
// eg: set @a = 1;
func CheckParamTypeInt64orUint64(param *driver.ParamMarkerExpr) (bool, uint64) {
	val := param.GetValue()
	switch v := val.(type) {
	case int64:
		if v >= 0 {
			return true, uint64(v)
		}
	case uint64:
		return true, v
	}
	return false, 0
}

func extractLimitCountOffset(ctx base.PlanContext, limit *ast.Limit) (count uint64,
	offset uint64, err error) {
	var isExpectedType bool
	if limit.Count != nil {
		count, _, isExpectedType = getUintFromNode(ctx, limit.Count, true)
		if !isExpectedType {
			return 0, 0, plannererrors.ErrWrongArguments.GenWithStackByArgs("LIMIT")
		}
	}
	if limit.Offset != nil {
		offset, _, isExpectedType = getUintFromNode(ctx, limit.Offset, true)
		if !isExpectedType {
			return 0, 0, plannererrors.ErrWrongArguments.GenWithStackByArgs("LIMIT")
		}
	}
	return count, offset, nil
}

func (b *PlanBuilder) buildLimit(src base.LogicalPlan, limit *ast.Limit) (base.LogicalPlan, error) {
	b.optFlag = b.optFlag | flagPushDownTopN
	var (
		offset, count uint64
		err           error
	)
	if count, offset, err = extractLimitCountOffset(b.ctx, limit); err != nil {
		return nil, err
	}

	if count > math.MaxUint64-offset {
		count = math.MaxUint64 - offset
	}
	if offset+count == 0 {
		tableDual := LogicalTableDual{RowCount: 0}.Init(b.ctx, b.getSelectOffset())
		tableDual.schema = src.Schema()
		tableDual.names = src.OutputNames()
		return tableDual, nil
	}
	li := LogicalLimit{
		Offset: offset,
		Count:  count,
	}.Init(b.ctx, b.getSelectOffset())
	if hint := b.TableHints(); hint != nil {
		li.PreferLimitToCop = hint.PreferLimitToCop
	}
	li.SetChildren(src)
	return li, nil
}

func resolveFromSelectFields(v *ast.ColumnNameExpr, fields []*ast.SelectField, ignoreAsName bool) (index int, err error) {
	var matchedExpr ast.ExprNode
	index = -1
	for i, field := range fields {
		if field.Auxiliary {
			continue
		}
		if field.Match(v, ignoreAsName) {
			curCol, isCol := field.Expr.(*ast.ColumnNameExpr)
			if !isCol {
				return i, nil
			}
			if matchedExpr == nil {
				matchedExpr = curCol
				index = i
			} else if !matchedExpr.(*ast.ColumnNameExpr).Name.Match(curCol.Name) &&
				!curCol.Name.Match(matchedExpr.(*ast.ColumnNameExpr).Name) {
				return -1, plannererrors.ErrAmbiguous.GenWithStackByArgs(curCol.Name.Name.L, clauseMsg[fieldList])
			}
		}
	}
	return
}

// havingWindowAndOrderbyExprResolver visits Expr tree.
// It converts ColumnNameExpr to AggregateFuncExpr and collects AggregateFuncExpr.
type havingWindowAndOrderbyExprResolver struct {
	inAggFunc    bool
	inWindowFunc bool
	inWindowSpec bool
	inExpr       bool
	err          error
	p            base.LogicalPlan
	selectFields []*ast.SelectField
	aggMapper    map[*ast.AggregateFuncExpr]int
	colMapper    map[*ast.ColumnNameExpr]int
	gbyItems     []*ast.ByItem
	outerSchemas []*expression.Schema
	outerNames   [][]*types.FieldName
	curClause    clauseCode
	prevClause   []clauseCode
}

func (a *havingWindowAndOrderbyExprResolver) pushCurClause(newClause clauseCode) {
	a.prevClause = append(a.prevClause, a.curClause)
	a.curClause = newClause
}

func (a *havingWindowAndOrderbyExprResolver) popCurClause() {
	a.curClause = a.prevClause[len(a.prevClause)-1]
	a.prevClause = a.prevClause[:len(a.prevClause)-1]
}

// Enter implements Visitor interface.
func (a *havingWindowAndOrderbyExprResolver) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggFunc = true
	case *ast.WindowFuncExpr:
		a.inWindowFunc = true
	case *ast.WindowSpec:
		a.inWindowSpec = true
	case *driver.ParamMarkerExpr, *ast.ColumnNameExpr, *ast.ColumnName:
	case *ast.SubqueryExpr, *ast.ExistsSubqueryExpr:
		// Enter a new context, skip it.
		// For example: select sum(c) + c + exists(select c from t) from t;
		return n, true
	case *ast.PartitionByClause:
		a.pushCurClause(partitionByClause)
	case *ast.OrderByClause:
		if a.inWindowSpec {
			a.pushCurClause(windowOrderByClause)
		}
	default:
		a.inExpr = true
	}
	return n, false
}

func (a *havingWindowAndOrderbyExprResolver) resolveFromPlan(v *ast.ColumnNameExpr, p base.LogicalPlan, resolveFieldsFirst bool) (int, error) {
	idx, err := expression.FindFieldName(p.OutputNames(), v.Name)
	if err != nil {
		return -1, err
	}
	schemaCols, outputNames := p.Schema().Columns, p.OutputNames()
	if idx < 0 {
		// For SQL like `select t2.a from t1 join t2 using(a) where t2.a > 0
		// order by t2.a`, the query plan will be `join->selection->sort`. The
		// schema of selection will be `[t1.a]`, thus we need to recursively
		// retrieve the `t2.a` from the underlying join.
		switch x := p.(type) {
		case *LogicalLimit, *LogicalSelection, *LogicalTopN, *LogicalSort, *LogicalMaxOneRow:
			return a.resolveFromPlan(v, p.Children()[0], resolveFieldsFirst)
		case *LogicalJoin:
			if len(x.fullNames) != 0 {
				idx, err = expression.FindFieldName(x.fullNames, v.Name)
				schemaCols, outputNames = x.fullSchema.Columns, x.fullNames
			}
		}
		if err != nil || idx < 0 {
			// nowhere to be found.
			return -1, err
		}
	}
	col := schemaCols[idx]
	if col.IsHidden {
		return -1, plannererrors.ErrUnknownColumn.GenWithStackByArgs(v.Name, clauseMsg[a.curClause])
	}
	name := outputNames[idx]
	newColName := &ast.ColumnName{
		Schema: name.DBName,
		Table:  name.TblName,
		Name:   name.ColName,
	}
	for i, field := range a.selectFields {
		if c, ok := field.Expr.(*ast.ColumnNameExpr); ok && c.Name.Match(newColName) {
			return i, nil
		}
	}
	// From https://github.com/pingcap/tidb/issues/51107
	// You should make the column in the having clause as the correlated column
	// which is not relation with select's fields and GroupBy's fields.
	// For SQLs like:
	//     SELECT * FROM `t1` WHERE NOT (`t1`.`col_1`>= (
	//          SELECT `t2`.`col_7`
	//             FROM (`t1`)
	//             JOIN `t2`
	//             WHERE ISNULL(`t2`.`col_3`) HAVING `t1`.`col_6`>1951988)
	//     ) ;
	//
	// if resolveFieldsFirst is false, the groupby is not nil.
	if resolveFieldsFirst && a.curClause == havingClause {
		return -1, nil
	}
	sf := &ast.SelectField{
		Expr:      &ast.ColumnNameExpr{Name: newColName},
		Auxiliary: true,
	}
	// appended with new select fields. set them with flag.
	if a.inAggFunc {
		// should skip check in FD for only full group by.
		sf.AuxiliaryColInAgg = true
	} else if a.curClause == orderByClause {
		// should skip check in FD for only full group by only when group by item are empty.
		sf.AuxiliaryColInOrderBy = true
	}
	sf.Expr.SetType(col.GetStaticType())
	a.selectFields = append(a.selectFields, sf)
	return len(a.selectFields) - 1, nil
}

// Leave implements Visitor interface.
func (a *havingWindowAndOrderbyExprResolver) Leave(n ast.Node) (node ast.Node, ok bool) {
	switch v := n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggFunc = false
		a.aggMapper[v] = len(a.selectFields)
		a.selectFields = append(a.selectFields, &ast.SelectField{
			Auxiliary: true,
			Expr:      v,
			AsName:    model.NewCIStr(fmt.Sprintf("sel_agg_%d", len(a.selectFields))),
		})
	case *ast.WindowFuncExpr:
		a.inWindowFunc = false
		if a.curClause == havingClause {
			a.err = plannererrors.ErrWindowInvalidWindowFuncUse.GenWithStackByArgs(strings.ToLower(v.Name))
			return node, false
		}
		if a.curClause == orderByClause {
			a.selectFields = append(a.selectFields, &ast.SelectField{
				Auxiliary: true,
				Expr:      v,
				AsName:    model.NewCIStr(fmt.Sprintf("sel_window_%d", len(a.selectFields))),
			})
		}
	case *ast.WindowSpec:
		a.inWindowSpec = false
	case *ast.PartitionByClause:
		a.popCurClause()
	case *ast.OrderByClause:
		if a.inWindowSpec {
			a.popCurClause()
		}
	case *ast.ColumnNameExpr:
		resolveFieldsFirst := true
		if a.inAggFunc || a.inWindowFunc || a.inWindowSpec || (a.curClause == orderByClause && a.inExpr) || a.curClause == fieldList {
			resolveFieldsFirst = false
		}
		if !a.inAggFunc && a.curClause != orderByClause {
			for _, item := range a.gbyItems {
				if col, ok := item.Expr.(*ast.ColumnNameExpr); ok &&
					(v.Name.Match(col.Name) || col.Name.Match(v.Name)) {
					resolveFieldsFirst = false
					break
				}
			}
		}
		var index int
		if resolveFieldsFirst {
			index, a.err = resolveFromSelectFields(v, a.selectFields, false)
			if a.err != nil {
				return node, false
			}
			if index != -1 && a.curClause == havingClause && ast.HasWindowFlag(a.selectFields[index].Expr) {
				a.err = plannererrors.ErrWindowInvalidWindowFuncAliasUse.GenWithStackByArgs(v.Name.Name.O)
				return node, false
			}
			if index == -1 {
				if a.curClause == orderByClause {
					index, a.err = a.resolveFromPlan(v, a.p, resolveFieldsFirst)
				} else if a.curClause == havingClause && v.Name.Table.L != "" {
					// For SQLs like:
					//   select a from t b having b.a;
					index, a.err = a.resolveFromPlan(v, a.p, resolveFieldsFirst)
					if a.err != nil {
						return node, false
					}
					if index != -1 {
						// For SQLs like:
						//   select a+1 from t having t.a;
						field := a.selectFields[index]
						if field.Auxiliary { // having can't use auxiliary field
							index = -1
						}
					}
				} else {
					index, a.err = resolveFromSelectFields(v, a.selectFields, true)
				}
			}
		} else {
			// We should ignore the err when resolving from schema. Because we could resolve successfully
			// when considering select fields.
			var err error
			index, err = a.resolveFromPlan(v, a.p, resolveFieldsFirst)
			_ = err
			if index == -1 && a.curClause != fieldList &&
				a.curClause != windowOrderByClause && a.curClause != partitionByClause {
				index, a.err = resolveFromSelectFields(v, a.selectFields, false)
				if index != -1 && a.curClause == havingClause && ast.HasWindowFlag(a.selectFields[index].Expr) {
					a.err = plannererrors.ErrWindowInvalidWindowFuncAliasUse.GenWithStackByArgs(v.Name.Name.O)
					return node, false
				}
			}
		}
		if a.err != nil {
			return node, false
		}
		if index == -1 {
			// If we can't find it any where, it may be a correlated columns.
			for _, names := range a.outerNames {
				idx, err1 := expression.FindFieldName(names, v.Name)
				if err1 != nil {
					a.err = err1
					return node, false
				}
				if idx >= 0 {
					return n, true
				}
			}
			a.err = plannererrors.ErrUnknownColumn.GenWithStackByArgs(v.Name.OrigColName(), clauseMsg[a.curClause])
			return node, false
		}
		if a.inAggFunc {
			return a.selectFields[index].Expr, true
		}
		a.colMapper[v] = index
	}
	return n, true
}

// resolveHavingAndOrderBy will process aggregate functions and resolve the columns that don't exist in select fields.
// If we found some columns that are not in select fields, we will append it to select fields and update the colMapper.
// When we rewrite the order by / having expression, we will find column in map at first.
func (b *PlanBuilder) resolveHavingAndOrderBy(ctx context.Context, sel *ast.SelectStmt, p base.LogicalPlan) (
	map[*ast.AggregateFuncExpr]int, map[*ast.AggregateFuncExpr]int, error) {
	extractor := &havingWindowAndOrderbyExprResolver{
		p:            p,
		selectFields: sel.Fields.Fields,
		aggMapper:    make(map[*ast.AggregateFuncExpr]int),
		colMapper:    b.colMapper,
		outerSchemas: b.outerSchemas,
		outerNames:   b.outerNames,
	}
	if sel.GroupBy != nil {
		extractor.gbyItems = sel.GroupBy.Items
	}
	// Extract agg funcs from having clause.
	if sel.Having != nil {
		extractor.curClause = havingClause
		n, ok := sel.Having.Expr.Accept(extractor)
		if !ok {
			return nil, nil, errors.Trace(extractor.err)
		}
		sel.Having.Expr = n.(ast.ExprNode)
	}
	havingAggMapper := extractor.aggMapper
	extractor.aggMapper = make(map[*ast.AggregateFuncExpr]int)
	// Extract agg funcs from order by clause.
	if sel.OrderBy != nil {
		extractor.curClause = orderByClause
		for _, item := range sel.OrderBy.Items {
			extractor.inExpr = false
			if ast.HasWindowFlag(item.Expr) {
				continue
			}
			n, ok := item.Expr.Accept(extractor)
			if !ok {
				return nil, nil, errors.Trace(extractor.err)
			}
			item.Expr = n.(ast.ExprNode)
		}
	}
	sel.Fields.Fields = extractor.selectFields
	// this part is used to fetch correlated column from sub-query item in order-by clause, and append the origin
	// auxiliary select filed in select list, otherwise, sub-query itself won't get the name resolved in outer schema.
	if sel.OrderBy != nil {
		for _, byItem := range sel.OrderBy.Items {
			if _, ok := byItem.Expr.(*ast.SubqueryExpr); ok {
				// correlated agg will be extracted completely latter.
				_, np, err := b.rewrite(ctx, byItem.Expr, p, nil, true)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
				correlatedCols := coreusage.ExtractCorrelatedCols4LogicalPlan(np)
				for _, cone := range correlatedCols {
					var colName *ast.ColumnName
					for idx, pone := range p.Schema().Columns {
						if cone.UniqueID == pone.UniqueID {
							pname := p.OutputNames()[idx]
							colName = &ast.ColumnName{
								Schema: pname.DBName,
								Table:  pname.TblName,
								Name:   pname.ColName,
							}
							break
						}
					}
					if colName != nil {
						columnNameExpr := &ast.ColumnNameExpr{Name: colName}
						for _, field := range sel.Fields.Fields {
							if c, ok := field.Expr.(*ast.ColumnNameExpr); ok && c.Name.Match(columnNameExpr.Name) && field.AsName.L == "" {
								// deduplicate select fields: don't append it once it already has one.
								// TODO: we add the field if it has alias, but actually they are the same column. We should not have two duplicate one.
								columnNameExpr = nil
								break
							}
						}
						if columnNameExpr != nil {
							sel.Fields.Fields = append(sel.Fields.Fields, &ast.SelectField{
								Auxiliary: true,
								Expr:      columnNameExpr,
							})
						}
					}
				}
			}
		}
	}
	return havingAggMapper, extractor.aggMapper, nil
}

func (b *PlanBuilder) extractAggFuncsInExprs(exprs []ast.ExprNode) ([]*ast.AggregateFuncExpr, map[*ast.AggregateFuncExpr]int) {
	extractor := &AggregateFuncExtractor{skipAggMap: b.correlatedAggMapper}
	for _, expr := range exprs {
		expr.Accept(extractor)
	}
	aggList := extractor.AggFuncs
	totalAggMapper := make(map[*ast.AggregateFuncExpr]int, len(aggList))

	for i, agg := range aggList {
		totalAggMapper[agg] = i
	}
	return aggList, totalAggMapper
}

func (b *PlanBuilder) extractAggFuncsInSelectFields(fields []*ast.SelectField) ([]*ast.AggregateFuncExpr, map[*ast.AggregateFuncExpr]int) {
	extractor := &AggregateFuncExtractor{skipAggMap: b.correlatedAggMapper}
	for _, f := range fields {
		n, _ := f.Expr.Accept(extractor)
		f.Expr = n.(ast.ExprNode)
	}
	aggList := extractor.AggFuncs
	totalAggMapper := make(map[*ast.AggregateFuncExpr]int, len(aggList))

	for i, agg := range aggList {
		totalAggMapper[agg] = i
	}
	return aggList, totalAggMapper
}

func (b *PlanBuilder) extractAggFuncsInByItems(byItems []*ast.ByItem) []*ast.AggregateFuncExpr {
	extractor := &AggregateFuncExtractor{skipAggMap: b.correlatedAggMapper}
	for _, f := range byItems {
		n, _ := f.Expr.Accept(extractor)
		f.Expr = n.(ast.ExprNode)
	}
	return extractor.AggFuncs
}

// extractCorrelatedAggFuncs extracts correlated aggregates which belong to outer query from aggregate function list.
func (b *PlanBuilder) extractCorrelatedAggFuncs(ctx context.Context, p base.LogicalPlan, aggFuncs []*ast.AggregateFuncExpr) (outer []*ast.AggregateFuncExpr, err error) {
	corCols := make([]*expression.CorrelatedColumn, 0, len(aggFuncs))
	cols := make([]*expression.Column, 0, len(aggFuncs))
	aggMapper := make(map[*ast.AggregateFuncExpr]int)
	for _, agg := range aggFuncs {
		for _, arg := range agg.Args {
			expr, _, err := b.rewrite(ctx, arg, p, aggMapper, true)
			if err != nil {
				return nil, err
			}
			corCols = append(corCols, expression.ExtractCorColumns(expr)...)
			cols = append(cols, expression.ExtractColumns(expr)...)
		}
		if len(corCols) > 0 && len(cols) == 0 {
			outer = append(outer, agg)
		}
		aggMapper[agg] = -1
		corCols, cols = corCols[:0], cols[:0]
	}
	return
}

// resolveWindowFunction will process window functions and resolve the columns that don't exist in select fields.
func (b *PlanBuilder) resolveWindowFunction(sel *ast.SelectStmt, p base.LogicalPlan) (
	map[*ast.AggregateFuncExpr]int, error) {
	extractor := &havingWindowAndOrderbyExprResolver{
		p:            p,
		selectFields: sel.Fields.Fields,
		aggMapper:    make(map[*ast.AggregateFuncExpr]int),
		colMapper:    b.colMapper,
		outerSchemas: b.outerSchemas,
		outerNames:   b.outerNames,
	}
	extractor.curClause = fieldList
	for _, field := range sel.Fields.Fields {
		if !ast.HasWindowFlag(field.Expr) {
			continue
		}
		n, ok := field.Expr.Accept(extractor)
		if !ok {
			return nil, extractor.err
		}
		field.Expr = n.(ast.ExprNode)
	}
	for _, spec := range sel.WindowSpecs {
		_, ok := spec.Accept(extractor)
		if !ok {
			return nil, extractor.err
		}
	}
	if sel.OrderBy != nil {
		extractor.curClause = orderByClause
		for _, item := range sel.OrderBy.Items {
			if !ast.HasWindowFlag(item.Expr) {
				continue
			}
			n, ok := item.Expr.Accept(extractor)
			if !ok {
				return nil, extractor.err
			}
			item.Expr = n.(ast.ExprNode)
		}
	}
	sel.Fields.Fields = extractor.selectFields
	return extractor.aggMapper, nil
}

// correlatedAggregateResolver visits Expr tree.
// It finds and collects all correlated aggregates which should be evaluated in the outer query.
type correlatedAggregateResolver struct {
	ctx       context.Context
	err       error
	b         *PlanBuilder
	outerPlan base.LogicalPlan

	// correlatedAggFuncs stores aggregate functions which belong to outer query
	correlatedAggFuncs []*ast.AggregateFuncExpr
}

// Enter implements Visitor interface.
func (r *correlatedAggregateResolver) Enter(n ast.Node) (ast.Node, bool) {
	if v, ok := n.(*ast.SelectStmt); ok {
		if r.outerPlan != nil {
			outerSchema := r.outerPlan.Schema()
			r.b.outerSchemas = append(r.b.outerSchemas, outerSchema)
			r.b.outerNames = append(r.b.outerNames, r.outerPlan.OutputNames())
			r.b.outerBlockExpand = append(r.b.outerBlockExpand, r.b.currentBlockExpand)
		}
		r.err = r.resolveSelect(v)
		return n, true
	}
	return n, false
}

// resolveSelect finds and collects correlated aggregates within the SELECT stmt.
// It resolves and builds FROM clause first to get a source plan, from which we can decide
// whether a column is correlated or not.
// Then it collects correlated aggregate from SELECT fields (including sub-queries), HAVING,
// ORDER BY, WHERE & GROUP BY.
// Finally it restore the original SELECT stmt.
func (r *correlatedAggregateResolver) resolveSelect(sel *ast.SelectStmt) (err error) {
	if sel.With != nil {
		l := len(r.b.outerCTEs)
		defer func() {
			r.b.outerCTEs = r.b.outerCTEs[:l]
		}()
		_, err := r.b.buildWith(r.ctx, sel.With)
		if err != nil {
			return err
		}
	}
	// collect correlated aggregate from sub-queries inside FROM clause.
	if err := r.collectFromTableRefs(sel.From); err != nil {
		return err
	}
	p, err := r.b.buildTableRefs(r.ctx, sel.From)
	if err != nil {
		return err
	}

	// similar to process in PlanBuilder.buildSelect
	originalFields := sel.Fields.Fields
	sel.Fields.Fields, err = r.b.unfoldWildStar(p, sel.Fields.Fields)
	if err != nil {
		return err
	}
	if r.b.capFlag&canExpandAST != 0 {
		originalFields = sel.Fields.Fields
	}

	hasWindowFuncField := r.b.detectSelectWindow(sel)
	if hasWindowFuncField {
		_, err = r.b.resolveWindowFunction(sel, p)
		if err != nil {
			return err
		}
	}

	_, _, err = r.b.resolveHavingAndOrderBy(r.ctx, sel, p)
	if err != nil {
		return err
	}

	// find and collect correlated aggregates recursively in sub-queries
	_, err = r.b.resolveCorrelatedAggregates(r.ctx, sel, p)
	if err != nil {
		return err
	}

	// collect from SELECT fields, HAVING, ORDER BY and window functions
	if r.b.detectSelectAgg(sel) {
		err = r.collectFromSelectFields(p, sel.Fields.Fields)
		if err != nil {
			return err
		}
	}

	// collect from WHERE
	err = r.collectFromWhere(p, sel.Where)
	if err != nil {
		return err
	}

	// collect from GROUP BY
	err = r.collectFromGroupBy(p, sel.GroupBy)
	if err != nil {
		return err
	}

	// restore the sub-query
	sel.Fields.Fields = originalFields
	r.b.handleHelper.popMap()
	return nil
}

func (r *correlatedAggregateResolver) collectFromTableRefs(from *ast.TableRefsClause) error {
	if from == nil {
		return nil
	}
	subResolver := &correlatedAggregateResolver{
		ctx: r.ctx,
		b:   r.b,
	}
	_, ok := from.TableRefs.Accept(subResolver)
	if !ok {
		return subResolver.err
	}
	if len(subResolver.correlatedAggFuncs) == 0 {
		return nil
	}
	r.correlatedAggFuncs = append(r.correlatedAggFuncs, subResolver.correlatedAggFuncs...)
	return nil
}

func (r *correlatedAggregateResolver) collectFromSelectFields(p base.LogicalPlan, fields []*ast.SelectField) error {
	aggList, _ := r.b.extractAggFuncsInSelectFields(fields)
	r.b.curClause = fieldList
	outerAggFuncs, err := r.b.extractCorrelatedAggFuncs(r.ctx, p, aggList)
	if err != nil {
		return nil
	}
	r.correlatedAggFuncs = append(r.correlatedAggFuncs, outerAggFuncs...)
	return nil
}

func (r *correlatedAggregateResolver) collectFromGroupBy(p base.LogicalPlan, groupBy *ast.GroupByClause) error {
	if groupBy == nil {
		return nil
	}
	aggList := r.b.extractAggFuncsInByItems(groupBy.Items)
	r.b.curClause = groupByClause
	outerAggFuncs, err := r.b.extractCorrelatedAggFuncs(r.ctx, p, aggList)
	if err != nil {
		return nil
	}
	r.correlatedAggFuncs = append(r.correlatedAggFuncs, outerAggFuncs...)
	return nil
}

func (r *correlatedAggregateResolver) collectFromWhere(p base.LogicalPlan, where ast.ExprNode) error {
	if where == nil {
		return nil
	}
	extractor := &AggregateFuncExtractor{skipAggMap: r.b.correlatedAggMapper}
	_, _ = where.Accept(extractor)
	r.b.curClause = whereClause
	outerAggFuncs, err := r.b.extractCorrelatedAggFuncs(r.ctx, p, extractor.AggFuncs)
	if err != nil {
		return err
	}
	r.correlatedAggFuncs = append(r.correlatedAggFuncs, outerAggFuncs...)
	return nil
}

// Leave implements Visitor interface.
func (r *correlatedAggregateResolver) Leave(n ast.Node) (ast.Node, bool) {
	if _, ok := n.(*ast.SelectStmt); ok {
		if r.outerPlan != nil {
			r.b.outerSchemas = r.b.outerSchemas[0 : len(r.b.outerSchemas)-1]
			r.b.outerNames = r.b.outerNames[0 : len(r.b.outerNames)-1]
			r.b.currentBlockExpand = r.b.outerBlockExpand[len(r.b.outerBlockExpand)-1]
			r.b.outerBlockExpand = r.b.outerBlockExpand[0 : len(r.b.outerBlockExpand)-1]
		}
	}
	return n, r.err == nil
}

// resolveCorrelatedAggregates finds and collects all correlated aggregates which should be evaluated
// in the outer query from all the sub-queries inside SELECT fields.
func (b *PlanBuilder) resolveCorrelatedAggregates(ctx context.Context, sel *ast.SelectStmt, p base.LogicalPlan) (map[*ast.AggregateFuncExpr]int, error) {
	resolver := &correlatedAggregateResolver{
		ctx:       ctx,
		b:         b,
		outerPlan: p,
	}
	correlatedAggList := make([]*ast.AggregateFuncExpr, 0)
	for _, field := range sel.Fields.Fields {
		_, ok := field.Expr.Accept(resolver)
		if !ok {
			return nil, resolver.err
		}
		correlatedAggList = append(correlatedAggList, resolver.correlatedAggFuncs...)
	}
	if sel.Having != nil {
		_, ok := sel.Having.Expr.Accept(resolver)
		if !ok {
			return nil, resolver.err
		}
		correlatedAggList = append(correlatedAggList, resolver.correlatedAggFuncs...)
	}
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			_, ok := item.Expr.Accept(resolver)
			if !ok {
				return nil, resolver.err
			}
			correlatedAggList = append(correlatedAggList, resolver.correlatedAggFuncs...)
		}
	}
	correlatedAggMap := make(map[*ast.AggregateFuncExpr]int)
	for _, aggFunc := range correlatedAggList {
		colMap := make(map[*types.FieldName]struct{}, len(p.Schema().Columns))
		allColFromAggExprNode(p, aggFunc, colMap)
		for k := range colMap {
			colName := &ast.ColumnName{
				Schema: k.DBName,
				Table:  k.TblName,
				Name:   k.ColName,
			}
			// Add the column referred in the agg func into the select list. So that we can resolve the agg func correctly.
			// And we need set the AuxiliaryColInAgg to true to help our only_full_group_by checker work correctly.
			sel.Fields.Fields = append(sel.Fields.Fields, &ast.SelectField{
				Auxiliary:         true,
				AuxiliaryColInAgg: true,
				Expr:              &ast.ColumnNameExpr{Name: colName},
			})
		}
		correlatedAggMap[aggFunc] = len(sel.Fields.Fields)
		sel.Fields.Fields = append(sel.Fields.Fields, &ast.SelectField{
			Auxiliary: true,
			Expr:      aggFunc,
			AsName:    model.NewCIStr(fmt.Sprintf("sel_subq_agg_%d", len(sel.Fields.Fields))),
		})
	}
	return correlatedAggMap, nil
}

// gbyResolver resolves group by items from select fields.
type gbyResolver struct {
	ctx        base.PlanContext
	fields     []*ast.SelectField
	schema     *expression.Schema
	names      []*types.FieldName
	err        error
	inExpr     bool
	isParam    bool
	skipAggMap map[*ast.AggregateFuncExpr]*expression.CorrelatedColumn

	exprDepth int // exprDepth is the depth of current expression in expression tree.
}

func (g *gbyResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	g.exprDepth++
	switch n := inNode.(type) {
	case *ast.SubqueryExpr, *ast.CompareSubqueryExpr, *ast.ExistsSubqueryExpr:
		return inNode, true
	case *driver.ParamMarkerExpr:
		g.isParam = true
		if g.exprDepth == 1 {
			_, isNull, isExpectedType := getUintFromNode(g.ctx, n, false)
			// For constant uint expression in top level, it should be treated as position expression.
			if !isNull && isExpectedType {
				return expression.ConstructPositionExpr(n), true
			}
		}
		return n, true
	case *driver.ValueExpr, *ast.ColumnNameExpr, *ast.ParenthesesExpr, *ast.ColumnName:
	default:
		g.inExpr = true
	}
	return inNode, false
}

func (g *gbyResolver) Leave(inNode ast.Node) (ast.Node, bool) {
	extractor := &AggregateFuncExtractor{skipAggMap: g.skipAggMap}
	switch v := inNode.(type) {
	case *ast.ColumnNameExpr:
		idx, err := expression.FindFieldName(g.names, v.Name)
		if idx < 0 || !g.inExpr {
			var index int
			index, g.err = resolveFromSelectFields(v, g.fields, false)
			if g.err != nil {
				g.err = plannererrors.ErrAmbiguous.GenWithStackByArgs(v.Name.Name.L, clauseMsg[groupByClause])
				return inNode, false
			}
			if idx >= 0 {
				return inNode, true
			}
			if index != -1 {
				ret := g.fields[index].Expr
				ret.Accept(extractor)
				if len(extractor.AggFuncs) != 0 {
					err = plannererrors.ErrIllegalReference.GenWithStackByArgs(v.Name.OrigColName(), "reference to group function")
				} else if ast.HasWindowFlag(ret) {
					err = plannererrors.ErrIllegalReference.GenWithStackByArgs(v.Name.OrigColName(), "reference to window function")
				} else {
					return ret, true
				}
			}
			g.err = err
			return inNode, false
		}
	case *ast.PositionExpr:
		pos, isNull, err := expression.PosFromPositionExpr(g.ctx.GetExprCtx(), g.ctx, v)
		if err != nil {
			g.err = plannererrors.ErrUnknown.GenWithStackByArgs()
		}
		if err != nil || isNull {
			return inNode, false
		}
		if pos < 1 || pos > len(g.fields) {
			g.err = errors.Errorf("Unknown column '%d' in 'group statement'", pos)
			return inNode, false
		}
		ret := g.fields[pos-1].Expr
		ret.Accept(extractor)
		if len(extractor.AggFuncs) != 0 || ast.HasWindowFlag(ret) {
			fieldName := g.fields[pos-1].AsName.String()
			if fieldName == "" {
				fieldName = g.fields[pos-1].Text()
			}
			g.err = plannererrors.ErrWrongGroupField.GenWithStackByArgs(fieldName)
			return inNode, false
		}
		return ret, true
	case *ast.ValuesExpr:
		if v.Column == nil {
			g.err = plannererrors.ErrUnknownColumn.GenWithStackByArgs("", "VALUES() function")
		}
	}
	return inNode, true
}

func tblInfoFromCol(from ast.ResultSetNode, name *types.FieldName) *model.TableInfo {
	tableList := ExtractTableList(from, true)
	for _, field := range tableList {
		if field.Name.L == name.TblName.L {
			return field.TableInfo
		}
	}
	return nil
}

func buildFuncDependCol(p base.LogicalPlan, cond ast.ExprNode) (*types.FieldName, *types.FieldName, error) {
	binOpExpr, ok := cond.(*ast.BinaryOperationExpr)
	if !ok {
		return nil, nil, nil
	}
	if binOpExpr.Op != opcode.EQ {
		return nil, nil, nil
	}
	lColExpr, ok := binOpExpr.L.(*ast.ColumnNameExpr)
	if !ok {
		return nil, nil, nil
	}
	rColExpr, ok := binOpExpr.R.(*ast.ColumnNameExpr)
	if !ok {
		return nil, nil, nil
	}
	lIdx, err := expression.FindFieldName(p.OutputNames(), lColExpr.Name)
	if err != nil {
		return nil, nil, err
	}
	rIdx, err := expression.FindFieldName(p.OutputNames(), rColExpr.Name)
	if err != nil {
		return nil, nil, err
	}
	if lIdx == -1 {
		return nil, nil, plannererrors.ErrUnknownColumn.GenWithStackByArgs(lColExpr.Name, "where clause")
	}
	if rIdx == -1 {
		return nil, nil, plannererrors.ErrUnknownColumn.GenWithStackByArgs(rColExpr.Name, "where clause")
	}
	return p.OutputNames()[lIdx], p.OutputNames()[rIdx], nil
}

func buildWhereFuncDepend(p base.LogicalPlan, where ast.ExprNode) (map[*types.FieldName]*types.FieldName, error) {
	whereConditions := splitWhere(where)
	colDependMap := make(map[*types.FieldName]*types.FieldName, 2*len(whereConditions))
	for _, cond := range whereConditions {
		lCol, rCol, err := buildFuncDependCol(p, cond)
		if err != nil {
			return nil, err
		}
		if lCol == nil || rCol == nil {
			continue
		}
		colDependMap[lCol] = rCol
		colDependMap[rCol] = lCol
	}
	return colDependMap, nil
}

func buildJoinFuncDepend(p base.LogicalPlan, from ast.ResultSetNode) (map[*types.FieldName]*types.FieldName, error) {
	switch x := from.(type) {
	case *ast.Join:
		if x.On == nil {
			return nil, nil
		}
		onConditions := splitWhere(x.On.Expr)
		colDependMap := make(map[*types.FieldName]*types.FieldName, len(onConditions))
		for _, cond := range onConditions {
			lCol, rCol, err := buildFuncDependCol(p, cond)
			if err != nil {
				return nil, err
			}
			if lCol == nil || rCol == nil {
				continue
			}
			lTbl := tblInfoFromCol(x.Left, lCol)
			if lTbl == nil {
				lCol, rCol = rCol, lCol
			}
			switch x.Tp {
			case ast.CrossJoin:
				colDependMap[lCol] = rCol
				colDependMap[rCol] = lCol
			case ast.LeftJoin:
				colDependMap[rCol] = lCol
			case ast.RightJoin:
				colDependMap[lCol] = rCol
			}
		}
		return colDependMap, nil
	default:
		return nil, nil
	}
}

func checkColFuncDepend(
	p base.LogicalPlan,
	name *types.FieldName,
	tblInfo *model.TableInfo,
	gbyOrSingleValueColNames map[*types.FieldName]struct{},
	whereDependNames, joinDependNames map[*types.FieldName]*types.FieldName,
) bool {
	for _, index := range tblInfo.Indices {
		if !index.Unique {
			continue
		}
		funcDepend := true
		// if all columns of some unique/pri indexes are determined, all columns left are check-passed.
		for _, indexCol := range index.Columns {
			iColInfo := tblInfo.Columns[indexCol.Offset]
			if !mysql.HasNotNullFlag(iColInfo.GetFlag()) {
				funcDepend = false
				break
			}
			cn := &ast.ColumnName{
				Schema: name.DBName,
				Table:  name.TblName,
				Name:   iColInfo.Name,
			}
			iIdx, err := expression.FindFieldName(p.OutputNames(), cn)
			if err != nil || iIdx < 0 {
				funcDepend = false
				break
			}
			iName := p.OutputNames()[iIdx]
			if _, ok := gbyOrSingleValueColNames[iName]; ok {
				continue
			}
			if wCol, ok := whereDependNames[iName]; ok {
				if _, ok = gbyOrSingleValueColNames[wCol]; ok {
					continue
				}
			}
			if jCol, ok := joinDependNames[iName]; ok {
				if _, ok = gbyOrSingleValueColNames[jCol]; ok {
					continue
				}
			}
			funcDepend = false
			break
		}
		if funcDepend {
			return true
		}
	}
	primaryFuncDepend := true
	hasPrimaryField := false
	for _, colInfo := range tblInfo.Columns {
		if !mysql.HasPriKeyFlag(colInfo.GetFlag()) {
			continue
		}
		hasPrimaryField = true
		pkName := &ast.ColumnName{
			Schema: name.DBName,
			Table:  name.TblName,
			Name:   colInfo.Name,
		}
		pIdx, err := expression.FindFieldName(p.OutputNames(), pkName)
		// It is possible that `pIdx < 0` and here is a case.
		// ```
		// CREATE TABLE `BB` (
		//   `pk` int(11) NOT NULL AUTO_INCREMENT,
		//   `col_int_not_null` int NOT NULL,
		//   PRIMARY KEY (`pk`)
		// );
		//
		// SELECT OUTR . col2 AS X
		// FROM
		//   BB AS OUTR2
		// INNER JOIN
		//   (SELECT col_int_not_null AS col1,
		//     pk AS col2
		//   FROM BB) AS OUTR ON OUTR2.col_int_not_null = OUTR.col1
		// GROUP BY OUTR2.col_int_not_null;
		// ```
		// When we enter `checkColFuncDepend`, `pkName.Table` is `OUTR` which is an alias, while `pkName.Name` is `pk`
		// which is a original name. Hence `expression.FindFieldName` will fail and `pIdx` will be less than 0.
		// Currently, when we meet `pIdx < 0`, we directly regard `primaryFuncDepend` as false and jump out. This way is
		// easy to implement but makes only-full-group-by checker not smart enough. Later we will refactor only-full-group-by
		// checker and resolve the inconsistency between the alias table name and the original column name.
		if err != nil || pIdx < 0 {
			primaryFuncDepend = false
			break
		}
		pCol := p.OutputNames()[pIdx]
		if _, ok := gbyOrSingleValueColNames[pCol]; ok {
			continue
		}
		if wCol, ok := whereDependNames[pCol]; ok {
			if _, ok = gbyOrSingleValueColNames[wCol]; ok {
				continue
			}
		}
		if jCol, ok := joinDependNames[pCol]; ok {
			if _, ok = gbyOrSingleValueColNames[jCol]; ok {
				continue
			}
		}
		primaryFuncDepend = false
		break
	}
	return primaryFuncDepend && hasPrimaryField
}

// ErrExprLoc is for generate the ErrFieldNotInGroupBy error info
type ErrExprLoc struct {
	Offset int
	Loc    string
}

func checkExprInGroupByOrIsSingleValue(
	p base.LogicalPlan,
	expr ast.ExprNode,
	offset int,
	loc string,
	gbyOrSingleValueColNames map[*types.FieldName]struct{},
	gbyExprs []ast.ExprNode,
	notInGbyOrSingleValueColNames map[*types.FieldName]ErrExprLoc,
) {
	if _, ok := expr.(*ast.AggregateFuncExpr); ok {
		return
	}
	if f, ok := expr.(*ast.FuncCallExpr); ok {
		if f.FnName.L == ast.Grouping {
			// just skip grouping function check here, because later in building plan phase, we
			// will do the grouping function valid check.
			return
		}
	}
	if _, ok := expr.(*ast.ColumnNameExpr); !ok {
		for _, gbyExpr := range gbyExprs {
			if ast.ExpressionDeepEqual(gbyExpr, expr) {
				return
			}
		}
	}
	// Function `any_value` can be used in aggregation, even `ONLY_FULL_GROUP_BY` is set.
	// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value for details
	if f, ok := expr.(*ast.FuncCallExpr); ok {
		if f.FnName.L == ast.AnyValue {
			return
		}
	}
	colMap := make(map[*types.FieldName]struct{}, len(p.Schema().Columns))
	allColFromExprNode(p, expr, colMap)
	for col := range colMap {
		if _, ok := gbyOrSingleValueColNames[col]; !ok {
			notInGbyOrSingleValueColNames[col] = ErrExprLoc{Offset: offset, Loc: loc}
		}
	}
}

func (b *PlanBuilder) checkOnlyFullGroupBy(p base.LogicalPlan, sel *ast.SelectStmt) (err error) {
	if sel.GroupBy != nil {
		err = b.checkOnlyFullGroupByWithGroupClause(p, sel)
	} else {
		err = b.checkOnlyFullGroupByWithOutGroupClause(p, sel)
	}
	return err
}

func addGbyOrSingleValueColName(p base.LogicalPlan, colName *ast.ColumnName, gbyOrSingleValueColNames map[*types.FieldName]struct{}) {
	idx, err := expression.FindFieldName(p.OutputNames(), colName)
	if err != nil || idx < 0 {
		return
	}
	gbyOrSingleValueColNames[p.OutputNames()[idx]] = struct{}{}
}

func extractSingeValueColNamesFromWhere(p base.LogicalPlan, where ast.ExprNode, gbyOrSingleValueColNames map[*types.FieldName]struct{}) {
	whereConditions := splitWhere(where)
	for _, cond := range whereConditions {
		binOpExpr, ok := cond.(*ast.BinaryOperationExpr)
		if !ok || binOpExpr.Op != opcode.EQ {
			continue
		}
		if colExpr, ok := binOpExpr.L.(*ast.ColumnNameExpr); ok {
			if _, ok := binOpExpr.R.(ast.ValueExpr); ok {
				addGbyOrSingleValueColName(p, colExpr.Name, gbyOrSingleValueColNames)
			}
		} else if colExpr, ok := binOpExpr.R.(*ast.ColumnNameExpr); ok {
			if _, ok := binOpExpr.L.(ast.ValueExpr); ok {
				addGbyOrSingleValueColName(p, colExpr.Name, gbyOrSingleValueColNames)
			}
		}
	}
}

func (*PlanBuilder) checkOnlyFullGroupByWithGroupClause(p base.LogicalPlan, sel *ast.SelectStmt) error {
	gbyOrSingleValueColNames := make(map[*types.FieldName]struct{}, len(sel.Fields.Fields))
	gbyExprs := make([]ast.ExprNode, 0, len(sel.Fields.Fields))
	for _, byItem := range sel.GroupBy.Items {
		expr := getInnerFromParenthesesAndUnaryPlus(byItem.Expr)
		if colExpr, ok := expr.(*ast.ColumnNameExpr); ok {
			addGbyOrSingleValueColName(p, colExpr.Name, gbyOrSingleValueColNames)
		} else {
			gbyExprs = append(gbyExprs, expr)
		}
	}
	// MySQL permits a nonaggregate column not named in a GROUP BY clause when ONLY_FULL_GROUP_BY SQL mode is enabled,
	// provided that this column is limited to a single value.
	// See https://dev.mysql.com/doc/refman/5.7/en/group-by-handling.html for details.
	extractSingeValueColNamesFromWhere(p, sel.Where, gbyOrSingleValueColNames)

	notInGbyOrSingleValueColNames := make(map[*types.FieldName]ErrExprLoc, len(sel.Fields.Fields))
	for offset, field := range sel.Fields.Fields {
		if field.Auxiliary {
			continue
		}
		checkExprInGroupByOrIsSingleValue(p, getInnerFromParenthesesAndUnaryPlus(field.Expr), offset, ErrExprInSelect, gbyOrSingleValueColNames, gbyExprs, notInGbyOrSingleValueColNames)
	}

	if sel.OrderBy != nil {
		for offset, item := range sel.OrderBy.Items {
			if colName, ok := item.Expr.(*ast.ColumnNameExpr); ok {
				index, err := resolveFromSelectFields(colName, sel.Fields.Fields, false)
				if err != nil {
					return err
				}
				// If the ByItem is in fields list, it has been checked already in above.
				if index >= 0 {
					continue
				}
			}
			checkExprInGroupByOrIsSingleValue(p, getInnerFromParenthesesAndUnaryPlus(item.Expr), offset, ErrExprInOrderBy, gbyOrSingleValueColNames, gbyExprs, notInGbyOrSingleValueColNames)
		}
	}
	if len(notInGbyOrSingleValueColNames) == 0 {
		return nil
	}

	whereDepends, err := buildWhereFuncDepend(p, sel.Where)
	if err != nil {
		return err
	}
	joinDepends, err := buildJoinFuncDepend(p, sel.From.TableRefs)
	if err != nil {
		return err
	}
	tblMap := make(map[*model.TableInfo]struct{}, len(notInGbyOrSingleValueColNames))
	for name, errExprLoc := range notInGbyOrSingleValueColNames {
		tblInfo := tblInfoFromCol(sel.From.TableRefs, name)
		if tblInfo == nil {
			continue
		}
		if _, ok := tblMap[tblInfo]; ok {
			continue
		}
		if checkColFuncDepend(p, name, tblInfo, gbyOrSingleValueColNames, whereDepends, joinDepends) {
			tblMap[tblInfo] = struct{}{}
			continue
		}
		switch errExprLoc.Loc {
		case ErrExprInSelect:
			if sel.GroupBy.Rollup {
				return plannererrors.ErrFieldInGroupingNotGroupBy.GenWithStackByArgs(strconv.Itoa(errExprLoc.Offset + 1))
			}
			return plannererrors.ErrFieldNotInGroupBy.GenWithStackByArgs(errExprLoc.Offset+1, errExprLoc.Loc, name.DBName.O+"."+name.TblName.O+"."+name.OrigColName.O)
		case ErrExprInOrderBy:
			return plannererrors.ErrFieldNotInGroupBy.GenWithStackByArgs(errExprLoc.Offset+1, errExprLoc.Loc, sel.OrderBy.Items[errExprLoc.Offset].Expr.Text())
		}
		return nil
	}
	return nil
}

func (*PlanBuilder) checkOnlyFullGroupByWithOutGroupClause(p base.LogicalPlan, sel *ast.SelectStmt) error {
	resolver := colResolverForOnlyFullGroupBy{
		firstOrderByAggColIdx: -1,
	}
	resolver.curClause = fieldList
	for idx, field := range sel.Fields.Fields {
		resolver.exprIdx = idx
		field.Accept(&resolver)
	}
	if len(resolver.nonAggCols) > 0 {
		if sel.Having != nil {
			sel.Having.Expr.Accept(&resolver)
		}
		if sel.OrderBy != nil {
			resolver.curClause = orderByClause
			for idx, byItem := range sel.OrderBy.Items {
				resolver.exprIdx = idx
				byItem.Expr.Accept(&resolver)
			}
		}
	}
	if resolver.firstOrderByAggColIdx != -1 && len(resolver.nonAggCols) > 0 {
		// SQL like `select a from t where a = 1 order by count(b)` is illegal.
		return plannererrors.ErrAggregateOrderNonAggQuery.GenWithStackByArgs(resolver.firstOrderByAggColIdx + 1)
	}
	if !resolver.hasAggFuncOrAnyValue || len(resolver.nonAggCols) == 0 {
		return nil
	}
	singleValueColNames := make(map[*types.FieldName]struct{}, len(sel.Fields.Fields))
	extractSingeValueColNamesFromWhere(p, sel.Where, singleValueColNames)
	whereDepends, err := buildWhereFuncDepend(p, sel.Where)
	if err != nil {
		return err
	}

	joinDepends, err := buildJoinFuncDepend(p, sel.From.TableRefs)
	if err != nil {
		return err
	}
	tblMap := make(map[*model.TableInfo]struct{}, len(resolver.nonAggCols))
	for i, colName := range resolver.nonAggCols {
		idx, err := expression.FindFieldName(p.OutputNames(), colName)
		if err != nil || idx < 0 {
			return plannererrors.ErrMixOfGroupFuncAndFields.GenWithStackByArgs(resolver.nonAggColIdxs[i]+1, colName.Name.O)
		}
		fieldName := p.OutputNames()[idx]
		if _, ok := singleValueColNames[fieldName]; ok {
			continue
		}
		tblInfo := tblInfoFromCol(sel.From.TableRefs, fieldName)
		if tblInfo == nil {
			continue
		}
		if _, ok := tblMap[tblInfo]; ok {
			continue
		}
		if checkColFuncDepend(p, fieldName, tblInfo, singleValueColNames, whereDepends, joinDepends) {
			tblMap[tblInfo] = struct{}{}
			continue
		}
		return plannererrors.ErrMixOfGroupFuncAndFields.GenWithStackByArgs(resolver.nonAggColIdxs[i]+1, colName.Name.O)
	}
	return nil
}

// colResolverForOnlyFullGroupBy visits Expr tree to find out if an Expr tree is an aggregation function.
// If so, find out the first column name that not in an aggregation function.
type colResolverForOnlyFullGroupBy struct {
	nonAggCols            []*ast.ColumnName
	exprIdx               int
	nonAggColIdxs         []int
	hasAggFuncOrAnyValue  bool
	firstOrderByAggColIdx int
	curClause             clauseCode
}

func (c *colResolverForOnlyFullGroupBy) Enter(node ast.Node) (ast.Node, bool) {
	switch t := node.(type) {
	case *ast.AggregateFuncExpr:
		c.hasAggFuncOrAnyValue = true
		if c.curClause == orderByClause {
			c.firstOrderByAggColIdx = c.exprIdx
		}
		return node, true
	case *ast.FuncCallExpr:
		// enable function `any_value` in aggregation even `ONLY_FULL_GROUP_BY` is set
		if t.FnName.L == ast.AnyValue {
			c.hasAggFuncOrAnyValue = true
			return node, true
		}
	case *ast.ColumnNameExpr:
		c.nonAggCols = append(c.nonAggCols, t.Name)
		c.nonAggColIdxs = append(c.nonAggColIdxs, c.exprIdx)
		return node, true
	case *ast.SubqueryExpr:
		return node, true
	}
	return node, false
}

func (*colResolverForOnlyFullGroupBy) Leave(node ast.Node) (ast.Node, bool) {
	return node, true
}

type aggColNameResolver struct {
	colNameResolver
}

func (*aggColNameResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	if _, ok := inNode.(*ast.ColumnNameExpr); ok {
		return inNode, true
	}
	return inNode, false
}

func allColFromAggExprNode(p base.LogicalPlan, n ast.Node, names map[*types.FieldName]struct{}) {
	extractor := &aggColNameResolver{
		colNameResolver: colNameResolver{
			p:     p,
			names: names,
		},
	}
	n.Accept(extractor)
}

type colNameResolver struct {
	p     base.LogicalPlan
	names map[*types.FieldName]struct{}
}

func (*colNameResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	switch inNode.(type) {
	case *ast.ColumnNameExpr, *ast.SubqueryExpr, *ast.AggregateFuncExpr:
		return inNode, true
	}
	return inNode, false
}

func (c *colNameResolver) Leave(inNode ast.Node) (ast.Node, bool) {
	if v, ok := inNode.(*ast.ColumnNameExpr); ok {
		idx, err := expression.FindFieldName(c.p.OutputNames(), v.Name)
		if err == nil && idx >= 0 {
			c.names[c.p.OutputNames()[idx]] = struct{}{}
		}
	}
	return inNode, true
}

func allColFromExprNode(p base.LogicalPlan, n ast.Node, names map[*types.FieldName]struct{}) {
	extractor := &colNameResolver{
		p:     p,
		names: names,
	}
	n.Accept(extractor)
}

func (b *PlanBuilder) resolveGbyExprs(ctx context.Context, p base.LogicalPlan, gby *ast.GroupByClause, fields []*ast.SelectField) (base.LogicalPlan, []expression.Expression, bool, error) {
	b.curClause = groupByClause
	exprs := make([]expression.Expression, 0, len(gby.Items))
	resolver := &gbyResolver{
		ctx:        b.ctx,
		fields:     fields,
		schema:     p.Schema(),
		names:      p.OutputNames(),
		skipAggMap: b.correlatedAggMapper,
	}
	for _, item := range gby.Items {
		resolver.inExpr = false
		resolver.exprDepth = 0
		resolver.isParam = false
		retExpr, _ := item.Expr.Accept(resolver)
		if resolver.err != nil {
			return nil, nil, false, errors.Trace(resolver.err)
		}
		if !resolver.isParam {
			item.Expr = retExpr.(ast.ExprNode)
		}

		itemExpr := retExpr.(ast.ExprNode)
		expr, np, err := b.rewrite(ctx, itemExpr, p, nil, true)
		if err != nil {
			return nil, nil, false, err
		}

		exprs = append(exprs, expr)
		p = np
	}
	return p, exprs, gby.Rollup, nil
}

func (*PlanBuilder) unfoldWildStar(p base.LogicalPlan, selectFields []*ast.SelectField) (resultList []*ast.SelectField, err error) {
	join, isJoin := p.(*LogicalJoin)
	for i, field := range selectFields {
		if field.WildCard == nil {
			resultList = append(resultList, field)
			continue
		}
		if field.WildCard.Table.L == "" && i > 0 {
			return nil, plannererrors.ErrInvalidWildCard
		}
		list := unfoldWildStar(field, p.OutputNames(), p.Schema().Columns)
		// For sql like `select t1.*, t2.* from t1 join t2 using(a)` or `select t1.*, t2.* from t1 natual join t2`,
		// the schema of the Join doesn't contain enough columns because the join keys are coalesced in this schema.
		// We should collect the columns from the fullSchema.
		if isJoin && join.fullSchema != nil && field.WildCard.Table.L != "" {
			list = unfoldWildStar(field, join.fullNames, join.fullSchema.Columns)
		}
		if len(list) == 0 {
			return nil, plannererrors.ErrBadTable.GenWithStackByArgs(field.WildCard.Table)
		}
		resultList = append(resultList, list...)
	}
	return resultList, nil
}

func unfoldWildStar(field *ast.SelectField, outputName types.NameSlice, column []*expression.Column) (resultList []*ast.SelectField) {
	dbName := field.WildCard.Schema
	tblName := field.WildCard.Table
	for i, name := range outputName {
		col := column[i]
		if col.IsHidden {
			continue
		}
		if (dbName.L == "" || dbName.L == name.DBName.L) &&
			(tblName.L == "" || tblName.L == name.TblName.L) &&
			col.ID != model.ExtraHandleID && col.ID != model.ExtraPidColID && col.ID != model.ExtraPhysTblID {
			colName := &ast.ColumnNameExpr{
				Name: &ast.ColumnName{
					Schema: name.DBName,
					Table:  name.TblName,
					Name:   name.ColName,
				}}
			colName.SetType(col.GetStaticType())
			field := &ast.SelectField{Expr: colName}
			field.SetText(nil, name.ColName.O)
			resultList = append(resultList, field)
		}
	}
	return resultList
}

func (b *PlanBuilder) addAliasName(ctx context.Context, selectStmt *ast.SelectStmt, p base.LogicalPlan) (resultList []*ast.SelectField, err error) {
	selectFields := selectStmt.Fields.Fields
	projOutNames := make([]*types.FieldName, 0, len(selectFields))
	for _, field := range selectFields {
		colNameField, isColumnNameExpr := field.Expr.(*ast.ColumnNameExpr)
		if isColumnNameExpr {
			colName := colNameField.Name.Name
			if field.AsName.L != "" {
				colName = field.AsName
			}
			projOutNames = append(projOutNames, &types.FieldName{
				TblName:     colNameField.Name.Table,
				OrigTblName: colNameField.Name.Table,
				ColName:     colName,
				OrigColName: colNameField.Name.Name,
				DBName:      colNameField.Name.Schema,
			})
		} else {
			// create view v as select name_const('col', 100);
			// The column in v should be 'col', so we call `buildProjectionField` to handle this.
			_, name, err := b.buildProjectionField(ctx, p, field, nil)
			if err != nil {
				return nil, err
			}
			projOutNames = append(projOutNames, name)
		}
	}

	// dedupMap is used for renaming a duplicated anonymous column
	dedupMap := make(map[string]int)
	anonymousFields := make([]bool, len(selectFields))

	for i, field := range selectFields {
		newField := *field
		if newField.AsName.L == "" {
			newField.AsName = projOutNames[i].ColName
		}

		if _, ok := field.Expr.(*ast.ColumnNameExpr); !ok && field.AsName.L == "" {
			anonymousFields[i] = true
		} else {
			anonymousFields[i] = false
			// dedupMap should be inited with all non-anonymous fields before renaming other duplicated anonymous fields
			dedupMap[newField.AsName.L] = 0
		}

		resultList = append(resultList, &newField)
	}

	// We should rename duplicated anonymous fields in the first SelectStmt of CreateViewStmt
	// See: https://github.com/pingcap/tidb/issues/29326
	if selectStmt.AsViewSchema {
		for i, field := range resultList {
			if !anonymousFields[i] {
				continue
			}

			oldName := field.AsName
			if dup, ok := dedupMap[field.AsName.L]; ok {
				if dup == 0 {
					field.AsName = model.NewCIStr(fmt.Sprintf("Name_exp_%s", field.AsName.O))
				} else {
					field.AsName = model.NewCIStr(fmt.Sprintf("Name_exp_%d_%s", dup, field.AsName.O))
				}
				dedupMap[oldName.L] = dup + 1
			} else {
				dedupMap[oldName.L] = 0
			}
		}
	}

	return resultList, nil
}

func (b *PlanBuilder) pushHintWithoutTableWarning(hint *ast.TableOptimizerHint) {
	var sb strings.Builder
	ctx := format.NewRestoreCtx(0, &sb)
	if err := hint.Restore(ctx); err != nil {
		return
	}
	b.ctx.GetSessionVars().StmtCtx.SetHintWarning(
		fmt.Sprintf("Hint %s is inapplicable. Please specify the table names in the arguments.", sb.String()))
}

func (b *PlanBuilder) pushTableHints(hints []*ast.TableOptimizerHint, currentLevel int) {
	hints = b.hintProcessor.GetCurrentStmtHints(hints, currentLevel)
	currentDB := b.ctx.GetSessionVars().CurrentDB
	warnHandler := b.ctx.GetSessionVars().StmtCtx
	planHints, subQueryHintFlags, err := h.ParsePlanHints(hints, currentLevel, currentDB,
		b.hintProcessor, b.ctx.GetSessionVars().StmtCtx.StraightJoinOrder,
		b.subQueryCtx == handlingExistsSubquery, b.subQueryCtx == notHandlingSubquery, warnHandler)
	if err != nil {
		return
	}
	b.tableHintInfo = append(b.tableHintInfo, planHints)
	b.subQueryHintFlags |= subQueryHintFlags
}

func (b *PlanBuilder) popVisitInfo() {
	if len(b.visitInfo) == 0 {
		return
	}
	b.visitInfo = b.visitInfo[:len(b.visitInfo)-1]
}

func (b *PlanBuilder) popTableHints() {
	hintInfo := b.tableHintInfo[len(b.tableHintInfo)-1]
	for _, warning := range h.CollectUnmatchedHintWarnings(hintInfo) {
		b.ctx.GetSessionVars().StmtCtx.SetHintWarning(warning)
	}
	b.tableHintInfo = b.tableHintInfo[:len(b.tableHintInfo)-1]
}

// TableHints returns the *TableHintInfo of PlanBuilder.
func (b *PlanBuilder) TableHints() *h.PlanHints {
	if len(b.tableHintInfo) == 0 {
		return nil
	}
	return b.tableHintInfo[len(b.tableHintInfo)-1]
}

func (b *PlanBuilder) buildSelect(ctx context.Context, sel *ast.SelectStmt) (p base.LogicalPlan, err error) {
	b.pushSelectOffset(sel.QueryBlockOffset)
	b.pushTableHints(sel.TableHints, sel.QueryBlockOffset)
	defer func() {
		b.popSelectOffset()
		// table hints are only visible in the current SELECT statement.
		b.popTableHints()
	}()
	if b.buildingRecursivePartForCTE {
		if sel.Distinct || sel.OrderBy != nil || sel.Limit != nil {
			return nil, plannererrors.ErrNotSupportedYet.GenWithStackByArgs("ORDER BY / LIMIT / SELECT DISTINCT in recursive query block of Common Table Expression")
		}
		if sel.GroupBy != nil {
			return nil, plannererrors.ErrCTERecursiveForbidsAggregation.FastGenByArgs(b.genCTETableNameForError())
		}
	}
	if sel.SelectStmtOpts != nil {
		origin := b.inStraightJoin
		b.inStraightJoin = sel.SelectStmtOpts.StraightJoin
		defer func() { b.inStraightJoin = origin }()
	}

	var (
		aggFuncs                      []*ast.AggregateFuncExpr
		havingMap, orderMap, totalMap map[*ast.AggregateFuncExpr]int
		windowAggMap                  map[*ast.AggregateFuncExpr]int
		correlatedAggMap              map[*ast.AggregateFuncExpr]int
		gbyCols                       []expression.Expression
		projExprs                     []expression.Expression
		rollup                        bool
	)

	// set for update read to true before building result set node
	if isForUpdateReadSelectLock(sel.LockInfo) {
		b.isForUpdateRead = true
	}

	if hints := b.TableHints(); hints != nil && hints.CTEMerge {
		// Verify Merge hints in the current query,
		// we will update parameters for those that meet the rules, and warn those that do not.
		// If the current query uses Merge Hint and the query is a CTE,
		// we update the HINT information for the current query.
		// If the current query is not a CTE query (it may be a subquery within a CTE query
		// or an external non-CTE query), we will give a warning.
		// In particular, recursive CTE have separate warnings, so they are no longer called.
		if b.buildingCTE {
			if b.isCTE {
				b.outerCTEs[len(b.outerCTEs)-1].forceInlineByHintOrVar = true
			} else if !b.buildingRecursivePartForCTE {
				// If there has subquery which is not CTE and using `MERGE()` hint, we will show this warning;
				b.ctx.GetSessionVars().StmtCtx.SetHintWarning(
					"Hint merge() is inapplicable. " +
						"Please check whether the hint is used in the right place, " +
						"you should use this hint inside the CTE.")
			}
		} else if !b.buildingCTE && !b.isCTE {
			b.ctx.GetSessionVars().StmtCtx.SetHintWarning(
				"Hint merge() is inapplicable. " +
					"Please check whether the hint is used in the right place, " +
					"you should use this hint inside the CTE.")
		}
	}

	var currentLayerCTEs []*cteInfo
	if sel.With != nil {
		l := len(b.outerCTEs)
		defer func() {
			b.outerCTEs = b.outerCTEs[:l]
		}()
		currentLayerCTEs, err = b.buildWith(ctx, sel.With)
		if err != nil {
			return nil, err
		}
	}

	p, err = b.buildTableRefs(ctx, sel.From)
	if err != nil {
		return nil, err
	}

	originalFields := sel.Fields.Fields
	sel.Fields.Fields, err = b.unfoldWildStar(p, sel.Fields.Fields)
	if err != nil {
		return nil, err
	}
	if b.capFlag&canExpandAST != 0 {
		// To be compatible with MySQL, we add alias name for each select field when creating view.
		sel.Fields.Fields, err = b.addAliasName(ctx, sel, p)
		if err != nil {
			return nil, err
		}
		originalFields = sel.Fields.Fields
	}

	if sel.GroupBy != nil {
		p, gbyCols, rollup, err = b.resolveGbyExprs(ctx, p, sel.GroupBy, sel.Fields.Fields)
		if err != nil {
			return nil, err
		}
	}

	if b.ctx.GetSessionVars().SQLMode.HasOnlyFullGroupBy() && sel.From != nil && !b.ctx.GetSessionVars().OptimizerEnableNewOnlyFullGroupByCheck {
		err = b.checkOnlyFullGroupBy(p, sel)
		if err != nil {
			return nil, err
		}
	}

	hasWindowFuncField := b.detectSelectWindow(sel)
	// Some SQL statements define WINDOW but do not use them. But we also need to check the window specification list.
	// For example: select id from t group by id WINDOW w AS (ORDER BY uids DESC) ORDER BY id;
	// We don't use the WINDOW w, but if the 'uids' column is not in the table t, we still need to report an error.
	if hasWindowFuncField || sel.WindowSpecs != nil {
		if b.buildingRecursivePartForCTE {
			return nil, plannererrors.ErrCTERecursiveForbidsAggregation.FastGenByArgs(b.genCTETableNameForError())
		}

		windowAggMap, err = b.resolveWindowFunction(sel, p)
		if err != nil {
			return nil, err
		}
	}
	// We must resolve having and order by clause before build projection,
	// because when the query is "select a+1 as b from t having sum(b) < 0", we must replace sum(b) to sum(a+1),
	// which only can be done before building projection and extracting Agg functions.
	havingMap, orderMap, err = b.resolveHavingAndOrderBy(ctx, sel, p)
	if err != nil {
		return nil, err
	}

	// We have to resolve correlated aggregate inside sub-queries before building aggregation and building projection,
	// for instance, count(a) inside the sub-query of "select (select count(a)) from t" should be evaluated within
	// the context of the outer query. So we have to extract such aggregates from sub-queries and put them into
	// SELECT field list.
	correlatedAggMap, err = b.resolveCorrelatedAggregates(ctx, sel, p)
	if err != nil {
		return nil, err
	}

	// b.allNames will be used in evalDefaultExpr(). Default function is special because it needs to find the
	// corresponding column name, but does not need the value in the column.
	// For example, select a from t order by default(b), the column b will not be in select fields. Also because
	// buildSort is after buildProjection, so we need get OutputNames before BuildProjection and store in allNames.
	// Otherwise, we will get select fields instead of all OutputNames, so that we can't find the column b in the
	// above example.
	b.allNames = append(b.allNames, p.OutputNames())
	defer func() { b.allNames = b.allNames[:len(b.allNames)-1] }()

	if sel.Where != nil {
		p, err = b.buildSelection(ctx, p, sel.Where, nil)
		if err != nil {
			return nil, err
		}
	}
	l := sel.LockInfo
	if l != nil && l.LockType != ast.SelectLockNone {
		for _, tName := range l.Tables {
			// CTE has no *model.HintedTable, we need to skip it.
			if tName.TableInfo == nil {
				continue
			}
			b.ctx.GetSessionVars().StmtCtx.LockTableIDs[tName.TableInfo.ID] = struct{}{}
		}
		p, err = b.buildSelectLock(p, l)
		if err != nil {
			return nil, err
		}
	}
	b.handleHelper.popMap()
	b.handleHelper.pushMap(nil)

	hasAgg := b.detectSelectAgg(sel)
	needBuildAgg := hasAgg
	if hasAgg {
		if b.buildingRecursivePartForCTE {
			return nil, plannererrors.ErrCTERecursiveForbidsAggregation.GenWithStackByArgs(b.genCTETableNameForError())
		}

		aggFuncs, totalMap = b.extractAggFuncsInSelectFields(sel.Fields.Fields)
		// len(aggFuncs) == 0 and sel.GroupBy == nil indicates that all the aggregate functions inside the SELECT fields
		// are actually correlated aggregates from the outer query, which have already been built in the outer query.
		// The only thing we need to do is to find them from b.correlatedAggMap in buildProjection.
		if len(aggFuncs) == 0 && sel.GroupBy == nil {
			needBuildAgg = false
		}
	}
	if needBuildAgg {
		// if rollup syntax is specified, Expand OP is required to replicate the data to feed different grouping layout.
		if rollup {
			p, gbyCols, err = b.buildExpand(p, gbyCols)
			if err != nil {
				return nil, err
			}
		}
		var aggIndexMap map[int]int
		p, aggIndexMap, err = b.buildAggregation(ctx, p, aggFuncs, gbyCols, correlatedAggMap)
		if err != nil {
			return nil, err
		}
		for agg, idx := range totalMap {
			totalMap[agg] = aggIndexMap[idx]
		}
	}

	var oldLen int
	// According to https://dev.mysql.com/doc/refman/8.0/en/window-functions-usage.html,
	// we can only process window functions after having clause, so `considerWindow` is false now.
	p, projExprs, oldLen, err = b.buildProjection(ctx, p, sel.Fields.Fields, totalMap, nil, false, sel.OrderBy != nil)
	if err != nil {
		return nil, err
	}

	if sel.Having != nil {
		b.curClause = havingClause
		p, err = b.buildSelection(ctx, p, sel.Having.Expr, havingMap)
		if err != nil {
			return nil, err
		}
	}

	b.windowSpecs, err = buildWindowSpecs(sel.WindowSpecs)
	if err != nil {
		return nil, err
	}

	var windowMapper map[*ast.WindowFuncExpr]int
	if hasWindowFuncField || sel.WindowSpecs != nil {
		windowFuncs := extractWindowFuncs(sel.Fields.Fields)
		// we need to check the func args first before we check the window spec
		err := b.checkWindowFuncArgs(ctx, p, windowFuncs, windowAggMap)
		if err != nil {
			return nil, err
		}
		groupedFuncs, orderedSpec, err := b.groupWindowFuncs(windowFuncs)
		if err != nil {
			return nil, err
		}
		p, windowMapper, err = b.buildWindowFunctions(ctx, p, groupedFuncs, orderedSpec, windowAggMap)
		if err != nil {
			return nil, err
		}
		// `hasWindowFuncField == false` means there's only unused named window specs without window functions.
		// In such case plan `p` is not changed, so we don't have to build another projection.
		if hasWindowFuncField {
			// Now we build the window function fields.
			p, projExprs, oldLen, err = b.buildProjection(ctx, p, sel.Fields.Fields, windowAggMap, windowMapper, true, false)
			if err != nil {
				return nil, err
			}
		}
	}

	if sel.Distinct {
		p, err = b.buildDistinct(p, oldLen)
		if err != nil {
			return nil, err
		}
	}

	if sel.OrderBy != nil {
		// We need to keep the ORDER BY clause for the following cases:
		// 1. The select is top level query, order should be honored
		// 2. The query has LIMIT clause
		// 3. The control flag requires keeping ORDER BY explicitly
		if len(b.qbOffset) == 1 || sel.Limit != nil || !b.ctx.GetSessionVars().RemoveOrderbyInSubquery {
			if b.ctx.GetSessionVars().SQLMode.HasOnlyFullGroupBy() {
				p, err = b.buildSortWithCheck(ctx, p, sel.OrderBy.Items, orderMap, windowMapper, projExprs, oldLen, sel.Distinct)
			} else {
				p, err = b.buildSort(ctx, p, sel.OrderBy.Items, orderMap, windowMapper)
			}
			if err != nil {
				return nil, err
			}
		}
	}

	if sel.Limit != nil {
		p, err = b.buildLimit(p, sel.Limit)
		if err != nil {
			return nil, err
		}
	}

	sel.Fields.Fields = originalFields
	if oldLen != p.Schema().Len() {
		proj := LogicalProjection{Exprs: expression.Column2Exprs(p.Schema().Columns[:oldLen])}.Init(b.ctx, b.getSelectOffset())
		proj.SetChildren(p)
		schema := expression.NewSchema(p.Schema().Clone().Columns[:oldLen]...)
		for _, col := range schema.Columns {
			col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
		}
		proj.names = p.OutputNames()[:oldLen]
		proj.SetSchema(schema)
		return b.tryToBuildSequence(currentLayerCTEs, proj), nil
	}

	return b.tryToBuildSequence(currentLayerCTEs, p), nil
}

func (b *PlanBuilder) tryToBuildSequence(ctes []*cteInfo, p base.LogicalPlan) base.LogicalPlan {
	if !b.ctx.GetSessionVars().EnableMPPSharedCTEExecution {
		return p
	}
	for i := len(ctes) - 1; i >= 0; i-- {
		if !ctes[i].nonRecursive {
			return p
		}
		if ctes[i].isInline || ctes[i].cteClass == nil {
			ctes = append(ctes[:i], ctes[i+1:]...)
		}
	}
	if len(ctes) == 0 {
		return p
	}
	lctes := make([]base.LogicalPlan, 0, len(ctes)+1)
	for _, cte := range ctes {
		lcte := LogicalCTE{
			cte:               cte.cteClass,
			cteAsName:         cte.def.Name,
			cteName:           cte.def.Name,
			seedStat:          cte.seedStat,
			onlyUsedAsStorage: true,
		}.Init(b.ctx, b.getSelectOffset())
		lcte.SetSchema(getResultCTESchema(cte.seedLP.Schema(), b.ctx.GetSessionVars()))
		lctes = append(lctes, lcte)
	}
	b.optFlag |= flagPushDownSequence
	seq := LogicalSequence{}.Init(b.ctx, b.getSelectOffset())
	seq.SetChildren(append(lctes, p)...)
	seq.SetOutputNames(p.OutputNames().Shallow())
	return seq
}

func (b *PlanBuilder) buildTableDual() *LogicalTableDual {
	b.handleHelper.pushMap(nil)
	return LogicalTableDual{RowCount: 1}.Init(b.ctx, b.getSelectOffset())
}

func (ds *DataSource) newExtraHandleSchemaCol() *expression.Column {
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.SetFlag(mysql.NotNullFlag | mysql.PriKeyFlag)
	return &expression.Column{
		RetType:  tp,
		UniqueID: ds.SCtx().GetSessionVars().AllocPlanColumnID(),
		ID:       model.ExtraHandleID,
		OrigName: fmt.Sprintf("%v.%v.%v", ds.DBName, ds.tableInfo.Name, model.ExtraHandleName),
	}
}

// AddExtraPhysTblIDColumn for partition table.
// 'select ... for update' on a partition table need to know the partition ID
// to construct the lock key, so this column is added to the chunk row.
// Also needed for checking against the sessions transaction buffer
func (ds *DataSource) AddExtraPhysTblIDColumn() *expression.Column {
	// Avoid adding multiple times (should never happen!)
	cols := ds.TblCols
	for i := len(cols) - 1; i >= 0; i-- {
		if cols[i].ID == model.ExtraPhysTblID {
			return cols[i]
		}
	}
	pidCol := &expression.Column{
		RetType:  types.NewFieldType(mysql.TypeLonglong),
		UniqueID: ds.SCtx().GetSessionVars().AllocPlanColumnID(),
		ID:       model.ExtraPhysTblID,
		OrigName: fmt.Sprintf("%v.%v.%v", ds.DBName, ds.tableInfo.Name, model.ExtraPhysTblIdName),
	}

	ds.Columns = append(ds.Columns, model.NewExtraPhysTblIDColInfo())
	schema := ds.Schema()
	schema.Append(pidCol)
	ds.names = append(ds.names, &types.FieldName{
		DBName:      ds.DBName,
		TblName:     ds.TableInfo().Name,
		ColName:     model.ExtraPhysTblIdName,
		OrigColName: model.ExtraPhysTblIdName,
	})
	ds.TblCols = append(ds.TblCols, pidCol)
	return pidCol
}

// getStatsTable gets statistics information for a table specified by "tableID".
// A pseudo statistics table is returned in any of the following scenario:
// 1. tidb-server started and statistics handle has not been initialized.
// 2. table row count from statistics is zero.
// 3. statistics is outdated.
// Note: please also update getLatestVersionFromStatsTable() when logic in this function changes.
func getStatsTable(ctx base.PlanContext, tblInfo *model.TableInfo, pid int64) *statistics.Table {
	statsHandle := domain.GetDomain(ctx).StatsHandle()
	var usePartitionStats, countIs0, pseudoStatsForUninitialized, pseudoStatsForOutdated bool
	var statsTbl *statistics.Table
	if ctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(ctx)
		defer func() {
			debugTraceGetStatsTbl(ctx,
				tblInfo,
				pid,
				statsHandle == nil,
				usePartitionStats,
				countIs0,
				pseudoStatsForUninitialized,
				pseudoStatsForOutdated,
				statsTbl,
			)
			debugtrace.LeaveContextCommon(ctx)
		}()
	}
	// 1. tidb-server started and statistics handle has not been initialized.
	if statsHandle == nil {
		return statistics.PseudoTable(tblInfo, false, true)
	}

	if pid == tblInfo.ID || ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		statsTbl = statsHandle.GetTableStats(tblInfo)
	} else {
		usePartitionStats = true
		statsTbl = statsHandle.GetPartitionStats(tblInfo, pid)
	}
	intest.Assert(statsTbl.ColAndIdxExistenceMap != nil, "The existence checking map must not be nil.")

	allowPseudoTblTriggerLoading := false
	// In OptObjectiveDeterminate mode, we need to ignore the real-time stats.
	// To achieve this, we copy the statsTbl and reset the real-time stats fields (set ModifyCount to 0 and set
	// RealtimeCount to the row count from the ANALYZE, which is fetched from loaded stats in GetAnalyzeRowCount()).
	if ctx.GetSessionVars().GetOptObjective() == variable.OptObjectiveDeterminate {
		analyzeCount := max(int64(statsTbl.GetAnalyzeRowCount()), 0)
		// If the two fields are already the values we want, we don't need to modify it, and also we don't need to copy.
		if statsTbl.RealtimeCount != analyzeCount || statsTbl.ModifyCount != 0 {
			// Here is a case that we need specially care about:
			// The original stats table from the stats cache is not a pseudo table, but the analyze row count is 0 (probably
			// because of no col/idx stats are loaded), which will makes it a pseudo table according to the rule 2 below.
			// Normally, a pseudo table won't trigger stats loading since we assume it means "no stats available", but
			// in such case, we need it able to trigger stats loading.
			// That's why we use the special allowPseudoTblTriggerLoading flag here.
			if !statsTbl.Pseudo && statsTbl.RealtimeCount > 0 && analyzeCount == 0 {
				allowPseudoTblTriggerLoading = true
			}
			// Copy it so we can modify the ModifyCount and the RealtimeCount safely.
			statsTbl = statsTbl.ShallowCopy()
			statsTbl.RealtimeCount = analyzeCount
			statsTbl.ModifyCount = 0
		}
	}

	// 2. table row count from statistics is zero.
	if statsTbl.RealtimeCount == 0 {
		countIs0 = true
		core_metrics.PseudoEstimationNotAvailable.Inc()
		return statistics.PseudoTable(tblInfo, allowPseudoTblTriggerLoading, true)
	}

	// 3. statistics is uninitialized or outdated.
	pseudoStatsForUninitialized = !statsTbl.IsInitialized()
	pseudoStatsForOutdated = ctx.GetSessionVars().GetEnablePseudoForOutdatedStats() && statsTbl.IsOutdated()
	if pseudoStatsForUninitialized || pseudoStatsForOutdated {
		tbl := *statsTbl
		tbl.Pseudo = true
		statsTbl = &tbl
		if pseudoStatsForUninitialized {
			core_metrics.PseudoEstimationNotAvailable.Inc()
		} else {
			core_metrics.PseudoEstimationOutdate.Inc()
		}
	}

	return statsTbl
}

// getLatestVersionFromStatsTable gets statistics information for a table specified by "tableID", and get the max
// LastUpdateVersion among all Columns and Indices in it.
// Its overall logic is quite similar to getStatsTable(). During plan cache matching, only the latest version is needed.
// In such case, compared to getStatsTable(), this function can save some copies, memory allocations and unnecessary
// checks. Also, this function won't trigger metrics changes.
func getLatestVersionFromStatsTable(ctx sessionctx.Context, tblInfo *model.TableInfo, pid int64) (version uint64) {
	statsHandle := domain.GetDomain(ctx).StatsHandle()
	// 1. tidb-server started and statistics handle has not been initialized. Pseudo stats table.
	if statsHandle == nil {
		return 0
	}

	var statsTbl *statistics.Table
	if pid == tblInfo.ID || ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		statsTbl = statsHandle.GetTableStats(tblInfo)
	} else {
		statsTbl = statsHandle.GetPartitionStats(tblInfo, pid)
	}

	// 2. Table row count from statistics is zero. Pseudo stats table.
	realtimeRowCount := statsTbl.RealtimeCount
	if ctx.GetSessionVars().GetOptObjective() == variable.OptObjectiveDeterminate {
		realtimeRowCount = max(int64(statsTbl.GetAnalyzeRowCount()), 0)
	}
	if realtimeRowCount == 0 {
		return 0
	}

	// 3. Not pseudo stats table. Return the max LastUpdateVersion among all Columns and Indices
	for _, col := range statsTbl.Columns {
		version = max(version, col.LastUpdateVersion)
	}
	for _, idx := range statsTbl.Indices {
		version = max(version, idx.LastUpdateVersion)
	}
	return version
}

func (b *PlanBuilder) tryBuildCTE(ctx context.Context, tn *ast.TableName, asName *model.CIStr) (base.LogicalPlan, error) {
	for i := len(b.outerCTEs) - 1; i >= 0; i-- {
		cte := b.outerCTEs[i]
		if cte.def.Name.L == tn.Name.L {
			if cte.isBuilding {
				if cte.nonRecursive {
					// Can't see this CTE, try outer definition.
					continue
				}

				// Building the recursive part.
				cte.useRecursive = true
				if cte.seedLP == nil {
					return nil, plannererrors.ErrCTERecursiveRequiresNonRecursiveFirst.FastGenByArgs(tn.Name.String())
				}

				if cte.enterSubquery || cte.recursiveRef {
					return nil, plannererrors.ErrInvalidRequiresSingleReference.FastGenByArgs(tn.Name.String())
				}

				cte.recursiveRef = true
				p := LogicalCTETable{name: cte.def.Name.String(), idForStorage: cte.storageID, seedStat: cte.seedStat, seedSchema: cte.seedLP.Schema()}.Init(b.ctx, b.getSelectOffset())
				p.SetSchema(getResultCTESchema(cte.seedLP.Schema(), b.ctx.GetSessionVars()))
				p.SetOutputNames(cte.seedLP.OutputNames())
				return p, nil
			}

			b.handleHelper.pushMap(nil)

			hasLimit := false
			limitBeg := uint64(0)
			limitEnd := uint64(0)
			if cte.limitLP != nil {
				hasLimit = true
				switch x := cte.limitLP.(type) {
				case *LogicalLimit:
					limitBeg = x.Offset
					limitEnd = x.Offset + x.Count
				case *LogicalTableDual:
					// Beg and End will both be 0.
				default:
					return nil, errors.Errorf("invalid type for limit plan: %v", cte.limitLP)
				}
			}

			if cte.cteClass == nil {
				cte.cteClass = &CTEClass{
					IsDistinct:               cte.isDistinct,
					seedPartLogicalPlan:      cte.seedLP,
					recursivePartLogicalPlan: cte.recurLP,
					IDForStorage:             cte.storageID,
					optFlag:                  cte.optFlag,
					HasLimit:                 hasLimit,
					LimitBeg:                 limitBeg,
					LimitEnd:                 limitEnd,
					pushDownPredicates:       make([]expression.Expression, 0),
					ColumnMap:                make(map[string]*expression.Column),
				}
			}
			var p base.LogicalPlan
			lp := LogicalCTE{cteAsName: tn.Name, cteName: tn.Name, cte: cte.cteClass, seedStat: cte.seedStat}.Init(b.ctx, b.getSelectOffset())
			prevSchema := cte.seedLP.Schema().Clone()
			lp.SetSchema(getResultCTESchema(cte.seedLP.Schema(), b.ctx.GetSessionVars()))

			// If current CTE query contain another CTE which 'containAggOrWindow' is true, current CTE 'containAggOrWindow' will be true
			if b.buildingCTE {
				b.outerCTEs[len(b.outerCTEs)-1].containAggOrWindow = cte.containAggOrWindow || b.outerCTEs[len(b.outerCTEs)-1].containAggOrWindow
			}
			// Compute cte inline
			b.computeCTEInlineFlag(cte)

			if cte.recurLP == nil && cte.isInline {
				saveCte := make([]*cteInfo, len(b.outerCTEs[i:]))
				copy(saveCte, b.outerCTEs[i:])
				b.outerCTEs = b.outerCTEs[:i]
				o := b.buildingCTE
				b.buildingCTE = false
				//nolint:all_revive,revive
				defer func() {
					b.outerCTEs = append(b.outerCTEs, saveCte...)
					b.buildingCTE = o
				}()
				return b.buildDataSourceFromCTEMerge(ctx, cte.def)
			}

			for i, col := range lp.schema.Columns {
				lp.cte.ColumnMap[string(col.HashCode())] = prevSchema.Columns[i]
			}
			p = lp
			p.SetOutputNames(cte.seedLP.OutputNames())
			if len(asName.String()) > 0 {
				lp.cteAsName = *asName
				var on types.NameSlice
				for _, name := range p.OutputNames() {
					cpOn := *name
					cpOn.TblName = *asName
					on = append(on, &cpOn)
				}
				p.SetOutputNames(on)
			}
			return p, nil
		}
	}

	return nil, nil
}

// computeCTEInlineFlag, Combine the declaration of CTE and the use of CTE to jointly determine **whether a CTE can be inlined**
/*
   There are some cases that CTE must be not inlined.
   1. CTE is recursive CTE.
   2. CTE contains agg or window and it is referenced by recursive part of CTE.
   3. Consumer count of CTE is more than one.
   If 1 or 2 conditions are met, CTE cannot be inlined.
   But if query is hint by 'merge()' or session variable "tidb_opt_force_inline_cte",
     CTE will still not be inlined but a warning will be recorded "Hint or session variables are invalid"
   If 3 condition is met, CTE can be inlined by hint and session variables.
*/
func (b *PlanBuilder) computeCTEInlineFlag(cte *cteInfo) {
	if cte.recurLP != nil {
		if cte.forceInlineByHintOrVar {
			b.ctx.GetSessionVars().StmtCtx.SetHintWarning(
				fmt.Sprintf("Recursive CTE %s can not be inlined by merge() or tidb_opt_force_inline_cte.", cte.def.Name))
		}
	} else if cte.containAggOrWindow && b.buildingRecursivePartForCTE {
		if cte.forceInlineByHintOrVar {
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(plannererrors.ErrCTERecursiveForbidsAggregation.FastGenByArgs(cte.def.Name))
		}
	} else if cte.consumerCount > 1 {
		if cte.forceInlineByHintOrVar {
			cte.isInline = true
		}
	} else {
		cte.isInline = true
	}
}

func (b *PlanBuilder) buildDataSourceFromCTEMerge(ctx context.Context, cte *ast.CommonTableExpression) (base.LogicalPlan, error) {
	p, err := b.buildResultSetNode(ctx, cte.Query.Query, true)
	if err != nil {
		return nil, err
	}
	b.handleHelper.popMap()
	outPutNames := p.OutputNames()
	for _, name := range outPutNames {
		name.TblName = cte.Name
		name.DBName = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
	}

	if len(cte.ColNameList) > 0 {
		if len(cte.ColNameList) != len(p.OutputNames()) {
			return nil, errors.New("CTE columns length is not consistent")
		}
		for i, n := range cte.ColNameList {
			outPutNames[i].ColName = n
		}
	}
	p.SetOutputNames(outPutNames)
	return p, nil
}

func (b *PlanBuilder) buildDataSource(ctx context.Context, tn *ast.TableName, asName *model.CIStr) (base.LogicalPlan, error) {
	b.optFlag |= flagPredicateSimplification
	dbName := tn.Schema
	sessionVars := b.ctx.GetSessionVars()

	if dbName.L == "" {
		// Try CTE.
		p, err := b.tryBuildCTE(ctx, tn, asName)
		if err != nil || p != nil {
			return p, err
		}
		dbName = model.NewCIStr(sessionVars.CurrentDB)
	}

	is := b.is
	if len(b.buildingViewStack) > 0 {
		// For tables in view, always ignore local temporary table, considering the below case:
		// If a user created a normal table `t1` and a view `v1` referring `t1`, and then a local temporary table with a same name `t1` is created.
		// At this time, executing 'select * from v1' should still return all records from normal table `t1` instead of temporary table `t1`.
		is = temptable.DetachLocalTemporaryTableInfoSchema(is)
	}

	tbl, err := is.TableByName(dbName, tn.Name)
	if err != nil {
		return nil, err
	}

	tbl, err = tryLockMDLAndUpdateSchemaIfNecessary(b.ctx, dbName, tbl, b.is)
	if err != nil {
		return nil, err
	}
	tableInfo := tbl.Meta()

	if b.isCreateView && tableInfo.TempTableType == model.TempTableLocal {
		return nil, plannererrors.ErrViewSelectTemporaryTable.GenWithStackByArgs(tn.Name)
	}

	var authErr error
	if sessionVars.User != nil {
		authErr = plannererrors.ErrTableaccessDenied.FastGenByArgs("SELECT", sessionVars.User.AuthUsername, sessionVars.User.AuthHostname, tableInfo.Name.L)
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, dbName.L, tableInfo.Name.L, "", authErr)

	if tbl.Type().IsVirtualTable() {
		if tn.TableSample != nil {
			return nil, expression.ErrInvalidTableSample.GenWithStackByArgs("Unsupported TABLESAMPLE in virtual tables")
		}
		return b.buildMemTable(ctx, dbName, tableInfo)
	}

	tblName := *asName
	if tblName.L == "" {
		tblName = tn.Name
	}

	if tableInfo.GetPartitionInfo() != nil {
		// If `UseDynamicPruneMode` already been false, then we don't need to check whether execute `flagPartitionProcessor`
		// otherwise we need to check global stats initialized for each partition table
		if !b.ctx.GetSessionVars().IsDynamicPartitionPruneEnabled() {
			b.optFlag = b.optFlag | flagPartitionProcessor
		} else {
			if !b.ctx.GetSessionVars().StmtCtx.UseDynamicPruneMode {
				b.optFlag = b.optFlag | flagPartitionProcessor
			} else {
				h := domain.GetDomain(b.ctx).StatsHandle()
				tblStats := h.GetTableStats(tableInfo)
				isDynamicEnabled := b.ctx.GetSessionVars().IsDynamicPartitionPruneEnabled()
				globalStatsReady := tblStats.IsAnalyzed()
				skipMissingPartition := b.ctx.GetSessionVars().SkipMissingPartitionStats
				// If we already enabled the tidb_skip_missing_partition_stats, the global stats can be treated as exist.
				allowDynamicWithoutStats := fixcontrol.GetBoolWithDefault(b.ctx.GetSessionVars().GetOptimizerFixControlMap(), fixcontrol.Fix44262, skipMissingPartition)

				// If dynamic partition prune isn't enabled or global stats is not ready, we won't enable dynamic prune mode in query
				usePartitionProcessor := !isDynamicEnabled || (!globalStatsReady && !allowDynamicWithoutStats)

				failpoint.Inject("forceDynamicPrune", func(val failpoint.Value) {
					if val.(bool) {
						if isDynamicEnabled {
							usePartitionProcessor = false
						}
					}
				})

				if usePartitionProcessor {
					b.optFlag = b.optFlag | flagPartitionProcessor
					b.ctx.GetSessionVars().StmtCtx.UseDynamicPruneMode = false
					if isDynamicEnabled {
						b.ctx.GetSessionVars().StmtCtx.AppendWarning(
							fmt.Errorf("disable dynamic pruning due to %s has no global stats", tableInfo.Name.String()))
					}
				}
			}
		}
		pt := tbl.(table.PartitionedTable)
		// check partition by name.
		if len(tn.PartitionNames) > 0 {
			pids := make(map[int64]struct{}, len(tn.PartitionNames))
			for _, name := range tn.PartitionNames {
				pid, err := tables.FindPartitionByName(tableInfo, name.L)
				if err != nil {
					return nil, err
				}
				pids[pid] = struct{}{}
			}
			pt = tables.NewPartitionTableWithGivenSets(pt, pids)
		}
		b.partitionedTable = append(b.partitionedTable, pt)
	} else if len(tn.PartitionNames) != 0 {
		return nil, plannererrors.ErrPartitionClauseOnNonpartitioned
	}

	possiblePaths, err := getPossibleAccessPaths(b.ctx, b.TableHints(), tn.IndexHints, tbl, dbName, tblName, b.isForUpdateRead, b.optFlag&flagPartitionProcessor > 0)
	if err != nil {
		return nil, err
	}

	if tableInfo.IsView() {
		if tn.TableSample != nil {
			return nil, expression.ErrInvalidTableSample.GenWithStackByArgs("Unsupported TABLESAMPLE in views")
		}

		// Get the hints belong to the current view.
		currentQBNameMap4View := make(map[string][]ast.HintTable)
		currentViewHints := make(map[string][]*ast.TableOptimizerHint)
		for qbName, viewQBNameHintTable := range b.hintProcessor.ViewQBNameToTable {
			if len(viewQBNameHintTable) == 0 {
				continue
			}
			viewSelectOffset := b.getSelectOffset()

			var viewHintSelectOffset int
			if viewQBNameHintTable[0].QBName.L == "" {
				// If we do not explicit set the qbName, we will set the empty qb name to @sel_1.
				viewHintSelectOffset = 1
			} else {
				viewHintSelectOffset = b.hintProcessor.GetHintOffset(viewQBNameHintTable[0].QBName, viewSelectOffset)
			}

			// Check whether the current view can match the view name in the hint.
			if viewQBNameHintTable[0].TableName.L == tblName.L && viewHintSelectOffset == viewSelectOffset {
				// If the view hint can match the current view, we pop the first view table in the query block hint's table list.
				// It means the hint belong the current view, the first view name in hint is matched.
				// Because of the nested views, so we should check the left table list in hint when build the data source from the view inside the current view.
				currentQBNameMap4View[qbName] = viewQBNameHintTable[1:]
				currentViewHints[qbName] = b.hintProcessor.ViewQBNameToHints[qbName]
				b.hintProcessor.ViewQBNameUsed[qbName] = struct{}{}
			}
		}
		return b.BuildDataSourceFromView(ctx, dbName, tableInfo, currentQBNameMap4View, currentViewHints)
	}

	if tableInfo.IsSequence() {
		if tn.TableSample != nil {
			return nil, expression.ErrInvalidTableSample.GenWithStackByArgs("Unsupported TABLESAMPLE in sequences")
		}
		// When the source is a Sequence, we convert it to a TableDual, as what most databases do.
		return b.buildTableDual(), nil
	}

	// remain tikv access path to generate point get acceess path if existed
	// see detail in issue: https://github.com/pingcap/tidb/issues/39543
	if !(b.isForUpdateRead && b.ctx.GetSessionVars().TxnCtx.IsExplicit) {
		// Skip storage engine check for CreateView.
		if b.capFlag&canExpandAST == 0 {
			possiblePaths, err = filterPathByIsolationRead(b.ctx, possiblePaths, tblName, dbName)
			if err != nil {
				return nil, err
			}
		}
	}

	// Try to substitute generate column only if there is an index on generate column.
	for _, index := range tableInfo.Indices {
		if index.State != model.StatePublic {
			continue
		}
		for _, indexCol := range index.Columns {
			colInfo := tbl.Cols()[indexCol.Offset]
			if colInfo.IsGenerated() && !colInfo.GeneratedStored {
				b.optFlag |= flagGcSubstitute
				break
			}
		}
	}

	var columns []*table.Column
	if b.inUpdateStmt {
		// create table t(a int, b int).
		// Imagine that, There are 2 TiDB instances in the cluster, name A, B. We add a column `c` to table t in the TiDB cluster.
		// One of the TiDB, A, the column type in its infoschema is changed to public. And in the other TiDB, the column type is
		// still StateWriteReorganization.
		// TiDB A: insert into t values(1, 2, 3);
		// TiDB B: update t set a = 2 where b = 2;
		// If we use tbl.Cols() here, the update statement, will ignore the col `c`, and the data `3` will lost.
		columns = tbl.WritableCols()
	} else if b.inDeleteStmt {
		// DeletableCols returns all columns of the table in deletable states.
		columns = tbl.DeletableCols()
	} else {
		columns = tbl.Cols()
	}
	// extract the IndexMergeHint
	var indexMergeHints []h.HintedIndex
	if hints := b.TableHints(); hints != nil {
		for i, hint := range hints.IndexMergeHintList {
			if hint.Match(dbName, tblName) {
				hints.IndexMergeHintList[i].Matched = true
				// check whether the index names in IndexMergeHint are valid.
				invalidIdxNames := make([]string, 0, len(hint.IndexHint.IndexNames))
				for _, idxName := range hint.IndexHint.IndexNames {
					hasIdxName := false
					for _, path := range possiblePaths {
						if path.IsTablePath() {
							if idxName.L == "primary" {
								hasIdxName = true
								break
							}
							continue
						}
						if idxName.L == path.Index.Name.L {
							hasIdxName = true
							break
						}
					}
					if !hasIdxName {
						invalidIdxNames = append(invalidIdxNames, idxName.String())
					}
				}
				if len(invalidIdxNames) == 0 {
					indexMergeHints = append(indexMergeHints, hint)
				} else {
					// Append warning if there are invalid index names.
					errMsg := fmt.Sprintf("use_index_merge(%s) is inapplicable, check whether the indexes (%s) "+
						"exist, or the indexes are conflicted with use_index/ignore_index/force_index hints.",
						hint.IndexString(), strings.Join(invalidIdxNames, ", "))
					b.ctx.GetSessionVars().StmtCtx.SetHintWarning(errMsg)
				}
			}
		}
	}
	ds := DataSource{
		DBName:              dbName,
		TableAsName:         asName,
		table:               tbl,
		tableInfo:           tableInfo,
		physicalTableID:     tableInfo.ID,
		astIndexHints:       tn.IndexHints,
		IndexHints:          b.TableHints().IndexHintList,
		indexMergeHints:     indexMergeHints,
		possibleAccessPaths: possiblePaths,
		Columns:             make([]*model.ColumnInfo, 0, len(columns)),
		partitionNames:      tn.PartitionNames,
		TblCols:             make([]*expression.Column, 0, len(columns)),
		preferPartitions:    make(map[int][]model.CIStr),
		is:                  b.is,
		isForUpdateRead:     b.isForUpdateRead,
	}.Init(b.ctx, b.getSelectOffset())
	var handleCols util.HandleCols
	schema := expression.NewSchema(make([]*expression.Column, 0, len(columns))...)
	names := make([]*types.FieldName, 0, len(columns))
	for i, col := range columns {
		ds.Columns = append(ds.Columns, col.ToInfo())
		names = append(names, &types.FieldName{
			DBName:      dbName,
			TblName:     tableInfo.Name,
			ColName:     col.Name,
			OrigTblName: tableInfo.Name,
			OrigColName: col.Name,
			// For update statement and delete statement, internal version should see the special middle state column, while user doesn't.
			NotExplicitUsable: col.State != model.StatePublic,
		})
		newCol := &expression.Column{
			UniqueID: sessionVars.AllocPlanColumnID(),
			ID:       col.ID,
			RetType:  col.FieldType.Clone(),
			OrigName: names[i].String(),
			IsHidden: col.Hidden,
		}
		if col.IsPKHandleColumn(tableInfo) {
			handleCols = util.NewIntHandleCols(newCol)
		}
		schema.Append(newCol)
		ds.TblCols = append(ds.TblCols, newCol)
	}
	// We append an extra handle column to the schema when the handle
	// column is not the primary key of "ds".
	if handleCols == nil {
		if tableInfo.IsCommonHandle {
			primaryIdx := tables.FindPrimaryIndex(tableInfo)
			handleCols = util.NewCommonHandleCols(b.ctx.GetSessionVars().StmtCtx, tableInfo, primaryIdx, ds.TblCols)
		} else {
			extraCol := ds.newExtraHandleSchemaCol()
			handleCols = util.NewIntHandleCols(extraCol)
			ds.Columns = append(ds.Columns, model.NewExtraHandleColInfo())
			schema.Append(extraCol)
			names = append(names, &types.FieldName{
				DBName:      dbName,
				TblName:     tableInfo.Name,
				ColName:     model.ExtraHandleName,
				OrigColName: model.ExtraHandleName,
			})
			ds.TblCols = append(ds.TblCols, extraCol)
		}
	}
	ds.handleCols = handleCols
	ds.unMutableHandleCols = handleCols
	handleMap := make(map[int64][]util.HandleCols)
	handleMap[tableInfo.ID] = []util.HandleCols{handleCols}
	b.handleHelper.pushMap(handleMap)
	ds.SetSchema(schema)
	ds.names = names
	ds.setPreferredStoreType(b.TableHints())
	ds.SampleInfo = tablesampler.NewTableSampleInfo(tn.TableSample, schema, b.partitionedTable)
	b.isSampling = ds.SampleInfo != nil

	for i, colExpr := range ds.Schema().Columns {
		var expr expression.Expression
		if i < len(columns) {
			if columns[i].IsGenerated() && !columns[i].GeneratedStored {
				var err error
				originVal := b.allowBuildCastArray
				b.allowBuildCastArray = true
				expr, _, err = b.rewrite(ctx, columns[i].GeneratedExpr.Clone(), ds, nil, true)
				b.allowBuildCastArray = originVal
				if err != nil {
					return nil, err
				}
				colExpr.VirtualExpr = expr.Clone()
			}
		}
	}

	// Init commonHandleCols and commonHandleLens for data source.
	if tableInfo.IsCommonHandle {
		ds.commonHandleCols, ds.commonHandleLens = expression.IndexInfo2Cols(ds.Columns, ds.schema.Columns, tables.FindPrimaryIndex(tableInfo))
	}
	// Init FullIdxCols, FullIdxColLens for accessPaths.
	for _, path := range ds.possibleAccessPaths {
		if !path.IsIntHandlePath {
			path.FullIdxCols, path.FullIdxColLens = expression.IndexInfo2Cols(ds.Columns, ds.schema.Columns, path.Index)

			// check whether the path's index has a tidb_shard() prefix and the index column count
			// more than 1. e.g. index(tidb_shard(a), a)
			// set UkShardIndexPath only for unique secondary index
			if !path.IsCommonHandlePath {
				// tidb_shard expression must be first column of index
				col := path.FullIdxCols[0]
				if col != nil &&
					expression.GcColumnExprIsTidbShard(col.VirtualExpr) &&
					len(path.Index.Columns) > 1 &&
					path.Index.Unique {
					path.IsUkShardIndexPath = true
					ds.containExprPrefixUk = true
				}
			}
		}
	}

	var result base.LogicalPlan = ds
	dirty := tableHasDirtyContent(b.ctx, tableInfo)
	if dirty || tableInfo.TempTableType == model.TempTableLocal || tableInfo.TableCacheStatusType == model.TableCacheStatusEnable {
		us := LogicalUnionScan{handleCols: handleCols}.Init(b.ctx, b.getSelectOffset())
		us.SetChildren(ds)
		if tableInfo.Partition != nil && b.optFlag&flagPartitionProcessor == 0 {
			// Adding ExtraPhysTblIDCol for UnionScan (transaction buffer handling)
			// Not using old static prune mode
			// Single TableReader for all partitions, needs the PhysTblID from storage
			_ = ds.AddExtraPhysTblIDColumn()
		}
		result = us
	}

	// Adding ExtraPhysTblIDCol for SelectLock (SELECT FOR UPDATE) is done when building SelectLock

	if sessionVars.StmtCtx.TblInfo2UnionScan == nil {
		sessionVars.StmtCtx.TblInfo2UnionScan = make(map[*model.TableInfo]bool)
	}
	sessionVars.StmtCtx.TblInfo2UnionScan[tableInfo] = dirty

	return result, nil
}

// ExtractFD implements the base.LogicalPlan interface.
func (ds *DataSource) ExtractFD() *fd.FDSet {
	// FD in datasource (leaf node) can be cached and reused.
	// Once the all conditions are not equal to nil, built it again.
	if ds.FDs() == nil || ds.allConds != nil {
		fds := &fd.FDSet{HashCodeToUniqueID: make(map[string]int)}
		allCols := intset.NewFastIntSet()
		// should use the column's unique ID avoiding fdSet conflict.
		for _, col := range ds.TblCols {
			// todo: change it to int64
			allCols.Insert(int(col.UniqueID))
		}
		// int pk doesn't store its index column in indexInfo.
		if ds.tableInfo.PKIsHandle {
			keyCols := intset.NewFastIntSet()
			for _, col := range ds.TblCols {
				if mysql.HasPriKeyFlag(col.RetType.GetFlag()) {
					keyCols.Insert(int(col.UniqueID))
				}
			}
			fds.AddStrictFunctionalDependency(keyCols, allCols)
			fds.MakeNotNull(keyCols)
		}
		// we should check index valid while forUpdateRead, see detail in https://github.com/pingcap/tidb/pull/22152
		var (
			latestIndexes map[int64]*model.IndexInfo
			changed       bool
			err           error
		)
		check := ds.SCtx().GetSessionVars().IsIsolation(ast.ReadCommitted) || ds.isForUpdateRead
		check = check && ds.SCtx().GetSessionVars().ConnectionID > 0
		if check {
			latestIndexes, changed, err = getLatestIndexInfo(ds.SCtx(), ds.table.Meta().ID, 0)
			if err != nil {
				ds.SetFDs(fds)
				return fds
			}
		}
		// other indices including common handle.
		for _, idx := range ds.tableInfo.Indices {
			keyCols := intset.NewFastIntSet()
			allColIsNotNull := true
			if ds.isForUpdateRead && changed {
				latestIndex, ok := latestIndexes[idx.ID]
				if !ok || latestIndex.State != model.StatePublic {
					continue
				}
			}
			if idx.State != model.StatePublic {
				continue
			}
			for _, idxCol := range idx.Columns {
				// Note: even the prefix column can also be the FD. For example:
				// unique(char_column(10)), will also guarantee the prefix to be
				// the unique which means the while column is unique too.
				refCol := ds.tableInfo.Columns[idxCol.Offset]
				if !mysql.HasNotNullFlag(refCol.GetFlag()) {
					allColIsNotNull = false
				}
				keyCols.Insert(int(ds.TblCols[idxCol.Offset].UniqueID))
			}
			if idx.Primary {
				fds.AddStrictFunctionalDependency(keyCols, allCols)
				fds.MakeNotNull(keyCols)
			} else if idx.Unique {
				if allColIsNotNull {
					fds.AddStrictFunctionalDependency(keyCols, allCols)
					fds.MakeNotNull(keyCols)
				} else {
					// unique index:
					// 1: normal value should be unique
					// 2: null value can be multiple
					// for this kind of lax to be strict, we need to make the determinant not-null.
					fds.AddLaxFunctionalDependency(keyCols, allCols)
				}
			}
		}
		// handle the datasource conditions (maybe pushed down from upper layer OP)
		if len(ds.allConds) != 0 {
			// extract the not null attributes from selection conditions.
			notnullColsUniqueIDs := extractNotNullFromConds(ds.allConds, ds)

			// extract the constant cols from selection conditions.
			constUniqueIDs := extractConstantCols(ds.allConds, ds.SCtx(), fds)

			// extract equivalence cols.
			equivUniqueIDs := extractEquivalenceCols(ds.allConds, ds.SCtx(), fds)

			// apply conditions to FD.
			fds.MakeNotNull(notnullColsUniqueIDs)
			fds.AddConstants(constUniqueIDs)
			for _, equiv := range equivUniqueIDs {
				fds.AddEquivalence(equiv[0], equiv[1])
			}
		}
		// build the dependency for generated columns.
		// the generated column is sequentially dependent on the forward column.
		// a int, b int as (a+1), c int as (b+1), here we can build the strict FD down:
		// {a} -> {b}, {b} -> {c}, put the maintenance of the dependencies between generated columns to the FD graph.
		notNullCols := intset.NewFastIntSet()
		for _, col := range ds.TblCols {
			if col.VirtualExpr != nil {
				dependencies := intset.NewFastIntSet()
				dependencies.Insert(int(col.UniqueID))
				// dig out just for 1 level.
				directBaseCol := expression.ExtractColumns(col.VirtualExpr)
				determinant := intset.NewFastIntSet()
				for _, col := range directBaseCol {
					determinant.Insert(int(col.UniqueID))
				}
				fds.AddStrictFunctionalDependency(determinant, dependencies)
			}
			if mysql.HasNotNullFlag(col.RetType.GetFlag()) {
				notNullCols.Insert(int(col.UniqueID))
			}
		}
		fds.MakeNotNull(notNullCols)
		ds.SetFDs(fds)
	}
	return ds.FDs()
}

func (b *PlanBuilder) timeRangeForSummaryTable() util.QueryTimeRange {
	const defaultSummaryDuration = 30 * time.Minute
	hints := b.TableHints()
	// User doesn't use TIME_RANGE hint
	if hints == nil || (hints.TimeRangeHint.From == "" && hints.TimeRangeHint.To == "") {
		to := time.Now()
		from := to.Add(-defaultSummaryDuration)
		return util.QueryTimeRange{From: from, To: to}
	}

	// Parse time specified by user via TIM_RANGE hint
	parse := func(s string) (time.Time, bool) {
		t, err := time.ParseInLocation(util.MetricTableTimeFormat, s, time.Local)
		if err != nil {
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
		}
		return t, err == nil
	}
	from, fromValid := parse(hints.TimeRangeHint.From)
	to, toValid := parse(hints.TimeRangeHint.To)
	switch {
	case !fromValid && !toValid:
		to = time.Now()
		from = to.Add(-defaultSummaryDuration)
	case fromValid && !toValid:
		to = from.Add(defaultSummaryDuration)
	case !fromValid && toValid:
		from = to.Add(-defaultSummaryDuration)
	}

	return util.QueryTimeRange{From: from, To: to}
}

func (b *PlanBuilder) buildMemTable(_ context.Context, dbName model.CIStr, tableInfo *model.TableInfo) (base.LogicalPlan, error) {
	// We can use the `tableInfo.Columns` directly because the memory table has
	// a stable schema and there is no online DDL on the memory table.
	schema := expression.NewSchema(make([]*expression.Column, 0, len(tableInfo.Columns))...)
	names := make([]*types.FieldName, 0, len(tableInfo.Columns))
	var handleCols util.HandleCols
	for _, col := range tableInfo.Columns {
		names = append(names, &types.FieldName{
			DBName:      dbName,
			TblName:     tableInfo.Name,
			ColName:     col.Name,
			OrigTblName: tableInfo.Name,
			OrigColName: col.Name,
		})
		// NOTE: Rewrite the expression if memory table supports generated columns in the future
		newCol := &expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			ID:       col.ID,
			RetType:  &col.FieldType,
		}
		if tableInfo.PKIsHandle && mysql.HasPriKeyFlag(col.GetFlag()) {
			handleCols = util.NewIntHandleCols(newCol)
		}
		schema.Append(newCol)
	}

	if handleCols != nil {
		handleMap := make(map[int64][]util.HandleCols)
		handleMap[tableInfo.ID] = []util.HandleCols{handleCols}
		b.handleHelper.pushMap(handleMap)
	} else {
		b.handleHelper.pushMap(nil)
	}

	// NOTE: Add a `LogicalUnionScan` if we support update memory table in the future
	p := LogicalMemTable{
		DBName:    dbName,
		TableInfo: tableInfo,
		Columns:   make([]*model.ColumnInfo, len(tableInfo.Columns)),
	}.Init(b.ctx, b.getSelectOffset())
	p.SetSchema(schema)
	p.names = names
	copy(p.Columns, tableInfo.Columns)

	// Some memory tables can receive some predicates
	switch dbName.L {
	case util2.MetricSchemaName.L:
		p.Extractor = newMetricTableExtractor()
	case util2.InformationSchemaName.L:
		switch strings.ToUpper(tableInfo.Name.O) {
		case infoschema.TableClusterConfig, infoschema.TableClusterLoad, infoschema.TableClusterHardware, infoschema.TableClusterSystemInfo:
			p.Extractor = &ClusterTableExtractor{}
		case infoschema.TableClusterLog:
			p.Extractor = &ClusterLogTableExtractor{}
		case infoschema.TableTiDBHotRegionsHistory:
			p.Extractor = &HotRegionsHistoryTableExtractor{}
		case infoschema.TableInspectionResult:
			p.Extractor = &InspectionResultTableExtractor{}
			p.QueryTimeRange = b.timeRangeForSummaryTable()
		case infoschema.TableInspectionSummary:
			p.Extractor = &InspectionSummaryTableExtractor{}
			p.QueryTimeRange = b.timeRangeForSummaryTable()
		case infoschema.TableInspectionRules:
			p.Extractor = &InspectionRuleTableExtractor{}
		case infoschema.TableMetricSummary, infoschema.TableMetricSummaryByLabel:
			p.Extractor = &MetricSummaryTableExtractor{}
			p.QueryTimeRange = b.timeRangeForSummaryTable()
		case infoschema.TableSlowQuery:
			p.Extractor = &SlowQueryExtractor{}
		case infoschema.TableStorageStats:
			p.Extractor = &TableStorageStatsExtractor{}
		case infoschema.TableTiFlashTables, infoschema.TableTiFlashSegments:
			p.Extractor = &TiFlashSystemTableExtractor{}
		case infoschema.TableStatementsSummary, infoschema.TableStatementsSummaryHistory:
			p.Extractor = &StatementsSummaryExtractor{}
		case infoschema.TableTiKVRegionPeers:
			p.Extractor = &TikvRegionPeersExtractor{}
		case infoschema.TableColumns:
			p.Extractor = &ColumnsTableExtractor{}
		case infoschema.TableTables,
			infoschema.TableReferConst,
			infoschema.TableKeyColumn,
			infoschema.TableStatistics:
			p.Extractor = &InfoSchemaTablesExtractor{}
		case infoschema.TableTiKVRegionStatus:
			p.Extractor = &TiKVRegionStatusExtractor{tablesID: make([]int64, 0)}
		}
	}
	return p, nil
}

// checkRecursiveView checks whether this view is recursively defined.
func (b *PlanBuilder) checkRecursiveView(dbName model.CIStr, tableName model.CIStr) (func(), error) {
	viewFullName := dbName.L + "." + tableName.L
	if b.buildingViewStack == nil {
		b.buildingViewStack = set.NewStringSet()
	}
	// If this view has already been on the building stack, it means
	// this view contains a recursive definition.
	if b.buildingViewStack.Exist(viewFullName) {
		return nil, plannererrors.ErrViewRecursive.GenWithStackByArgs(dbName.O, tableName.O)
	}
	// If the view is being renamed, we return the mysql compatible error message.
	if b.capFlag&renameView != 0 && viewFullName == b.renamingViewName {
		return nil, plannererrors.ErrNoSuchTable.GenWithStackByArgs(dbName.O, tableName.O)
	}
	b.buildingViewStack.Insert(viewFullName)
	return func() { delete(b.buildingViewStack, viewFullName) }, nil
}

// BuildDataSourceFromView is used to build base.LogicalPlan from view
// qbNameMap4View and viewHints are used for the view's hint.
// qbNameMap4View maps the query block name to the view table lists.
// viewHints group the view hints based on the view's query block name.
func (b *PlanBuilder) BuildDataSourceFromView(ctx context.Context, dbName model.CIStr, tableInfo *model.TableInfo, qbNameMap4View map[string][]ast.HintTable, viewHints map[string][]*ast.TableOptimizerHint) (base.LogicalPlan, error) {
	viewDepth := b.ctx.GetSessionVars().StmtCtx.ViewDepth
	b.ctx.GetSessionVars().StmtCtx.ViewDepth++
	deferFunc, err := b.checkRecursiveView(dbName, tableInfo.Name)
	if err != nil {
		return nil, err
	}
	defer deferFunc()

	charset, collation := b.ctx.GetSessionVars().GetCharsetInfo()
	viewParser := parser.New()
	viewParser.SetParserConfig(b.ctx.GetSessionVars().BuildParserConfig())
	selectNode, err := viewParser.ParseOneStmt(tableInfo.View.SelectStmt, charset, collation)
	if err != nil {
		return nil, err
	}
	originalVisitInfo := b.visitInfo
	b.visitInfo = make([]visitInfo, 0)

	// For the case that views appear in CTE queries,
	// we need to save the CTEs after the views are established.
	var saveCte []*cteInfo
	if len(b.outerCTEs) > 0 {
		saveCte = make([]*cteInfo, len(b.outerCTEs))
		copy(saveCte, b.outerCTEs)
	} else {
		saveCte = nil
	}
	o := b.buildingCTE
	b.buildingCTE = false
	defer func() {
		b.outerCTEs = saveCte
		b.buildingCTE = o
	}()

	hintProcessor := h.NewQBHintHandler(b.ctx.GetSessionVars().StmtCtx)
	selectNode.Accept(hintProcessor)
	currentQbNameMap4View := make(map[string][]ast.HintTable)
	currentQbHints4View := make(map[string][]*ast.TableOptimizerHint)
	currentQbHints := make(map[int][]*ast.TableOptimizerHint)
	currentQbNameMap := make(map[string]int)

	for qbName, viewQbNameHint := range qbNameMap4View {
		// Check whether the view hint belong the current view or its nested views.
		qbOffset := -1
		if len(viewQbNameHint) == 0 {
			qbOffset = 1
		} else if len(viewQbNameHint) == 1 && viewQbNameHint[0].TableName.L == "" {
			qbOffset = hintProcessor.GetHintOffset(viewQbNameHint[0].QBName, -1)
		} else {
			currentQbNameMap4View[qbName] = viewQbNameHint
			currentQbHints4View[qbName] = viewHints[qbName]
		}

		if qbOffset != -1 {
			// If the hint belongs to the current view and not belongs to it's nested views, we should convert the view hint to the normal hint.
			// After we convert the view hint to the normal hint, it can be reused the origin hint's infrastructure.
			currentQbHints[qbOffset] = viewHints[qbName]
			currentQbNameMap[qbName] = qbOffset

			delete(qbNameMap4View, qbName)
			delete(viewHints, qbName)
		}
	}

	hintProcessor.ViewQBNameToTable = qbNameMap4View
	hintProcessor.ViewQBNameToHints = viewHints
	hintProcessor.ViewQBNameUsed = make(map[string]struct{})
	hintProcessor.QBOffsetToHints = currentQbHints
	hintProcessor.QBNameToSelOffset = currentQbNameMap

	originHintProcessor := b.hintProcessor
	originPlannerSelectBlockAsName := b.ctx.GetSessionVars().PlannerSelectBlockAsName.Load()
	b.hintProcessor = hintProcessor
	newPlannerSelectBlockAsName := make([]ast.HintTable, hintProcessor.MaxSelectStmtOffset()+1)
	b.ctx.GetSessionVars().PlannerSelectBlockAsName.Store(&newPlannerSelectBlockAsName)
	defer func() {
		b.hintProcessor.HandleUnusedViewHints()
		b.hintProcessor = originHintProcessor
		b.ctx.GetSessionVars().PlannerSelectBlockAsName.Store(originPlannerSelectBlockAsName)
	}()
	selectLogicalPlan, err := b.Build(ctx, selectNode)
	if err != nil {
		if terror.ErrorNotEqual(err, plannererrors.ErrViewRecursive) &&
			terror.ErrorNotEqual(err, plannererrors.ErrNoSuchTable) &&
			terror.ErrorNotEqual(err, plannererrors.ErrInternal) &&
			terror.ErrorNotEqual(err, plannererrors.ErrFieldNotInGroupBy) &&
			terror.ErrorNotEqual(err, plannererrors.ErrMixOfGroupFuncAndFields) &&
			terror.ErrorNotEqual(err, plannererrors.ErrViewNoExplain) &&
			terror.ErrorNotEqual(err, plannererrors.ErrNotSupportedYet) {
			err = plannererrors.ErrViewInvalid.GenWithStackByArgs(dbName.O, tableInfo.Name.O)
		}
		return nil, err
	}
	pm := privilege.GetPrivilegeManager(b.ctx)
	if viewDepth != 0 &&
		b.ctx.GetSessionVars().StmtCtx.InExplainStmt &&
		pm != nil &&
		!pm.RequestVerification(b.ctx.GetSessionVars().ActiveRoles, dbName.L, tableInfo.Name.L, "", mysql.SelectPriv) {
		return nil, plannererrors.ErrViewNoExplain
	}
	if tableInfo.View.Security == model.SecurityDefiner {
		if pm != nil {
			for _, v := range b.visitInfo {
				if !pm.RequestVerificationWithUser(v.db, v.table, v.column, v.privilege, tableInfo.View.Definer) {
					return nil, plannererrors.ErrViewInvalid.GenWithStackByArgs(dbName.O, tableInfo.Name.O)
				}
			}
		}
		b.visitInfo = b.visitInfo[:0]
	}
	b.visitInfo = append(originalVisitInfo, b.visitInfo...)

	if b.ctx.GetSessionVars().StmtCtx.InExplainStmt {
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ShowViewPriv, dbName.L, tableInfo.Name.L, "", plannererrors.ErrViewNoExplain)
	}

	if len(tableInfo.Columns) != selectLogicalPlan.Schema().Len() {
		return nil, plannererrors.ErrViewInvalid.GenWithStackByArgs(dbName.O, tableInfo.Name.O)
	}

	return b.buildProjUponView(ctx, dbName, tableInfo, selectLogicalPlan)
}

func (b *PlanBuilder) buildProjUponView(_ context.Context, dbName model.CIStr, tableInfo *model.TableInfo, selectLogicalPlan base.Plan) (base.LogicalPlan, error) {
	columnInfo := tableInfo.Cols()
	cols := selectLogicalPlan.Schema().Clone().Columns
	outputNamesOfUnderlyingSelect := selectLogicalPlan.OutputNames().Shallow()
	// In the old version of VIEW implementation, tableInfo.View.Cols is used to
	// store the origin columns' names of the underlying SelectStmt used when
	// creating the view.
	if tableInfo.View.Cols != nil {
		cols = cols[:0]
		outputNamesOfUnderlyingSelect = outputNamesOfUnderlyingSelect[:0]
		for _, info := range columnInfo {
			idx := expression.FindFieldNameIdxByColName(selectLogicalPlan.OutputNames(), info.Name.L)
			if idx == -1 {
				return nil, plannererrors.ErrViewInvalid.GenWithStackByArgs(dbName.O, tableInfo.Name.O)
			}
			cols = append(cols, selectLogicalPlan.Schema().Columns[idx])
			outputNamesOfUnderlyingSelect = append(outputNamesOfUnderlyingSelect, selectLogicalPlan.OutputNames()[idx])
		}
	}

	projSchema := expression.NewSchema(make([]*expression.Column, 0, len(tableInfo.Columns))...)
	projExprs := make([]expression.Expression, 0, len(tableInfo.Columns))
	projNames := make(types.NameSlice, 0, len(tableInfo.Columns))
	for i, name := range outputNamesOfUnderlyingSelect {
		origColName := name.ColName
		if tableInfo.View.Cols != nil {
			origColName = tableInfo.View.Cols[i]
		}
		projNames = append(projNames, &types.FieldName{
			// TblName is the of view instead of the name of the underlying table.
			TblName:     tableInfo.Name,
			OrigTblName: name.OrigTblName,
			ColName:     columnInfo[i].Name,
			OrigColName: origColName,
			DBName:      dbName,
		})
		projSchema.Append(&expression.Column{
			UniqueID: cols[i].UniqueID,
			RetType:  cols[i].GetStaticType(),
		})
		projExprs = append(projExprs, cols[i])
	}
	projUponView := LogicalProjection{Exprs: projExprs}.Init(b.ctx, b.getSelectOffset())
	projUponView.names = projNames
	projUponView.SetChildren(selectLogicalPlan.(base.LogicalPlan))
	projUponView.SetSchema(projSchema)
	return projUponView, nil
}

// buildApplyWithJoinType builds apply plan with outerPlan and innerPlan, which apply join with particular join type for
// every row from outerPlan and the whole innerPlan.
func (b *PlanBuilder) buildApplyWithJoinType(outerPlan, innerPlan base.LogicalPlan, tp JoinType, markNoDecorrelate bool) base.LogicalPlan {
	b.optFlag = b.optFlag | flagPredicatePushDown | flagBuildKeyInfo | flagDecorrelate | flagConvertOuterToInnerJoin
	ap := LogicalApply{LogicalJoin: LogicalJoin{JoinType: tp}, NoDecorrelate: markNoDecorrelate}.Init(b.ctx, b.getSelectOffset())
	ap.SetChildren(outerPlan, innerPlan)
	ap.names = make([]*types.FieldName, outerPlan.Schema().Len()+innerPlan.Schema().Len())
	copy(ap.names, outerPlan.OutputNames())
	ap.SetSchema(expression.MergeSchema(outerPlan.Schema(), innerPlan.Schema()))
	setIsInApplyForCTE(innerPlan, ap.Schema())
	// Note that, tp can only be LeftOuterJoin or InnerJoin, so we don't consider other outer joins.
	if tp == LeftOuterJoin {
		b.optFlag = b.optFlag | flagEliminateOuterJoin
		resetNotNullFlag(ap.schema, outerPlan.Schema().Len(), ap.schema.Len())
	}
	for i := outerPlan.Schema().Len(); i < ap.Schema().Len(); i++ {
		ap.names[i] = types.EmptyName
	}
	ap.LogicalJoin.setPreferredJoinTypeAndOrder(b.TableHints())
	return ap
}

// buildSemiApply builds apply plan with outerPlan and innerPlan, which apply semi-join for every row from outerPlan and the whole innerPlan.
func (b *PlanBuilder) buildSemiApply(outerPlan, innerPlan base.LogicalPlan, condition []expression.Expression,
	asScalar, not, considerRewrite, markNoDecorrelate bool) (base.LogicalPlan, error) {
	b.optFlag = b.optFlag | flagPredicatePushDown | flagBuildKeyInfo | flagDecorrelate

	join, err := b.buildSemiJoin(outerPlan, innerPlan, condition, asScalar, not, considerRewrite)
	if err != nil {
		return nil, err
	}

	setIsInApplyForCTE(innerPlan, join.Schema())
	ap := &LogicalApply{LogicalJoin: *join, NoDecorrelate: markNoDecorrelate}
	ap.SetTP(plancodec.TypeApply)
	ap.SetSelf(ap)
	return ap, nil
}

// setIsInApplyForCTE indicates CTE is the in inner side of Apply and correlate.
// the storage of cte needs to be reset for each outer row.
// It's better to handle this in CTEExec.Close(), but cte storage is closed when SQL is finished.
func setIsInApplyForCTE(p base.LogicalPlan, apSchema *expression.Schema) {
	switch x := p.(type) {
	case *LogicalCTE:
		if len(coreusage.ExtractCorColumnsBySchema4LogicalPlan(p, apSchema)) > 0 {
			x.cte.IsInApply = true
		}
		setIsInApplyForCTE(x.cte.seedPartLogicalPlan, apSchema)
		if x.cte.recursivePartLogicalPlan != nil {
			setIsInApplyForCTE(x.cte.recursivePartLogicalPlan, apSchema)
		}
	default:
		for _, child := range p.Children() {
			setIsInApplyForCTE(child, apSchema)
		}
	}
}

func (b *PlanBuilder) buildMaxOneRow(p base.LogicalPlan) base.LogicalPlan {
	// The query block of the MaxOneRow operator should be the same as that of its child.
	maxOneRow := LogicalMaxOneRow{}.Init(b.ctx, p.QueryBlockOffset())
	maxOneRow.SetChildren(p)
	return maxOneRow
}

func (b *PlanBuilder) buildSemiJoin(outerPlan, innerPlan base.LogicalPlan, onCondition []expression.Expression, asScalar, not, forceRewrite bool) (*LogicalJoin, error) {
	b.optFlag |= flagConvertOuterToInnerJoin
	joinPlan := LogicalJoin{}.Init(b.ctx, b.getSelectOffset())
	for i, expr := range onCondition {
		onCondition[i] = expr.Decorrelate(outerPlan.Schema())
	}
	joinPlan.SetChildren(outerPlan, innerPlan)
	joinPlan.AttachOnConds(onCondition)
	joinPlan.names = make([]*types.FieldName, outerPlan.Schema().Len(), outerPlan.Schema().Len()+innerPlan.Schema().Len()+1)
	copy(joinPlan.names, outerPlan.OutputNames())
	if asScalar {
		newSchema := outerPlan.Schema().Clone()
		newSchema.Append(&expression.Column{
			RetType:  types.NewFieldType(mysql.TypeTiny),
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
		})
		joinPlan.names = append(joinPlan.names, types.EmptyName)
		joinPlan.SetSchema(newSchema)
		if not {
			joinPlan.JoinType = AntiLeftOuterSemiJoin
		} else {
			joinPlan.JoinType = LeftOuterSemiJoin
		}
	} else {
		joinPlan.SetSchema(outerPlan.Schema().Clone())
		if not {
			joinPlan.JoinType = AntiSemiJoin
		} else {
			joinPlan.JoinType = SemiJoin
		}
	}
	// Apply forces to choose hash join currently, so don't worry the hints will take effect if the semi join is in one apply.
	joinPlan.setPreferredJoinTypeAndOrder(b.TableHints())
	if forceRewrite {
		joinPlan.preferJoinType |= h.PreferRewriteSemiJoin
		b.optFlag |= flagSemiJoinRewrite
	}
	return joinPlan, nil
}

func getTableOffset(names []*types.FieldName, handleName *types.FieldName) (int, error) {
	for i, name := range names {
		if name.DBName.L == handleName.DBName.L && name.TblName.L == handleName.TblName.L {
			return i, nil
		}
	}
	return -1, errors.Errorf("Couldn't get column information when do update/delete")
}

// TblColPosInfo represents an mapper from column index to handle index.
type TblColPosInfo struct {
	TblID int64
	// Start and End represent the ordinal range [Start, End) of the consecutive columns.
	Start, End int
	// HandleOrdinal represents the ordinal of the handle column.
	HandleCols util.HandleCols
}

// MemoryUsage return the memory usage of TblColPosInfo
func (t *TblColPosInfo) MemoryUsage() (sum int64) {
	if t == nil {
		return
	}

	sum = size.SizeOfInt64 + size.SizeOfInt*2
	if t.HandleCols != nil {
		sum += t.HandleCols.MemoryUsage()
	}
	return
}

// TblColPosInfoSlice attaches the methods of sort.Interface to []TblColPosInfos sorting in increasing order.
type TblColPosInfoSlice []TblColPosInfo

// Len implements sort.Interface#Len.
func (c TblColPosInfoSlice) Len() int {
	return len(c)
}

// Swap implements sort.Interface#Swap.
func (c TblColPosInfoSlice) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

// Less implements sort.Interface#Less.
func (c TblColPosInfoSlice) Less(i, j int) bool {
	return c[i].Start < c[j].Start
}

// FindTblIdx finds the ordinal of the corresponding access column.
func (c TblColPosInfoSlice) FindTblIdx(colOrdinal int) (int, bool) {
	if len(c) == 0 {
		return 0, false
	}
	// find the smallest index of the range that its start great than colOrdinal.
	// @see https://godoc.org/sort#Search
	rangeBehindOrdinal := sort.Search(len(c), func(i int) bool { return c[i].Start > colOrdinal })
	if rangeBehindOrdinal == 0 {
		return 0, false
	}
	return rangeBehindOrdinal - 1, true
}

// buildColumns2Handle builds columns to handle mapping.
func buildColumns2Handle(
	names []*types.FieldName,
	tblID2Handle map[int64][]util.HandleCols,
	tblID2Table map[int64]table.Table,
	onlyWritableCol bool,
) (TblColPosInfoSlice, error) {
	var cols2Handles TblColPosInfoSlice
	for tblID, handleCols := range tblID2Handle {
		tbl := tblID2Table[tblID]
		var tblLen int
		if onlyWritableCol {
			tblLen = len(tbl.WritableCols())
		} else {
			tblLen = len(tbl.Cols())
		}
		for _, handleCol := range handleCols {
			offset, err := getTableOffset(names, names[handleCol.GetCol(0).Index])
			if err != nil {
				return nil, err
			}
			end := offset + tblLen
			cols2Handles = append(cols2Handles, TblColPosInfo{tblID, offset, end, handleCol})
		}
	}
	sort.Sort(cols2Handles)
	return cols2Handles, nil
}

func (b *PlanBuilder) buildUpdate(ctx context.Context, update *ast.UpdateStmt) (base.Plan, error) {
	b.pushSelectOffset(0)
	b.pushTableHints(update.TableHints, 0)
	defer func() {
		b.popSelectOffset()
		// table hints are only visible in the current UPDATE statement.
		b.popTableHints()
	}()

	b.inUpdateStmt = true
	b.isForUpdateRead = true

	if update.With != nil {
		l := len(b.outerCTEs)
		defer func() {
			b.outerCTEs = b.outerCTEs[:l]
		}()
		_, err := b.buildWith(ctx, update.With)
		if err != nil {
			return nil, err
		}
	}

	p, err := b.buildResultSetNode(ctx, update.TableRefs.TableRefs, false)
	if err != nil {
		return nil, err
	}

	tableList := ExtractTableList(update.TableRefs.TableRefs, false)
	for _, t := range tableList {
		dbName := t.Schema.L
		if dbName == "" {
			dbName = b.ctx.GetSessionVars().CurrentDB
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, dbName, t.Name.L, "", nil)
	}

	oldSchemaLen := p.Schema().Len()
	if update.Where != nil {
		p, err = b.buildSelection(ctx, p, update.Where, nil)
		if err != nil {
			return nil, err
		}
	}
	if b.ctx.GetSessionVars().TxnCtx.IsPessimistic {
		if update.TableRefs.TableRefs.Right == nil {
			// buildSelectLock is an optimization that can reduce RPC call.
			// We only need do this optimization for single table update which is the most common case.
			// When TableRefs.Right is nil, it is single table update.
			p, err = b.buildSelectLock(p, &ast.SelectLockInfo{
				LockType: ast.SelectLockForUpdate,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	if update.Order != nil {
		p, err = b.buildSort(ctx, p, update.Order.Items, nil, nil)
		if err != nil {
			return nil, err
		}
	}
	if update.Limit != nil {
		p, err = b.buildLimit(p, update.Limit)
		if err != nil {
			return nil, err
		}
	}

	// Add project to freeze the order of output columns.
	proj := LogicalProjection{Exprs: expression.Column2Exprs(p.Schema().Columns[:oldSchemaLen])}.Init(b.ctx, b.getSelectOffset())
	proj.SetSchema(expression.NewSchema(make([]*expression.Column, oldSchemaLen)...))
	proj.names = make(types.NameSlice, len(p.OutputNames()))
	copy(proj.names, p.OutputNames())
	copy(proj.schema.Columns, p.Schema().Columns[:oldSchemaLen])
	proj.SetChildren(p)
	p = proj

	utlr := &updatableTableListResolver{}
	update.Accept(utlr)
	orderedList, np, allAssignmentsAreConstant, err := b.buildUpdateLists(ctx, utlr.updatableTableList, update.List, p)
	if err != nil {
		return nil, err
	}
	p = np

	updt := Update{
		OrderedList:               orderedList,
		AllAssignmentsAreConstant: allAssignmentsAreConstant,
		VirtualAssignmentsOffset:  len(update.List),
	}.Init(b.ctx)
	updt.names = p.OutputNames()
	// We cannot apply projection elimination when building the subplan, because
	// columns in orderedList cannot be resolved. (^flagEliminateProjection should also be applied in postOptimize)
	updt.SelectPlan, _, err = DoOptimize(ctx, b.ctx, b.optFlag&^flagEliminateProjection, p)
	if err != nil {
		return nil, err
	}
	err = updt.ResolveIndices()
	if err != nil {
		return nil, err
	}
	tblID2Handle, err := resolveIndicesForTblID2Handle(b.handleHelper.tailMap(), updt.SelectPlan.Schema())
	if err != nil {
		return nil, err
	}
	tblID2table := make(map[int64]table.Table, len(tblID2Handle))
	for id := range tblID2Handle {
		tblID2table[id], _ = b.is.TableByID(id)
	}
	updt.TblColPosInfos, err = buildColumns2Handle(updt.OutputNames(), tblID2Handle, tblID2table, true)
	if err != nil {
		return nil, err
	}
	updt.PartitionedTable = b.partitionedTable
	updt.tblID2Table = tblID2table
	err = updt.buildOnUpdateFKTriggers(b.ctx, b.is, tblID2table)
	return updt, err
}

// GetUpdateColumnsInfo get the update columns info.
func GetUpdateColumnsInfo(tblID2Table map[int64]table.Table, tblColPosInfos TblColPosInfoSlice, size int) []*table.Column {
	colsInfo := make([]*table.Column, size)
	for _, content := range tblColPosInfos {
		tbl := tblID2Table[content.TblID]
		for i, c := range tbl.WritableCols() {
			colsInfo[content.Start+i] = c
		}
	}
	return colsInfo
}

type tblUpdateInfo struct {
	name                string
	pkUpdated           bool
	partitionColUpdated bool
}

// CheckUpdateList checks all related columns in updatable state.
func CheckUpdateList(assignFlags []int, updt *Update, newTblID2Table map[int64]table.Table) error {
	updateFromOtherAlias := make(map[int64]tblUpdateInfo)
	for _, content := range updt.TblColPosInfos {
		tbl := newTblID2Table[content.TblID]
		flags := assignFlags[content.Start:content.End]
		var update, updatePK, updatePartitionCol bool
		var partitionColumnNames []model.CIStr
		if pt, ok := tbl.(table.PartitionedTable); ok && pt != nil {
			partitionColumnNames = pt.GetPartitionColumnNames()
		}

		for i, col := range tbl.WritableCols() {
			// schema may be changed between building plan and building executor
			// If i >= len(flags), it means the target table has been added columns, then we directly skip the check
			if i >= len(flags) {
				continue
			}
			if flags[i] < 0 {
				continue
			}

			if col.State != model.StatePublic {
				return plannererrors.ErrUnknownColumn.GenWithStackByArgs(col.Name, clauseMsg[fieldList])
			}

			update = true
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				updatePK = true
			}
			for _, partColName := range partitionColumnNames {
				if col.Name.L == partColName.L {
					updatePartitionCol = true
				}
			}
		}
		if update {
			// Check for multi-updates on primary key,
			// see https://dev.mysql.com/doc/mysql-errors/5.7/en/server-error-reference.html#error_er_multi_update_key_conflict
			if otherTable, ok := updateFromOtherAlias[tbl.Meta().ID]; ok {
				if otherTable.pkUpdated || updatePK || otherTable.partitionColUpdated || updatePartitionCol {
					return plannererrors.ErrMultiUpdateKeyConflict.GenWithStackByArgs(otherTable.name, updt.names[content.Start].TblName.O)
				}
			} else {
				updateFromOtherAlias[tbl.Meta().ID] = tblUpdateInfo{
					name:                updt.names[content.Start].TblName.O,
					pkUpdated:           updatePK,
					partitionColUpdated: updatePartitionCol,
				}
			}
		}
	}
	return nil
}

// If tl is CTE, its HintedTable will be nil.
// Only used in build plan from AST after preprocess.
func isCTE(tl *ast.TableName) bool {
	return tl.TableInfo == nil
}

func (b *PlanBuilder) buildUpdateLists(ctx context.Context, tableList []*ast.TableName, list []*ast.Assignment, p base.LogicalPlan) (newList []*expression.Assignment, po base.LogicalPlan, allAssignmentsAreConstant bool, e error) {
	b.curClause = fieldList
	// modifyColumns indicates which columns are in set list,
	// and if it is set to `DEFAULT`
	modifyColumns := make(map[string]bool, p.Schema().Len())
	var columnsIdx map[*ast.ColumnName]int
	cacheColumnsIdx := false
	if len(p.OutputNames()) > 16 {
		cacheColumnsIdx = true
		columnsIdx = make(map[*ast.ColumnName]int, len(list))
	}
	for _, assign := range list {
		idx, err := expression.FindFieldName(p.OutputNames(), assign.Column)
		if err != nil {
			return nil, nil, false, err
		}
		if idx < 0 {
			return nil, nil, false, plannererrors.ErrUnknownColumn.GenWithStackByArgs(assign.Column.Name, "field list")
		}
		if cacheColumnsIdx {
			columnsIdx[assign.Column] = idx
		}
		name := p.OutputNames()[idx]
		foundListItem := false
		for _, tl := range tableList {
			if (tl.Schema.L == "" || tl.Schema.L == name.DBName.L) && (tl.Name.L == name.TblName.L) {
				if isCTE(tl) || tl.TableInfo.IsView() || tl.TableInfo.IsSequence() {
					return nil, nil, false, plannererrors.ErrNonUpdatableTable.GenWithStackByArgs(name.TblName.O, "UPDATE")
				}
				foundListItem = true
			}
		}
		if !foundListItem {
			// For case like:
			// 1: update (select * from t1) t1 set b = 1111111 ----- (no updatable table here)
			// 2: update (select 1 as a) as t, t1 set a=1      ----- (updatable t1 don't have column a)
			// --- subQuery is not counted as updatable table.
			return nil, nil, false, plannererrors.ErrNonUpdatableTable.GenWithStackByArgs(name.TblName.O, "UPDATE")
		}
		columnFullName := fmt.Sprintf("%s.%s.%s", name.DBName.L, name.TblName.L, name.ColName.L)
		// We save a flag for the column in map `modifyColumns`
		// This flag indicated if assign keyword `DEFAULT` to the column
		modifyColumns[columnFullName] = IsDefaultExprSameColumn(p.OutputNames()[idx:idx+1], assign.Expr)
	}

	// If columns in set list contains generated columns, raise error.
	// And, fill virtualAssignments here; that's for generated columns.
	virtualAssignments := make([]*ast.Assignment, 0)
	for _, tn := range tableList {
		if isCTE(tn) || tn.TableInfo.IsView() || tn.TableInfo.IsSequence() {
			continue
		}

		tableInfo := tn.TableInfo
		tableVal, found := b.is.TableByID(tableInfo.ID)
		if !found {
			return nil, nil, false, infoschema.ErrTableNotExists.FastGenByArgs(tn.DBInfo.Name.O, tableInfo.Name.O)
		}
		for i, colInfo := range tableVal.Cols() {
			if !colInfo.IsGenerated() {
				continue
			}
			columnFullName := fmt.Sprintf("%s.%s.%s", tn.DBInfo.Name.L, tn.Name.L, colInfo.Name.L)
			isDefault, ok := modifyColumns[columnFullName]
			if ok && colInfo.Hidden {
				return nil, nil, false, plannererrors.ErrUnknownColumn.GenWithStackByArgs(colInfo.Name, clauseMsg[fieldList])
			}
			// Note: For INSERT, REPLACE, and UPDATE, if a generated column is inserted into, replaced, or updated explicitly, the only permitted value is DEFAULT.
			// see https://dev.mysql.com/doc/refman/8.0/en/create-table-generated-columns.html
			if ok && !isDefault {
				return nil, nil, false, plannererrors.ErrBadGeneratedColumn.GenWithStackByArgs(colInfo.Name.O, tableInfo.Name.O)
			}
			virtualAssignments = append(virtualAssignments, &ast.Assignment{
				Column: &ast.ColumnName{Schema: tn.Schema, Table: tn.Name, Name: colInfo.Name},
				Expr:   tableVal.Cols()[i].GeneratedExpr.Clone(),
			})
		}
	}

	allAssignmentsAreConstant = true
	newList = make([]*expression.Assignment, 0, p.Schema().Len())
	tblDbMap := make(map[string]string, len(tableList))
	for _, tbl := range tableList {
		if isCTE(tbl) {
			continue
		}
		tblDbMap[tbl.Name.L] = tbl.DBInfo.Name.L
	}

	allAssignments := append(list, virtualAssignments...)
	dependentColumnsModified := make(map[int64]bool)
	for i, assign := range allAssignments {
		var idx int
		var err error
		if cacheColumnsIdx {
			if i, ok := columnsIdx[assign.Column]; ok {
				idx = i
			} else {
				idx, err = expression.FindFieldName(p.OutputNames(), assign.Column)
			}
		} else {
			idx, err = expression.FindFieldName(p.OutputNames(), assign.Column)
		}
		if err != nil {
			return nil, nil, false, err
		}
		col := p.Schema().Columns[idx]
		name := p.OutputNames()[idx]
		var newExpr expression.Expression
		var np base.LogicalPlan
		if i < len(list) {
			// If assign `DEFAULT` to column, fill the `defaultExpr.Name` before rewrite expression
			if expr := extractDefaultExpr(assign.Expr); expr != nil {
				expr.Name = assign.Column
			}
			newExpr, np, err = b.rewrite(ctx, assign.Expr, p, nil, true)
			if err != nil {
				return nil, nil, false, err
			}
			dependentColumnsModified[col.UniqueID] = true
		} else {
			// rewrite with generation expression
			rewritePreprocess := func(assign *ast.Assignment) func(expr ast.Node) ast.Node {
				return func(expr ast.Node) ast.Node {
					switch x := expr.(type) {
					case *ast.ColumnName:
						return &ast.ColumnName{
							Schema: assign.Column.Schema,
							Table:  assign.Column.Table,
							Name:   x.Name,
						}
					default:
						return expr
					}
				}
			}

			o := b.allowBuildCastArray
			b.allowBuildCastArray = true
			newExpr, np, err = b.rewriteWithPreprocess(ctx, assign.Expr, p, nil, nil, true, rewritePreprocess(assign))
			b.allowBuildCastArray = o
			if err != nil {
				return nil, nil, false, err
			}
			// check if the column is modified
			dependentColumns := expression.ExtractDependentColumns(newExpr)
			var isModified bool
			for _, col := range dependentColumns {
				if dependentColumnsModified[col.UniqueID] {
					isModified = true
					break
				}
			}
			// skip unmodified generated columns
			if !isModified {
				continue
			}
		}
		if _, isConst := newExpr.(*expression.Constant); !isConst {
			allAssignmentsAreConstant = false
		}
		p = np
		if cols := expression.ExtractColumnSet(newExpr); cols.Len() > 0 {
			b.ctx.GetSessionVars().StmtCtx.ColRefFromUpdatePlan.UnionWith(cols)
		}
		newList = append(newList, &expression.Assignment{Col: col, ColName: name.ColName, Expr: newExpr})
		dbName := name.DBName.L
		// To solve issue#10028, we need to get database name by the table alias name.
		if dbNameTmp, ok := tblDbMap[name.TblName.L]; ok {
			dbName = dbNameTmp
		}
		if dbName == "" {
			dbName = b.ctx.GetSessionVars().CurrentDB
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.UpdatePriv, dbName, name.OrigTblName.L, "", nil)
	}
	return newList, p, allAssignmentsAreConstant, nil
}

// extractDefaultExpr extract a `DefaultExpr` from `ExprNode`,
// If it is a `DEFAULT` function like `DEFAULT(a)`, return nil.
// Only if it is `DEFAULT` keyword, it will return the `DefaultExpr`.
func extractDefaultExpr(node ast.ExprNode) *ast.DefaultExpr {
	if expr, ok := node.(*ast.DefaultExpr); ok && expr.Name == nil {
		return expr
	}
	return nil
}

// IsDefaultExprSameColumn - DEFAULT or col = DEFAULT(col)
func IsDefaultExprSameColumn(names types.NameSlice, node ast.ExprNode) bool {
	if expr, ok := node.(*ast.DefaultExpr); ok {
		if expr.Name == nil {
			// col = DEFAULT
			return true
		}
		refIdx, err := expression.FindFieldName(names, expr.Name)
		if refIdx == 0 && err == nil {
			// col = DEFAULT(col)
			return true
		}
	}
	return false
}

func (b *PlanBuilder) buildDelete(ctx context.Context, ds *ast.DeleteStmt) (base.Plan, error) {
	b.pushSelectOffset(0)
	b.pushTableHints(ds.TableHints, 0)
	defer func() {
		b.popSelectOffset()
		// table hints are only visible in the current DELETE statement.
		b.popTableHints()
	}()

	b.inDeleteStmt = true
	b.isForUpdateRead = true

	if ds.With != nil {
		l := len(b.outerCTEs)
		defer func() {
			b.outerCTEs = b.outerCTEs[:l]
		}()
		_, err := b.buildWith(ctx, ds.With)
		if err != nil {
			return nil, err
		}
	}

	p, err := b.buildResultSetNode(ctx, ds.TableRefs.TableRefs, false)
	if err != nil {
		return nil, err
	}
	oldSchema := p.Schema()
	oldLen := oldSchema.Len()

	// For explicit column usage, should use the all-public columns.
	if ds.Where != nil {
		p, err = b.buildSelection(ctx, p, ds.Where, nil)
		if err != nil {
			return nil, err
		}
	}
	if b.ctx.GetSessionVars().TxnCtx.IsPessimistic {
		if !ds.IsMultiTable {
			p, err = b.buildSelectLock(p, &ast.SelectLockInfo{
				LockType: ast.SelectLockForUpdate,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	if ds.Order != nil {
		p, err = b.buildSort(ctx, p, ds.Order.Items, nil, nil)
		if err != nil {
			return nil, err
		}
	}

	if ds.Limit != nil {
		p, err = b.buildLimit(p, ds.Limit)
		if err != nil {
			return nil, err
		}
	}

	// If the delete is non-qualified it does not require Select Priv
	if ds.Where == nil && ds.Order == nil {
		b.popVisitInfo()
	}
	var authErr error
	sessionVars := b.ctx.GetSessionVars()

	proj := LogicalProjection{Exprs: expression.Column2Exprs(p.Schema().Columns[:oldLen])}.Init(b.ctx, b.getSelectOffset())
	proj.SetChildren(p)
	proj.SetSchema(oldSchema.Clone())
	proj.names = p.OutputNames()[:oldLen]
	p = proj

	handleColsMap := b.handleHelper.tailMap()
	for _, cols := range handleColsMap {
		for _, col := range cols {
			for i := 0; i < col.NumCols(); i++ {
				exprCol := col.GetCol(i)
				if proj.Schema().Contains(exprCol) {
					continue
				}
				proj.Exprs = append(proj.Exprs, exprCol)
				proj.Schema().Columns = append(proj.Schema().Columns, exprCol)
				proj.names = append(proj.names, types.EmptyName)
			}
		}
	}

	del := Delete{
		IsMultiTable: ds.IsMultiTable,
	}.Init(b.ctx)

	del.names = p.OutputNames()
	del.SelectPlan, _, err = DoOptimize(ctx, b.ctx, b.optFlag, p)
	if err != nil {
		return nil, err
	}

	tblID2Handle, err := resolveIndicesForTblID2Handle(handleColsMap, del.SelectPlan.Schema())
	if err != nil {
		return nil, err
	}

	// Collect visitInfo.
	if ds.Tables != nil {
		// Delete a, b from a, b, c, d... add a and b.
		updatableList := make(map[string]bool)
		tbInfoList := make(map[string]*ast.TableName)
		collectTableName(ds.TableRefs.TableRefs, &updatableList, &tbInfoList)
		for _, tn := range ds.Tables.Tables {
			var canUpdate, foundMatch = false, false
			name := tn.Name.L
			if tn.Schema.L == "" {
				canUpdate, foundMatch = updatableList[name]
			}

			if !foundMatch {
				if tn.Schema.L == "" {
					name = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB).L + "." + tn.Name.L
				} else {
					name = tn.Schema.L + "." + tn.Name.L
				}
				canUpdate, foundMatch = updatableList[name]
			}
			// check sql like: `delete b from (select * from t) as a, t`
			if !foundMatch {
				return nil, plannererrors.ErrUnknownTable.GenWithStackByArgs(tn.Name.O, "MULTI DELETE")
			}
			// check sql like: `delete a from (select * from t) as a, t`
			if !canUpdate {
				return nil, plannererrors.ErrNonUpdatableTable.GenWithStackByArgs(tn.Name.O, "DELETE")
			}
			tb := tbInfoList[name]
			tn.DBInfo = tb.DBInfo
			tn.TableInfo = tb.TableInfo
			if tn.TableInfo.IsView() {
				return nil, errors.Errorf("delete view %s is not supported now", tn.Name.O)
			}
			if tn.TableInfo.IsSequence() {
				return nil, errors.Errorf("delete sequence %s is not supported now", tn.Name.O)
			}
			if sessionVars.User != nil {
				authErr = plannererrors.ErrTableaccessDenied.FastGenByArgs("DELETE", sessionVars.User.AuthUsername, sessionVars.User.AuthHostname, tb.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DeletePriv, tb.DBInfo.Name.L, tb.Name.L, "", authErr)
		}
	} else {
		// Delete from a, b, c, d.
		tableList := ExtractTableList(ds.TableRefs.TableRefs, false)
		for _, v := range tableList {
			if isCTE(v) {
				return nil, plannererrors.ErrNonUpdatableTable.GenWithStackByArgs(v.Name.O, "DELETE")
			}
			if v.TableInfo.IsView() {
				return nil, errors.Errorf("delete view %s is not supported now", v.Name.O)
			}
			if v.TableInfo.IsSequence() {
				return nil, errors.Errorf("delete sequence %s is not supported now", v.Name.O)
			}
			dbName := v.Schema.L
			if dbName == "" {
				dbName = b.ctx.GetSessionVars().CurrentDB
			}
			if sessionVars.User != nil {
				authErr = plannererrors.ErrTableaccessDenied.FastGenByArgs("DELETE", sessionVars.User.AuthUsername, sessionVars.User.AuthHostname, v.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DeletePriv, dbName, v.Name.L, "", authErr)
		}
	}
	if del.IsMultiTable {
		// tblID2TableName is the table map value is an array which contains table aliases.
		// Table ID may not be unique for deleting multiple tables, for statements like
		// `delete from t as t1, t as t2`, the same table has two alias, we have to identify a table
		// by its alias instead of ID.
		tblID2TableName := make(map[int64][]*ast.TableName, len(ds.Tables.Tables))
		for _, tn := range ds.Tables.Tables {
			tblID2TableName[tn.TableInfo.ID] = append(tblID2TableName[tn.TableInfo.ID], tn)
		}
		tblID2Handle = del.cleanTblID2HandleMap(tblID2TableName, tblID2Handle, del.names)
	}
	tblID2table := make(map[int64]table.Table, len(tblID2Handle))
	for id := range tblID2Handle {
		tblID2table[id], _ = b.is.TableByID(id)
	}
	del.TblColPosInfos, err = buildColumns2Handle(del.names, tblID2Handle, tblID2table, false)
	if err != nil {
		return nil, err
	}
	err = del.buildOnDeleteFKTriggers(b.ctx, b.is, tblID2table)
	return del, err
}

func resolveIndicesForTblID2Handle(tblID2Handle map[int64][]util.HandleCols, schema *expression.Schema) (map[int64][]util.HandleCols, error) {
	newMap := make(map[int64][]util.HandleCols, len(tblID2Handle))
	for i, cols := range tblID2Handle {
		for _, col := range cols {
			resolvedCol, err := col.ResolveIndices(schema)
			if err != nil {
				return nil, err
			}
			newMap[i] = append(newMap[i], resolvedCol)
		}
	}
	return newMap, nil
}

func (p *Delete) cleanTblID2HandleMap(
	tablesToDelete map[int64][]*ast.TableName,
	tblID2Handle map[int64][]util.HandleCols,
	outputNames []*types.FieldName,
) map[int64][]util.HandleCols {
	for id, cols := range tblID2Handle {
		names, ok := tablesToDelete[id]
		if !ok {
			delete(tblID2Handle, id)
			continue
		}
		for i := len(cols) - 1; i >= 0; i-- {
			hCols := cols[i]
			var hasMatch bool
			for j := 0; j < hCols.NumCols(); j++ {
				if p.matchingDeletingTable(names, outputNames[hCols.GetCol(j).Index]) {
					hasMatch = true
					break
				}
			}
			if !hasMatch {
				cols = append(cols[:i], cols[i+1:]...)
			}
		}
		if len(cols) == 0 {
			delete(tblID2Handle, id)
			continue
		}
		tblID2Handle[id] = cols
	}
	return tblID2Handle
}

// matchingDeletingTable checks whether this column is from the table which is in the deleting list.
func (*Delete) matchingDeletingTable(names []*ast.TableName, name *types.FieldName) bool {
	for _, n := range names {
		if (name.DBName.L == "" || name.DBName.L == n.DBInfo.Name.L) && name.TblName.L == n.Name.L {
			return true
		}
	}
	return false
}

func getWindowName(name string) string {
	if name == "" {
		return "<unnamed window>"
	}
	return name
}

// buildProjectionForWindow builds the projection for expressions in the window specification that is not an column,
// so after the projection, window functions only needs to deal with columns.
func (b *PlanBuilder) buildProjectionForWindow(ctx context.Context, p base.LogicalPlan, spec *ast.WindowSpec, args []ast.ExprNode, aggMap map[*ast.AggregateFuncExpr]int) (base.LogicalPlan, []property.SortItem, []property.SortItem, []expression.Expression, error) {
	b.optFlag |= flagEliminateProjection

	var partitionItems, orderItems []*ast.ByItem
	if spec.PartitionBy != nil {
		partitionItems = spec.PartitionBy.Items
	}
	if spec.OrderBy != nil {
		orderItems = spec.OrderBy.Items
	}

	projLen := len(p.Schema().Columns) + len(partitionItems) + len(orderItems) + len(args)
	proj := LogicalProjection{Exprs: make([]expression.Expression, 0, projLen)}.Init(b.ctx, b.getSelectOffset())
	proj.SetSchema(expression.NewSchema(make([]*expression.Column, 0, projLen)...))
	proj.names = make([]*types.FieldName, p.Schema().Len(), projLen)
	for _, col := range p.Schema().Columns {
		proj.Exprs = append(proj.Exprs, col)
		proj.schema.Append(col)
	}
	copy(proj.names, p.OutputNames())

	propertyItems := make([]property.SortItem, 0, len(partitionItems)+len(orderItems))
	var err error
	p, propertyItems, err = b.buildByItemsForWindow(ctx, p, proj, partitionItems, propertyItems, aggMap)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	lenPartition := len(propertyItems)
	p, propertyItems, err = b.buildByItemsForWindow(ctx, p, proj, orderItems, propertyItems, aggMap)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	newArgList := make([]expression.Expression, 0, len(args))
	for _, arg := range args {
		newArg, np, err := b.rewrite(ctx, arg, p, aggMap, true)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		p = np
		switch newArg.(type) {
		case *expression.Column, *expression.Constant:
			newArgList = append(newArgList, newArg.Clone())
			continue
		}
		proj.Exprs = append(proj.Exprs, newArg)
		proj.names = append(proj.names, types.EmptyName)
		col := &expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  newArg.GetType(b.ctx.GetExprCtx().GetEvalCtx()),
		}
		proj.schema.Append(col)
		newArgList = append(newArgList, col)
	}

	proj.SetChildren(p)
	return proj, propertyItems[:lenPartition], propertyItems[lenPartition:], newArgList, nil
}

func (b *PlanBuilder) buildArgs4WindowFunc(ctx context.Context, p base.LogicalPlan, args []ast.ExprNode, aggMap map[*ast.AggregateFuncExpr]int) ([]expression.Expression, error) {
	b.optFlag |= flagEliminateProjection

	newArgList := make([]expression.Expression, 0, len(args))
	// use below index for created a new col definition
	// it's okay here because we only want to return the args used in window function
	newColIndex := 0
	for _, arg := range args {
		newArg, np, err := b.rewrite(ctx, arg, p, aggMap, true)
		if err != nil {
			return nil, err
		}
		p = np
		switch newArg.(type) {
		case *expression.Column, *expression.Constant:
			newArgList = append(newArgList, newArg.Clone())
			continue
		}
		col := &expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  newArg.GetType(b.ctx.GetExprCtx().GetEvalCtx()),
		}
		newColIndex++
		newArgList = append(newArgList, col)
	}
	return newArgList, nil
}

func (b *PlanBuilder) buildByItemsForWindow(
	ctx context.Context,
	p base.LogicalPlan,
	proj *LogicalProjection,
	items []*ast.ByItem,
	retItems []property.SortItem,
	aggMap map[*ast.AggregateFuncExpr]int,
) (base.LogicalPlan, []property.SortItem, error) {
	transformer := &itemTransformer{}
	for _, item := range items {
		newExpr, _ := item.Expr.Accept(transformer)
		item.Expr = newExpr.(ast.ExprNode)
		it, np, err := b.rewrite(ctx, item.Expr, p, aggMap, true)
		if err != nil {
			return nil, nil, err
		}
		p = np
		if it.GetType(b.ctx.GetExprCtx().GetEvalCtx()).GetType() == mysql.TypeNull {
			continue
		}
		if col, ok := it.(*expression.Column); ok {
			retItems = append(retItems, property.SortItem{Col: col, Desc: item.Desc})
			// We need to attempt to add this column because a subquery may be created during the expression rewrite process.
			// Therefore, we need to ensure that the column from the newly created query plan is added.
			// If the column is already in the schema, we don't need to add it again.
			if !proj.schema.Contains(col) {
				proj.Exprs = append(proj.Exprs, col)
				proj.names = append(proj.names, types.EmptyName)
				proj.schema.Append(col)
			}
			continue
		}
		proj.Exprs = append(proj.Exprs, it)
		proj.names = append(proj.names, types.EmptyName)
		col := &expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  it.GetType(b.ctx.GetExprCtx().GetEvalCtx()),
		}
		proj.schema.Append(col)
		retItems = append(retItems, property.SortItem{Col: col, Desc: item.Desc})
	}
	return p, retItems, nil
}

// buildWindowFunctionFrameBound builds the bounds of window function frames.
// For type `Rows`, the bound expr must be an unsigned integer.
// For type `Range`, the bound expr must be temporal or numeric types.
func (b *PlanBuilder) buildWindowFunctionFrameBound(_ context.Context, spec *ast.WindowSpec, orderByItems []property.SortItem, boundClause *ast.FrameBound) (*FrameBound, error) {
	frameType := spec.Frame.Type
	bound := &FrameBound{Type: boundClause.Type, UnBounded: boundClause.UnBounded, IsExplicitRange: false}
	if bound.UnBounded {
		return bound, nil
	}

	if frameType == ast.Rows {
		if bound.Type == ast.CurrentRow {
			return bound, nil
		}
		numRows, _, _ := getUintFromNode(b.ctx, boundClause.Expr, false)
		bound.Num = numRows
		return bound, nil
	}

	bound.CalcFuncs = make([]expression.Expression, len(orderByItems))
	bound.CmpFuncs = make([]expression.CompareFunc, len(orderByItems))
	if bound.Type == ast.CurrentRow {
		for i, item := range orderByItems {
			col := item.Col
			bound.CalcFuncs[i] = col
			bound.CmpFuncs[i] = expression.GetCmpFunction(b.ctx.GetExprCtx(), col, col)
		}
		return bound, nil
	}

	col := orderByItems[0].Col
	// TODO: We also need to raise error for non-deterministic expressions, like rand().
	val, err := evalAstExprWithPlanCtx(b.ctx, boundClause.Expr)
	if err != nil {
		return nil, plannererrors.ErrWindowRangeBoundNotConstant.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	expr := expression.Constant{Value: val, RetType: boundClause.Expr.GetType()}

	checker := &expression.ParamMarkerInPrepareChecker{}
	boundClause.Expr.Accept(checker)

	// If it has paramMarker and is in prepare stmt. We don't need to eval it since its value is not decided yet.
	if !checker.InPrepareStmt {
		// Do not raise warnings for truncate.
		exprCtx := exprctx.CtxWithHandleTruncateErrLevel(b.ctx.GetExprCtx(), errctx.LevelIgnore)
		uVal, isNull, err := expr.EvalInt(exprCtx.GetEvalCtx(), chunk.Row{})
		if uVal < 0 || isNull || err != nil {
			return nil, plannererrors.ErrWindowFrameIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
		}
	}

	bound.IsExplicitRange = true
	desc := orderByItems[0].Desc
	var funcName string
	if boundClause.Unit != ast.TimeUnitInvalid {
		// TODO: Perhaps we don't need to transcode this back to generic string
		unitVal := boundClause.Unit.String()
		unit := expression.Constant{
			Value:   types.NewStringDatum(unitVal),
			RetType: types.NewFieldType(mysql.TypeVarchar),
		}

		// When the order is asc:
		//   `+` for following, and `-` for the preceding
		// When the order is desc, `+` becomes `-` and vice-versa.
		funcName = ast.DateAdd
		if (!desc && bound.Type == ast.Preceding) || (desc && bound.Type == ast.Following) {
			funcName = ast.DateSub
		}

		bound.CalcFuncs[0], err = expression.NewFunctionBase(b.ctx.GetExprCtx(), funcName, col.RetType, col, &expr, &unit)
		if err != nil {
			return nil, err
		}
	} else {
		// When the order is asc:
		//   `+` for following, and `-` for the preceding
		// When the order is desc, `+` becomes `-` and vice-versa.
		funcName = ast.Plus
		if (!desc && bound.Type == ast.Preceding) || (desc && bound.Type == ast.Following) {
			funcName = ast.Minus
		}

		bound.CalcFuncs[0], err = expression.NewFunctionBase(b.ctx.GetExprCtx(), funcName, col.RetType, col, &expr)
		if err != nil {
			return nil, err
		}
	}

	cmpDataType := expression.GetAccurateCmpType(b.ctx.GetExprCtx().GetEvalCtx(), col, bound.CalcFuncs[0])
	bound.updateCmpFuncsAndCmpDataType(cmpDataType)
	return bound, nil
}

// buildWindowFunctionFrame builds the window function frames.
// See https://dev.mysql.com/doc/refman/8.0/en/window-functions-frames.html
func (b *PlanBuilder) buildWindowFunctionFrame(ctx context.Context, spec *ast.WindowSpec, orderByItems []property.SortItem) (*WindowFrame, error) {
	frameClause := spec.Frame
	if frameClause == nil {
		return nil, nil
	}
	frame := &WindowFrame{Type: frameClause.Type}
	var err error
	frame.Start, err = b.buildWindowFunctionFrameBound(ctx, spec, orderByItems, &frameClause.Extent.Start)
	if err != nil {
		return nil, err
	}
	frame.End, err = b.buildWindowFunctionFrameBound(ctx, spec, orderByItems, &frameClause.Extent.End)
	return frame, err
}

func (b *PlanBuilder) checkWindowFuncArgs(ctx context.Context, p base.LogicalPlan, windowFuncExprs []*ast.WindowFuncExpr, windowAggMap map[*ast.AggregateFuncExpr]int) error {
	checker := &expression.ParamMarkerInPrepareChecker{}
	for _, windowFuncExpr := range windowFuncExprs {
		if strings.ToLower(windowFuncExpr.Name) == ast.AggFuncGroupConcat {
			return plannererrors.ErrNotSupportedYet.GenWithStackByArgs("group_concat as window function")
		}
		args, err := b.buildArgs4WindowFunc(ctx, p, windowFuncExpr.Args, windowAggMap)
		if err != nil {
			return err
		}
		checker.InPrepareStmt = false
		for _, expr := range windowFuncExpr.Args {
			expr.Accept(checker)
		}
		desc, err := aggregation.NewWindowFuncDesc(b.ctx.GetExprCtx(), windowFuncExpr.Name, args, checker.InPrepareStmt)
		if err != nil {
			return err
		}
		if desc == nil {
			return plannererrors.ErrWrongArguments.GenWithStackByArgs(strings.ToLower(windowFuncExpr.Name))
		}
	}
	return nil
}

func getAllByItems(itemsBuf []*ast.ByItem, spec *ast.WindowSpec) []*ast.ByItem {
	itemsBuf = itemsBuf[:0]
	if spec.PartitionBy != nil {
		itemsBuf = append(itemsBuf, spec.PartitionBy.Items...)
	}
	if spec.OrderBy != nil {
		itemsBuf = append(itemsBuf, spec.OrderBy.Items...)
	}
	return itemsBuf
}

func restoreByItemText(item *ast.ByItem) string {
	var sb strings.Builder
	ctx := format.NewRestoreCtx(0, &sb)
	err := item.Expr.Restore(ctx)
	if err != nil {
		return ""
	}
	return sb.String()
}

func compareItems(lItems []*ast.ByItem, rItems []*ast.ByItem) bool {
	minLen := min(len(lItems), len(rItems))
	for i := 0; i < minLen; i++ {
		res := strings.Compare(restoreByItemText(lItems[i]), restoreByItemText(rItems[i]))
		if res != 0 {
			return res < 0
		}
		res = compareBool(lItems[i].Desc, rItems[i].Desc)
		if res != 0 {
			return res < 0
		}
	}
	return len(lItems) < len(rItems)
}

type windowFuncs struct {
	spec  *ast.WindowSpec
	funcs []*ast.WindowFuncExpr
}

// sortWindowSpecs sorts the window specifications by reversed alphabetical order, then we could add less `Sort` operator
// in physical plan because the window functions with the same partition by and order by clause will be at near places.
func sortWindowSpecs(groupedFuncs map[*ast.WindowSpec][]*ast.WindowFuncExpr, orderedSpec []*ast.WindowSpec) []windowFuncs {
	windows := make([]windowFuncs, 0, len(groupedFuncs))
	for _, spec := range orderedSpec {
		windows = append(windows, windowFuncs{spec, groupedFuncs[spec]})
	}
	lItemsBuf := make([]*ast.ByItem, 0, 4)
	rItemsBuf := make([]*ast.ByItem, 0, 4)
	sort.SliceStable(windows, func(i, j int) bool {
		lItemsBuf = getAllByItems(lItemsBuf, windows[i].spec)
		rItemsBuf = getAllByItems(rItemsBuf, windows[j].spec)
		return !compareItems(lItemsBuf, rItemsBuf)
	})
	return windows
}

func (b *PlanBuilder) buildWindowFunctions(ctx context.Context, p base.LogicalPlan, groupedFuncs map[*ast.WindowSpec][]*ast.WindowFuncExpr, orderedSpec []*ast.WindowSpec, aggMap map[*ast.AggregateFuncExpr]int) (base.LogicalPlan, map[*ast.WindowFuncExpr]int, error) {
	if b.buildingCTE {
		b.outerCTEs[len(b.outerCTEs)-1].containAggOrWindow = true
	}
	args := make([]ast.ExprNode, 0, 4)
	windowMap := make(map[*ast.WindowFuncExpr]int)
	for _, window := range sortWindowSpecs(groupedFuncs, orderedSpec) {
		args = args[:0]
		spec, funcs := window.spec, window.funcs
		for _, windowFunc := range funcs {
			args = append(args, windowFunc.Args...)
		}
		np, partitionBy, orderBy, args, err := b.buildProjectionForWindow(ctx, p, spec, args, aggMap)
		if err != nil {
			return nil, nil, err
		}
		if len(funcs) == 0 {
			// len(funcs) == 0 indicates this an unused named window spec,
			// so we just check for its validity and don't have to build plan for it.
			err := b.checkOriginWindowSpec(spec, orderBy)
			if err != nil {
				return nil, nil, err
			}
			continue
		}
		err = b.checkOriginWindowFuncs(funcs, orderBy)
		if err != nil {
			return nil, nil, err
		}
		frame, err := b.buildWindowFunctionFrame(ctx, spec, orderBy)
		if err != nil {
			return nil, nil, err
		}

		window := LogicalWindow{
			PartitionBy: partitionBy,
			OrderBy:     orderBy,
			Frame:       frame,
		}.Init(b.ctx, b.getSelectOffset())
		window.names = make([]*types.FieldName, np.Schema().Len())
		copy(window.names, np.OutputNames())
		schema := np.Schema().Clone()
		descs := make([]*aggregation.WindowFuncDesc, 0, len(funcs))
		preArgs := 0
		checker := &expression.ParamMarkerInPrepareChecker{}
		for _, windowFunc := range funcs {
			checker.InPrepareStmt = false
			for _, expr := range windowFunc.Args {
				expr.Accept(checker)
			}
			desc, err := aggregation.NewWindowFuncDesc(b.ctx.GetExprCtx(), windowFunc.Name, args[preArgs:preArgs+len(windowFunc.Args)], checker.InPrepareStmt)
			if err != nil {
				return nil, nil, err
			}
			if desc == nil {
				return nil, nil, plannererrors.ErrWrongArguments.GenWithStackByArgs(strings.ToLower(windowFunc.Name))
			}
			preArgs += len(windowFunc.Args)
			desc.WrapCastForAggArgs(b.ctx.GetExprCtx())
			descs = append(descs, desc)
			windowMap[windowFunc] = schema.Len()
			schema.Append(&expression.Column{
				UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  desc.RetTp,
			})
			window.names = append(window.names, types.EmptyName)
		}
		window.WindowFuncDescs = descs
		window.SetChildren(np)
		window.SetSchema(schema)
		p = window
	}
	return p, windowMap, nil
}

// checkOriginWindowFuncs checks the validity for original window specifications for a group of functions.
// Because the grouped specification is different from them, we should especially check them before build window frame.
func (b *PlanBuilder) checkOriginWindowFuncs(funcs []*ast.WindowFuncExpr, orderByItems []property.SortItem) error {
	for _, f := range funcs {
		if f.IgnoreNull {
			return plannererrors.ErrNotSupportedYet.GenWithStackByArgs("IGNORE NULLS")
		}
		if f.Distinct {
			return plannererrors.ErrNotSupportedYet.GenWithStackByArgs("<window function>(DISTINCT ..)")
		}
		if f.FromLast {
			return plannererrors.ErrNotSupportedYet.GenWithStackByArgs("FROM LAST")
		}
		spec := &f.Spec
		if f.Spec.Name.L != "" {
			spec = b.windowSpecs[f.Spec.Name.L]
		}
		if err := b.checkOriginWindowSpec(spec, orderByItems); err != nil {
			return err
		}
	}
	return nil
}

// checkOriginWindowSpec checks the validity for given window specification.
func (b *PlanBuilder) checkOriginWindowSpec(spec *ast.WindowSpec, orderByItems []property.SortItem) error {
	if spec.Frame == nil {
		return nil
	}
	if spec.Frame.Type == ast.Groups {
		return plannererrors.ErrNotSupportedYet.GenWithStackByArgs("GROUPS")
	}
	start, end := spec.Frame.Extent.Start, spec.Frame.Extent.End
	if start.Type == ast.Following && start.UnBounded {
		return plannererrors.ErrWindowFrameStartIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	if end.Type == ast.Preceding && end.UnBounded {
		return plannererrors.ErrWindowFrameEndIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	if start.Type == ast.Following && (end.Type == ast.Preceding || end.Type == ast.CurrentRow) {
		return plannererrors.ErrWindowFrameIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	if (start.Type == ast.Following || start.Type == ast.CurrentRow) && end.Type == ast.Preceding {
		return plannererrors.ErrWindowFrameIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
	}

	err := b.checkOriginWindowFrameBound(&start, spec, orderByItems)
	if err != nil {
		return err
	}
	err = b.checkOriginWindowFrameBound(&end, spec, orderByItems)
	if err != nil {
		return err
	}
	return nil
}

func (b *PlanBuilder) checkOriginWindowFrameBound(bound *ast.FrameBound, spec *ast.WindowSpec, orderByItems []property.SortItem) error {
	if bound.Type == ast.CurrentRow || bound.UnBounded {
		return nil
	}

	frameType := spec.Frame.Type
	if frameType == ast.Rows {
		if bound.Unit != ast.TimeUnitInvalid {
			return plannererrors.ErrWindowRowsIntervalUse.GenWithStackByArgs(getWindowName(spec.Name.O))
		}
		_, isNull, isExpectedType := getUintFromNode(b.ctx, bound.Expr, false)
		if isNull || !isExpectedType {
			return plannererrors.ErrWindowFrameIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
		}
		return nil
	}

	if len(orderByItems) != 1 {
		return plannererrors.ErrWindowRangeFrameOrderType.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	orderItemType := orderByItems[0].Col.RetType.GetType()
	isNumeric, isTemporal := types.IsTypeNumeric(orderItemType), types.IsTypeTemporal(orderItemType)
	if !isNumeric && !isTemporal {
		return plannererrors.ErrWindowRangeFrameOrderType.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	if bound.Unit != ast.TimeUnitInvalid && !isTemporal {
		return plannererrors.ErrWindowRangeFrameNumericType.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	if bound.Unit == ast.TimeUnitInvalid && !isNumeric {
		return plannererrors.ErrWindowRangeFrameTemporalType.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	return nil
}

func extractWindowFuncs(fields []*ast.SelectField) []*ast.WindowFuncExpr {
	extractor := &WindowFuncExtractor{}
	for _, f := range fields {
		n, _ := f.Expr.Accept(extractor)
		f.Expr = n.(ast.ExprNode)
	}
	return extractor.windowFuncs
}

func (b *PlanBuilder) handleDefaultFrame(spec *ast.WindowSpec, windowFuncName string) (*ast.WindowSpec, bool) {
	needFrame := aggregation.NeedFrame(windowFuncName)
	// According to MySQL, In the absence of a frame clause, the default frame depends on whether an ORDER BY clause is present:
	//   (1) With order by, the default frame is equivalent to "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW";
	//   (2) Without order by, the default frame is includes all partition rows, equivalent to "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
	//       or "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING", which is the same as an empty frame.
	// https://dev.mysql.com/doc/refman/8.0/en/window-functions-frames.html
	if needFrame && spec.Frame == nil && spec.OrderBy != nil {
		newSpec := *spec
		newSpec.Frame = &ast.FrameClause{
			Type: ast.Ranges,
			Extent: ast.FrameExtent{
				Start: ast.FrameBound{Type: ast.Preceding, UnBounded: true},
				End:   ast.FrameBound{Type: ast.CurrentRow},
			},
		}
		return &newSpec, true
	}
	// "RANGE/ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING" is equivalent to empty frame.
	if needFrame && spec.Frame != nil &&
		spec.Frame.Extent.Start.UnBounded && spec.Frame.Extent.End.UnBounded {
		newSpec := *spec
		newSpec.Frame = nil
		return &newSpec, true
	}
	if !needFrame {
		var updated bool
		newSpec := *spec

		// For functions that operate on the entire partition, the frame clause will be ignored.
		if spec.Frame != nil {
			specName := spec.Name.O
			b.ctx.GetSessionVars().StmtCtx.AppendNote(plannererrors.ErrWindowFunctionIgnoresFrame.FastGenByArgs(windowFuncName, getWindowName(specName)))
			newSpec.Frame = nil
			updated = true
		}
		if b.ctx.GetSessionVars().EnablePipelinedWindowExec {
			useDefaultFrame, defaultFrame := aggregation.UseDefaultFrame(windowFuncName)
			if useDefaultFrame {
				newSpec.Frame = &defaultFrame
				updated = true
			}
		}
		if updated {
			return &newSpec, true
		}
	}
	return spec, false
}

// append ast.WindowSpec to []*ast.WindowSpec if absent
func appendIfAbsentWindowSpec(specs []*ast.WindowSpec, ns *ast.WindowSpec) []*ast.WindowSpec {
	for _, spec := range specs {
		if spec == ns {
			return specs
		}
	}
	return append(specs, ns)
}

func specEqual(s1, s2 *ast.WindowSpec) (equal bool, err error) {
	if (s1 == nil && s2 != nil) || (s1 != nil && s2 == nil) {
		return false, nil
	}
	var sb1, sb2 strings.Builder
	ctx1 := format.NewRestoreCtx(0, &sb1)
	ctx2 := format.NewRestoreCtx(0, &sb2)
	if err = s1.Restore(ctx1); err != nil {
		return
	}
	if err = s2.Restore(ctx2); err != nil {
		return
	}
	return sb1.String() == sb2.String(), nil
}

// groupWindowFuncs groups the window functions according to the window specification name.
// TODO: We can group the window function by the definition of window specification.
func (b *PlanBuilder) groupWindowFuncs(windowFuncs []*ast.WindowFuncExpr) (map[*ast.WindowSpec][]*ast.WindowFuncExpr, []*ast.WindowSpec, error) {
	// updatedSpecMap is used to handle the specifications that have frame clause changed.
	updatedSpecMap := make(map[string][]*ast.WindowSpec)
	groupedWindow := make(map[*ast.WindowSpec][]*ast.WindowFuncExpr)
	orderedSpec := make([]*ast.WindowSpec, 0, len(windowFuncs))
	for _, windowFunc := range windowFuncs {
		if windowFunc.Spec.Name.L == "" {
			spec := &windowFunc.Spec
			if spec.Ref.L != "" {
				ref, ok := b.windowSpecs[spec.Ref.L]
				if !ok {
					return nil, nil, plannererrors.ErrWindowNoSuchWindow.GenWithStackByArgs(getWindowName(spec.Ref.O))
				}
				err := mergeWindowSpec(spec, ref)
				if err != nil {
					return nil, nil, err
				}
			}
			spec, _ = b.handleDefaultFrame(spec, windowFunc.Name)
			groupedWindow[spec] = append(groupedWindow[spec], windowFunc)
			orderedSpec = appendIfAbsentWindowSpec(orderedSpec, spec)
			continue
		}

		name := windowFunc.Spec.Name.L
		spec, ok := b.windowSpecs[name]
		if !ok {
			return nil, nil, plannererrors.ErrWindowNoSuchWindow.GenWithStackByArgs(windowFunc.Spec.Name.O)
		}
		newSpec, updated := b.handleDefaultFrame(spec, windowFunc.Name)
		if !updated {
			groupedWindow[spec] = append(groupedWindow[spec], windowFunc)
			orderedSpec = appendIfAbsentWindowSpec(orderedSpec, spec)
		} else {
			var updatedSpec *ast.WindowSpec
			if _, ok := updatedSpecMap[name]; !ok {
				updatedSpecMap[name] = []*ast.WindowSpec{newSpec}
				updatedSpec = newSpec
			} else {
				for _, spec := range updatedSpecMap[name] {
					eq, err := specEqual(spec, newSpec)
					if err != nil {
						return nil, nil, err
					}
					if eq {
						updatedSpec = spec
						break
					}
				}
				if updatedSpec == nil {
					updatedSpec = newSpec
					updatedSpecMap[name] = append(updatedSpecMap[name], newSpec)
				}
			}
			groupedWindow[updatedSpec] = append(groupedWindow[updatedSpec], windowFunc)
			orderedSpec = appendIfAbsentWindowSpec(orderedSpec, updatedSpec)
		}
	}
	// Unused window specs should also be checked in b.buildWindowFunctions,
	// so we add them to `groupedWindow` with empty window functions.
	for _, spec := range b.windowSpecs {
		if _, ok := groupedWindow[spec]; !ok {
			if _, ok = updatedSpecMap[spec.Name.L]; !ok {
				groupedWindow[spec] = nil
				orderedSpec = appendIfAbsentWindowSpec(orderedSpec, spec)
			}
		}
	}
	return groupedWindow, orderedSpec, nil
}

// resolveWindowSpec resolve window specifications for sql like `select ... from t window w1 as (w2), w2 as (partition by a)`.
// We need to resolve the referenced window to get the definition of current window spec.
func resolveWindowSpec(spec *ast.WindowSpec, specs map[string]*ast.WindowSpec, inStack map[string]bool) error {
	if inStack[spec.Name.L] {
		return errors.Trace(plannererrors.ErrWindowCircularityInWindowGraph)
	}
	if spec.Ref.L == "" {
		return nil
	}
	ref, ok := specs[spec.Ref.L]
	if !ok {
		return plannererrors.ErrWindowNoSuchWindow.GenWithStackByArgs(spec.Ref.O)
	}
	inStack[spec.Name.L] = true
	err := resolveWindowSpec(ref, specs, inStack)
	if err != nil {
		return err
	}
	inStack[spec.Name.L] = false
	return mergeWindowSpec(spec, ref)
}

func mergeWindowSpec(spec, ref *ast.WindowSpec) error {
	if ref.Frame != nil {
		return plannererrors.ErrWindowNoInherentFrame.GenWithStackByArgs(ref.Name.O)
	}
	if spec.PartitionBy != nil {
		return errors.Trace(plannererrors.ErrWindowNoChildPartitioning)
	}
	if ref.OrderBy != nil {
		if spec.OrderBy != nil {
			return plannererrors.ErrWindowNoRedefineOrderBy.GenWithStackByArgs(getWindowName(spec.Name.O), ref.Name.O)
		}
		spec.OrderBy = ref.OrderBy
	}
	spec.PartitionBy = ref.PartitionBy
	spec.Ref = model.NewCIStr("")
	return nil
}

func buildWindowSpecs(specs []ast.WindowSpec) (map[string]*ast.WindowSpec, error) {
	specsMap := make(map[string]*ast.WindowSpec, len(specs))
	for _, spec := range specs {
		if _, ok := specsMap[spec.Name.L]; ok {
			return nil, plannererrors.ErrWindowDuplicateName.GenWithStackByArgs(spec.Name.O)
		}
		newSpec := spec
		specsMap[spec.Name.L] = &newSpec
	}
	inStack := make(map[string]bool, len(specs))
	for _, spec := range specsMap {
		err := resolveWindowSpec(spec, specsMap, inStack)
		if err != nil {
			return nil, err
		}
	}
	return specsMap, nil
}

type updatableTableListResolver struct {
	updatableTableList []*ast.TableName
}

func (*updatableTableListResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	switch v := inNode.(type) {
	case *ast.UpdateStmt, *ast.TableRefsClause, *ast.Join, *ast.TableSource, *ast.TableName:
		return v, false
	}
	return inNode, true
}

func (u *updatableTableListResolver) Leave(inNode ast.Node) (ast.Node, bool) {
	if v, ok := inNode.(*ast.TableSource); ok {
		if s, ok := v.Source.(*ast.TableName); ok {
			if v.AsName.L != "" {
				newTableName := *s
				newTableName.Name = v.AsName
				newTableName.Schema = model.NewCIStr("")
				u.updatableTableList = append(u.updatableTableList, &newTableName)
			} else {
				u.updatableTableList = append(u.updatableTableList, s)
			}
		}
	}
	return inNode, true
}

// ExtractTableList is a wrapper for tableListExtractor and removes duplicate TableName
// If asName is true, extract AsName prior to OrigName.
// Privilege check should use OrigName, while expression may use AsName.
func ExtractTableList(node ast.Node, asName bool) []*ast.TableName {
	if node == nil {
		return []*ast.TableName{}
	}
	e := &tableListExtractor{
		asName:     asName,
		tableNames: []*ast.TableName{},
	}
	node.Accept(e)
	tableNames := e.tableNames
	m := make(map[string]map[string]*ast.TableName) // k1: schemaName, k2: tableName, v: ast.TableName
	for _, x := range tableNames {
		k1, k2 := x.Schema.L, x.Name.L
		// allow empty schema name OR empty table name
		if k1 != "" || k2 != "" {
			if _, ok := m[k1]; !ok {
				m[k1] = make(map[string]*ast.TableName)
			}
			m[k1][k2] = x
		}
	}
	tableNames = tableNames[:0]
	for _, x := range m {
		for _, v := range x {
			tableNames = append(tableNames, v)
		}
	}
	return tableNames
}

// tableListExtractor extracts all the TableNames from node.
type tableListExtractor struct {
	asName     bool
	tableNames []*ast.TableName
}

func (e *tableListExtractor) Enter(n ast.Node) (_ ast.Node, skipChildren bool) {
	innerExtract := func(inner ast.Node) []*ast.TableName {
		if inner == nil {
			return nil
		}
		innerExtractor := &tableListExtractor{
			asName:     e.asName,
			tableNames: []*ast.TableName{},
		}
		inner.Accept(innerExtractor)
		return innerExtractor.tableNames
	}

	switch x := n.(type) {
	case *ast.TableName:
		e.tableNames = append(e.tableNames, x)
	case *ast.TableSource:
		if s, ok := x.Source.(*ast.TableName); ok {
			if x.AsName.L != "" && e.asName {
				newTableName := *s
				newTableName.Name = x.AsName
				newTableName.Schema = model.NewCIStr("")
				e.tableNames = append(e.tableNames, &newTableName)
			} else {
				e.tableNames = append(e.tableNames, s)
			}
		} else if s, ok := x.Source.(*ast.SelectStmt); ok {
			if s.From != nil {
				innerList := innerExtract(s.From.TableRefs)
				if len(innerList) > 0 {
					innerTableName := innerList[0]
					if x.AsName.L != "" && e.asName {
						newTableName := *innerList[0]
						newTableName.Name = x.AsName
						newTableName.Schema = model.NewCIStr("")
						innerTableName = &newTableName
					}
					e.tableNames = append(e.tableNames, innerTableName)
				}
			}
		}
		return n, true

	case *ast.ShowStmt:
		if x.DBName != "" {
			e.tableNames = append(e.tableNames, &ast.TableName{Schema: model.NewCIStr(x.DBName)})
		}
	case *ast.CreateDatabaseStmt:
		e.tableNames = append(e.tableNames, &ast.TableName{Schema: x.Name})
	case *ast.AlterDatabaseStmt:
		e.tableNames = append(e.tableNames, &ast.TableName{Schema: x.Name})
	case *ast.DropDatabaseStmt:
		e.tableNames = append(e.tableNames, &ast.TableName{Schema: x.Name})

	case *ast.FlashBackDatabaseStmt:
		e.tableNames = append(e.tableNames, &ast.TableName{Schema: x.DBName})
		e.tableNames = append(e.tableNames, &ast.TableName{Schema: model.NewCIStr(x.NewName)})
	case *ast.FlashBackToTimestampStmt:
		if x.DBName.L != "" {
			e.tableNames = append(e.tableNames, &ast.TableName{Schema: x.DBName})
		}
	case *ast.FlashBackTableStmt:
		if newName := x.NewName; newName != "" {
			e.tableNames = append(e.tableNames, &ast.TableName{
				Schema: x.Table.Schema,
				Name:   model.NewCIStr(newName)})
		}

	case *ast.GrantStmt:
		if x.ObjectType == ast.ObjectTypeTable || x.ObjectType == ast.ObjectTypeNone {
			if x.Level.Level == ast.GrantLevelDB || x.Level.Level == ast.GrantLevelTable {
				e.tableNames = append(e.tableNames, &ast.TableName{
					Schema: model.NewCIStr(x.Level.DBName),
					Name:   model.NewCIStr(x.Level.TableName),
				})
			}
		}
	case *ast.RevokeStmt:
		if x.ObjectType == ast.ObjectTypeTable || x.ObjectType == ast.ObjectTypeNone {
			if x.Level.Level == ast.GrantLevelDB || x.Level.Level == ast.GrantLevelTable {
				e.tableNames = append(e.tableNames, &ast.TableName{
					Schema: model.NewCIStr(x.Level.DBName),
					Name:   model.NewCIStr(x.Level.TableName),
				})
			}
		}
	case *ast.BRIEStmt:
		if x.Kind == ast.BRIEKindBackup || x.Kind == ast.BRIEKindRestore {
			for _, v := range x.Schemas {
				e.tableNames = append(e.tableNames, &ast.TableName{Schema: model.NewCIStr(v)})
			}
		}
	case *ast.UseStmt:
		e.tableNames = append(e.tableNames, &ast.TableName{Schema: model.NewCIStr(x.DBName)})
	case *ast.ExecuteStmt:
		if v, ok := x.PrepStmt.(*PlanCacheStmt); ok {
			e.tableNames = append(e.tableNames, innerExtract(v.PreparedAst.Stmt)...)
		}
	}
	return n, false
}

func (*tableListExtractor) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

func collectTableName(node ast.ResultSetNode, updatableName *map[string]bool, info *map[string]*ast.TableName) {
	switch x := node.(type) {
	case *ast.Join:
		collectTableName(x.Left, updatableName, info)
		collectTableName(x.Right, updatableName, info)
	case *ast.TableSource:
		name := x.AsName.L
		var canUpdate bool
		var s *ast.TableName
		if s, canUpdate = x.Source.(*ast.TableName); canUpdate {
			if name == "" {
				name = s.Schema.L + "." + s.Name.L
				// it may be a CTE
				if s.Schema.L == "" {
					name = s.Name.L
				}
			}
			(*info)[name] = s
		}
		(*updatableName)[name] = canUpdate && s.Schema.L != ""
	}
}

func appendDynamicVisitInfo(vi []visitInfo, priv string, withGrant bool, err error) []visitInfo {
	return append(vi, visitInfo{
		privilege:        mysql.ExtendedPriv,
		dynamicPriv:      priv,
		dynamicWithGrant: withGrant,
		err:              err,
	})
}

func appendVisitInfo(vi []visitInfo, priv mysql.PrivilegeType, db, tbl, col string, err error) []visitInfo {
	return append(vi, visitInfo{
		privilege: priv,
		db:        db,
		table:     tbl,
		column:    col,
		err:       err,
	})
}

func getInnerFromParenthesesAndUnaryPlus(expr ast.ExprNode) ast.ExprNode {
	if pexpr, ok := expr.(*ast.ParenthesesExpr); ok {
		return getInnerFromParenthesesAndUnaryPlus(pexpr.Expr)
	}
	if uexpr, ok := expr.(*ast.UnaryOperationExpr); ok && uexpr.Op == opcode.Plus {
		return getInnerFromParenthesesAndUnaryPlus(uexpr.V)
	}
	return expr
}

// containDifferentJoinTypes checks whether `preferJoinType` contains different
// join types.
func containDifferentJoinTypes(preferJoinType uint) bool {
	preferJoinType &= ^h.PreferNoHashJoin
	preferJoinType &= ^h.PreferNoMergeJoin
	preferJoinType &= ^h.PreferNoIndexJoin
	preferJoinType &= ^h.PreferNoIndexHashJoin
	preferJoinType &= ^h.PreferNoIndexMergeJoin

	inlMask := h.PreferRightAsINLJInner ^ h.PreferLeftAsINLJInner
	inlhjMask := h.PreferRightAsINLHJInner ^ h.PreferLeftAsINLHJInner
	inlmjMask := h.PreferRightAsINLMJInner ^ h.PreferLeftAsINLMJInner
	hjRightBuildMask := h.PreferRightAsHJBuild ^ h.PreferLeftAsHJProbe
	hjLeftBuildMask := h.PreferLeftAsHJBuild ^ h.PreferRightAsHJProbe

	mppMask := h.PreferShuffleJoin ^ h.PreferBCJoin
	mask := inlMask ^ inlhjMask ^ inlmjMask ^ hjRightBuildMask ^ hjLeftBuildMask
	onesCount := bits.OnesCount(preferJoinType & ^mask & ^mppMask)
	if onesCount > 1 || onesCount == 1 && preferJoinType&mask > 0 {
		return true
	}

	cnt := 0
	if preferJoinType&inlMask > 0 {
		cnt++
	}
	if preferJoinType&inlhjMask > 0 {
		cnt++
	}
	if preferJoinType&inlmjMask > 0 {
		cnt++
	}
	if preferJoinType&hjLeftBuildMask > 0 {
		cnt++
	}
	if preferJoinType&hjRightBuildMask > 0 {
		cnt++
	}
	return cnt > 1
}

func hasMPPJoinHints(preferJoinType uint) bool {
	return (preferJoinType&h.PreferBCJoin > 0) || (preferJoinType&h.PreferShuffleJoin > 0)
}

// isJoinHintSupportedInMPPMode is used to check if the specified join hint is available under MPP mode.
func isJoinHintSupportedInMPPMode(preferJoinType uint) bool {
	if preferJoinType == 0 {
		return true
	}
	mppMask := h.PreferShuffleJoin ^ h.PreferBCJoin
	// Currently, TiFlash only supports HASH JOIN, so the hint for HASH JOIN is available while other join method hints are forbidden.
	joinMethodHintSupportedByTiflash := h.PreferHashJoin ^ h.PreferLeftAsHJBuild ^ h.PreferRightAsHJBuild ^ h.PreferLeftAsHJProbe ^ h.PreferRightAsHJProbe
	onesCount := bits.OnesCount(preferJoinType & ^joinMethodHintSupportedByTiflash & ^mppMask)
	return onesCount < 1
}

func (b *PlanBuilder) buildCte(ctx context.Context, cte *ast.CommonTableExpression, isRecursive bool) (p base.LogicalPlan, err error) {
	saveBuildingCTE := b.buildingCTE
	b.buildingCTE = true
	defer func() {
		b.buildingCTE = saveBuildingCTE
	}()

	if isRecursive {
		// buildingRecursivePartForCTE likes a stack. We save it before building a recursive CTE and restore it after building.
		// We need a stack because we need to handle the nested recursive CTE. And buildingRecursivePartForCTE indicates the innermost CTE.
		saveCheck := b.buildingRecursivePartForCTE
		b.buildingRecursivePartForCTE = false
		err = b.buildRecursiveCTE(ctx, cte.Query.Query)
		if err != nil {
			return nil, err
		}
		b.buildingRecursivePartForCTE = saveCheck
	} else {
		p, err = b.buildResultSetNode(ctx, cte.Query.Query, true)
		if err != nil {
			return nil, err
		}

		p, err = b.adjustCTEPlanOutputName(p, cte)
		if err != nil {
			return nil, err
		}

		cInfo := b.outerCTEs[len(b.outerCTEs)-1]
		cInfo.seedLP = p
	}
	return nil, nil
}

// buildRecursiveCTE handles the with clause `with recursive xxx as xx`.
func (b *PlanBuilder) buildRecursiveCTE(ctx context.Context, cte ast.ResultSetNode) error {
	b.isCTE = true
	cInfo := b.outerCTEs[len(b.outerCTEs)-1]
	switch x := (cte).(type) {
	case *ast.SetOprStmt:
		// 1. Handle the WITH clause if exists.
		if x.With != nil {
			l := len(b.outerCTEs)
			sw := x.With
			defer func() {
				b.outerCTEs = b.outerCTEs[:l]
				x.With = sw
			}()
			_, err := b.buildWith(ctx, x.With)
			if err != nil {
				return err
			}
		}
		// Set it to nil, so that when builds the seed part, it won't build again. Reset it in defer so that the AST doesn't change after this function.
		x.With = nil

		// 2. Build plans for each part of SetOprStmt.
		recursive := make([]base.LogicalPlan, 0)
		tmpAfterSetOptsForRecur := []*ast.SetOprType{nil}

		expectSeed := true
		for i := 0; i < len(x.SelectList.Selects); i++ {
			var p base.LogicalPlan
			var err error

			var afterOpr *ast.SetOprType
			switch y := x.SelectList.Selects[i].(type) {
			case *ast.SelectStmt:
				p, err = b.buildSelect(ctx, y)
				afterOpr = y.AfterSetOperator
			case *ast.SetOprSelectList:
				p, err = b.buildSetOpr(ctx, &ast.SetOprStmt{SelectList: y, With: y.With})
				afterOpr = y.AfterSetOperator
			}

			if expectSeed {
				if cInfo.useRecursive {
					// 3. If it fail to build a plan, it may be the recursive part. Then we build the seed part plan, and rebuild it.
					if i == 0 {
						return plannererrors.ErrCTERecursiveRequiresNonRecursiveFirst.GenWithStackByArgs(cInfo.def.Name.String())
					}

					// It's the recursive part. Build the seed part, and build this recursive part again.
					// Before we build the seed part, do some checks.
					if x.OrderBy != nil {
						return plannererrors.ErrNotSupportedYet.GenWithStackByArgs("ORDER BY over UNION in recursive Common Table Expression")
					}
					// Limit clause is for the whole CTE instead of only for the seed part.
					oriLimit := x.Limit
					x.Limit = nil

					// Check union type.
					if afterOpr != nil {
						if *afterOpr != ast.Union && *afterOpr != ast.UnionAll {
							return plannererrors.ErrNotSupportedYet.GenWithStackByArgs(fmt.Sprintf("%s between seed part and recursive part, hint: The operator between seed part and recursive part must bu UNION[DISTINCT] or UNION ALL", afterOpr.String()))
						}
						cInfo.isDistinct = *afterOpr == ast.Union
					}

					expectSeed = false
					cInfo.useRecursive = false

					// Build seed part plan.
					saveSelect := x.SelectList.Selects
					x.SelectList.Selects = x.SelectList.Selects[:i]
					// We're rebuilding the seed part, so we pop the result we built previously.
					for _i := 0; _i < i; _i++ {
						b.handleHelper.popMap()
					}
					p, err = b.buildSetOpr(ctx, x)
					if err != nil {
						return err
					}
					x.SelectList.Selects = saveSelect
					p, err = b.adjustCTEPlanOutputName(p, cInfo.def)
					if err != nil {
						return err
					}
					cInfo.seedLP = p

					// Rebuild the plan.
					i--
					b.buildingRecursivePartForCTE = true
					x.Limit = oriLimit
					continue
				}
				if err != nil {
					return err
				}
			} else {
				if err != nil {
					return err
				}
				if afterOpr != nil {
					if *afterOpr != ast.Union && *afterOpr != ast.UnionAll {
						return plannererrors.ErrNotSupportedYet.GenWithStackByArgs(fmt.Sprintf("%s between recursive part's selects, hint: The operator between recursive part's selects must bu UNION[DISTINCT] or UNION ALL", afterOpr.String()))
					}
				}
				if !cInfo.useRecursive {
					return plannererrors.ErrCTERecursiveRequiresNonRecursiveFirst.GenWithStackByArgs(cInfo.def.Name.String())
				}
				cInfo.useRecursive = false
				recursive = append(recursive, p)
				tmpAfterSetOptsForRecur = append(tmpAfterSetOptsForRecur, afterOpr)
			}
		}

		if len(recursive) == 0 {
			// In this case, even if SQL specifies "WITH RECURSIVE", the CTE is non-recursive.
			p, err := b.buildSetOpr(ctx, x)
			if err != nil {
				return err
			}
			p, err = b.adjustCTEPlanOutputName(p, cInfo.def)
			if err != nil {
				return err
			}
			cInfo.seedLP = p
			return nil
		}

		// Build the recursive part's logical plan.
		recurPart, err := b.buildUnion(ctx, recursive, tmpAfterSetOptsForRecur)
		if err != nil {
			return err
		}
		recurPart, err = b.buildProjection4CTEUnion(ctx, cInfo.seedLP, recurPart)
		if err != nil {
			return err
		}
		// 4. Finally, we get the seed part plan and recursive part plan.
		cInfo.recurLP = recurPart
		// Only need to handle limit if x is SetOprStmt.
		if x.Limit != nil {
			limit, err := b.buildLimit(cInfo.seedLP, x.Limit)
			if err != nil {
				return err
			}
			limit.SetChildren(limit.Children()[:0]...)
			cInfo.limitLP = limit
		}
		return nil
	default:
		p, err := b.buildResultSetNode(ctx, x, true)
		if err != nil {
			// Refine the error message.
			if errors.ErrorEqual(err, plannererrors.ErrCTERecursiveRequiresNonRecursiveFirst) {
				err = plannererrors.ErrCTERecursiveRequiresUnion.GenWithStackByArgs(cInfo.def.Name.String())
			}
			return err
		}
		p, err = b.adjustCTEPlanOutputName(p, cInfo.def)
		if err != nil {
			return err
		}
		cInfo.seedLP = p
		return nil
	}
}

func (b *PlanBuilder) adjustCTEPlanOutputName(p base.LogicalPlan, def *ast.CommonTableExpression) (base.LogicalPlan, error) {
	outPutNames := p.OutputNames()
	for _, name := range outPutNames {
		name.TblName = def.Name
		name.DBName = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
	}
	if len(def.ColNameList) > 0 {
		if len(def.ColNameList) != len(p.OutputNames()) {
			return nil, dbterror.ErrViewWrongList
		}
		for i, n := range def.ColNameList {
			outPutNames[i].ColName = n
		}
	}
	p.SetOutputNames(outPutNames)
	return p, nil
}

// prepareCTECheckForSubQuery prepares the check that the recursive CTE can't be referenced in subQuery. It's used before building a subQuery.
// For example: with recursive cte(n) as (select 1 union select * from (select * from cte) c1) select * from cte;
func (b *PlanBuilder) prepareCTECheckForSubQuery() []*cteInfo {
	modifiedCTE := make([]*cteInfo, 0)
	for _, cte := range b.outerCTEs {
		if cte.isBuilding && !cte.enterSubquery {
			cte.enterSubquery = true
			modifiedCTE = append(modifiedCTE, cte)
		}
	}
	return modifiedCTE
}

// resetCTECheckForSubQuery resets the related variable. It's used after leaving a subQuery.
func resetCTECheckForSubQuery(ci []*cteInfo) {
	for _, cte := range ci {
		cte.enterSubquery = false
	}
}

// genCTETableNameForError find the nearest CTE name.
func (b *PlanBuilder) genCTETableNameForError() string {
	name := ""
	for i := len(b.outerCTEs) - 1; i >= 0; i-- {
		if b.outerCTEs[i].isBuilding {
			name = b.outerCTEs[i].def.Name.String()
			break
		}
	}
	return name
}

func (b *PlanBuilder) buildWith(ctx context.Context, w *ast.WithClause) ([]*cteInfo, error) {
	// Check CTE name must be unique.
	nameMap := make(map[string]struct{})
	for _, cte := range w.CTEs {
		if _, ok := nameMap[cte.Name.L]; ok {
			return nil, plannererrors.ErrNonUniqTable
		}
		nameMap[cte.Name.L] = struct{}{}
	}
	ctes := make([]*cteInfo, 0, len(w.CTEs))
	for _, cte := range w.CTEs {
		b.outerCTEs = append(b.outerCTEs, &cteInfo{def: cte, nonRecursive: !w.IsRecursive, isBuilding: true, storageID: b.allocIDForCTEStorage, seedStat: &property.StatsInfo{}, consumerCount: cte.ConsumerCount})
		b.allocIDForCTEStorage++
		saveFlag := b.optFlag
		// Init the flag to flagPrunColumns, otherwise it's missing.
		b.optFlag = flagPrunColumns
		if b.ctx.GetSessionVars().EnableForceInlineCTE() {
			b.outerCTEs[len(b.outerCTEs)-1].forceInlineByHintOrVar = true
		}
		_, err := b.buildCte(ctx, cte, w.IsRecursive)
		if err != nil {
			return nil, err
		}
		b.outerCTEs[len(b.outerCTEs)-1].optFlag = b.optFlag
		b.outerCTEs[len(b.outerCTEs)-1].isBuilding = false
		b.optFlag = saveFlag
		// each cte (select statement) will generate a handle map, pop it out here.
		b.handleHelper.popMap()
		ctes = append(ctes, b.outerCTEs[len(b.outerCTEs)-1])
	}
	return ctes, nil
}

func (b *PlanBuilder) buildProjection4CTEUnion(_ context.Context, seed base.LogicalPlan, recur base.LogicalPlan) (base.LogicalPlan, error) {
	if seed.Schema().Len() != recur.Schema().Len() {
		return nil, plannererrors.ErrWrongNumberOfColumnsInSelect.GenWithStackByArgs()
	}
	exprs := make([]expression.Expression, len(seed.Schema().Columns))
	resSchema := getResultCTESchema(seed.Schema(), b.ctx.GetSessionVars())
	for i, col := range recur.Schema().Columns {
		if !resSchema.Columns[i].RetType.Equal(col.RetType) {
			exprs[i] = expression.BuildCastFunction4Union(b.ctx.GetExprCtx(), col, resSchema.Columns[i].RetType)
		} else {
			exprs[i] = col
		}
	}
	b.optFlag |= flagEliminateProjection
	proj := LogicalProjection{Exprs: exprs, AvoidColumnEvaluator: true}.Init(b.ctx, b.getSelectOffset())
	proj.SetSchema(resSchema)
	proj.SetChildren(recur)
	return proj, nil
}

// The recursive part/CTE's schema is nullable, and the UID should be unique.
func getResultCTESchema(seedSchema *expression.Schema, svar *variable.SessionVars) *expression.Schema {
	res := seedSchema.Clone()
	for _, col := range res.Columns {
		col.RetType = col.RetType.Clone()
		col.UniqueID = svar.AllocPlanColumnID()
		col.RetType.DelFlag(mysql.NotNullFlag)
		// Since you have reallocated unique id here, the old-cloned-cached hash code is not valid anymore.
		col.CleanHashCode()
	}
	return res
}
