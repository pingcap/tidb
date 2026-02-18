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
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

func (b *PlanBuilder) buildDistinct(child base.LogicalPlan, length int) (*logicalop.LogicalAggregation, error) {
	b.optFlag = b.optFlag | rule.FlagBuildKeyInfo
	b.optFlag = b.optFlag | rule.FlagPushDownAgg
	// flag it if cte contain distinct
	if b.buildingCTE {
		b.outerCTEs[len(b.outerCTEs)-1].containRecursiveForbiddenOperator = true
	}
	plan4Agg := logicalop.LogicalAggregation{
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
	plan4Agg.SetOutputNames(child.OutputNames())
	// Distinct will be rewritten as first_row, we reset the type here since the return type
	// of first_row is not always the same as the column arg of first_row.
	for i, col := range plan4Agg.Schema().Columns {
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
	if resultTp.EvalType() != types.ETInt && (a.EvalType() == types.ETInt || b.EvalType() == types.ETInt) &&
		(resultTp.GetFlen() < mysql.MaxIntWidth && resultTp.GetFlen() != types.UnspecifiedLength) {
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
	for i := range cols {
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

func (b *PlanBuilder) buildProjection4Union(_ context.Context, u *logicalop.LogicalUnionAll) error {
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
	u.SetSchema(expression.NewSchema(unionCols...))
	u.SetOutputNames(names)
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
		b.optFlag |= rule.FlagEliminateProjection
		proj := logicalop.LogicalProjection{Exprs: exprs}.Init(b.ctx, b.getSelectOffset())
		proj.SetSchema(u.Schema().Clone())
		// reset the schema type to make the "not null" flag right.
		for i, expr := range exprs {
			proj.Schema().Columns[i].RetType = expr.GetType(b.ctx.GetExprCtx().GetEvalCtx())
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

	for range setOpr.SelectList.Selects {
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
		proj := logicalop.LogicalProjection{Exprs: expression.Column2Exprs(setOprPlan.Schema().Columns[:oldLen])}.Init(b.ctx, b.getSelectOffset())
		proj.SetChildren(setOprPlan)
		schema := expression.NewSchema(setOprPlan.Schema().Clone().Columns[:oldLen]...)
		for _, col := range schema.Columns {
			col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
		}
		proj.SetOutputNames(setOprPlan.OutputNames()[:oldLen])
		proj.SetSchema(schema)
		return proj, nil
	}
	return setOprPlan, nil
}

func (b *PlanBuilder) buildSemiJoinForSetOperator(
	leftOriginPlan base.LogicalPlan,
	rightPlan base.LogicalPlan,
	joinType base.JoinType) (leftPlan base.LogicalPlan, err error) {
	leftPlan, err = b.buildDistinct(leftOriginPlan, leftOriginPlan.Schema().Len())
	if err != nil {
		return nil, err
	}

	joinPlan := logicalop.LogicalJoin{JoinType: joinType}.Init(b.ctx, b.getSelectOffset())
	joinPlan.SetChildren(leftPlan, rightPlan)
	joinPlan.SetSchema(leftPlan.Schema())
	joinPlan.SetOutputNames(make([]*types.FieldName, leftPlan.Schema().Len()))
	copy(joinPlan.OutputNames(), leftPlan.OutputNames())
	for j := range rightPlan.Schema().Columns {
		leftCol, rightCol := leftPlan.Schema().Columns[j], rightPlan.Schema().Columns[j]
		eqCond, err := expression.NewFunction(b.ctx.GetExprCtx(), ast.NullEQ, types.NewFieldType(mysql.TypeTiny), leftCol, rightCol)
		if err != nil {
			return nil, err
		}
		sf := eqCond.(*expression.ScalarFunction)
		_, _, ok := expression.IsColOpCol(sf)
		if leftCol.RetType.GetType() != rightCol.RetType.GetType() || !ok {
			joinPlan.OtherConditions = append(joinPlan.OtherConditions, eqCond)
		} else {
			joinPlan.EqualConditions = append(joinPlan.EqualConditions, sf)
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
		leftPlan, err = b.buildSemiJoinForSetOperator(leftPlan, rightPlan, base.SemiJoin)
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
			leftPlan, err = b.buildSemiJoinForSetOperator(leftPlan, rightPlan, base.AntiSemiJoin)
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
	b.optFlag |= rule.FlagEliminateUnionAllDualItem
	u := logicalop.LogicalUnionAll{}.Init(b.ctx, b.getSelectOffset())
	u.SetChildren(subPlan...)
	err := b.buildProjection4Union(ctx, u)
	return u, err
}
