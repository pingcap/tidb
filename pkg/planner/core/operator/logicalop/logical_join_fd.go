// Copyright 2024 PingCAP, Inc.
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

package logicalop

import (
	"maps"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/intset"
)

func (p *LogicalJoin) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.EqualConditions)+len(p.LeftConditions)+len(p.RightConditions)+len(p.OtherConditions))
	for _, fun := range p.EqualConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.LeftConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.RightConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.OtherConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	return corCols
}

// MaxOneRow inherits the BaseLogicalPlan.LogicalPlan.<16th> implementation.

// Children inherits the BaseLogicalPlan.LogicalPlan.<17th> implementation.

// SetChildren inherits the BaseLogicalPlan.LogicalPlan.<18th> implementation.

// SetChild inherits the BaseLogicalPlan.LogicalPlan.<19th> implementation.

// RollBackTaskMap inherits the BaseLogicalPlan.LogicalPlan.<20th> implementation.

// CanPushToCop inherits the BaseLogicalPlan.LogicalPlan.<21st> implementation.

// ExtractFD implements the base.LogicalPlan.<22th> interface.
func (p *LogicalJoin) ExtractFD() *funcdep.FDSet {
	switch p.JoinType {
	case base.InnerJoin:
		return p.ExtractFDForInnerJoin(nil)
	case base.LeftOuterJoin, base.RightOuterJoin:
		return p.ExtractFDForOuterJoin(nil)
	case base.SemiJoin:
		return p.ExtractFDForSemiJoin(nil)
	default:
		return &funcdep.FDSet{HashCodeToUniqueID: make(map[string]int)}
	}
}

// GetBaseLogicalPlan inherits the BaseLogicalPlan.LogicalPlan.<23th> implementation.

// ConvertOuterToInnerJoin implements base.LogicalPlan.<24th> interface.
func (p *LogicalJoin) ConvertOuterToInnerJoin(predicates []expression.Expression) base.LogicalPlan {
	innerTable := p.Children()[0]
	outerTable := p.Children()[1]
	switchChild := false

	if p.JoinType == base.LeftOuterJoin {
		innerTable, outerTable = outerTable, innerTable
		switchChild = true
	}

	// First, simplify this join
	if p.JoinType == base.LeftOuterJoin || p.JoinType == base.RightOuterJoin {
		canBeSimplified := false
		for _, expr := range predicates {
			isOk := util.IsNullRejected(p.SCtx(), innerTable.Schema(), expr, true)
			if isOk {
				canBeSimplified = true
				break
			}
		}
		if canBeSimplified {
			p.JoinType = base.InnerJoin
		}
	}

	// Next simplify join children

	combinedCond := mergeOnClausePredicates(p, predicates)
	if p.JoinType == base.LeftOuterJoin || p.JoinType == base.RightOuterJoin {
		innerTable = innerTable.ConvertOuterToInnerJoin(combinedCond)
		outerTable = outerTable.ConvertOuterToInnerJoin(predicates)
	} else if p.JoinType == base.InnerJoin || p.JoinType == base.SemiJoin {
		innerTable = innerTable.ConvertOuterToInnerJoin(combinedCond)
		outerTable = outerTable.ConvertOuterToInnerJoin(combinedCond)
	} else if p.JoinType == base.AntiSemiJoin {
		innerTable = innerTable.ConvertOuterToInnerJoin(predicates)
		outerTable = outerTable.ConvertOuterToInnerJoin(combinedCond)
	} else {
		innerTable = innerTable.ConvertOuterToInnerJoin(predicates)
		outerTable = outerTable.ConvertOuterToInnerJoin(predicates)
	}

	if switchChild {
		p.SetChild(0, outerTable)
		p.SetChild(1, innerTable)
	} else {
		p.SetChild(0, innerTable)
		p.SetChild(1, outerTable)
	}

	return p
}

// GetJoinChildStatsAndSchema gets the stats and schema of join children.
func (p *LogicalJoin) GetJoinChildStatsAndSchema() (stats0, stats1 *property.StatsInfo, schema0, schema1 *expression.Schema) {
	stats1, schema1 = p.Children()[1].StatsInfo(), p.Children()[1].Schema()
	stats0, schema0 = p.Children()[0].StatsInfo(), p.Children()[0].Schema()
	return
}

// *************************** end implementation of logicalPlan interface ***************************

// IsNAAJ checks if the join is a non-adjacent-join.
func (p *LogicalJoin) IsNAAJ() bool {
	return len(p.NAEQConditions) > 0
}

// Shallow copies a LogicalJoin struct.
func (p *LogicalJoin) Shallow() *LogicalJoin {
	join := *p
	return join.Init(p.SCtx(), p.QueryBlockOffset())
}

// ExtractFDForSemiJoin extracts FD for semi join.
func (p *LogicalJoin) ExtractFDForSemiJoin(equivFromApply [][]intset.FastIntSet) *funcdep.FDSet {
	// 1: since semi join will keep the part or all rows of the outer table, it's outer FD can be saved.
	// 2: the un-projected column will be left for the upper layer projection or already be pruned from bottom up.
	outerFD, _ := p.Children()[0].ExtractFD(), p.Children()[1].ExtractFD()
	fds := outerFD

	eqCondSlice := expression.ScalarFuncs2Exprs(p.EqualConditions)
	allConds := append(eqCondSlice, p.OtherConditions...)
	notNullColsFromFilters := util.ExtractNotNullFromConds(allConds, p)

	constUniqueIDs := util.ExtractConstantCols(p.LeftConditions, p.SCtx(), fds)

	for _, equiv := range equivFromApply {
		fds.AddEquivalence(equiv[0], equiv[1])
	}

	fds.MakeNotNull(notNullColsFromFilters)
	fds.AddConstants(constUniqueIDs)
	p.SetFDs(fds)
	return fds
}

// ExtractFDForInnerJoin extracts FD for inner join.
func (p *LogicalJoin) ExtractFDForInnerJoin(equivFromApply [][]intset.FastIntSet) *funcdep.FDSet {
	child := p.Children()
	rightFD := child[1].ExtractFD()
	leftFD := child[0].ExtractFD()
	fds := leftFD
	fds.MakeCartesianProduct(rightFD)

	eqCondSlice := expression.ScalarFuncs2Exprs(p.EqualConditions)
	// some join eq conditions are stored in the OtherConditions.
	allConds := append(eqCondSlice, p.OtherConditions...)
	notNullColsFromFilters := util.ExtractNotNullFromConds(allConds, p)

	constUniqueIDs := util.ExtractConstantCols(allConds, p.SCtx(), fds)

	equivUniqueIDs := util.ExtractEquivalenceCols(allConds, p.SCtx(), fds)

	fds.MakeNotNull(notNullColsFromFilters)
	fds.AddConstants(constUniqueIDs)
	for _, equiv := range equivUniqueIDs {
		fds.AddEquivalence(equiv[0], equiv[1])
	}
	for _, equiv := range equivFromApply {
		fds.AddEquivalence(equiv[0], equiv[1])
	}
	// merge the not-null-cols/registered-map from both side together.
	fds.NotNullCols.UnionWith(rightFD.NotNullCols)
	if fds.HashCodeToUniqueID == nil {
		fds.HashCodeToUniqueID = rightFD.HashCodeToUniqueID
	} else {
		for k, v := range rightFD.HashCodeToUniqueID {
			// If there's same constant in the different subquery, we might go into this IF branch.
			if _, ok := fds.HashCodeToUniqueID[k]; ok {
				continue
			}
			fds.HashCodeToUniqueID[k] = v
		}
	}
	for i, ok := rightFD.GroupByCols.Next(0); ok; i, ok = rightFD.GroupByCols.Next(i + 1) {
		fds.GroupByCols.Insert(i)
	}
	fds.HasAggBuilt = fds.HasAggBuilt || rightFD.HasAggBuilt
	p.SetFDs(fds)
	return fds
}

// ExtractFDForOuterJoin extracts FD for outer join.
func (p *LogicalJoin) ExtractFDForOuterJoin(equivFromApply [][]intset.FastIntSet) *funcdep.FDSet {
	outerFD, innerFD := p.Children()[0].ExtractFD(), p.Children()[1].ExtractFD()
	innerCondition := p.RightConditions
	outerCondition := p.LeftConditions
	outerCols, innerCols := intset.NewFastIntSet(), intset.NewFastIntSet()
	for _, col := range p.Children()[0].Schema().Columns {
		outerCols.Insert(int(col.UniqueID))
	}
	for _, col := range p.Children()[1].Schema().Columns {
		innerCols.Insert(int(col.UniqueID))
	}
	if p.JoinType == base.RightOuterJoin {
		innerFD, outerFD = outerFD, innerFD
		innerCondition = p.LeftConditions
		outerCondition = p.RightConditions
		innerCols, outerCols = outerCols, innerCols
	}

	eqCondSlice := expression.ScalarFuncs2Exprs(p.EqualConditions)
	allConds := append(eqCondSlice, p.OtherConditions...)
	allConds = append(allConds, innerCondition...)
	allConds = append(allConds, outerCondition...)
	notNullColsFromFilters := util.ExtractNotNullFromConds(allConds, p)

	filterFD := &funcdep.FDSet{HashCodeToUniqueID: make(map[string]int)}

	constUniqueIDs := util.ExtractConstantCols(allConds, p.SCtx(), filterFD)

	equivUniqueIDs := util.ExtractEquivalenceCols(allConds, p.SCtx(), filterFD)

	filterFD.AddConstants(constUniqueIDs)
	equivOuterUniqueIDs := intset.NewFastIntSet()
	equivAcrossNum := 0
	// apply (left join)
	//   +--- child0 [t1.a]
	//   +--- project [correlated col{t1.a} --> col#9]
	// since project correlated col{t1.a} is equivalent to t1.a, we should maintain t1.a == col#9. since apply is
	// a left join, the probe side will append null value for the non-matched row, so we should maintain the equivalence
	// before the fds.MakeOuterJoin(). Even correlated col{t1.a} is not from outer side here, we could also maintain
	// the equivalence before the fds.MakeOuterJoin().
	equivUniqueIDs = append(equivUniqueIDs, equivFromApply...)
	for _, equiv := range equivUniqueIDs {
		filterFD.AddEquivalence(equiv[0], equiv[1])
		if equiv[0].SubsetOf(outerCols) && equiv[1].SubsetOf(innerCols) {
			equivOuterUniqueIDs.UnionWith(equiv[0])
			equivAcrossNum++
			continue
		}
		if equiv[0].SubsetOf(innerCols) && equiv[1].SubsetOf(outerCols) {
			equivOuterUniqueIDs.UnionWith(equiv[1])
			equivAcrossNum++
		}
	}
	filterFD.MakeNotNull(notNullColsFromFilters)

	// pre-perceive the filters for the convenience judgement of 3.3.1.
	var opt funcdep.ArgOpts
	if equivAcrossNum > 0 {
		// find the equivalence FD across left and right cols.
		outerConditionUniqueIDs := intset.NewFastIntSet()
		if len(outerCondition) != 0 {
			expression.ExtractColumnsSetFromExpressions(&outerConditionUniqueIDs, nil, outerCondition...)
		}
		if len(p.OtherConditions) != 0 {
			// other condition may contain right side cols, it doesn't affect the judgement of intersection of non-left-equiv cols.
			expression.ExtractColumnsSetFromExpressions(&outerConditionUniqueIDs, nil, p.OtherConditions...)
		}
		// judge whether left filters is on non-left-equiv cols.
		if outerConditionUniqueIDs.Intersects(outerCols.Difference(equivOuterUniqueIDs)) {
			opt.SkipFDRule331 = true
		}
	} else {
		// if there is none across equivalence condition, skip rule 3.3.1.
		opt.SkipFDRule331 = true
	}

	opt.OnlyInnerFilter = len(eqCondSlice) == 0 && len(outerCondition) == 0 && len(p.OtherConditions) == 0
	if opt.OnlyInnerFilter {
		// if one of the inner condition is constant false, the inner side are all null, left make constant all of that.
		for _, one := range innerCondition {
			if c, ok := one.(*expression.Constant); ok && c.DeferredExpr == nil && c.ParamMarker == nil {
				if isTrue, err := c.Value.ToBool(p.SCtx().GetSessionVars().StmtCtx.TypeCtx()); err == nil {
					if isTrue == 0 {
						// c is false
						opt.InnerIsFalse = true
					}
				}
			}
		}
	}

	fds := outerFD
	fds.MakeOuterJoin(innerFD, filterFD, outerCols, innerCols, &opt)
	p.SetFDs(fds)
	return fds
}

// GetJoinKeys extracts join keys(columns) from EqualConditions. It returns left join keys, right
// join keys and an `isNullEQ` array which means the `joinKey[i]` is a `NullEQ` function. The `hasNullEQ`
// means whether there is a `NullEQ` of a join key.
func (p *LogicalJoin) GetJoinKeys() (leftKeys, rightKeys []*expression.Column, isNullEQ []bool, hasNullEQ bool) {
	for _, expr := range p.EqualConditions {
		l, r := expression.ExtractColumnsFromColOpCol(expr)
		leftKeys = append(leftKeys, l)
		rightKeys = append(rightKeys, r)
		isNullEQ = append(isNullEQ, expr.FuncName.L == ast.NullEQ)
		hasNullEQ = hasNullEQ || expr.FuncName.L == ast.NullEQ
	}
	return
}

// GetNAJoinKeys extracts join keys(columns) from NAEqualCondition.
func (p *LogicalJoin) GetNAJoinKeys() (leftKeys, rightKeys []*expression.Column) {
	for _, expr := range p.NAEQConditions {
		l, r := expression.ExtractColumnsFromColOpCol(expr)
		leftKeys = append(leftKeys, l)
		rightKeys = append(rightKeys, r)
	}
	return
}

// GetPotentialPartitionKeys return potential partition keys for join, the potential partition keys are
// the join keys of EqualConditions
func (p *LogicalJoin) GetPotentialPartitionKeys() (leftKeys, rightKeys []*property.MPPPartitionColumn) {
	for _, expr := range p.EqualConditions {
		_, coll := expr.CharsetAndCollation()
		collateID := property.GetCollateIDByNameForPartition(coll)
		l, r := expression.ExtractColumnsFromColOpCol(expr)
		leftKeys = append(leftKeys, &property.MPPPartitionColumn{Col: l, CollateID: collateID})
		rightKeys = append(rightKeys, &property.MPPPartitionColumn{Col: r, CollateID: collateID})
	}
	return
}

// Decorrelate eliminate the correlated column with if the col is in schema.
func (p *LogicalJoin) Decorrelate(schema *expression.Schema) {
	for i, cond := range p.LeftConditions {
		p.LeftConditions[i] = cond.Decorrelate(schema)
	}
	for i, cond := range p.RightConditions {
		p.RightConditions[i] = cond.Decorrelate(schema)
	}
	for i, cond := range p.OtherConditions {
		p.OtherConditions[i] = cond.Decorrelate(schema)
	}
	for i, cond := range p.EqualConditions {
		p.EqualConditions[i] = cond.Decorrelate(schema).(*expression.ScalarFunction)
	}
}

// ColumnSubstituteAll is used in projection elimination in apply de-correlation.
// Substitutions for all conditions should be successful, otherwise, we should keep all conditions unchanged.
func (p *LogicalJoin) ColumnSubstituteAll(schema *expression.Schema, exprs []expression.Expression) (hasFail bool) {
	// make a copy of exprs for convenience of substitution (may change/partially change the expr tree)
	cpLeftConditions := make(expression.CNFExprs, len(p.LeftConditions))
	cpRightConditions := make(expression.CNFExprs, len(p.RightConditions))
	cpOtherConditions := make(expression.CNFExprs, len(p.OtherConditions))
	cpEqualConditions := make([]*expression.ScalarFunction, len(p.EqualConditions))
	copy(cpLeftConditions, p.LeftConditions)
	copy(cpRightConditions, p.RightConditions)
	copy(cpOtherConditions, p.OtherConditions)
	copy(cpEqualConditions, p.EqualConditions)

	exprCtx := p.SCtx().GetExprCtx()
	// try to substitute columns in these condition.
	for i, cond := range cpLeftConditions {
		if hasFail, cpLeftConditions[i] = expression.ColumnSubstituteAll(exprCtx, cond, schema, exprs); hasFail {
			return
		}
	}

	for i, cond := range cpRightConditions {
		if hasFail, cpRightConditions[i] = expression.ColumnSubstituteAll(exprCtx, cond, schema, exprs); hasFail {
			return
		}
	}

	for i, cond := range cpOtherConditions {
		if hasFail, cpOtherConditions[i] = expression.ColumnSubstituteAll(exprCtx, cond, schema, exprs); hasFail {
			return
		}
	}

	for i, cond := range cpEqualConditions {
		var tmp expression.Expression
		if hasFail, tmp = expression.ColumnSubstituteAll(exprCtx, cond, schema, exprs); hasFail {
			return
		}
		cpEqualConditions[i] = tmp.(*expression.ScalarFunction)
	}

	// if all substituted, change them atomically here.
	p.LeftConditions = cpLeftConditions
	p.RightConditions = cpRightConditions
	p.OtherConditions = cpOtherConditions
	p.EqualConditions = cpEqualConditions

	for i := len(p.EqualConditions) - 1; i >= 0; i-- {
		newCond := p.EqualConditions[i]

		// If the columns used in the new filter all come from the left child,
		// we can push this filter to it.
		if expression.ExprFromSchema(newCond, p.Children()[0].Schema()) {
			p.LeftConditions = append(p.LeftConditions, newCond)
			p.EqualConditions = slices.Delete(p.EqualConditions, i, i+1)
			continue
		}

		// If the columns used in the new filter all come from the right
		// child, we can push this filter to it.
		if expression.ExprFromSchema(newCond, p.Children()[1].Schema()) {
			p.RightConditions = append(p.RightConditions, newCond)
			p.EqualConditions = slices.Delete(p.EqualConditions, i, i+1)
			continue
		}
		_, _, ok := expression.IsColOpCol(newCond)
		// If the columns used in the new filter are not all expression.Column,
		// we can not use it as join's equal condition.
		if !ok {
			p.OtherConditions = append(p.OtherConditions, newCond)
			p.EqualConditions = slices.Delete(p.EqualConditions, i, i+1)
			continue
		}

		p.EqualConditions[i] = newCond
	}
	return false
}

// AttachOnConds extracts on conditions for join and set the `EqualConditions`, `LeftConditions`, `RightConditions` and
// `OtherConditions` by the result of extract.
func (p *LogicalJoin) AttachOnConds(onConds []expression.Expression) {
	eq, left, right, other := p.extractOnCondition(onConds, false, false)
	p.AppendJoinConds(eq, left, right, other)
}

// AppendJoinConds appends new join conditions.
func (p *LogicalJoin) AppendJoinConds(eq []*expression.ScalarFunction, left, right, other []expression.Expression) {
	p.EqualConditions = append(eq, p.EqualConditions...)
	p.LeftConditions = append(left, p.LeftConditions...)
	p.RightConditions = append(right, p.RightConditions...)
	p.OtherConditions = append(other, p.OtherConditions...)
}

func (p *LogicalJoin) isAllUniqueIDInTheSameLeaf(cond expression.Expression) bool {
	colset := slices.Collect(
		maps.Keys(expression.ExtractColumnsMapFromExpressions(nil, cond)),
	)
	if len(colset) == 1 {
		return true
	}

	for _, schema := range p.allJoinLeaf {
		inTheSameSchema := true
		// if we find a column in the table, the other is not in the same table.
		// They are impossible in the same table. we can directly return.
		findedSchema := false
		for _, unique := range colset {
			if !slices.ContainsFunc(schema.Columns, func(c *expression.Column) bool {
				return c.UniqueID == unique
			}) {
				inTheSameSchema = false
				break
			}
			findedSchema = true
		}
		if inTheSameSchema {
			return true
		} else if findedSchema {
			return false
		}
	}
	return false
}

// getAllJoinLeaf is to get all datasource's schema
func getAllJoinLeaf(plan base.LogicalPlan) []*expression.Schema {
	switch p := plan.(type) {
	case *DataSource, *LogicalAggregation, *LogicalProjection:
		// Because sometimes we put the output of the aggregation into the schema,
		// we can consider it as a new table.
		return []*expression.Schema{p.Schema()}
	default:
		result := make([]*expression.Schema, 0, len(p.Children()))
		for _, child := range p.Children() {
			result = append(result, getAllJoinLeaf(child)...)
		}
		return result
	}
}

// ExtractJoinKeys extract join keys as a schema for child with childIdx.
func (p *LogicalJoin) ExtractJoinKeys(childIdx int) *expression.Schema {
	joinKeys := make([]*expression.Column, 0, len(p.EqualConditions))
	for _, eqCond := range p.EqualConditions {
		joinKeys = append(joinKeys, eqCond.GetArgs()[childIdx].(*expression.Column))
	}
	return expression.NewSchema(joinKeys...)
}

// ExtractUsedCols extracts all the needed columns.
func (p *LogicalJoin) ExtractUsedCols(parentUsedCols []*expression.Column) (leftCols []*expression.Column, rightCols []*expression.Column) {
	for _, eqCond := range p.EqualConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(eqCond)...)
	}
	for _, leftCond := range p.LeftConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(leftCond)...)
	}
	for _, rightCond := range p.RightConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(rightCond)...)
	}
	for _, otherCond := range p.OtherConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(otherCond)...)
	}
	for _, naeqCond := range p.NAEQConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(naeqCond)...)
	}
	lChild := p.Children()[0]
	rChild := p.Children()[1]
	lSchema := lChild.Schema()
	rSchema := rChild.Schema()
	var lFullSchema, rFullSchema *expression.Schema
	// parentused col = t2.a
	// leftChild schema = t1.a(t2.a) + and others
	// rightChild schema = t3 related + and others
	if join, ok := lChild.(*LogicalJoin); ok {
		lFullSchema = join.FullSchema
	}
	if join, ok := rChild.(*LogicalJoin); ok {
		rFullSchema = join.FullSchema
	}
	for _, col := range parentUsedCols {
		if (lSchema != nil && lSchema.Contains(col)) ||
			(lFullSchema != nil && lFullSchema.Contains(col)) {
			leftCols = append(leftCols, col)
		} else if (rSchema != nil && rSchema.Contains(col)) ||
			(rFullSchema != nil && rFullSchema.Contains(col)) {
			rightCols = append(rightCols, col)
		}
	}
	return leftCols, rightCols
}

// MergeSchema merge the schema of left and right child of join.
func (p *LogicalJoin) MergeSchema() {
	p.SetSchema(BuildLogicalJoinSchema(p.JoinType, p))
}

// pushDownTopNToChild will push a topN to one child of join. The idx stands for join child index. 0 is for left child.
// When it's outer join and there's unique key information. The TopN can be totally pushed down to the join.
// We just need reserve the ORDER informaion
func (p *LogicalJoin) pushDownTopNToChild(topN *LogicalTopN, idx int) (base.LogicalPlan, bool) {
	if topN == nil {
		return p.Children()[idx].PushDownTopN(nil), false
	}

	for _, by := range topN.ByItems {
		cols := expression.ExtractColumns(by.Expr)
		for _, col := range cols {
			if !p.Children()[idx].Schema().Contains(col) {
				return p.Children()[idx].PushDownTopN(nil), false
			}
		}
	}
	count, offset := topN.Count+topN.Offset, uint64(0)
	selfEliminated := false
	if p.JoinType == base.LeftOuterJoin {
		innerChild := p.Children()[1]
		innerJoinKey := make([]*expression.Column, 0, len(p.EqualConditions))
		isNullEQ := false
		for _, eqCond := range p.EqualConditions {
			innerJoinKey = append(innerJoinKey, eqCond.GetArgs()[1].(*expression.Column))
			if eqCond.FuncName.L == ast.NullEQ {
				isNullEQ = true
			}
		}
		// If it's unique key(unique with not null), we can push the offset down safely whatever the join key is normal eq or nulleq.
		// If the join key is nulleq, then we can only push the offset down when the inner side is unique key.
		// Only when the join key is normal eq, we can push the offset down when the inner side is unique(could be null).
		if innerChild.Schema().IsUnique(true, innerJoinKey...) ||
			(!isNullEQ && innerChild.Schema().IsUnique(false, innerJoinKey...)) {
			count, offset = topN.Count, topN.Offset
			selfEliminated = true
		}
	} else if p.JoinType == base.RightOuterJoin {
		innerChild := p.Children()[0]
		innerJoinKey := make([]*expression.Column, 0, len(p.EqualConditions))
		isNullEQ := false
		for _, eqCond := range p.EqualConditions {
			innerJoinKey = append(innerJoinKey, eqCond.GetArgs()[0].(*expression.Column))
			if eqCond.FuncName.L == ast.NullEQ {
				isNullEQ = true
			}
		}
		if innerChild.Schema().IsUnique(true, innerJoinKey...) ||
			(!isNullEQ && innerChild.Schema().IsUnique(false, innerJoinKey...)) {
			count, offset = topN.Count, topN.Offset
			selfEliminated = true
		}
	}

	newTopN := LogicalTopN{
		Count:            count,
		Offset:           offset,
		ByItems:          make([]*util.ByItems, len(topN.ByItems)),
		PreferLimitToCop: topN.PreferLimitToCop,
	}.Init(topN.SCtx(), topN.QueryBlockOffset())
	for i := range topN.ByItems {
		newTopN.ByItems[i] = topN.ByItems[i].Clone()
	}
	return p.Children()[idx].PushDownTopN(newTopN), selfEliminated
}

// Add a new selection between parent plan and current plan with candidate predicates
/*
+-------------+                                    +-------------+
| parentPlan  |                                    | parentPlan  |
+-----^-------+                                    +-----^-------+
      |           --addCandidateSelection--->            |
+-----+-------+                              +-----------+--------------+
| currentPlan |                              |        selection         |
+-------------+                              |   candidate predicate    |
                                             +-----------^--------------+
                                                         |
                                                         |
                                                    +----+--------+
                                                    | currentPlan |
                                                    +-------------+
*/
// If the currentPlan at the top of query plan, return new root plan (selection)
// Else return nil
func addCandidateSelection(currentPlan base.LogicalPlan, currentChildIdx int, parentPlan base.LogicalPlan,
	candidatePredicates []expression.Expression) (newRoot base.LogicalPlan) {
	// generate a new selection for candidatePredicates
	selection := LogicalSelection{Conditions: candidatePredicates}.Init(currentPlan.SCtx(), currentPlan.QueryBlockOffset())
	// add selection above of p
	if parentPlan == nil {
		newRoot = selection
	} else {
		parentPlan.SetChild(currentChildIdx, selection)
	}
	selection.SetChildren(currentPlan)
	if parentPlan == nil {
		return newRoot
	}
	return nil
}
