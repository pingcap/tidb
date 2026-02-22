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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/property"
	h "github.com/pingcap/tidb/pkg/util/hint"
)

func (la *LogicalAggregation) HasDistinct() bool {
	for _, aggFunc := range la.AggFuncs {
		if aggFunc.HasDistinct {
			return true
		}
	}
	return false
}

// HasOrderBy shows whether LogicalAggregation has functions with order-by items.
func (la *LogicalAggregation) HasOrderBy() bool {
	for _, aggFunc := range la.AggFuncs {
		if len(aggFunc.OrderByItems) > 0 {
			return true
		}
	}
	return false
}

// CopyAggHints copies the aggHints from another LogicalAggregation.
func (la *LogicalAggregation) CopyAggHints(agg *LogicalAggregation) {
	// TODO: Copy the hint may make the un-applicable hint throw the
	// same warning message more than once. We'd better add a flag for
	// `HaveThrownWarningMessage` to avoid this. Besides, finalAgg and
	// partialAgg (in cascades planner) should share the same hint, instead
	// of a copy.
	la.PreferAggType = agg.PreferAggType
	la.PreferAggToCop = agg.PreferAggToCop
}

// IsPartialModeAgg returns if all of the AggFuncs are partialMode.
func (la *LogicalAggregation) IsPartialModeAgg() bool {
	// Since all of the AggFunc share the same AggMode, we only need to check the first one.
	return la.AggFuncs[0].Mode == aggregation.Partial1Mode
}

// IsCompleteModeAgg returns if all of the AggFuncs are CompleteMode.
func (la *LogicalAggregation) IsCompleteModeAgg() bool {
	// Since all of the AggFunc share the same AggMode, we only need to check the first one.
	return la.AggFuncs[0].Mode == aggregation.CompleteMode
}

// GetGroupByCols returns the columns that are group-by items.
// For example, `group by a, b, c+d` will return [a, b].
func (la *LogicalAggregation) GetGroupByCols() []*expression.Column {
	groupByCols := make([]*expression.Column, 0, len(la.GroupByItems))
	for _, item := range la.GroupByItems {
		if col, ok := item.(*expression.Column); ok {
			groupByCols = append(groupByCols, col)
		}
	}
	return groupByCols
}

// GetPotentialPartitionKeys return potential partition keys for aggregation, the potential partition keys are the group by keys
func (la *LogicalAggregation) GetPotentialPartitionKeys() []*property.MPPPartitionColumn {
	groupByCols := make([]*property.MPPPartitionColumn, 0, len(la.GroupByItems))
	for _, item := range la.GroupByItems {
		if col, ok := item.(*expression.Column); ok {
			groupByCols = append(groupByCols, &property.MPPPartitionColumn{
				Col:       col,
				CollateID: property.GetCollateIDByNameForPartition(col.GetStaticType().GetCollate()),
			})
		}
	}
	return groupByCols
}

// GetUsedCols extracts all of the Columns used by agg including GroupByItems and AggFuncs.
func (la *LogicalAggregation) GetUsedCols() (usedCols []*expression.Column) {
	for _, groupByItem := range la.GroupByItems {
		usedCols = append(usedCols, expression.ExtractColumns(groupByItem)...)
	}
	for _, aggDesc := range la.AggFuncs {
		for _, expr := range aggDesc.Args {
			usedCols = append(usedCols, expression.ExtractColumns(expr)...)
		}
		for _, expr := range aggDesc.OrderByItems {
			usedCols = append(usedCols, expression.ExtractColumns(expr.Expr)...)
		}
	}
	return usedCols
}

// ResetHintIfConflicted resets the PreferAggType if they are conflicted,
// and returns the two PreferAggType hints.
func (la *LogicalAggregation) ResetHintIfConflicted() (preferHash bool, preferStream bool) {
	preferHash = (la.PreferAggType & h.PreferHashAgg) > 0
	preferStream = (la.PreferAggType & h.PreferStreamAgg) > 0
	if preferHash && preferStream {
		la.SCtx().GetSessionVars().StmtCtx.SetHintWarning("Optimizer aggregation hints are conflicted")
		la.PreferAggType = 0
		preferHash, preferStream = false, false
	}
	return
}

// DistinctArgsMeetsProperty checks if the distinct args meet the property.
func (la *LogicalAggregation) DistinctArgsMeetsProperty() bool {
	for _, aggFunc := range la.AggFuncs {
		if aggFunc.HasDistinct {
			for _, distinctArg := range aggFunc.Args {
				if !expression.Contains(la.SCtx().GetExprCtx().GetEvalCtx(), la.GroupByItems, distinctArg) {
					return false
				}
			}
		}
	}
	return true
}

// pushDownPredicatesForAggregation split a CNF condition to two parts, can be pushed-down or can not be pushed-down below aggregation.
// It would consider the CNF.
// For example,
// (a > 1 or avg(b) > 1) and (a < 3), and `avg(b) > 1` can't be pushed-down.
// Then condsToPush: a < 3, ret: a > 1 or avg(b) > 1
func (la *LogicalAggregation) pushDownCNFPredicatesForAggregation(cond expression.Expression, groupByColumns *expression.Schema, exprsOriginal []expression.Expression, pushDownFunc pushDownPredicatesFunc) (condsToPush, ret []expression.Expression) {
	subCNFItem := expression.SplitCNFItems(cond)
	if len(subCNFItem) == 1 {
		return la.pushDownPredicates(subCNFItem[0], groupByColumns, exprsOriginal, pushDownFunc)
	}
	exprCtx := la.SCtx().GetExprCtx()
	for _, item := range subCNFItem {
		condsToPushForItem, retForItem := la.pushDownDNFPredicates(item, groupByColumns, exprsOriginal, pushDownFunc)
		if len(condsToPushForItem) > 0 {
			condsToPush = append(condsToPush, expression.ComposeDNFCondition(exprCtx, condsToPushForItem...))
		}
		if len(retForItem) > 0 {
			ret = append(ret, expression.ComposeDNFCondition(exprCtx, retForItem...))
		}
	}
	return condsToPush, ret
}

// pushDownDNFPredicates split a DNF condition to two parts, can be pushed-down or can not be pushed-down below aggregation.
// It would consider the DNF.
// For example,
// (a > 1 and avg(b) > 1) or (a < 3), and `avg(b) > 1` can't be pushed-down.
// Then condsToPush: (a < 3) and (a > 1), ret: (a > 1 and avg(b) > 1) or (a < 3)
func (la *LogicalAggregation) pushDownDNFPredicates(cond expression.Expression, groupByColumns *expression.Schema, exprsOriginal []expression.Expression, pushDownFunc pushDownPredicatesFunc) (_, _ []expression.Expression) {
	subDNFItem := expression.SplitDNFItems(cond)
	if len(subDNFItem) == 1 {
		return la.pushDownPredicates(subDNFItem[0], groupByColumns, exprsOriginal, pushDownFunc)
	}
	condsToPush := make([]expression.Expression, 0, len(subDNFItem))
	var ret []expression.Expression
	exprCtx := la.SCtx().GetExprCtx()
	for _, item := range subDNFItem {
		condsToPushForItem, retForItem := la.pushDownCNFPredicatesForAggregation(item, groupByColumns, exprsOriginal, pushDownFunc)
		if len(condsToPushForItem) <= 0 {
			return nil, []expression.Expression{cond}
		}
		condsToPush = append(condsToPush, expression.ComposeCNFCondition(exprCtx, condsToPushForItem...))
		if len(retForItem) > 0 {
			ret = append(ret, expression.ComposeCNFCondition(exprCtx, retForItem...))
		}
	}
	if len(ret) == 0 {
		// All the condition can be pushed down.
		return []expression.Expression{cond}, nil
	}
	dnfPushDownCond := expression.ComposeDNFCondition(exprCtx, condsToPush...)
	// Some condition can't be pushed down, we need to keep all the condition.
	return []expression.Expression{dnfPushDownCond}, []expression.Expression{cond}
}

// splitCondForAggregation splits the condition into those who can be pushed and others.
func (la *LogicalAggregation) splitCondForAggregation(predicates []expression.Expression) (condsToPush, ret []expression.Expression) {
	exprsOriginal := make([]expression.Expression, 0, len(la.AggFuncs))
	for _, fun := range la.AggFuncs {
		exprsOriginal = append(exprsOriginal, fun.Args[0])
	}
	groupByColumns := expression.NewSchema(la.GetGroupByCols()...)
	aggFirstRowColumns := expression.NewSchema(la.getAggFuncsColsForFirstRow()...)
	// It's almost the same as pushDownCNFPredicatesForAggregation, except that the condition is a slice.
	for _, cond := range predicates {
		subCondsToPush, subRet := la.pushDownDNFPredicates(cond, groupByColumns, exprsOriginal, la.pushDownPredicatesByGroupby)
		if len(subCondsToPush) > 0 {
			condsToPush = append(condsToPush, subCondsToPush...)
		}
		if len(subRet) > 0 {
			// If we cannot find columns that can be pushed down in the GROUP BY clause,
			// we will then look for columns that can be pushed down in the aggregate functions.
			// Currently, only the first row is supported.
			for _, s := range subRet {
				subCondsToPush1, subRet1 := la.pushDownDNFPredicates(s, aggFirstRowColumns, exprsOriginal, la.pushDownPredicatesByAggFuncs)
				if len(subCondsToPush1) > 0 {
					condsToPush = append(condsToPush, subCondsToPush1...)
				}
				ret = append(ret, subRet1...)
			}
		}
	}
	return condsToPush, ret
}

// getAggFuncsColsForFirstRow gets the columns that are used by first_row agg functions.
func (la *LogicalAggregation) getAggFuncsColsForFirstRow() (aggFuncsCols []*expression.Column) {
	aggFuncsCols = make([]*expression.Column, 0, len(la.AggFuncs))
	for idx, col := range la.Schema().Columns {
		aggFunc := la.AggFuncs[idx]
		if aggFunc.Name == ast.AggFuncFirstRow {
			cols := expression.ExtractColumns(aggFunc.Args[0])
			if len(cols) == 1 {
				aggFuncsCols = append(aggFuncsCols, col)
			}
		}
	}
	return aggFuncsCols
}

// pushDownPredicates split a condition to two parts, can be pushed-down or can not be pushed-down below aggregation.
func (*LogicalAggregation) pushDownPredicates(cond expression.Expression, groupByColumns *expression.Schema, exprsOriginal []expression.Expression, pushDownFunc pushDownPredicatesFunc) (condsToPush, ret []expression.Expression) {
	switch c := cond.(type) {
	case *expression.Constant:
		condsToPush = append(condsToPush, cond)
		// Consider SQL list "select sum(b) from t group by a having 1=0". "1=0" is a constant predicate which should be
		// retained and pushed down at the same time. Because we will get a wrong query result that contains one column
		// with value 0 rather than an empty query result.
		ret = append(ret, cond)
	case *expression.ScalarFunction:
		return pushDownFunc(c, groupByColumns, exprsOriginal)
	default:
		ret = append(ret, cond)
	}
	return condsToPush, ret
}

type pushDownPredicatesFunc func(cond *expression.ScalarFunction, cols *expression.Schema, exprsOriginal []expression.Expression) (_, _ []expression.Expression)

func (la *LogicalAggregation) pushDownPredicatesByGroupby(cond *expression.ScalarFunction, groupByColumns *expression.Schema, exprsOriginal []expression.Expression) (condsToPush, ret []expression.Expression) {
	extractedCols := expression.ExtractColumns(cond)
	ok := true
	for _, col := range extractedCols {
		if !groupByColumns.Contains(col) {
			ok = false
			break
		}
	}
	if ok {
		newFunc := expression.ColumnSubstitute(la.SCtx().GetExprCtx(), cond, la.Schema(), exprsOriginal)
		condsToPush = append(condsToPush, newFunc)
	} else {
		ret = append(ret, cond)
	}
	return
}

// pushDownPredicatesByAggFuncs is only used for firstrow
func (la *LogicalAggregation) pushDownPredicatesByAggFuncs(cond *expression.ScalarFunction, aggFirstRowColumns *expression.Schema, exprsOriginal []expression.Expression) (condsToPush, ret []expression.Expression) {
	extractedCols := expression.ExtractColumnsMapFromExpressions(nil, cond)
	if len(extractedCols) != 1 || len(aggFirstRowColumns.Columns) == 0 {
		ret = append(ret, cond)
		return condsToPush, ret
	}
	var schemaCol *expression.Column
	// `aggFirstRowColumns` are the output columns of the first row function.
	// We compare the columns in the push-down predicates with the output columns of the first row.
	// The columns that match are the ones that can be pushed down.
	// The length of extractedCols is 1, so this loop will only run once.
	for _, col := range extractedCols {
		schemaCol = aggFirstRowColumns.RetrieveColumn(col)
	}
	if schemaCol != nil {
		newFunc := expression.ColumnSubstitute(la.SCtx().GetExprCtx(), cond, la.Schema(), exprsOriginal)
		condsToPush = append(condsToPush, newFunc)
	} else {
		ret = append(ret, cond)
	}
	return condsToPush, ret
}

// BuildSelfKeyInfo builds the key information for the aggregation itself.
func (la *LogicalAggregation) BuildSelfKeyInfo(selfSchema *expression.Schema) {
	groupByCols := la.GetGroupByCols()
	if len(groupByCols) == len(la.GroupByItems) && len(la.GroupByItems) > 0 {
		indices := selfSchema.ColumnsIndices(groupByCols)
		if indices != nil {
			newKey := make([]*expression.Column, 0, len(indices))
			for _, i := range indices {
				newKey = append(newKey, selfSchema.Columns[i])
			}
			selfSchema.PKOrUK = append(selfSchema.PKOrUK, newKey)
		}
	}
	if len(la.GroupByItems) == 0 {
		la.SetMaxOneRow(true)
	}
}

// CanPullUp checks if an aggregation can be pulled up. An aggregate function like count(*) cannot be pulled up.
func (la *LogicalAggregation) CanPullUp() bool {
	if len(la.GroupByItems) > 0 {
		return false
	}
	for _, f := range la.AggFuncs {
		for _, arg := range f.Args {
			expr, err := expression.EvaluateExprWithNull(la.SCtx().GetExprCtx(), la.Children()[0].Schema(), arg, true)
			if err != nil {
				return false
			}
			if con, ok := expr.(*expression.Constant); !ok || !con.Value.IsNull() {
				return false
			}
		}
	}
	return true
}

func (*LogicalAggregation) getGroupNDVs(childProfile *property.StatsInfo, gbyCols []*expression.Column) []property.GroupNDV {
	// now, both the way, we do maintain the group ndv bottom up: colGroups is not 0, or we are in memo.
	//
	// Check if the child profile provides GroupNDV for the GROUP BY columns.
	// Note that gbyCols may not be the exact GROUP BY columns, e.g, GROUP BY a+b,
	// but we have no other approaches for the NDV estimation of these cases
	// except for using the independent assumption, unless we can use stats of expression index.
	groupNDV := childProfile.GetGroupNDV4Cols(gbyCols)
	if groupNDV == nil {
		return nil
	}
	return []property.GroupNDV{*groupNDV}
}
