// Copyright 2022 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/util/intset"
)

type skewDistinctAggRewriter struct {
}

// skewDistinctAggRewriter will rewrite group distinct aggregate into 2 level aggregates, e.g.:
//
//	select S_NATIONKEY as s, count(S_SUPPKEY), count(distinct S_NAME) from supplier group by s;
//
// will be rewritten to
//
//	select S_NATIONKEY as s, sum(c), count(S_NAME) from (
//	  select S_NATIONKEY, S_NAME, count(S_SUPPKEY) c from supplier group by S_NATIONKEY, S_NAME
//	) as T group by s;
//
// If the group key is highly skewed and the distinct key has large number of distinct values
// (a.k.a. high cardinality), the query execution will be slow. This rule may help to ease the
// skew issue.
//
// The rewrite rule only applies to query that satisfies:
// - The aggregate has at least 1 group by column (the group key can be columns or expressions)
// - The aggregate has 1 and only 1 distinct aggregate function (limited to count, avg, sum)
//
// This rule is disabled by default. Use tidb_opt_skew_distinct_agg to enable the rule.
func (a *skewDistinctAggRewriter) rewriteSkewDistinctAgg(agg *LogicalAggregation, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	// only group aggregate is applicable
	if len(agg.GroupByItems) == 0 {
		return nil
	}

	// the number of distinct aggregate functions
	nDistinct := 0
	// distinct columns collected from original aggregate
	distinctCols := make([]expression.Expression, 0, 3 /* arbitrary value*/)

	for _, aggFunc := range agg.AggFuncs {
		if aggFunc.HasDistinct {
			nDistinct++
			distinctCols = append(distinctCols, aggFunc.Args...)
		}
		// TODO: support multiple DQA on same column, e.g. count(distinct x), sum(distinct x)
		if nDistinct > 1 {
			return nil
		}
		if !a.isQualifiedAgg(aggFunc) {
			return nil
		}
	}

	// we only deal with single distinct aggregate for now, no more, no less
	if nDistinct != 1 {
		return nil
	}

	// count(distinct a,b,c) group by d
	// will generate a bottom agg with group by a,b,c,d
	bottomAggGroupbyItems := make([]expression.Expression, 0, len(agg.GroupByItems)+len(distinctCols))
	bottomAggGroupbyItems = append(bottomAggGroupbyItems, agg.GroupByItems...)
	bottomAggGroupbyItems = append(bottomAggGroupbyItems, distinctCols...)

	// aggregate functions for top aggregate
	topAggFuncs := make([]*aggregation.AggFuncDesc, 0, len(agg.AggFuncs))
	// aggregate functions for bottom aggregate
	bottomAggFuncs := make([]*aggregation.AggFuncDesc, 0, len(agg.AggFuncs))
	// output schema for top aggregate
	topAggSchema := agg.schema.Clone()
	// output schema for bottom aggregate
	bottomAggSchema := expression.NewSchema(make([]*expression.Column, 0, agg.schema.Len())...)

	// columns used by group by items in the original aggregate
	groupCols := make([]*expression.Column, 0, 3)
	// columns that should be used by firstrow(), which will be appended to
	// bottomAgg schema and aggregate functions
	firstRowCols := intset.NewFastIntSet()
	for _, groupByItem := range agg.GroupByItems {
		usedCols := expression.ExtractColumns(groupByItem)
		groupCols = append(groupCols, usedCols...)
		for _, col := range usedCols {
			firstRowCols.Insert(int(col.UniqueID))
		}
	}

	// we only care about non-distinct count() agg function
	cntIndexes := make([]int, 0, 3)

	// now decompose original aggregate functions into 2 level aggregate functions,
	// except distinct function. each agg function is in COMPLETE mode.
	for i, aggFunc := range agg.AggFuncs {
		// have to clone it to avoid unexpected modification by others, (︶︹︺)
		newAggFunc := aggFunc.Clone()
		if aggFunc.HasDistinct {
			// TODO: support count(distinct a,b,c)
			if len(aggFunc.Args) != 1 {
				return nil
			}

			for _, arg := range aggFunc.Args {
				firstRow, err := aggregation.NewAggFuncDesc(agg.SCtx().GetExprCtx(), ast.AggFuncFirstRow,
					[]expression.Expression{arg}, false)
				if err != nil {
					return nil
				}
				bottomAggFuncs = append(bottomAggFuncs, firstRow)
				bottomAggSchema.Append(arg.(*expression.Column))
			}

			// now the distinct is not needed anymore
			newAggFunc.HasDistinct = false
			topAggFuncs = append(topAggFuncs, newAggFunc)
		} else {
			// only count() will be decomposed to sum() + count(), the others will keep same
			// original aggregate functions will go to bottom aggregate without any change
			bottomAggFuncs = append(bottomAggFuncs, newAggFunc)

			// cast to Column, if failed, we know it is Constant, ignore the error message,
			// we will later create a new schema column
			aggCol, ok := newAggFunc.Args[0].(*expression.Column)

			// firstrow() doesn't change the input value and type
			if newAggFunc.Name == ast.AggFuncFirstRow {
				if ok {
					firstRowCols.Remove(int(aggCol.UniqueID))
				}
			} else {
				aggCol = &expression.Column{
					UniqueID: agg.SCtx().GetSessionVars().AllocPlanColumnID(),
					RetType:  newAggFunc.RetTp,
				}
			}
			bottomAggSchema.Append(aggCol)

			if newAggFunc.Name == ast.AggFuncCount {
				cntIndexes = append(cntIndexes, i)
				sumAggFunc, err := aggregation.NewAggFuncDesc(agg.SCtx().GetExprCtx(), ast.AggFuncSum,
					[]expression.Expression{aggCol}, false)
				if err != nil {
					return nil
				}
				topAggFuncs = append(topAggFuncs, sumAggFunc)
				topAggSchema.Columns[i] = &expression.Column{
					UniqueID: agg.SCtx().GetSessionVars().AllocPlanColumnID(),
					RetType:  sumAggFunc.RetTp,
				}
			} else {
				topAggFunc := aggFunc.Clone()
				topAggFunc.Args = make([]expression.Expression, 0, len(aggFunc.Args))
				topAggFunc.Args = append(topAggFunc.Args, aggCol)
				topAggFuncs = append(topAggFuncs, topAggFunc)
			}
		}
	}

	for _, col := range groupCols {
		// the col is used by GROUP BY clause, but not in the output schema, e.g.
		// SELECT count(DISTINCT a) FROM t GROUP BY b;
		// column b is not in the output schema, we have to add it to the bottom agg schema
		if firstRowCols.Has(int(col.UniqueID)) {
			firstRow, err := aggregation.NewAggFuncDesc(agg.SCtx().GetExprCtx(), ast.AggFuncFirstRow,
				[]expression.Expression{col}, false)
			if err != nil {
				return nil
			}
			bottomAggFuncs = append(bottomAggFuncs, firstRow)
			bottomAggSchema.Append(col)
		}
	}

	// now create the bottom and top aggregate operators
	bottomAgg := LogicalAggregation{
		AggFuncs:      bottomAggFuncs,
		GroupByItems:  bottomAggGroupbyItems,
		PreferAggType: agg.PreferAggType,
	}.Init(agg.SCtx(), agg.QueryBlockOffset())
	bottomAgg.SetChildren(agg.Children()...)
	bottomAgg.SetSchema(bottomAggSchema)

	topAgg := LogicalAggregation{
		AggFuncs:       topAggFuncs,
		GroupByItems:   agg.GroupByItems,
		PreferAggToCop: agg.PreferAggToCop,
	}.Init(agg.SCtx(), agg.QueryBlockOffset())
	topAgg.SetChildren(bottomAgg)
	topAgg.SetSchema(topAggSchema)

	if len(cntIndexes) == 0 {
		appendSkewDistinctAggRewriteTraceStep(agg, topAgg, opt)
		return topAgg
	}

	// it has count(), we have split it into sum()+count(), since sum() returns decimal
	// we have to return a project operator that casts decimal to bigint
	proj := LogicalProjection{
		Exprs: make([]expression.Expression, 0, len(agg.AggFuncs)),
	}.Init(agg.SCtx(), agg.QueryBlockOffset())
	for _, column := range topAggSchema.Columns {
		proj.Exprs = append(proj.Exprs, column.Clone())
	}

	// wrap sum() with cast function to keep output data type same
	for _, index := range cntIndexes {
		exprType := proj.Exprs[index].GetType(agg.SCtx().GetExprCtx().GetEvalCtx())
		targetType := agg.schema.Columns[index].GetStaticType()
		if !exprType.Equal(targetType) {
			proj.Exprs[index] = expression.BuildCastFunction(agg.SCtx().GetExprCtx(), proj.Exprs[index], targetType)
		}
	}
	proj.SetSchema(agg.schema.Clone())
	proj.SetChildren(topAgg)
	appendSkewDistinctAggRewriteTraceStep(agg, proj, opt)
	return proj
}

func (*skewDistinctAggRewriter) isQualifiedAgg(aggFunc *aggregation.AggFuncDesc) bool {
	if aggFunc.Mode != aggregation.CompleteMode {
		return false
	}
	if len(aggFunc.OrderByItems) > 0 || len(aggFunc.Args) > 1 {
		return false
	}

	for _, arg := range aggFunc.Args {
		if _, ok := arg.(*expression.Column); !ok {
			if _, ok := arg.(*expression.Constant); !ok {
				return false
			}
		}
	}

	switch aggFunc.Name {
	case ast.AggFuncFirstRow, ast.AggFuncCount, ast.AggFuncSum, ast.AggFuncMax, ast.AggFuncMin:
		return true
	case ast.AggFuncBitAnd, ast.AggFuncBitOr, ast.AggFuncBitXor:
		return false
	case ast.AggFuncAvg:
		return aggFunc.HasDistinct
	default:
		return false
	}
}

func appendSkewDistinctAggRewriteTraceStep(agg *LogicalAggregation, result base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) {
	reason := func() string {
		return fmt.Sprintf("%v_%v has a distinct agg function", agg.TP(), agg.ID())
	}
	action := func() string {
		return fmt.Sprintf("%v_%v is rewritten to a %v_%v", agg.TP(), agg.ID(), result.TP(), result.ID())
	}

	opt.AppendStepToCurrent(agg.ID(), agg.TP(), reason, action)
}

func (a *skewDistinctAggRewriter) optimize(ctx context.Context, p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	newChildren := make([]base.LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild, planChanged, err := a.optimize(ctx, child, opt)
		if err != nil {
			return nil, planChanged, err
		}
		newChildren = append(newChildren, newChild)
	}
	p.SetChildren(newChildren...)
	agg, ok := p.(*LogicalAggregation)
	if !ok {
		return p, planChanged, nil
	}
	if newAgg := a.rewriteSkewDistinctAgg(agg, opt); newAgg != nil {
		return newAgg, planChanged, nil
	}
	return p, planChanged, nil
}

func (*skewDistinctAggRewriter) name() string {
	return "skew_distinct_agg_rewrite"
}
