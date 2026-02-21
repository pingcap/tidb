// Copyright 2025 PingCAP, Inc.
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

package physicalop

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
)

// AggInfo stores the information of an Aggregation.
type AggInfo struct {
	AggFuncs     []*aggregation.AggFuncDesc
	GroupByItems []expression.Expression
	Schema       *expression.Schema
}

// BuildFinalModeAggregation splits either LogicalAggregation or PhysicalAggregation to finalAgg and partial1Agg,
// returns the information of partial and final agg.
// partialIsCop means whether partial agg is a cop task. When partialIsCop is false,
// we do not set the AggMode for partialAgg cause it may be split further when
// building the aggregate executor(e.g. buildHashAgg will split the AggDesc further for parallel executing).
// firstRowFuncMap is a map between partial first_row to final first_row, will be used in RemoveUnnecessaryFirstRow
func BuildFinalModeAggregation(
	sctx base.PlanContext, original *AggInfo, partialIsCop bool, isMPPTask bool) (partial, final *AggInfo, firstRowFuncMap map[*aggregation.AggFuncDesc]*aggregation.AggFuncDesc) {
	ectx := sctx.GetExprCtx().GetEvalCtx()

	firstRowFuncMap = make(map[*aggregation.AggFuncDesc]*aggregation.AggFuncDesc, len(original.AggFuncs))
	partial = &AggInfo{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, len(original.AggFuncs)),
		GroupByItems: original.GroupByItems,
		Schema:       expression.NewSchema(),
	}
	partialCursor := 0
	final = &AggInfo{
		AggFuncs:     make([]*aggregation.AggFuncDesc, len(original.AggFuncs)),
		GroupByItems: make([]expression.Expression, 0, len(original.GroupByItems)),
		Schema:       original.Schema,
	}

	partialGbySchema := expression.NewSchema()
	// add group by columns
	for _, gbyExpr := range partial.GroupByItems {
		var gbyCol *expression.Column
		if col, ok := gbyExpr.(*expression.Column); ok {
			gbyCol = col
		} else {
			gbyCol = &expression.Column{
				UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  gbyExpr.GetType(ectx),
			}
		}
		partialGbySchema.Append(gbyCol)
		final.GroupByItems = append(final.GroupByItems, gbyCol)
	}

	// TODO: Refactor the way of constructing aggregation functions.
	// This for loop is ugly, but I do not find a proper way to reconstruct
	// it right away.

	// group_concat is special when pushing down, it cannot take the two phase execution if no distinct but with orderBy, and other cases are also different:
	// for example: group_concat([distinct] expr0, expr1[, order by expr2] separator ‘,’)
	// no distinct, no orderBy: can two phase
	// 		[final agg] group_concat(col#1,’,’)
	// 		[part  agg] group_concat(expr0, expr1,’,’) -> col#1
	// no distinct,  orderBy: only one phase
	// distinct, no orderBy: can two phase
	// 		[final agg] group_concat(distinct col#0, col#1,’,’)
	// 		[part  agg] group by expr0 ->col#0, expr1 -> col#1
	// distinct,  orderBy: can two phase
	// 		[final agg] group_concat(distinct col#0, col#1, order by col#2,’,’)
	// 		[part  agg] group by expr0 ->col#0, expr1 -> col#1; agg function: firstrow(expr2)-> col#2

	for i, aggFunc := range original.AggFuncs {
		finalAggFunc := &aggregation.AggFuncDesc{HasDistinct: false}
		finalAggFunc.Name = aggFunc.Name
		finalAggFunc.OrderByItems = aggFunc.OrderByItems
		args := make([]expression.Expression, 0, len(aggFunc.Args))
		if aggFunc.HasDistinct {
			/*
				eg: SELECT COUNT(DISTINCT a), SUM(b) FROM t GROUP BY c

				change from
					[root] group by: c, funcs:count(distinct a), funcs:sum(b)
				to
					[root] group by: c, funcs:count(distinct a), funcs:sum(b)
						[cop]: group by: c, a
			*/
			// onlyAddFirstRow means if the distinctArg does not occur in group by items,
			// it should be replaced with a firstrow() agg function, needed for the order by items of group_concat()
			getDistinctExpr := func(distinctArg expression.Expression, onlyAddFirstRow bool) (ret expression.Expression) {
				// 1. add all args to partial.GroupByItems
				foundInGroupBy := false
				for j, gbyExpr := range partial.GroupByItems {
					if gbyExpr.Equal(ectx, distinctArg) && gbyExpr.GetType(ectx).Equal(distinctArg.GetType(ectx)) {
						// if the two expressions exactly the same in terms of data types and collation, then can avoid it.
						foundInGroupBy = true
						ret = partialGbySchema.Columns[j]
						break
					}
				}
				if !foundInGroupBy {
					var gbyCol *expression.Column
					if col, ok := distinctArg.(*expression.Column); ok {
						gbyCol = col
					} else {
						gbyCol = &expression.Column{
							UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
							RetType:  distinctArg.GetType(ectx),
						}
					}
					// 2. add group by items if needed
					if !onlyAddFirstRow {
						partial.GroupByItems = append(partial.GroupByItems, distinctArg)
						partialGbySchema.Append(gbyCol)
						ret = gbyCol
					}
					// 3. add firstrow() if needed
					if !partialIsCop || onlyAddFirstRow {
						// if partial is a cop task, firstrow function is redundant since group by items are outputted
						// by group by schema, and final functions use group by schema as their arguments.
						// if partial agg is not cop, we must append firstrow function & schema, to output the group by
						// items.
						// maybe we can unify them sometime.
						// only add firstrow for order by items of group_concat()
						firstRow, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{distinctArg}, false)
						if err != nil {
							panic("NewAggFuncDesc FirstRow meets error: " + err.Error())
						}
						partial.AggFuncs = append(partial.AggFuncs, firstRow)
						newCol, _ := gbyCol.Clone().(*expression.Column)
						newCol.RetType = firstRow.RetTp
						partial.Schema.Append(newCol)
						if onlyAddFirstRow {
							ret = newCol
						}
						partialCursor++
					}
				}
				return ret
			}

			for j, distinctArg := range aggFunc.Args {
				// the last arg of ast.AggFuncGroupConcat is the separator, so just put it into the final agg
				if aggFunc.Name == ast.AggFuncGroupConcat && j+1 == len(aggFunc.Args) {
					args = append(args, distinctArg)
					continue
				}
				args = append(args, getDistinctExpr(distinctArg, false))
			}

			byItems := make([]*util.ByItems, 0, len(aggFunc.OrderByItems))
			for _, byItem := range aggFunc.OrderByItems {
				byItems = append(byItems, &util.ByItems{Expr: getDistinctExpr(byItem.Expr, true), Desc: byItem.Desc})
			}

			if aggFunc.HasDistinct && isMPPTask && aggFunc.GroupingID > 0 {
				// keep the groupingID as it was, otherwise the new split final aggregate's ganna lost its groupingID info.
				finalAggFunc.GroupingID = aggFunc.GroupingID
			}

			finalAggFunc.OrderByItems = byItems
			finalAggFunc.HasDistinct = aggFunc.HasDistinct
			// In logical optimize phase, the Agg->PartitionUnion->TableReader may become
			// Agg1->PartitionUnion->Agg2->TableReader, and the Agg2 is a partial aggregation.
			// So in the push down here, we need to add a new if-condition check:
			// If the original agg mode is partial already, the finalAggFunc's mode become Partial2.
			if aggFunc.Mode == aggregation.CompleteMode {
				finalAggFunc.Mode = aggregation.CompleteMode
			} else if aggFunc.Mode == aggregation.Partial1Mode || aggFunc.Mode == aggregation.Partial2Mode {
				finalAggFunc.Mode = aggregation.Partial2Mode
			}
		} else {
			if aggFunc.Name == ast.AggFuncGroupConcat && len(aggFunc.OrderByItems) > 0 {
				// group_concat can only run in one phase if it has order by items but without distinct property
				partial = nil
				final = original
				return
			}
			if aggregation.NeedCount(finalAggFunc.Name) {
				// only Avg and Count need count
				if isMPPTask && finalAggFunc.Name == ast.AggFuncCount {
					// For MPP base.Task, the final count() is changed to sum().
					// Note: MPP mode does not run avg() directly, instead, avg() -> sum()/(case when count() = 0 then 1 else count() end),
					// so we do not process it here.
					finalAggFunc.Name = ast.AggFuncSum
				} else {
					// avg branch
					ft := types.NewFieldType(mysql.TypeLonglong)
					ft.SetFlen(21)
					ft.SetCharset(charset.CharsetBin)
					ft.SetCollate(charset.CollationBin)
					partial.Schema.Append(&expression.Column{
						UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
						RetType:  ft,
					})
					args = append(args, partial.Schema.Columns[partialCursor])
					partialCursor++
				}
			}
			if finalAggFunc.Name == ast.AggFuncApproxCountDistinct {
				ft := types.NewFieldType(mysql.TypeString)
				ft.SetCharset(charset.CharsetBin)
				ft.SetCollate(charset.CollationBin)
				ft.AddFlag(mysql.NotNullFlag)
				partial.Schema.Append(&expression.Column{
					UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
					RetType:  ft,
				})
				args = append(args, partial.Schema.Columns[partialCursor])
				partialCursor++
			}
			if aggregation.NeedValue(finalAggFunc.Name) {
				partial.Schema.Append(&expression.Column{
					UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
					RetType:  original.Schema.Columns[i].GetType(ectx),
				})
				args = append(args, partial.Schema.Columns[partialCursor])
				partialCursor++
			}
			if aggFunc.Name == ast.AggFuncAvg {
				cntAgg := aggFunc.Clone()
				cntAgg.Name = ast.AggFuncCount
				exprCtx := sctx.GetExprCtx()
				err := cntAgg.TypeInfer(exprCtx)
				if err != nil { // must not happen
					partial = nil
					final = original
					return
				}
				partial.Schema.Columns[partialCursor-2].RetType = cntAgg.RetTp
				// we must call deep clone in this case, to avoid sharing the arguments.
				sumAgg := aggFunc.Clone()
				sumAgg.Name = ast.AggFuncSum
				if err = sumAgg.TypeInfer4AvgSum(exprCtx.GetEvalCtx(), aggFunc.RetTp); err != nil {
					partial = nil
					final = original
					return
				}
				partial.Schema.Columns[partialCursor-1].RetType = sumAgg.RetTp
				partial.AggFuncs = append(partial.AggFuncs, cntAgg, sumAgg)
			} else if aggFunc.Name == ast.AggFuncApproxCountDistinct || aggFunc.Name == ast.AggFuncGroupConcat {
				newAggFunc := aggFunc.Clone()
				newAggFunc.Name = aggFunc.Name
				newAggFunc.RetTp = partial.Schema.Columns[partialCursor-1].GetType(ectx)
				partial.AggFuncs = append(partial.AggFuncs, newAggFunc)
				if aggFunc.Name == ast.AggFuncGroupConcat {
					// append the last separator arg
					args = append(args, aggFunc.Args[len(aggFunc.Args)-1])
				}
			} else {
				// other agg desc just split into two parts
				partialFuncDesc := aggFunc.Clone()
				partial.AggFuncs = append(partial.AggFuncs, partialFuncDesc)
				if aggFunc.Name == ast.AggFuncFirstRow {
					firstRowFuncMap[partialFuncDesc] = finalAggFunc
				}
			}

			// In logical optimize phase, the Agg->PartitionUnion->TableReader may become
			// Agg1->PartitionUnion->Agg2->TableReader, and the Agg2 is a partial aggregation.
			// So in the push down here, we need to add a new if-condition check:
			// If the original agg mode is partial already, the finalAggFunc's mode become Partial2.
			if aggFunc.Mode == aggregation.CompleteMode {
				finalAggFunc.Mode = aggregation.FinalMode
			} else if aggFunc.Mode == aggregation.Partial1Mode || aggFunc.Mode == aggregation.Partial2Mode {
				finalAggFunc.Mode = aggregation.Partial2Mode
			}
		}

		finalAggFunc.Args = args
		finalAggFunc.RetTp = aggFunc.RetTp
		final.AggFuncs[i] = finalAggFunc
	}
	partial.Schema.Append(partialGbySchema.Columns...)
	if partialIsCop {
		for _, f := range partial.AggFuncs {
			f.Mode = aggregation.Partial1Mode
		}
	}
	return
}

// ExplainInfo implements Plan interface.
func (p *BasePhysicalAgg) ExplainInfo() string {
	return p.explainInfo(false)
}

func (p *BasePhysicalAgg) explainInfo(normalized bool) string {
	sortedExplainExpressionList := expression.SortedExplainExpressionList
	if normalized {
		sortedExplainExpressionList = func(_ expression.EvalContext, exprs []expression.Expression) []byte {
			return expression.SortedExplainNormalizedExpressionList(exprs)
		}
	}

	builder := &strings.Builder{}
	if len(p.GroupByItems) > 0 {
		builder.WriteString("group by:")
		builder.Write(sortedExplainExpressionList(p.SCtx().GetExprCtx().GetEvalCtx(), p.GroupByItems))
		builder.WriteString(", ")
	}
	for i := range p.AggFuncs {
		builder.WriteString("funcs:")
		var colName string
		if normalized {
			colName = p.Schema().Columns[i].ExplainNormalizedInfo()
		} else {
			colName = p.Schema().Columns[i].ExplainInfo(p.SCtx().GetExprCtx().GetEvalCtx())
		}
		builder.WriteString(aggregation.ExplainAggFunc(p.SCtx().GetExprCtx().GetEvalCtx(), p.AggFuncs[i], normalized))
		builder.WriteString("->")
		builder.WriteString(colName)
		if i+1 < len(p.AggFuncs) {
			builder.WriteString(", ")
		}
	}
	if p.TiFlashFineGrainedShuffleStreamCount > 0 {
		fmt.Fprintf(builder, ", stream_count: %d", p.TiFlashFineGrainedShuffleStreamCount)
	}
	return builder.String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *BasePhysicalAgg) ExplainNormalizedInfo() string {
	return p.explainInfo(true)
}

// ResolveIndices implements Plan interface.
func (p *BasePhysicalAgg) ResolveIndices() (err error) {
	err = p.PhysicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	for _, aggFun := range p.AggFuncs {
		for i, arg := range aggFun.Args {
			aggFun.Args[i], err = arg.ResolveIndices(p.Children()[0].Schema())
			if err != nil {
				return err
			}
		}
		for _, byItem := range aggFun.OrderByItems {
			byItem.Expr, err = byItem.Expr.ResolveIndices(p.Children()[0].Schema())
			if err != nil {
				return err
			}
		}
	}
	for i, item := range p.GroupByItems {
		p.GroupByItems[i], err = item.ResolveIndices(p.Children()[0].Schema())
		if err != nil {
			return err
		}
	}
	return
}

// ExhaustPhysicalPlans4LogicalAggregation will be called by LogicalAggregation in logicalOp pkg.
func ExhaustPhysicalPlans4LogicalAggregation(lp base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	la := lp.(*logicalop.LogicalAggregation)
	preferHash, preferStream := la.ResetHintIfConflicted()
	hashAggs := getHashAggs(la, prop)
	if len(hashAggs) > 0 && preferHash {
		return hashAggs, true, nil
	}
	streamAggs := getStreamAggs(la, prop)
	if len(streamAggs) > 0 && preferStream {
		return streamAggs, true, nil
	}
	aggs := append(hashAggs, streamAggs...)

	if streamAggs == nil && preferStream && !prop.IsSortItemEmpty() {
		la.SCtx().GetSessionVars().StmtCtx.SetHintWarning("Optimizer Hint STREAM_AGG is inapplicable")
	}
	return aggs, !(preferStream || preferHash), nil
}

// TODO: support more operators and distinct later
func checkCanPushDownToMPP(la *logicalop.LogicalAggregation) bool {
	hasUnsupportedDistinct := false
	for _, agg := range la.AggFuncs {
		// MPP does not support distinct except count distinct now
		if agg.HasDistinct {
			if agg.Name != ast.AggFuncCount && agg.Name != ast.AggFuncGroupConcat {
				hasUnsupportedDistinct = true
			}
		}
		// MPP does not support AggFuncApproxCountDistinct now
		if agg.Name == ast.AggFuncApproxCountDistinct {
			hasUnsupportedDistinct = true
		}
	}
	if hasUnsupportedDistinct {
		warnErr := errors.NewNoStackError("Aggregation can not be pushed to storage layer in mpp mode because it contains agg function with distinct")
		if la.SCtx().GetSessionVars().StmtCtx.InExplainStmt {
			la.SCtx().GetSessionVars().StmtCtx.AppendWarning(warnErr)
		} else {
			la.SCtx().GetSessionVars().StmtCtx.AppendExtraWarning(warnErr)
		}
		return false
	}
	return CheckAggCanPushCop(la.SCtx(), la.AggFuncs, la.GroupByItems, kv.TiFlash)
}
