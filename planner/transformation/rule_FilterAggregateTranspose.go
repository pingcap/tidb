package transformation

import (
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/memo"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/set"
)

// FilterAggregateTransposeRule pushes the filters through the aggregate:
// before: filter(aggregate(any), f1, f2, ...)
// after:  aggregate(filter(any, f1, f2, ...)),      totaly push through
//    or:  filter(aggregate(filter(any, f1)), f2), partialy push through
type FilterAggregateTransposeRule struct {
	baseTransform
}

// NewFilterAggregateTransposeRule creates a new FilterAggregateTransposeRule.
func NewFilterAggregateTransposeRule() *FilterAggregateTransposeRule {
	pattern := memo.BuildPattern(memo.OperandSelection, memo.BuildPattern(memo.OperandAggregation))
	return &FilterAggregateTransposeRule{baseTransform{pattern}}
}

// OnTransform push the filters through the aggregate:
// before: filter(aggregate(any), f1, f2, ...)
// after:  aggregate(filter(any, f1, f2, ...)),      totaly push through
//    or:  filter(aggregate(filter(any, f1)), f2), partialy push through
func (r *FilterAggregateTransposeRule) OnTransform(sctx sessionctx.Context, old *memo.ExprIter) (new *memo.GroupExpr, eraseOld bool, err error) {
	selGroupExpr := old.GetGroupExpr()
	aggGroupExpr := old.Children[0].GetGroupExpr()
	pushed, remained := r.collectPushedFilters(selGroupExpr, aggGroupExpr)
	if len(pushed) == 0 {
		return nil, false, nil
	}
	// construct the first selection before aggregation.
	newSel := plannercore.LogicalSelection{Conditions: pushed}.Init(sctx)
	newSelGroupExpr := memo.NewGroupExpr(newSel)
	newSelGroupExpr.Children = aggGroupExpr.Children
	newSelGroup := memo.NewGroup(newSelGroupExpr)
	// construct the new aggregation upon the new selection.
	agg := aggGroupExpr.ExprNode.(*plannercore.LogicalAggregation)
	newAgg := plannercore.LogicalAggregation{
		AggFuncs:     agg.AggFuncs,
		GroupByItems: agg.GroupByItems,
	}.Init(sctx)
	newAggGroupExpr := memo.NewGroupExpr(newAgg)
	newAggGroupExpr.Children = []*memo.Group{newSelGroup}
	// no more selection opon the new aggregation.
	if len(remained) == 0 {
		return newAggGroupExpr, true, nil
	}
	// otherwise, construct a new selection upon the new aggregation.
	newAggGroup := memo.NewGroup(newAggGroupExpr)
	newSel = plannercore.LogicalSelection{Conditions: remained}.Init(sctx)
	newSelGroupExpr = memo.NewGroupExpr(newSel)
	newSelGroupExpr.Children = []*memo.Group{newAggGroup}
	return newSelGroupExpr, true, nil
}

func (r *FilterAggregateTransposeRule) collectPushedFilters(selGroupExpr, aggGroupExpr *memo.GroupExpr) (pushed, remained []expression.Expression) {
	sel := selGroupExpr.ExprNode.(*plannercore.LogicalSelection)
	agg := aggGroupExpr.ExprNode.(*plannercore.LogicalAggregation)
	gbyColIds := r.collectGbyCols(agg)
	for _, cond := range sel.Conditions {
		if !r.coveredByGbyCols(cond, gbyColIds) {
			if remained == nil {
				remained = make([]expression.Expression, 0, len(sel.Conditions)-len(pushed))
			}
			remained = append(remained, cond)
		} else {
			if pushed == nil {
				pushed = make([]expression.Expression, 0, len(sel.Conditions)-len(remained))
			}
			pushed = append(pushed, cond)
		}
	}
	return pushed, remained
}

func (r *FilterAggregateTransposeRule) collectGbyCols(agg *plannercore.LogicalAggregation) set.Int64Set {
	gbyColIds := set.NewInt64Set()
	for i := range agg.GroupByItems {
		gbyCol, isCol := agg.GroupByItems[i].(*expression.Column)
		if isCol {
			gbyColIds.Insert(gbyCol.UniqueID)
		}
	}
	return gbyColIds
}

func (r *FilterAggregateTransposeRule) coveredByGbyCols(filter expression.Expression, gbyColIds set.Int64Set) bool {
	switch v := filter.(type) {
	case *expression.Column:
		return gbyColIds.Exist(v.UniqueID)
	case *expression.ScalarFunction:
		for _, arg := range v.GetArgs() {
			if !r.coveredByGbyCols(arg, gbyColIds) {
				return false
			}
		}
	}
	return true
}
