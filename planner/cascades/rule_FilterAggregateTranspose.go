// Copyright 2018 PingCAP, Inc.
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

package cascades

import (
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/set"
)

/**
type Transformation interface {
	GetPattern() *Pattern
	Match(expr *ExprIter) (matched bool)
	OnTransform(old *ExprIter) (new *GroupExpr, eraseOld bool, err error)
}
*/

type baseRuleImpl struct {
	pattern *Pattern
}

func (r *baseRuleImpl) GetPattern() *Pattern {
	return r.pattern
}

func (r *baseRuleImpl) Match(expr *ExprIter) bool {
	return true
}

type FilterAggregateTransposeRule struct {
	baseRuleImpl
}

func NewFilterAggregateTransposeRule() *FilterAggregateTransposeRule {
	pattern := BuildPattern(OperandSelection, BuildPattern(OperandAggregation))
	return &FilterAggregateTransposeRule{baseRuleImpl{pattern}}
}

// OnTransform push the filters through the aggregate:
// before: filter(aggregate(any), f1, f2, ...)
// after:  aggregate(filter(any, f1, f2, ...)),      totaly push through
//    or:  filter(aggregate(filter(any, f1)), f2), partialy push through
func (r *FilterAggregateTransposeRule) OnTransform(sctx sessionctx.Context, old *ExprIter) (new *GroupExpr, eraseOld bool, err error) {
	selGroupExpr := old.GetGroupExpr()
	aggGroupExpr := old.children[0].GetGroupExpr()

	pushed, remained := r.collectPushedFilters(selGroupExpr, aggGroupExpr)
	if len(pushed) == 0 {
		return nil, false, nil
	}

	// construct the first selection before aggregation.
	newSel := plannercore.LogicalSelection{Conditions: pushed}.Init(sctx)
	newSelGroupExpr := NewGroupExpr(newSel)
	newSelGroupExpr.children = aggGroupExpr.children
	newSelGroup := NewGroup(newSelGroupExpr)

	// construct the new aggregation upon the new selection.
	agg := aggGroupExpr.exprNode.(*plannercore.LogicalAggregation)
	newAgg := plannercore.LogicalAggregation{
		AggFuncs:     agg.AggFuncs,
		GroupByItems: agg.GroupByItems,
	}.Init(sctx)
	newAggGroupExpr := NewGroupExpr(newAgg)
	newAggGroupExpr.children = []*Group{newSelGroup}

	// no more selection opon the new aggregation.
	if len(remained) == 0 {
		return newAggGroupExpr, true, nil
	}

	// otherwise, construct a new selection upon the new aggregation.
	newAggGroup := NewGroup(newAggGroupExpr)
	newSel = plannercore.LogicalSelection{Conditions: remained}.Init(sctx)
	newSelGroupExpr = NewGroupExpr(newSel)
	newSelGroupExpr.children = []*Group{newAggGroup}
	return newSelGroupExpr, true, nil
}

func (r *FilterAggregateTransposeRule) collectPushedFilters(selGroupExpr, aggGroupExpr *GroupExpr) (pushed, remained []expression.Expression) {
	sel := selGroupExpr.exprNode.(*plannercore.LogicalSelection)
	agg := aggGroupExpr.exprNode.(*plannercore.LogicalAggregation)
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
