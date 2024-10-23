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

package core

import (
	"context"
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)

// AggregationSimplifier is used to prune constant items in group by items
type AggregationSimplifier struct {
}

// Optimize implements the base.LogicalOptRule.<0th> interface.
func (as *AggregationSimplifier) Optimize(ctx context.Context, p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	if _, ok := p.(*logicalop.LogicalAggregation); ok {
		as.tryToPruneGroupByItems(p, opt)
	}
	// recursive optimize
	for _, children := range p.Children() {
		p, planChanged, err := as.Optimize(ctx, children, opt)
		if err != nil {
			return p, planChanged, err
		}
	}
	return p, planChanged, nil
}

// tryToPruneGroupByItems will eliminate constant group by items
func (as AggregationSimplifier) tryToPruneGroupByItems(p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) {
	la := p.(*logicalop.LogicalAggregation)
	// pull up constant predicates below this LogicalAggregation
	candidateConstantPredicates := la.Children()[0].PullUpConstantPredicates()
	if len(la.GroupByItems) < 1 || candidateConstantPredicates == nil {
		return
	}
	ctx := la.SCtx().GetExprCtx().GetEvalCtx()
	for i := len(la.GroupByItems) - 1; i >= 0; i-- {
		if item, ok := la.GroupByItems[i].(*expression.Column); ok {
			// traverse the candidateConstantPredicates we pulled up
			for _, candidatePredicate := range candidateConstantPredicates {
				scalarFunction, ok := candidatePredicate.(*expression.ScalarFunction)
				if !ok || scalarFunction.FuncName.L != ast.EQ {
					continue
				}
				column, _ := expression.ValidCompareConstantPredicateHelper(ctx, scalarFunction, true)
				if column == nil {
					column, _ = expression.ValidCompareConstantPredicateHelper(ctx, scalarFunction, false)
				}
				if column == nil || !column.EqualColumn(la.GroupByItems[i]) {
					continue
				}
				// new we find the item
				la.GroupByItems = append(la.GroupByItems[:i], la.GroupByItems[i+1:]...)
				appendAggSimplifierTraceStep(la, item, opt)
				break
			}
		}
	}
	// If all the group by items are pruned, we should add a constant 1 to keep the correctness.
	// Because `select count(*) from t` is different from `select count(*) from t group by 1`.
	if len(la.GroupByItems) == 0 {
		la.GroupByItems = []expression.Expression{expression.NewOne()}
	}
}

func appendAggSimplifierTraceStep(la *logicalop.LogicalAggregation, prunedItem *expression.Column, opt *optimizetrace.LogicalOptimizeOp) {
	reason := func() string {
		return fmt.Sprintf("%s is a constant", prunedItem.String())
	}
	action := func() string {
		return fmt.Sprintf("pruned %s from %v_%v.GroupByItems", prunedItem.String(), la.TP(), la.ID())
	}
	opt.AppendStepToCurrent(la.ID(), la.TP(), reason, action)
}

// Name implements the base.LogicalOptRule.<1st> interface.
func (as *AggregationSimplifier) Name() string {
	return "aggregation_simplifier"
}
