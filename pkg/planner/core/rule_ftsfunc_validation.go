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

package core

import (
	"context"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

type ftsFuncValidation struct {
}

func (ftsFuncValidation) Name() string {
	return "ftsFuncValidation"
}

// Optimize implements the LogicalOptRule interface.
// The check is performed here because we don't know whether a function is invalid or not until:
// 1. it's pushed down after applying the predicate push down rule.
// 2. it's really checked whether can be used to build a fts index request.
// So final check is performed here.
func (f *ftsFuncValidation) Optimize(ctx context.Context, p base.LogicalPlan, _ *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	return p, false, f.doQuickValidation(ctx, p)
}

func (f *ftsFuncValidation) doQuickValidation(ctx context.Context, p base.LogicalPlan) error {
	switch x := p.(type) {
	case *logicalop.LogicalProjection:
		if expression.ContainsFullTextSearchFn(x.Exprs...) {
			return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' cannot be used in SELECT fields. It can be used in WHERE only")
		}
	case *logicalop.LogicalSelection:
		if expression.ContainsFullTextSearchFn(x.Conditions...) {
			return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' must be used alone. It cannot be placed inside any other function or expression as a parameter, or used multiple times. A valid example: SELECT * FROM <TABLE> WHERE FTS_MATCH_WORD(...)")
		}
	case *logicalop.LogicalTopN:
		for _, item := range x.ByItems {
			if expression.ContainsFullTextSearchFn(item.Expr) {
				return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' in ORDER BY is not supported")
			}
		}
	case *logicalop.LogicalSort:
		for _, item := range x.ByItems {
			if expression.ContainsFullTextSearchFn(item.Expr) {
				return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' in ORDER BY clause is not supported")
			}
		}
	case *logicalop.LogicalJoin:
		if expression.ContainsFullTextSearchFn(x.OtherConditions...) {
			return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' cannot be used in JOIN ON conditions")
		}
		if expression.ContainsFullTextSearchFn(x.LeftConditions...) {
			return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' cannot be used in JOIN ON conditions")
		}
		if expression.ContainsFullTextSearchFn(x.RightConditions...) {
			return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' cannot be used in JOIN ON conditions")
		}
	case *logicalop.LogicalWindow:
		for _, item := range x.WindowFuncDescs {
			if expression.ContainsFullTextSearchFn(item.Args...) {
				return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' cannot be used in window function")
			}
		}
	case *logicalop.LogicalAggregation:
		for _, agg := range x.AggFuncs {
			if expression.ContainsFullTextSearchFn(agg.Args...) {
				return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' cannot be used in GROUP BY or HAVING")
			}
		}
		if expression.ContainsFullTextSearchFn(x.GroupByItems...) {
			return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' cannot be used in GROUP BY or HAVING")
		}
	case *logicalop.LogicalCTE:
		// current cte's optimization is a separate process, so we don't need to check the cte's children.
	case *logicalop.LogicalSequence:
		// last child is the main query.
		return f.doQuickValidation(ctx, x.Children()[len(x.Children())-1])
	}
	for _, child := range p.Children() {
		if err := f.doQuickValidation(ctx, child); err != nil {
			return err
		}
	}
	return nil
}
