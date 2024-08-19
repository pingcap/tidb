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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)

func mergeOnClausePredicates(p *LogicalJoin, predicates []expression.Expression) []expression.Expression {
	combinedCond := make([]expression.Expression, 0,
		len(p.LeftConditions)+len(p.RightConditions)+
			len(p.EqualConditions)+len(p.OtherConditions)+
			len(predicates))
	combinedCond = append(combinedCond, p.LeftConditions...)
	combinedCond = append(combinedCond, p.RightConditions...)
	combinedCond = append(combinedCond, expression.ScalarFuncs2Exprs(p.EqualConditions)...)
	combinedCond = append(combinedCond, p.OtherConditions...)
	combinedCond = append(combinedCond, predicates...)
	return combinedCond
}

// ConvertOuterToInnerJoin converts outer to inner joins if the unmtaching rows are filtered.
type ConvertOuterToInnerJoin struct {
}

// Optimize implements base.LogicalOptRule.<0th> interface.
// convertOuterToInnerJoin is refactoring of the outer to inner join logic that used to be part of predicate push down.
// The rewrite passes down predicates from selection (WHERE clause) and join predicates (ON clause).
// All nodes except LogicalJoin are pass through where the rewrite is done for the child and nothing for the node itself.
// The main logic is applied for joins:
//  1. Traversal is preorder and the passed down predicate is checked for the left/right after join
//  2. The ON clause and passed down predicate (from higher selects or joins) are comined and applied to join children.
//     This logic depends on the join type with the following logic:
//     - For left/right outer joins, the ON clause an be applied only on the inner side (null producing side)
//     - For inner/semi joins, the ON clause can be applied on both children
//     - For anti semi joins, ON clause applied only on left side
//     - For all other cases, do not pass ON clause.
func (*ConvertOuterToInnerJoin) Optimize(_ context.Context, p base.LogicalPlan, _ *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	return p.ConvertOuterToInnerJoin(nil), planChanged, nil
}

// LogicalAggregation just works since schema = child + aggregate expressions. No need to map predicates.
// Also, predicates involving aggregate expressions are not null filtering. IsNullReject always returns
// false for those cases.

// Name implements base.LogicalOptRule.<1st> interface.
func (*ConvertOuterToInnerJoin) Name() string {
	return "convert_outer_to_inner_joins"
}
