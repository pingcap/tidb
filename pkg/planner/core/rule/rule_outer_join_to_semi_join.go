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

package rule

import (
	"context"

	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

// OuterJoinToSemiJoin is a logical optimization rule that rewrites a `LogicalJoin` (Outer Join)
// into an `AntiSemiJoin`. The transformation is triggered by a subsequent `LogicalSelection`
// that effectively filters for non-matched rows from the outer join.
//
// The core idea is to identify queries whose semantics are to find rows from one table that
// have NO match in another. This rule recognizes such patterns and changes the `LogicalJoin`'s
// `JoinType` to `AntiSemiJoin`.
//
// A key part of this transformation is the creation of a `LogicalProjection` on top of the
// new `AntiSemiJoin`. This projection is responsible for generating the `NULL` values for the
// columns of the outer table, which is the expected result for this type of query.
//
// The rule is triggered if the `WHERE` clause checks for `IS NULL` on a column from the
// inner table that is guaranteed to be non-null if a match had occurred. This guarantee
// comes from two main patterns identified in the `CanConvertAntiJoin` function:
//
//  1. The `IS NULL` check is on a column that is part of the join condition.
//     SQL Example: `SELECT B.* FROM A RIGHT JOIN B ON A.id = B.a_id WHERE A.id IS NULL`
//
//  2. The `IS NULL` check is on a column that has a `NOT NULL` constraint in its table definition.
//     SQL Example: `SELECT A.* FROM A LEFT JOIN B ON A.id = B.a_id WHERE B.non_null_col IS NULL`
//
// In both cases, the original `LogicalSelection` is eliminated, and the plan is rewritten to
// `LogicalProjection -> LogicalJoin(AntiSemiJoin)`.
type OuterJoinToSemiJoin struct{}

// Optimize implements base.LogicalOptRule.<0th> interface.
func (o *OuterJoinToSemiJoin) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	result, isChanged := o.recursivePlan(p)
	return result, isChanged, nil
}

func (o *OuterJoinToSemiJoin) recursivePlan(p base.LogicalPlan) (base.LogicalPlan, bool) {
	var isChanged bool
	for idx, child := range p.Children() {
		if sel, ok := child.(*logicalop.LogicalSelection); ok {
			join, ok := sel.Children()[0].(*logicalop.LogicalJoin)
			if ok {
				proj, ok := join.CanConvertAntiJoin(sel.Conditions, sel.Schema())
				if ok {
					if proj != nil {
						proj.SetChildren(join)
						p.SetChild(idx, proj)
					} else {
						p.SetChild(idx, join)
					}
					isChanged = true
				}
			}
		}
		_, changed := o.recursivePlan(child)
		isChanged = isChanged || changed
	}
	return p, isChanged
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*OuterJoinToSemiJoin) Name() string {
	return "outer_join_semi_join"
}
