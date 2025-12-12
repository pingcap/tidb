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

// OuterJoinToSemiJoin is a logical optimization rule that converts a `LogicalJoin` operator
// (LEFT or RIGHT outer join) into a more efficient `LogicalSemiJoin` operator. While users
// cannot write `SEMI JOIN` directly in SQL, the optimizer can apply this transformation
// internally when a `WHERE` clause makes an outer join behave like a semi join. This avoids
// building the full join result and significantly improves performance.
//
// This rule identifies two main patterns that can be transformed into a `LogicalSemiJoin` operator:
//
//  1. With a standard `SemiJoin` mode:
//     When a `WHERE` clause contains a "null-rejecting" predicate on the inner table of a
//     `LEFT JOIN`, it filters out all non-matching rows. If the query only needs to check for
//     the existence of matching rows, the `LogicalJoin` can be converted to a `LogicalSemiJoin`.
//     SQL Example: `SELECT a.* FROM a LEFT JOIN b ON a.id=b.id WHERE b.val > 0`
//     This is transformed internally into a plan with a `LogicalSemiJoin` operator.
//
//  2. With an `Anti` mode (`AntiSemiJoin`):
//     When a `WHERE` clause checks for `IS NULL` on a `NOT NULL` column of the inner table,
//     it's a clear indication that the query seeks rows from the outer table that have NO match.
//     This pattern is transformed internally into a `LogicalSemiJoin` operator with its `JoinType`
//     set to `AntiSemiJoin`.
//     SQL Example: `SELECT a.* FROM a LEFT JOIN b ON a.id=b.id WHERE b.id IS NULL`
//     This is transformed internally into a plan with an `AntiSemiJoin` operator.
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
