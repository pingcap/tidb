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

package memo

import (
	"fmt"

	"github.com/pingcap/tidb/planner/core"
)

// DumpGroupLogicalPlans dumps all the group expressions in the group.
func DumpGroupLogicalPlans(g *Group) []string {
	result := make([]string, 0, g.Equivalents.Len())
	for elem := g.Equivalents.Front(); elem != nil; elem = elem.Next() {
		result = append(result, dumpGroupExpr(elem.Value.(*GroupExpr))...)
	}
	return result
}

func dumpGroupExpr(ge *GroupExpr) []string {
	switch x := ge.ExprNode.(type) {
	case *core.DataSource:
		return []string{x.ExplainID()}
	case *core.LogicalProjection, *core.LogicalLimit, *core.LogicalTopN,
		*core.LogicalSelection, *core.LogicalSort, *core.LogicalAggregation:
		return expandGroupExprs(x.ExplainID(), DumpGroupLogicalPlans(ge.Children[0]))
	case *core.LogicalJoin, *core.LogicalApply:
		return expandGroupExprs(x.ExplainID(), DumpGroupLogicalPlans(ge.Children[0]), DumpGroupLogicalPlans(ge.Children[0]))
	default:
		panic(fmt.Sprintf("unsupported plan node in dumpGroupExpr: %T", x))
	}
}

func expandGroupExprs(funcName string, args ...[]string) []string {
	if len(args) == 0 {
		return []string{funcName}
	}

	if len(args) == 1 {
		if len(args[0]) == 0 {
			return []string{funcName}
		}
		result := make([]string, 0, len(args[0]))
		for i := range args[0] {
			result = append(result, fmt.Sprintf("%s(%s)", funcName, args[0][i]))
		}
		return result
	}

	if len(args) == 2 {
		if len(args[0]) == 0 || len(args[1]) == 0 {
			return []string{funcName}
		}
		result := make([]string, 0, len(args[0])*len(args[1]))
		for i := range args[0] {
			for j := range args[1] {
				result = append(result, fmt.Sprintf("%s(%s, %s)", funcName, args[0][i], args[1][j]))
			}
		}
		return result
	}

	panic(fmt.Sprintf("len(args) should not exeed 2, current is %v", len(args)))
}
