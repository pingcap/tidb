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

package util

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	h "github.com/pingcap/tidb/pkg/util/hint"
)

// JoinType constants to avoid import cycle
const (
	InnerJoin = iota
	LeftOuterJoin
	RightOuterJoin
	SemiJoin
	AntiSemiJoin
	LeftOuterSemiJoin
	AntiLeftOuterSemiJoin
)

// JoinGroupResult represents the result of extracting a join group
type JoinGroupResult struct {
	Group              []base.LogicalPlan
	EqEdges            []*expression.ScalarFunction
	OtherConds         []expression.Expression
	JoinTypes          []*JoinTypeWithExtMsg
	JoinOrderHintInfo  []*h.PlanHints
	JoinMethodHintInfo map[int]*JoinMethodHint
	HasOuterJoin       bool
}

// JoinTypeWithExtMsg represents join type with extended message
type JoinTypeWithExtMsg struct {
	JoinType int // Use int instead of logicalop.JoinType to avoid import cycle
	ExtMsg   string
}

// JoinMethodHint represents join method hint information
type JoinMethodHint struct {
	Method string
	Tables []string
}

// ExtractJoinGroups extracts all join groups from the given plan
func ExtractJoinGroups(p base.LogicalPlan) []*JoinGroupResult {
	var groups []*JoinGroupResult
	extractJoinGroupsRecursive(p, &groups)
	return groups
}

// extractJoinGroupsRecursive recursively extracts join groups
func extractJoinGroupsRecursive(p base.LogicalPlan, groups *[]*JoinGroupResult) {
	switch x := p.(type) {
	case *LogicalJoin:
		group := extractJoinGroup(p)
		if group != nil && len(group.Group) > 1 {
			*groups = append(*groups, group)
		}
	default:
		for _, child := range p.Children() {
			extractJoinGroupsRecursive(child, groups)
		}
	}
}

// extractJoinGroup extracts a single join group starting from the given join node
func extractJoinGroup(p base.LogicalPlan) *JoinGroupResult {
	join, ok := p.(*LogicalJoin)
	if !ok {
		return nil
	}

	group := &JoinGroupResult{
		Group:              make([]base.LogicalPlan, 0),
		EqEdges:            make([]*expression.ScalarFunction, 0),
		OtherConds:         make([]expression.Expression, 0),
		JoinTypes:          make([]*JoinTypeWithExtMsg, 0),
		JoinOrderHintInfo:  make([]*h.PlanHints, 0),
		JoinMethodHintInfo: make(map[int]*JoinMethodHint),
		HasOuterJoin:       false,
	}

	// Extract join group recursively
	extractJoinGroupRecursive(join, group)

	return group
}

// extractJoinGroupRecursive recursively extracts join information
func extractJoinGroupRecursive(p base.LogicalPlan, group *JoinGroupResult) {
	switch x := p.(type) {
	case *LogicalJoin:
		// Check if this is an outer join
		if x.JoinType != InnerJoin {
			group.HasOuterJoin = true
		}

		// Add join type information
		joinType := &JoinTypeWithExtMsg{
			JoinType: x.JoinType,
			ExtMsg:   "",
		}
		group.JoinTypes = append(group.JoinTypes, joinType)

		// Extract equal conditions and other conditions
		for _, cond := range x.EqualConditions {
			group.EqEdges = append(group.EqEdges, cond)
		}
		for _, cond := range x.OtherConditions {
			group.OtherConds = append(group.OtherConds, cond)
		}

		// Recursively process children
		extractJoinGroupRecursive(x.Children()[0], group)
		extractJoinGroupRecursive(x.Children()[1], group)

	default:
		// This is a leaf node (table or other operator)
		group.Group = append(group.Group, p)
	}
}

// CanReorder checks if the join group can be reordered
func (g *JoinGroupResult) CanReorder() bool {
	// Cannot reorder if there are outer joins
	if g.HasOuterJoin {
		return false
	}

	// Need at least 2 tables to reorder
	return len(g.Group) >= 2
}
