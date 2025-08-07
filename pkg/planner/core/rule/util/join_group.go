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

// JoinGroupResult represents a group of tables that can be joined together
type JoinGroupResult struct {
	Group              []base.LogicalPlan
	EqEdges            []*expression.ScalarFunction
	OtherConds         []expression.Expression
	JoinTypes          []*JoinTypeWithExtMsg
	JoinOrderHintInfo  []*h.PlanHints
	JoinMethodHintInfo map[int]*JoinMethodHint
	HasOuterJoin       bool
}

// JoinTypeWithExtMsg represents join type with additional message
type JoinTypeWithExtMsg struct {
	JoinType int // Use int instead of logicalop.JoinType to avoid import cycle
	ExtMsg   string
}

// JoinMethodHint represents join method hint information
type JoinMethodHint struct {
	Method string
	Tables []string
}

// JoinGroupExtractor interface for extracting join groups
type JoinGroupExtractor interface {
	IsJoin(p base.LogicalPlan) bool
	GetJoinType(p base.LogicalPlan) int
	GetEqualConditions(p base.LogicalPlan) []*expression.ScalarFunction
	GetOtherConditions(p base.LogicalPlan) []expression.Expression
	GetLeftConditions(p base.LogicalPlan) []expression.Expression
	GetRightConditions(p base.LogicalPlan) []expression.Expression
	GetHintInfo(p base.LogicalPlan) *h.PlanHints
	GetPreferJoinType(p base.LogicalPlan) uint
	GetStraightJoin(p base.LogicalPlan) bool
	GetPreferJoinOrder(p base.LogicalPlan) bool
}

// ExtractJoinGroups extracts all join groups from the given plan using the provided extractor
func ExtractJoinGroups(p base.LogicalPlan, extractor JoinGroupExtractor) []*JoinGroupResult {
	var groups []*JoinGroupResult
	extractJoinGroupsRecursive(p, &groups, extractor)
	return groups
}

// extractJoinGroupsRecursive recursively extracts join groups
func extractJoinGroupsRecursive(p base.LogicalPlan, groups *[]*JoinGroupResult, extractor JoinGroupExtractor) {
	if extractor.IsJoin(p) {
		group := extractJoinGroup(p, extractor)
		if group != nil && len(group.Group) > 1 {
			*groups = append(*groups, group)
		}
	} else {
		for _, child := range p.Children() {
			extractJoinGroupsRecursive(child, groups, extractor)
		}
	}
}

// extractJoinGroup extracts a single join group starting from the given join node
func extractJoinGroup(p base.LogicalPlan, extractor JoinGroupExtractor) *JoinGroupResult {
	if !extractor.IsJoin(p) {
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
	extractJoinGroupRecursive(p, group, extractor)

	return group
}

// extractJoinGroupRecursive recursively extracts join information
func extractJoinGroupRecursive(p base.LogicalPlan, group *JoinGroupResult, extractor JoinGroupExtractor) {
	if extractor.IsJoin(p) {
		// Check if this is an outer join
		joinType := extractor.GetJoinType(p)
		if joinType != 0 { // 0 = InnerJoin
			group.HasOuterJoin = true
		}

		// Add join type information
		joinTypeWithExt := &JoinTypeWithExtMsg{
			JoinType: joinType,
			ExtMsg:   "",
		}
		group.JoinTypes = append(group.JoinTypes, joinTypeWithExt)

		// Extract equal conditions and other conditions
		eqConds := extractor.GetEqualConditions(p)
		otherConds := extractor.GetOtherConditions(p)
		
		group.EqEdges = append(group.EqEdges, eqConds...)
		group.OtherConds = append(group.OtherConds, otherConds...)

		// Recursively process children
		children := p.Children()
		if len(children) >= 2 {
			extractJoinGroupRecursive(children[0], group, extractor)
			extractJoinGroupRecursive(children[1], group, extractor)
		}
	} else {
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
