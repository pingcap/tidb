// Copyright 2026 PingCAP, Inc.
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

package joinorder

import (
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	utilhint "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// OrderedLeadingChoice is the unique vertex that owns all columns in an
// order-aware TopN property.
type OrderedLeadingChoice struct {
	Carrier base.LogicalPlan
	Table   utilhint.HintedTable
}

// OrderedLeadingAnnotation describes an internal Leading preference that can
// be applied atomically to one join anchor.
type OrderedLeadingAnnotation struct {
	Anchor *logicalop.LogicalJoin
	Choice *OrderedLeadingChoice
	Hints  *utilhint.PlanHints
}

// FindOrderedLeadingChoice returns the only eligible carrier in an inner equi
// join group. User hints and join-method hints are intentionally treated as a
// hard boundary for this first implementation.
func FindOrderedLeadingChoice(root base.LogicalPlan, order []property.SortItem) (*OrderedLeadingChoice, bool) {
	if root == nil || len(order) == 0 {
		return nil, false
	}
	group := extractJoinGroup(root)
	if group == nil || len(group.vertexes) < 2 || !group.allInnerJoin || len(group.leadingHints) != 0 || len(group.vertexHints) != 0 {
		return nil, false
	}
	if !hasOnlyInnerEquiJoins(root) {
		return nil, false
	}

	var carrier base.LogicalPlan
	for _, vertex := range group.vertexes {
		if !containsAllOrderColumns(vertex.Schema(), order) {
			continue
		}
		if carrier != nil {
			return nil, false
		}
		carrier = vertex
	}
	if carrier == nil {
		return nil, false
	}
	table := plannerutil.ExtractTableAlias(carrier, group.root.QueryBlockOffset())
	if table == nil {
		return nil, false
	}
	return &OrderedLeadingChoice{Carrier: carrier, Table: *table}, true
}

// BuildOrderedLeadingAnnotation creates an internal Leading annotation without
// mutating the logical plan. The hint must uniquely match its carrier through
// the same operand matcher used by greedy join reorder. Callers can validate
// and apply a complete set of annotations atomically.
func BuildOrderedLeadingAnnotation(root base.LogicalPlan, choice *OrderedLeadingChoice) (*OrderedLeadingAnnotation, bool) {
	if root == nil || choice == nil || choice.Carrier == nil {
		return nil, false
	}
	anchor := orderedLeadingAnchor(root)
	if anchor == nil {
		return nil, false
	}
	group := extractJoinGroup(root)
	hints, ok := buildInternalLeading(choice.Table)
	if !ok {
		logInternalLeadingFailure(group, "cannot construct the carrier hint")
		return nil, false
	}
	// Checking only the carrier would miss a hint that also matches another
	// operand. Reuse greedy's lookup across the complete group and verify that
	// the unique match is the carrier whose order was checked.
	astTable := hints.LeadingList.Items[0].(*ast.HintTable)
	matched, _, ok := FindAndRemovePlanByAstHint(root.SCtx(), slices.Clone(group.vertexes), astTable, group.ownerOffset,
		func(plan base.LogicalPlan) base.LogicalPlan { return plan })
	if !ok || matched != choice.Carrier {
		logInternalLeadingFailure(group, "hint does not uniquely match the carrier")
		return nil, false
	}
	return &OrderedLeadingAnnotation{Anchor: anchor, Choice: choice, Hints: hints}, true
}

func logInternalLeadingFailure(group *joinGroup, reason string) {
	logutil.BgLogger().Debug("internal order-aware leading hint is inapplicable",
		zap.Int("joinPlanID", group.root.ID()),
		zap.Int("ownerQueryBlock", group.ownerOffset),
		zap.String("reason", reason))
}

// ApplyOrderedLeadingAnnotations applies a previously validated annotation
// set. The caller must perform conflict checks before calling this function.
func ApplyOrderedLeadingAnnotations(annotations []*OrderedLeadingAnnotation) {
	for _, annotation := range annotations {
		if annotation == nil || annotation.Anchor == nil || annotation.Hints == nil {
			continue
		}
		annotation.Anchor.InternalHintInfo = annotation.Hints
		annotation.Anchor.InternalPreferJoinOrder = true
	}
}

func containsAllOrderColumns(schema *expression.Schema, order []property.SortItem) bool {
	if schema == nil {
		return false
	}
	for _, item := range order {
		found := false
		for _, col := range schema.Columns {
			if col != nil && item.Col != nil && col.EqualColumn(item.Col) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func orderedLeadingAnchor(root base.LogicalPlan) *logicalop.LogicalJoin {
	if join, ok := root.(*logicalop.LogicalJoin); ok {
		return join
	}
	if len(root.Children()) == 1 {
		return orderedLeadingAnchor(root.Children()[0])
	}
	return nil
}

func hasOnlyInnerEquiJoins(root base.LogicalPlan) bool {
	valid := true
	var visit func(base.LogicalPlan)
	visit = func(plan base.LogicalPlan) {
		if !valid || plan == nil {
			return
		}
		if join, ok := plan.(*logicalop.LogicalJoin); ok {
			if join.JoinType != logicalop.InnerJoin || len(join.EqualConditions) == 0 || join.StraightJoin ||
				join.PreferJoinOrder || join.InternalPreferJoinOrder || join.PreferJoinType != 0 ||
				(join.HintInfo != nil && len(join.HintInfo.LeadingJoinOrder) > 0) ||
				slices.ContainsFunc(join.EqualConditions, func(cond *expression.ScalarFunction) bool {
					return cond == nil || cond.FuncName.L == ast.NullEQ
				}) {
				valid = false
				return
			}
		}
		for _, child := range plan.Children() {
			visit(child)
		}
	}
	visit(root)
	return valid
}

func buildInternalLeading(table utilhint.HintedTable) (*utilhint.PlanHints, bool) {
	astTable := &ast.HintTable{DBName: table.DBName, TableName: table.TblName}
	if table.SelectOffset > 1 {
		qbName, err := utilhint.GenerateQBName(utilhint.TypeSelect, table.SelectOffset)
		if err != nil {
			return nil, false
		}
		astTable.QBName = qbName
	}
	return &utilhint.PlanHints{
		LeadingJoinOrder: []utilhint.HintedTable{table},
		LeadingList:      &ast.LeadingList{Items: []any{astTable}},
	}, true
}
