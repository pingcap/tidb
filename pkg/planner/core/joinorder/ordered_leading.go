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
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/hint"
)

// AnnotateOrderedLeading tries to attach an internal LEADING preference to the
// top join of the current join group. The preference points to the leaf whose
// index can preserve the requested ordering after accounting for single-table
// equality filters.
func AnnotateOrderedLeading(root base.LogicalPlan, orderings [][]*expression.Column, parentFilters []expression.Expression) bool {
	if root == nil || len(orderings) == 0 {
		return false
	}

	group := extractJoinGroup(root)
	if len(group.vertexes) <= 1 || len(group.leadingHints) > 0 {
		return false
	}

	anchor := findLeadingHintAnchor(group.root)
	if anchor == nil || anchor.PreferJoinOrder || anchor.PreferJoinType > 0 || anchor.HintInfo != nil {
		return false
	}

	leadingTable := findOrderedLeadingTable(group, orderings, parentFilters)
	if leadingTable == nil {
		return false
	}

	// once order is aware-ed, setting the leading hint on the anchor join and mark it as prefer join order.
	// This is an internal hint that won't be exposed to users, we just take advantage of the existing leading hint preferring.
	anchor.PreferJoinOrder = true
	anchor.HintInfo = buildSingleTableLeadingHint(leadingTable)
	return true
}

func findLeadingHintAnchor(root base.LogicalPlan) *logicalop.LogicalJoin {
	for root != nil {
		switch node := root.(type) {
		case *logicalop.LogicalJoin:
			return node
		case *logicalop.LogicalSelection:
			if len(node.Children()) == 0 {
				return nil
			}
			root = node.Children()[0]
		default:
			return nil
		}
	}
	return nil
}

func buildSingleTableLeadingHint(table *hint.HintedTable) *hint.PlanHints {
	if table == nil {
		return nil
	}
	qbName := ast.CIStr{}
	if table.SelectOffset > 0 {
		if generatedQBName, err := hint.GenerateQBName(hint.TypeSelect, table.SelectOffset); err == nil {
			qbName = generatedQBName
		}
	}
	return &hint.PlanHints{
		LeadingJoinOrder: []hint.HintedTable{*table},
		LeadingList: &ast.LeadingList{
			Items: []interface{}{
				&ast.HintTable{
					DBName:    table.DBName,
					TableName: table.TblName,
					QBName:    qbName,
				},
			},
		},
	}
}

// findOrderedLeadingTable picks one leaf table to seed an internal LEADING hint.
// Each entry in orderings is treated as one complete ordering requirement rather
// than a set of independent columns. A vertex qualifies only when it contains
// all columns from one ordering vector and one of its indexes can preserve that
// full ordering after accounting for local equality predicates. The first
// vertex that can also be represented as a single hinted table is returned,
// because the current rule only needs one leading seed, not every ordered
// candidate in the group.
func findOrderedLeadingTable(group *joinGroup, orderings [][]*expression.Column, parentFilters []expression.Expression) *hint.HintedTable {
	groupSelectionConds := collectSelectionConds(group)
	for _, orderingCols := range orderings {
		orderingColIDs, orderingColUniqueIDs := normalizeOrderingColumns(orderingCols)
		if len(orderingColIDs) == 0 || len(orderingColIDs) != len(orderingCols) {
			continue
		}
		for _, vertex := range group.vertexes {
			if !schemaContainsAllOrderingColumns(vertex, orderingColUniqueIDs) {
				continue
			}
			if !tableHasIndexMatchingOrdering(vertex, orderingColIDs, groupSelectionConds, parentFilters) {
				continue
			}
			// once we found a vertex that can satisfy the ordering requirement, we try to extract a single-table hint from it.
			// This is because the current optimization only needs one leading seed, and a single-table hint is more likely to
			// be useful and applicable in the final plan.
			if tableAlias := util.ExtractTableAlias(vertex, vertex.QueryBlockOffset()); tableAlias != nil {
				return tableAlias
			}
		}
	}
	return nil
}

func collectSelectionConds(group *joinGroup) []expression.Expression {
	if len(group.selConds) == 0 {
		return nil
	}
	conds := make([]expression.Expression, 0, len(group.selConds))
	for _, exprs := range group.selConds {
		conds = append(conds, exprs...)
	}
	return conds
}

// normalizeOrderingColumns validates the ordering columns and extracts their IDs and UniqueIDs.
func normalizeOrderingColumns(orderingCols []*expression.Column) ([]int64, map[int64]struct{}) {
	orderingColIDs := make([]int64, 0, len(orderingCols))
	orderingColUniqueIDs := make(map[int64]struct{}, len(orderingCols))
	for _, col := range orderingCols {
		if col == nil || col.ID <= 0 || col.UniqueID <= 0 {
			return nil, nil
		}
		orderingColIDs = append(orderingColIDs, col.ID)
		orderingColUniqueIDs[col.UniqueID] = struct{}{}
	}
	return orderingColIDs, orderingColUniqueIDs
}

func schemaContainsAllOrderingColumns(plan base.LogicalPlan, orderingColUniqueIDs map[int64]struct{}) bool {
	if len(orderingColUniqueIDs) == 0 {
		return false
	}
	schema := plan.Schema()
	if schema == nil || len(schema.Columns) < len(orderingColUniqueIDs) {
		return false
	}
	matched := 0
	for _, col := range schema.Columns {
		if _, ok := orderingColUniqueIDs[col.UniqueID]; ok {
			matched++
		}
	}
	return matched == len(orderingColUniqueIDs)
}

func tableHasIndexMatchingOrdering(
	plan base.LogicalPlan,
	orderingColIDs []int64,
	groupSelectionConds []expression.Expression,
	parentFilters []expression.Expression,
) bool {
	ds := findDataSource(plan)
	if ds == nil {
		return false
	}

	equalityColIDs := collectEqualityPredicateColumnIDs(plan, groupSelectionConds, parentFilters)
	for _, idx := range ds.TableInfo.Indices {
		if idx.State != model.StatePublic || idx.Invisible {
			continue
		}
		if indexMatchesOrdering(idx, ds, orderingColIDs, equalityColIDs) {
			return true
		}
	}
	return false
}

func indexMatchesOrdering(
	idx *model.IndexInfo,
	ds *logicalop.DataSource,
	orderingColIDs []int64,
	equalityColIDs map[int64]struct{},
) bool {
	if len(orderingColIDs) == 0 {
		return false
	}
	orderPos := 0
	for _, idxCol := range idx.Columns {
		if idxCol.Offset >= len(ds.TableInfo.Columns) {
			return false
		}
		colID := ds.TableInfo.Columns[idxCol.Offset].ID
		if orderPos < len(orderingColIDs) && colID == orderingColIDs[orderPos] {
			orderPos++
			continue
		}
		if orderPos == 0 {
			if _, ok := equalityColIDs[colID]; ok {
				continue
			}
		}
		return false
	}
	return orderPos == len(orderingColIDs)
}

func collectEqualityPredicateColumnIDs(
	plan base.LogicalPlan,
	groupSelectionConds []expression.Expression,
	parentFilters []expression.Expression,
) map[int64]struct{} {
	result := make(map[int64]struct{})
	collectPlanLocalEqualityPredicateColumnIDs(plan, result)

	schema := plan.Schema()
	if schema == nil {
		return result
	}
	addEqualityColumnsFromLocalConds(result, schema, groupSelectionConds)
	addEqualityColumnsFromLocalConds(result, schema, parentFilters)
	return result
}

func collectPlanLocalEqualityPredicateColumnIDs(plan base.LogicalPlan, result map[int64]struct{}) {
	switch node := plan.(type) {
	case *logicalop.LogicalSelection:
		for _, cond := range node.Conditions {
			extractEqualityColumns(cond, result)
		}
	case *logicalop.DataSource:
		for _, cond := range node.AllConds {
			extractEqualityColumns(cond, result)
		}
	}
	for _, child := range plan.Children() {
		collectPlanLocalEqualityPredicateColumnIDs(child, result)
	}
}

func addEqualityColumnsFromLocalConds(result map[int64]struct{}, schema *expression.Schema, conds []expression.Expression) {
	for _, cond := range conds {
		if !condBelongsToSchema(cond, schema) {
			continue
		}
		extractEqualityColumns(cond, result)
	}
}

func condBelongsToSchema(cond expression.Expression, schema *expression.Schema) bool {
	if cond == nil || schema == nil {
		return false
	}
	cols := expression.ExtractColumns(cond)
	if len(cols) == 0 {
		return false
	}
	return slices.IndexFunc(cols, func(col *expression.Column) bool {
		return !schema.Contains(col)
	}) < 0
}

func extractEqualityColumns(expr expression.Expression, result map[int64]struct{}) {
	sf, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return
	}

	if sf.FuncName.L == ast.LogicAnd {
		for _, arg := range sf.GetArgs() {
			extractEqualityColumns(arg, result)
		}
		return
	}

	if sf.FuncName.L == ast.EQ && len(sf.GetArgs()) == 2 {
		col, ok := sf.GetArgs()[0].(*expression.Column)
		if ok && isDeterministicConstExpr(sf.GetArgs()[1]) && col.ID > 0 {
			result[col.ID] = struct{}{}
		}
		col, ok = sf.GetArgs()[1].(*expression.Column)
		if ok && isDeterministicConstExpr(sf.GetArgs()[0]) && col.ID > 0 {
			result[col.ID] = struct{}{}
		}
		return
	}

	if sf.FuncName.L == ast.In {
		args := sf.GetArgs()
		if len(args) < 2 {
			return
		}
		col, ok := args[0].(*expression.Column)
		if !ok || col.ID <= 0 {
			return
		}
		for _, arg := range args[1:] {
			if !isDeterministicConstExpr(arg) {
				return
			}
		}
		result[col.ID] = struct{}{}
	}
}

func isDeterministicConstExpr(expr expression.Expression) bool {
	return len(expression.ExtractColumns(expr)) == 0 && !expression.IsMutableEffectsExpr(expr)
}

func findDataSource(plan base.LogicalPlan) *logicalop.DataSource {
	if ds, ok := plan.(*logicalop.DataSource); ok {
		return ds
	}
	for _, child := range plan.Children() {
		if ds := findDataSource(child); ds != nil {
			return ds
		}
	}
	return nil
}
