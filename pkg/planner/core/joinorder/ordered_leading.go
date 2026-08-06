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

// OrderedLeadingChoice records the vertex that carries the ordering requirement
// within the current join group and, when possible, the single-table identity
// that can be bridged into an internal LEADING hint on the group's anchor join.
type OrderedLeadingChoice struct {
	CarrierVertex base.LogicalPlan
	LeadingTable  *hint.HintedTable
	Vertices      []base.LogicalPlan
}

// FindOrderedLeadingChoice returns the vertex that should keep carrying the
// current ordering requirement inside this join group. The actual question of
// whether that vertex's subtree can really preserve the order is handled by the
// recursive caller.
func FindOrderedLeadingChoice(root base.LogicalPlan, orderingCols []*expression.Column) *OrderedLeadingChoice {
	if root == nil || len(orderingCols) == 0 {
		return nil
	}

	group := extractJoinGroup(root)
	if len(group.vertexes) <= 1 {
		return nil
	}

	return findOrderedLeadingChoice(group, orderingCols)
}

// TryAnnotateOrderedLeading attaches an internal LEADING preference to the top
// join of the current join group once the chosen carrier vertex has already
// proven, through recursive exploration, that its subtree can satisfy the
// ordering requirement.
func TryAnnotateOrderedLeading(root base.LogicalPlan, choice *OrderedLeadingChoice) bool {
	if root == nil || choice == nil {
		return false
	}

	group := extractJoinGroup(root)
	anchor := findLeadingHintAnchor(group.root)
	if len(group.leadingHints) > 0 || anchor == nil || anchor.PreferJoinOrder || anchor.InternalPreferJoinOrder ||
		anchor.PreferJoinType > 0 || anchor.HintInfo != nil || anchor.InternalHintInfo != nil || choice.LeadingTable == nil {
		return false
	}

	// Record the ordered-leading choice separately from user hints so downstream
	// warning paths can keep treating user-provided LEADING differently from this
	// synthesized preference.
	anchor.InternalPreferJoinOrder = true
	anchor.InternalHintInfo = buildSingleTableLeadingHint(choice.LeadingTable)
	return true
}

// findLeadingHintAnchor returns the top join that should receive the internal
// LEADING hint for the current join group. The group root may be wrapped by a
// Selection because TiDB can keep a pushed-down filter above a reorderable join
// group, so we skip such wrappers and stop once the tree shape is no longer
// "Selection -> Join".
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
			Items: []any{
				&ast.HintTable{
					DBName:    table.DBName,
					TableName: table.TblName,
					QBName:    qbName,
				},
			},
		},
	}
}

// findOrderedLeadingChoice picks one vertex that should keep carrying the
// ordering requirement inside the current join group. Each ordering vector must
// still belong to one vertex as a whole instead of column-by-column. For
// example, if the join group is {t1, t2, t3} and the required order is
// (t2.a, t2.b), we only pick a vertex whose schema contains both columns. The
// recursive caller will then keep exploring that vertex's subtree to prove
// whether the order can really be preserved. If the chosen vertex can also be
// represented as a single hinted table, we additionally return the bridged
// LEADING target for the current anchor join.
func findOrderedLeadingChoice(group *joinGroup, orderingCols []*expression.Column) *OrderedLeadingChoice {
	_, orderingColUniqueIDs := normalizeOrderingColumns(orderingCols)
	if len(orderingColUniqueIDs) == 0 || len(orderingColUniqueIDs) != len(orderingCols) {
		return nil
	}
	for _, vertex := range group.vertexes {
		if !schemaContainsAllOrderingColumns(vertex, orderingColUniqueIDs) {
			continue
		}
		choice := &OrderedLeadingChoice{CarrierVertex: vertex}
		// The candidate may still be rejected by the caller if the current group
		// cannot bridge it into a single-table internal LEADING hint.
		choice.LeadingTable = util.ExtractTableAlias(vertex, vertex.QueryBlockOffset())
		choice.Vertices = group.vertexes
		return choice
	}
	return nil
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

// DsSatisfiesOrdering proves that the chosen DataSource can provide the
// requested ordering locally after the order-aware rule has recursively
// descended to the carrier leaf.
func DsSatisfiesOrdering(
	ds *logicalop.DataSource,
	orderingCols []*expression.Column,
	parentFilters []expression.Expression,
) bool {
	if ds == nil {
		return false
	}
	orderingColIDs, orderingColUniqueIDs := normalizeOrderingColumns(orderingCols)
	if len(orderingColIDs) == 0 || len(orderingColIDs) != len(orderingCols) {
		return false
	}
	if !schemaContainsAllOrderingColumns(ds, orderingColUniqueIDs) {
		return false
	}
	return tableHasIndexMatchingOrdering(ds, orderingColIDs, nil, parentFilters)
}

func tableHasIndexMatchingOrdering(
	ds *logicalop.DataSource,
	orderingColIDs []int64,
	groupSelectionConds []expression.Expression,
	parentFilters []expression.Expression,
) bool {
	if ds == nil {
		return false
	}

	// parentFilters carries deterministic single-table predicates collected from
	// ancestors outside the current join group. They matter because they can fix
	// leading index columns even when the ORDER BY itself only mentions a suffix.
	// Example:
	//   where t.category = 'hot' order by t.id
	// can still use index(category, id) to preserve the order of t.id.
	equalityColIDs := collectEqualityPredicateColumnIDs(ds, groupSelectionConds, parentFilters)
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
			if orderPos == len(orderingColIDs) {
				return true
			}
			continue
		}
		if orderPos == 0 {
			// Equality predicates let us skip unmatched index-prefix columns before
			// we consume the first ORDER BY column. Example:
			//   index(category, id)
			//   where category = 'hot'
			//   order by id
			// The category prefix is fixed to one value, so scanning the remaining
			// suffix still preserves the order of id. Once we have started matching
			// ORDER BY columns, any mismatch breaks the required order.
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
	// The join-group Selection conditions and ancestor filters are both optional
	// sources of single-table equalities. We only keep predicates whose columns
	// all belong to the current vertex, so passing ancestor filters to child
	// groups is safe even when those filters mention other tables.
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

	switch sf.FuncName.L {
	case ast.LogicAnd:
		for _, arg := range sf.GetArgs() {
			extractEqualityColumns(arg, result)
		}
		return
	case ast.EQ:
		if len(sf.GetArgs()) != 2 {
			return
		}
		// We only treat const equalities as index-prefix eliminators. For
		// example, index(category, id) can satisfy "where category = 'hot'
		// order by id". Keeping this conservative EQ-to-const case still helps
		// TP-style queries without pulling in general column-equivalence
		// reasoning such as inferring order from "a = b".
		col, ok := sf.GetArgs()[0].(*expression.Column)
		if ok && isDeterministicConstExpr(sf.GetArgs()[1]) && col.ID > 0 {
			result[col.ID] = struct{}{}
		}
		col, ok = sf.GetArgs()[1].(*expression.Column)
		if ok && isDeterministicConstExpr(sf.GetArgs()[0]) && col.ID > 0 {
			result[col.ID] = struct{}{}
		}
		return
	case ast.In:
		args := sf.GetArgs()
		// Only singleton IN behaves like a fixed prefix here. Multi-value IN still
		// scans multiple point ranges on the leading index column, which does not
		// preserve the global order of later index columns.
		if len(args) != 2 {
			return
		}
		col, ok := args[0].(*expression.Column)
		if !ok || col.ID <= 0 {
			return
		}
		if !isDeterministicConstExpr(args[1]) {
			return
		}
		result[col.ID] = struct{}{}
	}
}

func isDeterministicConstExpr(expr expression.Expression) bool {
	return len(expression.ExtractColumns(expr)) == 0 && !expression.IsMutableEffectsExpr(expr)
}
