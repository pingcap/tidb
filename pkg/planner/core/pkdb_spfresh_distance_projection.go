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

package core

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/types"
)

// tryEnableSPFreshVectorSearchDistanceProjection enables a SPFresh-only distance projection rewrite:
//   - Always: ensure SPFreshScan outputs a virtual distance column and rewrite the ordering to use it,
//     so TiDB won't re-calculate vec_*_distance in execution.
//   - Optional: if the vector column becomes unused after the rewrite, remove it from SPFreshScan output,
//     so `/spfresh/search.required_columns` won't contain it.
//
// This is intentionally isolated from the existing TiFlash/HNSW logic (task.go:tryEnableVectorSearchDistanceProjection).
func tryEnableSPFreshVectorSearchDistanceProjection(plan base.PhysicalPlan) {
	spfreshDistanceProjTraverse(plan, nil)
}

func spfreshDistanceProjTraverse(p base.PhysicalPlan, parent base.PhysicalPlan) {
	if topN, ok := p.(*PhysicalTopN); ok {
		tryEnableSPFreshDistanceProjectionOnTopN(parent, topN)
	} else if limit, ok := p.(*PhysicalLimit); ok {
		tryEnableSPFreshDistanceProjectionOnLimit(parent, limit)
	}
	for _, child := range p.Children() {
		spfreshDistanceProjTraverse(child, p)
	}
}

func tryEnableSPFreshDistanceProjectionOnTopN(parent base.PhysicalPlan, topN *PhysicalTopN) {
	if topN == nil || len(topN.Children()) != 1 {
		return
	}
	if len(topN.ByItems) != 1 || topN.ByItems[0].Desc {
		// Vector search only supports single-item ascending order for now.
		return
	}
	var (
		scan       *PhysicalSPFreshVectorScan
		bottomProj *PhysicalProjection
		orderIdx   int
		vs         *expression.VectorHelper
	)

	switch child := topN.Children()[0].(type) {
	case *PhysicalSPFreshVectorScan:
		scan = child
		// TopN(order by vec_*_distance(vec_col, const)) -> SPFreshScan
		vs = expression.ExtractVectorHelper(topN.ByItems[0].Expr)
	case *PhysicalProjection:
		// TopN(order by Column(distance)) -> Projection(vec_*_distance(vec_col, const)) -> SPFreshScan
		bottomProj = child
		if len(bottomProj.Children()) != 1 {
			return
		}
		var ok bool
		scan, ok = bottomProj.Children()[0].(*PhysicalSPFreshVectorScan)
		if !ok {
			return
		}
		orderByCol, ok := topN.ByItems[0].Expr.(*expression.Column)
		if !ok {
			return
		}
		orderIdx = orderByCol.Index
		if orderIdx < 0 || orderIdx >= len(bottomProj.Exprs) {
			return
		}
		vs = expression.ExtractVectorHelper(bottomProj.Exprs[orderIdx])
	default:
		return
	}

	if scan == nil || scan.Index == nil || !scan.Index.IsSPFreshIndex() {
		return
	}
	if vs == nil || vs.Column == nil {
		return
	}
	if !spfreshVectorHelperMatchesScan(scan, vs) {
		return
	}
	vecColID := vs.Column.ID

	distanceIdx := ensureSPFreshDistanceColumn(scan)
	if distanceIdx < 0 {
		return
	}
	scan.EnableDistanceProj = true

	// Rewrite the ordering to use worker-returned distance, avoiding TiDB-side re-calculation.
	distCol := scan.Schema().Columns[distanceIdx].Clone().(*expression.Column)
	distCol.Index = distanceIdx
	if bottomProj == nil {
		topN.ByItems[0].Expr = distCol
	} else {
		// Replace the heavy distance function in the projection with a direct distance column reference.
		bottomProj.Exprs[orderIdx] = distCol
	}

	// Optional: remove the vector column from scan output when it becomes unused after the rewrite.
	if bottomProj != nil {
		// The TopN reads from the projection output. If the projection doesn't need vec in its expressions
		// after the rewrite, and nothing above TopN reads vec, we can remove vec from scan output.
		vecOutIdx := findSchemaColumnIdxByID(bottomProj.Schema(), vecColID)
		for i, expr := range bottomProj.Exprs {
			// Ignore the passthrough output column itself. We'll remove it if it's unused above.
			if i == vecOutIdx {
				continue
			}
			if expression.HasColumnWithCondition(expr, func(col *expression.Column) bool {
				return col.ID == vecColID
			}) {
				return
			}
		}
		// Generated columns may still depend on vec.
		if virtualGeneratedDependsOnColumnID(scan.Columns, vecColID) {
			return
		}
		if vecOutIdx >= 0 {
			// Only remove vec from the projection output if nothing above TopN uses it.
			proj, ok := parent.(*PhysicalProjection)
			if !ok || proj == nil {
				return
			}
			for _, expr := range proj.Exprs {
				if expression.HasColumnWithCondition(expr, func(col *expression.Column) bool {
					return col.ID == vecColID
				}) {
					return
				}
			}
			bottomProj.Exprs = removeExprByIdx(bottomProj.Exprs, vecOutIdx)
			removeSchemaColumnByIdx(bottomProj.Schema(), vecOutIdx)
			// Release TopN shares its child's schema, so removing from the child
			// already updates TopN. Do not remove the same slot twice.
			rebindProjectionExprColumns(proj, topN.Schema())
			// Refresh TopN's order-by column index if it's a direct column reference.
			if orderByCol, ok := topN.ByItems[0].Expr.(*expression.Column); ok {
				orderByCol.Index = bottomProj.Schema().ColumnIndex(orderByCol)
			}
		}
		scanColIdx := findColumnInfoIdxByID(scan.Columns, vecColID)
		scanSchemaIdx := findSchemaColumnIdxByID(scan.Schema(), vecColID)
		if scanColIdx < 0 || scanSchemaIdx < 0 || scanColIdx != scanSchemaIdx {
			return
		}
		scan.Columns = removeColumnInfoByIdx(scan.Columns, scanColIdx)
		removeSchemaColumnByIdx(scan.Schema(), scanSchemaIdx)

		rebindProjectionExprColumns(bottomProj, scan.Schema())
		return
	}

	// Simple removal case without a bottom projection: use parent Projection to decide vec is unused.
	proj, ok := parent.(*PhysicalProjection)
	if !ok || proj == nil {
		return
	}
	for _, expr := range proj.Exprs {
		if expression.HasColumnWithCondition(expr, func(col *expression.Column) bool {
			return col.ID == vecColID
		}) {
			return
		}
	}
	// Generated columns may still depend on vec.
	if virtualGeneratedDependsOnColumnID(scan.Columns, vecColID) {
		return
	}
	scanColIdx := findColumnInfoIdxByID(scan.Columns, vecColID)
	scanSchemaIdx := findSchemaColumnIdxByID(scan.Schema(), vecColID)
	topNSchemaIdx := findSchemaColumnIdxByID(topN.Schema(), vecColID)
	if scanColIdx < 0 || scanSchemaIdx < 0 || topNSchemaIdx < 0 || scanColIdx != scanSchemaIdx {
		return
	}
	scan.Columns = removeColumnInfoByIdx(scan.Columns, scanColIdx)
	removeSchemaColumnByIdx(scan.Schema(), scanSchemaIdx)

	rebindProjectionExprColumns(proj, topN.Schema())

	// The scan schema was changed. Refresh the final distance column index for TopN's order-by column.
	distanceIdx = scan.Schema().Len() - 1
	if distanceIdx >= 0 {
		topN.ByItems[0].Expr.(*expression.Column).Index = distanceIdx
	}
}

func tryEnableSPFreshDistanceProjectionOnLimit(parent base.PhysicalPlan, limit *PhysicalLimit) {
	if limit == nil || len(limit.Children()) != 1 {
		return
	}
	if scan, ok := limit.Children()[0].(*PhysicalSPFreshVectorScan); ok {
		tryPruneSPFreshVectorColumnOnLimit(parent, limit, scan)
		return
	}
	bottomProj, ok := limit.Children()[0].(*PhysicalProjection)
	if !ok || len(bottomProj.Children()) != 1 {
		return
	}
	scan, ok := bottomProj.Children()[0].(*PhysicalSPFreshVectorScan)
	if !ok || scan.Index == nil || !scan.Index.IsSPFreshIndex() {
		return
	}

	distExprIdx := -1
	var vs *expression.VectorHelper
	for i, expr := range bottomProj.Exprs {
		helper := expression.ExtractVectorHelper(expr)
		if helper == nil || helper.Column == nil {
			continue
		}
		if !spfreshVectorHelperMatchesScan(scan, helper) {
			continue
		}
		distExprIdx = i
		vs = helper
		break
	}
	if distExprIdx < 0 || vs == nil || vs.Column == nil {
		return
	}
	vecColID := vs.Column.ID

	distanceIdx := ensureSPFreshDistanceColumn(scan)
	if distanceIdx < 0 {
		return
	}
	scan.EnableDistanceProj = true

	// Replace the heavy distance function in the projection with the worker-returned distance column.
	distCol := scan.Schema().Columns[distanceIdx].Clone().(*expression.Column)
	distCol.Index = distanceIdx
	bottomProj.Exprs[distExprIdx] = distCol

	// If nothing above Limit needs the projected distance column, drop it entirely.
	if proj, ok := parent.(*PhysicalProjection); ok && proj != nil {
		distOutColID := bottomProj.Schema().Columns[distExprIdx].ID
		usedAbove := false
		for _, expr := range proj.Exprs {
			if expressionUsesColumnID(expr, distOutColID) {
				usedAbove = true
				break
			}
		}
		if !usedAbove {
			bottomProj.Exprs = removeExprByIdx(bottomProj.Exprs, distExprIdx)
			removeSchemaColumnByIdx(bottomProj.Schema(), distExprIdx)
			if limitSchemaIdx := findSchemaColumnIdxByID(limit.Schema(), distOutColID); limitSchemaIdx >= 0 {
				removeSchemaColumnByIdx(limit.Schema(), limitSchemaIdx)
			}
			removeScanColumnByID(scan, model.VirtualColVecSearchDistanceID)
			scan.EnableDistanceProj = false
			rebindProjectionExprColumns(proj, limit.Schema())
		}
	}

	vecOutIdx := findSchemaColumnIdxByID(bottomProj.Schema(), vecColID)
	for i, expr := range bottomProj.Exprs {
		if i == vecOutIdx && isDirectColumnExpr(expr, vecColID) {
			// Ignore a passthrough vector output column. It can be removed below
			// if the Limit parent does not need it.
			continue
		}
		if expressionUsesColumnID(expr, vecColID) {
			return
		}
	}
	if virtualGeneratedDependsOnColumnID(scan.Columns, vecColID) {
		return
	}
	if vecOutIdx >= 0 {
		proj, ok := parent.(*PhysicalProjection)
		if !ok || proj == nil || projectionUsesColumnID(proj, vecColID) {
			return
		}
		bottomProj.Exprs = removeExprByIdx(bottomProj.Exprs, vecOutIdx)
		removeSchemaColumnByIdx(bottomProj.Schema(), vecOutIdx)
		if limitSchemaIdx := findSchemaColumnIdxByID(limit.Schema(), vecColID); limitSchemaIdx >= 0 {
			removeSchemaColumnByIdx(limit.Schema(), limitSchemaIdx)
		}
		rebindProjectionExprColumns(proj, limit.Schema())
	}
	if !removeScanColumnByID(scan, vecColID) {
		return
	}

	rebindProjectionExprColumns(bottomProj, scan.Schema())
	_ = resolveIndexForInlineProjection(&limit.physicalSchemaProducer)
}

func tryPruneSPFreshVectorColumnOnLimit(parent base.PhysicalPlan, limit *PhysicalLimit, scan *PhysicalSPFreshVectorScan) {
	if scan == nil || limit == nil {
		return
	}
	vecColID, ok := spfreshScanVectorColumnID(scan)
	if !ok {
		return
	}
	proj, ok := parent.(*PhysicalProjection)
	if ok && proj != nil {
		enableDistanceProjectionOnLimitParentProjection(proj, limit, scan, vecColID)
		if projectionUsesColumnID(proj, vecColID) {
			rebindProjectionExprColumns(proj, limit.Schema())
			return
		}
	} else if findSchemaColumnIdxByID(limit.Schema(), vecColID) >= 0 {
		return
	}
	if limitSchemaIdx := findSchemaColumnIdxByID(limit.Schema(), vecColID); limitSchemaIdx >= 0 {
		removeSchemaColumnByIdx(limit.Schema(), limitSchemaIdx)
	}
	if virtualGeneratedDependsOnColumnID(scan.Columns, vecColID) {
		if proj != nil {
			rebindProjectionExprColumns(proj, limit.Schema())
		}
		return
	}
	scanColIdx := findColumnInfoIdxByID(scan.Columns, vecColID)
	scanSchemaIdx := findSchemaColumnIdxByID(scan.Schema(), vecColID)
	if scanColIdx < 0 || scanSchemaIdx < 0 || scanColIdx != scanSchemaIdx {
		if proj != nil {
			rebindProjectionExprColumns(proj, limit.Schema())
		}
		return
	}
	scan.Columns = removeColumnInfoByIdx(scan.Columns, scanColIdx)
	removeSchemaColumnByIdx(scan.Schema(), scanSchemaIdx)
	_ = resolveIndexForInlineProjection(&limit.physicalSchemaProducer)
	if proj != nil {
		rebindProjectionExprColumns(proj, limit.Schema())
	}
}

func ensureSPFreshDistanceColumn(scan *PhysicalSPFreshVectorScan) int {
	if scan.Schema() == nil {
		return -1
	}
	for _, c := range scan.Columns {
		if c != nil && c.ID == model.VirtualColVecSearchDistanceID {
			// Already appended.
			refreshSchemaColumnIndexes(scan.Schema())
			return scan.Schema().Len() - 1
		}
	}

	scan.Columns = append(scan.Columns, &model.ColumnInfo{
		ID:        model.VirtualColVecSearchDistanceID,
		FieldType: *types.NewFieldType(mysql.TypeDouble),
		Offset:    len(scan.Columns),
	})
	distCol := &expression.Column{
		UniqueID: scan.SCtx().GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldType(mysql.TypeDouble),
		ID:       model.VirtualColVecSearchDistanceID,
	}
	scan.Schema().Append(distCol)
	refreshColumnInfoOffsets(scan.Columns)
	refreshSchemaColumnIndexes(scan.Schema())
	return scan.Schema().Len() - 1
}

func findColumnInfoIdxByID(cols []*model.ColumnInfo, colID int64) int {
	for i := range cols {
		if cols[i] != nil && cols[i].ID == colID {
			return i
		}
	}
	return -1
}

func findSchemaColumnIdxByID(schema *expression.Schema, colID int64) int {
	if schema == nil {
		return -1
	}
	for i := range schema.Columns {
		if schema.Columns[i] != nil && schema.Columns[i].ID == colID {
			return i
		}
	}
	return -1
}

func removeColumnInfoByIdx(cols []*model.ColumnInfo, idx int) []*model.ColumnInfo {
	copy(cols[idx:], cols[idx+1:])
	cols[len(cols)-1] = nil
	cols = cols[:len(cols)-1]
	refreshColumnInfoOffsets(cols)
	return cols
}

func removeSchemaColumnByIdx(schema *expression.Schema, idx int) {
	copy(schema.Columns[idx:], schema.Columns[idx+1:])
	schema.Columns[len(schema.Columns)-1] = nil
	schema.Columns = schema.Columns[:len(schema.Columns)-1]
	refreshSchemaColumnIndexes(schema)
}

func removeScanColumnByID(scan *PhysicalSPFreshVectorScan, colID int64) bool {
	scanColIdx := findColumnInfoIdxByID(scan.Columns, colID)
	scanSchemaIdx := findSchemaColumnIdxByID(scan.Schema(), colID)
	if scanColIdx < 0 || scanSchemaIdx < 0 || scanColIdx != scanSchemaIdx {
		return false
	}
	scan.Columns = removeColumnInfoByIdx(scan.Columns, scanColIdx)
	removeSchemaColumnByIdx(scan.Schema(), scanSchemaIdx)
	return true
}

func projectionUsesColumnID(proj *PhysicalProjection, colID int64) bool {
	for _, expr := range proj.Exprs {
		if expressionUsesColumnID(expr, colID) {
			return true
		}
	}
	return false
}

func expressionUsesColumnID(expr expression.Expression, colID int64) bool {
	return expression.HasColumnWithCondition(expr, func(col *expression.Column) bool {
		return col.ID == colID
	})
}

func isDirectColumnExpr(expr expression.Expression, colID int64) bool {
	col, ok := expr.(*expression.Column)
	return ok && col.ID == colID
}

func removeExprByIdx(exprs []expression.Expression, idx int) []expression.Expression {
	copy(exprs[idx:], exprs[idx+1:])
	exprs[len(exprs)-1] = nil
	return exprs[:len(exprs)-1]
}

func virtualGeneratedDependsOnColumnID(cols []*model.ColumnInfo, colID int64) bool {
	var colName string
	for _, c := range cols {
		if c != nil && c.ID == colID {
			colName = c.Name.L
			break
		}
	}
	if colName == "" {
		return false
	}
	for _, c := range cols {
		if c == nil || !c.IsVirtualGenerated() || len(c.Dependences) == 0 {
			continue
		}
		if _, ok := c.Dependences[colName]; ok {
			return true
		}
	}
	return false
}

func refreshSchemaColumnIndexes(schema *expression.Schema) {
	if schema == nil {
		return
	}
	for i, col := range schema.Columns {
		if col != nil {
			col.Index = i
		}
	}
}

func refreshColumnInfoOffsets(cols []*model.ColumnInfo) {
	for i, col := range cols {
		if col != nil {
			col.Offset = i
		}
	}
}

func enableDistanceProjectionOnLimitParentProjection(proj *PhysicalProjection, limit *PhysicalLimit, scan *PhysicalSPFreshVectorScan, vecColID int64) {
	if proj == nil || limit == nil || scan == nil {
		return
	}

	found := false
	for _, expr := range proj.Exprs {
		helper := expression.ExtractVectorHelper(expr)
		if helper != nil && helper.Column != nil && helper.Column.ID == vecColID && spfreshVectorHelperMatchesScan(scan, helper) {
			found = true
			break
		}
	}
	if !found {
		return
	}

	distanceIdx := ensureSPFreshDistanceColumn(scan)
	if distanceIdx < 0 {
		return
	}
	scan.EnableDistanceProj = true

	limitDistanceIdx := findSchemaColumnIdxByID(limit.Schema(), model.VirtualColVecSearchDistanceID)
	if limitDistanceIdx < 0 {
		distCol := scan.Schema().Columns[distanceIdx].Clone().(*expression.Column)
		limit.Schema().Append(distCol)
		refreshSchemaColumnIndexes(limit.Schema())
		limitDistanceIdx = limit.Schema().Len() - 1
	}

	distCol := limit.Schema().Columns[limitDistanceIdx].Clone().(*expression.Column)
	for i, expr := range proj.Exprs {
		helper := expression.ExtractVectorHelper(expr)
		if helper == nil || helper.Column == nil || helper.Column.ID != vecColID || !spfreshVectorHelperMatchesScan(scan, helper) {
			continue
		}
		proj.Exprs[i] = distCol.Clone().(*expression.Column)
	}
	_ = resolveIndexForInlineProjection(&limit.physicalSchemaProducer)
}

func spfreshScanVectorColumnID(scan *PhysicalSPFreshVectorScan) (int64, bool) {
	if scan == nil {
		return 0, false
	}
	vecColID, err := model.VectorIndexColumnID(scan.Table, scan.Index)
	if err != nil {
		return 0, false
	}
	return vecColID, true
}

func spfreshVectorHelperMatchesScan(scan *PhysicalSPFreshVectorScan, helper *expression.VectorHelper) bool {
	if scan == nil {
		return false
	}
	if !vectorHelperMatchesIndex(scan.Table, scan.Index, helper) {
		return false
	}
	if scan.QueryVecParam != nil || helper.VecParam != nil {
		// Equal values at the first execution do not imply equal future bindings.
		return scan.QueryVecParam != nil && helper.VecParam != nil && scan.QueryVecParam.Equals(helper.VecParam)
	}
	return helper.Vec.Compare(scan.QueryVec) == 0
}

func rebindProjectionExprColumns(proj *PhysicalProjection, childSchema *expression.Schema) {
	if proj == nil || childSchema == nil {
		return
	}
	for _, expr := range proj.Exprs {
		for _, col := range expression.ExtractColumns(expr) {
			col.Index = childSchema.ColumnIndex(col)
		}
	}
}
