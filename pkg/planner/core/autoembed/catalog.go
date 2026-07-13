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

package autoembed

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
)

// resolveDataSourceAutoEmbedInfo proves a provenance root from one public,
// visible, stored generated table column whose parsed expression is a direct
// supported EMBED_TEXT call. Catalog SQL text is never copied to plan columns.
func resolveDataSourceAutoEmbedInfo(ds *logicalop.DataSource, target *expression.Column) autoEmbedResolveResult {
	if ds == nil || ds.Schema() == nil || ds.Table == nil {
		return autoEmbedResolveResult{}
	}
	planCol, _, found, ambiguous := findAutoEmbedColumn(ds.Schema(), target)
	if ambiguous {
		return autoEmbedResolveResult{found: true}
	}
	if !found {
		return autoEmbedResolveResult{}
	}
	var matched *table.Column
	for _, tblCol := range ds.Table.Cols() {
		if tblCol == nil || tblCol.ColumnInfo == nil || tblCol.ID != planCol.ID {
			continue
		}
		if matched != nil {
			return autoEmbedResolveResult{found: true}
		}
		matched = tblCol
	}
	return autoEmbedInfoFromTableColumn(matched, planCol)
}

// resolveInsertTargetAutoEmbedInfo requires pointer identity in TableSchema.
// UniqueID matching is reserved for the SELECT snapshot, preventing a cloned
// source output from being reinterpreted as the destination table column.
func resolveInsertTargetAutoEmbedInfo(insert *physicalop.Insert, target *expression.Column) (*expression.AutoEmbedInfo, bool) {
	if insert == nil || insert.TableSchema == nil || insert.Table == nil || target == nil {
		return nil, false
	}
	matchedIdx := -1
	for idx, col := range insert.TableSchema.Columns {
		if col != target {
			continue
		}
		if matchedIdx >= 0 {
			return nil, true
		}
		matchedIdx = idx
	}
	if matchedIdx < 0 {
		return nil, false
	}
	cols := insert.Table.Cols()
	if matchedIdx >= len(cols) {
		return nil, true
	}
	result := autoEmbedInfoFromTableColumn(cols[matchedIdx], target)
	return result.info, true
}

// autoEmbedInfoFromTableColumn validates the catalog constraints shared by
// DataSource and INSERT target roots, then extracts immutable metadata.
func autoEmbedInfoFromTableColumn(tblCol *table.Column, planCol *expression.Column) autoEmbedResolveResult {
	result := autoEmbedResolveResult{found: true}
	if tblCol == nil || tblCol.ColumnInfo == nil || planCol == nil || planCol.RetType == nil ||
		tblCol.State != model.StatePublic || tblCol.Hidden || !tblCol.IsGenerated() || !tblCol.GeneratedStored ||
		!planCol.RetType.EvalType().IsVectorKind() || !autoEmbedFieldTypesCompatible(planCol.RetType, &tblCol.FieldType) ||
		tblCol.GeneratedExpr == nil || tblCol.GeneratedExpr.Internal() == nil ||
		!expression.IsAutoEmbedFnCallAST(tblCol.GeneratedExpr.Internal()) {
		return result
	}
	info, err := expression.ExtractAutoEmbedInfoFromAST(tblCol.GeneratedExpr.Internal())
	if err != nil {
		return result
	}
	result.info = copyAutoEmbedInfo(info)
	return result
}

// findAutoEmbedColumn prefers exact object identity, then accepts one
// type-compatible UniqueID match for cloning operators. Duplicate candidates
// are ambiguity, not an arbitrary first match.
func findAutoEmbedColumn(schema *expression.Schema, target *expression.Column) (matched *expression.Column, idx int, found bool, ambiguous bool) {
	if schema == nil || target == nil {
		return nil, -1, false, false
	}
	pointerIdx := -1
	for idx, col := range schema.Columns {
		if col != target {
			continue
		}
		if pointerIdx >= 0 {
			return nil, -1, false, true
		}
		pointerIdx = idx
	}
	if pointerIdx >= 0 {
		return schema.Columns[pointerIdx], pointerIdx, true, false
	}
	matchIdx := -1
	for idx, col := range schema.Columns {
		if !autoEmbedColumnsMatch(col, target) {
			continue
		}
		if matchIdx >= 0 {
			return nil, -1, false, true
		}
		matchIdx = idx
	}
	if matchIdx < 0 {
		return nil, -1, false, false
	}
	return schema.Columns[matchIdx], matchIdx, true, false
}

func findAutoEmbedColumnByID(schema *expression.Schema, id int64) *expression.Column {
	if schema == nil {
		return nil
	}
	var matched *expression.Column
	for _, col := range schema.Columns {
		if col == nil || col.UniqueID != id {
			continue
		}
		if matched != nil {
			return nil
		}
		matched = col
	}
	return matched
}

func autoEmbedColumnsMatch(left, right *expression.Column) bool {
	return left != nil && right != nil && left.UniqueID == right.UniqueID && autoEmbedVectorTypesCompatible(left, right)
}

func autoEmbedVectorTypesCompatible(left, right *expression.Column) bool {
	return left != nil && right != nil && autoEmbedFieldTypesCompatible(left.RetType, right.RetType)
}

// autoEmbedFieldTypesCompatible tolerates only NotNull drift because MaxOneRow
// clears that flag while keeping the same value. Dimensions and every other
// FieldType property remain part of the proof.
func autoEmbedFieldTypesCompatible(left, right *types.FieldType) bool {
	if left == nil || right == nil || !left.EvalType().IsVectorKind() || !right.EvalType().IsVectorKind() {
		return false
	}
	leftCopy := left.Clone()
	rightCopy := right.Clone()
	leftCopy.DelFlag(mysql.NotNullFlag)
	rightCopy.DelFlag(mysql.NotNullFlag)
	return leftCopy.Equals(rightCopy)
}

func copyAutoEmbedInfo(info *expression.AutoEmbedInfo) *expression.AutoEmbedInfo {
	if info == nil {
		return nil
	}
	cloned := *info
	return &cloned
}
