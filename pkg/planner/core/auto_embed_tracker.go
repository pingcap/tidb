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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// autoEmbedColumnTracker owns planner-local metadata for stored generated
// vector columns defined as EMBED_TEXT(...). Metadata is keyed by
// expression.Column.UniqueID, so every boundary that allocates a new output
// column must classify the boundary and update metadata through one of these
// intent methods.
//
// Boundary contract:
//
//	Boundary class | Examples                                      | Metadata rule
//	alias          | transparent projection, ORDER BY trim         | copy from source to the new UniqueID
//	derived        | cast, calculation, aggregation, distinct      | clear output metadata
//	opaque         | view, scalar subquery, apply right subtree    | clear the whole boundary schema
//	coalesced      | UNION ALL output column                       | copy only if all sources have identical metadata and compatible type
//	using          | USING/NATURAL common column                   | copy only if both sides are identical and compatible; otherwise clear output and sources
//
// DML paths must not keep mismatched USING/NATURAL metadata merely to preserve
// qualified source-column access. When source and coalesced output metadata
// cannot be represented separately, mismatches are invalidated.
type autoEmbedColumnTracker struct {
	infoByColumnID map[int64]*expression.AutoEmbedInfo
}

func newAutoEmbedColumnTracker() *autoEmbedColumnTracker {
	return &autoEmbedColumnTracker{infoByColumnID: make(map[int64]*expression.AutoEmbedInfo)}
}

func (t *autoEmbedColumnTracker) Reset() {
	if t == nil {
		return
	}
	if t.infoByColumnID == nil {
		t.infoByColumnID = make(map[int64]*expression.AutoEmbedInfo)
		return
	}
	for id := range t.infoByColumnID {
		delete(t.infoByColumnID, id)
	}
}

func (t *autoEmbedColumnTracker) ensureMap() {
	if t.infoByColumnID == nil {
		t.infoByColumnID = make(map[int64]*expression.AutoEmbedInfo)
	}
}

func cloneAutoEmbedInfo(info *expression.AutoEmbedInfo) *expression.AutoEmbedInfo {
	if info == nil {
		return nil
	}
	cloned := *info
	return &cloned
}

func (t *autoEmbedColumnTracker) RecordColumnID(id int64, info *expression.AutoEmbedInfo) {
	if t == nil || info == nil {
		return
	}
	t.ensureMap()
	t.infoByColumnID[id] = cloneAutoEmbedInfo(info)
}

func (t *autoEmbedColumnTracker) RecordColumn(col *expression.Column, info *expression.AutoEmbedInfo) {
	if col == nil {
		return
	}
	t.RecordColumnID(col.UniqueID, info)
}

func (t *autoEmbedColumnTracker) LookupColumnID(id int64) (*expression.AutoEmbedInfo, bool) {
	if t == nil || t.infoByColumnID == nil {
		return nil, false
	}
	info, ok := t.infoByColumnID[id]
	return info, ok
}

func (t *autoEmbedColumnTracker) LookupColumn(col *expression.Column) (*expression.AutoEmbedInfo, bool) {
	if col == nil {
		return nil, false
	}
	return t.LookupColumnID(col.UniqueID)
}

func (t *autoEmbedColumnTracker) copyToNewColumnID(dstID, srcID int64) bool {
	info, ok := t.LookupColumnID(srcID)
	if !ok {
		t.clearColumnID(dstID)
		return false
	}
	t.RecordColumnID(dstID, info)
	return true
}

func (t *autoEmbedColumnTracker) copyToNewColumn(dst, src *expression.Column) bool {
	if dst == nil || src == nil {
		return false
	}
	return t.copyToNewColumnID(dst.UniqueID, src.UniqueID)
}

func (t *autoEmbedColumnTracker) clearColumnID(id int64) {
	if t == nil || t.infoByColumnID == nil {
		return
	}
	delete(t.infoByColumnID, id)
}

func (t *autoEmbedColumnTracker) clearColumn(col *expression.Column) {
	if col == nil {
		return
	}
	t.clearColumnID(col.UniqueID)
}

func (t *autoEmbedColumnTracker) invalidateColumns(cols ...*expression.Column) {
	for _, col := range cols {
		t.clearColumn(col)
	}
}

func (t *autoEmbedColumnTracker) invalidateSchema(schema *expression.Schema) {
	if schema == nil {
		return
	}
	for _, col := range schema.Columns {
		t.clearColumn(col)
	}
}

func (t *autoEmbedColumnTracker) mergeIdenticalForOutput(outputID int64, inputIDs ...int64) bool {
	var mergedInfo *expression.AutoEmbedInfo
	for _, inputID := range inputIDs {
		info, ok := t.LookupColumnID(inputID)
		if !ok {
			t.clearColumnID(outputID)
			return false
		}
		if mergedInfo == nil {
			mergedInfo = info
			continue
		}
		if !mergedInfo.Equal(info) {
			t.clearColumnID(outputID)
			return false
		}
	}
	if mergedInfo == nil {
		t.clearColumnID(outputID)
		return false
	}
	t.RecordColumnID(outputID, mergedInfo)
	return true
}

func (t *autoEmbedColumnTracker) mergeIdenticalForOutputColumn(output *expression.Column, inputs ...*expression.Column) bool {
	if output == nil {
		return false
	}
	inputIDs := make([]int64, 0, len(inputs))
	for _, input := range inputs {
		if input == nil {
			t.clearColumn(output)
			return false
		}
		inputIDs = append(inputIDs, input.UniqueID)
	}
	return t.mergeIdenticalForOutput(output.UniqueID, inputIDs...)
}

func (t *autoEmbedColumnTracker) onProjectedAlias(newCol, srcCol *expression.Column) {
	t.copyToNewColumn(newCol, srcCol)
}

func (t *autoEmbedColumnTracker) onDerivedColumns(cols ...*expression.Column) {
	t.invalidateColumns(cols...)
}

func (t *autoEmbedColumnTracker) onOpaqueBoundary(schema *expression.Schema) {
	t.invalidateSchema(schema)
}

func (t *autoEmbedColumnTracker) onCoalescedColumn(out *expression.Column, srcs ...*expression.Column) bool {
	if out == nil {
		return false
	}
	for _, src := range srcs {
		if src == nil || out.RetType == nil || src.RetType == nil || !src.RetType.Equal(out.RetType) {
			t.clearColumn(out)
			return false
		}
	}
	return t.mergeIdenticalForOutputColumn(out, srcs...)
}

func (t *autoEmbedColumnTracker) onUsingCoalesce(out, left, right *expression.Column) bool {
	if out == nil || left == nil || right == nil ||
		out.RetType == nil || left.RetType == nil || right.RetType == nil ||
		!left.RetType.Equal(out.RetType) || !right.RetType.Equal(out.RetType) ||
		!t.mergeIdenticalForOutputColumn(out, left, right) {
		t.invalidateColumns(out, left, right)
		return false
	}
	return true
}

func (b *PlanBuilder) autoEmbedTracker() *autoEmbedColumnTracker {
	if b.autoEmbedColumnTracker == nil {
		b.autoEmbedColumnTracker = newAutoEmbedColumnTracker()
	}
	return b.autoEmbedColumnTracker
}

func (b *PlanBuilder) onAutoEmbedOpaqueBoundary(schema *expression.Schema) {
	if intest.InTest {
		b.autoEmbedOracle().recordOpaqueBoundary(schema)
	}
	if intest.InTest && autoEmbedOracleSkipOpaqueBoundaryInvalidationForTest() {
		return
	}
	b.autoEmbedTracker().onOpaqueBoundary(schema)
}

// cloneSchemaReallocIDs clones schema, allocates a fresh UniqueID for each
// column, and propagates auto-embed metadata positionally. It is only for
// transparent projection or set-operation finalization where each output column
// is a pure alias of the source column at the same position.
func (b *PlanBuilder) cloneSchemaReallocIDs(src *expression.Schema) *expression.Schema {
	if src == nil {
		return nil
	}
	schema := src.Clone()
	for i, col := range schema.Columns {
		col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
		b.autoEmbedTracker().onProjectedAlias(col, src.Columns[i])
	}
	return schema
}

func (b *PlanBuilder) recordAutoEmbedColumnFromTableColumn(tblCol *table.Column, planCol *expression.Column) error {
	if tblCol == nil || planCol == nil || planCol.RetType == nil {
		return nil
	}
	if tblCol.State != model.StatePublic || tblCol.Hidden || !tblCol.IsGenerated() || !tblCol.GeneratedStored {
		return nil
	}
	if !planCol.RetType.EvalType().IsVectorKind() {
		return nil
	}
	if tblCol.GeneratedExpr == nil || tblCol.GeneratedExpr.Internal() == nil {
		return errors.Errorf("generated expression for column %s is unavailable", tblCol.Name.O)
	}
	genExpr := tblCol.GeneratedExpr.Internal()
	if !expression.IsAutoEmbedFnCallAST(genExpr) {
		return nil
	}
	info, err := expression.ExtractAutoEmbedInfoFromAST(genExpr)
	if err != nil {
		return errors.Annotatef(err, "column %s cannot be used as auto-embedding vector column", tblCol.Name.O)
	}
	b.autoEmbedTracker().RecordColumn(planCol, info)
	return nil
}

func (b *PlanBuilder) recordAutoEmbedColumnsFromTableColumns(tblCols []*table.Column, schema *expression.Schema) error {
	if schema == nil {
		return nil
	}
	for i, tblCol := range tblCols {
		if tblCol == nil {
			continue
		}
		idx := i
		if tblCol.Offset >= 0 && tblCol.Offset < schema.Len() {
			idx = tblCol.Offset
		}
		if idx < 0 || idx >= schema.Len() {
			continue
		}
		if err := b.recordAutoEmbedColumnFromTableColumn(tblCol, schema.Columns[idx]); err != nil {
			return err
		}
	}
	return nil
}
