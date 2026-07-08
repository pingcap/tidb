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
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/table"
)

type autoEmbedOracleLineage map[int64]*expression.AutoEmbedInfo

type autoEmbedLineageObservation struct {
	plan      base.LogicalPlan
	function  string
	colName   string
	colID     int64
	embedInfo *expression.AutoEmbedInfo
}

type autoEmbedLineageOracle struct {
	observations   []autoEmbedLineageObservation
	opaqueColumnID map[int64]struct{}
}

var autoEmbedOracleSkipOpaqueBoundary atomic.Bool

func newAutoEmbedLineageOracle() *autoEmbedLineageOracle {
	return &autoEmbedLineageOracle{
		opaqueColumnID: make(map[int64]struct{}),
	}
}

func (b *PlanBuilder) autoEmbedOracle() *autoEmbedLineageOracle {
	if b.autoEmbedLineageOracle == nil {
		b.autoEmbedLineageOracle = newAutoEmbedLineageOracle()
	}
	return b.autoEmbedLineageOracle
}

func (o *autoEmbedLineageOracle) reset() {
	if o == nil {
		return
	}
	o.observations = o.observations[:0]
	if o.opaqueColumnID == nil {
		o.opaqueColumnID = make(map[int64]struct{})
		return
	}
	for id := range o.opaqueColumnID {
		delete(o.opaqueColumnID, id)
	}
}

func (o *autoEmbedLineageOracle) beginBuild() int {
	if o == nil {
		return 0
	}
	return len(o.observations)
}

func (o *autoEmbedLineageOracle) finishBuild(mark int, _ base.Plan, buildErr error) error {
	if o == nil {
		return buildErr
	}
	if mark < 0 || mark > len(o.observations) {
		mark = len(o.observations)
	}
	defer func() {
		o.observations = o.observations[:mark]
	}()
	if buildErr != nil {
		return buildErr
	}
	for _, obs := range o.observations[mark:] {
		if err := o.validateObservation(obs); err != nil {
			return err
		}
	}
	return nil
}

func (o *autoEmbedLineageOracle) recordOpaqueBoundary(schema *expression.Schema) {
	if o == nil || schema == nil {
		return
	}
	if o.opaqueColumnID == nil {
		o.opaqueColumnID = make(map[int64]struct{})
	}
	for _, col := range schema.Columns {
		if col != nil {
			o.opaqueColumnID[col.UniqueID] = struct{}{}
		}
	}
}

func (o *autoEmbedLineageOracle) recordVectorSearchHit(plan base.LogicalPlan, function, colName string, col *expression.Column, info *expression.AutoEmbedInfo) {
	if o == nil || plan == nil || col == nil || info == nil {
		return
	}
	o.observations = append(o.observations, autoEmbedLineageObservation{
		plan:      plan,
		function:  function,
		colName:   colName,
		colID:     col.UniqueID,
		embedInfo: cloneAutoEmbedInfo(info),
	})
}

func (o *autoEmbedLineageOracle) validateObservation(obs autoEmbedLineageObservation) error {
	lineage := o.deriveLineage(obs.plan)
	oracleInfo, ok := lineage[obs.colID]
	if ok && oracleInfo.Equal(obs.embedInfo) {
		return nil
	}
	if obs.colName == "" {
		obs.colName = "<unknown column>"
	}
	if ok {
		return errors.Errorf(
			"auto-embed lineage oracle rejected %s() tracker hit for %s (UniqueID=%d): tracker metadata does not match independently derived lineage",
			obs.function, obs.colName, obs.colID,
		)
	}
	return errors.Errorf(
		"auto-embed lineage oracle rejected %s() tracker hit for %s (UniqueID=%d): column is not traceable through transparent logical plan lineage",
		obs.function, obs.colName, obs.colID,
	)
}

func (o *autoEmbedLineageOracle) deriveLineage(plan base.LogicalPlan) autoEmbedOracleLineage {
	if plan == nil {
		return autoEmbedOracleLineage{}
	}
	switch p := plan.(type) {
	case *logicalop.DataSource:
		return o.deriveDataSourceLineage(p)
	case *logicalop.LogicalProjection:
		return o.deriveProjectionLineage(p)
	case *logicalop.LogicalUnionAll:
		return o.deriveUnionAllLineage(p)
	case *logicalop.LogicalPartitionUnionAll:
		return o.deriveUnionAllLineage(p)
	case *logicalop.LogicalApply:
		return o.deriveApplyLineage(p)
	case *logicalop.LogicalJoin:
		return o.deriveJoinLineage(p)
	case *logicalop.LogicalSelection,
		*logicalop.LogicalSort,
		*logicalop.LogicalTopN,
		*logicalop.LogicalLimit,
		*logicalop.LogicalLock,
		*logicalop.LogicalUnionScan,
		*logicalop.LogicalWindow:
		return o.deriveUnaryPassthroughLineage(plan)
	case *logicalop.LogicalSequence:
		return o.deriveSequenceLineage(p)
	default:
		return autoEmbedOracleLineage{}
	}
}

func (o *autoEmbedLineageOracle) deriveDataSourceLineage(ds *logicalop.DataSource) autoEmbedOracleLineage {
	lineage := autoEmbedOracleLineage{}
	if ds == nil || ds.Table == nil || ds.Schema() == nil {
		return lineage
	}
	tblColsByID := make(map[int64]*table.Column, len(ds.Table.Cols()))
	for _, col := range ds.Table.Cols() {
		if col != nil && col.ColumnInfo != nil {
			tblColsByID[col.ID] = col
		}
	}
	for _, planCol := range ds.Schema().Columns {
		if planCol == nil {
			continue
		}
		tblCol := tblColsByID[planCol.ID]
		info, ok := autoEmbedOracleInfoFromTableColumn(tblCol, planCol)
		if ok {
			lineage[planCol.UniqueID] = info
		}
	}
	return o.withoutOpaqueColumns(lineage)
}

func autoEmbedOracleInfoFromTableColumn(tblCol *table.Column, planCol *expression.Column) (*expression.AutoEmbedInfo, bool) {
	if tblCol == nil || tblCol.ColumnInfo == nil || planCol == nil || planCol.RetType == nil {
		return nil, false
	}
	if tblCol.State != model.StatePublic || tblCol.Hidden || !tblCol.IsGenerated() || !tblCol.GeneratedStored {
		return nil, false
	}
	if !planCol.RetType.EvalType().IsVectorKind() {
		return nil, false
	}
	if tblCol.GeneratedExpr == nil || tblCol.GeneratedExpr.Internal() == nil {
		return nil, false
	}
	info, err := expression.ExtractAutoEmbedInfoFromAST(tblCol.GeneratedExpr.Internal())
	if err != nil {
		return nil, false
	}
	return info, true
}

func (o *autoEmbedLineageOracle) deriveProjectionLineage(p *logicalop.LogicalProjection) autoEmbedOracleLineage {
	lineage := autoEmbedOracleLineage{}
	if p == nil || p.Schema() == nil || len(p.Children()) != 1 {
		return lineage
	}
	childLineage := o.deriveLineage(p.Children()[0])
	for i, expr := range p.Exprs {
		if i >= p.Schema().Len() {
			break
		}
		srcCol, ok := expr.(*expression.Column)
		if !ok {
			continue
		}
		info, ok := childLineage[srcCol.UniqueID]
		if !ok {
			continue
		}
		outCol := p.Schema().Columns[i]
		if autoEmbedOracleColumnTypesMatch(outCol, srcCol) {
			lineage[outCol.UniqueID] = cloneAutoEmbedInfo(info)
		}
	}
	return o.withoutOpaqueColumns(lineage)
}

func (o *autoEmbedLineageOracle) deriveUnionAllLineage(plan base.LogicalPlan) autoEmbedOracleLineage {
	lineage := autoEmbedOracleLineage{}
	if plan == nil || plan.Schema() == nil || len(plan.Children()) == 0 {
		return lineage
	}
	childLineages := make([]autoEmbedOracleLineage, 0, len(plan.Children()))
	for _, child := range plan.Children() {
		childLineages = append(childLineages, o.deriveLineage(child))
	}
	for i, outCol := range plan.Schema().Columns {
		if outCol == nil {
			continue
		}
		var merged *expression.AutoEmbedInfo
		valid := true
		for childIdx, child := range plan.Children() {
			if child.Schema() == nil || i >= child.Schema().Len() {
				valid = false
				break
			}
			srcCol := child.Schema().Columns[i]
			info, ok := childLineages[childIdx][srcCol.UniqueID]
			if !ok || !autoEmbedOracleColumnTypesMatch(outCol, srcCol) {
				valid = false
				break
			}
			if merged == nil {
				merged = info
				continue
			}
			if !merged.Equal(info) {
				valid = false
				break
			}
		}
		if valid && merged != nil {
			lineage[outCol.UniqueID] = cloneAutoEmbedInfo(merged)
		}
	}
	return o.withoutOpaqueColumns(lineage)
}

func (o *autoEmbedLineageOracle) deriveApplyLineage(p *logicalop.LogicalApply) autoEmbedOracleLineage {
	if p == nil || len(p.Children()) == 0 {
		return autoEmbedOracleLineage{}
	}
	left := p.Children()[0]
	return o.copyLineageBySameColumnID(p.Schema(), left.Schema(), o.deriveLineage(left))
}

func (o *autoEmbedLineageOracle) deriveJoinLineage(p *logicalop.LogicalJoin) autoEmbedOracleLineage {
	lineage := autoEmbedOracleLineage{}
	if p == nil {
		return lineage
	}
	childLineage := autoEmbedOracleLineage{}
	childCols := make(map[int64]*expression.Column)
	for _, child := range p.Children() {
		for id, info := range o.deriveLineage(child) {
			childLineage[id] = info
		}
		if child.Schema() == nil {
			continue
		}
		for _, col := range child.Schema().Columns {
			if col != nil {
				childCols[col.UniqueID] = col
			}
		}
	}
	o.copyJoinSchemaLineage(lineage, p.Schema(), childCols, childLineage)
	if p.FullSchema != nil {
		o.copyJoinSchemaLineage(lineage, p.FullSchema, childCols, childLineage)
	}
	o.applyUsingCoalesceChecks(lineage, p, childLineage, childCols)
	return o.withoutOpaqueColumns(lineage)
}

func (o *autoEmbedLineageOracle) copyJoinSchemaLineage(dst autoEmbedOracleLineage, schema *expression.Schema, childCols map[int64]*expression.Column, childLineage autoEmbedOracleLineage) {
	if schema == nil {
		return
	}
	for _, outCol := range schema.Columns {
		if outCol == nil {
			continue
		}
		info, ok := childLineage[outCol.UniqueID]
		if !ok {
			continue
		}
		if srcCol := childCols[outCol.UniqueID]; autoEmbedOracleColumnTypesMatch(outCol, srcCol) {
			dst[outCol.UniqueID] = cloneAutoEmbedInfo(info)
		}
	}
}

func (o *autoEmbedLineageOracle) applyUsingCoalesceChecks(lineage autoEmbedOracleLineage, p *logicalop.LogicalJoin, childLineage autoEmbedOracleLineage, childCols map[int64]*expression.Column) {
	if p == nil || len(p.RedundantColsToOutputIdx) == 0 || p.Schema() == nil {
		return
	}
	for redundantID, visibleIdx := range p.RedundantColsToOutputIdx {
		if visibleIdx < 0 || visibleIdx >= p.Schema().Len() {
			continue
		}
		visibleCol := p.Schema().Columns[visibleIdx]
		redundantCol := autoEmbedOracleFindColumnByID(p.FullSchema, redundantID)
		visibleInfo, visibleOK := childLineage[visibleCol.UniqueID]
		redundantInfo, redundantOK := childLineage[redundantID]
		if visibleOK && redundantOK &&
			visibleInfo.Equal(redundantInfo) &&
			autoEmbedOracleColumnTypesMatch(visibleCol, childCols[visibleCol.UniqueID]) &&
			autoEmbedOracleColumnTypesMatch(visibleCol, redundantCol) {
			lineage[visibleCol.UniqueID] = cloneAutoEmbedInfo(visibleInfo)
			lineage[redundantID] = cloneAutoEmbedInfo(redundantInfo)
			continue
		}
		delete(lineage, visibleCol.UniqueID)
		delete(lineage, redundantID)
	}
}

func (o *autoEmbedLineageOracle) deriveUnaryPassthroughLineage(plan base.LogicalPlan) autoEmbedOracleLineage {
	if plan == nil || len(plan.Children()) != 1 {
		return autoEmbedOracleLineage{}
	}
	child := plan.Children()[0]
	return o.copyLineageBySameColumnID(plan.Schema(), child.Schema(), o.deriveLineage(child))
}

func (o *autoEmbedLineageOracle) deriveSequenceLineage(p *logicalop.LogicalSequence) autoEmbedOracleLineage {
	if p == nil || len(p.Children()) == 0 {
		return autoEmbedOracleLineage{}
	}
	child := p.Children()[len(p.Children())-1]
	return o.copyLineageBySameColumnID(p.Schema(), child.Schema(), o.deriveLineage(child))
}

func (o *autoEmbedLineageOracle) copyLineageBySameColumnID(outSchema, childSchema *expression.Schema, childLineage autoEmbedOracleLineage) autoEmbedOracleLineage {
	lineage := autoEmbedOracleLineage{}
	if outSchema == nil || childSchema == nil {
		return lineage
	}
	for _, outCol := range outSchema.Columns {
		if outCol == nil {
			continue
		}
		info, ok := childLineage[outCol.UniqueID]
		if !ok {
			continue
		}
		childCol := autoEmbedOracleFindColumnByID(childSchema, outCol.UniqueID)
		if autoEmbedOracleColumnTypesMatch(outCol, childCol) {
			lineage[outCol.UniqueID] = cloneAutoEmbedInfo(info)
		}
	}
	return o.withoutOpaqueColumns(lineage)
}

func (o *autoEmbedLineageOracle) withoutOpaqueColumns(lineage autoEmbedOracleLineage) autoEmbedOracleLineage {
	if o == nil || len(o.opaqueColumnID) == 0 {
		return lineage
	}
	for id := range lineage {
		if _, ok := o.opaqueColumnID[id]; ok {
			delete(lineage, id)
		}
	}
	return lineage
}

func autoEmbedOracleFindColumnByID(schema *expression.Schema, id int64) *expression.Column {
	if schema == nil {
		return nil
	}
	for _, col := range schema.Columns {
		if col != nil && col.UniqueID == id {
			return col
		}
	}
	return nil
}

func autoEmbedOracleColumnTypesMatch(outCol, srcCol *expression.Column) bool {
	if outCol == nil || srcCol == nil || outCol.RetType == nil || srcCol.RetType == nil {
		return false
	}
	return outCol.RetType.Equal(srcCol.RetType)
}

func autoEmbedOracleSkipOpaqueBoundaryInvalidationForTest() bool {
	return autoEmbedOracleSkipOpaqueBoundary.Load()
}

// SetAutoEmbedOracleSkipOpaqueBoundaryInvalidationForTest makes opaque-boundary
// tracker invalidation fail open so tests can prove the independent oracle catches it.
func SetAutoEmbedOracleSkipOpaqueBoundaryInvalidationForTest(skip bool) func() {
	old := autoEmbedOracleSkipOpaqueBoundary.Swap(skip)
	return func() {
		autoEmbedOracleSkipOpaqueBoundary.Store(old)
	}
}
