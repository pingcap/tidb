// Copyright 2025 PingCAP, Inc.
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

package physicalop

import (
	"fmt"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/telemetry"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

func (p *PhysicalTableScan) haveCorCol() bool {
	for _, cond := range p.AccessCondition {
		if len(expression.ExtractCorColumns(cond)) > 0 {
			return true
		}
	}
	return false
}

// IsFullScan is to judge whether the PhysicalTableScan is full-scan
func (p *PhysicalTableScan) IsFullScan() bool {
	if len(p.RangeInfo) > 0 || p.haveCorCol() {
		return false
	}
	var unsignedIntHandle bool
	if p.Table.PKIsHandle {
		if pkColInfo := p.Table.GetPkColInfo(); pkColInfo != nil {
			unsignedIntHandle = mysql.HasUnsignedFlag(pkColInfo.GetFlag())
		}
	}
	for _, ran := range p.Ranges {
		if !ran.IsFullRange(unsignedIntHandle) {
			return false
		}
	}
	return true
}

// AppendExtraHandleCol is that If there is a table reader which needs to keep order, we should append a pk to table scan.
func (p *PhysicalTableScan) AppendExtraHandleCol(ds *logicalop.DataSource) (*expression.Column, bool) {
	handleCols := ds.HandleCols
	if handleCols != nil {
		return handleCols.GetCol(0), false
	}
	handleCol := ds.NewExtraHandleSchemaCol()
	p.Schema().Append(handleCol)
	p.Columns = append(p.Columns, model.NewExtraHandleColInfo())
	return handleCol, true
}

// BuildPushedDownSelection is to build pushed-down selection
func (p *PhysicalTableScan) BuildPushedDownSelection(stats *property.StatsInfo, indexHints []*ast.IndexHint) *PhysicalSelection {
	if p.StoreType == kv.TiFlash {
		handleTiFlashPredicatePushDown(p.SCtx(), p, indexHints)
		conditions := make([]expression.Expression, 0, len(p.FilterCondition)-len(p.LateMaterializationFilterCondition))
		for _, cond := range p.FilterCondition {
			if !expression.Contains(p.SCtx().GetExprCtx().GetEvalCtx(), p.LateMaterializationFilterCondition, cond) {
				conditions = append(conditions, cond)
			}
		}
		if len(conditions) == 0 {
			return nil
		}
		return PhysicalSelection{Conditions: conditions}.Init(p.SCtx(), stats, p.QueryBlockOffset())
	}
	return PhysicalSelection{Conditions: p.FilterCondition}.Init(p.SCtx(), stats, p.QueryBlockOffset())
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalTableScan) GetPlanCostVer1(_ property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalTableScan(p, option)
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = rows * log2(row-size) * scan-factor
// log2(row-size) is from experimenp.
func (p *PhysicalTableScan) GetPlanCostVer2(taskType property.TaskType,
	option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PhysicalTableScan(p, taskType, option, isChildOfINL...)
}

// GetScanRowSize is to get the row size when to scan.
func (p *PhysicalTableScan) GetScanRowSize() float64 {
	if p.StoreType == kv.TiKV {
		cols := p.TblCols
		// _tidb_commit_ts is not a real extra column stored in the disk, and it should not bring extra cost, so we
		// exclude it from the cost here.
		for i, col := range cols {
			if col.ID == model.ExtraCommitTSID {
				cols = slices.Delete(slices.Clone(cols), i, i+1)
				break
			}
		}
		return cardinality.GetTableAvgRowSize(p.SCtx(), p.TblColHists, cols, p.StoreType, true)
	}
	// If `p.handleCol` is nil, then the schema of tableScan doesn't have handle column.
	// This logic can be ensured in column pruning.
	return cardinality.GetTableAvgRowSize(p.SCtx(), p.TblColHists, p.Schema().Columns, p.StoreType, p.HandleCols != nil)
}

// ResolveIndices implements Plan interface.
func (p *PhysicalTableScan) ResolveIndices() (err error) {
	err = p.PhysicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	return p.ResolveIndicesItself()
}

// ResolveIndicesItself implements PhysicalTableScan interface.
func (p *PhysicalTableScan) ResolveIndicesItself() (err error) {
	for i, column := range p.Schema().Columns {
		column.Index = i
	}
	for i, expr := range p.LateMaterializationFilterCondition {
		p.LateMaterializationFilterCondition[i], err = expr.ResolveIndices(p.Schema())
		if err != nil {
			// Check if there is duplicate virtual expression column matched.
			newCond, isOk := expr.ResolveIndicesByVirtualExpr(p.SCtx().GetExprCtx().GetEvalCtx(), p.Schema())
			if isOk {
				p.LateMaterializationFilterCondition[i] = newCond
				continue
			}
			return err
		}
	}
	return
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalTableScan) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	if storeType == kv.TiFlash && p.Table.GetPartitionInfo() != nil && p.IsMPPOrBatchCop && p.SCtx().GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return p.partitionTableScanToPBForFlash(ctx)
	}
	tsExec := tables.BuildTableScanFromInfos(p.Table, p.Columns, p.StoreType == kv.TiFlash)
	tsExec.Desc = p.Desc
	keepOrder := p.KeepOrder
	tsExec.KeepOrder = &keepOrder
	tsExec.IsFastScan = &(ctx.TiFlashFastScan)

	if len(p.LateMaterializationFilterCondition) > 0 {
		client := ctx.GetClient()
		conditions, err := expression.ExpressionsToPBList(ctx.GetExprCtx().GetEvalCtx(), p.LateMaterializationFilterCondition, client)
		if err != nil {
			return nil, err
		}
		tsExec.PushedDownFilterConditions = conditions
	}

	for _, idx := range p.UsedColumnarIndexes {
		if idx != nil && idx.QueryInfo != nil {
			queryInfoCopy := *idx.QueryInfo
			tsExec.UsedColumnarIndexes = append(tsExec.UsedColumnarIndexes, &queryInfoCopy)
		}
	}

	var err error
	tsExec.RuntimeFilterList, err = RuntimeFilterListToPB(ctx, p.runtimeFilterList, ctx.GetClient())
	if err != nil {
		return nil, errors.Trace(err)
	}
	tsExec.MaxWaitTimeMs = int32(p.maxWaitTimeMs)

	if p.isPartition {
		tsExec.TableId = p.PhysicalTableID
	}
	executorID := ""
	if storeType == kv.TiFlash {
		executorID = p.ExplainID().String()

		telemetry.CurrentTiflashTableScanCount.Inc()
		if *(tsExec.IsFastScan) {
			telemetry.CurrentTiflashTableScanWithFastScanCount.Inc()
		}
	}
	err = tables.SetPBColumnsDefaultValue(ctx.GetExprCtx(), tsExec.Columns, p.Columns)
	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tsExec, ExecutorId: &executorID}, err
}

func (p *PhysicalTableScan) partitionTableScanToPBForFlash(ctx *base.BuildPBContext) (*tipb.Executor, error) {
	ptsExec := tables.BuildPartitionTableScanFromInfos(p.Table, p.Columns, ctx.TiFlashFastScan)
	telemetry.CurrentTiflashTableScanCount.Inc()
	if *(ptsExec.IsFastScan) {
		telemetry.CurrentTiflashTableScanWithFastScanCount.Inc()
	}

	if len(p.LateMaterializationFilterCondition) > 0 {
		client := ctx.GetClient()
		conditions, err := expression.ExpressionsToPBList(ctx.GetExprCtx().GetEvalCtx(), p.LateMaterializationFilterCondition, client)
		if err != nil {
			return nil, err
		}
		ptsExec.PushedDownFilterConditions = conditions
	}

	// set runtime filter
	var err error
	ptsExec.RuntimeFilterList, err = RuntimeFilterListToPB(ctx, p.runtimeFilterList, ctx.GetClient())
	if err != nil {
		return nil, errors.Trace(err)
	}
	ptsExec.MaxWaitTimeMs = int32(p.maxWaitTimeMs)

	ptsExec.Desc = p.Desc

	for _, idx := range p.UsedColumnarIndexes {
		if idx != nil && idx.QueryInfo != nil {
			queryInfoCopy := *idx.QueryInfo
			ptsExec.UsedColumnarIndexes = append(ptsExec.UsedColumnarIndexes, &queryInfoCopy)
		}
	}

	executorID := p.ExplainID().String()
	err = tables.SetPBColumnsDefaultValue(ctx.GetExprCtx(), ptsExec.Columns, p.Columns)
	return &tipb.Executor{Tp: tipb.ExecType_TypePartitionTableScan, PartitionTableScan: ptsExec, ExecutorId: &executorID}, err
}

// BuildIndexMergeTableScan returns Selection that will be pushed to TiKV.
// Filters that cannot be pushed to TiKV are also returned, and an extra Selection above IndexMergeReader will be constructed later.
func BuildIndexMergeTableScan(ds *logicalop.DataSource, tableFilters []expression.Expression,
	totalRowCount float64, matchProp bool) (base.PhysicalPlan, []expression.Expression, bool, error) {
	ts := PhysicalTableScan{
		Table:           ds.TableInfo,
		Columns:         slices.Clone(ds.Columns),
		TableAsName:     ds.TableAsName,
		DBName:          ds.DBName,
		PhysicalTableID: ds.PhysicalTableID,
		HandleCols:      ds.HandleCols,
		TblCols:         ds.TblCols,
		TblColHists:     ds.TblColHists,
	}.Init(ds.SCtx(), ds.QueryBlockOffset())
	ts.SetIsPartition(ds.PartitionDefIdx != nil)
	ts.SetSchema(ds.Schema().Clone())
	err := setIndexMergeTableScanHandleCols(ds, ts)
	if err != nil {
		return nil, nil, false, err
	}
	ts.SetStats(ds.TableStats.ScaleByExpectCnt(ds.SCtx().GetSessionVars(), totalRowCount))
	usedStats := ds.SCtx().GetSessionVars().StmtCtx.GetUsedStatsInfo(false)
	if usedStats != nil && usedStats.GetUsedInfo(ts.PhysicalTableID) != nil {
		ts.UsedStatsInfo = usedStats.GetUsedInfo(ts.PhysicalTableID)
	}
	if ds.StatisticTable.Pseudo {
		ts.StatsInfo().StatsVersion = statistics.PseudoVersion
	}
	var currentTopPlan base.PhysicalPlan = ts
	if len(tableFilters) > 0 {
		pushedFilters, remainingFilters := extractFiltersForIndexMerge(util.GetPushDownCtx(ds.SCtx()), tableFilters)
		pushedFilters1, remainingFilters1 := SplitSelCondsWithVirtualColumn(pushedFilters)
		pushedFilters = pushedFilters1
		remainingFilters = append(remainingFilters, remainingFilters1...)
		if len(pushedFilters) != 0 {
			selectivity, err := cardinality.Selectivity(ds.SCtx(), ds.TableStats.HistColl, pushedFilters, nil)
			if err != nil {
				logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
				selectivity = cost.SelectionFactor
			}
			sel := PhysicalSelection{Conditions: pushedFilters}.Init(ts.SCtx(), ts.StatsInfo().ScaleByExpectCnt(ts.SCtx().GetSessionVars(), selectivity*totalRowCount), ts.QueryBlockOffset())
			sel.SetChildren(ts)
			currentTopPlan = sel
		}
		if len(remainingFilters) > 0 {
			return currentTopPlan, remainingFilters, false, nil
		}
	}
	// If we don't need to use ordered scan, we don't need do the following codes for adding new columns.
	if !matchProp {
		return currentTopPlan, nil, false, nil
	}

	// Add the row handle into the schema.
	columnAdded := false
	if ts.Table.PKIsHandle {
		pk := ts.Table.GetPkColInfo()
		pkCol := expression.ColInfo2Col(ts.TblCols, pk)
		if !ts.Schema().Contains(pkCol) {
			ts.Schema().Append(pkCol)
			ts.Columns = append(ts.Columns, pk)
			columnAdded = true
		}
	} else if ts.Table.IsCommonHandle {
		idxInfo := ts.Table.GetPrimaryKey()
		for _, idxCol := range idxInfo.Columns {
			col := ts.TblCols[idxCol.Offset]
			if !ts.Schema().Contains(col) {
				columnAdded = true
				ts.Schema().Append(col)
				ts.Columns = append(ts.Columns, col.ToInfo())
			}
		}
	} else if ts.HandleCols != nil && !ts.Schema().Contains(ts.HandleCols.GetCol(0)) {
		ts.Schema().Append(ts.HandleCols.GetCol(0))
		ts.Columns = append(ts.Columns, model.NewExtraHandleColInfo())
		columnAdded = true
	} else if ds.Table.Type().IsClusterTable() {
		// For cluster tables without HandleCols, use the first column like preferKeyColumnFromTable does.
		// This handles the case when column-prune-logical rule is blocked and cluster tables don't have
		// their handle cols set up through the normal column pruning path.
		if len(ts.TblCols) > 0 {
			col := ts.TblCols[0]
			if !ts.Schema().Contains(col) {
				ts.Schema().Append(col)
				// Find the corresponding ColumnInfo from table metadata
				if colInfo := model.FindColumnInfoByID(ts.Table.Columns, col.ID); colInfo != nil {
					ts.Columns = append(ts.Columns, colInfo)
				} else {
					ts.Columns = append(ts.Columns, col.ToInfo())
				}
				columnAdded = true
			}
		} else if len(ts.Table.Columns) > 0 {
			// Fallback to table metadata columns
			colMeta := ts.Table.Columns[0]
			col := &expression.Column{
				RetType:  colMeta.FieldType.Clone(),
				UniqueID: ts.SCtx().GetSessionVars().AllocPlanColumnID(),
				ID:       colMeta.ID,
				OrigName: fmt.Sprintf("%v.%v.%v", ts.DBName, ts.Table.Name, colMeta.Name),
			}
			if !ts.Schema().Contains(col) {
				ts.Schema().Append(col)
				ts.Columns = append(ts.Columns, colMeta)
				columnAdded = true
			}
		}
	}

	// For the global index of the partitioned table, we also need the PhysicalTblID to identify the rows from each partition.
	if ts.Table.GetPartitionInfo() != nil && ts.SCtx().GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		tmpColumns, tmpSchema, newColAdded := AddExtraPhysTblIDColumn(ts.SCtx(), ts.Columns, ts.Schema())
		ts.Columns = tmpColumns
		ts.SetSchema(tmpSchema)
		columnAdded = columnAdded || newColAdded
	}
	return currentTopPlan, nil, columnAdded, nil
}

// extractFiltersForIndexMerge returns:
// `pushed`: exprs that can be pushed to TiKV.
// `remaining`: exprs that can NOT be pushed to TiKV but can be pushed to other storage engines.
// Why do we need this func?
// IndexMerge only works on TiKV, so we need to find all exprs that cannot be pushed to TiKV, and add a new Selection above IndexMergeReader.
//
//	But the new Selection should exclude the exprs that can NOT be pushed to ALL the storage engines.
//	Because these exprs have already been put in another Selection(check rule_predicate_push_down).
func extractFiltersForIndexMerge(ctx expression.PushDownContext, filters []expression.Expression) (pushed []expression.Expression, remaining []expression.Expression) {
	for _, expr := range filters {
		if expression.CanExprsPushDown(ctx, []expression.Expression{expr}, kv.TiKV) {
			pushed = append(pushed, expr)
			continue
		}
		if expression.CanExprsPushDown(ctx, []expression.Expression{expr}, kv.UnSpecified) {
			remaining = append(remaining, expr)
		}
	}
	return
}

// setIndexMergeTableScanHandleCols set the handle columns of the table scan.
func setIndexMergeTableScanHandleCols(ds *logicalop.DataSource, ts *PhysicalTableScan) (err error) {
	handleCols := ds.HandleCols
	if handleCols == nil {
		if ds.Table.Type().IsClusterTable() {
			// For cluster tables without handles, ts.HandleCols remains nil.
			// Cluster tables don't support ExtraHandleID (-1) as they are memory tables.
			return nil
		}
		handleCols = util.NewIntHandleCols(ds.NewExtraHandleSchemaCol())
	}
	hdColNum := handleCols.NumCols()
	exprCols := make([]*expression.Column, 0, hdColNum)
	for i := range hdColNum {
		col := handleCols.GetCol(i)
		exprCols = append(exprCols, col)
	}
	ts.HandleCols, err = handleCols.ResolveIndices(expression.NewSchema(exprCols...))
	return
}
