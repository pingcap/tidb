// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package implementation

import (
	"math"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/memo"
	"github.com/pingcap/tidb/statistics"
)

// TableDualImpl implementation of PhysicalTableDual.
type TableDualImpl struct {
	baseImpl
}

// NewTableDualImpl creates a new table dual Implementation.
func NewTableDualImpl(dual *plannercore.PhysicalTableDual) *TableDualImpl {
	return &TableDualImpl{baseImpl{plan: dual}}
}

// CalcCost calculates the cost of the table dual Implementation.
func (impl *TableDualImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	return 0
}

// MemTableScanImpl implementation of PhysicalTableDual.
type MemTableScanImpl struct {
	baseImpl
}

// NewMemTableScanImpl creates a new table dual Implementation.
func NewMemTableScanImpl(dual *plannercore.PhysicalMemTable) *MemTableScanImpl {
	return &MemTableScanImpl{baseImpl{plan: dual}}
}

// CalcCost calculates the cost of the table dual Implementation.
func (impl *MemTableScanImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	return 0
}

// TableReaderImpl implementation of PhysicalTableReader.
type TableReaderImpl struct {
	baseImpl
	tblColHists *statistics.HistColl
}

// NewTableReaderImpl creates a new table reader Implementation.
func NewTableReaderImpl(reader *plannercore.PhysicalTableReader, hists *statistics.HistColl) *TableReaderImpl {
	base := baseImpl{plan: reader}
	impl := &TableReaderImpl{
		baseImpl:    base,
		tblColHists: hists,
	}
	return impl
}

// CalcCost calculates the cost of the table reader Implementation.
func (impl *TableReaderImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	reader := impl.plan.(*plannercore.PhysicalTableReader)
	width := impl.tblColHists.GetAvgRowSize(impl.plan.SCtx(), reader.Schema().Columns, false, false)
	sessVars := reader.SCtx().GetSessionVars()
	networkCost := outCount * sessVars.NetworkFactor * width
	// copTasks are run in parallel, to make the estimated cost closer to execution time, we amortize
	// the cost to cop iterator workers. According to `CopClient::Send`, the concurrency
	// is Min(DistSQLScanConcurrency, numRegionsInvolvedInScan), since we cannot infer
	// the number of regions involved, we simply use DistSQLScanConcurrency.
	copIterWorkers := float64(sessVars.DistSQLScanConcurrency)
	impl.cost = (networkCost + children[0].GetCost()) / copIterWorkers
	return impl.cost
}

// GetCostLimit implements Implementation interface.
func (impl *TableReaderImpl) GetCostLimit(costLimit float64, children ...memo.Implementation) float64 {
	reader := impl.plan.(*plannercore.PhysicalTableReader)
	sessVars := reader.SCtx().GetSessionVars()
	copIterWorkers := float64(sessVars.DistSQLScanConcurrency)
	if math.MaxFloat64/copIterWorkers < costLimit {
		return math.MaxFloat64
	}
	return costLimit * copIterWorkers
}

// TableScanImpl implementation of PhysicalTableScan.
type TableScanImpl struct {
	baseImpl
	tblColHists *statistics.HistColl
	tblCols     []*expression.Column
}

// NewTableScanImpl creates a new table scan Implementation.
func NewTableScanImpl(plan plannercore.PhysicalPlan, cols []*expression.Column, hists *statistics.HistColl) *TableScanImpl {
	impl := &TableScanImpl{
		baseImpl:    baseImpl{plan: plan},
		tblColHists: hists,
		tblCols:     cols,
	}
	return impl
}

// CalcCost calculates the cost of the table scan Implementation.
func (impl *TableScanImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	selCost := 0.0
	tsCost := 0.0
	sessVars := impl.plan.SCtx().GetSessionVars()
	var ts *plannercore.PhysicalTableScan
	if sel, ok := impl.plan.(*plannercore.PhysicalSelection); ok {
		selCost = sel.StatsCount() * sessVars.CopCPUFactor
		ts = sel.Children()[0].(*plannercore.PhysicalTableScan)
	} else {
		ts = impl.plan.(*plannercore.PhysicalTableScan)
	}

	width := impl.tblColHists.GetTableAvgRowSize(impl.plan.SCtx(), impl.tblCols, kv.TiKV, true)
	tsCost = ts.StatsCount() * sessVars.ScanFactor * width
	if ts.Desc {
		tsCost = ts.StatsCount() * sessVars.DescScanFactor * width
	}
	tsCost += float64(len(ts.Ranges)) * sessVars.SeekFactor
	impl.cost = tsCost + selCost
	return impl.cost
}

// AttachChildren implements Implementation interface.
func (impl *TableScanImpl) AttachChildren(children ...memo.Implementation) memo.Implementation {
	return impl
}

// IndexReaderImpl is the implementation of PhysicalIndexReader.
type IndexReaderImpl struct {
	baseImpl
	tblColHists *statistics.HistColl
}

// GetCostLimit implements Implementation interface.
func (impl *IndexReaderImpl) GetCostLimit(costLimit float64, children ...memo.Implementation) float64 {
	reader := impl.plan.(*plannercore.PhysicalIndexReader)
	sessVars := reader.SCtx().GetSessionVars()
	copIterWorkers := float64(sessVars.DistSQLScanConcurrency)
	if math.MaxFloat64/copIterWorkers < costLimit {
		return math.MaxFloat64
	}
	return costLimit * copIterWorkers
}

// CalcCost implements Implementation interface.
func (impl *IndexReaderImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	reader := impl.plan.(*plannercore.PhysicalIndexReader)
	sessVars := reader.SCtx().GetSessionVars()
	networkCost := outCount * sessVars.NetworkFactor * impl.tblColHists.GetAvgRowSize(reader.SCtx(), children[0].GetPlan().Schema().Columns, true, false)
	copIterWorkers := float64(sessVars.DistSQLScanConcurrency)
	impl.cost = (networkCost + children[0].GetCost()) / copIterWorkers
	return impl.cost
}

// NewIndexReaderImpl creates a new IndexReader Implementation.
func NewIndexReaderImpl(reader *plannercore.PhysicalIndexReader, tblColHists *statistics.HistColl) *IndexReaderImpl {
	return &IndexReaderImpl{
		baseImpl:    baseImpl{plan: reader},
		tblColHists: tblColHists,
	}
}

// IndexScanImpl is the Implementation of PhysicalIndexScan.
type IndexScanImpl struct {
	baseImpl
	tblColHists *statistics.HistColl
}

// CalcCost implements Implementation interface.
func (impl *IndexScanImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	selCost := 0.0
	isCost := 0.0
	sessVars := impl.plan.SCtx().GetSessionVars()
	var is *plannercore.PhysicalIndexScan
	if sel, ok := impl.plan.(*plannercore.PhysicalSelection); ok {
		selCost = sel.StatsCount() * sessVars.CopCPUFactor
		is = sel.Children()[0].(*plannercore.PhysicalIndexScan)
	} else {
		is = impl.plan.(*plannercore.PhysicalIndexScan)
	}
	rowSize := impl.tblColHists.GetIndexAvgRowSize(is.SCtx(), is.Schema().Columns, is.Index.Unique)
	isCost = is.StatsCount() * rowSize * sessVars.ScanFactor
	if is.Desc {
		isCost = is.StatsCount() * rowSize * sessVars.DescScanFactor
	}
	isCost += float64(len(is.Ranges)) * sessVars.SeekFactor
	impl.cost = selCost + isCost
	return impl.cost
}

// AttachChildren implements Implementation interface.
func (impl *IndexScanImpl) AttachChildren(children ...memo.Implementation) memo.Implementation {
	return impl
}

// NewIndexScanImpl creates a new IndexScan Implementation.
func NewIndexScanImpl(scan plannercore.PhysicalPlan, tblColHists *statistics.HistColl) *IndexScanImpl {
	return &IndexScanImpl{
		baseImpl:    baseImpl{plan: scan},
		tblColHists: tblColHists,
	}
}

// IndexLookUpReaderImpl is the Implementation of PhysicalIndexLookUpReader.
type IndexLookUpReaderImpl struct {
	baseImpl

	KeepOrder   bool
	tblColHists *statistics.HistColl
	extraProj   *plannercore.PhysicalProjection
}

// NewIndexLookUpReaderImpl creates a new table reader Implementation.
func NewIndexLookUpReaderImpl(reader plannercore.PhysicalPlan, hists *statistics.HistColl, proj *plannercore.PhysicalProjection) *IndexLookUpReaderImpl {
	base := baseImpl{plan: reader}
	impl := &IndexLookUpReaderImpl{
		baseImpl:    base,
		tblColHists: hists,
		extraProj:   proj,
	}
	return impl
}

// CalcCost calculates the cost of the table reader Implementation.
func (impl *IndexLookUpReaderImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	var reader *plannercore.PhysicalIndexLookUpReader
	if impl.extraProj != nil {
		impl.cost += impl.extraProj.GetCost(impl.extraProj.Stats().RowCount)
		reader = impl.extraProj.Children()[0].(*plannercore.PhysicalIndexLookUpReader)
	} else {
		reader = impl.plan.(*plannercore.PhysicalIndexLookUpReader)
	}
	reader.IndexPlan, reader.TablePlan = children[0].GetPlan(), children[1].GetPlan()
	// Add cost of building table reader executors. Handles are extracted in batch style,
	// each handle is a range, the CPU cost of building copTasks should be:
	// (indexRows / batchSize) * batchSize * CPUFactor
	// Since we don't know the number of copTasks built, ignore these network cost now.
	sessVars := reader.SCtx().GetSessionVars()
	// Add children cost.
	impl.cost += (children[0].GetCost() + children[1].GetCost()) / float64(sessVars.DistSQLScanConcurrency)
	indexRows := reader.IndexPlan.Stats().RowCount
	impl.cost += indexRows * sessVars.CPUFactor
	// Add cost of worker goroutines in index lookup.
	numTblWorkers := float64(sessVars.IndexLookupConcurrency)
	impl.cost += (numTblWorkers + 1) * sessVars.ConcurrencyFactor
	// When building table reader executor for each batch, we would sort the handles. CPU
	// cost of sort is:
	// CPUFactor * batchSize * Log2(batchSize) * (indexRows / batchSize)
	indexLookupSize := float64(sessVars.IndexLookupSize)
	batchSize := math.Min(indexLookupSize, indexRows)
	if batchSize > 2 {
		sortCPUCost := (indexRows * math.Log2(batchSize) * sessVars.CPUFactor) / numTblWorkers
		impl.cost += sortCPUCost
	}
	// Also, we need to sort the retrieved rows if index lookup reader is expected to return
	// ordered results. Note that row count of these two sorts can be different, if there are
	// operators above table scan.
	tableRows := reader.TablePlan.Stats().RowCount
	selectivity := tableRows / indexRows
	batchSize = math.Min(indexLookupSize*selectivity, tableRows)
	if impl.KeepOrder && batchSize > 2 {
		sortCPUCost := (tableRows * math.Log2(batchSize) * sessVars.CPUFactor) / numTblWorkers
		impl.cost += sortCPUCost
	}
	return impl.cost
}

// ScaleCostLimit implements Implementation interface.
func (impl *IndexLookUpReaderImpl) ScaleCostLimit(costLimit float64) float64 {
	sessVars := impl.plan.SCtx().GetSessionVars()
	copIterWorkers := float64(sessVars.DistSQLScanConcurrency)
	if math.MaxFloat64/copIterWorkers < costLimit {
		return math.MaxFloat64
	}
	return costLimit * copIterWorkers
}

// AttachChildren implements Implementation AttachChildren interface.
func (impl *IndexLookUpReaderImpl) AttachChildren(children ...memo.Implementation) memo.Implementation {
	reader := impl.plan.(*plannercore.PhysicalIndexLookUpReader)
	reader.TablePlans = plannercore.FlattenPushDownPlan(reader.TablePlan)
	tableScan := reader.TablePlans[len(reader.TablePlans)-1].(*plannercore.PhysicalTableScan)
	if tableScan.Schema().ColumnIndex(tableScan.HandleCol) == -1 {
		tableScan.Schema().Append(tableScan.HandleCol)
		if tableScan.HandleCol.ID == model.ExtraHandleID {
			tableScan.Columns = append(tableScan.Columns, model.NewExtraHandleColInfo())
		}
	}
	reader.IndexPlans = plannercore.FlattenPushDownPlan(reader.IndexPlan)
	if impl.extraProj != nil {
		impl.extraProj.SetChildren(reader)
		impl.plan = impl.extraProj
	}
	return impl
}
