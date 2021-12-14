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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package implementation

import (
	"math"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
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
	tblInfo     *model.TableInfo
	tblColHists *statistics.HistColl
}

// NewTableReaderImpl creates a new table reader Implementation.
func NewTableReaderImpl(reader *plannercore.PhysicalTableReader, source *plannercore.DataSource) *TableReaderImpl {
	base := baseImpl{plan: reader}
	impl := &TableReaderImpl{
		baseImpl:    base,
		tblInfo:     source.TableInfo(),
		tblColHists: source.TblColHists,
	}
	return impl
}

// CalcCost calculates the cost of the table reader Implementation.
func (impl *TableReaderImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	reader := impl.plan.(*plannercore.PhysicalTableReader)
	width := impl.tblColHists.GetAvgRowSize(impl.plan.SCtx(), reader.Schema().Columns, false, false)
	sessVars := reader.SCtx().GetSessionVars()
	// TableReaderImpl don't have tableInfo property, so using nil to replace it.
	// Todo add the tableInfo property for the TableReaderImpl.
	networkCost := outCount * sessVars.GetNetworkFactor(impl.tblInfo) * width
	// copTasks are run in parallel, to make the estimated cost closer to execution time, we amortize
	// the cost to cop iterator workers. According to `CopClient::Send`, the concurrency
	// is Min(DistSQLScanConcurrency, numRegionsInvolvedInScan), since we cannot infer
	// the number of regions involved, we simply use DistSQLScanConcurrency.
	copIterWorkers := float64(sessVars.DistSQLScanConcurrency())
	impl.cost = (networkCost + children[0].GetCost()) / copIterWorkers
	return impl.cost
}

// GetCostLimit implements Implementation interface.
func (impl *TableReaderImpl) GetCostLimit(costLimit float64, children ...memo.Implementation) float64 {
	reader := impl.plan.(*plannercore.PhysicalTableReader)
	sessVars := reader.SCtx().GetSessionVars()
	copIterWorkers := float64(sessVars.DistSQLScanConcurrency())
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
func NewTableScanImpl(ts *plannercore.PhysicalTableScan, cols []*expression.Column, hists *statistics.HistColl) *TableScanImpl {
	base := baseImpl{plan: ts}
	impl := &TableScanImpl{
		baseImpl:    base,
		tblColHists: hists,
		tblCols:     cols,
	}
	return impl
}

// CalcCost calculates the cost of the table scan Implementation.
func (impl *TableScanImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	ts := impl.plan.(*plannercore.PhysicalTableScan)
	width := impl.tblColHists.GetTableAvgRowSize(impl.plan.SCtx(), impl.tblCols, kv.TiKV, true)
	sessVars := ts.SCtx().GetSessionVars()
	impl.cost = outCount * sessVars.GetScanFactor(ts.Table) * width
	if ts.Desc {
		impl.cost = outCount * sessVars.GetDescScanFactor(ts.Table) * width
	}
	return impl.cost
}

// IndexReaderImpl is the implementation of PhysicalIndexReader.
type IndexReaderImpl struct {
	baseImpl
	tblInfo     *model.TableInfo
	tblColHists *statistics.HistColl
}

// GetCostLimit implements Implementation interface.
func (impl *IndexReaderImpl) GetCostLimit(costLimit float64, children ...memo.Implementation) float64 {
	reader := impl.plan.(*plannercore.PhysicalIndexReader)
	sessVars := reader.SCtx().GetSessionVars()
	copIterWorkers := float64(sessVars.DistSQLScanConcurrency())
	if math.MaxFloat64/copIterWorkers < costLimit {
		return math.MaxFloat64
	}
	return costLimit * copIterWorkers
}

// CalcCost implements Implementation interface.
func (impl *IndexReaderImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	reader := impl.plan.(*plannercore.PhysicalIndexReader)
	sessVars := reader.SCtx().GetSessionVars()
	networkCost := outCount * sessVars.GetNetworkFactor(impl.tblInfo) * impl.tblColHists.GetAvgRowSize(reader.SCtx(), children[0].GetPlan().Schema().Columns, true, false)
	copIterWorkers := float64(sessVars.DistSQLScanConcurrency())
	impl.cost = (networkCost + children[0].GetCost()) / copIterWorkers
	return impl.cost
}

// NewIndexReaderImpl creates a new IndexReader Implementation.
func NewIndexReaderImpl(reader *plannercore.PhysicalIndexReader, source *plannercore.DataSource) *IndexReaderImpl {
	return &IndexReaderImpl{
		baseImpl:    baseImpl{plan: reader},
		tblInfo:     source.TableInfo(),
		tblColHists: source.TblColHists,
	}
}

// IndexScanImpl is the Implementation of PhysicalIndexScan.
type IndexScanImpl struct {
	baseImpl
	tblColHists *statistics.HistColl
}

// CalcCost implements Implementation interface.
func (impl *IndexScanImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	is := impl.plan.(*plannercore.PhysicalIndexScan)
	sessVars := is.SCtx().GetSessionVars()
	rowSize := impl.tblColHists.GetIndexAvgRowSize(is.SCtx(), is.Schema().Columns, is.Index.Unique)
	cost := outCount * rowSize * sessVars.GetScanFactor(is.Table)
	if is.Desc {
		cost = outCount * rowSize * sessVars.GetDescScanFactor(is.Table)
	}
	cost += float64(len(is.Ranges)) * sessVars.GetSeekFactor(is.Table)
	impl.cost = cost
	return impl.cost
}

// NewIndexScanImpl creates a new IndexScan Implementation.
func NewIndexScanImpl(scan *plannercore.PhysicalIndexScan, tblColHists *statistics.HistColl) *IndexScanImpl {
	return &IndexScanImpl{
		baseImpl:    baseImpl{plan: scan},
		tblColHists: tblColHists,
	}
}
