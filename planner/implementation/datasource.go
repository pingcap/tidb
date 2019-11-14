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
	"github.com/pingcap/tidb/expression"
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
	var width float64
	if impl.plan.SCtx().GetSessionVars().EnableChunkRPC {
		width = impl.tblColHists.GetAvgRowSizeChunkFormat(reader.Schema().Columns)
	} else {
		width = impl.tblColHists.GetAvgRowSize(reader.Schema().Columns, false)
	}
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
	var width float64
	if impl.plan.SCtx().GetSessionVars().EnableChunkRPC {
		width = impl.tblColHists.GetAvgRowSizeChunkFormat(impl.tblCols)
	} else {
		width = impl.tblColHists.GetAvgRowSize(impl.tblCols, false)
	}
	sessVars := ts.SCtx().GetSessionVars()
	impl.cost = outCount * sessVars.ScanFactor * width
	if ts.Desc {
		impl.cost = outCount * sessVars.DescScanFactor * width
	}
	return impl.cost
}
