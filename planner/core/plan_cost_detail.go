// Copyright 2022 PingCAP, Inc.
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
	"fmt"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/tracing"
)

const (
	// RowCountLbl indicates for rowCount
	RowCountLbl = "rowCount"
	// RowSizeLbl indicates rowSize
	RowSizeLbl = "rowSize"

	// NetworkFactorLbl indicates networkFactor
	NetworkFactorLbl = "networkFactor"
	// SeekFactorLbl indicates seekFactor
	SeekFactorLbl = "seekFactor"
	// ScanFactorLbl indicates for scanFactor
	ScanFactorLbl = "scanFactor"

	// ScanConcurrencyLbl indicates sql scan concurrency
	ScanConcurrencyLbl = "scanConcurrency"

	// NetSeekCostLbl indicates netSeek cost
	NetSeekCostLbl = "netSeekCost"
	// TablePlanCostLbl indicates tablePlan cost
	TablePlanCostLbl = "tablePlanCost"
	// IndexPlanCostLbl indicates indexPlan cost
	IndexPlanCostLbl = "indexPlanCost"
)

func setPointGetPlanCostDetail(p *PointGetPlan, opt *physicalOptimizeOp,
	rowSize, networkFactor, seekFactor float64) {
	if opt == nil {
		return
	}
	detail := tracing.NewPhysicalPlanCostDetail(p.ID(), p.TP())
	detail.AddParam(RowSizeLbl, rowSize).
		AddParam(NetworkFactorLbl, networkFactor).
		AddParam(SeekFactorLbl, seekFactor).
		SetDesc(fmt.Sprintf("%s*%s+%s", RowSizeLbl, NetworkFactorLbl, SeekFactorLbl))
	opt.appendPlanCostDetail(detail)
}

func setBatchPointGetPlanCostDetail(p *BatchPointGetPlan, opt *physicalOptimizeOp,
	rowCount, rowSize, networkFactor, seekFactor float64, scanConcurrency int) {
	if opt == nil {
		return
	}
	detail := tracing.NewPhysicalPlanCostDetail(p.ID(), p.TP())
	detail.AddParam(RowCountLbl, rowCount).
		AddParam(RowSizeLbl, rowSize).
		AddParam(NetworkFactorLbl, networkFactor).
		AddParam(SeekFactorLbl, seekFactor).
		AddParam(ScanConcurrencyLbl, scanConcurrency).
		SetDesc(fmt.Sprintf("(%s*%s*%s+%s*%s)/%s",
			RowCountLbl, RowSizeLbl, NetworkFactorLbl, RowCountLbl, SeekFactorLbl, ScanConcurrencyLbl))
	opt.appendPlanCostDetail(detail)
}

func setPhysicalTableOrIndexScanCostDetail(p PhysicalPlan, opt *physicalOptimizeOp,
	rowCount, rowSize, scanFactor float64, costModelVersion int) {
	if opt == nil {
		return
	}
	_, ok1 := p.(*PhysicalTableScan)
	_, ok2 := p.(*PhysicalIndexScan)
	if !ok1 && !ok2 {
		return
	}
	detail := tracing.NewPhysicalPlanCostDetail(p.ID(), p.TP())
	detail.AddParam(RowCountLbl, rowCount).
		AddParam(RowSizeLbl, rowSize).
		AddParam(ScanFactorLbl, scanFactor)
	var desc string
	if costModelVersion == modelVer1 {
		desc = fmt.Sprintf("%s*%s*%s", RowCountLbl, RowSizeLbl, ScanFactorLbl)
	} else {
		desc = fmt.Sprintf("%s*log2(%s)*%s", RowCountLbl, RowSizeLbl, ScanFactorLbl)
	}
	detail.SetDesc(desc)
	opt.appendPlanCostDetail(detail)
}

func setPhysicalTableReaderCostDetail(p *PhysicalTableReader, opt *physicalOptimizeOp,
	rowCount, rowSize, networkFactor, netSeekCost, tablePlanCost float64,
	scanConcurrency int, storeType kv.StoreType) {
	// tracer haven't support non tikv plan for now
	if opt == nil || storeType != kv.TiKV {
		return
	}
	detail := tracing.NewPhysicalPlanCostDetail(p.ID(), p.TP())
	detail.AddParam(RowCountLbl, rowCount).
		AddParam(RowSizeLbl, rowSize).
		AddParam(NetworkFactorLbl, networkFactor).
		AddParam(NetSeekCostLbl, netSeekCost).
		AddParam(TablePlanCostLbl, tablePlanCost).
		AddParam(ScanConcurrencyLbl, scanConcurrency)
	detail.SetDesc(fmt.Sprintf("(%s+%s*%s*%s+%s)/%s", TablePlanCostLbl,
		RowCountLbl, RowSizeLbl, NetworkFactorLbl, NetSeekCostLbl, ScanConcurrencyLbl))
	opt.appendPlanCostDetail(detail)
}

func setPhysicalIndexReaderCostDetail(p *PhysicalIndexReader, opt *physicalOptimizeOp,
	rowCount, rowSize, networkFactor, netSeekCost, indexPlanCost float64,
	scanConcurrency int) {
	if opt == nil {
		return
	}
	detail := tracing.NewPhysicalPlanCostDetail(p.ID(), p.TP())
	detail.AddParam(RowCountLbl, rowCount).
		AddParam(RowSizeLbl, rowSize).
		AddParam(NetworkFactorLbl, networkFactor).
		AddParam(NetSeekCostLbl, netSeekCost).
		AddParam(IndexPlanCostLbl, indexPlanCost).
		AddParam(ScanConcurrencyLbl, scanConcurrency)
	detail.SetDesc(fmt.Sprintf("(%s+%s*%s*%s+%s)/%s", IndexPlanCostLbl,
		RowCountLbl, RowSizeLbl, NetworkFactorLbl, NetSeekCostLbl, ScanConcurrencyLbl))
	opt.appendPlanCostDetail(detail)
}
