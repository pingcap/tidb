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

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/util/tracing"
)

const (
	// RowCountLbl indicates for rowCount
	RowCountLbl = "rowCount"
	// RowSizeLbl indicates rowSize
	RowSizeLbl = "rowSize"
	// BuildRowCountLbl indicates rowCount on build side
	BuildRowCountLbl = "buildRowCount"
	// ProbeRowCountLbl indicates rowCount on probe side
	ProbeRowCountLbl = "probeRowCount"
	// NumPairsLbl indicates numPairs
	NumPairsLbl = "numPairs"

	// NetworkFactorLbl indicates networkFactor
	NetworkFactorLbl = "networkFactor"
	// SeekFactorLbl indicates seekFactor
	SeekFactorLbl = "seekFactor"
	// ScanFactorLbl indicates for scanFactor
	ScanFactorLbl = "scanFactor"
	// SelectionFactorLbl indicates selection factor
	SelectionFactorLbl = "selectionFactor"
	// CPUFactorLbl indicates cpu factor
	CPUFactorLbl = "cpuFactor"
	// MemoryFactorLbl indicates mem factor
	MemoryFactorLbl = "memoryFactor"
	// DiskFactorLbl indicates disk factor
	DiskFactorLbl = "diskFactor"
	// ConcurrencyFactorLbl indicates for concurrency factor
	ConcurrencyFactorLbl = "concurrencyFactor"

	// ScanConcurrencyLbl indicates sql scan concurrency
	ScanConcurrencyLbl = "scanConcurrency"
	// HashJoinConcurrencyLbl indicates concurrency for hash join
	HashJoinConcurrencyLbl = "hashJoinConcurrency"

	// NetSeekCostLbl indicates netSeek cost
	NetSeekCostLbl = "netSeekCost"
	// TablePlanCostLbl indicates tablePlan cost
	TablePlanCostLbl = "tablePlanCost"
	// IndexPlanCostLbl indicates indexPlan cost
	IndexPlanCostLbl = "indexPlanCost"

	// ProbeCostDetailLbl indicates probeCost
	ProbeCostDetailLbl = "probeCostDetail"
	// ProbeCostDescLbl indicates description for probe cost
	ProbeCostDescLbl = "probeCostDesc"
	// CPUCostDetailLbl indicates cpuCost detail
	CPUCostDetailLbl = "cpuCostDetail"
	// CPUCostDescLbl indicates description for cpu cost
	CPUCostDescLbl = "cpuCostDesc"
	// MemCostDetailLbl indicates mem cost detail
	MemCostDetailLbl = "memCostDetail"
	// MemCostDescLbl indicates description for mem cost
	MemCostDescLbl = "memCostDesc"
	// DiskCostDetailLbl indicates disk cost detail
	DiskCostDetailLbl = "diskCostDetail"
	// DiskCostDescLbl indicates description for disk cost
	DiskCostDescLbl = "diskCostDesc"
	// ProbeDiskCostLbl indicates probe disk cost detail
	ProbeDiskCostLbl = "probeDiskCostDetail"
	// ProbeDiskCostDescLbl indicates description for probe disk cost
	ProbeDiskCostDescLbl = "probeDiskCostDesc"

	// MemQuotaLbl indicates memory quota
	MemQuotaLbl = "memQuota"
)

func setPointGetPlanCostDetail(p *PointGetPlan, opt *optimizetrace.PhysicalOptimizeOp,
	rowSize, networkFactor, seekFactor float64) {
	if opt == nil {
		return
	}
	detail := tracing.NewPhysicalPlanCostDetail(p.ID(), p.TP())
	detail.AddParam(RowSizeLbl, rowSize).
		AddParam(NetworkFactorLbl, networkFactor).
		AddParam(SeekFactorLbl, seekFactor).
		SetDesc(fmt.Sprintf("%s*%s+%s", RowSizeLbl, NetworkFactorLbl, SeekFactorLbl))
	appendPlanCostDetail4PhysicalOptimizeOp(opt, detail)
}

func setBatchPointGetPlanCostDetail(p *BatchPointGetPlan, opt *optimizetrace.PhysicalOptimizeOp,
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
	appendPlanCostDetail4PhysicalOptimizeOp(opt, detail)
}

func setPhysicalTableOrIndexScanCostDetail(p base.PhysicalPlan, opt *optimizetrace.PhysicalOptimizeOp,
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
	appendPlanCostDetail4PhysicalOptimizeOp(opt, detail)
}

func setPhysicalTableReaderCostDetail(p *PhysicalTableReader, opt *optimizetrace.PhysicalOptimizeOp,
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
	appendPlanCostDetail4PhysicalOptimizeOp(opt, detail)
}

func setPhysicalIndexReaderCostDetail(p *PhysicalIndexReader, opt *optimizetrace.PhysicalOptimizeOp,
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
	appendPlanCostDetail4PhysicalOptimizeOp(opt, detail)
}

func setPhysicalHashJoinCostDetail(p *PhysicalHashJoin, opt *optimizetrace.PhysicalOptimizeOp, spill bool,
	buildCnt, probeCnt, cpuFactor, rowSize, numPairs,
	cpuCost, probeCPUCost, memCost, diskCost, probeDiskCost,
	diskFactor, memoryFactor, concurrencyFactor float64,
	memQuota int64) {
	if opt == nil {
		return
	}
	detail := tracing.NewPhysicalPlanCostDetail(p.ID(), p.TP())
	diskCostDetail := &HashJoinDiskCostDetail{
		Spill:           spill,
		UseOuterToBuild: p.UseOuterToBuild,
		BuildRowCount:   buildCnt,
		DiskFactor:      diskFactor,
		RowSize:         rowSize,
		ProbeDiskCost: &HashJoinProbeDiskCostDetail{
			SelectionFactor: cost.SelectionFactor,
			NumPairs:        numPairs,
			HasConditions:   len(p.LeftConditions)+len(p.RightConditions) > 0,
			Cost:            probeDiskCost,
		},
		Cost: diskCost,
	}
	memoryCostDetail := &HashJoinMemoryCostDetail{
		Spill:         spill,
		MemQuota:      memQuota,
		RowSize:       rowSize,
		BuildRowCount: buildCnt,
		MemoryFactor:  memoryFactor,
		Cost:          memCost,
	}
	cpuCostDetail := &HashJoinCPUCostDetail{
		BuildRowCount:     buildCnt,
		CPUFactor:         cpuFactor,
		ConcurrencyFactor: concurrencyFactor,
		ProbeCost: &HashJoinProbeCostDetail{
			NumPairs:        numPairs,
			HasConditions:   len(p.LeftConditions)+len(p.RightConditions) > 0,
			SelectionFactor: cost.SelectionFactor,
			ProbeRowCount:   probeCnt,
			Cost:            probeCPUCost,
		},
		HashJoinConcurrency: p.Concurrency,
		Spill:               spill,
		Cost:                cpuCost,
		UseOuterToBuild:     p.UseOuterToBuild,
	}

	// record cpu cost detail
	detail.AddParam(CPUCostDetailLbl, cpuCostDetail).
		AddParam(CPUCostDescLbl, cpuCostDetail.desc()).
		AddParam(ProbeCostDescLbl, cpuCostDetail.probeCostDesc())
	// record memory cost detail
	detail.AddParam(MemCostDetailLbl, memoryCostDetail).
		AddParam(MemCostDescLbl, memoryCostDetail.desc())
	// record disk cost detail
	detail.AddParam(DiskCostDetailLbl, diskCostDetail).
		AddParam(DiskCostDescLbl, diskCostDetail.desc()).
		AddParam(ProbeDiskCostDescLbl, diskCostDetail.probeDesc())

	detail.SetDesc(fmt.Sprintf("%s+%s+%s+all children cost", CPUCostDetailLbl, MemCostDetailLbl, DiskCostDetailLbl))
	appendPlanCostDetail4PhysicalOptimizeOp(opt, detail)
}

// HashJoinProbeCostDetail indicates probe cpu cost detail
type HashJoinProbeCostDetail struct {
	NumPairs        float64 `json:"numPairs"`
	HasConditions   bool    `json:"hasConditions"`
	SelectionFactor float64 `json:"selectionFactor"`
	ProbeRowCount   float64 `json:"probeRowCount"`
	Cost            float64 `json:"cost"`
}

// HashJoinCPUCostDetail indicates cpu cost detail
type HashJoinCPUCostDetail struct {
	BuildRowCount       float64                  `json:"buildRowCount"`
	CPUFactor           float64                  `json:"cpuFactor"`
	ConcurrencyFactor   float64                  `json:"concurrencyFactor"`
	ProbeCost           *HashJoinProbeCostDetail `json:"probeCost"`
	HashJoinConcurrency uint                     `json:"hashJoinConcurrency"`
	Spill               bool                     `json:"spill"`
	Cost                float64                  `json:"cost"`
	UseOuterToBuild     bool                     `json:"useOuterToBuild"`
}

func (h *HashJoinCPUCostDetail) desc() string {
	var cpuCostDesc string
	buildCostDesc := fmt.Sprintf("%s*%s", BuildRowCountLbl, CPUFactorLbl)
	cpuCostDesc = fmt.Sprintf("%s+%s+(%s+1)*%s)", buildCostDesc, ProbeCostDetailLbl, HashJoinConcurrencyLbl, ConcurrencyFactorLbl)
	if h.UseOuterToBuild {
		if h.Spill {
			cpuCostDesc = fmt.Sprintf("%s+%s", cpuCostDesc, buildCostDesc)
		} else {
			cpuCostDesc = fmt.Sprintf("%s+%s/%s", cpuCostDesc, buildCostDesc, HashJoinConcurrencyLbl)
		}
	}
	return cpuCostDesc
}

func (h *HashJoinCPUCostDetail) probeCostDesc() string {
	var probeCostDesc string
	if h.ProbeCost.HasConditions {
		probeCostDesc = fmt.Sprintf("(%s*%s*%s+%s*%s)/%s",
			NumPairsLbl, CPUFactorLbl, SelectionFactorLbl,
			ProbeRowCountLbl, CPUFactorLbl, HashJoinConcurrencyLbl)
	} else {
		probeCostDesc = fmt.Sprintf("(%s*%s)/%s",
			NumPairsLbl, CPUFactorLbl,
			HashJoinConcurrencyLbl)
	}
	return probeCostDesc
}

// HashJoinMemoryCostDetail indicates memory cost detail
type HashJoinMemoryCostDetail struct {
	Spill         bool    `json:"spill"`
	MemQuota      int64   `json:"memQuota"`
	RowSize       float64 `json:"rowSize"`
	BuildRowCount float64 `json:"buildRowCount"`
	MemoryFactor  float64 `json:"memoryFactor"`
	Cost          float64 `json:"cost"`
}

func (h *HashJoinMemoryCostDetail) desc() string {
	memCostDesc := fmt.Sprintf("%s*%s", BuildRowCountLbl, MemoryFactorLbl)
	if h.Spill {
		memCostDesc = fmt.Sprintf("%s*%s/(%s*%s)", memCostDesc, MemQuotaLbl, RowSizeLbl, BuildRowCountLbl)
	}
	return memCostDesc
}

// HashJoinProbeDiskCostDetail indicates probe disk cost detail
type HashJoinProbeDiskCostDetail struct {
	SelectionFactor float64 `json:"selectionFactor"`
	NumPairs        float64 `json:"numPairs"`
	HasConditions   bool    `json:"hasConditions"`
	Cost            float64 `json:"cost"`
}

// HashJoinDiskCostDetail indicates disk cost detail
type HashJoinDiskCostDetail struct {
	Spill           bool                         `json:"spill"`
	UseOuterToBuild bool                         `json:"useOuterToBuild"`
	BuildRowCount   float64                      `json:"buildRowCount"`
	DiskFactor      float64                      `json:"diskFactor"`
	RowSize         float64                      `json:"rowSize"`
	ProbeDiskCost   *HashJoinProbeDiskCostDetail `json:"probeDiskCost"`
	Cost            float64                      `json:"cost"`
}

func (h *HashJoinDiskCostDetail) desc() string {
	if !h.Spill {
		return ""
	}
	buildDiskCost := fmt.Sprintf("%s*%s*%s", BuildRowCountLbl, DiskFactorLbl, RowSizeLbl)
	desc := fmt.Sprintf("%s+%s", buildDiskCost, ProbeDiskCostLbl)
	if h.UseOuterToBuild {
		desc = fmt.Sprintf("%s+%s", desc, buildDiskCost)
	}
	return desc
}

func (h *HashJoinDiskCostDetail) probeDesc() string {
	if !h.Spill {
		return ""
	}
	desc := fmt.Sprintf("%s*%s*%s", NumPairsLbl, DiskFactorLbl, RowSizeLbl)
	if h.ProbeDiskCost.HasConditions {
		desc = fmt.Sprintf("%s*%s", desc, SelectionFactorLbl)
	}
	return desc
}
