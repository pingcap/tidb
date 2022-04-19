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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/statistics"
)

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *basePhysicalPlan) GetPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		// just calculate the cost once and always reuse it
		return p.planCost, nil
	}
	p.planCost = 0 // the default implementation, the operator have no cost
	for _, child := range p.children {
		childCost, err := child.GetPlanCost(taskType)
		if err != nil {
			return 0, err
		}
		p.planCost += childCost
	}
	p.planCostInit = true
	return p.planCost, nil
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalSelection) GetPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	var cpuFactor float64
	switch taskType {
	case property.RootTaskType, property.MppTaskType:
		cpuFactor = p.ctx.GetSessionVars().CPUFactor
	case property.CopSingleReadTaskType, property.CopDoubleReadTaskType:
		cpuFactor = p.ctx.GetSessionVars().CopCPUFactor
	default:
		return 0, errors.Errorf("unknown task type %v", taskType)
	}
	childCost, err := p.children[0].GetPlanCost(taskType)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost
	p.planCost += p.children[0].StatsCount() * cpuFactor // selection cost: rows * cpu-factor
	p.planCostInit = true
	return p.planCost, nil
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalProjection) GetPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	childCost, err := p.children[0].GetPlanCost(taskType)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost
	p.planCost += p.GetCost(p.StatsCount()) // projection cost
	p.planCostInit = true
	return p.planCost, nil
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexLookUpReader) GetPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	p.planCost = 0
	// child's cost
	for _, child := range []PhysicalPlan{p.indexPlan, p.tablePlan} {
		childCost, err := child.GetPlanCost(taskType)
		if err != nil {
			return 0, err
		}
		p.planCost += childCost
	}

	// index-side net I/O cost: rows * row-size * net-factor
	netFactor := p.ctx.GetSessionVars().GetNetworkFactor(nil)
	rowSize := getTblStats(p.indexPlan).GetAvgRowSize(p.ctx, p.indexPlan.Schema().Columns, true, false)
	p.planCost += p.indexPlan.StatsCount() * rowSize * netFactor

	// index-side net seek cost
	p.planCost += estimateNetSeekCost(p.indexPlan)

	// table-side net I/O cost: rows * row-size * net-factor
	tblRowSize := getTblStats(p.tablePlan).GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false)
	p.planCost += p.tablePlan.StatsCount() * tblRowSize * netFactor

	// consider concurrency
	p.planCost /= float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())

	// lookup-cost(table-size seek cost)
	p.planCost += p.GetCost()
	p.planCostInit = true
	return p.planCost, nil
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexReader) GetPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	// child's cost
	childCost, err := p.indexPlan.GetPlanCost(property.CopSingleReadTaskType)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost
	// net I/O cost: rows * row-size * net-factor
	tblStats := getTblStats(p.indexPlan)
	rowSize := tblStats.GetAvgRowSize(p.ctx, p.indexPlan.Schema().Columns, true, false)
	p.planCost += p.indexPlan.StatsCount() * rowSize * p.ctx.GetSessionVars().GetNetworkFactor(nil)
	// net seek cost
	p.planCost += estimateNetSeekCost(p.indexPlan)
	// consider concurrency
	p.planCost /= float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())

	p.planCostInit = true
	return p.planCost, nil
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalTableReader) GetPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	p.planCost = 0
	netFactor := p.ctx.GetSessionVars().GetNetworkFactor(nil)
	switch p.StoreType {
	case kv.TiKV:
		// child's cost
		childCost, err := p.tablePlan.GetPlanCost(property.CopSingleReadTaskType)
		if err != nil {
			return 0, err
		}
		p.planCost = childCost
		// net I/O cost: rows * row-size * net-factor
		rowSize := getTblStats(p.tablePlan).GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false)
		p.planCost += p.tablePlan.StatsCount() * rowSize * netFactor
		// net seek cost
		p.planCost += estimateNetSeekCost(p.tablePlan)
		// consider concurrency
		p.planCost /= float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	case kv.TiFlash:
		return 0, errors.New("not implemented")
	}
	p.planCostInit = true
	return p.planCost, nil
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexMergeReader) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalTableScan) GetPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	// scan cost: rows * row-size * scan-factor
	scanFactor := p.ctx.GetSessionVars().GetScanFactor(nil)
	if p.Desc {
		scanFactor = p.ctx.GetSessionVars().GetDescScanFactor(nil)
	}
	p.planCost = p.StatsCount() * p.getScanRowSize() * scanFactor
	p.planCostInit = true
	return p.planCost, nil
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexScan) GetPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	// scan cost: rows * row-size * scan-factor
	scanFactor := p.ctx.GetSessionVars().GetScanFactor(nil)
	if p.Desc {
		scanFactor = p.ctx.GetSessionVars().GetDescScanFactor(nil)
	}
	p.planCost = p.StatsCount() * p.getScanRowSize() * scanFactor
	p.planCostInit = true
	return p.planCost, nil
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexJoin) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexHashJoin) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexMergeJoin) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalApply) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalMergeJoin) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalHashJoin) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalStreamAgg) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalHashAgg) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalSort) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalTopN) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *BatchPointGetPlan) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PointGetPlan) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalUnionAll) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalExchangeReceiver) GetPlanCost(taskType property.TaskType) (float64, error) {
	return 0, errors.New("not implemented")
}

// estimateNetSeekCost calculates the net seek cost for the plan.
// for TiKV, it's len(access-range) * seek-factor,
// and for TiFlash, it's len(access-range) * len(access-column) * seek-factor.
func estimateNetSeekCost(copTaskPlan PhysicalPlan) float64 {
	switch x := copTaskPlan.(type) {
	case *PhysicalTableScan:
		if x.StoreType == kv.TiFlash { // the old TiFlash interface uses cop-task protocol
			return float64(len(x.Ranges)) * float64(len(x.Columns)) * x.ctx.GetSessionVars().GetSeekFactor(x.Table)
		} else { // TiKV
			return float64(len(x.Ranges)) * x.ctx.GetSessionVars().GetSeekFactor(x.Table)
		}
	case *PhysicalIndexScan: // TiKV
		return float64(len(x.Ranges)) * x.ctx.GetSessionVars().GetSeekFactor(x.Table)
	default:
		return estimateNetSeekCost(copTaskPlan.Children()[0])
	}
}

// getTblStats returns the tbl-stats of this plan, which contains all columns before pruning.
func getTblStats(copTaskPlan PhysicalPlan) *statistics.HistColl {
	switch x := copTaskPlan.(type) {
	case *PhysicalTableScan:
		return x.tblColHists
	case *PhysicalIndexScan:
		return x.tblColHists
	default:
		return getTblStats(copTaskPlan.Children()[0])
	}
}
