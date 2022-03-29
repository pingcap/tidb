package core

import (
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/property"
)

func calRowWidth4Operator(retCols []*expression.Column) float64 {
	panic("TODO")
}

func (p *PhysicalSort) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}
	p.planCost = 0
	for _, child := range p.children {
		p.planCost += child.CalPlanCost(taskType)
	}
	p.planCost += p.GetCost(p.children[0].StatsCount(), p.Schema())
	p.planCostInit = true
	return p.planCost
}

func (p *PhysicalStreamAgg) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}
	p.planCost = 0
	for _, child := range p.children {
		p.planCost += child.CalPlanCost(taskType)
	}
	p.planCost += p.GetCost(p.children[0].StatsCount(), taskType == property.RootTaskType)
	p.planCostInit = true
	return p.planCost
}

func (p *PhysicalHashAgg) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}
	p.planCost = 0
	for _, child := range p.children {
		p.planCost += child.CalPlanCost(taskType)
	}
	switch taskType {
	case property.RootTaskType:
		p.planCost += p.GetCost(p.children[0].StatsCount(), true, false)
	case property.CopSingleReadTaskType, property.CopDoubleReadTaskType:
		p.planCost += p.GetCost(p.children[0].StatsCount(), false, false)
	case property.MppTaskType:
		p.planCost += p.GetCost(p.children[0].StatsCount(), false, true)
	default:
		panic("TODO")
	}
	p.planCostInit = true
	return p.planCost
}

func (p *PhysicalUnionAll) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *PhysicalExchangeReceiver) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *PhysicalExchangeSender) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *PhysicalMergeJoin) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}
	p.planCost = 0
	for _, child := range p.children {
		p.planCost += child.CalPlanCost(taskType)
	}
	p.planCost += p.GetCost(p.children[0].StatsCount(), p.children[1].StatsCount())
	p.planCostInit = true
	return p.planCost
}

func (p *PhysicalHashJoin) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}
	p.planCost = 0
	for _, child := range p.children {
		p.planCost += child.CalPlanCost(taskType)
	}
	p.planCost += p.GetCost(p.children[0].StatsCount(), p.children[1].StatsCount())
	p.planCostInit = true
	return p.planCost
}

func (p *PhysicalTopN) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}
	p.planCost = 0
	for _, child := range p.children {
		p.planCost += child.CalPlanCost(taskType)
	}
	p.planCost += p.GetCost(p.children[0].StatsCount(), taskType == property.RootTaskType)
	p.planCostInit = true
	return p.planCost

}

func (p *PhysicalProjection) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}
	p.planCost = 0
	for _, child := range p.children {
		p.planCost += child.CalPlanCost(taskType)
	}
	p.planCost += p.GetCost(p.children[0].StatsCount())
	p.planCostInit = true
	return p.planCost
}

func (p *PhysicalTableScan) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *PhysicalIndexScan) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *PhysicalIndexLookUpReader) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *PhysicalIndexReader) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *PhysicalTableReader) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}
	p.planCost = 0
	// accumulate child's cost
	childTaskType := property.CopSingleReadTaskType
	if p.StoreType == kv.TiFlash {
		childTaskType = property.MppTaskType
	}
	p.planCost += p.tablePlan.CalPlanCost(childTaskType)

	// accumulate net-cost
	rowCount := p.tablePlan.StatsCount()
	netFactor := p.ctx.GetSessionVars().GetNetworkFactor(nil)
	rowWidth := p.tablePlan.CalRowWidth()
	p.planCost += rowCount * rowWidth * netFactor

	// take concurrency into account
	copIterWorkers := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	p.planCost /= copIterWorkers
	p.planCostInit = true
	return p.planCost
}

func (p *PhysicalSelection) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *PhysicalLimit) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p PhysicalMaxOneRow) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *PhysicalTableDual) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *NominalSort) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *PhysicalIndexMergeReader) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}
	p.planCost = 0
	netFactor := p.ctx.GetSessionVars().GetNetworkFactor(nil)
	if tblScan := p.tablePlan; tblScan != nil {
		rowCount := tblScan.StatsCount()
		rowWidth := p.tablePlan.CalRowWidth()
		p.planCost += rowCount * rowWidth * netFactor                     // accumulate net-cost
		p.planCost += tblScan.CalPlanCost(property.CopSingleReadTaskType) // accumulate child's cost
	}
	for _, idxScan := range p.partialPlans {
		rowCount := idxScan.StatsCount()
		rowWidth := idxScan.CalRowWidth()
		p.planCost += rowCount * rowWidth * netFactor                     // accumulate net-cost
		p.planCost += idxScan.CalPlanCost(property.CopSingleReadTaskType) // accumulate child's cost
	}

	// consider concurrency
	copIterWorkers := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	p.planCost /= copIterWorkers
	p.planCostInit = true
	return p.planCost
}

func (p *PhysicalLock) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *PhysicalIndexJoin) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *PhysicalUnionScan) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *PhysicalWindow) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *PhysicalShuffle) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *PhysicalShuffleReceiverStub) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *PhysicalTableSample) CalPlanCost(taskType property.TaskType) float64 {
	return 0
}

func (p *BatchPointGetPlan) CalPlanCost(taskType property.TaskType) float64 {
	return 0
}

func (p *BatchPointGetPlan) CalRowWidth() float64 {
	panic("TODO")
}

func (p *PointGetPlan) CalPlanCost(taskType property.TaskType) float64 {
	return 0
}

func (p *PointGetPlan) CalRowWidth() float64 {
	panic("TODO")
}

func (p *PhysicalShow) CalPlanCost(taskType property.TaskType) float64 {
	return 0
}

func (p *PhysicalShowDDLJobs) CalPlanCost(taskType property.TaskType) float64 {
	return 0
}

func (p *PhysicalMemTable) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *PhysicalCTE) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *PhysicalCTETable) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *basePhysicalAgg) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *PhysicalSimpleWrapper) CalPlanCost(taskType property.TaskType) float64 {
	return 0
}
