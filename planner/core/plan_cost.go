package core

import (
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/property"
	"math"
)

func (p *basePhysicalPlan) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}
	// the default implementation, the operator have no cost
	p.planCost = 0
	for _, child := range p.children {
		p.planCost += child.CalPlanCost(taskType)
	}
	p.planCostInit = true
	return p.planCost
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
	if p.planCostInit {
		return p.planCost
	}
	var childMaxCost float64
	for _, child := range p.children {
		childMaxCost = math.Max(childMaxCost, child.CalPlanCost(taskType))
	}
	p.planCost = childMaxCost + float64(1+len(p.children))*p.ctx.GetSessionVars().ConcurrencyFactor
	p.planCostInit = true
	return p.planCost
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

func (p *PhysicalApply) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
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
	if p.planCostInit {
		return p.planCost
	}
	var cpuFactor float64
	switch taskType {
	case property.RootTaskType:
		cpuFactor = p.ctx.GetSessionVars().CPUFactor
	case property.CopSingleReadTaskType, property.CopDoubleReadTaskType:
		cpuFactor = p.ctx.GetSessionVars().CopCPUFactor
	case property.MppTaskType:
		cpuFactor = p.ctx.GetSessionVars().CPUFactor // TODO: introduce a new factor for TiFlash?
	default:
		panic("TODO")
	}
	p.planCost = p.children[0].CalPlanCost(taskType)
	p.planCost += cpuFactor * p.children[0].StatsCount()
	p.planCostInit = true
	return p.planCost
}

func (p PhysicalMaxOneRow) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *PhysicalTableDual) CalPlanCost(taskType property.TaskType) float64 {
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

func (p *PhysicalIndexJoin) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *PhysicalUnionScan) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
}

func (p *BatchPointGetPlan) CalRowWidth() float64 {
	panic("TODO")
}

func (p *PointGetPlan) CalRowWidth() float64 {
	panic("TODO")
}
