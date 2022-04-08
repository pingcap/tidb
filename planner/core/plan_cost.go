package core

import (
	"github.com/pingcap/tidb/statistics"
	"math"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/property"
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

// ============================== Selection/Projection ==============================

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

func (p *PhysicalProjection) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}
	p.planCost = p.children[0].CalPlanCost(taskType)
	p.planCost += p.GetCost(p.StatsCount())
	p.planCostInit = true
	return p.planCost
}

// ============================== DataSource ==============================

func (p *PhysicalIndexLookUpReader) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}
	p.planCost = p.indexPlan.CalPlanCost(taskType)
	p.planCost += p.tablePlan.CalPlanCost(taskType)

	// index net-cost
	idxCount := p.indexPlan.StatsCount()
	indexWidth := getHistCollSafely(p).GetAvgRowSize(p.ctx, p.indexPlan.Schema().Columns, true, false)
	p.planCost += idxCount * indexWidth * p.ctx.GetSessionVars().GetNetworkFactor(nil)

	// table net cost
	tblScanWidth := getHistCollSafely(p).GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false)
	tblCount := p.tablePlan.StatsCount()
	p.planCost += tblCount * p.ctx.GetSessionVars().GetNetworkFactor(nil) * tblScanWidth

	// consider concurrency
	p.planCost /= float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())

	p.planCost += p.GetCost() // lookup-cost on TiDB
	p.planCostInit = true
	return p.planCost
}

func (p *PhysicalIndexReader) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}
	p.planCost = p.indexPlan.CalPlanCost(property.CopSingleReadTaskType) // accumulate child's cost

	// accumulate net-cost
	rowCount := p.indexPlan.StatsCount()
	netFactor := p.ctx.GetSessionVars().GetNetworkFactor(nil)
	rowWidth :=  getHistCollSafely(p).GetAvgRowSize(p.ctx, p.indexPlan.Schema().Columns, true, false)
	p.planCost += rowCount * rowWidth * netFactor

	// consider concurrency
	copIterWorkers := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	p.planCost /= copIterWorkers
	p.planCostInit = true
	return p.planCost
}

func (p *PhysicalTableReader) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}
	p.planCost = 0
	netFactor := p.ctx.GetSessionVars().GetNetworkFactor(nil)
	switch p.StoreType {
	case kv.TiKV:
		p.planCost += p.tablePlan.CalPlanCost(property.CopSingleReadTaskType) //  child's cost
		rowWidth := getHistCollSafely(p).GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false)
		p.planCost += p.tablePlan.StatsCount() * rowWidth * netFactor // net cost
		p.planCost /= float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	case kv.TiFlash:
		p.planCost += p.tablePlan.CalPlanCost(property.MppTaskType) //  child's cost
		rowWidth := collectRowSizeFromMPPPlan(p.tablePlan)
		p.planCost += p.tablePlan.StatsCount() * rowWidth * netFactor // net cost
		p.planCost /= p.ctx.GetSessionVars().CopTiFlashConcurrencyFactor
	}
	p.planCostInit = true
	return p.planCost
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

// ============================== Scan ==============================

func (p *PhysicalTableScan) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}
	p.planCost = 0

	// scan cost
	rowCount := p.StatsCount()
	rowWidth := p.tableRowSize
	if rowWidth == 0 {
		switch p.StoreType {
		case kv.TiKV:
			rowWidth = getHistCollSafely(p).GetTableAvgRowSize(p.ctx, p.schema.Columns, kv.TiKV, true)
		default:
			rowWidth = getHistCollSafely(p).GetTableAvgRowSize(p.ctx, p.schema.Columns, p.StoreType, p.HandleCols != nil)
		}
	}
	scanFactor := p.ctx.GetSessionVars().GetScanFactor(nil)
	if p.Desc {
		scanFactor = p.ctx.GetSessionVars().GetDescScanFactor(nil)
	}
	p.planCost += rowCount * rowWidth * scanFactor

	// request cost
	switch p.StoreType {
	case kv.TiKV:
		p.planCost += float64(len(p.Ranges)) * p.ctx.GetSessionVars().GetSeekFactor(nil)
	case kv.TiFlash:
		p.planCost += float64(len(p.Ranges)) * float64(len(p.Columns)) * p.ctx.GetSessionVars().GetSeekFactor(nil)
	}

	p.planCostInit = true
	return p.planCost
}

func (p *PhysicalIndexScan) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}
	p.planCost = 0

	// scan cost
	scanFactor := p.ctx.GetSessionVars().GetScanFactor(nil)
	if p.Desc {
		scanFactor = p.ctx.GetSessionVars().GetDescScanFactor(nil)
	}
	p.planCost += p.StatsCount() * p.indexRowSize * scanFactor

	// request cost
	p.planCost += float64(len(p.Ranges)) * p.ctx.GetSessionVars().GetSeekFactor(nil)

	p.planCostInit = true
	return p.planCost
}

// ============================== Join ==============================

func (p *PhysicalIndexJoin) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}
	outerChild, innerChild := p.children[1-p.InnerChildIdx], p.children[p.InnerChildIdx]
	p.planCost = p.GetCost(outerChild.StatsCount(), innerChild.StatsCount(), outerChild.CalPlanCost(taskType), innerChild.CalPlanCost(taskType))
	p.planCostInit = true
	return p.planCost
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
	if p.planCostInit {
		return p.planCost
	}
	lChild, rChild := p.children[0], p.children[1]
	lCnt, rCnt := lChild.StatsCount(), rChild.StatsCount()
	lCost, rCost := lChild.CalPlanCost(taskType), rChild.CalPlanCost(taskType)
	p.planCost = p.GetCost(lCnt, rCnt, lCost, rCost)
	p.planCostInit = true
	return p.planCost
}

// ============================== Agg ==============================

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

// ============================== Sort/Limit ==============================

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

// ============================== PointGet ==============================

func (p *BatchPointGetPlan) CalPlanCost(taskType property.TaskType) float64 {
	return 0
}

func (p *BatchPointGetPlan) CalRowWidth() float64 {
	panic("TODO")
}

func (p *PointGetPlan) CalRowWidth() float64 {
	panic("TODO")
}

func (p *PointGetPlan) CalPlanCost(taskType property.TaskType) float64 {
	return 0
}

// ============================== Others ==============================

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
	if p.planCostInit {
		return p.planCost
	}
	p.planCost = p.children[0].CalPlanCost(taskType)
	// accumulate net cost
	// TODO: this formula is wrong since it doesn't consider tableRowSize, fix it later
	p.planCost += p.children[0].StatsCount() * p.ctx.GetSessionVars().GetNetworkFactor(nil)
	p.planCostInit = true
	return p.planCost
}

// HistColl may not be propagated to some upper operators, so get HistColl recursively for safety.
func getHistCollSafely(p PhysicalPlan) *statistics.HistColl {
	if p.Stats().HistColl != nil {
		return p.Stats().HistColl
	}
	var children []PhysicalPlan
	switch x := p.(type) {
	case *PhysicalTableReader:
		children = append(children, x.tablePlan)
	case *PhysicalIndexReader:
		children = append(children, x.indexPlan)
	case *PhysicalIndexLookUpReader:
		children = append(children, x.tablePlan, x.indexPlan)
	case *PhysicalIndexMergeReader:
		children = append(children, x.tablePlan)
		children = append(children, x.partialPlans...)
	default:
		children = append(children, p.Children()...)
	}
	for _, c := range children {
		if hist := getHistCollSafely(c); hist != nil {
			return hist
		}
	}
	return nil
}
