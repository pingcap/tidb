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
	// child's cost
	p.planCost = p.indexPlan.CalPlanCost(taskType)
	p.planCost += p.tablePlan.CalPlanCost(taskType)

	// index net I/O cost
	idxCount := p.indexPlan.StatsCount()
	idxRowSize := getHistCollSafely(p).GetAvgRowSize(p.ctx, p.indexPlan.Schema().Columns, true, false)
	p.planCost += idxCount * idxRowSize * p.ctx.GetSessionVars().GetNetworkFactor(nil)

	// index net seek cost
	p.planCost += getSeekCost(p)

	// table net I/O cost
	tblRowSize := getHistCollSafely(p).GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false)
	tblCount := p.tablePlan.StatsCount()
	p.planCost += tblCount * p.ctx.GetSessionVars().GetNetworkFactor(nil) * tblRowSize

	// consider concurrency
	p.planCost /= float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())

	// lookup-cost (table net seek cost)
	// TODO: consider lookup concurrency for this cost?
	p.planCost += p.GetCost()
	p.planCostInit = true
	return p.planCost
}

func (p *PhysicalIndexReader) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}

	// child's cost
	p.planCost = p.indexPlan.CalPlanCost(property.CopSingleReadTaskType)
	// net I/O cost
	rowSize := getHistCollSafely(p).GetAvgRowSize(p.ctx, p.indexPlan.Schema().Columns, true, false)
	p.planCost += p.indexPlan.StatsCount() * rowSize * p.ctx.GetSessionVars().GetNetworkFactor(nil)
	// net seek cost
	p.planCost += getSeekCost(p)
	// consider concurrency
	p.planCost /= float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())

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
		p.planCost += p.tablePlan.CalPlanCost(property.CopSingleReadTaskType)
		// net I/O cost
		rowSize := getHistCollSafely(p).GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false)
		p.planCost += p.tablePlan.StatsCount() * rowSize * netFactor
		// net seek cost
		p.planCost += getSeekCost(p)
		// consider concurrency
		p.planCost /= float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	case kv.TiFlash:
		p.planCost += p.tablePlan.CalPlanCost(property.MppTaskType) //  child's cost
		// net I/O cost
		rowSize := collectRowSizeFromMPPPlan(p.tablePlan)
		p.planCost += p.tablePlan.StatsCount() * rowSize * netFactor
		// net seek cost
		p.planCost += getSeekCost(p)
		// consider concurrency
		p.planCost /= p.ctx.GetSessionVars().CopTiFlashConcurrencyFactor
	}

	p.planCostInit = true
	return p.planCost
}

func (p *PhysicalIndexMergeReader) CalPlanCost(taskType property.TaskType) float64 {
	panic("TODO")
	//if p.planCostInit {
	//	return p.planCost
	//}
	//p.planCost = 0
	//netFactor := p.ctx.GetSessionVars().GetNetworkFactor(nil)
	//if tblScan := p.tablePlan; tblScan != nil {
	//	p.planCost += tblScan.CalPlanCost(property.CopSingleReadTaskType) // child's cost
	//	tableRowSize := getBottomPlan(tblScan).(*PhysicalTableScan).tableRowSize
	//	p.planCost += tblScan.StatsCount() * tableRowSize * netFactor // accumulate net-cost
	//}
	//for _, idxScan := range p.partialPlans {
	//	p.planCost += idxScan.CalPlanCost(property.CopSingleReadTaskType) // child's cost
	//	indexRowSize := getBottomPlan(idxScan).(*PhysicalIndexScan).indexRowSize
	//	p.planCost += idxScan.StatsCount() * indexRowSize * netFactor // accumulate net-cost
	//}
	//
	//// consider concurrency
	//copIterWorkers := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	//p.planCost /= copIterWorkers
	//p.planCostInit = true
	//return p.planCost
}

// ============================== Scan ==============================

func (p *PhysicalTableScan) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}
	p.planCost = 0

	// scan cost
	rowCount := p.StatsCount()
	var rowSize float64
	switch p.StoreType {
	case kv.TiKV:
		rowSize = getHistCollSafely(p).GetTableAvgRowSize(p.ctx, p.schema.Columns, kv.TiKV, true)
	default:
		rowSize = getHistCollSafely(p).GetTableAvgRowSize(p.ctx, p.schema.Columns, p.StoreType, p.HandleCols != nil)
	}
	scanFactor := p.ctx.GetSessionVars().GetScanFactor(nil)
	if p.Desc {
		scanFactor = p.ctx.GetSessionVars().GetDescScanFactor(nil)
	}
	p.planCost += rowCount * rowSize * scanFactor
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
	rowSize := p.indexScanRowSize(p.Index, p.dataSource, true)
	p.planCost += p.StatsCount() * rowSize * scanFactor

	p.planCostInit = true
	return p.planCost
}

// ============================== Join ==============================

func (p *PhysicalIndexJoin) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}
	outerChild, innerChild := p.children[1-p.InnerChildIdx], p.children[p.InnerChildIdx]
	// NOTICE: seek cost of the probe side is not considered in the old model so minus it here for compatibility, we'll fix it later
	probeSeekCost := getSeekCost(innerChild) / float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	p.planCost = p.GetCost(outerChild.StatsCount(), innerChild.StatsCount(), outerChild.CalPlanCost(taskType), innerChild.CalPlanCost(taskType)-probeSeekCost)
	p.planCostInit = true
	return p.planCost
}

func (p *PhysicalIndexHashJoin) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}
	outerChild, innerChild := p.children[1-p.InnerChildIdx], p.children[p.InnerChildIdx]
	// NOTICE: same as IndexJoin
	probeSeekCost := getSeekCost(innerChild) / float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	p.planCost = p.GetCost(outerChild.StatsCount(), innerChild.StatsCount(), outerChild.CalPlanCost(taskType), innerChild.CalPlanCost(taskType)-probeSeekCost)
	p.planCostInit = true
	return p.planCost
}

func (p *PhysicalIndexMergeJoin) CalPlanCost(taskType property.TaskType) float64 {
	if p.planCostInit {
		return p.planCost
	}
	outerChild, innerChild := p.children[1-p.InnerChildIdx], p.children[p.InnerChildIdx]
	// NOTICE: same as IndexJoin
	probeSeekCost := getSeekCost(innerChild) / float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	p.planCost = p.GetCost(outerChild.StatsCount(), innerChild.StatsCount(), outerChild.CalPlanCost(taskType), innerChild.CalPlanCost(taskType)-probeSeekCost)
	p.planCostInit = true
	return p.planCost
}

func (p *PhysicalApply) CalPlanCost(taskType property.TaskType) float64 {
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

func getSeekCost(p PhysicalPlan) float64 {
	switch x := p.(type) {
	case *PhysicalTableReader:
		return getSeekCost(x.tablePlan)
	case *PhysicalIndexReader:
		return getSeekCost(x.indexPlan)
	case *PhysicalIndexLookUpReader:
		return getSeekCost(x.indexPlan)
	case *PhysicalTableScan:
		if x.StoreType == kv.TiFlash {
			return float64(len(x.Ranges)) * float64(len(x.Columns)) * x.ctx.GetSessionVars().GetSeekFactor(x.Table)
		} else { // TiKV
			return float64(len(x.Ranges)) * x.ctx.GetSessionVars().GetSeekFactor(x.Table)
		}
	case *PhysicalIndexScan:
		return float64(len(x.Ranges)) * x.ctx.GetSessionVars().GetSeekFactor(x.Table)
	default:
		return getSeekCost(p.Children()[0])
	}
}

func getBottomPlan(p PhysicalPlan) PhysicalPlan {
	if len(p.Children()) == 0 {
		return p
	}
	return getBottomPlan(p.Children()[0])
}
