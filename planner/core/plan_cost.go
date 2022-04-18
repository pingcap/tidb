package core

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/statistics"
	"math"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/property"
)

func (p *basePhysicalPlan) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	// the default implementation, the operator have no cost
	p.planCost = 0
	for _, child := range p.children {
		childCost, err := child.CalPlanCost(taskType)
		if err != nil {
			return 0, err
		}
		p.planCost += childCost
	}
	p.planCostInit = true
	return p.planCost, nil
}

// ============================== Selection/Projection ==============================

func (p *PhysicalSelection) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
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
		return 0, errors.Errorf("unknown task type %v", taskType)
	}
	childCost, err := p.children[0].CalPlanCost(taskType)
	if err != nil {
		return 0, err
	}
	p.planCost += childCost
	p.planCost += cpuFactor * p.children[0].StatsCount()
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalProjection) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	childCost, err := p.children[0].CalPlanCost(taskType)
	if err != nil {
		return 0, err
	}
	p.planCost += childCost
	p.planCost += p.GetCost(p.StatsCount())
	p.planCostInit = true
	return p.planCost, nil
}

// ============================== DataSource ==============================

func (p *PhysicalIndexLookUpReader) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	p.planCost = 0
	// child's cost
	for _, child := range []PhysicalPlan{p.indexPlan, p.tablePlan} {
		childCost, err := child.CalPlanCost(taskType)
		if err != nil {
			return 0, err
		}
		p.planCost += childCost
	}

	// index net I/O cost
	idxCount := p.indexPlan.StatsCount()
	tblStats := getTblStats(p.indexPlan)
	rowSize := tblStats.GetAvgRowSize(p.ctx, p.indexPlan.Schema().Columns, true, false)
	p.planCost += idxCount * rowSize * p.ctx.GetSessionVars().GetNetworkFactor(nil)

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
	return p.planCost, nil
}

func (p *PhysicalIndexReader) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}

	// child's cost
	childCost, err := p.indexPlan.CalPlanCost(property.CopSingleReadTaskType)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost
	// net I/O cost
	tblStats := getTblStats(p.indexPlan)
	rowSize := tblStats.GetAvgRowSize(p.ctx, p.indexPlan.Schema().Columns, true, false)
	p.planCost += p.indexPlan.StatsCount() * rowSize * p.ctx.GetSessionVars().GetNetworkFactor(nil)
	// net seek cost
	p.planCost += getSeekCost(p)
	// consider concurrency
	p.planCost /= float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())

	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalTableReader) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}

	p.planCost = 0
	netFactor := p.ctx.GetSessionVars().GetNetworkFactor(nil)
	switch p.StoreType {
	case kv.TiKV:
		// child's cost
		childCost, err := p.tablePlan.CalPlanCost(property.CopSingleReadTaskType)
		if err != nil {
			return 0, err
		}
		p.planCost = childCost
		// net I/O cost
		rowSize := getHistCollSafely(p).GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false)
		p.planCost += p.tablePlan.StatsCount() * rowSize * netFactor
		// net seek cost
		p.planCost += getSeekCost(p)
		// consider concurrency
		p.planCost /= float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	case kv.TiFlash:
		var concurrency, rowSize, seekCost float64
		_, isMPP := p.tablePlan.(*PhysicalExchangeSender)
		if isMPP {
			// mpp protocol
			concurrency = p.ctx.GetSessionVars().CopTiFlashConcurrencyFactor
			rowSize = collectRowSizeFromMPPPlan(p.tablePlan)
			seekCost = accumulateNetSeekCost4MPP(p.tablePlan)
		} else {
			// cop protocol
			concurrency = float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
			rowSize = getHistCollSafely(p).GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false)
			seekCost = getSeekCost(p)
		}

		//  child's cost
		childCost, err := p.tablePlan.CalPlanCost(property.MppTaskType)
		if err != nil {
			return 0, err
		}
		p.planCost = childCost
		// net I/O cost
		p.planCost += p.tablePlan.StatsCount() * rowSize * netFactor
		// net seek cost
		p.planCost += seekCost
		// consider concurrency
		p.planCost /= concurrency
		// consider tidb_enforce_mpp
		if isMPP && p.ctx.GetSessionVars().IsMPPEnforced() {
			p.planCost /= 1000000000
		}
	}

	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalIndexMergeReader) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	p.planCost = 0
	netFactor := p.ctx.GetSessionVars().GetNetworkFactor(nil)
	if tblScan := p.tablePlan; tblScan != nil {
		childCost, err := tblScan.CalPlanCost(property.CopSingleReadTaskType)
		if err != nil {
			return 0, err
		}
		p.planCost += childCost // child's cost
		tblStats := getTblStats(tblScan)
		rowSize := tblStats.GetAvgRowSize(p.ctx, tblScan.Schema().Columns, false, false)
		p.planCost += tblScan.StatsCount() * rowSize * netFactor // net I/O cost
	}
	for _, idxScan := range p.partialPlans {
		childCost, err := idxScan.CalPlanCost(property.CopSingleReadTaskType)
		if err != nil {
			return 0, err
		}
		p.planCost += childCost // child's cost
		tblStats := getTblStats(idxScan)
		rowSize := tblStats.GetAvgRowSize(p.ctx, idxScan.Schema().Columns, true, false)
		p.planCost += idxScan.StatsCount() * rowSize * netFactor // net I/O cost
	}

	// consider concurrency
	copIterWorkers := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	p.planCost /= copIterWorkers
	p.planCostInit = true
	return p.planCost, nil
}

// ============================== Scan ==============================

func (p *PhysicalTableScan) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	p.planCost = 0

	// scan cost
	scanFactor := p.ctx.GetSessionVars().GetScanFactor(nil)
	if p.Desc {
		scanFactor = p.ctx.GetSessionVars().GetDescScanFactor(nil)
	}
	p.planCost += p.StatsCount() * p.getScanRowSize() * scanFactor
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalIndexScan) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	p.planCost = 0

	// scan cost
	scanFactor := p.ctx.GetSessionVars().GetScanFactor(nil)
	if p.Desc {
		scanFactor = p.ctx.GetSessionVars().GetDescScanFactor(nil)
	}
	p.planCost += p.StatsCount() * p.getScanRowSize() * scanFactor

	p.planCostInit = true
	return p.planCost, nil
}

// ============================== Join ==============================

func (p *PhysicalIndexJoin) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	outerChild, innerChild := p.children[1-p.InnerChildIdx], p.children[p.InnerChildIdx]
	outerCost, err := outerChild.CalPlanCost(taskType)
	if err != nil {
		return 0, err
	}
	innerCost, err := innerChild.CalPlanCost(taskType)
	if err != nil {
		return 0, err
	}
	p.planCost = p.GetCost(outerChild.StatsCount(), innerChild.StatsCount(), outerCost, innerCost)
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalIndexHashJoin) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	outerChild, innerChild := p.children[1-p.InnerChildIdx], p.children[p.InnerChildIdx]
	outerCost, err := outerChild.CalPlanCost(taskType)
	if err != nil {
		return 0, err
	}
	innerCost, err := innerChild.CalPlanCost(taskType)
	if err != nil {
		return 0, err
	}
	p.planCost = p.GetCost(outerChild.StatsCount(), innerChild.StatsCount(), outerCost, innerCost)
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalIndexMergeJoin) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	outerChild, innerChild := p.children[1-p.InnerChildIdx], p.children[p.InnerChildIdx]
	outerCost, err := outerChild.CalPlanCost(taskType)
	if err != nil {
		return 0, err
	}
	innerCost, err := innerChild.CalPlanCost(taskType)
	if err != nil {
		return 0, err
	}
	p.planCost = p.GetCost(outerChild.StatsCount(), innerChild.StatsCount(), outerCost, innerCost)
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalApply) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	outerChild, innerChild := p.children[1-p.InnerChildIdx], p.children[p.InnerChildIdx]
	outerCost, err := outerChild.CalPlanCost(taskType)
	if err != nil {
		return 0, err
	}
	innerCost, err := innerChild.CalPlanCost(taskType)
	if err != nil {
		return 0, err
	}
	p.planCost = p.GetCost(outerChild.StatsCount(), innerChild.StatsCount(), outerCost, innerCost)
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalMergeJoin) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	p.planCost = 0
	for _, child := range p.children {
		childCost, err := child.CalPlanCost(taskType)
		if err != nil {
			return 0, err
		}
		p.planCost += childCost
	}
	p.planCost += p.GetCost(p.children[0].StatsCount(), p.children[1].StatsCount())
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalHashJoin) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	p.planCost = 0
	for _, child := range p.children {
		childCost, err := child.CalPlanCost(taskType)
		if err != nil {
			return 0, err
		}
		p.planCost += childCost
	}
	p.planCost += p.GetCost(p.children[0].StatsCount(), p.children[1].StatsCount())
	p.planCostInit = true
	return p.planCost, nil
}

// ============================== Agg ==============================

func (p *PhysicalStreamAgg) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	childCost, err := p.children[0].CalPlanCost(taskType)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost
	p.planCost += p.GetCost(p.children[0].StatsCount(), taskType == property.RootTaskType)
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalHashAgg) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	childCost, err := p.children[0].CalPlanCost(taskType)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost
	switch taskType {
	case property.RootTaskType:
		p.planCost += p.GetCost(p.children[0].StatsCount(), true, false)
	case property.CopSingleReadTaskType, property.CopDoubleReadTaskType:
		p.planCost += p.GetCost(p.children[0].StatsCount(), false, false)
	case property.MppTaskType:
		p.planCost += p.GetCost(p.children[0].StatsCount(), false, true)
	default:
		return 0, errors.Errorf("unknown task type %v", taskType)
	}
	p.planCostInit = true
	return p.planCost, nil
}

// ============================== Sort/Limit ==============================

func (p *PhysicalSort) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	childCost, err := p.children[0].CalPlanCost(taskType)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost
	p.planCost += p.GetCost(p.children[0].StatsCount(), p.Schema())
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalTopN) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	childCost, err := p.children[0].CalPlanCost(taskType)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost
	p.planCost += p.GetCost(p.children[0].StatsCount(), taskType == property.RootTaskType)
	p.planCostInit = true
	return p.planCost, nil
}

// ============================== PointGet ==============================

func (p *BatchPointGetPlan) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, nil
}

func (p *PointGetPlan) CalPlanCost(taskType property.TaskType) (float64, error) {
	return 0, nil
}

// ============================== Others ==============================

func (p *PhysicalUnionAll) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	var childMaxCost float64
	for _, child := range p.children {
		childCost, err := child.CalPlanCost(taskType)
		if err != nil {
			return 0, err
		}
		childMaxCost = math.Max(childMaxCost, childCost)
	}
	p.planCost = childMaxCost + float64(1+len(p.children))*p.ctx.GetSessionVars().ConcurrencyFactor
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalExchangeReceiver) CalPlanCost(taskType property.TaskType) (float64, error) {
	if p.planCostInit {
		return p.planCost, nil
	}
	childCost, err := p.children[0].CalPlanCost(taskType)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost
	// accumulate net cost
	// TODO: this formula is wrong since it doesn't consider tableRowSize, fix it later
	p.planCost += p.children[0].StatsCount() * p.ctx.GetSessionVars().GetNetworkFactor(nil)
	p.planCostInit = true
	return p.planCost, nil
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

func getTblStats(p PhysicalPlan) *statistics.HistColl {
	switch x := p.(type) {
	case *PhysicalTableScan:
		return x.tblColHists
	case *PhysicalIndexScan:
		return x.tblColHists
	default:
		for _, c := range p.Children() {
			if tblStats := getTblStats(c); tblStats != nil {
				return tblStats
			}
		}
	}
	return nil
}
