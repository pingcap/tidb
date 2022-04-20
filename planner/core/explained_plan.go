package core

import (
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/texttree"
)

type ExplainedPhysicalPlan struct {
	Main []*ExplainedPhysicalOperator
	CTEs [][]*ExplainedPhysicalOperator

	ctesToExplain []*PhysicalCTE

	// We'll need to get runtime stats from the session.
	explainCtx sessionctx.Context
}

type ExplainedPhysicalOperator struct {
	// A reference to the original operator. May be useful when the information below is insufficient.
	Origin Plan

	// ID, position and classification
	ExplainID         string
	TextTreeExplainID string
	Depth             uint64
	DriverSide        DriverSide
	IsRoot            bool
	StoreType         kv.StoreType
	// ReqType is only meaningful when IsRoot is false.
	ReqType readReqType

	// Basic operator information
	StatsInfoAvailable bool
	// EstRows is only meaningful when StatsInfoAvailable is true.
	EstRows float64

	ExplainInfo string

	IsPhysicalPlan bool
	// EstCost is only meaningful when IsPhysicalPlan is true.
	EstCost float64

	ActRows     int64
	RootStats   *execdetails.RootRuntimeStats
	CopStats    *execdetails.CopRuntimeStats
	MemTracker  *memory.Tracker
	DiskTracker *memory.Tracker
}

type DriverSide uint

const (
	Empty DriverSide = iota
	BuildSide
	ProbeSide
	SeedPart
	RecursivePart
)

func (d DriverSide) String() string {
	switch d {
	case Empty:
		return ""
	case BuildSide:
		return "(Build)"
	case ProbeSide:
		return "(Probe)"
	case SeedPart:
		return "(Seed Part)"
	case RecursivePart:
		return "(Recursive Part)"
	}
	return ""
}

type additionalInfo struct {
	depth       uint64
	driverSide  DriverSide
	isRoot      bool
	storeType   kv.StoreType
	reqType     readReqType
	indent      string
	isLastChild bool
}

func ExplainPhysicalPlan(p Plan, sctx sessionctx.Context) *ExplainedPhysicalPlan {
	res := &ExplainedPhysicalPlan{explainCtx: sctx}
	initInfo := &additionalInfo{
		depth:       0,
		driverSide:  Empty,
		isRoot:      true,
		storeType:   kv.TiDB,
		indent:      "",
		isLastChild: true,
	}
	res.explainRecursively(p, initInfo)

	explainedCTEPlan := make(map[int]struct{})
	for _, cte := range res.ctesToExplain {
		cteDef := (*CTEDefinition)(cte)
		if _, ok := explainedCTEPlan[cteDef.CTE.IDForStorage]; ok {
			continue
		}
		explained := res.explainSingle(cteDef, initInfo)
		if explained != nil {
			res.Main = append(res.Main, explained)
		}
		childInfo := &additionalInfo{
			depth:       initInfo.depth + 1,
			driverSide:  SeedPart,
			isRoot:      true,
			storeType:   kv.TiDB,
			indent:      texttree.Indent4Child(initInfo.indent, initInfo.isLastChild),
			isLastChild: cteDef.RecurPlan == nil,
		}
		res.explainRecursively(cteDef.SeedPlan, childInfo)
		if cteDef.RecurPlan != nil {
			childInfo.driverSide = RecursivePart
			childInfo.isLastChild = true
			res.explainRecursively(cteDef.RecurPlan, childInfo)
		}
		explainedCTEPlan[cteDef.CTE.IDForStorage] = struct{}{}
	}
	return res
}

func (f *ExplainedPhysicalPlan) explainSingle(p Plan, info *additionalInfo) *ExplainedPhysicalOperator {
	rawId := p.ExplainID().String()
	if rawId == "_0" {
		return nil
	}
	res := &ExplainedPhysicalOperator{
		Origin:            p,
		ExplainID:         rawId,
		TextTreeExplainID: texttree.PrettyIdentifier(rawId+info.driverSide.String(), info.indent, info.isLastChild),
		DriverSide:        info.driverSide,
		IsRoot:            info.isRoot,
		StoreType:         info.storeType,
		Depth:             info.depth,
	}

	if si := p.statsInfo(); si != nil {
		res.StatsInfoAvailable = true
		res.EstRows = si.RowCount
	} else {
		res.StatsInfoAvailable = false
	}

	if pp, ok := p.(PhysicalPlan); ok {
		res.IsPhysicalPlan = true
		res.EstCost = pp.Cost()
	}

	res.ExplainInfo = p.ExplainInfo()

	runtimeStats := f.explainCtx.GetSessionVars().StmtCtx.RuntimeStatsColl
	id := p.ID()
	if runtimeStats != nil {
		if runtimeStats.ExistsRootStats(id) {
			rootStats := runtimeStats.GetRootStats(id)
			res.RootStats = rootStats
			res.ActRows = rootStats.GetActRows()
		}
		if runtimeStats.ExistsCopStats(id) {
			copStats := runtimeStats.GetCopStats(id)
			res.CopStats = copStats
			res.ActRows = copStats.GetActRows()
		}
	}

	res.MemTracker = f.explainCtx.GetSessionVars().StmtCtx.MemTracker.SearchTrackerWithoutLock(id)
	res.DiskTracker = f.explainCtx.GetSessionVars().StmtCtx.DiskTracker.SearchTrackerWithoutLock(id)

	return res
}

// Note that info should not be modified in this method.
func (f *ExplainedPhysicalPlan) explainRecursively(p Plan, info *additionalInfo) {
	explained := f.explainSingle(p, info)
	if explained != nil {
		f.Main = append(f.Main, explained)
	}

	childInfo := &additionalInfo{
		depth:  info.depth + 1,
		indent: texttree.Indent4Child(info.indent, info.isLastChild),
	}
	// For physical operators, we just enumerate their children and collect their information.
	// Note that some physical operators are special, and they are handled below.
	if physPlan, ok := p.(PhysicalPlan); ok {
		driverSideInfo := make([]DriverSide, len(physPlan.Children()))

		switch plan := physPlan.(type) {
		case *PhysicalApply:
			driverSideInfo[plan.InnerChildIdx] = BuildSide
			driverSideInfo[1-plan.InnerChildIdx] = ProbeSide
		case *PhysicalHashJoin:
			if plan.UseOuterToBuild {
				driverSideInfo[plan.InnerChildIdx] = BuildSide
				driverSideInfo[1-plan.InnerChildIdx] = ProbeSide
			} else {
				driverSideInfo[plan.InnerChildIdx] = ProbeSide
				driverSideInfo[1-plan.InnerChildIdx] = BuildSide
			}
		case *PhysicalMergeJoin:
			if plan.JoinType == RightOuterJoin {
				driverSideInfo[0] = BuildSide
				driverSideInfo[1] = ProbeSide
			} else {
				driverSideInfo[0] = ProbeSide
				driverSideInfo[1] = BuildSide
			}
		case *PhysicalIndexJoin:
			driverSideInfo[plan.InnerChildIdx] = BuildSide
			driverSideInfo[1-plan.InnerChildIdx] = ProbeSide
		case *PhysicalIndexMergeJoin:
			driverSideInfo[plan.InnerChildIdx] = BuildSide
			driverSideInfo[1-plan.InnerChildIdx] = ProbeSide
		case *PhysicalIndexHashJoin:
			driverSideInfo[plan.InnerChildIdx] = BuildSide
			driverSideInfo[1-plan.InnerChildIdx] = ProbeSide
		}

		for i := range physPlan.Children() {
			childInfo.isRoot = info.isRoot
			childInfo.storeType = info.storeType
			childInfo.driverSide = driverSideInfo[i]
			childInfo.isLastChild = i == len(physPlan.Children())-1
			f.explainRecursively(physPlan.Children()[i], childInfo)
		}
	}

	// For part of physical operators and some special operators, we need some special logic to get their "children".
	// For PhysicalCTE, we need to add the plan tree into flatTree.ctesToExplain.
	switch plan := p.(type) {
	case *PhysicalTableReader:
		childInfo.isRoot = false
		childInfo.storeType = plan.StoreType
		childInfo.reqType = plan.ReadReqType
		childInfo.driverSide = Empty
		childInfo.isLastChild = true
		f.explainRecursively(plan.tablePlan, childInfo)
	case *PhysicalIndexReader:
		childInfo.isRoot = false
		childInfo.reqType = Cop
		childInfo.storeType = kv.TiKV
		childInfo.driverSide = Empty
		childInfo.isLastChild = true
		f.explainRecursively(plan.indexPlan, childInfo)
	case *PhysicalIndexLookUpReader:
		childInfo.isRoot = false
		childInfo.reqType = Cop
		childInfo.storeType = kv.TiKV
		childInfo.driverSide = BuildSide
		childInfo.isLastChild = false
		f.explainRecursively(plan.indexPlan, childInfo)
		childInfo.driverSide = ProbeSide
		childInfo.isLastChild = true
		f.explainRecursively(plan.tablePlan, childInfo)
	case *PhysicalIndexMergeReader:
		childInfo.isRoot = false
		childInfo.reqType = Cop
		childInfo.storeType = kv.TiKV
		for _, pchild := range plan.partialPlans {
			childInfo.driverSide = BuildSide
			childInfo.isLastChild = false
			f.explainRecursively(pchild, childInfo)
		}
		childInfo.driverSide = ProbeSide
		childInfo.isLastChild = true
		f.explainRecursively(plan.tablePlan, childInfo)
	case *PhysicalShuffleReceiverStub:
		childInfo.isRoot = true
		childInfo.driverSide = Empty
		childInfo.isLastChild = true
		f.explainRecursively(plan.DataSource, childInfo)
	case *PhysicalCTE:
		f.ctesToExplain = append(f.ctesToExplain, plan)
	case *Insert:
		if plan.SelectPlan != nil {
			childInfo.isRoot = true
			childInfo.driverSide = Empty
			childInfo.isLastChild = true
			f.explainRecursively(plan.SelectPlan, childInfo)
		}
	case *Update:
		if plan.SelectPlan != nil {
			childInfo.isRoot = true
			childInfo.driverSide = Empty
			childInfo.isLastChild = true
			f.explainRecursively(plan.SelectPlan, childInfo)
		}
	case *Delete:
		if plan.SelectPlan != nil {
			childInfo.isRoot = true
			childInfo.driverSide = Empty
			childInfo.isLastChild = true
			f.explainRecursively(plan.SelectPlan, childInfo)
		}
	case *Execute:
		if plan.Plan != nil {
			childInfo.isRoot = true
			childInfo.indent = info.indent
			childInfo.driverSide = Empty
			childInfo.isLastChild = true
			f.explainRecursively(plan.Plan, childInfo)
		}
	}
	return
}
