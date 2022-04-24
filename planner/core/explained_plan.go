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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/texttree"
)

type ExplainedPhysicalPlan struct {
	Main ExplainedTree
	CTEs []ExplainedTree

	ctesToExplain []*PhysicalCTE

	// We'll need the session to get the runtime stats.
	explainCtx sessionctx.Context
}

type ExplainedTree []*ExplainedOperator

type ExplainedOperator struct {
	// A reference to the original operator. It will be useful when the information below is insufficient.
	Origin Plan

	// ID, position and classification
	ExplainID         string
	TextTreeExplainID string
	Depth             uint64
	DriverSide        DriverSide
	IsRoot            bool
	StoreType         kv.StoreType
	// ReqType is only meaningful when IsRoot is false.
	ReqType ReadReqType

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

type operatorCtx struct {
	depth       uint64
	driverSide  DriverSide
	isRoot      bool
	storeType   kv.StoreType
	reqType     ReadReqType
	indent      string
	isLastChild bool
}

func ExplainPhysicalPlan(p Plan, sctx sessionctx.Context) *ExplainedPhysicalPlan {
	res := &ExplainedPhysicalPlan{explainCtx: sctx}
	initInfo := &operatorCtx{
		depth:       0,
		driverSide:  Empty,
		isRoot:      true,
		storeType:   kv.TiDB,
		indent:      "",
		isLastChild: true,
	}
	res.Main = res.explainRecursively(p, initInfo, nil)

	explainedCTEPlan := make(map[int]struct{}, len(res.ctesToExplain))
	for _, cte := range res.ctesToExplain {
		cteDef := (*CTEDefinition)(cte)
		if _, ok := explainedCTEPlan[cteDef.CTE.IDForStorage]; ok {
			continue
		}
		cteExplained := res.explainCTERecursively(cteDef, initInfo, nil)
		res.CTEs = append(res.CTEs, cteExplained)
		explainedCTEPlan[cteDef.CTE.IDForStorage] = struct{}{}
	}
	return res
}

func (f *ExplainedPhysicalPlan) explainSingle(p Plan, info *operatorCtx) *ExplainedOperator {
	rawId := p.ExplainID().String()
	if rawId == "_0" {
		return nil
	}
	res := &ExplainedOperator{
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
func (f *ExplainedPhysicalPlan) explainRecursively(p Plan, info *operatorCtx, target ExplainedTree) ExplainedTree {
	explained := f.explainSingle(p, info)
	if explained != nil {
		target = append(target, explained)
	}

	childInfo := &operatorCtx{
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
			target = f.explainRecursively(physPlan.Children()[i], childInfo, target)
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
		target = f.explainRecursively(plan.tablePlan, childInfo, target)
	case *PhysicalIndexReader:
		childInfo.isRoot = false
		childInfo.reqType = Cop
		childInfo.storeType = kv.TiKV
		childInfo.driverSide = Empty
		childInfo.isLastChild = true
		target = f.explainRecursively(plan.indexPlan, childInfo, target)
	case *PhysicalIndexLookUpReader:
		childInfo.isRoot = false
		childInfo.reqType = Cop
		childInfo.storeType = kv.TiKV
		childInfo.driverSide = BuildSide
		childInfo.isLastChild = false
		target = f.explainRecursively(plan.indexPlan, childInfo, target)
		childInfo.driverSide = ProbeSide
		childInfo.isLastChild = true
		target = f.explainRecursively(plan.tablePlan, childInfo, target)
	case *PhysicalIndexMergeReader:
		childInfo.isRoot = false
		childInfo.reqType = Cop
		childInfo.storeType = kv.TiKV
		for _, pchild := range plan.partialPlans {
			childInfo.driverSide = BuildSide
			childInfo.isLastChild = false
			target = f.explainRecursively(pchild, childInfo, target)
		}
		childInfo.driverSide = ProbeSide
		childInfo.isLastChild = true
		target = f.explainRecursively(plan.tablePlan, childInfo, target)
	case *PhysicalShuffleReceiverStub:
		childInfo.isRoot = true
		childInfo.driverSide = Empty
		childInfo.isLastChild = true
		target = f.explainRecursively(plan.DataSource, childInfo, target)
	case *PhysicalCTE:
		f.ctesToExplain = append(f.ctesToExplain, plan)
	case *Insert:
		if plan.SelectPlan != nil {
			childInfo.isRoot = true
			childInfo.driverSide = Empty
			childInfo.isLastChild = true
			target = f.explainRecursively(plan.SelectPlan, childInfo, target)
		}
	case *Update:
		if plan.SelectPlan != nil {
			childInfo.isRoot = true
			childInfo.driverSide = Empty
			childInfo.isLastChild = true
			target = f.explainRecursively(plan.SelectPlan, childInfo, target)
		}
	case *Delete:
		if plan.SelectPlan != nil {
			childInfo.isRoot = true
			childInfo.driverSide = Empty
			childInfo.isLastChild = true
			target = f.explainRecursively(plan.SelectPlan, childInfo, target)
		}
	case *Execute:
		if plan.Plan != nil {
			childInfo.isRoot = true
			childInfo.indent = info.indent
			childInfo.driverSide = Empty
			childInfo.isLastChild = true
			target = f.explainRecursively(plan.Plan, childInfo, target)
		}
	}
	return target
}

func (f *ExplainedPhysicalPlan) explainCTERecursively(cteDef *CTEDefinition, info *operatorCtx, target ExplainedTree) ExplainedTree {
	explained := f.explainSingle(cteDef, info)
	if explained != nil {
		target = append(target, explained)
	}
	childInfo := &operatorCtx{
		depth:       info.depth + 1,
		driverSide:  SeedPart,
		isRoot:      true,
		storeType:   kv.TiDB,
		indent:      texttree.Indent4Child(info.indent, info.isLastChild),
		isLastChild: cteDef.RecurPlan == nil,
	}
	target = f.explainRecursively(cteDef.SeedPlan, childInfo, target)
	if cteDef.RecurPlan != nil {
		childInfo.driverSide = RecursivePart
		childInfo.isLastChild = true
		target = f.explainRecursively(cteDef.RecurPlan, childInfo, target)
	}
	return target
}
