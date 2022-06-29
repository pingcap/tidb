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
	"github.com/pingcap/tidb/util/texttree"
)

// FlatPhysicalPlan provides an easier structure to traverse a plan and collect needed information.
// Note: Although it's named FlatPhysicalPlan, there also could be Insert, Delete and Update at the beginning of Main.
type FlatPhysicalPlan struct {
	Main FlatPlanTree
	CTEs []FlatPlanTree

	// InExecute and InExplain are expected to handle some special cases. Usually you don't need to use them.

	// InExecute means if the original plan tree contains Execute operator.
	//
	// Be careful when trying to use this, InExecute is true doesn't mean we are handling an EXECUTE statement.
	// When collecting information from the plan in an EXECUTE statement, usually we directly use the plan
	// in Execute.Plan, not Execute itself, so InExecute will be false.
	//
	// When will InExecute be true? When you're using "EXPLAIN FOR CONNECTION" to get the last plan of
	// a connection (usually we will record Explain.TargetPlan for an EXPLAIN statement) and that plan
	// is from an EXECUTE statement, we will collect from Execute itself, not directly from Execute.Plan,
	// then InExecute will be true.
	InExecute bool

	// InExplain means if the original plan tree contains Explain operator.
	InExplain bool

	// The fields below are only used when building the FlatPhysicalPlan.
	buildSideFirst bool
	ctesToFlatten  []*PhysicalCTE
}

// FlatPlanTree is a simplified plan tree.
// It arranges all operators in the tree as a slice, ordered by the order of traversing the tree, which means a
// depth-first traversal plus some special rule for some operators.
type FlatPlanTree []*FlatOperator

// GetSelectPlan skips Insert, Delete and Update at the beginning of the FlatPlanTree.
// Note:
//     It returns a reference to the original FlatPlanTree, please avoid modifying the returned value.
//     Since you get a part of the original slice, you need to adjust the FlatOperator.Depth and FlatOperator.ChildrenIdx when using them.
func (e FlatPlanTree) GetSelectPlan() FlatPlanTree {
	if len(e) == 0 {
		return nil
	}
	for i, op := range e {
		switch op.Origin.(type) {
		case *Insert, *Delete, *Update:
		default:
			return e[i:]
		}
	}
	return nil
}

// FlatOperator is a simplified operator.
// It contains a reference to the original operator and some usually needed information.
type FlatOperator struct {
	// A reference to the original operator.
	Origin Plan

	ChildrenIdx []int

	// NeedReverseDriverSide means if we need to reverse the order of children to keep build side before probe side.
	//
	// Specifically, it means if the below are all true:
	// 1. this operator has two children
	// 2. the first child's DriverSide is the probe side and the second's is the build side.
	//
	// If you call FlattenPhysicalPlan with buildSideFirst true, NeedReverseDriverSide will be useless.
	NeedReverseDriverSide bool

	Depth      uint32
	DriverSide DriverSide
	IsRoot     bool
	StoreType  kv.StoreType
	// ReqType is only meaningful when IsRoot is false.
	ReqType ReadReqType

	// The below two fields are mainly for text tree formatting. See texttree.PrettyIdentifier().
	TextTreeIndent string
	IsLastChild    bool

	IsPhysicalPlan bool
}

// DriverSide indicates the operator's location from its parent.
// It's useful for index join, apply, index lookup, cte and so on.
type DriverSide uint8

const (
	// Empty means DriverSide is meaningless for this operator.
	Empty DriverSide = iota
	// BuildSide means this operator is at the build side of its parent
	BuildSide
	// ProbeSide means this operator is at the probe side of its parent
	ProbeSide
	// SeedPart means this operator is the seed part of its parent (a cte)
	SeedPart
	// RecursivePart means this operator is the recursive part of its parent (a cte)
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
	depth       uint32
	driverSide  DriverSide
	isRoot      bool
	storeType   kv.StoreType
	reqType     ReadReqType
	indent      string
	isLastChild bool
}

// FlattenPhysicalPlan generates a FlatPhysicalPlan from a PhysicalPlan, Insert, Delete, Update, Explain or Execute.
func FlattenPhysicalPlan(p Plan, buildSideFirst bool) *FlatPhysicalPlan {
	if p == nil {
		return nil
	}
	res := &FlatPhysicalPlan{
		buildSideFirst: buildSideFirst,
	}
	initInfo := &operatorCtx{
		depth:       0,
		driverSide:  Empty,
		isRoot:      true,
		storeType:   kv.TiDB,
		indent:      "",
		isLastChild: true,
	}
	res.Main, _ = res.flattenRecursively(p, initInfo, nil)

	flattenedCTEPlan := make(map[int]struct{}, len(res.ctesToFlatten))

	// Note that ctesToFlatten may be modified during the loop, so we manually loop over it instead of using for...range.
	for i := 0; i < len(res.ctesToFlatten); i++ {
		cte := res.ctesToFlatten[i]
		cteDef := (*CTEDefinition)(cte)
		if _, ok := flattenedCTEPlan[cteDef.CTE.IDForStorage]; ok {
			continue
		}
		cteExplained := res.flattenCTERecursively(cteDef, initInfo, nil)
		res.CTEs = append(res.CTEs, cteExplained)
		flattenedCTEPlan[cteDef.CTE.IDForStorage] = struct{}{}
	}
	return res
}

func (f *FlatPhysicalPlan) flattenSingle(p Plan, info *operatorCtx) *FlatOperator {
	// Some operators are not inited and given an ExplainID. So their explain IDs are "_0"
	// (when in EXPLAIN FORMAT = 'brief' it will be ""), we skip such operators.
	// Examples: Explain, Execute
	if len(p.TP()) == 0 && p.ID() == 0 {
		return nil
	}
	res := &FlatOperator{
		Origin:         p,
		DriverSide:     info.driverSide,
		IsRoot:         info.isRoot,
		StoreType:      info.storeType,
		Depth:          info.depth,
		ReqType:        info.reqType,
		TextTreeIndent: info.indent,
		IsPhysicalPlan: info.isLastChild,
	}

	if _, ok := p.(PhysicalPlan); ok {
		res.IsPhysicalPlan = true
	}
	return res
}

// Note that info should not be modified in this method.
func (f *FlatPhysicalPlan) flattenRecursively(p Plan, info *operatorCtx, target FlatPlanTree) (res FlatPlanTree, idx int) {
	idx = -1
	flat := f.flattenSingle(p, info)
	if flat != nil {
		target = append(target, flat)
		idx = len(target) - 1
	}
	childIdxs := make([]int, 0)
	var childIdx int
	childCtx := &operatorCtx{
		depth:  info.depth + 1,
		indent: texttree.Indent4Child(info.indent, info.isLastChild),
	}
	// For physical operators, we just enumerate their children and collect their information.
	// Note that some physical operators are special, and they are handled below this part.
	if physPlan, ok := p.(PhysicalPlan); ok {
		driverSideInfo := make([]DriverSide, len(physPlan.Children()))

		switch plan := physPlan.(type) {
		case *PhysicalApply:
			driverSideInfo[plan.InnerChildIdx] = ProbeSide
			driverSideInfo[1-plan.InnerChildIdx] = BuildSide
		case *PhysicalHashJoin:
			if plan.UseOuterToBuild {
				driverSideInfo[plan.InnerChildIdx] = ProbeSide
				driverSideInfo[1-plan.InnerChildIdx] = BuildSide
			} else {
				driverSideInfo[plan.InnerChildIdx] = BuildSide
				driverSideInfo[1-plan.InnerChildIdx] = ProbeSide
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
			driverSideInfo[plan.InnerChildIdx] = ProbeSide
			driverSideInfo[1-plan.InnerChildIdx] = BuildSide
		case *PhysicalIndexMergeJoin:
			driverSideInfo[plan.InnerChildIdx] = ProbeSide
			driverSideInfo[1-plan.InnerChildIdx] = BuildSide
		case *PhysicalIndexHashJoin:
			driverSideInfo[plan.InnerChildIdx] = ProbeSide
			driverSideInfo[1-plan.InnerChildIdx] = BuildSide
		}

		children := make([]PhysicalPlan, len(physPlan.Children()))
		copy(children, physPlan.Children())
		if len(driverSideInfo) == 2 &&
			driverSideInfo[0] == ProbeSide &&
			driverSideInfo[1] == BuildSide {
			if f.buildSideFirst {
				// Put the build side before the probe side if buildSideFirst is true.
				driverSideInfo[0], driverSideInfo[1] = driverSideInfo[1], driverSideInfo[0]
				children[0], children[1] = children[1], children[0]
			} else if flat != nil {
				// Set NeedReverseDriverSide to true if buildSideFirst is false.
				flat.NeedReverseDriverSide = true
			}
		}

		for i := range children {
			childCtx.isRoot = info.isRoot
			childCtx.storeType = info.storeType
			childCtx.reqType = info.reqType
			childCtx.driverSide = driverSideInfo[i]
			childCtx.isLastChild = i == len(children)-1
			target, childIdx = f.flattenRecursively(children[i], childCtx, target)
			childIdxs = append(childIdxs, childIdx)
		}
	}

	// For part of physical operators and some special operators, we need some special logic to get their "children".
	// For PhysicalCTE, we need to add the plan tree into flatTree.ctesToFlatten.
	switch plan := p.(type) {
	case *PhysicalTableReader:
		childCtx.isRoot = false
		childCtx.storeType = plan.StoreType
		childCtx.reqType = plan.ReadReqType
		childCtx.driverSide = Empty
		childCtx.isLastChild = true
		target, childIdx = f.flattenRecursively(plan.tablePlan, childCtx, target)
		childIdxs = append(childIdxs, childIdx)
	case *PhysicalIndexReader:
		childCtx.isRoot = false
		childCtx.reqType = Cop
		childCtx.storeType = kv.TiKV
		childCtx.driverSide = Empty
		childCtx.isLastChild = true
		target, childIdx = f.flattenRecursively(plan.indexPlan, childCtx, target)
		childIdxs = append(childIdxs, childIdx)
	case *PhysicalIndexLookUpReader:
		childCtx.isRoot = false
		childCtx.reqType = Cop
		childCtx.storeType = kv.TiKV
		childCtx.driverSide = BuildSide
		childCtx.isLastChild = false
		target, childIdx = f.flattenRecursively(plan.indexPlan, childCtx, target)
		childIdxs = append(childIdxs, childIdx)
		childCtx.driverSide = ProbeSide
		childCtx.isLastChild = true
		target, childIdx = f.flattenRecursively(plan.tablePlan, childCtx, target)
		childIdxs = append(childIdxs, childIdx)
	case *PhysicalIndexMergeReader:
		childCtx.isRoot = false
		childCtx.reqType = Cop
		childCtx.storeType = kv.TiKV
		for _, pchild := range plan.partialPlans {
			childCtx.driverSide = BuildSide
			childCtx.isLastChild = false
			target, childIdx = f.flattenRecursively(pchild, childCtx, target)
			childIdxs = append(childIdxs, childIdx)
		}
		childCtx.driverSide = ProbeSide
		childCtx.isLastChild = true
		target, childIdx = f.flattenRecursively(plan.tablePlan, childCtx, target)
		childIdxs = append(childIdxs, childIdx)
	case *PhysicalShuffleReceiverStub:
		childCtx.isRoot = true
		childCtx.driverSide = Empty
		childCtx.isLastChild = true
		target, childIdx = f.flattenRecursively(plan.DataSource, childCtx, target)
		childIdxs = append(childIdxs, childIdx)
	case *PhysicalCTE:
		f.ctesToFlatten = append(f.ctesToFlatten, plan)
	case *Insert:
		if plan.SelectPlan != nil {
			childCtx.isRoot = true
			childCtx.driverSide = Empty
			childCtx.isLastChild = true
			target, childIdx = f.flattenRecursively(plan.SelectPlan, childCtx, target)
			childIdxs = append(childIdxs, childIdx)
		}
	case *Update:
		if plan.SelectPlan != nil {
			childCtx.isRoot = true
			childCtx.driverSide = Empty
			childCtx.isLastChild = true
			target, childIdx = f.flattenRecursively(plan.SelectPlan, childCtx, target)
			childIdxs = append(childIdxs, childIdx)
		}
	case *Delete:
		if plan.SelectPlan != nil {
			childCtx.isRoot = true
			childCtx.driverSide = Empty
			childCtx.isLastChild = true
			target, childIdx = f.flattenRecursively(plan.SelectPlan, childCtx, target)
			childIdxs = append(childIdxs, childIdx)
		}
	case *Execute:
		f.InExecute = true
		if plan.Plan != nil {
			childCtx.isRoot = true
			childCtx.indent = info.indent
			childCtx.driverSide = Empty
			childCtx.isLastChild = true
			target, childIdx = f.flattenRecursively(plan.Plan, childCtx, target)
			childIdxs = append(childIdxs, childIdx)
		}
	case *Explain:
		f.InExplain = true
		// Explain is ignored in flattenSingle(). We start to explain its TargetPlan from a new operatorCtx.
		if plan.TargetPlan != nil {
			initInfo := &operatorCtx{
				depth:       0,
				driverSide:  Empty,
				isRoot:      true,
				storeType:   kv.TiDB,
				indent:      "",
				isLastChild: true,
			}
			target, childIdx = f.flattenRecursively(plan.TargetPlan, initInfo, target)
			childIdxs = append(childIdxs, childIdx)
		}
	}
	if flat != nil {
		flat.ChildrenIdx = childIdxs
	}
	return target, idx
}

func (f *FlatPhysicalPlan) flattenCTERecursively(cteDef *CTEDefinition, info *operatorCtx, target FlatPlanTree) FlatPlanTree {
	flat := f.flattenSingle(cteDef, info)
	if flat != nil {
		target = append(target, flat)
	}
	childIdxs := make([]int, 0)
	var childIdx int
	childInfo := &operatorCtx{
		depth:       info.depth + 1,
		driverSide:  SeedPart,
		isRoot:      true,
		storeType:   kv.TiDB,
		indent:      texttree.Indent4Child(info.indent, info.isLastChild),
		isLastChild: cteDef.RecurPlan == nil,
	}
	target, childIdx = f.flattenRecursively(cteDef.SeedPlan, childInfo, target)
	childIdxs = append(childIdxs, childIdx)
	if cteDef.RecurPlan != nil {
		childInfo.driverSide = RecursivePart
		childInfo.isLastChild = true
		target, childIdx = f.flattenRecursively(cteDef.RecurPlan, childInfo, target)
		childIdxs = append(childIdxs, childIdx)
	}
	if flat != nil {
		flat.ChildrenIdx = childIdxs
	}
	return target
}
