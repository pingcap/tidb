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
	"slices"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/texttree"
	"go.uber.org/zap"
)

// FlatPhysicalPlan provides an easier structure to traverse a plan and collect needed information.
// Note: Although it's named FlatPhysicalPlan, there also could be Insert, Delete and Update at the beginning of Main.
type FlatPhysicalPlan struct {
	Main             FlatPlanTree
	CTEs             []FlatPlanTree
	ScalarSubQueries []FlatPlanTree

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

// GetSelectPlan skips Insert, Delete, and Update at the beginning of the FlatPlanTree and the foreign key check/cascade plan at the end of the FlatPlanTree.
// Note:
//
//	It returns a reference to the original FlatPlanTree, please avoid modifying the returned value.
//	The second return value is the offset. Because the returned FlatPlanTree is a part of the original slice, you need to minus them by the offset when using the returned FlatOperator.Depth and FlatOperator.ChildrenIdx.
func (e FlatPlanTree) GetSelectPlan() (FlatPlanTree, int) {
	if len(e) == 0 {
		return nil, 0
	}
	hasDML := false
	for i, op := range e {
		switch op.Origin.(type) {
		case *Insert, *Delete, *Update:
			hasDML = true
		default:
			if hasDML {
				for j := i; j < len(e); j++ {
					switch e[j].Origin.(type) {
					case *FKCheck, *FKCascade:
						// The later plans are belong to foreign key check/cascade plans, doesn't belong to select plan, just skip it.
						return e[i:j], i
					}
				}
			}
			return e[i:], i
		}
	}
	return nil, 0
}

// FlatOperator is a simplified operator.
// It contains a reference to the original operator and some usually needed information.
type FlatOperator struct {
	// A reference to the original operator.
	Origin base.Plan

	// With ChildrenIdx and ChildrenEndIdx, we can locate every children subtrees of this operator in the FlatPlanTree.
	// For example, the first children subtree is flatTree[ChildrenIdx[0] : ChildrenIdx[1]], the last children subtree
	// is flatTree[ChildrenIdx[n-1] : ChildrenEndIdx+1].

	// ChildrenIdx is the indexes of the children of this operator in the FlatPlanTree.
	// It's ordered from small to large.
	ChildrenIdx []int
	// ChildrenEndIdx is the index of the last operator of children subtrees of this operator in the FlatPlanTree.
	ChildrenEndIdx int

	// NeedReverseDriverSide means if we need to reverse the order of children to keep build side before probe side.
	//
	// Specifically, it means if the below are all true:
	// 1. this operator has two children
	// 2. the first child's Label is the probe side and the second's is the build side.
	//
	// If you call FlattenPhysicalPlan with buildSideFirst true, NeedReverseDriverSide will be useless.
	NeedReverseDriverSide bool

	Depth     uint32
	Label     OperatorLabel
	IsRoot    bool
	StoreType kv.StoreType
	// ReqType is only meaningful when IsRoot is false.
	ReqType ReadReqType

	// The below two fields are mainly for text tree formatting. See texttree.PrettyIdentifier().
	TextTreeIndent string
	IsLastChild    bool

	IsPhysicalPlan bool
}

// OperatorLabel acts as some additional information to the name, usually it means its relationship with its parent.
// It's useful for index join, apply, index lookup, cte and so on.
type OperatorLabel uint8

const (
	// Empty means OperatorLabel is meaningless for this operator.
	Empty OperatorLabel = iota
	// BuildSide means this operator is at the build side of its parent
	BuildSide
	// ProbeSide means this operator is at the probe side of its parent
	ProbeSide
	// SeedPart means this operator is the seed part of its parent (a cte)
	SeedPart
	// RecursivePart means this operator is the recursive part of its parent (a cte)
	RecursivePart
)

func (d OperatorLabel) String() string {
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
	label       OperatorLabel
	isRoot      bool
	storeType   kv.StoreType
	reqType     ReadReqType
	indent      string
	isLastChild bool
}

// FlattenPhysicalPlan generates a FlatPhysicalPlan from a PhysicalPlan, Insert, Delete, Update, Explain or Execute.
func FlattenPhysicalPlan(p base.Plan, buildSideFirst bool) *FlatPhysicalPlan {
	if p == nil {
		return nil
	}
	res := &FlatPhysicalPlan{
		buildSideFirst: buildSideFirst,
	}
	initInfo := &operatorCtx{
		depth:       0,
		label:       Empty,
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
	if p.SCtx() == nil || p.SCtx().GetSessionVars() == nil {
		return res
	}
	for _, scalarSubQ := range p.SCtx().GetSessionVars().MapScalarSubQ {
		castedScalarSubQ, ok := scalarSubQ.(*ScalarSubqueryEvalCtx)
		if !ok {
			logutil.BgLogger().Debug("Wrong item regiestered as scalar subquery", zap.String("the wrong item", fmt.Sprintf("%T", scalarSubQ)))
			continue
		}
		subQExplained := res.flattenScalarSubQRecursively(castedScalarSubQ, initInfo, nil)
		res.ScalarSubQueries = append(res.ScalarSubQueries, subQExplained)
	}
	return res
}

func (*FlatPhysicalPlan) flattenSingle(p base.Plan, info *operatorCtx) *FlatOperator {
	// Some operators are not initialized and given an ExplainID. So their explain IDs are "_0"
	// (when in EXPLAIN FORMAT = 'brief' it will be ""), we skip such operators.
	// Examples: Explain, Execute
	if len(p.TP()) == 0 && p.ID() == 0 {
		return nil
	}
	res := &FlatOperator{
		Origin:         p,
		Label:          info.label,
		IsRoot:         info.isRoot,
		StoreType:      info.storeType,
		Depth:          info.depth,
		ReqType:        info.reqType,
		TextTreeIndent: info.indent,
		IsLastChild:    info.isLastChild,
	}

	if _, ok := p.(base.PhysicalPlan); ok {
		res.IsPhysicalPlan = true
	}
	return res
}

// Note that info should not be modified in this method.
func (f *FlatPhysicalPlan) flattenRecursively(p base.Plan, info *operatorCtx, target FlatPlanTree) (res FlatPlanTree, idx int) {
	idx = -1
	flat := f.flattenSingle(p, info)
	if flat != nil {
		target = append(target, flat)
		idx = len(target) - 1
	}
	childIdxs := make([]int, 0)
	var childIdx int
	childCtx := &operatorCtx{
		depth:     info.depth + 1,
		isRoot:    info.isRoot,
		storeType: info.storeType,
		reqType:   info.reqType,
		indent:    texttree.Indent4Child(info.indent, info.isLastChild),
	}
	// For physical operators, we just enumerate their children and collect their information.
	// Note that some physical operators are special, and they are handled below this part.
	if physPlan, ok := p.(base.PhysicalPlan); ok {
		label := make([]OperatorLabel, len(physPlan.Children()))

		switch plan := physPlan.(type) {
		case *PhysicalApply:
			label[plan.InnerChildIdx] = ProbeSide
			label[1-plan.InnerChildIdx] = BuildSide
		case *PhysicalHashJoin:
			if plan.UseOuterToBuild {
				label[plan.InnerChildIdx] = ProbeSide
				label[1-plan.InnerChildIdx] = BuildSide
			} else {
				label[plan.InnerChildIdx] = BuildSide
				label[1-plan.InnerChildIdx] = ProbeSide
			}
		case *PhysicalMergeJoin:
			if plan.JoinType == RightOuterJoin {
				label[0] = BuildSide
				label[1] = ProbeSide
			} else {
				label[0] = ProbeSide
				label[1] = BuildSide
			}
		case *PhysicalIndexJoin:
			label[plan.InnerChildIdx] = ProbeSide
			label[1-plan.InnerChildIdx] = BuildSide
		case *PhysicalIndexMergeJoin:
			label[plan.InnerChildIdx] = ProbeSide
			label[1-plan.InnerChildIdx] = BuildSide
		case *PhysicalIndexHashJoin:
			label[plan.InnerChildIdx] = ProbeSide
			label[1-plan.InnerChildIdx] = BuildSide
		}

		children := make([]base.PhysicalPlan, len(physPlan.Children()))
		copy(children, physPlan.Children())
		if len(label) == 2 &&
			label[0] == ProbeSide &&
			label[1] == BuildSide {
			if f.buildSideFirst {
				// Put the build side before the probe side if buildSideFirst is true.
				label[0], label[1] = label[1], label[0]
				children[0], children[1] = children[1], children[0]
			} else if flat != nil {
				// Set NeedReverseDriverSide to true if buildSideFirst is false.
				flat.NeedReverseDriverSide = true
			}
		}

		for i := range children {
			childCtx.label = label[i]
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
		childCtx.label = Empty
		childCtx.isLastChild = true
		target, childIdx = f.flattenRecursively(plan.tablePlan, childCtx, target)
		childIdxs = append(childIdxs, childIdx)
	case *PhysicalIndexReader:
		childCtx.isRoot = false
		childCtx.reqType = Cop
		childCtx.storeType = kv.TiKV
		childCtx.label = Empty
		childCtx.isLastChild = true
		target, childIdx = f.flattenRecursively(plan.indexPlan, childCtx, target)
		childIdxs = append(childIdxs, childIdx)
	case *PhysicalIndexLookUpReader:
		childCtx.isRoot = false
		childCtx.reqType = Cop
		childCtx.storeType = kv.TiKV
		childCtx.label = BuildSide
		childCtx.isLastChild = false
		target, childIdx = f.flattenRecursively(plan.indexPlan, childCtx, target)
		childIdxs = append(childIdxs, childIdx)
		childCtx.label = ProbeSide
		childCtx.isLastChild = true
		target, childIdx = f.flattenRecursively(plan.tablePlan, childCtx, target)
		childIdxs = append(childIdxs, childIdx)
	case *PhysicalIndexMergeReader:
		childCtx.isRoot = false
		childCtx.reqType = Cop
		childCtx.storeType = kv.TiKV
		for _, pchild := range plan.partialPlans {
			childCtx.label = BuildSide
			childCtx.isLastChild = false
			target, childIdx = f.flattenRecursively(pchild, childCtx, target)
			childIdxs = append(childIdxs, childIdx)
		}
		childCtx.label = ProbeSide
		childCtx.isLastChild = true
		target, childIdx = f.flattenRecursively(plan.tablePlan, childCtx, target)
		childIdxs = append(childIdxs, childIdx)
	case *PhysicalShuffleReceiverStub:
		childCtx.isRoot = true
		childCtx.label = Empty
		childCtx.isLastChild = true
		target, childIdx = f.flattenRecursively(plan.DataSource, childCtx, target)
		childIdxs = append(childIdxs, childIdx)
	case *PhysicalCTE:
		// We shallow copy the PhysicalCTE here because we don't want the probeParents (see comments in PhysicalPlan
		// for details) to affect the row count display of the independent CTE plan tree.
		copiedCTE := *plan
		copiedCTE.probeParents = nil
		if info.isRoot {
			// If it's executed in TiDB, we need to record it since we don't have producer and consumer
			f.ctesToFlatten = append(f.ctesToFlatten, &copiedCTE)
		}
	case *Insert:
		if plan.SelectPlan != nil {
			childCtx.isRoot = true
			childCtx.label = Empty
			childCtx.isLastChild = len(plan.FKChecks) == 0 && len(plan.FKCascades) == 0
			target, childIdx = f.flattenRecursively(plan.SelectPlan, childCtx, target)
			childIdxs = append(childIdxs, childIdx)
		}
		target, childIdxs = f.flattenForeignKeyChecksAndCascades(childCtx, target, childIdxs, plan.FKChecks, plan.FKCascades, true)
	case *ImportInto:
		if plan.SelectPlan != nil {
			childCtx.isRoot = true
			childCtx.label = Empty
			childCtx.isLastChild = true
			target, childIdx = f.flattenRecursively(plan.SelectPlan, childCtx, target)
			childIdxs = append(childIdxs, childIdx)
		}
	case *Update:
		if plan.SelectPlan != nil {
			childCtx.isRoot = true
			childCtx.label = Empty
			childCtx.isLastChild = len(plan.FKChecks) == 0 && len(plan.FKCascades) == 0
			target, childIdx = f.flattenRecursively(plan.SelectPlan, childCtx, target)
			childIdxs = append(childIdxs, childIdx)
		}
		target, childIdxs = f.flattenForeignKeyChecksAndCascadesMap(childCtx, target, childIdxs, plan.FKChecks, plan.FKCascades)
	case *Delete:
		if plan.SelectPlan != nil {
			childCtx.isRoot = true
			childCtx.label = Empty
			childCtx.isLastChild = len(plan.FKChecks) == 0 && len(plan.FKCascades) == 0
			target, childIdx = f.flattenRecursively(plan.SelectPlan, childCtx, target)
			childIdxs = append(childIdxs, childIdx)
		}
		target, childIdxs = f.flattenForeignKeyChecksAndCascadesMap(childCtx, target, childIdxs, plan.FKChecks, plan.FKCascades)
	case *Execute:
		f.InExecute = true
		if plan.Plan != nil {
			childCtx.isRoot = true
			childCtx.indent = info.indent
			childCtx.label = Empty
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
				label:       Empty,
				isRoot:      true,
				storeType:   kv.TiDB,
				indent:      "",
				isLastChild: true,
			}
			target, childIdx = f.flattenRecursively(plan.TargetPlan, initInfo, target)
			childIdxs = append(childIdxs, childIdx)
		}
	case *FKCascade:
		for i, child := range plan.CascadePlans {
			childCtx.label = Empty
			childCtx.isLastChild = i == len(plan.CascadePlans)-1
			target, childIdx = f.flattenRecursively(child, childCtx, target)
			childIdxs = append(childIdxs, childIdx)
		}
	}
	if flat != nil {
		flat.ChildrenIdx = childIdxs
		flat.ChildrenEndIdx = len(target) - 1
	}
	return target, idx
}

func (f *FlatPhysicalPlan) flattenForeignKeyChecksAndCascadesMap(childCtx *operatorCtx, target FlatPlanTree, childIdxs []int, fkChecksMap map[int64][]*FKCheck, fkCascadesMap map[int64][]*FKCascade) (FlatPlanTree, []int) {
	tids := make([]int64, 0, len(fkChecksMap))
	for tid := range fkChecksMap {
		tids = append(tids, tid)
	}
	// sort by table id for explain result stable.
	slices.Sort(tids)
	for i, tid := range tids {
		target, childIdxs = f.flattenForeignKeyChecksAndCascades(childCtx, target, childIdxs, fkChecksMap[tid], nil, len(fkCascadesMap) == 0 && i == len(tids)-1)
	}
	tids = tids[:0]
	for tid := range fkCascadesMap {
		tids = append(tids, tid)
	}
	slices.Sort(tids)
	for i, tid := range tids {
		target, childIdxs = f.flattenForeignKeyChecksAndCascades(childCtx, target, childIdxs, nil, fkCascadesMap[tid], i == len(tids)-1)
	}
	return target, childIdxs
}

func (f *FlatPhysicalPlan) flattenForeignKeyChecksAndCascades(childCtx *operatorCtx, target FlatPlanTree, childIdxs []int, fkChecks []*FKCheck, fkCascades []*FKCascade, isLast bool) (FlatPlanTree, []int) {
	var childIdx int
	for i, fkCheck := range fkChecks {
		childCtx.isRoot = true
		childCtx.label = Empty
		childCtx.isLastChild = isLast && len(fkCascades) == 0 && i == len(fkChecks)-1
		target, childIdx = f.flattenRecursively(fkCheck, childCtx, target)
		childIdxs = append(childIdxs, childIdx)
	}
	for i, fkCascade := range fkCascades {
		childCtx.isRoot = true
		childCtx.label = Empty
		childCtx.isLastChild = isLast && i == len(fkCascades)-1
		target, childIdx = f.flattenRecursively(fkCascade, childCtx, target)
		childIdxs = append(childIdxs, childIdx)
	}
	return target, childIdxs
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
		label:       SeedPart,
		isRoot:      true,
		storeType:   kv.TiDB,
		indent:      texttree.Indent4Child(info.indent, info.isLastChild),
		isLastChild: cteDef.RecurPlan == nil,
	}
	target, childIdx = f.flattenRecursively(cteDef.SeedPlan, childInfo, target)
	childIdxs = append(childIdxs, childIdx)
	if cteDef.RecurPlan != nil {
		childInfo.label = RecursivePart
		childInfo.isLastChild = true
		target, childIdx = f.flattenRecursively(cteDef.RecurPlan, childInfo, target)
		childIdxs = append(childIdxs, childIdx)
	}
	if flat != nil {
		flat.ChildrenIdx = childIdxs
	}
	return target
}

func (f *FlatPhysicalPlan) flattenScalarSubQRecursively(scalarSubQ *ScalarSubqueryEvalCtx, info *operatorCtx, target FlatPlanTree) FlatPlanTree {
	flat := f.flattenSingle(scalarSubQ, info)
	if flat != nil {
		target = append(target, flat)
	}
	childIdxs := make([]int, 0)
	var childIdx int
	childInfo := &operatorCtx{
		depth:       info.depth + 1,
		label:       Empty,
		isRoot:      true,
		storeType:   kv.TiDB,
		indent:      texttree.Indent4Child(info.indent, info.isLastChild),
		isLastChild: true,
	}
	target, childIdx = f.flattenRecursively(scalarSubQ.scalarSubQuery, childInfo, target)
	childIdxs = append(childIdxs, childIdx)
	if flat != nil {
		flat.ChildrenIdx = childIdxs
	}
	return target
}
