// Copyright 2015 PingCAP, Inc.
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
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/baseimpl"
	fd "github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/tracing"
)

// AsSctx converts PlanContext to sessionctx.Context.
func AsSctx(pctx base.PlanContext) (sessionctx.Context, error) {
	sctx, ok := pctx.(sessionctx.Context)
	if !ok {
		return nil, errors.New("the current PlanContext cannot be converted to sessionctx.Context")
	}
	return sctx, nil
}

func enforceProperty(p *property.PhysicalProperty, tsk base.Task, ctx base.PlanContext) base.Task {
	if p.TaskTp == property.MppTaskType {
		mpp, ok := tsk.(*MppTask)
		if !ok || mpp.Invalid() {
			return base.InvalidTask
		}
		if !p.IsSortItemAllForPartition() {
			ctx.GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because operator `Sort` is not supported now.")
			return base.InvalidTask
		}
		tsk = mpp.enforceExchanger(p)
	}
	// when task is double cop task warping a index merge reader, tsk.plan() may be nil when indexPlanFinished is marked
	// as false, while the real plan is in idxMergePartPlans. tsk.plan()==nil is not right here.
	if p.IsSortItemEmpty() || tsk.Invalid() {
		return tsk
	}
	if p.TaskTp != property.MppTaskType {
		tsk = tsk.ConvertToRootTask(ctx)
	}
	sortReqProp := &property.PhysicalProperty{TaskTp: property.RootTaskType, SortItems: p.SortItems, ExpectedCnt: math.MaxFloat64}
	sort := PhysicalSort{
		ByItems:       make([]*util.ByItems, 0, len(p.SortItems)),
		IsPartialSort: p.IsSortItemAllForPartition(),
	}.Init(ctx, tsk.Plan().StatsInfo(), tsk.Plan().QueryBlockOffset(), sortReqProp)
	for _, col := range p.SortItems {
		sort.ByItems = append(sort.ByItems, &util.ByItems{Expr: col.Col, Desc: col.Desc})
	}
	return sort.Attach2Task(tsk)
}

// optimizeByShuffle insert `PhysicalShuffle` to optimize performance by running in a parallel manner.
func optimizeByShuffle(tsk base.Task, ctx base.PlanContext) base.Task {
	if tsk.Plan() == nil {
		return tsk
	}

	switch p := tsk.Plan().(type) {
	case *PhysicalWindow:
		if shuffle := optimizeByShuffle4Window(p, ctx); shuffle != nil {
			return shuffle.Attach2Task(tsk)
		}
	case *PhysicalMergeJoin:
		if shuffle := optimizeByShuffle4MergeJoin(p, ctx); shuffle != nil {
			return shuffle.Attach2Task(tsk)
		}
	case *PhysicalStreamAgg:
		if shuffle := optimizeByShuffle4StreamAgg(p, ctx); shuffle != nil {
			return shuffle.Attach2Task(tsk)
		}
	}
	return tsk
}

func optimizeByShuffle4Window(pp *PhysicalWindow, ctx base.PlanContext) *PhysicalShuffle {
	concurrency := ctx.GetSessionVars().WindowConcurrency()
	if concurrency <= 1 {
		return nil
	}

	sort, ok := pp.Children()[0].(*PhysicalSort)
	if !ok {
		// Multi-thread executing on SORTED data source is not effective enough by current implementation.
		// TODO: Implement a better one.
		return nil
	}
	tail, dataSource := sort, sort.Children()[0]

	partitionBy := make([]*expression.Column, 0, len(pp.PartitionBy))
	for _, item := range pp.PartitionBy {
		partitionBy = append(partitionBy, item.Col)
	}
	ndv, _ := cardinality.EstimateColsNDVWithMatchedLen(partitionBy, dataSource.Schema(), dataSource.StatsInfo())
	if ndv <= 1 {
		return nil
	}
	concurrency = min(concurrency, int(ndv))

	byItems := make([]expression.Expression, 0, len(pp.PartitionBy))
	for _, item := range pp.PartitionBy {
		byItems = append(byItems, item.Col)
	}
	reqProp := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	shuffle := PhysicalShuffle{
		Concurrency:  concurrency,
		Tails:        []base.PhysicalPlan{tail},
		DataSources:  []base.PhysicalPlan{dataSource},
		SplitterType: PartitionHashSplitterType,
		ByItemArrays: [][]expression.Expression{byItems},
	}.Init(ctx, pp.StatsInfo(), pp.QueryBlockOffset(), reqProp)
	return shuffle
}

func optimizeByShuffle4StreamAgg(pp *PhysicalStreamAgg, ctx base.PlanContext) *PhysicalShuffle {
	concurrency := ctx.GetSessionVars().StreamAggConcurrency()
	if concurrency <= 1 {
		return nil
	}

	sort, ok := pp.Children()[0].(*PhysicalSort)
	if !ok {
		// Multi-thread executing on SORTED data source is not effective enough by current implementation.
		// TODO: Implement a better one.
		return nil
	}
	tail, dataSource := sort, sort.Children()[0]

	partitionBy := make([]*expression.Column, 0, len(pp.GroupByItems))
	for _, item := range pp.GroupByItems {
		if col, ok := item.(*expression.Column); ok {
			partitionBy = append(partitionBy, col)
		}
	}
	ndv, _ := cardinality.EstimateColsNDVWithMatchedLen(partitionBy, dataSource.Schema(), dataSource.StatsInfo())
	if ndv <= 1 {
		return nil
	}
	concurrency = min(concurrency, int(ndv))

	reqProp := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	shuffle := PhysicalShuffle{
		Concurrency:  concurrency,
		Tails:        []base.PhysicalPlan{tail},
		DataSources:  []base.PhysicalPlan{dataSource},
		SplitterType: PartitionHashSplitterType,
		ByItemArrays: [][]expression.Expression{util.CloneExprs(pp.GroupByItems)},
	}.Init(ctx, pp.StatsInfo(), pp.QueryBlockOffset(), reqProp)
	return shuffle
}

func optimizeByShuffle4MergeJoin(pp *PhysicalMergeJoin, ctx base.PlanContext) *PhysicalShuffle {
	concurrency := ctx.GetSessionVars().MergeJoinConcurrency()
	if concurrency <= 1 {
		return nil
	}

	children := pp.Children()
	dataSources := make([]base.PhysicalPlan, len(children))
	tails := make([]base.PhysicalPlan, len(children))

	for i := range children {
		sort, ok := children[i].(*PhysicalSort)
		if !ok {
			// Multi-thread executing on SORTED data source is not effective enough by current implementation.
			// TODO: Implement a better one.
			return nil
		}
		tails[i], dataSources[i] = sort, sort.Children()[0]
	}

	leftByItemArray := make([]expression.Expression, 0, len(pp.LeftJoinKeys))
	for _, col := range pp.LeftJoinKeys {
		leftByItemArray = append(leftByItemArray, col.Clone())
	}
	rightByItemArray := make([]expression.Expression, 0, len(pp.RightJoinKeys))
	for _, col := range pp.RightJoinKeys {
		rightByItemArray = append(rightByItemArray, col.Clone())
	}
	reqProp := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	shuffle := PhysicalShuffle{
		Concurrency:  concurrency,
		Tails:        tails,
		DataSources:  dataSources,
		SplitterType: PartitionHashSplitterType,
		ByItemArrays: [][]expression.Expression{leftByItemArray, rightByItemArray},
	}.Init(ctx, pp.StatsInfo(), pp.QueryBlockOffset(), reqProp)
	return shuffle
}

type baseLogicalPlan struct {
	baseimpl.Plan

	taskMap map[string]base.Task
	// taskMapBak forms a backlog stack of taskMap, used to roll back the taskMap.
	taskMapBak []string
	// taskMapBakTS stores the timestamps of logs.
	taskMapBakTS []uint64
	self         base.LogicalPlan
	maxOneRow    bool
	children     []base.LogicalPlan
	// fdSet is a set of functional dependencies(FDs) which powers many optimizations,
	// including eliminating unnecessary DISTINCT operators, simplifying ORDER BY columns,
	// removing Max1Row operators, and mapping semi-joins to inner-joins.
	// for now, it's hard to maintain in individual operator, build it from bottom up when using.
	fdSet *fd.FDSet
}

// ExtractFD return the children[0]'s fdSet if there are no adding/removing fd in this logic plan.
func (p *baseLogicalPlan) ExtractFD() *fd.FDSet {
	if p.fdSet != nil {
		return p.fdSet
	}
	fds := &fd.FDSet{HashCodeToUniqueID: make(map[string]int)}
	for _, ch := range p.children {
		fds.AddFrom(ch.ExtractFD())
	}
	return fds
}

func (p *baseLogicalPlan) MaxOneRow() bool {
	return p.maxOneRow
}

// ExplainInfo implements Plan interface.
func (*baseLogicalPlan) ExplainInfo() string {
	return ""
}

func getEstimatedProbeCntFromProbeParents(probeParents []base.PhysicalPlan) float64 {
	res := float64(1)
	for _, pp := range probeParents {
		switch pp.(type) {
		case *PhysicalApply, *PhysicalIndexHashJoin, *PhysicalIndexMergeJoin, *PhysicalIndexJoin:
			if join, ok := pp.(interface{ getInnerChildIdx() int }); ok {
				outer := pp.Children()[1-join.getInnerChildIdx()]
				res *= outer.StatsInfo().RowCount
			}
		}
	}
	return res
}

func getActualProbeCntFromProbeParents(pps []base.PhysicalPlan, statsColl *execdetails.RuntimeStatsColl) int64 {
	res := int64(1)
	for _, pp := range pps {
		switch pp.(type) {
		case *PhysicalApply, *PhysicalIndexHashJoin, *PhysicalIndexMergeJoin, *PhysicalIndexJoin:
			if join, ok := pp.(interface{ getInnerChildIdx() int }); ok {
				outerChildID := pp.Children()[1-join.getInnerChildIdx()].ID()
				actRows := int64(1)
				if statsColl.ExistsRootStats(outerChildID) {
					actRows = statsColl.GetRootStats(outerChildID).GetActRows()
				}
				if statsColl.ExistsCopStats(outerChildID) {
					actRows = statsColl.GetCopStats(outerChildID).GetActRows()
				}
				// TODO: For PhysicalApply, we need to consider cache hit ratio in JoinRuntimeStats and use actRows/(1-ratio) here.
				res *= actRows
			}
		}
	}
	return res
}

type basePhysicalPlan struct {
	baseimpl.Plan

	childrenReqProps []*property.PhysicalProperty
	self             base.PhysicalPlan
	children         []base.PhysicalPlan

	// used by the new cost interface
	planCostInit bool
	planCost     float64
	planCostVer2 costusage.CostVer2

	// probeParents records the IndexJoins and Applys with this operator in their inner children.
	// Please see comments in op.PhysicalPlan for details.
	probeParents []base.PhysicalPlan

	// Only for MPP. If TiFlashFineGrainedShuffleStreamCount > 0:
	// 1. For ExchangeSender, means its output will be partitioned by hash key.
	// 2. For ExchangeReceiver/Window/Sort, means its input is already partitioned.
	TiFlashFineGrainedShuffleStreamCount uint64
}

func (p *basePhysicalPlan) cloneWithSelf(newSelf base.PhysicalPlan) (*basePhysicalPlan, error) {
	base := &basePhysicalPlan{
		Plan:                                 p.Plan,
		self:                                 newSelf,
		TiFlashFineGrainedShuffleStreamCount: p.TiFlashFineGrainedShuffleStreamCount,
		probeParents:                         p.probeParents,
	}
	for _, child := range p.children {
		cloned, err := child.Clone()
		if err != nil {
			return nil, err
		}
		base.children = append(base.children, cloned)
	}
	for _, prop := range p.childrenReqProps {
		if prop == nil {
			continue
		}
		base.childrenReqProps = append(base.childrenReqProps, prop.CloneEssentialFields())
	}
	return base, nil
}

// Clone implements op.PhysicalPlan interface.
func (p *basePhysicalPlan) Clone() (base.PhysicalPlan, error) {
	return nil, errors.Errorf("%T doesn't support cloning", p.self)
}

// ExplainInfo implements Plan interface.
func (*basePhysicalPlan) ExplainInfo() string {
	return ""
}

// ExplainNormalizedInfo implements op.PhysicalPlan interface.
func (*basePhysicalPlan) ExplainNormalizedInfo() string {
	return ""
}

func (p *basePhysicalPlan) GetChildReqProps(idx int) *property.PhysicalProperty {
	return p.childrenReqProps[idx]
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (*basePhysicalPlan) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	return nil
}

// MemoryUsage return the memory usage of baseop.PhysicalPlan
func (p *basePhysicalPlan) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.Plan.MemoryUsage() + size.SizeOfSlice + int64(cap(p.childrenReqProps))*size.SizeOfPointer +
		size.SizeOfSlice + int64(cap(p.children)+1)*size.SizeOfInterface + size.SizeOfFloat64 +
		size.SizeOfUint64 + size.SizeOfBool

	for _, prop := range p.childrenReqProps {
		sum += prop.MemoryUsage()
	}
	for _, plan := range p.children {
		sum += plan.MemoryUsage()
	}
	return
}

func (p *basePhysicalPlan) GetEstRowCountForDisplay() float64 {
	if p == nil {
		return 0
	}
	return p.StatsInfo().RowCount * getEstimatedProbeCntFromProbeParents(p.probeParents)
}

func (p *basePhysicalPlan) GetActualProbeCnt(statsColl *execdetails.RuntimeStatsColl) int64 {
	if p == nil {
		return 1
	}
	return getActualProbeCntFromProbeParents(p.probeParents, statsColl)
}

func (p *basePhysicalPlan) SetProbeParents(probeParents []base.PhysicalPlan) {
	p.probeParents = probeParents
}

// GetLogicalTS4TaskMap get the logical TimeStamp now to help rollback the TaskMap changes after that.
func (p *baseLogicalPlan) GetLogicalTS4TaskMap() uint64 {
	p.SCtx().GetSessionVars().StmtCtx.TaskMapBakTS++
	return p.SCtx().GetSessionVars().StmtCtx.TaskMapBakTS
}

// RollBackTaskMap implements LogicalPlan interface.
func (p *baseLogicalPlan) RollBackTaskMap(ts uint64) {
	if !p.SCtx().GetSessionVars().StmtCtx.StmtHints.TaskMapNeedBackUp() {
		return
	}
	if len(p.taskMapBak) > 0 {
		// Rollback all the logs with TimeStamp TS.
		n := len(p.taskMapBak)
		for i := 0; i < n; i++ {
			cur := p.taskMapBak[i]
			if p.taskMapBakTS[i] < ts {
				continue
			}

			// Remove the i_th log.
			p.taskMapBak = append(p.taskMapBak[:i], p.taskMapBak[i+1:]...)
			p.taskMapBakTS = append(p.taskMapBakTS[:i], p.taskMapBakTS[i+1:]...)
			i--
			n--

			// Roll back taskMap.
			p.taskMap[cur] = nil
		}
	}
	for _, child := range p.children {
		child.RollBackTaskMap(ts)
	}
}

func (p *baseLogicalPlan) getTask(prop *property.PhysicalProperty) base.Task {
	key := prop.HashCode()
	return p.taskMap[string(key)]
}

func (p *baseLogicalPlan) storeTask(prop *property.PhysicalProperty, task base.Task) {
	key := prop.HashCode()
	if p.SCtx().GetSessionVars().StmtCtx.StmtHints.TaskMapNeedBackUp() {
		// Empty string for useless change.
		ts := p.GetLogicalTS4TaskMap()
		p.taskMapBakTS = append(p.taskMapBakTS, ts)
		p.taskMapBak = append(p.taskMapBak, string(key))
	}
	p.taskMap[string(key)] = task
}

// HasMaxOneRow returns if the LogicalPlan will output at most one row.
func HasMaxOneRow(p base.LogicalPlan, childMaxOneRow []bool) bool {
	if len(childMaxOneRow) == 0 {
		// The reason why we use this check is that, this function
		// is used both in planner/core and planner/cascades.
		// In cascades planner, LogicalPlan may have no `children`.
		return false
	}
	switch x := p.(type) {
	case *LogicalLock, *LogicalLimit, *LogicalSort, *LogicalSelection,
		*LogicalApply, *LogicalProjection, *LogicalWindow, *LogicalAggregation:
		return childMaxOneRow[0]
	case *LogicalMaxOneRow:
		return true
	case *LogicalJoin:
		switch x.JoinType {
		case SemiJoin, AntiSemiJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
			return childMaxOneRow[0]
		default:
			return childMaxOneRow[0] && childMaxOneRow[1]
		}
	}
	return false
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *baseLogicalPlan) BuildKeyInfo(_ *expression.Schema, _ []*expression.Schema) {
	childMaxOneRow := make([]bool, len(p.children))
	for i := range p.children {
		childMaxOneRow[i] = p.children[i].MaxOneRow()
	}
	p.maxOneRow = utilfuncp.HasMaxOneRowUtil(p.self, childMaxOneRow)
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *logicalSchemaProducer) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	selfSchema.Keys = nil
	p.baseLogicalPlan.BuildKeyInfo(selfSchema, childSchema)

	// default implementation for plans has only one child: proprgate child keys
	// multi-children plans are likely to have particular implementation.
	if len(childSchema) == 1 {
		for _, key := range childSchema[0].Keys {
			indices := selfSchema.ColumnsIndices(key)
			if indices == nil {
				continue
			}
			newKey := make([]*expression.Column, 0, len(key))
			for _, i := range indices {
				newKey = append(newKey, selfSchema.Columns[i])
			}
			selfSchema.Keys = append(selfSchema.Keys, newKey)
		}
	}
}

func newBaseLogicalPlan(ctx base.PlanContext, tp string, self base.LogicalPlan, qbOffset int) baseLogicalPlan {
	return baseLogicalPlan{
		taskMap:      make(map[string]base.Task),
		taskMapBak:   make([]string, 0, 10),
		taskMapBakTS: make([]uint64, 0, 10),
		Plan:         baseimpl.NewBasePlan(ctx, tp, qbOffset),
		self:         self,
	}
}

func newBasePhysicalPlan(ctx base.PlanContext, tp string, self base.PhysicalPlan, offset int) basePhysicalPlan {
	return basePhysicalPlan{
		Plan: baseimpl.NewBasePlan(ctx, tp, offset),
		self: self,
	}
}

func (*baseLogicalPlan) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (p *baseLogicalPlan) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	if len(p.children) == 0 {
		return p.self, nil
	}
	var err error
	p.children[0], err = p.children[0].PruneColumns(parentUsedCols, opt)
	if err != nil {
		return nil, err
	}
	return p.self, nil
}

// Schema implements Plan Schema interface.
func (p *baseLogicalPlan) Schema() *expression.Schema {
	return p.children[0].Schema()
}

func (p *baseLogicalPlan) OutputNames() types.NameSlice {
	return p.children[0].OutputNames()
}

func (p *baseLogicalPlan) SetOutputNames(names types.NameSlice) {
	p.children[0].SetOutputNames(names)
}

// Schema implements Plan Schema interface.
func (p *basePhysicalPlan) Schema() *expression.Schema {
	return p.children[0].Schema()
}

// Children implements LogicalPlan Children interface.
func (p *baseLogicalPlan) Children() []base.LogicalPlan {
	return p.children
}

// Children implements op.PhysicalPlan Children interface.
func (p *basePhysicalPlan) Children() []base.PhysicalPlan {
	return p.children
}

// SetChildren implements LogicalPlan SetChildren interface.
func (p *baseLogicalPlan) SetChildren(children ...base.LogicalPlan) {
	p.children = children
}

// SetChildren implements op.PhysicalPlan SetChildren interface.
func (p *basePhysicalPlan) SetChildren(children ...base.PhysicalPlan) {
	p.children = children
}

// SetChild implements LogicalPlan SetChild interface.
func (p *baseLogicalPlan) SetChild(i int, child base.LogicalPlan) {
	p.children[i] = child
}

// SetChild implements op.PhysicalPlan SetChild interface.
func (p *basePhysicalPlan) SetChild(i int, child base.PhysicalPlan) {
	p.children[i] = child
}

// BuildPlanTrace implements Plan
func (p *basePhysicalPlan) BuildPlanTrace() *tracing.PlanTrace {
	tp := ""
	info := ""
	if p.self != nil {
		tp = p.self.TP()
		info = p.self.ExplainInfo()
	}

	planTrace := &tracing.PlanTrace{ID: p.ID(), TP: tp, ExplainInfo: info}
	for _, child := range p.Children() {
		planTrace.Children = append(planTrace.Children, child.BuildPlanTrace())
	}
	return planTrace
}

// BuildPlanTrace implements Plan
func (p *baseLogicalPlan) BuildPlanTrace() *tracing.PlanTrace {
	planTrace := &tracing.PlanTrace{ID: p.ID(), TP: p.TP(), ExplainInfo: p.self.ExplainInfo()}
	for _, child := range p.Children() {
		planTrace.Children = append(planTrace.Children, child.BuildPlanTrace())
	}
	return planTrace
}

// AppendChildCandidate implements PhysicalPlan interface.
func (p *basePhysicalPlan) AppendChildCandidate(op *optimizetrace.PhysicalOptimizeOp) {
	if len(p.Children()) < 1 {
		return
	}
	childrenID := make([]int, 0)
	for _, child := range p.Children() {
		childCandidate := &tracing.CandidatePlanTrace{
			PlanTrace: &tracing.PlanTrace{TP: child.TP(), ID: child.ID(),
				ExplainInfo: child.ExplainInfo()},
		}
		op.AppendCandidate(childCandidate)
		child.AppendChildCandidate(op)
		childrenID = append(childrenID, child.ID())
	}
	op.GetTracer().Candidates[p.ID()].PlanTrace.AppendChildrenID(childrenID...)
}
