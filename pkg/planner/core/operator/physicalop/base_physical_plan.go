// Copyright 2024 PingCAP, Inc.
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

package physicalop

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/baseimpl"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
)

//go:generate go run ../../generator/plan_cache/plan_clone_generator.go -- plan_clone_generated.go
var (
	_ base.PhysicalPlan = &PhysicalSelection{}
	_ base.PhysicalPlan = &PhysicalProjection{}
	_ base.PhysicalPlan = &PhysicalTopN{}
	_ base.PhysicalPlan = &PhysicalMaxOneRow{}
	_ base.PhysicalPlan = &PhysicalTableDual{}
	_ base.PhysicalPlan = &PhysicalUnionAll{}
	_ base.PhysicalPlan = &PhysicalSort{}
	_ base.PhysicalPlan = &NominalSort{}
	_ base.PhysicalPlan = &PhysicalLock{}
	_ base.PhysicalPlan = &PhysicalLimit{}
	_ base.PhysicalPlan = &PhysicalIndexScan{}
	_ base.PhysicalPlan = &PhysicalTableScan{}
	_ base.PhysicalPlan = &PhysicalTableReader{}
	_ base.PhysicalPlan = &PhysicalIndexReader{}
	_ base.PhysicalPlan = &PhysicalIndexLookUpReader{}
	_ base.PhysicalPlan = &PhysicalIndexMergeReader{}
	_ base.PhysicalPlan = &PhysicalHashAgg{}
	_ base.PhysicalPlan = &PhysicalStreamAgg{}
	_ base.PhysicalPlan = &PhysicalApply{}
	_ base.PhysicalPlan = &PhysicalIndexJoin{}
	_ base.PhysicalPlan = &PhysicalHashJoin{}
	_ base.PhysicalPlan = &PhysicalMergeJoin{}
	_ base.PhysicalPlan = &PhysicalUnionScan{}
	_ base.PhysicalPlan = &PhysicalWindow{}
	_ base.PhysicalPlan = &PhysicalShuffle{}
	_ base.PhysicalPlan = &PhysicalShuffleReceiverStub{}
	_ base.PhysicalPlan = &BatchPointGetPlan{}
	_ base.PhysicalPlan = &PhysicalTableSample{}
	_ base.PhysicalPlan = &PhysicalSequence{}
)

// AddExtraPhysTblIDColumn for partition table.
// For keepOrder with partition table,
// we need use partitionHandle to distinct two handles,
// the `_tidb_rowid` in different partitions can have the same value.
func AddExtraPhysTblIDColumn(sctx base.PlanContext, columns []*model.ColumnInfo, schema *expression.Schema) ([]*model.ColumnInfo, *expression.Schema, bool) {
	// Not adding the ExtraPhysTblID if already exists
	if model.FindColumnInfoByID(columns, model.ExtraPhysTblID) != nil {
		return columns, schema, false
	}
	colInfo := model.NewExtraPhysTblIDColInfo()
	colExpr := &expression.Column{
		RetType:  types.NewFieldType(mysql.TypeLonglong),
		UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
		ID:       model.ExtraPhysTblID,
	}

	// Keep `_tidb_commit_ts` as the last extra column, so insert `_tidb_tid`
	// before it when `_tidb_commit_ts` is already requested.
	commitTsOffset := -1
	for i := len(columns) - 1; i >= 0; i-- {
		if columns[i].ID == model.ExtraCommitTsID {
			commitTsOffset = i
			break
		}
	}
	if commitTsOffset == -1 || commitTsOffset >= len(schema.Columns) {
		columns = append(columns, colInfo)
		schema.Append(colExpr)
		return columns, schema, true
	}

	columns = append(columns, nil)
	copy(columns[commitTsOffset+1:], columns[commitTsOffset:])
	columns[commitTsOffset] = colInfo

	schema.Columns = append(schema.Columns, nil)
	copy(schema.Columns[commitTsOffset+1:], schema.Columns[commitTsOffset:])
	schema.Columns[commitTsOffset] = colExpr
	return columns, schema, true
}

// CollectPlanStatsVersion uses to collect the statistics version of the plan.
func CollectPlanStatsVersion(plan base.PhysicalPlan, statsInfos map[string]uint64) map[string]uint64 {
	for _, child := range plan.Children() {
		statsInfos = CollectPlanStatsVersion(child, statsInfos)
	}
	switch copPlan := plan.(type) {
	case *PhysicalTableReader:
		statsInfos = CollectPlanStatsVersion(copPlan.TablePlan, statsInfos)
	case *PhysicalIndexReader:
		statsInfos = CollectPlanStatsVersion(copPlan.IndexPlan, statsInfos)
	case *PhysicalIndexLookUpReader:
		// For index loop up, only the indexPlan is necessary,
		// because they use the same stats and we do not set the stats info for tablePlan.
		statsInfos = CollectPlanStatsVersion(copPlan.IndexPlan, statsInfos)
	case *PhysicalIndexScan:
		statsInfos[copPlan.Table.Name.O] = copPlan.StatsInfo().StatsVersion
	case *PhysicalTableScan:
		statsInfos[copPlan.Table.Name.O] = copPlan.StatsInfo().StatsVersion
	}

	return statsInfos
}

// SafeClone clones this op.PhysicalPlan and handles its panic.
func SafeClone(sctx base.PlanContext, v base.PhysicalPlan) (_ base.PhysicalPlan, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("%v", r)
		}
	}()
	return v.Clone(sctx)
}

// BasePhysicalPlan is the common structure that used in physical plan.
type BasePhysicalPlan struct {
	baseimpl.Plan

	childrenReqProps []*property.PhysicalProperty `plan-cache-clone:"shallow"`
	Self             base.PhysicalPlan
	children         []base.PhysicalPlan

	// used by the new cost interface
	PlanCostInit bool
	PlanCost     float64
	PlanCostVer2 costusage.CostVer2 `plan-cache-clone:"shallow"`

	// probeParents records the IndexJoins and Applys with this operator in their inner children.
	// Please see comments in op.PhysicalPlan for details.
	probeParents []base.PhysicalPlan `plan-cache-clone:"shallow"`

	// Only for MPP. If TiFlashFineGrainedShuffleStreamCount > 0:
	// 1. For ExchangeSender, means its output will be partitioned by hash key.
	// 2. For ExchangeReceiver/Window/Sort, means its input is already partitioned.
	TiFlashFineGrainedShuffleStreamCount uint64
}

// ******************************* start implementation of Plan interface *******************************

// ExplainInfo implements Plan ExplainInfo interface.
func (*BasePhysicalPlan) ExplainInfo() string {
	return ""
}

// Schema implements Plan Schema interface.
func (p *BasePhysicalPlan) Schema() *expression.Schema {
	return p.children[0].Schema()
}

// ******************************* end implementation of Plan interface *********************************

// *************************** start implementation of PhysicalPlan interface ***************************

// GetPlanCostVer1 implements the base.PhysicalPlan.<0th> interface.
// which calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *BasePhysicalPlan) GetPlanCostVer1(taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.PlanCostInit && !costusage.HasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		// just calculate the cost once and always reuse it
		return p.PlanCost, nil
	}
	p.PlanCost = 0 // the default implementation, the operator have no cost
	for _, child := range p.children {
		childCost, err := child.GetPlanCostVer1(taskType, option)
		if err != nil {
			return 0, err
		}
		p.PlanCost += childCost
	}
	p.PlanCostInit = true
	return p.PlanCost, nil
}

// GetPlanCostVer2 implements the base.PhysicalPlan.<1st> interface.
// which calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *BasePhysicalPlan) GetPlanCostVer2(taskType property.TaskType, option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
	if p.PlanCostInit && !costusage.HasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}
	childCosts := make([]costusage.CostVer2, 0, len(p.children))
	for _, child := range p.children {
		childCost, err := child.GetPlanCostVer2(taskType, option)
		if err != nil {
			return costusage.ZeroCostVer2, err
		}
		childCosts = append(childCosts, childCost)
	}
	if len(childCosts) == 0 {
		p.PlanCostVer2 = costusage.NewZeroCostVer2(costusage.TraceCost(option))
	} else {
		p.PlanCostVer2 = costusage.SumCostVer2(childCosts...)
	}
	p.PlanCostInit = true
	return p.PlanCostVer2, nil
}

// Attach2Task implements the base.PhysicalPlan.<2nd> interface.
func (p *BasePhysicalPlan) Attach2Task(tasks ...base.Task) base.Task {
	t := tasks[0].ConvertToRootTask(p.SCtx())
	return utilfuncp.AttachPlan2Task(p.Self, t)
}

// ToPB implements the base.PhysicalPlan.<3rd> interface.
func (p *BasePhysicalPlan) ToPB(_ *base.BuildPBContext, _ kv.StoreType) (*tipb.Executor, error) {
	return nil, errors.Errorf("plan %s fails converts to PB", p.Plan.ExplainID())
}

// GetChildReqProps implements the base.PhysicalPlan.<4th> interface.
func (p *BasePhysicalPlan) GetChildReqProps(idx int) *property.PhysicalProperty {
	return p.childrenReqProps[idx]
}

// StatsCount implements the base.PhysicalPlan.<5th> interface.
func (p *BasePhysicalPlan) StatsCount() float64 {
	return p.StatsInfo().RowCount
}

// ExtractCorrelatedCols implements the base.PhysicalPlan.<6th> interface.
func (*BasePhysicalPlan) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	return nil
}

// Children implements the base.PhysicalPlan.<7th> interface.
func (p *BasePhysicalPlan) Children() []base.PhysicalPlan {
	return p.children
}

// SetChildren implements the base.PhysicalPlan.<8th> interface.
func (p *BasePhysicalPlan) SetChildren(children ...base.PhysicalPlan) {
	p.children = children
}

// SetChild implements the base.PhysicalPlan.<9th> interface.
func (p *BasePhysicalPlan) SetChild(i int, child base.PhysicalPlan) {
	p.children[i] = child
}

// ResolveIndices implements the base.PhysicalPlan.<10th> interface.
func (p *BasePhysicalPlan) ResolveIndices() (err error) {
	for _, child := range p.children {
		err = child.ResolveIndices()
		if err != nil {
			return err
		}
	}
	return
}

// StatsInfo inherits the BasePhysicalPlan.Plan's implementation for <11th>.

// SetStats inherits the BasePhysicalPlan.Plan's implementation for <12th>.

// ExplainNormalizedInfo implements the base.PhysicalPlan.<13th> interface.
func (*BasePhysicalPlan) ExplainNormalizedInfo() string {
	return ""
}

// Clone implements the base.PhysicalPlan.<14th> interface.
func (p *BasePhysicalPlan) Clone(base.PlanContext) (base.PhysicalPlan, error) {
	return nil, errors.Errorf("%T doesn't support cloning", p.Self)
}

// MemoryUsage implements the base.PhysicalPlan.<16th> interface.
func (p *BasePhysicalPlan) MemoryUsage() (sum int64) {
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

// SetProbeParents implements base.PhysicalPlan.<17th> interface.
func (p *BasePhysicalPlan) SetProbeParents(probeParents []base.PhysicalPlan) {
	p.probeParents = probeParents
}

// GetEstRowCountForDisplay implements base.PhysicalPlan.<18th> interface.
func (p *BasePhysicalPlan) GetEstRowCountForDisplay() float64 {
	if p == nil {
		return 0
	}
	return p.StatsInfo().RowCount * utilfuncp.GetEstimatedProbeCntFromProbeParents(p.probeParents)
}

// GetActualProbeCnt implements base.PhysicalPlan.<19th> interface.
func (p *BasePhysicalPlan) GetActualProbeCnt(statsColl *execdetails.RuntimeStatsColl) int64 {
	if p == nil {
		return 1
	}
	return utilfuncp.GetActualProbeCntFromProbeParents(p.probeParents, statsColl)
}

// *************************** end implementation of PhysicalPlan interface *****************************

// CloneForPlanCacheWithSelf clones the plan with new self.
func (p *BasePhysicalPlan) CloneForPlanCacheWithSelf(newCtx base.PlanContext, newSelf base.PhysicalPlan) (*BasePhysicalPlan, bool) {
	cloned := new(BasePhysicalPlan)
	*cloned = *p
	cloned.SetSCtx(newCtx)
	cloned.Self = newSelf
	cloned.children = make([]base.PhysicalPlan, 0, len(p.children))
	for _, child := range p.children {
		clonedChild, ok := child.CloneForPlanCache(newCtx)
		if !ok {
			return nil, false
		}
		clonedPP, ok := clonedChild.(base.PhysicalPlan)
		if !ok {
			return nil, false
		}
		cloned.children = append(cloned.children, clonedPP)
	}
	return cloned, true
}

// CloneWithSelf clones the plan with new self.
func (p *BasePhysicalPlan) CloneWithSelf(newCtx base.PlanContext, newSelf base.PhysicalPlan) (*BasePhysicalPlan, error) {
	base := &BasePhysicalPlan{
		Plan:                                 p.Plan,
		Self:                                 newSelf,
		TiFlashFineGrainedShuffleStreamCount: p.TiFlashFineGrainedShuffleStreamCount,
		probeParents:                         p.probeParents,
	}
	base.SetSCtx(newCtx)
	for _, child := range p.children {
		cloned, err := child.Clone(newCtx)
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

// SetChildrenReqProps set the BasePhysicalPlan's childrenReqProps.
func (p *BasePhysicalPlan) SetChildrenReqProps(reqProps []*property.PhysicalProperty) {
	p.childrenReqProps = reqProps
}

// SetXthChildReqProps set the BasePhysicalPlan's x-th child as required property.
func (p *BasePhysicalPlan) SetXthChildReqProps(x int, reqProps *property.PhysicalProperty) {
	p.childrenReqProps[x] = reqProps
}

// NewBasePhysicalPlan creates a new BasePhysicalPlan.
func NewBasePhysicalPlan(ctx base.PlanContext, tp string, self base.PhysicalPlan, offset int) BasePhysicalPlan {
	return BasePhysicalPlan{
		Plan: baseimpl.NewBasePlan(ctx, tp, offset),
		Self: self,
	}
}

func admitIndexJoinProps(children []*property.PhysicalProperty, prop *property.PhysicalProperty) []*property.PhysicalProperty {
	if prop.TaskTp == property.MppTaskType {
		// if the parent prop is mppTask, we assume it couldn't contain indexJoinProp by default,
		// which is guaranteed by the parent physical plans enumeration.
		return children
	}
	// only admit root & cop task type to push down indexJoinProp.
	if prop.IndexJoinProp != nil {
		newChildren := children[:0]
		for _, child := range children {
			if child.TaskTp != property.MppTaskType {
				child.IndexJoinProp = prop.IndexJoinProp
				// only admit non-mpp task prop.
				newChildren = append(newChildren, child)
			}
		}
		children = newChildren
	}
	return children
}

func admitIndexJoinProp(child, prop *property.PhysicalProperty) *property.PhysicalProperty {
	if prop.TaskTp == property.MppTaskType {
		// if the parent prop is mppTask, we assume it couldn't contain indexJoinProp by default,
		// which is guaranteed by the parent physical plans enumeration.
		return child
	}
	// only admit root & cop task type to push down indexJoinProp.
	if prop.IndexJoinProp != nil {
		if child.TaskTp != property.MppTaskType {
			child.IndexJoinProp = prop.IndexJoinProp
		} else {
			// only admit non-mpp task prop.
			child = nil
		}
	}
	return child
}

func admitIndexJoinTypes(types []property.TaskType, prop *property.PhysicalProperty) []property.TaskType {
	if prop.TaskTp == property.MppTaskType {
		// if the parent prop is mppTask, we assume it couldn't contain indexJoinProp by default,
		// which is guaranteed by the parent physical plans enumeration.
		return types
	}
	// only admit root & cop task type to push down indexJoinProp.
	if prop.IndexJoinProp != nil {
		newTypes := types[:0]
		for _, tp := range types {
			if tp != property.MppTaskType {
				newTypes = append(newTypes, tp)
			}
		}
		types = newTypes
	}
	return types
}

// GetStatsInfo gets the statistics info from a physical plan tree.
func GetStatsInfo(i any) map[string]uint64 {
	if i == nil {
		// it's a workaround for https://github.com/pingcap/tidb/issues/17419
		// To entirely fix this, uncomment the assertion in TestPreparedIssue17419
		return nil
	}
	p := i.(base.Plan)
	var physicalPlan base.PhysicalPlan
	switch x := p.(type) {
	case *Insert:
		physicalPlan = x.SelectPlan
	case *Update:
		physicalPlan = x.SelectPlan
	case *Delete:
		physicalPlan = x.SelectPlan
	case base.PhysicalPlan:
		physicalPlan = x
	}

	if physicalPlan == nil {
		return nil
	}

	statsInfos := make(map[string]uint64)
	statsInfos = CollectPlanStatsVersion(physicalPlan, statsInfos)
	return statsInfos
}

// FindBestTask converts the logical plan to the physical plan.
// It is called recursively from the parent to the children to create the result physical plan.
// Some logical plans will convert the children to the physical plans in different ways, and return the one
// With the lowest cost and how many plans are found in this function.
func FindBestTask(e base.LogicalPlan, prop *property.PhysicalProperty) (bestTask base.Task, err error) {
	// since different logical operator may have different findBestTask before like:
	// 	utilfuncp.FindBestTask4BaseLogicalPlan = findBestTask
	//	utilfuncp.FindBestTask4LogicalCTE = findBestTask4LogicalCTE
	//	utilfuncp.FindBestTask4LogicalShow = findBestTask4LogicalShow
	//	utilfuncp.FindBestTask4LogicalCTETable = findBestTask4LogicalCTETable
	//	utilfuncp.FindBestTask4LogicalMemTable = findBestTask4LogicalMemTable
	//	utilfuncp.FindBestTask4LogicalDataSource = findBestTask4LogicalDataSource
	//	utilfuncp.FindBestTask4LogicalShowDDLJobs = findBestTask4LogicalShowDDLJobs
	// once we call GE's findBestTask from group expression level, we should judge from here, and get the
	// wrapped logical plan and then call their specific function pointer to handle logic inside. At the
	// same time, we will pass ge (also implement LogicalPlan interface) as the first parameter for iterate
	// ge's children in memo scenario.
	// And since base.LogicalPlan is a common parent pointer of GE and LogicalPlan, we can use same portal.
	switch lop := e.GetWrappedLogicalPlan().(type) {
	case *logicalop.LogicalCTE:
		return findBestTask4LogicalCTE(e, prop)
	case *logicalop.LogicalShow:
		return findBestTask4LogicalShow(e, prop)
	case *logicalop.LogicalCTETable:
		return findBestTask4LogicalCTETable(e, prop)
	case *logicalop.LogicalMemTable:
		return findBestTask4LogicalMemTable(lop, prop)
	case *logicalop.LogicalTableDual:
		return findBestTask4LogicalTableDual(lop, prop)
	case *logicalop.DataSource:
		return utilfuncp.FindBestTask4LogicalDataSource(e, prop)
	case *logicalop.LogicalShowDDLJobs:
		return findBestTask4LogicalShowDDLJobs(e, prop)
	case *logicalop.MockDataSource:
		return findBestTask4LogicalMockDatasource(lop, prop)
	default:
		return utilfuncp.FindBestTask4BaseLogicalPlan(e, prop)
	}
}
