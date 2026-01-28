// Copyright 2025 PingCAP, Inc.
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
	"bytes"
	"fmt"
	"math"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
)

// PhysicalTopN is the physical operator of topN.
type PhysicalTopN struct {
	PhysicalSchemaProducer

	ByItems     []*util.ByItems
	PartitionBy []property.SortItem
	Offset      uint64
	Count       uint64
}

// ExhaustPhysicalPlans4LogicalTopN exhausts PhysicalTopN plans from LogicalTopN.
func ExhaustPhysicalPlans4LogicalTopN(lt *logicalop.LogicalTopN, prop *property.PhysicalProperty) ([][]base.PhysicalPlan, bool, error) {
	if MatchItems(prop, lt.ByItems) {
		// PhysicalTopN and PhysicalLimit are in different slices
		// so we can support preferring a specific LIMIT or TopN within one slice.
		// For example, among all LIMIT tasks, we prefer the one pushed down to TiKV.
		// Then we compare the preferred task from each slice by their actual cost.
		return [][]base.PhysicalPlan{getPhysTopN(lt, prop), getPhysLimits(lt, prop)}, true, nil
	}
	return nil, true, nil
}

// Init initializes PhysicalTopN.
func (p PhysicalTopN) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalTopN {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeTopN, &p, offset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	return &p
}

// GetPartitionBy returns partition by fields
func (p *PhysicalTopN) GetPartitionBy() []property.SortItem {
	return p.PartitionBy
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalTopN) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalTopN)
	*cloned = *p
	cloned.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.PhysicalSchemaProducer = *base
	cloned.ByItems = make([]*util.ByItems, 0, len(p.ByItems))
	for _, it := range p.ByItems {
		cloned.ByItems = append(cloned.ByItems, it.Clone())
	}
	cloned.PartitionBy = make([]property.SortItem, 0, len(p.PartitionBy))
	for _, it := range p.PartitionBy {
		cloned.PartitionBy = append(cloned.PartitionBy, it.Clone())
	}
	return cloned, nil
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalTopN) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		corCols = append(corCols, expression.ExtractCorColumns(item.Expr)...)
	}
	return corCols
}

// MemoryUsage return the memory usage of PhysicalTopN
func (p *PhysicalTopN) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.BasePhysicalPlan.MemoryUsage() + size.SizeOfSlice + int64(cap(p.ByItems))*size.SizeOfPointer + size.SizeOfUint64*2
	for _, byItem := range p.ByItems {
		sum += byItem.MemoryUsage()
	}
	for _, item := range p.PartitionBy {
		sum += item.MemoryUsage()
	}
	return
}

// ExplainInfo implements Plan interface.
func (p *PhysicalTopN) ExplainInfo() string {
	ectx := p.SCtx().GetExprCtx().GetEvalCtx()
	buffer := bytes.NewBufferString("")
	if len(p.GetPartitionBy()) > 0 {
		buffer = property.ExplainPartitionBy(ectx, buffer, p.GetPartitionBy(), false)
		buffer.WriteString(" ")
	}
	if len(p.ByItems) > 0 {
		// Add order by text to separate partition by. Otherwise, do not add order by to
		// avoid breaking existing TopN tests.
		if len(p.GetPartitionBy()) > 0 {
			buffer.WriteString("order by ")
		}
		buffer = util.ExplainByItems(p.SCtx().GetExprCtx().GetEvalCtx(), buffer, p.ByItems)
	}
	switch p.SCtx().GetSessionVars().EnableRedactLog {
	case perrors.RedactLogDisable:
		fmt.Fprintf(buffer, ", offset:%v, count:%v", p.Offset, p.Count)
	case perrors.RedactLogMarker:
		fmt.Fprintf(buffer, ", offset:‹%v›, count:‹%v›", p.Offset, p.Count)
	case perrors.RedactLogEnable:
		fmt.Fprintf(buffer, ", offset:?, count:?")
	}
	return buffer.String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalTopN) ExplainNormalizedInfo() string {
	ectx := p.SCtx().GetExprCtx().GetEvalCtx()
	buffer := bytes.NewBufferString("")
	if len(p.GetPartitionBy()) > 0 {
		buffer = property.ExplainPartitionBy(ectx, buffer, p.GetPartitionBy(), true)
		buffer.WriteString(" ")
	}
	if len(p.ByItems) > 0 {
		// Add order by text to separate partition by. Otherwise, do not add order by to
		// avoid breaking existing TopN tests.
		if len(p.GetPartitionBy()) > 0 {
			buffer.WriteString("order by ")
		}
		buffer = explainNormalizedByItems(buffer, p.ByItems)
	}
	return buffer.String()
}

func explainNormalizedByItems(buffer *bytes.Buffer, byItems []*util.ByItems) *bytes.Buffer {
	for i, item := range byItems {
		if item.Desc {
			fmt.Fprintf(buffer, "%s:desc", item.Expr.ExplainNormalizedInfo())
		} else {
			fmt.Fprintf(buffer, "%s", item.Expr.ExplainNormalizedInfo())
		}

		if i+1 < len(byItems) {
			buffer.WriteString(", ")
		}
	}
	return buffer
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalTopN) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	client := ctx.GetClient()
	topNExec := &tipb.TopN{
		Limit: p.Count,
	}
	evalCtx := ctx.GetExprCtx().GetEvalCtx()
	for _, item := range p.ByItems {
		topNExec.OrderBy = append(topNExec.OrderBy, expression.SortByItemToPB(evalCtx, client, item.Expr, item.Desc))
	}
	for _, item := range p.PartitionBy {
		topNExec.PartitionBy = append(topNExec.PartitionBy, expression.SortByItemToPB(evalCtx, client, item.Col.Clone(), item.Desc))
	}
	executorID := ""
	if storeType == kv.TiFlash {
		var err error
		topNExec.Child, err = p.Children()[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, perrors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeTopN, TopN: topNExec, ExecutorId: &executorID}, nil
}

// GetCost computes cost of TopN operator itself.
func (p *PhysicalTopN) GetCost(count float64, isRoot bool) float64 {
	heapSize := max(float64(p.Offset+p.Count), 2.0)
	sessVars := p.SCtx().GetSessionVars()
	// Ignore the cost of `doCompaction` in current implementation of `TopNExec`, since it is the
	// special side-effect of our Chunk format in TiDB layer, which may not exist in coprocessor's
	// implementation, or may be removed in the future if we change data format.
	// Note that we are using worst complexity to compute CPU cost, because it is simpler compared with
	// considering probabilities of average complexity, i.e, we may not need adjust heap for each input
	// row.
	var cpuCost float64
	if isRoot {
		cpuCost = count * math.Log2(heapSize) * sessVars.GetCPUFactor()
	} else {
		cpuCost = count * math.Log2(heapSize) * sessVars.GetCopCPUFactor()
	}
	memoryCost := heapSize * sessVars.GetMemoryFactor()
	return cpuCost + memoryCost
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalTopN) GetPlanCostVer1(taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalTopN(p, taskType, option)
}

// GetPlanCostVer2 calculates the cost of the plan if it has not been calculated yet and returns a CostVer2 object.
func (p *PhysicalTopN) GetPlanCostVer2(taskType property.TaskType, option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PhysicalTopN(p, taskType, option, isChildOfINL...)
}

// ResolveIndices implements Plan interface.
func (p *PhysicalTopN) ResolveIndices() (err error) {
	return utilfuncp.ResolveIndices4PhysicalTopN(p)
}

// Attach2Task implements the PhysicalPlan interface.
func (p *PhysicalTopN) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4PhysicalTopN(p, tasks...)
}

func getPhysTopN(lt *logicalop.LogicalTopN, prop *property.PhysicalProperty) []base.PhysicalPlan {
	// topN should always generate rootTaskType for:
	// case1: after v7.5, since tiFlash Cop has been banned, mppTaskType may return invalid task when there are some root conditions.
	// case2: for index merge case which can only be run in root type, topN and limit can't be pushed to the inside index merge when it's an intersection.
	// note: don't change the task enumeration order here.
	allTaskTypes := []property.TaskType{property.CopSingleReadTaskType, property.CopMultiReadTaskType, property.RootTaskType}
	// we move the pushLimitOrTopNForcibly check to attach2Task to do the prefer choice.
	mppAllowed := lt.SCtx().GetSessionVars().IsMPPAllowed()
	if mppAllowed {
		allTaskTypes = append(allTaskTypes, property.MppTaskType)
	}
	ret := make([]base.PhysicalPlan, 0, len(allTaskTypes))
	for _, tp := range allTaskTypes {
		resultProp := &property.PhysicalProperty{TaskTp: tp, ExpectedCnt: math.MaxFloat64,
			CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown}
		topN := PhysicalTopN{
			ByItems:     lt.ByItems,
			PartitionBy: lt.PartitionBy,
			Count:       lt.Count,
			Offset:      lt.Offset,
		}.Init(lt.SCtx(), lt.StatsInfo(), lt.QueryBlockOffset(), resultProp)
		topN.SetSchema(lt.Schema())
		ret = append(ret, topN)
	}

	// Generate additional candidate plans for partial order optimization using prefix index.
	if canUsePartialOrder4TopN(lt) {
		topNWithPartialOrderProperty := getPhysTopNWithPartialOrderProperty(lt, prop)
		ret = append(ret, topNWithPartialOrderProperty...)
	}

	// If we can generate MPP task and there's vector distance function in the order by column.
	// We will try to generate a property for possible vector indexes.
	if mppAllowed {
		if len(lt.ByItems) != 1 {
			return ret
		}
		vs := expression.InterpretVectorSearchExpr(lt.ByItems[0].Expr)
		if vs == nil {
			return ret
		}
		// Currently vector index only accept ascending order.
		if lt.ByItems[0].Desc {
			return ret
		}
		// Currently, we only deal with the case the TopN is directly above a DataSource.
		ds, ok := lt.Children()[0].(*logicalop.DataSource)
		if !ok {
			return ret
		}
		// Reject any filters.
		if len(ds.PushedDownConds) > 0 {
			return ret
		}
		resultProp := &property.PhysicalProperty{
			TaskTp:            property.MppTaskType,
			ExpectedCnt:       math.MaxFloat64,
			CTEProducerStatus: prop.CTEProducerStatus,
		}
		resultProp.VectorProp.VSInfo = vs
		resultProp.VectorProp.TopK = uint32(lt.Count + lt.Offset)
		topN := PhysicalTopN{
			ByItems:     lt.ByItems,
			PartitionBy: lt.PartitionBy,
			Count:       lt.Count,
			Offset:      lt.Offset,
		}.Init(lt.SCtx(), lt.StatsInfo(), lt.QueryBlockOffset(), resultProp)
		topN.SetSchema(lt.Schema())
		ret = append(ret, topN)
	}
	return ret
}

// canUsePartialOrder4TopN checks if the TopN's child tree satisfies the conditions
// for partial order optimization.
// Supported patterns:
// 1. TopN -> DataSource
// 2. TopN -> Selection -> DataSource
// 3. TopN -> Projection -> DataSource
// 4. TopN -> Projection -> Selection -> DataSource
// 5. TopN -> Selection -> Projection -> DataSource
//
// Note: This function only checks if the query pattern is supported.
// The actual check of whether PartialOrderInfo can pass through Projection
// is done in LogicalProjection.TryToGetChildProp during physical optimization.
func canUsePartialOrder4TopN(lt *logicalop.LogicalTopN) bool {
	if !lt.SCtx().GetSessionVars().OptPartialOrderedIndexForTopN {
		return false
	}
	// Must have ORDER BY columns
	if len(lt.ByItems) == 0 {
		return false
	}

	return checkPartialOrderPattern(lt.SCtx(), lt.Children()[0])
}

// checkPartialOrderPattern recursively checks if the plan tree matches
// a supported pattern for partial order optimization.
// Supported intermediate operators: Selection, Projection
// Terminal operator: DataSource
func checkPartialOrderPattern(ctx base.PlanContext, plan base.LogicalPlan) bool {
	switch p := plan.(type) {
	case *logicalop.DataSource:
		return true

	case *logicalop.LogicalSelection:
		// Selection can pass through, check its child
		if len(p.Children()) != 1 {
			return false
		}
		return checkPartialOrderPattern(ctx, p.Children()[0])

	case *logicalop.LogicalProjection:
		// Projection can pass through (actual column check is done in TryToGetChildProp)
		if len(p.Children()) != 1 {
			return false
		}
		return checkPartialOrderPattern(ctx, p.Children()[0])

	default:
		// Other operators are not supported
		return false
	}
}

// getPhysTopNWithPartialOrderProperty generates PhysicalTopN plans with partial order property
// that use partial order optimization with prefix index.
func getPhysTopNWithPartialOrderProperty(lt *logicalop.LogicalTopN, prop *property.PhysicalProperty) []base.PhysicalPlan {
	// Convert logical TopN ByItems to SortItems for PartialOrderInfo
	sortItems := make([]*property.SortItem, 0, len(lt.ByItems))
	for _, byItem := range lt.ByItems {
		col, ok := byItem.Expr.(*expression.Column)
		if !ok {
			// ORDER BY must be simple column references for partial order optimization
			return nil
		}
		sortItems = append(sortItems, &property.SortItem{
			Col:  col,
			Desc: byItem.Desc,
		})
	}

	// Create PhysicalProperty with PartialOrderInfo
	// Use CopMultiReadTaskType for IndexLookUp
	partialOrderProp := &property.PhysicalProperty{
		TaskTp: property.CopMultiReadTaskType,
		// TODO: change it to limit + offset + N
		ExpectedCnt: math.MaxFloat64,
		PartialOrderInfo: &property.PartialOrderInfo{
			SortItems: sortItems,
		},
		CTEProducerStatus: prop.CTEProducerStatus,
		NoCopPushDown:     prop.NoCopPushDown,
	}

	topN := PhysicalTopN{
		ByItems:     lt.ByItems,
		PartitionBy: lt.PartitionBy,
		Count:       lt.Count,
		Offset:      lt.Offset,
	}.Init(lt.SCtx(), lt.StatsInfo(), lt.QueryBlockOffset(), partialOrderProp)
	topN.SetSchema(lt.Schema())

	return []base.PhysicalPlan{topN}
}
