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
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
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
		buffer = util.ExplainPartitionBy(ectx, buffer, p.GetPartitionBy(), false)
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
		buffer = util.ExplainPartitionBy(ectx, buffer, p.GetPartitionBy(), true)
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
func (p *PhysicalTopN) GetPlanCostVer1(taskType property.TaskType, option *optimizetrace.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalTopN(p, taskType, option)
}

// GetPlanCostVer2 calculates the cost of the plan if it has not been calculated yet and returns a CostVer2 object.
func (p *PhysicalTopN) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error) {
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
