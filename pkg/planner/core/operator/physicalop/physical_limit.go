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

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
)

// PhysicalLimit is the physical operator of Limit.
type PhysicalLimit struct {
	PhysicalSchemaProducer

	PartitionBy []property.SortItem
	Offset      uint64
	Count       uint64
}

// Init initializes PhysicalLimit.
func (p PhysicalLimit) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalLimit {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeLimit, &p, offset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	return &p
}

// GetPartitionBy returns partition by fields
func (p *PhysicalLimit) GetPartitionBy() []property.SortItem {
	return p.PartitionBy
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalLimit) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalLimit)
	*cloned = *p
	cloned.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.PartitionBy = make([]property.SortItem, 0, len(p.PartitionBy))
	for _, it := range p.PartitionBy {
		cloned.PartitionBy = append(cloned.PartitionBy, it.Clone())
	}
	cloned.PhysicalSchemaProducer = *base
	return cloned, nil
}

// MemoryUsage return the memory usage of PhysicalLimit
func (p *PhysicalLimit) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + size.SizeOfUint64*2
	return
}

// ExplainInfo implements Plan interface.
func (p *PhysicalLimit) ExplainInfo() string {
	ectx := p.SCtx().GetExprCtx().GetEvalCtx()
	redact := p.SCtx().GetSessionVars().EnableRedactLog
	buffer := bytes.NewBufferString("")
	if len(p.GetPartitionBy()) > 0 {
		buffer = util.ExplainPartitionBy(ectx, buffer, p.GetPartitionBy(), false)
		fmt.Fprintf(buffer, ", ")
	}
	if redact == perrors.RedactLogDisable {
		fmt.Fprintf(buffer, "offset:%v, count:%v", p.Offset, p.Count)
	} else if redact == perrors.RedactLogMarker {
		fmt.Fprintf(buffer, "offset:‹%v›, count:‹%v›", p.Offset, p.Count)
	} else if redact == perrors.RedactLogEnable {
		fmt.Fprintf(buffer, "offset:?, count:?")
	}
	return buffer.String()
}

// CloneForPlanCache implements the base.Plan interface.
func (p *PhysicalLimit) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalLimit)
	*cloned = *p
	basePlan, baseOK := p.PhysicalSchemaProducer.CloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.PhysicalSchemaProducer = *basePlan
	cloned.PartitionBy = util.CloneSortItems(p.PartitionBy)
	return cloned, true
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalLimit) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	client := ctx.GetClient()
	limitExec := &tipb.Limit{
		Limit: p.Count,
	}
	executorID := ""
	cc := make(expression.CloneContext, 2)
	for _, item := range p.PartitionBy {
		limitExec.PartitionBy = append(limitExec.PartitionBy, expression.SortByItemToPB(ctx.GetExprCtx().GetEvalCtx(), client, item.Col.Clone(cc), item.Desc))
	}
	if storeType == kv.TiFlash {
		var err error
		limitExec.Child, err = p.Children()[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, perrors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeLimit, Limit: limitExec, ExecutorId: &executorID}, nil
}

// ResolveIndices implements the base.PhysicalPlan interface.
func (p *PhysicalLimit) ResolveIndices() (err error) {
	return utilfuncp.ResolveIndices4PhysicalLimit(p)
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalLimit) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4PhysicalLimit(p, tasks...)
}
