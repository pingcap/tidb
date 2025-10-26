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
	"fmt"
	"math"
	"strconv"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

// PhysicalSequence is the physical representation of LogicalSequence. Used to mark the CTE producers in the plan tree.
type PhysicalSequence struct {
	PhysicalSchemaProducer
}

// Init initializes PhysicalSequence
func (p PhysicalSequence) Init(ctx base.PlanContext, stats *property.StatsInfo, blockOffset int, props ...*property.PhysicalProperty) *PhysicalSequence {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeSequence, &p, blockOffset)
	p.SetStats(stats)
	p.SetChildrenReqProps(props)
	return &p
}

// MemoryUsage returns the memory usage of the PhysicalSequence.
func (p *PhysicalSequence) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage()

	return
}

// ExplainID overrides the ExplainID.
func (p *PhysicalSequence) ExplainID(_ ...bool) fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		if p.SCtx() != nil && p.SCtx().GetSessionVars().StmtCtx.IgnoreExplainIDSuffix {
			return p.TP()
		}
		return p.TP() + "_" + strconv.Itoa(p.ID())
	})
}

// ExplainInfo overrides the ExplainInfo.
func (*PhysicalSequence) ExplainInfo() string {
	res := "Sequence Node"
	return res
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalSequence) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalSequence)
	*cloned = *p
	cloned.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.PhysicalSchemaProducer = *base
	return cloned, nil
}

// Schema returns its last child(which is the main query tree)'s schema.
func (p *PhysicalSequence) Schema() *expression.Schema {
	return p.Children()[len(p.Children())-1].Schema()
}

// Attach2Task implements the PhysicalPlan interface.
func (p *PhysicalSequence) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4PhysicalSequence(p, tasks...)
}

// ExhaustPhysicalPlans4LogicalSequence generates PhysicalSequence plans from LogicalSequence.
func ExhaustPhysicalPlans4LogicalSequence(super base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	g, ls := base.GetGEAndLogicalOp[*logicalop.LogicalSequence](super)
	possibleChildrenProps := make([][]*property.PhysicalProperty, 0, 2)
	anyType := &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.AnyType, CanAddEnforcer: true,
		CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown}
	if prop.TaskTp == property.MppTaskType {
		if prop.CTEProducerStatus == property.SomeCTEFailedMpp {
			return nil, true, nil
		}
		anyType.CTEProducerStatus = property.AllCTECanMpp
		possibleChildrenProps = append(possibleChildrenProps, []*property.PhysicalProperty{anyType, prop.CloneEssentialFields()})
	} else {
		copied := prop.CloneEssentialFields()
		copied.CTEProducerStatus = property.SomeCTEFailedMpp
		possibleChildrenProps = append(possibleChildrenProps, []*property.PhysicalProperty{{TaskTp: property.RootTaskType, ExpectedCnt: math.MaxFloat64, CTEProducerStatus: property.SomeCTEFailedMpp}, copied})
	}

	if prop.TaskTp != property.MppTaskType && prop.CTEProducerStatus != property.SomeCTEFailedMpp &&
		ls.SCtx().GetSessionVars().IsMPPAllowed() && prop.IsSortItemEmpty() {
		possibleChildrenProps = append(possibleChildrenProps, []*property.PhysicalProperty{anyType, anyType.CloneEssentialFields()})
	}
	var seqSchema *expression.Schema
	if g != nil {
		seqSchema = g.GetInputSchema(g.InputsLen() - 1)
	} else {
		seqSchema = ls.Children()[ls.ChildLen()-1].Schema()
	}
	seqs := make([]base.PhysicalPlan, 0, len(possibleChildrenProps))
	for _, propChoice := range possibleChildrenProps {
		childReqs := make([]*property.PhysicalProperty, 0, ls.ChildLen())
		for range ls.ChildLen() - 1 {
			childReqs = append(childReqs, propChoice[0].CloneEssentialFields())
		}
		childReqs = append(childReqs, propChoice[1])
		seq := PhysicalSequence{}.Init(ls.SCtx(), ls.StatsInfo(), ls.QueryBlockOffset(), childReqs...)
		seq.SetSchema(seqSchema)
		seqs = append(seqs, seq)
	}
	return seqs, true, nil
}
