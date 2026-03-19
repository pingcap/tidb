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
	"math"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
)

// EnforceProperty act as portal to enforce a physical property to a task.
func EnforceProperty(p *property.PhysicalProperty, tsk base.Task, ctx base.PlanContext, fd *funcdep.FDSet) base.Task {
	if p.TaskTp == property.MppTaskType {
		mpp, ok := tsk.(*MppTask)
		if !ok || mpp.Invalid() {
			return base.InvalidTask
		}
		if !p.IsSortItemAllForPartition() {
			ctx.GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because operator `Sort` is not supported now.")
			return base.InvalidTask
		}
		tsk = mpp.EnforceExchanger(p, fd)
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

// EnforceExchanger enforces an exchange operator on the top of the mpp task if necessary.
func (t *MppTask) EnforceExchanger(prop *property.PhysicalProperty, fd *funcdep.FDSet) *MppTask {
	if !property.NeedEnforceExchanger(t.partTp, t.HashCols, prop, fd) {
		return t
	}
	return t.Copy().(*MppTask).EnforceExchangerImpl(prop)
}

// EnforceExchangerImpl enforces an exchange operator on the top of the mpp task.
func (t *MppTask) EnforceExchangerImpl(prop *property.PhysicalProperty) *MppTask {
	if collate.NewCollationEnabled() && !t.p.SCtx().GetSessionVars().HashExchangeWithNewCollation && prop.MPPPartitionTp == property.HashType {
		for _, col := range prop.MPPPartitionCols {
			if types.IsString(col.Col.RetType.GetType()) {
				t.p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because when `new_collation_enabled` is true, HashJoin or HashAgg with string key is not supported now.")
				return &MppTask{}
			}
		}
	}
	ctx := t.p.SCtx()
	sender := PhysicalExchangeSender{
		ExchangeType: prop.MPPPartitionTp.ToExchangeType(),
		HashCols:     prop.MPPPartitionCols,
	}.Init(ctx, t.p.StatsInfo())

	if ctx.GetSessionVars().ChooseMppVersion() >= kv.MppVersionV1 {
		sender.CompressionMode = ctx.GetSessionVars().ChooseMppExchangeCompressionMode()
	}

	sender.SetChildren(t.p)
	receiver := PhysicalExchangeReceiver{}.Init(ctx, t.p.StatsInfo())
	receiver.SetChildren(sender)
	nt := &MppTask{
		p:        receiver,
		partTp:   prop.MPPPartitionTp,
		HashCols: prop.MPPPartitionCols,
	}
	nt.Warnings.CopyFrom(&t.Warnings)
	return nt
}
