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
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func tryExpandVirtualColumn(p base.PhysicalPlan) {
	if ts, ok := p.(*PhysicalTableScan); ok {
		ts.Columns = ExpandVirtualColumn(ts.Columns, ts.Schema(), ts.Table.Columns)
		return
	}
	for _, child := range p.Children() {
		tryExpandVirtualColumn(child)
	}
}

func collectPartitionInfosFromMPPPlan(p *PhysicalTableReader, mppPlan base.PhysicalPlan) {
	switch x := mppPlan.(type) {
	case *PhysicalTableScan:
		p.TableScanAndPartitionInfos = append(p.TableScanAndPartitionInfos, TableScanAndPartitionInfo{TableScan: x, PhysPlanPartInfo: x.PlanPartInfo})
	default:
		for _, ch := range mppPlan.Children() {
			collectPartitionInfosFromMPPPlan(p, ch)
		}
	}
}

// NeedEnforceExchanger checks if we need to enforce an exchange operator on the top of the mpp task.
// TODO: make it as private
func (t *MppTask) NeedEnforceExchanger(prop *property.PhysicalProperty, fd *funcdep.FDSet) bool {
	switch prop.MPPPartitionTp {
	case property.AnyType:
		return false
	case property.BroadcastType:
		return true
	case property.SinglePartitionType:
		return t.partTp != property.SinglePartitionType
	default:
		if t.partTp != property.HashType {
			return true
		}
		// for example, if already partitioned by hash(B,C), then same (A,B,C) must distribute on a same node.
		if fd != nil && len(t.HashCols) != 0 {
			return prop.NeedMPPExchangeByEquivalence(t.HashCols, fd)
		}
		if len(prop.MPPPartitionCols) != len(t.HashCols) {
			return true
		}
		for i, col := range prop.MPPPartitionCols {
			if !col.Equal(t.HashCols[i]) {
				return true
			}
		}
		return false
	}
}

// EnforceExchanger enforces an exchange operator on the top of the mpp task if necessary.
// TODO: make it as private
func (t *MppTask) EnforceExchanger(prop *property.PhysicalProperty, fd *funcdep.FDSet) *MppTask {
	if !t.NeedEnforceExchanger(prop, fd) {
		return t
	}
	return t.Copy().(*MppTask).EnforceExchangerImpl(prop)
}

// EnforceExchangerImpl enforces an exchange operator on the top of the mpp task.
// TODO: make it as private
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

func (t *CopTask) handleRootTaskConds(ctx base.PlanContext, newTask *RootTask) {
	if len(t.RootTaskConds) > 0 {
		selectivity, _, err := cardinality.Selectivity(ctx, t.TblColHists, t.RootTaskConds, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = cost.SelectionFactor
		}
		sel := PhysicalSelection{Conditions: t.RootTaskConds}.Init(ctx, newTask.GetPlan().StatsInfo().Scale(ctx.GetSessionVars(), selectivity), newTask.GetPlan().QueryBlockOffset())
		sel.FromDataSource = true
		sel.SetChildren(newTask.GetPlan())
		newTask.SetPlan(sel)
	}
}

// FinishIndexPlan means we no longer add plan to index plan, and compute the network cost for it.
func (t *CopTask) FinishIndexPlan() {
	if t.IndexPlanFinished {
		return
	}
	t.IndexPlanFinished = true
	// index merge case is specially handled for now.
	// We need a elegant way to solve the stats of index merge in this case.
	if t.TablePlan != nil && t.IndexPlan != nil {
		ts := t.TablePlan.(*PhysicalTableScan)
		originStats := ts.StatsInfo()
		ts.SetStats(t.IndexPlan.StatsInfo())
		if originStats != nil {
			// keep the original stats version
			ts.StatsInfo().StatsVersion = originStats.StatsVersion
		}
	}
}

// GetStoreType gets the store type of the cop task.
func (t *CopTask) GetStoreType() kv.StoreType {
	if t.TablePlan == nil {
		return kv.TiKV
	}
	p := t.TablePlan
	for len(p.Children()) > 0 {
		if len(p.Children()) > 1 {
			return kv.TiFlash
		}
		p = p.Children()[0]
	}
	if ts, ok := p.(*PhysicalTableScan); ok {
		return ts.StoreType
	}
	return kv.TiKV
}
