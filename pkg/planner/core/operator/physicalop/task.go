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
	p := t.IndexPlan
	if t.TablePlan != nil && (t.IndexPlanFinished || p == nil) {
		p = t.TablePlan
	}
	for len(p.Children()) > 0 {
		if len(p.Children()) > 1 {
			return kv.TiFlash
		}
		p = p.Children()[0]
	}
	switch x := p.(type) {
	case *PhysicalTableScan:
		return x.StoreType
	case *PhysicalIndexScan:
		return x.StoreType
	}
	return kv.TiKV
}
