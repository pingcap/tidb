// Copyright 2017 PingCAP, Inc.
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
	"slices"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
)

func attach2TaskForMpp(p *physicalop.PhysicalHashAgg, tasks ...base.Task) base.Task {
	ectx := p.SCtx().GetExprCtx().GetEvalCtx()

	t := tasks[0].Copy()
	mpp, ok := t.(*physicalop.MppTask)
	if !ok {
		return base.InvalidTask
	}
	switch p.MppRunMode {
	case physicalop.Mpp1Phase:
		// 1-phase agg: when the partition columns can be satisfied, where the plan does not need to enforce Exchange
		// only push down the original agg
		proj := p.ConvertAvgForMPP()
		attachPlan2Task(p, mpp)
		if proj != nil {
			attachPlan2Task(proj, mpp)
		}
		return mpp
	case physicalop.Mpp2Phase:
		// TODO: when partition property is matched by sub-plan, we actually needn't do extra an exchange and final agg.
		proj := p.ConvertAvgForMPP()
		partialAgg, finalAgg := p.NewPartialAggregate(kv.TiFlash, true)
		if partialAgg == nil {
			return base.InvalidTask
		}
		attachPlan2Task(partialAgg, mpp)
		partitionCols := p.MppPartitionCols
		if len(partitionCols) == 0 {
			items := finalAgg.(*physicalop.PhysicalHashAgg).GroupByItems
			partitionCols = make([]*property.MPPPartitionColumn, 0, len(items))
			for _, expr := range items {
				col, ok := expr.(*expression.Column)
				if !ok {
					return base.InvalidTask
				}
				partitionCols = append(partitionCols, &property.MPPPartitionColumn{
					Col:       col,
					CollateID: property.GetCollateIDByNameForPartition(col.GetType(ectx).GetCollate()),
				})
			}
		}
		if partialHashAgg, ok := partialAgg.(*physicalop.PhysicalHashAgg); ok && len(partitionCols) != 0 {
			partialHashAgg.TiflashPreAggMode = p.SCtx().GetSessionVars().TiFlashPreAggMode
		}
		prop := &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.HashType, MPPPartitionCols: partitionCols}
		newMpp := mpp.EnforceExchangerImpl(prop)
		if newMpp.Invalid() {
			return newMpp
		}
		attachPlan2Task(finalAgg, newMpp)
		// TODO: how to set 2-phase cost?
		if proj != nil {
			attachPlan2Task(proj, newMpp)
		}
		return newMpp
	case physicalop.MppTiDB:
		partialAgg, finalAgg := p.NewPartialAggregate(kv.TiFlash, false)
		if partialAgg != nil {
			attachPlan2Task(partialAgg, mpp)
		}
		t = mpp.ConvertToRootTask(p.SCtx())
		attachPlan2Task(finalAgg, t)
		return t
	case physicalop.MppScalar:
		prop := &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.SinglePartitionType}
		if !property.NeedEnforceExchanger(mpp.GetPartitionType(), mpp.HashCols, prop, nil) {
			// On the one hand: when the low layer already satisfied the single partition layout, just do the all agg computation in the single node.
			return attach2TaskForMpp1Phase(p, mpp)
		}
		// On the other hand: try to split the mppScalar agg into multi phases agg **down** to multi nodes since data already distributed across nodes.
		// we have to check it before the content of p has been modified
		canUse3StageAgg, groupingSets := p.Scale3StageForDistinctAgg()
		proj := p.ConvertAvgForMPP()
		partialAgg, finalAgg := p.NewPartialAggregate(kv.TiFlash, true)
		if finalAgg == nil {
			return base.InvalidTask
		}

		final, middle, partial, proj4Partial, err := adjust3StagePhaseAgg(p, partialAgg, finalAgg, canUse3StageAgg, groupingSets, mpp)
		if err != nil {
			return base.InvalidTask
		}

		// partial agg proj would be null if one scalar agg cannot run in two-phase mode
		if proj4Partial != nil {
			attachPlan2Task(proj4Partial, mpp)
		}

		// partial agg would be null if one scalar agg cannot run in two-phase mode
		if partial != nil {
			attachPlan2Task(partial, mpp)
		}

		if middle != nil && canUse3StageAgg {
			items := partial.(*physicalop.PhysicalHashAgg).GroupByItems
			partitionCols := make([]*property.MPPPartitionColumn, 0, len(items))
			for _, expr := range items {
				col, ok := expr.(*expression.Column)
				if !ok {
					continue
				}
				partitionCols = append(partitionCols, &property.MPPPartitionColumn{
					Col:       col,
					CollateID: property.GetCollateIDByNameForPartition(col.GetType(ectx).GetCollate()),
				})
			}

			exProp := &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.HashType, MPPPartitionCols: partitionCols}
			newMpp := mpp.EnforceExchanger(exProp, nil)
			attachPlan2Task(middle, newMpp)
			mpp = newMpp
			if partialHashAgg, ok := partial.(*physicalop.PhysicalHashAgg); ok && len(partitionCols) != 0 {
				partialHashAgg.TiflashPreAggMode = p.SCtx().GetSessionVars().TiFlashPreAggMode
			}
		}

		// prop here still be the first generated single-partition requirement.
		newMpp := mpp.EnforceExchanger(prop, nil)
		attachPlan2Task(final, newMpp)
		if proj == nil {
			proj = physicalop.PhysicalProjection{
				Exprs: make([]expression.Expression, 0, len(p.Schema().Columns)),
			}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset())
			for _, col := range p.Schema().Columns {
				proj.Exprs = append(proj.Exprs, col)
			}
			proj.SetSchema(p.Schema())
		}
		attachPlan2Task(proj, newMpp)
		return newMpp
	default:
		return base.InvalidTask
	}
}

// attach2Task4PhysicalHashAgg implements the PhysicalPlan interface.
func attach2Task4PhysicalHashAgg(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalHashAgg)
	t := tasks[0].Copy()
	if cop, ok := t.(*physicalop.CopTask); ok {
		if len(cop.RootTaskConds) == 0 && len(cop.IdxMergePartPlans) == 0 {
			copTaskType := cop.GetStoreType()
			partialAgg, finalAgg := p.NewPartialAggregate(copTaskType, false)
			if partialAgg != nil {
				if cop.TablePlan != nil {
					cop.FinishIndexPlan()
					// the partialAgg attachment didn't follow the attachPlan2Task function, so here we actively call
					// inheritStatsFromBottomForIndexJoinInner(p, t) to inherit stats from the bottom plan for index
					// join inner side. note: partialAgg will share stats with finalAgg.
					inheritStatsFromBottomElemForIndexJoinInner(partialAgg, cop.IndexJoinInfo, cop.TablePlan.StatsInfo())
					partialAgg.SetChildren(cop.TablePlan)
					cop.TablePlan = partialAgg
					// If needExtraProj is true, a projection will be created above the PhysicalIndexLookUpReader to make sure
					// the schema is the same as the original DataSource schema.
					// However, we pushed down the agg here, the partial agg was placed on the top of tablePlan, and the final
					// agg will be placed above the PhysicalIndexLookUpReader, and the schema will be set correctly for them.
					// If we add the projection again, the projection will be between the PhysicalIndexLookUpReader and
					// the partial agg, and the schema will be broken.
					cop.NeedExtraProj = false
				} else {
					// the partialAgg attachment didn't follow the attachPlan2Task function, so here we actively call
					// inheritStatsFromBottomForIndexJoinInner(p, t) to inherit stats from the bottom plan for index
					// join inner side. note: partialAgg will share stats with finalAgg.
					inheritStatsFromBottomElemForIndexJoinInner(partialAgg, cop.IndexJoinInfo, cop.IndexPlan.StatsInfo())
					partialAgg.SetChildren(cop.IndexPlan)
					cop.IndexPlan = partialAgg
				}
			}
			// In `newPartialAggregate`, we are using stats of final aggregation as stats
			// of `partialAgg`, so the network cost of transferring result rows of `partialAgg`
			// to TiDB is normally under-estimated for hash aggregation, since the group-by
			// column may be independent of the column used for region distribution, so a closer
			// estimation of network cost for hash aggregation may multiply the number of
			// regions involved in the `partialAgg`, which is unknown however.
			t = cop.ConvertToRootTask(p.SCtx())
			attachPlan2Task(finalAgg, t)
		} else {
			t = cop.ConvertToRootTask(p.SCtx())
			attachPlan2Task(p, t)
		}
	} else if _, ok := t.(*physicalop.MppTask); ok {
		return attach2TaskForMpp(p, tasks...)
	} else {
		attachPlan2Task(p, t)
	}
	return t
}

func attach2TaskForMPP4PhysicalWindow(p *physicalop.PhysicalWindow, mpp *physicalop.MppTask) base.Task {
	// FIXME: currently, tiflash's join has different schema with TiDB,
	// so we have to rebuild the schema of join and operators which may inherit schema from join.
	// for window, we take the sub-plan's schema, and the schema generated by windowDescs.
	columns := p.Schema().Clone().Columns[len(p.Schema().Columns)-len(p.WindowFuncDescs):]
	p.SetSchema(expression.MergeSchema(mpp.Plan().Schema(), expression.NewSchema(columns...)))

	failpoint.Inject("CheckMPPWindowSchemaLength", func() {
		if len(p.Schema().Columns) != len(mpp.Plan().Schema().Columns)+len(p.WindowFuncDescs) {
			panic("mpp physical window has incorrect schema length")
		}
	})

	return attachPlan2Task(p, mpp)
}

func attach2Task4PhysicalWindow(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalWindow)
	if mpp, ok := tasks[0].Copy().(*physicalop.MppTask); ok && p.StoreTp == kv.TiFlash {
		return attach2TaskForMPP4PhysicalWindow(p, mpp)
	}
	t := tasks[0].ConvertToRootTask(p.SCtx())
	return attachPlan2Task(p.Self, t)
}

// attach2Task4PhysicalCTEStorage implements the PhysicalPlan interface.
func attach2Task4PhysicalCTEStorage(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalCTEStorage)
	t := tasks[0].Copy()
	if mpp, ok := t.(*physicalop.MppTask); ok {
		p.SetChildren(t.Plan())
		nt := physicalop.NewMppTask(p,
			mpp.GetPartitionType(), mpp.GetHashCols(),
			mpp.GetTblColHists(), mpp.GetWarnings())
		return nt
	}
	t.ConvertToRootTask(p.SCtx())
	p.SetChildren(t.Plan())
	ta := &physicalop.RootTask{}
	ta.SetPlan(p)
	ta.Warnings.CopyFrom(&t.(*physicalop.RootTask).Warnings)
	return ta
}

// attach2Task4PhysicalSequence implements PhysicalSequence.Attach2Task.
func attach2Task4PhysicalSequence(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalSequence)

	for _, t := range tasks {
		_, isMpp := t.(*physicalop.MppTask)
		if !isMpp {
			return tasks[len(tasks)-1]
		}
	}

	lastTask := tasks[len(tasks)-1].(*physicalop.MppTask)

	children := make([]base.PhysicalPlan, 0, len(tasks))
	for _, t := range tasks {
		children = append(children, t.Plan())
	}

	p.SetChildren(children...)

	mppTask := physicalop.NewMppTask(p, lastTask.GetPartitionType(), lastTask.GetHashCols(), lastTask.GetTblColHists(), nil)
	tmpWarnings := make([]*physicalop.SimpleWarnings, 0, len(tasks))
	for _, t := range tasks {
		if mpp, ok := t.(*physicalop.MppTask); ok {
			tmpWarnings = append(tmpWarnings, &mpp.Warnings)
			continue
		}
		if root, ok := t.(*physicalop.RootTask); ok {
			tmpWarnings = append(tmpWarnings, &root.Warnings)
			continue
		}
		if cop, ok := t.(*physicalop.CopTask); ok {
			tmpWarnings = append(tmpWarnings, &cop.Warnings)
		}
	}
	mppTask.Warnings.CopyFrom(tmpWarnings...)
	return mppTask
}

func collectRowSizeFromMPPPlan(mppPlan base.PhysicalPlan) (rowSize float64) {
	if mppPlan != nil && mppPlan.StatsInfo() != nil && mppPlan.StatsInfo().HistColl != nil {
		schemaCols := mppPlan.Schema().Columns
		for i, col := range schemaCols {
			if col.ID == model.ExtraCommitTSID {
				schemaCols = slices.Delete(slices.Clone(schemaCols), i, i+1)
				break
			}
		}
		return cardinality.GetAvgRowSize(mppPlan.SCtx(), mppPlan.StatsInfo().HistColl, schemaCols, false, false)
	}
	return 1 // use 1 as lower-bound for safety
}

func accumulateNetSeekCost4MPP(p base.PhysicalPlan) (cost float64) {
	if ts, ok := p.(*physicalop.PhysicalTableScan); ok {
		return float64(len(ts.Ranges)) * float64(len(ts.Columns)) * ts.SCtx().GetSessionVars().GetSeekFactor(ts.Table)
	}
	for _, c := range p.Children() {
		cost += accumulateNetSeekCost4MPP(c)
	}
	return
}
