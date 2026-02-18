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
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/baseimpl"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// attach2Task4PhysicalProjection implements PhysicalPlan interface.
func attach2Task4PhysicalProjection(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalProjection)
	t := tasks[0].Copy()
	if cop, ok := t.(*physicalop.CopTask); ok {
		if (len(cop.RootTaskConds) == 0 && len(cop.IdxMergePartPlans) == 0) && expression.CanExprsPushDown(util.GetPushDownCtx(p.SCtx()), p.Exprs, cop.GetStoreType()) {
			copTask := attachPlan2Task(p, cop)
			return copTask
		}
	} else if mpp, ok := t.(*physicalop.MppTask); ok {
		if expression.CanExprsPushDown(util.GetPushDownCtx(p.SCtx()), p.Exprs, kv.TiFlash) {
			p.SetChildren(mpp.Plan())
			mpp.SetPlan(p)
			return mpp
		}
	}
	t = t.ConvertToRootTask(p.SCtx())
	t = attachPlan2Task(p, t)
	return t
}

// attach2Task4PhysicalExpand implements PhysicalPlan interface.
func attach2Task4PhysicalExpand(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalExpand)
	t := tasks[0].Copy()
	// current expand can only be run in MPP TiFlash mode or Root Tidb mode.
	// if expr inside could not be pushed down to tiFlash, it will error in converting to pb side.
	if mpp, ok := t.(*physicalop.MppTask); ok {
		p.SetChildren(mpp.Plan())
		mpp.SetPlan(p)
		return mpp
	}
	// For root task
	// since expand should be in root side accordingly, convert to root task now.
	root := t.ConvertToRootTask(p.SCtx())
	t = attachPlan2Task(p, root)
	return t
}

func attach2MppTasks4PhysicalUnionAll(p *physicalop.PhysicalUnionAll, tasks ...base.Task) base.Task {
	t := physicalop.NewMppTask(p, property.AnyType, nil, nil, nil)
	childPlans := make([]base.PhysicalPlan, 0, len(tasks))
	for _, tk := range tasks {
		if mpp, ok := tk.(*physicalop.MppTask); ok && !tk.Invalid() {
			childPlans = append(childPlans, mpp.Plan())
			continue
		}
		return base.InvalidTask
	}
	if len(childPlans) == 0 {
		return base.InvalidTask
	}
	p.SetChildren(childPlans...)
	return t
}

// attach2Task4PhysicalUnionAll implements PhysicalPlan interface logic.
func attach2Task4PhysicalUnionAll(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalUnionAll)
	for _, t := range tasks {
		if _, ok := t.(*physicalop.MppTask); ok {
			if p.TP() == plancodec.TypePartitionUnion {
				// In attach2MppTasks(), will attach PhysicalUnion to mppTask directly.
				// But PartitionUnion cannot pushdown to tiflash, so here disable PartitionUnion pushdown to tiflash explicitly.
				// For now, return base.InvalidTask immediately, we can refine this by letting childTask of PartitionUnion convert to rootTask.
				return base.InvalidTask
			}
			return attach2MppTasks4PhysicalUnionAll(p, tasks...)
		}
	}
	t := &physicalop.RootTask{}
	t.SetPlan(p)
	childPlans := make([]base.PhysicalPlan, 0, len(tasks))
	for _, task := range tasks {
		task = task.ConvertToRootTask(p.SCtx())
		childPlans = append(childPlans, task.Plan())
	}
	p.SetChildren(childPlans...)
	return t
}

// attach2Task4PhysicalSelection implements PhysicalPlan interface.
func attach2Task4PhysicalSelection(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	sel := pp.(*physicalop.PhysicalSelection)
	if mppTask, _ := tasks[0].(*physicalop.MppTask); mppTask != nil { // always push to mpp task.
		if expression.CanExprsPushDown(util.GetPushDownCtx(sel.SCtx()), sel.Conditions, kv.TiFlash) {
			return attachPlan2Task(sel, mppTask.Copy())
		}
	}
	t := tasks[0].ConvertToRootTask(sel.SCtx())
	return attachPlan2Task(sel, t)
}

func inheritStatsFromBottomElemForIndexJoinInner(p base.PhysicalPlan, indexJoinInfo *physicalop.IndexJoinInfo, stats *property.StatsInfo) {
	var isIndexJoin bool
	switch p.(type) {
	case *physicalop.PhysicalIndexJoin, *physicalop.PhysicalIndexHashJoin, *physicalop.PhysicalIndexMergeJoin:
		isIndexJoin = true
	default:
	}
	// indexJoinInfo != nil means the child Task comes from an index join inner side.
	// !isIndexJoin means the childTask only be passed through to indexJoin as an END.
	if !isIndexJoin && indexJoinInfo != nil {
		switch p.(type) {
		case *physicalop.PhysicalSelection:
			// todo: for simplicity, we can just inherit it from child.
			// scale(1) means a cloned stats information same as the input stats.
			p.SetStats(stats.Scale(p.SCtx().GetSessionVars(), 1))
		case *physicalop.PhysicalProjection:
			// mainly about the rowEst, proj doesn't change that.
			p.SetStats(stats.Scale(p.SCtx().GetSessionVars(), 1))
		case *physicalop.PhysicalHashAgg, *physicalop.PhysicalStreamAgg:
			// todo: for simplicity, we can just inherit it from child.
			p.SetStats(stats.Scale(p.SCtx().GetSessionVars(), 1))
		case *physicalop.PhysicalUnionScan:
			// todo: for simplicity, we can just inherit it from child.
			p.SetStats(stats.Scale(p.SCtx().GetSessionVars(), 1))
		default:
			p.SetStats(stats.Scale(p.SCtx().GetSessionVars(), 1))
		}
	}
}

func inheritStatsFromBottomTaskForIndexJoinInner(p base.PhysicalPlan, t base.Task) {
	var indexJoinInfo *physicalop.IndexJoinInfo
	switch v := t.(type) {
	case *physicalop.CopTask:
		indexJoinInfo = v.IndexJoinInfo
	case *physicalop.RootTask:
		indexJoinInfo = v.IndexJoinInfo
	default:
		// index join's inner side couldn't be a mppTask, leave it.
	}
	inheritStatsFromBottomElemForIndexJoinInner(p, indexJoinInfo, t.Plan().StatsInfo())
}

// attach2Task4PhysicalStreamAgg implements PhysicalPlan interface.
func attach2Task4PhysicalStreamAgg(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalStreamAgg)
	t := tasks[0].Copy()
	if cop, ok := t.(*physicalop.CopTask); ok {
		// We should not push agg down across
		//  1. double read, since the data of second read is ordered by handle instead of index. The `extraHandleCol` is added
		//     if the double read needs to keep order. So we just use it to decided
		//     whether the following plan is double read with order reserved.
		//  2. the case that there's filters should be calculated on TiDB side.
		//  3. the case of index merge
		if (cop.IndexPlan != nil && cop.TablePlan != nil && cop.KeepOrder) || len(cop.RootTaskConds) > 0 || len(cop.IdxMergePartPlans) > 0 {
			t = cop.ConvertToRootTask(p.SCtx())
			attachPlan2Task(p, t)
		} else {
			storeType := cop.GetStoreType()
			// TiFlash doesn't support Stream Aggregation
			if storeType == kv.TiFlash && len(p.GroupByItems) > 0 {
				return base.InvalidTask
			}
			partialAgg, finalAgg := p.NewPartialAggregate(storeType, false)
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
			// COP Task -> Root Task, warnings inherited inside.
			t = cop.ConvertToRootTask(p.SCtx())
			attachPlan2Task(finalAgg, t)
		}
	} else if mpp, ok := t.(*physicalop.MppTask); ok {
		t = mpp.ConvertToRootTask(p.SCtx())
		attachPlan2Task(p, t)
	} else {
		attachPlan2Task(p, t)
	}
	return t
}

func attach2TaskForMpp1Phase(p *physicalop.PhysicalHashAgg, mpp *physicalop.MppTask) base.Task {
	// 1-phase agg: when the partition columns can be satisfied, where the plan does not need to enforce Exchange
	// only push down the original agg
	proj := p.ConvertAvgForMPP()
	attachPlan2Task(p.Self, mpp)
	if proj != nil {
		attachPlan2Task(proj, mpp)
	}
	return mpp
}

// scaleStats4GroupingSets scale the derived stats because the lower source has been expanded.
//
//	 parent OP   <- logicalAgg   <- children OP    (derived stats)
//	                    ｜
//	                    v
//	parent OP   <-  physicalAgg  <- children OP    (stats  used)
//	                    |
//	         +----------+----------+----------+
//	       Final       Mid     Partial    Expand
//
// physical agg stats is reasonable from the whole, because expand operator is designed to facilitate
// the Mid and Partial Agg, which means when leaving the Final, its output rowcount could be exactly
// the same as what it derived(estimated) before entering physical optimization phase.
//
// From the cost model correctness, for these inserted sub-agg and even expand operator, we should
// recompute the stats for them particularly.
//
// for example: grouping sets {<a>},{<b>}, group by items {a,b,c,groupingID}
// after expand:
//
//	 a,   b,   c,  groupingID
//	...  null  c    1   ---+
//	...  null  c    1      +------- replica group 1
//	...  null  c    1   ---+
//	null  ...  c    2   ---+
//	null  ...  c    2      +------- replica group 2
//	null  ...  c    2   ---+
//
// since null value is seen the same when grouping data (groupingID in one replica is always the same):
//   - so the num of group in replica 1 is equal to NDV(a,c)
//   - so the num of group in replica 2 is equal to NDV(b,c)
//
// in a summary, the total num of group of all replica is equal to = Σ:NDV(each-grouping-set-cols, normal-group-cols)
func scaleStats4GroupingSets(p *physicalop.PhysicalHashAgg, groupingSets expression.GroupingSets, groupingIDCol *expression.Column,
	childSchema *expression.Schema, childStats *property.StatsInfo) {
	idSets := groupingSets.AllSetsColIDs()
	normalGbyCols := make([]*expression.Column, 0, len(p.GroupByItems))
	for _, gbyExpr := range p.GroupByItems {
		cols := expression.ExtractColumns(gbyExpr)
		for _, col := range cols {
			if !idSets.Has(int(col.UniqueID)) && col.UniqueID != groupingIDCol.UniqueID {
				normalGbyCols = append(normalGbyCols, col)
			}
		}
	}
	sumNDV := float64(0)
	groupingSetCols := make([]*expression.Column, 0, 4)
	for _, groupingSet := range groupingSets {
		// for every grouping set, pick its cols out, and combine with normal group cols to get the ndv.
		groupingSetCols = groupingSet.ExtractCols(groupingSetCols)
		groupingSetCols = append(groupingSetCols, normalGbyCols...)
		ndv, _ := cardinality.EstimateColsNDVWithMatchedLen(p.SCtx(), groupingSetCols, childSchema, childStats)
		groupingSetCols = groupingSetCols[:0]
		sumNDV += ndv
	}
	// After group operator, all same rows are grouped into one row, that means all
	// change the sub-agg's stats
	if p.StatsInfo() != nil {
		// equivalence to a new cloned one. (cause finalAgg and partialAgg may share a same copy of stats)
		cpStats := p.StatsInfo().Scale(p.SCtx().GetSessionVars(), 1)
		cpStats.RowCount = sumNDV
		// We cannot estimate the ColNDVs for every output, so we use a conservative strategy.
		for k := range cpStats.ColNDVs {
			cpStats.ColNDVs[k] = sumNDV
		}
		// for old groupNDV, if it's containing one more grouping set cols, just plus the NDV where the col is excluded.
		// for example: old grouping NDV(b,c), where b is in grouping sets {<a>},{<b>}. so when countering the new NDV:
		// cases:
		// new grouping NDV(b,c) := old NDV(b,c) + NDV(null, c) = old NDV(b,c) + DNV(c).
		// new grouping NDV(a,b,c) := old NDV(a,b,c) + NDV(null,b,c) + NDV(a,null,c) = old NDV(a,b,c) + NDV(b,c) + NDV(a,c)
		allGroupingSetsIDs := groupingSets.AllSetsColIDs()
		for _, oneGNDV := range cpStats.GroupNDVs {
			newGNDV := oneGNDV.NDV
			intersectionIDs := make([]int64, 0, len(oneGNDV.Cols))
			for i, id := range oneGNDV.Cols {
				if allGroupingSetsIDs.Has(int(id)) {
					// when meet an id in grouping sets, skip it (cause its null) and append the rest ids to count the incrementNDV.
					beforeLen := len(intersectionIDs)
					intersectionIDs = append(intersectionIDs, oneGNDV.Cols[i:]...)
					incrementNDV, _ := cardinality.EstimateColsDNVWithMatchedLenFromUniqueIDs(
						p.SCtx(), intersectionIDs, childSchema, childStats)
					newGNDV += incrementNDV
					// restore the before intersectionIDs slice.
					intersectionIDs = intersectionIDs[:beforeLen]
				}
				// insert ids one by one.
				intersectionIDs = append(intersectionIDs, id)
			}
			oneGNDV.NDV = newGNDV
		}
		p.SetStats(cpStats)
	}
}

// adjust3StagePhaseAgg generate 3 stage aggregation for single/multi count distinct if applicable.
//
//	select count(distinct a), count(b) from foo
//
// will generate plan:
//
//	HashAgg sum(#1), sum(#2)                              -> final agg
//	 +- Exchange Passthrough
//	     +- HashAgg count(distinct a) #1, sum(#3) #2      -> middle agg
//	         +- Exchange HashPartition by a
//	             +- HashAgg count(b) #3, group by a       -> partial agg
//	                 +- TableScan foo
//
//	select count(distinct a), count(distinct b), count(c) from foo
//
// will generate plan:
//
//	HashAgg sum(#1), sum(#2), sum(#3)                                           -> final agg
//	 +- Exchange Passthrough
//	     +- HashAgg count(distinct a) #1, count(distinct b) #2, sum(#4) #3      -> middle agg
//	         +- Exchange HashPartition by a,b,groupingID
//	             +- HashAgg count(c) #4, group by a,b,groupingID                -> partial agg
//	                 +- Expand {<a>}, {<b>}                                     -> expand
//	                     +- TableScan foo
func adjust3StagePhaseAgg(p *physicalop.PhysicalHashAgg, partialAgg, finalAgg base.PhysicalPlan, canUse3StageAgg bool,
	groupingSets expression.GroupingSets, mpp *physicalop.MppTask) (final, mid, part, proj4Part base.PhysicalPlan, _ error) {
	ectx := p.SCtx().GetExprCtx().GetEvalCtx()

	if !(partialAgg != nil && canUse3StageAgg) {
		// quick path: return the original finalAgg and partiAgg.
		return finalAgg, nil, partialAgg, nil, nil
	}
	if len(groupingSets) == 0 {
		// single distinct agg mode.
		clonedAgg, err := finalAgg.Clone(p.SCtx())
		if err != nil {
			return nil, nil, nil, nil, err
		}

		// step1: adjust middle agg.
		middleHashAgg := clonedAgg.(*physicalop.PhysicalHashAgg)
		distinctPos := 0
		middleSchema := expression.NewSchema()
		schemaMap := make(map[int64]*expression.Column, len(middleHashAgg.AggFuncs))
		for i, fun := range middleHashAgg.AggFuncs {
			col := &expression.Column{
				UniqueID: p.SCtx().GetSessionVars().AllocPlanColumnID(),
				RetType:  fun.RetTp,
			}
			if fun.HasDistinct {
				distinctPos = i
				fun.Mode = aggregation.Partial1Mode
			} else {
				fun.Mode = aggregation.Partial2Mode
				originalCol := fun.Args[0].(*expression.Column)
				// mapping the current partial output column with the agg origin arg column. (final agg arg should use this one)
				schemaMap[originalCol.UniqueID] = col
			}
			middleSchema.Append(col)
		}
		middleHashAgg.SetSchema(middleSchema)

		// step2: adjust final agg.
		finalHashAgg := finalAgg.(*physicalop.PhysicalHashAgg)
		finalAggDescs := make([]*aggregation.AggFuncDesc, 0, len(finalHashAgg.AggFuncs))
		for i, fun := range finalHashAgg.AggFuncs {
			newArgs := make([]expression.Expression, 0, 1)
			if distinctPos == i {
				// change count(distinct) to sum()
				fun.Name = ast.AggFuncSum
				fun.HasDistinct = false
				newArgs = append(newArgs, middleSchema.Columns[i])
			} else {
				for _, arg := range fun.Args {
					newCol, err := arg.RemapColumn(schemaMap)
					if err != nil {
						return nil, nil, nil, nil, err
					}
					newArgs = append(newArgs, newCol)
				}
			}
			fun.Mode = aggregation.FinalMode
			fun.Args = newArgs
			finalAggDescs = append(finalAggDescs, fun)
		}
		finalHashAgg.AggFuncs = finalAggDescs
		// partialAgg is im-mutated from args.
		return finalHashAgg, middleHashAgg, partialAgg, nil, nil
	}
	// multi distinct agg mode, having grouping sets.
	// set the default expression to constant 1 for the convenience to choose default group set data.
	var groupingIDCol expression.Expression
	// enforce Expand operator above the children.
	// physical plan is enumerated without children from itself, use mpp subtree instead p.children.
	// scale(len(groupingSets)) will change the NDV, while Expand doesn't change the NDV and groupNDV.
	stats := mpp.Plan().StatsInfo().Scale(p.SCtx().GetSessionVars(), float64(1))
	stats.RowCount = stats.RowCount * float64(len(groupingSets))
	physicalExpand := physicalop.PhysicalExpand{
		GroupingSets: groupingSets,
	}.Init(p.SCtx(), stats, mpp.Plan().QueryBlockOffset())
	// generate a new column as groupingID to identify which this row is targeting for.
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.SetFlag(mysql.UnsignedFlag | mysql.NotNullFlag)
	groupingIDCol = &expression.Column{
		UniqueID: p.SCtx().GetSessionVars().AllocPlanColumnID(),
		RetType:  tp,
	}
	// append the physical expand op with groupingID column.
	physicalExpand.SetSchema(mpp.Plan().Schema().Clone())
	physicalExpand.Schema().Append(groupingIDCol.(*expression.Column))
	physicalExpand.GroupingIDCol = groupingIDCol.(*expression.Column)
	// attach PhysicalExpand to mpp
	attachPlan2Task(physicalExpand, mpp)

	// having group sets
	clonedAgg, err := finalAgg.Clone(p.SCtx())
	if err != nil {
		return nil, nil, nil, nil, err
	}
	cloneHashAgg := clonedAgg.(*physicalop.PhysicalHashAgg)
	// Clone(), it will share same base-plan elements from the finalAgg, including id,tp,stats. Make a new one here.
	cloneHashAgg.Plan = baseimpl.NewBasePlan(cloneHashAgg.SCtx(), cloneHashAgg.TP(), cloneHashAgg.QueryBlockOffset())
	cloneHashAgg.SetStats(finalAgg.StatsInfo()) // reuse the final agg stats here.

	// step1: adjust partial agg, for normal agg here, adjust it to target for specified group data.
	// Since we may substitute the first arg of normal agg with case-when expression here, append a
	// customized proj here rather than depending on postOptimize to insert a blunt one for us.
	//
	// proj4Partial output all the base col from lower op + caseWhen proj cols.
	proj4Partial := new(physicalop.PhysicalProjection).Init(p.SCtx(), mpp.Plan().StatsInfo(), mpp.Plan().QueryBlockOffset())
	for _, col := range mpp.Plan().Schema().Columns {
		proj4Partial.Exprs = append(proj4Partial.Exprs, col)
	}
	proj4Partial.SetSchema(mpp.Plan().Schema().Clone())

	partialHashAgg := partialAgg.(*physicalop.PhysicalHashAgg)
	partialHashAgg.GroupByItems = append(partialHashAgg.GroupByItems, groupingIDCol)
	partialHashAgg.Schema().Append(groupingIDCol.(*expression.Column))
	// it will create a new stats for partial agg.
	scaleStats4GroupingSets(partialHashAgg, groupingSets, groupingIDCol.(*expression.Column), proj4Partial.Schema(), proj4Partial.StatsInfo())
	for _, fun := range partialHashAgg.AggFuncs {
		if !fun.HasDistinct {
			// for normal agg phase1, we should also modify them to target for specified group data.
			// Expr = (case when groupingID = targeted_groupingID then arg else null end)
			eqExpr := expression.NewFunctionInternal(p.SCtx().GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), groupingIDCol, expression.NewUInt64Const(fun.GroupingID))
			caseWhen := expression.NewFunctionInternal(p.SCtx().GetExprCtx(), ast.Case, fun.Args[0].GetType(ectx), eqExpr, fun.Args[0], expression.NewNull())
			caseWhenProjCol := &expression.Column{
				UniqueID: p.SCtx().GetSessionVars().AllocPlanColumnID(),
				RetType:  fun.Args[0].GetType(ectx),
			}
			proj4Partial.Exprs = append(proj4Partial.Exprs, caseWhen)
			proj4Partial.Schema().Append(caseWhenProjCol)
			fun.Args[0] = caseWhenProjCol
		}
	}

	// step2: adjust middle agg
	// middleHashAgg shared the same stats with the final agg does.
	middleHashAgg := cloneHashAgg
	middleSchema := expression.NewSchema()
	schemaMap := make(map[int64]*expression.Column, len(middleHashAgg.AggFuncs))
	for _, fun := range middleHashAgg.AggFuncs {
		col := &expression.Column{
			UniqueID: p.SCtx().GetSessionVars().AllocPlanColumnID(),
			RetType:  fun.RetTp,
		}
		if fun.HasDistinct {
			// let count distinct agg aggregate on whole-scope data rather using case-when expr to target on specified group. (agg null strict attribute)
			fun.Mode = aggregation.Partial1Mode
		} else {
			fun.Mode = aggregation.Partial2Mode
			originalCol := fun.Args[0].(*expression.Column)
			// record the origin column unique id down before change it to be case when expr.
			// mapping the current partial output column with the agg origin arg column. (final agg arg should use this one)
			schemaMap[originalCol.UniqueID] = col
		}
		middleSchema.Append(col)
	}
	middleHashAgg.SetSchema(middleSchema)

	// step3: adjust final agg
	finalHashAgg := finalAgg.(*physicalop.PhysicalHashAgg)
	finalAggDescs := make([]*aggregation.AggFuncDesc, 0, len(finalHashAgg.AggFuncs))
	for i, fun := range finalHashAgg.AggFuncs {
		newArgs := make([]expression.Expression, 0, 1)
		if fun.HasDistinct {
			// change count(distinct) agg to sum()
			fun.Name = ast.AggFuncSum
			fun.HasDistinct = false
			// count(distinct a,b) -> become a single partial result col.
			newArgs = append(newArgs, middleSchema.Columns[i])
		} else {
			// remap final normal agg args to be output schema of middle normal agg.
			for _, arg := range fun.Args {
				newCol, err := arg.RemapColumn(schemaMap)
				if err != nil {
					return nil, nil, nil, nil, err
				}
				newArgs = append(newArgs, newCol)
			}
		}
		fun.Mode = aggregation.FinalMode
		fun.Args = newArgs
		fun.GroupingID = 0
		finalAggDescs = append(finalAggDescs, fun)
	}
	finalHashAgg.AggFuncs = finalAggDescs
	return finalHashAgg, middleHashAgg, partialHashAgg, proj4Partial, nil
}

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
