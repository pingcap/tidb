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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/paging"
	"github.com/pingcap/tipb/go-tipb"
)

// calcPagingCost calculates the cost for paging processing which may increase the seekCnt and reduce scanned rows.
func calcPagingCost(ctx base.PlanContext, indexPlan base.PhysicalPlan, expectCnt uint64) float64 {
	sessVars := ctx.GetSessionVars()
	indexRows := indexPlan.StatsCount()
	sourceRows := extractRows(indexPlan)
	// with paging, the scanned rows is always less than or equal to source rows.
	if uint64(sourceRows) < expectCnt {
		expectCnt = uint64(sourceRows)
	}
	seekCnt := paging.CalculateSeekCnt(expectCnt)
	indexSelectivity := float64(1)
	if sourceRows > indexRows {
		indexSelectivity = indexRows / sourceRows
	}
	pagingCst := seekCnt*sessVars.GetSeekFactor(nil) + float64(expectCnt)*sessVars.GetCPUFactor()
	pagingCst *= indexSelectivity

	// we want the diff between idxCst and pagingCst here,
	// however, the idxCst does not contain seekFactor, so a seekFactor needs to be removed
	return math.Max(pagingCst-sessVars.GetSeekFactor(nil), 0)
}

// attach2Task4PhysicalLimit attach limit to different cases.
// For Normal Index Lookup
// 1: attach the limit to table side or index side of normal index lookup cop task. (normal case, old code, no more
// explanation here)
//
// For Index Merge:
// 2: attach the limit to **table** side for index merge intersection case, cause intersection will invalidate the
// fetched limit+offset rows from each partial index plan, you can not decide how many you want in advance for partial
// index path, actually. After we sink limit to table side, we still need an upper root limit to control the real limit
// count admission.
//
// 3: attach the limit to **index** side for index merge union case, because each index plan will output the fetched
// limit+offset (* N path) rows, you still need an embedded pushedLimit inside index merge reader to cut it down.
//
// 4: attach the limit to the TOP of root index merge operator if there is some root condition exists for index merge
// intersection/union case.
func attach2Task4PhysicalLimit(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalLimit)
	t := tasks[0].Copy()
	newPartitionBy := make([]property.SortItem, 0, len(p.GetPartitionBy()))
	for _, expr := range p.GetPartitionBy() {
		newPartitionBy = append(newPartitionBy, expr.Clone())
	}

	sunk := false
	if cop, ok := t.(*physicalop.CopTask); ok {
		suspendLimitAboveTablePlan := func() {
			newCount := p.Offset + p.Count
			childProfile := cop.TablePlan.StatsInfo()
			// but "regionNum" is unknown since the copTask can be a double read, so we ignore it now.
			stats := property.DeriveLimitStats(childProfile, float64(newCount))
			pushedDownLimit := physicalop.PhysicalLimit{PartitionBy: newPartitionBy, Count: newCount}.Init(p.SCtx(), stats, p.QueryBlockOffset())
			pushedDownLimit.SetChildren(cop.TablePlan)
			cop.TablePlan = pushedDownLimit
			// Don't use clone() so that Limit and its children share the same schema. Otherwise, the virtual generated column may not be resolved right.
			pushedDownLimit.SetSchema(pushedDownLimit.Children()[0].Schema())
			t = cop.ConvertToRootTask(p.SCtx())
		}
		if len(cop.IdxMergePartPlans) == 0 {
			// For double read which requires order being kept, the limit cannot be pushed down to the table side,
			// because handles would be reordered before being sent to table scan.
			if (!cop.KeepOrder || !cop.IndexPlanFinished || cop.IndexPlan == nil) && len(cop.RootTaskConds) == 0 {
				// When limit is pushed down, we should remove its offset.
				newCount := p.Offset + p.Count
				childProfile := cop.Plan().StatsInfo()
				// Strictly speaking, for the row count of stats, we should multiply newCount with "regionNum",
				// but "regionNum" is unknown since the copTask can be a double read, so we ignore it now.
				stats := property.DeriveLimitStats(childProfile, float64(newCount))
				pushedDownLimit := physicalop.PhysicalLimit{PartitionBy: newPartitionBy, Count: newCount}.Init(p.SCtx(), stats, p.QueryBlockOffset())
				cop = attachPlan2Task(pushedDownLimit, cop).(*physicalop.CopTask)
				// Don't use clone() so that Limit and its children share the same schema. Otherwise the virtual generated column may not be resolved right.
				pushedDownLimit.SetSchema(pushedDownLimit.Children()[0].Schema())
			}
			t = cop.ConvertToRootTask(p.SCtx())
			sunk = sinkIntoIndexLookUp(p, t)
		} else if !cop.IdxMergeIsIntersection {
			// We only support push part of the order prop down to index merge build case.
			if len(cop.RootTaskConds) == 0 {
				// For double read which requires order being kept, the limit cannot be pushed down to the table side,
				// because handles would be reordered before being sent to table scan.
				if cop.IndexPlanFinished && !cop.KeepOrder {
					// when the index plan is finished and index plan is not ordered, sink the limit to the index merge table side.
					suspendLimitAboveTablePlan()
				} else if !cop.IndexPlanFinished {
					// cop.IndexPlanFinished = false indicates the table side is a pure table-scan, sink the limit to the index merge index side.
					newCount := p.Offset + p.Count
					limitChildren := make([]base.PhysicalPlan, 0, len(cop.IdxMergePartPlans))
					for _, partialScan := range cop.IdxMergePartPlans {
						childProfile := partialScan.StatsInfo()
						stats := property.DeriveLimitStats(childProfile, float64(newCount))
						pushedDownLimit := physicalop.PhysicalLimit{PartitionBy: newPartitionBy, Count: newCount}.Init(p.SCtx(), stats, p.QueryBlockOffset())
						pushedDownLimit.SetChildren(partialScan)
						pushedDownLimit.SetSchema(pushedDownLimit.Children()[0].Schema())
						limitChildren = append(limitChildren, pushedDownLimit)
					}
					cop.IdxMergePartPlans = limitChildren
					t = cop.ConvertToRootTask(p.SCtx())
					sunk = sinkIntoIndexMerge(p, t)
				} else {
					// when there are some limitations, just sink the limit upon the index merge reader.
					t = cop.ConvertToRootTask(p.SCtx())
					sunk = sinkIntoIndexMerge(p, t)
				}
			} else {
				// when there are some root conditions, just sink the limit upon the index merge reader.
				t = cop.ConvertToRootTask(p.SCtx())
				sunk = sinkIntoIndexMerge(p, t)
			}
		} else if cop.IdxMergeIsIntersection {
			// In the index merge with intersection case, only the limit can be pushed down to the index merge table side.
			// Note Difference:
			// IndexMerge.PushedLimit is applied before table scan fetching, limiting the indexPartialPlan rows returned (it maybe ordered if orderBy items not empty)
			// TableProbeSide sink limit is applied on the top of table plan, which will quickly shut down the both fetch-back and read-back process.
			if len(cop.RootTaskConds) == 0 {
				if cop.IndexPlanFinished {
					// indicates the table side is not a pure table-scan, so we could only append the limit upon the table plan.
					suspendLimitAboveTablePlan()
				} else {
					t = cop.ConvertToRootTask(p.SCtx())
					sunk = sinkIntoIndexMerge(p, t)
				}
			} else {
				// Otherwise, suspend the limit out of index merge reader.
				t = cop.ConvertToRootTask(p.SCtx())
				sunk = sinkIntoIndexMerge(p, t)
			}
		} else {
			// Whatever the remained case is, we directly convert to it to root task.
			t = cop.ConvertToRootTask(p.SCtx())
		}
	} else if mpp, ok := t.(*physicalop.MppTask); ok {
		newCount := p.Offset + p.Count
		childProfile := mpp.Plan().StatsInfo()
		stats := property.DeriveLimitStats(childProfile, float64(newCount))
		pushedDownLimit := physicalop.PhysicalLimit{Count: newCount, PartitionBy: newPartitionBy}.Init(p.SCtx(), stats, p.QueryBlockOffset())
		mpp = attachPlan2Task(pushedDownLimit, mpp).(*physicalop.MppTask)
		pushedDownLimit.SetSchema(pushedDownLimit.Children()[0].Schema())
		t = mpp.ConvertToRootTask(p.SCtx())
	}
	if sunk {
		return t
	}
	// Skip limit with partition on the root. This is a derived topN and window function
	// will take care of the filter.
	if len(p.GetPartitionBy()) > 0 {
		return t
	}
	return attachPlan2Task(p, t)
}

func sinkIntoIndexLookUp(p *physicalop.PhysicalLimit, t base.Task) bool {
	root := t.(*physicalop.RootTask)
	reader, isDoubleRead := root.GetPlan().(*physicalop.PhysicalIndexLookUpReader)
	proj, isProj := root.GetPlan().(*physicalop.PhysicalProjection)
	if !isDoubleRead && !isProj {
		return false
	}
	if isProj {
		reader, isDoubleRead = proj.Children()[0].(*physicalop.PhysicalIndexLookUpReader)
		if !isDoubleRead {
			return false
		}
	}

	// We can sink Limit into IndexLookUpReader only if tablePlan contains no Selection.
	ts, isTableScan := reader.TablePlan.(*physicalop.PhysicalTableScan)
	if !isTableScan {
		return false
	}

	// If this happens, some Projection Operator must be inlined into this Limit. (issues/14428)
	// For example, if the original plan is `IndexLookUp(col1, col2) -> Limit(col1, col2) -> Project(col1)`,
	//  then after inlining the Project, it will be `IndexLookUp(col1, col2) -> Limit(col1)` here.
	// If the Limit is sunk into the IndexLookUp, the IndexLookUp's schema needs to be updated as well,
	// So we add an extra projection to solve the problem.
	if p.Schema().Len() != reader.Schema().Len() {
		extraProj := physicalop.PhysicalProjection{
			Exprs: expression.Column2Exprs(p.Schema().Columns),
		}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), nil)
		extraProj.SetSchema(p.Schema())
		// If the root.p is already a Projection. We left the optimization for the later Projection Elimination.
		extraProj.SetChildren(root.GetPlan())
		root.SetPlan(extraProj)
	}

	reader.PushedLimit = &physicalop.PushedDownLimit{
		Offset: p.Offset,
		Count:  p.Count,
	}
	if originStats := ts.StatsInfo(); originStats.RowCount >= p.StatsInfo().RowCount {
		// Only reset the table scan stats when its row estimation is larger than the limit count.
		// When indexLookUp push down is enabled, some rows have been looked up in TiKV side,
		// and the rows processed by the TiDB table scan may be less than the limit count.
		ts.SetStats(p.StatsInfo())
		if originStats != nil {
			// keep the original stats version
			ts.StatsInfo().StatsVersion = originStats.StatsVersion
		}
	}
	reader.SetStats(p.StatsInfo())
	if isProj {
		proj.SetStats(p.StatsInfo())
	}
	return true
}

func sinkIntoIndexMerge(p *physicalop.PhysicalLimit, t base.Task) bool {
	root := t.(*physicalop.RootTask)
	imReader, isIm := root.GetPlan().(*physicalop.PhysicalIndexMergeReader)
	proj, isProj := root.GetPlan().(*physicalop.PhysicalProjection)
	if !isIm && !isProj {
		return false
	}
	if isProj {
		imReader, isIm = proj.Children()[0].(*physicalop.PhysicalIndexMergeReader)
		if !isIm {
			return false
		}
	}
	ts, ok := imReader.TablePlan.(*physicalop.PhysicalTableScan)
	if !ok {
		return false
	}
	imReader.PushedLimit = &physicalop.PushedDownLimit{
		Count:  p.Count,
		Offset: p.Offset,
	}
	// since ts.statsInfo.rowcount may dramatically smaller than limit.statsInfo.
	// like limit: rowcount=1
	//      ts:    rowcount=0.0025
	originStats := ts.StatsInfo()
	if originStats != nil {
		// keep the original stats version
		ts.StatsInfo().StatsVersion = originStats.StatsVersion
		if originStats.RowCount < p.StatsInfo().RowCount {
			ts.StatsInfo().RowCount = originStats.RowCount
		}
	}
	needProj := p.Schema().Len() != root.GetPlan().Schema().Len()
	if !needProj {
		for i := range p.Schema().Len() {
			if !p.Schema().Columns[i].EqualColumn(root.GetPlan().Schema().Columns[i]) {
				needProj = true
				break
			}
		}
	}
	if needProj {
		extraProj := physicalop.PhysicalProjection{
			Exprs: expression.Column2Exprs(p.Schema().Columns),
		}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), nil)
		extraProj.SetSchema(p.Schema())
		// If the root.p is already a Projection. We left the optimization for the later Projection Elimination.
		extraProj.SetChildren(root.GetPlan())
		root.SetPlan(extraProj)
	}
	return true
}

// attach2Task4PhysicalSort is basic logic of Attach2Task which implements PhysicalPlan interface.
func attach2Task4PhysicalSort(p base.PhysicalPlan, tasks ...base.Task) base.Task {
	intest.Assert(p.(*physicalop.PhysicalSort) != nil)
	t := tasks[0].Copy()
	t = attachPlan2Task(p, t)
	return t
}

// attach2Task4NominalSort implements PhysicalPlan interface.
func attach2Task4NominalSort(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.NominalSort)
	if p.OnlyColumn {
		return tasks[0]
	}
	t := tasks[0].Copy()
	t = attachPlan2Task(p, t)
	return t
}

func getPushedDownTopN(p *physicalop.PhysicalTopN, childPlan base.PhysicalPlan, storeTp kv.StoreType) (topN, newGlobalTopN *physicalop.PhysicalTopN) {
	fixValue := fixcontrol.GetBoolWithDefault(p.SCtx().GetSessionVars().GetOptimizerFixControlMap(), fixcontrol.Fix56318, true)
	// HeavyFunctionOptimize: if TopN's ByItems is a HeavyFunction (currently mainly for Vector Search), we will change
	// the ByItems in order to reuse the function result.
	byItemIndex := make([]int, 0)
	for i, byItem := range p.ByItems {
		if ContainHeavyFunction(byItem.Expr) {
			byItemIndex = append(byItemIndex, i)
		}
	}
	if fixValue && len(byItemIndex) > 0 {
		x, err := p.Clone(p.SCtx())
		if err != nil {
			return nil, nil
		}
		newGlobalTopN = x.(*physicalop.PhysicalTopN)
		// the projecton's construction cannot be create if the AllowProjectionPushDown is disable.
		if storeTp == kv.TiKV && !p.SCtx().GetSessionVars().AllowProjectionPushDown {
			newGlobalTopN = nil
		}
	}
	newByItems := make([]*util.ByItems, 0, len(p.ByItems))
	for _, expr := range p.ByItems {
		newByItems = append(newByItems, expr.Clone())
	}
	newPartitionBy := make([]property.SortItem, 0, len(p.GetPartitionBy()))
	for _, expr := range p.GetPartitionBy() {
		newPartitionBy = append(newPartitionBy, expr.Clone())
	}
	newCount := p.Offset + p.Count
	childProfile := childPlan.StatsInfo()
	// Strictly speaking, for the row count of pushed down TopN, we should multiply newCount with "regionNum",
	// but "regionNum" is unknown since the copTask can be a double read, so we ignore it now.
	stats := property.DeriveLimitStats(childProfile, float64(newCount))

	// Add a extra physicalProjection to save the distance column, a example like :
	// select id from t order by vec_distance(vec, '[1,2,3]') limit x
	// The Plan will be modified like:
	//
	// Original: DataSource(id, vec) -> TopN(by vec->dis) -> Projection(id)
	//                                  └─Byitem: vec_distance(vec, '[1,2,3]')
	//
	// New:      DataSource(id, vec) -> Projection(id, vec->dis) -> TopN(by dis) -> Projection(id)
	//                                  └─Byitem: dis
	//
	// Note that for plan now, TopN has its own schema and does not use the schema of children.
	if newGlobalTopN != nil {
		// create a new PhysicalProjection to calculate the distance columns, and add it into plan route
		bottomProjSchemaCols := make([]*expression.Column, 0, len(childPlan.Schema().Columns))
		bottomProjExprs := make([]expression.Expression, 0, len(childPlan.Schema().Columns))
		for _, col := range newGlobalTopN.Schema().Columns {
			newCol := col.Clone().(*expression.Column)
			bottomProjSchemaCols = append(bottomProjSchemaCols, newCol)
			bottomProjExprs = append(bottomProjExprs, newCol)
		}
		type DistanceColItem struct {
			Index       int
			DistanceCol *expression.Column
		}
		distanceCols := make([]DistanceColItem, 0)
		for _, idx := range byItemIndex {
			bottomProjExprs = append(bottomProjExprs, newGlobalTopN.ByItems[idx].Expr)
			distanceCol := &expression.Column{
				UniqueID: newGlobalTopN.SCtx().GetSessionVars().AllocPlanColumnID(),
				RetType:  newGlobalTopN.ByItems[idx].Expr.GetType(p.SCtx().GetExprCtx().GetEvalCtx()),
			}
			distanceCols = append(distanceCols, DistanceColItem{
				Index:       idx,
				DistanceCol: distanceCol,
			})
		}
		for _, dis := range distanceCols {
			bottomProjSchemaCols = append(bottomProjSchemaCols, dis.DistanceCol)
		}

		bottomProj := physicalop.PhysicalProjection{
			Exprs: bottomProjExprs,
		}.Init(p.SCtx(), stats, p.QueryBlockOffset(), p.GetChildReqProps(0))
		bottomProj.SetSchema(expression.NewSchema(bottomProjSchemaCols...))
		bottomProj.SetChildren(childPlan)

		topN := physicalop.PhysicalTopN{
			ByItems:     newByItems,
			PartitionBy: newPartitionBy,
			Count:       newCount,
		}.Init(p.SCtx(), stats, p.QueryBlockOffset(), p.GetChildReqProps(0))
		// mppTask's topN
		for _, item := range distanceCols {
			topN.ByItems[item.Index].Expr = item.DistanceCol
		}

		// rootTask's topn, need reuse the distance col
		for _, expr := range distanceCols {
			newGlobalTopN.ByItems[expr.Index].Expr = expr.DistanceCol
		}
		topN.SetChildren(bottomProj)

		// orderByCol is the column `distanceCol`, so this explain always success.
		orderByCol, _ := topN.ByItems[0].Expr.(*expression.Column)
		orderByCol.Index = len(bottomProj.Exprs) - 1

		// try to Check and modify plan when it is possible to not scanning vector column at all.
		tryReturnDistanceFromIndex(topN, newGlobalTopN, childPlan, bottomProj)

		return topN, newGlobalTopN
	}

	topN = physicalop.PhysicalTopN{
		ByItems:     newByItems,
		PartitionBy: newPartitionBy,
		Count:       newCount,
	}.Init(p.SCtx(), stats, p.QueryBlockOffset(), p.GetChildReqProps(0))
	topN.SetChildren(childPlan)
	return topN, newGlobalTopN
}

// tryReturnDistanceFromIndex checks whether the vector in the plan can be removed and a distance column will be added.
// Consider this situation sql statement: select id from t order by vec_distance(vec, '[1,2,3]') limit x
// The plan like:
//
// DataSource(id, vec) -> Projection1(id, vec->dis) -> TopN(by dis) -> Projection2(id)
// └─Schema: id, vec
//
// In vector index, the distance result already exists, so there is no need to calculate it again in projection1.
// We can directly read the distance result. After this Optimization, the plan will be modified to:
//
// DataSource(id, dis) -> TopN(by dis) -> Projection2(id)
// └─Schema: id, dis
func tryReturnDistanceFromIndex(local, global *physicalop.PhysicalTopN, childPlan base.PhysicalPlan, proj *physicalop.PhysicalProjection) bool {
	tableScan, ok := childPlan.(*physicalop.PhysicalTableScan)
	if !ok {
		return false
	}

	orderByCol, _ := local.ByItems[0].Expr.(*expression.Column)
	var annQueryInfo *physicalop.ColumnarIndexExtra
	for _, idx := range tableScan.UsedColumnarIndexes {
		if idx != nil && idx.QueryInfo.IndexType == tipb.ColumnarIndexType_TypeVector && idx.QueryInfo != nil {
			annQueryInfo = idx
			break
		}
	}
	if annQueryInfo == nil {
		return false
	}

	// If the vector column is only used in the VectorSearch and no where
	// else, then it can be eliminated in TableScan.
	if orderByCol.Index < 0 || orderByCol.Index >= len(proj.Exprs) {
		return false
	}

	isVecColumnInUse := false
	for idx, projExpr := range proj.Exprs {
		if idx == orderByCol.Index {
			// Skip the distance function projection itself.
			continue
		}
		flag := expression.HasColumnWithCondition(projExpr, func(col *expression.Column) bool {
			return col.ID == annQueryInfo.QueryInfo.GetAnnQueryInfo().GetColumn().ColumnId
		})
		if flag {
			isVecColumnInUse = true
			break
		}
	}

	if isVecColumnInUse {
		return false
	}

	// append distance column to the table scan
	virtualDistanceColInfo := &model.ColumnInfo{
		ID:        model.VirtualColVecSearchDistanceID,
		FieldType: *types.NewFieldType(mysql.TypeFloat),
		Offset:    len(tableScan.Columns) - 1,
	}

	virtualDistanceCol := &expression.Column{
		UniqueID: tableScan.SCtx().GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldType(mysql.TypeFloat),
	}

	// remove the vector column in order to read distance directly by virtualDistanceCol
	vectorIdx := -1
	for i, col := range tableScan.Columns {
		if col.ID == annQueryInfo.QueryInfo.GetAnnQueryInfo().GetColumn().ColumnId {
			vectorIdx = i
			break
		}
	}
	if vectorIdx == -1 {
		return false
	}

	// set the EnableDistanceProj to modify the read process of tiflash.
	annQueryInfo.QueryInfo.GetAnnQueryInfo().EnableDistanceProj = true

	// append the distance column to the last position in columns and schema.
	tableScan.Columns = slices.Delete(tableScan.Columns, vectorIdx, vectorIdx+1)
	tableScan.Columns = append(tableScan.Columns, virtualDistanceColInfo)

	tableScan.Schema().Columns = slices.Delete(tableScan.Schema().Columns, vectorIdx, vectorIdx+1)
	tableScan.Schema().Append(virtualDistanceCol)

	// The children of topN are currently projections. After optimization, we no longer
	// need the projection and directly set the children to tablescan.
	local.SetChildren(tableScan)

	// modify the topN's ByItem
	local.ByItems[0].Expr = virtualDistanceCol
	global.ByItems[0].Expr = virtualDistanceCol
	local.ByItems[0].Expr.(*expression.Column).Index = tableScan.Schema().Len() - 1

	return true
}

// ContainHeavyFunction check if the expr contains a function that need to do HeavyFunctionOptimize. Currently this only applies
// to Vector data types and their functions. The HeavyFunctionOptimize eliminate the usage of the function in TopN operators
// to avoid vector distance re-calculation of TopN in the root task.
func ContainHeavyFunction(expr expression.Expression) bool {
	sf, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return false
	}
	if _, ok := HeavyFunctionNameMap[sf.FuncName.L]; ok {
		return true
	}
	return slices.ContainsFunc(sf.GetArgs(), ContainHeavyFunction)
}

// canPushToIndexPlan checks if this TopN can be pushed to the index side of copTask.
// It can be pushed to the index side when all columns used by ByItems are available from the index side and there's no prefix index column.
func canPushToIndexPlan(indexPlan base.PhysicalPlan, byItemCols []*expression.Column) bool {
	// If we call canPushToIndexPlan and there's no index plan, we should go into the index merge case.
	// Index merge case is specially handled for now. So we directly return false here.
	// So we directly return false.
	if indexPlan == nil {
		return false
	}
	schema := indexPlan.Schema()
	for _, col := range byItemCols {
		pos := schema.ColumnIndex(col)
		if pos == -1 {
			return false
		}
		if schema.Columns[pos].IsPrefix {
			return false
		}
	}
	return true
}

// canExpressionConvertedToPB checks whether each of the the expression in TopN can be converted to pb.
func canExpressionConvertedToPB(p *physicalop.PhysicalTopN, storeTp kv.StoreType) bool {
	exprs := make([]expression.Expression, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		exprs = append(exprs, item.Expr)
	}
	return expression.CanExprsPushDown(util.GetPushDownCtx(p.SCtx()), exprs, storeTp)
}

// containVirtualColumn checks whether TopN.ByItems contains virtual generated columns.
func containVirtualColumn(p *physicalop.PhysicalTopN, tCols []*expression.Column) bool {
	tColSet := make(map[int64]struct{}, len(tCols))
	for _, tCol := range tCols {
		if tCol.ID > 0 && tCol.VirtualExpr != nil {
			tColSet[tCol.ID] = struct{}{}
		}
	}
	for _, by := range p.ByItems {
		cols := expression.ExtractColumns(by.Expr)
		for _, col := range cols {
			if _, ok := tColSet[col.ID]; ok {
				// A column with ID > 0 indicates that the column can be resolved by data source.
				return true
			}
		}
	}
	return false
}

// canPushDownToTiKV checks whether this topN can be pushed down to TiKV.
func canPushDownToTiKV(p *physicalop.PhysicalTopN, copTask *physicalop.CopTask) bool {
	if !canExpressionConvertedToPB(p, kv.TiKV) {
		return false
	}
	if len(copTask.RootTaskConds) != 0 {
		return false
	}
	if !copTask.IndexPlanFinished && len(copTask.IdxMergePartPlans) > 0 {
		for _, partialPlan := range copTask.IdxMergePartPlans {
			if containVirtualColumn(p, partialPlan.Schema().Columns) {
				return false
			}
		}
	} else if containVirtualColumn(p, copTask.Plan().Schema().Columns) {
		return false
	}
	return true
}

// canPushDownToTiFlash checks whether this topN can be pushed down to TiFlash.
func canPushDownToTiFlash(p *physicalop.PhysicalTopN, mppTask *physicalop.MppTask) bool {
	if !canExpressionConvertedToPB(p, kv.TiFlash) {
		return false
	}
	if containVirtualColumn(p, mppTask.Plan().Schema().Columns) {
		return false
	}
	return true
}

// For https://github.com/pingcap/tidb/issues/51723,
// This function only supports `CLUSTER_SLOW_QUERY`,
// it will change plan from
// TopN -> TableReader -> TableFullScan[cop] to
// TopN -> TableReader -> Limit[cop] -> TableFullScan[cop] + keepOrder
func pushLimitDownToTiDBCop(p *physicalop.PhysicalTopN, copTsk *physicalop.CopTask) (base.Task, bool) {
	if copTsk.IndexPlan != nil || copTsk.TablePlan == nil {
		return nil, false
	}

	var (
		selOnTblScan   *physicalop.PhysicalSelection
		selSelectivity float64
		tblScan        *physicalop.PhysicalTableScan
		err            error
		ok             bool
	)

	copTsk.TablePlan, err = copTsk.TablePlan.Clone(p.SCtx())
	if err != nil {
		return nil, false
	}
	finalTblScanPlan := copTsk.TablePlan
	for len(finalTblScanPlan.Children()) > 0 {
		selOnTblScan, _ = finalTblScanPlan.(*physicalop.PhysicalSelection)
		finalTblScanPlan = finalTblScanPlan.Children()[0]
	}

	if tblScan, ok = finalTblScanPlan.(*physicalop.PhysicalTableScan); !ok {
		return nil, false
	}

	// Check the table is `CLUSTER_SLOW_QUERY` or not.
	if tblScan.Table.Name.O != infoschema.ClusterTableSlowLog {
		return nil, false
	}

	colsProp, ok := physicalop.GetPropByOrderByItems(p.ByItems)
	if !ok {
		return nil, false
	}
	// For cluster tables, HandleCols may be nil. Skip this optimization if HandleCols is not available.
	if tblScan.HandleCols == nil {
		return nil, false
	}
	if len(colsProp.SortItems) != 1 || !colsProp.SortItems[0].Col.Equal(p.SCtx().GetExprCtx().GetEvalCtx(), tblScan.HandleCols.GetCol(0)) {
		return nil, false
	}
	if selOnTblScan != nil && tblScan.StatsInfo().RowCount > 0 {
		selSelectivity = selOnTblScan.StatsInfo().RowCount / tblScan.StatsInfo().RowCount
	}
	tblScan.Desc = colsProp.SortItems[0].Desc
	tblScan.KeepOrder = true

	childProfile := copTsk.Plan().StatsInfo()
	newCount := p.Offset + p.Count
	stats := property.DeriveLimitStats(childProfile, float64(newCount))
	pushedLimit := physicalop.PhysicalLimit{
		Count: newCount,
	}.Init(p.SCtx(), stats, p.QueryBlockOffset())
	pushedLimit.SetSchema(copTsk.TablePlan.Schema())
	copTsk = attachPlan2Task(pushedLimit, copTsk).(*physicalop.CopTask)
	child := pushedLimit.Children()[0]
	child.SetStats(child.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), float64(newCount)))
	if selSelectivity > 0 && selSelectivity < 1 {
		scaledRowCount := child.StatsInfo().RowCount / selSelectivity
		tblScan.SetStats(tblScan.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), scaledRowCount))
	}
	rootTask := copTsk.ConvertToRootTask(p.SCtx())
	return attachPlan2Task(p, rootTask), true
}

// Attach2Task implements the PhysicalPlan interface.
func attach2Task4PhysicalTopN(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalTopN)
	t := tasks[0].Copy()

	// Handle partial order TopN first: when CopTask carries PartialOrderMatchResult,
	// it means skylinePruning has found a prefix index that can provide partial order.
	if copTask, ok := t.(*physicalop.CopTask); ok {
		if copTask.PartialOrderMatchResult != nil && copTask.PartialOrderMatchResult.Matched {
			return handlePartialOrderTopN(p, copTask)
		}
	}

	cols := make([]*expression.Column, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		cols = append(cols, expression.ExtractColumns(item.Expr)...)
	}
	needPushDown := len(cols) > 0
	if copTask, ok := t.(*physicalop.CopTask); ok && needPushDown && copTask.GetStoreType() == kv.TiDB && len(copTask.RootTaskConds) == 0 {
		newTask, changed := pushLimitDownToTiDBCop(p, copTask)
		if changed {
			return newTask
		}
	}
	if copTask, ok := t.(*physicalop.CopTask); ok && needPushDown && canPushDownToTiKV(p, copTask) && len(copTask.RootTaskConds) == 0 {
		// If all columns in topN are from index plan, we push it to index plan, otherwise we finish the index plan and
		// push it to table plan.
		var pushedDownTopN *physicalop.PhysicalTopN
		var newGlobalTopN *physicalop.PhysicalTopN
		if !copTask.IndexPlanFinished && canPushToIndexPlan(copTask.IndexPlan, cols) {
			pushedDownTopN, newGlobalTopN = getPushedDownTopN(p, copTask.IndexPlan, copTask.GetStoreType())
			copTask.IndexPlan = pushedDownTopN
			if newGlobalTopN != nil {
				rootTask := t.ConvertToRootTask(newGlobalTopN.SCtx())
				// Skip TopN with partition on the root. This is a derived topN and window function
				// will take care of the filter.
				if len(p.GetPartitionBy()) > 0 {
					return t
				}
				return attachPlan2Task(newGlobalTopN, rootTask)
			}
		} else {
			// It works for both normal index scan and index merge scan.
			copTask.FinishIndexPlan()
			pushedDownTopN, newGlobalTopN = getPushedDownTopN(p, copTask.TablePlan, copTask.GetStoreType())
			copTask.TablePlan = pushedDownTopN
			if newGlobalTopN != nil {
				rootTask := t.ConvertToRootTask(newGlobalTopN.SCtx())
				// Skip TopN with partition on the root. This is a derived topN and window function
				// will take care of the filter.
				if len(p.GetPartitionBy()) > 0 {
					return t
				}
				return attachPlan2Task(newGlobalTopN, rootTask)
			}
		}
	} else if mppTask, ok := t.(*physicalop.MppTask); ok && needPushDown && canPushDownToTiFlash(p, mppTask) {
		pushedDownTopN, newGlobalTopN := getPushedDownTopN(p, mppTask.Plan(), kv.TiFlash)
		mppTask.SetPlan(pushedDownTopN)
		if newGlobalTopN != nil {
			rootTask := t.ConvertToRootTask(newGlobalTopN.SCtx())
			// Skip TopN with partition on the root. This is a derived topN and window function
			// will take care of the filter.
			if len(p.GetPartitionBy()) > 0 {
				return t
			}
			return attachPlan2Task(newGlobalTopN, rootTask)
		}
	}
	rootTask := t.ConvertToRootTask(p.SCtx())
	// Skip TopN with partition on the root. This is a derived topN and window function
	// will take care of the filter.
	if len(p.GetPartitionBy()) > 0 {
		return t
	}
	return attachPlan2Task(p, rootTask)
}

// handlePartialOrderTopN handles the partial order TopN scenario.
// It fills the partial-order-related fields on the TopN itself and, when possible,
// pushes down a special Limit with prefix information to the index side.
// There are two different cases:
//
// Case1: Two phase TopN, where TiDB keeps TopN and TiKV applies a partial-order Limit:
//
//	TopN(with partial info)
//	  └─IndexLookUp
//	     └─Limit(with partial info)
//	... (other operators)
//
// Case2: One phase TopN, where the whole TopN can only be executed in TiDB:
//
//	TopN(with partial info)
//	  ├─IndexPlan
//	  └─TablePlan
func handlePartialOrderTopN(p *physicalop.PhysicalTopN, copTask *physicalop.CopTask) base.Task {
	matchResult := copTask.PartialOrderMatchResult

	// Init partial order params PrefixCol and PrefixLen.
	// PartialOrderedLimit = Count + Offset.
	// TopN(with partial order) executor will short-cut read when it already handle "p.Count + p.Offset" rows.
	// Also it need to read X more rows (which prefix value is same as the last line prefix value) to ensure correctness.
	partialOrderedLimit := p.Count + p.Offset
	p.PrefixLen = matchResult.PrefixLen
	// Find the corresponding prefix column in TopN's schema.
	// matchResult.PrefixCol is from IndexScan's schema, but
	// Projection operators may remap columns. We need to find the column in TopN's
	// schema that has the same UniqueID as matchResult.PrefixCol.
	// Column UniqueID remains unchanged even after Projection remapping.
	p.PrefixCol = nil
	for _, col := range p.Schema().Columns {
		if col.UniqueID == matchResult.PrefixCol.UniqueID {
			p.PrefixCol = col
			break
		}
	}
	// Fallback: if not found in rootTask schema (should not happen)
	if p.PrefixCol == nil {
		return base.InvalidTask
	}

	// Decide whether we can push a special Limit down to the index plan.
	// Conditions:
	//   - Not an IndexMerge.
	//   - IndexPlan is not finished (IndexPlanFinished == false).
	//   - No root task conditions.
	// Since the output of the table plan is not ordered. So if limit can be pushed down, it must be pushed down to the index plan.
	// Therefore, we performed this related check here.
	canPushLimit := false
	if len(copTask.IdxMergePartPlans) == 0 &&
		!copTask.IndexPlanFinished &&
		len(copTask.RootTaskConds) == 0 &&
		copTask.IndexPlan != nil {
		canPushLimit = true
	}

	if canPushLimit {
		// Two-layer mode: TiDB TopN(with partial order info.) + TiKV limit(with partial order info.)
		// The estRows of partial order TopN : N + X
		// N: The partialOrderedLimit, N means the value of TopN, N = Count + Offset.
		// X: The estimated extra rows to read to fulfill the TopN.
		// We need to read more prefix values that are the same as the last line
		// to ensure the correctness of the final calculation of the Top n rows.
		maxX := estimateMaxXForPartialOrder()
		estimatedRows := float64(partialOrderedLimit) + float64(maxX)
		childProfile := copTask.IndexPlan.StatsInfo()
		limitStats := property.DeriveLimitStats(childProfile, estimatedRows)

		pushedDownLimit := physicalop.PhysicalLimit{
			Count:     partialOrderedLimit,
			PrefixCol: p.PrefixCol,
			PrefixLen: matchResult.PrefixLen,
		}.Init(p.SCtx(), limitStats, p.QueryBlockOffset())
		pushedDownLimit.SetChildren(copTask.IndexPlan)
		pushedDownLimit.SetSchema(copTask.IndexPlan.Schema())
		copTask.IndexPlan = pushedDownLimit
	}

	// Always keep TopN in TiDB as the upper layer.
	rootTask := copTask.ConvertToRootTask(p.SCtx())
	return attachPlan2Task(p, rootTask)
}

// estimateMaxXForPartialOrder estimates the extra rows X to read for partial order optimization.
// This value is used for statistics (row count estimation).
func estimateMaxXForPartialOrder() uint64 {
	// TODO: implement it by TopN/buckets and adjust it by session variable.
	return 0
}
