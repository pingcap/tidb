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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/baseimpl"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/paging"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"go.uber.org/zap"
)

func attachPlan2Task(p base.PhysicalPlan, t base.Task) base.Task {
	switch v := t.(type) {
	case *CopTask:
		if v.indexPlanFinished {
			p.SetChildren(v.tablePlan)
			v.tablePlan = p
		} else {
			p.SetChildren(v.indexPlan)
			v.indexPlan = p
		}
	case *RootTask:
		p.SetChildren(v.GetPlan())
		v.SetPlan(p)
	case *MppTask:
		p.SetChildren(v.p)
		v.p = p
	}
	return t
}

// finishIndexPlan means we no longer add plan to index plan, and compute the network cost for it.
func (t *CopTask) finishIndexPlan() {
	if t.indexPlanFinished {
		return
	}
	t.indexPlanFinished = true
	// index merge case is specially handled for now.
	// We need a elegant way to solve the stats of index merge in this case.
	if t.tablePlan != nil && t.indexPlan != nil {
		ts := t.tablePlan.(*PhysicalTableScan)
		originStats := ts.StatsInfo()
		ts.SetStats(t.indexPlan.StatsInfo())
		if originStats != nil {
			// keep the original stats version
			ts.StatsInfo().StatsVersion = originStats.StatsVersion
		}
	}
}

func (t *CopTask) getStoreType() kv.StoreType {
	if t.tablePlan == nil {
		return kv.TiKV
	}
	tp := t.tablePlan
	for len(tp.Children()) > 0 {
		if len(tp.Children()) > 1 {
			return kv.TiFlash
		}
		tp = tp.Children()[0]
	}
	if ts, ok := tp.(*PhysicalTableScan); ok {
		return ts.StoreType
	}
	return kv.TiKV
}

// Attach2Task implements PhysicalPlan interface.
func (p *basePhysicalPlan) Attach2Task(tasks ...base.Task) base.Task {
	t := tasks[0].ConvertToRootTask(p.SCtx())
	return attachPlan2Task(p.self, t)
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalUnionScan) Attach2Task(tasks ...base.Task) base.Task {
	// We need to pull the projection under unionScan upon unionScan.
	// Since the projection only prunes columns, it's ok the put it upon unionScan.
	if sel, ok := tasks[0].Plan().(*PhysicalSelection); ok {
		if pj, ok := sel.children[0].(*PhysicalProjection); ok {
			// Convert unionScan->selection->projection to projection->unionScan->selection.
			sel.SetChildren(pj.children...)
			p.SetChildren(sel)
			p.SetStats(tasks[0].Plan().StatsInfo())
			rt, _ := tasks[0].(*RootTask)
			rt.SetPlan(p)
			pj.SetChildren(p)
			return pj.Attach2Task(tasks...)
		}
	}
	if pj, ok := tasks[0].Plan().(*PhysicalProjection); ok {
		// Convert unionScan->projection to projection->unionScan, because unionScan can't handle projection as its children.
		p.SetChildren(pj.children...)
		p.SetStats(tasks[0].Plan().StatsInfo())
		rt, _ := tasks[0].(*RootTask)
		rt.SetPlan(pj.children[0])
		pj.SetChildren(p)
		return pj.Attach2Task(p.basePhysicalPlan.Attach2Task(tasks...))
	}
	p.SetStats(tasks[0].Plan().StatsInfo())
	return p.basePhysicalPlan.Attach2Task(tasks...)
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalApply) Attach2Task(tasks ...base.Task) base.Task {
	lTask := tasks[0].ConvertToRootTask(p.SCtx())
	rTask := tasks[1].ConvertToRootTask(p.SCtx())
	p.SetChildren(lTask.Plan(), rTask.Plan())
	p.schema = BuildPhysicalJoinSchema(p.JoinType, p)
	t := &RootTask{}
	t.SetPlan(p)
	return t
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalIndexMergeJoin) Attach2Task(tasks ...base.Task) base.Task {
	innerTask := p.innerTask
	outerTask := tasks[1-p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.Plan(), innerTask.Plan())
	} else {
		p.SetChildren(innerTask.Plan(), outerTask.Plan())
	}
	t := &RootTask{}
	t.SetPlan(p)
	return t
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalIndexHashJoin) Attach2Task(tasks ...base.Task) base.Task {
	innerTask := p.innerTask
	outerTask := tasks[1-p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.Plan(), innerTask.Plan())
	} else {
		p.SetChildren(innerTask.Plan(), outerTask.Plan())
	}
	t := &RootTask{}
	t.SetPlan(p)
	return t
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalIndexJoin) Attach2Task(tasks ...base.Task) base.Task {
	innerTask := p.innerTask
	outerTask := tasks[1-p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.Plan(), innerTask.Plan())
	} else {
		p.SetChildren(innerTask.Plan(), outerTask.Plan())
	}
	t := &RootTask{}
	t.SetPlan(p)
	return t
}

// RowSize for cost model ver2 is simplified, always use this function to calculate row size.
func getAvgRowSize(stats *property.StatsInfo, cols []*expression.Column) (size float64) {
	if stats.HistColl != nil {
		size = cardinality.GetAvgRowSizeDataInDiskByRows(stats.HistColl, cols)
	} else {
		// Estimate using just the type info.
		for _, col := range cols {
			size += float64(chunk.EstimateTypeWidth(col.GetStaticType()))
		}
	}
	return
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalHashJoin) Attach2Task(tasks ...base.Task) base.Task {
	if p.storeTp == kv.TiFlash {
		return p.attach2TaskForTiFlash(tasks...)
	}
	lTask := tasks[0].ConvertToRootTask(p.SCtx())
	rTask := tasks[1].ConvertToRootTask(p.SCtx())
	p.SetChildren(lTask.Plan(), rTask.Plan())
	task := &RootTask{}
	task.SetPlan(p)
	return task
}

// TiDB only require that the types fall into the same catalog but TiFlash require the type to be exactly the same, so
// need to check if the conversion is a must
func needConvert(tp *types.FieldType, rtp *types.FieldType) bool {
	// all the string type are mapped to the same type in TiFlash, so
	// do not need convert for string types
	if types.IsString(tp.GetType()) && types.IsString(rtp.GetType()) {
		return false
	}
	if tp.GetType() != rtp.GetType() {
		return true
	}
	if tp.GetType() != mysql.TypeNewDecimal {
		return false
	}
	if tp.GetDecimal() != rtp.GetDecimal() {
		return true
	}
	// for decimal type, TiFlash have 4 different impl based on the required precision
	if tp.GetFlen() >= 0 && tp.GetFlen() <= 9 && rtp.GetFlen() >= 0 && rtp.GetFlen() <= 9 {
		return false
	}
	if tp.GetFlen() > 9 && tp.GetFlen() <= 18 && rtp.GetFlen() > 9 && rtp.GetFlen() <= 18 {
		return false
	}
	if tp.GetFlen() > 18 && tp.GetFlen() <= 38 && rtp.GetFlen() > 18 && rtp.GetFlen() <= 38 {
		return false
	}
	if tp.GetFlen() > 38 && tp.GetFlen() <= 65 && rtp.GetFlen() > 38 && rtp.GetFlen() <= 65 {
		return false
	}
	return true
}

func negotiateCommonType(lType, rType *types.FieldType) (*types.FieldType, bool, bool) {
	commonType := types.AggFieldType([]*types.FieldType{lType, rType})
	if commonType.GetType() == mysql.TypeNewDecimal {
		lExtend := 0
		rExtend := 0
		cDec := rType.GetDecimal()
		if lType.GetDecimal() < rType.GetDecimal() {
			lExtend = rType.GetDecimal() - lType.GetDecimal()
		} else if lType.GetDecimal() > rType.GetDecimal() {
			rExtend = lType.GetDecimal() - rType.GetDecimal()
			cDec = lType.GetDecimal()
		}
		lLen, rLen := lType.GetFlen()+lExtend, rType.GetFlen()+rExtend
		cLen := max(lLen, rLen)
		commonType.SetDecimalUnderLimit(cDec)
		commonType.SetFlenUnderLimit(cLen)
	} else if needConvert(lType, commonType) || needConvert(rType, commonType) {
		if mysql.IsIntegerType(commonType.GetType()) {
			// If the target type is int, both TiFlash and Mysql only support cast to Int64
			// so we need to promote the type to Int64
			commonType.SetType(mysql.TypeLonglong)
			commonType.SetFlen(mysql.MaxIntWidth)
		}
	}
	return commonType, needConvert(lType, commonType), needConvert(rType, commonType)
}

func getProj(ctx base.PlanContext, p base.PhysicalPlan) *PhysicalProjection {
	proj := PhysicalProjection{
		Exprs: make([]expression.Expression, 0, len(p.Schema().Columns)),
	}.Init(ctx, p.StatsInfo(), p.QueryBlockOffset())
	for _, col := range p.Schema().Columns {
		proj.Exprs = append(proj.Exprs, col)
	}
	proj.SetSchema(p.Schema().Clone())
	proj.SetChildren(p)
	return proj
}

func appendExpr(p *PhysicalProjection, expr expression.Expression) *expression.Column {
	p.Exprs = append(p.Exprs, expr)

	col := &expression.Column{
		UniqueID: p.SCtx().GetSessionVars().AllocPlanColumnID(),
		RetType:  expr.GetType(p.SCtx().GetExprCtx().GetEvalCtx()),
	}
	col.SetCoercibility(expr.Coercibility())
	p.schema.Append(col)
	return col
}

// TiFlash join require that partition key has exactly the same type, while TiDB only guarantee the partition key is the same catalog,
// so if the partition key type is not exactly the same, we need add a projection below the join or exchanger if exists.
func (p *PhysicalHashJoin) convertPartitionKeysIfNeed(lTask, rTask *MppTask) (*MppTask, *MppTask) {
	lp := lTask.p
	if _, ok := lp.(*PhysicalExchangeReceiver); ok {
		lp = lp.Children()[0].Children()[0]
	}
	rp := rTask.p
	if _, ok := rp.(*PhysicalExchangeReceiver); ok {
		rp = rp.Children()[0].Children()[0]
	}
	// to mark if any partition key needs to convert
	lMask := make([]bool, len(lTask.hashCols))
	rMask := make([]bool, len(rTask.hashCols))
	cTypes := make([]*types.FieldType, len(lTask.hashCols))
	lChanged := false
	rChanged := false
	for i := range lTask.hashCols {
		lKey := lTask.hashCols[i]
		rKey := rTask.hashCols[i]
		cType, lConvert, rConvert := negotiateCommonType(lKey.Col.RetType, rKey.Col.RetType)
		if lConvert {
			lMask[i] = true
			cTypes[i] = cType
			lChanged = true
		}
		if rConvert {
			rMask[i] = true
			cTypes[i] = cType
			rChanged = true
		}
	}
	if !lChanged && !rChanged {
		return lTask, rTask
	}
	var lProj, rProj *PhysicalProjection
	if lChanged {
		lProj = getProj(p.SCtx(), lp)
		lp = lProj
	}
	if rChanged {
		rProj = getProj(p.SCtx(), rp)
		rp = rProj
	}

	lPartKeys := make([]*property.MPPPartitionColumn, 0, len(rTask.hashCols))
	rPartKeys := make([]*property.MPPPartitionColumn, 0, len(lTask.hashCols))
	for i := range lTask.hashCols {
		lKey := lTask.hashCols[i]
		rKey := rTask.hashCols[i]
		if lMask[i] {
			cType := cTypes[i].Clone()
			cType.SetFlag(lKey.Col.RetType.GetFlag())
			lCast := expression.BuildCastFunction(p.SCtx().GetExprCtx(), lKey.Col, cType)
			lKey = &property.MPPPartitionColumn{Col: appendExpr(lProj, lCast), CollateID: lKey.CollateID}
		}
		if rMask[i] {
			cType := cTypes[i].Clone()
			cType.SetFlag(rKey.Col.RetType.GetFlag())
			rCast := expression.BuildCastFunction(p.SCtx().GetExprCtx(), rKey.Col, cType)
			rKey = &property.MPPPartitionColumn{Col: appendExpr(rProj, rCast), CollateID: rKey.CollateID}
		}
		lPartKeys = append(lPartKeys, lKey)
		rPartKeys = append(rPartKeys, rKey)
	}
	// if left or right child changes, we need to add enforcer.
	if lChanged {
		nlTask := lTask.Copy().(*MppTask)
		nlTask.p = lProj
		nlTask = nlTask.enforceExchanger(&property.PhysicalProperty{
			TaskTp:           property.MppTaskType,
			MPPPartitionTp:   property.HashType,
			MPPPartitionCols: lPartKeys,
		})
		lTask = nlTask
	}
	if rChanged {
		nrTask := rTask.Copy().(*MppTask)
		nrTask.p = rProj
		nrTask = nrTask.enforceExchanger(&property.PhysicalProperty{
			TaskTp:           property.MppTaskType,
			MPPPartitionTp:   property.HashType,
			MPPPartitionCols: rPartKeys,
		})
		rTask = nrTask
	}
	return lTask, rTask
}

func (p *PhysicalHashJoin) attach2TaskForMpp(tasks ...base.Task) base.Task {
	lTask, lok := tasks[0].(*MppTask)
	rTask, rok := tasks[1].(*MppTask)
	if !lok || !rok {
		return base.InvalidTask
	}
	if p.mppShuffleJoin {
		// protection check is case of some bugs
		if len(lTask.hashCols) != len(rTask.hashCols) || len(lTask.hashCols) == 0 {
			return base.InvalidTask
		}
		lTask, rTask = p.convertPartitionKeysIfNeed(lTask, rTask)
	}
	p.SetChildren(lTask.Plan(), rTask.Plan())
	// outer task is the task that will pass its MPPPartitionType to the join result
	// for broadcast inner join, it should be the non-broadcast side, since broadcast side is always the build side, so
	// just use the probe side is ok.
	// for hash inner join, both side is ok, by default, we use the probe side
	// for outer join, it should always be the outer side of the join
	// for semi join, it should be the left side(the same as left out join)
	outerTaskIndex := 1 - p.InnerChildIdx
	if p.JoinType != InnerJoin {
		if p.JoinType == RightOuterJoin {
			outerTaskIndex = 1
		} else {
			outerTaskIndex = 0
		}
	}
	// can not use the task from tasks because it maybe updated.
	outerTask := lTask
	if outerTaskIndex == 1 {
		outerTask = rTask
	}
	task := &MppTask{
		p:        p,
		partTp:   outerTask.partTp,
		hashCols: outerTask.hashCols,
	}
	// Current TiFlash doesn't support receive Join executors' schema info directly from TiDB.
	// Instead, it calculates Join executors' output schema using algorithm like BuildPhysicalJoinSchema which
	// produces full semantic schema.
	// Thus, the column prune optimization achievements will be abandoned here.
	// To avoid the performance issue, add a projection here above the Join operator to prune useless columns explicitly.
	// TODO(hyb): transfer Join executors' schema to TiFlash through DagRequest, and use it directly in TiFlash.
	defaultSchema := BuildPhysicalJoinSchema(p.JoinType, p)
	hashColArray := make([]*expression.Column, 0, len(task.hashCols))
	// For task.hashCols, these columns may not be contained in pruned columns:
	// select A.id from A join B on A.id = B.id; Suppose B is probe side, and it's hash inner join.
	// After column prune, the output schema of A join B will be A.id only; while the task's hashCols will be B.id.
	// To make matters worse, the hashCols may be used to check if extra cast projection needs to be added, then the newly
	// added projection will expect B.id as input schema. So make sure hashCols are included in task.p's schema.
	// TODO: planner should takes the hashCols attribute into consideration when perform column pruning; Or provide mechanism
	// to constraint hashCols are always chosen inside Join's pruned schema
	for _, hashCol := range task.hashCols {
		hashColArray = append(hashColArray, hashCol.Col)
	}
	if p.schema.Len() < defaultSchema.Len() {
		if p.schema.Len() > 0 {
			proj := PhysicalProjection{
				Exprs: expression.Column2Exprs(p.schema.Columns),
			}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset())

			proj.SetSchema(p.Schema().Clone())
			for _, hashCol := range hashColArray {
				if !proj.Schema().Contains(hashCol) && defaultSchema.Contains(hashCol) {
					joinCol := defaultSchema.Columns[defaultSchema.ColumnIndex(hashCol)]
					proj.Exprs = append(proj.Exprs, joinCol)
					proj.Schema().Append(joinCol.Clone().(*expression.Column))
				}
			}
			attachPlan2Task(proj, task)
		} else {
			if len(hashColArray) == 0 {
				constOne := expression.NewOne()
				expr := make([]expression.Expression, 0, 1)
				expr = append(expr, constOne)
				proj := PhysicalProjection{
					Exprs: expr,
				}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset())

				proj.schema = expression.NewSchema(&expression.Column{
					UniqueID: proj.SCtx().GetSessionVars().AllocPlanColumnID(),
					RetType:  constOne.GetType(p.SCtx().GetExprCtx().GetEvalCtx()),
				})
				attachPlan2Task(proj, task)
			} else {
				proj := PhysicalProjection{
					Exprs: make([]expression.Expression, 0, len(hashColArray)),
				}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset())

				clonedHashColArray := make([]*expression.Column, 0, len(task.hashCols))
				for _, hashCol := range hashColArray {
					if defaultSchema.Contains(hashCol) {
						joinCol := defaultSchema.Columns[defaultSchema.ColumnIndex(hashCol)]
						proj.Exprs = append(proj.Exprs, joinCol)
						clonedHashColArray = append(clonedHashColArray, joinCol.Clone().(*expression.Column))
					}
				}

				proj.SetSchema(expression.NewSchema(clonedHashColArray...))
				attachPlan2Task(proj, task)
			}
		}
	}
	p.schema = defaultSchema
	return task
}

func (p *PhysicalHashJoin) attach2TaskForTiFlash(tasks ...base.Task) base.Task {
	lTask, lok := tasks[0].(*CopTask)
	rTask, rok := tasks[1].(*CopTask)
	if !lok || !rok {
		return p.attach2TaskForMpp(tasks...)
	}
	p.SetChildren(lTask.Plan(), rTask.Plan())
	p.schema = BuildPhysicalJoinSchema(p.JoinType, p)
	if !lTask.indexPlanFinished {
		lTask.finishIndexPlan()
	}
	if !rTask.indexPlanFinished {
		rTask.finishIndexPlan()
	}

	task := &CopTask{
		tblColHists:       rTask.tblColHists,
		indexPlanFinished: true,
		tablePlan:         p,
	}
	return task
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalMergeJoin) Attach2Task(tasks ...base.Task) base.Task {
	lTask := tasks[0].ConvertToRootTask(p.SCtx())
	rTask := tasks[1].ConvertToRootTask(p.SCtx())
	p.SetChildren(lTask.Plan(), rTask.Plan())
	t := &RootTask{}
	t.SetPlan(p)
	return t
}

func buildIndexLookUpTask(ctx base.PlanContext, t *CopTask) *RootTask {
	newTask := &RootTask{}
	p := PhysicalIndexLookUpReader{
		tablePlan:        t.tablePlan,
		indexPlan:        t.indexPlan,
		ExtraHandleCol:   t.extraHandleCol,
		CommonHandleCols: t.commonHandleCols,
		expectedCnt:      t.expectCnt,
		keepOrder:        t.keepOrder,
	}.Init(ctx, t.tablePlan.QueryBlockOffset())
	p.PlanPartInfo = t.physPlanPartInfo
	setTableScanToTableRowIDScan(p.tablePlan)
	p.SetStats(t.tablePlan.StatsInfo())
	// Do not inject the extra Projection even if t.needExtraProj is set, or the schema between the phase-1 agg and
	// the final agg would be broken. Please reference comments for the similar logic in
	// (*copTask).convertToRootTaskImpl() for the PhysicalTableReader case.
	// We need to refactor these logics.
	aggPushedDown := false
	switch p.tablePlan.(type) {
	case *PhysicalHashAgg, *PhysicalStreamAgg:
		aggPushedDown = true
	}

	if t.needExtraProj && !aggPushedDown {
		schema := t.originSchema
		proj := PhysicalProjection{Exprs: expression.Column2Exprs(schema.Columns)}.Init(ctx, p.StatsInfo(), t.tablePlan.QueryBlockOffset(), nil)
		proj.SetSchema(schema)
		proj.SetChildren(p)
		newTask.SetPlan(proj)
	} else {
		newTask.SetPlan(p)
	}
	return newTask
}

func extractRows(p base.PhysicalPlan) float64 {
	f := float64(0)
	for _, c := range p.Children() {
		if len(c.Children()) != 0 {
			f += extractRows(c)
		} else {
			f += c.StatsInfo().RowCount
		}
	}
	return f
}

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

func (t *CopTask) handleRootTaskConds(ctx base.PlanContext, newTask *RootTask) {
	if len(t.rootTaskConds) > 0 {
		selectivity, _, err := cardinality.Selectivity(ctx, t.tblColHists, t.rootTaskConds, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = cost.SelectionFactor
		}
		sel := PhysicalSelection{Conditions: t.rootTaskConds}.Init(ctx, newTask.GetPlan().StatsInfo().Scale(selectivity), newTask.GetPlan().QueryBlockOffset())
		sel.fromDataSource = true
		sel.SetChildren(newTask.GetPlan())
		newTask.SetPlan(sel)
	}
}

// setTableScanToTableRowIDScan is to update the isChildOfIndexLookUp attribute of PhysicalTableScan child
func setTableScanToTableRowIDScan(p base.PhysicalPlan) {
	if ts, ok := p.(*PhysicalTableScan); ok {
		ts.SetIsChildOfIndexLookUp(true)
	} else {
		for _, child := range p.Children() {
			setTableScanToTableRowIDScan(child)
		}
	}
}

// Attach2Task attach limit to different cases.
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
func (p *PhysicalLimit) Attach2Task(tasks ...base.Task) base.Task {
	t := tasks[0].Copy()
	newPartitionBy := make([]property.SortItem, 0, len(p.GetPartitionBy()))
	for _, expr := range p.GetPartitionBy() {
		newPartitionBy = append(newPartitionBy, expr.Clone())
	}

	sunk := false
	if cop, ok := t.(*CopTask); ok {
		suspendLimitAboveTablePlan := func() {
			newCount := p.Offset + p.Count
			childProfile := cop.tablePlan.StatsInfo()
			// but "regionNum" is unknown since the copTask can be a double read, so we ignore it now.
			stats := deriveLimitStats(childProfile, float64(newCount))
			pushedDownLimit := PhysicalLimit{PartitionBy: newPartitionBy, Count: newCount}.Init(p.SCtx(), stats, p.QueryBlockOffset())
			pushedDownLimit.SetChildren(cop.tablePlan)
			cop.tablePlan = pushedDownLimit
			// Don't use clone() so that Limit and its children share the same schema. Otherwise, the virtual generated column may not be resolved right.
			pushedDownLimit.SetSchema(pushedDownLimit.children[0].Schema())
			t = cop.ConvertToRootTask(p.SCtx())
		}
		if len(cop.idxMergePartPlans) == 0 {
			// For double read which requires order being kept, the limit cannot be pushed down to the table side,
			// because handles would be reordered before being sent to table scan.
			if (!cop.keepOrder || !cop.indexPlanFinished || cop.indexPlan == nil) && len(cop.rootTaskConds) == 0 {
				// When limit is pushed down, we should remove its offset.
				newCount := p.Offset + p.Count
				childProfile := cop.Plan().StatsInfo()
				// Strictly speaking, for the row count of stats, we should multiply newCount with "regionNum",
				// but "regionNum" is unknown since the copTask can be a double read, so we ignore it now.
				stats := deriveLimitStats(childProfile, float64(newCount))
				pushedDownLimit := PhysicalLimit{PartitionBy: newPartitionBy, Count: newCount}.Init(p.SCtx(), stats, p.QueryBlockOffset())
				cop = attachPlan2Task(pushedDownLimit, cop).(*CopTask)
				// Don't use clone() so that Limit and its children share the same schema. Otherwise the virtual generated column may not be resolved right.
				pushedDownLimit.SetSchema(pushedDownLimit.children[0].Schema())
			}
			t = cop.ConvertToRootTask(p.SCtx())
			sunk = p.sinkIntoIndexLookUp(t)
		} else if !cop.idxMergeIsIntersection {
			// We only support push part of the order prop down to index merge build case.
			if len(cop.rootTaskConds) == 0 {
				// For double read which requires order being kept, the limit cannot be pushed down to the table side,
				// because handles would be reordered before being sent to table scan.
				if cop.indexPlanFinished && !cop.keepOrder {
					// when the index plan is finished and index plan is not ordered, sink the limit to the index merge table side.
					suspendLimitAboveTablePlan()
				} else if !cop.indexPlanFinished {
					// cop.indexPlanFinished = false indicates the table side is a pure table-scan, sink the limit to the index merge index side.
					newCount := p.Offset + p.Count
					limitChildren := make([]base.PhysicalPlan, 0, len(cop.idxMergePartPlans))
					for _, partialScan := range cop.idxMergePartPlans {
						childProfile := partialScan.StatsInfo()
						stats := deriveLimitStats(childProfile, float64(newCount))
						pushedDownLimit := PhysicalLimit{PartitionBy: newPartitionBy, Count: newCount}.Init(p.SCtx(), stats, p.QueryBlockOffset())
						pushedDownLimit.SetChildren(partialScan)
						pushedDownLimit.SetSchema(pushedDownLimit.children[0].Schema())
						limitChildren = append(limitChildren, pushedDownLimit)
					}
					cop.idxMergePartPlans = limitChildren
					t = cop.ConvertToRootTask(p.SCtx())
					sunk = p.sinkIntoIndexMerge(t)
				} else {
					// when there are some limitations, just sink the limit upon the index merge reader.
					t = cop.ConvertToRootTask(p.SCtx())
					sunk = p.sinkIntoIndexMerge(t)
				}
			} else {
				// when there are some root conditions, just sink the limit upon the index merge reader.
				t = cop.ConvertToRootTask(p.SCtx())
				sunk = p.sinkIntoIndexMerge(t)
			}
		} else if cop.idxMergeIsIntersection {
			// In the index merge with intersection case, only the limit can be pushed down to the index merge table side.
			// Note Difference:
			// IndexMerge.PushedLimit is applied before table scan fetching, limiting the indexPartialPlan rows returned (it maybe ordered if orderBy items not empty)
			// TableProbeSide sink limit is applied on the top of table plan, which will quickly shut down the both fetch-back and read-back process.
			if len(cop.rootTaskConds) == 0 {
				if cop.indexPlanFinished {
					// indicates the table side is not a pure table-scan, so we could only append the limit upon the table plan.
					suspendLimitAboveTablePlan()
				} else {
					t = cop.ConvertToRootTask(p.SCtx())
					sunk = p.sinkIntoIndexMerge(t)
				}
			} else {
				// Otherwise, suspend the limit out of index merge reader.
				t = cop.ConvertToRootTask(p.SCtx())
				sunk = p.sinkIntoIndexMerge(t)
			}
		} else {
			// Whatever the remained case is, we directly convert to it to root task.
			t = cop.ConvertToRootTask(p.SCtx())
		}
	} else if mpp, ok := t.(*MppTask); ok {
		newCount := p.Offset + p.Count
		childProfile := mpp.Plan().StatsInfo()
		stats := deriveLimitStats(childProfile, float64(newCount))
		pushedDownLimit := PhysicalLimit{Count: newCount, PartitionBy: newPartitionBy}.Init(p.SCtx(), stats, p.QueryBlockOffset())
		mpp = attachPlan2Task(pushedDownLimit, mpp).(*MppTask)
		pushedDownLimit.SetSchema(pushedDownLimit.children[0].Schema())
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

func (p *PhysicalLimit) sinkIntoIndexLookUp(t base.Task) bool {
	root := t.(*RootTask)
	reader, isDoubleRead := root.GetPlan().(*PhysicalIndexLookUpReader)
	proj, isProj := root.GetPlan().(*PhysicalProjection)
	if !isDoubleRead && !isProj {
		return false
	}
	if isProj {
		reader, isDoubleRead = proj.Children()[0].(*PhysicalIndexLookUpReader)
		if !isDoubleRead {
			return false
		}
	}

	// We can sink Limit into IndexLookUpReader only if tablePlan contains no Selection.
	ts, isTableScan := reader.tablePlan.(*PhysicalTableScan)
	if !isTableScan {
		return false
	}

	// If this happens, some Projection Operator must be inlined into this Limit. (issues/14428)
	// For example, if the original plan is `IndexLookUp(col1, col2) -> Limit(col1, col2) -> Project(col1)`,
	//  then after inlining the Project, it will be `IndexLookUp(col1, col2) -> Limit(col1)` here.
	// If the Limit is sunk into the IndexLookUp, the IndexLookUp's schema needs to be updated as well,
	// So we add an extra projection to solve the problem.
	if p.Schema().Len() != reader.Schema().Len() {
		extraProj := PhysicalProjection{
			Exprs: expression.Column2Exprs(p.schema.Columns),
		}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), nil)
		extraProj.SetSchema(p.schema)
		// If the root.p is already a Projection. We left the optimization for the later Projection Elimination.
		extraProj.SetChildren(root.GetPlan())
		root.SetPlan(extraProj)
	}

	reader.PushedLimit = &PushedDownLimit{
		Offset: p.Offset,
		Count:  p.Count,
	}
	originStats := ts.StatsInfo()
	ts.SetStats(p.StatsInfo())
	if originStats != nil {
		// keep the original stats version
		ts.StatsInfo().StatsVersion = originStats.StatsVersion
	}
	reader.SetStats(p.StatsInfo())
	if isProj {
		proj.SetStats(p.StatsInfo())
	}
	return true
}

func (p *PhysicalLimit) sinkIntoIndexMerge(t base.Task) bool {
	root := t.(*RootTask)
	imReader, isIm := root.GetPlan().(*PhysicalIndexMergeReader)
	proj, isProj := root.GetPlan().(*PhysicalProjection)
	if !isIm && !isProj {
		return false
	}
	if isProj {
		imReader, isIm = proj.Children()[0].(*PhysicalIndexMergeReader)
		if !isIm {
			return false
		}
	}
	ts, ok := imReader.tablePlan.(*PhysicalTableScan)
	if !ok {
		return false
	}
	imReader.PushedLimit = &PushedDownLimit{
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
	needProj := p.schema.Len() != root.GetPlan().Schema().Len()
	if !needProj {
		for i := 0; i < p.schema.Len(); i++ {
			if !p.schema.Columns[i].EqualColumn(root.GetPlan().Schema().Columns[i]) {
				needProj = true
				break
			}
		}
	}
	if needProj {
		extraProj := PhysicalProjection{
			Exprs: expression.Column2Exprs(p.schema.Columns),
		}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), nil)
		extraProj.SetSchema(p.schema)
		// If the root.p is already a Projection. We left the optimization for the later Projection Elimination.
		extraProj.SetChildren(root.GetPlan())
		root.SetPlan(extraProj)
	}
	return true
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalSort) Attach2Task(tasks ...base.Task) base.Task {
	t := tasks[0].Copy()
	t = attachPlan2Task(p, t)
	return t
}

// Attach2Task implements PhysicalPlan interface.
func (p *NominalSort) Attach2Task(tasks ...base.Task) base.Task {
	if p.OnlyColumn {
		return tasks[0]
	}
	t := tasks[0].Copy()
	t = attachPlan2Task(p, t)
	return t
}

func (p *PhysicalTopN) getPushedDownTopN(childPlan base.PhysicalPlan) *PhysicalTopN {
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
	stats := deriveLimitStats(childProfile, float64(newCount))
	topN := PhysicalTopN{
		ByItems:     newByItems,
		PartitionBy: newPartitionBy,
		Count:       newCount,
	}.Init(p.SCtx(), stats, p.QueryBlockOffset(), p.GetChildReqProps(0))
	topN.SetChildren(childPlan)
	return topN
}

// canPushToIndexPlan checks if this TopN can be pushed to the index side of copTask.
// It can be pushed to the index side when all columns used by ByItems are available from the index side and there's no prefix index column.
func (*PhysicalTopN) canPushToIndexPlan(indexPlan base.PhysicalPlan, byItemCols []*expression.Column) bool {
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
func (p *PhysicalTopN) canExpressionConvertedToPB(storeTp kv.StoreType) bool {
	exprs := make([]expression.Expression, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		exprs = append(exprs, item.Expr)
	}
	return expression.CanExprsPushDown(GetPushDownCtx(p.SCtx()), exprs, storeTp)
}

// containVirtualColumn checks whether TopN.ByItems contains virtual generated columns.
func (p *PhysicalTopN) containVirtualColumn(tCols []*expression.Column) bool {
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
func (p *PhysicalTopN) canPushDownToTiKV(copTask *CopTask) bool {
	if !p.canExpressionConvertedToPB(kv.TiKV) {
		return false
	}
	if len(copTask.rootTaskConds) != 0 {
		return false
	}
	if !copTask.indexPlanFinished && len(copTask.idxMergePartPlans) > 0 {
		for _, partialPlan := range copTask.idxMergePartPlans {
			if p.containVirtualColumn(partialPlan.Schema().Columns) {
				return false
			}
		}
	} else if p.containVirtualColumn(copTask.Plan().Schema().Columns) {
		return false
	}
	return true
}

// canPushDownToTiFlash checks whether this topN can be pushed down to TiFlash.
func (p *PhysicalTopN) canPushDownToTiFlash(mppTask *MppTask) bool {
	if !p.canExpressionConvertedToPB(kv.TiFlash) {
		return false
	}
	if p.containVirtualColumn(mppTask.Plan().Schema().Columns) {
		return false
	}
	return true
}

// Attach2Task implements physical plan
func (p *PhysicalTopN) Attach2Task(tasks ...base.Task) base.Task {
	t := tasks[0].Copy()
	cols := make([]*expression.Column, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		cols = append(cols, expression.ExtractColumns(item.Expr)...)
	}
	needPushDown := len(cols) > 0
	if copTask, ok := t.(*CopTask); ok && needPushDown && p.canPushDownToTiKV(copTask) && len(copTask.rootTaskConds) == 0 {
		// If all columns in topN are from index plan, we push it to index plan, otherwise we finish the index plan and
		// push it to table plan.
		var pushedDownTopN *PhysicalTopN
		if !copTask.indexPlanFinished && p.canPushToIndexPlan(copTask.indexPlan, cols) {
			pushedDownTopN = p.getPushedDownTopN(copTask.indexPlan)
			copTask.indexPlan = pushedDownTopN
		} else {
			// It works for both normal index scan and index merge scan.
			copTask.finishIndexPlan()
			pushedDownTopN = p.getPushedDownTopN(copTask.tablePlan)
			copTask.tablePlan = pushedDownTopN
		}
	} else if mppTask, ok := t.(*MppTask); ok && needPushDown && p.canPushDownToTiFlash(mppTask) {
		pushedDownTopN := p.getPushedDownTopN(mppTask.p)
		mppTask.p = pushedDownTopN
	}
	rootTask := t.ConvertToRootTask(p.SCtx())
	// Skip TopN with partition on the root. This is a derived topN and window function
	// will take care of the filter.
	if len(p.GetPartitionBy()) > 0 {
		return t
	}
	return attachPlan2Task(p, rootTask)
}

// Attach2Task implements the PhysicalPlan interface.
func (p *PhysicalExpand) Attach2Task(tasks ...base.Task) base.Task {
	t := tasks[0].Copy()
	// current expand can only be run in MPP TiFlash mode.
	if mpp, ok := t.(*MppTask); ok {
		p.SetChildren(mpp.p)
		mpp.p = p
		return mpp
	}
	return base.InvalidTask
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalProjection) Attach2Task(tasks ...base.Task) base.Task {
	t := tasks[0].Copy()
	if cop, ok := t.(*CopTask); ok {
		if (len(cop.rootTaskConds) == 0 && len(cop.idxMergePartPlans) == 0) && expression.CanExprsPushDown(GetPushDownCtx(p.SCtx()), p.Exprs, cop.getStoreType()) {
			copTask := attachPlan2Task(p, cop)
			return copTask
		}
	} else if mpp, ok := t.(*MppTask); ok {
		if expression.CanExprsPushDown(GetPushDownCtx(p.SCtx()), p.Exprs, kv.TiFlash) {
			p.SetChildren(mpp.p)
			mpp.p = p
			return mpp
		}
	}
	t = t.ConvertToRootTask(p.SCtx())
	t = attachPlan2Task(p, t)
	if root, ok := tasks[0].(*RootTask); ok && root.IsEmpty() {
		t.(*RootTask).SetEmpty(true)
	}
	return t
}

func (p *PhysicalUnionAll) attach2MppTasks(tasks ...base.Task) base.Task {
	t := &MppTask{p: p}
	childPlans := make([]base.PhysicalPlan, 0, len(tasks))
	for _, tk := range tasks {
		if mpp, ok := tk.(*MppTask); ok && !tk.Invalid() {
			childPlans = append(childPlans, mpp.Plan())
		} else if root, ok := tk.(*RootTask); ok && root.IsEmpty() {
			continue
		} else {
			return base.InvalidTask
		}
	}
	if len(childPlans) == 0 {
		return base.InvalidTask
	}
	p.SetChildren(childPlans...)
	return t
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalUnionAll) Attach2Task(tasks ...base.Task) base.Task {
	for _, t := range tasks {
		if _, ok := t.(*MppTask); ok {
			if p.TP() == plancodec.TypePartitionUnion {
				// In attach2MppTasks(), will attach PhysicalUnion to mppTask directly.
				// But PartitionUnion cannot pushdown to tiflash, so here disable PartitionUnion pushdown to tiflash explicitly.
				// For now, return base.InvalidTask immediately, we can refine this by letting childTask of PartitionUnion convert to rootTask.
				return base.InvalidTask
			}
			return p.attach2MppTasks(tasks...)
		}
	}
	t := &RootTask{}
	t.SetPlan(p)
	childPlans := make([]base.PhysicalPlan, 0, len(tasks))
	for _, task := range tasks {
		task = task.ConvertToRootTask(p.SCtx())
		childPlans = append(childPlans, task.Plan())
	}
	p.SetChildren(childPlans...)
	return t
}

// Attach2Task implements PhysicalPlan interface.
func (sel *PhysicalSelection) Attach2Task(tasks ...base.Task) base.Task {
	if mppTask, _ := tasks[0].(*MppTask); mppTask != nil { // always push to mpp task.
		if expression.CanExprsPushDown(GetPushDownCtx(sel.SCtx()), sel.Conditions, kv.TiFlash) {
			return attachPlan2Task(sel, mppTask.Copy())
		}
	}
	t := tasks[0].ConvertToRootTask(sel.SCtx())
	return attachPlan2Task(sel, t)
}

// CheckAggCanPushCop checks whether the aggFuncs and groupByItems can
// be pushed down to coprocessor.
func CheckAggCanPushCop(sctx base.PlanContext, aggFuncs []*aggregation.AggFuncDesc, groupByItems []expression.Expression, storeType kv.StoreType) bool {
	sc := sctx.GetSessionVars().StmtCtx
	ret := true
	reason := ""
	pushDownCtx := GetPushDownCtx(sctx)
	for _, aggFunc := range aggFuncs {
		// if the aggFunc contain VirtualColumn or CorrelatedColumn, it can not be pushed down.
		if expression.ContainVirtualColumn(aggFunc.Args) || expression.ContainCorrelatedColumn(aggFunc.Args) {
			reason = "expressions of AggFunc `" + aggFunc.Name + "` contain virtual column or correlated column, which is not supported now"
			ret = false
			break
		}
		if !aggregation.CheckAggPushDown(sctx.GetExprCtx().GetEvalCtx(), aggFunc, storeType) {
			reason = "AggFunc `" + aggFunc.Name + "` is not supported now"
			ret = false
			break
		}
		if !expression.CanExprsPushDownWithExtraInfo(GetPushDownCtx(sctx), aggFunc.Args, storeType, aggFunc.Name == ast.AggFuncSum) {
			reason = "arguments of AggFunc `" + aggFunc.Name + "` contains unsupported exprs"
			ret = false
			break
		}
		orderBySize := len(aggFunc.OrderByItems)
		if orderBySize > 0 {
			exprs := make([]expression.Expression, 0, orderBySize)
			for _, item := range aggFunc.OrderByItems {
				exprs = append(exprs, item.Expr)
			}
			if !expression.CanExprsPushDownWithExtraInfo(GetPushDownCtx(sctx), exprs, storeType, false) {
				reason = "arguments of AggFunc `" + aggFunc.Name + "` contains unsupported exprs in order-by clause"
				ret = false
				break
			}
		}
		pb, _ := aggregation.AggFuncToPBExpr(pushDownCtx, aggFunc, storeType)
		if pb == nil {
			reason = "AggFunc `" + aggFunc.Name + "` can not be converted to pb expr"
			ret = false
			break
		}
	}
	if ret && expression.ContainVirtualColumn(groupByItems) {
		reason = "groupByItems contain virtual columns, which is not supported now"
		ret = false
	}
	if ret && !expression.CanExprsPushDown(GetPushDownCtx(sctx), groupByItems, storeType) {
		reason = "groupByItems contain unsupported exprs"
		ret = false
	}

	if !ret {
		storageName := storeType.Name()
		if storeType == kv.UnSpecified {
			storageName = "storage layer"
		}
		warnErr := errors.NewNoStackError("Aggregation can not be pushed to " + storageName + " because " + reason)
		if sc.InExplainStmt {
			sc.AppendWarning(warnErr)
		} else {
			sc.AppendExtraWarning(warnErr)
		}
	}
	return ret
}

// AggInfo stores the information of an Aggregation.
type AggInfo struct {
	AggFuncs     []*aggregation.AggFuncDesc
	GroupByItems []expression.Expression
	Schema       *expression.Schema
}

// BuildFinalModeAggregation splits either LogicalAggregation or PhysicalAggregation to finalAgg and partial1Agg,
// returns the information of partial and final agg.
// partialIsCop means whether partial agg is a cop task. When partialIsCop is false,
// we do not set the AggMode for partialAgg cause it may be split further when
// building the aggregate executor(e.g. buildHashAgg will split the AggDesc further for parallel executing).
// firstRowFuncMap is a map between partial first_row to final first_row, will be used in RemoveUnnecessaryFirstRow
func BuildFinalModeAggregation(
	sctx base.PlanContext, original *AggInfo, partialIsCop bool, isMPPTask bool) (partial, final *AggInfo, firstRowFuncMap map[*aggregation.AggFuncDesc]*aggregation.AggFuncDesc) {
	ectx := sctx.GetExprCtx().GetEvalCtx()

	firstRowFuncMap = make(map[*aggregation.AggFuncDesc]*aggregation.AggFuncDesc, len(original.AggFuncs))
	partial = &AggInfo{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, len(original.AggFuncs)),
		GroupByItems: original.GroupByItems,
		Schema:       expression.NewSchema(),
	}
	partialCursor := 0
	final = &AggInfo{
		AggFuncs:     make([]*aggregation.AggFuncDesc, len(original.AggFuncs)),
		GroupByItems: make([]expression.Expression, 0, len(original.GroupByItems)),
		Schema:       original.Schema,
	}

	partialGbySchema := expression.NewSchema()
	// add group by columns
	for _, gbyExpr := range partial.GroupByItems {
		var gbyCol *expression.Column
		if col, ok := gbyExpr.(*expression.Column); ok {
			gbyCol = col
		} else {
			gbyCol = &expression.Column{
				UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  gbyExpr.GetType(ectx),
			}
		}
		partialGbySchema.Append(gbyCol)
		final.GroupByItems = append(final.GroupByItems, gbyCol)
	}

	// TODO: Refactor the way of constructing aggregation functions.
	// This for loop is ugly, but I do not find a proper way to reconstruct
	// it right away.

	// group_concat is special when pushing down, it cannot take the two phase execution if no distinct but with orderBy, and other cases are also different:
	// for example: group_concat([distinct] expr0, expr1[, order by expr2] separator ‘,’)
	// no distinct, no orderBy: can two phase
	// 		[final agg] group_concat(col#1,’,’)
	// 		[part  agg] group_concat(expr0, expr1,’,’) -> col#1
	// no distinct,  orderBy: only one phase
	// distinct, no orderBy: can two phase
	// 		[final agg] group_concat(distinct col#0, col#1,’,’)
	// 		[part  agg] group by expr0 ->col#0, expr1 -> col#1
	// distinct,  orderBy: can two phase
	// 		[final agg] group_concat(distinct col#0, col#1, order by col#2,’,’)
	// 		[part  agg] group by expr0 ->col#0, expr1 -> col#1; agg function: firstrow(expr2)-> col#2

	for i, aggFunc := range original.AggFuncs {
		finalAggFunc := &aggregation.AggFuncDesc{HasDistinct: false}
		finalAggFunc.Name = aggFunc.Name
		finalAggFunc.OrderByItems = aggFunc.OrderByItems
		args := make([]expression.Expression, 0, len(aggFunc.Args))
		if aggFunc.HasDistinct {
			/*
				eg: SELECT COUNT(DISTINCT a), SUM(b) FROM t GROUP BY c

				change from
					[root] group by: c, funcs:count(distinct a), funcs:sum(b)
				to
					[root] group by: c, funcs:count(distinct a), funcs:sum(b)
						[cop]: group by: c, a
			*/
			// onlyAddFirstRow means if the distinctArg does not occur in group by items,
			// it should be replaced with a firstrow() agg function, needed for the order by items of group_concat()
			getDistinctExpr := func(distinctArg expression.Expression, onlyAddFirstRow bool) (ret expression.Expression) {
				// 1. add all args to partial.GroupByItems
				foundInGroupBy := false
				for j, gbyExpr := range partial.GroupByItems {
					if gbyExpr.Equal(ectx, distinctArg) && gbyExpr.GetType(ectx).Equal(distinctArg.GetType(ectx)) {
						// if the two expressions exactly the same in terms of data types and collation, then can avoid it.
						foundInGroupBy = true
						ret = partialGbySchema.Columns[j]
						break
					}
				}
				if !foundInGroupBy {
					var gbyCol *expression.Column
					if col, ok := distinctArg.(*expression.Column); ok {
						gbyCol = col
					} else {
						gbyCol = &expression.Column{
							UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
							RetType:  distinctArg.GetType(ectx),
						}
					}
					// 2. add group by items if needed
					if !onlyAddFirstRow {
						partial.GroupByItems = append(partial.GroupByItems, distinctArg)
						partialGbySchema.Append(gbyCol)
						ret = gbyCol
					}
					// 3. add firstrow() if needed
					if !partialIsCop || onlyAddFirstRow {
						// if partial is a cop task, firstrow function is redundant since group by items are outputted
						// by group by schema, and final functions use group by schema as their arguments.
						// if partial agg is not cop, we must append firstrow function & schema, to output the group by
						// items.
						// maybe we can unify them sometime.
						// only add firstrow for order by items of group_concat()
						firstRow, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{distinctArg}, false)
						if err != nil {
							panic("NewAggFuncDesc FirstRow meets error: " + err.Error())
						}
						partial.AggFuncs = append(partial.AggFuncs, firstRow)
						newCol, _ := gbyCol.Clone().(*expression.Column)
						newCol.RetType = firstRow.RetTp
						partial.Schema.Append(newCol)
						if onlyAddFirstRow {
							ret = newCol
						}
						partialCursor++
					}
				}
				return ret
			}

			for j, distinctArg := range aggFunc.Args {
				// the last arg of ast.AggFuncGroupConcat is the separator, so just put it into the final agg
				if aggFunc.Name == ast.AggFuncGroupConcat && j+1 == len(aggFunc.Args) {
					args = append(args, distinctArg)
					continue
				}
				args = append(args, getDistinctExpr(distinctArg, false))
			}

			byItems := make([]*util.ByItems, 0, len(aggFunc.OrderByItems))
			for _, byItem := range aggFunc.OrderByItems {
				byItems = append(byItems, &util.ByItems{Expr: getDistinctExpr(byItem.Expr, true), Desc: byItem.Desc})
			}

			if aggFunc.HasDistinct && isMPPTask && aggFunc.GroupingID > 0 {
				// keep the groupingID as it was, otherwise the new split final aggregate's ganna lost its groupingID info.
				finalAggFunc.GroupingID = aggFunc.GroupingID
			}

			finalAggFunc.OrderByItems = byItems
			finalAggFunc.HasDistinct = aggFunc.HasDistinct
			// In logical optimize phase, the Agg->PartitionUnion->TableReader may become
			// Agg1->PartitionUnion->Agg2->TableReader, and the Agg2 is a partial aggregation.
			// So in the push down here, we need to add a new if-condition check:
			// If the original agg mode is partial already, the finalAggFunc's mode become Partial2.
			if aggFunc.Mode == aggregation.CompleteMode {
				finalAggFunc.Mode = aggregation.CompleteMode
			} else if aggFunc.Mode == aggregation.Partial1Mode || aggFunc.Mode == aggregation.Partial2Mode {
				finalAggFunc.Mode = aggregation.Partial2Mode
			}
		} else {
			if aggFunc.Name == ast.AggFuncGroupConcat && len(aggFunc.OrderByItems) > 0 {
				// group_concat can only run in one phase if it has order by items but without distinct property
				partial = nil
				final = original
				return
			}
			if aggregation.NeedCount(finalAggFunc.Name) {
				// only Avg and Count need count
				if isMPPTask && finalAggFunc.Name == ast.AggFuncCount {
					// For MPP base.Task, the final count() is changed to sum().
					// Note: MPP mode does not run avg() directly, instead, avg() -> sum()/(case when count() = 0 then 1 else count() end),
					// so we do not process it here.
					finalAggFunc.Name = ast.AggFuncSum
				} else {
					// avg branch
					ft := types.NewFieldType(mysql.TypeLonglong)
					ft.SetFlen(21)
					ft.SetCharset(charset.CharsetBin)
					ft.SetCollate(charset.CollationBin)
					partial.Schema.Append(&expression.Column{
						UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
						RetType:  ft,
					})
					args = append(args, partial.Schema.Columns[partialCursor])
					partialCursor++
				}
			}
			if finalAggFunc.Name == ast.AggFuncApproxCountDistinct {
				ft := types.NewFieldType(mysql.TypeString)
				ft.SetCharset(charset.CharsetBin)
				ft.SetCollate(charset.CollationBin)
				ft.AddFlag(mysql.NotNullFlag)
				partial.Schema.Append(&expression.Column{
					UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
					RetType:  ft,
				})
				args = append(args, partial.Schema.Columns[partialCursor])
				partialCursor++
			}
			if aggregation.NeedValue(finalAggFunc.Name) {
				partial.Schema.Append(&expression.Column{
					UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
					RetType:  original.Schema.Columns[i].GetType(ectx),
				})
				args = append(args, partial.Schema.Columns[partialCursor])
				partialCursor++
			}
			if aggFunc.Name == ast.AggFuncAvg {
				cntAgg := aggFunc.Clone()
				cntAgg.Name = ast.AggFuncCount
				err := cntAgg.TypeInfer(sctx.GetExprCtx())
				if err != nil { // must not happen
					partial = nil
					final = original
					return
				}
				partial.Schema.Columns[partialCursor-2].RetType = cntAgg.RetTp
				// we must call deep clone in this case, to avoid sharing the arguments.
				sumAgg := aggFunc.Clone()
				sumAgg.Name = ast.AggFuncSum
				sumAgg.TypeInfer4AvgSum(sumAgg.RetTp)
				partial.Schema.Columns[partialCursor-1].RetType = sumAgg.RetTp
				partial.AggFuncs = append(partial.AggFuncs, cntAgg, sumAgg)
			} else if aggFunc.Name == ast.AggFuncApproxCountDistinct || aggFunc.Name == ast.AggFuncGroupConcat {
				newAggFunc := aggFunc.Clone()
				newAggFunc.Name = aggFunc.Name
				newAggFunc.RetTp = partial.Schema.Columns[partialCursor-1].GetType(ectx)
				partial.AggFuncs = append(partial.AggFuncs, newAggFunc)
				if aggFunc.Name == ast.AggFuncGroupConcat {
					// append the last separator arg
					args = append(args, aggFunc.Args[len(aggFunc.Args)-1])
				}
			} else {
				// other agg desc just split into two parts
				partialFuncDesc := aggFunc.Clone()
				partial.AggFuncs = append(partial.AggFuncs, partialFuncDesc)
				if aggFunc.Name == ast.AggFuncFirstRow {
					firstRowFuncMap[partialFuncDesc] = finalAggFunc
				}
			}

			// In logical optimize phase, the Agg->PartitionUnion->TableReader may become
			// Agg1->PartitionUnion->Agg2->TableReader, and the Agg2 is a partial aggregation.
			// So in the push down here, we need to add a new if-condition check:
			// If the original agg mode is partial already, the finalAggFunc's mode become Partial2.
			if aggFunc.Mode == aggregation.CompleteMode {
				finalAggFunc.Mode = aggregation.FinalMode
			} else if aggFunc.Mode == aggregation.Partial1Mode || aggFunc.Mode == aggregation.Partial2Mode {
				finalAggFunc.Mode = aggregation.Partial2Mode
			}
		}

		finalAggFunc.Args = args
		finalAggFunc.RetTp = aggFunc.RetTp
		final.AggFuncs[i] = finalAggFunc
	}
	partial.Schema.Append(partialGbySchema.Columns...)
	if partialIsCop {
		for _, f := range partial.AggFuncs {
			f.Mode = aggregation.Partial1Mode
		}
	}
	return
}

// convertAvgForMPP converts avg(arg) to sum(arg)/(case when count(arg)=0 then 1 else count(arg) end), in detail:
// 1.rewrite avg() in the final aggregation to count() and sum(), and reconstruct its schema.
// 2.replace avg() with sum(arg)/(case when count(arg)=0 then 1 else count(arg) end) and reuse the original schema of the final aggregation.
// If there is no avg, nothing is changed and return nil.
func (p *basePhysicalAgg) convertAvgForMPP() *PhysicalProjection {
	newSchema := expression.NewSchema()
	newSchema.Keys = p.schema.Keys
	newSchema.UniqueKeys = p.schema.UniqueKeys
	newAggFuncs := make([]*aggregation.AggFuncDesc, 0, 2*len(p.AggFuncs))
	exprs := make([]expression.Expression, 0, 2*len(p.schema.Columns))
	// add agg functions schema
	for i, aggFunc := range p.AggFuncs {
		if aggFunc.Name == ast.AggFuncAvg {
			// inset a count(column)
			avgCount := aggFunc.Clone()
			avgCount.Name = ast.AggFuncCount
			err := avgCount.TypeInfer(p.SCtx().GetExprCtx())
			if err != nil { // must not happen
				return nil
			}
			newAggFuncs = append(newAggFuncs, avgCount)
			avgCountCol := &expression.Column{
				UniqueID: p.SCtx().GetSessionVars().AllocPlanColumnID(),
				RetType:  avgCount.RetTp,
			}
			newSchema.Append(avgCountCol)
			// insert a sum(column)
			avgSum := aggFunc.Clone()
			avgSum.Name = ast.AggFuncSum
			avgSum.TypeInfer4AvgSum(avgSum.RetTp)
			newAggFuncs = append(newAggFuncs, avgSum)
			avgSumCol := &expression.Column{
				UniqueID: p.schema.Columns[i].UniqueID,
				RetType:  avgSum.RetTp,
			}
			newSchema.Append(avgSumCol)
			// avgSumCol/(case when avgCountCol=0 then 1 else avgCountCol end)
			eq := expression.NewFunctionInternal(p.SCtx().GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), avgCountCol, expression.NewZero())
			caseWhen := expression.NewFunctionInternal(p.SCtx().GetExprCtx(), ast.Case, avgCountCol.RetType, eq, expression.NewOne(), avgCountCol)
			divide := expression.NewFunctionInternal(p.SCtx().GetExprCtx(), ast.Div, avgSumCol.RetType, avgSumCol, caseWhen)
			divide.(*expression.ScalarFunction).RetType = p.schema.Columns[i].RetType
			exprs = append(exprs, divide)
		} else {
			// other non-avg agg use the old schema as it did.
			newAggFuncs = append(newAggFuncs, aggFunc)
			newSchema.Append(p.schema.Columns[i])
			exprs = append(exprs, p.schema.Columns[i])
		}
	}
	// no avgs
	// for final agg, always add project due to in-compatibility between TiDB and TiFlash
	if len(p.schema.Columns) == len(newSchema.Columns) && !p.IsFinalAgg() {
		return nil
	}
	// add remaining columns to exprs
	for i := len(p.AggFuncs); i < len(p.schema.Columns); i++ {
		exprs = append(exprs, p.schema.Columns[i])
	}
	proj := PhysicalProjection{
		Exprs:                exprs,
		CalculateNoDelay:     false,
		AvoidColumnEvaluator: false,
	}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), p.GetChildReqProps(0).CloneEssentialFields())
	proj.SetSchema(p.schema)

	p.AggFuncs = newAggFuncs
	p.schema = newSchema

	return proj
}

func (p *basePhysicalAgg) newPartialAggregate(copTaskType kv.StoreType, isMPPTask bool) (partial, final base.PhysicalPlan) {
	// Check if this aggregation can push down.
	if !CheckAggCanPushCop(p.SCtx(), p.AggFuncs, p.GroupByItems, copTaskType) {
		return nil, p.self
	}
	partialPref, finalPref, firstRowFuncMap := BuildFinalModeAggregation(p.SCtx(), &AggInfo{
		AggFuncs:     p.AggFuncs,
		GroupByItems: p.GroupByItems,
		Schema:       p.Schema().Clone(),
	}, true, isMPPTask)
	if partialPref == nil {
		return nil, p.self
	}
	if p.TP() == plancodec.TypeStreamAgg && len(partialPref.GroupByItems) != len(finalPref.GroupByItems) {
		return nil, p.self
	}
	// Remove unnecessary FirstRow.
	partialPref.AggFuncs = RemoveUnnecessaryFirstRow(p.SCtx(),
		finalPref.GroupByItems, partialPref.AggFuncs, partialPref.GroupByItems, partialPref.Schema, firstRowFuncMap)
	if copTaskType == kv.TiDB {
		// For partial agg of TiDB cop task, since TiDB coprocessor reuse the TiDB executor,
		// and TiDB aggregation executor won't output the group by value,
		// so we need add `firstrow` aggregation function to output the group by value.
		aggFuncs, err := genFirstRowAggForGroupBy(p.SCtx(), partialPref.GroupByItems)
		if err != nil {
			return nil, p.self
		}
		partialPref.AggFuncs = append(partialPref.AggFuncs, aggFuncs...)
	}
	p.AggFuncs = partialPref.AggFuncs
	p.GroupByItems = partialPref.GroupByItems
	p.schema = partialPref.Schema
	partialAgg := p.self
	// Create physical "final" aggregation.
	prop := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	if p.TP() == plancodec.TypeStreamAgg {
		finalAgg := basePhysicalAgg{
			AggFuncs:     finalPref.AggFuncs,
			GroupByItems: finalPref.GroupByItems,
			MppRunMode:   p.MppRunMode,
		}.initForStream(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), prop)
		finalAgg.schema = finalPref.Schema
		return partialAgg, finalAgg
	}

	finalAgg := basePhysicalAgg{
		AggFuncs:     finalPref.AggFuncs,
		GroupByItems: finalPref.GroupByItems,
		MppRunMode:   p.MppRunMode,
	}.initForHash(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), prop)
	finalAgg.schema = finalPref.Schema
	// partialAgg and finalAgg use the same ref of stats
	return partialAgg, finalAgg
}

func (p *basePhysicalAgg) scale3StageForDistinctAgg() (bool, expression.GroupingSets) {
	if p.canUse3Stage4SingleDistinctAgg() {
		return true, nil
	}
	return p.canUse3Stage4MultiDistinctAgg()
}

// canUse3Stage4MultiDistinctAgg returns true if this agg can use 3 stage for multi distinct aggregation
func (p *basePhysicalAgg) canUse3Stage4MultiDistinctAgg() (can bool, gss expression.GroupingSets) {
	if !p.SCtx().GetSessionVars().Enable3StageDistinctAgg || !p.SCtx().GetSessionVars().Enable3StageMultiDistinctAgg || len(p.GroupByItems) > 0 {
		return false, nil
	}
	defer func() {
		// some clean work.
		if !can {
			for _, fun := range p.AggFuncs {
				fun.GroupingID = 0
			}
		}
	}()
	// groupingSets is alias of []GroupingSet, the below equal to = make([]GroupingSet, 0, 2)
	groupingSets := make(expression.GroupingSets, 0, 2)
	for _, fun := range p.AggFuncs {
		if fun.HasDistinct {
			if fun.Name != ast.AggFuncCount {
				// now only for multi count(distinct x)
				return false, nil
			}
			for _, arg := range fun.Args {
				// bail out when args are not simple column, see GitHub issue #35417
				if _, ok := arg.(*expression.Column); !ok {
					return false, nil
				}
			}
			// here it's a valid count distinct agg with normal column args, collecting its distinct expr.
			groupingSets = append(groupingSets, expression.GroupingSet{fun.Args})
			// groupingID now is the offset of target grouping in GroupingSets.
			// todo: it may be changed after grouping set merge in the future.
			fun.GroupingID = len(groupingSets)
		} else if len(fun.Args) > 1 {
			return false, nil
		}
		// banned group_concat(x order by y)
		if len(fun.OrderByItems) > 0 || fun.Mode != aggregation.CompleteMode {
			return false, nil
		}
	}
	compressed := groupingSets.Merge()
	if len(compressed) != len(groupingSets) {
		p.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("Some grouping sets should be merged"))
		// todo arenatlx: some grouping set should be merged which is not supported by now temporarily.
		return false, nil
	}
	if groupingSets.NeedCloneColumn() {
		// todo: column clone haven't implemented.
		return false, nil
	}
	if len(groupingSets) > 1 {
		// fill the grouping ID for normal agg.
		for _, fun := range p.AggFuncs {
			if fun.GroupingID == 0 {
				// the grouping ID hasn't set. find the targeting grouping set.
				groupingSetOffset := groupingSets.TargetOne(fun.Args)
				if groupingSetOffset == -1 {
					// todo: if we couldn't find a existed current valid group layout, we need to copy the column out from being filled with null value.
					p.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("couldn't find a proper group set for normal agg"))
					return false, nil
				}
				// starting with 1
				fun.GroupingID = groupingSetOffset + 1
			}
		}
		return true, groupingSets
	}
	return false, nil
}

// canUse3Stage4SingleDistinctAgg returns true if this agg can use 3 stage for distinct aggregation
func (p *basePhysicalAgg) canUse3Stage4SingleDistinctAgg() bool {
	num := 0
	if !p.SCtx().GetSessionVars().Enable3StageDistinctAgg || len(p.GroupByItems) > 0 {
		return false
	}
	for _, fun := range p.AggFuncs {
		if fun.HasDistinct {
			num++
			if num > 1 || fun.Name != ast.AggFuncCount {
				return false
			}
			for _, arg := range fun.Args {
				// bail out when args are not simple column, see GitHub issue #35417
				if _, ok := arg.(*expression.Column); !ok {
					return false
				}
			}
		} else if len(fun.Args) > 1 {
			return false
		}

		if len(fun.OrderByItems) > 0 || fun.Mode != aggregation.CompleteMode {
			return false
		}
	}
	return num == 1
}

func genFirstRowAggForGroupBy(ctx base.PlanContext, groupByItems []expression.Expression) ([]*aggregation.AggFuncDesc, error) {
	aggFuncs := make([]*aggregation.AggFuncDesc, 0, len(groupByItems))
	for _, groupBy := range groupByItems {
		agg, err := aggregation.NewAggFuncDesc(ctx.GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{groupBy}, false)
		if err != nil {
			return nil, err
		}
		aggFuncs = append(aggFuncs, agg)
	}
	return aggFuncs, nil
}

// RemoveUnnecessaryFirstRow removes unnecessary FirstRow of the aggregation. This function can be
// used for both LogicalAggregation and PhysicalAggregation.
// When the select column is same with the group by key, the column can be removed and gets value from the group by key.
// e.g
// select a, count(b) from t group by a;
// The schema is [firstrow(a), count(b), a]. The column firstrow(a) is unnecessary.
// Can optimize the schema to [count(b), a] , and change the index to get value.
func RemoveUnnecessaryFirstRow(
	sctx base.PlanContext,
	finalGbyItems []expression.Expression,
	partialAggFuncs []*aggregation.AggFuncDesc,
	partialGbyItems []expression.Expression,
	partialSchema *expression.Schema,
	firstRowFuncMap map[*aggregation.AggFuncDesc]*aggregation.AggFuncDesc) []*aggregation.AggFuncDesc {
	partialCursor := 0
	newAggFuncs := make([]*aggregation.AggFuncDesc, 0, len(partialAggFuncs))
	for _, aggFunc := range partialAggFuncs {
		if aggFunc.Name == ast.AggFuncFirstRow {
			canOptimize := false
			for j, gbyExpr := range partialGbyItems {
				if j >= len(finalGbyItems) {
					// after distinct push, len(partialGbyItems) may larger than len(finalGbyItems)
					// for example,
					// select /*+ HASH_AGG() */ a, count(distinct a) from t;
					// will generate to,
					//   HashAgg root  funcs:count(distinct a), funcs:firstrow(a)"
					//     HashAgg cop  group by:a, funcs:firstrow(a)->Column#6"
					// the firstrow in root task can not be removed.
					break
				}
				// Skip if it's a constant.
				// For SELECT DISTINCT SQRT(1) FROM t.
				// We shouldn't remove the firstrow(SQRT(1)).
				if _, ok := gbyExpr.(*expression.Constant); ok {
					continue
				}
				if gbyExpr.Equal(sctx.GetExprCtx().GetEvalCtx(), aggFunc.Args[0]) {
					canOptimize = true
					firstRowFuncMap[aggFunc].Args[0] = finalGbyItems[j]
					break
				}
			}
			if canOptimize {
				partialSchema.Columns = append(partialSchema.Columns[:partialCursor], partialSchema.Columns[partialCursor+1:]...)
				continue
			}
		}
		partialCursor += computePartialCursorOffset(aggFunc.Name)
		newAggFuncs = append(newAggFuncs, aggFunc)
	}
	return newAggFuncs
}

func computePartialCursorOffset(name string) int {
	offset := 0
	if aggregation.NeedCount(name) {
		offset++
	}
	if aggregation.NeedValue(name) {
		offset++
	}
	if name == ast.AggFuncApproxCountDistinct {
		offset++
	}
	return offset
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalStreamAgg) Attach2Task(tasks ...base.Task) base.Task {
	t := tasks[0].Copy()
	if cop, ok := t.(*CopTask); ok {
		// We should not push agg down across
		//  1. double read, since the data of second read is ordered by handle instead of index. The `extraHandleCol` is added
		//     if the double read needs to keep order. So we just use it to decided
		//     whether the following plan is double read with order reserved.
		//  2. the case that there's filters should be calculated on TiDB side.
		//  3. the case of index merge
		if (cop.indexPlan != nil && cop.tablePlan != nil && cop.keepOrder) || len(cop.rootTaskConds) > 0 || len(cop.idxMergePartPlans) > 0 {
			t = cop.ConvertToRootTask(p.SCtx())
			attachPlan2Task(p, t)
		} else {
			storeType := cop.getStoreType()
			// TiFlash doesn't support Stream Aggregation
			if storeType == kv.TiFlash && len(p.GroupByItems) > 0 {
				return base.InvalidTask
			}
			partialAgg, finalAgg := p.newPartialAggregate(storeType, false)
			if partialAgg != nil {
				if cop.tablePlan != nil {
					cop.finishIndexPlan()
					partialAgg.SetChildren(cop.tablePlan)
					cop.tablePlan = partialAgg
					// If needExtraProj is true, a projection will be created above the PhysicalIndexLookUpReader to make sure
					// the schema is the same as the original DataSource schema.
					// However, we pushed down the agg here, the partial agg was placed on the top of tablePlan, and the final
					// agg will be placed above the PhysicalIndexLookUpReader, and the schema will be set correctly for them.
					// If we add the projection again, the projection will be between the PhysicalIndexLookUpReader and
					// the partial agg, and the schema will be broken.
					cop.needExtraProj = false
				} else {
					partialAgg.SetChildren(cop.indexPlan)
					cop.indexPlan = partialAgg
				}
			}
			t = cop.ConvertToRootTask(p.SCtx())
			attachPlan2Task(finalAgg, t)
		}
	} else if mpp, ok := t.(*MppTask); ok {
		t = mpp.ConvertToRootTask(p.SCtx())
		attachPlan2Task(p, t)
	} else {
		attachPlan2Task(p, t)
	}
	return t
}

// cpuCostDivisor computes the concurrency to which we would amortize CPU cost
// for hash aggregation.
func (p *PhysicalHashAgg) cpuCostDivisor(hasDistinct bool) (divisor, con float64) {
	if hasDistinct {
		return 0, 0
	}
	sessionVars := p.SCtx().GetSessionVars()
	finalCon, partialCon := sessionVars.HashAggFinalConcurrency(), sessionVars.HashAggPartialConcurrency()
	// According to `ValidateSetSystemVar`, `finalCon` and `partialCon` cannot be less than or equal to 0.
	if finalCon == 1 && partialCon == 1 {
		return 0, 0
	}
	// It is tricky to decide which concurrency we should use to amortize CPU cost. Since cost of hash
	// aggregation is tend to be under-estimated as explained in `attach2Task`, we choose the smaller
	// concurrecy to make some compensation.
	return math.Min(float64(finalCon), float64(partialCon)), float64(finalCon + partialCon)
}

func (p *PhysicalHashAgg) attach2TaskForMpp1Phase(mpp *MppTask) base.Task {
	// 1-phase agg: when the partition columns can be satisfied, where the plan does not need to enforce Exchange
	// only push down the original agg
	proj := p.convertAvgForMPP()
	attachPlan2Task(p.self, mpp)
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
func (p *PhysicalHashAgg) scaleStats4GroupingSets(groupingSets expression.GroupingSets, groupingIDCol *expression.Column,
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
	for _, groupingSet := range groupingSets {
		// for every grouping set, pick its cols out, and combine with normal group cols to get the ndv.
		groupingSetCols := groupingSet.ExtractCols()
		groupingSetCols = append(groupingSetCols, normalGbyCols...)
		ndv, _ := cardinality.EstimateColsNDVWithMatchedLen(groupingSetCols, childSchema, childStats)
		sumNDV += ndv
	}
	// After group operator, all same rows are grouped into one row, that means all
	// change the sub-agg's stats
	if p.StatsInfo() != nil {
		// equivalence to a new cloned one. (cause finalAgg and partialAgg may share a same copy of stats)
		cpStats := p.StatsInfo().Scale(1)
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
					incrementNDV, _ := cardinality.EstimateColsDNVWithMatchedLenFromUniqueIDs(intersectionIDs, childSchema, childStats)
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
func (p *PhysicalHashAgg) adjust3StagePhaseAgg(partialAgg, finalAgg base.PhysicalPlan, canUse3StageAgg bool,
	groupingSets expression.GroupingSets, mpp *MppTask) (final, mid, part, proj4Part base.PhysicalPlan, _ error) {
	ectx := p.SCtx().GetExprCtx().GetEvalCtx()

	if !(partialAgg != nil && canUse3StageAgg) {
		// quick path: return the original finalAgg and partiAgg.
		return finalAgg, nil, partialAgg, nil, nil
	}
	if len(groupingSets) == 0 {
		// single distinct agg mode.
		clonedAgg, err := finalAgg.Clone()
		if err != nil {
			return nil, nil, nil, nil, err
		}

		// step1: adjust middle agg.
		middleHashAgg := clonedAgg.(*PhysicalHashAgg)
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
		middleHashAgg.schema = middleSchema

		// step2: adjust final agg.
		finalHashAgg := finalAgg.(*PhysicalHashAgg)
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
	stats := mpp.p.StatsInfo().Scale(float64(1))
	stats.RowCount = stats.RowCount * float64(len(groupingSets))
	physicalExpand := PhysicalExpand{
		GroupingSets: groupingSets,
	}.Init(p.SCtx(), stats, mpp.p.QueryBlockOffset())
	// generate a new column as groupingID to identify which this row is targeting for.
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.SetFlag(mysql.UnsignedFlag | mysql.NotNullFlag)
	groupingIDCol = &expression.Column{
		UniqueID: p.SCtx().GetSessionVars().AllocPlanColumnID(),
		RetType:  tp,
	}
	// append the physical expand op with groupingID column.
	physicalExpand.SetSchema(mpp.p.Schema().Clone())
	physicalExpand.schema.Append(groupingIDCol.(*expression.Column))
	physicalExpand.GroupingIDCol = groupingIDCol.(*expression.Column)
	// attach PhysicalExpand to mpp
	attachPlan2Task(physicalExpand, mpp)

	// having group sets
	clonedAgg, err := finalAgg.Clone()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	cloneHashAgg := clonedAgg.(*PhysicalHashAgg)
	// Clone(), it will share same base-plan elements from the finalAgg, including id,tp,stats. Make a new one here.
	cloneHashAgg.Plan = baseimpl.NewBasePlan(cloneHashAgg.SCtx(), cloneHashAgg.TP(), cloneHashAgg.QueryBlockOffset())
	cloneHashAgg.SetStats(finalAgg.StatsInfo()) // reuse the final agg stats here.

	// step1: adjust partial agg, for normal agg here, adjust it to target for specified group data.
	// Since we may substitute the first arg of normal agg with case-when expression here, append a
	// customized proj here rather than depending on postOptimize to insert a blunt one for us.
	//
	// proj4Partial output all the base col from lower op + caseWhen proj cols.
	proj4Partial := new(PhysicalProjection).Init(p.SCtx(), mpp.p.StatsInfo(), mpp.p.QueryBlockOffset())
	for _, col := range mpp.p.Schema().Columns {
		proj4Partial.Exprs = append(proj4Partial.Exprs, col)
	}
	proj4Partial.SetSchema(mpp.p.Schema().Clone())

	partialHashAgg := partialAgg.(*PhysicalHashAgg)
	partialHashAgg.GroupByItems = append(partialHashAgg.GroupByItems, groupingIDCol)
	partialHashAgg.schema.Append(groupingIDCol.(*expression.Column))
	// it will create a new stats for partial agg.
	partialHashAgg.scaleStats4GroupingSets(groupingSets, groupingIDCol.(*expression.Column), proj4Partial.Schema(), proj4Partial.StatsInfo())
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
	middleHashAgg.schema = middleSchema

	// step3: adjust final agg
	finalHashAgg := finalAgg.(*PhysicalHashAgg)
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

func (p *PhysicalHashAgg) attach2TaskForMpp(tasks ...base.Task) base.Task {
	ectx := p.SCtx().GetExprCtx().GetEvalCtx()

	t := tasks[0].Copy()
	mpp, ok := t.(*MppTask)
	if !ok {
		return base.InvalidTask
	}
	switch p.MppRunMode {
	case Mpp1Phase:
		// 1-phase agg: when the partition columns can be satisfied, where the plan does not need to enforce Exchange
		// only push down the original agg
		proj := p.convertAvgForMPP()
		attachPlan2Task(p, mpp)
		if proj != nil {
			attachPlan2Task(proj, mpp)
		}
		return mpp
	case Mpp2Phase:
		// TODO: when partition property is matched by sub-plan, we actually needn't do extra an exchange and final agg.
		proj := p.convertAvgForMPP()
		partialAgg, finalAgg := p.newPartialAggregate(kv.TiFlash, true)
		if partialAgg == nil {
			return base.InvalidTask
		}
		attachPlan2Task(partialAgg, mpp)
		partitionCols := p.MppPartitionCols
		if len(partitionCols) == 0 {
			items := finalAgg.(*PhysicalHashAgg).GroupByItems
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
		prop := &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.HashType, MPPPartitionCols: partitionCols}
		newMpp := mpp.enforceExchangerImpl(prop)
		if newMpp.Invalid() {
			return newMpp
		}
		attachPlan2Task(finalAgg, newMpp)
		// TODO: how to set 2-phase cost?
		if proj != nil {
			attachPlan2Task(proj, newMpp)
		}
		return newMpp
	case MppTiDB:
		partialAgg, finalAgg := p.newPartialAggregate(kv.TiFlash, false)
		if partialAgg != nil {
			attachPlan2Task(partialAgg, mpp)
		}
		t = mpp.ConvertToRootTask(p.SCtx())
		attachPlan2Task(finalAgg, t)
		return t
	case MppScalar:
		prop := &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.SinglePartitionType}
		if !mpp.needEnforceExchanger(prop) {
			// On the one hand: when the low layer already satisfied the single partition layout, just do the all agg computation in the single node.
			return p.attach2TaskForMpp1Phase(mpp)
		}
		// On the other hand: try to split the mppScalar agg into multi phases agg **down** to multi nodes since data already distributed across nodes.
		// we have to check it before the content of p has been modified
		canUse3StageAgg, groupingSets := p.scale3StageForDistinctAgg()
		proj := p.convertAvgForMPP()
		partialAgg, finalAgg := p.newPartialAggregate(kv.TiFlash, true)
		if finalAgg == nil {
			return base.InvalidTask
		}

		final, middle, partial, proj4Partial, err := p.adjust3StagePhaseAgg(partialAgg, finalAgg, canUse3StageAgg, groupingSets, mpp)
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
			items := partial.(*PhysicalHashAgg).GroupByItems
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
			newMpp := mpp.enforceExchanger(exProp)
			attachPlan2Task(middle, newMpp)
			mpp = newMpp
		}

		// prop here still be the first generated single-partition requirement.
		newMpp := mpp.enforceExchanger(prop)
		attachPlan2Task(final, newMpp)
		if proj == nil {
			proj = PhysicalProjection{
				Exprs: make([]expression.Expression, 0, len(p.Schema().Columns)),
			}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset())
			for _, col := range p.Schema().Columns {
				proj.Exprs = append(proj.Exprs, col)
			}
			proj.SetSchema(p.schema)
		}
		attachPlan2Task(proj, newMpp)
		return newMpp
	default:
		return base.InvalidTask
	}
}

// Attach2Task implements the PhysicalPlan interface.
func (p *PhysicalHashAgg) Attach2Task(tasks ...base.Task) base.Task {
	t := tasks[0].Copy()
	if cop, ok := t.(*CopTask); ok {
		if len(cop.rootTaskConds) == 0 && len(cop.idxMergePartPlans) == 0 {
			copTaskType := cop.getStoreType()
			partialAgg, finalAgg := p.newPartialAggregate(copTaskType, false)
			if partialAgg != nil {
				if cop.tablePlan != nil {
					cop.finishIndexPlan()
					partialAgg.SetChildren(cop.tablePlan)
					cop.tablePlan = partialAgg
					// If needExtraProj is true, a projection will be created above the PhysicalIndexLookUpReader to make sure
					// the schema is the same as the original DataSource schema.
					// However, we pushed down the agg here, the partial agg was placed on the top of tablePlan, and the final
					// agg will be placed above the PhysicalIndexLookUpReader, and the schema will be set correctly for them.
					// If we add the projection again, the projection will be between the PhysicalIndexLookUpReader and
					// the partial agg, and the schema will be broken.
					cop.needExtraProj = false
				} else {
					partialAgg.SetChildren(cop.indexPlan)
					cop.indexPlan = partialAgg
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
	} else if _, ok := t.(*MppTask); ok {
		return p.attach2TaskForMpp(tasks...)
	} else {
		attachPlan2Task(p, t)
	}
	return t
}

func (p *PhysicalWindow) attach2TaskForMPP(mpp *MppTask) base.Task {
	// FIXME: currently, tiflash's join has different schema with TiDB,
	// so we have to rebuild the schema of join and operators which may inherit schema from join.
	// for window, we take the sub-plan's schema, and the schema generated by windowDescs.
	columns := p.Schema().Clone().Columns[len(p.Schema().Columns)-len(p.WindowFuncDescs):]
	p.schema = expression.MergeSchema(mpp.Plan().Schema(), expression.NewSchema(columns...))

	failpoint.Inject("CheckMPPWindowSchemaLength", func() {
		if len(p.Schema().Columns) != len(mpp.Plan().Schema().Columns)+len(p.WindowFuncDescs) {
			panic("mpp physical window has incorrect schema length")
		}
	})

	return attachPlan2Task(p, mpp)
}

// Attach2Task implements the PhysicalPlan interface.
func (p *PhysicalWindow) Attach2Task(tasks ...base.Task) base.Task {
	if mpp, ok := tasks[0].Copy().(*MppTask); ok && p.storeTp == kv.TiFlash {
		return p.attach2TaskForMPP(mpp)
	}
	t := tasks[0].ConvertToRootTask(p.SCtx())
	return attachPlan2Task(p.self, t)
}

// Attach2Task implements the PhysicalPlan interface.
func (p *PhysicalCTEStorage) Attach2Task(tasks ...base.Task) base.Task {
	t := tasks[0].Copy()
	if mpp, ok := t.(*MppTask); ok {
		p.SetChildren(t.Plan())
		return &MppTask{
			p:           p,
			partTp:      mpp.partTp,
			hashCols:    mpp.hashCols,
			tblColHists: mpp.tblColHists,
		}
	}
	t.ConvertToRootTask(p.SCtx())
	p.SetChildren(t.Plan())
	ta := &RootTask{}
	ta.SetPlan(p)
	return ta
}

// Attach2Task implements the PhysicalPlan interface.
func (p *PhysicalSequence) Attach2Task(tasks ...base.Task) base.Task {
	for _, t := range tasks {
		_, isMpp := t.(*MppTask)
		if !isMpp {
			return tasks[len(tasks)-1]
		}
	}

	lastTask := tasks[len(tasks)-1].(*MppTask)

	children := make([]base.PhysicalPlan, 0, len(tasks))
	for _, t := range tasks {
		children = append(children, t.Plan())
	}

	p.SetChildren(children...)

	mppTask := &MppTask{
		p:           p,
		partTp:      lastTask.partTp,
		hashCols:    lastTask.hashCols,
		tblColHists: lastTask.tblColHists,
	}
	return mppTask
}

func collectPartitionInfosFromMPPPlan(p *PhysicalTableReader, mppPlan base.PhysicalPlan) {
	switch x := mppPlan.(type) {
	case *PhysicalTableScan:
		p.TableScanAndPartitionInfos = append(p.TableScanAndPartitionInfos, tableScanAndPartitionInfo{x, x.PlanPartInfo})
	default:
		for _, ch := range mppPlan.Children() {
			collectPartitionInfosFromMPPPlan(p, ch)
		}
	}
}

func collectRowSizeFromMPPPlan(mppPlan base.PhysicalPlan) (rowSize float64) {
	if mppPlan != nil && mppPlan.StatsInfo() != nil && mppPlan.StatsInfo().HistColl != nil {
		return cardinality.GetAvgRowSize(mppPlan.SCtx(), mppPlan.StatsInfo().HistColl, mppPlan.Schema().Columns, false, false)
	}
	return 1 // use 1 as lower-bound for safety
}

func accumulateNetSeekCost4MPP(p base.PhysicalPlan) (cost float64) {
	if ts, ok := p.(*PhysicalTableScan); ok {
		return float64(len(ts.Ranges)) * float64(len(ts.Columns)) * ts.SCtx().GetSessionVars().GetSeekFactor(ts.Table)
	}
	for _, c := range p.Children() {
		cost += accumulateNetSeekCost4MPP(c)
	}
	return
}

func tryExpandVirtualColumn(p base.PhysicalPlan) {
	if ts, ok := p.(*PhysicalTableScan); ok {
		ts.Columns = ExpandVirtualColumn(ts.Columns, ts.schema, ts.Table.Columns)
		return
	}
	for _, child := range p.Children() {
		tryExpandVirtualColumn(child)
	}
}

func (t *MppTask) needEnforceExchanger(prop *property.PhysicalProperty) bool {
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
		// TODO: consider equalivant class
		// TODO: `prop.IsSubsetOf` is enough, instead of equal.
		// for example, if already partitioned by hash(B,C), then same (A,B,C) must distribute on a same node.
		if len(prop.MPPPartitionCols) != len(t.hashCols) {
			return true
		}
		for i, col := range prop.MPPPartitionCols {
			if !col.Equal(t.hashCols[i]) {
				return true
			}
		}
		return false
	}
}

func (t *MppTask) enforceExchanger(prop *property.PhysicalProperty) *MppTask {
	if !t.needEnforceExchanger(prop) {
		return t
	}
	return t.Copy().(*MppTask).enforceExchangerImpl(prop)
}

func (t *MppTask) enforceExchangerImpl(prop *property.PhysicalProperty) *MppTask {
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
	return &MppTask{
		p:        receiver,
		partTp:   prop.MPPPartitionTp,
		hashCols: prop.MPPPartitionCols,
	}
}
