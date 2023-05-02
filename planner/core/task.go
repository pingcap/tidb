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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/paging"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tidb/util/size"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var (
	_ task = &copTask{}
	_ task = &rootTask{}
	_ task = &mppTask{}
)

// task is a new version of `PhysicalPlanInfo`. It stores cost information for a task.
// A task may be CopTask, RootTask, MPPTaskMeta or a ParallelTask.
type task interface {
	count() float64
	copy() task
	plan() PhysicalPlan
	invalid() bool
	convertToRootTask(ctx sessionctx.Context) *rootTask
	MemoryUsage() int64
}

// copTask is a task that runs in a distributed kv store.
// TODO: In future, we should split copTask to indexTask and tableTask.
type copTask struct {
	indexPlan PhysicalPlan
	tablePlan PhysicalPlan
	// indexPlanFinished means we have finished index plan.
	indexPlanFinished bool
	// keepOrder indicates if the plan scans data by order.
	keepOrder bool
	// needExtraProj means an extra prune is needed because
	// in double read / index merge cases, they may output one more column for handle(row id).
	needExtraProj bool
	// originSchema is the target schema to be projected to when needExtraProj is true.
	originSchema *expression.Schema

	extraHandleCol   *expression.Column
	commonHandleCols []*expression.Column
	// tblColHists stores the original stats of DataSource, it is used to get
	// average row width when computing network cost.
	tblColHists *statistics.HistColl
	// tblCols stores the original columns of DataSource before being pruned, it
	// is used to compute average row width when computing scan cost.
	tblCols []*expression.Column

	idxMergePartPlans      []PhysicalPlan
	idxMergeIsIntersection bool
	idxMergeAccessMVIndex  bool

	// rootTaskConds stores select conditions containing virtual columns.
	// These conditions can't push to TiKV, so we have to add a selection for rootTask
	rootTaskConds []expression.Expression

	// For table partition.
	partitionInfo PartitionInfo

	// expectCnt is the expected row count of upper task, 0 for unlimited.
	// It's used for deciding whether using paging distsql.
	expectCnt uint64
}

func (t *copTask) invalid() bool {
	return t.tablePlan == nil && t.indexPlan == nil
}

func (t *rootTask) invalid() bool {
	return t.p == nil
}

func (t *copTask) count() float64 {
	if t.indexPlanFinished {
		return t.tablePlan.statsInfo().RowCount
	}
	return t.indexPlan.statsInfo().RowCount
}

func (t *copTask) copy() task {
	nt := *t
	return &nt
}

func (t *copTask) plan() PhysicalPlan {
	if t.indexPlanFinished {
		return t.tablePlan
	}
	return t.indexPlan
}

func attachPlan2Task(p PhysicalPlan, t task) task {
	switch v := t.(type) {
	case *copTask:
		if v.indexPlanFinished {
			p.SetChildren(v.tablePlan)
			v.tablePlan = p
		} else {
			p.SetChildren(v.indexPlan)
			v.indexPlan = p
		}
	case *rootTask:
		p.SetChildren(v.p)
		v.p = p
	case *mppTask:
		p.SetChildren(v.p)
		v.p = p
	}
	return t
}

// finishIndexPlan means we no longer add plan to index plan, and compute the network cost for it.
func (t *copTask) finishIndexPlan() {
	if t.indexPlanFinished {
		return
	}
	t.indexPlanFinished = true
	// index merge case is specially handled for now.
	// We need a elegant way to solve the stats of index merge in this case.
	if t.tablePlan != nil && t.indexPlan != nil {
		ts := t.tablePlan.(*PhysicalTableScan)
		originStats := ts.stats
		ts.stats = t.indexPlan.statsInfo()
		if originStats != nil {
			// keep the original stats version
			ts.stats.StatsVersion = originStats.StatsVersion
		}
	}
}

func (t *copTask) getStoreType() kv.StoreType {
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

// MemoryUsage return the memory usage of copTask
func (t *copTask) MemoryUsage() (sum int64) {
	if t == nil {
		return
	}

	sum = size.SizeOfInterface*(2+int64(cap(t.idxMergePartPlans)+cap(t.rootTaskConds))) + size.SizeOfBool*3 + size.SizeOfUint64 +
		size.SizeOfPointer*(3+int64(cap(t.commonHandleCols)+cap(t.tblCols))) + size.SizeOfSlice*4 + t.partitionInfo.MemoryUsage()
	if t.indexPlan != nil {
		sum += t.indexPlan.MemoryUsage()
	}
	if t.tablePlan != nil {
		sum += t.tablePlan.MemoryUsage()
	}
	if t.originSchema != nil {
		sum += t.originSchema.MemoryUsage()
	}
	if t.extraHandleCol != nil {
		sum += t.extraHandleCol.MemoryUsage()
	}

	for _, col := range t.commonHandleCols {
		sum += col.MemoryUsage()
	}
	for _, col := range t.tblCols {
		sum += col.MemoryUsage()
	}
	for _, p := range t.idxMergePartPlans {
		sum += p.MemoryUsage()
	}
	for _, expr := range t.rootTaskConds {
		sum += expr.MemoryUsage()
	}
	return
}

func (p *basePhysicalPlan) attach2Task(tasks ...task) task {
	t := tasks[0].convertToRootTask(p.ctx)
	return attachPlan2Task(p.self, t)
}

func (p *PhysicalUnionScan) attach2Task(tasks ...task) task {
	// We need to pull the projection under unionScan upon unionScan.
	// Since the projection only prunes columns, it's ok the put it upon unionScan.
	if sel, ok := tasks[0].plan().(*PhysicalSelection); ok {
		if pj, ok := sel.children[0].(*PhysicalProjection); ok {
			// Convert unionScan->selection->projection to projection->unionScan->selection.
			sel.SetChildren(pj.children...)
			p.SetChildren(sel)
			p.stats = tasks[0].plan().statsInfo()
			rt, _ := tasks[0].(*rootTask)
			rt.p = p
			pj.SetChildren(p)
			return pj.attach2Task(tasks...)
		}
	}
	if pj, ok := tasks[0].plan().(*PhysicalProjection); ok {
		// Convert unionScan->projection to projection->unionScan, because unionScan can't handle projection as its children.
		p.SetChildren(pj.children...)
		p.stats = tasks[0].plan().statsInfo()
		rt, _ := tasks[0].(*rootTask)
		rt.p = pj.children[0]
		pj.SetChildren(p)
		return pj.attach2Task(p.basePhysicalPlan.attach2Task(tasks...))
	}
	p.stats = tasks[0].plan().statsInfo()
	return p.basePhysicalPlan.attach2Task(tasks...)
}

func (p *PhysicalApply) attach2Task(tasks ...task) task {
	lTask := tasks[0].convertToRootTask(p.ctx)
	rTask := tasks[1].convertToRootTask(p.ctx)
	p.SetChildren(lTask.plan(), rTask.plan())
	p.schema = BuildPhysicalJoinSchema(p.JoinType, p)
	t := &rootTask{
		p: p,
	}
	return t
}

func (p *PhysicalIndexMergeJoin) attach2Task(tasks ...task) task {
	innerTask := p.innerTask
	outerTask := tasks[1-p.InnerChildIdx].convertToRootTask(p.ctx)
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.plan(), innerTask.plan())
	} else {
		p.SetChildren(innerTask.plan(), outerTask.plan())
	}
	t := &rootTask{
		p: p,
	}
	return t
}

func (p *PhysicalIndexHashJoin) attach2Task(tasks ...task) task {
	innerTask := p.innerTask
	outerTask := tasks[1-p.InnerChildIdx].convertToRootTask(p.ctx)
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.plan(), innerTask.plan())
	} else {
		p.SetChildren(innerTask.plan(), outerTask.plan())
	}
	t := &rootTask{
		p: p,
	}
	return t
}

func (p *PhysicalIndexJoin) attach2Task(tasks ...task) task {
	innerTask := p.innerTask
	outerTask := tasks[1-p.InnerChildIdx].convertToRootTask(p.ctx)
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.plan(), innerTask.plan())
	} else {
		p.SetChildren(innerTask.plan(), outerTask.plan())
	}
	t := &rootTask{
		p: p,
	}
	return t
}

func getAvgRowSize(stats *property.StatsInfo, cols []*expression.Column) (size float64) {
	if stats.HistColl != nil {
		size = stats.HistColl.GetAvgRowSizeListInDisk(cols)
	} else {
		// Estimate using just the type info.
		for _, col := range cols {
			size += float64(chunk.EstimateTypeWidth(col.GetType()))
		}
	}
	return
}

func (p *PhysicalHashJoin) attach2Task(tasks ...task) task {
	if p.storeTp == kv.TiFlash {
		return p.attach2TaskForTiFlash(tasks...)
	}
	lTask := tasks[0].convertToRootTask(p.ctx)
	rTask := tasks[1].convertToRootTask(p.ctx)
	p.SetChildren(lTask.plan(), rTask.plan())
	task := &rootTask{
		p: p,
	}
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
		cLen := mathutil.Max(lLen, rLen)
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

func getProj(ctx sessionctx.Context, p PhysicalPlan) *PhysicalProjection {
	proj := PhysicalProjection{
		Exprs: make([]expression.Expression, 0, len(p.Schema().Columns)),
	}.Init(ctx, p.statsInfo(), p.SelectBlockOffset())
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
		UniqueID: p.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  expr.GetType(),
	}
	col.SetCoercibility(expr.Coercibility())
	p.schema.Append(col)
	return col
}

// TiFlash join require that partition key has exactly the same type, while TiDB only guarantee the partition key is the same catalog,
// so if the partition key type is not exactly the same, we need add a projection below the join or exchanger if exists.
func (p *PhysicalHashJoin) convertPartitionKeysIfNeed(lTask, rTask *mppTask) (*mppTask, *mppTask) {
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
		lProj = getProj(p.ctx, lp)
		lp = lProj
	}
	if rChanged {
		rProj = getProj(p.ctx, rp)
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
			lCast := expression.BuildCastFunction(p.ctx, lKey.Col, cType)
			lKey = &property.MPPPartitionColumn{Col: appendExpr(lProj, lCast), CollateID: lKey.CollateID}
		}
		if rMask[i] {
			cType := cTypes[i].Clone()
			cType.SetFlag(rKey.Col.RetType.GetFlag())
			rCast := expression.BuildCastFunction(p.ctx, rKey.Col, cType)
			rKey = &property.MPPPartitionColumn{Col: appendExpr(rProj, rCast), CollateID: rKey.CollateID}
		}
		lPartKeys = append(lPartKeys, lKey)
		rPartKeys = append(rPartKeys, rKey)
	}
	// if left or right child changes, we need to add enforcer.
	if lChanged {
		nlTask := lTask.copy().(*mppTask)
		nlTask.p = lProj
		nlTask = nlTask.enforceExchanger(&property.PhysicalProperty{
			TaskTp:           property.MppTaskType,
			MPPPartitionTp:   property.HashType,
			MPPPartitionCols: lPartKeys,
		})
		lTask = nlTask
	}
	if rChanged {
		nrTask := rTask.copy().(*mppTask)
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

func (p *PhysicalHashJoin) attach2TaskForMpp(tasks ...task) task {
	lTask, lok := tasks[0].(*mppTask)
	rTask, rok := tasks[1].(*mppTask)
	if !lok || !rok {
		return invalidTask
	}
	if p.mppShuffleJoin {
		// protection check is case of some bugs
		if len(lTask.hashCols) != len(rTask.hashCols) || len(lTask.hashCols) == 0 {
			return invalidTask
		}
		lTask, rTask = p.convertPartitionKeysIfNeed(lTask, rTask)
	}
	p.SetChildren(lTask.plan(), rTask.plan())
	p.schema = BuildPhysicalJoinSchema(p.JoinType, p)

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
	task := &mppTask{
		p:        p,
		partTp:   outerTask.partTp,
		hashCols: outerTask.hashCols,
	}
	return task
}

func (p *PhysicalHashJoin) attach2TaskForTiFlash(tasks ...task) task {
	lTask, lok := tasks[0].(*copTask)
	rTask, rok := tasks[1].(*copTask)
	if !lok || !rok {
		return p.attach2TaskForMpp(tasks...)
	}
	p.SetChildren(lTask.plan(), rTask.plan())
	p.schema = BuildPhysicalJoinSchema(p.JoinType, p)
	if !lTask.indexPlanFinished {
		lTask.finishIndexPlan()
	}
	if !rTask.indexPlanFinished {
		rTask.finishIndexPlan()
	}

	task := &copTask{
		tblColHists:       rTask.tblColHists,
		indexPlanFinished: true,
		tablePlan:         p,
	}
	return task
}

func (p *PhysicalMergeJoin) attach2Task(tasks ...task) task {
	lTask := tasks[0].convertToRootTask(p.ctx)
	rTask := tasks[1].convertToRootTask(p.ctx)
	p.SetChildren(lTask.plan(), rTask.plan())
	t := &rootTask{
		p: p,
	}
	return t
}

func buildIndexLookUpTask(ctx sessionctx.Context, t *copTask) *rootTask {
	newTask := &rootTask{}
	p := PhysicalIndexLookUpReader{
		tablePlan:        t.tablePlan,
		indexPlan:        t.indexPlan,
		ExtraHandleCol:   t.extraHandleCol,
		CommonHandleCols: t.commonHandleCols,
		expectedCnt:      t.expectCnt,
		keepOrder:        t.keepOrder,
	}.Init(ctx, t.tablePlan.SelectBlockOffset())
	p.PartitionInfo = t.partitionInfo
	setTableScanToTableRowIDScan(p.tablePlan)
	p.stats = t.tablePlan.statsInfo()

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
		proj := PhysicalProjection{Exprs: expression.Column2Exprs(schema.Columns)}.Init(ctx, p.stats, t.tablePlan.SelectBlockOffset(), nil)
		proj.SetSchema(schema)
		proj.SetChildren(p)
		newTask.p = proj
	} else {
		newTask.p = p
	}
	return newTask
}

func extractRows(p PhysicalPlan) float64 {
	f := float64(0)
	for _, c := range p.Children() {
		if len(c.Children()) != 0 {
			f += extractRows(c)
		} else {
			f += c.statsInfo().RowCount
		}
	}
	return f
}

// calcPagingCost calculates the cost for paging processing which may increase the seekCnt and reduce scanned rows.
func calcPagingCost(ctx sessionctx.Context, indexPlan PhysicalPlan, expectCnt uint64) float64 {
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

func (t *rootTask) convertToRootTask(_ sessionctx.Context) *rootTask {
	return t.copy().(*rootTask)
}

func (t *copTask) convertToRootTask(ctx sessionctx.Context) *rootTask {
	// copy one to avoid changing itself.
	return t.copy().(*copTask).convertToRootTaskImpl(ctx)
}

func (t *copTask) convertToRootTaskImpl(ctx sessionctx.Context) *rootTask {
	// copTasks are run in parallel, to make the estimated cost closer to execution time, we amortize
	// the cost to cop iterator workers. According to `CopClient::Send`, the concurrency
	// is Min(DistSQLScanConcurrency, numRegionsInvolvedInScan), since we cannot infer
	// the number of regions involved, we simply use DistSQLScanConcurrency.
	t.finishIndexPlan()
	// Network cost of transferring rows of table scan to TiDB.
	if t.tablePlan != nil {
		tp := t.tablePlan
		for len(tp.Children()) > 0 {
			if len(tp.Children()) == 1 {
				tp = tp.Children()[0]
			} else {
				join := tp.(*PhysicalHashJoin)
				tp = join.children[1-join.InnerChildIdx]
			}
		}
		ts := tp.(*PhysicalTableScan)
		prevColumnLen := len(ts.Columns)
		prevSchema := ts.schema.Clone()
		ts.Columns = ExpandVirtualColumn(ts.Columns, ts.schema, ts.Table.Columns)
		if !t.needExtraProj && len(ts.Columns) > prevColumnLen {
			// Add an projection to make sure not to output extract columns.
			t.needExtraProj = true
			t.originSchema = prevSchema
		}
	}
	newTask := &rootTask{}
	if t.idxMergePartPlans != nil {
		p := PhysicalIndexMergeReader{
			partialPlans:       t.idxMergePartPlans,
			tablePlan:          t.tablePlan,
			IsIntersectionType: t.idxMergeIsIntersection,
			AccessMVIndex:      t.idxMergeAccessMVIndex,
		}.Init(ctx, t.idxMergePartPlans[0].SelectBlockOffset())
		p.PartitionInfo = t.partitionInfo
		setTableScanToTableRowIDScan(p.tablePlan)
		newTask.p = p
		t.handleRootTaskConds(ctx, newTask)
		if t.needExtraProj {
			schema := t.originSchema
			proj := PhysicalProjection{Exprs: expression.Column2Exprs(schema.Columns)}.Init(ctx, p.stats, t.idxMergePartPlans[0].SelectBlockOffset(), nil)
			proj.SetSchema(schema)
			proj.SetChildren(p)
			newTask.p = proj
		}
		return newTask
	}
	if t.indexPlan != nil && t.tablePlan != nil {
		newTask = buildIndexLookUpTask(ctx, t)
	} else if t.indexPlan != nil {
		p := PhysicalIndexReader{indexPlan: t.indexPlan}.Init(ctx, t.indexPlan.SelectBlockOffset())
		p.PartitionInfo = t.partitionInfo
		p.stats = t.indexPlan.statsInfo()
		newTask.p = p
	} else {
		tp := t.tablePlan
		for len(tp.Children()) > 0 {
			if len(tp.Children()) == 1 {
				tp = tp.Children()[0]
			} else {
				join := tp.(*PhysicalHashJoin)
				tp = join.children[1-join.InnerChildIdx]
			}
		}
		ts := tp.(*PhysicalTableScan)
		p := PhysicalTableReader{
			tablePlan:      t.tablePlan,
			StoreType:      ts.StoreType,
			IsCommonHandle: ts.Table.IsCommonHandle,
		}.Init(ctx, t.tablePlan.SelectBlockOffset())
		p.PartitionInfo = t.partitionInfo
		p.stats = t.tablePlan.statsInfo()

		// If agg was pushed down in attach2Task(), the partial agg was placed on the top of tablePlan, the final agg was
		// placed above the PhysicalTableReader, and the schema should have been set correctly for them, the schema of
		// partial agg contains the columns needed by the final agg.
		// If we add the projection here, the projection will be between the final agg and the partial agg, then the
		// schema will be broken, the final agg will fail to find needed columns in ResolveIndices().
		// Besides, the agg would only be pushed down if it doesn't contain virtual columns, so virtual column should not be affected.
		aggPushedDown := false
		switch p.tablePlan.(type) {
		case *PhysicalHashAgg, *PhysicalStreamAgg:
			aggPushedDown = true
		}

		if t.needExtraProj && !aggPushedDown {
			proj := PhysicalProjection{Exprs: expression.Column2Exprs(t.originSchema.Columns)}.Init(ts.ctx, ts.stats, ts.SelectBlockOffset(), nil)
			proj.SetSchema(t.originSchema)
			proj.SetChildren(p)
			newTask.p = proj
		} else {
			newTask.p = p
		}
	}

	t.handleRootTaskConds(ctx, newTask)
	return newTask
}

func (t *copTask) handleRootTaskConds(ctx sessionctx.Context, newTask *rootTask) {
	if len(t.rootTaskConds) > 0 {
		selectivity, _, err := t.tblColHists.Selectivity(ctx, t.rootTaskConds, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = SelectionFactor
		}
		sel := PhysicalSelection{Conditions: t.rootTaskConds}.Init(ctx, newTask.p.statsInfo().Scale(selectivity), newTask.p.SelectBlockOffset())
		sel.fromDataSource = true
		sel.SetChildren(newTask.p)
		newTask.p = sel
	}
}

// setTableScanToTableRowIDScan is to update the isChildOfIndexLookUp attribute of PhysicalTableScan child
func setTableScanToTableRowIDScan(p PhysicalPlan) {
	if ts, ok := p.(*PhysicalTableScan); ok {
		ts.SetIsChildOfIndexLookUp(true)
	} else {
		for _, child := range p.Children() {
			setTableScanToTableRowIDScan(child)
		}
	}
}

// rootTask is the final sink node of a plan graph. It should be a single goroutine on tidb.
type rootTask struct {
	p       PhysicalPlan
	isEmpty bool // isEmpty indicates if this task contains a dual table and returns empty data.
	// TODO: The flag 'isEmpty' is only checked by Projection and UnionAll. We should support more cases in the future.
}

func (t *rootTask) copy() task {
	return &rootTask{
		p: t.p,
	}
}

func (t *rootTask) count() float64 {
	return t.p.statsInfo().RowCount
}

func (t *rootTask) plan() PhysicalPlan {
	return t.p
}

// MemoryUsage return the memory usage of rootTask
func (t *rootTask) MemoryUsage() (sum int64) {
	if t == nil {
		return
	}

	sum = size.SizeOfInterface + size.SizeOfBool
	if t.p != nil {
		sum += t.p.MemoryUsage()
	}
	return sum
}

func (p *PhysicalLimit) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	newPartitionBy := make([]property.SortItem, 0, len(p.GetPartitionBy()))
	for _, expr := range p.GetPartitionBy() {
		newPartitionBy = append(newPartitionBy, expr.Clone())
	}

	sunk := false
	if cop, ok := t.(*copTask); ok {
		if len(cop.idxMergePartPlans) == 0 {
			// For double read which requires order being kept, the limit cannot be pushed down to the table side,
			// because handles would be reordered before being sent to table scan.
			if (!cop.keepOrder || !cop.indexPlanFinished || cop.indexPlan == nil) && len(cop.rootTaskConds) == 0 {
				// When limit is pushed down, we should remove its offset.
				newCount := p.Offset + p.Count
				childProfile := cop.plan().statsInfo()
				// Strictly speaking, for the row count of stats, we should multiply newCount with "regionNum",
				// but "regionNum" is unknown since the copTask can be a double read, so we ignore it now.
				stats := deriveLimitStats(childProfile, float64(newCount))
				pushedDownLimit := PhysicalLimit{PartitionBy: newPartitionBy, Count: newCount}.Init(p.ctx, stats, p.blockOffset)
				cop = attachPlan2Task(pushedDownLimit, cop).(*copTask)
				// Don't use clone() so that Limit and its children share the same schema. Otherwise the virtual generated column may not be resolved right.
				pushedDownLimit.SetSchema(pushedDownLimit.children[0].Schema())
			}
			t = cop.convertToRootTask(p.ctx)
			sunk = p.sinkIntoIndexLookUp(t)
		} else if !cop.idxMergeIsIntersection {
			// We only support push part of the order prop down to index merge case.
			if !cop.keepOrder && !cop.indexPlanFinished && len(cop.rootTaskConds) == 0 {
				newCount := p.Offset + p.Count
				limitChildren := make([]PhysicalPlan, 0, len(cop.idxMergePartPlans))
				for _, partialScan := range cop.idxMergePartPlans {
					childProfile := partialScan.statsInfo()
					stats := deriveLimitStats(childProfile, float64(newCount))
					pushedDownLimit := PhysicalLimit{PartitionBy: newPartitionBy, Count: newCount}.Init(p.ctx, stats, p.blockOffset)
					pushedDownLimit.SetChildren(partialScan)
					pushedDownLimit.SetSchema(pushedDownLimit.children[0].Schema())
					limitChildren = append(limitChildren, pushedDownLimit)
				}
				cop.idxMergePartPlans = limitChildren
			}
			t = cop.convertToRootTask(p.ctx)
		} else {
			// Whatever the remained case is, we directly convert to it to root task.
			t = cop.convertToRootTask(p.ctx)
		}
	} else if mpp, ok := t.(*mppTask); ok {
		newCount := p.Offset + p.Count
		childProfile := mpp.plan().statsInfo()
		stats := deriveLimitStats(childProfile, float64(newCount))
		pushedDownLimit := PhysicalLimit{Count: newCount, PartitionBy: newPartitionBy}.Init(p.ctx, stats, p.blockOffset)
		mpp = attachPlan2Task(pushedDownLimit, mpp).(*mppTask)
		pushedDownLimit.SetSchema(pushedDownLimit.children[0].Schema())
		t = mpp.convertToRootTask(p.ctx)
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

func (p *PhysicalLimit) sinkIntoIndexLookUp(t task) bool {
	root := t.(*rootTask)
	reader, isDoubleRead := root.p.(*PhysicalIndexLookUpReader)
	proj, isProj := root.p.(*PhysicalProjection)
	if !isDoubleRead && !isProj {
		return false
	}
	if isProj {
		reader, isDoubleRead = proj.Children()[0].(*PhysicalIndexLookUpReader)
		if !isDoubleRead {
			return false
		}
	}

	// If this happens, some Projection Operator must be inlined into this Limit. (issues/14428)
	// For example, if the original plan is `IndexLookUp(col1, col2) -> Limit(col1, col2) -> Project(col1)`,
	//  then after inlining the Project, it will be `IndexLookUp(col1, col2) -> Limit(col1)` here.
	// If the Limit is sunk into the IndexLookUp, the IndexLookUp's schema needs to be updated as well,
	//  but updating it here is not safe, so do not sink Limit into this IndexLookUp in this case now.
	if p.Schema().Len() != reader.Schema().Len() {
		return false
	}

	// We can sink Limit into IndexLookUpReader only if tablePlan contains no Selection.
	ts, isTableScan := reader.tablePlan.(*PhysicalTableScan)
	if !isTableScan {
		return false
	}
	reader.PushedLimit = &PushedDownLimit{
		Offset: p.Offset,
		Count:  p.Count,
	}
	originStats := ts.stats
	ts.stats = p.stats
	if originStats != nil {
		// keep the original stats version
		ts.stats.StatsVersion = originStats.StatsVersion
	}
	reader.stats = p.stats
	if isProj {
		proj.stats = p.stats
	}
	return true
}

func (p *PhysicalSort) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	t = attachPlan2Task(p, t)
	return t
}

func (p *NominalSort) attach2Task(tasks ...task) task {
	if p.OnlyColumn {
		return tasks[0]
	}
	t := tasks[0].copy()
	t = attachPlan2Task(p, t)
	return t
}

func (p *PhysicalTopN) getPushedDownTopN(childPlan PhysicalPlan) *PhysicalTopN {
	newByItems := make([]*util.ByItems, 0, len(p.ByItems))
	for _, expr := range p.ByItems {
		newByItems = append(newByItems, expr.Clone())
	}
	newPartitionBy := make([]property.SortItem, 0, len(p.GetPartitionBy()))
	for _, expr := range p.GetPartitionBy() {
		newPartitionBy = append(newPartitionBy, expr.Clone())
	}
	newCount := p.Offset + p.Count
	childProfile := childPlan.statsInfo()
	// Strictly speaking, for the row count of pushed down TopN, we should multiply newCount with "regionNum",
	// but "regionNum" is unknown since the copTask can be a double read, so we ignore it now.
	stats := deriveLimitStats(childProfile, float64(newCount))
	topN := PhysicalTopN{
		ByItems:     newByItems,
		PartitionBy: newPartitionBy,
		Count:       newCount,
	}.Init(p.ctx, stats, p.blockOffset, p.GetChildReqProps(0))
	topN.SetChildren(childPlan)
	return topN
}

// canPushToIndexPlan checks if this TopN can be pushed to the index side of copTask.
// It can be pushed to the index side when all columns used by ByItems are available from the index side and
//
//	there's no prefix index column.
func (p *PhysicalTopN) canPushToIndexPlan(indexPlan PhysicalPlan, byItemCols []*expression.Column) bool {
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
	return expression.CanExprsPushDown(p.ctx.GetSessionVars().StmtCtx, exprs, p.ctx.GetClient(), storeTp)
}

// containVirtualColumn checks whether TopN.ByItems contains virtual generated columns.
func (p *PhysicalTopN) containVirtualColumn(tCols []*expression.Column) bool {
	for _, by := range p.ByItems {
		cols := expression.ExtractColumns(by.Expr)
		for _, col := range cols {
			for _, tCol := range tCols {
				// A column with ID > 0 indicates that the column can be resolved by data source.
				if tCol.ID > 0 && tCol.ID == col.ID && tCol.VirtualExpr != nil {
					return true
				}
			}
		}
	}
	return false
}

// canPushDownToTiKV checks whether this topN can be pushed down to TiKV.
func (p *PhysicalTopN) canPushDownToTiKV(copTask *copTask) bool {
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
	} else if p.containVirtualColumn(copTask.plan().Schema().Columns) {
		return false
	}
	return true
}

// canPushDownToTiFlash checks whether this topN can be pushed down to TiFlash.
func (p *PhysicalTopN) canPushDownToTiFlash(mppTask *mppTask) bool {
	if !p.canExpressionConvertedToPB(kv.TiFlash) {
		return false
	}
	if p.containVirtualColumn(mppTask.plan().Schema().Columns) {
		return false
	}
	return true
}

func (p *PhysicalTopN) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	cols := make([]*expression.Column, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		cols = append(cols, expression.ExtractColumns(item.Expr)...)
	}
	needPushDown := len(cols) > 0
	if copTask, ok := t.(*copTask); ok && needPushDown && p.canPushDownToTiKV(copTask) && len(copTask.rootTaskConds) == 0 {
		newTask, changed := p.pushPartialTopNDownToCop(copTask)
		if changed {
			return newTask
		}
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
	} else if mppTask, ok := t.(*mppTask); ok && needPushDown && p.canPushDownToTiFlash(mppTask) {
		pushedDownTopN := p.getPushedDownTopN(mppTask.p)
		mppTask.p = pushedDownTopN
	}
	rootTask := t.convertToRootTask(p.ctx)
	// Skip TopN with partition on the root. This is a derived topN and window function
	// will take care of the filter.
	if len(p.GetPartitionBy()) > 0 {
		return t
	}
	return attachPlan2Task(p, rootTask)
}

// pushPartialTopNDownToCop is a temp solution for index merge. It actually does the same thing as DataSource's isMatchProp.
// We need to support a more enhanced read strategy in the execution phase. So that we can achieve Limit(TiDB)->Reader(TiDB)->Limit(TiKV/TiFlash)->Scan(TiKV/TiFlash).
// Before that is done, we use this logic to provide a way to keep the order property when reading from TiKV, so that we can use the orderliness of index to speed up the query.
// Here we can change the execution plan to TopN(TiDB)->Reader(TiDB)->Limit(TiKV)->Scan(TiKV).(TiFlash is not supported).
func (p *PhysicalTopN) pushPartialTopNDownToCop(copTsk *copTask) (task, bool) {
	if copTsk.getStoreType() != kv.TiKV {
		return nil, false
	}
	copTsk = copTsk.copy().(*copTask)
	if len(copTsk.rootTaskConds) > 0 {
		return nil, false
	}
	colsProp, ok := GetPropByOrderByItems(p.ByItems)
	if !ok {
		return nil, false
	}
	allSameOrder, isDesc := colsProp.AllSameOrder()
	if !allSameOrder {
		return nil, false
	}
	if len(copTsk.idxMergePartPlans) > 0 && copTsk.idxMergeIsIntersection {
		return nil, false
	}
	// This func only consider index merge.
	if len(copTsk.idxMergePartPlans) == 0 {
		return nil, false
	}
	var (
		selSelectivityOnPartialScan []float64

		partialScans      []PhysicalPlan
		clonedPartialPlan []PhysicalPlan
	)
	if len(copTsk.idxMergePartPlans) > 0 {
		// calculate selectivities for each partial plan in advance and clone partial plans since we may modify their stats later.
		partialScans = make([]PhysicalPlan, 0, len(copTsk.idxMergePartPlans))
		selSelectivityOnPartialScan = make([]float64, len(copTsk.idxMergePartPlans))
		for i, scan := range copTsk.idxMergePartPlans {
			selSelectivityOnPartialScan[i] = 1
			clonedScan, err := scan.Clone()
			if err != nil {
				return nil, false
			}
			clonedPartialPlan = append(clonedPartialPlan, clonedScan)
			finalScan := clonedScan
			var partialSel *PhysicalSelection
			for len(finalScan.Children()) > 0 {
				partialSel, _ = finalScan.(*PhysicalSelection)
				finalScan = finalScan.Children()[0]
			}
			if partialSel != nil && finalScan.statsInfo().RowCount > 0 {
				selSelectivityOnPartialScan[i] = partialSel.statsInfo().RowCount / finalScan.statsInfo().RowCount
			}
			if plan, ok := finalScan.(*PhysicalTableScan); ok {
				plan.ByItems = p.ByItems
				tblInfo := plan.Table
				if tblInfo.GetPartitionInfo() != nil {
					plan.Columns = append(plan.Columns, model.NewExtraPhysTblIDColInfo())
					plan.schema.Append(&expression.Column{
						RetType:  types.NewFieldType(mysql.TypeLonglong),
						UniqueID: p.ctx.GetSessionVars().AllocPlanColumnID(),
						ID:       model.ExtraPhysTblID,
					})
				}
			}
			if plan, ok := finalScan.(*PhysicalIndexScan); ok {
				plan.ByItems = p.ByItems
				tblInfo := plan.Table
				if tblInfo.GetPartitionInfo() != nil && !plan.Index.Global {
					plan.Columns = append(plan.Columns, model.NewExtraPhysTblIDColInfo())
					plan.schema.Append(&expression.Column{
						RetType:  types.NewFieldType(mysql.TypeLonglong),
						UniqueID: p.ctx.GetSessionVars().AllocPlanColumnID(),
						ID:       model.ExtraPhysTblID,
					})
				}
			}
			partialScans = append(partialScans, finalScan)
		}
	}

	newPartitionBy := make([]property.SortItem, 0, len(p.GetPartitionBy()))
	for _, expr := range p.GetPartitionBy() {
		newPartitionBy = append(newPartitionBy, expr.Clone())
	}
	if !copTsk.indexPlanFinished {
		// If indexPlan side isn't finished, there's no selection on the table side.
		if len(copTsk.idxMergePartPlans) > 0 {
			// Deal with index merge case.
			propMatched := p.checkSubScans(colsProp, isDesc, partialScans...)
			if !propMatched {
				// If there's one used index cannot match the prop.
				return nil, false
			}
			newCopSubPlans := p.addPartialLimitForSubScans(clonedPartialPlan, partialScans, selSelectivityOnPartialScan)
			copTsk.idxMergePartPlans = newCopSubPlans
			cloned, err := copTsk.tablePlan.Clone()
			if err != nil {
				return nil, false
			}
			clonedTblScan := cloned.(*PhysicalTableScan)
			clonedTblScan.statsInfo().ScaleByExpectCnt(float64(p.Count+p.Offset) * float64(len(copTsk.idxMergePartPlans)))
			tblInfo := clonedTblScan.Table
			if tblInfo.PKIsHandle {
				pk := tblInfo.GetPkColInfo()
				pkCol := expression.ColInfo2Col(clonedTblScan.tblCols, pk)
				if !clonedTblScan.Schema().Contains(pkCol) {
					clonedTblScan.Schema().Append(pkCol)
					clonedTblScan.Columns = append(clonedTblScan.Columns, pk)
				}
			} else if tblInfo.IsCommonHandle {
				idxInfo := tblInfo.GetPrimaryKey()
				for _, idxCol := range idxInfo.Columns {
					c := clonedTblScan.tblCols[idxCol.Offset]
					if !clonedTblScan.Schema().Contains(c) {
						clonedTblScan.Schema().Append(c)
						clonedTblScan.Columns = append(clonedTblScan.Columns, c.ToInfo())
					}
				}
			} else {
				if !clonedTblScan.Schema().Contains(clonedTblScan.HandleCols.GetCol(0)) {
					clonedTblScan.Schema().Append(clonedTblScan.HandleCols.GetCol(0))
					clonedTblScan.Columns = append(clonedTblScan.Columns, model.NewExtraHandleColInfo())
				}
			}
			clonedTblScan.HandleCols, err = clonedTblScan.HandleCols.ResolveIndices(clonedTblScan.Schema())
			if err != nil {
				return nil, false
			}
			copTsk.tablePlan = clonedTblScan
			copTsk.indexPlanFinished = true
			rootTask := copTsk.convertToRootTask(p.ctx)
			if indexMerge, ok := rootTask.p.(*PhysicalIndexMergeReader); ok {
				indexMerge.PushedLimit = &PushedDownLimit{
					Offset: p.Offset,
					Count:  p.Count,
				}
				indexMerge.ByItems = p.ByItems
				indexMerge.KeepOrder = true
				return rootTask, true
			}
		}
	} else {
		return nil, false
	}

	rootTask := copTsk.convertToRootTask(p.ctx)
	return attachPlan2Task(p, rootTask), true
}

// checkOrderPropForSubIndexScan checks whether these index columns can meet the specified order property.
func (p *PhysicalTopN) checkOrderPropForSubIndexScan(idxCols []*expression.Column, idxColLens []int, constColsByCond []bool, colsProp *property.PhysicalProperty) bool {
	// If the number of the by-items is bigger than the index columns. We cannot push down since it must not keep order.
	if len(idxCols) < len(colsProp.SortItems) {
		return false
	}
	idxPos := 0
	for _, byItem := range colsProp.SortItems {
		found := false
		for ; idxPos < len(idxCols); idxPos++ {
			if idxColLens[idxPos] == types.UnspecifiedLength && idxCols[idxPos].Equal(p.SCtx(), byItem.Col) {
				found = true
				idxPos++
				break
			}
			if idxPos >= len(constColsByCond) || !constColsByCond[idxPos] {
				found = false
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// checkSubScans checks whether all these Scans can meet the specified order property.
func (p *PhysicalTopN) checkSubScans(colsProp *property.PhysicalProperty, isDesc bool, scans ...PhysicalPlan) bool {
	for _, scan := range scans {
		switch x := scan.(type) {
		case *PhysicalIndexScan:
			propMatched := p.checkOrderPropForSubIndexScan(x.IdxCols, x.IdxColLens, x.constColsByCond, colsProp)
			if !propMatched {
				return false
			}
			x.KeepOrder = true
			x.Desc = isDesc
		case *PhysicalTableScan:
			if x.HandleCols == nil {
				return false
			}

			if x.HandleCols.IsInt() {
				pk := x.HandleCols.GetCol(0)
				if len(colsProp.SortItems) != 1 || !colsProp.SortItems[0].Col.Equal(p.SCtx(), pk) {
					return false
				}
			} else {
				idxCols, idxColLens := expression.IndexInfo2PrefixCols(x.Columns, x.Schema().Columns, tables.FindPrimaryIndex(x.Table))
				matched := p.checkOrderPropForSubIndexScan(idxCols, idxColLens, x.constColsByCond, colsProp)
				if !matched {
					return false
				}
			}
			x.KeepOrder = true
			x.Desc = isDesc
		default:
			return false
		}
	}
	// Return true when all sub index plan matched.
	return true
}

func (p *PhysicalTopN) addPartialLimitForSubScans(copSubPlans []PhysicalPlan, finalPartialScans []PhysicalPlan, selSelectivities []float64) []PhysicalPlan {
	limitAddedPlan := make([]PhysicalPlan, 0, len(copSubPlans))
	for i, copSubPlan := range copSubPlans {
		childProfile := copSubPlan.statsInfo()
		newCount := p.Offset + p.Count
		stats := deriveLimitStats(childProfile, float64(newCount))
		pushedLimit := PhysicalLimit{
			Count: newCount,
		}.Init(p.SCtx(), stats, p.SelectBlockOffset())
		pushedLimit.SetSchema(copSubPlan.Schema())
		pushedLimit.SetChildren(copSubPlan)
		// A similar but simplified logic compared the ExpectedCnt handling logic in getOriginalPhysicalIndexScan.
		child := pushedLimit.Children()[0]
		// The row count of the direct child of Limit should be adjusted to be no larger than the Limit.Count.
		child.SetStats(child.statsInfo().ScaleByExpectCnt(float64(newCount)))
		// The Limit->Selection->IndexScan case:
		// adjust the row count of IndexScan according to the selectivity of the Selection.
		if selSelectivities[i] > 0 && selSelectivities[i] < 1 {
			scaledRowCount := child.Stats().RowCount / selSelectivities[i]
			finalPartialScans[i].SetStats(finalPartialScans[i].Stats().ScaleByExpectCnt(scaledRowCount))
		}
		limitAddedPlan = append(limitAddedPlan, pushedLimit)
	}
	return limitAddedPlan
}

func (p *PhysicalProjection) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	if cop, ok := t.(*copTask); ok {
		if (len(cop.rootTaskConds) == 0 && len(cop.idxMergePartPlans) == 0) && expression.CanExprsPushDown(p.ctx.GetSessionVars().StmtCtx, p.Exprs, p.ctx.GetClient(), cop.getStoreType()) {
			copTask := attachPlan2Task(p, cop)
			return copTask
		}
	} else if mpp, ok := t.(*mppTask); ok {
		if expression.CanExprsPushDown(p.ctx.GetSessionVars().StmtCtx, p.Exprs, p.ctx.GetClient(), kv.TiFlash) {
			p.SetChildren(mpp.p)
			mpp.p = p
			return mpp
		}
	}
	t = t.convertToRootTask(p.ctx)
	t = attachPlan2Task(p, t)
	if root, ok := tasks[0].(*rootTask); ok && root.isEmpty {
		t.(*rootTask).isEmpty = true
	}
	return t
}

func (p *PhysicalUnionAll) attach2MppTasks(tasks ...task) task {
	t := &mppTask{p: p}
	childPlans := make([]PhysicalPlan, 0, len(tasks))
	for _, tk := range tasks {
		if mpp, ok := tk.(*mppTask); ok && !tk.invalid() {
			childPlans = append(childPlans, mpp.plan())
		} else if root, ok := tk.(*rootTask); ok && root.isEmpty {
			continue
		} else {
			return invalidTask
		}
	}
	if len(childPlans) == 0 {
		return invalidTask
	}
	p.SetChildren(childPlans...)
	return t
}

func (p *PhysicalUnionAll) attach2Task(tasks ...task) task {
	for _, t := range tasks {
		if _, ok := t.(*mppTask); ok {
			if p.TP() == plancodec.TypePartitionUnion {
				// In attach2MppTasks(), will attach PhysicalUnion to mppTask directly.
				// But PartitionUnion cannot pushdown to tiflash, so here disable PartitionUnion pushdown to tiflash explicitly.
				// For now, return invalidTask immediately, we can refine this by letting childTask of PartitionUnion convert to rootTask.
				return invalidTask
			}
			return p.attach2MppTasks(tasks...)
		}
	}
	t := &rootTask{p: p}
	childPlans := make([]PhysicalPlan, 0, len(tasks))
	for _, task := range tasks {
		task = task.convertToRootTask(p.ctx)
		childPlans = append(childPlans, task.plan())
	}
	p.SetChildren(childPlans...)
	return t
}

func (sel *PhysicalSelection) attach2Task(tasks ...task) task {
	if mppTask, _ := tasks[0].(*mppTask); mppTask != nil { // always push to mpp task.
		sc := sel.ctx.GetSessionVars().StmtCtx
		if expression.CanExprsPushDown(sc, sel.Conditions, sel.ctx.GetClient(), kv.TiFlash) {
			return attachPlan2Task(sel, mppTask.copy())
		}
	}
	t := tasks[0].convertToRootTask(sel.ctx)
	return attachPlan2Task(sel, t)
}

// CheckAggCanPushCop checks whether the aggFuncs and groupByItems can
// be pushed down to coprocessor.
func CheckAggCanPushCop(sctx sessionctx.Context, aggFuncs []*aggregation.AggFuncDesc, groupByItems []expression.Expression, storeType kv.StoreType) bool {
	sc := sctx.GetSessionVars().StmtCtx
	client := sctx.GetClient()
	ret := true
	reason := ""
	for _, aggFunc := range aggFuncs {
		// if the aggFunc contain VirtualColumn or CorrelatedColumn, it can not be pushed down.
		if expression.ContainVirtualColumn(aggFunc.Args) || expression.ContainCorrelatedColumn(aggFunc.Args) {
			reason = "expressions of AggFunc `" + aggFunc.Name + "` contain virtual column or correlated column, which is not supported now"
			ret = false
			break
		}
		if !aggregation.CheckAggPushDown(aggFunc, storeType) {
			reason = "AggFunc `" + aggFunc.Name + "` is not supported now"
			ret = false
			break
		}
		if !expression.CanExprsPushDownWithExtraInfo(sc, aggFunc.Args, client, storeType, aggFunc.Name == ast.AggFuncSum) {
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
			if !expression.CanExprsPushDownWithExtraInfo(sc, exprs, client, storeType, false) {
				reason = "arguments of AggFunc `" + aggFunc.Name + "` contains unsupported exprs in order-by clause"
				ret = false
				break
			}
		}
		pb, _ := aggregation.AggFuncToPBExpr(sctx, client, aggFunc, storeType)
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
	if ret && !expression.CanExprsPushDown(sc, groupByItems, client, storeType) {
		reason = "groupByItems contain unsupported exprs"
		ret = false
	}

	if !ret {
		storageName := storeType.Name()
		if storeType == kv.UnSpecified {
			storageName = "storage layer"
		}
		warnErr := errors.New("Aggregation can not be pushed to " + storageName + " because " + reason)
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
	sctx sessionctx.Context, original *AggInfo, partialIsCop bool, isMPPTask bool) (partial, final *AggInfo, firstRowFuncMap map[*aggregation.AggFuncDesc]*aggregation.AggFuncDesc) {
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
				RetType:  gbyExpr.GetType(),
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
					if gbyExpr.Equal(sctx, distinctArg) && gbyExpr.GetType().Equal(distinctArg.GetType()) {
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
							RetType:  distinctArg.GetType(),
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
						firstRow, err := aggregation.NewAggFuncDesc(sctx, ast.AggFuncFirstRow, []expression.Expression{distinctArg}, false)
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
					// For MPP Task, the final count() is changed to sum().
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
					RetType:  original.Schema.Columns[i].GetType(),
				})
				args = append(args, partial.Schema.Columns[partialCursor])
				partialCursor++
			}
			if aggFunc.Name == ast.AggFuncAvg {
				cntAgg := aggFunc.Clone()
				cntAgg.Name = ast.AggFuncCount
				err := cntAgg.TypeInfer(sctx)
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
				newAggFunc.RetTp = partial.Schema.Columns[partialCursor-1].GetType()
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
			err := avgCount.TypeInfer(p.ctx)
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
			eq := expression.NewFunctionInternal(p.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), avgCountCol, expression.NewZero())
			caseWhen := expression.NewFunctionInternal(p.ctx, ast.Case, avgCountCol.RetType, eq, expression.NewOne(), avgCountCol)
			divide := expression.NewFunctionInternal(p.ctx, ast.Div, avgSumCol.RetType, avgSumCol, caseWhen)
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
	}.Init(p.SCtx(), p.stats, p.SelectBlockOffset(), p.GetChildReqProps(0).CloneEssentialFields())
	proj.SetSchema(p.schema)

	p.AggFuncs = newAggFuncs
	p.schema = newSchema

	return proj
}

func (p *basePhysicalAgg) newPartialAggregate(copTaskType kv.StoreType, isMPPTask bool) (partial, final PhysicalPlan) {
	// Check if this aggregation can push down.
	if !CheckAggCanPushCop(p.ctx, p.AggFuncs, p.GroupByItems, copTaskType) {
		return nil, p.self
	}
	partialPref, finalPref, firstRowFuncMap := BuildFinalModeAggregation(p.ctx, &AggInfo{
		AggFuncs:     p.AggFuncs,
		GroupByItems: p.GroupByItems,
		Schema:       p.Schema().Clone(),
	}, true, isMPPTask)
	if partialPref == nil {
		return nil, p.self
	}
	if p.tp == plancodec.TypeStreamAgg && len(partialPref.GroupByItems) != len(finalPref.GroupByItems) {
		return nil, p.self
	}
	// Remove unnecessary FirstRow.
	partialPref.AggFuncs = RemoveUnnecessaryFirstRow(p.ctx,
		finalPref.GroupByItems, partialPref.AggFuncs, partialPref.GroupByItems, partialPref.Schema, firstRowFuncMap)
	if copTaskType == kv.TiDB {
		// For partial agg of TiDB cop task, since TiDB coprocessor reuse the TiDB executor,
		// and TiDB aggregation executor won't output the group by value,
		// so we need add `firstrow` aggregation function to output the group by value.
		aggFuncs, err := genFirstRowAggForGroupBy(p.ctx, partialPref.GroupByItems)
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
	if p.tp == plancodec.TypeStreamAgg {
		finalAgg := basePhysicalAgg{
			AggFuncs:     finalPref.AggFuncs,
			GroupByItems: finalPref.GroupByItems,
			MppRunMode:   p.MppRunMode,
		}.initForStream(p.ctx, p.stats, p.blockOffset, prop)
		finalAgg.schema = finalPref.Schema
		return partialAgg, finalAgg
	}

	finalAgg := basePhysicalAgg{
		AggFuncs:     finalPref.AggFuncs,
		GroupByItems: finalPref.GroupByItems,
		MppRunMode:   p.MppRunMode,
	}.initForHash(p.ctx, p.stats, p.blockOffset, prop)
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
	if !p.ctx.GetSessionVars().Enable3StageDistinctAgg || !p.ctx.GetSessionVars().Enable3StageMultiDistinctAgg || len(p.GroupByItems) > 0 {
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
		p.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("Some grouping sets should be merged"))
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
					p.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("couldn't find a proper group set for normal agg"))
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
	if !p.ctx.GetSessionVars().Enable3StageDistinctAgg || len(p.GroupByItems) > 0 {
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

func genFirstRowAggForGroupBy(ctx sessionctx.Context, groupByItems []expression.Expression) ([]*aggregation.AggFuncDesc, error) {
	aggFuncs := make([]*aggregation.AggFuncDesc, 0, len(groupByItems))
	for _, groupBy := range groupByItems {
		agg, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncFirstRow, []expression.Expression{groupBy}, false)
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
	sctx sessionctx.Context,
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
				if gbyExpr.Equal(sctx, aggFunc.Args[0]) {
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

func (p *PhysicalStreamAgg) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	if cop, ok := t.(*copTask); ok {
		// We should not push agg down across
		//  1. double read, since the data of second read is ordered by handle instead of index. The `extraHandleCol` is added
		//     if the double read needs to keep order. So we just use it to decided
		//     whether the following plan is double read with order reserved.
		//  2. the case that there's filters should be calculated on TiDB side.
		//  3. the case of index merge
		if (cop.indexPlan != nil && cop.tablePlan != nil && cop.keepOrder) || len(cop.rootTaskConds) > 0 || len(cop.idxMergePartPlans) > 0 {
			t = cop.convertToRootTask(p.ctx)
			attachPlan2Task(p, t)
		} else {
			storeType := cop.getStoreType()
			// TiFlash doesn't support Stream Aggregation
			if storeType == kv.TiFlash && len(p.GroupByItems) > 0 {
				return invalidTask
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
			t = cop.convertToRootTask(p.ctx)
			attachPlan2Task(finalAgg, t)
		}
	} else if mpp, ok := t.(*mppTask); ok {
		t = mpp.convertToRootTask(p.ctx)
		attachPlan2Task(p, t)
	} else {
		attachPlan2Task(p, t)
	}
	return t
}

// cpuCostDivisor computes the concurrency to which we would amortize CPU cost
// for hash aggregation.
func (p *PhysicalHashAgg) cpuCostDivisor(hasDistinct bool) (float64, float64) {
	if hasDistinct {
		return 0, 0
	}
	sessionVars := p.ctx.GetSessionVars()
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

func (p *PhysicalHashAgg) attach2TaskForMpp1Phase(mpp *mppTask) task {
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
		// for every grouping set, pick its cols out, and combine with normal group cols to get the NDV.
		groupingSetCols := groupingSet.ExtractCols()
		groupingSetCols = append(groupingSetCols, normalGbyCols...)
		NDV, _ := getColsNDVWithMatchedLen(groupingSetCols, childSchema, childStats)
		sumNDV += NDV
	}
	// After group operator, all same rows are grouped into one row, that means all
	// change the sub-agg's stats
	if p.stats != nil {
		// equivalence to a new cloned one. (cause finalAgg and partialAgg may share a same copy of stats)
		cpStats := p.stats.Scale(1)
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
					incrementNDV, _ := getColsDNVWithMatchedLenFromUniqueIDs(intersectionIDs, childSchema, childStats)
					newGNDV += incrementNDV
					// restore the before intersectionIDs slice.
					intersectionIDs = intersectionIDs[:beforeLen]
				}
				// insert ids one by one.
				intersectionIDs = append(intersectionIDs, id)
			}
			oneGNDV.NDV = newGNDV
		}
		p.stats = cpStats
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
func (p *PhysicalHashAgg) adjust3StagePhaseAgg(partialAgg, finalAgg PhysicalPlan, canUse3StageAgg bool,
	groupingSets expression.GroupingSets, mpp *mppTask) (final, mid, part, proj4Part PhysicalPlan, _ error) {
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
				UniqueID: p.ctx.GetSessionVars().AllocPlanColumnID(),
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
	stats := mpp.p.statsInfo().Scale(float64(1))
	stats.RowCount = stats.RowCount * float64(len(groupingSets))
	physicalExpand := PhysicalExpand{
		GroupingSets: groupingSets,
	}.Init(p.ctx, stats, mpp.p.SelectBlockOffset())
	// generate a new column as groupingID to identify which this row is targeting for.
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.SetFlag(mysql.UnsignedFlag | mysql.NotNullFlag)
	groupingIDCol = &expression.Column{
		UniqueID: p.ctx.GetSessionVars().AllocPlanColumnID(),
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
	cloneHashAgg.basePlan = newBasePlan(cloneHashAgg.ctx, cloneHashAgg.tp, cloneHashAgg.blockOffset)
	cloneHashAgg.stats = finalAgg.Stats() // reuse the final agg stats here.

	// step1: adjust partial agg, for normal agg here, adjust it to target for specified group data.
	// Since we may substitute the first arg of normal agg with case-when expression here, append a
	// customized proj here rather than depending on postOptimize to insert a blunt one for us.
	//
	// proj4Partial output all the base col from lower op + caseWhen proj cols.
	proj4Partial := new(PhysicalProjection).Init(p.ctx, mpp.p.statsInfo(), mpp.p.SelectBlockOffset())
	for _, col := range mpp.p.Schema().Columns {
		proj4Partial.Exprs = append(proj4Partial.Exprs, col)
	}
	proj4Partial.SetSchema(mpp.p.Schema().Clone())

	partialHashAgg := partialAgg.(*PhysicalHashAgg)
	partialHashAgg.GroupByItems = append(partialHashAgg.GroupByItems, groupingIDCol)
	partialHashAgg.schema.Append(groupingIDCol.(*expression.Column))
	// it will create a new stats for partial agg.
	partialHashAgg.scaleStats4GroupingSets(groupingSets, groupingIDCol.(*expression.Column), proj4Partial.Schema(), proj4Partial.statsInfo())
	for _, fun := range partialHashAgg.AggFuncs {
		if !fun.HasDistinct {
			// for normal agg phase1, we should also modify them to target for specified group data.
			// Expr = (case when groupingID = targeted_groupingID then arg else null end)
			eqExpr := expression.NewFunctionInternal(p.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), groupingIDCol, expression.NewUInt64Const(fun.GroupingID))
			caseWhen := expression.NewFunctionInternal(p.ctx, ast.Case, fun.Args[0].GetType(), eqExpr, fun.Args[0], expression.NewNull())
			caseWhenProjCol := &expression.Column{
				UniqueID: p.ctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  fun.Args[0].GetType(),
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
			UniqueID: p.ctx.GetSessionVars().AllocPlanColumnID(),
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

func (p *PhysicalHashAgg) attach2TaskForMpp(tasks ...task) task {
	t := tasks[0].copy()
	mpp, ok := t.(*mppTask)
	if !ok {
		return invalidTask
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
			return invalidTask
		}
		attachPlan2Task(partialAgg, mpp)
		partitionCols := p.MppPartitionCols
		if len(partitionCols) == 0 {
			items := finalAgg.(*PhysicalHashAgg).GroupByItems
			partitionCols = make([]*property.MPPPartitionColumn, 0, len(items))
			for _, expr := range items {
				col, ok := expr.(*expression.Column)
				if !ok {
					return invalidTask
				}
				partitionCols = append(partitionCols, &property.MPPPartitionColumn{
					Col:       col,
					CollateID: property.GetCollateIDByNameForPartition(col.GetType().GetCollate()),
				})
			}
		}
		prop := &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.HashType, MPPPartitionCols: partitionCols}
		newMpp := mpp.enforceExchangerImpl(prop)
		if newMpp.invalid() {
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
		t = mpp.convertToRootTask(p.ctx)
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
			return invalidTask
		}

		final, middle, partial, proj4Partial, err := p.adjust3StagePhaseAgg(partialAgg, finalAgg, canUse3StageAgg, groupingSets, mpp)
		if err != nil {
			return invalidTask
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
					CollateID: property.GetCollateIDByNameForPartition(col.GetType().GetCollate()),
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
			}.Init(p.ctx, p.statsInfo(), p.SelectBlockOffset())
			for _, col := range p.Schema().Columns {
				proj.Exprs = append(proj.Exprs, col)
			}
			proj.SetSchema(p.schema)
		}
		attachPlan2Task(proj, newMpp)
		return newMpp
	default:
		return invalidTask
	}
}

func (p *PhysicalHashAgg) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	final := p
	if cop, ok := t.(*copTask); ok {
		if len(cop.rootTaskConds) == 0 && len(cop.idxMergePartPlans) == 0 {
			copTaskType := cop.getStoreType()
			partialAgg, finalAgg := p.newPartialAggregate(copTaskType, false)
			if finalAgg != nil {
				final = finalAgg.(*PhysicalHashAgg)
			}
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
			t = cop.convertToRootTask(p.ctx)
			attachPlan2Task(finalAgg, t)
		} else {
			t = cop.convertToRootTask(p.ctx)
			attachPlan2Task(p, t)
		}
	} else if _, ok := t.(*mppTask); ok {
		return final.attach2TaskForMpp(tasks...)
	} else {
		attachPlan2Task(p, t)
	}
	return t
}

func (p *PhysicalWindow) attach2TaskForMPP(mpp *mppTask) task {
	// FIXME: currently, tiflash's join has different schema with TiDB,
	// so we have to rebuild the schema of join and operators which may inherit schema from join.
	// for window, we take the sub-plan's schema, and the schema generated by windowDescs.
	columns := p.Schema().Clone().Columns[len(p.Schema().Columns)-len(p.WindowFuncDescs):]
	p.schema = expression.MergeSchema(mpp.plan().Schema(), expression.NewSchema(columns...))

	failpoint.Inject("CheckMPPWindowSchemaLength", func() {
		if len(p.Schema().Columns) != len(mpp.plan().Schema().Columns)+len(p.WindowFuncDescs) {
			panic("mpp physical window has incorrect schema length")
		}
	})

	return attachPlan2Task(p, mpp)
}

func (p *PhysicalWindow) attach2Task(tasks ...task) task {
	if mpp, ok := tasks[0].copy().(*mppTask); ok && p.storeTp == kv.TiFlash {
		return p.attach2TaskForMPP(mpp)
	}
	t := tasks[0].convertToRootTask(p.ctx)
	return attachPlan2Task(p.self, t)
}

// mppTask can not :
// 1. keep order
// 2. support double read
// 3. consider virtual columns.
// 4. TODO: partition prune after close
type mppTask struct {
	p PhysicalPlan

	partTp   property.MPPPartitionType
	hashCols []*property.MPPPartitionColumn

	// rootTaskConds record filters of TableScan that cannot be pushed down to TiFlash.

	// For logical plan like: HashAgg -> Selection -> TableScan, if filters in Selection cannot be pushed to TiFlash.
	// Planner will generate physical plan like: PhysicalHashAgg -> PhysicalSelection -> TableReader -> PhysicalTableScan(cop tiflash)
	// Because planner will make mppTask invalid directly then use copTask directly.

	// But in DisaggregatedTiFlash mode, cop and batchCop protocol is disabled, so we have to consider this situation for mppTask.
	// When generating PhysicalTableScan, if prop.TaskTp is RootTaskType, mppTask will be converted to rootTask,
	// and filters in rootTaskConds will be added in a Selection which will be executed in TiDB.
	// So physical plan be like: PhysicalHashAgg -> PhysicalSelection -> TableReader -> ExchangeSender -> PhysicalTableScan(mpp tiflash)
	rootTaskConds []expression.Expression
	tblColHists   *statistics.HistColl
}

func (t *mppTask) count() float64 {
	return t.p.statsInfo().RowCount
}

func (t *mppTask) copy() task {
	nt := *t
	return &nt
}

func (t *mppTask) plan() PhysicalPlan {
	return t.p
}

func (t *mppTask) invalid() bool {
	return t.p == nil
}

func (t *mppTask) convertToRootTask(ctx sessionctx.Context) *rootTask {
	return t.copy().(*mppTask).convertToRootTaskImpl(ctx)
}

// MemoryUsage return the memory usage of mppTask
func (t *mppTask) MemoryUsage() (sum int64) {
	if t == nil {
		return
	}

	sum = size.SizeOfInterface + size.SizeOfInt + size.SizeOfSlice + int64(cap(t.hashCols))*size.SizeOfPointer
	if t.p != nil {
		sum += t.p.MemoryUsage()
	}
	return
}

func collectPartitionInfosFromMPPPlan(p *PhysicalTableReader, mppPlan PhysicalPlan) {
	switch x := mppPlan.(type) {
	case *PhysicalTableScan:
		p.PartitionInfos = append(p.PartitionInfos, tableScanAndPartitionInfo{x, x.PartitionInfo})
	default:
		for _, ch := range mppPlan.Children() {
			collectPartitionInfosFromMPPPlan(p, ch)
		}
	}
}

func collectRowSizeFromMPPPlan(mppPlan PhysicalPlan) (rowSize float64) {
	if mppPlan != nil && mppPlan.Stats() != nil && mppPlan.Stats().HistColl != nil {
		return mppPlan.Stats().HistColl.GetAvgRowSize(mppPlan.SCtx(), mppPlan.Schema().Columns, false, false)
	}
	return 1 // use 1 as lower-bound for safety
}

func accumulateNetSeekCost4MPP(p PhysicalPlan) (cost float64) {
	if ts, ok := p.(*PhysicalTableScan); ok {
		return float64(len(ts.Ranges)) * float64(len(ts.Columns)) * ts.SCtx().GetSessionVars().GetSeekFactor(ts.Table)
	}
	for _, c := range p.Children() {
		cost += accumulateNetSeekCost4MPP(c)
	}
	return
}

func tryExpandVirtualColumn(p PhysicalPlan) {
	if ts, ok := p.(*PhysicalTableScan); ok {
		ts.Columns = ExpandVirtualColumn(ts.Columns, ts.schema, ts.Table.Columns)
		return
	}
	for _, child := range p.Children() {
		tryExpandVirtualColumn(child)
	}
}

func (t *mppTask) convertToRootTaskImpl(ctx sessionctx.Context) *rootTask {
	// In disaggregated-tiflash mode, need to consider generated column.
	tryExpandVirtualColumn(t.p)
	sender := PhysicalExchangeSender{
		ExchangeType: tipb.ExchangeType_PassThrough,
	}.Init(ctx, t.p.statsInfo())
	sender.SetChildren(t.p)

	p := PhysicalTableReader{
		tablePlan: sender,
		StoreType: kv.TiFlash,
	}.Init(ctx, t.p.SelectBlockOffset())
	p.stats = t.p.statsInfo()
	collectPartitionInfosFromMPPPlan(p, t.p)
	rt := &rootTask{
		p: p,
	}

	if len(t.rootTaskConds) > 0 {
		// Some Filter cannot be pushed down to TiFlash, need to add Selection in rootTask,
		// so this Selection will be executed in TiDB.
		_, isTableScan := t.p.(*PhysicalTableScan)
		_, isSelection := t.p.(*PhysicalSelection)
		if isSelection {
			_, isTableScan = t.p.Children()[0].(*PhysicalTableScan)
		}
		if !isTableScan {
			// Need to make sure oriTaskPlan is TableScan, because rootTaskConds is part of TableScan.FilterCondition.
			// It's only for TableScan. This is ensured by converting mppTask to rootTask just after TableScan is built,
			// so no other operators are added into this mppTask.
			logutil.BgLogger().Error("expect Selection or TableScan for mppTask.p", zap.String("mppTask.p", t.p.TP()))
			return invalidTask
		}
		selectivity, _, err := t.tblColHists.Selectivity(ctx, t.rootTaskConds, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = SelectionFactor
		}
		sel := PhysicalSelection{Conditions: t.rootTaskConds}.Init(ctx, rt.p.statsInfo().Scale(selectivity), rt.p.SelectBlockOffset())
		sel.fromDataSource = true
		sel.SetChildren(rt.p)
		rt.p = sel
	}
	return rt
}

func (t *mppTask) needEnforceExchanger(prop *property.PhysicalProperty) bool {
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

func (t *mppTask) enforceExchanger(prop *property.PhysicalProperty) *mppTask {
	if !t.needEnforceExchanger(prop) {
		return t
	}
	return t.copy().(*mppTask).enforceExchangerImpl(prop)
}

func (t *mppTask) enforceExchangerImpl(prop *property.PhysicalProperty) *mppTask {
	if collate.NewCollationEnabled() && !t.p.SCtx().GetSessionVars().HashExchangeWithNewCollation && prop.MPPPartitionTp == property.HashType {
		for _, col := range prop.MPPPartitionCols {
			if types.IsString(col.Col.RetType.GetType()) {
				t.p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because when `new_collation_enabled` is true, HashJoin or HashAgg with string key is not supported now.")
				return &mppTask{}
			}
		}
	}
	ctx := t.p.SCtx()
	sender := PhysicalExchangeSender{
		ExchangeType: prop.MPPPartitionTp.ToExchangeType(),
		HashCols:     prop.MPPPartitionCols,
	}.Init(ctx, t.p.statsInfo())

	if ctx.GetSessionVars().ChooseMppVersion() >= kv.MppVersionV1 {
		sender.CompressionMode = ctx.GetSessionVars().ChooseMppExchangeCompressionMode()
	}

	sender.SetChildren(t.p)
	receiver := PhysicalExchangeReceiver{}.Init(ctx, t.p.statsInfo())
	receiver.SetChildren(sender)
	return &mppTask{
		p:        receiver,
		partTp:   prop.MPPPartitionTp,
		hashCols: prop.MPPPartitionCols,
	}
}
