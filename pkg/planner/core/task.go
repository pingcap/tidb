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
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/baseimpl"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/paging"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// HeavyFunctionNameMap stores function names that is worth to do HeavyFunctionOptimize.
// Currently this only applies to Vector data types and their functions. The HeavyFunctionOptimize
// eliminate the usage of the function in TopN operators to avoid vector distance re-calculation
// of TopN in the root task.
var HeavyFunctionNameMap = map[string]struct{}{
	"vec_cosine_distance":        {},
	"vec_l1_distance":            {},
	"vec_l2_distance":            {},
	"vec_negative_inner_product": {},
	"vec_dims":                   {},
	"vec_l2_norm":                {},
}

func attachPlan2Task(p base.PhysicalPlan, t base.Task) base.Task {
	// since almost all current physical plan will be attached to bottom encapsulated task.
	// we do the stats inheritance here for all the index join inner task.
	inheritStatsFromBottomTaskForIndexJoinInner(p, t)
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
		ts := t.tablePlan.(*physicalop.PhysicalTableScan)
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
	if ts, ok := tp.(*physicalop.PhysicalTableScan); ok {
		return ts.StoreType
	}
	return kv.TiKV
}

// attach2Task4PhysicalUnionScan implements PhysicalPlan interface.
func attach2Task4PhysicalUnionScan(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalUnionScan)
	// when it arrives here, physical union scan will absolutely require a root task type,
	// so convert child to root task type first.
	task := tasks[0].ConvertToRootTask(p.SCtx())
	// We need to pull the projection under unionScan upon unionScan.
	// Since the projection only prunes columns, it's ok the put it upon unionScan.
	if sel, ok := task.Plan().(*physicalop.PhysicalSelection); ok {
		if pj, ok := sel.Children()[0].(*physicalop.PhysicalProjection); ok {
			// Convert unionScan->selection->projection to projection->unionScan->selection.
			// shallow clone sel
			clonedSel := *sel
			clonedSel.SetChildren(pj.Children()...)
			// set child will substitute original child slices, not an in-place change.
			p.SetChildren(&clonedSel)
			p.SetStats(task.Plan().StatsInfo())
			rt := task.(*RootTask)
			rt.SetPlan(p) // root task plan current is p headed.
			// shallow clone proj.
			clonedProj := *pj
			// set child will substitute original child slices, not an in-place change.
			clonedProj.SetChildren(p)
			return clonedProj.Attach2Task(task)
		}
	}
	if pj, ok := task.Plan().(*physicalop.PhysicalProjection); ok {
		// Convert unionScan->projection to projection->unionScan, because unionScan can't handle projection as its children.
		p.SetChildren(pj.Children()...)
		p.SetStats(task.Plan().StatsInfo())
		rt, _ := task.(*RootTask)
		rt.SetPlan(pj.Children()[0])
		// shallow clone proj.
		clonedProj := *pj
		// set child will substitute original child slices, not an in-place change.
		clonedProj.SetChildren(p)
		return clonedProj.Attach2Task(p.BasePhysicalPlan.Attach2Task(task))
	}
	p.SetStats(task.Plan().StatsInfo())
	// once task is copTask type here, it may be converted proj + tablePlan here.
	// then when it's connected with union-scan here, we may get as: union-scan + proj + tablePlan
	// while proj is not allowed to be built under union-scan in execution layer currently.
	return p.BasePhysicalPlan.Attach2Task(task)
}

// attach2Task4PhysicalApply implements PhysicalPlan interface.
func attach2Task4PhysicalApply(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalApply)
	lTask := tasks[0].ConvertToRootTask(p.SCtx())
	rTask := tasks[1].ConvertToRootTask(p.SCtx())
	p.SetChildren(lTask.Plan(), rTask.Plan())
	p.SetSchema(BuildPhysicalJoinSchema(p.JoinType, p))
	t := &RootTask{}
	t.SetPlan(p)
	// inherit left and right child's warnings.
	t.warnings.CopyFrom(&lTask.(*RootTask).warnings, &rTask.(*RootTask).warnings)
	return t
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalIndexMergeJoin) Attach2Task(tasks ...base.Task) base.Task {
	outerTask := tasks[1-p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.Plan(), p.InnerPlan)
	} else {
		p.SetChildren(p.InnerPlan, outerTask.Plan())
	}
	t := &RootTask{}
	t.SetPlan(p)
	return t
}

func indexHashJoinAttach2TaskV1(p *physicalop.PhysicalIndexHashJoin, tasks ...base.Task) base.Task {
	outerTask := tasks[1-p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.Plan(), p.InnerPlan)
	} else {
		p.SetChildren(p.InnerPlan, outerTask.Plan())
	}
	t := &RootTask{}
	t.SetPlan(p)
	return t
}

func indexHashJoinAttach2TaskV2(p *physicalop.PhysicalIndexHashJoin, tasks ...base.Task) base.Task {
	outerTask := tasks[1-p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	innerTask := tasks[p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	// only fill the wrapped physical index join is ok.
	completePhysicalIndexJoin(&p.PhysicalIndexJoin, innerTask.(*RootTask), innerTask.Plan().Schema(), outerTask.Plan().Schema(), true)
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.Plan(), innerTask.Plan())
	} else {
		p.SetChildren(innerTask.Plan(), outerTask.Plan())
	}
	t := &RootTask{}
	t.SetPlan(p)
	t.warnings.CopyFrom(&outerTask.(*RootTask).warnings, &innerTask.(*RootTask).warnings)
	return t
}

// attach2Task4PhysicalIndexHashJoin implements PhysicalPlan interface.
func attach2Task4PhysicalIndexHashJoin(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalIndexHashJoin)
	if p.SCtx().GetSessionVars().EnhanceIndexJoinBuildV2 {
		return indexHashJoinAttach2TaskV2(p, tasks...)
	}
	return indexHashJoinAttach2TaskV1(p, tasks...)
}

func indexJoinAttach2TaskV1(p *physicalop.PhysicalIndexJoin, tasks ...base.Task) base.Task {
	outerTask := tasks[1-p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.Plan(), p.InnerPlan)
	} else {
		p.SetChildren(p.InnerPlan, outerTask.Plan())
	}
	t := &RootTask{}
	t.SetPlan(p)
	return t
}

func indexJoinAttach2TaskV2(p *physicalop.PhysicalIndexJoin, tasks ...base.Task) base.Task {
	outerTask := tasks[1-p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	innerTask := tasks[p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	completePhysicalIndexJoin(p, innerTask.(*RootTask), innerTask.Plan().Schema(), outerTask.Plan().Schema(), true)
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.Plan(), innerTask.Plan())
	} else {
		p.SetChildren(innerTask.Plan(), outerTask.Plan())
	}
	t := &RootTask{}
	t.SetPlan(p)
	t.warnings.CopyFrom(&outerTask.(*RootTask).warnings, &innerTask.(*RootTask).warnings)
	return t
}

func attach2Task4PhysicalIndexJoin(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalIndexJoin)
	if p.SCtx().GetSessionVars().EnhanceIndexJoinBuildV2 {
		return indexJoinAttach2TaskV2(p, tasks...)
	}
	return indexJoinAttach2TaskV1(p, tasks...)
}

// RowSize for cost model ver2 is simplified, always use this function to calculate row size.
func getAvgRowSize(stats *property.StatsInfo, cols []*expression.Column) (size float64) {
	if stats.HistColl != nil {
		size = max(cardinality.GetAvgRowSizeDataInDiskByRows(stats.HistColl, cols), 0)
	} else {
		// Estimate using just the type info.
		for _, col := range cols {
			size += max(float64(chunk.EstimateTypeWidth(col.GetStaticType())), 0)
		}
	}
	return
}

// attach2Task4PhysicalHashJoin implements PhysicalPlan interface.
func attach2Task4PhysicalHashJoin(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalHashJoin)
	if p.StoreTp == kv.TiFlash {
		return attach2TaskForTiFlash4PhysicalHashJoin(p, tasks...)
	}
	rTask := tasks[1].ConvertToRootTask(p.SCtx())
	lTask := tasks[0].ConvertToRootTask(p.SCtx())
	p.SetChildren(lTask.Plan(), rTask.Plan())
	task := &RootTask{}
	task.SetPlan(p)
	task.warnings.CopyFrom(&rTask.(*RootTask).warnings, &lTask.(*RootTask).warnings)
	return task
}

func attach2TaskForTiFlash4PhysicalHashJoin(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalHashJoin)
	rTask, rok := tasks[1].(*CopTask)
	lTask, lok := tasks[0].(*CopTask)
	if !lok || !rok {
		return attach2TaskForMpp4PhysicalHashJoin(p, tasks...)
	}
	rRoot := rTask.ConvertToRootTask(p.SCtx())
	lRoot := lTask.ConvertToRootTask(p.SCtx())
	p.SetChildren(lRoot.Plan(), rRoot.Plan())
	p.SetSchema(BuildPhysicalJoinSchema(p.JoinType, p))
	task := &RootTask{}
	task.SetPlan(p)
	task.warnings.CopyFrom(&rTask.warnings, &lTask.warnings)
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

func negotiateCommonType(lType, rType *types.FieldType) (_ *types.FieldType, _, _ bool) {
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

func getProj(ctx base.PlanContext, p base.PhysicalPlan) *physicalop.PhysicalProjection {
	proj := physicalop.PhysicalProjection{
		Exprs: make([]expression.Expression, 0, len(p.Schema().Columns)),
	}.Init(ctx, p.StatsInfo(), p.QueryBlockOffset())
	for _, col := range p.Schema().Columns {
		proj.Exprs = append(proj.Exprs, col)
	}
	proj.SetSchema(p.Schema().Clone())
	proj.SetChildren(p)
	return proj
}

func appendExpr(p *physicalop.PhysicalProjection, expr expression.Expression) *expression.Column {
	p.Exprs = append(p.Exprs, expr)

	col := &expression.Column{
		UniqueID: p.SCtx().GetSessionVars().AllocPlanColumnID(),
		RetType:  expr.GetType(p.SCtx().GetExprCtx().GetEvalCtx()),
	}
	col.SetCoercibility(expr.Coercibility())
	p.Schema().Append(col)
	return col
}

// TiFlash join require that partition key has exactly the same type, while TiDB only guarantee the partition key is the same catalog,
// so if the partition key type is not exactly the same, we need add a projection below the join or exchanger if exists.
func convertPartitionKeysIfNeed4PhysicalHashJoin(pp base.PhysicalPlan, lTask, rTask *MppTask) (_, _ *MppTask) {
	p := pp.(*physicalop.PhysicalHashJoin)
	lp := lTask.p
	if _, ok := lp.(*physicalop.PhysicalExchangeReceiver); ok {
		lp = lp.Children()[0].Children()[0]
	}
	rp := rTask.p
	if _, ok := rp.(*physicalop.PhysicalExchangeReceiver); ok {
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
	var lProj, rProj *physicalop.PhysicalProjection
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
		}, nil)
		lTask = nlTask
	}
	if rChanged {
		nrTask := rTask.Copy().(*MppTask)
		nrTask.p = rProj
		nrTask = nrTask.enforceExchanger(&property.PhysicalProperty{
			TaskTp:           property.MppTaskType,
			MPPPartitionTp:   property.HashType,
			MPPPartitionCols: rPartKeys,
		}, nil)
		rTask = nrTask
	}
	return lTask, rTask
}

func enforceExchangerByBackup4PhysicalHashJoin(pp base.PhysicalPlan, task *MppTask, idx int, expectedCols int) *MppTask {
	p := pp.(*physicalop.PhysicalHashJoin)
	if backupHashProp := p.GetChildReqProps(idx); backupHashProp != nil {
		if len(backupHashProp.MPPPartitionCols) == expectedCols {
			return task.enforceExchangerImpl(backupHashProp)
		}
	}
	return nil
}

func attach2TaskForMpp4PhysicalHashJoin(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	const (
		left  = 0
		right = 1
	)
	rTask, rok := tasks[right].(*MppTask)
	lTask, lok := tasks[left].(*MppTask)
	if !lok || !rok {
		return base.InvalidTask
	}
	p := pp.(*physicalop.PhysicalHashJoin)
	if p.MppShuffleJoin {
		if len(lTask.hashCols) == 0 || len(rTask.hashCols) == 0 {
			// if the hash columns are empty, this is very likely a bug.
			return base.InvalidTask
		}
		if len(lTask.hashCols) != len(rTask.hashCols) {
			// if the hash columns are not the same, The most likely scenario is that
			// they have undergone exchange optimization, removing some hash columns.
			// In this case, we need to restore them on the side that is missing.
			if len(lTask.hashCols) < len(rTask.hashCols) {
				lTask = enforceExchangerByBackup4PhysicalHashJoin(p, lTask, left, len(rTask.hashCols))
			} else {
				rTask = enforceExchangerByBackup4PhysicalHashJoin(p, rTask, right, len(lTask.hashCols))
			}
			if lTask == nil || rTask == nil {
				return base.InvalidTask
			}
		}
		lTask, rTask = convertPartitionKeysIfNeed4PhysicalHashJoin(p, lTask, rTask)
	}
	p.SetChildren(lTask.Plan(), rTask.Plan())
	// outer task is the task that will pass its MPPPartitionType to the join result
	// for broadcast inner join, it should be the non-broadcast side, since broadcast side is always the build side, so
	// just use the probe side is ok.
	// for hash inner join, both side is ok, by default, we use the probe side
	// for outer join, it should always be the outer side of the join
	// for semi join, it should be the left side(the same as left out join)
	outerTaskIndex := 1 - p.InnerChildIdx
	if p.JoinType != logicalop.InnerJoin {
		if p.JoinType == logicalop.RightOuterJoin {
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
	task.warnings.CopyFrom(&rTask.warnings, &lTask.warnings)
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
	if p.Schema().Len() < defaultSchema.Len() {
		if p.Schema().Len() > 0 {
			proj := physicalop.PhysicalProjection{
				Exprs: expression.Column2Exprs(p.Schema().Columns),
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
				proj := physicalop.PhysicalProjection{
					Exprs: expr,
				}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset())

				proj.SetSchema(expression.NewSchema(&expression.Column{
					UniqueID: proj.SCtx().GetSessionVars().AllocPlanColumnID(),
					RetType:  constOne.GetType(p.SCtx().GetExprCtx().GetEvalCtx()),
				}))
				attachPlan2Task(proj, task)
			} else {
				proj := physicalop.PhysicalProjection{
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
	p.SetSchema(defaultSchema)
	return task
}

// attach2Task4PhysicalMergeJoin implements PhysicalPlan interface.
func attach2Task4PhysicalMergeJoin(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalMergeJoin)
	lTask := tasks[0].ConvertToRootTask(p.SCtx())
	rTask := tasks[1].ConvertToRootTask(p.SCtx())
	p.SetChildren(lTask.Plan(), rTask.Plan())
	t := &RootTask{}
	t.SetPlan(p)
	t.warnings.CopyFrom(&rTask.(*RootTask).warnings, &lTask.(*RootTask).warnings)
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
	p.SetStats(t.tablePlan.StatsInfo())
	// Do not inject the extra Projection even if t.needExtraProj is set, or the schema between the phase-1 agg and
	// the final agg would be broken. Please reference comments for the similar logic in
	// (*copTask).convertToRootTaskImpl() for the PhysicalTableReader case.
	// We need to refactor these logics.
	aggPushedDown := false
	switch p.tablePlan.(type) {
	case *physicalop.PhysicalHashAgg, *physicalop.PhysicalStreamAgg:
		aggPushedDown = true
	}

	if t.needExtraProj && !aggPushedDown {
		schema := t.originSchema
		proj := physicalop.PhysicalProjection{Exprs: expression.Column2Exprs(schema.Columns)}.Init(ctx, p.StatsInfo(), t.tablePlan.QueryBlockOffset(), nil)
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
		sel := physicalop.PhysicalSelection{Conditions: t.rootTaskConds}.Init(ctx, newTask.GetPlan().StatsInfo().Scale(selectivity), newTask.GetPlan().QueryBlockOffset())
		sel.FromDataSource = true
		sel.SetChildren(newTask.GetPlan())
		newTask.SetPlan(sel)
	}
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
	if cop, ok := t.(*CopTask); ok {
		suspendLimitAboveTablePlan := func() {
			newCount := p.Offset + p.Count
			childProfile := cop.tablePlan.StatsInfo()
			// but "regionNum" is unknown since the copTask can be a double read, so we ignore it now.
			stats := util.DeriveLimitStats(childProfile, float64(newCount))
			pushedDownLimit := physicalop.PhysicalLimit{PartitionBy: newPartitionBy, Count: newCount}.Init(p.SCtx(), stats, p.QueryBlockOffset())
			pushedDownLimit.SetChildren(cop.tablePlan)
			cop.tablePlan = pushedDownLimit
			// Don't use clone() so that Limit and its children share the same schema. Otherwise, the virtual generated column may not be resolved right.
			pushedDownLimit.SetSchema(pushedDownLimit.Children()[0].Schema())
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
				stats := util.DeriveLimitStats(childProfile, float64(newCount))
				pushedDownLimit := physicalop.PhysicalLimit{PartitionBy: newPartitionBy, Count: newCount}.Init(p.SCtx(), stats, p.QueryBlockOffset())
				cop = attachPlan2Task(pushedDownLimit, cop).(*CopTask)
				// Don't use clone() so that Limit and its children share the same schema. Otherwise the virtual generated column may not be resolved right.
				pushedDownLimit.SetSchema(pushedDownLimit.Children()[0].Schema())
			}
			t = cop.ConvertToRootTask(p.SCtx())
			sunk = sinkIntoIndexLookUp(p, t)
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
						stats := util.DeriveLimitStats(childProfile, float64(newCount))
						pushedDownLimit := physicalop.PhysicalLimit{PartitionBy: newPartitionBy, Count: newCount}.Init(p.SCtx(), stats, p.QueryBlockOffset())
						pushedDownLimit.SetChildren(partialScan)
						pushedDownLimit.SetSchema(pushedDownLimit.Children()[0].Schema())
						limitChildren = append(limitChildren, pushedDownLimit)
					}
					cop.idxMergePartPlans = limitChildren
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
	} else if mpp, ok := t.(*MppTask); ok {
		newCount := p.Offset + p.Count
		childProfile := mpp.Plan().StatsInfo()
		stats := util.DeriveLimitStats(childProfile, float64(newCount))
		pushedDownLimit := physicalop.PhysicalLimit{Count: newCount, PartitionBy: newPartitionBy}.Init(p.SCtx(), stats, p.QueryBlockOffset())
		mpp = attachPlan2Task(pushedDownLimit, mpp).(*MppTask)
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
	root := t.(*RootTask)
	reader, isDoubleRead := root.GetPlan().(*PhysicalIndexLookUpReader)
	proj, isProj := root.GetPlan().(*physicalop.PhysicalProjection)
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
	ts, isTableScan := reader.tablePlan.(*physicalop.PhysicalTableScan)
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

func sinkIntoIndexMerge(p *physicalop.PhysicalLimit, t base.Task) bool {
	root := t.(*RootTask)
	imReader, isIm := root.GetPlan().(*PhysicalIndexMergeReader)
	proj, isProj := root.GetPlan().(*physicalop.PhysicalProjection)
	if !isIm && !isProj {
		return false
	}
	if isProj {
		imReader, isIm = proj.Children()[0].(*PhysicalIndexMergeReader)
		if !isIm {
			return false
		}
	}
	ts, ok := imReader.tablePlan.(*physicalop.PhysicalTableScan)
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
	stats := util.DeriveLimitStats(childProfile, float64(newCount))

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
func canPushDownToTiKV(p *physicalop.PhysicalTopN, copTask *CopTask) bool {
	if !canExpressionConvertedToPB(p, kv.TiKV) {
		return false
	}
	if len(copTask.rootTaskConds) != 0 {
		return false
	}
	if !copTask.indexPlanFinished && len(copTask.idxMergePartPlans) > 0 {
		for _, partialPlan := range copTask.idxMergePartPlans {
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
func canPushDownToTiFlash(p *physicalop.PhysicalTopN, mppTask *MppTask) bool {
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
func pushLimitDownToTiDBCop(p *physicalop.PhysicalTopN, copTsk *CopTask) (base.Task, bool) {
	if copTsk.indexPlan != nil || copTsk.tablePlan == nil {
		return nil, false
	}

	var (
		selOnTblScan   *physicalop.PhysicalSelection
		selSelectivity float64
		tblScan        *physicalop.PhysicalTableScan
		err            error
		ok             bool
	)

	copTsk.tablePlan, err = copTsk.tablePlan.Clone(p.SCtx())
	if err != nil {
		return nil, false
	}
	finalTblScanPlan := copTsk.tablePlan
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

	colsProp, ok := GetPropByOrderByItems(p.ByItems)
	if !ok {
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
	stats := util.DeriveLimitStats(childProfile, float64(newCount))
	pushedLimit := physicalop.PhysicalLimit{
		Count: newCount,
	}.Init(p.SCtx(), stats, p.QueryBlockOffset())
	pushedLimit.SetSchema(copTsk.tablePlan.Schema())
	copTsk = attachPlan2Task(pushedLimit, copTsk).(*CopTask)
	child := pushedLimit.Children()[0]
	child.SetStats(child.StatsInfo().ScaleByExpectCnt(float64(newCount)))
	if selSelectivity > 0 && selSelectivity < 1 {
		scaledRowCount := child.StatsInfo().RowCount / selSelectivity
		tblScan.SetStats(tblScan.StatsInfo().ScaleByExpectCnt(scaledRowCount))
	}
	rootTask := copTsk.ConvertToRootTask(p.SCtx())
	return attachPlan2Task(p, rootTask), true
}

// Attach2Task implements the PhysicalPlan interface.
func attach2Task4PhysicalTopN(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalTopN)
	t := tasks[0].Copy()
	cols := make([]*expression.Column, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		cols = append(cols, expression.ExtractColumns(item.Expr)...)
	}
	needPushDown := len(cols) > 0
	if copTask, ok := t.(*CopTask); ok && needPushDown && copTask.getStoreType() == kv.TiDB && len(copTask.rootTaskConds) == 0 {
		newTask, changed := pushLimitDownToTiDBCop(p, copTask)
		if changed {
			return newTask
		}
	}
	if copTask, ok := t.(*CopTask); ok && needPushDown && canPushDownToTiKV(p, copTask) && len(copTask.rootTaskConds) == 0 {
		// If all columns in topN are from index plan, we push it to index plan, otherwise we finish the index plan and
		// push it to table plan.
		var pushedDownTopN *physicalop.PhysicalTopN
		var newGlobalTopN *physicalop.PhysicalTopN
		if !copTask.indexPlanFinished && canPushToIndexPlan(copTask.indexPlan, cols) {
			pushedDownTopN, newGlobalTopN = getPushedDownTopN(p, copTask.indexPlan, copTask.getStoreType())
			copTask.indexPlan = pushedDownTopN
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
			copTask.finishIndexPlan()
			pushedDownTopN, newGlobalTopN = getPushedDownTopN(p, copTask.tablePlan, copTask.getStoreType())
			copTask.tablePlan = pushedDownTopN
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
	} else if mppTask, ok := t.(*MppTask); ok && needPushDown && canPushDownToTiFlash(p, mppTask) {
		pushedDownTopN, newGlobalTopN := getPushedDownTopN(p, mppTask.p, kv.TiFlash)
		mppTask.p = pushedDownTopN
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

// attach2Task4PhysicalProjection implements PhysicalPlan interface.
func attach2Task4PhysicalProjection(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalProjection)
	t := tasks[0].Copy()
	if cop, ok := t.(*CopTask); ok {
		if (len(cop.rootTaskConds) == 0 && len(cop.idxMergePartPlans) == 0) && expression.CanExprsPushDown(util.GetPushDownCtx(p.SCtx()), p.Exprs, cop.getStoreType()) {
			copTask := attachPlan2Task(p, cop)
			return copTask
		}
	} else if mpp, ok := t.(*MppTask); ok {
		if expression.CanExprsPushDown(util.GetPushDownCtx(p.SCtx()), p.Exprs, kv.TiFlash) {
			p.SetChildren(mpp.p)
			mpp.p = p
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
	if mpp, ok := t.(*MppTask); ok {
		p.SetChildren(mpp.p)
		mpp.p = p
		return mpp
	}
	// For root task
	// since expand should be in root side accordingly, convert to root task now.
	root := t.ConvertToRootTask(p.SCtx())
	t = attachPlan2Task(p, root)
	return t
}

func attach2MppTasks4PhysicalUnionAll(p *physicalop.PhysicalUnionAll, tasks ...base.Task) base.Task {
	t := &MppTask{p: p}
	childPlans := make([]base.PhysicalPlan, 0, len(tasks))
	for _, tk := range tasks {
		if mpp, ok := tk.(*MppTask); ok && !tk.Invalid() {
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
		if _, ok := t.(*MppTask); ok {
			if p.TP() == plancodec.TypePartitionUnion {
				// In attach2MppTasks(), will attach PhysicalUnion to mppTask directly.
				// But PartitionUnion cannot pushdown to tiflash, so here disable PartitionUnion pushdown to tiflash explicitly.
				// For now, return base.InvalidTask immediately, we can refine this by letting childTask of PartitionUnion convert to rootTask.
				return base.InvalidTask
			}
			return attach2MppTasks4PhysicalUnionAll(p, tasks...)
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

// attach2Task4PhysicalSelection implements PhysicalPlan interface.
func attach2Task4PhysicalSelection(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	sel := pp.(*physicalop.PhysicalSelection)
	if mppTask, _ := tasks[0].(*MppTask); mppTask != nil { // always push to mpp task.
		if expression.CanExprsPushDown(util.GetPushDownCtx(sel.SCtx()), sel.Conditions, kv.TiFlash) {
			return attachPlan2Task(sel, mppTask.Copy())
		}
	}
	t := tasks[0].ConvertToRootTask(sel.SCtx())
	return attachPlan2Task(sel, t)
}

func inheritStatsFromBottomElemForIndexJoinInner(p base.PhysicalPlan, indexJoinInfo *IndexJoinInfo, stats *property.StatsInfo) {
	var isIndexJoin bool
	switch p.(type) {
	case *physicalop.PhysicalIndexJoin, *physicalop.PhysicalIndexHashJoin, *PhysicalIndexMergeJoin:
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
			p.SetStats(stats.Scale(1))
		case *physicalop.PhysicalProjection:
			// mainly about the rowEst, proj doesn't change that.
			p.SetStats(stats.Scale(1))
		case *physicalop.PhysicalHashAgg, *physicalop.PhysicalStreamAgg:
			// todo: for simplicity, we can just inherit it from child.
			p.SetStats(stats.Scale(1))
		case *physicalop.PhysicalUnionScan:
			// todo: for simplicity, we can just inherit it from child.
			p.SetStats(stats.Scale(1))
		default:
			p.SetStats(stats.Scale(1))
		}
	}
}

func inheritStatsFromBottomTaskForIndexJoinInner(p base.PhysicalPlan, t base.Task) {
	var indexJoinInfo *IndexJoinInfo
	switch v := t.(type) {
	case *CopTask:
		indexJoinInfo = v.IndexJoinInfo
	case *RootTask:
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
			partialAgg, finalAgg := p.NewPartialAggregate(storeType, false)
			if partialAgg != nil {
				if cop.tablePlan != nil {
					cop.finishIndexPlan()
					// the partialAgg attachment didn't follow the attachPlan2Task function, so here we actively call
					// inheritStatsFromBottomForIndexJoinInner(p, t) to inherit stats from the bottom plan for index
					// join inner side. note: partialAgg will share stats with finalAgg.
					inheritStatsFromBottomElemForIndexJoinInner(partialAgg, cop.IndexJoinInfo, cop.tablePlan.StatsInfo())
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
					// the partialAgg attachment didn't follow the attachPlan2Task function, so here we actively call
					// inheritStatsFromBottomForIndexJoinInner(p, t) to inherit stats from the bottom plan for index
					// join inner side. note: partialAgg will share stats with finalAgg.
					inheritStatsFromBottomElemForIndexJoinInner(partialAgg, cop.IndexJoinInfo, cop.indexPlan.StatsInfo())
					partialAgg.SetChildren(cop.indexPlan)
					cop.indexPlan = partialAgg
				}
			}
			// COP Task -> Root Task, warnings inherited inside.
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

func attach2TaskForMpp1Phase(p *physicalop.PhysicalHashAgg, mpp *MppTask) base.Task {
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
func adjust3StagePhaseAgg(p *physicalop.PhysicalHashAgg, partialAgg, finalAgg base.PhysicalPlan, canUse3StageAgg bool,
	groupingSets expression.GroupingSets, mpp *MppTask) (final, mid, part, proj4Part base.PhysicalPlan, _ error) {
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
	stats := mpp.p.StatsInfo().Scale(float64(1))
	stats.RowCount = stats.RowCount * float64(len(groupingSets))
	physicalExpand := physicalop.PhysicalExpand{
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
	proj4Partial := new(physicalop.PhysicalProjection).Init(p.SCtx(), mpp.p.StatsInfo(), mpp.p.QueryBlockOffset())
	for _, col := range mpp.p.Schema().Columns {
		proj4Partial.Exprs = append(proj4Partial.Exprs, col)
	}
	proj4Partial.SetSchema(mpp.p.Schema().Clone())

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
	mpp, ok := t.(*MppTask)
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
		if !mpp.needEnforceExchanger(prop, nil) {
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
			newMpp := mpp.enforceExchanger(exProp, nil)
			attachPlan2Task(middle, newMpp)
			mpp = newMpp
			if partialHashAgg, ok := partial.(*physicalop.PhysicalHashAgg); ok && len(partitionCols) != 0 {
				partialHashAgg.TiflashPreAggMode = p.SCtx().GetSessionVars().TiFlashPreAggMode
			}
		}

		// prop here still be the first generated single-partition requirement.
		newMpp := mpp.enforceExchanger(prop, nil)
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
	if cop, ok := t.(*CopTask); ok {
		if len(cop.rootTaskConds) == 0 && len(cop.idxMergePartPlans) == 0 {
			copTaskType := cop.getStoreType()
			partialAgg, finalAgg := p.NewPartialAggregate(copTaskType, false)
			if partialAgg != nil {
				if cop.tablePlan != nil {
					cop.finishIndexPlan()
					// the partialAgg attachment didn't follow the attachPlan2Task function, so here we actively call
					// inheritStatsFromBottomForIndexJoinInner(p, t) to inherit stats from the bottom plan for index
					// join inner side. note: partialAgg will share stats with finalAgg.
					inheritStatsFromBottomElemForIndexJoinInner(partialAgg, cop.IndexJoinInfo, cop.tablePlan.StatsInfo())
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
					// the partialAgg attachment didn't follow the attachPlan2Task function, so here we actively call
					// inheritStatsFromBottomForIndexJoinInner(p, t) to inherit stats from the bottom plan for index
					// join inner side. note: partialAgg will share stats with finalAgg.
					inheritStatsFromBottomElemForIndexJoinInner(partialAgg, cop.IndexJoinInfo, cop.indexPlan.StatsInfo())
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
		return attach2TaskForMpp(p, tasks...)
	} else {
		attachPlan2Task(p, t)
	}
	return t
}

func attach2TaskForMPP4PhysicalWindow(p *physicalop.PhysicalWindow, mpp *MppTask) base.Task {
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
	if mpp, ok := tasks[0].Copy().(*MppTask); ok && p.StoreTp == kv.TiFlash {
		return attach2TaskForMPP4PhysicalWindow(p, mpp)
	}
	t := tasks[0].ConvertToRootTask(p.SCtx())
	return attachPlan2Task(p.Self, t)
}

// attach2Task4PhysicalCTEStorage implements the PhysicalPlan interface.
func attach2Task4PhysicalCTEStorage(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalCTEStorage)
	t := tasks[0].Copy()
	if mpp, ok := t.(*MppTask); ok {
		p.SetChildren(t.Plan())
		nt := &MppTask{
			p:           p,
			partTp:      mpp.partTp,
			hashCols:    mpp.hashCols,
			tblColHists: mpp.tblColHists,
		}
		nt.warnings.CopyFrom(&mpp.warnings)
		return nt
	}
	t.ConvertToRootTask(p.SCtx())
	p.SetChildren(t.Plan())
	ta := &RootTask{}
	ta.SetPlan(p)
	ta.warnings.CopyFrom(&t.(*RootTask).warnings)
	return ta
}

// attach2Task4PhysicalSequence implements PhysicalSequence.Attach2Task.
func attach2Task4PhysicalSequence(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalSequence)

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
	tmpWarnings := make([]*simpleWarnings, 0, len(tasks))
	for _, t := range tasks {
		if mpp, ok := t.(*MppTask); ok {
			tmpWarnings = append(tmpWarnings, &mpp.warnings)
			continue
		}
		if root, ok := t.(*RootTask); ok {
			tmpWarnings = append(tmpWarnings, &root.warnings)
			continue
		}
		if cop, ok := t.(*CopTask); ok {
			tmpWarnings = append(tmpWarnings, &cop.warnings)
		}
	}
	mppTask.warnings.CopyFrom(tmpWarnings...)
	return mppTask
}

func collectPartitionInfosFromMPPPlan(p *PhysicalTableReader, mppPlan base.PhysicalPlan) {
	switch x := mppPlan.(type) {
	case *physicalop.PhysicalTableScan:
		p.TableScanAndPartitionInfos = append(p.TableScanAndPartitionInfos, physicalop.TableScanAndPartitionInfo{TableScan: x, PhysPlanPartInfo: x.PlanPartInfo})
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
	if ts, ok := p.(*physicalop.PhysicalTableScan); ok {
		return float64(len(ts.Ranges)) * float64(len(ts.Columns)) * ts.SCtx().GetSessionVars().GetSeekFactor(ts.Table)
	}
	for _, c := range p.Children() {
		cost += accumulateNetSeekCost4MPP(c)
	}
	return
}

func tryExpandVirtualColumn(p base.PhysicalPlan) {
	if ts, ok := p.(*physicalop.PhysicalTableScan); ok {
		ts.Columns = ExpandVirtualColumn(ts.Columns, ts.Schema(), ts.Table.Columns)
		return
	}
	for _, child := range p.Children() {
		tryExpandVirtualColumn(child)
	}
}

func (t *MppTask) needEnforceExchanger(prop *property.PhysicalProperty, fd *funcdep.FDSet) bool {
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
		if fd != nil && len(t.hashCols) != 0 {
			return prop.NeedMPPExchangeByEquivalence(t.hashCols, fd)
		}
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

func (t *MppTask) enforceExchanger(prop *property.PhysicalProperty, fd *funcdep.FDSet) *MppTask {
	if !t.needEnforceExchanger(prop, fd) {
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
	sender := physicalop.PhysicalExchangeSender{
		ExchangeType: prop.MPPPartitionTp.ToExchangeType(),
		HashCols:     prop.MPPPartitionCols,
	}.Init(ctx, t.p.StatsInfo())

	if ctx.GetSessionVars().ChooseMppVersion() >= kv.MppVersionV1 {
		sender.CompressionMode = ctx.GetSessionVars().ChooseMppExchangeCompressionMode()
	}

	sender.SetChildren(t.p)
	receiver := physicalop.PhysicalExchangeReceiver{}.Init(ctx, t.p.StatsInfo())
	receiver.SetChildren(sender)
	nt := &MppTask{
		p:        receiver,
		partTp:   prop.MPPPartitionTp,
		hashCols: prop.MPPPartitionCols,
	}
	nt.warnings.CopyFrom(&t.warnings)
	return nt
}

// IndexJoinInfo is generated by index join's inner ds, which will build their own index choice based
// the indexJoinProp pushed down by index join. While index join still need some feedback by this kind
// choice info to make index join runtime compatible, like IdxColLens will help truncate the index key
// to construct suitable lookup contents. KeyOff2IdxOff will help ds to quickly locate the index column
// from lookup contents. Ranges will be used to rebuild the underlying index range if there is any parameter
// affecting the index join's inner range. CompareFilters will be used to quickly evaluate the last-col's
// non-eq range.
// This kind of IndexJoinInfo will be wrapped as a part of CopTask or RootTask, which will be passed upward
// to targeted indexJoin to complete the physicalIndexJoin's detail: ref:
type IndexJoinInfo struct {
	// The following fields are used to keep index join aware of inner plan's index/pk choice.
	IdxColLens     []int
	KeyOff2IdxOff  []int
	Ranges         ranger.MutableRanges
	CompareFilters *physicalop.ColWithCmpFuncManager
}
