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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func attachPlan2Task(p base.PhysicalPlan, t base.Task) base.Task {
	// since almost all current physical plan will be attached to bottom encapsulated task.
	// we do the stats inheritance here for all the index join inner task.
	inheritStatsFromBottomTaskForIndexJoinInner(p, t)
	switch v := t.(type) {
	case *physicalop.CopTask:
		if v.IndexPlanFinished {
			p.SetChildren(v.TablePlan)
			v.TablePlan = p
		} else {
			p.SetChildren(v.IndexPlan)
			v.IndexPlan = p
		}
	case *physicalop.RootTask:
		p.SetChildren(v.GetPlan())
		v.SetPlan(p)
	case *physicalop.MppTask:
		p.SetChildren(v.Plan())
		v.SetPlan(p)
	}
	return t
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
			rt := task.(*physicalop.RootTask)
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
		rt, _ := task.(*physicalop.RootTask)
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
	p.SetSchema(physicalop.BuildPhysicalJoinSchema(p.JoinType, p))
	t := &physicalop.RootTask{}
	t.SetPlan(p)
	// inherit left and right child's warnings.
	t.Warnings.CopyFrom(&lTask.(*physicalop.RootTask).Warnings, &rTask.(*physicalop.RootTask).Warnings)
	return t
}

// attach2Task4PhysicalIndexMergeJoin implements PhysicalPlan interface.
func attach2Task4PhysicalIndexMergeJoin(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalIndexMergeJoin)
	outerTask := tasks[1-p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.Plan(), p.InnerPlan)
	} else {
		p.SetChildren(p.InnerPlan, outerTask.Plan())
	}
	t := &physicalop.RootTask{}
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
	t := &physicalop.RootTask{}
	t.SetPlan(p)
	return t
}

func indexHashJoinAttach2TaskV2(p *physicalop.PhysicalIndexHashJoin, tasks ...base.Task) base.Task {
	outerTask := tasks[1-p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	innerTask := tasks[p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	// only fill the wrapped physical index join is ok.
	completePhysicalIndexJoin(&p.PhysicalIndexJoin, innerTask.(*physicalop.RootTask), innerTask.Plan().Schema(), outerTask.Plan().Schema(), true)
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.Plan(), innerTask.Plan())
	} else {
		p.SetChildren(innerTask.Plan(), outerTask.Plan())
	}
	t := &physicalop.RootTask{}
	t.SetPlan(p)
	t.Warnings.CopyFrom(&outerTask.(*physicalop.RootTask).Warnings, &innerTask.(*physicalop.RootTask).Warnings)
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
	t := &physicalop.RootTask{}
	t.SetPlan(p)
	return t
}

func indexJoinAttach2TaskV2(p *physicalop.PhysicalIndexJoin, tasks ...base.Task) base.Task {
	outerTask := tasks[1-p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	innerTask := tasks[p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	completePhysicalIndexJoin(p, innerTask.(*physicalop.RootTask), innerTask.Plan().Schema(), outerTask.Plan().Schema(), true)
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.Plan(), innerTask.Plan())
	} else {
		p.SetChildren(innerTask.Plan(), outerTask.Plan())
	}
	t := &physicalop.RootTask{}
	t.SetPlan(p)
	t.Warnings.CopyFrom(&outerTask.(*physicalop.RootTask).Warnings, &innerTask.(*physicalop.RootTask).Warnings)
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
	task := &physicalop.RootTask{}
	task.SetPlan(p)
	task.Warnings.CopyFrom(&rTask.(*physicalop.RootTask).Warnings, &lTask.(*physicalop.RootTask).Warnings)
	return task
}

func attach2TaskForTiFlash4PhysicalHashJoin(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalHashJoin)
	rTask, rok := tasks[1].(*physicalop.CopTask)
	lTask, lok := tasks[0].(*physicalop.CopTask)
	if !lok || !rok {
		return attach2TaskForMpp4PhysicalHashJoin(p, tasks...)
	}
	rRoot := rTask.ConvertToRootTask(p.SCtx())
	lRoot := lTask.ConvertToRootTask(p.SCtx())
	p.SetChildren(lRoot.Plan(), rRoot.Plan())
	p.SetSchema(physicalop.BuildPhysicalJoinSchema(p.JoinType, p))
	task := &physicalop.RootTask{}
	task.SetPlan(p)
	task.Warnings.CopyFrom(&rTask.Warnings, &lTask.Warnings)
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
func convertPartitionKeysIfNeed4PhysicalHashJoin(pp base.PhysicalPlan, lTask, rTask *physicalop.MppTask) (_, _ *physicalop.MppTask) {
	p := pp.(*physicalop.PhysicalHashJoin)
	lp := lTask.Plan()
	if _, ok := lp.(*physicalop.PhysicalExchangeReceiver); ok {
		lp = lp.Children()[0].Children()[0]
	}
	rp := rTask.Plan()
	if _, ok := rp.(*physicalop.PhysicalExchangeReceiver); ok {
		rp = rp.Children()[0].Children()[0]
	}
	// to mark if any partition key needs to convert
	lMask := make([]bool, len(lTask.HashCols))
	rMask := make([]bool, len(rTask.HashCols))
	cTypes := make([]*types.FieldType, len(lTask.HashCols))
	lChanged := false
	rChanged := false
	for i := range lTask.HashCols {
		lKey := lTask.HashCols[i]
		rKey := rTask.HashCols[i]
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

	lPartKeys := make([]*property.MPPPartitionColumn, 0, len(rTask.HashCols))
	rPartKeys := make([]*property.MPPPartitionColumn, 0, len(lTask.HashCols))
	for i := range lTask.HashCols {
		lKey := lTask.HashCols[i]
		rKey := rTask.HashCols[i]
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
		nlTask := lTask.Copy().(*physicalop.MppTask)
		nlTask.SetPlan(lProj)
		nlTask = nlTask.EnforceExchanger(&property.PhysicalProperty{
			TaskTp:           property.MppTaskType,
			MPPPartitionTp:   property.HashType,
			MPPPartitionCols: lPartKeys,
		}, nil)
		lTask = nlTask
	}
	if rChanged {
		nrTask := rTask.Copy().(*physicalop.MppTask)
		nrTask.SetPlan(rProj)
		nrTask = nrTask.EnforceExchanger(&property.PhysicalProperty{
			TaskTp:           property.MppTaskType,
			MPPPartitionTp:   property.HashType,
			MPPPartitionCols: rPartKeys,
		}, nil)
		rTask = nrTask
	}
	return lTask, rTask
}

func enforceExchangerByBackup4PhysicalHashJoin(pp base.PhysicalPlan, task *physicalop.MppTask, idx int, expectedCols int) *physicalop.MppTask {
	p := pp.(*physicalop.PhysicalHashJoin)
	if backupHashProp := p.GetChildReqProps(idx); backupHashProp != nil {
		if len(backupHashProp.MPPPartitionCols) == expectedCols {
			return task.EnforceExchangerImpl(backupHashProp)
		}
	}
	return nil
}

func attach2TaskForMpp4PhysicalHashJoin(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	const (
		left  = 0
		right = 1
	)
	rTask, rok := tasks[right].(*physicalop.MppTask)
	lTask, lok := tasks[left].(*physicalop.MppTask)
	if !lok || !rok {
		return base.InvalidTask
	}
	p := pp.(*physicalop.PhysicalHashJoin)
	if p.MppShuffleJoin {
		if len(lTask.HashCols) == 0 || len(rTask.HashCols) == 0 {
			// if the hash columns are empty, this is very likely a bug.
			return base.InvalidTask
		}
		if len(lTask.HashCols) != len(rTask.HashCols) {
			// if the hash columns are not the same, The most likely scenario is that
			// they have undergone exchange optimization, removing some hash columns.
			// In this case, we need to restore them on the side that is missing.
			if len(lTask.HashCols) < len(rTask.HashCols) {
				lTask = enforceExchangerByBackup4PhysicalHashJoin(p, lTask, left, len(rTask.HashCols))
			} else {
				rTask = enforceExchangerByBackup4PhysicalHashJoin(p, rTask, right, len(lTask.HashCols))
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
	if p.JoinType != base.InnerJoin {
		if p.JoinType == base.RightOuterJoin {
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
	task := physicalop.NewMppTask(p,
		outerTask.GetPartitionType(),
		outerTask.GetHashCols(),
		nil, rTask.GetWarnings(), lTask.GetWarnings())
	// Current TiFlash doesn't support receive Join executors' schema info directly from TiDB.
	// Instead, it calculates Join executors' output schema using algorithm like BuildPhysicalJoinSchema which
	// produces full semantic schema.
	// Thus, the column prune optimization achievements will be abandoned here.
	// To avoid the performance issue, add a projection here above the Join operator to prune useless columns explicitly.
	// TODO(hyb): transfer Join executors' schema to TiFlash through DagRequest, and use it directly in TiFlash.
	defaultSchema := physicalop.BuildPhysicalJoinSchema(p.JoinType, p)
	hashColArray := make([]*expression.Column, 0, len(task.HashCols))
	// For task.hashCols, these columns may not be contained in pruned columns:
	// select A.id from A join B on A.id = B.id; Suppose B is probe side, and it's hash inner join.
	// After column prune, the output schema of A join B will be A.id only; while the task's hashCols will be B.id.
	// To make matters worse, the hashCols may be used to check if extra cast projection needs to be added, then the newly
	// added projection will expect B.id as input schema. So make sure hashCols are included in task.p's schema.
	// TODO: planner should takes the hashCols attribute into consideration when perform column pruning; Or provide mechanism
	// to constraint hashCols are always chosen inside Join's pruned schema
	for _, hashCol := range task.HashCols {
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

				clonedHashColArray := make([]*expression.Column, 0, len(task.HashCols))
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
	t := &physicalop.RootTask{}
	t.SetPlan(p)
	t.Warnings.CopyFrom(&rTask.(*physicalop.RootTask).Warnings, &lTask.(*physicalop.RootTask).Warnings)
	return t
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
