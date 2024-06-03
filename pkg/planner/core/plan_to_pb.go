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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
)

// ToPB implements PhysicalPlan ToPB interface.
func (p *basePhysicalPlan) ToPB(_ *base.BuildPBContext, _ kv.StoreType) (*tipb.Executor, error) {
	return nil, errors.Errorf("plan %s fails converts to PB", p.Plan.ExplainID())
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalExpand) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	if len(p.LevelExprs) > 0 {
		return p.toPBV2(ctx, storeType)
	}
	client := ctx.GetClient()
	groupingSetsPB, err := p.GroupingSets.ToPB(ctx.GetExprCtx().GetEvalCtx(), client)
	if err != nil {
		return nil, err
	}
	expand := &tipb.Expand{
		GroupingSets: groupingSetsPB,
	}
	executorID := ""
	if storeType == kv.TiFlash {
		var err error
		expand.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeExpand, Expand: expand, ExecutorId: &executorID}, nil
}

func (p *PhysicalExpand) toPBV2(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	client := ctx.GetClient()
	projExprsPB := make([]*tipb.ExprSlice, 0, len(p.LevelExprs))
	evalCtx := ctx.GetExprCtx().GetEvalCtx()
	for _, exprs := range p.LevelExprs {
		expressionsPB, err := expression.ExpressionsToPBList(evalCtx, exprs, client)
		if err != nil {
			return nil, err
		}
		projExprsPB = append(projExprsPB, &tipb.ExprSlice{Exprs: expressionsPB})
	}
	expand2 := &tipb.Expand2{
		ProjExprs:            projExprsPB,
		GeneratedOutputNames: p.ExtraGroupingColNames,
	}
	executorID := ""
	if storeType == kv.TiFlash {
		var err error
		expand2.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeExpand2, Expand2: expand2, ExecutorId: &executorID}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalHashAgg) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	client := ctx.GetClient()
	groupByExprs, err := expression.ExpressionsToPBList(ctx.GetExprCtx().GetEvalCtx(), p.GroupByItems, client)
	if err != nil {
		return nil, err
	}
	aggExec := &tipb.Aggregation{
		GroupBy: groupByExprs,
	}
	pushDownCtx := GetPushDownCtx(p.SCtx())
	for _, aggFunc := range p.AggFuncs {
		agg, err := aggregation.AggFuncToPBExpr(pushDownCtx, aggFunc, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		aggExec.AggFunc = append(aggExec.AggFunc, agg)
	}
	executorID := ""
	if storeType == kv.TiFlash {
		var err error
		aggExec.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &tipb.Executor{
		Tp:                            tipb.ExecType_TypeAggregation,
		Aggregation:                   aggExec,
		ExecutorId:                    &executorID,
		FineGrainedShuffleStreamCount: p.TiFlashFineGrainedShuffleStreamCount,
		FineGrainedShuffleBatchSize:   ctx.TiFlashFineGrainedShuffleBatchSize,
	}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalStreamAgg) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	client := ctx.GetClient()
	evalCtx := ctx.GetExprCtx().GetEvalCtx()
	pushDownCtx := GetPushDownCtxFromBuildPBContext(ctx)
	groupByExprs, err := expression.ExpressionsToPBList(evalCtx, p.GroupByItems, client)
	if err != nil {
		return nil, err
	}
	aggExec := &tipb.Aggregation{
		GroupBy: groupByExprs,
	}
	for _, aggFunc := range p.AggFuncs {
		agg, err := aggregation.AggFuncToPBExpr(pushDownCtx, aggFunc, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		aggExec.AggFunc = append(aggExec.AggFunc, agg)
	}
	executorID := ""
	if storeType == kv.TiFlash {
		var err error
		aggExec.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeStreamAgg, Aggregation: aggExec, ExecutorId: &executorID}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalSelection) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	client := ctx.GetClient()
	conditions, err := expression.ExpressionsToPBList(ctx.GetExprCtx().GetEvalCtx(), p.Conditions, client)
	if err != nil {
		return nil, err
	}
	selExec := &tipb.Selection{
		Conditions: conditions,
	}
	executorID := ""
	if storeType == kv.TiFlash {
		var err error
		selExec.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeSelection, Selection: selExec, ExecutorId: &executorID}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalProjection) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	client := ctx.GetClient()
	exprs, err := expression.ProjectionExpressionsToPBList(ctx.GetExprCtx().GetEvalCtx(), p.Exprs, client)
	if err != nil {
		return nil, err
	}
	projExec := &tipb.Projection{
		Exprs: exprs,
	}
	executorID := ""
	if !(storeType == kv.TiFlash || storeType == kv.TiKV) {
		return nil, errors.Errorf("the projection can only be pushed down to TiFlash or TiKV now, not %s", storeType.Name())
	}
	projExec.Child, err = p.children[0].ToPB(ctx, storeType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	executorID = p.ExplainID().String()
	return &tipb.Executor{Tp: tipb.ExecType_TypeProjection, Projection: projExec, ExecutorId: &executorID}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalTopN) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	client := ctx.GetClient()
	topNExec := &tipb.TopN{
		Limit: p.Count,
	}
	evalCtx := ctx.GetExprCtx().GetEvalCtx()
	for _, item := range p.ByItems {
		topNExec.OrderBy = append(topNExec.OrderBy, expression.SortByItemToPB(evalCtx, client, item.Expr, item.Desc))
	}
	for _, item := range p.PartitionBy {
		topNExec.PartitionBy = append(topNExec.PartitionBy, expression.SortByItemToPB(evalCtx, client, item.Col.Clone(), item.Desc))
	}
	executorID := ""
	if storeType == kv.TiFlash {
		var err error
		topNExec.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeTopN, TopN: topNExec, ExecutorId: &executorID}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalLimit) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	client := ctx.GetClient()
	limitExec := &tipb.Limit{
		Limit: p.Count,
	}
	executorID := ""
	for _, item := range p.PartitionBy {
		limitExec.PartitionBy = append(limitExec.PartitionBy, expression.SortByItemToPB(ctx.GetExprCtx().GetEvalCtx(), client, item.Col.Clone(), item.Desc))
	}
	if storeType == kv.TiFlash {
		var err error
		limitExec.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeLimit, Limit: limitExec, ExecutorId: &executorID}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalTableScan) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	if storeType == kv.TiFlash && p.Table.GetPartitionInfo() != nil && p.IsMPPOrBatchCop && p.SCtx().GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return p.partitionTableScanToPBForFlash(ctx)
	}
	tsExec := tables.BuildTableScanFromInfos(p.Table, p.Columns)
	tsExec.Desc = p.Desc
	keepOrder := p.KeepOrder
	tsExec.KeepOrder = &keepOrder
	tsExec.IsFastScan = &(ctx.TiFlashFastScan)

	if len(p.LateMaterializationFilterCondition) > 0 {
		client := ctx.GetClient()
		conditions, err := expression.ExpressionsToPBList(ctx.GetExprCtx().GetEvalCtx(), p.LateMaterializationFilterCondition, client)
		if err != nil {
			return nil, err
		}
		tsExec.PushedDownFilterConditions = conditions
	}

	var err error
	tsExec.RuntimeFilterList, err = RuntimeFilterListToPB(ctx, p.runtimeFilterList, ctx.GetClient())
	if err != nil {
		return nil, errors.Trace(err)
	}
	tsExec.MaxWaitTimeMs = int32(p.maxWaitTimeMs)

	if p.isPartition {
		tsExec.TableId = p.physicalTableID
	}
	executorID := ""
	if storeType == kv.TiFlash {
		executorID = p.ExplainID().String()
	}
	err = tables.SetPBColumnsDefaultValue(ctx.GetExprCtx(), tsExec.Columns, p.Columns)
	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tsExec, ExecutorId: &executorID}, err
}

func (p *PhysicalTableScan) partitionTableScanToPBForFlash(ctx *base.BuildPBContext) (*tipb.Executor, error) {
	ptsExec := tables.BuildPartitionTableScanFromInfos(p.Table, p.Columns, ctx.TiFlashFastScan)

	if len(p.LateMaterializationFilterCondition) > 0 {
		client := ctx.GetClient()
		conditions, err := expression.ExpressionsToPBList(ctx.GetExprCtx().GetEvalCtx(), p.LateMaterializationFilterCondition, client)
		if err != nil {
			return nil, err
		}
		ptsExec.PushedDownFilterConditions = conditions
	}

	// set runtime filter
	var err error
	ptsExec.RuntimeFilterList, err = RuntimeFilterListToPB(ctx, p.runtimeFilterList, ctx.GetClient())
	if err != nil {
		return nil, errors.Trace(err)
	}
	ptsExec.MaxWaitTimeMs = int32(p.maxWaitTimeMs)

	ptsExec.Desc = p.Desc
	executorID := p.ExplainID().String()
	err = tables.SetPBColumnsDefaultValue(ctx.GetExprCtx(), ptsExec.Columns, p.Columns)
	return &tipb.Executor{Tp: tipb.ExecType_TypePartitionTableScan, PartitionTableScan: ptsExec, ExecutorId: &executorID}, err
}

// checkCoverIndex checks whether we can pass unique info to TiKV. We should push it if and only if the length of
// range and index are equal.
func checkCoverIndex(idx *model.IndexInfo, ranges []*ranger.Range) bool {
	// If the index is (c1, c2) but the query range only contains c1, it is not a unique get.
	if !idx.Unique {
		return false
	}
	for _, rg := range ranges {
		if len(rg.LowVal) != len(idx.Columns) {
			return false
		}
		for _, v := range rg.LowVal {
			if v.IsNull() {
				// a unique index may have duplicated rows with NULLs, so we cannot set the unique attribute to true when the range has NULL
				// please see https://github.com/pingcap/tidb/issues/29650 for more details
				return false
			}
		}
		for _, v := range rg.HighVal {
			if v.IsNull() {
				return false
			}
		}
	}
	return true
}

// FindColumnInfoByID finds ColumnInfo in cols by ID.
func FindColumnInfoByID(colInfos []*model.ColumnInfo, id int64) *model.ColumnInfo {
	for _, info := range colInfos {
		if info.ID == id {
			return info
		}
	}
	return nil
}

// ToPB generates the pb structure.
func (e *PhysicalExchangeSender) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	child, err := e.Children()[0].ToPB(ctx, kv.TiFlash)
	if err != nil {
		return nil, errors.Trace(err)
	}

	encodedTask := make([][]byte, 0, len(e.TargetTasks))

	for _, task := range e.TargetTasks {
		encodedStr, err := task.ToPB().Marshal()
		if err != nil {
			return nil, errors.Trace(err)
		}
		encodedTask = append(encodedTask, encodedStr)
	}

	encodedUpstreamCTETask := make([]*tipb.EncodedBytesSlice, 0, len(e.TargetCTEReaderTasks))
	for _, cteRTasks := range e.TargetCTEReaderTasks {
		encodedTasksForOneCTEReader := &tipb.EncodedBytesSlice{
			EncodedTasks: make([][]byte, 0, len(cteRTasks)),
		}
		for _, task := range cteRTasks {
			encodedStr, err := task.ToPB().Marshal()
			if err != nil {
				return nil, err
			}
			encodedTasksForOneCTEReader.EncodedTasks = append(encodedTasksForOneCTEReader.EncodedTasks, encodedStr)
		}
		encodedUpstreamCTETask = append(encodedUpstreamCTETask, encodedTasksForOneCTEReader)
	}

	hashCols := make([]expression.Expression, 0, len(e.HashCols))
	hashColTypes := make([]*tipb.FieldType, 0, len(e.HashCols))
	for _, col := range e.HashCols {
		hashCols = append(hashCols, col.Col)
		tp, err := expression.ToPBFieldTypeWithCheck(col.Col.RetType, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tp.Collate = col.CollateID
		hashColTypes = append(hashColTypes, tp)
	}
	allFieldTypes := make([]*tipb.FieldType, 0, len(e.Schema().Columns))
	for _, column := range e.Schema().Columns {
		pbType, err := expression.ToPBFieldTypeWithCheck(column.RetType, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		allFieldTypes = append(allFieldTypes, pbType)
	}
	hashColPb, err := expression.ExpressionsToPBList(ctx.GetExprCtx().GetEvalCtx(), hashCols, ctx.GetClient())
	if err != nil {
		return nil, errors.Trace(err)
	}
	ecExec := &tipb.ExchangeSender{
		Tp:                  e.ExchangeType,
		EncodedTaskMeta:     encodedTask,
		PartitionKeys:       hashColPb,
		Child:               child,
		Types:               hashColTypes,
		AllFieldTypes:       allFieldTypes,
		Compression:         e.CompressionMode.ToTipbCompressionMode(),
		UpstreamCteTaskMeta: encodedUpstreamCTETask,
	}
	executorID := e.ExplainID().String()
	return &tipb.Executor{
		Tp:                            tipb.ExecType_TypeExchangeSender,
		ExchangeSender:                ecExec,
		ExecutorId:                    &executorID,
		FineGrainedShuffleStreamCount: e.TiFlashFineGrainedShuffleStreamCount,
		FineGrainedShuffleBatchSize:   ctx.TiFlashFineGrainedShuffleBatchSize,
	}, nil
}

// ToPB generates the pb structure.
func (e *PhysicalExchangeReceiver) ToPB(ctx *base.BuildPBContext, _ kv.StoreType) (*tipb.Executor, error) {
	encodedTask := make([][]byte, 0, len(e.Tasks))

	for _, task := range e.Tasks {
		encodedStr, err := task.ToPB().Marshal()
		if err != nil {
			return nil, errors.Trace(err)
		}
		encodedTask = append(encodedTask, encodedStr)
	}

	fieldTypes := make([]*tipb.FieldType, 0, len(e.Schema().Columns))
	for _, column := range e.Schema().Columns {
		pbType, err := expression.ToPBFieldTypeWithCheck(column.RetType, kv.TiFlash)
		if err != nil {
			return nil, errors.Trace(err)
		}
		fieldTypes = append(fieldTypes, pbType)
	}
	ecExec := &tipb.ExchangeReceiver{
		EncodedTaskMeta: encodedTask,
		FieldTypes:      fieldTypes,
	}
	if e.IsCTEReader {
		encodedTaskShallowCopy := make([][]byte, len(e.Tasks))
		copy(encodedTaskShallowCopy, encodedTask)
		ecExec.OriginalCtePrdocuerTaskMeta = encodedTaskShallowCopy
	}
	executorID := e.ExplainID().String()
	return &tipb.Executor{
		Tp:                            tipb.ExecType_TypeExchangeReceiver,
		ExchangeReceiver:              ecExec,
		ExecutorId:                    &executorID,
		FineGrainedShuffleStreamCount: e.TiFlashFineGrainedShuffleStreamCount,
		FineGrainedShuffleBatchSize:   ctx.TiFlashFineGrainedShuffleBatchSize,
	}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalIndexScan) ToPB(_ *base.BuildPBContext, _ kv.StoreType) (*tipb.Executor, error) {
	columns := make([]*model.ColumnInfo, 0, p.schema.Len())
	tableColumns := p.Table.Cols()
	for _, col := range p.schema.Columns {
		if col.ID == model.ExtraHandleID {
			columns = append(columns, model.NewExtraHandleColInfo())
		} else if col.ID == model.ExtraPhysTblID {
			columns = append(columns, model.NewExtraPhysTblIDColInfo())
		} else if col.ID == model.ExtraPidColID {
			columns = append(columns, model.NewExtraPartitionIDColInfo())
		} else {
			columns = append(columns, FindColumnInfoByID(tableColumns, col.ID))
		}
	}
	var pkColIDs []int64
	if p.NeedCommonHandle {
		pkColIDs = tables.TryGetCommonPkColumnIds(p.Table)
	}
	idxExec := &tipb.IndexScan{
		TableId:          p.Table.ID,
		IndexId:          p.Index.ID,
		Columns:          util.ColumnsToProto(columns, p.Table.PKIsHandle, true),
		Desc:             p.Desc,
		PrimaryColumnIds: pkColIDs,
	}
	if p.isPartition {
		idxExec.TableId = p.physicalTableID
	}
	unique := checkCoverIndex(p.Index, p.Ranges)
	idxExec.Unique = &unique
	return &tipb.Executor{Tp: tipb.ExecType_TypeIndexScan, IdxScan: idxExec}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalHashJoin) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	client := ctx.GetClient()

	if len(p.LeftJoinKeys) > 0 && len(p.LeftNAJoinKeys) > 0 {
		return nil, errors.Errorf("join key and na join key can not both exist")
	}

	isNullAwareSemiJoin := len(p.LeftNAJoinKeys) > 0
	var leftJoinKeys, rightJoinKeys []*expression.Column
	if isNullAwareSemiJoin {
		leftJoinKeys = p.LeftNAJoinKeys
		rightJoinKeys = p.RightNAJoinKeys
	} else {
		leftJoinKeys = p.LeftJoinKeys
		rightJoinKeys = p.RightJoinKeys
	}

	leftKeys := make([]expression.Expression, 0, len(leftJoinKeys))
	rightKeys := make([]expression.Expression, 0, len(rightJoinKeys))
	for _, leftKey := range leftJoinKeys {
		leftKeys = append(leftKeys, leftKey)
	}
	for _, rightKey := range rightJoinKeys {
		rightKeys = append(rightKeys, rightKey)
	}

	lChildren, err := p.children[0].ToPB(ctx, storeType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rChildren, err := p.children[1].ToPB(ctx, storeType)
	if err != nil {
		return nil, errors.Trace(err)
	}

	evalCtx := ctx.GetExprCtx().GetEvalCtx()
	left, err := expression.ExpressionsToPBList(evalCtx, leftKeys, client)
	if err != nil {
		return nil, err
	}
	right, err := expression.ExpressionsToPBList(evalCtx, rightKeys, client)
	if err != nil {
		return nil, err
	}

	leftConditions, err := expression.ExpressionsToPBList(evalCtx, p.LeftConditions, client)
	if err != nil {
		return nil, err
	}
	rightConditions, err := expression.ExpressionsToPBList(evalCtx, p.RightConditions, client)
	if err != nil {
		return nil, err
	}

	var otherConditionsInJoin expression.CNFExprs
	var otherEqConditionsFromIn expression.CNFExprs
	/// For anti join, equal conditions from `in` clause requires additional processing,
	/// for example, treat `null` as true.
	if p.JoinType == AntiSemiJoin || p.JoinType == AntiLeftOuterSemiJoin || p.JoinType == LeftOuterSemiJoin {
		for _, condition := range p.OtherConditions {
			if expression.IsEQCondFromIn(condition) {
				otherEqConditionsFromIn = append(otherEqConditionsFromIn, condition)
			} else {
				otherConditionsInJoin = append(otherConditionsInJoin, condition)
			}
		}
	} else {
		otherConditionsInJoin = p.OtherConditions
	}
	otherConditions, err := expression.ExpressionsToPBList(evalCtx, otherConditionsInJoin, client)
	if err != nil {
		return nil, err
	}
	otherEqConditions, err := expression.ExpressionsToPBList(evalCtx, otherEqConditionsFromIn, client)
	if err != nil {
		return nil, err
	}

	pbJoinType := tipb.JoinType_TypeInnerJoin
	switch p.JoinType {
	case LeftOuterJoin:
		pbJoinType = tipb.JoinType_TypeLeftOuterJoin
	case RightOuterJoin:
		pbJoinType = tipb.JoinType_TypeRightOuterJoin
	case SemiJoin:
		pbJoinType = tipb.JoinType_TypeSemiJoin
	case AntiSemiJoin:
		pbJoinType = tipb.JoinType_TypeAntiSemiJoin
	case LeftOuterSemiJoin:
		pbJoinType = tipb.JoinType_TypeLeftOuterSemiJoin
	case AntiLeftOuterSemiJoin:
		pbJoinType = tipb.JoinType_TypeAntiLeftOuterSemiJoin
	}

	var equalConditions []*expression.ScalarFunction
	if isNullAwareSemiJoin {
		equalConditions = p.NAEqualConditions
	} else {
		equalConditions = p.EqualConditions
	}
	probeFiledTypes := make([]*tipb.FieldType, 0, len(equalConditions))
	buildFiledTypes := make([]*tipb.FieldType, 0, len(equalConditions))
	for _, equalCondition := range equalConditions {
		retType := equalCondition.RetType.Clone()
		chs, coll := equalCondition.CharsetAndCollation()
		retType.SetCharset(chs)
		retType.SetCollate(coll)
		ty, err := expression.ToPBFieldTypeWithCheck(retType, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		probeFiledTypes = append(probeFiledTypes, ty)
		buildFiledTypes = append(buildFiledTypes, ty)
	}

	// runtime filter
	rfListPB, err := RuntimeFilterListToPB(ctx, p.runtimeFilterList, client)
	if err != nil {
		return nil, errors.Trace(err)
	}
	join := &tipb.Join{
		JoinType:                pbJoinType,
		JoinExecType:            tipb.JoinExecType_TypeHashJoin,
		InnerIdx:                int64(p.InnerChildIdx),
		LeftJoinKeys:            left,
		RightJoinKeys:           right,
		ProbeTypes:              probeFiledTypes,
		BuildTypes:              buildFiledTypes,
		LeftConditions:          leftConditions,
		RightConditions:         rightConditions,
		OtherConditions:         otherConditions,
		OtherEqConditionsFromIn: otherEqConditions,
		Children:                []*tipb.Executor{lChildren, rChildren},
		IsNullAwareSemiJoin:     &isNullAwareSemiJoin,
		RuntimeFilterList:       rfListPB,
	}

	executorID := p.ExplainID().String()
	return &tipb.Executor{
		Tp:                            tipb.ExecType_TypeJoin,
		Join:                          join,
		ExecutorId:                    &executorID,
		FineGrainedShuffleStreamCount: p.TiFlashFineGrainedShuffleStreamCount,
		FineGrainedShuffleBatchSize:   ctx.TiFlashFineGrainedShuffleBatchSize,
	}, nil
}

// ToPB converts FrameBound to tipb structure.
func (fb *FrameBound) ToPB(ctx *base.BuildPBContext) (*tipb.WindowFrameBound, error) {
	pbBound := &tipb.WindowFrameBound{
		Type:      tipb.WindowBoundType(fb.Type),
		Unbounded: fb.UnBounded,
	}
	offset := fb.Num
	pbBound.Offset = &offset

	if fb.IsExplicitRange {
		rangeFrame, err := expression.ExpressionsToPBList(ctx.GetExprCtx().GetEvalCtx(), fb.CalcFuncs, ctx.GetClient())
		if err != nil {
			return nil, err
		}

		pbBound.FrameRange = rangeFrame[0]
		pbBound.CmpDataType = &fb.CmpDataType
	}

	return pbBound, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalWindow) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	client := ctx.GetClient()

	windowExec := &tipb.Window{}

	windowExec.FuncDesc = make([]*tipb.Expr, 0, len(p.WindowFuncDescs))
	evalCtx := ctx.GetExprCtx().GetEvalCtx()
	for _, desc := range p.WindowFuncDescs {
		windowExec.FuncDesc = append(windowExec.FuncDesc, aggregation.WindowFuncToPBExpr(evalCtx, client, desc))
	}
	for _, item := range p.PartitionBy {
		windowExec.PartitionBy = append(windowExec.PartitionBy, expression.SortByItemToPB(evalCtx, client, item.Col.Clone(), item.Desc))
	}
	for _, item := range p.OrderBy {
		windowExec.OrderBy = append(windowExec.OrderBy, expression.SortByItemToPB(evalCtx, client, item.Col.Clone(), item.Desc))
	}

	if p.Frame != nil {
		windowExec.Frame = &tipb.WindowFrame{
			Type: tipb.WindowFrameType(p.Frame.Type),
		}
		if p.Frame.Start != nil {
			start, err := p.Frame.Start.ToPB(ctx)
			if err != nil {
				return nil, err
			}
			windowExec.Frame.Start = start
		}
		if p.Frame.End != nil {
			end, err := p.Frame.End.ToPB(ctx)
			if err != nil {
				return nil, err
			}
			windowExec.Frame.End = end
		}
	}

	var err error
	windowExec.Child, err = p.children[0].ToPB(ctx, storeType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	executorID := p.ExplainID().String()
	return &tipb.Executor{
		Tp:                            tipb.ExecType_TypeWindow,
		Window:                        windowExec,
		ExecutorId:                    &executorID,
		FineGrainedShuffleStreamCount: p.TiFlashFineGrainedShuffleStreamCount,
		FineGrainedShuffleBatchSize:   ctx.TiFlashFineGrainedShuffleBatchSize,
	}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalSort) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	if !p.IsPartialSort {
		return nil, errors.Errorf("sort %s can't convert to pb, because it isn't a partial sort", p.Plan.ExplainID())
	}

	client := ctx.GetClient()

	sortExec := &tipb.Sort{}
	for _, item := range p.ByItems {
		sortExec.ByItems = append(sortExec.ByItems, expression.SortByItemToPB(ctx.GetExprCtx().GetEvalCtx(), client, item.Expr, item.Desc))
	}
	isPartialSort := p.IsPartialSort
	sortExec.IsPartialSort = &isPartialSort

	var err error
	sortExec.Child, err = p.children[0].ToPB(ctx, storeType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	executorID := p.ExplainID().String()
	return &tipb.Executor{
		Tp:                            tipb.ExecType_TypeSort,
		Sort:                          sortExec,
		ExecutorId:                    &executorID,
		FineGrainedShuffleStreamCount: p.TiFlashFineGrainedShuffleStreamCount,
		FineGrainedShuffleBatchSize:   ctx.TiFlashFineGrainedShuffleBatchSize,
	}, nil
}
