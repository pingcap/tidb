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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/telemetry"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
)

// ToPB implements PhysicalPlan ToPB interface.
func (p *basePhysicalPlan) ToPB(_ sessionctx.Context, _ kv.StoreType) (*tipb.Executor, error) {
	return nil, errors.Errorf("plan %s fails converts to PB", p.basePlan.ExplainID())
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalExpand) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	groupingSetsPB, err := p.GroupingSets.ToPB(sc, client)
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

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalHashAgg) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	groupByExprs, err := expression.ExpressionsToPBList(sc, p.GroupByItems, client)
	if err != nil {
		return nil, err
	}
	aggExec := &tipb.Aggregation{
		GroupBy: groupByExprs,
	}
	for _, aggFunc := range p.AggFuncs {
		agg, err := aggregation.AggFuncToPBExpr(ctx, client, aggFunc, storeType)
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
		FineGrainedShuffleBatchSize:   ctx.GetSessionVars().TiFlashFineGrainedShuffleBatchSize,
	}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalStreamAgg) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	groupByExprs, err := expression.ExpressionsToPBList(sc, p.GroupByItems, client)
	if err != nil {
		return nil, err
	}
	aggExec := &tipb.Aggregation{
		GroupBy: groupByExprs,
	}
	for _, aggFunc := range p.AggFuncs {
		agg, err := aggregation.AggFuncToPBExpr(ctx, client, aggFunc, storeType)
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
func (p *PhysicalSelection) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	conditions, err := expression.ExpressionsToPBList(sc, p.Conditions, client)
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
func (p *PhysicalProjection) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	exprs, err := expression.ExpressionsToPBList(sc, p.Exprs, client)
	if err != nil {
		return nil, err
	}
	projExec := &tipb.Projection{
		Exprs: exprs,
	}
	executorID := ""
	if storeType == kv.TiFlash || storeType == kv.TiKV {
		var err error
		projExec.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	} else {
		return nil, errors.Errorf("the projection can only be pushed down to TiFlash or TiKV now, not %s", storeType.Name())
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeProjection, Projection: projExec, ExecutorId: &executorID}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalTopN) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	topNExec := &tipb.TopN{
		Limit: p.Count,
	}
	for _, item := range p.ByItems {
		topNExec.OrderBy = append(topNExec.OrderBy, expression.SortByItemToPB(sc, client, item.Expr, item.Desc))
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
func (p *PhysicalLimit) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	limitExec := &tipb.Limit{
		Limit: p.Count,
	}
	executorID := ""
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
func (p *PhysicalTableScan) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	if storeType == kv.TiFlash && p.Table.GetPartitionInfo() != nil && p.IsMPPOrBatchCop && p.ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return p.partitionTableScanToPBForFlash(ctx)
	}
	tsExec := tables.BuildTableScanFromInfos(p.Table, p.Columns)
	tsExec.Desc = p.Desc
	keepOrder := p.KeepOrder
	tsExec.KeepOrder = &keepOrder
	tsExec.IsFastScan = &(ctx.GetSessionVars().TiFlashFastScan)

	if p.isPartition {
		tsExec.TableId = p.physicalTableID
	}
	executorID := ""
	if storeType == kv.TiFlash {
		executorID = p.ExplainID().String()

		telemetry.CurrentTiflashTableScanCount.Inc()
		if *(tsExec.IsFastScan) {
			telemetry.CurrentTiflashTableScanWithFastScanCount.Inc()
		}
	}
	err := tables.SetPBColumnsDefaultValue(ctx, tsExec.Columns, p.Columns)
	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tsExec, ExecutorId: &executorID}, err
}

func (p *PhysicalTableScan) partitionTableScanToPBForFlash(ctx sessionctx.Context) (*tipb.Executor, error) {
	ptsExec := tables.BuildPartitionTableScanFromInfos(p.Table, p.Columns, ctx.GetSessionVars().TiFlashFastScan)
	telemetry.CurrentTiflashTableScanCount.Inc()
	if *(ptsExec.IsFastScan) {
		telemetry.CurrentTiflashTableScanWithFastScanCount.Inc()
	}
	ptsExec.Desc = p.Desc
	executorID := p.ExplainID().String()
	err := tables.SetPBColumnsDefaultValue(ctx, ptsExec.Columns, p.Columns)
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
func (e *PhysicalExchangeSender) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
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
	hashColPb, err := expression.ExpressionsToPBList(ctx.GetSessionVars().StmtCtx, hashCols, ctx.GetClient())
	if err != nil {
		return nil, errors.Trace(err)
	}
	ecExec := &tipb.ExchangeSender{
		Tp:              e.ExchangeType,
		EncodedTaskMeta: encodedTask,
		PartitionKeys:   hashColPb,
		Child:           child,
		Types:           hashColTypes,
		AllFieldTypes:   allFieldTypes,
		Compression:     e.CompressionMode.ToTipbCompressionMode(),
	}
	executorID := e.ExplainID().String()
	return &tipb.Executor{
		Tp:                            tipb.ExecType_TypeExchangeSender,
		ExchangeSender:                ecExec,
		ExecutorId:                    &executorID,
		FineGrainedShuffleStreamCount: e.TiFlashFineGrainedShuffleStreamCount,
		FineGrainedShuffleBatchSize:   ctx.GetSessionVars().TiFlashFineGrainedShuffleBatchSize,
	}, nil
}

// ToPB generates the pb structure.
func (e *PhysicalExchangeReceiver) ToPB(ctx sessionctx.Context, _ kv.StoreType) (*tipb.Executor, error) {
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
	executorID := e.ExplainID().String()
	return &tipb.Executor{
		Tp:                            tipb.ExecType_TypeExchangeReceiver,
		ExchangeReceiver:              ecExec,
		ExecutorId:                    &executorID,
		FineGrainedShuffleStreamCount: e.TiFlashFineGrainedShuffleStreamCount,
		FineGrainedShuffleBatchSize:   ctx.GetSessionVars().TiFlashFineGrainedShuffleBatchSize,
	}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalIndexScan) ToPB(_ sessionctx.Context, _ kv.StoreType) (*tipb.Executor, error) {
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
	var pkColIds []int64
	if p.NeedCommonHandle {
		pkColIds = tables.TryGetCommonPkColumnIds(p.Table)
	}
	idxExec := &tipb.IndexScan{
		TableId:          p.Table.ID,
		IndexId:          p.Index.ID,
		Columns:          util.ColumnsToProto(columns, p.Table.PKIsHandle, true),
		Desc:             p.Desc,
		PrimaryColumnIds: pkColIds,
	}
	if p.isPartition {
		idxExec.TableId = p.physicalTableID
	}
	unique := checkCoverIndex(p.Index, p.Ranges)
	idxExec.Unique = &unique
	return &tipb.Executor{Tp: tipb.ExecType_TypeIndexScan, IdxScan: idxExec}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalHashJoin) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	// todo: mpp na-key toPB.
	leftJoinKeys := make([]expression.Expression, 0, len(p.LeftJoinKeys))
	rightJoinKeys := make([]expression.Expression, 0, len(p.RightJoinKeys))
	for _, leftKey := range p.LeftJoinKeys {
		leftJoinKeys = append(leftJoinKeys, leftKey)
	}
	for _, rightKey := range p.RightJoinKeys {
		rightJoinKeys = append(rightJoinKeys, rightKey)
	}
	lChildren, err := p.children[0].ToPB(ctx, storeType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rChildren, err := p.children[1].ToPB(ctx, storeType)
	if err != nil {
		return nil, errors.Trace(err)
	}

	left, err := expression.ExpressionsToPBList(sc, leftJoinKeys, client)
	if err != nil {
		return nil, err
	}
	right, err := expression.ExpressionsToPBList(sc, rightJoinKeys, client)
	if err != nil {
		return nil, err
	}

	leftConditions, err := expression.ExpressionsToPBList(sc, p.LeftConditions, client)
	if err != nil {
		return nil, err
	}
	rightConditions, err := expression.ExpressionsToPBList(sc, p.RightConditions, client)
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
	otherConditions, err := expression.ExpressionsToPBList(sc, otherConditionsInJoin, client)
	if err != nil {
		return nil, err
	}
	otherEqConditions, err := expression.ExpressionsToPBList(sc, otherEqConditionsFromIn, client)
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
	probeFiledTypes := make([]*tipb.FieldType, 0, len(p.EqualConditions))
	buildFiledTypes := make([]*tipb.FieldType, 0, len(p.EqualConditions))
	for _, equalCondition := range p.EqualConditions {
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
	// todo: arenatlx, push down hash join
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
	}

	executorID := p.ExplainID().String()
	return &tipb.Executor{
		Tp:                            tipb.ExecType_TypeJoin,
		Join:                          join,
		ExecutorId:                    &executorID,
		FineGrainedShuffleStreamCount: p.TiFlashFineGrainedShuffleStreamCount,
		FineGrainedShuffleBatchSize:   ctx.GetSessionVars().TiFlashFineGrainedShuffleBatchSize,
	}, nil
}

// ToPB converts FrameBound to tipb structure.
func (fb *FrameBound) ToPB(ctx sessionctx.Context) (*tipb.WindowFrameBound, error) {
	pbBound := &tipb.WindowFrameBound{
		Type:      tipb.WindowBoundType(fb.Type),
		Unbounded: fb.UnBounded,
	}
	offset := fb.Num
	pbBound.Offset = &offset

	calcFuncs, err := expression.ExpressionsToPBList(ctx.GetSessionVars().StmtCtx, fb.CalcFuncs, ctx.GetClient())
	if err != nil {
		return nil, err
	}

	pbBound.CalcFuncs = calcFuncs
	return pbBound, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalWindow) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()

	windowExec := &tipb.Window{}

	windowExec.FuncDesc = make([]*tipb.Expr, 0, len(p.WindowFuncDescs))
	for _, desc := range p.WindowFuncDescs {
		windowExec.FuncDesc = append(windowExec.FuncDesc, aggregation.WindowFuncToPBExpr(ctx, client, desc))
	}
	for _, item := range p.PartitionBy {
		windowExec.PartitionBy = append(windowExec.PartitionBy, expression.SortByItemToPB(sc, client, item.Col.Clone(), item.Desc))
	}
	for _, item := range p.OrderBy {
		windowExec.OrderBy = append(windowExec.OrderBy, expression.SortByItemToPB(sc, client, item.Col.Clone(), item.Desc))
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
		FineGrainedShuffleBatchSize:   ctx.GetSessionVars().TiFlashFineGrainedShuffleBatchSize,
	}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalSort) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	if !p.IsPartialSort {
		return nil, errors.Errorf("sort %s can't convert to pb, because it isn't a partial sort", p.basePlan.ExplainID())
	}

	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()

	sortExec := &tipb.Sort{}
	for _, item := range p.ByItems {
		sortExec.ByItems = append(sortExec.ByItems, expression.SortByItemToPB(sc, client, item.Expr, item.Desc))
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
		FineGrainedShuffleBatchSize:   ctx.GetSessionVars().TiFlashFineGrainedShuffleBatchSize,
	}, nil
}
