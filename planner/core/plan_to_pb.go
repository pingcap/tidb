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
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
)

// ToPB implements PhysicalPlan ToPB interface.
func (p *basePhysicalPlan) ToPB(_ sessionctx.Context, _ kv.StoreType) (*tipb.Executor, error) {
	return nil, errors.Errorf("plan %s fails converts to PB", p.basePlan.ExplainID())
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
		aggExec.AggFunc = append(aggExec.AggFunc, aggregation.AggFuncToPBExpr(sc, client, aggFunc))
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
	return &tipb.Executor{Tp: tipb.ExecType_TypeAggregation, Aggregation: aggExec, ExecutorId: &executorID}, nil
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
		aggExec.AggFunc = append(aggExec.AggFunc, aggregation.AggFuncToPBExpr(sc, client, aggFunc))
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
	if storeType == kv.TiFlash {
		var err error
		projExec.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
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
	tsExec := tables.BuildTableScanFromInfos(p.Table, p.Columns)
	tsExec.Desc = p.Desc
	if p.isPartition {
		tsExec.TableId = p.physicalTableID
	}
	executorID := ""
	if storeType == kv.TiFlash && p.IsGlobalRead {
		tsExec.NextReadEngine = tipb.EngineType_TiFlash
		ranges, err := distsql.TableHandleRangesToKVRanges(ctx.GetSessionVars().StmtCtx, []int64{tsExec.TableId}, p.Table.IsCommonHandle, p.Ranges, nil)
		if err != nil {
			return nil, err
		}
		for _, keyRange := range ranges {
			tsExec.Ranges = append(tsExec.Ranges, tipb.KeyRange{Low: keyRange.StartKey, High: keyRange.EndKey})
		}
	}
	if storeType == kv.TiFlash {
		executorID = p.ExplainID().String()
	}
	err := SetPBColumnsDefaultValue(ctx, tsExec.Columns, p.Columns)
	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tsExec, ExecutorId: &executorID}, err
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
	}
	return true
}

func findColumnInfoByID(infos []*model.ColumnInfo, id int64) *model.ColumnInfo {
	for _, info := range infos {
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

	encodedTask := make([][]byte, 0, len(e.Tasks))

	for _, task := range e.Tasks {
		encodedStr, err := task.ToPB().Marshal()
		if err != nil {
			return nil, errors.Trace(err)
		}
		encodedTask = append(encodedTask, encodedStr)
	}

	hashCols := make([]expression.Expression, 0, len(e.HashCols))
	for _, col := range e.HashCols {
		hashCols = append(hashCols, col)
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
	}
	executorID := e.ExplainID().String()
	return &tipb.Executor{
		Tp:             tipb.ExecType_TypeExchangeSender,
		ExchangeSender: ecExec,
		ExecutorId:     &executorID,
	}, nil
}

// ToPB generates the pb structure.
func (e *PhysicalExchangeReceiver) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
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
		fieldTypes = append(fieldTypes, expression.ToPBFieldType(column.RetType))
	}
	ecExec := &tipb.ExchangeReceiver{
		EncodedTaskMeta: encodedTask,
		FieldTypes:      fieldTypes,
	}
	executorID := e.ExplainID().String()
	return &tipb.Executor{
		Tp:               tipb.ExecType_TypeExchangeReceiver,
		ExchangeReceiver: ecExec,
		ExecutorId:       &executorID,
	}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalIndexScan) ToPB(ctx sessionctx.Context, _ kv.StoreType) (*tipb.Executor, error) {
	columns := make([]*model.ColumnInfo, 0, p.schema.Len())
	tableColumns := p.Table.Cols()
	for _, col := range p.schema.Columns {
		if col.ID == model.ExtraHandleID {
			columns = append(columns, model.NewExtraHandleColInfo())
		} else if col.ID == model.ExtraPidColID {
			columns = append(columns, model.NewExtraPartitionIDColInfo())
		} else {
			columns = append(columns, findColumnInfoByID(tableColumns, col.ID))
		}
	}
	var pkColIds []int64
	if p.NeedCommonHandle {
		pkColIds = tables.TryGetCommonPkColumnIds(p.Table)
	}
	idxExec := &tipb.IndexScan{
		TableId:          p.Table.ID,
		IndexId:          p.Index.ID,
		Columns:          util.ColumnsToProto(columns, p.Table.PKIsHandle),
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
	otherConditions, err := expression.ExpressionsToPBList(sc, p.OtherConditions, client)
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
		chs, coll := equalCondition.CharsetAndCollation(ctx)
		retType.Charset = chs
		retType.Collate = coll
		probeFiledTypes = append(probeFiledTypes, expression.ToPBFieldType(retType))
		buildFiledTypes = append(buildFiledTypes, expression.ToPBFieldType(retType))
	}
	join := &tipb.Join{
		JoinType:        pbJoinType,
		JoinExecType:    tipb.JoinExecType_TypeHashJoin,
		InnerIdx:        int64(p.InnerChildIdx),
		LeftJoinKeys:    left,
		RightJoinKeys:   right,
		ProbeTypes:      probeFiledTypes,
		BuildTypes:      buildFiledTypes,
		LeftConditions:  leftConditions,
		RightConditions: rightConditions,
		OtherConditions: otherConditions,
		Children:        []*tipb.Executor{lChildren, rChildren},
	}

	executorID := p.ExplainID().String()
	return &tipb.Executor{Tp: tipb.ExecType_TypeJoin, Join: join, ExecutorId: &executorID}, nil
}

// SetPBColumnsDefaultValue sets the default values of tipb.ColumnInfos.
func SetPBColumnsDefaultValue(ctx sessionctx.Context, pbColumns []*tipb.ColumnInfo, columns []*model.ColumnInfo) error {
	for i, c := range columns {
		// For virtual columns, we set their default values to NULL so that TiKV will return NULL properly,
		// They real values will be compute later.
		if c.IsGenerated() && !c.GeneratedStored {
			pbColumns[i].DefaultVal = []byte{codec.NilFlag}
		}
		if c.GetOriginDefaultValue() == nil {
			continue
		}

		sessVars := ctx.GetSessionVars()
		originStrict := sessVars.StrictSQLMode
		sessVars.StrictSQLMode = false
		d, err := table.GetColOriginDefaultValue(ctx, c)
		sessVars.StrictSQLMode = originStrict
		if err != nil {
			return err
		}

		pbColumns[i].DefaultVal, err = tablecodec.EncodeValue(sessVars.StmtCtx, nil, d)
		if err != nil {
			return err
		}
	}
	return nil
}

// SupportStreaming returns true if a pushed down operation supports using coprocessor streaming API.
// Note that this function handle pushed down physical plan only! It's called in constructDAGReq.
// Some plans are difficult (if possible) to implement streaming, and some are pointless to do so.
// TODO: Support more kinds of physical plan.
func SupportStreaming(p PhysicalPlan) bool {
	switch p.(type) {
	case *PhysicalIndexScan, *PhysicalSelection, *PhysicalTableScan:
		return true
	}
	return false
}
