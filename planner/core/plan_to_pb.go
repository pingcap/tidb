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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
)

// ToPB implements PhysicalPlan ToPB interface.
func (p *basePhysicalPlan) ToPB(_ sessionctx.Context) (*tipb.Executor, error) {
	return nil, errors.Errorf("plan %s fails converts to PB", p.basePlan.ExplainID())
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalHashAgg) ToPB(ctx sessionctx.Context) (*tipb.Executor, error) {
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	aggExec := &tipb.Aggregation{
		GroupBy: expression.ExpressionsToPBList(sc, p.GroupByItems, client),
	}
	for _, aggFunc := range p.AggFuncs {
		aggExec.AggFunc = append(aggExec.AggFunc, aggregation.AggFuncToPBExpr(sc, client, aggFunc))
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeAggregation, Aggregation: aggExec}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalStreamAgg) ToPB(ctx sessionctx.Context) (*tipb.Executor, error) {
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	aggExec := &tipb.Aggregation{
		GroupBy: expression.ExpressionsToPBList(sc, p.GroupByItems, client),
	}
	for _, aggFunc := range p.AggFuncs {
		aggExec.AggFunc = append(aggExec.AggFunc, aggregation.AggFuncToPBExpr(sc, client, aggFunc))
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeStreamAgg, Aggregation: aggExec}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalSelection) ToPB(ctx sessionctx.Context) (*tipb.Executor, error) {
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	selExec := &tipb.Selection{
		Conditions: expression.ExpressionsToPBList(sc, p.Conditions, client),
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeSelection, Selection: selExec}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalTopN) ToPB(ctx sessionctx.Context) (*tipb.Executor, error) {
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	topNExec := &tipb.TopN{
		Limit: p.Count,
	}
	for _, item := range p.ByItems {
		topNExec.OrderBy = append(topNExec.OrderBy, expression.SortByItemToPB(sc, client, item.Expr, item.Desc))
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeTopN, TopN: topNExec}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalLimit) ToPB(ctx sessionctx.Context) (*tipb.Executor, error) {
	limitExec := &tipb.Limit{
		Limit: p.Count,
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeLimit, Limit: limitExec}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalTableScan) ToPB(ctx sessionctx.Context) (*tipb.Executor, error) {
	tsExec := &tipb.TableScan{
		TableId: p.Table.ID,
		Columns: util.ColumnsToProto(p.Columns, p.Table.PKIsHandle),
		Desc:    p.Desc,
	}
	if p.isPartition {
		tsExec.TableId = p.physicalTableID
	}
	err := SetPBColumnsDefaultValue(ctx, tsExec.Columns, p.Columns)
	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tsExec}, err
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

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalIndexScan) ToPB(ctx sessionctx.Context) (*tipb.Executor, error) {
	columns := make([]*model.ColumnInfo, 0, p.schema.Len())
	tableColumns := p.Table.Cols()
	for _, col := range p.schema.Columns {
		if col.ID == model.ExtraHandleID {
			columns = append(columns, model.NewExtraHandleColInfo())
		} else {
			columns = append(columns, findColumnInfoByID(tableColumns, col.ID))
		}
	}
	idxExec := &tipb.IndexScan{
		TableId: p.Table.ID,
		IndexId: p.Index.ID,
		Columns: util.ColumnsToProto(columns, p.Table.PKIsHandle),
		Desc:    p.Desc,
	}
	if p.isPartition {
		idxExec.TableId = p.physicalTableID
	}
	unique := checkCoverIndex(p.Index, p.Ranges)
	idxExec.Unique = &unique
	return &tipb.Executor{Tp: tipb.ExecType_TypeIndexScan, IdxScan: idxExec}, nil
}

// SetPBColumnsDefaultValue sets the default values of tipb.ColumnInfos.
func SetPBColumnsDefaultValue(ctx sessionctx.Context, pbColumns []*tipb.ColumnInfo, columns []*model.ColumnInfo) error {
	for i, c := range columns {
		// For virtual columns, we set their default values to NULL so that TiKV will return NULL properly,
		// They real values will be compute later.
		if c.IsGenerated() && !c.GeneratedStored {
			pbColumns[i].DefaultVal = []byte{codec.NilFlag}
		}
		if c.OriginDefaultValue == nil {
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
