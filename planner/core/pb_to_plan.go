// Copyright 2019 PingCAP, Inc.
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
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tipb/go-tipb"
)

// PBPlanBuilder uses to build physical plan from dag protocol buffers.
type PBPlanBuilder struct {
	sctx sessionctx.Context
	tps  []*types.FieldType
	is   infoschema.InfoSchema
}

// NewPBPlanBuilder creates a new pb plan builder.
func NewPBPlanBuilder(sctx sessionctx.Context, is infoschema.InfoSchema) *PBPlanBuilder {
	return &PBPlanBuilder{sctx: sctx, is: is}
}

// Build builds physical plan from dag protocol buffers.
func (b *PBPlanBuilder) Build(executors []*tipb.Executor) (p PhysicalPlan, err error) {
	var src PhysicalPlan
	for i := 0; i < len(executors); i++ {
		curr, err := b.pbToPhysicalPlan(executors[i])
		if err != nil {
			return nil, errors.Trace(err)
		}
		curr.SetChildren(src)
		src = curr
	}
	return src, nil
}

func (b *PBPlanBuilder) pbToPhysicalPlan(e *tipb.Executor) (p PhysicalPlan, err error) {
	switch e.Tp {
	case tipb.ExecType_TypeTableScan:
		p, err = b.pbToTableScan(e)
	case tipb.ExecType_TypeSelection:
		p, err = b.pbToSelection(e)
	case tipb.ExecType_TypeTopN:
		p, err = b.pbToTopN(e)
	case tipb.ExecType_TypeLimit:
		p, err = b.pbToLimit(e)
	case tipb.ExecType_TypeAggregation:
		p, err = b.pbToAgg(e, false)
	case tipb.ExecType_TypeStreamAgg:
		p, err = b.pbToAgg(e, true)
	default:
		// TODO: Support other types.
		err = errors.Errorf("this exec type %v doesn't support yet.", e.GetTp())
	}
	return p, err
}

func (b *PBPlanBuilder) pbToTableScan(e *tipb.Executor) (PhysicalPlan, error) {
	tblScan := e.TblScan
	tbl, ok := b.is.TableByID(tblScan.TableId)
	if !ok {
		return nil, infoschema.ErrTableNotExists.GenWithStack("Table which ID = %d does not exist.", tblScan.TableId)
	}
	// Currently only support cluster table.
	if !tbl.Type().IsClusterTable() {
		return nil, errors.Errorf("table %s is not a cluster table", tbl.Meta().Name.L)
	}
	columns, err := b.convertColumnInfo(tbl.Meta(), tblScan.Columns)
	if err != nil {
		return nil, err
	}
	schema := b.buildTableScanSchema(tbl.Meta(), columns)
	p := PhysicalMemTable{
		Table:   tbl.Meta(),
		Columns: columns,
	}.Init(b.sctx, nil, 0)
	p.SetSchema(schema)
	return p, nil
}

func (b *PBPlanBuilder) buildTableScanSchema(tblInfo *model.TableInfo, columns []*model.ColumnInfo) *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, len(columns))...)
	for _, col := range tblInfo.Columns {
		for _, colInfo := range columns {
			if col.ID != colInfo.ID {
				continue
			}
			newCol := &expression.Column{
				UniqueID: b.sctx.GetSessionVars().AllocPlanColumnID(),
				ID:       col.ID,
				RetType:  &col.FieldType,
			}
			schema.Append(newCol)
		}
	}
	return schema
}

func (b *PBPlanBuilder) pbToSelection(e *tipb.Executor) (PhysicalPlan, error) {
	conds, err := expression.PBToExprs(e.Selection.Conditions, b.tps, b.sctx.GetSessionVars().StmtCtx)
	if err != nil {
		return nil, err
	}
	p := PhysicalSelection{
		Conditions: conds,
	}.Init(b.sctx, nil, 0)
	return p, nil
}

func (b *PBPlanBuilder) pbToTopN(e *tipb.Executor) (PhysicalPlan, error) {
	topN := e.TopN
	sc := b.sctx.GetSessionVars().StmtCtx
	byItems := make([]*ByItems, 0, len(topN.OrderBy))
	for _, item := range topN.OrderBy {
		expr, err := expression.PBToExpr(item.Expr, b.tps, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		byItems = append(byItems, &ByItems{Expr: expr, Desc: item.Desc})
	}
	p := PhysicalTopN{
		ByItems: byItems,
		Count:   topN.Limit,
	}.Init(b.sctx, nil, 0)
	return p, nil
}

func (b *PBPlanBuilder) pbToLimit(e *tipb.Executor) (PhysicalPlan, error) {
	p := PhysicalLimit{
		Count: e.Limit.Limit,
	}.Init(b.sctx, nil, 0)
	return p, nil
}

func (b *PBPlanBuilder) pbToAgg(e *tipb.Executor, isStreamAgg bool) (PhysicalPlan, error) {
	aggFuncs, groupBys, err := b.getAggInfo(e)
	if err != nil {
		return nil, errors.Trace(err)
	}
	schema := b.buildAggSchema(aggFuncs, groupBys)
	baseAgg := basePhysicalAgg{
		AggFuncs:     aggFuncs,
		GroupByItems: groupBys,
	}
	baseAgg.schema = schema
	var partialAgg PhysicalPlan
	if isStreamAgg {
		partialAgg = baseAgg.initForHash(b.sctx, nil, 0)
	} else {
		partialAgg = baseAgg.initForStream(b.sctx, nil, 0)
	}
	return partialAgg, nil
}

func (b *PBPlanBuilder) buildAggSchema(aggFuncs []*aggregation.AggFuncDesc, groupBys []expression.Expression) *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, len(aggFuncs)+len(groupBys))...)
	for _, agg := range aggFuncs {
		newCol := &expression.Column{
			UniqueID: b.sctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  agg.RetTp,
		}
		schema.Append(newCol)
	}
	return schema
}

func (b *PBPlanBuilder) getAggInfo(executor *tipb.Executor) ([]*aggregation.AggFuncDesc, []expression.Expression, error) {
	var err error
	aggFuncs := make([]*aggregation.AggFuncDesc, 0, len(executor.Aggregation.AggFunc))
	sc := b.sctx.GetSessionVars().StmtCtx
	for _, expr := range executor.Aggregation.AggFunc {
		aggFunc, err := aggregation.PBExprToAggFuncDesc(sc, expr, b.tps)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		aggFuncs = append(aggFuncs, aggFunc)
	}
	groupBys, err := expression.PBToExprs(executor.Aggregation.GetGroupBy(), b.tps, b.sctx.GetSessionVars().StmtCtx)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return aggFuncs, groupBys, nil
}

func (b *PBPlanBuilder) convertColumnInfo(tblInfo *model.TableInfo, pbColumns []*tipb.ColumnInfo) ([]*model.ColumnInfo, error) {
	columns := make([]*model.ColumnInfo, 0, len(pbColumns))
	tps := make([]*types.FieldType, 0, len(pbColumns))
	for _, col := range pbColumns {
		found := false
		for _, colInfo := range tblInfo.Columns {
			if col.ColumnId == colInfo.ID {
				columns = append(columns, colInfo)
				tps = append(tps, colInfo.FieldType.Clone())
				found = true
				break
			}
		}
		if !found {
			return nil, errors.Errorf("Column ID %v of table %v not found", col.ColumnId, tblInfo.Name.L)
		}
	}
	b.tps = tps
	return columns, nil
}
