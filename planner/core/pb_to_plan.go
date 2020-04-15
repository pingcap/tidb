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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/v4/expression"
	"github.com/pingcap/tidb/v4/expression/aggregation"
	"github.com/pingcap/tidb/v4/infoschema"
	"github.com/pingcap/tidb/v4/sessionctx"
	"github.com/pingcap/tidb/v4/types"
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
	_, src = b.predicatePushDown(src, nil)
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
	dbInfo, ok := b.is.SchemaByTable(tbl.Meta())
	if !ok {
		return nil, infoschema.ErrDatabaseNotExists.GenWithStack("Database of table ID = %d does not exist.", tblScan.TableId)
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
		DBName:  dbInfo.Name,
		Table:   tbl.Meta(),
		Columns: columns,
	}.Init(b.sctx, nil, 0)
	p.SetSchema(schema)
	if strings.ToUpper(p.Table.Name.O) == infoschema.ClusterTableSlowLog {
		p.Extractor = &SlowQueryExtractor{}
	}
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
		partialAgg = baseAgg.initForStream(b.sctx, nil, 0)
	} else {
		partialAgg = baseAgg.initForHash(b.sctx, nil, 0)
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
	for _, expr := range executor.Aggregation.AggFunc {
		aggFunc, err := aggregation.PBExprToAggFuncDesc(b.sctx, expr, b.tps)
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

func (b *PBPlanBuilder) predicatePushDown(p PhysicalPlan, predicates []expression.Expression) ([]expression.Expression, PhysicalPlan) {
	if p == nil {
		return predicates, p
	}
	switch p.(type) {
	case *PhysicalMemTable:
		memTable := p.(*PhysicalMemTable)
		if memTable.Extractor == nil {
			return predicates, p
		}
		names := make([]*types.FieldName, 0, len(memTable.Columns))
		for _, col := range memTable.Columns {
			names = append(names, &types.FieldName{
				TblName:     memTable.Table.Name,
				ColName:     col.Name,
				OrigTblName: memTable.Table.Name,
				OrigColName: col.Name,
			})
		}
		// Set the expression column unique ID.
		// Since the expression is build from PB, It has not set the expression column ID yet.
		schemaCols := memTable.schema.Columns
		cols := expression.ExtractColumnsFromExpressions([]*expression.Column{}, predicates, nil)
		for i := range cols {
			cols[i].UniqueID = schemaCols[cols[i].Index].UniqueID
		}
		predicates = memTable.Extractor.Extract(b.sctx, memTable.schema, names, predicates)
		return predicates, memTable
	case *PhysicalSelection:
		selection := p.(*PhysicalSelection)
		conditions, child := b.predicatePushDown(p.Children()[0], selection.Conditions)
		if len(conditions) > 0 {
			selection.Conditions = conditions
			selection.SetChildren(child)
			return predicates, selection
		}
		return predicates, child
	default:
		if children := p.Children(); len(children) > 0 {
			_, child := b.predicatePushDown(children[0], nil)
			p.SetChildren(child)
		}
		return predicates, p
	}
}
