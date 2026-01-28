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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tipb/go-tipb"
)

// PBPlanBuilder uses to build physical plan from dag protocol buffers.
type PBPlanBuilder struct {
	sctx   base.PlanContext
	tps    []*types.FieldType
	is     infoschema.InfoSchema
	ranges []*coprocessor.KeyRange
}

// NewPBPlanBuilder creates a new pb plan builder.
func NewPBPlanBuilder(sctx base.PlanContext, is infoschema.InfoSchema, ranges []*coprocessor.KeyRange) *PBPlanBuilder {
	return &PBPlanBuilder{sctx: sctx, is: is, ranges: ranges}
}

// Build builds physical plan from dag protocol buffers.
func (b *PBPlanBuilder) Build(executors []*tipb.Executor) (p base.PhysicalPlan, err error) {
	var src base.PhysicalPlan
	for i := range executors {
		curr, err := b.pbToPhysicalPlan(executors[i], src)
		if err != nil {
			return nil, errors.Trace(err)
		}
		src = curr
	}
	_, src = b.predicatePushDown(src, nil)
	return src, nil
}

func (b *PBPlanBuilder) pbToPhysicalPlan(e *tipb.Executor, subPlan base.PhysicalPlan) (p base.PhysicalPlan, err error) {
	switch e.Tp {
	case tipb.ExecType_TypeTableScan:
		p, err = b.pbToTableScan(e)
	case tipb.ExecType_TypeSelection:
		p, err = b.pbToSelection(e)
	case tipb.ExecType_TypeProjection:
		p, err = b.pbToProjection(e)
	case tipb.ExecType_TypeTopN:
		p, err = b.pbToTopN(e)
	case tipb.ExecType_TypeLimit:
		p, err = b.pbToLimit(e)
	case tipb.ExecType_TypeAggregation:
		p, err = b.pbToAgg(e, false)
	case tipb.ExecType_TypeStreamAgg:
		p, err = b.pbToAgg(e, true)
	case tipb.ExecType_TypeKill:
		p, err = b.pbToKill(e)
	case tipb.ExecType_TypeBroadcastQuery:
		p, err = b.pbToBroadcastQuery(e)
	default:
		// TODO: Support other types.
		err = errors.Errorf("this exec type %v doesn't support yet", e.GetTp())
	}
	if subPlan != nil {
		// p may nil if the executor is not supported, for example, Projection.
		p.SetChildren(subPlan)
	}
	// The limit missed its output cols via the protobuf.
	// We need to add it back and do a ResolveIndicies for the later inline projection.
	if limit, ok := p.(*physicalop.PhysicalLimit); ok {
		limit.SetSchema(p.Children()[0].Schema().Clone())
		for i, col := range limit.Schema().Columns {
			col.Index = i
		}
	}
	return p, err
}

func (b *PBPlanBuilder) pbToTableScan(e *tipb.Executor) (base.PhysicalPlan, error) {
	tblScan := e.TblScan
	tbl, ok := b.is.TableByID(context.Background(), tblScan.TableId)
	if !ok {
		return nil, infoschema.ErrTableNotExists.GenWithStack("Table which ID = %d does not exist.", tblScan.TableId)
	}
	dbInfo, ok := infoschema.SchemaByTable(b.is, tbl.Meta())
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
	p := physicalop.PhysicalMemTable{
		DBName:  dbInfo.Name,
		Table:   tbl.Meta(),
		Columns: columns,
	}.Init(b.sctx, &property.StatsInfo{}, 0)
	p.SetSchema(schema)
	switch strings.ToUpper(p.Table.Name.O) {
	case infoschema.ClusterTableSlowLog:
		extractor := &SlowQueryExtractor{}
		extractor.Desc = tblScan.Desc
		if b.ranges != nil {
			err := extractor.buildTimeRangeFromKeyRange(b.ranges)
			if err != nil {
				return nil, err
			}
		}
		p.Extractor = extractor
	case infoschema.ClusterTableStatementsSummary, infoschema.ClusterTableStatementsSummaryHistory:
		p.Extractor = &StatementsSummaryExtractor{}
	case infoschema.ClusterTableTiDBIndexUsage:
		p.Extractor = NewInfoSchemaTiDBIndexUsageExtractor()
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

func (b *PBPlanBuilder) pbToProjection(e *tipb.Executor) (base.PhysicalPlan, error) {
	exprs, err := expression.PBToExprs(b.sctx.GetExprCtx(), e.Projection.Exprs, b.tps)
	if err != nil {
		return nil, errors.Trace(err)
	}
	p := physicalop.PhysicalProjection{
		Exprs: exprs,
	}.Init(b.sctx, &property.StatsInfo{}, 0, &property.PhysicalProperty{})
	return p, nil
}

func (b *PBPlanBuilder) pbToSelection(e *tipb.Executor) (base.PhysicalPlan, error) {
	conds, err := expression.PBToExprs(b.sctx.GetExprCtx(), e.Selection.Conditions, b.tps)
	if err != nil {
		return nil, err
	}
	p := physicalop.PhysicalSelection{
		Conditions: conds,
	}.Init(b.sctx, &property.StatsInfo{}, 0, &property.PhysicalProperty{})
	return p, nil
}

func (b *PBPlanBuilder) pbToTopN(e *tipb.Executor) (base.PhysicalPlan, error) {
	topN := e.TopN
	byItems := make([]*util.ByItems, 0, len(topN.OrderBy))
	exprCtx := b.sctx.GetExprCtx()
	for _, item := range topN.OrderBy {
		expr, err := expression.PBToExpr(exprCtx, item.Expr, b.tps)
		if err != nil {
			return nil, errors.Trace(err)
		}
		byItems = append(byItems, &util.ByItems{Expr: expr, Desc: item.Desc})
	}
	p := physicalop.PhysicalTopN{
		ByItems: byItems,
		Count:   topN.Limit,
	}.Init(b.sctx, &property.StatsInfo{}, 0, &property.PhysicalProperty{})
	return p, nil
}

func (b *PBPlanBuilder) pbToLimit(e *tipb.Executor) (base.PhysicalPlan, error) {
	p := physicalop.PhysicalLimit{
		Count: e.Limit.Limit,
	}.Init(b.sctx, &property.StatsInfo{}, 0, &property.PhysicalProperty{})
	return p, nil
}

func (b *PBPlanBuilder) pbToAgg(e *tipb.Executor, isStreamAgg bool) (base.PhysicalPlan, error) {
	aggFuncs, groupBys, err := b.getAggInfo(e)
	if err != nil {
		return nil, errors.Trace(err)
	}
	schema := b.buildAggSchema(aggFuncs, groupBys)
	baseAgg := physicalop.BasePhysicalAgg{
		AggFuncs:     aggFuncs,
		GroupByItems: groupBys,
	}
	var partialAgg base.PhysicalPlan
	if isStreamAgg {
		partialAgg = baseAgg.InitForStream(b.sctx, &property.StatsInfo{}, 0, schema, &property.PhysicalProperty{})
	} else {
		partialAgg = baseAgg.InitForHash(b.sctx, &property.StatsInfo{}, 0, schema, &property.PhysicalProperty{})
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
	exprCtx := b.sctx.GetExprCtx()
	for _, expr := range executor.Aggregation.AggFunc {
		aggFunc, err := aggregation.PBExprToAggFuncDesc(exprCtx, expr, b.tps)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		aggFuncs = append(aggFuncs, aggFunc)
	}
	groupBys, err := expression.PBToExprs(exprCtx, executor.Aggregation.GetGroupBy(), b.tps)
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

func (*PBPlanBuilder) pbToKill(e *tipb.Executor) (base.PhysicalPlan, error) {
	node := &ast.KillStmt{
		ConnectionID: e.Kill.ConnID,
		Query:        e.Kill.Query,
	}
	simple := &Simple{Statement: node, IsFromRemote: true, ResolveCtx: resolve.NewContext()}
	return &PhysicalPlanWrapper{Inner: simple}, nil
}

func (b *PBPlanBuilder) pbToBroadcastQuery(e *tipb.Executor) (base.PhysicalPlan, error) {
	vars := b.sctx.GetSessionVars()
	charset, collation := vars.GetCharsetInfo()
	pa := parser.New()
	stmt, err := pa.ParseOneStmt(*e.BroadcastQuery.Query, charset, collation)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var innerPlan base.Plan
	switch x := stmt.(type) {
	case *ast.AdminStmt:
		if x.Tp != ast.AdminReloadBindings {
			return nil, errors.Errorf("unexpected admin statement %s in broadcast query", *e.BroadcastQuery.Query)
		}
		innerPlan = &SQLBindPlan{SQLBindOp: OpReloadBindings, IsFromRemote: true}
	case *ast.RefreshStatsStmt:
		innerPlan = &Simple{Statement: stmt, IsFromRemote: true, ResolveCtx: resolve.NewContext()}
	default:
		return nil, errors.Errorf("unexpected statement %s in broadcast query", *e.BroadcastQuery.Query)
	}
	return &PhysicalPlanWrapper{Inner: innerPlan}, nil
}

func (b *PBPlanBuilder) predicatePushDown(physicalPlan base.PhysicalPlan, predicates []expression.Expression) ([]expression.Expression, base.PhysicalPlan) {
	if physicalPlan == nil {
		return predicates, physicalPlan
	}
	switch plan := physicalPlan.(type) {
	case *physicalop.PhysicalMemTable:
		memTable := plan
		if memTable.Extractor == nil {
			return predicates, plan
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
		schemaCols := memTable.Schema().Columns
		cols := expression.ExtractAllColumnsFromExpressions(predicates, nil)
		for _, col := range cols {
			col.UniqueID = schemaCols[col.Index].UniqueID
		}
		predicates = memTable.Extractor.Extract(b.sctx, memTable.Schema(), names, predicates)
		return predicates, memTable
	case *physicalop.PhysicalSelection:
		selection := plan
		conditions, child := b.predicatePushDown(plan.Children()[0], selection.Conditions)
		if len(conditions) > 0 {
			selection.Conditions = conditions
			selection.SetChildren(child)
			return predicates, selection
		}
		return predicates, child
	default:
		if children := plan.Children(); len(children) > 0 {
			_, child := b.predicatePushDown(children[0], nil)
			plan.SetChildren(child)
		}
		return predicates, plan
	}
}
