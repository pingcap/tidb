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

package plan

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tipb/go-tipb"
)

// ToPB implements PhysicalPlan ToPB interface.
func (p *basePhysicalPlan) ToPB(_ context.Context) (*tipb.Executor, error) {
	return nil, errors.Errorf("plan %s fails converts to PB", p.basePlan.id)
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalAggregation) ToPB(ctx context.Context) (*tipb.Executor, error) {
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	aggExec := &tipb.Aggregation{
		GroupBy: expression.ExpressionsToPBList(sc, p.GroupByItems, client),
	}
	for _, aggFunc := range p.AggFuncs {
		aggExec.AggFunc = append(aggExec.AggFunc, expression.AggFuncToPBExpr(sc, client, aggFunc))
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeAggregation, Aggregation: aggExec}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *Selection) ToPB(ctx context.Context) (*tipb.Executor, error) {
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	selExec := &tipb.Selection{
		Conditions: expression.ExpressionsToPBList(sc, p.Conditions, client),
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeSelection, Selection: selExec}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *TopN) ToPB(ctx context.Context) (*tipb.Executor, error) {
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
func (p *Limit) ToPB(ctx context.Context) (*tipb.Executor, error) {
	limitExec := &tipb.Limit{
		Limit: p.Count,
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeLimit, Limit: limitExec}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalTableScan) ToPB(ctx context.Context) (*tipb.Executor, error) {
	tsExec := &tipb.TableScan{
		TableId: p.Table.ID,
		Columns: distsql.ColumnsToProto(p.Columns, p.Table.PKIsHandle),
		Desc:    p.Desc,
	}
	err := setPBColumnsDefaultValue(ctx, tsExec.Columns, p.Columns)
	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tsExec}, errors.Trace(err)
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalIndexScan) ToPB(ctx context.Context) (*tipb.Executor, error) {
	columns := make([]*model.ColumnInfo, 0, p.schema.Len())
	for _, col := range p.schema.Columns {
		columns = append(columns, p.Table.Columns[col.Position])
	}
	idxExec := &tipb.IndexScan{
		TableId: p.Table.ID,
		IndexId: p.Index.ID,
		Columns: distsql.ColumnsToProto(columns, p.Table.PKIsHandle),
		Desc:    p.Desc,
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeIndexScan, IdxScan: idxExec}, nil
}

func setPBColumnsDefaultValue(ctx context.Context, pbColumns []*tipb.ColumnInfo, columns []*model.ColumnInfo) error {
	for i, c := range columns {
		if c.OriginDefaultValue == nil {
			continue
		}

		sessVars := ctx.GetSessionVars()
		originStrict := sessVars.StrictSQLMode
		sessVars.StrictSQLMode = false
		d, err := table.GetColOriginDefaultValue(ctx, c)
		sessVars.StrictSQLMode = originStrict
		if err != nil {
			return errors.Trace(err)
		}

		pbColumns[i].DefaultVal, err = tablecodec.EncodeValue(d, ctx.GetSessionVars().GetTimeZone())
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
