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

type pbPlanBuilder struct {
	sctx sessionctx.Context
	tps  []*types.FieldType
	is   infoschema.InfoSchema
}

// NewPBPlanBuilder creates a new pb plan builder.
func NewPBPlanBuilder(sctx sessionctx.Context, is infoschema.InfoSchema) *pbPlanBuilder {
	return &pbPlanBuilder{sctx: sctx, is: is}
}

func (b *pbPlanBuilder) PBToPhysicalPlan(e *tipb.Executor) (p PhysicalPlan, err error) {
	switch e.Tp {
	case tipb.ExecType_TypeMemTableScan:
		p, err = b.pbToMemTableScan(e)
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

func (b *pbPlanBuilder) pbToMemTableScan(e *tipb.Executor) (PhysicalPlan, error) {
	memTbl := e.MemTblScan
	if !infoschema.IsClusterMemTable(memTbl.DbName, memTbl.TableName) {
		return nil, errors.Errorf("table %s is not a tidb memory table", memTbl.TableName)
	}
	dbName := model.NewCIStr(memTbl.DbName)
	tbl, err := b.is.TableByName(dbName, model.NewCIStr(memTbl.TableName))
	if err != nil {
		return nil, err
	}
	columns, err := convertColumnInfo(tbl.Meta(), memTbl)
	if err != nil {
		return nil, err
	}
	schema := b.buildMemTableScanSchema(tbl.Meta(), columns)
	p := PhysicalMemTable{
		DBName:  dbName,
		Table:   tbl.Meta(),
		Columns: columns,
	}.Init(b.sctx, nil, 0)
	p.SetSchema(schema)
	return p, nil
}

func (b *pbPlanBuilder) buildMemTableScanSchema(tblInfo *model.TableInfo, columns []*model.ColumnInfo) *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, len(columns))...)
	tps := make([]*types.FieldType, 0, len(columns))
	for _, col := range tblInfo.Columns {
		for _, colInfo := range columns {
			tps = append(tps, colInfo.FieldType.Clone())
			if col.ID == colInfo.ID {
				newCol := &expression.Column{
					UniqueID: b.sctx.GetSessionVars().AllocPlanColumnID(),
					ID:       col.ID,
					RetType:  &col.FieldType,
				}
				schema.Append(newCol)
			}
		}
	}
	b.tps = tps
	return schema
}

func (b *pbPlanBuilder) pbToSelection(e *tipb.Executor) (PhysicalPlan, error) {
	conds, err := expression.PBToExprs(e.Selection.Conditions, b.tps, b.sctx.GetSessionVars().StmtCtx)
	if err != nil {
		return nil, err
	}
	p := PhysicalSelection{
		Conditions: conds,
	}.Init(b.sctx, nil, 0)
	return p, nil
}

func (b *pbPlanBuilder) pbToTopN(e *tipb.Executor) (PhysicalPlan, error) {
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

func (b *pbPlanBuilder) pbToLimit(e *tipb.Executor) (PhysicalPlan, error) {
	p := PhysicalLimit{
		Count: e.Limit.Limit,
	}.Init(b.sctx, nil, 0)
	return p, nil
}

func (b *pbPlanBuilder) pbToAgg(e *tipb.Executor, isStreamAgg bool) (PhysicalPlan, error) {
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

func (b *pbPlanBuilder) buildAggSchema(aggFuncs []*aggregation.AggFuncDesc, groupBys []expression.Expression) *expression.Schema {
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

func (b *pbPlanBuilder) getAggInfo(executor *tipb.Executor) ([]*aggregation.AggFuncDesc, []expression.Expression, error) {
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

func convertColumnInfo(tblInfo *model.TableInfo, memTbl *tipb.MemTableScan) ([]*model.ColumnInfo, error) {
	columns := make([]*model.ColumnInfo, 0, len(memTbl.Columns))
	for _, col := range memTbl.Columns {
		found := false
		for _, colInfo := range tblInfo.Columns {
			if col.ColumnId == colInfo.ID {
				columns = append(columns, colInfo)
				found = true
				break
			}
		}
		if !found {
			return nil, errors.Errorf("Column ID %v of table %v.%v not found", col.ColumnId, "information_schema", memTbl.TableName)
		}
	}
	return columns, nil
}
