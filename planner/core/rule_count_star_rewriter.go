package core

import (
	"context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
)

/**
The countStarRewriter is used to rewrite count(*) -> count(not null column)

Pattern:
LogcialAggregation
       |
   DataSource

Optimize:
Table
  <k1 bool not null, k2 int null, k3 bigint not null>

Case1 there are columns exists in datasource
Query: select count(*) from table where k3=1
Rule: pick the shortest not null column exists in datasource
Rewritten Query: select count(k3) from table where k3=1

Case2 there is no columns exists in datasource
Query: select count(*) from table
Rule: pick the shortest not null column from origin table
Rewritten Query: select count(k1) from table

Attention:
Since count(*) is directly translated into count(1) during grammar parsing,
the rewritten pattern actually matches count(1)
*/

type countStarRewriter struct {
}

func (c *countStarRewriter) optimize(ctx context.Context, p LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	return c.countStarRewriter(p, opt)
}

func (c *countStarRewriter) countStarRewriter(p LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	// match pattern agg(count(*)) -> datasource
	if agg, ok := p.(*LogicalAggregation); ok {
		// todo if all kinds of data source could be matched ?
		if dataSource, ok := agg.Children()[0].(*DataSource); ok {
			for _, aggFunc := range agg.AggFuncs {
				if aggFunc.Name == "count" && len(aggFunc.Args) == 1 {
					if constExpr, ok := aggFunc.Args[0].(*expression.Constant); ok {
						if constExpr.Value.GetInt64() == 1 {
							// case 1: pick not null column already exists in the datasource
							if len(dataSource.Columns) > 0 {
								rewriteCount1ToCountColumn(dataSource, aggFunc)
								continue
							}
							// case 2: pick not null column from table
							// pickNotNullColumnFromTable(dataSource, agg, aggFunc)
						}
					}
				}
			}
		}
	}
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild, err := c.countStarRewriter(child, opt)
		if err != nil {
			return nil, err
		}
		newChildren = append(newChildren, newChild)
	}
	p.SetChildren(newChildren...)
	return p, nil
}

func pickNotNullColumnFromTable(dataSource *DataSource, logicalAgg *LogicalAggregation, aggFunc *aggregation.AggFuncDesc) {
	var newAggColumn *model.ColumnInfo
	for _, column := range dataSource.tableInfo.Columns {
		// remove varchar
		if column.Hidden {
			continue
		}
		if mysql.HasNotNullFlag(column.GetFlag()) {
			if newAggColumn == nil || column.GetFlen() < newAggColumn.GetFlen() {
				newAggColumn = column
			}
		}
	}
	if newAggColumn != nil {
		// update count(1) -> count(newAggColumn)
		aggArgs := &expression.Column{
			RetType:  &newAggColumn.FieldType,
			UniqueID: logicalAgg.ctx.GetSessionVars().AllocPlanColumnID(),
			ID:       newAggColumn.ID}
		aggFunc.Args[0] = aggArgs
		// update datasource columns
		dataSource.Columns = append(dataSource.Columns, newAggColumn)
		dataSource.schema.Columns = append(dataSource.schema.Columns, aggArgs)
	}
}

// Pick the shortest and not null column from Data Source
// If there is no not null column in Data Source, the count(1) will not be rewritten.
func rewriteCount1ToCountColumn(dataSource *DataSource, aggFunc *aggregation.AggFuncDesc) {
	var newAggColumn *expression.Column
	for _, columnExistsInDataSource := range dataSource.schema.Columns {
		if columnExistsInDataSource.IsHidden {
			continue
		}
		if mysql.HasNotNullFlag(columnExistsInDataSource.GetType().GetFlag()) {
			if newAggColumn == nil || columnExistsInDataSource.GetType().GetFlen() < newAggColumn.GetType().GetFlen() {
				newAggColumn = columnExistsInDataSource
			}
		}
	}
	if newAggColumn != nil {
		// update count(1) -> count(newAggColumn)
		aggFunc.Args[0] = &expression.Column{
			RetType:  newAggColumn.GetType(),
			UniqueID: newAggColumn.UniqueID,
			ID:       newAggColumn.ID}
	}
}

func (*countStarRewriter) name() string {
	return "count_star_rewriter"
}
