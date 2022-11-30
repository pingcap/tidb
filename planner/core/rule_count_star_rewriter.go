// Copyright 2022 PingCAP, Inc.
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

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/parser/mysql"
)

/**
The countStarRewriter is used to rewrite
    count(*) -> count(not null column)
Attention:
Since count(*) is directly translated into count(1) during grammar parsing,
the rewritten pattern actually matches count(constant)

Pattern:
LogcialAggregation: count(constant)
       |
   DataSource

Optimize:
Table
  <k1 bool not null, k2 int null, k3 bigint not null>

Case1 there are columns from datasource
Query: select count(*) from table where k3=1
CountStarRewriterRule: pick the narrowest not null column from datasource
Rewritten Query: select count(k3) from table where k3=1

Case2 there is no columns from datasource
Query: select count(*) from table
ColumnPruningRule: pick k1 as the narrowest not null column from origin table @Function.preferNotNullColumnFromTable
                   datasource.columns: k1
CountStarRewriterRule: rewrite count(*) -> count(k1)
Rewritten Query: select count(k1) from table

*/

type countStarRewriter struct {
}

func (c *countStarRewriter) optimize(ctx context.Context, p LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	return c.countStarRewriter(p, opt)
}

func (c *countStarRewriter) countStarRewriter(p LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	c.matchPatternAndRewrite(p)
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

func (c *countStarRewriter) matchPatternAndRewrite(p LogicalPlan) {
	// match pattern agg(count(constant)) -> datasource
	agg, ok := p.(*LogicalAggregation)
	if !ok || len(agg.GroupByItems) > 0 {
		return
	}
	dataSource, ok := agg.Children()[0].(*DataSource)
	if !ok {
		return
	}
	for _, aggFunc := range agg.AggFuncs {
		if aggFunc.Name != "count" || len(aggFunc.Args) != 1 || aggFunc.HasDistinct {
			continue
		}
		constExpr, ok := aggFunc.Args[0].(*expression.Constant)
		if !ok || constExpr.Value.IsNull() || len(dataSource.Columns) == 0 {
			continue
		}
		// rewrite
		rewriteCountConstantToCountColumn(dataSource, aggFunc)
	}
}

// Pick the narrowest and not null column from Data Source
// If there is no not null column in Data Source, the count(constant) will not be rewritten.
func rewriteCountConstantToCountColumn(dataSource *DataSource, aggFunc *aggregation.AggFuncDesc) {
	var newAggColumn *expression.Column
	for _, columnFromDataSource := range dataSource.schema.Columns {
		if columnFromDataSource.GetType().IsVarLengthType() {
			continue
		}
		if mysql.HasNotNullFlag(columnFromDataSource.GetType().GetFlag()) {
			if newAggColumn == nil || columnFromDataSource.GetType().GetFlen() < newAggColumn.GetType().GetFlen() {
				newAggColumn = columnFromDataSource
			}
		}
	}
	if newAggColumn != nil {
		// update count(1) -> count(newAggColumn)
		aggFunc.Args[0] = newAggColumn
	}
}

func (*countStarRewriter) name() string {
	return "count_star_rewriter"
}
