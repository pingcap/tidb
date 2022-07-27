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
	"fmt"
	"math"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
)

type partitionEliminator struct{}

func (e *partitionEliminator) optimize(_ context.Context, p LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	if p.SCtx().GetSessionVars().UseDynamicPartitionPrune() {
		return p, nil
	}
	return e.pruneRangePartitionByMinMax(p), nil
}

// pruneRangePartitionByMinMax prunes range partition according to min/max, eg:
// For range partition table:
// create table t (x int) partition by range (x) (
// partition p0 values less than (5),
// partition p1 values less than (10),
// partition p2 values less than (15)
// );
// we can transform following sql
// select max(x) from (union all
//      select * from p0
//      select * from p1
//      select * from p2)
// into
// select max(x) from (select * from p2)
func (e *partitionEliminator) pruneRangePartitionByMinMax(p LogicalPlan) LogicalPlan {
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChildren = append(newChildren, e.pruneRangePartitionByMinMax(child))
	}
	p.SetChildren(newChildren...)
	if agg, ok := p.(*LogicalAggregation); ok {
		if !e.checkAggFuncs(agg) {
			return p
		}
		if e.checkForbiddenOperators(agg) {
			return p
		}
		if pu := e.findPartitionUnion(agg); pu != nil {
			if !e.checkPartitionUnionAll(agg.AggFuncs[0], pu) {
				return p
			}
			e.eliminatePartitionByMinMax(pu, agg.AggFuncs[0])
		}
	}
	return p
}

// we only support single max/min now
func (e *partitionEliminator) checkAggFuncs(agg *LogicalAggregation) bool {
	if len(agg.AggFuncs) > 1 {
		return false
	}
	if agg.AggFuncs[0].Name != ast.AggFuncMax && agg.AggFuncs[0].Name != ast.AggFuncMin {
		return false
	}
	return true
}

// Some Operators may break the rule thus we can't prune partition table if we wound them.
// eg:
// create table t (x int, a int) partition by range (x) (
// partition p0 values less than (5),
// partition p1 values less than (10),
// partition p2 values less than (15)
// );
// select max(x) from t where a > 10 can't be transformed into
// select max(x) from (select * from p2 where a > 10)
// select max(x) from t,t1 where t.x = t1.x also shouldn't be pruned partition table
func (e *partitionEliminator) checkForbiddenOperators(p LogicalPlan) bool {
	switch p.(type) {
	case *LogicalSelection:
		return true
	case *LogicalJoin:
		return true
	case *LogicalUnionAll:
		return true
	}
	exists := false
	for _, child := range p.Children() {
		exists = exists || e.checkForbiddenOperators(child)
	}
	return exists
}

func (e *partitionEliminator) findPartitionUnion(p LogicalPlan) *LogicalPartitionUnionAll {
	if pu, ok := p.(*LogicalPartitionUnionAll); ok {
		return pu
	}
	for _, child := range p.Children() {
		pp := e.findPartitionUnion(child)
		if pp != nil {
			return pp
		}
	}
	return nil
}

func (e *partitionEliminator) checkPartitionUnionAll(aggFunc *aggregation.AggFuncDesc, p *LogicalPartitionUnionAll) bool {
	for _, child := range p.Children() {
		if ds, ok := child.(*DataSource); ok {
			if !e.checkDataSource(aggFunc, ds) {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

// checkDataSource will check whether datasource is range partition table
func (e *partitionEliminator) checkDataSource(aggFunc *aggregation.AggFuncDesc, ds *DataSource) bool {
	pi := ds.tableInfo.GetPartitionInfo()
	if pi == nil {
		return false
	}
	if pi.Type != model.PartitionTypeRange || !pi.Enable {
		return false
	}
	aggCol, ok := aggFunc.Args[0].(*expression.Column)
	if !ok {
		return false
	}
	if !e.checkIsAggColSameAsPartitionKey(aggCol, ds, pi) {
		return false
	}

	if !e.checkPushDownCond(aggCol, ds) {
		return false
	}
	return true
}

// checkIsAggColSameAsPartitionKey checks whether aggFunc's column arg is as same as partition table key
func (e *partitionEliminator) checkIsAggColSameAsPartitionKey(aggCol *expression.Column, ds *DataSource, pi *model.PartitionInfo) bool {
	colName := pi.Expr[1 : len(pi.Expr)-1]
	tableName := ds.tableInfo.Name.L
	is := ds.SCtx().GetDomainInfoSchema().(infoschema.InfoSchema)
	db, ok := is.SchemaByTable(ds.tableInfo)
	if !ok {
		return false
	}
	dbName := db.Name.L
	return aggCol.OrigName == fmt.Sprintf("%s.%s.%s", dbName, tableName, colName)
}

// currently we only allowed notNull, lt, gt as pushDown conditions.
// Otherwise, it may break the prune range partition rule.
func (e *partitionEliminator) checkPushDownCond(aggCol *expression.Column, ds *DataSource) bool {
	for _, cond := range ds.pushedDownConds {
		if !e.checkGTorLT(aggCol, cond) && !e.checkNotNull(aggCol, cond) {
			return false
		}
	}
	return true
}

// check whether the condition is gt(aggCol, constant) or lt(aggCol, constant)
func (e *partitionEliminator) checkGTorLT(aggCol *expression.Column, cond expression.Expression) bool {
	sf, ok := cond.(*expression.ScalarFunction)
	if !ok {
		return false
	}
	if sf.FuncName.L != ast.GT && sf.FuncName.L != ast.LT {
		return false
	}
	arg0, arg1 := sf.GetArgs()[0], sf.GetArgs()[1]
	if _, ok := arg0.(*expression.Constant); ok {
		return e.checkSameColumnAsAggCol(aggCol, arg1)
	}
	if _, ok := arg1.(*expression.Constant); ok {
		return e.checkSameColumnAsAggCol(aggCol, arg0)
	}
	return false
}

// check whether the condition is not(isnull(aggCol))
func (e *partitionEliminator) checkNotNull(aggCol *expression.Column, cond expression.Expression) bool {
	sf, ok := cond.(*expression.ScalarFunction)
	if !ok {
		return false
	}
	if sf.FuncName.L != ast.UnaryNot {
		return false
	}
	sf, ok = sf.GetArgs()[0].(*expression.ScalarFunction)
	if !ok {
		return false
	}
	if sf.FuncName.L != ast.IsNull {
		return false
	}
	return e.checkSameColumnAsAggCol(aggCol, sf.GetArgs()[0])
}

func (e *partitionEliminator) eliminatePartitionByMinMax(pu *LogicalPartitionUnionAll, aggFunc *aggregation.AggFuncDesc) {
	newChildren := make([]LogicalPlan, 0, 1)
	if aggFunc.Name == ast.AggFuncMax {
		maxID := int64(0)
		var maxChild *DataSource
		for _, child := range pu.Children() {
			ds := child.(*DataSource)
			if ds.physicalTableID > maxID {
				maxID = ds.physicalTableID
				maxChild = ds
			}
		}
		newChildren = append(newChildren, maxChild)
	} else if aggFunc.Name == ast.AggFuncMin {
		minID := int64(math.MaxInt64)
		var minChild *DataSource
		for _, child := range pu.Children() {
			ds := child.(*DataSource)
			if ds.physicalTableID < minID {
				minID = ds.physicalTableID
				minChild = ds
			}
		}
		newChildren = append(newChildren, minChild)
	} else {
		newChildren = append(newChildren, pu.Children()...)
	}
	pu.SetChildren(newChildren...)
}

// checkSameColumnAsAggCol checks whether the expr is same as aggCol
func (e *partitionEliminator) checkSameColumnAsAggCol(aggCol *expression.Column, expr expression.Expression) bool {
	col, ok := expr.(*expression.Column)
	if !ok {
		return false
	}
	if aggCol.OrigName != col.OrigName {
		return false
	}
	return true
}

func (e *partitionEliminator) name() string {
	return "partition_eliminator"
}
