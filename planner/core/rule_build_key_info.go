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
	"context"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
)

type buildKeySolver struct{}

func (s *buildKeySolver) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	buildKeyInfo(lp)
	return lp, nil
}

// buildKeyInfo recursively calls LogicalPlan's BuildKeyInfo method.
func buildKeyInfo(lp LogicalPlan) {
	for _, child := range lp.Children() {
		buildKeyInfo(child)
	}
	childSchema := make([]*expression.Schema, len(lp.Children()))
	for i, child := range lp.Children() {
		childSchema[i] = child.Schema()
	}
	lp.BuildKeyInfo(lp.Schema(), childSchema)
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (la *LogicalAggregation) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	if la.IsPartialModeAgg() {
		return
	}
	la.logicalSchemaProducer.BuildKeyInfo(selfSchema, childSchema)
	for _, key := range childSchema[0].Keys {
		indices := selfSchema.ColumnsIndices(key)
		if indices == nil {
			continue
		}
		newKey := make([]*expression.Column, 0, len(key))
		for _, i := range indices {
			newKey = append(newKey, selfSchema.Columns[i])
		}
		selfSchema.Keys = append(selfSchema.Keys, newKey)
	}
	if len(la.groupByCols) == len(la.GroupByItems) && len(la.GroupByItems) > 0 {
		indices := selfSchema.ColumnsIndices(la.groupByCols)
		if indices != nil {
			newKey := make([]*expression.Column, 0, len(indices))
			for _, i := range indices {
				newKey = append(newKey, selfSchema.Columns[i])
			}
			selfSchema.Keys = append(selfSchema.Keys, newKey)
		}
	}
	if len(la.GroupByItems) == 0 {
		la.maxOneRow = true
	}
}

// If a condition is the form of (uniqueKey = constant) or (uniqueKey = Correlated column), it returns at most one row.
// This function will check it.
func (p *LogicalSelection) checkMaxOneRowCond(unique expression.Expression, constOrCorCol expression.Expression, childSchema *expression.Schema) bool {
	col, ok := unique.(*expression.Column)
	if !ok {
		return false
	}
	if !childSchema.IsUniqueKey(col) {
		return false
	}
	_, okCon := constOrCorCol.(*expression.Constant)
	if okCon {
		return true
	}
	_, okCorCol := constOrCorCol.(*expression.CorrelatedColumn)
	return okCorCol
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *LogicalSelection) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	p.baseLogicalPlan.BuildKeyInfo(selfSchema, childSchema)
	for _, cond := range p.Conditions {
		if sf, ok := cond.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.EQ {
			if p.checkMaxOneRowCond(sf.GetArgs()[0], sf.GetArgs()[1], childSchema[0]) || p.checkMaxOneRowCond(sf.GetArgs()[1], sf.GetArgs()[0], childSchema[0]) {
				p.maxOneRow = true
				break
			}
		}
	}
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *LogicalLimit) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	p.baseLogicalPlan.BuildKeyInfo(selfSchema, childSchema)
	if p.Count == 1 {
		p.maxOneRow = true
	}
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *LogicalTopN) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	p.baseLogicalPlan.BuildKeyInfo(selfSchema, childSchema)
	if p.Count == 1 {
		p.maxOneRow = true
	}
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *LogicalTableDual) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	p.baseLogicalPlan.BuildKeyInfo(selfSchema, childSchema)
	if p.RowCount == 1 {
		p.maxOneRow = true
	}
}

// A bijection exists between columns of a projection's schema and this projection's Exprs.
// Sometimes we need a schema made by expr of Exprs to convert a column in child's schema to a column in this projection's Schema.
func (p *LogicalProjection) buildSchemaByExprs(selfSchema *expression.Schema) *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, selfSchema.Len())...)
	for _, expr := range p.Exprs {
		if col, isCol := expr.(*expression.Column); isCol {
			schema.Append(col)
		} else {
			// If the expression is not a column, we add a column to occupy the position.
			schema.Append(&expression.Column{
				UniqueID: p.ctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  expr.GetType(),
			})
		}
	}
	return schema
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *LogicalProjection) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	p.logicalSchemaProducer.BuildKeyInfo(selfSchema, childSchema)
	schema := p.buildSchemaByExprs(selfSchema)
	for _, key := range childSchema[0].Keys {
		indices := schema.ColumnsIndices(key)
		if indices == nil {
			continue
		}
		newKey := make([]*expression.Column, 0, len(key))
		for _, i := range indices {
			newKey = append(newKey, selfSchema.Columns[i])
		}
		selfSchema.Keys = append(selfSchema.Keys, newKey)
	}
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *LogicalJoin) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	p.logicalSchemaProducer.BuildKeyInfo(selfSchema, childSchema)
	switch p.JoinType {
	case SemiJoin, LeftOuterSemiJoin, AntiSemiJoin, AntiLeftOuterSemiJoin:
		selfSchema.Keys = childSchema[0].Clone().Keys
	case InnerJoin, LeftOuterJoin, RightOuterJoin:
		// If there is no equal conditions, then cartesian product can't be prevented and unique key information will destroy.
		if len(p.EqualConditions) == 0 {
			return
		}
		lOk := false
		rOk := false
		// Such as 'select * from t1 join t2 where t1.a = t2.a and t1.b = t2.b'.
		// If one sides (a, b) is a unique key, then the unique key information is remained.
		// But we don't consider this situation currently.
		// Only key made by one column is considered now.
		for _, expr := range p.EqualConditions {
			ln := expr.GetArgs()[0].(*expression.Column)
			rn := expr.GetArgs()[1].(*expression.Column)
			for _, key := range childSchema[0].Keys {
				if len(key) == 1 && key[0].Equal(p.ctx, ln) {
					lOk = true
					break
				}
			}
			for _, key := range childSchema[1].Keys {
				if len(key) == 1 && key[0].Equal(p.ctx, rn) {
					rOk = true
					break
				}
			}
		}
		// For inner join, if one side of one equal condition is unique key,
		// another side's unique key information will all be reserved.
		// If it's an outer join, NULL value will fill some position, which will destroy the unique key information.
		if lOk && p.JoinType != LeftOuterJoin {
			selfSchema.Keys = append(selfSchema.Keys, childSchema[1].Keys...)
		}
		if rOk && p.JoinType != RightOuterJoin {
			selfSchema.Keys = append(selfSchema.Keys, childSchema[0].Keys...)
		}
	}
}

// checkIndexCanBeKey checks whether an Index can be a Key in schema.
func checkIndexCanBeKey(idx *model.IndexInfo, columns []*model.ColumnInfo, schema *expression.Schema) expression.KeyInfo {
	if !idx.Unique {
		return nil
	}
	newKey := make([]*expression.Column, 0, len(idx.Columns))
	ok := true
	for _, idxCol := range idx.Columns {
		// The columns of this index should all occur in column schema.
		// Since null value could be duplicate in unique key. So we check NotNull flag of every column.
		find := false
		for i, col := range columns {
			if idxCol.Name.L == col.Name.L {
				if !mysql.HasNotNullFlag(col.Flag) {
					break
				}
				newKey = append(newKey, schema.Columns[i])
				find = true
				break
			}
		}
		if !find {
			ok = false
			break
		}
	}
	if ok {
		return newKey
	}
	return nil
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (ds *DataSource) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	selfSchema.Keys = nil
	for _, path := range ds.possibleAccessPaths {
		if path.IsIntHandlePath {
			continue
		}
		if newKey := checkIndexCanBeKey(path.Index, ds.Columns, selfSchema); newKey != nil {
			selfSchema.Keys = append(selfSchema.Keys, newKey)
		}
	}
	if ds.tableInfo.PKIsHandle {
		for i, col := range ds.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				selfSchema.Keys = append(selfSchema.Keys, []*expression.Column{selfSchema.Columns[i]})
				break
			}
		}
	}
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (ts *LogicalTableScan) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	ts.Source.BuildKeyInfo(selfSchema, childSchema)
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (is *LogicalIndexScan) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	selfSchema.Keys = nil
	for _, path := range is.Source.possibleAccessPaths {
		if path.IsTablePath() {
			continue
		}
		if newKey := checkIndexCanBeKey(path.Index, is.Columns, selfSchema); newKey != nil {
			selfSchema.Keys = append(selfSchema.Keys, newKey)
		}
	}
	handle := is.getPKIsHandleCol(selfSchema)
	if handle != nil {
		selfSchema.Keys = append(selfSchema.Keys, []*expression.Column{handle})
	}
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (tg *TiKVSingleGather) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	selfSchema.Keys = childSchema[0].Keys
}

func (*buildKeySolver) name() string {
	return "build_keys"
}
