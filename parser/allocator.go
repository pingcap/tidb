// Copyright 2015 PingCAP, Inc.
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
package parser

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/util/types"
)

type allocator struct {
	cache           []yySymType
	yylval          yySymType
	yyVAL           yySymType
	fieldType       []types.FieldType
	valueExpr       []ast.ValueExpr
	deleteStmt      []ast.DeleteStmt
	insertStmt      []ast.InsertStmt
	unionStmt       []ast.UnionStmt
	updateStmt      []ast.UpdateStmt
	selectStmt      []ast.SelectStmt
	showStmt        []ast.ShowStmt
	assignment      []ast.Assignment
	byItem          []ast.ByItem
	fieldList       []ast.FieldList
	groupByClause   []ast.GroupByClause
	havingClause    []ast.HavingClause
	join            []ast.Join
	limit           []ast.Limit
	onCondition     []ast.OnCondition
	orderByClause   []ast.OrderByClause
	selectField     []ast.SelectField
	tableName       []ast.TableName
	tableSource     []ast.TableSource
	unionSelectList []ast.UnionSelectList
	wildCardField   []ast.WildCardField
	tableRefsClause []ast.TableRefsClause
}

func newAllocator() *allocator {
	return &allocator{
		cache: make([]yySymType, 140),
	}
}

func (ac *allocator) newValueExpr(v interface{}) *ast.ValueExpr {
	if ve, ok := v.(*ast.ValueExpr); ok {
		return ve
	}

	tp := ac.allocFieldType()
	tp.Init(v)

	ve := ac.allocValueExpr()
	ve.SetValue(v)
	ve.Type = tp

	return ve
}

func (ac *allocator) newFieldType(tp byte) *types.FieldType {
	ret := ac.allocFieldType()
	ret.Tp = tp
	ret.Flen = types.UnspecifiedLength
	ret.Decimal = types.UnspecifiedLength
	return ret
}

func (ac *allocator) allocFieldType() *types.FieldType {
	if len(ac.fieldType) == cap(ac.fieldType) {
		capacity := cap(ac.fieldType)
		switch {
		case capacity == 0:
			capacity = 256
		case capacity > 1024:
			capacity += 1024
		default:
			capacity *= 2
		}
		ac.fieldType = make([]types.FieldType, 0, capacity)
	}
	ac.fieldType = ac.fieldType[:len(ac.fieldType)+1]
	return &ac.fieldType[len(ac.fieldType)-1]
}

func (ac *allocator) reset() {
	for i := 0; i < len(ac.valueExpr); i++ {
		ac.valueExpr[i] = ast.ValueExpr{}
	}
	ac.valueExpr = ac.valueExpr[:0]

	for i := 0; i < len(ac.deleteStmt); i++ {
		ac.deleteStmt[i] = ast.DeleteStmt{}
	}
	ac.deleteStmt = ac.deleteStmt[:0]

	for i := 0; i < len(ac.insertStmt); i++ {
		ac.insertStmt[i] = ast.InsertStmt{}
	}
	ac.insertStmt = ac.insertStmt[:0]

	for i := 0; i < len(ac.unionStmt); i++ {
		ac.unionStmt[i] = ast.UnionStmt{}
	}
	ac.unionStmt = ac.unionStmt[:0]

	for i := 0; i < len(ac.updateStmt); i++ {
		ac.updateStmt[i] = ast.UpdateStmt{}
	}
	ac.updateStmt = ac.updateStmt[:0]

	for i := 0; i < len(ac.selectStmt); i++ {
		ac.selectStmt[i] = ast.SelectStmt{}
	}
	ac.selectStmt = ac.selectStmt[:0]

	for i := 0; i < len(ac.showStmt); i++ {
		ac.showStmt[i] = ast.ShowStmt{}
	}
	ac.showStmt = ac.showStmt[:0]

	for i := 0; i < len(ac.assignment); i++ {
		ac.assignment[i] = ast.Assignment{}
	}
	ac.assignment = ac.assignment[:0]

	for i := 0; i < len(ac.byItem); i++ {
		ac.byItem[i] = ast.ByItem{}
	}
	ac.byItem = ac.byItem[:0]

	for i := 0; i < len(ac.fieldList); i++ {
		ac.fieldList[i] = ast.FieldList{}
	}
	ac.fieldList = ac.fieldList[:0]

	for i := 0; i < len(ac.groupByClause); i++ {
		ac.groupByClause[i] = ast.GroupByClause{}
	}
	ac.groupByClause = ac.groupByClause[:0]

	for i := 0; i < len(ac.havingClause); i++ {
		ac.havingClause[i] = ast.HavingClause{}
	}
	ac.havingClause = ac.havingClause[:0]

	for i := 0; i < len(ac.join); i++ {
		ac.join[i] = ast.Join{}
	}
	ac.join = ac.join[:0]

	for i := 0; i < len(ac.limit); i++ {
		ac.limit[i] = ast.Limit{}
	}
	ac.limit = ac.limit[:0]

	for i := 0; i < len(ac.onCondition); i++ {
		ac.onCondition[i] = ast.OnCondition{}
	}
	ac.onCondition = ac.onCondition[:0]

	for i := 0; i < len(ac.orderByClause); i++ {
		ac.orderByClause[i] = ast.OrderByClause{}
	}
	ac.orderByClause = ac.orderByClause[:0]

	for i := 0; i < len(ac.selectField); i++ {
		ac.selectField[i] = ast.SelectField{}
	}
	ac.selectField = ac.selectField[:0]

	for i := 0; i < len(ac.tableName); i++ {
		ac.tableName[i] = ast.TableName{}
	}
	ac.tableName = ac.tableName[:0]

	for i := 0; i < len(ac.tableRefsClause); i++ {
		ac.tableRefsClause[i] = ast.TableRefsClause{}
	}
	ac.tableRefsClause = ac.tableRefsClause[:0]

	for i := 0; i < len(ac.tableSource); i++ {
		ac.tableSource[i] = ast.TableSource{}
	}
	ac.tableSource = ac.tableSource[:0]

	for i := 0; i < len(ac.unionSelectList); i++ {
		ac.unionSelectList[i] = ast.UnionSelectList{}
	}
	ac.unionSelectList = ac.unionSelectList[:0]

	for i := 0; i < len(ac.wildCardField); i++ {
		ac.wildCardField[i] = ast.WildCardField{}
	}
	ac.wildCardField = ac.wildCardField[:0]
}

func (ac *allocator) allocTableRefsClause() *ast.TableRefsClause {
	if len(ac.tableRefsClause) == cap(ac.tableRefsClause) {
		capacity := cap(ac.tableRefsClause)
		switch {
		case capacity == 0:
			capacity = 32
		case capacity > 1024:
			capacity += 256
		default:
			capacity *= 2
		}
		ac.tableRefsClause = make([]ast.TableRefsClause, 0, capacity)
	}
	ac.tableRefsClause = ac.tableRefsClause[:len(ac.tableRefsClause)+1]
	return &ac.tableRefsClause[len(ac.tableRefsClause)-1]
}

func (ac *allocator) allocTableSource() *ast.TableSource {
	if len(ac.tableSource) == cap(ac.tableSource) {
		capacity := cap(ac.tableSource)
		switch {
		case capacity == 0:
			capacity = 32
		case capacity > 1024:
			capacity += 256
		default:
			capacity *= 2
		}
		ac.tableSource = make([]ast.TableSource, 0, capacity)
	}
	ac.tableSource = ac.tableSource[:len(ac.tableSource)+1]
	return &ac.tableSource[len(ac.tableSource)-1]
}

func (ac *allocator) allocValueExpr() *ast.ValueExpr {
	if len(ac.valueExpr) == cap(ac.valueExpr) {
		capacity := cap(ac.valueExpr)
		switch {
		case capacity == 0:
			capacity = 512
		default:
			capacity *= 2
		}
		ac.valueExpr = make([]ast.ValueExpr, 0, capacity)
	}
	ac.valueExpr = ac.valueExpr[:len(ac.valueExpr)+1]
	return &ac.valueExpr[len(ac.valueExpr)-1]
}

func (ac *allocator) allocDeleteStmt() *ast.DeleteStmt {
	if len(ac.deleteStmt) == cap(ac.deleteStmt) {
		capacity := cap(ac.deleteStmt)
		switch {
		case capacity == 0:
			capacity = 32
		case capacity > 1024:
			capacity += 256
		default:
			capacity *= 2
		}
		ac.deleteStmt = make([]ast.DeleteStmt, 0, capacity)
	}
	ac.deleteStmt = ac.deleteStmt[:len(ac.deleteStmt)+1]
	return &ac.deleteStmt[len(ac.deleteStmt)-1]
}

func (ac *allocator) allocInsertStmt() *ast.InsertStmt {
	if len(ac.insertStmt) == cap(ac.insertStmt) {
		capacity := cap(ac.insertStmt)
		switch {
		case capacity == 0:
			capacity = 32
		case capacity > 1024:
			capacity += 256
		default:
			capacity *= 2
		}
		ac.insertStmt = make([]ast.InsertStmt, 0, capacity)
	}
	ac.insertStmt = ac.insertStmt[:len(ac.insertStmt)+1]
	return &ac.insertStmt[len(ac.insertStmt)-1]
}

func (ac *allocator) allocUnionStmt() *ast.UnionStmt {
	if len(ac.unionStmt) == cap(ac.unionStmt) {
		capacity := cap(ac.unionStmt)
		switch {
		case capacity == 0:
			capacity = 32
		case capacity > 1024:
			capacity += 256
		default:
			capacity *= 2
		}
		ac.unionStmt = make([]ast.UnionStmt, 0, capacity)
	}
	ac.unionStmt = ac.unionStmt[:len(ac.unionStmt)+1]
	return &ac.unionStmt[len(ac.unionStmt)-1]
}

func (ac *allocator) allocUpdateStmt() *ast.UpdateStmt {
	if len(ac.updateStmt) == cap(ac.updateStmt) {
		capacity := cap(ac.updateStmt)
		switch {
		case capacity == 0:
			capacity = 32
		case capacity > 1024:
			capacity += 256
		default:
			capacity *= 2
		}
		ac.updateStmt = make([]ast.UpdateStmt, 0, capacity)
	}
	ac.updateStmt = ac.updateStmt[:len(ac.updateStmt)+1]
	return &ac.updateStmt[len(ac.updateStmt)-1]
}

func (ac *allocator) allocSelectStmt() *ast.SelectStmt {
	if len(ac.selectStmt) == cap(ac.selectStmt) {
		capacity := cap(ac.selectStmt)
		switch {
		case capacity == 0:
			capacity = 32
		case capacity > 1024:
			capacity += 256
		default:
			capacity *= 2
		}
		ac.selectStmt = make([]ast.SelectStmt, 0, capacity)
	}
	ac.selectStmt = ac.selectStmt[:len(ac.selectStmt)+1]
	return &ac.selectStmt[len(ac.selectStmt)-1]
}

func (ac *allocator) allocShowStmt() *ast.ShowStmt {
	if len(ac.showStmt) == cap(ac.showStmt) {
		capacity := cap(ac.showStmt)
		switch {
		case capacity == 0:
			capacity = 32
		case capacity > 1024:
			capacity += 256
		default:
			capacity *= 2
		}
		ac.showStmt = make([]ast.ShowStmt, 0, capacity)
	}
	ac.showStmt = ac.showStmt[:len(ac.showStmt)+1]
	return &ac.showStmt[len(ac.showStmt)-1]
}

func (ac *allocator) allocAssignment() *ast.Assignment {
	if len(ac.assignment) == cap(ac.assignment) {
		capacity := cap(ac.assignment)
		switch {
		case capacity == 0:
			capacity = 32
		case capacity > 1024:
			capacity += 256
		default:
			capacity *= 2
		}
		ac.assignment = make([]ast.Assignment, 0, capacity)
	}
	ac.assignment = ac.assignment[:len(ac.assignment)+1]
	return &ac.assignment[len(ac.assignment)-1]
}

func (ac *allocator) allocByItem() *ast.ByItem {
	if len(ac.byItem) == cap(ac.byItem) {
		capacity := cap(ac.byItem)
		switch {
		case capacity == 0:
			capacity = 32
		case capacity > 1024:
			capacity += 256
		default:
			capacity *= 2
		}
		ac.byItem = make([]ast.ByItem, 0, capacity)
	}
	ac.byItem = ac.byItem[:len(ac.byItem)+1]
	return &ac.byItem[len(ac.byItem)-1]
}

func (ac *allocator) allocFieldList() *ast.FieldList {
	if len(ac.fieldList) == cap(ac.fieldList) {
		capacity := cap(ac.fieldList)
		switch {
		case capacity == 0:
			capacity = 32
		case capacity > 1024:
			capacity += 256
		default:
			capacity *= 2
		}
		ac.fieldList = make([]ast.FieldList, 0, capacity)
	}
	ac.fieldList = ac.fieldList[:len(ac.fieldList)+1]
	return &ac.fieldList[len(ac.fieldList)-1]
}

func (ac *allocator) allocGroupByClause() *ast.GroupByClause {
	if len(ac.groupByClause) == cap(ac.groupByClause) {
		capacity := cap(ac.groupByClause)
		switch {
		case capacity == 0:
			capacity = 32
		case capacity > 1024:
			capacity += 256
		default:
			capacity *= 2
		}
		ac.groupByClause = make([]ast.GroupByClause, 0, capacity)
	}
	ac.groupByClause = ac.groupByClause[:len(ac.groupByClause)+1]
	return &ac.groupByClause[len(ac.groupByClause)-1]
}

func (ac *allocator) allocHavingClause() *ast.HavingClause {
	if len(ac.havingClause) == cap(ac.havingClause) {
		capacity := cap(ac.havingClause)
		switch {
		case capacity == 0:
			capacity = 32
		case capacity > 1024:
			capacity += 256
		default:
			capacity *= 2
		}
		ac.havingClause = make([]ast.HavingClause, 0, capacity)
	}
	ac.havingClause = ac.havingClause[:len(ac.havingClause)+1]
	return &ac.havingClause[len(ac.havingClause)-1]
}

func (ac *allocator) allocJoin() *ast.Join {
	if len(ac.join) == cap(ac.join) {
		capacity := cap(ac.join)
		switch {
		case capacity == 0:
			capacity = 32
		case capacity > 1024:
			capacity += 256
		default:
			capacity *= 2
		}
		ac.join = make([]ast.Join, 0, capacity)
	}
	ac.join = ac.join[:len(ac.join)+1]
	return &ac.join[len(ac.join)-1]
}

func (ac *allocator) allocLimit() *ast.Limit {
	if len(ac.limit) == cap(ac.limit) {
		capacity := cap(ac.limit)
		switch {
		case capacity == 0:
			capacity = 32
		case capacity > 1024:
			capacity += 256
		default:
			capacity *= 2
		}
		ac.limit = make([]ast.Limit, 0, capacity)
	}
	ac.limit = ac.limit[:len(ac.limit)+1]
	return &ac.limit[len(ac.limit)-1]
}

func (ac *allocator) allocOnCondition() *ast.OnCondition {
	if len(ac.onCondition) == cap(ac.onCondition) {
		capacity := cap(ac.onCondition)
		switch {
		case capacity == 0:
			capacity = 32
		case capacity > 1024:
			capacity += 256
		default:
			capacity *= 2
		}
		ac.onCondition = make([]ast.OnCondition, 0, capacity)
	}
	ac.onCondition = ac.onCondition[:len(ac.onCondition)+1]
	return &ac.onCondition[len(ac.onCondition)-1]
}

func (ac *allocator) allocOrderByClause() *ast.OrderByClause {
	if len(ac.orderByClause) == cap(ac.orderByClause) {
		capacity := cap(ac.orderByClause)
		switch {
		case capacity == 0:
			capacity = 32
		case capacity > 1024:
			capacity += 256
		default:
			capacity *= 2
		}
		ac.orderByClause = make([]ast.OrderByClause, 0, capacity)
	}
	ac.orderByClause = ac.orderByClause[:len(ac.orderByClause)+1]
	return &ac.orderByClause[len(ac.orderByClause)-1]
}

func (ac *allocator) allocSelectField() *ast.SelectField {
	if len(ac.selectField) == cap(ac.selectField) {
		capacity := cap(ac.selectField)
		switch {
		case capacity == 0:
			capacity = 32
		case capacity > 1024:
			capacity += 256
		default:
			capacity *= 2
		}
		ac.selectField = make([]ast.SelectField, 0, capacity)
	}
	ac.selectField = ac.selectField[:len(ac.selectField)+1]
	return &ac.selectField[len(ac.selectField)-1]
}

func (ac *allocator) allocTableName() *ast.TableName {
	if len(ac.tableName) == cap(ac.tableName) {
		capacity := cap(ac.tableName)
		switch {
		case capacity == 0:
			capacity = 32
		case capacity > 1024:
			capacity += 256
		default:
			capacity *= 2
		}
		ac.tableName = make([]ast.TableName, 0, capacity)
	}
	ac.tableName = ac.tableName[:len(ac.tableName)+1]
	return &ac.tableName[len(ac.tableName)-1]
}

func (ac *allocator) allocUnionSelectList() *ast.UnionSelectList {
	if len(ac.unionSelectList) == cap(ac.unionSelectList) {
		capacity := cap(ac.unionSelectList)
		switch {
		case capacity == 0:
			capacity = 32
		case capacity > 1024:
			capacity += 256
		default:
			capacity *= 2
		}
		ac.unionSelectList = make([]ast.UnionSelectList, 0, capacity)
	}
	ac.unionSelectList = ac.unionSelectList[:len(ac.unionSelectList)+1]
	return &ac.unionSelectList[len(ac.unionSelectList)-1]
}

func (ac *allocator) allocWildCardField() *ast.WildCardField {
	if len(ac.wildCardField) == cap(ac.wildCardField) {
		capacity := cap(ac.wildCardField)
		switch {
		case capacity == 0:
			capacity = 32
		case capacity > 1024:
			capacity += 256
		default:
			capacity *= 2
		}
		ac.wildCardField = make([]ast.WildCardField, 0, capacity)
	}
	ac.wildCardField = ac.wildCardField[:len(ac.wildCardField)+1]
	return &ac.wildCardField[len(ac.wildCardField)-1]
}
