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

// Allocator is an object allocator for Parse.
// It can reduce object allocation during parsing. You should call Reset to reuse memory.
// Note that the returned ast.StmtNode is allocated from the allocator, so you MUST NOT
// reset the allocator when you are still using them.
type Allocator struct {
	cache           []yySymType
	yylval          yySymType
	yyVAL           yySymType
	fieldType       []types.FieldType
	valueExpr       []ast.ValueExpr
	insertStmt      []ast.InsertStmt
	selectStmt      []ast.SelectStmt
	join            []ast.Join
	tableName       []ast.TableName
	tableSource     []ast.TableSource
	tableRefsClause []ast.TableRefsClause
}

// NewAllocator news an Allocator object, which can be used for Parse.
func NewAllocator() *Allocator {
	return &Allocator{
		cache: make([]yySymType, 140),
	}
}

func (ac *Allocator) newValueExpr(v interface{}) *ast.ValueExpr {
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

func (ac *Allocator) newFieldType(tp byte) *types.FieldType {
	ret := ac.allocFieldType()
	ret.Tp = tp
	ret.Flen = types.UnspecifiedLength
	ret.Decimal = types.UnspecifiedLength
	return ret
}

func (ac *Allocator) allocFieldType() *types.FieldType {
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
	ac.fieldType[len(ac.fieldType)-1] = types.FieldType{}
	return &ac.fieldType[len(ac.fieldType)-1]
}

// Reset resets the Allocator's inner field so the memory space can be reused.
func (ac *Allocator) Reset() {
	ac.valueExpr = ac.valueExpr[:0]
	ac.fieldType = ac.fieldType[:0]
	ac.insertStmt = ac.insertStmt[:0]
	ac.selectStmt = ac.selectStmt[:0]
	ac.join = ac.join[:0]
	ac.tableName = ac.tableName[:0]
	ac.tableSource = ac.tableSource[:0]
	ac.tableRefsClause = ac.tableRefsClause[:0]
}

func (ac *Allocator) allocTableRefsClause() *ast.TableRefsClause {
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
	ac.tableRefsClause[len(ac.tableRefsClause)-1] = ast.TableRefsClause{}
	return &ac.tableRefsClause[len(ac.tableRefsClause)-1]
}

func (ac *Allocator) allocTableSource() *ast.TableSource {
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
	ac.tableSource[len(ac.tableSource)-1] = ast.TableSource{}
	return &ac.tableSource[len(ac.tableSource)-1]
}

func (ac *Allocator) allocValueExpr() *ast.ValueExpr {
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
	ac.valueExpr[len(ac.valueExpr)-1] = ast.ValueExpr{}
	return &ac.valueExpr[len(ac.valueExpr)-1]
}

func (ac *Allocator) allocInsertStmt() *ast.InsertStmt {
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
	ac.insertStmt[len(ac.insertStmt)-1] = ast.InsertStmt{}
	return &ac.insertStmt[len(ac.insertStmt)-1]
}

func (ac *Allocator) allocSelectStmt() *ast.SelectStmt {
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
	ac.selectStmt[len(ac.selectStmt)-1] = ast.SelectStmt{}
	return &ac.selectStmt[len(ac.selectStmt)-1]
}

func (ac *Allocator) allocJoin() *ast.Join {
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
	ac.join[len(ac.join)-1] = ast.Join{}
	return &ac.join[len(ac.join)-1]
}

func (ac *Allocator) allocTableName() *ast.TableName {
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
	ac.tableName[len(ac.tableName)-1] = ast.TableName{}
	return &ac.tableName[len(ac.tableName)-1]
}
