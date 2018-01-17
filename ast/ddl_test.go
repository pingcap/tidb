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

package ast_test

import (
	. "github.com/pingcap/check"
	. "github.com/pingcap/tidb/ast"
)

var _ = Suite(&testDDLSuite{})

type testDDLSuite struct {
}

func (ts *testDDLSuite) TestDDLVisitorCover(c *C) {
	ce := &checkExpr{}
	constraint := &Constraint{Keys: []*IndexColName{{Column: &ColumnName{}}, {Column: &ColumnName{}}}, Refer: &ReferenceDef{}, Option: &IndexOption{}}

	alterTableSpec := &AlterTableSpec{Constraint: constraint, Options: []*TableOption{{}}, NewTable: &TableName{}, NewColumns: []*ColumnDef{{Name: &ColumnName{}}}, OldColumnName: &ColumnName{}, Position: &ColumnPosition{RelativeColumn: &ColumnName{}}}

	stmts := []struct {
		node             Node
		expectedEnterCnt int
		expectedLeaveCnt int
	}{
		{&CreateDatabaseStmt{}, 0, 0},
		{&DropDatabaseStmt{}, 0, 0},
		{&DropIndexStmt{Table: &TableName{}}, 0, 0},
		{&DropTableStmt{Tables: []*TableName{{}, {}}}, 0, 0},
		{&RenameTableStmt{OldTable: &TableName{}, NewTable: &TableName{}}, 0, 0},
		{&TruncateTableStmt{Table: &TableName{}}, 0, 0},

		// TODO: cover children
		{&AlterTableStmt{Table: &TableName{}, Specs: []*AlterTableSpec{alterTableSpec}}, 0, 0},
		{&CreateIndexStmt{Table: &TableName{}}, 0, 0},
		{&CreateTableStmt{Table: &TableName{}, ReferTable: &TableName{}}, 0, 0},
		{&AlterTableSpec{}, 0, 0},
		{&ColumnDef{Name: &ColumnName{}, Options: []*ColumnOption{{Expr: ce}}}, 1, 1},
		{&ColumnOption{Expr: ce}, 1, 1},
		{&ColumnPosition{RelativeColumn: &ColumnName{}}, 0, 0},
		{&Constraint{Keys: []*IndexColName{{Column: &ColumnName{}}, {Column: &ColumnName{}}}, Refer: &ReferenceDef{}, Option: &IndexOption{}}, 0, 0},
		{&IndexColName{Column: &ColumnName{}}, 0, 0},
		{&ReferenceDef{Table: &TableName{}, IndexColNames: []*IndexColName{{Column: &ColumnName{}}, {Column: &ColumnName{}}}, OnDelete: &OnDeleteOpt{}, OnUpdate: &OnUpdateOpt{}}, 0, 0},
	}

	for _, v := range stmts {
		ce.reset()
		v.node.Accept(checkVisitor{})
		c.Check(ce.enterCnt, Equals, v.expectedEnterCnt)
		c.Check(ce.leaveCnt, Equals, v.expectedLeaveCnt)
		v.node.Accept(visitor1{})
	}
}
