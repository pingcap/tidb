// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/mock"
)

var _ = Suite(&testPartitionPruningSuite{})

type testPartitionPruningSuite struct {
	partitionProcessor
}

func (s *testPartitionPruningSuite) TestCanBePrune(c *C) {
	p := parser.New()
	stmt, err := p.ParseOneStmt("create table t (d datetime not null)", "", "")
	c.Assert(err, IsNil)
	tblInfo, err := ddl.BuildTableInfoFromAST(stmt.(*ast.CreateTableStmt))
	c.Assert(err, IsNil)

	// For the following case:
	// CREATE TABLE t1 ( recdate  DATETIME NOT NULL )
	// PARTITION BY RANGE( TO_DAYS(recdate) ) (
	// 	PARTITION p0 VALUES LESS THAN ( TO_DAYS('2007-03-08') ),
	// 	PARTITION p1 VALUES LESS THAN ( TO_DAYS('2007-04-01') )
	// );
	// SELECT * FROM t1 WHERE recdate < '2007-03-08 00:00:00';
	// SELECT * FROM t1 WHERE recdate > '2018-03-08 00:00:00';

	ctx := mock.NewContext()
	columns := expression.ColumnInfos2ColumnsWithDBName(ctx, model.NewCIStr("t"), tblInfo.Name, tblInfo.Columns)
	schema := expression.NewSchema(columns...)
	partitionExpr, err := expression.ParseSimpleExprsWithSchema(ctx, "to_days(d) < to_days('2007-03-08') and to_days(d) >= to_days('2007-03-07')", schema)
	c.Assert(err, IsNil)
	queryExpr, err := expression.ParseSimpleExprsWithSchema(ctx, "d < '2000-03-08 00:00:00'", schema)
	c.Assert(err, IsNil)
	succ, err := s.canBePruned(ctx, nil, partitionExpr[0], queryExpr)
	c.Assert(err, IsNil)
	c.Assert(succ, IsTrue)

	queryExpr, err = expression.ParseSimpleExprsWithSchema(ctx, "d > '2018-03-08 00:00:00'", schema)
	c.Assert(err, IsNil)
	succ, err = s.canBePruned(ctx, nil, partitionExpr[0], queryExpr)
	c.Assert(err, IsNil)
	c.Assert(succ, IsTrue)
}
