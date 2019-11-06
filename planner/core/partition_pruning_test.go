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
	columns, names := expression.ColumnInfos2ColumnsAndNames(ctx, model.NewCIStr("t"), tblInfo.Name, tblInfo.Columns)
	schema := expression.NewSchema(columns...)
	partitionExpr, err := expression.ParseSimpleExprsWithNames(ctx, "to_days(d) < to_days('2007-03-08') and to_days(d) >= to_days('2007-03-07')", schema, names)
	c.Assert(err, IsNil)
	queryExpr, err := expression.ParseSimpleExprsWithNames(ctx, "d < '2000-03-08 00:00:00'", schema, names)
	c.Assert(err, IsNil)
	succ, err := s.canBePruned(ctx, nil, partitionExpr[0], queryExpr)
	c.Assert(err, IsNil)
	c.Assert(succ, IsTrue)

	queryExpr, err = expression.ParseSimpleExprsWithNames(ctx, "d > '2018-03-08 00:00:00'", schema, names)
	c.Assert(err, IsNil)
	succ, err = s.canBePruned(ctx, nil, partitionExpr[0], queryExpr)
	c.Assert(err, IsNil)
	c.Assert(succ, IsTrue)

	// For the following case:
	// CREATE TABLE quarterly_report_status (
	// 	report_id INT NOT NULL,
	// 	report_status VARCHAR(20) NOT NULL,
	// 	report_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)
	// PARTITION BY RANGE (UNIX_TIMESTAMP(report_updated)) (
	// 	PARTITION p0 VALUES LESS THAN (UNIX_TIMESTAMP('2008-01-01 00:00:00')),
	// 	PARTITION p1 VALUES LESS THAN (UNIX_TIMESTAMP('2008-04-01 00:00:00')),
	// 	PARTITION p2 VALUES LESS THAN (UNIX_TIMESTAMP('2010-01-01 00:00:00')),
	// 	PARTITION p3 VALUES LESS THAN (MAXVALUE)
	// );
	stmt, err = p.ParseOneStmt("create table t (report_updated timestamp)", "", "")
	c.Assert(err, IsNil)
	tblInfo, err = ddl.BuildTableInfoFromAST(stmt.(*ast.CreateTableStmt))
	c.Assert(err, IsNil)
	columns, names = expression.ColumnInfos2ColumnsAndNames(ctx, model.NewCIStr("t"), tblInfo.Name, tblInfo.Columns)
	schema = expression.NewSchema(columns...)

	partitionExpr, err = expression.ParseSimpleExprsWithNames(ctx, "unix_timestamp(report_updated) < unix_timestamp('2008-04-01') and unix_timestamp(report_updated) >= unix_timestamp('2008-01-01')", schema, names)
	c.Assert(err, IsNil)
	queryExpr, err = expression.ParseSimpleExprsWithNames(ctx, "report_updated > '2008-05-01 00:00:00'", schema, names)
	c.Assert(err, IsNil)
	succ, err = s.canBePruned(ctx, nil, partitionExpr[0], queryExpr)
	c.Assert(err, IsNil)
	c.Assert(succ, IsTrue)

	queryExpr, err = expression.ParseSimpleExprsWithNames(ctx, "report_updated > unix_timestamp('2008-05-01 00:00:00')", schema, names)
	c.Assert(err, IsNil)
	succ, err = s.canBePruned(ctx, nil, partitionExpr[0], queryExpr)
	c.Assert(err, IsNil)
	_ = succ
	// c.Assert(succ, IsTrue)
	// TODO: Uncomment the check after fixing issue https://github.com/pingcap/tidb/issues/12028
	// report_updated > unix_timestamp('2008-05-01 00:00:00') is converted to gt(t.t.report_updated, <nil>)
	// Because unix_timestamp('2008-05-01 00:00:00') is fold to constant int 1564761600, and compare it with timestamp (report_updated)
	// need to convert 1564761600 to a timestamp, during that step, an error happen and the result is set to <nil>
}
