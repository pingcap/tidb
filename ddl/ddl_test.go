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

package ddl_test

import (
	"testing"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/stmt/stmts"
	"github.com/pingcap/tidb/table"
	qerror "github.com/pingcap/tidb/util/errors"
	"github.com/pingcap/tidb/util/errors2"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	store kv.Storage
	do    *domain.Domain
}

func (ts *testSuite) SetUpSuite(c *C) {
	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	c.Assert(err, IsNil)
	ts.store = store
	do, err := domain.NewDomain(ts.store)
	c.Assert(err, IsNil)
	ts.do = do
}

func (ts *testSuite) TestT(c *C) {
	se, _ := tidb.CreateSession(ts.store)
	ctx := se.(context.Context)
	schemaName := model.NewCIStr("test")
	tblName := model.NewCIStr("t")
	tbIdent := table.Ident{
		Schema: schemaName,
		Name:   tblName,
	}
	noExist := model.NewCIStr("noexist")

	err := ts.do.DDL().CreateSchema(ctx, tbIdent.Schema)
	c.Assert(err, IsNil)

	err = ts.do.DDL().CreateSchema(ctx, tbIdent.Schema)
	c.Assert(errors2.ErrorEqual(err, ddl.ErrExists), IsTrue)

	tbStmt := statement("create table t (a int primary key not null, b varchar(255), key idx_b (b), c int, d int unique)").(*stmts.CreateTableStmt)

	err = ts.do.DDL().CreateTable(ctx, table.Ident{Schema: noExist, Name: tbIdent.Name}, tbStmt.Cols, tbStmt.Constraints)
	c.Assert(errors2.ErrorEqual(err, qerror.ErrDatabaseNotExist), IsTrue)
	err = ts.do.DDL().CreateTable(ctx, tbIdent, tbStmt.Cols, tbStmt.Constraints)
	c.Assert(err, IsNil)
	err = ts.do.DDL().CreateTable(ctx, tbIdent, tbStmt.Cols, tbStmt.Constraints)
	c.Assert(errors2.ErrorEqual(err, ddl.ErrExists), IsTrue)

	tbIdent2 := tbIdent
	tbIdent2.Name = model.NewCIStr("t2")
	tbStmt = statement("create table t2 (a int unique not null)").(*stmts.CreateTableStmt)
	err = ts.do.DDL().CreateTable(ctx, tbIdent2, tbStmt.Cols, tbStmt.Constraints)
	c.Assert(err, IsNil)

	tb, err := ts.do.InfoSchema().TableByName(tbIdent.Schema, tbIdent.Name)
	c.Assert(err, IsNil)
	_, err = tb.AddRecord(ctx, []interface{}{1, "b", 2, 4})
	c.Assert(err, IsNil)

	alterStmt := statement("alter table t add column aa int first").(*stmts.AlterTableStmt)
	ts.do.DDL().AlterTable(ctx, tbIdent, alterStmt.Specs)
	c.Assert(alterStmt.Specs[0].String(), Not(Equals), "")
	// Check indices info
	tbl, err := ts.do.InfoSchema().TableByName(schemaName, tblName)
	c.Assert(err, IsNil)
	c.Assert(tbl, NotNil)
	expectedOffset := make(map[string]int)
	expectedOffset["a"] = 1
	expectedOffset["b"] = 2
	expectedOffset["d"] = 4
	for _, idx := range tbl.Indices() {
		for _, col := range idx.Columns {
			o, ok := expectedOffset[col.Name.L]
			c.Assert(ok, IsTrue)
			c.Assert(col.Offset, Equals, o)
		}
	}
	alterStmt = statement("alter table t add column bb int after b").(*stmts.AlterTableStmt)
	err = ts.do.DDL().AlterTable(ctx, tbIdent, alterStmt.Specs)
	c.Assert(err, IsNil)
	c.Assert(alterStmt.Specs[0].String(), Not(Equals), "")
	// Inserting a duplicated column will cause error.
	alterStmt = statement("alter table t add column bb int after b").(*stmts.AlterTableStmt)
	err = ts.do.DDL().AlterTable(ctx, tbIdent, alterStmt.Specs)
	c.Assert(err, NotNil)

	idxStmt := statement("CREATE INDEX idx_c ON t (c)").(*stmts.CreateIndexStmt)
	idxName := model.NewCIStr(idxStmt.IndexName)
	err = ts.do.DDL().CreateIndex(ctx, tbIdent, idxStmt.Unique, idxName, idxStmt.IndexColNames)
	c.Assert(err, IsNil)
	tbs := ts.do.InfoSchema().SchemaTables(tbIdent.Schema)
	c.Assert(len(tbs), Equals, 2)
	err = ts.do.DDL().DropIndex(ctx, tbIdent.Schema, tbIdent.Name, idxName)
	c.Assert(err, IsNil)
	err = ts.do.DDL().DropTable(ctx, tbIdent)
	c.Assert(err, IsNil)
	tbs = ts.do.InfoSchema().SchemaTables(tbIdent.Schema)
	c.Assert(len(tbs), Equals, 1)

	err = ts.do.DDL().DropSchema(ctx, noExist)
	c.Assert(errors2.ErrorEqual(err, ddl.ErrNotExists), IsTrue)
	err = ts.do.DDL().DropSchema(ctx, tbIdent.Schema)
	c.Assert(err, IsNil)
}

func (ts *testSuite) TestConstraintNames(c *C) {
	se, _ := tidb.CreateSession(ts.store)
	ctx := se.(context.Context)
	schemaName := model.NewCIStr("test")
	tblName := model.NewCIStr("t")
	tbIdent := table.Ident{
		Schema: schemaName,
		Name:   tblName,
	}
	err := ts.do.DDL().CreateSchema(ctx, tbIdent.Schema)
	c.Assert(err, IsNil)
	tbStmt := statement("create table t (a int, b int, index a (a, b), index a (a))").(*stmts.CreateTableStmt)
	err = ts.do.DDL().CreateTable(ctx, tbIdent, tbStmt.Cols, tbStmt.Constraints)
	c.Assert(err, NotNil)

	tbStmt = statement("create table t (a int, b int, index A (a, b), index (a))").(*stmts.CreateTableStmt)
	err = ts.do.DDL().CreateTable(ctx, tbIdent, tbStmt.Cols, tbStmt.Constraints)
	c.Assert(err, IsNil)
	tbl, err := ts.do.InfoSchema().TableByName(schemaName, tblName)
	indices := tbl.Indices()
	c.Assert(len(indices), Equals, 2)
	c.Assert(indices[0].Name.O, Equals, "A")
	c.Assert(indices[1].Name.O, Equals, "a_2")

	err = ts.do.DDL().DropSchema(ctx, tbIdent.Schema)
	c.Assert(err, IsNil)
}

func statement(sql string) stmt.Statement {
	log.Debug("Compile", sql)
	lexer := parser.NewLexer(sql)
	parser.YYParse(lexer)
	return lexer.Stmts()[0]
}
