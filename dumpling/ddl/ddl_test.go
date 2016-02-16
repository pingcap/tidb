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
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/coldef"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/stmt/stmts"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	store       kv.Storage
	charsetInfo *coldef.CharsetOpt
}

func (ts *testSuite) SetUpSuite(c *C) {
	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	c.Assert(err, IsNil)
	ts.store = store
	ts.charsetInfo = &coldef.CharsetOpt{
		Chs: "utf8",
		Col: "utf8_bin",
	}

}

func (ts *testSuite) TestDDL(c *C) {
	// TODO: rewrite the test.
	c.Skip("this test assume statement to be `stmts` types, which has changed.")
	se, _ := tidb.CreateSession(ts.store)
	ctx := se.(context.Context)
	schemaName := model.NewCIStr("test_ddl")
	tblName := model.NewCIStr("t")
	tbIdent := table.Ident{
		Schema: schemaName,
		Name:   tblName,
	}
	noExist := model.NewCIStr("noexist")

	err := sessionctx.GetDomain(ctx).DDL().CreateSchema(ctx, tbIdent.Schema, ts.charsetInfo)
	c.Assert(err, IsNil)
	err = sessionctx.GetDomain(ctx).DDL().CreateSchema(ctx, tbIdent.Schema, ts.charsetInfo)
	c.Assert(terror.ErrorEqual(err, infoschema.DatabaseExists), IsTrue)

	tbStmt := statement(ctx, "create table t (a int primary key not null, b varchar(255), key idx_b (b), c int, d int unique)").(*stmts.CreateTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().CreateTable(ctx, table.Ident{Schema: noExist, Name: tbIdent.Name}, tbStmt.Cols, tbStmt.Constraints)
	c.Assert(infoschema.DatabaseNotExists.Equal(err), IsTrue)

	err = sessionctx.GetDomain(ctx).DDL().CreateTable(ctx, tbIdent, tbStmt.Cols, tbStmt.Constraints)
	c.Assert(err, IsNil)

	err = sessionctx.GetDomain(ctx).DDL().CreateTable(ctx, tbIdent, tbStmt.Cols, tbStmt.Constraints)
	c.Assert(terror.ErrorEqual(err, infoschema.TableExists), IsTrue)

	tb, err := sessionctx.GetDomain(ctx).InfoSchema().TableByName(tbIdent.Schema, tbIdent.Name)
	c.Assert(err, IsNil)
	c.Assert(tb, NotNil)
	_, err = tb.AddRecord(ctx, []interface{}{1, "b", 2, 4})
	c.Assert(err, IsNil)

	alterStmt := statement(ctx, "alter table t add column aa int first").(*stmts.AlterTableStmt)
	sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent, alterStmt.Specs)
	c.Assert(alterStmt.Specs[0].String(), Not(Equals), "")

	tbl, err := sessionctx.GetDomain(ctx).InfoSchema().TableByName(schemaName, tblName)
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

	alterStmt = statement(ctx, "alter table t add column bb int after b").(*stmts.AlterTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent, alterStmt.Specs)
	c.Assert(err, IsNil)
	c.Assert(alterStmt.Specs[0].String(), Not(Equals), "")

	// Test add a duplicated column to table, get an error.
	alterStmt = statement(ctx, "alter table t add column bb int after b").(*stmts.AlterTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent, alterStmt.Specs)
	c.Assert(err, NotNil)

	// Test column schema change in t2.
	tbIdent2 := tbIdent
	tbIdent2.Name = model.NewCIStr("t2")
	tbStmt = statement(ctx, "create table t2 (a int unique not null)").(*stmts.CreateTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().CreateTable(ctx, tbIdent2, tbStmt.Cols, tbStmt.Constraints)
	c.Assert(err, IsNil)
	tb, err = sessionctx.GetDomain(ctx).InfoSchema().TableByName(tbIdent2.Schema, tbIdent2.Name)
	c.Assert(err, IsNil)
	c.Assert(tb, NotNil)
	rid0, err := tb.AddRecord(ctx, []interface{}{1})
	c.Assert(err, IsNil)
	rid1, err := tb.AddRecord(ctx, []interface{}{2})
	c.Assert(err, IsNil)

	alterStmt = statement(ctx, `alter table t2 add b enum("bb") first`).(*stmts.AlterTableStmt)
	sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent2, alterStmt.Specs)
	c.Assert(alterStmt.Specs[0].String(), Not(Equals), "")
	tb, err = sessionctx.GetDomain(ctx).InfoSchema().TableByName(tbIdent2.Schema, tbIdent2.Name)
	c.Assert(err, IsNil)
	c.Assert(tb, NotNil)
	cols, err := tb.Row(ctx, rid0)
	c.Assert(err, IsNil)
	c.Assert(len(cols), Equals, 2)
	c.Assert(cols[0], Equals, nil)
	c.Assert(cols[1], Equals, int64(1))

	alterStmt = statement(ctx, `alter table t2 add c varchar(255) default "abc" after b`).(*stmts.AlterTableStmt)
	sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent2, alterStmt.Specs)
	c.Assert(alterStmt.Specs[0].String(), Not(Equals), "")
	tb, err = sessionctx.GetDomain(ctx).InfoSchema().TableByName(tbIdent2.Schema, tbIdent2.Name)
	c.Assert(err, IsNil)
	c.Assert(tb, NotNil)
	cols, err = tb.Row(ctx, rid1)
	c.Assert(err, IsNil)
	c.Assert(len(cols), Equals, 3)
	c.Assert(cols[0], Equals, nil)
	c.Assert(cols[1], BytesEquals, []byte("abc"))
	c.Assert(cols[2], Equals, int64(2))
	rid3, err := tb.AddRecord(ctx, []interface{}{mysql.Enum{Name: "bb", Value: 1}, "c", 3})
	c.Assert(err, IsNil)
	cols, err = tb.Row(ctx, rid3)
	c.Assert(err, IsNil)
	c.Assert(len(cols), Equals, 3)
	c.Assert(cols[0], Equals, mysql.Enum{Name: "bb", Value: 1})
	c.Assert(cols[1], BytesEquals, []byte("c"))
	c.Assert(cols[2], Equals, int64(3))

	// Test add column after a not exist column, get an error.
	alterStmt = statement(ctx, `alter table t2 add b int after xxxx`).(*stmts.AlterTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent2, alterStmt.Specs)
	c.Assert(err, NotNil)

	// Test add column to a not exist table, get an error.
	tbIdent3 := tbIdent
	tbIdent3.Name = model.NewCIStr("t3")
	alterStmt = statement(ctx, `alter table t3 add b int first`).(*stmts.AlterTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent3, alterStmt.Specs)
	c.Assert(err, NotNil)

	// Test drop column.
	alterStmt = statement(ctx, "alter table t2 drop column b").(*stmts.AlterTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent2, alterStmt.Specs)
	c.Assert(err, IsNil)
	tb, err = sessionctx.GetDomain(ctx).InfoSchema().TableByName(tbIdent2.Schema, tbIdent2.Name)
	c.Assert(err, IsNil)
	c.Assert(tb, NotNil)

	cols, err = tb.Row(ctx, rid0)
	c.Assert(err, IsNil)
	c.Assert(len(cols), Equals, 2)
	c.Assert(cols[0], BytesEquals, []byte("abc"))
	c.Assert(cols[1], Equals, int64(1))

	// Test drop a not exist column from table, get an error.
	alterStmt = statement(ctx, `alter table t2 drop column xxx`).(*stmts.AlterTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent2, alterStmt.Specs)
	c.Assert(err, NotNil)

	// Test drop column from a not exist table, get an error.
	alterStmt = statement(ctx, `alter table t3 drop column a`).(*stmts.AlterTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent3, alterStmt.Specs)
	c.Assert(err, NotNil)

	// Test index schema change.
	idxStmt := statement(ctx, "CREATE INDEX idx_c ON t (c)").(*stmts.CreateIndexStmt)
	idxName := model.NewCIStr(idxStmt.IndexName)
	err = sessionctx.GetDomain(ctx).DDL().CreateIndex(ctx, tbIdent, idxStmt.Unique, idxName, idxStmt.IndexColNames)
	c.Assert(err, IsNil)
	tbs := sessionctx.GetDomain(ctx).InfoSchema().SchemaTables(tbIdent.Schema)
	c.Assert(len(tbs), Equals, 2)
	err = sessionctx.GetDomain(ctx).DDL().DropIndex(ctx, tbIdent, idxName)
	c.Assert(err, IsNil)

	alterStmt = statement(ctx, "alter table t add index idx_c (c)").(*stmts.AlterTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent, alterStmt.Specs)
	c.Assert(err, IsNil)

	alterStmt = statement(ctx, "alter table t drop index idx_c").(*stmts.AlterTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent, alterStmt.Specs)
	c.Assert(err, IsNil)

	alterStmt = statement(ctx, "alter table t add unique index idx_c (c)").(*stmts.AlterTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent, alterStmt.Specs)
	c.Assert(err, IsNil)

	alterStmt = statement(ctx, "alter table t drop index idx_c").(*stmts.AlterTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent, alterStmt.Specs)
	c.Assert(err, IsNil)

	err = sessionctx.GetDomain(ctx).DDL().DropTable(ctx, tbIdent)
	c.Assert(err, IsNil)

	tbs = sessionctx.GetDomain(ctx).InfoSchema().SchemaTables(tbIdent.Schema)
	c.Assert(len(tbs), Equals, 1)

	err = sessionctx.GetDomain(ctx).DDL().DropSchema(ctx, noExist)
	c.Assert(terror.ErrorEqual(err, infoschema.DatabaseNotExists), IsTrue)

	err = sessionctx.GetDomain(ctx).DDL().DropSchema(ctx, tbIdent.Schema)
	c.Assert(err, IsNil)
}

func (ts *testSuite) TestConstraintNames(c *C) {
	// TODO: rewrite the test.
	c.Skip("this test assume statement to be `stmts` types, which has changed.")
	se, _ := tidb.CreateSession(ts.store)
	ctx := se.(context.Context)
	schemaName := model.NewCIStr("test_constraint")
	tblName := model.NewCIStr("t")
	tbIdent := table.Ident{
		Schema: schemaName,
		Name:   tblName,
	}

	err := sessionctx.GetDomain(ctx).DDL().CreateSchema(ctx, tbIdent.Schema, ts.charsetInfo)
	c.Assert(err, IsNil)

	tbStmt := statement(ctx, "create table t (a int, b int, index a (a, b), index a (a))").(*stmts.CreateTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().CreateTable(ctx, tbIdent, tbStmt.Cols, tbStmt.Constraints)
	c.Assert(err, NotNil)

	tbStmt = statement(ctx, "create table t (a int, b int, index A (a, b), index (a))").(*stmts.CreateTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().CreateTable(ctx, tbIdent, tbStmt.Cols, tbStmt.Constraints)
	c.Assert(err, IsNil)
	tbl, err := sessionctx.GetDomain(ctx).InfoSchema().TableByName(schemaName, tblName)
	indices := tbl.Indices()
	c.Assert(len(indices), Equals, 2)
	c.Assert(indices[0].Name.O, Equals, "A")
	c.Assert(indices[1].Name.O, Equals, "a_2")

	err = sessionctx.GetDomain(ctx).DDL().DropSchema(ctx, tbIdent.Schema)
	c.Assert(err, IsNil)
}

func (ts *testSuite) TestAlterTableColumn(c *C) {
	// TODO: rewrite the test.
	c.Skip("this test assume statement to be `stmts` types, which has changed.")
	se, _ := tidb.CreateSession(ts.store)
	ctx := se.(context.Context)
	schemaName := model.NewCIStr("test_alter_add_column")
	tbIdent := table.Ident{
		Schema: schemaName,
		Name:   model.NewCIStr("t"),
	}

	err := sessionctx.GetDomain(ctx).DDL().CreateSchema(ctx, tbIdent.Schema, ts.charsetInfo)
	c.Assert(err, IsNil)

	tbStmt := statement(ctx, "create table t (a int, b int)").(*stmts.CreateTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().CreateTable(ctx, tbIdent, tbStmt.Cols, tbStmt.Constraints)
	c.Assert(err, IsNil)

	alterStmt := statement(ctx, "alter table t add column c int PRIMARY KEY").(*stmts.AlterTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent, alterStmt.Specs)
	c.Assert(err, NotNil)

	alterStmt = statement(ctx, "alter table t add column c int AUTO_INCREMENT PRIMARY KEY").(*stmts.AlterTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent, alterStmt.Specs)
	c.Assert(err, NotNil)

	alterStmt = statement(ctx, "alter table t add column c int UNIQUE").(*stmts.AlterTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent, alterStmt.Specs)
	c.Assert(err, NotNil)

	alterStmt = statement(ctx, "alter table t add column c int UNIQUE KEY").(*stmts.AlterTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent, alterStmt.Specs)
	c.Assert(err, NotNil)

	alterStmt = statement(ctx, "alter table t add column c int, add column d int").(*stmts.AlterTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent, alterStmt.Specs)
	c.Assert(err, NotNil)

	// Notice: Now we have not supported.
	// alterStmt = statement(ctx, "alter table t add column c int KEY").(*stmts.AlterTableStmt)
	// err = sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent, alterStmt.Specs)
	// c.Assert(err, NotNil)

	tbIdent2 := table.Ident{
		Schema: schemaName,
		Name:   model.NewCIStr("t1"),
	}

	tbStmt = statement(ctx, "create table t1 (a int, b int, c int, d int, index A (a, b))").(*stmts.CreateTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().CreateTable(ctx, tbIdent2, tbStmt.Cols, tbStmt.Constraints)
	c.Assert(err, IsNil)

	alterStmt = statement(ctx, "alter table t1 drop column a").(*stmts.AlterTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent2, alterStmt.Specs)
	c.Assert(err, NotNil)

	alterStmt = statement(ctx, "alter table t1 drop column b").(*stmts.AlterTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent2, alterStmt.Specs)
	c.Assert(err, NotNil)

	alterStmt = statement(ctx, "alter table t1 drop column c, drop column d").(*stmts.AlterTableStmt)
	err = sessionctx.GetDomain(ctx).DDL().AlterTable(ctx, tbIdent, alterStmt.Specs)
	c.Assert(err, NotNil)

	err = sessionctx.GetDomain(ctx).DDL().DropSchema(ctx, tbIdent.Schema)
	c.Assert(err, IsNil)
}

func statement(ctx context.Context, sql string) stmt.Statement {
	log.Debug("[ddl] Compile", sql)
	s, _ := parser.ParseOneStmt(sql, "", "")
	compiler := &executor.Compiler{}
	stm, _ := compiler.Compile(ctx, s)
	return stm
}

func init() {
	log.SetLevelByString("error")
}
