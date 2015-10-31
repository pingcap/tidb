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

package optimizer_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/ast/parser"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/optimizer"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/testkit"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testInfoBinderSuite{})

type testInfoBinderSuite struct {
}

type binderVerifier struct {
	c *C
}

func (bv *binderVerifier) Enter(node ast.Node) (ast.Node, bool) {
	return node, false
}

func (bv *binderVerifier) Leave(in ast.Node) (out ast.Node, ok bool) {
	switch v := in.(type) {
	case *ast.ColumnName:
		bv.c.Assert(v.ColumnInfo, NotNil)
	case *ast.TableName:
		bv.c.Assert(v.TableInfo, NotNil)
	}
	return in, true
}

func (ts *testInfoBinderSuite) TestInfoBinder(c *C) {
	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	c.Assert(err, IsNil)
	defer store.Close()
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	domain := sessionctx.GetDomain(testKit.Se.(context.Context))

	src := "SELECT c1 from t"
	l := parser.NewLexer(src)
	c.Assert(parser.YYParse(l), Equals, 0)
	stmts := l.Stmts()
	c.Assert(len(stmts), Equals, 1)
	v := &optimizer.InfoBinder{
		Info:          domain.InfoSchema(),
		DefaultSchema: model.NewCIStr("test"),
	}
	selectStmt := stmts[0].(*ast.SelectStmt)
	selectStmt.Accept(v)

	verifier := &binderVerifier{
		c: c,
	}
	selectStmt.Accept(verifier)
}
