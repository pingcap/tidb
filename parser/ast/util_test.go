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
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	. "github.com/pingcap/parser/ast"
	. "github.com/pingcap/parser/format"
	"github.com/pingcap/tidb/types/parser_driver"
)

var _ = Suite(&testCacheableSuite{})

type testCacheableSuite struct {
}

func (s *testCacheableSuite) TestCacheable(c *C) {
	// test non-SelectStmt
	var stmt Node = &DeleteStmt{}
	c.Assert(IsReadOnly(stmt), IsFalse)

	stmt = &InsertStmt{}
	c.Assert(IsReadOnly(stmt), IsFalse)

	stmt = &UpdateStmt{}
	c.Assert(IsReadOnly(stmt), IsFalse)

	stmt = &ExplainStmt{}
	c.Assert(IsReadOnly(stmt), IsTrue)

	stmt = &ExplainStmt{}
	c.Assert(IsReadOnly(stmt), IsTrue)

	stmt = &DoStmt{}
	c.Assert(IsReadOnly(stmt), IsTrue)

	stmt = &ExplainStmt{
		Stmt: &InsertStmt{},
	}
	c.Assert(IsReadOnly(stmt), IsTrue)

	stmt = &ExplainStmt{
		Analyze: true,
		Stmt:    &InsertStmt{},
	}
	c.Assert(IsReadOnly(stmt), IsFalse)

	stmt = &ExplainStmt{
		Stmt: &SelectStmt{},
	}
	c.Assert(IsReadOnly(stmt), IsTrue)

	stmt = &ExplainStmt{
		Analyze: true,
		Stmt:    &SelectStmt{},
	}
	c.Assert(IsReadOnly(stmt), IsTrue)

}

// CleanNodeText set the text of node and all child node empty.
// For test only.
func CleanNodeText(node Node) {
	var cleaner nodeTextCleaner
	node.Accept(&cleaner)
}

// nodeTextCleaner clean the text of a node and it's child node.
// For test only.
type nodeTextCleaner struct {
}

// Enter implements Visitor interface.
func (checker *nodeTextCleaner) Enter(in Node) (out Node, skipChildren bool) {
	in.SetText("")
	switch node := in.(type) {
	case *Constraint:
		if node.Option != nil {
			if node.Option.KeyBlockSize == 0x0 && node.Option.Tp == 0 && node.Option.Comment == "" {
				node.Option = nil
			}
		}
	case *FuncCallExpr:
		node.FnName.O = strings.ToLower(node.FnName.O)
		switch node.FnName.L {
		case "convert":
			node.Args[1].(*driver.ValueExpr).Datum.SetBytes(nil)
		}
	case *AggregateFuncExpr:
		node.F = strings.ToLower(node.F)
	case *FieldList:
		for _, f := range node.Fields {
			f.Offset = 0
		}
	case *AlterTableSpec:
		for _, opt := range node.Options {
			opt.StrValue = strings.ToLower(opt.StrValue)
		}
	}
	return in, false
}

// Leave implements Visitor interface.
func (checker *nodeTextCleaner) Leave(in Node) (out Node, ok bool) {
	return in, true
}

type NodeRestoreTestCase struct {
	sourceSQL string
	expectSQL string
}

func RunNodeRestoreTest(c *C, nodeTestCases []NodeRestoreTestCase, template string, extractNodeFunc func(node Node) Node) {
	RunNodeRestoreTestWithFlags(c, nodeTestCases, template, extractNodeFunc, DefaultRestoreFlags)
}

func RunNodeRestoreTestWithFlags(c *C, nodeTestCases []NodeRestoreTestCase, template string, extractNodeFunc func(node Node) Node, flags RestoreFlags) {
	parser := parser.New()
	parser.EnableWindowFunc(true)
	for _, testCase := range nodeTestCases {
		sourceSQL := fmt.Sprintf(template, testCase.sourceSQL)
		expectSQL := fmt.Sprintf(template, testCase.expectSQL)
		stmt, err := parser.ParseOneStmt(sourceSQL, "", "")
		comment := Commentf("source %#v", testCase)
		c.Assert(err, IsNil, comment)
		var sb strings.Builder
		err = extractNodeFunc(stmt).Restore(NewRestoreCtx(flags, &sb))
		c.Assert(err, IsNil, comment)
		restoreSql := fmt.Sprintf(template, sb.String())
		comment = Commentf("source %#v; restore %v", testCase, restoreSql)
		c.Assert(restoreSql, Equals, expectSQL, comment)
		stmt2, err := parser.ParseOneStmt(restoreSql, "", "")
		c.Assert(err, IsNil, comment)
		CleanNodeText(stmt)
		CleanNodeText(stmt2)
		c.Assert(stmt2, DeepEquals, stmt, comment)
	}
}
