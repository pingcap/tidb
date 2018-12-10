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
}

func (s *testCacheableSuite) TestWriteName(c *C) {
	var sb strings.Builder
	WriteName(&sb, "")
	sb.WriteString(";")
	WriteName(&sb, "abc")
	sb.WriteString(";")
	WriteName(&sb, "ab`c")
	sb.WriteString(";")
	WriteName(&sb, "ab``c")
	sb.WriteString(";")
	WriteName(&sb, "ab` `c")
	sb.WriteString(";")
	c.Assert(sb.String(), Equals, "``;`abc`;`ab``c`;`ab````c`;`ab`` ``c`;")
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
	parser := parser.New()
	for _, testCase := range nodeTestCases {
		sourceSQL := fmt.Sprintf(template, testCase.sourceSQL)
		expectSQL := fmt.Sprintf(template, testCase.expectSQL)
		stmt, err := parser.ParseOneStmt(sourceSQL, "", "")
		comment := Commentf("source %#v", testCase)
		c.Assert(err, IsNil, comment)
		var sb strings.Builder
		err = extractNodeFunc(stmt).Restore(&sb)
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
