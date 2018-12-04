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

package ast

import (
	"strings"

	. "github.com/pingcap/check"
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
