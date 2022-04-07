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
	"testing"

	"github.com/pingcap/tidb/parser"
	. "github.com/pingcap/tidb/parser/ast"
	. "github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/test_driver"
	"github.com/stretchr/testify/require"
)

func TestCacheable(t *testing.T) {
	// test non-SelectStmt
	var stmt Node = &DeleteStmt{}
	require.False(t, IsReadOnly(stmt))

	stmt = &InsertStmt{}
	require.False(t, IsReadOnly(stmt))

	stmt = &UpdateStmt{}
	require.False(t, IsReadOnly(stmt))

	stmt = &ExplainStmt{}
	require.True(t, IsReadOnly(stmt))

	stmt = &ExplainStmt{}
	require.True(t, IsReadOnly(stmt))

	stmt = &DoStmt{}
	require.True(t, IsReadOnly(stmt))

	stmt = &ExplainStmt{
		Stmt: &InsertStmt{},
	}
	require.True(t, IsReadOnly(stmt))

	stmt = &ExplainStmt{
		Analyze: true,
		Stmt:    &InsertStmt{},
	}
	require.False(t, IsReadOnly(stmt))

	stmt = &ExplainStmt{
		Stmt: &SelectStmt{},
	}
	require.True(t, IsReadOnly(stmt))

	stmt = &ExplainStmt{
		Analyze: true,
		Stmt:    &SelectStmt{},
	}
	require.True(t, IsReadOnly(stmt))

	stmt = &ShowStmt{}
	require.True(t, IsReadOnly(stmt))

	stmt = &ShowStmt{}
	require.True(t, IsReadOnly(stmt))
}

func TestUnionReadOnly(t *testing.T) {
	selectReadOnly := &SelectStmt{}
	selectForUpdate := &SelectStmt{
		LockInfo: &SelectLockInfo{LockType: SelectLockForUpdate},
	}
	selectForUpdateNoWait := &SelectStmt{
		LockInfo: &SelectLockInfo{LockType: SelectLockForUpdateNoWait},
	}

	setOprStmt := &SetOprStmt{
		SelectList: &SetOprSelectList{
			Selects: []Node{selectReadOnly, selectReadOnly},
		},
	}
	require.True(t, IsReadOnly(setOprStmt))

	setOprStmt.SelectList.Selects = []Node{selectReadOnly, selectReadOnly, selectReadOnly}
	require.True(t, IsReadOnly(setOprStmt))

	setOprStmt.SelectList.Selects = []Node{selectReadOnly, selectForUpdate}
	require.False(t, IsReadOnly(setOprStmt))

	setOprStmt.SelectList.Selects = []Node{selectReadOnly, selectForUpdateNoWait}
	require.False(t, IsReadOnly(setOprStmt))

	setOprStmt.SelectList.Selects = []Node{selectForUpdate, selectForUpdateNoWait}
	require.False(t, IsReadOnly(setOprStmt))

	setOprStmt.SelectList.Selects = []Node{selectReadOnly, selectForUpdate, selectForUpdateNoWait}
	require.False(t, IsReadOnly(setOprStmt))
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
	in.SetText(nil, "")
	in.SetOriginTextPosition(0)
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
			node.Args[1].(*test_driver.ValueExpr).Datum.SetBytes(nil)
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
	case *Join:
		node.ExplicitParens = false
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

func runNodeRestoreTest(t *testing.T, nodeTestCases []NodeRestoreTestCase, template string, extractNodeFunc func(node Node) Node) {
	runNodeRestoreTestWithFlags(t, nodeTestCases, template, extractNodeFunc, DefaultRestoreFlags)
}

func runNodeRestoreTestWithFlags(t *testing.T, nodeTestCases []NodeRestoreTestCase, template string, extractNodeFunc func(node Node) Node, flags RestoreFlags) {
	p := parser.New()
	p.EnableWindowFunc(true)
	for _, testCase := range nodeTestCases {
		sourceSQL := fmt.Sprintf(template, testCase.sourceSQL)
		expectSQL := fmt.Sprintf(template, testCase.expectSQL)
		stmt, err := p.ParseOneStmt(sourceSQL, "", "")
		comment := fmt.Sprintf("source %#v", testCase)
		require.NoError(t, err, comment)
		var sb strings.Builder
		err = extractNodeFunc(stmt).Restore(NewRestoreCtx(flags, &sb))
		require.NoError(t, err, comment)
		restoreSql := fmt.Sprintf(template, sb.String())
		comment = fmt.Sprintf("source %#v; restore %v", testCase, restoreSql)
		require.Equal(t, expectSQL, restoreSql, comment)
		stmt2, err := p.ParseOneStmt(restoreSql, "", "")
		require.NoError(t, err, comment)
		CleanNodeText(stmt)
		CleanNodeText(stmt2)
		require.Equal(t, stmt, stmt2, comment)
	}
}

// runNodeRestoreTestWithFlagsStmtChange likes runNodeRestoreTestWithFlags but not check if the ASTs are same.
// Sometimes the AST are different and it's expected.
func runNodeRestoreTestWithFlagsStmtChange(t *testing.T, nodeTestCases []NodeRestoreTestCase, template string, extractNodeFunc func(node Node) Node, flags RestoreFlags) {
	p := parser.New()
	p.EnableWindowFunc(true)
	for _, testCase := range nodeTestCases {
		sourceSQL := fmt.Sprintf(template, testCase.sourceSQL)
		expectSQL := fmt.Sprintf(template, testCase.expectSQL)
		stmt, err := p.ParseOneStmt(sourceSQL, "", "")
		comment := fmt.Sprintf("source %#v", testCase)
		require.NoError(t, err, comment)
		var sb strings.Builder
		err = extractNodeFunc(stmt).Restore(NewRestoreCtx(flags, &sb))
		require.NoError(t, err, comment)
		restoreSql := fmt.Sprintf(template, sb.String())
		comment = fmt.Sprintf("source %#v; restore %v", testCase, restoreSql)
		require.Equal(t, expectSQL, restoreSql, comment)
	}
}
