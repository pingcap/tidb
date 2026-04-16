// Copyright 2025 PingCAP, Inc.

package ast_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestTrigger(t *testing.T) {
	p := parser.New()
	testcases := []string{
		"CREATE TRIGGER trig1 BEFORE INSERT ON t1 FOR EACH ROW SELECT 1;",
		"CREATE TRIGGER IF NOT EXISTS trig1 BEFORE INSERT ON t1 FOR EACH ROW SELECT 1;",
		"CREATE DEFINER = CURRENT_USER TRIGGER trig1 BEFORE INSERT ON t1 FOR EACH ROW SELECT 1;",
		"CREATE DEFINER = `root`@`%` TRIGGER trig1 AFTER UPDATE ON t1 FOR EACH ROW SELECT 1;",
		"CREATE TRIGGER trig1 BEFORE DELETE ON t1 FOR EACH ROW SELECT 1;",
		"CREATE TRIGGER trig1 AFTER INSERT ON t1 FOR EACH ROW BEGIN SELECT 1; END;",
		"CREATE TRIGGER trig1 BEFORE UPDATE ON t1 FOR EACH ROW FOLLOWS trig2 SELECT 1;",
		"CREATE TRIGGER trig1 AFTER INSERT ON t1 FOR EACH ROW PRECEDES trig2 SELECT 1;",

		"CREATE TRIGGER trig1 BEFORE INSERT ON t1 FOR EACH ROW SET @x = 1;",
	}
	for _, testcase := range testcases {
		stmt, _, err := p.Parse(testcase, "", "")
		if err != nil {
			fmt.Println(testcase)
		}
		require.NoError(t, err)
		_, ok := stmt[0].(*ast.CreateTriggerStmt)
		require.True(t, ok, testcase)
	}
}

func TestShowCreateTrigger(t *testing.T) {
	p := parser.New()
	stmt, _, err := p.Parse("SHOW CREATE TRIGGER trig1", "", "")
	require.NoError(t, err)
	showStmt, ok := stmt[0].(*ast.ShowStmt)
	require.True(t, ok)
	require.Equal(t, ast.ShowStmtType(ast.ShowCreateTrigger), showStmt.Tp)
	require.Equal(t, "trig1", showStmt.Trigger.Name.O)
}

func TestDropTrigger(t *testing.T) {
	p := parser.New()
	testcases := []string{
		"DROP TRIGGER trig1",
		"DROP TRIGGER IF EXISTS trig1",
		"DROP TRIGGER `db`.`trig1`",
	}
	for _, testcase := range testcases {
		stmt, _, err := p.Parse(testcase, "", "")
		require.NoError(t, err)
		_, ok := stmt[0].(*ast.DropTriggerStmt)
		require.True(t, ok, testcase)
	}
}

func TestTriggerRestore(t *testing.T) {
	testCases := []NodeRestoreTestCase{
		{
			"CREATE TRIGGER `trig1` BEFORE INSERT ON `t1` FOR EACH ROW SELECT 1",
			"CREATE DEFINER = CURRENT_USER TRIGGER `trig1` BEFORE INSERT ON `t1` FOR EACH ROW SELECT 1",
		},
		{
			"CREATE DEFINER = CURRENT_USER TRIGGER `trig1` BEFORE INSERT ON `t1` FOR EACH ROW SELECT 1",
			"CREATE DEFINER = CURRENT_USER TRIGGER `trig1` BEFORE INSERT ON `t1` FOR EACH ROW SELECT 1",
		},
		{
			"CREATE TRIGGER `trig1` AFTER UPDATE ON `t1` FOR EACH ROW SELECT 1",
			"CREATE DEFINER = CURRENT_USER TRIGGER `trig1` AFTER UPDATE ON `t1` FOR EACH ROW SELECT 1",
		},
		{
			"CREATE TRIGGER `trig1` BEFORE DELETE ON `t1` FOR EACH ROW SELECT 1",
			"CREATE DEFINER = CURRENT_USER TRIGGER `trig1` BEFORE DELETE ON `t1` FOR EACH ROW SELECT 1",
		},
		{
			"CREATE TRIGGER `trig1` BEFORE INSERT ON `t1` FOR EACH ROW FOLLOWS `trig2` SELECT 1",
			"CREATE DEFINER = CURRENT_USER TRIGGER `trig1` BEFORE INSERT ON `t1` FOR EACH ROW FOLLOWS `trig2` SELECT 1",
		},
		{
			"CREATE TRIGGER `trig1` BEFORE INSERT ON `t1` FOR EACH ROW PRECEDES `trig2` SELECT 1",
			"CREATE DEFINER = CURRENT_USER TRIGGER `trig1` BEFORE INSERT ON `t1` FOR EACH ROW PRECEDES `trig2` SELECT 1",
		},
	}
	extractNodeFunc := func(node ast.Node) ast.Node {
		return node.(*ast.CreateTriggerStmt)
	}
	runNodeRestoreTest(t, testCases, "%s", extractNodeFunc)
}

func TestDropTriggerRestore(t *testing.T) {
	testCases := []NodeRestoreTestCase{
		{
			"DROP TRIGGER `trig1`",
			"DROP TRIGGER `trig1`",
		},
		{
			"DROP TRIGGER IF EXISTS `trig1`",
			"DROP TRIGGER IF EXISTS `trig1`",
		},
	}
	extractNodeFunc := func(node ast.Node) ast.Node {
		return node.(*ast.DropTriggerStmt)
	}
	runNodeRestoreTest(t, testCases, "%s", extractNodeFunc)
}
