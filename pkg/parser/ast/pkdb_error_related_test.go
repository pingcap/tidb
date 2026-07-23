package ast_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestSingalVisitorCover(t *testing.T) {
	stmts := []ast.Node{
		&ast.Signal{},
		&ast.Signal{},
	}
	for _, v := range stmts {
		v.Accept(visitor{})
		v.Accept(visitor1{})
	}
}

func TestSignal(t *testing.T) {
	p := parser.New()
	testcases := []string{"signal SQLSTATE '000000'", "signal SQLSTATE value '000000'",
		"signal SQLSTATE '000000' set CLASS_ORIGIN = a",
		"signal SQLSTATE '000000' set CLASS_ORIGIN = 'xxxxx'",
		"signal SQLSTATE '000000' set CLASS_ORIGIN =  @a",
		"signal SQLSTATE '000000' set SUBCLASS_ORIGIN =  @a",
		"signal SQLSTATE '000000' set CONSTRAINT_CATALOG =  @a",
		"signal SQLSTATE '000000' set CONSTRAINT_SCHEMA =  @a",
		"signal SQLSTATE '000000' set CONSTRAINT_NAME =  @a",
		"signal SQLSTATE '000000' set CATALOG_NAME =  @a",
		"signal SQLSTATE '000000' set SCHEMA_NAME =  @a",
		"signal SQLSTATE '000000' set TABLE_NAME =  @a",
		"signal SQLSTATE '000000' set COLUMN_NAME =  @a",
		"signal SQLSTATE '000000' set CURSOR_NAME =  @a",
		"signal SQLSTATE '000000' set MESSAGE_TEXT =  @a",
		"signal SQLSTATE '000000' set MYSQL_ERRNO =  @a",
		"signal SQLSTATE '000000' set MYSQL_ERRNO =  @a,COLUMN_NAME =  @a,CONSTRAINT_NAME =  @a",
		"signal SQLSTATE '000000' set MYSQL_ERRNO =  @a,COLUMN_NAME =  @a,CONSTRAINT_NAME =  @a,CLASS_ORIGIN = @a,SUBCLASS_ORIGIN=@a,CONSTRAINT_CATALOG=@a,CONSTRAINT_SCHEMA=@a,CATALOG_NAME=@a,SCHEMA_NAME=@a,TABLE_NAME=@a,CURSOR_NAME=@a,MESSAGE_TEXT=@a",
	}
	for _, testcase := range testcases {
		stmt, _, err := p.Parse(testcase, "", "")
		if err != nil {
			fmt.Println(testcase)
		}
		require.NoError(t, err)
		_, ok := stmt[0].(*ast.Signal)
		require.True(t, ok, testcase)
	}
}

func TestSignalRestore(t *testing.T) {
	testCases := []NodeRestoreTestCase{
		{
			"SIGNAL SQLSTATE '000000'",
			"SIGNAL SQLSTATE '000000'",
		},
		{
			"SIGNAL SQLSTATE '000000' SET CLASS_ORIGIN = @`a`",
			"SIGNAL SQLSTATE '000000' SET CLASS_ORIGIN = @`a`",
		},
		{
			"SIGNAL SQLSTATE '000000' SET CLASS_ORIGIN = `a`",
			"SIGNAL SQLSTATE '000000' SET CLASS_ORIGIN = `a`",
		},
		{
			"SIGNAL SQLSTATE '000000' SET CLASS_ORIGIN = _UTF8MB4'xxxx'",
			"SIGNAL SQLSTATE '000000' SET CLASS_ORIGIN = _UTF8MB4'xxxx'",
		},
		{
			"SIGNAL SQLSTATE '000000' SET CLASS_ORIGIN = 1",
			"SIGNAL SQLSTATE '000000' SET CLASS_ORIGIN = 1",
		},
		{
			"SIGNAL SQLSTATE '000000' SET CLASS_ORIGIN = 1, MESSAGE_TEXT = _UTF8MB4'xxxx'",
			"SIGNAL SQLSTATE '000000' SET CLASS_ORIGIN = 1, MESSAGE_TEXT = _UTF8MB4'xxxx'",
		},
		{
			"SIGNAL SQLSTATE '000000' SET MYSQL_ERRNO = @`a`, COLUMN_NAME = @`a`, CONSTRAINT_NAME = @`a`, CLASS_ORIGIN = @`a`, SUBCLASS_ORIGIN = @`a`, CONSTRAINT_CATALOG = @`a`, CONSTRAINT_SCHEMA = @`a`, CATALOG_NAME = @`a`, SCHEMA_NAME = @`a`, TABLE_NAME = @`a`, CURSOR_NAME = @`a`, MESSAGE_TEXT = @`a`",
			"SIGNAL SQLSTATE '000000' SET MYSQL_ERRNO = @`a`, COLUMN_NAME = @`a`, CONSTRAINT_NAME = @`a`, CLASS_ORIGIN = @`a`, SUBCLASS_ORIGIN = @`a`, CONSTRAINT_CATALOG = @`a`, CONSTRAINT_SCHEMA = @`a`, CATALOG_NAME = @`a`, SCHEMA_NAME = @`a`, TABLE_NAME = @`a`, CURSOR_NAME = @`a`, MESSAGE_TEXT = @`a`",
		},
	}
	extractNodeFunc := func(node ast.Node) ast.Node {
		return node.(*ast.Signal)
	}
	runNodeRestoreTest(t, testCases, "%s", extractNodeFunc)
}

func TestGetDiagnostics(t *testing.T) {
	p := parser.New()
	testcases := []string{"Get DIAGNOSTICS a = NUMBER", "Get DIAGNOSTICS @a = NUMBER", "Get DIAGNOSTICS @a = ROW_COUNT",
		"Get CURRENT DIAGNOSTICS a = NUMBER", "Get STACKED DIAGNOSTICS a = NUMBER", "Get STACKED DIAGNOSTICS a = NUMBER,b = ROW_COUNT",
		"Get DIAGNOSTICS CONDITION a a = CLASS_ORIGIN",
		"Get DIAGNOSTICS CONDITION @a a = CLASS_ORIGIN, b = MESSAGE_TEXT", "Get DIAGNOSTICS CONDITION @a a = CLASS_ORIGIN, b = COLUMN_NAME",
	}
	for _, testcase := range testcases {
		stmt, _, err := p.Parse(testcase, "", "")
		if err != nil {
			fmt.Println(testcase)
		}
		require.NoError(t, err)
		_, ok := stmt[0].(*ast.GetDiagnosticsStmt)
		require.True(t, ok, testcase)
	}
}

func TestGetDiagnosticsRestore(t *testing.T) {
	testCases := []NodeRestoreTestCase{
		{
			"GET CURRENT DIAGNOSTICS `a` = NUMBER",
			"GET CURRENT DIAGNOSTICS `a` = NUMBER",
		},
		{
			"GET STACKED DIAGNOSTICS `a` = NUMBER",
			"GET STACKED DIAGNOSTICS `a` = NUMBER",
		},
		{
			"GET STACKED DIAGNOSTICS `a` = ROW_COUNT",
			"GET STACKED DIAGNOSTICS `a` = ROW_COUNT",
		},
		{
			"GET STACKED DIAGNOSTICS `a` = ROW_COUNT, `b` = NUMBER",
			"GET STACKED DIAGNOSTICS `a` = ROW_COUNT, `b` = NUMBER",
		},
		{
			"GET STACKED DIAGNOSTICS CONDITION @`a` `a` = CLASS_ORIGIN",
			"GET STACKED DIAGNOSTICS CONDITION @`a` `a` = CLASS_ORIGIN",
		},
		{
			"GET STACKED DIAGNOSTICS CONDITION @`a` `a` = CLASS_ORIGIN, @`b` = COLUMN_NAME",
			"GET STACKED DIAGNOSTICS CONDITION @`a` `a` = CLASS_ORIGIN, @`b` = COLUMN_NAME",
		},
	}
	extractNodeFunc := func(node ast.Node) ast.Node {
		return node.(*ast.GetDiagnosticsStmt)
	}
	runNodeRestoreTest(t, testCases, "%s", extractNodeFunc)
}
