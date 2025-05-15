package ast_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

func TestLoadableFunctionVisitorCover(t *testing.T) {
	stmts := []ast.Node{
		&ast.CreateLoadableFunctionStmt{},
		&ast.DropFunctionStmt{},
	}
	for _, v := range stmts {
		v.Accept(visitor{})
		v.Accept(visitor1{})
	}
}

func TestFunctionRestore(t *testing.T) {
	testCases := []NodeRestoreTestCase{
		{
			"CREATE FUNCTION `myfunc` RETURNS INTEGER SONAME 'myfunc.so'",
			"CREATE FUNCTION `myfunc` RETURNS INTEGER SONAME 'myfunc.so'",
		},
		{
			"CREATE AGGREGATE FUNCTION `myagg` RETURNS REAL SONAME 'myagg.so'",
			"CREATE AGGREGATE FUNCTION `myagg` RETURNS REAL SONAME 'myagg.so'",
		},
		{
			"CREATE FUNCTION IF NOT EXISTS `myfunc` RETURNS STRING SONAME 'myfunc.so'",
			"CREATE FUNCTION IF NOT EXISTS `myfunc` RETURNS STRING SONAME 'myfunc.so'",
		},
		{
			"CREATE FUNCTION IF NOT EXISTS mydb.`myfunc` RETURNS decimal SONAME 'myfunc.so'",
			"CREATE FUNCTION IF NOT EXISTS `mydb`.`myfunc` RETURNS DECIMAL SONAME 'myfunc.so'",
		},
		{
			"DROP FUNCTION `myfunc`",
			"DROP FUNCTION `myfunc`",
		},
		{
			"DROP FUNCTION IF EXISTS mydb.myfunc",
			"DROP FUNCTION IF EXISTS `mydb`.`myfunc`",
		},
	}
	extractNodeFunc := func(node ast.Node) ast.Node {
		switch n := node.(type) {
		case *ast.CreateLoadableFunctionStmt:
			return n
		case *ast.DropFunctionStmt:
			return n
		default:
			return nil
		}
	}
	runNodeRestoreTest(t, testCases, "%s", extractNodeFunc)
}
