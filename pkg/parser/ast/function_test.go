package ast_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
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
			"create function myfunc returns int SONAME 'myfunc.so'",
			// > The keyword INT is a synonym for INTEGER
			// https://dev.mysql.com/doc/refman/8.0/en/numeric-types.html
			"CREATE FUNCTION `myfunc` RETURNS INTEGER SONAME 'myfunc.so'",
		},
		{
			"CREATE FUNCTION `myfunc` RETURNS decimal SONAME 'myfunc.so'",
			"CREATE FUNCTION `myfunc` RETURNS DECIMAL SONAME 'myfunc.so'",
		},
		{
			"CREATE FUNCTION `myfunc` RETURNS DEC SONAME 'myfunc.so'",
			// > the keywords DEC and FIXED are synonyms for DECIMAL.
			// https://dev.mysql.com/doc/refman/8.0/en/numeric-types.html
			"CREATE FUNCTION `myfunc` RETURNS DECIMAL SONAME 'myfunc.so'",
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

func TestParseError(t *testing.T) {
	cases := [][2]string{
		{
			"CREATE FUNCTION db.fn RETURNS INTEGER SONAME 'myfunc.so'",
			// loadable function should not have db name.
			// > the unqualified name refers to the loadable function
			// https://dev.mysql.com/doc/refman/8.4/en/function-resolution.html
			//
			// however we are not keeping the error position the same as MySQL for simplicity.
			`line 1 column 19 near ".fn RETURNS INTEGER SONAME 'myfunc.so'"`,
		},
		{
			"CREATE FUNCTION fn RETURNS FLOAT SONAME 'myfunc.so'",
			`line 1 column 32 near "FLOAT SONAME 'myfunc.so'"`,
		},
		{
			"CREATE FUNCTION fn RETURNS SMALLINT SONAME 'myfunc.so'",
			`line 1 column 35 near "SMALLINT SONAME 'myfunc.so'"`,
		},
		{
			"CREATE FUNCTION fn RETURNS FIXED SONAME 'myfunc.so'",
			// although FIXED is a synonym for DECIMAL, this SQL really fails to execute.
			`line 1 column 32 near "FIXED SONAME 'myfunc.so'"`,
		},
		{
			"CREATE FUNCTION fn RETURNS DOUBLE SONAME 'myfunc.so'",
			`line 1 column 33 near "DOUBLE SONAME 'myfunc.so'"`,
		},
		{
			"CREATE FUNCTION fn RETURNS DOUBLE PRECISION SONAME 'myfunc.so'",
			`line 1 column 33 near "DOUBLE PRECISION SONAME 'myfunc.so'"`,
		},
	}

	p := parser.New()
	for _, c := range cases {
		_, _, err := p.Parse(c[0], "", "")
		require.ErrorContains(t, err, c[1])
	}
}
