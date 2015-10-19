package parser

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testParserSuite{})

var _ = Suite(&testParserSuite{})

type testParserSuite struct {
}

func (s *testParserSuite) TestSimple(c *C) {
	// Testcase for unreserved keywords
	unreservedKws := []string{
		"auto_increment", "after", "begin", "bit", "bool", "boolean", "charset", "columns", "commit",
		"date", "datetime", "deallocate", "do", "end", "engine", "engines", "execute", "first", "full",
		"local", "names", "offset", "password", "prepare", "quick", "rollback", "session", "signed",
		"start", "global", "tables", "text", "time", "timestamp", "transaction", "truncate", "unknown",
		"value", "warnings", "year", "now", "substring", "mode", "any", "some", "user", "identified",
		"collation", "comment", "avg_row_length", "checksum", "compression", "connection", "key_block_size",
		"max_rows", "min_rows", "national", "row", "quarter", "escape",
	}
	for _, kw := range unreservedKws {
		src := fmt.Sprintf("SELECT %s FROM tbl;", kw)
		l := NewLexer(src)
		c.Assert(yyParse(l), Equals, 0)
		c.Assert(l.errs, HasLen, 0, Commentf("source %s", src))
	}

	// Testcase for prepared statement
	src := "SELECT id+?, id+? from t;"
	l := NewLexer(src)
	c.Assert(yyParse(l), Equals, 0)
	c.Assert(len(l.Stmts()), Equals, 1)

	// Testcase for -- Comment and unary -- operator
	src = "CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED); -- foo\nSelect --1 from foo;"
	l = NewLexer(src)
	c.Assert(yyParse(l), Equals, 0)
	c.Assert(len(l.Stmts()), Equals, 2)

	// Testcase for CONVERT(expr,type)
	src = "SELECT CONVERT('111', SIGNED);"
	l = NewLexer(src)
	c.Assert(yyParse(l), Equals, 0)
	st := l.Stmts()[0]
	ss, ok := st.(*ast.SelectStmt)
	c.Assert(ok, IsTrue)
	c.Assert(len(ss.Fields), Equals, 1)
	cv, ok := ss.Fields[0].Expr.(*ast.FuncCastExpr)
	c.Assert(ok, IsTrue)
	c.Assert(cv.FunctionType, Equals, ast.CastConvertFunction)

	// For query start with comment
	srcs := []string{
		"/* some comments */ SELECT CONVERT('111', SIGNED) ;",
		"/* some comments */ /*comment*/ SELECT CONVERT('111', SIGNED) ;",
		"SELECT /*comment*/ CONVERT('111', SIGNED) ;",
		"SELECT CONVERT('111', /*comment*/ SIGNED) ;",
		"SELECT CONVERT('111', SIGNED) /*comment*/;",
	}
	for _, src := range srcs {
		l = NewLexer(src)
		c.Assert(yyParse(l), Equals, 0)
		st = l.Stmts()[0]
		ss, ok = st.(*ast.SelectStmt)
		c.Assert(ok, IsTrue)
	}
}
