package ast_test

import (
	"bytes"
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
)

var _ = Suite(&testAstFormatSuite{})

type testAstFormatSuite struct {
}

func getDefaultCharsetAndCollate() (string, string) {
	return "utf8", "utf8_bin"
}

func (ts *testAstFormatSuite) TestAstFormat(c *C) {
	var testcases = []struct {
		input  string
		output string
	}{
		// Literals.
		{`null`, `NULL`},
		{`true`, `TRUE`},
		{`350`, `350`},
		{`001e-12`, `1e-12`}, // Float.
		{`345.678`, `345.678`},
		{`00.0001000`, `0.0001000`}, // Decimal.
		{`null`, `NULL`},
		{`"Hello, world"`, `"Hello, world"`},
		{`'Hello, world'`, `"Hello, world"`},
		{`'Hello, "world"'`, `"Hello, \"world\""`},
		{`_utf8'你好'`, `"你好"`},
		{`x'bcde'`, "x'bcde'"},
		{`x''`, "x''"},
		{`x'0035'`, "x'0035'"}, // Shouldn't trim leading zero.
		{`b'00111111'`, `b'111111'`},
		{`time'10:10:10.123'`, ast.TimeLiteral + `("10:10:10.123")`},
		{`timestamp'1999-01-01 10:0:0.123'`, ast.TimestampLiteral + `("1999-01-01 10:0:0.123")`},
		{`date '1700-01-01'`, ast.DateLiteral + `("1700-01-01")`},

		// Expressions.
		{`f between 30 and 50`, "`f` BETWEEN 30 AND 50"},
		{`f not between 30 and 50`, "`f` NOT BETWEEN 30 AND 50"},
		{`345 + "  hello  "`, `345 + "  hello  "`},
		{`"hello world"    >=    'hello world'`, `"hello world" >= "hello world"`},
		{`case 3 when 1 then false else true end`, `CASE 3 WHEN 1 THEN FALSE ELSE TRUE END`},
		{`database.table.column`, "`database`.`table`.`column`"}, // ColumnNameExpr
		{`3 is null`, `3 IS NULL`},
		{`3 is not null`, `3 IS NOT NULL`},
		{`3 is true`, `3 IS TRUE`},
		{`3 is not true`, `3 IS NOT TRUE`},
		{`3 is false`, `3 IS FALSE`},
		{`  ( x is false  )`, "(`x` IS FALSE)"},
		{`3 in ( a,b,"h",6 )`, "3 IN (`a`,`b`,\"h\",6)"},
		{`3 not in ( a,b,"h",6 )`, "3 NOT IN (`a`,`b`,\"h\",6)"},
		{`"abc" like '%b%'`, `"abc" LIKE "%b%"`},
		{`"abc" not like '%b%'`, `"abc" NOT LIKE "%b%"`},
		{`"abc" like '%b%' escape '_'`, `"abc" LIKE "%b%" ESCAPE '_'`},
		{`"abc" regexp '.*bc?'`, `"abc" REGEXP ".*bc?"`},
		{`"abc" not regexp '.*bc?'`, `"abc" NOT REGEXP ".*bc?"`},
		{`-  4`, `-4`},
		{`- ( - 4 ) `, `-(-4)`},
		{`a%b`, "`a` % `b`"},
		{`a%b+6`, "`a` % `b` + 6"},
		{`a%(b+6)`, "`a` % (`b` + 6)"},
		// Functions.
		{` json_extract ( a,'$.b',"$.\"c d\"" ) `, "json_extract(`a`, \"$.b\", \"$.\\\"c d\\\"\")"},
		{` length ( a )`, "length(`a`)"},
		{`a -> '$.a'`, "json_extract(`a`, \"$.a\")"},
		{`a.b ->> '$.a'`, "json_unquote(json_extract(`a`.`b`, \"$.a\"))"},
		{`DATE_ADD('1970-01-01', interval 3 second)`, `date_add("1970-01-01", INTERVAL 3 SECOND)`},
		{`TIMESTAMPDIFF(month, '2001-01-01', '2001-02-02 12:03:05.123')`, `timestampdiff(MONTH, "2001-01-01", "2001-02-02 12:03:05.123")`},
		// Cast, Convert and Binary.
		// There should not be spaces between 'cast' and '(' unless 'IGNORE_SPACE' mode is set.
		// see: https://dev.mysql.com/doc/refman/5.7/en/function-resolution.html
		{` cast( a as signed ) `, "CAST(`a` AS SIGNED)"},
		{` cast( a as unsigned integer) `, "CAST(`a` AS UNSIGNED)"},
		{` cast( a as char(3) binary) `, "CAST(`a` AS CHAR(3) BINARY)"},
		{` cast( a as decimal ) `, "CAST(`a` AS DECIMAL(11))"},
		{` cast( a as decimal (3) ) `, "CAST(`a` AS DECIMAL(3))"},
		{` cast( a as decimal (3,3) ) `, "CAST(`a` AS DECIMAL(3, 3))"},
		{` ((case when (c0 = 0) then 0 when (c0 > 0) then (c1 / c0) end)) `, "((CASE WHEN (`c0` = 0) THEN 0 WHEN (`c0` > 0) THEN (`c1` / `c0`) END))"},
		{` convert (a, signed) `, "CONVERT(`a`, SIGNED)"},
		{` binary "hello"`, `BINARY "hello"`},
	}
	for _, tt := range testcases {
		expr := fmt.Sprintf("select %s", tt.input)
		charset, collation := getDefaultCharsetAndCollate()
		stmts, _, err := parser.New().Parse(expr, charset, collation)
		node := stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr
		c.Assert(err, IsNil)

		writer := bytes.NewBufferString("")
		node.Format(writer)
		c.Assert(writer.String(), Equals, tt.output)
	}
}
