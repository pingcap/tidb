# Quick Start

This is an example showing how to parse a text SQL into an AST tree.

```go
import (
	"fmt"
	"github.com/pingcap/parser"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

func example() {
	p := parser.New()

	stmtNodes, _, err := p.Parse("select * from tbl where id = 1", "", "")

	fmt.Println(stmtNodes[0], err)
}
```

Let's explain the details line by line.

## Import driver

```go
import _ "github.com/pingcap/tidb/types/parser_driver"
```

This means we are using the parser driver provided by TiDB, which decides how to parse the basic data types in SQL, such as numbers, string literals, booleans, nulls, etc.

We can also customize our own driver.

## Create instance
```go
p := parser.New()
```

Instantiate a parser.

**note:** it is **NOT** goroutine safe, we should try to keep it in a single goroutine or synchronize in a multi-thread environment. Moreover, the parser is not lightweight, it is better to reuse it if possible.

## Parse text SQL
```go
stmtNodes, _, err := p.Parse("select * from tbl where id = 1", "", "")
```

The signiture of `Parser.Parse` is
```go
func (parser *Parser) Parse(sql, charset, collation string) (stmt []ast.StmtNode, warns []error, err error)
```

Here we pass an empty string to `charset` and `collation` to let the parser choose the default values for us.

The type of `stmtNodes` is `[]ast.StmtNode`, which forms multiple AST trees. If we want to parse a single SQL statement, use `Parser.ParseOneStmt()` instead.

```go
fmt.Println(stmtNodes[0], err)
```

Since the SQL we provided above is valid, now we should have a syntax tree parsed from `select * from tbl where id = 1`.
