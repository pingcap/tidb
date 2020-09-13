# Quickstart

This parser is highly compatible with MySQL syntax. You can use it as a library, parse a text SQL into an AST tree, and traverse the AST nodes.

In this example, you will build a project, which can extract all the column names from a text SQL.

## Prerequisites

- [Golang](https://golang.org/dl/) version 1.13 or above. You can follow the instructions in the official [installation page](https://golang.org/doc/install) (check it by `go version`)

## Create a Project

```bash
mkdir colx && cd colx
go mod init colx && touch main.go
```

## Import Dependencies

First of all, you need to use `go get` to fetch the dependencies through git hash. The git hashes are available in [release page](https://github.com/pingcap/parser/releases). Take `v4.0.2` as an example:

```bash
go get -v github.com/pingcap/parser@3a18f1e
```

> **NOTE**
>
> You may want to use advanced API on expressions (a kind of AST node), such as numbers, string literals, booleans, nulls, etc. It is strongly recommended to use the `types` package in TiDB repo with the following command:
>
> ```bash
> go get -v github.com/pingcap/tidb/types/parser_driver@328b6d0
> ```
> and import it in your golang source code:
> ```go
> import _ "github.com/pingcap/tidb/types/parser_driver"
> ```

Your directory should contain the following three files:
```
.
├── go.mod
├── go.sum
└── main.go
```

Now, open `main.go` with your favorite editor, and start coding!

## Parse SQL text

To convert a SQL text to an AST tree, you need to:
1. Use the [`parser.New()`](https://pkg.go.dev/github.com/pingcap/parser?tab=doc#New) function to instantiate a parser, and
2. Invoke the method [`Parse(sql, charset, collation)`](https://pkg.go.dev/github.com/pingcap/parser?tab=doc#Parser.Parse) on the parser.

```go
package main

import (
	"fmt"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/parser/test_driver"
)

func parse(sql string) (*ast.StmtNode, error) {
	p := parser.New()

	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}

	return &stmtNodes[0], nil
}

func main() {
	astNode, err := parse("SELECT a, b FROM t")
	if err != nil {
		fmt.Printf("parse error: %v\n", err.Error())
		return
	}
	fmt.Printf("%v\n", *astNode)
}
```

Test the parser by running the following command:

```bash
go run main.go
```

If the parser runs properly, you should get a result like this:

```
&{{{{SELECT a, b FROM t}}} {[]} 0xc0000a1980 false 0xc00000e7a0 <nil> 0xc0000a19b0 <nil> <nil> [] <nil> <nil> none [] false false 0 <nil>}
```

> **NOTE**
>
> Here are a few things you might want to know:
> - To use a parser, a `parser_driver` is required. It decides how to parse the basic data types in SQL.
>
>   You can use [`github.com/pingcap/parser/test_driver`](https://pkg.go.dev/github.com/pingcap/parser/test_driver) as the `parser_driver` for test. Again, if you need advanced features, please use the `parser_driver` in TiDB (run `go get -v github.com/pingcap/tidb/types/parser_driver@328b6d0` and import it).
> - The instantiated parser object is not goroutine safe. It is better to keep it in a single goroutine.
> - The instantiated parser object is not lightweight. It is better to reuse it if possible.
> - The 2nd and 3rd arguments of [`parser.Parse()`](https://pkg.go.dev/github.com/pingcap/parser?tab=doc#Parser.Parse) are charset and collation respectively. If you pass an empty string into it, a default value is chosen.


## Traverse AST Nodes

Now you get the AST tree root of a SQL statement. It is time to extract the column names by traverse.

Parser implements the interface [`ast.Node`](https://pkg.go.dev/github.com/pingcap/parser/ast?tab=doc#Node) for each kind of AST node, such as SelectStmt, TableName, ColumnName. [`ast.Node`](https://pkg.go.dev/github.com/pingcap/parser/ast?tab=doc#Node) provides a method `Accept(v Visitor) (node Node, ok bool)` to allow any struct that has implemented [`ast.Visitor`](https://pkg.go.dev/github.com/pingcap/parser/ast?tab=doc#Visitor) to traverse itself.

[`ast.Visitor`](https://pkg.go.dev/github.com/pingcap/parser/ast?tab=doc#Visitor) is defined as follows:
```go
type Visitor interface {
	Enter(n Node) (node Node, skipChildren bool)
	Leave(n Node) (node Node, ok bool)
}
```

Now you can define your own visitor, `colX`(columnExtractor):

```go
type colX struct{
	colNames []string
}

func (v *colX) Enter(in ast.Node) (ast.Node, bool) {
	if name, ok := in.(*ast.ColumnName); ok {
		v.colNames = append(v.colNames, name.Name.O)
	}
	return in, false
}

func (v *colX) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
```

Finally, wrap `colX` in a simple function:

```go
func extract(rootNode *ast.StmtNode) []string {
	v := &colX{}
	(*rootNode).Accept(v)
	return v.colNames
}
```

And slightly modify the main function:

```go
func main() {
	if len(os.Args) != 2 {
		fmt.Println("usage: colx 'SQL statement'")
		return
	}
	sql := os.Args[1]
	astNode, err := parse(sql)
	if err != nil {
		fmt.Printf("parse error: %v\n", err.Error())
		return
	}
	fmt.Printf("%v\n", extract(astNode))
}
```

Test your program:

```bash
go build && ./colx 'select a, b from t'
```

```
[a b]
```

You can also try a different SQL statement as an input. For example:

```console
$ ./colx 'SELECT a, b FROM t GROUP BY (a, b) HAVING a > c ORDER BY b'
[a b a b a c b]

If necessary, you can deduplicate by yourself.

$ ./colx 'SELECT a, b FROM t/invalid_str'
parse error: line 1 column 19 near "/invalid_str"
```

Enjoy!
