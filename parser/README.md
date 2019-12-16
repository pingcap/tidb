# Parser

[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/parser)](https://goreportcard.com/report/github.com/pingcap/parser) [![CircleCI Status](https://circleci.com/gh/pingcap/parser.svg?style=shield)](https://circleci.com/gh/pingcap/parser) [![GoDoc](https://godoc.org/github.com/pingcap/parser?status.svg)](https://godoc.org/github.com/pingcap/parser)
[![codecov](https://codecov.io/gh/pingcap/parser/branch/master/graph/badge.svg)](https://codecov.io/gh/pingcap/parser)

TiDB SQL Parser

## How to use it

```go
import (
	"fmt"
	"github.com/pingcap/parser"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

// This example show how to parse a text sql into ast.
func example() {

	// 0. make sure import parser_driver implemented by TiDB(user also can implement own driver by self).
	// and add `import _ "github.com/pingcap/tidb/types/parser_driver"` in the head of file.

	// 1. Create a parser. The parser is NOT goroutine safe and should
	// not be shared among multiple goroutines. However, parser is also
	// heavy, so each goroutine should reuse its own local instance if
	// possible.
	p := parser.New()

	// 2. Parse a text SQL into AST([]ast.StmtNode).
	stmtNodes, _, err := p.Parse("select * from tbl where id = 1", "", "")

	// 3. Use AST to do cool things.
	fmt.Println(stmtNodes[0], err)
}
```

See [https://godoc.org/github.com/pingcap/parser](https://godoc.org/github.com/pingcap/parser)

## How to update parser for TiDB

Assuming that you want to file a PR (pull request) to TiDB, and your PR includes a change in the parser, follow these steps to update the parser in TiDB.

### Step 1: Make changes in your parser repository

Fork this repository to your own account and commit the changes to your repository.

> **Note:**
>
> - Don't forget to run `make test` before you commit!
> - Make sure `parser.go` is updated.

Suppose the forked repository is `https://github.com/your-repo/parser`.

### Step 2: Make your parser changes take effect in TiDB and run CI

1. In your TiDB repository, execute the `replace` instruction to make your parser changes take effect:

    ```
    GO111MODULE=on go mod edit -replace github.com/pingcap/parser=github.com/your-repo/parser@your-branch
    ```

2. `make dev` to run CI in TiDB.

3. File a PR to TiDB.

### Step 3: Merge the PR about the parser to this repository

File a PR to this repository. **Link the related PR in TiDB in your PR description or comment.**

This PR will be reviewed, and if everything goes well, it will be merged.

### Step 4: Update TiDB to use the latest parser

In your TiDB pull request, modify the `go.mod` file manually or use this command:

```
GO111MODULE=on go get -u github.com/pingcap/parser@master
```

Make sure the `replace` instruction is changed back to the `require` instruction and the version is the latest.
