# Parser

[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/parser)](https://goreportcard.com/report/github.com/pingcap/parser) [![CircleCI Status](https://circleci.com/gh/pingcap/parser.svg?style=shield)](https://circleci.com/gh/pingcap/parser) [![GoDoc](https://godoc.org/github.com/pingcap/parser?status.svg)](https://godoc.org/github.com/pingcap/parser)
[![codecov](https://codecov.io/gh/pingcap/parser/branch/master/graph/badge.svg)](https://codecov.io/gh/pingcap/parser)

TiDB SQL Parser

## How to use it

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
