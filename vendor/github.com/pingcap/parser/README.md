# Parser

[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/parser)](https://goreportcard.com/report/github.com/pingcap/parser) [![CircleCI Status](https://circleci.com/gh/pingcap/parser.svg?style=shield)](https://circleci.com/gh/pingcap/parser) [![GoDoc](https://godoc.org/github.com/pingcap/parser?status.svg)](https://godoc.org/github.com/pingcap/parser)

TiDB SQL Parser

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

1. In your TiDB repository, modify the `go.mod` file, remove `github.com/pingcap/parser` from the `require` instruction, and add a new line at the end of the file like this:

    ```
    replace github.com/pingcap/parser => github.com/your-repo/parser v0.0.0-20181102150703-4acd198f5092
    ```

    This change tells TiDB to use the modified parser from your repository. You can just use below command to replace the dependent parser version:

    ```
    GO111MODULE=on go mod edit -replace github.com/pingcap/parser=github.com/your-repo/parser@your-branch
    ```

2. You can get correct version information by running this command in your TiDB directory:

    ```
    GO111MODULE=on go get -u github.com/your-repo/parser@master
    ```

    If some error is reported, you can ignore it and still edit the `go.mod` file manually.

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
