# Parser - A MySQL Compatible SQL Parser

[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/parser)](https://goreportcard.com/report/github.com/pingcap/parser)
[![CircleCI Status](https://circleci.com/gh/pingcap/parser.svg?style=shield)](https://circleci.com/gh/pingcap/parser)
[![GoDoc](https://godoc.org/github.com/pingcap/parser?status.svg)](https://godoc.org/github.com/pingcap/parser)
[![codecov](https://codecov.io/gh/pingcap/parser/branch/master/graph/badge.svg)](https://codecov.io/gh/pingcap/parser)

The goal of this project is to build a Golang parser that is fully compatible with MySQL syntax, easy to extend, and high performance. Currently, features supported by parser are as follows:

- Highly compatible with MySQL: it supports almost all features of MySQL. The main SQL parser is a hand-written recursive-descent parser in [hparser/](https://github.com/pingcap/tidb/blob/master/pkg/parser/hparser/). SQL hints are parsed by a separate hand-written recursive-descent parser in [hintparser.go](https://github.com/pingcap/tidb/blob/master/pkg/parser/hintparser.go).
- Extensible: adding a new syntax requires adding parsing logic to the appropriate file in the `hparser/` package and the corresponding AST node definitions.
- Good performance: the hand-written parser avoids the overhead of table-driven parsing and supports arena allocation for AST nodes.

## How to use it

Please read the [quickstart](https://github.com/pingcap/tidb/blob/master/pkg/parser/docs/quickstart.md).

## Future

- Support more MySQL syntax
- Optimize the code structure, make it easier to extend
- Improve performance and benchmark
- Improve the quality of code and comments

## Getting Help

- [GitHub Issue](https://github.com/pingcap/tidb/issues)
- [Stack Overflow](https://stackoverflow.com/questions/tagged/tidb)
- [User Group (Chinese)](https://asktug.com/)

If you have any questions, feel free to discuss in sig-ddl. Here are the steps to join:
1. Join [TiDB Slack community](https://pingcap.com/tidbslack/), and then
2. Join [sig-ddl Slack channel](https://slack.tidb.io/invite?team=tidb-community&channel=sig-ddl&ref=github_sig).

## Users

These projects use this parser. Please feel free to extend this list if you
found you are one of the users but not listed here:

- [pingcap/tidb](https://github.com/pingcap/tidb)
- [XiaoMi/soar](https://github.com/XiaoMi/soar)
- [XiaoMi/Gaea](https://github.com/XiaoMi/Gaea)
- [sql-machine-learning/sqlflow](https://github.com/sql-machine-learning/sqlflow)
- [nooncall/shazam](https://github.com/nooncall/shazam)
- [bytebase/bytebase](https://github.com/bytebase/bytebase)
- [kyleconroy/sqlc](https://github.com/kyleconroy/sqlc)
- [block/spirit](https://github.com/block/spirit)

## Contributing

Contributions are welcomed and greatly appreciated. See [Contribution Guide](https://github.com/pingcap/community/blob/master/contributors/README.md) for details on submitting patches and the contribution workflow.

## Acknowledgments

Thanks [cznic](https://github.com/cznic) for providing some great open-source tools.

## License
Parser is under the Apache 2.0 license. See the LICENSE file for details.

## More resources

- TiDB documentation

    - [English](https://docs.pingcap.com/tidb/stable)
    - [简体中文](https://docs.pingcap.com/zh/tidb/stable)
    
- TiDB blog

    - [English](https://pingcap.com/blog/)
    - [简体中文](https://pingcap.com/blog-cn/)
