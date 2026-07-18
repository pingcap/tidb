# Parser-ring integration input inventory

The parser ring in the Rust rewrite design must replay every SQL input under
`tests/**/t/*.test`. Before parser parity can be claimed, this generated
inventory makes that source corpus auditable without pretending that an
inventory is a successful port.

```sh
cd rust
cargo run -p difftest --bin integration_parser_inventory -- --write
cargo run -p difftest --bin integration_parser_inventory -- --check
```

`corpus/coverage/integration_parser_inventory.tsv` has one row for each SQL
input that mysql-tester dispatches to TiDB, with its repository-relative
fixture path, first and final source lines, the mysqltest delimiter active for
that input, its boundary kind, and the exact SQL after applying the fixture
runner's per-line trimming. Tabs, line breaks, backslashes, and control
characters in the final SQL column are escaped, so every row remains a single
TSV record.

`corpus/coverage/integration_runner_directive_inventory.tsv` is the companion
five-column record of every recognized client-side runner command:
`source_path`, `source_start_line`, `source_end_line`, normalized `command`,
and the exact runner `payload`. It prevents a command such as `connection
conn1;` from being silently dropped or falsely treated as a TiDB parser gap.
The two inventories are generated and freshness-checked together: every
recognized command is represented by either a dispatched SQL row (`query`) or
a runner-directive row.

## Fixture grammar source

The inventory follows the implementation TiDB actually invokes from
`tests/integrationtest/run-tests.sh`: the pinned
`github.com/pingcap/mysql-tester` revision `f2d90ea9522d30c9a8e8d70cc31c7f016ca2801f`,
specifically `src/main.go`'s `tester.loadQueries` and `src/query.go`'s
`ParseQuery`. `tests/integrationtest2/run-tests.sh` pins the compatible
`github.com/bb7133/mysql-tester` revision
`2148bd9e5299de307244a15ed0047c953a035dc4`; its extra
`backup_and_restore`, `dump_and_import`, and `replication_checkpoint`
directives are likewise omitted as non-SQL control lines.

- Leading/trailing whitespace is trimmed from each physical fixture line.
- The PingCAP runner's `commandMap` is case-insensitive and applies both to
  `--` command forms and to a completed unprefixed input. Thus direct
  `connect (...)`, `connection conn1;`, and `disconnect conn1;` are runner
  directives just like their `--` forms; they are recorded only in the
  directive inventory. `query`/`--query` are the exception: their payload is
  a `Q_QUERY` execution input and is retained in the SQL inventory with
  boundary `directive_query`. A directive before a completed SQL input and an
  unknown `--` directive are rejected, matching the runner's fixture
  discipline. The current corpus has no SQL-bearing `let`; if one is added,
  generation fails until its backtick-expression execution grammar is
  explicitly implemented.
- Both `delimiter X` and `--delimiter X` change the active terminator. Like
  `loadQueries`, if one physical line completes multiple statements, its last
  terminator closes the one multi-statement input instead of fabricating extra
  runner invocations.
- The upstream runner finds terminators with raw `LastIndex`, even when the
  last delimiter sits in a malformed quoted token or trailing SQL comment. The
  inventory records that exact input boundary. It also scans normal SQL syntax
  (quoted tokens and `#`/`-- `/`/* ... */` comments): when the runner's raw
  boundary differs from that lexical boundary, the row is explicitly labeled
  `runner_raw_fallback`. Thus no quoted/comment delimiter is silently treated
  as ordinary SQL syntax, while parser-error inputs and fixture behavior remain
  complete and reproducible.

## Deliberate limits

This is a fixture-input inventory, not a SQL parser and not a parity result.
It does not execute mysqltest control-flow directives, source nested fixture
files, substitute variables, reproduce client-side session state, or decide
which inputs TiDB will reject. The PingCAP integration runner pin and the
bb7133 integrationtest2 fork share the command map except that the fork adds
`backup_and_restore`, `dump_and_import`, and `replication_checkpoint`; all are
recognized by this generator. It also uses MySQL's normal backslash-escape
convention for single/double-quoted literals; a fixture whose active SQL mode
changes that lexical convention needs a parser-ring test and an explicit
scanner extension before its row can be treated as a faithful replay contract.

The checked-in TSV is a hard freshness gate. It does **not** mark any input as
ported, restored, executable, or semantically compatible.
