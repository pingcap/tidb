# Integration parser differential ring

`corpus/coverage/integration_parser_inventory.tsv` is the source-backed list
of every SQL input found by the integration-test fixture scanner. Its companion
`integration_parser_golden.tsv` records what the production Go parser did with
that exact input: whether `Parser.Parse` accepted it, how many statements it
returned, and the canonical restore of every returned statement.

The golden is a static test-time oracle. The normal Rust test does **not** run
Go, and intentionally reports the current parser subset instead of pretending
that unsupported TiDB syntax is passing:

```sh
cd rust
cargo test -j 12 -p difftest-parser-tests --test integration_parser_diff -- --nocapture
```

It emits the exact totals for Go accepted/rejected inputs and the Rust replay
states (`matched`, `parse_failure`, and `restore_mismatch`, plus explicit
asymmetry counts). It fails if the checked golden no longer names the exact
checked inventory, or if the reviewed `EXPECTED_COUNTS` snapshot in
`tests/integration_parser_diff.rs` changes. A parser coverage increase must
update that snapshot deliberately in the same review; never regenerate this
Go-only oracle to hide a Rust parser outcome change.

## Regeneration

First refresh the source inventory, then capture Go evidence and validate it:

```sh
cd rust
cargo run -p difftest --bin integration_parser_inventory -- --write
cargo run -p difftest --bin integration_parser_inventory -- --check
cargo run -p difftest --bin integration_parser_golden -- --write
cargo run -p difftest --bin integration_parser_golden -- --check
```

`--write` builds `godump` only when its executable is missing or older than
`godump/main.go`, using `go build -p 12`. It sends all inputs through the
`framed-restore` mode; the ordinary `godump` line protocol must not be used for
this ring.

## Wire format

Requests are adjacent byte-counted frames:

```text
@<input-index> <sql-byte-length>\n<exact SQL bytes>
```

Responses have the same property:

```text
@<input-index> <A|P|R> <statement-count> <payload-byte-length>\n<payload bytes>
```

`A` means Go parsed and restored every returned statement, `P` means Go
rejected the input, and `R` means Go parsed but a restore failed. An `A`
payload contains one big-endian `u64` byte length and exact restored SQL bytes
for every returned statement. The payload is intentionally bytes rather than
UTF-8 text: Go restoration can preserve non-UTF-8 literal bytes. No newlines, semicolons, comments, tabs, or
control bytes are delimiters. The checked TSV copies each inventory row's
`boundary` unchanged: in particular, `runner_raw_fallback` remains evidence
that mysql-tester's raw last-delimiter behavior produced the input; it is never
relabeled as a normal lexical SQL boundary.

## Limits

This ring proves parser-source coverage and Go parser behavior only. It does
not execute fixture SQL, reproduce mysql-tester session state, establish
planner/executor parity, or make the Rust parser's current parse failures
acceptable. It directly identifies missing grammar
and restore paths.
