# Static integration-parser porting queue

`integration_parser_queue` turns the checked Go parser oracle into a
deterministic Rust work queue. It is a prioritization artifact, not a parity
claim: every input still appears in exactly one current Rust outcome category.

```sh
cd rust
cargo run -j 12 -p difftest --bin integration_parser_queue -- --check
```

The command first validates the shape and ordering of both checked TSV inputs:
the source fixture inventory and its static Go parser golden. It then runs only
the Rust parser. No Go command or Go binary is started by `--check`.

The report has two TSV sections:

- `summary` gives the exact count for each mutually exclusive replay outcome
  used by `integration_parser_diff`.
- `queue` lists every non-match group, ordered by descending input count, then
  outcome and normalized leading SQL shape. Each group includes up to three
  source-ordered examples with fixture path, original line range, boundary,
  and exact escaped SQL.

Leading shapes intentionally keep high-value statement families together:
`CREATE TABLE`, `ALTER TABLE`, `START TRANSACTION`, and similar object/action
pairs retain two words; ordinary statements such as `SELECT`, `INSERT`, and
`SET` retain their leading keyword. Identifiers and literals never become a
queue key, which keeps the task list stable when fixture data changes.

`integration_parser_golden --write` is the only Go-dependent refresh step. If
fixtures change, refresh the inventory and golden first; the queue will reject
their stale ordering or escaped payload before reporting parser work.
