# `pkg/types` `STR_TO_DATE` exhausted-token receipt

Status: bounded Rust parity fix implemented in the isolated worktree and
validated against fetched Go `master`. Go source and Bazel metadata were not
changed.

Comparison source: Go `origin/master` at
`fc7788ff517c3407dc7e000be989ab23e6648211`.

## Inventory completed before editing

The complete temporal owners were rechecked before editing, including every
production file, test/benchmark, parser-driver fixture, generated/platform
variant, and build artifact:

| Tree | Files | Lines |
| --- | ---: | ---: |
| `pkg/types` (including `parser_driver`) | 60 | 28,703 Go lines |
| `rust/crates/tidb-datatype` | 104 | 52,114 Rust source/test/manifest lines plus 8 data/docs artifacts |
| `rust/crates/tidb-expr/src/time_fn` temporal owner | 7 | 7,334 Rust source/test lines |

The Go behavior-bearing files read were `pkg/types/time.go`
(`strToDate`/`mysqlTimeFix`), `pkg/types/time_test.go`,
`pkg/expression/builtin_time.go`, `pkg/expression/builtin_time_test.go`, and
the complete BUILD metadata for those owners. The Rust owner chain was
`tidb-datatype/src/str_to_date.rs` and the live
`tidb-expr/src/time_fn/calendar.rs` evaluator plus their source-derived tests.
No Go, generated, fixture, platform, or build file changed.

## Go behavior restored

Go's recursive `strToDate` records `ctx[token] = 0` and returns success when
the input is exhausted before the remaining format. `mysqlTimeFix` then uses
token presence: `%p` paired with `%H` is invalid, an empty `%p` with a zero
hour is invalid, and an absent `%p` after a valid 12-hour clock is treated as
AM. Rust previously returned from the parser without recording that state, so
the live expression path accepted `%H` + exhausted `%p` and an empty `%p` as
zero-valued results.

Both Rust owners now capture the source token-presence state before stopping.
The `%f` empty-fraction and skip-token paths retain their existing source
behavior; the state change is limited to the meridiem/clock fix contract.

## Focused regressions

- `str_to_date::tests::exhausted_format_tokens_preserve_go_meridiem_fix_state`
  covers `%H` + missing `%p` (NULL), `%h` + missing `%p` (implicit AM), and an
  empty `%p` (NULL).
- `time_fn::tests::str_to_date_exhausted_tokens_preserve_go_meridiem_fix_state`
  pins the same three cases through the live expression evaluator.

All existing datatype and expression `STR_TO_DATE` source vectors continue to
pass, including empty fractions, `%r`/`%T`, Unicode `%.`, and partial-date mode
cases.

## Ready validation

Commands run from `rust/`:

```text
cargo fmt --all
cargo test --offline --locked -p tidb-datatype --lib str_to_date -- --nocapture
cargo test --offline --locked -p tidb-expr --lib str_to_date -- --nocapture
cargo test --offline --locked -p tidb-datatype --all-targets -- --test-threads=1
cargo test --offline --locked -p tidb-expr --all-targets -- --test-threads=1
cargo check --offline --locked -p tidb-datatype -p tidb-expr --all-targets
cargo fmt --all -- --check
git diff --check
cargo clippy --offline --locked -p tidb-datatype -p tidb-expr --all-targets -- -D warnings
```

Results:

- Focused owner suites: PASS (6 datatype tests and 6 expression tests).
- Serialized `tidb-datatype`: PASS (393 unit tests and 63 generated/source
  integration tests; benchmark targets compiled).
- Serialized `tidb-expr`: 1,130 passed, 1 pre-existing external HTTP
  JSON-schema fixture failure, and 125 documented gap tests ignored. The full
  `STR_TO_DATE` source suite and new regression pass.
- Owner compilation, formatting, and whitespace checks: PASS.
- Strict clippy is blocked by pre-existing diagnostics in unrelated generated
  `tidb-proto` output and `tidb-mysql/src/consts.rs:117-120`
  (`map-or-identity`), not by this batch.

## Risks and remaining boundaries

This change covers only format-token exhaustion and its meridiem/clock fix
state. Zero-date SQL-mode flags, invalid-date handling, DST transitions,
numeric-zero parsing, and other temporal findings remain separate boundaries;
T7–T9, T11–T12, T14, and T16 are not claimed by this receipt.
