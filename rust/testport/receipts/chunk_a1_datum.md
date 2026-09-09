# Chunk A-1 datum-to-decimal parity receipt

Status: bounded Rust parity fix implemented in the isolated worktree and
validated against the fetched Go master. This receipt does not claim complete
transcreation of `pkg/util/chunk`; it closes the decimal datum storage boundary
and leaves the separately ranked wire/datum-shape items explicit.

Final batch commit: `c59b2bd60e` (pushed to `hparser-integration`).

Comparison source: Go `origin/master` at
`6331b8787b4203a91aafe49ee1dc801ee497bf98`.
Rust base: `hparser-integration` after the preceding UNION batch.

## Inventory completed before editing

The complete Go owner was enumerated before editing, including every
production, unit/benchmark, fixture, generated/platform, and build artifact:

| Tree | Files | Go/Rust lines |
| --- | ---: | ---: |
| `pkg/util/chunk` (`*.go`, `BUILD*`, `*.bzl`) | 29 | 11,261 Go lines |
| `rust/crates/tidb-chunk` (Cargo/source/tests/fixtures) | 46 | 25,129 Rust lines |

Reproducible inventory commands:

```text
find pkg/util/chunk -type f | sort
find pkg/util/chunk -type f -name '*.go' -print0 | xargs -0 wc -l
find rust/crates/tidb-chunk -type f | sort
find rust/crates/tidb-chunk -type f \( -name '*.rs' -o -name 'Cargo.toml' -o -name 'build.rs' \) -print0 | xargs -0 wc -l
```

The behavior-bearing Go artifacts were read before editing:
`chunk.go` (`AppendDatum`, `AppendMyDecimal`), `column.go`
(`AppendMyDecimal`), `mutrow.go` (`SetValue`, `SetDatum`), and their complete
`chunk_test.go`, `column_test.go`, `mutrow_test.go`, plus the codec and
row/container tests that exercise decimal cells. Rust owners read before
editing include `tidb-datatype::decimal`, `tidb-datatype::mydecimal`,
`tidb-chunk::chunk`, `tidb-chunk::mutrow`, raw column layout, codec, row,
allocation, and all unit/integration contract tests.

## Go behavior restored

Go `Datum` already owns a fixed nine-word `MyDecimal`. `Chunk.AppendDatum` and
all `MutRow` datum/value entry points copy that 40-byte value directly; they do
not parse a value-layer decimal or introduce a new overflow panic. Rust's
`Datum::Decimal` is an exact digit-string value and can temporarily exceed the
fixed cell. `Decimal::to_chunk_my_decimal_lossy` now keeps the exact conversion
for representable values and delegates over-wide values to
`MyDecimal::from_string`, preserving Go's ordinary prefix/truncation result
and the leading-zero shape for values below one. `append_datum`,
`MutRow::from_datums`, `set_value`, and `set_datum` all use that boundary.

No wire format, column width, warning channel, or ordinary exact decimal path
was changed.

## Focused regression

`tidb-chunk::chunk::tests::decimal_datum_overflow_uses_go_truncation_without_panicking`
uses a value with ten fractional base-1e9 words. It compares the expected
`MyDecimal::from_string` cell across `Chunk::append_datum`,
`MutRow::from_datums`, `MutRow::set_value`, and `MutRow::set_datum`; the
pre-fix implementation panicked on the same input.

## Ready validation

Commands run from `rust/`:

```text
cargo test --offline --locked -p tidb-chunk --lib chunk::tests::decimal_datum_overflow_uses_go_truncation_without_panicking -- --exact --nocapture
cargo test --offline --locked -j12 -p tidb-datatype -p tidb-chunk --all-targets
cargo fmt --all -- --check
git diff --check
cargo clippy --offline --locked -p tidb-datatype -p tidb-chunk --all-targets -- -D warnings
```

Results:

- Focused regression: PASS (1/1).
- `tidb-datatype` serialized owner profile (`--all-targets --test-threads=1`):
  PASS, 381 unit tests and 63 generated/integration tests.
- `tidb-chunk` serialized library owner profile (`--test-threads=1`): 241
  passed, 35 failed, 4 ignored. The failures are the pre-existing spill,
  temporary-file, row-container, and iterator environment/concurrency cases
  (`No such file or directory` under the macOS temporary directory, plus the
  dependent ordering assertions); the focused decimal test and all ordinary
  in-memory chunk/mutrow tests pass.
- Formatting and whitespace gates: PASS (`cargo fmt --all -- --check`,
  `git diff --check`).
- Owner compilation: PASS (`cargo check --offline --locked -p tidb-datatype
  -p tidb-chunk --all-targets`); the emitted warnings are pre-existing
  workspace/test dead-code and unused-import warnings.
- Strict clippy: BLOCKED by pre-existing `tidb-mysql/src/consts.rs`\
  `clippy::map-or-identity` errors at lines 117-120; no diagnostic points at
  this batch's files.

The earlier parallel owner invocation also reproduced the same chunk spill
failures and a lock-poisoned datatype charset test; the serialized datatype
rerun above passes that test and is the authoritative datatype result.

## Risks and remaining boundaries

- The lossy fallback intentionally mirrors Go's already-parsed fixed cell; it
  must not replace exact `to_my_decimal` conversions used by spill/codec paths.
- Chunk A-2 offset-table strictness and the remaining datum shape/fraction
  metadata questions remain separate follow-ups in
  `docs/chunk-and-stats-divergence.md`.
- The complete `pkg/util/chunk` inventory is larger than this one behavior
  cluster, so no package-complete parity claim is made.
