# `pkg/types` duration `RoundFrac` tie-direction receipt

Status: bounded Rust parity fix implemented in the isolated worktree and
validated against fetched Go `master`. Go source and Bazel metadata were not
changed.

Comparison source: Go `origin/master` at
`fc7788ff517c3407dc7e000be989ab23e6648211`.

## Inventory completed before editing

The complete temporal owners were rechecked before editing, including
production files, tests/benchmarks, parser-driver fixtures, generated and
platform/build artifacts:

| Tree | Files | Lines |
| --- | ---: | ---: |
| `pkg/types` (including `parser_driver`) | 60 | 28,703 Go lines |
| `rust/crates/tidb-datatype` | 104 | 52,114 Rust source/test/manifest lines plus 8 data/docs artifacts |

The behavior-bearing Go files read were `pkg/types/time.go`
(`Duration.RoundFrac` and the `time.Time.Round` delegation),
`pkg/types/time_test.go` (duration rounding rows), and the complete
`pkg/types/BUILD.bazel`. The Rust owner chain was
`src/duration.rs::round_duration_fsp`, `MySqlDuration::round_frac`, its
duration tests, and the temporal expression comment that documents the rule.
No Go, generated, fixture, platform, or build file changed.

## Go behavior restored

Go's `Duration.RoundFrac` adds the duration to a zero `time.Time` and calls
`time.Time.Round`. Go rounds to the nearest multiple; an exact halfway value
rounds up toward positive infinity. Therefore a negative exact tie moves
toward zero (`-1.5ms` → `-1ms`), while a negative value past the tie still
moves away from zero (`-1.501ms` → `-2ms`).

Rust previously used `(value - half) / unit` for all negative inputs, which
made the exact tie away-from-zero and returned `-2ms` for `-1.5ms`. The helper
now uses sign-aware integer arithmetic: positive values use the ordinary
nearest-value half-up expression; negative magnitudes subtract one before the
division so only values strictly past the midpoint move away from zero. FSP
normalization, the early-return behavior, overflow handling, and all positive
source rows remain unchanged.

## Focused regression

`duration_tests::round_duration_fsp_matches_source_round_rows` preserves the
Go source vectors and adds both distinguishing boundaries:

- `round_duration_fsp(-1_500_000, 6, 3)` returns `-1_000_000` ns;
- `round_duration_fsp(-1_501_000, 6, 3)` returns `-2_000_000` ns.

The existing duration-method and temporal codec regressions were rerun as
guards against metadata or packing changes.

## Ready validation

Commands run from `rust/`:

```text
cargo fmt --all
cargo test --offline --locked -p tidb-datatype --lib duration_tests::round_duration_fsp_matches_source_round_rows -- --exact --nocapture
cargo test --offline --locked -p tidb-datatype --lib duration_tests::duration_methods_match_source_rows -- --exact --nocapture
cargo test --offline --locked -p tidb-datatype --lib mysql_time::tests::test_codec -- --exact --nocapture
cargo test --offline --locked -p tidb-datatype --all-targets -- --test-threads=1
cargo check --offline --locked -p tidb-datatype --all-targets
cargo fmt --all -- --check
git diff --check
cargo clippy --offline --locked -p tidb-datatype --all-targets -- -D warnings
```

Results:

- Focused rounding, duration-method, and codec regressions: PASS.
- Serialized owner profile: PASS (392 unit tests and 63 generated/source
  integration tests; benchmark targets compiled).
- Owner compilation, formatting, and whitespace checks: PASS.
- Strict clippy is blocked by the pre-existing
  `tidb-mysql/src/consts.rs:117-120` `map-or-identity` diagnostics (and any
  generated workspace diagnostics), not by this batch.

## Risks and remaining boundaries

This change is limited to the live duration FSP rounding helper. Datetime and
timestamp rounding, parser DST adjustment, zero-date context flags, timezone
projection, and the separate positive-only datetime fallback helper retain
their existing behavior. Findings T7–T12, T14, and T16 remain open and are not
claimed by this receipt.
