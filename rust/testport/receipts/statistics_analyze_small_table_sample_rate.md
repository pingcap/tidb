# `pkg/statistics` / `pkg/executor` — small-table ANALYZE sample-rate receipt

## Scope and complete owner inventory

This batch follows Go-master (`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`,
2026-09-06) for the auto-adjusted sample rate used by `ANALYZE TABLE`. Before
editing, the complete tracked `pkg/statistics` tree was inventoried with
`git ls-tree -r --name-only master -- pkg/statistics`:

| Go package tree | Tracked files | Go production | Go tests | non-Go/build/fixture inputs |
| --- | ---: | ---: | ---: | ---: |
| `pkg/statistics` (including all nested handle/cache/auto-analyze trees) | 198 | 79 | 74 | 40 Bazel files, 3 testdata/golden fixtures, 1 `OWNERS`, 1 README, 2 generated/bootstrap-marked files |

The review covered every listed production and test file, all nested statistics
subpackages, build inputs, fixtures, and metadata. No Go file was edited. The
direct Go contract is distributed across:

* `pkg/executor/builder.go:3160-3165` — reads the stats-meta count before
  constructing an analyze task;
* `pkg/executor/builder.go:3246-3290` — `getAdjustedSampleRate`: a present
  zero-count stats row scans at rate `1`, while an unknown stats handle uses
  the `0.001` fallback;
* `pkg/statistics/handle/storage/read.go:43-76` — a missing `stats_meta` row
  returns count `0` with `isNull=true`, which the DDL-backed path exposes as
  the zero-count table state;
* `pkg/statistics/row_sampler.go:108-171,437-458` — chooses Bernoulli versus
  reservoir sampling and retains each row according to the chosen rate; and
* `pkg/executor/test/analyzetest/analyze_test.go:458-484,583-624` — captures
  the `0.5`/`1.0` rate boundaries and the small-table full-scan behavior.

## Failure and implementation

`rust/crates/tidb-session/src/analyze_arm.rs` previously passed `None` to the
shared `adjusted_sample_rate` whenever an in-process table had no cached
statistics. That input means “unknown stats handle” and correctly maps to
`0.001`; it does not model an existing table whose Go `stats_meta` count is
zero. A fresh five/ten-row in-process table could therefore retain no sampled
rows, publish no TopN, and make an indexed predicate estimate `1.00` instead
of its exact row count.

The in-process catalog is itself proof that the table exists. Its analyzer now
maps a missing per-table statistics entry to `Some(0)`, matching the Go
stats-meta read for a new table. The shared helper keeps its distinct
`None`/`None` unknown-count result of `0.001`, and the `Some(0)` branch scans
at rate `1.0`; cluster callers and their PD-aware inputs are unchanged.

## Regression coverage

`rust/crates/tidb-session/src/tests_analyze.rs` adds
`fresh_in_process_analyze_uses_full_sample_for_a_small_table`, which creates a
new table with repeated values, runs an unqualified `ANALYZE TABLE`, and
asserts the exact `3.00` estimate for `a = 2`. The existing ten-row captured
distribution test now passes through the same path. The helper boundary test
in `rust/crates/tidb-executor/src/tests_analyze_suite_source.rs` records the
Go `0.001` unknown-count fallback explicitly.

Focused commands:

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib tests_analyze::fresh_in_process_analyze_uses_full_sample_for_a_small_table \
  -- --nocapture --test-threads=1
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib tests_analyze::analyze_publishes_the_row_count_and_the_distribution \
  -- --nocapture --test-threads=1
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib tests_analyze_suite_source::analyze_auto_adjusted_sample_rate_boundaries \
  -- --nocapture --test-threads=1
```

All three passed. The broader `tests_analyze::` module still has one
pre-existing cross-thread statistics-loader failure; it is unrelated to this
sample-rate path and is not used as the batch gate.

Ready validation profile:

```text
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --all-targets
GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint
```

