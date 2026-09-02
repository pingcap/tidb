# `pkg/statistics/handle/logutil` — complete package transcreation

Pinned Go source: `c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete inventory

The package has exactly two artifacts and 67 lines, both read in full from the
detached Go-master worktree before this authority refresh:

| Artifact | Lines | Git blob | Disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 12 | `49bc63b86585e24f3fce6099584e5ed3e72d3df1` | public library metadata inventoried |
| `logutil.go` | 55 | `c894f778665035674a7690defeb23d57f781560e` | four constructors and two package-level factories mapped below |

There is no `doc.go`, package test, fixture, benchmark, fuzz target, generated
source/input, or build/platform variant.

## Rust ownership and behavior

`rust/crates/tidb-stats-handle-logutil` is the single package owner and only
composes the completed `tidb-util::logutil` implementation:

- `stats_logger` returns the background logger with `category=stats`;
- `stats_err_verbose_logger` returns the error-verbose logger with that same
  category;
- `stats_sample_logger` shares one sampled logger with a five-minute window
  and first-one admission;
- `stats_err_verbose_sample_logger` shares one error-verbose sampled logger
  with a ten-minute window and first-one admission.

Cloning a sampled Rust handle shares its sink and sampler state, matching the
Go factory's `sync.Once` pointer identity. No second logging backend or
statistics-specific sampling policy was introduced. Consumer call sites are
owned and reconciled by their respective complete Go packages.

The previous Rust-only emitted-file test remains removed. The pinned Go source
has no original test artifact, and the shared factory's source tests belong to
the separate `pkg/util/logutil` owner.

## Validation

Profile: Ready. This is one atomic package authority refresh inside the
continuing repository-wide parity audit, not a whole-repository claim.

- Complete pinned-package inventory/diff gate passed; current source is byte
  identical to c605 for this package.
- Current and detached Go package probes passed (`[no test files]`).
- `cargo test -p tidb-stats-handle-logutil`: passed (zero tests, matching the
  source inventory).
- `cargo check -p tidb-stats-handle-logutil`: passed.
- Rust formatting, the pinned repository lint gate, scoped diff hygiene,
  commit integrity, push, pull, and remote SHA verification pass.

No Go, Bazel, or module file changed in this batch, so `make bazel_prepare`
was not required.

## Risk and unverified boundaries

- Correctness: fields, base logger selection, sampling windows, and first-one
  admission are delegated to the shared logger owner and match Go.
- Compatibility: the four constructors retain their established Rust API;
  only a source-absent emitted-file test was removed.
- Performance: unsampled calls create one cheap contextual logger handle;
  sampled calls clone one shared handle, matching Go's factory lifetime.
- Broad integration and RealTiKV suites were not run because this source-test-
  free package is covered by its owner compile and downstream consumer gates.
