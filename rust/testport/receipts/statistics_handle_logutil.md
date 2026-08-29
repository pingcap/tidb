# `pkg/statistics/handle/logutil` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly two artifacts, both read in full and byte-compared
against the pin:

- `BUILD.bazel` — one public Go library over `logutil.go`, depending on the
  shared TiDB logger and zap field API;
- `logutil.go` — four logger constructors and two package-level sampled-logger
  factories.

There is no `doc.go`, package test, fixture, benchmark, generated source/input,
or build/platform variant.

## Rust ownership and behavior

`rust/crates/tidb-stats-handle-logutil` is the package owner and only composes
the completed `tidb-util::logutil` implementation:

- `stats_logger` returns the background logger with `category=stats`;
- `stats_err_verbose_logger` returns the error-verbose logger with that same
  category;
- `stats_sample_logger` shares one sampled logger with a five-minute window
  and first-one admission;
- `stats_err_verbose_sample_logger` shares one error-verbose sampled logger
  with a ten-minute window and first-one admission.

Cloning a sampled Rust handle shares its sink and sampler state, matching the
Go factory's `sync.Once` pointer identity. No second logging backend or
statistics-specific sampling policy was introduced. Existing consumers will
adopt this owner as their complete Go packages are audited; this package does
not rewrite those consumer packages speculatively.

## Validation

Profile: WIP. This completes one atomic package in the continuing parity audit,
not a repository-wide readiness claim.

- Complete pinned-package inventory/diff gate: passed.
- Pinned Go package test: passed; the package has no test files.
- `cargo check -p tidb-stats-handle-logutil`: passed.
- `cargo test -p tidb-stats-handle-logutil`: passed; the package has no tests.
- Scoped `cargo fmt -p tidb-stats-handle-logutil --check`: passed.
- `git diff --check`: passed.

No Go or Bazel source changed, so `make bazel_prepare` is not required.

## Risk and unverified boundaries

- Correctness: fields, base logger selection, sampling windows, and admission
  count are source-exact and delegated to the shared logger owner.
- Compatibility: the package is additive until consumer packages are audited;
  it does not change their current behavior prematurely.
- Performance: each unsampled call creates one cheap contextual logger handle;
  sampled calls clone one shared handle, matching Go's factory lifetime.
- Repository-wide lint and integration suites remain deferred to the Ready
  profile after the full parity goal is complete.
