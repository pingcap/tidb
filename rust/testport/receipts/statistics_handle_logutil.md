# `pkg/statistics/handle/logutil` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 12 | `49bc63b86585e24f3fce6099584e5ed3e72d3df1` | `d63dc86b2e575887302c8ea490ab797fc7cae881912006370a0d9d55958f4b84` | public library metadata inventoried |
| `logutil.go` | 55 | `c894f778665035674a7690defeb23d57f781560e` | `c4a7fbd6337bc9f81ed758458938cef99d1a464176319cc60dd1e7045df47ada` | four constructors and two package-level factories mapped below |

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
statistics-specific sampling policy was introduced. Consumer call sites are
owned and reconciled by their respective complete Go packages.

The previous Rust-only emitted-file test was removed. Pinned `logutil.go` has
no original test artifact, and the shared factory's source tests belong to the
separate `pkg/util/logutil` owner.

## Validation

Profile: WIP. This completes one atomic package in the continuing parity audit,
not a repository-wide readiness claim.

- Complete pinned-package inventory/diff gate: passed.
- Pinned Go package test: passed; the package has no test files.
- `cargo check -p tidb-stats-handle-logutil`: passed.
- `cargo test -p tidb-stats-handle-logutil`: passed; zero tests, matching the
  source inventory.
- Scoped `cargo fmt -p tidb-stats-handle-logutil --check`: passed.
- `git diff --check`: passed.

No Go or Bazel source changed, so `make bazel_prepare` is not required.

## Risk and unverified boundaries

- Correctness: fields, base logger selection, sampling windows, and admission
  count are source-exact and delegated to the shared logger owner.
- Compatibility: the four constructors retain the established Rust API; this
  audit removes only a non-source test.
- Performance: each unsampled call creates one cheap contextual logger handle;
  sampled calls clone one shared handle, matching Go's factory lifetime.
- Repository-wide lint and integration suites remain deferred to the Ready
  profile after the full parity goal is complete.
