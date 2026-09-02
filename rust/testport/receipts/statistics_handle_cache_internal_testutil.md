# `pkg/statistics/handle/cache/internal/testutil` → `tidb-stats-handle-cache-internal-testutil`

Pinned source: `c6054025ed4c32ab3672a2a24ea46892714d21ec` (Go `master` at the
audit boundary).

## Atomic inventory

| Artifact | Lines | Git blob | SHA-256 | Rust owner |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 14 | `bca31c934cc04e651524816942cf7877f9621d9b` | `f0158b92c287531f9c6b289eebf5dea7ed85a7ae1d64815d6e40ac2eb723ce07` | workspace target and crate manifest |
| `testutil.go` | 95 | `ca2c179152567e3feaa22a7632d06d8d8ee17ce2` | `3abb61788e60bcb9fe086e2609a77d15195d0a449ed0da498eb3f8309982c21a` | `src/lib.rs` |

The support package has no generated, platform-specific, test, fixture-file,
or benchmark artifacts. This is the complete two-artifact, 109-line Go
package; the current checkout is byte-identical to the pinned Go master
source.

## Behavior mapping

- `new_mock_statistics_table` returns a shared actual `tidb_stats::Table`
  with the source zero-valued `HistColl` and one-based column/index IDs.
- Negative column/index counts produce no entries, matching the Go `int` loop.
- Optional CMS sketches use depth/width 1, optional TopN values have capacity
  one and contain the empty byte value with count one, and optional histograms
  retain the source metadata and one-bucket allocation hint.
- Initial columns and indexes have full-load status; append helpers deliberately
  omit it, matching their source struct literals.
- Memory accounting comes from the real Rust sketches and the native
  histogram allocation, so cache tests exercise the same production table
  memory path instead of caller-provided fixture costs.
- Append helpers derive the next ID from the current table map length and add
  a CMS-only item.

The former `MockStatisticsTableShape` and its two source-absent tests were
removed. They produced no table, sketches, histogram, load status, memory
accounting, or append behavior.

## Validation

Ready profile: this is a source-test-free support package, so the package gate
is strict compilation/linting plus the affected statistics owner gate.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/statistics/handle/cache/internal/testutil -count=1` (current checkout; `[no test files]`)
- same pinned Go command in `/tmp/tidb-go-latest-c605` (detached Go master; `[no test files]`)
- `env OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats-handle-cache-internal-testutil`
- `env OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 clippy --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats-handle-cache-internal-testutil --no-deps -- -D warnings`
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- pinned `make lint`
- `git diff --check`

No Go or Bazel source changed; `make bazel_prepare` is not required for this
documentation-only receipt refresh.
