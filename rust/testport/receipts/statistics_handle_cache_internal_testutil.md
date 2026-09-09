# `pkg/statistics/handle/cache/internal/testutil` → `tidb-stats-handle-cache-internal-testutil`

Historical pinned source: `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
Current Go source rechecked at `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.

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

## Follow-up: discardable table-constructor return (2026-09-06)

The complete two-artifact, 109-line Go package was re-read at current
`origin/master` `f2c346fe4f368ff855e17c1f62e28a89ba7f9723` and remains
byte-identical to the historical pin. It contains the 95-line production file
and 14-line Bazel target only: no package doc, Go test, testdata, fixture,
generated input/output, example, benchmark, fuzz target, or platform/build-tag
variant exists. All three Go functions and the complete two-file Rust owner
(`Cargo.toml` and `src/lib.rs`) were reviewed, along with every Rust consumer
in the LFU, map cache, parent cache, and cache benchmark.

Go permits callers to discard `NewMockStatisticsTable`; Rust's direct
`new_mock_statistics_table` counterpart instead emitted an
`unused_must_use` diagnostic. The annotation was removed without changing the
table, histogram, sketch, TopN, load-state, ID, or memory-accounting behavior.
The focused unit regression invokes the constructor under
`#[deny(unused_must_use)]`; it failed before the implementation edit with
exactly one diagnostic and passes afterward.

Ready validation for this follow-up:

```text
OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-stats-handle-cache-internal-testutil --lib source_return_value_may_be_ignored_like_go --offline --locked -- --nocapture
PASS; 1 passed, 0 failed.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-stats-handle-cache-internal-testutil --offline --locked -- --test-threads=1
PASS; 1 unit test passed, 0 failed; doc tests had 0 tests.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-stats-handle-cache-internal-testutil --all-targets --offline --locked
PASS; pre-existing dependency warnings remain outside this crate.

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PASS.

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
PASS.

git diff --check
PASS.
```

Only Rust source/tests and parity documentation changed. No Go, Bazel, Cargo
metadata, or module dependency changed, so `make bazel_prepare` is not
required. No Go test exists to rerun, and the return-contract-only edit leaves
the already-covered production table behavior unchanged.
