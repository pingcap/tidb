# `pkg/util/mvmap` — complete package transcreation

Pinned TiDB source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly six artifacts, all read in full: `mvmap.go`, `fnv.go`,
`mvmap_test.go`, `bench_test.go`, `main_test.go`, and `BUILD.bazel`. There is no
package doc, fixture, generated input/output, platform variant, README, or
ownership file. The local Go package is byte-identical to the pin.

Production behavior comprises the private FNV-1 64-bit hash, packed data and
entry arenas, hash-collision and repeated-key chains, input-order value lookup,
value count, and insertion-order iteration. The package has two unit tests and
two benchmark families. `TestMain` only installs common Go test state and
goleak exclusions; it contains no package behavior.

## Rust ownership and audit result

`rust/crates/tidb-util/src/mvmap/` is the sole production owner. It preserves
the source arena sizes, reserved null entry, wrapping FNV arithmetic, linked
hash buckets, full result-vector reversal, borrowed arena results, and iterator
termination. Rust slices replace Go slice views without changing their
aliasing lifetime: returned values still refer to the map's packed storage.

The audit removed `MVMap::default`, the standard-library `Iterator` extension
surface, two benchmark bodies incorrectly installed as unit tests, and two
supplemental FNV tests absent from Go. The public iterator now exposes only the
source-shaped `next` operation. The inline suite contains exactly the two Go
tests, and `benches/mvmap.rs` contains both source benchmark families.

Go's ordinary DISTINCT aggregation consumer uses `MVMap`; Rust previously
bypassed it with a `HashSet`. `tidb-exec` now uses the package owner for the
same get-before-put membership protocol and no longer exposes a Rust-only
default constructor for that state. Go's other consumer is index lookup join;
Rust has no complete counterpart for that executor package, so no substitute
consumer was invented here. That remains an explicit executor-package gap.

The owner also carried four explicit Rust-only `#[must_use]` diagnostics on
the source-shaped `MVMap::new`, `get`, `len`, and `new_iterator` methods. The
focused `return_values_may_be_ignored_like_go` regression discards all four
under `#[deny(unused_must_use)]`: the detached pre-fix owner failed with
exactly four diagnostics, and the corrected owner passes.

## Validation

Profile: Ready for this focused parity fix within the continuing
package-by-package audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/mvmap` — passed.
- `go test ./pkg/util/mvmap -count=1` — passed, 2 tests.
- `cargo test --offline --locked -p tidb-util --lib mvmap:: --no-fail-fast` — passed, 2 tests.
- `cargo test --offline --locked -p tidb-util --no-run` — passed.
- `cargo bench --offline --locked -p tidb-util --bench mvmap --no-run` — passed.
- `cargo test --offline --locked -p tidb-exec --test all aggregate_distinct` — passed, 2 consumer tests.
- `cargo check --offline --locked -p tidb-server` — passed.
- `cargo fmt -p tidb-util -p tidb-exec -- --check` and `git diff --check` — passed.
- `OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib mvmap::tests::return_values_may_be_ignored_like_go -- --exact --nocapture` — passed after the four-error pre-fix failure.
- `OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib mvmap::tests -- --test-threads=1` — passed, 2 tests.
- `OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install OPENSSL_STATIC=1 cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --bench mvmap` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed as the Ready gate.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: both exact package tests pass on Go and Rust, and the ordinary
  DISTINCT consumer's source-derived tests pass after it is wired to `MVMap`.
- Compatibility: removes Rust-only constructors and iterator extensions with
  no in-tree callers. The source-shaped constructor, lookup, insertion, length,
  and iteration operations remain.
- Performance: DISTINCT no longer duplicates each encoded key into a second
  hash-table allocation; it follows Go's packed arena owner. Both benchmark
  executables compile but were not timed in WIP.
