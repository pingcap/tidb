# `pkg/util/generic` — Go-master parity audit receipt

Go authority: `origin/master` at `c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete inventory

The package contains exactly five artifacts, all read in full (478 lines
total):

- `bounded_min_heap.go` (115 lines) and `sync_map.go` (68 lines): production
  bounded best-N heap and RWMutex-backed generic map;
- `bounded_min_heap_test.go` (210 lines): seven source test identities;
- `sync_map_test.go` (62 lines): the source `TestSyncMap` identity;
- `BUILD.bazel` (23 lines): production and flaky test targets.

There is no `doc.go`, package harness, fixture, testdata, benchmark, fuzz
target, example, generated/platform variant, nested package, or other build
input. All five files are byte-identical to Go master.

## Rust ownership and parity

`rust/crates/tidb-util/src/generic/{bounded_min_heap,sync_map}.rs` is the
complete owner. The heap preserves nil-comparator and negative-capacity
panics, zero-capacity behavior, signed comparator magnitude and wrapping
negation for best-to-worst sorting, and all source replacement rules. `SyncMap`
preserves Go's `(value, bool)` semantics through `Option`, key snapshots, and
lock-poison recovery. The owner retains exactly the eight source-derived tests;
the stats TopN consumer uses this canonical heap rather than a duplicate.

No Go or Rust production delta was found in this rolling audit, so no new
package-local semantic regression was warranted. The owner did, however,
carry six Rust-only `#[must_use]` diagnostics on the source-shaped heap/map
constructors and observers. The focused `return_values_may_be_ignored_like_go`
regression discards all six under `#[deny(unused_must_use)]`: the detached
pre-fix owner failed with exactly six diagnostics, and the corrected owner
passes.

## Validation (Ready profile)

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/generic -count=1` — passed (eight tests).
- `(cd /tmp/tidb-go-latest-c605 && PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/generic -count=1)` — passed (eight tests).
- `env OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib 'generic::' --offline --locked -- --test-threads=1` — passed (eight tests).
- `OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib generic::tests::return_values_may_be_ignored_like_go -- --exact --nocapture` — passed after the six-error pre-fix failure.
- `OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib 'generic::' -- --test-threads=1` — passed; 9 tests including the discard-contract regression.
- `OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install OPENSSL_STATIC=1 cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --all-targets` — passed.
- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed in the clean detached Go-master checkout; the active checkout may be temporarily instrumented by the concurrent failpoint test worker.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed as the Ready gate.
- `git diff --check` — passed for the documentation diff.

No Go, import section, test target, Bazel file, or module dependency changed,
so `make bazel_prepare` is not required. The package has no failpoint use, so
the failpoint wrapper is not applicable.

## Risks and boundaries

- Correctness: source tests cover heap ranking, replacement, capacities,
  comparator direction, safety checks, and map store/load/delete/key behavior.
- Compatibility: no public API or runtime behavior changed in this batch.
- Performance: no production code changed; heap and map complexity are
  unchanged.
- Not verified locally: broad stats TopN production integration after this
  no-delta refresh; prior consumer tests remain the integration evidence.
