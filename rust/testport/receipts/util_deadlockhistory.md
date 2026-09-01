# `pkg/util/deadlockhistory` — Go-master parity audit receipt

Go authority: `origin/master` at `c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete inventory

The package contains exactly four artifacts, all read in full (669 lines
total):

- `deadlock_history.go` (242 lines): column contracts, wait-chain/record
  values, datum conversion, bounded thread-safe history, global history, and
  TiKV error conversion;
- `deadlock_history_test.go` (354 lines): the four source test identities for
  collection, datum conversion, error conversion, and resize;
- `main_test.go` (33 lines): `TestMain`, common setup, and leak exclusions;
- `BUILD.bazel` (40 lines): production and flaky test targets.

There is no `doc.go`, fixture, testdata, benchmark, fuzz target, example,
generated/platform variant, nested package, or other build input. All four
files are byte-identical to Go master.

## Rust ownership and parity

`rust/crates/tidb-executor/src/deadlock_history.rs` owns the package behavior:
column constants and datum/null rules, wait-chain conversion, timestamp
precision, bounded FIFO retention, monotonic IDs, clear/resize semantics,
pointer-sharing snapshots, and the process-global history. The executor owns
retryable admission and the session information-schema reader owns key/digest
lookup, matching Go's package boundaries. Rust-only row renderers, decoders,
retry policies, configuration, and alternate recording entry points are not
present.

No Go or Rust production delta was found in this rolling audit, so no new
package-local regression was warranted. The existing four source-derived Rust
tests remain the focused regression carrier.

## Validation (Ready profile)

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/deadlockhistory -count=1` — passed (four tests).
- `(cd /tmp/tidb-go-latest-c605 && PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/deadlockhistory -count=1)` — passed (four tests).
- `env OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-executor --lib deadlock_history::tests --offline --locked -- --test-threads=1` — passed (four tests).
- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed for the current repository Ready gate.
- `git diff --check` — passed.

This batch changes documentation only; no Go source, import section, test
function, Bazel file, or module dependency changed, so `make bazel_prepare` is
not required. The package has no failpoint use, so the failpoint wrapper is not
applicable.

## Risks and boundaries

- Correctness: source tests cover IDs, retention, nullability, timestamp
  conversion, digest decoding, and resize-to-zero behavior.
- Compatibility: no public API or runtime behavior changed in this batch.
- Performance: no production code changed; the bounded mutex-backed history is
  unchanged.
- Not verified locally: a live TiKV deadlock followed by distributed
  `CLUSTER_DEADLOCKS`; ordinary session/executor regressions remain the local
  integration evidence.
