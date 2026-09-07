# `pkg/util/deadlockhistory` — Go-master parity audit receipt

Go authority: `origin/master` at `c767f6fd8c01e9dcb459767611c0c1d4110d210d`.

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

The current-master audit found four Rust-only `#[must_use]` contracts on
Go-shaped return values: error conversion, datum conversion, history
construction, and history reads. Go permits all four results to be ignored,
so the attributes were removed without changing runtime behavior. A focused
Rust regression uses `#[deny(unused_must_use)]` and deliberately ignores each
return value; it failed before the fix with exactly four diagnostics and passes
after the fix.

## Validation (Ready profile)

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/deadlockhistory -count=1` — passed (four tests).
- `(cd /tmp/tidb-go-latest-c605 && PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/deadlockhistory -count=1)` — passed (four tests).
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-executor --lib deadlock_history::tests::deadlock_history_returns_may_be_ignored_like_go -- --exact --nocapture` — passed (1/1 focused regression; 1218 filtered).
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -q -p tidb-executor --lib deadlock_history::tests -- --test-threads=1` — passed (5/5 owner tests).
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor --all-targets` — passed.
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-session --all-targets` — passed.
- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `git diff --check` — passed.

The repository-level Ready gate (`PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint`) is run after this receipt and ExecPlan update.

This batch changes Rust production/tests and documentation only; no Go source,
import section, Go test function, Bazel file, or module dependency changed, so
`make bazel_prepare` is not required. The package has no failpoint use, so the
failpoint wrapper is not applicable.

## Risks and boundaries

- Correctness: source tests cover IDs, retention, nullability, timestamp
  conversion, digest decoding, and resize-to-zero behavior.
- Compatibility: Rust now accepts the same ignored return values as Go; no
  runtime behavior or public signature changed.
- Performance: no hot-path logic changed; the bounded mutex-backed history is
  unchanged.
- Not verified locally: a live TiKV deadlock followed by distributed
  `CLUSTER_DEADLOCKS`; ordinary session/executor integration remains outside
  this focused package gate.
