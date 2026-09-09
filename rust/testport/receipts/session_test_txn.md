# `pkg/session/test/txn` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package is test-only: three tracked artifacts and 622 lines. Every test
line and BUILD declaration was read before comparing Rust. There is no
production source, `doc.go`, fixture directory, generated output, benchmark,
fuzz target, or platform/build-tag variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 31 | `7cf6d4ddaf72cf11bcba3c3fe5ce60979eb9809d` | `8db2972333856270f87a4a24e6ed3646a867064c44fe858049ee4e57056dfc4f` | flaky eleven-shard transaction test target and dependency inventory |
| `main_test.go` | 62 | `eb0e748eeb0782956813494ed2bf79f06f33a9cb` | `d9c6a20e7227e281a709a296945a14b80bb5885fc36df87c5196b5c4f9eab595` | TestMain, goleak, failpoint, and async-commit harness |
| `txn_test.go` | 529 | `e85ea63cb49b283ae378774ca5f21a8d4a77aba4` | `1185b61b7a7e3220b5929cf76c63a957d10d9f0e1f146d13cd87c01a5da6d242` | eleven transaction lifecycle, conflict, timestamp, and membuffer tests |

The package has thirteen functions: TestMain, one lazy-initialize helper,
and eleven top-level tests. They cover autocommit status and mode switches,
lazy transaction initialization, disabled retry and latch conflicts,
restricted-read-only commit checks, retryable error composition, concurrent
duplicate-key rollback, protocol `InTrans` state, commit-timestamp ordering,
UnionScan/membuffer snapshot reads, memory-quota cleanup, and rollback of a
killed pessimistic transaction.

## Rust ownership and explicit boundary

The source-carrier crate records TestMain and all eleven Go tests as explicit
carriers; only `TestAutocommit` has a runnable session assertion. This batch
added the previously missing `TestPanicOnRollbackKilledTxn` carrier from the
current Go master after the historical b148 slice was pinned. Rust's
`tidb-session` owns autocommit variables, transaction status, and session
boundaries. `tidb-exec` has a narrow `LazyTxnState` predicate test, while
`tidb-txnkv` provides typed transaction errors, retry decisions, transaction
size limits, mem-buffer interfaces, and commit/snapshot protocol primitives.

Those pieces do not form a dependency-closed owner for the Go package's
mock-TiKV conflict/retry choreography, restricted-read-only privilege checks,
Oracle failpoint ordering, UnionScan/membuffer behavior, memory-leak quota
assertions, or killed-transaction cleanup. The Go `Txn` API and protocol
status lifecycle are also not exposed as a compatible Rust session surface.
No Rust-only behavior was found to remove, and no safe missing behavior can be
implemented in this test-only package without inventing a second transaction
or storage authority. This package is therefore recorded as an explicit
SEED/boundary; remaining parity belongs to coordinated session, executor,
storage, and transaction owners.

## Validation and risk

Profile: **Ready** for the carrier-plus-receipt batch. The Rust source change
is test-only; no Go source, imports, test declarations, Bazel metadata, or
module files changed, so `make bazel_prepare` was not required. No production
behavior changed, so a new behavioral regression test was not applicable.

```text
(cd <detached-origin/master-worktree> && \
 PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
 GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
 ./tools/check/failpoint-go-test.sh ./pkg/session/test/txn -count=1)
# passed: pkg/session/test/txn (72.033s); failpoints enabled and disabled
```

The package was tested from an exact detached Go-master worktree. Additional
Ready checks were:

```text
rustfmt +nightly-2026-08-22 --edition 2024 --check \
  rust/crates/tidb-session/src/tests_session_part5_source.rs
# passed

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
# passed

cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml \
  -p tidb-session --lib test_panic_on_rollback_killed_txn --offline --locked \
  -- --nocapture
# blocked (RC=101): openssl-sys could not find pkg-config/OpenSSL
```

Not verified here: Bazel execution, full Go repository tests, real multi-store
conflict timing, Oracle failpoint ordering in Rust, or a future
dependency-closed Rust transaction/session implementation. Compatibility and
performance risk are unchanged because the only code change is an ignored
source carrier.

This receipt certifies the bounded test-package inventory and ownership
decision; it is not a repository-wide transcreation claim.
