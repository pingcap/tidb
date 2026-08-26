# `util/async` source-artifact audit

Source of truth: `tikv/client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

Rust toolchain: `nightly-2026-08-22`.

## Atomic inventory

The pinned package contains exactly four artifacts and 495 lines:

| Artifact | Role | Lines | SHA-256 |
| --- | --- | ---: | --- |
| `util/async/core.go` | callback, pool, and executor production API | 83 | `d50d07afc5d3d758197590fb96e751795c29337cef3030916867d01ba1b857d5` |
| `util/async/runloop.go` | run-loop production API | 158 | `3603957af99f22ffc44ea58a7459a6a9b30c0b23a5c6d9457e648a269a95b6f1` |
| `util/async/core_test.go` | callback tests and mock executor | 90 | `433f87fe459027f8b3e12dd343781e9dcbfe7a40375cd74e95ee3b1ce07ed82f` |
| `util/async/runloop_test.go` | run-loop tests | 164 | `4b01a58a40df5ba1ea63474831c4f53de4a337cf360cfdfb21ca08ab0cecebcb` |

There is no package-local `doc.go`, `main_test.go`, build-tag or platform
variant, generated source or input, fixture, benchmark, example, metadata,
`OWNERS`, or build file. The parent package's `util/main_test.go` declares
`package util`, not `package async`, and is therefore not support for this
child package.

## Production surface and Rust integration

`src/async_util.rs` owns the package and `src/lib.rs` exposes it as the ordinary
public `tikv_client::async_util` module. The re-audit removed the former
`#[doc(hidden)]`: client-go's `util/async` is a public importable package, so a
public-but-undiscoverable Rust module was an API-surface divergence.

| client-go surface | Rust owner and decision |
| --- | --- |
| `Pool.Go` | `Pool::spawn(Task)`; the default run loop starts a thread and an injected pool owns custom dispatch. |
| `Executor.Append` | `Executor::append(Vec<Task>)`; ownership replaces Go variadics while retaining order and concurrent safety. |
| `Callback`, `NewCallback`, `Executor` | Cloneable `Callback<T, E>`, `Callback::new`, and `Callback::executor`; `Option<E>` preserves nullable Go errors. |
| `Inject` | `Callback::inject`; injectors execute in reverse registration order. |
| `Invoke`, `Schedule`, `sync.Once` | `invoke`, `schedule`, and one shared atomic claim preserve exactly-once behavior across clones and all call orders. Scheduling without an executor panics after consuming the claim, matching a nil Go executor. |
| `StateIdle`, `StateWaiting`, `StateRunning` | Public `State::{Idle, Waiting, Running}` with the source zero/default state. |
| embedded optional `RunLoop.Pool` | `RunLoop::set_pool` uses synchronized shared ownership instead of an unsafely mutable public interface field. |
| `State`, `NumRunnable`, `Append` | `state`, `num_runnable`, and `Executor::append`; empty appends are no-ops and a waiting loop receives one wakeup. |
| `Exec(context.Context)` | `RunLoop::execute(&Cancellation)` returns `(usize, Result<_, RunLoopError>)`, preserving task counts, cancellation, and the exact concurrent-execution error. |
| running/runnable batch rotation | `VecDeque<Task>` retains source order, includes nested appends in the current execution, leaves later appends for the next execution, and restores all unexecuted work before cancellation returns. |

Rust's `Cancellation` is the native mapping for the cancellation-only portion
of `context.Context`; parent/child propagation is also used by Rust retry and
request owners. Client-go consumers that use callbacks and `RunLoop` map to
native futures and Tokio workers in Rust. Those integration decisions are
already closed by their owning package receipts rather than duplicated here.

## Original unit-test mapping

The two Go test files contain nine top-level tests. `TestFulfillOnce` contains
four named subtests, yielding 12 independently executable source cases. Every
case now has its own source-named Rust test rather than sharing one aggregated
test:

| client-go case | Rust test |
| --- | --- |
| `TestInjectOrder` | `source_test_inject_order` |
| `TestFulfillOnce/InvokeTwice` | `source_test_fulfill_once_invoke_twice` |
| `TestFulfillOnce/ScheduleTwice` | `source_test_fulfill_once_schedule_twice` |
| `TestFulfillOnce/InvokeSchedule` | `source_test_fulfill_once_invoke_schedule` |
| `TestFulfillOnce/ScheduleInvoke` | `source_test_fulfill_once_schedule_invoke` |
| `TestGo` | `source_test_go` |
| `TestExecWait` | `source_test_exec_wait` |
| `TestExecOnce` | `source_test_exec_once` |
| `TestExecTwice` | `source_test_exec_twice` |
| `TestExecCancelWhileRunning` | `source_test_exec_cancel_while_running` |
| `TestExecCancelWhileWaiting` | `source_test_exec_cancel_while_waiting` |
| `TestExecConcurrent` | `source_test_exec_concurrent` |

The wait/wakeup and delayed-second-execution ports use explicit synchronization
instead of millisecond sleeps, preserving the source state transitions without
timing flakiness. Two additional tests cover source-public surfaces not asserted
locally by Go: executor identity, empty append, exact errors, child cancellation,
and parent/child directionality. `tests/public_async_util_tests.rs` is an
ordinary downstream-crate gate that implements both public traits and uses the
callback and run-loop API without any private feature.

## Direct client-go consumers

Mechanical exact-import search finds 17 files:

- Production: `internal/client/client.go`, `client_async.go`,
  `client_batch.go`, `client_collapse.go`, and `client_interceptor.go`;
  `internal/locate/region_request.go`;
  `internal/mockstore/mocktikv/rpc.go`; and
  `txnkv/txnsnapshot/client_helper.go` and `snapshot_async.go`.
- Tests/support: `integration_tests/async_commit_test.go`;
  `internal/client/client_async_test.go`, `client_interceptor_test.go`, and
  `client_test.go`; `internal/locate/region_request3_test.go` and
  `region_request_test.go`; `tikv/test_util.go`; and
  `txnkv/transaction/test_util.go`.

No other pinned Go file imports `github.com/tikv/client-go/v2/util/async`.

## Validation

Exact pinned Go package and race suites:

```text
env GOCACHE=/private/tmp/client-go-util-async-build-cache \
    GOMODCACHE=/private/tmp/client-go-util-async-module-cache \
    /private/tmp/go1.25.12/bin/go test ./util/async -count=1
# passed

env GOCACHE=/private/tmp/client-go-util-async-race-build-cache \
    GOMODCACHE=/private/tmp/client-go-util-async-module-cache \
    /private/tmp/go1.25.12/bin/go test -race ./util/async -count=1
# passed
```

Focused and public API gates:

```text
cargo +nightly-2026-08-22 test -p tikv-client \
    async_util::tests::source_test --lib --no-default-features
# 12 passed

cargo +nightly-2026-08-22 test -p tikv-client \
    async_util::tests --lib --all-features
# 14 passed

cargo +nightly-2026-08-22 test -p tikv-client \
    --test public_async_util_tests --no-default-features
cargo +nightly-2026-08-22 test -p tikv-client \
    --test public_async_util_tests --all-features
# 1 passed in each configuration
```

Complete matrices and strict gates:

```text
cargo +nightly-2026-08-22 test -p tikv-client source_ --lib \
    --no-default-features --quiet
# 585 passed
cargo +nightly-2026-08-22 test -p tikv-client source_ --lib \
    --all-features --quiet
# 582 passed

cargo +nightly-2026-08-22 test -p tikv-client --lib \
    --no-default-features --quiet
# 892 passed; 1 unrelated intentional ignore
cargo +nightly-2026-08-22 test -p tikv-client --lib \
    --all-features --quiet
# 889 passed; 1 unrelated intentional ignore

cargo +nightly-2026-08-22 test --workspace --no-default-features --quiet
cargo +nightly-2026-08-22 check --workspace --all-targets --all-features
cargo +nightly-2026-08-22 clippy --workspace --all-targets \
    --all-features -- -D warnings
env RUSTDOCFLAGS='-Dwarnings --document-private-items' \
    cargo +nightly-2026-08-22 doc --workspace --all-features --no-deps
cargo +nightly-2026-08-22 test --workspace --doc --all-features --quiet
# all passed; 51 doctests

cargo +nightly-2026-08-22 fmt --all -- --check
git diff --check
# passed
```

No live TiKV or PD validation applies to this in-process scheduling package.
