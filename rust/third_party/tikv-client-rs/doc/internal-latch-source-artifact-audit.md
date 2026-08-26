# `internal/latch` source-artifact audit

This is the atomic completion receipt for client-go package `internal/latch`, pinned at commit `52c1e76cec993571493c81de442bcbef90cdc106`. Its primary Rust owner is `src/transaction/latch.rs`, with configuration, client-lifecycle, commit, and typed-error integration in `src/config.rs`, `src/transaction/client.rs`, `src/transaction/transaction.rs`, and `src/common/errors.rs`. Validation uses `nightly-2026-08-22`.

## Complete source inventory

The package is exactly five artifacts and 758 lines:

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `internal/latch/latch.go` | 325 | `51dbf98a251de012c60b09a5496f7b857845234cf5800b387eacf7d0a2297493` | `src/transaction/latch.rs` state machine |
| `internal/latch/scheduler.go` | 141 | `1c1dc734899c3ac2af0373c64444d5c863ca00b3863101c37ce17aa78d850a8a` | `src/transaction/latch.rs` async scheduler and guard |
| `internal/latch/latch_test.go` | 163 | `9d530271a237723a3a41be9350d6820897d3d0e0c94db9316b455737ac9c2de3` | three individually named `source_test_*` ports |
| `internal/latch/scheduler_test.go` | 104 | `a565ecfa5339c94348e61867e66866649d4dcb80a781dc019fe450c138666ebd` | `source_test_with_concurrency` and its generator |
| `internal/latch/main_test.go` | 25 | `e115ddc1ac8a88f24b55d7068bc6b14713bed007b99306bd5e7b4e5ab77af741` | task-joining and no-background-task native disposition |

There is no `doc.go`, benchmark, example, fixture, generated source or input, build/platform variant, package metadata, package-specific build file, build tag, or generation directive.

Mechanical import inventory finds exactly four direct Go importers: production owners `tikv/kv.go` and `txnkv/transaction/2pc.go`, plus support/test owners `txnkv/transaction/test_util.go` and `txnkv/transaction/txn_file_test.go`. The actual commit algorithm in `txnkv/transaction/txn.go` consumes the scheduler through the `KVStore` interface declared in `2pc.go`; it is therefore a semantic consumer without a direct package import. The broader root-store and transaction algorithms retain their own completed receipts.

## Production mapping and differential findings

| client-go surface | Rust behavior and integration decision |
| --- | --- |
| slot table and lock generation | Murmur3 x86-32, power-of-two capacity rounding, bytewise key sorting, and one required slot per key are exact. Rust takes ownership of the key vector, so sorting has the same observable boundary and owned keys provide the source release-time anti-retention copy from their first insertion. Zero capacity is rejected before construction instead of permitting client-go's unsigned underflow and impractical allocation. |
| acquisition and waiting | Per-key owner/max-commit state, partial multi-key acquisition, sorted deadlock avoidance, first matching waiter selection, and first-acquire stale rejection are exact. A safe `HashMap` replaces the source linked list without changing key identity or queue ordering. |
| release and wakeup | Acquired slots release in reverse order, each retained maximum commit timestamp is monotonic, and a stale waiter is granted the released slot before being marked stale so it can release all partial ownership. Wrong-owner release remains an invariant failure. |
| recycling | Five-entry opportunistic recycling, exact two-minute idle expiration, one-minute global interval, greater-than-50,000 unlock counter, commit-greater-than-start gate, physical-TSO subtraction, and reset-then-increment ordering are preserved. Rust performs global recycling synchronously under its scheduler mutex; client-go launches it asynchronously. Expired entries cannot affect a later valid transaction, so this changes scheduling latency but not transaction outcomes. |
| scheduler | Rust uses a mutex-serialized async state machine, one-shot notifications, and RAII guards instead of blocking wait groups and a background 100-entry unlock channel. Successful acquire/release/wakeup observations are preserved. Dropping a waiting future removes it from every wait queue, releases partial ownership, and wakes successors, providing the native cancellation behavior unavailable from source `Lock`. |
| close lifecycle | Close remains idempotent and later unlocks remain ignored. The audit found that `TransactionClient::close` previously left a shared scheduler open when another client or transaction retained its `Arc`; close now explicitly closes the scheduler before lock-resolver and transport shutdown, matching `KVStore.Close`. |
| store/configuration API | Disabled-by-default `TxnLocalLatches`, positive-capacity validation, and one scheduler shared by cloned clients map source `EnableTxnLocalLatches`, which is documented as a before-use operation, to the construction-time `Config::with_txn_local_latches` builder. Public `TransactionClient::is_latch_enabled` supplies the source query and is also available through the root `KvStore` facade. |
| commit integration | Optimistic transactions acquire all final mutation keys before the first RPC, reject a stale lock with exact typed text, retain the guard through commit, and publish only a successful commit timestamp. Pessimistic and pipelined transactions bypass latches exactly as `txn.go` requires. |

## Complete original-test mapping

The source declares exactly four ordinary tests plus `TestMain`:

| Source declaration | Rust evidence |
| --- | --- |
| `TestWakeUp` | `source_test_wake_up` |
| `TestFirstAcquireFailedWithStale` | `source_test_first_acquire_failed_with_stale` |
| `TestRecycle` | `source_test_recycle` |
| `TestWithConcurrency` | `source_test_with_concurrency` |
| `TestMain` with `goleak.VerifyTestMain` | The Rust scheduler owns no background task; the concurrency port explicitly awaits all ten workers and closes the scheduler. Complete library execution therefore ends with no package-owned detached task. |

The concurrency port preserves the source channel capacity of 100, ten workers, 999 generated transactions, `a` through `h` key table, `[100, 60, 40, 20]` selection chances, and per-transaction uniqueness rule. Native regressions additionally cover dropped-waiter cleanup after partial acquisition, idempotent close/ignored unlock, known Murmur3 and capacity boundaries, configuration validation, shared-scheduler shutdown, and stale optimistic rejection before any RPC.

## Validation boundary

Final validation passed on `nightly-2026-08-22-aarch64-apple-darwin` (`rustc 1.100.0-nightly (c656540d6 2026-08-21)`):

- `/private/tmp/go1.25.12/bin/go test ./internal/latch -count=1`: passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib transaction::latch::tests:: -- --nocapture`: 7 passed.
- Focused latch configuration/lifecycle tests: 4 passed.
- Focused stale optimistic-commit consumer regression: 1 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib source_ --quiet`: 568 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features source_ --quiet`: 565 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --quiet`: 885 passed and one unrelated test remained ignored.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet`: 882 passed and one unrelated test remained ignored.
- `cargo +nightly-2026-08-22 check --all-targets --all-features`: passed.
- `cargo +nightly-2026-08-22 clippy -p tikv-client --lib --all-features --message-format short -- -D warnings`: passed cleanly.
- `cargo +nightly-2026-08-22 doc -p tikv-client --no-deps --all-features --document-private-items`: passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --doc --all-features --quiet`: 51 passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check` and `git diff --check`: passed.

The Rust baseline before this batch is `63eb2e3442cd42da3e515806e142c84a70e5cb5d`; all five source identities, line counts, four ordinary declarations, `TestMain`, and four direct imports were recomputed from the pinned checkout. `/private/tmp/go1.25.12/bin/go test -race` could not link because this extracted toolchain does not contain `runtime/race`; ordinary source execution passes and Rust's complete concurrency/unit gates remain authoritative locally. No live cluster is required for the deterministic local scheduler itself; the consumer regression proves stale rejection occurs before transport dispatch.
