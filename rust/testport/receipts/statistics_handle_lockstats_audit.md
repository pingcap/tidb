# `pkg/statistics/handle/lockstats` audit

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob | Disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 54 | `81f80a75142d23e4dbd7488f5cba936685a6d26b` | build metadata inventoried |
| `lock_stats.go` | 305 | `b6465219b46882fa79fd90e5e96ddada68ab4801` | unclaimed: runtime owner absent |
| `lock_stats_test.go` | 336 | `7f6219fd8fe9f8520d373d8d59be931f02f116a4` | six tests inventoried; not ported |
| `main_test.go` | 34 | `52f60bbcecf9bbbcd9331f927f52a4683672d173` | test harness inventoried; not ported |
| `query_lock.go` | 55 | `cdf877d409985781d79fc89004b7585e0214ad8b` | unclaimed: runtime owner absent |
| `query_lock_test.go` | 153 | `93bef1a47a53fffb8bc9f2550a19297121357e5c` | two tests inventoried; not ported |
| `unlock_stats.go` | 218 | `8ccc848466eef012017d07418dbd4a4f46d0a6b2` | unclaimed: runtime owner absent |
| `unlock_stats_test.go` | 365 | `152d2073bf2de09b60ab5706661239fc54ea54fc` | five tests inventoried; not ported |

The package has no generated, platform-specific, benchmark, fixture, or other
support artifacts.

## Package behavior and blockers

The package constructs the `types.StatsLock` implementation over a system
session pool. Every public lock/unlock method synchronizes stats session
variables and wraps its complete mutation in a pessimistic transaction. It
loads current lock rows with the internal statistics foreground context,
inserts table and partition locks, advances stats-meta versions using the
transaction start TS, detects already-locked objects, sorts warning names,
and reports partial success. Unlocking reads accumulated count/modify deltas,
clamps table counts at zero, propagates partition deltas to global table
metadata, deletes lock rows, rejects partition unlock beneath a whole-table
lock, and preserves error ordering. Query methods execute storage reads and
filter requested IDs. The tests exercise all of those SQL sequences, error
paths, diagnostic forms, and the version failpoint.

Rust has the lower-level SQL and system-session contracts, but not the
complete shared `handle/types` package or an ordinary statistics-handle owner
that constructs and exposes `StatsLock`. Landing only free functions would
continue the current disconnected path rather than produce Go's package.

## Removed non-parity carriers

The `lock_messages`, `locked_tables`, and `stats_delta` modules extracted only
three deterministic internals and exposed new public APIs. Their eight tests,
one duplicate batch test, and five ignored empty functions could not detect
transaction, storage, version, error, or lifecycle drift. All were removed.
The package remains explicitly unclaimed.

## Validation

WIP profile: removal of disconnected carriers is checked through the affected
statistics and lock-executor owners.

- `cargo check --locked -p tidb-stats -p tidb-exec`
- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
- `cargo nextest run --locked -p tidb-exec -E 'test(/lock_stats_exec_source/)' --no-fail-fast`
- `rustfmt --edition 2021 --check crates/tidb-stats/src/lib.rs crates/tidb-stats/tests/statistics_part6_source.rs`
- `git diff --check`
