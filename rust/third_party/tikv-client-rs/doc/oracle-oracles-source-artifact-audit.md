# `oracle/oracles` source-artifact audit

This is the atomic completion receipt for client-go package `oracle/oracles`, pinned at commit `52c1e76cec993571493c81de442bcbef90cdc106`. Its Rust owner is `src/oracle/oracles.rs` plus the typed PD timestamp-source adapter, validated with `nightly-2026-08-22`.

## Complete source inventory

The package is exactly eight artifacts and 2,063 lines:

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `local.go` | 155 | `47933f2d5d19f26ded2f5a0341b2665c691505c850964b7e371c85927b45694b` | `LocalOracle` and immediate future |
| `local_external_timestamp.go` | 54 | `84d499237ea4b17667d2777e5182359f1f72b542988cb42e59aef0b4da797588` | `LocalExternalTimestamp` |
| `mock.go` | 173 | `229e4440b80e19e60ffc905f2054237d67624e0e613b85dd03b3999378515bbb` | `MockOracle` and immediate future |
| `pd.go` | 786 | `54ae8a8b497e7c128ab507a9c3741ef29c6ac9e0deb1df3bb8fbf8d113ffb52c` | `PdOracle`, source adapter, refresh/validation tasks |
| `local_test.go` | 87 | `9dcacb7d31fdf4b1c76a5c0bd4eb3bf795b139e838206e28e5b244be0bd87322` | three source-named Rust tests |
| `pd_test.go` | 691 | `7757e0bc2c5b9adff88e0829248b6c5971ce4830a403e2fea2ffd8a83b98ea73` | ten source-named Rust tests |
| `export_test.go` | 92 | `ad5e95ef01f6a4f6afa049b40bc14f53f19dc28faa4f8bcb7eb0235eea2583e1` | injectable timestamp sources, cache/time hooks, deferred loop start |
| `main_test.go` | 25 | `cae0e5a1de2c907c51fc040bd3e5471cd1999088c885fd8390dba203febac626` | explicit refresh-task shutdown regression |

There is no `doc.go`, benchmark, example, fixture, generated source or input, build/platform variant, package metadata, or package-specific build file. Four Go files import the concrete child package directly: `integration_tests/store_test.go`, `internal/locate/region_request_test.go`, `oracle/oracles/local_test.go`, and `tikv/kv.go`.

## Production mapping and differential findings

| client-go surface | Rust behavior and correction |
| --- | --- |
| local oracle | Physical time plus logical increments yields 100,000 distinct timestamps at a fixed millisecond; immediate futures, low-resolution aliases, stale time, expiration, minimum TSO, noop validation, and time hooks match. Go-duration nanosecond overflow is now preserved for extreme TTL/staleness inputs. |
| local external timestamp | Atomic compare/exchange preserves the global-TSO upper bound, monotonicity, idempotence, and exact errors for future or decreasing values. |
| mock oracle | Enable/disable, signed-nanosecond offset, same-physical logical monotonicity, minimum TSO, immediate futures, stale/expiration behavior, and external timestamps match. `UntilExpired` now uses Go's saturated `time.Time.Sub(...).Milliseconds()` semantics instead of pre-truncating both instants. |
| PD construction and TSO | Global cache seeding, optional refresh start, per-scope monotonic caches, async fetch start, minimum/external TSO delegation, exact positive-interval validation, and successful-fetch warnings beyond the source's 30 ms threshold match. The pinned source's `getTimestamp` intentionally calls plain `GetTS(ctx)`; transaction scope partitions the cache but does not alter that RPC at this revision. |
| low-resolution and stale TSO | Sync/async cache hits and misses, first-miss fetch side effect, nanosecond-precise arrival-time estimation, invalid previous-second handling, and non-future concurrent updates match. Invalid asynchronous scope now retains the exact `get low resolution timestamp async fail` error rather than being rewritten as a read-validation error. |
| adaptive refresh | Immediate shrink, preserve margin, 500 ms floor, short-read recovery blocking, nanosecond-precise 20 ms/s recovery, five-minute delay, normal/adapting/recovering/unadjustable transitions, manual interval changes, and source ticker cadence/drop behavior match. |
| read validation singleflight | Validation enablement, invalid/latest/future rules, stale-only adaptation, one retry, scope-keyed coalescing, different-client protection, and cancellation isolation match. The shared fetch now runs in its own task and removes its flight on completion, so canceling the initiating/only waiter cannot cancel or strand the source request. |
| async future lifecycle | PD timestamp work starts immediately, updates the cache on success, records wait latency, rejects a second wait, and is aborted when the owning Rust future is dropped—the native counterpart of canceling the source context. |
| leak harness | `close` wakes the sole refresh task and tests await its finished state. Completed validation flights remove themselves even if their only waiter is canceled; an externally blocked source still governs its background singleflight request's completion, matching client-go. |

## Complete original-test mapping

The source declares exactly 13 ordinary tests plus `TestMain`:

| Source declaration | Rust evidence |
| --- | --- |
| `TestLocalOracle` | `source_test_local_oracle` |
| `TestIsExpired` | `source_test_is_expired` |
| `TestLocalOracle_UntilExpired` | `source_test_local_oracle_until_expired` |
| `TestPDOracle_UntilExpired` | `source_test_pd_oracle_until_expired` |
| `TestPdOracle_GetStaleTimestamp` | `source_test_pd_oracle_get_stale_timestamp` |
| `TestPdOracle_SetLowResolutionTimestampUpdateInterval` | `source_test_pd_oracle_set_low_resolution_timestamp_update_interval` |
| `TestNonFutureStaleTSO` | `source_test_non_future_stale_tso` |
| `TestAdaptiveUpdateTSInterval` | `source_test_adaptive_update_ts_interval` |
| `TestValidateReadTS` | `source_test_validate_read_ts` |
| `TestValidateReadTSForStaleReadReusingGetTSResult` | `source_test_validate_read_ts_for_stale_read_reusing_get_ts_result` |
| `TestValidateReadTSForNormalReadDoNotAffectUpdateInterval` | `source_test_validate_read_ts_for_normal_read_do_not_affect_update_interval` |
| `TestSetLastTSAlwaysPushTS` | `source_test_set_last_ts_always_push_ts` |
| `TestValidateReadTSFromDifferentSource` | `source_test_validate_read_ts_from_different_source` |

The ports retain the 100,000-timestamp uniqueness check, exact expiration values, stale error cases, live three-interval scheduler bounds, 100 non-future races, every adaptive transition/configuration assertion, future-read retry sequence, the complete four-case/five-waiter success/cancellation matrix, normal-read no-adjustment checks, 100-way concurrent cache invariant, and older-shared-result retry. Native regressions additionally cover exact async error identity, extreme Go-duration wrapping, dropped-future cancellation, sole-waiter cancellation, independent flight cleanup, external timestamps, signed-nanosecond mock offsets, and task shutdown.

## Validation boundary

Final validation passed on `nightly-2026-08-22-aarch64-apple-darwin` (`rustc 1.100.0-nightly (c656540d6 2026-08-21)`):

- `/private/tmp/go1.25.12/bin/go test ./oracle ./oracle/oracles -count=1`: parent had no tests; all child tests passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib oracle::oracles::tests::source_test_ -- --nocapture`: all 13 original declarations passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib oracle:: -- --nocapture`: 32 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib source_ --quiet`: 558 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features source_ --quiet`: 555 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --quiet`: 879 passed and one unrelated test remained ignored.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet`: 876 passed and one unrelated test remained ignored.
- `cargo +nightly-2026-08-22 check --all-targets --all-features`: passed.
- `cargo +nightly-2026-08-22 clippy -p tikv-client --lib --all-features --message-format short -- -D warnings`: passed cleanly.
- `cargo +nightly-2026-08-22 doc -p tikv-client --no-deps --all-features --document-private-items`: passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --doc --all-features --quiet`: 51 passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check` and `git diff --check`: passed.

The Rust baseline before this batch is `b6ffad470e8f385fbdc125e4cdc123c032a88f24`; source identity, all eight line counts, and SHA-256 values were recomputed from the pinned checkout. Deterministic injected timestamp sources cover package-owned behavior; no live PD service is required. Concrete PD protobuf transport remains validated by its independent client/transport receipts.
