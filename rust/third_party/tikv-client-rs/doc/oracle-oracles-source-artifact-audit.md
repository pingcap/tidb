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
| adaptive refresh | Immediate shrink, preserve margin, 500 ms floor, short-read recovery blocking, nanosecond-precise 20 ms/s recovery, five-minute delay, normal/adapting/recovering/unadjustable transitions, manual interval changes, and source ticker cadence/drop behavior match. The short-read marker now preserves the source's forward-only `UnixMilli` value: sub-millisecond precision is discarded and a backward wall-clock sample cannot reduce the marker. |
| read validation singleflight | Validation enablement, invalid/latest/future rules, stale-only adaptation, one retry, scope-keyed coalescing, different-client protection, and cancellation isolation match. Flight identity now uses the raw transaction-scope string exactly like `singleflight.Group.Do(opt.TxnScope, ...)`: empty and explicit `global` scopes retain distinct in-flight PD requests even though both share the normalized global timestamp cache. The shared fetch runs in its own task and removes its flight on completion, so canceling the initiating/only waiter cannot cancel or strand the source request. |
| async future lifecycle | PD timestamp work starts immediately, updates the cache on success, records wait latency, rejects a second wait, and is aborted when the owning Rust future is dropped—the native counterpart of canceling the source context. |
| leak harness | `close` wakes the sole refresh task and tests await its finished state. Completed validation flights remove themselves even if their only waiter is canceled; an externally blocked source still governs its background singleflight request's completion, matching client-go. |

## Complete original-test mapping

The source declares exactly 13 ordinary tests plus `TestMain`:

| Source declaration | Rust evidence |
| --- | --- |
| `TestLocalOracle` | `source_go_oracle_oracles_local_test_TestLocalOracle` |
| `TestIsExpired` | `source_go_oracle_oracles_local_test_TestIsExpired` |
| `TestLocalOracle_UntilExpired` | `source_go_oracle_oracles_local_test_TestLocalOracle_UntilExpired` |
| `TestPDOracle_UntilExpired` | `source_go_oracle_oracles_pd_test_TestPDOracle_UntilExpired` |
| `TestPdOracle_GetStaleTimestamp` | `source_go_oracle_oracles_pd_test_TestPdOracle_GetStaleTimestamp` |
| `TestPdOracle_SetLowResolutionTimestampUpdateInterval` | `source_go_oracle_oracles_pd_test_TestPdOracle_SetLowResolutionTimestampUpdateInterval` |
| `TestNonFutureStaleTSO` | `source_go_oracle_oracles_pd_test_TestNonFutureStaleTSO` |
| `TestAdaptiveUpdateTSInterval` | `source_go_oracle_oracles_pd_test_TestAdaptiveUpdateTSInterval` |
| `TestValidateReadTS` | `source_go_oracle_oracles_pd_test_TestValidateReadTS` |
| `TestValidateReadTSForStaleReadReusingGetTSResult` | `source_go_oracle_oracles_pd_test_TestValidateReadTSForStaleReadReusingGetTSResult` |
| `TestValidateReadTSForNormalReadDoNotAffectUpdateInterval` | `source_go_oracle_oracles_pd_test_TestValidateReadTSForNormalReadDoNotAffectUpdateInterval` |
| `TestSetLastTSAlwaysPushTS` | `source_go_oracle_oracles_pd_test_TestSetLastTSAlwaysPushTS` |
| `TestValidateReadTSFromDifferentSource` | `source_go_oracle_oracles_pd_test_TestValidateReadTSFromDifferentSource` |

The 13 Rust identities above are independently selectable definitions rather than comments, aliases, or one grouped test. Mechanical declaration reconciliation reports 13 Go tests, 13 Rust definitions, and zero missing, extra, or duplicate identities. `TestMain` is an explicit harness disposition because Rust tests await package-owned tasks directly instead of invoking Go's process-wide goleak wrapper.

The ports retain the 100,000-timestamp uniqueness check, exact expiration values, stale error cases, live three-interval scheduler bounds, 100 non-future races, every adaptive transition/configuration assertion, future-read retry sequence, the complete four-case/five-waiter success/cancellation matrix, normal-read no-adjustment checks, 100-way concurrent cache invariant, and older-shared-result retry. Native regressions additionally cover exact async error identity, extreme Go-duration wrapping, dropped-future cancellation, sole-waiter cancellation, independent flight cleanup, external timestamps, signed-nanosecond mock offsets, task shutdown, raw-scope validation-flight identity, and forward-only Unix-millisecond adaptive markers.

The two latest differential regressions were red before their production fixes. `source_validation_singleflight_keeps_empty_and_global_scope_keys_distinct` timed out after 250 ms waiting for the third source call because empty and explicit `global` validation requests incorrectly coalesced. `source_adaptive_short_read_time_is_monotonic_unix_milliseconds` observed a 1.234 ms marker where client-go stores 1 ms and also guards against a later backward-clock sample.

## Validation boundary

Final validation passed on `nightly-2026-08-22-aarch64-apple-darwin` (`rustc 1.100.0-nightly (c656540d6 2026-08-21)`):

- `env GOMODCACHE=/private/tmp/client-go-txnlock-module-cache GOCACHE=/private/tmp/client-go-go-cache /private/tmp/go1.25.12-full/bin/go test ./oracle/oracles -count=1`: passed in 3.412 s.
- `env GOMODCACHE=/private/tmp/client-go-txnlock-module-cache GOCACHE=/private/tmp/client-go-oracles-race-cache /private/tmp/go1.25.12-full/bin/go test -race ./oracle/oracles -count=1`: passed in 4.535 s.
- `cargo test --no-default-features --lib source_go_oracle_oracles_ -- --nocapture`: all 13 independently named source tests passed.
- `cargo test --all-features --lib source_go_oracle_oracles_ -- --nocapture`: all 13 independently named source tests passed.
- `cargo test --no-default-features --lib oracle::oracles::tests -- --nocapture`: all 29 package tests passed.
- `cargo test --all-features --lib oracle::oracles::tests -- --nocapture`: all 29 package tests passed.
- `cargo nextest run --config-file config/nextest.toml --all --no-default-features`: 1,409 tests passed and two configuration-specific tests were skipped.
- `cargo nextest run --config-file config/nextest.toml --all --all-features --lib --status-level fail --final-status-level fail`: 1,384 tests passed and six configuration-specific tests were skipped.
- `make check`, `make doc`, `cargo fmt --all -- --check`, and `git diff --check`: passed.
- Mechanical source declaration/definition reconciliation reports 13/13 exact identities with zero missing, extra, or duplicate ports.

The Rust baseline before this independent re-audit batch is `44a054d0aff4088c9c8f584eb7313a6733c98dee`; source identity, all eight line counts, and SHA-256 values were recomputed from the pinned checkout. Deterministic injected timestamp sources cover package-owned behavior; no live PD service is required. Concrete PD protobuf transport remains validated by its independent client/transport receipts.
