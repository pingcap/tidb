# `txnkv/rangetask` source-artifact audit

This is the atomic completion receipt for client-go package `txnkv/rangetask`, pinned at commit `52c1e76cec993571493c81de442bcbef90cdc106`. The Rust implementation is in `tikv-client` on `nightly-2026-08-22`.

## Complete source inventory

`git ls-tree -r --name-only 52c1e76cec993571493c81de442bcbef90cdc106 txnkv/rangetask` contains exactly two files:

| Source artifact | SHA-256 | Rust owner |
| --- | --- | --- |
| `txnkv/rangetask/range_task.go` (358 lines) | `7b4bf9e45ecfa73e01ca8761464530fbfc3b1f541af6f491cdccdf1446c6d83f` | `src/transaction/range_task.rs`, `src/stats.rs`, `src/pd/client.rs`, `src/region_cache.rs` |
| `txnkv/rangetask/delete_range.go` (185 lines) | `8f1e175d9bf1ab18c0f4a373d4535eb9541ea95a17ee0e3c173019184c2bd122` | `src/transaction/range_task.rs`, `src/transaction/client.rs`, `src/transaction/requests.rs` |

The package has no `doc.go`, colocated Go test/support file, build-tag or platform variant, generated source/input, fixture, package build file, or metadata file. The repository-level `integration_tests/range_task_test.go` (265 lines; SHA-256 `2d1eaaa41c420168363a1e3a66dfb1226c21d07f5f8744e1d865942615ca3be8`) is the only external source test dedicated to this package; both of its complete case matrices are transcreated in `src/transaction/range_task.rs`.

## Production mapping

| client-go production surface | Rust mapping and integration decision |
| --- | --- |
| Ten-minute progress interval, 128-region group default, and completed/failed metric labels | Native duration/constants in `range_task.rs`; Prometheus gauge and enqueue histogram registration/update sites in `stats.rs`. |
| `Runner`, `TaskStat`, and `TaskHandler` | Public `transaction::range_task::{Runner, TaskStat, RangeTaskHandler}`. The Go callback alias maps to an async trait so handlers can await TiKV operations without blocking an executor thread. |
| `NewRangeTaskRunner` and `NewRangeTaskRunnerWithID` | `Runner::new` and `Runner::new_with_id`; names are owned strings, and an empty explicit identifier falls back to the metric name exactly as the source does. |
| `SetStatLogInterval` and `SetRegionsPerTask` | Public setters preserve the source defaults and the positive region-count assertion. Rust also rejects zero worker concurrency immediately; zero is not a usable source input because it leaves the Go producer permanently blocked with no workers. |
| `NewLocateRegionBackoffer` | Public `new_locate_region_backoffer`, with a fresh cancellation-owned 20,000-ms cumulative source backoff budget. |
| `RunOnRange` producer | `Runner::run_on_range`: empty bounded ranges return before discovery; unbounded ends remain empty; each load owns a fresh backoffer; PD scans are bounded by `regions_per_task`; the final task is clipped to the requested end. `PdRpcClient` supplies the source cache-loading path, including encoded V1/V2 keys, empty/gapped/leaderless response retry, legacy ScanRegions compatibility, and cache insertion. |
| Worker pool/channel | One async worker per configured concurrency and a queue of the same capacity. The producer records enqueue wait around every send attempt. Handler errors contribute their returned stats, cancel shared work, and the result is selected in source worker order. Producer-load errors take precedence after queued workers drain. |
| Counters, metrics, and logging | Completed count and its gauge reset on every run/exit. Failed count and gauge remain cumulative, matching the source's deliberate field/update behavior. Empty/start/progress/load-error/handler-error/final logs use separate metric and log identifiers and redact keys. |
| `storage` | Split along native ownership: generic discovery is expressed by the public `PdClient` bound; DeleteRange transport remains on `transaction::Client`, whose plans own region request routing, API codec, retry, timeout, interceptor, resource-control, and response decoding. No second store abstraction is introduced. |
| `DeleteRangeTask` constructors/state | Public `DeleteRangeTask::{new,new_notify}` stores the client, range, concurrency, mode, and last completed-region count. `execute` updates the count even when a later region fails, preserving the source object's post-error observability. |
| DeleteRange per-region handling | `DeleteRangeHandler` intersects the task with current regions, checks shared cancellation between sends, constructs exact bounds plus `notify_only`, uses a fresh 100,000-ms source retry owner for each regional request, and counts every successful response. Region errors are handled by the shared region-request plan; missing/wrong transport bodies are typed transport errors. A non-empty `DeleteRangeResponse.error` is terminal with the source message. |
| High-level compatibility entrypoints | `transaction::Client::{delete_range,delete_range_with_concurrency,notify_delete_range_with_concurrency}` expose the direct idiomatic `Result<usize>` form. `DeleteRangeTask` provides the reusable source-shaped stateful form. |

Rust future drop is the native equivalent of canceling the outer Go context: dropping `run_on_range` aborts its owned `JoinSet`, while an in-flight handler error uses the explicit broadcast `Cancellation` passed to every handler and retry owner. Go stack-wrapping has no distinct observable Rust type; the original typed error and message are retained.

## Test/support mapping

The original integration test creates 27 mock regions split at `a` through `z`, then runs ten bounded/unbounded/empty/binary ranges at worker concurrencies 1 through 4 and region-group sizes 1 through 5. `original_integration_range_and_batch_matrix` reproduces that complete table, including NUL and `0xff` keys, sorted concurrent results, group clipping, completed counts, and failed counts. `original_integration_error_matrix` reproduces every injected failing subrange at all four concurrencies and proves cancellation, incomplete completion, and one failed region.

Additional source-derived tests cover bounded concurrency, first-error stop, empty-identifier fallback, custom logging cadence, fresh locate budget, enqueue metrics, completed reset, cumulative failed counts, DeleteRange bounds/notify mode, the 100,000-ms per-region budget, successful response counting, terminal server errors, and V2 response decoding. `MockPdClient::with_regions` is test-only support for the original alphabet-split topology; its default behavior is unchanged.

The source package does not contain a DeleteRange live-cluster test or fixture. Its wire behavior is therefore validated at the package's request/routing seams, matching the source test boundary. Live destructive behavior remains part of the final cross-client `tikv` integration gate, not an omitted artifact of this package.

## Consumer inventory

Every pinned source consumer was inspected:

- `tikv/kv.go` maps to the transactional client DeleteRange entrypoints and the public stateful task.
- `tikv/gc.go` and `txnkv/transaction/pipelined_flush.go` consume the now-public generic runner and handler contract; their GC/transaction algorithms remain owned by their separate completed receipts.
- `tikv/split_region.go` consumes the now-public locate backoffer factory; split/scatter orchestration remains owned by `tikv`.
- `integration_tests/range_task_test.go` is fully represented by the two Rust matrices above.

Those downstream algorithms do not belong to `txnkv/rangetask`; their ledger status is unchanged. The complete reusable scheduler and DeleteRange behavior required by them is available without inventing partial ownership.

## Completion gates

The package is complete when the focused range-task and DeleteRange suites, default and all-feature library suites, all-target/all-feature check, rustfmt check, and diff check pass on the pinned toolchain. The exact commands and final counts are recorded in the package ledger receipt.
