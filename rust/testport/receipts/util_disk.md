# `pkg/util/disk` — Go-master parity audit receipt

Status: complete dependency-closed audit against current Go `master`; the Rust
owner now exposes Go's discardable constructor contract. This remains one
bounded package claim inside the ongoing repository parity goal.

## Go-master inventory

| Artifact | Lines | Blob |
| --- | ---: | --- |
| `pkg/util/disk/BUILD.bazel` | 38 | `50185bc9af68165e36a342315b487e0f109c2e5e` |
| `pkg/util/disk/tempDir.go` | 127 | `c798ff6a34a3da8cb612bd37ae5d72af07638d8e` |
| `pkg/util/disk/tracker.go` | 30 | `4def0cea71fe91fc51c294aec6b55830483f3125` |
| `pkg/util/disk/tempDir_test.go` | 55 | `76e207eadd6b52230e9f40d744178cfb8d27aa58` |
| `pkg/util/disk/main_test.go` | 33 | `056351761b00edd47fa8f09dc3577361e9f13124` |

Comparison authority: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02). The five-artifact,
283-line package is unchanged at that authority. Every production, test, and
BUILD artifact was read in full before editing.

There is no `doc.go`, fixture, testdata, benchmark, fuzz target, generated
source, or build/platform source variant. `main_test.go` only installs TiDB's
Go test setup and goroutine-leak exclusions; Rust's native test harness starts
none of those Go goroutines, so it needs no executable support shim.

## Go-to-Rust mapping

| Go contract | Rust owner |
| --- | --- |
| `Tracker = memory.Tracker` | `tidb_util::disk::Tracker` |
| `NewTracker` | `tidb_util::disk::new_tracker` |
| `NewGlobalTracker` | `tidb_util::disk::new_global_tracker` |
| `CheckAndInitTempDir` plus singleflight | `disk::check_and_init_temp_dir`, serialized over the complete check/init operation |
| `InitializeTempDir` | `disk::initialize_temp_dir`: mkdir `0750`, exclusive `_dir.lock`, read directory, asynchronous stale-entry cleanup preserving `_dir.lock` and `record` |
| `CleanUp` | `disk::clean_up` releases the global directory lock |
| `CheckAndCreateDir` | `disk::check_and_create_dir` with recursive `0750` creation |
| private `checkTempDirExist` | private `check_temp_dir_exist` |
| `TestRemoveDir` | `disk::temp_dir::tests::test_remove_dir`, including ten concurrent reinitializers |
| Bazel library/test targets | `tidb-util::disk` and its source unit test |

Production consumers now follow Go ownership: server startup initializes the
directory and cleans it up on return; chunk-by-chunk and row-by-row spill file
initialization rechecks it; memory-usage alarm record directories use
`check_and_create_dir`; the global spill tracker uses `new_global_tracker`.
Server startup also reuses the existing direct translation of Go
`Config.UpdateTempStoragePath`; the duplicate server-local endpoint encoder and
its duplicate unit test were removed.

## Removed non-Go package surface

`tidb_util::disk` previously exported `SpillStorage`, its spec/encryption/error
types, quota text, and an immutable policy abstraction absent from Go's disk
package. The still-required cross-package startup/config/executor integration
seam moved to `tidb_util::spill_storage`; its duplicated directory lease and
stale-file sweep were deleted in favor of the canonical disk functions. Six
Rust-only lease/quota/encryption tests attached to that duplicate authority
were removed. The server-local spill-path encoding test and
`memoryusagealarm`'s private copy of `CheckAndCreateDir` were also removed.

The two public tracker constructors carried explicit Rust-only `#[must_use]`
annotations. The new `return_values_may_be_ignored_like_go` regression failed
before the fix with two `unused_must_use` errors; removing those annotations
lets Go-style discarded constructor calls compile without weakening global
lint policy.

## Validation (Ready profile)

Commands run from the repository root unless noted:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/disk -count=1
    (cd /tmp/tidb-go-latest-c605 && PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/disk -count=1)
    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib disk::tests::return_values_may_be_ignored_like_go --offline --locked -- --exact
    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib disk::temp_dir::tests::test_remove_dir --offline --locked -- --exact --nocapture
    cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
    git diff --check

The current and detached latest-master Go suites pass; both focused Rust
regressions pass. Ready formatting, pinned repository lint, and diff hygiene
pass. The Bazel preparation gate and failpoint toggling are not required
because this batch changes only Rust and documentation.

## Risk and unverified targets

The risk boundary is process-global directory locking and asynchronous stale
cleanup. The source test covers deletion and concurrent reinitialization; the
chunk test covers the real spill consumer. Windows and unsupported-target
runtime locking are not executed on this macOS host. Pre-existing warnings in
unrelated crates remain unchanged.

## Follow-up: `SpillStorage::open` creates its own root (2026-09-09)

Go's `disk.InitializeTempDir` creates the GLOBAL configured temp directory
(`tempDir.go`), and every spill file lives under it. This port's `SpillStorage`
can be rooted at a DIFFERENT path: the standalone executor roots one under
`std::env::temp_dir()` (`mem_quota.rs:167`), and a server roots one under the
configured endpoint/UID path. `SpillStorage::open` only applied the quota
check, so that path was never created and the first `DataInDiskByChunks` spill
failed with `SpillFailed("No such file or directory ... /defaultChunkDataInDiskByChunksPath-...")`.

`open` now calls `std::fs::create_dir_all(&spec.path)` before the quota check
(idempotent, and it makes the capacity probe see the real directory).

Regression: the new `spill_storage::tests::open_creates_the_storage_directory`
fails before and passes after. This unblocked 16 executor tests, all of which
failed on the same missing directory:
`hash_agg::parallel::tests::parallel_hashagg_spills_partial_results_and_finishes`,
`hash_agg_spill_tests::test_random_fail`,
`join::merge_path_tests::{a_large_duplicate_group_spills_and_matches_the_unspilled_result,
merge_close_unbinds_its_spill_action, right_merge_spills_its_left_inner_group,
spilled_merge_cross_product_honors_required_rows}`,
`join::spill_tests::{a_spilled_build_side_answers_exactly_the_unspilled_rows,
a_spilled_build_side_keeps_null_and_miss_semantics,
a_spilled_outer_join_pads_exactly_where_the_unspilled_one_does,
a_spilled_preserved_build_side_scans_unmatched_rows,
close_unbinds_the_spill_action_from_the_session_tracker,
the_read_back_buffer_does_not_accumulate_across_a_spilled_probe}`,
`mem_quota::tests::{disabling_tmp_storage_detaches_the_session_from_global_disk_quota,
statement_disk_usage_reaches_the_one_startup_global_tracker}`, and
`tests_merge_join_in_disk_source::{merge_join_in_disk_rows_under_log_quota,
shuffle_merge_join_in_disk_rows_under_log_quota}`.

Ready validation: `tidb-executor` lib serialized 1197 passed / 50 failed
versus 1181 / 66, the 16 above and no additions; `tidb-util --lib` focused
regression passes; `cargo check --locked --all-targets` clean;
`rustfmt --edition 2021 --check` clean on the changed file;
`git diff --check -- rust`. The remaining
`hash_agg_spill_tests::test_get_correct_result` still fails, now on its own
assertion that a spill file existed while the aggregation ran.
