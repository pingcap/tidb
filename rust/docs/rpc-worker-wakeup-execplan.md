# Reduce RPC and coprocessor scheduling contention

This living plan follows root `PLANS.md`.

## Purpose / Big Picture


Reduce synchronization and fragmented batching on the generic RPC-to-query response path, and reuse index-join key buffers, without changing cancellation, response ordering or backpressure. This contributes to, but does not establish, the active 25% throughput and latency objective across sysbench, TPC-C and TPC-H.

## Progress


- [x] Read pinned client-go batch response ownership and TiDB coprocessor worker/task-sender ownership; compare with Rust and recent profile artifacts.
- [x] Separate monotonic reply cancellation from mutex-protected delivery ownership.
- [x] Replace task-admission broadcasts with normal/small-lane receiver notifications; retain broadcasts for shutdown/exhaustion.
- [x] Extend existing Go-derived concurrent task coverage and run grouped validation/build/lint for these scheduling changes.
- [x] Stop the prematurely started benchmark on the user's request; do not qualify partial samples.
- [x] Broaden the optimization batch: preserve per-store collection across interleaved admissions, combine partitioned point reads, and reuse index-join key buffers.
- [x] Run grouped correctness checks for the full batch; keep benchmarks stopped until a substantial optimization batch is ready to measure.
- [x] Cache fixed runtime parallelism and replace arbitration AST restoration with ordinary SQL normalization, including cached point gets.
- [x] Validate the setup changes, then resolve the DDL admission mismatch and invalid test ordering assumptions exposed by the wider session suite.
- [x] Index retained physical-table lookups per catalog image and remove coprocessor usage reporting's duplicate table/storage ownership; validate grouped executor/session checks.
- [x] Align direct/unique batch-point initialization and chunk-bounded decoding with Go; verify read counts, error timing and early close.
- [x] Reuse ordinary/legacy/generated point-decoder metadata within each reader, with mixed-format multi-chunk validation.
- [x] Build the accumulated changes once and run a short rotating read-only comparison plus one diagnostic profile; retain the small/noisy results without claiming the performance objective.
- [x] Share timestamp-scoped snapshot values between point and batch reads; preserve max-TS bypass, error/visibility ordering, missing values and real RPC accounting.
- [x] Validate the cache with 587 transaction tests, lint and server compilation (`/private/tmp/snapshot-cache-server-check.log`); reproduce nine unrelated storage failures without the cache and retain them as unresolved work.
- [x] Separate snapshot BatchGet's 5120-key limit from Commit's 16 KiB byte limit; retain completed values across pair-lock/region retries and move routed keys into requests.
- [x] Compare the nine pre-existing tidb-exec failures with Go: correct seven fixture expectations and remove two Rust-only refusal sweeps; pass the grouped package checks without a benchmark.
- [x] Keep index-join NULL rejection chunk-backed in worker and synchronous paths; eliminate the shared handle reader's duplicate key/handle index and validate grouped executor/session checks.
- [x] Resume index-join probes across RequiredRows boundaries, bound worker-result delivery, reuse residual scratch and seed the initial batch from the requested size; verify the regression and join-kind matrix.
- [x] Replace eager index-worker output with prepared inner chunks/maps and one demand-driven consumer; verify residual timing and prepared/failed-task memory ownership.
- [x] Measure the accumulated batch with three short read-only rounds and answer-checked Q11/Q21; capture separate Rust/Go Q21 profiles, preserving the negative/noisy result.
- [x] Compare index-hash versus index-lookup physical-plan selection and worker ownership: the live planner retains both choices but the executor discarded the variant.
- [x] Preserve physical `kind` / `keep_outer_order`; implement outer-hash / inner-probe matching, overlap hash construction with inner fetching, and hand off bounded recyclable output chunks. Keep ordinary lookup demand-driven.
- [x] Deliver unordered index-hash chunks through one bounded ready-result channel across active tasks; preserve ordered/ordinary task channels and verify terminal ownership plus one live Q21 answer check.
- [ ] Implement incremental inner fetching for eligible index-hash join kinds, following Go's 4096-row fetch window. Current output is bounded, but inner fetch still completes per task before probing.

## Context and Source Evidence


The checkout is `/private/tmp/tidb-read-setup.APKpRf/server-build`. Preserve its existing uncommitted chunk/join/index-merge fixes. `go.mod` pins client-go `v2.0.8-0.20260831103552-e4905600583b`; its `internal/client/client_batch.go` uses an atomic canceled flag and a one-result response channel. Rust `rust/crates/tidb-txnkv/src/rpc/batch/completion.rs` already uses a native oneshot, but every cancellation check takes its delivery-state mutex.

Go `pkg/store/copr/coprocessor.go` creates one-slot normal/small task channels, has one sequential task sender constrained by tokens, and wakes receivers through those channels. Rust `rust/crates/tidb-distsql/src/cop_paging/cop_iterator.rs` folds that sender into shared task state but broadcasts on every admission, dequeue and retirement. Preserve its one-slot queues and retirement-based admission window; notify only the lane receiving new work.

Recent profiles are `/private/tmp/tidb-candidate-batch.x5N519/sysbench-running-paths.json` and the matching raw traces. Completion, iterator delivery and synchronization appear in their stacks. Samples labelled Running include waits; these are bottleneck leads, not measured CPU savings.

## Plan of Work and Milestones


First retain the same oneshot terminal owner while moving the canceled flag to an atomic. Listener registration and cancellation remain serialized by the state mutex; listeners execute outside it. Then factor task admission so a worker dequeue or consumer retirement fills only available lane slots, notifies those receivers, and wakes all waiters only when no more tasks can arrive or the iterator closes. Extend the existing Go-derived small-task concurrency test with repeated mixed-lane admission under both ordering modes. Validate all affected crate unit tests and aggregated integration targets together, then build the server and run lint.

## Validation


Run from this checkout with 12 workers:

    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-txnkv -p tidb-distsql -p tidb-codec -p tidb-executor --lib -j12 -- --test-threads=12
    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-txnkv -p tidb-distsql -p tidb-codec -p tidb-executor --test all -j12 -- --test-threads=12
    cargo build --manifest-path rust/Cargo.toml -p tidb-server --bin tidb-server --release -j12
    GOMAXPROCS=12 GOFLAGS=-p=12 make -j12 lint
    git diff --check

Require exact result counts/order, no missed wakeups, no stranded lane, and existing deadline/cancellation/reconnect behavior. Benchmarks must retain binary identities, equal fixture/query checks and alternating Rust/Go controls; preserve negative/tail results. Do not infer 25% or whole-package parity from these tests.

## Surprises & Discoveries


The latest Q21 profiles execute different index-join algorithms: installed Go has `IndexNestedLoopHashJoin` / `indexHashJoinInnerWorker.handleTask`, while Rust has `JoinExec` / `run_index_task` / `build_index_lookup_map`. Thus a shared SQL workload does not isolate equivalent operator costs. Go's ordinary lookup join prepares inner rows/maps before consumer-side probing; its index-hash join is a separate worker algorithm. Preserve that distinction rather than restoring eager output to every Rust index join. The Go binary is the installed nightly, not a build of the current authoritative source. Profile percentages from different profilers are not directly comparable CPU budgets.

Go also sends `finCopResp` through its unordered response channel. Rust's separate task-finished rendezvous is therefore not removed. The ordinary RPC reply already bypasses callback queues; no blanket pool rewrite is justified by that path.

## Decision Log


2026-09-15 ready-result follow-up: Go `runUnordered` reads a shared `resultCh`, whereas `runInOrder` reads the current task's channel. Replace only Rust's unordered hash delivery with a bounded shared channel, retaining one output buffer per worker and the existing admission limit. Track active task count and total charged outer bytes until a terminal result transfers/releases ownership; a worker that exits without a terminal result must wake the consumer with an error, not leave a shared channel waiting forever. Preserve task-local channels for ordered hash and ordinary lookup joins. Verify a later ready task while the first remains unsent, completion/error accounting, and early-close cleanup before proceeding to incremental inner fetching.

2026-09-15 follow-up: the planner already enumerates, costs and retains `PhysicalIndexJoin.kind = IndexHashJoin` and `keep_outer_order` in the live `find_best_task/dispatch.rs`. `driver/physical_builder.rs::build_index_join` discards both. A shared enum variant name is not evidence that the planner lacks index-hash joins. The underlying fix belongs to executor construction and worker execution, not an extra planner enumeration or a workload-specific hint. Go `index_lookup_hash_join.go` builds the outer hash table, probes inner rows with collision verification, tracks per-outer match status, and optionally gathers matches in outer order. Implement that distinction with bounded output ownership; retain ordinary `index_lookup_join.go` consumer-side residual evaluation. The existing storage fork/reader boundaries remain shared. Verify duplicate keys, NULLs, residuals, join sides, ordered/unordered delivery, small output chunks and early-close/error ownership together before another benchmark.

2026-09-15: optimize synchronization within the existing ownership model, using native atomic cancellation and lane-specific notifications. Do not alter the number of workers, task window, reply-channel semantics, or Go's task-finished handshake. No new dependency or workload-specific rule is needed.

## Idempotence and Recovery


Preserve concurrent edits and the previous test evidence. Do not reset the worktree or force-push. Only `hparser-integration` is authorized remotely. Start and stop only task-owned benchmark processes; retain data and traces.

## Current Setup-Cost Milestone


Latest lifecycle alignment: Go `join/index_lookup_join.go:innerWorker.handleTask` constructs lookup content, fetches inner chunks and builds the map; `IndexLookUpJoin.Next` performs the join. The physical Rust builder selects `PhysicalPlan::IndexJoin`, not a separate IndexHashJoin, but previously used the index-hash worker's eager output shape. Workers now return owned inner chunks and their lookup map to the existing resumable consumer. Bounded task prefetch, parallel inner reads/map building and outer order remain; output and ON evaluation happen only as requested. Worker-specific output/residual implementations and the prejoined-result queue are deleted. Worker and synchronous readers share map construction. This avoids retaining a task's complete join fanout or evaluating rows beyond a consumer's early stop; it does not avoid fetching the inner rows of already-prefetched tasks.

Prepared batches carry their chunk-memory charge until installed in the consumer. Drop releases it on abandoned receivers, failed sends or preparation errors; successful installation transfers it to the existing task-release path. Failed workers return the original outer batch so its charge remains owned, and a disconnected worker result explicitly releases the popped task's outer charge. This does not add cancellation/joining of already-running detached workers, and the existing lookup-map allocation accounting is unchanged.

The regression failed before the rewrite because worker preparation evaluated ABS of an overflowing inner value (`/private/tmp/index-worker-prepare-before.log`). It now verifies successful preparation, one successful requested row, the overflow only on the next request, and exact residual-evaluation counts. It also checks prepared-result drops, failed delivery, and failed outer-task ownership followed by Close. The 30-case continuation matrix now uses actual prepared worker batches versus synchronous lookup state, not hand-built joined outputs. Final grouped results: 1315 executor unit, 328 executor integration, 1723 session unit and 336 session integration checks pass (3702 passed, 393 existing ignored), in `/private/tmp/index-worker-prepare-final.log`. Lint passes in `/private/tmp/index-worker-prepare-lint.log`; diff checks pass. This follow-up changes `rust/crates/tidb-executor/src/join.rs`, `join_tests.rs`, and this plan. Exact validation:

    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib -j12 index_worker_preparation_does_not_evaluate -- --test-threads=12
    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-executor -p tidb-session --lib --test all -j12 --no-fail-fast -- --test-threads=12
    GOMAXPROCS=12 GOFLAGS=-p=12 make -j12 lint
    git diff --check

No benchmark, release build, live deployment, commit or push ran for this lifecycle batch. Retained work and error timing are source/test verified; workload throughput and latency benefit are not measured. The 25% cross-workload goal and complete Go parity remain open.

The preceding batching alignment follows Go `IndexLookUpJoin.Next`'s retained `innerIter` and `req.IsFull()` boundary. Rust retains the per-outer probe position, match flag and encoded key across calls, reuses residual scratch, seeds the initial outer batch from the requested size, and fills output across task boundaries. The cursor is restored on errors; Open/Close discard it with the task state. Unmatched padding and semi/anti early termination occur only when an outer row is settled. Its remaining worker-side eager materialization is now removed by the lifecycle change above.

The new Go-derived regression in `join_tests.rs` failed before production changes: requesting one row returned ten (`/private/tmp/index-required-rows-before.log`). It now covers 30 combinations of synchronous/prejoined output, inner/left/right/semi/anti joins, and no/partial/all-rejecting residual predicates, with changing requested sizes, duplicate inner matches and unmatched padding. Grouped checks pass 1314 executor unit, 328 executor integration, 1723 session unit and 336 session integration tests (3701 passed, 393 existing ignored) in `/private/tmp/index-required-rows-tests.log`. The expanded matrix then passes in `/private/tmp/index-required-rows-matrix.log`; lint passes in `/private/tmp/index-required-rows-lint.log`. Changed files for this follow-up: `rust/crates/tidb-executor/src/join.rs`, `join_tests.rs`, and this plan. Exact validation:

    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib -j12 index_join_resumes_one_outer_rows_matches -- --test-threads=12
    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-executor -p tidb-session --lib --test all -j12 --no-fail-fast -- --test-threads=12
    GOMAXPROCS=12 GOFLAGS=-p=12 make -j12 lint
    git diff --check

This is verified chunk/lifecycle behavior, not a measured throughput or latency improvement. No benchmark, release build, live deployment, commit or push ran for the follow-up. The 25% cross-workload objective remains open, and unrelated work remains preserved.

Latest optimization batch: index-join inner NULL rejection now preserves chunk ownership in both the live `join.rs` worker and synchronous paths. Previously these paths materialized every inner column as Datum rows and rebuilt a List solely to apply this predicate. Go `pkg/executor/select.go:SelectionExec.Next` filters chunk rows and `pkg/executor/join/index_lookup_join.go:fetchInnerResults` retains chunks. Rust now composes a selection vector with each incoming chunk's existing logical order, preserving duplicates and original column buffers. All-pass batches allocate no selection; all-rejected batches do not enter the List or masquerade as source EOF. Memory accounting retains the physical chunk-buffer cost rather than estimating copied Datum rows. Three superseded materialization helpers are removed. Partially selected chunks retain their original capacity, not just selected-row payloads; no claim is made that this reduces memory in every selectivity regime.

The shared local handle-batch reader now constructs one flat physical-key array and walks each handle's contiguous key window against the BatchGet result. This removes the duplicate key/handle vector, common-handle cloning per partition and BTreeMap grouping. Caller order, duplicate/missing handles, first-matching-partition behavior and one BatchGet remain. No selected physical partitions produces all missing slots without a storage request. This is a generic lookup-boundary improvement, not a change to remote coprocessor routing. The separate `index_lookup_join.rs` file is not wired into a Rust module, so its apparent pointer-copy hotspot was not edited.

Files changed: `rust/crates/tidb-executor/src/join.rs`, `join_tests.rs`, `kv_table/table_scan.rs`, the existing `driver/tests/point_get.rs`, and this plan. The NULL-selection test covers unchanged payload-buffer addresses, existing reordered/duplicate selections, all-pass/all-rejected chunks, multiple NULL predicates and invalid offsets. The existing batch-point test now also exercises the shared projected reader across four partitions with integer/common handles and duplicates/misses. Grouped validation passes 1313 executor unit, 328 executor integration, 1723 session unit and 336 session integration checks (3700 passed, 393 existing ignored), in `/private/tmp/index-chunk-filter-tests.log`. Lint passes in `/private/tmp/index-chunk-filter-lint.log`; diff checks pass. Exact commands:

    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-executor -p tidb-session --lib --test all -j12 --no-fail-fast -- --test-threads=12
    GOMAXPROCS=12 GOFLAGS=-p=12 make -j12 lint
    git diff --check

No benchmark, new release build, live deployment, commit or push ran for this batch. The changes remove demonstrated copying/materialization, but throughput/latency gains and the 25% cross-workload goal remain unverified. The original dirty files and unrelated JIT draft remain preserved.

Latest fixture-alignment follow-up: all nine previously recorded `tidb-exec` failures are accounted for. Seven were obsolete fixture expectations: Go permits arithmetic/MOD pushdown, so the mock coprocessor's root-only cases now use unsupported TAN (including NOT, projection and ordering); mixed-signedness PB comparisons retain each child's flags; TIMESTAMP decoding explicitly supplies the UTC reader timezone; column metadata uses Go `CharsetNameToID` (utf8mb4 46, not expression collation 45); the analyze fixture supplies its known 100-row count instead of accidentally using Go's unknown-count 0.001 sampling fallback; DROP DATABASE checks the target's absence while preserving mysql. Two Rust-only generic refusal sweeps were removed because they froze old unsupported shapes and port-specific wording. Existing focused Go-derived DDL error/admission tests remain. Removing the sweeps does not implement the remaining CREATE generated-column or other DDL gaps.

Files changed in this follow-up are the seven `rust/crates/tidb-exec/tests/` files `analyze_added_column_source.rs`, `cluster_ddl_alter_source.rs`, `cluster_ddl_source.rs`, `cop_scan_partial_predicate_limit_source.rs`, `prepared_dml_lowering_source.rs`, `result_response_source.rs`, and `wide_scan_selection_source.rs`, plus this plan. No production behavior changed in this follow-up. Source owners: Go `pkg/expression/infer_pushdown.go`, `expr_to_pb.go`, `builtin_compare.go`; `pkg/tablecodec/tablecodec.go:Unflatten`; `pkg/server/internal/column/convert.go:ConvertColumnInfo`; `pkg/parser/mysql/charset.go:CharsetNameToID`; `pkg/executor/builder.go:getAdjustedSampleRate`. The mysql catalog fixture and nearby generated-column tests establish the obsolete DDL expectations.

Grouped validation passes 332 unit and 826 integration tests, with three existing ignored (`/private/tmp/exec-fixture-alignment-final.log`). Lint passes (`/private/tmp/exec-fixture-alignment-lint.log`), and `git diff --check` passes. The first grouped run caught an incorrect expected TAN ordering introduced during the fixture update; the final run corrects it. Exact commands:

    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-exec --lib --test all -j12 --no-fail-fast -- --test-threads=12
    GOMAXPROCS=12 GOFLAGS=-p=12 make -j12 lint
    git diff --check

No benchmark, release build, live deployment, commit or push was performed in this follow-up. Performance gains and whole-package parity remain unverified. The previously observed pessimistic-lock nondeterminism is still open. Unrelated files and the JIT draft remain untouched.

Latest snapshot-batch follow-up: pinned client-go `txnkv/txnsnapshot/snapshot.go:batchGetKeysByRegions` uses `batchGetSize = 5120`, counting keys rather than bytes. Rust previously called `group_keys`, inheriting Commit's 16 KiB limit. `transaction/region_batches.rs` now shares the existing region walk with separate source-derived sizing functions: Commit retains its byte rule and snapshot BatchGet counts keys. Routed key buffers move into wire requests rather than being copied again. Unary and BatchCommands receive limits already match Go's MaxInt64-1; no transport-limit change was needed.

Go `collectBatchGetResponseData`/`batchGetSingleRegion` also preserve clean pairs while retrying only locked keys; response-level errors discard that response's pairs and retry its whole pending batch. Rust's `coordinator/snapshot_read.rs` now retains completed values across rounds, gathers retries from the affected request or lock-owned keys, and consumes the other already-published responses instead of breaking at the first region error. Only complete, visibility-checked results populate the snapshot cache. Region routing/recovery, the existing timeout/backoff budgets and publication-before-wait behavior remain in place. This does not eliminate the existing round-completion barrier or claim full Go backoffer/async-worker parity.

The existing `snapshot_lock_wait_source.rs` fixture now checks 5121 long keys (64 bytes each) and short keys (two bytes), pair-level locks with an empty outer pair key, response-level errors with untrustworthy pairs, successful batches alongside a retryable region error, cache population across retries, and no cache pollution after a fatal later batch. Before the fix, long keys generated 21 requests of 256/.../1 keys instead of Go's 5120/1 (`/private/tmp/snapshot-batch-retry-before.log`). Afterward, both key-width cases produce two requests; the retry case produces 5120/1/5120 without rereading the successful one-key batch. These are injected command-boundary checks, not live wire or workload performance measurements.

This follow-up changes `rust/crates/tidb-txnkv/src/transaction/region_batches.rs`, `rust/crates/tidb-txnkv/src/transaction/coordinator/snapshot_read.rs`, the existing lock-read test, and this plan. Final grouped checks pass 170 unit and 417 integration tests (10 existing ignored) in `/private/tmp/snapshot-batch-retry-final.log`; lint passes in `/private/tmp/snapshot-batch-retry-lint.log`. Exact commands:

    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-txnkv --test all -j12 a_snapshot_read_waits_out_a_live_lock_beyond_four_attempts -- --test-threads=12
    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-txnkv --lib --test all -j12 --no-fail-fast -- --test-threads=12
    GOMAXPROCS=12 GOFLAGS=-p=12 make -j12 lint
    cargo check --manifest-path rust/Cargo.toml -p tidb-server -j12
    rustfmt --check --edition 2021 --config skip_children=true rust/crates/tidb-txnkv/src/transaction/region_batches.rs rust/crates/tidb-txnkv/src/transaction/coordinator/snapshot_read.rs rust/crates/tidb-txnkv/tests/snapshot_lock_wait_source.rs
    git diff --check

No benchmark or release build ran for this batch; the last measured binary below predates both snapshot changes. The earlier nine broader storage failures were left for the fixture-alignment follow-up above. All changes remain uncommitted, with collaborators' work preserved. The 25% throughput/latency objective is still unproven.

Latest measurement: `/private/tmp/tidb-accumulated-check.VK71af/results.md` contains exact commands, binary/source identities, fixture checks, all rounds/tails, and the new raw profile. Release build passes (`cargo build --manifest-path rust/Cargo.toml -p tidb-server --bin tidb-server --release -j12`, log `/private/tmp/tidb-accumulated-release-build.log`). Candidate SHA256 is `e63165d7f620a28a8c3e23d15e7efd2ccba36888c7eab67786248319c0031ba1`. Three rotating 15-second/eight-client sysbench read-only rounds show median TPS 1871.25 baseline / 1893.19 candidate / 2023.54 installed Go; mean latency 4.27 / 4.22 / 3.95 ms. Rust paired throughput changes are -3.59%, +1.17%, +1.08%, with candidate worst latency 17.25 ms versus baseline 13.26 ms. This is not a reliable win or the 25% objective. No full benchmark, TPC-C, or TPC-H ran. The subsequent 10-second Instruments capture is excluded from timings; its 31659 samples still show point-read RPC/wait ownership dominating decoder self frames. Profile guidance prevents counting child-scan waits as aggregation CPU. Production sources were unchanged during measurement; prior grouped tests/lint remain the correctness evidence.

The snapshot-cache follow-up is implemented; it is newer than the measured binary above. Pinned client-go `txnkv/txnsnapshot/snapshot.go:Get`, `BatchGetWithTier`, `UpdateSnapshotCache`, and `SetSnapshotTS` are authoritative. Previously Rust `tidb-exec::SessionSnapshot::get` always routed ordinary reads through coordinator `snapshot_get_at`. The coordinator now owns one native value/absence map shared by point and batch reads; only uncached keys are grouped and dispatched, and batch results have one entry per present key. Read-timestamp changes, including scans and empty batches, discard the old image. MaxUint64 reads never populate it. Errors and post-response GC-visibility failures cannot populate it; fully cached reads follow Go's early return without a new visibility check. Go's 10 GiB soft retention threshold protects the current operation's returned values, with native retained-byte accounting and no eviction walk below the threshold. Returning owned Vec values still copies bytes; this is not a zero-copy cache or a measured performance gain.

`SnapshotGetResult.region` and `.publication` are now optional: cache hits report None and zero physical RPCs instead of inventing a publication. Actual RPC paths retain both. The existing live-2PC receipt assertions were adapted to unwrap their known physical reads, and the repeated-read/start-TS assertion now expects one Get instead of three. Existing lock-read coverage exercises shared point/batch values and misses, duplicates, mixed cached/error batches, caller mutation isolation, scan/point timestamp changes in both directions, max-TS bypass and correct physical counters. A direct cache test covers soft-limit retention, replacement accounting, timestamp cleanup and max-TS bypass. The existing third-party tikv-client cache is tied to that client's private mutation buffer; the production coordinator cannot reuse it without importing its separate transaction owner.

Changed in this follow-up: `rust/crates/tidb-txnkv/src/transaction/coordinator/{mod,snapshot_read}.rs`, tests `snapshot_lock_wait_source.rs`, `start_ts_conflict_fidelity_source.rs`, and `optimistic_2pc_realtikv_source.rs`, plus this plan. The pre-fix repeated-read regression fails with one RPC instead of zero (`/private/tmp/snapshot-cache-before.log`). Final transaction checks pass 170 unit and 417 integration tests, with 10 existing ignored (`/private/tmp/snapshot-cache-txn-final.log`). Lint passes in `/private/tmp/snapshot-cache-lint-reviewed.log`; the first sandboxed attempt could not resolve the Go module proxy. Formatting and diff checks pass. Exact commands from this checkout:

    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-txnkv --test all -j12 a_snapshot_read_waits_out_a_live_lock_beyond_four_attempts -- --test-threads=12
    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-txnkv -p tidb-exec --lib --test all -j12 --no-fail-fast -- --test-threads=12
    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-exec --test all -j12 -- --test-threads=12
    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-txnkv --lib --test all -j12 --no-fail-fast -- --test-threads=12
    GOMAXPROCS=12 GOFLAGS=-p=12 make -j12 lint
    cargo check --manifest-path rust/Cargo.toml -p tidb-server -j12
    git diff --check

The original wider cache run passed 332 unit tests and 819 integration tests, with nine failures and three ignored (`/private/tmp/snapshot-cache-grouped.log`). All nine failures reproduced unchanged with only the cache's two production files temporarily restored to their pre-cache versions (`/private/tmp/snapshot-cache-baseline-exec.log`); the exact cache patch was then restored. Their Go-source triage and current passing results are documented in the fixture-alignment follow-up above. One pessimistic-lock test also failed in the initial grouped run and passed unchanged in the final transaction run; its nondeterminism is not fixed. No new release build, live-TiKV cache validation or benchmark ran for the cache, and the 25% cross-workload goal remains open. All edits are uncommitted; collaborators' files and the JIT draft remain untouched.

Point-decoder reuse is implemented and locally verified. Go `executor.NewRowDecoder` is built per reader (`PointGetExecutor.Init` and the batch builder). Rust's batch pull already decoded only requested rows, but each `KvTable::decode_row_entry` rebuilt V2 column/handle metadata or a general generated-column decoder. That existing dispatch now belongs to reader-owned `PointRowDecoder`: determine the immutable generated-column shape once; initialize each encountered format's metadata lazily; reuse it across Next calls; clear it on Open/Close. The table and statement context remain authoritative, the format/default/collation dispatch is unchanged, and single-row callers use the same implementation without retaining a cross-statement cache. This removes repeated metadata setup; it does not eliminate per-row datums, value copies into chunks, or general-decoder row maps.

Changes in this increment: executor `kv_table.rs` and `access_path.rs`; existing `tests/row_decoder_source.rs` fixtures now run through multi-chunk point reads, including mixed old/new formats, defaults, NULLs, generated values, and reopen. The grouped run passed all executor unit/integration tests and session integration tests, but exposed `group_by_true_is_the_position_one_reference` requiring fixed order without ORDER BY. The session test in `tests_harvested_relation_engine.rs` now explicitly orders its ascending case; the rerun passes all 1,723 session unit tests. Final totals are 3,699 passing tests and 393 existing ignored. Logs: `/private/tmp/point-decoder-reuse-tests.log`, `/private/tmp/point-decoder-reuse-session-final.log`, and `/private/tmp/point-decoder-reuse-lint.log`. Lint, formatting, and diff checks pass. Exact commands:

    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-executor -p tidb-session --lib --test all -j12 --no-fail-fast -- --test-threads=12
    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib -j12 -- --test-threads=12
    GOMAXPROCS=12 GOFLAGS=-p=12 make -j12 lint
    rustfmt --check --edition 2021 --config skip_children=true rust/crates/tidb-executor/src/kv_table.rs rust/crates/tidb-executor/src/access_path.rs rust/crates/tidb-executor/tests/row_decoder_source.rs rust/crates/tidb-session/src/tests_harvested_relation_engine.rs
    git diff --check

At implementation validation time, no release build, deployment or benchmark ran; the later short measurement is recorded above. The 25% objective remains unverified. Changes are uncommitted; other contributors' files remain untouched.

Batch-point pull lifecycle follow-up is implemented and locally verified. Go `pkg/executor/batch_point_get.go:Next` initializes once on first pull, retains encoded `values`, and decodes until the caller's chunk is full. Rust previously fetched and decoded the complete handle set in `HandleSourceExec::open` and ignored `RequiredRows`. It now retains one owned storage-result map plus ordered keys, preserving missing/duplicate/routed/unrouted handles without copying value bytes. Direct-handle and secondary-unique reads initialize on first Next, honor the requested chunk size, and release fetched values on Close. Single point reads keep a single encoded value without allocating a batch map. Batch decode errors leave the cursor at the failed row; single-point reads mark done before decoding, matching their distinct Go owners. The storage batch size is unchanged: all record keys still go through one batched read, and unique-index reads retain separate index/record batches. Only materialization is bounded by the caller's chunk.

The extended existing `batch_point_get_is_chosen_only_for_the_shapes_go_accepts` test failed before production edits because Open made one read instead of zero (`/private/tmp/batch-pull-before.log`). It now verifies zero Open reads, one record fetch across small Next calls, reopen behavior, two unique-index/record batches, early Close before a malformed later row, and repeated failure without skipping that row when pulled. The old hidden-column test now reads actual encoded storage instead of injecting a decoded preload. The initial grouped build found that old fixture and a mistaken test storage-access assumption; both were corrected. Final grouped checks pass 1,312 executor unit, 328 executor integration, 1,723 session unit and 336 session integration tests (393 existing ignored) in `/private/tmp/batch-pull-final.log`. Lint passes in `/private/tmp/batch-pull-lint.log`; formatting and diff checks pass. Exact commands from this checkout:

    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib -j12 batch_point_get_is_chosen_only_for_the_shapes_go_accepts -- --test-threads=12
    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-executor -p tidb-session --lib --test all -j12 --no-fail-fast -- --test-threads=12
    GOMAXPROCS=12 GOFLAGS=-p=12 make -j12 lint
    rustfmt --check --edition 2021 --config skip_children=true rust/crates/tidb-executor/src/access_path.rs rust/crates/tidb-executor/src/kv_table.rs rust/crates/tidb-executor/src/driver/tests/point_get.rs
    git diff --check

The pull-lifecycle increment changes executor `access_path.rs`, `kv_table.rs`, `driver/tests/point_get.rs`, and this plan. Storage/decode errors now arise from Next instead of Open, as in Go. Local tests cover that lifecycle; live TiKV locking behavior was not revalidated. Full benchmarks remain stopped; no release build, deployment or performance gain is claimed. Direct-to-chunk decoding remains an opportunity; per-reader metadata reuse is now implemented in the follow-up above. All accumulated changes remain uncommitted and other contributors' files are preserved.

The next setup batch is implemented and locally validated. The saved `owner-profile.txt` under `/private/tmp/tidb-combined-batching.kuvxqY` includes table-ID lookup and `KvTable::clone` self frames during physical executor construction; these are leads, not a measurement of this revision. Go `infoschema.TableByID` indexes immutable schema metadata. Rust now shares a lazy ID-to-name directory across `Catalog` clones and `CatalogSnapshot`, including clones made before its first initialization. Lookup resolves names against the current image, without retaining another table/storage copy. Both mutation counters and the mutable temporary-row overlay invalidate the directory; an unshared initialization cell is reused. This conservative invalidation also rebuilds after DML mutable access, which can alter IDs in this API. The directory costs O(tables + partitions) memory and one walk per invalidated image; repeated lookups are indexed.

The extended catalog shared-view test checks initialization sharing, ID replacement, logical versus partition IDs, partition read restriction, old snapshot retention, and deletion. Existing session tests cover temporary-table overlays. Go `IndexUsageReporter` consumes IDs and statistics; cop reporting now retains these alone, moving the sole table handle into each scan/index reader. Reporting before child close, pseudo-stat suppression and partition statistics are unchanged. This increment changes `driver/catalog.rs`, `driver/physical_builder.rs`, and `driver/index_usage_reporter.rs` in `tidb-executor`, plus the existing virtual-generated-column session integration test (explicit ORDER BY for an otherwise unordered DISTINCT assertion) and this plan. The first grouped run exposed an incorrect test fixture lookup and that unordered assertion; both are corrected. `/private/tmp/catalog-reader-setup-final.log` passes all 1,312 executor unit, 328 executor integration, 1,723 session unit and 336 session integration tests, with 393 existing ignored. Lint passes in `/private/tmp/catalog-reader-setup-lint.log`; formatting and diff checks pass. Commands from this checkout:

    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-executor -p tidb-session --lib --test all -j12 --no-fail-fast -- --test-threads=12
    GOMAXPROCS=12 GOFLAGS=-p=12 make -j12 lint
    rustfmt --check --edition 2021 --config skip_children=true rust/crates/tidb-executor/src/driver/catalog.rs rust/crates/tidb-executor/src/driver/physical_builder.rs rust/crates/tidb-executor/src/driver/index_usage_reporter.rs rust/crates/tidb-session/tests/group_by_virtual_generated_source.rs
    git diff --check

No release build, deployment or benchmark ran for this batch. It remains uncommitted, and no throughput/latency gain or 25% goal completion is claimed. Instruments guidance was used to distinguish saved self-time frames from inclusive waits; no fresh trace was captured. Its environment check found xctrace/Python, but template listing aborted in the sandbox; that is not evidence about runtime performance.

Follow-up after commit `113c6a689d`: the DDL admission gap is fixed without changing the later rewrite error. Go `GetModifiableColumnJob` checks nullable-to-NOT-NULL changes before index validation and mutations; `checkForNullValue` skips only non-TIMESTAMP to TIMESTAMP conversion and returns 1265 with the new lower-case name and LIMIT-1 row count. Rust now uses a projected cursor with early exit, preserving the lower-level 1138 worker check. Existing tests cover CHANGE naming, row-count semantics, unchanged rows/schema after refusal, and the timestamp exception. The executor test now distinguishes pre-existing NULLs from the lower-level rewrite boundary. The synchronous Rust path cannot prove online DDL races or failover.

Three session tests assumed an order that their SQL did not guarantee: rollup NULL ties, un-ordered GROUP BY, and un-ordered DISTINCT. Their queries now explicitly order the results, including GROUPING(b) as the rollup tie-breaker. Production sorting/aggregation is unchanged; Go's sort implementation does not guarantee stable tie order.

Current validation: 1,312 executor unit tests, 328 executor integration tests, 1,723 session unit tests and 336 session integration tests pass (393 existing ignored in total). The pre-fix NULL regression failed with 1138 instead of 1265 in `/private/tmp/modify-null-precheck-before.log`. Grouped results are `/private/tmp/modify-null-precheck-final.log`; that run exposed the DISTINCT ordering assertion, corrected and rechecked in `/private/tmp/modify-null-precheck-session-final.log`. Lint passes in `/private/tmp/modify-null-precheck-lint.log`. Exact checks:

    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib -j12 tests_modify_column_null -- --test-threads=12
    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-executor -p tidb-session --lib --test all -j12 --no-fail-fast -- --test-threads=12
    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib -j12 -- --test-threads=12
    GOMAXPROCS=12 GOFLAGS=-p=12 make -j12 lint
    git diff --check

Changed files in this follow-up: executor `ddl/alter_table.rs` and `tests_ddl_modify_column_types.rs`; session `tests_modify_column_null.rs`, `tests_core/aggregates.rs`, and `tests_harvested_relation_engine.rs`; this plan. Changes are local and uncommitted. No release build, deployment or benchmark was run, and no performance improvement or whole-package parity is claimed. Other contributors' files remain untouched.


Continue the accumulated optimization batch without a full benchmark. Go `pkg/store/copr/store.go:NewStore` retains GOMAXPROCS once. Rust `rpc/execution.rs::go_max_procs` queries the OS on every iterator construction even though the query runtime's size is fixed at first use. Resolve available parallelism once and use the same value for both callers. This does not add a CPU cap or change worker topology.

Go `pkg/sessionctx/stmtctx/stmtctx.go:SQLDigest` normalizes OriginalSQL; `pkg/session/session.go` registers its normalized text with the arbitrator. Rust `dispatch.rs` instead restores an AST with default database qualifiers, uses binding-specific normalization, and computes a discarded SHA256. Replace that with ordinary SQL normalization through a shared session helper. Call it in general execution and the cached point-get path, which currently retains a stale previous key. Preserve empty keys when arbitration is disabled and original prepared SQL when parameters change. No parser, binding or quota algorithm changes are intended.

The existing `lib.rs::session_source_tests::test_mem_arbitrator_session` now covers raw SQL versus restored AST, comments/aliases, a cached point-get following a different statement, and disabling arbitration. It failed before production edits: the key unexpectedly qualified `t` as `test.t` (`/private/tmp/statement-setup-before.log`). It passes after the change. The test uses an enabled arbitrator with the session's `nolimit` policy to inspect key selection without running a process quota worker; this does not claim new arbitration-allocation coverage.

Files changed in the setup-cost milestone, now committed and pushed in `113c6a689d`: `rust/crates/tidb-txnkv/src/rpc/execution.rs`, `rust/crates/tidb-session/src/dispatch.rs`, `rust/crates/tidb-session/src/lib.rs`, and this plan. The CPU count remains fixed alongside the existing fixed runtime; live CPU-affinity resizing is not implemented. Ordinary normalization avoids AST restoration, database qualification and discarded hashing. The cached point-get now selects its own key before building its statement context. The JIT design draft and other contributors' files are preserved.

Exact checks from this checkout:

    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib -j12 test_mem_arbitrator_session -- --test-threads=12
    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-session -p tidb-txnkv -p tidb-distsql --lib -j12 --no-fail-fast -- --test-threads=12
    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all -j12 -- --test-threads=12
    GOMAXPROCS=12 GOFLAGS=-p=12 make -j12 lint
    git diff --check

The initial sandboxed unit run is `/private/tmp/statement-setup-unit.log`; OS memory queries were denied. The authorized repeat is `/private/tmp/statement-setup-unit-reviewed.log`: 30 distributed-SQL and 169 transaction unit tests pass; session has 1718 passed, 5 failed, 209 ignored. All 336 session integration tests pass (`/private/tmp/statement-setup-integration.log`), lint passes (`/private/tmp/statement-setup-lint.log`), and diff/format checks pass. No release build, live deployment or benchmark ran for this increment; performance benefit and the 25% goal remain unverified.

The wider failures from the setup-cost milestone are resolved by the follow-up above. The NULL expectations remain 1265; the production admission check was corrected. Ordering assertions now have explicit SQL ordering, without adding a production stable-sort requirement.

## Outcomes & Retrospective


Ready-result follow-up: unordered index-hash tasks now publish chunks and terminal results to one bounded shared channel. A later ready task no longer waits for the head task to finish. Ordered index-hash and ordinary lookup retain their task-local channels. Admission counts active tasks, not received chunks, preserving the existing five-worker bound. `IndexUnordered` owns pending outer-row charges, releases them on completion/error/Close, and transfers them back to the consumer when a fork needs the synchronous reader. A sender that exits without a terminal message reports an error so other live senders cannot hide that failure. Chunk buffers retain the preceding swap/recycle and small-request behavior.

The ready-task regression first failed against an extracted version of the existing per-task receiver pattern (`/private/tmp/index-ready-before.log`): the second task had sent a result while the first was unsent, but the head receiver timed out. This was a channel-pattern regression, not a failing live SQL query. The replacement shared queue passes it. Worker tests now exercise both real queue modes, early close, zero-copy delivery, an abandoned worker with another sender still alive, and transfer of a refused fork's memory charge. Grouped checks pass 332 tidb-exec unit, 826 integration, 1321 executor unit, 328 integration, 1723 session unit and 336 integration tests: 4866 passed, 396 existing ignored. Lint, release build, new-module formatting and diff checks pass. Logs: `/private/tmp/index-ready-grouped.log`, `/private/tmp/index-ready-lint.log`, `/private/tmp/index-ready-release.log`.

One read-only, answer-checked Q21 execution passed on the new release binary against the existing SF1 TiKV fixture (`/private/tmp/tidb-index-ready.EucbNn/q21-check.log`). The fixture still has 6001215 lineitems. No data reset, timing comparison, profile or full benchmark ran. Its single timing sample is not performance evidence. Candidate SHA256: `f5ea3d820efed45a87c3ccac64f8700ac818d52056e9b8eb7cb7d631b91de83a`. Before/after the check, the tracked diff hashed to `c755c5b769f8286dc3afdbcf8bf9bf8237b34b6ad7699bf7200c2489e3bb06dd`; the still-untracked new `join/index_hash.rs` separately hashed to `7e543deb082cc32cbc5cffc3e692c9c553db0fd280ae3e10241d186bfacb5e01`. This plan update postdates those hashes. The candidate on port 34042 was stopped and reaped; existing control servers and clusters remain untouched.

Files in this follow-up: executor `join.rs`, `join/index_hash.rs`, `join_tests.rs`, and this plan. Exact checks from this checkout:

    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib -j12 unordered_index_hash_receives_ready_task_without_waiting_for_first -- --test-threads=12
    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-executor -p tidb-session -p tidb-exec --lib --test all -j12 --no-fail-fast -- --test-threads=12
    GOMAXPROCS=12 GOFLAGS=-p=12 make -j12 lint
    cargo build --manifest-path rust/Cargo.toml -p tidb-server --bin tidb-server --release -j12
    rustfmt --check --edition 2021 rust/crates/tidb-executor/src/join/index_hash.rs
    /Users/qiliu/.tiup/components/bench/v1.12.0/go-tpc tpch run -H127.0.0.1 -P34042 -Uroot -Dtpch --sf 1 --queries q21 --count 1 -T1 --check --time 1m
    git diff --check

Incremental inner fetching is still open: Go `supportIncrementalLookUp` admits unordered inner/left/right/anti-semi joins and sets `maxRowsPerFetch = 4096`. Rust's current `fetch_index_task` / `PrefetchedDrain::run` still drains the entire inner task before probing. The next change should retain hash/match status across bounded fetch windows, emit unmatched outer rows only at true inner EOF, and preserve the whole-inner behavior for ordered/other variants. Active-read cancellation also remains incomplete. No 25% gain or whole-package parity is claimed; all accumulated edits remain uncommitted.

Index-hash execution follow-up: `driver/physical_builder.rs` now preserves the planner's existing `IndexHashJoin` selection and ordering contract. New `join/index_hash.rs` builds an outer-row hash table, verifies equality after hash collisions, probes inner chunk rows, tracks semi/outer match status, and gathers per-outer inner pointers for ordered output. Integer keys hash directly from chunks through the existing hash helper, without allocating a byte-key vector per row; a probe retains its hash across duplicate candidates and output boundaries. Forked lookup tasks overlap the CPU-pool hash build with inner I/O, then evaluate joins on their own worker and send bounded chunks. Complete chunks transfer column buffers to the consumer and recycle its old buffers to the worker; smaller requests retain a row cursor. Hash-map, match-pointer, prepared-input and output ownership are released on normal completion, failed delivery and early close. Synchronous inner subtrees use the same matching algorithm. Ordinary index lookup keeps consumer-side ON evaluation and never builds hash-variant output.

The Go-derived traversal regression failed with `[1,2]` instead of inner-probe order `[2,1]` before the algorithm change (`/private/tmp/index-hash-variant-before.log`). The existing five-kind / three-residual / two-preparation-path matrix now also runs ordinary, ordered-hash and unordered-hash modes (90 cases). Additional cases cover NULL-safe keys, injected hash collisions, duplicate outer/inner keys, small requested chunks, worker backpressure, zero-copy buffer identity and early Close. An intermediate run exposed an unrelated GROUP BY test expecting sorted output; `tidb-session/src/tests_column_prune.rs` now explicitly orders by its existing grouped column, leaving its decoded-column assertion unchanged. No production aggregate behavior changed.

Changed in this follow-up: executor `join.rs`, new `join/index_hash.rs`, `join_tests.rs`, `driver/physical_builder.rs`; the single session test SQL; this plan. Earlier accumulated changes and the unrelated JIT design draft are preserved. Final grouped results pass 1318 executor unit, 328 executor integration, 1723 session unit and 336 session integration tests: 3705 passed, 393 existing ignored. Lint, new-module formatting and diff checks pass. Logs are `/private/tmp/index-hash-variant-verified.log` and `/private/tmp/index-hash-variant-lint-final.log`. Commands:

    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib -j12 index_hash_join_builds_outer_and_probes_inner_rows -- --test-threads=12
    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-executor -p tidb-session --lib --test all -j12 --no-fail-fast -- --test-threads=12
    GOMAXPROCS=12 GOFLAGS=-p=12 make -j12 lint
    rustfmt --check --edition 2021 rust/crates/tidb-executor/src/join/index_hash.rs
    git diff --check

Remaining design gaps after the ready-result follow-up are incremental inner fetching and active-read cancellation. Its existing detached I/O lanes can finish already-admitted reads after Close; result-channel shutdown unblocks output workers but does not cancel those reads. These are next implementation work, not full executor/package parity. The later release build and single live answer check are recorded above; no throughput/latency gain is claimed. The 25% cross-workload goal remains active; all edits remain uncommitted.

Latest bounded measurement (2026-09-15): `/private/tmp/tidb-current-batch.Er377Y` contains the raw timing logs, JSON results, separate profiles and analysis. No full benchmark or TPC-C run was performed. Three rotating 15-second, eight-client sysbench read-only rounds gave median baseline/candidate/Go throughput 1892.30/1912.30/2047.99 TPS; Rust mean latency 4.23/4.18 ms and p95 4.57/4.57 ms. Rust throughput pairs were mixed, including an initial regression; the approximately 1.06% median difference is not a reliable gain. Q11/Q21, after one warmup and three rotating measured rounds of six queries each, passed answer checks but took median 2.00588/2.02945/1.91555 seconds including driver startup. The candidate was approximately 1.18% slower than the saved Rust baseline. No 25% claim is justified.

The release build passed. Candidate SHA256 is `496d818936834601ba448b3291ecc62591c63d901370ff51b63d67f0e36603e9`; saved baseline is `8f168fed9e7bdc453f009f56df138e74708a4d7b8f113b2d71abfbae62627485`. Both before and after measurement, the tracked binary diff against HEAD `113c6a689da00af911256e10ed9add90741c19ea` hashed to `28ca7fa142e256cbcc6b40377131c08a0d283f2c878882962f3ec2846bb52d8c` (this results update postdates that hash). Existing data was not reset or modified: both Go and candidate saw sysbench COUNT/MIN/MAX/SUM(k) `100011/1/100278/5010268704`, and the configured random domain remained 100000. The shared TPC-H SF1 fixture had 6001215 lineitems. Timing preceded profiling, with no overlapping workload drivers. TLS/server-build differences limit Go control interpretation.

The ten-second Rust Q21 trace contains 11147 samples. `build_index_lookup_map` has 722 inclusive samples, including hash-table insertion/comparison work; `TableScanExec` has 715 inclusive samples, largely chunk copying under `RemoteRowCursor::append_clean_chunk`. Hash-join probe setup and allocation/copy leaves also recur. Zero named samples for `prepare_index_inner` are not evidence of zero work (inlining can hide the frame). Go's separate profile shows index-hash workers, hash-join probing and lookup-content sorting. Instruments guidance was used to separate self work from inclusive waits; these are investigation leads, not a quantified explanation of the small timing delta. Read `owner-profile.txt`, `go-profile-top.txt`, and `go-profile-cumulative.txt` alongside the raw profiles.

Commands used from this checkout:

    cargo build --manifest-path rust/Cargo.toml -p tidb-server --bin tidb-server --release -j12
    python3 rust/scripts/compare-sysbench.py --server before:34040 --server after:34042 --server go:34000 --database sysbench_current --workload /private/tmp/tidb-sysbench-current.PZ3bEb/install/share/sysbench/oltp_read_only.lua --threads 8 --seconds 15 --rounds 3 --table-size 100000 --sysbench /private/tmp/tidb-sysbench-current.PZ3bEb/install/bin/sysbench --output /private/tmp/tidb-current-batch.Er377Y/sysbench
    python3 /private/tmp/tidb-current-batch.Er377Y/compare-joins.py
    /Users/qiliu/.codex/skills/instruments/scripts/trace-record.sh -d 10 -p 81402 -o /private/tmp/tidb-current-batch.Er377Y/join-candidate.trace
    curl --fail --silent --show-error 'http://127.0.0.1:40080/debug/pprof/profile?seconds=10' -o /private/tmp/tidb-current-batch.Er377Y/join-go.pprof
    python3 /private/tmp/tidb-accumulated-check.VK71af/analyze-owner.py /private/tmp/tidb-current-batch.Er377Y/join-candidate.trace JoinExec prepare_index_inner build_index_lookup_map TableScanExec
    go tool pprof -top -nodecount=35 /private/tmp/tidb-current-batch.Er377Y/join-go.pprof
    go tool pprof -top -cum -nodecount=70 /private/tmp/tidb-current-batch.Er377Y/join-go.pprof
    git diff --check

Each profile used a separate `/Users/qiliu/.tiup/components/bench/v1.12.0/go-tpc tpch run -H127.0.0.1 -P34042 -Uroot -Dtpch --sf 1 --queries q21 --count 1000 -T1 --check --time 25s` workload; Go used port 34000. Profiled timings are excluded from the comparison. The temporary candidate was stopped after capture; existing controls and clusters were preserved. This measurement turn changes only this plan in the repository; previous grouped correctness/lint evidence still applies to the unchanged production sources. All accumulated edits remain uncommitted. The next implementation should address demonstrated operator-design gaps as a coherent batch, without another tiny-change benchmark cycle.

The initial scheduling changes pass 199 unit tests and 666 integration tests (12 existing ignored), release compilation and lint. Logs are `/private/tmp/rpc-wakeup-unit.log`, `/private/tmp/rpc-wakeup-integration-reviewed.log`, `/private/tmp/rpc-wakeup-build.log`, and `/private/tmp/rpc-wakeup-lint.log`. Integration tests require localhost fixture access and `RUST_MIN_STACK=33554432`, already specified in `rust/.cargo/config.toml`; invoking Cargo with a manifest from the parent checkout does not load that directory's config automatically.

The user stopped the premature benchmark and requested substantially more optimization before measuring again. Driver session 15836 exited 143. Partial artifacts in `/private/tmp/tidb-rpc-wakeup.sMDg5a` are not performance qualification. The saved baseline hash is `8f168fed9e7bdc453f009f56df138e74708a4d7b8f113b2d71abfbae62627485`; the first candidate hash `921cd4af1b24da819c2b02f5ff8d338944dbdeadaa8bd52d724b7c8ad07a6461` predates the broader batching change. No 25% or performance claim is made.

Source follow-up: Go `internal/client/conn_batch.go` has one admission queue per store. Rust's shared owner stopped collection at the first different-store admission, fragmenting an A/B/A/B/A burst into singleton batches. `collect_more` now retains other-store submissions in a bounded pending deque and continues same-store collection, stopping at every non-submission lifecycle command. Per-store ordering, deadline checks, receipt ownership and the 128-command wire bound remain unchanged. The existing collection test exercises the interleaved burst and a close-before-next-generation barrier.

Go `pkg/executor/batch_point_get.go:403-444` constructs keys for every selected physical table and calls `BatchGet` once. Rust `KvTable::stored_records_batched` instead grouped keys in a BTreeMap and made one synchronous storage call per partition. The extended existing point-get test failed with four calls versus one (`/private/tmp/cross-partition-batch-before.log`). The implementation now sends one flat key vector and retains output positions, preserving missing/duplicate handles and the unrouted candidate order without cloning each key. The test covers integer and common handles across four partitions.

Go `pkg/executor/join/index_lookup_join.go:582-638,800-832` reuses a 64-byte-capacity encoding buffer across rows and copies each inner key directly into its multi-value map. Rust allocated per-row encoding buffers and retained a second task-sized `(key, row pointer)` vector before map insertion. `Encoder::append_key_in_timezone` now exposes Go's caller-owned append contract, with the existing allocating API delegating to it. Both index-join loops reuse their buffers; the inner loop encodes one datum at a time and inserts directly. The map's arena copies key bytes, so reuse cannot alias stored keys. Existing time-zone behavior is unchanged (index join still uses UTC); this optimization does not claim to fix that separately documented parity gap. The existing Go codec test covers composite append, prefix preservation, buffer reuse and both collation modes. No new test harness or benchmark was added.

The broader batch passes 1,557 unit tests and 1,161 integration tests (196 existing ignored), plus `make -j12 lint` and `git diff --check`. Logs: `/private/tmp/broad-batching-unit-reviewed.log`, `/private/tmp/broad-batching-integration.log`, `/private/tmp/broad-batching-lint.log`. The unit/integration/lint commands in Validation were run with the displayed worker and stack settings. Release compilation was not repeated for this broader batch; the saved candidate remains the earlier scheduling-only binary. No benchmark, new deployment, performance gain or whole-package parity is claimed for these edits.

The pre-fix command was `RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib batch_point_get_is_chosen_only_for_the_shapes_go_accepts -j12 -- --test-threads=12`. The first grouped run additionally exposed an existing assertion requiring one call per partition, now corrected to Go's combined call. The partitioned composite-IN SQL currently uses a scan rather than the batch fast plan; its answers are verified, and common-handle batching is separately checked at the shared reader boundary. Routed/unrouted integer and common handles, duplicate/missing handles and empty input are covered. That planner-selection gap remains open, not hidden by the batching checks.

Files in this increment: `rust/crates/tidb-txnkv/src/rpc/transport_runtime.rs`, `rust/crates/tidb-txnkv/src/rpc/transport_runtime/batching.rs`, `rust/crates/tidb-codec/src/datum.rs`, `rust/crates/tidb-codec/tests/codec_package_source.rs`, `rust/crates/tidb-executor/src/index_lookup_join.rs`, `rust/crates/tidb-executor/src/kv_table.rs`, `rust/crates/tidb-executor/src/access_path.rs`, `rust/crates/tidb-executor/src/driver/tests/point_get.rs`, and this plan. Earlier worktree changes remain intact. Self-review checked lifecycle barriers, bounded lookahead, candidate ordering, map-copy ownership and the append API's unchanged encoding dispatch.
