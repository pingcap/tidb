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
- [x] Validate the setup changes; preserve five wider session-suite failures for source-based follow-up rather than claiming a clean full suite.

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


Go also sends `finCopResp` through its unordered response channel. Rust's separate task-finished rendezvous is therefore not removed. The ordinary RPC reply already bypasses callback queues; no blanket pool rewrite is justified by that path.

## Decision Log


2026-09-15: optimize synchronization within the existing ownership model, using native atomic cancellation and lane-specific notifications. Do not alter the number of workers, task window, reply-channel semantics, or Go's task-finished handshake. No new dependency or workload-specific rule is needed.

## Idempotence and Recovery


Preserve concurrent edits and the previous test evidence. Do not reset the worktree or force-push. Only `hparser-integration` is authorized remotely. Start and stop only task-owned benchmark processes; retain data and traces.

## Current Setup-Cost Milestone


Continue the accumulated optimization batch without a full benchmark. Go `pkg/store/copr/store.go:NewStore` retains GOMAXPROCS once. Rust `rpc/execution.rs::go_max_procs` queries the OS on every iterator construction even though the query runtime's size is fixed at first use. Resolve available parallelism once and use the same value for both callers. This does not add a CPU cap or change worker topology.

Go `pkg/sessionctx/stmtctx/stmtctx.go:SQLDigest` normalizes OriginalSQL; `pkg/session/session.go` registers its normalized text with the arbitrator. Rust `dispatch.rs` instead restores an AST with default database qualifiers, uses binding-specific normalization, and computes a discarded SHA256. Replace that with ordinary SQL normalization through a shared session helper. Call it in general execution and the cached point-get path, which currently retains a stale previous key. Preserve empty keys when arbitration is disabled and original prepared SQL when parameters change. No parser, binding or quota algorithm changes are intended.

The existing `lib.rs::session_source_tests::test_mem_arbitrator_session` now covers raw SQL versus restored AST, comments/aliases, a cached point-get following a different statement, and disabling arbitration. It failed before production edits: the key unexpectedly qualified `t` as `test.t` (`/private/tmp/statement-setup-before.log`). It passes after the change. The test uses an enabled arbitrator with the session's `nolimit` policy to inspect key selection without running a process quota worker; this does not claim new arbitration-allocation coverage.

Files changed in this milestone: `rust/crates/tidb-txnkv/src/rpc/execution.rs`, `rust/crates/tidb-session/src/dispatch.rs`, `rust/crates/tidb-session/src/lib.rs`, and this plan. The CPU count remains fixed alongside the existing fixed runtime; live CPU-affinity resizing is not implemented. Ordinary normalization avoids AST restoration, database qualification and discarded hashing. The cached point-get now selects its own key before building its statement context. All changes remain local and uncommitted; the JIT design draft and other contributors' files are preserved.

Exact checks from this checkout:

    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib -j12 test_mem_arbitrator_session -- --test-threads=12
    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-session -p tidb-txnkv -p tidb-distsql --lib -j12 --no-fail-fast -- --test-threads=12
    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all -j12 -- --test-threads=12
    GOMAXPROCS=12 GOFLAGS=-p=12 make -j12 lint
    git diff --check

The initial sandboxed unit run is `/private/tmp/statement-setup-unit.log`; OS memory queries were denied. The authorized repeat is `/private/tmp/statement-setup-unit-reviewed.log`: 30 distributed-SQL and 169 transaction unit tests pass; session has 1718 passed, 5 failed, 209 ignored. All 336 session integration tests pass (`/private/tmp/statement-setup-integration.log`), lint passes (`/private/tmp/statement-setup-lint.log`), and diff/format checks pass. No release build, live deployment or benchmark ran for this increment; performance benefit and the 25% goal remain unverified.

Wider failures are not silently accepted as a clean suite. `tests_core::ddl::modify_column` and two `tests_modify_column_null` cases expect 1265 but receive 1138. Go `pkg/ddl/modify_column.go:1970` calls the admission precheck with `isDataTruncated=true`; `pkg/ddl/column.go:checkForNullValue` returns 1265 there. A later worker check can return 1138, which Rust currently reaches instead. The tests should not simply be changed to 1138. `tests_core::aggregates::grouping_with_rollup` fixes the relative order of tied NULL sort keys, and `tests_harvested_relation_engine::only_full_group_by_pins_by_name_by_where_equality_and_by_candidate_key` expects a fixed group order without ORDER BY; these fail on alternate valid orderings. Follow-up should distinguish these assertion issues from the DDL admission gap using the Go source and existing tests. No production DDL or ordering changes were made in this setup-cost milestone.

## Outcomes & Retrospective


The initial scheduling changes pass 199 unit tests and 666 integration tests (12 existing ignored), release compilation and lint. Logs are `/private/tmp/rpc-wakeup-unit.log`, `/private/tmp/rpc-wakeup-integration-reviewed.log`, `/private/tmp/rpc-wakeup-build.log`, and `/private/tmp/rpc-wakeup-lint.log`. Integration tests require localhost fixture access and `RUST_MIN_STACK=33554432`, already specified in `rust/.cargo/config.toml`; invoking Cargo with a manifest from the parent checkout does not load that directory's config automatically.

The user stopped the premature benchmark and requested substantially more optimization before measuring again. Driver session 15836 exited 143. Partial artifacts in `/private/tmp/tidb-rpc-wakeup.sMDg5a` are not performance qualification. The saved baseline hash is `8f168fed9e7bdc453f009f56df138e74708a4d7b8f113b2d71abfbae62627485`; the first candidate hash `921cd4af1b24da819c2b02f5ff8d338944dbdeadaa8bd52d724b7c8ad07a6461` predates the broader batching change. No 25% or performance claim is made.

Source follow-up: Go `internal/client/conn_batch.go` has one admission queue per store. Rust's shared owner stopped collection at the first different-store admission, fragmenting an A/B/A/B/A burst into singleton batches. `collect_more` now retains other-store submissions in a bounded pending deque and continues same-store collection, stopping at every non-submission lifecycle command. Per-store ordering, deadline checks, receipt ownership and the 128-command wire bound remain unchanged. The existing collection test exercises the interleaved burst and a close-before-next-generation barrier.

Go `pkg/executor/batch_point_get.go:403-444` constructs keys for every selected physical table and calls `BatchGet` once. Rust `KvTable::stored_records_batched` instead grouped keys in a BTreeMap and made one synchronous storage call per partition. The extended existing point-get test failed with four calls versus one (`/private/tmp/cross-partition-batch-before.log`). The implementation now sends one flat key vector and retains output positions, preserving missing/duplicate handles and the unrouted candidate order without cloning each key. The test covers integer and common handles across four partitions.

Go `pkg/executor/join/index_lookup_join.go:582-638,800-832` reuses a 64-byte-capacity encoding buffer across rows and copies each inner key directly into its multi-value map. Rust allocated per-row encoding buffers and retained a second task-sized `(key, row pointer)` vector before map insertion. `Encoder::append_key_in_timezone` now exposes Go's caller-owned append contract, with the existing allocating API delegating to it. Both index-join loops reuse their buffers; the inner loop encodes one datum at a time and inserts directly. The map's arena copies key bytes, so reuse cannot alias stored keys. Existing time-zone behavior is unchanged (index join still uses UTC); this optimization does not claim to fix that separately documented parity gap. The existing Go codec test covers composite append, prefix preservation, buffer reuse and both collation modes. No new test harness or benchmark was added.

The broader batch passes 1,557 unit tests and 1,161 integration tests (196 existing ignored), plus `make -j12 lint` and `git diff --check`. Logs: `/private/tmp/broad-batching-unit-reviewed.log`, `/private/tmp/broad-batching-integration.log`, `/private/tmp/broad-batching-lint.log`. The unit/integration/lint commands in Validation were run with the displayed worker and stack settings. Release compilation was not repeated for this broader batch; the saved candidate remains the earlier scheduling-only binary. No benchmark, new deployment, performance gain or whole-package parity is claimed for these edits.

The pre-fix command was `RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib batch_point_get_is_chosen_only_for_the_shapes_go_accepts -j12 -- --test-threads=12`. The first grouped run additionally exposed an existing assertion requiring one call per partition, now corrected to Go's combined call. The partitioned composite-IN SQL currently uses a scan rather than the batch fast plan; its answers are verified, and common-handle batching is separately checked at the shared reader boundary. Routed/unrouted integer and common handles, duplicate/missing handles and empty input are covered. That planner-selection gap remains open, not hidden by the batching checks.

Files in this increment: `rust/crates/tidb-txnkv/src/rpc/transport_runtime.rs`, `rust/crates/tidb-txnkv/src/rpc/transport_runtime/batching.rs`, `rust/crates/tidb-codec/src/datum.rs`, `rust/crates/tidb-codec/tests/codec_package_source.rs`, `rust/crates/tidb-executor/src/index_lookup_join.rs`, `rust/crates/tidb-executor/src/kv_table.rs`, `rust/crates/tidb-executor/src/access_path.rs`, `rust/crates/tidb-executor/src/driver/tests/point_get.rs`, and this plan. Earlier worktree changes remain intact. Self-review checked lifecycle barriers, bounded lookahead, candidate ordering, map-copy ownership and the append API's unchanged encoding dispatch.
