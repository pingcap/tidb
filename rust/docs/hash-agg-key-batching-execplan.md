# Share chunk-wide hash-aggregation key construction

This living plan follows root `PLANS.md`.

## Purpose / Big Picture


Use Go's chunk-wide grouping-key construction in both Rust hash-aggregation modes. Reuse typed column encoding and buffers instead of serial row-wise expression dispatch, retaining the existing per-group spill decision. This is a generic optimization toward the active cross-workload 25% objective, not a performance or package-completion claim.

## Progress


- [x] Read Go `aggregate.GetGroupKey`, serial `HashAggExec.execute`, partial-worker `updatePartialResult`, and both Rust paths.
- [x] Extend the existing evaluation-order test to reproduce serial divergence.
- [x] Extract shared reusable key buffers and integrate both paths, including selected rows and cop partial group-value output.
- [x] Run grouped executor checks and lint; leave benchmarks stopped during this optimization batch.
- [x] Build one release candidate containing this change and the accumulated earlier optimizations.
- [x] Measure the accumulated batch once across all three workloads, then capture diagnostic Rust/Go profiles separately.
- [x] Remove redundant aggregation control-only pool tasks and empty in-memory final buckets; validate this as the start of the next optimization batch, without another benchmark.

## Surprises & Discoveries


The complete combined measurement at `/private/tmp/tidb-combined-batching.kuvxqY/results.md` did not show a reliable speedup. TPC-H warm wall time changed -0.22%, sysbench median throughput +0.60%, and TPC-C paired throughput changes were -4.52%, +3.83%, -10.26%. The 25% objective remains unmet. Separate Instruments and Go CPU captures are diagnostic only. Rust's apparent SortExec share largely includes child aggregation and scan operations, not sorting: only 515 of 6129 sort-containing samples descend through SortPartition::sort_impl; 2526 include HashAggExec::next. Raw trace and owner breakdown remain in the measurement directory. These sample counts include waits and are not removable CPU estimates.

## Decision Log


- Decision: Retain Go's configured partial lanes, bucket hashing, reusable chunk ownership and spill barriers, but do not submit a native task to an idle lane merely to detach its maps. A queued map barrier can be completed by its current task after folding, without another pool handoff. Omit final tasks only for provably empty in-memory buckets after the default global group is installed; spilled sources retain their existing workers. Rationale: Go waits for in-flight chunks before accessing spill state (`agg_hash_executor.go:fetchChildData`, `spillIfNeed`); its goroutine lifecycle does not require a new task for map collection. No workload-specific threshold or serial fallback is introduced. Date/Author: 2026-09-15, Codex.

## Next Milestone


Extend the existing single-chunk and empty-global-group tests in `hash_agg/parallel.rs` to demonstrate redundant task execution. Then extract partial map detachment from `PartialWorker::process`, complete idle and queued control barriers without re-enqueueing, and filter empty in-memory final buckets. Preserve panic/error publication, FIFO ordering, resource return before fold, and worker completion on Close. Do not hold a lane mutex while evaluating expressions. Barrier reply channels have capacity one and exactly one send, so reply publication cannot block. Run the two tests before edits, then the executor unit/integration suites and lint as one grouped check after all edits. Existing tests cover multiple spill rounds, DISTINCT, early Close, cancellation, default rows and RequiredRows behavior. Preserve all unrelated work, especially `plan-cache-jit-design.md`.

## Outcomes & Retrospective


The grouping-key change is locally validated but its accumulated measured benefit is not established. The task-scheduling change is now implemented in `hash_agg/parallel.rs`. The extended existing tests failed before the change: single-chunk input ran five partial workers instead of one, and empty input ran five instead of zero (`/private/tmp/hash-agg-handoffs-before.log`). They now pass, with the empty global aggregation retaining exactly one final output holder. All 1312 executor unit tests and 328 executor integration tests pass (184 existing ignored). Existing tests exercise repeated spills, early close and cancellation as well as aggregation answers. Logs: `/private/tmp/hash-agg-handoffs-unit.log`, `/private/tmp/hash-agg-handoffs-integration.log`, `/private/tmp/hash-agg-handoffs-lint.log`.

Commands run from this checkout were the unit/integration/lint/diff commands in Validation below. The pre-change reproduction used `RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib -j12 hash_agg::parallel::tests:: -- --test-threads=12`. Lint initially could not access its pinned Go tool dependency through the network sandbox; the authorized retry succeeded. No Go files or generated build metadata changed. Review confirmed that idle map access requires ownership of the worker under its lane lock, queued barriers stay FIFO, expression work stays outside that lock, and reply publication cannot block on the fresh capacity-one channel. Empty-bucket filtering happens after the global default group is installed and never examines spilled files eagerly.

The running release candidate predates the task-scheduling change. No new release build, live SQL run, throughput/latency gain or full-package parity is claimed for this individual increment. The next performance check remains deferred until further coherent optimizations have accumulated; the active 25% objective is not achieved.

## Source and Design


Checkout: `/private/tmp/tidb-read-setup.APKpRf/server-build`. Go `pkg/executor/aggregate/agg_util.go:106` evaluates a complete grouping expression column before encoding it. Both `agg_hash_executor.go:761` and `agg_hash_partial_worker.go:updatePartialResult` call it before aggregate updates. Rust's `hash_agg/parallel.rs::PipelineKeyBuffer` already follows that order; `hash_agg.rs::fold_chunk` instead interleaves grouping expressions and aggregate arguments per row. Reuse the existing parallel key encoding in a shared helper rather than introducing a second implementation. Retain direct integer/string column handling and the parallel single-integer map representation. Serial cop partial output must retain evaluated group datums without evaluating expressions a second time. Owned grouping keys must not alias reused buffers.

Preserve per-group creation and spill checks. Go serial aggregate-state memory deltas accumulate once per chunk; do not move per-group allocation charges to the chunk boundary. Shared column reads must not recursively acquire the same column lock. Existing cancellation, spill, output and close tests remain required.

## Validation


Run from this checkout, with 12 workers and the stack size from `rust/.cargo/config.toml`:

    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib group_key_expressions_precede_aggregate_inputs -j12 -- --test-threads=12
    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib -j12 -- --test-threads=12
    RUST_MIN_STACK=33554432 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --test all -j12 -- --test-threads=12
    GOMAXPROCS=12 GOFLAGS=-p=12 make -j12 lint
    cargo build --manifest-path rust/Cargo.toml -p tidb-server --bin tidb-server --release -j12
    git diff --check

The first test must show Go's column-before-aggregate order in both execution modes. Existing scalar-codec oracle tests cover selected rows, signed/unsigned keys, collation, decimals and buffer reuse. Full executor checks cover spill rounds, DISTINCT, aggregate errors and partial/final integration. Build one combined release candidate after these checks; no benchmark has been restarted for individual edits.

## Decisions and Recovery


Preserve the dirty worktree and existing fixes. No Go or generated files change. Do not reset, force-push or touch other contributors' servers. Only `hparser-integration` is authorized remotely. No benchmark is needed to establish the demonstrated evaluation-order mismatch; speed remains unverified until the eventual grouped measurement.

## Outcomes


The existing ordering test failed before the production change: serial Rust read `a,b,c,a,b,c,a,b,c`, whereas Go completes each grouping column before aggregate arguments (`a,a,a,b,b,b,c,c,c`). Evidence: `/private/tmp/hash-agg-key-batch-before.log`. It passes after the change as part of 1,312 executor unit tests. All 328 executor integration tests pass (184 existing ignored), and lint plus diff checks pass. Logs: `/private/tmp/hash-agg-key-batch-unit-reviewed.log`, `/private/tmp/hash-agg-key-batch-integration.log`, `/private/tmp/hash-agg-key-batch-lint.log`. An initial compilation caught a remaining reference to the removed serial-only key-shape field; the parallel plan now derives that unchanged shape at its own construction boundary.

New shared owner: `rust/crates/tidb-executor/src/hash_agg/group_key.rs`. Integration and test changes: `rust/crates/tidb-executor/src/hash_agg.rs`, `rust/crates/tidb-executor/src/hash_agg/parallel.rs`. The shared builder replaces the old serial encoding loop and duplicate parallel implementation, not an added fallback. It borrows each direct integer/string column once, including composite keys and selection vectors. Parallel key-buffer memory is accounted after preparation, as Go does around GetGroupKey; serial keeps Go's existing per-group allocation charges and chunk-wide aggregate-state charges. Cop partial output retains evaluated datums without repeating expressions. Close releases the serial key buffers. Tests compare both retained-datum and direct-column encoding with the scalar Go-codec oracle.

Risks and limits: grouping-key buffers now retain one allocation per row up to the input chunk size, as in Go, rather than one serial scratch key; performance and peak memory effects require the combined measurement. Shared column encoding no longer re-enters a column read lock for every integer cell. Spill, cancellation and result checks pass, but this is not full Go-package parity or production qualification. The active 25% throughput and latency goal remains unachieved.

The combined release build succeeded in 82 seconds (`/private/tmp/accumulated-batching-build.log`). Saved candidate: `/private/tmp/tidb-rpc-wakeup.sMDg5a/server-batched`, SHA256 `e2c268a77072b423abed720f02c048c231c1f3a8c8ab56bbd03e60e0c8085683`, from detached HEAD `2fcb2c84db2e68e89f21a65cc894b1e7912e8531` plus the local changes before the subsequent aggregation-handoff optimization. It contains the RPC scheduling, interleaved-store batching, combined partition reads, index-join buffer reuse and shared aggregation-key changes. It was measured once as an accumulated batch; current results and binary identities are in `/private/tmp/tidb-combined-batching.kuvxqY/results.md` and `identities.md`. No code was committed or pushed. Do not rebuild or benchmark the new handoff optimization separately.
