# Keep one native transaction owner

This living ExecPlan follows repository `PLANS.md`. It continues the user's follow-Go instruction after the protocol/Variables integration. Keep Progress, Surprises & Discoveries, Decision Log and Outcomes & Retrospective current.

## Purpose and acceptance


TiDB master embeds client-go's KVTxn and shares its lock resolver and buffer. Rust SQL, internal writers and coprocessor reads must likewise delegate to client-rust, with one commit/rollback/heartbeat engine, one resolver/cache/cleanup pool and one transaction MemDB. The incoming performance work through `f143926e3e`, plus the concurrent lookup-range optimization `f36f98d51f`, must stay effective. This is atomic integration and retirement of existing owners, not a claim of complete Go-package transcreation or benchmark parity.

## Source and inventory


Go master is `51a1a4abfc192a91f98fe968ad87eced9221f663`. It pins client-go `v2.0.8-0.20260928031501-8edb23f6c7ee` and kvproto `v0.0.0-20260820070758-623e58e60fa9`. Native client master is `8b890e2b0e1c40842f91dd865783431cf986a811`. The complete artifact inventories for the Go transaction, snapshot, resolver, unionstore and TiDB driver packages are in `native-transaction-integration-inventory.json`, including original tests and support/build artifacts. Earlier integration receipts are recoverable from commit `3e3cdcf0f8` at `rust/docs/transaction-consolidation-execplan.md` and `rust/docs/client-rust-transaction-boundary-execplan.md`; their validation is historical, not evidence for this new tree.

TiDB driver ownership is in `pkg/store/driver/txn/txn_driver.go`; no package doc.go exists. The native dependency supplies the transaction and resolver packages. Rust `transaction/client.rs` and `driver/client_bridge.rs` adapt TiDB transport, timestamps, errors and publication evidence. SQL `cluster_storage.rs` forwards buffer access and keeps statement metadata. None of those adapters may decide a second 2PC, lock-resolution or heartbeat protocol.

## Progress


- [x] Pull both remotes and refresh Go master; retain seven incoming allocation reductions and fast-forward the later eighth optimization without conflicts.
- [x] Restore the complete published native dependency and remove five absorbed patches.
- [x] Merge the existing native adapters with current protocol and performance changes.
- [x] Remove the alternate transaction coordinator, buffer, heartbeat and resolver algorithms.
- [x] Reproduce six native-buffer acquisitions for a two-entry batch, then pass with one acquisition and unchanged rollback behavior.
- [x] Verify native library, buffer, transaction, embedded and SQL regression gates.
- [x] Reproduce and fix optimistic mode selection and lock resolution from both Tokio runtime flavors.
- [x] Complete coprocessor and real-server validation and self-review the integration diff.
- [x] Validate the concurrent lookup integration (two targeted tests and all affected targets); prepare the reviewed atomic integration for the mandatory delivery gates below.
- [ ] Replace residual RPC-type lifetime inference with explicit caller/store/background operation contexts in a subsequent native boundary change.

## Plan and decisions


The restored coordinator depended on private snapshot-statistics hooks. Keeping those hooks exposed would perpetuate the second engine; therefore dependency update and engine retirement must be atomic. Use the existing public native interfaces. Keep only the four Tonic/Prost compatibility patches, regenerate protobuf output through the sync script, and keep build caches out of the vendor tree. No generated output is hand-edited.

Use a three-way integration of the reviewed adapters from `3e3cdcf0f8` against the restoration `32b55c041c`, preserving new HEAD edits. Resolve transaction mutation conflicts by retaining `into_parts` and retiring the duplicate encoder/sorter: native MemDB is the canonical mutation owner. Bound explicit commits must directly commit that buffer, never drain it into another mutation vector. Unbound compatibility calls retain owned draining. Keep batched staging, clone-free write metrics, borrowed table handles and point-read physical-ID access. Exclude unrelated historical changes to coprocessor summary collection and unordered scans from this integration.

Use native MemDB size/length metadata for write metrics, as Go does. Batch staging borrows the native transaction once, including its flags and SQL statement metadata. Single-key and batched SQL staging share the same implementation. Complete protocol types introduced by `20850f9017` eliminate bridge protobuf transcoding; retain ordinary encoding only at the RPC wire boundary.

## Validation and acceptance


No Go, Bazel or Go module inputs change. Rust test workflows need no Go failpoint toggling or Bazel preparation. A new isolated checkout would still follow the repository preparation policy. Run Cargo commands from `rust/`.

    cargo test --locked -p tidb-executor --lib owned_batch_borrows_the_native_buffer_once
    # failed before the batching repair: 6 acquisitions rather than 1
    cargo test --locked -p tidb-executor --lib cluster_storage::tests
    # 17 passed, including the regression and statement/savepoint rollback
    cargo check --locked -p tidb-server -p tidb-txnkv -p tidb-distsql -p tidb-unistore --all-targets
    # passed after public API adaptation and merge resolution
    cargo test --locked --manifest-path third_party/tikv-client-rs/Cargo.toml --lib -- --test-threads=1
    # 1,394 passed, 2 existing ignores
    cargo test --locked -p tidb-txnkv --lib
    # 136 passed, 1 existing ignore
    cargo test --locked -p tidb-txnkv --test all
    # 419 passed, 2 previously recorded region-cache failures, 10 ignored
    cargo test --locked -p tidb-txnkv --test lock_resolver_source --test snapshot_lock_wait_source --test snapshot_scan_page_deadline_source --test region_error_recovery_source
    # 78 passed, including native resolution from both Tokio runtime flavors
    cargo test --locked -p tidb-txnkv --lib native_transaction_starts_optimistic
    # failed before the mode repair; passed in the subsequent library run
    cargo test --locked -p tidb-txnkv --test lock_resolver_source synchronous_lock_recovery_runs_inside_both_tokio_runtime_flavors
    # failed before the runtime repair; passed in the subsequent resolver run
    cargo test --locked -p tidb-unistore --test client_transaction
    # 15 passed
    cargo test --locked -p tidb-exec --lib multi_statement_transaction::tests
    # 11 passed
    cargo test --locked -p tidb-server --lib cluster_session_node::tests::transactions
    # 25 passed
    cargo test --locked -p tidb-executor --lib handle_lookup
    # 2 passed on the combined f36f98d51f delivery tree
    cargo test --locked -p tidb-distsql --test all
    # 236 passed, the same 20 baseline failures, 2 ignored

`cargo check --offline` updated three root lockfile dependency entries for the restored adapters/tests. The source-only vendor sync does not ship a standalone Cargo.lock, so `cargo generate-lockfile --offline --manifest-path third_party/tikv-client-rs/Cargo.toml` generated that ignored test artifact before locked native testing. Root `make lint` passed. Run `git diff --check`, commit using `TERM=xterm git -c core.hooksPath=hooks commit`, and confirm its locked server build. Immediately before push, rerun `cd rust && cargo build --locked -p tidb-server`, then push `HEAD:hparser-integration` without force.

## Surprises & Discoveries


The dependency refresh exposed private snapshot-statistics calls in the restored coordinator, confirming split ownership. Cargo also ran a stale protocol build-script binary referencing already-removed projected schemas; touching the unchanged build.rs forces recompilation. Do not resurrect missing protocol projections to satisfy stale build output.

The transaction aggregate still reports `region_cache_source::stale_merge_parent_does_not_evict_newer_split_child` and `region_cache_source::stale_same_region_loader_result_is_rejected_without_eviction`. Both were independently recorded as failing before the earlier consolidation; this integration does not claim to resolve region-cache ownership or those tests.

The first real TiKV sysbench run attempted all eight text/prepared workload cells. Prepared read/write samples and the DDL concurrency load exposed a real failure: the resolver adapter rejected every entered Tokio runtime, including the blocking sections used by coprocessor workers. The fixed adapter runs on the same native resolver runtime, yields multithreaded Tokio workers with `block_in_place`, and uses a scoped waiting thread for current-thread callers. It does not introduce another retry, routing or lock owner. Both runtime flavors have a failing-before/passing-after regression.

The internal-writer suite also exposed an order-dependent test assumption: `a_live_deadlock_failure_is_recorded_before_it_reaches_sql` expected record ID 1 after clearing the global history, but observed 2 after another test inserted a record. Both Go `DeadlockHistory.Clear` and the Rust implementation preserve the ID allocator. The assertion now requires an allocated positive ID; its record count, error code and full wait-chain checks remain unchanged. All 11 internal-writer tests pass after the test correction.

Review also found `new_pessimistic` beneath a comment promising Go's optimistic default. Go `NewTiKVTxn` starts with `isPessimistic == false`; TiDB selects pessimistic mode later. A regression now verifies actual native mode before and after the existing session promotion. An exploratory assertion about empty wire `pessimistic_actions` was discarded after reading Go `buildPrewriteRequest`, which allocates skip-check actions for optimistic mutations too; that representation is not the mode contract.

## Decision Log


The user's continuation authorizes proceeding with Go's one-owner design while preserving newer work. The integration is based on actual owner boundaries and targeted regressions, not the title of the restoration commit. Native algorithms and public API fixes are synchronized from the published repository; compatibility-only patches remain local. Snapshot/routing/transport adapters remain until their complete native coverage is verified. The bridge's remaining transaction RPC-type lifetime inference is a separately identified gap; do not describe this integration as full lifetime parity.

## Real-cluster validation


Build from `rust/` with `cargo build --locked --release -p tidb-server`. From the repository root, run:

    PATH=/Users/qiliu/.tiup/bin:/opt/homebrew/bin:/opt/homebrew/opt/mysql-client/bin:$PATH SYSBENCH_RUST_SERVER=/Users/qiliu/projects/tidb/rust/target/release/tidb-server SYSBENCH_AUTH_USER=root SYSBENCH_AUTH_HOST=% SYSBENCH_RUN_TIME=3 SYSBENCH_SAMPLES=3 SYSBENCH_WARM_BUDGET=120 SYSBENCH_OUT_DIR=/private/tmp/native-consolidation-sysbench-20260930-fixed bash rust/scripts/run-sysbench-ladder.sh

The harness owns its local cluster and cleans up its processes, ports and TiUP data on exit. It is a measurement harness and returns zero even for failed workload rungs; inspect its `FAIL`/`FATAL` lines and sample counts. Three three-second samples per cell are a bounded local comparison, not a production performance claim. The Go control is the harness's `v9.0.0-beta.2.pre-nightly`, not Go master. The measured release binary contains this transaction integration over `f143926e3e`. While the post-fix run was in progress, the branch advanced to `f36f98d51f` with an independent lookup-range optimization; it was fast-forwarded without touching this integration. Validate that combined delivery tree with the lookup tests and mandatory locked server builds. The benchmark is not a timing measurement of that later lookup change. The pre-fix run is retained separately at `/private/tmp/native-consolidation-sysbench-20260930`; the post-fix run passed all 24 Rust samples (eight cells, three each), all 24 Go control samples, matching initial/post-run row checksums, all 24 manual SQL/index checks, and all six concurrent DDL/load checks. There are no `FAIL` or `FATAL` lines. The cleanup trap completed with no remaining owned ports. TPC-C, TPC-H and YCSB are not measured by this harness.

## Outcomes & Retrospective


The owner integration and its regression gates are verified; the two region-cache and 20 DistSQL failures remain exactly the recorded baseline failures. The concurrent lookup update passed both targeted tests and the combined all-target check. Logs are `/private/tmp/native-{batch-red,buffer-green,consolidation-check,integration-txnkv-lib,integration-txnkv-all,integration-client-lib,integration-embedded,integration-internal,integration-sql,integration-distsql,integration-lint}.log`. The first real-server run verified matching row checksums and all 24 manual SQL/index checks, but failed prepared read/write and concurrent DDL load on the runtime-boundary defect described above. Its timing samples are not acceptance evidence. Performance evidence currently establishes a structural reduction from six native-buffer accesses to one per batch, constant-time buffer metrics and removal of encode/decode conversions. The post-fix local sysbench comparison below measures this integration but establishes neither performance parity nor a before/after gain over the starting branch. Rust remains slower for point reads and write-heavy workloads in this small single-thread workload. TPC-C, TPC-H and YCSB were not run. Real TiKV fault coverage and broad Go differential tests remain separate gates.

Local sysbench receipt (microseconds per statement, text `disable` and prepared `auto`; conditions and release baseline are specified above):

| Workload | ps mode | Rust us/stmt median [min..max] | Go us/stmt median [min..max] | Rust excess (of medians) | Rust qps med | Go qps med |
| --- | --- | --- | --- | --- | --- | --- |
| `oltp_point_select` | disable | 167.37 [152.53..170.32] n=3 | 125.24 [125.08..126.73] n=3 | **42.12 us** | 5974.94 | 7984.47 |
| `oltp_point_select` | auto | 154.58 [145.52..165.70] n=3 | 117.37 [111.66..117.69] n=3 | **37.21 us** | 6469.02 | 8519.81 |
| `oltp_read_only` | disable | 203.87 [196.73..211.94] n=3 | 214.69 [214.11..215.58] n=3 | **-10.82 us** | 4905.20 | 4657.92 |
| `oltp_read_only` | auto | 142.03 [140.17..146.55] n=3 | 187.89 [184.61..188.65] n=3 | **-45.86 us** | 7040.64 | 5322.13 |
| `oltp_write_only` | disable | 174.67 [174.01..178.51] n=3 | 162.51 [153.28..163.16] n=3 | **12.16 us** | 5725.05 | 6153.59 |
| `oltp_write_only` | auto | 161.28 [159.82..164.95] n=3 | 146.37 [141.17..150.02] n=3 | **14.92 us** | 6200.35 | 6832.18 |
| `oltp_read_write` | disable | 337.30 [322.78..339.37] n=3 | 216.47 [214.96..256.99] n=3 | **120.83 us** | 2964.69 | 4619.54 |
| `oltp_read_write` | auto | 219.85 [217.98..223.27] n=3 | 177.79 [171.84..182.41] n=3 | **42.06 us** | 4548.63 | 5624.60 |


## Recovery


The native dependency, adapters and removed engines form one atomic integration. Do not restore a second engine or snapshot hook patch to fix an adapter test. Preserve the shared protocol integration and incoming allocation reductions. Never force-push over a concurrent update; fetch, reconcile and repeat affected tests/build gates.

## Follow-up integration milestone (2026-09-30)


The integration was restored after revert 2471be70e9 at the user's explicit request. The dependency is now client-rust 884589f. Follow-up fixes, current validation, unresolved cache/RPC and assertion ownership, and the pending background-lifetime patch are recorded in remove-extra-storage-policies-execplan.md. Earlier validation and delivery statements above describe their original milestone, not a complete parity claim for this follow-up.
