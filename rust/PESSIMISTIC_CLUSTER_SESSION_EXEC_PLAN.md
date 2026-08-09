# Make wide SQL cluster sessions execute pessimistic transactions correctly

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan follows the repository-wide rules in `AGENTS.md`, including the regression-test requirement for bug fixes and the WIP/Ready validation profiles.

## Purpose / Big Picture

The Rust TiDB wide-SQL server currently accepts `BEGIN PESSIMISTIC` and `SELECT ... FOR UPDATE`, but the production cluster-session path always opens an optimistic transaction. The SQL statement returns rows without taking TiKV pessimistic locks. Under TPC-C, concurrent updates therefore reach optimistic Prewrite and surface error 9007 instead of waiting and serializing like Go TiDB. This is both a compatibility defect and a throughput defect because go-tpc loses transactions that TiDB should complete.

After this plan is complete, a wide-SQL connection whose transaction mode is pessimistic will read, lock, retry, and commit through a real `RealPessimisticTransaction`. A second writer to a row held by `SELECT ... FOR UPDATE` or DML will block until the first transaction commits or rolls back. If a newer committed version is discovered while locking, the whole SQL statement will be retried after advancing `for_update_ts`, and every retry read will use that new timestamp. TPC-C at 16 threads will report no 9007 errors caused by the missing pessimistic path. PD, TiProxy, and TiKV remain unmodified nightly binaries, and no client SQL, transaction boundary, batching, or retry behavior is changed.

## Progress

- [x] (2026-08-08) Reproduced the user-visible TPC-C symptom through TiProxy: the 16-thread, one-warehouse smoke completed with about 14 distinct 9007 optimistic write-conflict events.
- [x] (2026-08-08) Removed TiProxy from the hypothesis: the same conflict behavior reproduces by connecting directly to a Rust TiDB node.
- [x] (2026-08-08) Built the minimal lock probe: connection 1 holds `BEGIN; SELECT ... FOR UPDATE` for five seconds while connection 2 updates the same row. Connection 2 completed in 44 ms instead of blocking.
- [x] (2026-08-08) Traced the production path to `RealClusterTransactions::begin`, which unconditionally creates the optimistic `tidb_exec::cluster_table_storage::SessionTransaction`.
- [x] (2026-08-08) Confirmed the lower transaction tier already has PessimisticLock, pessimistic rollback, pessimistic Prewrite actions, fair locking, lock TTL keep-alive, and statement-error classification.
- [ ] Add a deterministic, agent-runnable regression around the cluster-session transaction seam that fails because the mode and lock request are currently discarded.
- [ ] Add read-at-`for_update_ts` support to the transaction coordinator with targeted tests proving a retry sees the version that caused the conflict.
- [ ] Extract or deepen the existing `MultiStatementTransaction` pessimistic lifecycle so both the bounded real-TiKV node and the wide-SQL transaction worker use one implementation.
- [ ] Pass `SessionTxnMode` from parsed `BEGIN` and the autocommit-off path into `ClusterTransactions::begin`.
- [ ] Collect the raw row and index keys a wide-SQL statement must lock without changing SQL execution semantics.
- [ ] Run DML and locking reads through statement-scoped lock acquisition; on a retryable conflict restore the statement buffer image, advance `for_update_ts`, bind a new snapshot at that timestamp, and rerun the whole statement.
- [ ] Release only locks newly acquired by a statement that fails, while preserving locks and staged mutations from earlier successful statements.
- [ ] Stop lock keep-alive before commit or rollback, and commit with pessimistic actions for every locked mutation.
- [ ] Turn the two-connection real-cluster probe green: the contender must remain blocked until the holder ends.
- [ ] Run targeted Rust tests, `cargo fmt --all -- --check`, scoped Clippy with `-D warnings`, and a release build on `i4i-test-4` only.
- [ ] Roll the release binary to the three Rust TiDB nodes and repeat the 16-thread TPC-C smoke; accept only with zero missing-lock 9007 events.
- [ ] Capture CPU profiles through TiProxy under the corrected TPC-C workload and rank the remaining StockLevel and Delivery costs before performance edits.

## Surprises & Discoveries

- Observation: SQL mode parsing is not the defect. Bare `BEGIN` resolves to the `pessimistic` default, and explicit `BEGIN PESSIMISTIC` is represented by `SessionTxnMode::Pessimistic` in the bounded node.
  Evidence: `rust/crates/tidb-server/src/session_transaction.rs` stores the mode and its targeted tests pass.

- Observation: there are two explicit-transaction implementations. `rust/crates/tidb-exec/src/multi_statement_transaction.rs` supports optimistic and pessimistic transactions but is bound to one `ConfiguredTable` with integer clustered handles. `rust/crates/tidb-exec/src/cluster_table_storage.rs::SessionTransaction` serves the wide-SQL cluster path but is optimistic-only.
  Evidence: `RealClusterTransactions::begin` calls the latter unconditionally, while `real_tikv_node` calls `MultiStatementTransaction::begin(mode, ...)`.

- Observation: merely calling `PessimisticLock` after a statement is not correct. `RealPessimisticTransaction::advance_for_update_ts` advances the lock timestamp, but its current snapshot API delegates to the optimistic coordinator, whose Get and Scan requests are fixed at `start_ts`.
  Evidence: `transaction/coordinator/snapshot_read.rs` writes `version: self.start_ts` into both request types. A lock-conflict retry that reused this snapshot could overwrite the winning value after computing from an old version.

- Observation: the wide executor already stages exact TiKV-format record and index keys in `MutationBuffer`, and statement rollback already restores a byte-for-byte `BufferImage`.
  Evidence: `rust/crates/tidb-executor/src/cluster_storage.rs` stores a key-ordered `BTreeMap<Key, Option<Vec<u8>>>`; cluster-session tests already assert statement-level restoration.

- Observation: the one-warehouse smoke is diagnostic only. Its `tpmC: 42.1` and approximately 15.8-second StockLevel latency cannot be used as a formal benchmark result.
  Evidence: `/mnt/nvme/hparser-bench/evidence/hparser-tpcc-16f5635e-t16-w1-20260808T173740Z` on `i4i-test-4`.

## Decision Log

- Decision: do not add client retries, rewrite single UPDATE statements into batch updates, or alter go-tpc transaction boundaries.
  Rationale: those changes would hide the server compatibility defect and alter TPC-C semantics.
  Date/Author: 2026-08-09, Codex.

- Decision: keep PD, TiProxy, and TiKV on unmodified nightly binaries.
  Rationale: the `hparser-integration` objective is strict Go TiDB compatibility; the defect and fix belong in Rust TiDB.
  Date/Author: 2026-08-09, Codex.

- Decision: reuse and deepen the pessimistic lifecycle already exercised by `MultiStatementTransaction` rather than create a second independent coordinator policy in `tidb-server`.
  Rationale: lock retry, fair-lock conflict handling, lock rollback, TTL keep-alive, and Prewrite actions are consistency policy. Duplicating them would let the bounded and wide paths drift.
  Date/Author: 2026-08-09, Codex.

- Decision: statement retry surrounds SQL execution, buffer staging, lock acquisition, and snapshot binding as one unit.
  Rationale: after a lock conflict, values computed before the lock are stale. Retrying only PessimisticLock or Prewrite can produce a lost update.
  Date/Author: 2026-08-09, Codex.

- Decision: all Rust editing, formatting, compilation, tests, release builds, and deployment validation run on `i4i-test-4:/mnt/nvme/src/tidb-hparser-integration`; Cargo commands are serialized.
  Rationale: this is the user-requested fast repair environment and avoids divergent local artifacts.
  Date/Author: 2026-08-09, Codex.

## Outcomes & Retrospective

The planner fixes and release deployment preceding this plan are complete, and the TPC-C smoke now reaches real transaction contention instead of failing on prepared statements, decoding, unsupported expressions, or panics. The remaining correctness blocker is the missing pessimistic transaction connection described here. No completion claim is made until the real-cluster lock probe blocks correctly and the 16-thread TPC-C smoke reaches zero missing-lock 9007 events.

## Context and Orientation

The production binary boots `ClusterSessionFactory` from `rust/crates/tidb-server/src/cluster_session_node/boot.rs`. Each connection becomes `ClusterServerSession` in `cluster_session_node/mod.rs`. It owns a `MutationBuffer`, a shared `SwappableSnapshot`, and an optional `OpenClusterTransaction`. `with_statement` binds a snapshot, takes a buffer image, executes the SQL driver, unbinds the snapshot, and either publishes or restores staged writes.

The transaction seam is `rust/crates/tidb-server/src/cluster_session_node/transactions.rs`. `ClusterTransactions` opens autocommit snapshots, commits autocommit buffers, and begins an explicit transaction. `OpenClusterTransaction` currently exposes only `snapshot`, `commit`, and `rollback`; it cannot accept a mode or lock keys. `RealClusterTransactions` currently wraps the optimistic-only transaction-thread implementation in `rust/crates/tidb-exec/src/cluster_table_storage.rs`.

The usable pessimistic engine is `rust/crates/tidb-txnkv/src/transaction/pessimistic.rs::RealPessimisticTransaction`. A `for_update_ts` is the timestamp used by a pessimistic statement to check and acquire locks. It starts equal to transaction `start_ts` and advances when a newer commit wins. The SQL statement must reread at the advanced timestamp before recomputing its mutations. A pessimistic action is the per-mutation Prewrite marker telling TiKV to verify an existing pessimistic lock instead of repeating an optimistic conflict check.

`rust/crates/tidb-exec/src/multi_statement_transaction.rs` is the existing consumer of that engine. It already selects optimistic or pessimistic mode, performs lock acquisition, keeps a primary lock alive, releases statement-added locks on failure, and commits with pessimistic actions. Its table/handle-specific planning responsibilities must be separated from its general transaction lifecycle before the wide session reuses it.

The existing red end-to-end probe uses the deployed cluster: TiProxy listens on `i4i-test-4:6000`; Rust TiDB nodes listen on port 4000 on `i4i-test-1`, `i4i-test-2`, and `i4i-test-3`. The probe must also be runnable by direct connection to one Rust node so TiProxy remains excluded from the verdict.

## Plan of Work

First, add a coordinator-level read method that takes an explicit snapshot version while retaining the existing transaction start timestamp for commit state and lock ownership. Refactor the existing Get and Scan bodies so `snapshot_get` and `snapshot_scan` remain exact-start-ts wrappers, while pessimistic statement reads call the versioned helpers with current `for_update_ts`. Lock resolution and GC visibility checks must use the read version, not transaction `start_ts`. Add focused transaction tests that build two committed versions around a timestamp advancement and prove the versioned read returns the newer value while the original snapshot API remains repeatable at `start_ts`.

Second, isolate the general lifecycle from `MultiStatementTransaction`: mode selection, current statement read timestamp, raw-key lock acquisition, lock-failure classification, advancing `for_update_ts`, statement-added-lock rollback, primary-lock TTL keep-alive, pessimistic Prewrite plan, and commit/rollback. Keep configured-table row decoding and prepared-write planning in `MultiStatementTransaction`. Both the bounded node and wide transaction worker must call the shared lifecycle.

Third, deepen `ClusterTransactions::begin` so it receives `SessionTxnMode`, fair-locking configuration, and commit protocol. Deepen `OpenClusterTransaction` so one statement can obtain a snapshot at the transaction's current statement timestamp and submit a deterministic raw-key lock set. Preserve optimistic behavior when the selected mode is optimistic.

Fourth, add a per-statement lock journal at the storage/executor boundary. It must identify raw record and index keys read or mutated by the statement and distinguish keys already held before the statement. DML uses blocking wait semantics. `SELECT ... FOR UPDATE` carries the parsed `NOWAIT` or `WAIT n` policy and locks the returned row keys before rows are returned to the client. Plain SELECT statements collect no locks. Avoid parsing SQL strings with ad hoc text matching; carry structured lock intent from the parser/planner/session.

Fifth, place the pessimistic retry loop in `ClusterServerSession::with_bound_statement`. Each attempt starts from the same pre-statement buffer image and auto-id retry state, binds a snapshot at the transaction's current statement timestamp, executes the SQL, then locks the collected keys. A retryable lock conflict restores the buffer image and session statement state, advances `for_update_ts`, rebuilds/rebinds, and reruns the closure. A nonretryable statement failure releases only locks added by that attempt and leaves the explicit transaction usable. A transaction-scoped failure abandons the transaction and resets the buffer.

Sixth, extend mock-cluster tests to assert mode propagation, lock-key ordering and deduplication, full-statement retry after a simulated conflict, survival of earlier locks and writes, and cleanup of only the failed statement's additions. Add an ignored-by-default real-cluster test or agent-runnable script for the two-connection blocking probe. Run it red before the implementation and green after it.

Finally, run the WIP checks after each milestone and the Ready checks before any completion claim. Build a release binary, record its SHA-256, roll it across the three Rust TiDB nodes one at a time, verify one listener per node, and rerun the diagnostic TPC-C smoke through TiProxy. Only after transaction correctness is green should CPU profiling drive StockLevel and Delivery optimization.

## Concrete Steps

All commands below run on `i4i-test-4`. Enter the Rust workspace and load Cargo into non-login shells:

    cd /mnt/nvme/src/tidb-hparser-integration/rust
    source "$HOME/.cargo/env"

Run the tight cluster-session unit feedback loop with the exact new test filter recorded when the regression is added:

    cargo test -p tidb-server cluster_session_node::tests::transactions::<new_pessimistic_test>

Run coordinator tests after read-version work:

    cargo test -p tidb-txnkv <new_for_update_read_test>

Run the full targeted transaction surfaces, serially:

    cargo test -p tidb-server cluster_session_node::tests::transactions
    cargo test -p tidb-server cluster_session_node::tests::prepared_transactions
    cargo test -p tidb-server cluster_session_node::tests::autocommit_transactions
    cargo test -p tidb-exec multi_statement_transaction
    cargo test -p tidb-txnkv transaction::pessimistic
    cargo test -p tidb-txnkv transaction::coordinator

Run the Rust quality gates:

    cargo fmt --all -- --check
    cargo clippy -p tidb-txnkv -p tidb-exec -p tidb-executor -p tidb-session -p tidb-server --all-targets -- -D warnings
    cargo build --release -p tidb-server

Run the 16-thread diagnostic smoke without changing go-tpc semantics:

    tiup bench:v1.12.0 tpcc run -H 127.0.0.1 -P 6000 -U root -D tpcc_rowid_fix_t16 -T 16 --warehouses 1 --time 30s

Preserve command lines, exit codes, error counts, latency/throughput output, binary hashes, and process/listener checks under a new timestamped directory in `/mnt/nvme/hparser-bench/evidence`.

## Validation and Acceptance

The regression test must fail before the implementation because no lock request reaches a transaction, then pass after the implementation. The coordinator regression must prove a statement retry reads at advanced `for_update_ts`; checking only that a timestamp number advanced is insufficient.

The real-cluster holder/contender probe is green only when the contender remains blocked while the holder owns the lock and completes after holder commit or rollback. A fast 9007, a fast successful overwrite, or a five-second transport timeout are all failures.

Existing optimistic transaction tests must remain green: `BEGIN OPTIMISTIC` retains repeatable reads and reports its conflicts at commit. Autocommit point-get max-ts behavior must remain unchanged. Prepared and text transaction-control statements must select identical modes.

The diagnostic TPC-C run must use 16 threads, preserve stock go-tpc SQL and transaction boundaries, exit zero, and contain zero prepared-statement errors, decode errors, unsupported-expression errors, panics, and missing-lock 9007 events. The one-warehouse result remains diagnostic and is not copied into a formal benchmark comparison.

## Idempotence and Recovery

Targeted tests, formatting, Clippy, release builds, probes, and TPC-C runs are safe to repeat. Never use `git reset`, `git checkout`, or any command that discards the existing staged or unstaged remote work. Before each source edit, inspect both `git diff` and `git diff --cached` for the target file and apply a minimal patch on top.

Release deployment is rolling. Copy the current binary to a distinct backup path before replacing it, stop and start only the one node being updated, verify its SHA and listener, then continue. If a new node fails its health query, restore that node's immediately preceding binary without changing the other two.

A failed statement must be recoverable without reconnecting: restore its `BufferImage`, release its newly acquired locks, and keep earlier transaction state. A transaction-scoped coordinator or keep-alive failure is not recoverable at statement scope; abandon it and return the exact client-visible error.

## Artifacts and Notes

Current deployed candidate SHA-256:

    16f5635ec773677ff52cf2ad6ed68547c4b53939fe94f06237cbe0153979848f

Current diagnostic TPC-C evidence:

    /mnt/nvme/hparser-bench/evidence/hparser-tpcc-16f5635e-t16-w1-20260808T173740Z

Current manual lock-probe logs:

    /tmp/for-update-holder.log
    /tmp/for-update-contender.log

The observed contender duration before the fix is 44 ms while the holder sleeps for five seconds.

## Interfaces and Dependencies

`tidb-txnkv` remains the only layer that emits PessimisticLock, PessimisticRollback, CheckTxnStatus/heartbeat, and pessimistic Prewrite RPC fields. Its versioned snapshot read API must preserve transaction ownership at `start_ts` while accepting a separate read version for pessimistic statements.

`tidb-exec` owns the shared explicit-transaction lifecycle because both server modes already depend on it. The lifecycle accepts raw TiKV keys and structured wait policy; it must not depend on a single table or integer handle.

`tidb-executor` owns `MutationBuffer`, `BufferImage`, `ClusterTableStorage`, and the per-statement access journal because this is where raw keys cross between SQL execution and storage.

`tidb-session` carries structured transaction mode and locking-read intent from parsed AST. It must not send TiKV RPCs.

`tidb-server` owns the connection lifecycle: selecting the mode, binding the per-attempt snapshot, rerunning the statement, deciding whether an error is statement- or transaction-scoped, and exposing exact MySQL errors.

Revision note (2026-08-09): created from the confirmed missing-lock reproduction and current deployed TPC-C evidence; no implementation has yet been claimed.
