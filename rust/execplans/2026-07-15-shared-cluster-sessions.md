# Shared catalog sessions for the transaction source suite

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current as work proceeds.

Reference: `PLANS.md` at the repository root; this plan follows its required format.

## Purpose / Big Picture

The existing Rust seed exposes one `tidb_exec::Database`, so every test has one catalog and one session conflated into the same object. Go's `pkg/session/test/txn/txn_test.go:346 TestErrorRollback` proves a different contract: four independent sessions concurrently operate on one MockStore table, and all successful autocommit updates become visible in the shared table. After this slice, callers can create a shared `Cluster`, derive independent `Session`s, and run the Go test's actual thread topology without sharing session variables or simulating an interleaving.

The scope is deliberately the smallest trustworthy architectural seam. It covers a versioned shared catalog, autocommit compare-and-swap publication, and bounded complete-statement retry. It does not claim explicit transaction write buffers, TiKV lock management, distributed retry errors, global variables, shared auto-ID allocation, or a production wire server. Those require a real transactional storage layer and remain explicit next work.

## Progress

- [x] (2026-07-15) Read `PLANS.md`, the rewrite design, `Database`, and the complete remaining `pkg/session/test/txn` source inventory.
- [x] (2026-07-15) Identified `TestErrorRollback` as the earliest source test that directly proves shared storage plus independent sessions; earlier remaining tests require a larger retry/config/global surface.
- [x] (2026-07-15) Add the `Cluster` and `Session` ownership boundary in `tidb-exec`, using short-lock snapshot and version-CAS publication.
- [x] (2026-07-15) Add a source-ordered, actually concurrent Rust port of `TestErrorRollback`, with observed CAS conflicts/retries.
- [x] (2026-07-15) Add a one-worker SQL transcript corpus and ledger evidence; the unit test, not the sequential corpus, is the multi-session proof.
- [x] (2026-07-15) Run focused WIP validation with twelve jobs: the shared-cluster tests, all executor unit tests, strict executor Clippy, and the ledger check pass.
- [x] (2026-07-15) Publish source-defined catalog effects independently of statement outcome, including an erroring multi-table DROP retried after stale-version CAS, and prove that an erroring DML with no table delta does not advance the catalog version.
- [x] (2026-07-15) Re-run focused WIP validation after the parallel `tidb-expr` edit became coherent: six shared-cluster tests, all 229 executor unit tests, strict executor Clippy, the table differential, the ledger check, and owned-file rustfmt check pass.
- [x] (2026-07-15) Extract the single-session seed transaction lifecycle from `Database` into source-owned `transaction.rs`, move the mirrored lifecycle tests to `tests/transaction.rs`, and add a regression proving rollback preserves nontransactional autocommit/isolation settings.
- [x] (2026-07-15) Re-run the transaction extraction WIP gate with twelve jobs: 48 focused transaction tests, six shared-cluster tests, all 230 executor unit tests, table and query differentials, strict all-target executor Clippy, and workspace formatting pass.

## Surprises & Discoveries

- Observation: `Database` is intentionally single-session and contains `Rc<RefCell<...>>` session facilities, so placing it directly behind `Arc<Mutex<_>>` would not permit source-style threads and would mix session state.
  Evidence: `rust/crates/tidb-exec/src/lib.rs` declares per-session user variables, RNG, sequence state, and a source-owned `TransactionState` inside `Database`.
- Observation: `TestErrorRollback` has no explicit transaction or retry assertion; each loop attempts a duplicate autocommit INSERT and then an autocommit UPDATE. Its observable assertion is exactly `c2 = 4 * 20` after four goroutines finish.
  Evidence: `pkg/session/test/txn/txn_test.go:346-376`.
- Observation: the source deliberately ignores the INSERT result, so exactly one worker can insert `(1, 1)` successfully before later attempts report duplicates.
  Evidence: source uses `_, _ = tk.Exec("insert into t_rollback values (1, 1)")`; the first real threaded Rust run returned `OK` for that statement.
- Observation: a full-table copy/publish while holding one mutex would serialize execution and prevent a stale-write conflict from existing at all.
  Evidence: architectural review before validation required a short-lock snapshot plus version compare-and-swap, so concurrent workers execute locally before publication.
- Observation: `Database::Clone` is shallow for `Rc<RefCell<...>>` session cells such as user variables, RNG, sequences, statement IDs, and auto-ID state.
  Evidence: `Database` declares these fields as `Rc<RefCell<_>>`; a discarded working clone could otherwise leak a side effect into its source session.
- Observation: SQL outcome and catalog effects are not equivalent. Go's `DROP TABLE ta, tb, tc` removes `ta` and `tc` but returns an error for missing `tb`.
  Evidence: `rust/difftests/corpus/table/drop_table.txt` and its Go-produced golden record `ERR`, followed by errors selecting both existing targets.
- Observation: the documented table differential is owned by the `difftest-result-tests` package, not the `difftest` package.
  Evidence: Cargo rejected `cargo test -p difftest --test table_diff` with the owning-package hint; the corrected command passed one test.
- Observation: the current Rust seed has only two transaction phases that it can honestly represent: idle, or active with a rollback catalog image and savepoints. Go's `LazyTxn` separately represents invalid, pending-TSO, and valid-KV states.
  Evidence: `pkg/session/txn.go` owns `invalidTxn`, `PendingTxn`, and `ValidTxn`; the extracted `rust/crates/tidb-exec/src/transaction.rs` deliberately owns only `TransactionPhase::{Idle, Active}` and documents the missing protocol boundary.

## Decision Log

- Decision: introduce a separate `Cluster` with a typed shared catalog and a separate `Session` object rather than changing every `Database` field to interior mutability.
  Rationale: catalog ownership and session ownership become explicit, the type can be sent to real worker threads, and the single-session executor remains stable for its existing differential corpus.
  Date/Author: 2026-07-15 / Codex.
- Decision: use the existing `Database` execution path as the statement evaluator while holding the shared-catalog mutex, then publish only successful autocommit statement catalog changes.
  Rationale: this was superseded before validation because holding the mutex across evaluation serializes away the source's retry surface.
  Date/Author: 2026-07-15 / Codex.
- Decision: snapshot `{tables, version}` under a short lock, evaluate on a local `Database` clone, then publish through version CAS; retry the whole autocommit statement up to local `tidb_retry_limit`.
  Rationale: this retains existing SQL execution and its statement atomicity while making stale snapshots, real compare-and-swap conflicts, and retry behavior observable under real threads.
  Date/Author: 2026-07-15 / Codex.
- Decision: retain the local-`Database` clone as a temporary compatibility adapter, not the final runtime abstraction.
  Rationale: it lets this narrow source test establish ownership and CAS behavior without a broad executor rewrite. A positive shared-autocommit capability envelope now prevents Rc-backed/nontransactional side effects from entering the adapter. Delete both the adapter and its envelope when the transactional runtime owns an explicit per-session write buffer, statement context, auto-ID allocator, sequence state, and row-level MVCC read/write set; that milestone must replace full-table cloning with typed storage operations.
  Date/Author: 2026-07-15 / Codex.
- Decision: recognize `tidb_retry_limit` as typed per-session compatibility state only in the new session seam.
  Rationale: the source sets it in every worker. The source test does not observe retry scheduling, and implementing an invented retry engine would be false coverage. The value must remain local to demonstrate that sessions do not share settings.
  Date/Author: 2026-07-15 / Codex.
- Decision: represent each local attempt as independent `outcome` and `CatalogEffects`; CAS-publish a changed table image even when `outcome` is `Err`, but never publish or version an error with no table delta.
  Rationale: success-gated publication loses valid Go DDL effects, while error-gated publication would create false versions for atomic DML failures. Comparing the working and input table images captures the general contract without statement-specific DROP logic.
  Date/Author: 2026-07-15 / Codex.
- Decision: make the seed lifecycle a typed `TransactionPhase` whose active variant owns the rollback catalog image and savepoints, while autocommit and isolation remain per-session settings on `TransactionState`.
  Rationale: savepoints can no longer survive an idle transaction through independent `bool`/`Option` fields, and rollback can replace only transactional catalog state without rolling back session settings. The enum does not invent Go's pending TSO or valid `kv.Transaction` states.
  Date/Author: 2026-07-15 / Codex.

## Outcomes & Retrospective

The shared-storage boundary now has real four-thread evidence: the source test's four workers reach at least three stale-version CAS conflicts, then complete-statement retries publish all 80 increments. Publication is no longer success-gated: a typed statement attempt carries its SQL outcome separately from its catalog effects, so the Go-backed multi-table DROP both returns `UnknownTable("tb")` and durably removes `ta` and `tc`. A deterministic stale-CAS regression proves the erroring attempt is re-evaluated before publication, while a duplicate-key DML regression proves no-delta errors do not create catalog versions. Independently, the single-session seed lifecycle now has one source-owned `TransactionState` and an exhaustive idle/active phase instead of five transaction fields spread across `Database`; its rollback-catalog image remains an in-memory compatibility model, not a TiKV transaction. The local-`Database` compatibility adapter remains intentionally temporary. Future work must replace it with an explicit per-session write buffer, a begin-version/read set, row-level storage operations, shared auto-ID allocation, and commit-time conflict/lock protocol before enabling explicit transaction statements through `Session`.

## Context and Orientation

`rust/crates/tidb-exec/src/lib.rs` exports `Database`, an in-memory single-session executor. Its `tables: BTreeMap<String, Table>` is catalog data; `transaction: TransactionState`, user variables, clocks, and RNG are session data. `rust/crates/tidb-exec/src/transaction.rs` owns the seed's autocommit, isolation, lazy implicit begin, explicit begin, commit/rollback, and savepoint transitions. `rust/crates/tidb-exec/src/database.rs` coordinates statements and catalog changes and owns the source-shaped distinction between errors with no table effects and DDL errors that retain table effects. `rust/crates/tidb-exec/src/catalog.rs` defines the cloneable table representation.

The new `Cluster` lives in `rust/crates/tidb-exec/src/cluster.rs`. A cluster owns `Arc<Mutex<SharedCatalog>>`; `SharedCatalog` contains tables and a monotonically increasing commit version. A `Session` owns its own `Database`, including `tidb_retry_limit`. `Session::run` copies `{tables, version}` under a short lock, delegates execution to a local `Database` clone without holding that lock, computes a typed catalog effect by comparing the input and working table images, and publishes a changed image only when a version compare-and-swap succeeds. Publication is independent of whether the same attempt returns `Ok` or `Err`. A conflict discards that local working image and retries the entire statement from a new snapshot; an unchanged image returns immediately without advancing the version. No `Database` is shared between OS threads.

This first milestone is limited to autocommit publication. `BEGIN`, `COMMIT`, `ROLLBACK`, and `SET autocommit=0` must reject through `Session::run` until session-local write buffers plus version validation are implemented. This avoids accidentally claiming real transaction semantics simply because `Database` has a single-session rollback snapshot.

## Plan of Work

Add `cluster.rs`, export `Cluster` and `Session` from `lib.rs`, and keep the existing single-session API stable. Make `Cluster` cheaply cloneable so each worker can create one session. The library-facing `run(&Stmt)` keeps parser ownership outside execution. Before the temporary local-`Database` clone is created, require a positive capability envelope: only `USE`, a single integer `tidb_retry_limit` SET, ordinary non-temporary CREATE/DROP TABLE without default/auto-ID options, literal INSERT, pure-expression single-table UPDATE, and a pure single-table SELECT are eligible. Reject every other AST shape before it can mutate shallow-Rc session state.

Port `SET @@session.tidb_retry_limit` as normal typed `Database` session state with Go's signed range and default. For all other statements, reject explicit/non-autocommit transaction forms before borrowing the catalog. Delegate `USE`, CREATE, INSERT, UPDATE, SELECT, and normal errors to the existing executor. Capture a statement attempt as independent `outcome` and `CatalogEffects`: unchanged images return without publication, while changed images use the same version-CAS path for both success and error outcomes. A stale publication increments an observable conflict counter and retries the whole statement from a new snapshot; successful retry count is observable too.

In the unit test module, start four scoped OS threads. Each worker creates a new `Session`, sets the source retry limit, repeats the source duplicate INSERT and UPDATE twenty times, and preserves the source's ignored INSERT result: the first attempt may succeed and later attempts duplicate. The parent session creates the table and final row, then verifies the source's exact final `SELECT` result. Add a second local-state assertion showing two sessions carry distinct retry-limit values while reading the same catalog. Add source-backed regressions for error-plus-effects multi-table DROP, deterministic stale-CAS retry of that DROP, and no-version publication for duplicate-key DML with no table delta.

Add a new table corpus topic containing one worker's SQL-only source sequence and a coverage evidence line. The source corpus cannot encode its Go goroutine topology, so the Rust unit test is the authoritative concurrency proof; the evidence text must state that distinction and must not call the sequential table differential a concurrency test.

## Concrete Steps

From `/Users/qiliu/projects/tidb/rust`:

    rustfmt --edition 2021 --check crates/tidb-exec/src/catalog.rs crates/tidb-exec/src/cluster.rs crates/tidb-exec/src/tests/cluster.rs
    cargo test -j 12 -p tidb-exec --lib shared_cluster -q
    cargo test -j 12 -p tidb-exec --lib -q
    cargo clippy -j 12 -p tidb-exec --lib -- -D warnings
    cargo test -j 12 -p difftest-result-tests --test table_diff table_execution_matches_go_engine -q
    cargo run -j 12 -q -p difftest --bin go_test_ledger -- --write
    cargo run -j 12 -q -p difftest --bin go_test_ledger -- --check

The later transaction-state extraction additionally used:

    cargo test -j 12 -p tidb-exec --lib transaction
    cargo test -j 12 -p tidb-exec --lib shared_cluster
    cargo test -j 12 -p tidb-exec --lib
    cargo test -j 12 -p difftest-result-tests --test table_diff
    cargo test -j 12 -p difftest-result-tests --test query_diff
    cargo clippy -j 12 -p tidb-exec --all-targets -- -D warnings
    cargo fmt --all -- --check

The focused unit test must show four actual worker threads completing and `RS:80` from the shared table. The table differential must retain every existing Go corpus result.

## Validation and Acceptance

Acceptance is behavioral:

1. Four independently created sessions execute the source loop concurrently against one cluster and the final `SELECT c2 ...` returns `RS:80`.
2. The source-ignored INSERT result is either the one successful insert or `ExecError::DuplicateKey`; the final shared row still reaches 80 updates.
3. A session-local `tidb_retry_limit` change remains invisible to another session.
4. Existing single-session table differentials keep passing.
5. The Go ledger records the port only as `PARTIAL`, explicitly excluding retry scheduling, TiKV locks, and conflict behavior not exercised by this source test.
6. `DROP TABLE ta, tb, tc` returns `UnknownTable("tb")`, removes `ta` and `tc`, retains a concurrent table after stale-CAS retry, and increments the real conflict/retry counters exactly once.
7. A duplicate-key error with no table delta leaves the shared catalog version unchanged.

## Idempotence and Recovery

All commands only format, test, or regenerate derived ledger data. If a test fails, do not update its golden or ledger status to hide the failure. The new module is additive; recovery is deleting it and its focused tests in one coherent revert, without touching existing `Database` behavior.

## Artifacts and Notes

The Go source loop is:

    for range 4 goroutines:
        SET @@session.tidb_retry_limit = 100
        repeat 20:
            INSERT INTO t_rollback VALUES (1, 1)  // result ignored by Go; first may succeed
            UPDATE t_rollback SET c2 = c2 + 1 WHERE c1 = 0

With one shared committed catalog and statement serialization, every successful update observes the preceding committed value, hence the final observable source assertion is `80`.

## Interfaces and Dependencies

In `tidb-exec`, define these public interfaces:

    #[derive(Clone, Default)]
    pub struct Cluster { /* Arc<Mutex<SharedCatalog>> */ }

    pub struct Session { /* local retry_limit and a Cluster handle */ }

    impl Cluster {
        pub fn new() -> Self;
        pub fn session(&self) -> Session;
    }

    impl Session {
        pub fn run(&mut self, stmt: &tidb_ast::Stmt) -> Result<Outcome, ExecError>;
        pub fn retry_limit(&self) -> i64;
    }

`Cluster` must be `Send + Sync`; each worker creates and owns its non-`Send` `Session` locally because `Database` deliberately retains connection-scoped `Rc` state. `Session::run` must never expose the mutex guard to a caller, must not hold it while SQL executes, and must retain it only for snapshot/CAS operations.

Changed 2026-07-15: created after the source audit established that no remaining `pkg/session/test/txn` test fit the prior single-session-only boundary.
