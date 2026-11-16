# Port Go's realtikv transaction packages

This ExecPlan follows `PLANS.md` at the repository root and is maintained as the work proceeds.

## Purpose / Big Picture


Exercise the Rust server against real TiKV with the complete tests from Go's `tests/realtikvtest/txntest` and `tests/realtikvtest/pessimistictest`. Preserve each test's assertions, concurrency and Go identity. Fix demonstrated Rust divergences by reading and porting their Go owners. Acceptance requires live results, not a passing run with `TIDB_REALTIKV_PD` unset.

## Progress


- [x] Read root guidance, runner skill, harness, and Go test inventory; initial worktree is clean.
- [x] (codex) Port all 71 transaction tests and 75 pessimistic tests (7 ignored, each a Go skip), retaining Go skip reasons and classic/nextgen conditions.
- [x] (codex) Extend the testkit harness for Go helpers used by these tests.
- [ ] (2026-11-16, in progress) Run live tests serially, fix Rust bugs and prove regression assertions fail before their fixes. txntest run 2: 51/71. Fixed since: the DDL owner lease (each test waited ~45s for the previous test's owner), client failpoint `pause` blocking runtime workers (the FK test hung), the transaction's session ID never reaching the committer (every `sessionID > 0` client failpoint was dead), `KeyError` text, per-table `AS OF`, the pessimistic FK shared lock, the interceptor helper's nested executor, and MODIFY COLUMN's storage-error identity. Open: stale-read group (`test_stale_*`, `test_staleness_and_history_read`, `test_stmt_ctx_stale_flag`, `test_validate_read_only_in_staleness_transaction`), `test_replica_read_effect_scope`, the transaction-info digests of prepared statements and subqueries.
- [x] (2026-11-16) First `realtikv_pessimistictest::` run on its own cluster: 64/70.
- [x] (2026-11-17) Live after the fixes: txntest 67/71 and pessimistictest 68/70 (latest full runs plus verified subsets). Fixed and verified: EXECUTE of a prepared COMMIT/ROLLBACK; Go's pessimistic retry failpoints; `decideTxnMode` at statement start; RPC interceptors on coprocessor and request-plan sends; EXECUTE's prepared digest in the transaction info; a stale transaction clearing `tidb_snapshot`; the stale flag cleared at record-set close; Go's stale-TSO clock for `AS OF` and its TIME conversion; `delayForAsyncCommit` at its five DDL sites; reorg failures keeping their Go identity (`[kv:8025]`); EXPLAIN of DML in a stale transaction refusing `GetForUpdateTS`; LOAD DATA validated before the file; the GLOBAL `tidb_txn_entry_size_limit` reaching backfill buffers.
- [ ] Open: txntest `test_replica_read_effect_scope` (a point get inside a pessimistic transaction is sent without follower read), `test_stale_read_no_backoff` (the stale point get never reaches the interceptor), `test_stale_select` (an `AS OF` read of a row older than an added column shows `""`; the current schema shows NULL); pessimistictest `test_plan_cache_schema_change`, `test_statements_interrupted_on_disconnect`.

## Context and Orientation


`rust/crates/tidb-server/tests/realtikvtest.rs` provides `RealTiKV`, one in-process server domain connected to PD, and `Tk`, one session. `realtikv!()` returns early without a configured PD. Register the two new test modules in `tests/all.rs`. Go tests are the source of truth; Go production files must not change. The structural register consists of `structural-findings.md` and `.json` in this directory; currently no row names these two missing packages.

## Plan of Work


First port SQL-only transaction and isolation cases as one coherent batch, preserving ordered operations between sessions. Then port concurrent lock cases, prepared statements, transaction introspection, stale reads and server-disconnect cases. Read the corresponding Go testkit and implementation before adding a helper or repairing behavior. Record concrete gaps outside this scope in both register formats with unused IDs; an ignored test must name its gap. Unported tests are not accepted as a completed package.

## Concrete Steps


Live runs, one package per fresh playground (PD 24891, stopped and cleaned afterwards by the runner):

    WT=txn /Users/qiliu/projects/tidb-wt/scripts/realtikv-run.sh <log> 'test(realtikv_txntest::)'

The paragraphs below were codex's setup and are superseded where they differ.

Run all Cargo commands from `rust/`, with `CARGO_CACHE_AUTO_CLEAN_FREQUENCY=never CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0`. Use the default shared target directory and only one Cargo command at a time. Do not commit, push, clean Cargo artifacts or change other worktrees.

    cargo check -j4 -p tidb-server --tests
    TIDB_REALTIKV_PD=127.0.0.1:26891 cargo nextest run -j1 --build-jobs 4 -p tidb-server --test all -E 'test(realtikv_txntest::) | test(realtikv_pessimistictest::)'

The explicit serial live-test instruction takes precedence over the general four-job instruction for nextest test execution; compilation remains four jobs. Crate unit suites use `cargo nextest run -j4 --build-jobs 4 -p <changed-crate> --lib`. No workspace suite.

Start an owned TiUP playground with `--mode tikv-slim --pd 1 --kv 1 --pd.port 26891 --kv.port 26271 --without-monitor`, using `TIUP_INSTANCE_DATA_DIR=/tmp/txn-live` and the installed playground binary to keep all runtime data in the authorized temporary directory. Check `/pd/api/v1/version` and `/pd/api/v1/stores` before tests. Do not use the other sessions' ports 24891/24271 or 25891/25271.

## Validation and Acceptance


Compare the Rust function inventory with every top-level Go test (excluding `TestMain`). Review each skip against its actual Go condition. Run the live filter serially and all changed crates' unit suites. The known baseline `tidb-session` extension-auth test failure is exempt. A killed/hung live test invalidates subsequent cluster evidence: stop and restart the owned playground with fresh data before drawing conclusions. Suspended processes and tool timeouts may indicate the integrator's gate; wait and retry.

## Idempotence and Recovery


Preserve unrelated changes. Only remove `/tmp/txn-live` after the owned playground exits, and verify both assigned endpoints are no longer reachable. Preserve failure logs outside that data directory for review. Do not remove any other process or cluster data.

## Surprises & Discoveries

- Observation: every realtikv test after the first waited about 45s in its first DDL. The previous test process's DDL owner key stayed in etcd until its 60s lease expired, because Rust stops the DDL only when the last `Arc` of the domain drops; Go's harness calls `ddl.CloseOwnerManager`. Register row S06.
- Observation: `fail`'s `pause` blocks the OS thread. Evaluated inside a tokio task it blocks a runtime worker, and PD timestamp futures on that runtime starve (`test_dml_with_add_foreign_key` hung for 10 minutes). Client-go's pause parks one goroutine; `util::eval_failpoint_async` parks the task instead.
- Observation: Go sets `kv.SessionID` (`SetOptionsOnTxnActive`) and `util.SessionID` in the statement ctx (`ExecStmt.Exec`, `doCommit`); Rust set neither, so client failpoints gated on `sessionID > 0` (`beforePrewrite`, `beforePessimisticLock`, ...) never fired from SQL.

The checked-in register has five unrelated rows and no transaction-test row. Both Go packages together contain 8,762 source lines, including support functions. Some `t.Skip` calls are conditional on mock storage and therefore do not skip this live port.

## Decision Log


- Decision: use four compilation jobs and serial live tests, overriding root guidance's general twelve-worker preference. Rationale: the user's explicit memory and shared-database constraints. Date: 2026-11-16.

## Outcomes & Retrospective


Work in progress. The initial ten live pessimistic cases passed. The four-case transaction-state run passed `TestEntriesCountAndSize` and failed `TestRunning`, `TestCommitting`, and `TestRollbackTxnState` before their missing Go session failpoints were installed. With those three hooks, all three failed cases passed on the same healthy cluster (103.619 seconds, `/tmp/txn-live-state-hooks.log`). No transaction-state behavioral change was required: the suspected rollback ordering issue was not reproduced. A serial run of the current 86 nonignored ports is in progress (`/tmp/txn-live-batch90.log`); additional ports are being written while it runs. Full package parity is not yet established.

## Artifacts and Notes


Playground log: `/tmp/txn-playground.log`. Test source identities are recorded in each Rust function's doc line.

## Interfaces and Dependencies


Use the existing `RealTiKV::new_tk`, `Tk::must_exec`, `Tk::must_query` and coded errors. Extend these with Go-named helpers only where a port requires them. Use `tidb-failpoint` and Go's full failpoint paths and terms for production failpoints.

### Live verification ledger

- `cargo check -j4 -p tidb-server --tests`: passed after correcting port-only type errors.
- Serial nextest filter for the four transaction-state tests, `--no-fail-fast`: 1 passed, 3 failed before hooks (`/tmp/txn-live-state.log`).
- Serial nextest filter for running/committing/rollback, `--no-fail-fast`: 3 passed after hooks (`/tmp/txn-live-state-hooks.log`).
- Initial two interrupted runs were discarded after the interruption; the owned playground was stopped and recreated with fresh data each time. Earlier completed tests remain individual observations only.
- Harness shutdown now closes workers, resource groups, leases and storage authority following the production shutdown sequence. The 42-60 second inter-test waits still occur, so this has not established a timing improvement.

### Coherent batch after the first baseline

The initial 86-case baseline was interrupted deliberately after 33 completed cases to compile the larger port. It recorded 24 passes and 9 failures; the 34th case was interrupted and is not evidence. The playground exited and both assigned ports were verified closed before its data was removed. Log: `/tmp/txn-live-batch90.log`.

Draft fixes awaiting compilation/live validation: promoted FOR SHARE classification and prepared-cache key; SQL kill and lock-expiration flags passed to native lock contexts; Go checkTxnAborted; SELECT lock deadline and statement lock-wait origin; propagation of the caller RPC interceptor context. The partition case was a harness mistake: Go Result.Check compares formatted rows, so the Rust helper now compares joined rows. The concurrent-insert port now uses Go GetForUpdateTS's max(startTS, forUpdateTS).

Go failpoints added at the native beforePessimisticLock/beforePrewrite/doNotKeepAlive sites, expression injectNow/injectSafeTS sites, and DDL onCreateForeignKey afterCheckForeignKeyConstrain site. The DDL hook is at the existing successful write-only check before publishing write-reorganization; it changes no durable schema transition. Read docs/agents/ddl/README.md and owning Go foreign_key.go before adding it.

New cargo check: `/tmp/txn-check-next.log` (in progress). No crate suites have yet been run. All draft behavioral fixes still require regression-before/after evidence where the first baseline did not cover them.

### Second live verification

Targeted live run of ten previous failures/new state assertions completed: nine passed, timeout mapping failed (498.334 seconds, `/tmp/txn-live-fixes.log`). Passed: concurrent insert; promoted share with/without prepared cache; kill/TTL manager; kill lock wait; partition locking; expired pessimistic locks; issue62775 commit timestamps; transaction GoString. Timeout correctly interrupts at one second but was flattened to a generic transaction error. Porting `ToTiDBErr` signal mapping and preserving statement-scoped cancellation. Both lazy uniqueness failures lose an earlier statement's duplicate hint because the SQL lock bridge filtered hints to this statement's new absence checks; copying hints for all requested lock keys fixes the ownership error. Live reruns pending.

Current inventory: 69/71 transaction tests and 73/76 pessimistic tests have Rust functions; not yet all validated. Fourteen crates including native client now have code changes; their suites are pending.
