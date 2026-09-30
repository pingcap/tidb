# Remove extra storage policies and use native client owners

This living ExecPlan follows repository `PLANS.md`. Keep Progress, Surprises & Discoveries, Decision Log and Outcomes & Retrospective current.

## Purpose and acceptance


The user explicitly requested removal of all reviewed duplicate owners and extra policies, a client-rust dependency update, and Go-compatible behavior. This authorizes replacing the integration deliberately reverted in `2471be70e9`. Start from the previously validated integration in `b1534707`, preserving all subsequent unrelated commits. The accepted boundary is Go master and its pinned client-go packages, with native Rust synchronization and typed errors. This is maintenance of existing ports, not a claim that incomplete packages have become completely transcreated.

Production transactions must have one native transaction engine, MemDB and lock resolver; caller settings and cancellation must determine behavior. Remove local mutation admission limits, SQL-category mutation rewriting/coalescing, restrictive lock decoding, fixed session lock waits, permanent full-key attempt histories, hand-written SET time_zone SQL parsing, and cancellation identified by error display text. The previously reviewed duplicate retry/cache/RPC owners must be reconciled against native APIs and migrated without deleting required DistSQL or PD behavior. All explicit retry limits, SQL error identities, snapshot metadata, shared locks, metrics, and existing performance improvements remain acceptance requirements.

## Progress


- [x] Pull TiDB integration/master and client-rust master; both working trees initially clean.
- [x] Restore native transaction/MemDB/resolver integration without committing over newer work.
- [x] Replace textual cancellation identity in native client-rust; reproduce, test, commit and push.
- [x] Synchronize TiDB to the published native revision with only required transport compatibility patches.
- [x] Remove facade mutation budgets, SQL-category rewriting, duplicate value history, and production attempt-history allocations; retain explicit test observation.
- [x] Thread session lock timeout through active SQL statements and global timeout through statistics workers; load configured native buffer size limits.
- [ ] Audit remaining contextless helpers and table-layer assertion propagation.
- [x] Carry table-selected assertions through ordinary record/index writes using the native MemDB stage path; reproduce missing assertions and validate insert/update/delete, key moves and statement rollback.
- [ ] Wire session assertion and commit-protocol options to transaction activation and remove the separate generic mutation constructor's assertion policy.
- [x] Review the complete assertion-error boundary, preserve wire metadata and error 8141 through commit/locking, and remove the incorrect duplicate-key conversion.
- [ ] Consolidate unique-index existence probes; only NotFound may mean absence and typed storage errors must survive.
- [x] Replace all three executor pessimistic statement retry caps with Go's live global configuration; reproduce configured exhaustion and statement rollback and pass targeted tests/lint.
- [x] Validate the retry-policy repair with targeted regressions, root lint and the locked server build; publication uses the mandatory hook and fresh pre-push build gates.
- [x] Remove special SQL parsing and execute supported storage SET assignments through the normal parser/session-variable path.
- [x] Publish native cancellation/lifetime fixes and synchronize TiDB to client-rust 884589f.
- [ ] Complete remaining background lifetime scopes; concrete patch awaits user approval after automatic review rejection.
- [ ] Consolidate remaining retry/cache/RPC implementations with native owners and retain TiDB-owned adapters.
- [ ] Verify affected packages, regression behavior, required lint and locked server builds; commit and push.

## Source and package inventory


TiDB baseline is `2471be70e9030f36284506f1b87baadebaf3e32d`; Go master is `51a1a4abfc192a91f98fe968ad87eced9221f663`; pinned client-go is `v2.0.8-0.20260928031501-8edb23f6c7ee`. Native baseline is `515e4aab0e4a98d891641ab726209748752eb50e`. The restored `native-transaction-integration-inventory.json` retains complete package evidence for transaction/resolver integration. Extend source inventory for the package owners touched by this follow-up; do not treat individual fixes as package-completion receipts. No deeper AGENTS.md was found in the affected Rust or native source trees.

## Milestones and implementation


First, restore the prior integrated boundary using the inverse of the exact revert, then update the native error package. Add regressions that distinguish an ordinary StringError containing context canceled from the cancellation sentinel. They must fail on the native baseline. Introduce a typed context-cancellation error with the same display spelling as Go, replace native producers and consumers, preserve remote gRPC cancellation handling, and verify full library tests and strict Clippy before native publication.

Next, run `rust/scripts/sync-tikv-client-rs.sh` against the published master. The restored source-only sync preserves build caches, regenerates protocols from source and applies only the four tonic/prost compatibility patches. Do not manually edit generated artifacts. Update root Cargo.lock through Cargo if necessary, then use locked commands. Verify MemDB size limits, flags and transaction commit behavior rather than retaining separate mutation histories or SQL-operation categories.

Session behavior is a separate milestone. Read the existing parser/SET executor and variable store, add failing regression cases for malformed SET prefixes, legitimate parsed SET forms, and nondefault lock-wait settings. Remove the special text recognizer and route normal statements to the existing owner. Keep Go timeutil's timezone-value parser and error mapping.

The remaining client boundary milestone audits every production retry/cache/RPC caller, migrates to native interfaces, and deletes only the replaced owner. Use existing native cancellation, routing, batch RPC and metric types; extend the native library where its Go contract is missing. Do not use a locally maintained protocol algorithm as the permanent adapter.

## Validation and commands


Native commands run from `/Users/qiliu/projects/client-rust`:

    cargo test --locked --lib source_cancellation_uses_identity_not_error_text
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings

TiDB commands run from `rust/`, selected according to the final change:

    cargo check --locked -p tidb-server --all-targets
    cargo test --locked -p tidb-txnkv --lib
    cargo test --locked -p tidb-txnkv --test all
    cargo test --locked -p tidb-txnkv --test lock_resolver_source --test snapshot_lock_wait_source --test snapshot_scan_page_deadline_source --test region_error_recovery_source
    cargo test --locked -p tidb-exec --lib multi_statement_transaction::tests
    cargo test --locked -p tidb-server --lib cluster_session_node::tests::transactions
    cargo test --locked -p tidb-unistore --test client_transaction

Add focused SET/session regressions and whole affected retry/cache/RPC package validation as implementation proceeds. Run root `make lint`. No Bazel preparation is required unless Go files/imports, Bazel metadata, or Go module inputs change. Commit through `TERM=xterm git -c core.hooksPath=hooks commit` so the mandatory locked server build runs. Rerun `cd rust && cargo build --locked -p tidb-server` after commit and immediately before each push of Rust changes. Push native to master and TiDB to hparser-integration without force. Preserve existing regression tests and account for baseline failures explicitly.

## Surprises & Discoveries


The branch's revert restored local algorithms and an older native vendor snapshot. The latest explicit user instruction resolves the earlier pending restore decision. Prior integration validation recorded two region-cache failures and 20 DistSQL failures; those are historical baseline observations, not permission to ignore new failures or evidence of parity. The follow-up must verify any affected failures again.

## Decision Log


Restore the shared native boundary rather than patching each local algorithm independently, because Go places those algorithms in client-go. Native correctness/API changes are published upstream before vendoring. Required Rust adapters and transport-version compatibility remain. The removal list is an architectural migration: remove restrictions only together with their Go-aligned owner, never by bypassing validation or swallowing errors.

## Outcomes & Retrospective


Native cancellation identity was published as 89926da, followed by operation lifetime propagation as 884589f on client-rust master. Full native library validation passed 1,401 tests with two ignored; strict Clippy passed. The public resolver pool configuration change then passed all 42 resolver tests and Clippy. TiDB was synchronized through the maintained source/protobuf generation script to 884589f with four transport compatibility patches. Native changes make ordinary rollback, secondary commit, automatic heartbeat and region fan-out carry explicit background scopes. Injected TiDB clients disable asynchronous resolver scheduling by setting the pool size to zero instead of closing its lifetime.

The local prewrite/commit/rollback/heartbeat/resolver engines, SQL value undo log and mutation coalescer are deleted. The facade's count/byte budgets, arguments, constants, error variants and SQL sizing pass are removed. BufferMutation carries Set/Delete/Lock with independent flags/assertions. Native buffer limits load TiDB globals at construction, process configuration initializes those globals, and SQL buffer failures retain SQL identity instead of panicking. Autocommit hands over MemDB without rebuilding its entries. Full-key attempt histories are disabled by default, including injected clients, and explicitly enabled only in tests that inspect them. The SET prefix recognizer is removed from both lightweight TiKV node variants; supported storage assignments use the existing parser and SET owner, including warnings. Statement locks use native staging inspection and Go KeyNeedToLock, retaining same-value writes and native flags. Active statement waits use session values, including changes inside an open transaction; statistics workers use the global setting.

Regression evidence includes malformed SET prefixes, configured entry/transaction limits, lazy insert-delete flags, equal-value statement writes, native lock flags, session wait changes and foreground commit/rollback cancellation. Foreground cancellation failed when the previous request-type heuristic was temporarily restored, and all seven bridge ownership tests passed after restoring explicit lifetime dispatch. Logs are /private/tmp/tidb-removal-foreground-red.log and -green.log. The former stale-overlap region failures exposed cached children returned outside the requested key/end-key/ID; query-specific checks now reject them. The default batch region loader previously returned only each range's first region; walking to the range end fixed all 20 DistSQL failures. SQL PrepareTxnCtx behavior also required resetting current TSO at the beginning of the next autocommit statement. Bootstrap tests now account for the already-present metadata-lock view; the embedded ambiguity regression uses the typed cancellation sentinel.

Validated results: txnkv library 136 passed, one ignored, followed by seven bridge ownership tests after one new regression; aggregate txnkv 422 passed, ten ignored; configured limits one passed; SQL buffer 20 passed; multi-statement transactions 11 passed; server transaction tests 26 passed; SET six passed; embedded transactions 15 passed; bootstrap eight passed; DML planning 83 passed; account writes 13 passed; system variables five passed; resolver/snapshot suites 78 passed; DistSQL library 37 passed, one ignored; DistSQL integration 256 passed, two ignored. Root make lint completed successfully. macOS system-memory queries and loopback fixtures required execution outside the sandbox; their initial permission failures are not product failures. Existing compiler/linker/jemalloc warnings remain. The hook build and separate pre-push locked build passed, and this milestone was published as TiDB 381f04663c on hparser-integration.

Remaining work is explicit. The local region cache and low-level RPC owner migration is not complete. The bridge still recognizes heartbeat requests as detached until every native heartbeat path carries scope. The unapplied /private/tmp/client-rust-background-lifetimes.patch covers pipelined rollback, cleanup, secondary completion, heartbeat and transaction-file secondaries; automatic approval review rejected the multi-path write as a transaction correctness risk and a user approval question is pending. Mutation assertions still need a table-owner audit: Go lazy optimistic INSERT uses AssertUnknown, while pessimistic/eager insertion uses AssertNotExist; the generic convenience constructor currently chooses NotExist. The subsequent native buffer handoff milestone below removes the unbound-session mutation reconstruction path. Contextless helper default waits require review. No complete package, full parity, benchmark improvement or RealTiKV validation is claimed by this milestone.

## Recovery and artifact ownership


Both repositories were clean before work; current changes belong to this request. Preserve other incoming changes on fetch/pull and do not reset shared branches. Regenerate vendor protocols from pinned inputs when build output is stale. Logs use `/private/tmp/native-cancellation-*.log` and `/private/tmp/tidb-removal-*.log`. Do not delete active build caches or shared worktrees to reclaim space.

Revision note: updated on 2026-09-30 with native 884589f, explicit lifetime regressions, native statement staging, region coverage repairs, current test results and unresolved ownership work.

## Native buffer handoff follow-up (2026-09-30)


The repositories were refreshed before this continuation; TiDB hparser-integration and client-rust master were already current. Go master remains 51a1a4abfc192a91f98fe968ad87eced9221f663 and the native dependency remains 884589f. This repairs the existing session/storage adapters, without claiming completion of a newly transcreated package.

Go pkg/store/driver/txn/unionstore_driver.go wraps the client MemDB directly. The remaining SessionTransaction::commit_with fallback instead copied only values, tombstones and the presume flag into new BufferMutation objects. Its deletion branch dropped even that flag, converting a lazy insert followed by delete into an unconditional Delete. A regression over the actual embedded store committed and removed an existing value on baseline 381f04663c; the fixed path retains the native CheckNotExists mutation and returns SQL 1062 with the table-layer duplicate hint. The original value remains unchanged.

SessionTransaction::commit_with now binds an unbound SQL MemDB intact and commits through that same native owner. The obsolete staged_mutations_from_entries converter, its separate schema helper, and unused snapshot_staged/take_staged/take_snapshot copy APIs are removed. MutationBuffer::bind_native can attach an empty SQL handle to an already locked native transaction without replacing its flags. Pending SQL writes must still be bound before acquiring locks, as the binding API already requires; the adapter does not merge two independent transaction buffers. The existing transfer regression now exercises take_native_buffer directly and verifies assertions and tombstone flags.

The unified commit path also derives schema-lease table IDs from both buffered keys and appended mutations. A separate regression recorded only table 10 on baseline and now records tables 10 and 20. The empty-handle/pessimistic-lock test verifies that a restricted transaction can lock first, append its write at commit, and read the committed value afterward. A native-buffer unit regression reproduced the previous binding panic and now retains both lock and assertion metadata.

Validation commands run from rust/:

    cargo test --locked -p tidb-server --lib transaction_buffer_tests
    cargo test --locked -p tidb-executor --lib cluster_storage::tests
    cargo test --locked -p tidb-exec --lib cluster_table_storage::tests
    cargo test --locked -p tidb-server --lib cluster_session_node::tests::transactions
    cargo test --locked -p tidb-server --lib every_shape_the_ddl_admits_the_loader_loads
    cargo test --locked -p tidb-server --lib check_constraint_runs_through_the_owner_job_queue
    cargo test --locked -p tidb-server --lib exchange_partition_validates_and_swaps_real_rows_atomically

Results: three embedded transaction regressions passed; 21 SQL buffer tests, six adapter tests and 26 SQL transaction tests passed. DDL shape/loading and persisted constraint-job tests passed. The partition-exchange test failed with "this ALTER TABLE action is not supported yet"; rerunning against the unchanged production files from 381f04663c reproduced the identical error. It is not counted as a pass. Rechecking the final regression tests on the original commit path reproduced both metadata/lease failures; the pessimistic lock test passed on both. Logs are /private/tmp/tidb-native-buffer-*.log. Root make lint and both locked hook/pre-push builds passed. The buffer-handoff milestone was committed and pushed as a4068f62a3. No RealTiKV cluster or benchmark was run.

Automatic approval review again rejected applying /private/tmp/client-rust-background-lifetimes.patch, stating that a broad "continue" does not explicitly approve the previously rejected transaction-sensitive patch. A new specific approval question is pending. Before that rejection, a native regression reproduced rollback replacing the supplied background owner. Its test-only changes are saved at /private/tmp/client-rust-background-lifetime-regressions.patch; the native working tree was restored clean and no blocked production change was applied. After explicit approval, apply the regression patch and production patch, run full native library tests and strict Clippy, publish to master, synchronize TiDB, remove the remaining heartbeat request-type heuristic and validate both build gates before pushing.

Remaining ownership work remains the region/RPC migration, table assertion propagation and contextless/default retry policies. This follow-up completes only the buffer handoff repair and deletion milestone.

## Shared pessimistic statement retry policy (2026-09-30)


TiDB hparser-integration was pulled with --ff-only and Go master fetched again; both were current. Go master remains 51a1a4abfc192a91f98fe968ad87eced9221f663. A fresh ls-remote confirms client-rust master is still 884589f0365053c0f5bd300209751187a4811782, already used by the dependency. No native or vendor changes are needed for this milestone. This remains maintenance of existing executor/session adapters, not a whole-package transcreation completion claim.

The upstream owner is pkg/executor.ExecStmt.handlePessimisticLockError, in adapter.go at the pinned master, with configuration owned by pkg/config.PessimisticTxn.MaxRetryCount. The Go gate loads global configuration after each retryable statement error, refuses when retryCount reaches the configured count, returns the unregistered error "pessimistic lock retry limit reached", and charges one replay before OnStmtRetry. Rust had three independent gates: eight retries in MultiStatementTransaction, and fixed 256 retries in the restricted SQL loop and ClusterServerSession. All ignored the configured limit; the lightweight path also returned the last conflict/deadlock instead of Go's limit error.

The shared PessimisticStatementRetry in tidb-exec/src/pessimistic_lock_error.rs now owns that counter, live configuration lookup and SQL 1105/HY000 exhaustion error. All three loops use it. REPLACE/ON DUPLICATE planning retains one counter across newly discovered lock batches, rather than allocating a fresh budget inside each acquisition; the redundant lock_keys forwarding method is removed. The observation counter uses usize like its configuration owner, avoiding truncation through the former u32 counter. KV request backoffs and optimistic transaction retries remain separate owners.

Regression coverage uses the existing isolated-process server harness so global configuration changes cannot race unrelated tests. Zero-budget conflicts previously retried and succeeded in all three paths; the unchanged-code logs are /private/tmp/tidb-pessimistic-retry-red.log and /private/tmp/tidb-pessimistic-lightweight-red.log. The fixed tests exercise zero and two retries, exact-boundary success, a limit above 256, a limit lowered during the statement, refreshed snapshot timestamps, pre-execution and post-execution locking, rollback of failed statement writes, retention of prior transaction writes, and successful later commit. The lightweight test uses a real in-process TiKV write after the locking transaction's timestamp to cause its conflict. No external cluster is required.

Commands run from rust/:

    cargo test --locked -p tidb-server --lib pessimistic_statement_retries_
    cargo test --locked -p tidb-server --lib lightweight_pessimistic_statement_retries_obey_config
    cargo test --locked -p tidb-exec --lib multi_statement_transaction::tests
    cargo test --locked -p tidb-exec --lib pessimistic_lock_error::tests
    cargo test --locked -p tidb-exec --lib cluster_table_storage::tests
    cargo test --locked -p tidb-server --lib cluster_session_node::tests::transactions
    cargo test --locked -p tidb-server --lib transaction_buffer_tests

The first command passes all three new regressions after the fix. The five surrounding suites pass 11, 11, 6, 28 and 4 tests respectively (60 total, including the regressions). Root make lint and git diff --check passed. Logs are /private/tmp/tidb-pessimistic-*-tests.log and /private/tmp/tidb-pessimistic-retry-lint.log. The first pre-commit hook stopped on an unqualified sql_error call visible only in test builds. Qualifying transactions::sql_error fixed production compilation; all 28 SQL transaction tests and cargo build --locked -p tidb-server then passed. Commit with TERM=xterm git -c core.hooksPath=hooks commit so the hook checks the final staged code, then rerun cargo build --locked -p tidb-server immediately before git push origin HEAD:hparser-integration. No Go, Bazel or module input changed, so bazel_prepare is not required.

Compatibility intentionally changes the lightweight path's default retry ceiling from eight to Go's configured default of 256, and reports Go's generic exhaustion error rather than the previous final conflict. Nondefault configuration now affects every statement loop. The uncontended lock path does not read the retry configuration. RealTiKV, sysbench, TPC-C, TPC-H and YCSB were not run; no throughput gain or full parity claim is made. The separate native background-lifetime patch remains unapplied after automatic approval review rejected it; it requires the already-requested specific approval. Table assertion ownership and the remaining region/RPC migration are still open.

Changed files for the retry-policy milestone: rust/crates/tidb-exec/src/pessimistic_lock_error.rs, rust/crates/tidb-exec/src/multi_statement_transaction.rs, rust/crates/tidb-exec/src/cluster_table_storage.rs, rust/crates/tidb-server/src/cluster_session_node/mod.rs, rust/crates/tidb-server/src/cluster_session_node/tests/mock_cluster.rs, rust/crates/tidb-server/src/cluster_session_node/tests/transactions.rs, rust/crates/tidb-server/src/unistore_node.rs, and this ExecPlan. No dependencies or generated files changed.

## Table-owned assertion metadata (2026-09-30)


The continuation fast-forwarded hparser-integration from 412c668a59 to c5a13dc5290515967e66d9aec0932accee6839a9, retaining the incoming DDL/warning changes. Go master advanced to 6b2781326b722f217a61852ab403350858549bd0. The source contract for this maintenance repair is pkg/table/tables: assertion.go's first-assertion rule, tables.go's add/update/remove record decisions, and index.go's public-index create/delete decisions. The Rust catalog loader currently admits public indexes; this change does not add transitional index or partition DDL support and does not claim complete transcreation of that Go package. A fresh client-rust master lookup still reports 884589f0365053c0f5bd300209751187a4811782, already synchronized in TiDB.

Ordinary KvTable writes previously carried only values and presume-not-exists marks through TableStorage. Even an eager INSERT left native record and index flags at KeyFlags(0). The regression failed on that baseline. TableStorage now carries explicit assertions alongside Set/Delete. ClusterTableStorage delegates those operations to the existing MutationBuffer::stage_owned_batch; there is no second assertion map or assertion transition implementation. Stores without MVCC retain their existing immediate value writes. This preserves native first-assertion-wins behavior and statement rollback without another key copy or buffer borrow.

KvTable chooses Unknown for an optimistic insert whose duplicate check actually deferred a local miss; eager or pessimistic inserts choose NotExist. A local tombstone is an observed deletion, not a local miss. The distinct-index branch follows the actual lazy-check result; the non-distinct-index branch follows Go's lazy-check option. Existing record/index updates and deletes assert Exist, newly inserted keys assert NotExist, and moving a clustered record key deletes the old key with Exist and writes the new key with NotExist. Rewrites of an index payload under the same key keep Exist. Repeated writes preserve the first assertion through the native buffer's existing rule.

The regression exercises eager/lazy checking in both transaction modes, unique and non-unique indexes, both row-delete entry points, update/delete/reinsert sequences, native statement rollback, a local tombstone without an earlier assertion, and clustered-key movement. Existing SQL-value behavior is covered by the surrounding executor and SQL transaction suites.

Changed files: rust/crates/tidb-executor/src/storage.rs; rust/crates/tidb-executor/src/cluster_storage.rs; rust/crates/tidb-executor/src/kv_table.rs; rust/crates/tidb-executor/src/kv_table/index_entries.rs; this ExecPlan. Existing formatting outside the mutation paths was preserved. No Go, Bazel, dependency, generated source or native client file changed.

Commands run from rust/:

    cargo test --locked -p tidb-executor --lib table_writes_preserve_go_assertions
    cargo test --locked -p tidb-executor --lib cluster_storage::tests
    cargo test --locked -p tidb-executor --lib kv_table::tests
    cargo test --locked -p tidb-executor --lib driver::tests::dml
    cargo test --locked -p tidb-server --lib cluster_session_node::tests::transactions
    cargo test --locked -p tidb-server --lib transaction_buffer_tests
    cargo test --locked -p tidb-executor --lib kv_table::

The targeted suites passed 22, 16, 9, 28 and 4 tests (79 total, including the new regression). The broader kv_table:: run passed 65 and failed test_in_memory_alloc and test_issue40584: both observed allocator.next() = 30001 instead of 2 / 20001. Temporarily restoring all four production files from unchanged c5a13dc529 and repeating the exact broader command reproduced the same two failures and 65 passes. All working edits were restored afterward. These failures are not counted as passes. Red/green and baseline logs are /private/tmp/tidb-table-assertions-*.log. Root make lint and git diff --check passed. Publication must still pass the mandatory locked-build pre-commit hook and a fresh cargo build --locked -p tidb-server immediately before pushing hparser-integration. No bazel_prepare is required.

The audit found two additional assertion ownership gaps that this metadata repair does not resolve. First, the SQL tidb_txn_assertion_level variable is registered, but its value is not forwarded to the native transaction's set_assertion_level: native transactions default to Off while Go applies the session level on transaction activation (pkg/sessiontxn/isolation/base.go and internal.SetTxnAssertionLevel). Second, the independent BufferMutation::insert convenience constructor always supplies NotExist even for lazy optimistic writes. That constructor and the lightweight/system mutation planners still need an explicit table-owned policy. Do not claim end-to-end assertion-level parity from the metadata tests. RealTiKV, fault-injected assertion enforcement and sysbench/TPC-C/TPC-H/YCSB performance were not verified. The separate background-lifetime patch remains unapplied pending its previously requested specific approval after automatic review rejection.


## Transaction integration review and assertion errors (2026-09-30)

The integration branch was clean at 56666f41ce606722cac33195e97d4089c15cc37c. `git pull --ff-only origin hparser-integration` reported current; `git fetch origin master` confirmed Go master 6b2781326b722f217a61852ab403350858549bd0. `git ls-remote https://github.com/ngaut/client-rust.git refs/heads/master` again returned 884589f0365053c0f5bd300209751187a4811782, matching the clean native checkout and existing TiDB dependency. No dependency update is necessary.

This review follows the transaction integration from SQL settings and table writes through MemDB, locking, native commit, error conversion, region/RPC adapters, cancellation, and protocol inputs. It revisits all five earlier diff comments. It is maintenance/review evidence, not a whole-package completion receipt or a claim that every Rust crate has been exhaustively reviewed. The complete upstream packages remain the atomic completion units; the findings below must not be reported as completed ports.

The assertion-error repair is complete across its existing adapters. Go master `pkg/session/session.go:handleAssertionFailure` always constructs `kv.ErrAssertionFailed` (8141) from the key, assertion enum, start timestamp, existing start timestamp and existing commit timestamp. `client-go/txnkv/transaction/2pc.go:extractKeyExistsErr` separately handles ErrKeyExist and its presume flag. Rust incorrectly treated NotExist assertion failures with a retained hint as duplicate entry 1062, erased all three timestamps and reduced the assertion enum to a bool. Its pessimistic lock path changed the same error to generic 1105. The prior comments claiming Go converts both identities were incorrect and have been removed.

TransactionCause now retains every assertion wire field, including unknown enum numbers. The driver copies the complete error into that cause. Both commit outcomes (including failed cleanup) and lock failures use the shared registered ERR_ASSERTION_FAILED template and SQL formatter, with Go's lowercase hex key. Only AlreadyExists consumes a duplicate-entry hint; no error policy is inferred from the assertion direction. This changes error reporting only after an actual native assertion failure. Ordinary duplicates retain 1062, and assertions remain transaction-fatal and non-retryable. No new transaction engine, assertion evaluator, redaction policy or handwritten message template was introduced. Go's additional MVCC history logging is outside this adapter repair and is not claimed implemented.

Two regressions failed against the unchanged production code: the hinted assertion returned 1062 rather than 8141, and the lock failure returned 1105 rather than 8141. The final tests cover both directions, None/unknown enum values, all timestamp fields, hinted/unhinted commit failures, cleanup failures, lock fatality and nested native error wrappers. The real native mock-store regression seeds a committed version, violates NotExist and Exist under Strict, and verifies exact timestamps plus unchanged committed data through the public driver commit boundary. This directly exercises native assertion enforcement; it does not demonstrate session assertion-option wiring, which remains absent.

### Findings still open after this repair

| Priority | Boundary and evidence | Go contract and required change |
| --- | --- | --- |
| P1 | Bound buffer lifetime: incoming 545fff9a84 adds a bound-buffer drain that ignores `access == false`; NativeMemBuffer::read already falls back to an empty local buffer after its native owner ends. The SQL commit path can then see an empty buffer and return success without publication. | Go retains the active transaction and its MemDB through commit. Keep the actual lock/write transaction in the autocommit handoff until publication; an ended owner must surface failure rather than empty success. Draining into a fresh transaction also cannot replace ownership of pessimistic locks and their timestamp. |
| P1 | Session activation: `tidb_txn_assertion_level` is registered, but no production first-party path calls native `set_assertion_level`; native CommitSettings defaults to Off. Explicit begin, pending autocommit and fallback commit all omit the option. | `pkg/sessiontxn/isolation/base.go:SetOptionsOnTxnActive` and `internal.SetTxnAssertionLevel` apply it once when the transaction activates. Pass a session-owned option set through every activation path; do not set a process-wide Fast default or change an existing transaction on each statement. |
| P2 | Commit protocol settings: `session_commit_protocol()` always reads TiKV bootstrap defaults. RealClusterTransactions begin, PendingSessionTransaction::wait and fallback commit call it without the session values, so accepted SETs for `tidb_enable_async_commit` and `tidb_enable_1pc` do not control the transaction. | Go applies both session flags in SetOptionsOnTxnActive. Repair with the same activation owner as assertion level, preserving restricted-session and lightweight entry points. Remove the misleading claim that this node lacks a SET-able variable store. |
| P1 | Unique-index probes: `kv_table/index_entries.rs:rewrite_changed_index_entries` has two `store.get(...).is_ok()` checks; `kv_table.rs:create_index_in` and `duplicate_entry_error` have the same pattern. A backend/timeout/cancellation error therefore becomes absence. | Go `pkg/table/tables/index.go:Create` returns every error except ErrNotFound. All four paths need one error-preserving existence decision, with rollback behavior covered for DDL backfill. This review establishes the source-path defect; fault-injected SQL reproduction is still required before that repair. |
| P2 | Competing insert policy: BufferMutation::insert combines presume-not-exists with AssertNotExist. Configured DML plan_insert and system_row_write use it independently of ordinary KvTable. The former intentionally performs no snapshot existence read. | Go table/index owners choose AssertUnknown for lazy optimistic misses and AssertNotExist for eager/pessimistic insertion. Preserve independent flag/assertion inputs and converge table policy across these callers; changing every insert to Unknown would also be wrong. |
| P2 | Protocol baseline: check-tipb-proto-projection.py resolves this branch's June TiPB via go.mod; Go master pins September fed7bc47c39d. A descriptor comparison against master's pin reports ExecType stale: 21 local values versus 22 upstream, missing TypeExplainForConnection=21. | Select and record the authoritative Go-master dependency pin as a package input; regenerate complete schema inputs and validate against it. Adding just this enum would leave the baseline-selection gap intact. The checker verifies existing projected message fields, not complete message coverage. |
| Open migration | Region/RPC ownership: ClientPd delegates to TiDB's RegionCache, synchronous region recovery and transport runtime; native equivalents still exist. RegionBackoffBudget itself already wraps the native RetryBackoffer and must not be mistaken for an independent retry algorithm. | Finish one routing/cache/RPC owner while retaining required DistSQL, transport and PD capabilities. Do not delete adapters solely because the native package has similarly named types. No new runtime failure is claimed from duplication alone. |
| Blocked repair | Background lifetime: explicit resolver scopes and ordinary native rollback/secondary commit/heartbeat are integrated, but the bridge still has a heartbeat request-type exception, and additional pipelined/transaction-file cleanup paths are covered by the saved unapplied patch. | Carry lifetime at the operation owner through all paths, then remove the heuristic. The exact native patch remains subject to the already-requested specific approval after automatic review rejected its transaction-sensitive, multi-path changes. |

The earlier diff comments were rechecked against the current native implementation. Explicit none/finite lock retries share wait_for_lock_retry without an unconditional callback overriding explicit options. CheckSecondaryLocks borrows the caller owner and region workers fork/merge history. Locked snapshot entries retain timestamp eligibility independently of lock flags. The native mock RPC returns its original Locked response after the simulated delay, without silently acquiring a released lock; this follows client-go's mocktikv simulateServerSideWaitLock, which differs from unistore's server-side normal/force wake-up implementation. Resolver background scopes cross the TiDB bridge explicitly while foreground resolution still observes statement cancellation. Existing regressions for these paths passed; that evidence does not close the broader background-lifetime item above.

Changed files for the assertion-error repair: rust/crates/tidb-txnkv/src/transaction/state.rs; rust/crates/tidb-txnkv/src/driver/tikv_transaction.rs; rust/crates/tidb-txnkv/tests/tikv_transaction_driver_source.rs; rust/crates/tidb-exec/src/pessimistic_lock_error.rs; rust/crates/tidb-exec/src/cluster_table_storage.rs; this ExecPlan. No native source, generated source, Go/Bazel/module file or dependency pin changed.

Validation commands from rust/:

    cargo test --locked -p tidb-exec --lib assertion_failure_
    cargo test --locked -p tidb-txnkv --lib driver::tikv_transaction::tests
    cargo test --locked -p tidb-txnkv --test all tikv_transaction_driver_source
    cargo test --locked -p tidb-exec --lib pessimistic_lock_error::tests
    cargo test --locked -p tidb-exec --lib cluster_table_storage::tests
    cargo test --locked -p tidb-txnkv --lib driver::client_bridge::ownership_regressions
    cargo test --locked -p tidb-exec --lib multi_statement_transaction::tests
    cargo test --locked -p tidb-server --lib cluster_session_node::tests::transactions
    cargo test --locked -p tidb-server --lib transaction_buffer_tests

The surrounding final suites passed 4, 9, 13, 6, 7, 11, 28 and 4 tests respectively (82 total, including the new regressions). Native verification from /Users/qiliu/projects/client-rust:

    cargo test --locked --lib ownership_regressions
    cargo test --locked --lib secondary_
    cargo test --locked --lib locked_snapshot
    cargo test --locked --lib wait_contract_tests

These passed 19, 10, 1 and 1 test invocations respectively; filters overlap, so they are not 31 unique tests. The native source tree remains clean. TiDB aggregates tikv_transaction_driver_source under --test all; the initial standalone-target command was rejected without running tests. An initial bridge filter ending in ::tests selected zero tests and was corrected to ::ownership_regressions, which ran seven. Native library tests cannot be invoked with -p tikv-client from TiDB's workspace because native dev dependencies belong to its own workspace; they were run from the native checkout instead.

Logs are /private/tmp/tidb-assertion-review-*.log and /private/tmp/tidb-review-*.log. The separate Go-master TiPB descriptor check reused compare_projection from rust/scripts/check-tipb-proto-projection.py with the September module-cache directory, without changing go.mod or generated inputs; its single enum mismatch is recorded in /private/tmp/tidb-review-tipb-master.log. The branch-pinned lint check alone cannot disprove that mismatch.

Root make lint initially could not resolve proxy.golang.org inside the sandbox, then passed when rerun with network permission. git diff --check and manual diff review passed. Publication uses `TERM=xterm git -c core.hooksPath=hooks commit` to run the required `cd rust && cargo build --locked -p tidb-server`, followed after commit by a separate fresh `cargo build --locked -p tidb-server` from rust/ and `git push origin HEAD:hparser-integration` from the repository root. Gate logs are /private/tmp/tidb-assertion-review-commit.log, -prepush.log and -push.log; neither build may be skipped. No bazel_prepare is required because no triggering input changed. No RealTiKV cluster, end-to-end session assertion-level enforcement, broad table auto-ID suite, sysbench, TPC-C, TPC-H or YCSB run was performed. No throughput improvement or full Go parity is claimed. The prior two baseline auto-ID test failures remain outside this repair.


Publication concurrency note: the first push was rejected because 545fff9a84da177eb093ff51d40f6202bc7be4d5 arrived on hparser-integration after the initial pull. That incoming buffer-drain commit was reviewed and preserved; only this unpublished assertion-error commit was rebased onto it. The newly observed ended-owner finding is listed first above. The incoming commit itself records an INSERT visibility residual. A temporary focused regression bound one staged write to a native MemDB, ended its owner, then checked both is_empty and take_native_buffer. It is saved for the ownership repair in /private/tmp/tidb-review-ended-buffer-regression.patch; the source file is restored afterward and no failing test is committed. It failed with `is_empty=true, drained_len=0`, confirming silent empty-buffer success is possible after owner loss; the log is /private/tmp/tidb-review-ended-buffer-red.log. The exact scoped command was `cd rust && cargo test --locked -p tidb-executor --lib review_ended_buffer_owner`. This is a confirmed open failure, not a passed validation gate. The existing buffer, adapter, SQL transaction and embedded transaction suites were repeated after rebase and passed 22, 6, 28 and 4 tests respectively. Those tests do not cover the ended-owner failure. The amended commit reruns the mandatory hook, followed by another fresh locked server build before retrying a normal, non-forced push.
