# Session workstream

This workstream owns the Rust statement/session context above the scalar
datatype and below the server connection. Its Go source of truth is
`pkg/sessionctx/stmtctx/**`, `pkg/sessionctx/variable/**`, `pkg/errctx/**`,
and the session lifecycle under `pkg/session/**`. Session workers must keep
statement state, warning publication, SQL-mode/type flags, and connection
variables as separate source-owned seams; they must not hide bootstrap,
authentication, or storage behavior in a context helper.

The first bounded error-policy leaf is `tidb-exec::error_context`. It ports
the fixed seven `ErrGroup` categories, copy-on-write level maps,
`ResolveErrLevel`'s ignore-over-warn precedence, and the source flag boundary
for truncate, zero-in-date, and divided-by-zero handling. It returns a typed
`ErrorDisposition` only: it does not match Go errno wrappers, append warnings,
render messages, mutate SQL mode, or attach to a live `StatementContext`. The
adjacent `statement_status` leaf remains the owner of ordered warning entries
and published counters; a future session owner will compose both at the real
statement boundary.

The adjacent `tidb-exec::warning_publication` leaf is intentionally a
read-only view over those ordered entries. `WarningPublication` preserves the
`Error`/`Warning`/`Note` level sequence and computes the protocol-sized total
and error counts without introducing a second mutable warning sink. Its source
owner is `pkg/util/context/warn.go`; StaticWarnHandler locking and mutation,
IgnoreWarn, SQLWarn JSON/error rendering, and live session/protocol attachment
remain external. `statement_status` remains the sole Rust owner of mutable
statement warning storage and finish/reset lifecycle.

Open session obligations include the complete `types.Context` conversion
flags, SQL-mode derivation, warning/error producers, charset and collation
lifecycle, system/user variables beyond the existing executor seed,
bootstrap, privilege/session authentication, and connection-scoped reset and
retry wiring. Unsupported behavior must remain explicit rather than being
silently treated as a warning or ignored error.

The isolation metadata ring now lives at `tidb-exec::isolation_state`. It
preserves TiDB's complete four-value `tx_isolation` enum, case-insensitive
name/ordinal normalization, canonical readback, and the source
`oneShotDef`/`oneShotSet`/`oneShotUse` transition used by
`IsolationLevelForNewTxn` and `SetTxnIsolationLevelOneShotStateForNextTxn`.
`READ-COMMITTED` and `REPEATABLE-READ` are exposed as the current executor's
storage-capable values without silently deleting the other two enum members.
Focused tests are in `tidb-exec/tests/isolation_state_source.rs`, with
source/test ownership in `evidence/source/session-isolation-state-wave.tsv`
and `evidence/tests/session-warning-status-publication.tsv`. The leaf deliberately
does not own `SessionVars`, warning publication for skipped levels, KV/TSO
activation, MVCC snapshots, or commit/rollback orchestration; a future session
owner must compose those pieces around this value boundary.

The non-transactional DML admission ring now lives at
`tidb-exec::nontransactional`. `NonTransactionalSessionState` is an explicit
snapshot of the source facts consumed by `pkg/session/nontransactional.go`:
autocommit with no active transaction, the batch-DML compatibility gate, weak
read consistency, and a pinned `tidb_snapshot`. `NonTransactionalDmlKind`
keeps `INSERT ... SELECT`, `REPLACE ... SELECT`, `UPDATE`, and `DELETE`
distinct from insert-without-SELECT and unsupported statements, and the
admission function preserves the source check order with typed rejection
reasons. Focused tests are in
`tidb-exec/tests/nontransactional_source.rs`, with ownership in
`evidence/source/session-nontransactional-policy-wave.tsv` and
`evidence/tests/session-nontransactional-policy-wave.tsv`.

This is a policy leaf only: it does not open/commit a transaction, parse or
validate AST table references/read clauses, select shard columns, execute
workers, publish metrics, inject failpoints, or aggregate job errors. The
existing `transaction.rs` remains the owner of transaction phase transitions;
a future session owner must pass its live `SessionVars` facts into this gate.

The retry metadata ring now lives at `tidb-exec::retry_info`. It ports the
dependency-closed deterministic portion of `pkg/sessionctx/variable/session.go`
used by the session retry loop: source-order auto-increment and auto-random ID
queues, replay offset reset, dropped prepared-statement ID cleanup, and the
`Retrying`/`LastRcReadTS` lifecycle fields. Focused tests are in
`tidb-exec/tests/retry_info_source.rs`, with the `sessionctx/variable/session.go`
source fragment co-located in `evidence/source/session-isolation-state-wave.tsv`
and direct test evidence in `evidence/tests/session-retry-info-wave.tsv`.

This is metadata only. It does not run the retry loop, rebuild plans, reset
statement contexts, begin/rollback transactions, update retry metrics, or
delete prepared statements from a plan cache. Those effects remain owned by
the future session and transaction orchestration layers.

The reserved row-ID ring now lives at `tidb-exec::reserved_row_id`. It ports
the pure `ReservedRowIDAlloc` counter from `pkg/sessionctx/stmtctx/stmtctx.go`:
resetting a `(base, max)` reservation, consuming `base + 1` through inclusive
`max`, and treating `base >= max` as exhausted. Focused tests are in
`tidb-exec/tests/reserved_row_id_source.rs`, with ownership in
the co-located `evidence/source/session-warning-status-publication.tsv` and
`evidence/tests/session-reserved-row-id-wave.tsv`.

This value owner does not reserve IDs from storage, attach to a mutate context,
mutate rows, or participate in statement reset. Those lifecycle and allocator
effects remain outside the dependency-closed leaf.

The sequence session-state ring now lives at `tidb-exec::sequence_state`. It
ports the pure numeric latest-value map from
`pkg/sessionctx/variable/sequence_state.go`: update and missing lookup,
copied snapshots, and the source `maps.Copy` merge behavior used by the
`SessionStates.SequenceLatestValues` migration field. Focused tests are in
`tidb-exec/tests/sequence_state_source.rs`, with ownership in
`evidence/source/session-sequence-state-wave.tsv` and
`evidence/tests/session-sequence-state-wave.tsv`.

This leaf does not execute `NEXTVAL`/`LASTVAL`, allocate sequence values,
serialize the complete session-state JSON envelope, or own the Go mutex/live
session. The existing executor's named sequence catalog remains a separate
runtime owner until those boundaries converge.

The session status-flag ring now lives at `tidb-exec::session_status`. It ports
the atomic `SetStatusFlag`/`HasStatusFlag`/`Status` bitfield contract from
`pkg/sessionctx/variable/session.go`, including TiDB's default autocommit bit
and the source transaction/cursor masks. Focused tests are in
`tidb-exec/tests/session_status_source.rs`, with ownership in
`evidence/source/session-status-flags-wave.tsv` and
`evidence/tests/session-status-flags-wave.tsv`.

This is a bitfield owner only. It does not set `TxnCtx.IsExplicit`, open or
commit transactions, manage cursor recordsets, or encode result packets; the
future session and protocol owners must compose those effects around this
atomic status value.

The removed-system-variable policy ring now lives at `tidb-exec::removed_sysvar`.
It ports the complete 13-entry name/reason registry from
`pkg/sessionctx/variable/removed.go`, including exact (case-sensitive) lookup
and compatibility reasons. Focused tests are in
`tidb-exec/tests/removed_sysvar_source.rs`, with ownership in
`evidence/source/session-removed-sysvar-wave.tsv` and
`evidence/tests/session-removed-sysvar-wave.tsv`.

This is a policy registry only. It does not normalize parser names, construct
`ErrVariableNoLongerSupported`, or decide SET versus SELECT behavior; those
effects remain session error and dispatch ownership.

The session option-value ring now lives at `tidb-exec::option_values`. It ports
the dependency-closed text helpers from `pkg/sessionctx/variable/varsutil.go`:
canonical boolean ON/OFF output, case-insensitive true/false and ON/OFF table
conversions with unknown-value pass-through, and the narrow `ON`/`1` TiDB
option predicate. Focused tests are in
`tidb-exec/tests/option_values_source.rs`, with source ownership merged into
the canonical `evidence/source/session-warning-status-publication.tsv` row for
`pkg/sessionctx/variable/varsutil.go` and test ownership in
`evidence/tests/session-option-values-wave.tsv`.

This is a conversion/predicate leaf only. It does not validate a concrete
system-variable type, parse SQL expressions, mutate `SessionVars`, or publish
warnings; those remain session runtime responsibilities.

The statement push-down flag ring now lives at `tidb-exec::statement_pushdown`.
It ports the dependency-closed bit synthesis from
`pkg/sessionctx/stmtctx/stmtctx.go`: truncation/overflow and zero-in-date
conversion bits, divided-by-zero level handling, source statement-kind
precedence, and `LOAD DATA`/restricted-SQL mode bits. Focused tests are in
`tidb-exec/tests/statement_pushdown_source.rs`, with source ownership merged
into the canonical `evidence/source/session-warning-status-publication.tsv` row for
`pkg/sessionctx/stmtctx/stmtctx.go` and test ownership in
`evidence/tests/session-statement-pushdown-wave.tsv`.

This is wire-bit metadata only. It does not own a live `StatementContext`,
parse SQL, build a TiKV request, serialize protocol packets, or execute a
coprocessor task; those effects remain statement, server, and storage
responsibilities.

The statement-context ID ring now lives at `tidb-exec::context_id`. It ports
the source process-local atomic sequence from `pkg/util/context/context.go`,
preserving non-zero monotonic IDs used on statement creation and reset.
Focused tests are in `tidb-exec/tests/context_id_source.rs`, with ownership in
`evidence/source/session-context-id-wave.tsv` and
`evidence/tests/session-context-id-wave.tsv`.

This is an allocator leaf only. It does not construct or reset a
`StatementContext`, acquire lifecycle locks, set timezone or warning state, or
attach IDs to a session; those effects remain session runtime responsibilities.

The session-context key ring now lives at `tidb-exec::session_context_key`. It
ports the integer-backed `basicCtxType` labels from
`pkg/sessionctx/context.go`: query-string, bootstrap/upgrade, and last-DDL
keys retain their source display strings, while arbitrary integer values
format as `unknown`. Focused tests are in
`tidb-exec/tests/session_context_key_source.rs`, with ownership in
`evidence/source/session-context-key-wave.tsv` and
`evidence/tests/session-context-key-wave.tsv`.

This is key metadata only. It does not implement `context.Context` storage,
session lifecycle, query-text attachment, bootstrap execution, DDL execution,
or snapshot validation; those effects remain session and storage owners.

The status-variable registry ring now lives at `tidb-exec::status_registry`.
It ports the provider contract from `pkg/sessionctx/variable/statusvar.go`:
providers return named values, each value receives the provider's global/
session scope, registration can be removed, and collection propagates provider
errors without fabricating status data. Focused tests are in
`tidb-exec/tests/status_registry_source.rs`, with ownership in
`evidence/source/session-status-registry-wave.tsv` and
`evidence/tests/session-status-registry-wave.tsv`.

This registry is metadata-only. It does not own live `SessionVars`, TLS
cipher/version lookup, atomic connect-attribute counters, the Go global
registry mutex, protocol status-variable rendering, or server publication;
those remain session and server responsibilities.

The process-information clone ring now lives at `tidb-exec::process_info`. It
ports the dependency-closed part of `sessmgr.ProcessInfo.Clone`: tested
identity fields, the runtime-statistics callback slot, and statement,
reference-counter, and memory-tracker owners are copied without changing
their shared identity. Focused tests are in
`tidb-exec/tests/process_info_source.rs`, with ownership in
`evidence/source/session-process-info-wave.tsv` and
`evidence/tests/session-process-info-wave.tsv`.

This is a shallow metadata boundary only. It does not render SHOW PROCESSLIST
rows, account tracker bytes, decode command/status bits, resolve transaction
timestamps, or mutate the session manager; those live process-list effects
remain external.

The advisory-lock reference-state ring now lives at
`tidb-exec::advisory_lock_state`. It ports the owner identity and source
reference-count increment/decrement/readback contract from
`pkg/session/advisory_locks.go`, preserving repeated `GET_LOCK` acquisitions
until matching `RELEASE_LOCK` calls reach zero. Focused tests are in
`tidb-exec/tests/advisory_lock_state_source.rs`, with ownership in
`evidence/source/session-advisory-locks-wave.tsv` and
`evidence/tests/session-advisory-locks-wave.tsv`.

This is reference metadata only. It does not execute the SQL-backed pessimistic
transaction, validate or normalize lock names, implement timeouts, roll back
the lock session, or publish `GET_LOCK`/`RELEASE_LOCK` results; those remain
session/expression responsibilities.

The transaction-running-state ring now lives at
`tidb-exec::txn_running_state`. It ports the five source `TxnRunningState`
discriminants and `TIDB_TRX.STATE` labels from
`pkg/session/txninfo/txn_info.go`, including the source distinction between the
`LockAcquiring` state and its `LockWaiting` display label. Focused tests are in
`tidb-exec/tests/txn_running_state_source.rs`, with ownership in
`evidence/source/session-txn-running-state-wave.tsv` and
`evidence/tests/session-txn-running-state-wave.tsv`.

This is state metadata only. It does not observe live transaction threads,
collect lock/timing metrics, retain SQL digests or process information, or
publish `TIDB_TRX` rows; those remain session, storage, and infoschema
responsibilities.

The transaction-history summary ring now lives at `tidb-exec::txn_summary`. It
ports the FNV-1a digest over a transaction's SQL-digest sequence plus the
distinct-sequence LRU promotion, capacity eviction, and resize behavior
from `pkg/session/txninfo/summary.go`. Focused tests are in
`tidb-exec/tests/txn_summary_source.rs`, with ownership in
`evidence/source/session-txn-summary-wave.tsv` and
`evidence/tests/session-txn-summary-wave.tsv`.

This is deterministic summary metadata only. It does not apply minimum-duration
filters, implement the test-only `Clean` hook, serialize JSON Datum rows,
synchronize a global recorder, observe live transactions, or publish
`TRX_SUMMARY`; those remain session/infoschema responsibilities.

The system-session pool capacity ring now lives at
`tidb-exec::session_pool_capacity`. It ports `PoolMaxSize` and the
`NewAdvancedSessionPool` boundary that maps non-positive or over-limit
capacities to the source maximum from `pkg/session/syssession/pool.go`.
Focused tests are in `tidb-exec/tests/session_pool_capacity_source.rs`, with
ownership in `evidence/source/session-pool-capacity-wave.tsv` and
`evidence/tests/session-pool-capacity-wave.tsv`.

This is capacity policy only. It does not create the factory/channel, apply
suppressed assertions, own internal-session context, transfer/reset sessions,
or close the pool; those remain system-session lifecycle responsibilities.

The system-variable scope ring now lives at `tidb-exec::sysvar_scope`. It ports
the `ScopeNone`/`ScopeGlobal`/`ScopeSession`/`ScopeInstance` bit values and the
source `String` rendering order from `pkg/sessionctx/vardef/tidb_vars.go`.
Focused tests are in `tidb-exec/tests/sysvar_scope_source.rs`, with ownership
in `evidence/source/session-sysvar-scope-wave.tsv` and
`evidence/tests/session-sysvar-scope-wave.tsv`.

This is scope metadata only. It does not register variables, validate types or
values, dispatch SET/GET, or persist global/session state; those remain the
system-variable runtime responsibilities.

The charset-variable grouping ring now lives at
`tidb-exec::charset_variable_groups`. It ports the ordered
`SetNamesVariables`/`SetCharsetVariables` lists from
`pkg/sessionctx/vardef/sysvar.go`, including their shared client/results
boundary. Focused tests are in
`tidb-exec/tests/charset_variable_groups_source.rs`, with ownership in
`evidence/source/session-charset-variable-groups-wave.tsv` and
`evidence/tests/session-charset-variable-groups-wave.tsv`.

This is grouping metadata only. It does not parse or execute SET statements,
validate collations, mutate SessionVars, or convert charset payloads; those
remain session/parser responsibilities.

The system-variable type-kind ring now lives at `tidb-exec::sysvar_type`. It
ports the byte-backed `TypeFlag` discriminants `TypeStr` through
`TypeDuration` (0..7) from `pkg/sessionctx/vardef/tidb_vars.go`, while keeping
unknown byte values representable. Focused tests are in
`tidb-exec/tests/sysvar_type_source.rs`, with ownership in
`evidence/source/session-sysvar-type-wave.tsv` and
`evidence/tests/session-sysvar-type-wave.tsv`.

This is type-kind metadata only. It does not register SysVars, validate or
parse values, invoke hooks, or convert session-variable payloads; those remain
system-variable runtime responsibilities.

The variable-error code ring now lives at `tidb-exec::sysvar_error`. It ports
the MySQL/TiDB numeric identities wrapped by `pkg/sessionctx/variable/error.go`,
including the ten codes exercised by `TestError`. Focused tests are in
`tidb-exec/tests/sysvar_error_source.rs`, with ownership in
`evidence/source/session-sysvar-error-wave.tsv` and
`evidence/tests/session-sysvar-error-wave.tsv`.

This is error-code metadata only. It does not construct dbterror values,
render messages or SQLSTATE, convert to protocol errors, or publish warnings;
those remain session/protocol responsibilities.

The SET_VAR hint-updatable registry ring now lives at
`tidb-exec::hint_updatable_vars`. It ports the complete 128-name exact-case
registry from `pkg/sessionctx/variable/setvar_affect.go`, including the
source-excluded `tidb_read_staleness` boundary. Focused tests are in
`tidb-exec/tests/hint_updatable_vars_source.rs`, with ownership in
`evidence/source/session-hint-updatable-vars-wave.tsv` and
`evidence/tests/session-hint-updatable-vars-wave.tsv`.

This is registry metadata only. It does not mutate a SysVar marker, parse or
apply optimizer hints, validate values, or drive planner behavior; those remain
session/planner responsibilities.

The session-token timing ring now lives at
`tidb-exec::session_token_timing`. It ports the classic/Starter token lifetime,
certificate reload interval, and old-certificate grace windows from
`pkg/sessionctx/sessionstates/session_token.go`, preserving the exact 1m/8h,
10m/24h, and 15m/36h mode-specific values. Focused tests are in
`tidb-exec/tests/session_token_timing_source.rs`, with ownership in
`evidence/source/session-token-timing-wave.tsv` and
`evidence/tests/session-token-timing-wave.tsv`.

This is timing metadata only. It does not load or rotate certificates, sign or
validate tokens, apply failpoints, serialize session state, or authenticate a
migrated session; those remain session/server responsibilities.

The next-generation read-only-variable ring now lives at
`tidb-exec::nextgen_readonly_vars`. It ports the case-insensitive six-name
predicate from `pkg/sessionctx/vardef/runtime.go`, preserving exact literals,
unknown-name rejection, and suffix non-matches. Focused tests are in
`tidb-exec/tests/nextgen_readonly_vars_source.rs`, with ownership in
`evidence/source/session-nextgen-readonly-vars-wave.tsv` and
`evidence/tests/session-nextgen-readonly-vars-wave.tsv`.

This is a name predicate only. It does not select a kernel, register system
variables, update lease atomics, enforce read-only writes, or apply the
next-generation configuration gate; those remain session/configuration
responsibilities.

The read-only/no-op system-variable ring now lives at
`tidb-exec::noop_read_only`. It ports the first five compatibility
registrations from `pkg/sessionctx/variable/noop.go`: canonical names and
aliases for `tx_read_only`/`transaction_read_only`, global/session scope
metadata, the `offline_mode` diagnostic distinction, and the exact
`checkReadOnly` ON/1 plus OFF/ON/WARN policy exercised by
`TestReadOnlyNoop`. Focused tests are in
`tidb-exec/tests/noop_read_only_source.rs`, with ownership in
`evidence/source/session-warning-status-publication.tsv` and
`evidence/tests/session-warning-status-publication.tsv`.

This is compatibility policy metadata only. It does not register the full
`noopSysVars` list, normalize arbitrary SysVar types, mutate `SessionVars`,
publish statement warnings/errors, or persist global variables; those remain
session/variable responsibilities.

The session reuse/close state ring now lives at
`tidb-exec::session_reuse_state`. It ports the dependency-closed
`IsAvoidReuse`, owner-gated `OwnerMarkAvoidReuse`, and idempotent `Close`
transitions from `pkg/session/syssession/session.go`, preserving the rule
that a panic can make a still-open session ineligible for reuse and that
non-owners cannot close or mark it. Focused tests are in
`tidb-exec/tests/session_reuse_state_source.rs`, with ownership in
`evidence/source/session-reuse-state-wave.tsv` and
`evidence/tests/session-reuse-state-wave.tsv`.

This is state metadata only. It does not identify owners, run resign/became
owner hooks, close a `SessionContext`, defer closure for in-use operations,
transfer ownership, or synchronize concurrent operations; the system-session
and pool owners compose those effects around this ring.

The bootstrap system-database filter ring now lives at
`tidb-exec::system_db_filter`. It ports `systemDBFilter` from
`pkg/session/global_init.go`: schema diffs are never skipped, while only the
lower-case `mysql` system database is loaded during global-variable bootstrap
(the exact `metadef.IsSystemDB` boundary). Focused tests are in
`tidb-exec/tests/system_db_filter_source.rs`, with ownership in
`evidence/source/session-system-db-filter-wave.tsv` and
`evidence/tests/session-system-db-filter-wave.tsv`.

This is filter policy metadata only. It does not normalize `DBInfo.Name.L`,
create a temporary domain, load system tables, initialize global variables, or
manage schema-diff workers; bootstrap and infoschema owners compose those
effects around this predicate.

The bootstrap upgrade-version ring now lives at
`tidb-exec::upgrade_versions`. It ports the complete ordered
`upgradeToVerFunctions` version registry from `pkg/session/upgrade_def.go`,
including the intentionally skipped historical versions, the
`upgradeToVer<N>` naming contract, and `currentBootstrapVersion = 263`.
Focused tests are in `tidb-exec/tests/upgrade_versions_source.rs`, with
ownership in `evidence/source/session-upgrade-versions-wave.tsv` and
`evidence/tests/session-upgrade-versions-wave.tsv`.

This is registry metadata only. It does not carry Go function pointers, run
upgrade SQL/DDL, mutate bootstrap rows, retry failures, or support the source
test hook that overrides the current version; upgrade execution remains a
bootstrap/session responsibility.

The non-transactional DML metric-label ring now lives at
`tidb-exec::session_metrics`. It ports the first production bindings from
`pkg/session/metrics/metrics.go`, preserving the shared counter family’s exact
`delete`, `insert`, `update` label identity and initialization order exercised
by `TestNonTransactionalMetrics`. Focused tests are in
`tidb-exec/tests/session_metrics_source.rs`, with ownership in
`evidence/source/session-metrics-labels-wave.tsv` and
`evidence/tests/session-metrics-labels-wave.tsv`.

This is metric-label metadata only. It does not register Prometheus families,
write or collect counters, attach statement labels, or implement
non-transactional DML admission/execution; session metrics and executor
owners compose those effects around this ring.

The hash-join version ring now lives at `tidb-exec::hash_join_version`. It
ports the source `legacy`/`optimized` literals, TiFlash legacy default, and
case-insensitive `IsOptimizedVersion` predicate from
`pkg/executor/join/joinversion/join_version.go`, covering the version boundary
exercised by `TestTiDBHashJoinVersion`. Focused tests are in
`tidb-exec/tests/hash_join_version_source.rs`, with ownership in
`evidence/source/executor-hash-join-version-wave.tsv` and
`evidence/tests/executor-hash-join-version-wave.tsv`.

This is version metadata only. It does not probe runtime pointer support,
apply non-GA join gates, validate or mutate system variables, choose a planner
join implementation, or execute hash joins; planner, session, and executor
owners compose those effects around this predicate.

The slow-log rule-composition ring now lives at
`tidb-exec::slow_log_match`. It ports the source `Match` AND-within/OR-across
truth table and `ShouldWriteSlowLog` precedence (session rules, then
connection-specific global rules, then the `-1` global sentinel) from
`pkg/executor/adapter_slow_log.go`. Focused tests are in
`tidb-exec/tests/slow_log_match_source.rs`, with ownership in
`evidence/source/executor-slow-log-match-wave.tsv` and
`evidence/tests/executor-slow-log-match-wave.tsv`.

This is logical composition only. It does not parse rule text, resolve field
accessors or thresholds, maintain SessionVars/global rule maps, collect slow
query items, or write logs; the executor/session slow-log owners compose those
effects around this ring.

The privilege-set helper ring now lives at `tidb-exec::privilege_set`. It
ports `SetFromString`, `setToString`, `addToSet`, and `deleteFromSet` from
`pkg/executor/utils.go`, preserving comma splitting (including empty members),
empty-value handling, exact duplicate suppression, insertion order, and
first-match deletion. Focused tests are in
`tidb-exec/tests/privilege_set_source.rs`, with ownership in
`evidence/source/executor-privilege-set-wave.tsv` and
`evidence/tests/executor-privilege-set-wave.tsv`.

This is collection metadata only. It does not parse privilege SQL, apply
GRANT/REVOKE authorization, persist `mysql.*` privilege rows, or perform
collation-aware comparisons; executor/privilege owners compose those effects
around these helpers.

The effective-authentication-plugin ring now lives at
`tidb-exec::effective_auth_plugin`. It ports the dependency-closed selection
helper from `pkg/executor/simple.go`: an explicit plugin wins, an empty plugin
uses `default_authentication_plugin`, and an unavailable default falls back to
`mysql_native_password`. Focused tests are in
`tidb-exec/tests/effective_auth_plugin_source.rs`, with ownership in
`evidence/source/executor-effective-auth-plugin-wave.tsv` and
`evidence/tests/executor-effective-auth-plugin-wave.tsv`.

This is value resolution only. It does not decode privilege-cache rows,
determine dual-password capability, mutate authentication variables, compare
plugins during `ALTER USER`, store passwords, or authenticate a live session;
those remain executor, privilege, and session responsibilities.

The BroadcastQuery compatibility-error ring now lives at
`tidb-exec::broadcast_query_error`. It ports the nil-safe two-fragment
classifier from `pkg/executor/analyze.go`: an error is an unsupported peer only
when it contains both `exec type` and `doesn't support yet`. Focused tests are
in `tidb-exec/tests/broadcast_query_error_source.rs`, with ownership in
`evidence/source/executor-broadcast-query-error-wave.tsv` and
`evidence/tests/executor-broadcast-query-error-wave.tsv`.

This is error-message classification only. It does not unwrap Go error chains,
dial TiDB RPC endpoints, emit warnings, flush statistics, or choose analyze
fallback behavior; those remain executor, statistics, and RPC responsibilities.

The insert row/column work-accounting ring now lives at
`tidb-exec::insert_rows_col_multiply`. It ports the zero-aware,
`i64::MAX`-saturating `rowCount * insertColumnCount` helper from
`pkg/executor/insert_common.go`. Focused tests are in
`tidb-exec/tests/insert_rows_col_multiply_source.rs`, with ownership in
`evidence/source/executor-insert-rows-col-multiply-wave.tsv` and
`evidence/tests/executor-insert-rows-col-multiply-wave.tsv`.

This is arithmetic metadata only. It does not own `InsertValues`, count rows,
publish RUV2 metrics, update statement labels, or execute inserts; those remain
executor/session metric responsibilities.

The readable-size parsing ring now lives at `tidb-exec::readable_size`. It
ports `convertReadableSizeToByteSize` from `pkg/executor/inspection_result.go`,
including decimal bytes, case-sensitive binary suffixes through PiB, the
three-byte suffix boundary, and source-compatible uint64 multiplication.
Focused tests are in `tidb-exec/tests/readable_size_source.rs`, with ownership
in `evidence/source/executor-readable-size-wave.tsv` and
`evidence/tests/executor-readable-size-wave.tsv`.

This is parser metadata only. It does not retrieve inspection rows, append
warnings, aggregate block-cache state, or render diagnostic results; those
remain executor/inspection responsibilities.

The SHOW PLACEMENT label-aggregation ring now lives at
`tidb-exec::placement_labels`. It ports the deterministic portion of
`pkg/executor/show_placement.go`: nil stores are ignored, values are grouped
by label key, duplicates are removed, and keys/values are emitted in
lexicographic order. Focused tests are in
`tidb-exec/tests/placement_labels_source.rs`, with ownership in
`evidence/source/executor-placement-labels-wave.tsv` and
`evidence/tests/executor-placement-labels-wave.tsv`.

This is decoded-label metadata only. It does not parse BinaryJSON, reject
malformed/non-array JSON, query PD or store status, resolve placement policy,
or encode SQL result rows; those remain executor/placement responsibilities.

The integer SET CONFIG JSON ring now lives at `tidb-exec::config_int_json`. It
ports the ETInt branch from `pkg/executor/set_config.go`: boolean-flag values
map zero to `false` and all non-zero values to `true`, while ordinary integers
remain decimal JSON numbers. Focused tests are in
`tidb-exec/tests/config_int_json_source.rs`, with ownership in
`evidence/source/executor-config-int-json-wave.tsv` and
`evidence/tests/executor-config-int-json-wave.tsv`.

This is scalar rendering only. It does not evaluate expressions, reject null
or unsupported types, escape config keys, render strings/reals/decimals, send
HTTP requests, or publish SET CONFIG warnings; those remain executor/session
responsibilities.

The ordered missing-handle ring now lives at `tidb-exec::lack_handles`. It
ports `GetLackHandles` from `pkg/executor/distsql.go`: expected handles are
walked in order, matches are removed from the obtained set, and missing output
stops at the source cardinality difference. Focused tests are in
`tidb-exec/tests/lack_handles_source.rs`. Its indivisible source/test ownership
is now consolidated under `executor-table-index-reader-runtime`, recorded by
`evidence/transfers/executor-table-index-reader-runtime.tsv`; the implementation
and test remain unchanged while the successor adds the connected reader runtime.

This is collection reconciliation only. It does not encode KV handles, fetch
index/table rows, run lookup workers, report consistency errors, or access
storage; those remain executor/DAG and storage responsibilities.

The slow-log field-splitting ring now lives at `tidb-exec::slow_log_split`. It
ports the deterministic `splitByColon`/bracket matcher from
`pkg/executor/slow_query.go`: ASCII key starts, plain and empty values, nested
same-type `[]`/`{}` values, and malformed-bracket rejection are preserved.
Focused tests are in `tidb-exec/tests/slow_log_split_source.rs`, with
ownership in `evidence/source/executor-slow-log-split-wave.tsv` and
`evidence/tests/executor-slow-log-split-wave.tsv`.

This is string parsing only. It does not read slow-log files, emit parse
warnings, convert timestamps, populate datums, or apply privilege/time-range
filters; those remain executor/session responsibilities.

The slow-log threshold ring now lives at `tidb-exec::slow_log_threshold`. It
ports the source `MatchEqual`, numeric `>=`, `uint64FromNonNegative`, and
`matchZero` boundaries from `pkg/sessionctx/variable/slow_log.go` into an
explicit typed value domain. Focused tests are in
`tidb-exec/tests/slow_log_threshold_source.rs`, with ownership in
`evidence/source/session-slow-log-threshold-wave.tsv` and
`evidence/tests/session-slow-log-threshold-wave.tsv`.

This is threshold metadata only. It does not parse slow-log fields, register
accessors, populate `SlowQueryLogItems`, read statement/session state, or
render a slow log; those lifecycle and I/O effects remain external.

The slow-log rule metadata ring now lives at `tidb-exec::slow_log_rules`. It
ports the source condition/rule grouping, session effective-field invalidation
marker, and global connection-ID map from `pkg/sessionctx/slowlogrule/rules.go`.
Focused tests are in `tidb-exec/tests/slow_log_rules_source.rs`, with
ownership in `evidence/source/session-slow-log-rules-wave.tsv` and
`evidence/tests/session-slow-log-rules-wave.tsv`.

This is rule metadata only. It does not parse raw rule strings, evaluate AND/
OR conditions, compute global hashes, or attach rules to a live
`SessionVars`; those remain slow-log and session runtime responsibilities.

The analyze-worker panic ring now lives at
`tidb-exec::analyze_panic_error`. It ports the dependency-closed
`getAnalyzePanicErr`/`isAnalyzeWorkerPanic` branches from
`pkg/executor/analyze_utils.go`: the exact global analyze-memory sentinel maps
to the source OOM error, non-sentinel errors are propagated by message, and
plain/other recovered values use the worker-panic sentinel. Focused tests are
in `tidb-exec/tests/analyze_panic_error_source.rs`, with ownership in
`evidence/source/executor-analyze-panic-error-wave.tsv` and
`evidence/tests/executor-analyze-panic-error-wave.tsv`.

This is panic-value classification only. It does not recover goroutines, emit
logs, schedule analyze workers, publish memory errors, or retry analyze work;
those remain executor and analyze-session responsibilities.

The delete row/column accounting ring now lives at
`tidb-exec::delete_rows_col_multiply`. It ports the dependency-closed
`addDeleteRowsColMultiply` saturating accumulator from
`pkg/executor/delete.go`: non-positive deltas and an already saturated total
remain unchanged, while positive overflow clamps at `math.MaxInt64`. Focused
tests are in `tidb-exec/tests/delete_rows_col_multiply_source.rs`, with
ownership in `evidence/source/executor-delete-rows-col-multiply-wave.tsv` and
`evidence/tests/executor-delete-rows-col-multiply-wave.tsv`.

This is metric arithmetic only. It does not iterate DELETE chunks, build
handles, apply foreign-key filtering, flush batches, mutate rows, or publish
RUV2/session metrics; those remain executor and session responsibilities.

The CTE first-error ring now lives at `tidb-exec::cte_first_error`. It ports
the dependency-closed `setFirstErr` rule from `pkg/executor/cte.go`: a first
non-`nil` error is retained unchanged, a new error is adopted only when the
current value is empty, and nil/new combinations remain nil-safe. Focused
tests are in `tidb-exec/tests/cte_first_error_source.rs`, with ownership in
`evidence/source/executor-cte-first-error-wave.tsv` and
`evidence/tests/executor-cte-first-error-wave.tsv`.

This is precedence metadata only. It does not log later errors, close CTE
workers, order cleanup, trigger failpoints, spill storage, or execute CTE
queries; those remain executor and session responsibilities.

The traffic form ring now lives at `tidb-exec::traffic_form`. It ports the
deterministic `getForm`/`url.Values.Encode` boundary from
`pkg/executor/traffic.go`: field keys are sorted lexicographically, spaces use
`+`, unreserved bytes pass through, and all other bytes use uppercase
percent-encoding. Focused tests are in
`tidb-exec/tests/traffic_form_source.rs`, with ownership in
`evidence/source/executor-traffic-form-wave.tsv` and
`evidence/tests/executor-traffic-form-wave.tsv`.

This is form serialization only. It does not parse TRAFFIC statements, add
start timestamps, discover TiProxy servers, choose object-store paths, send
HTTP requests, or publish session warnings; those remain executor/session
responsibilities.

The DDL-job comment ring now lives at `tidb-exec::ddl_job_comments`. It ports
the dependency-closed `showCommentsFromJob` and `showCommentsFromSubjob`
formatters from `pkg/executor/show_ddl_jobs.go`, preserving analyze labels,
reorg/DXF/cloud ordering, default worker settings, placement labels, and the
next-gen early-return boundary. Focused source-contract tests are in
`tidb-exec/tests/ddl_job_comments_source.rs`, with ownership in
`evidence/source/executor-ddl-job-comments-wave.tsv` and
`evidence/tests/executor-ddl-job-comments-wave.tsv`.

This is metadata formatting only. It does not own DDL job/reorg storage,
kernel-mode selection, job execution, subjob lifecycle, or `SHOW DDL JOBS`
result-row assembly; those remain executor/DDL responsibilities.

The dynamic system-variable initial-value ring now lives at
`tidb-exec::global_sysvar_initial`. It ports the dependency-closed
`GlobalSystemVariableInitialValue` override table from
`pkg/sessionctx/variable/sysvar.go`, taking explicit store/test/kernel facts
to preserve TiKV async/1PC defaults, test OOM/auto-analyze defaults, row-format
installation, assertion level, mutation-checker, and pessimistic fair-locking
branches. Focused source-contract tests are in
`tidb-exec/tests/global_sysvar_initial_source.rs`, with ownership in
`evidence/source/session-global-sysvar-initial-wave.tsv` and
`evidence/tests/session-global-sysvar-initial-wave.tsv`.

This is an initial-value policy only. It does not read global configuration or
test singletons, register system variables, mutate `SessionVars`, validate SET
values, or publish session state; those remain variable/session responsibilities.

The tagged-pointer metadata ring now lives at `tidb-exec::tagged_ptr`. It
ports the dependency-closed high-bit tag-width, mask initialization, tag
extraction, encode, and raw-address clearing rules from
`pkg/executor/join/tagged_ptr.go`. Focused source-contract tests are in
`tidb-exec/tests/tagged_ptr_source.rs`, with ownership in
`evidence/source/executor-tagged-ptr-wave.tsv` and
`evidence/tests/executor-tagged-ptr-wave.tsv`.

This is raw-address metadata only. It does not allocate or dereference unsafe
pointers, own GC identity, store join rows, or coordinate concurrent hash-join
execution; those remain join/executor responsibilities.

The population-standard-deviation finalization ring now lives at
`tidb-exec::stddevpop`. It ports the dependency-closed final boundary from
`pkg/executor/aggfuncs/func_stddevpop.go`: zero count emits NULL, while a
nonzero count returns `sqrt(variance / count)`, including source NaN behavior
for negative variance. Focused source-contract tests are in
`tidb-exec/tests/stddevpop_source.rs`, with ownership in
`evidence/source/executor-stddevpop-wave.tsv` and
`evidence/tests/executor-stddevpop-wave.tsv`.

This is final numeric normalization only. It does not accumulate or merge
variance state, coerce input values, write chunks, construct aggregate
functions, or execute window/aggregate operators; those remain executor and
aggregate responsibilities.

The sample-standard-deviation finalization ring now lives at
`tidb-exec::stddevsamp`. It ports the dependency-closed final boundary from
`pkg/executor/aggfuncs/func_stddevsamp.go`: counts at or below one emit NULL,
while larger counts return `sqrt(variance / (count - 1))`, including source
NaN behavior for negative variance. Focused source-contract tests are in
`tidb-exec/tests/stddevsamp_source.rs`, with ownership in
`evidence/source/executor-stddevsamp-wave.tsv` and
`evidence/tests/executor-stddevsamp-wave.tsv`.

This is sample numeric normalization only. It does not accumulate or merge
variance state, coerce input values, write chunks, construct aggregate
functions, or execute aggregate operators; those remain executor and
aggregate responsibilities.

The sample-variance finalization ring now lives at `tidb-exec::varsamp`. It
ports the dependency-closed final boundary from
`pkg/executor/aggfuncs/func_varsamp.go`: counts at or below one emit NULL,
while larger counts return `variance / (count - 1)`, preserving signed and
floating-point results. Focused source-contract tests are in
`tidb-exec/tests/varsamp_source.rs`, with ownership in
`evidence/source/executor-varsamp-wave.tsv` and
`evidence/tests/executor-varsamp-wave.tsv`.

This is sample numeric normalization only. It does not accumulate or merge
variance state, coerce input values, write chunks, construct aggregate
functions, or execute aggregate operators; those remain executor and
aggregate responsibilities.

The cumulative-distribution ranking ring now lives at `tidb-exec::cume_dist`.
It ports the dependency-closed `curIdx`/`lastRank` peer-run loop and partial
state shape from `pkg/executor/aggfuncs/func_cume_dist.go` over caller-provided
sorted keys. Focused source-contract tests are in
`tidb-exec/tests/cume_dist_source.rs`, with ownership in
`evidence/source/executor-cume-dist-wave.tsv` and
`evidence/tests/executor-cume-dist-wave.tsv`.

This is peer-rank metadata only. It does not compare typed chunk rows, write
result chunks, account executor memory, construct window functions, or schedule
window execution; those remain aggregate/executor responsibilities.

The NTILE state ring now lives at `tidb-exec::ntile`. It ports the
dependency-closed five-field partial state, quotient/remainder update,
group-advance, reset, and zero-divisor NULL rules from
`pkg/executor/aggfuncs/func_ntile.go`. Focused source-contract tests are in
`tidb-exec/tests/ntile_source.rs`, with ownership in
`evidence/source/executor-ntile-wave.tsv` and
`evidence/tests/executor-ntile-wave.tsv`.

This is NTILE state arithmetic only. It does not coerce the divisor, write
chunks, construct aggregate functions, account row memory, or schedule window
execution; those remain aggregate/executor responsibilities.

The statement-context reference ring now lives at
`tidb-exec::statement_refcount`. It ports the atomic `ReferenceCount`
frozen/no-reference sentinel and CAS transitions from
`pkg/sessionctx/stmtctx/stmtctx.go`, which gate cached context reuse in
`pkg/sessionctx/variable/session.go`. Focused source-contract tests are in
`tidb-exec/tests/statement_refcount_source.rs`, with source ownership merged
into the canonical `evidence/source/session-warning-status-publication.tsv` row for
`pkg/sessionctx/stmtctx/stmtctx.go` and test ownership in
`evidence/tests/session-statement-refcount-wave.tsv`.

This is a synchronization primitive only. It does not own cached context
objects, reset locks, session-variable fallback allocation, or statement
execution; the session lifecycle must compose those effects around this
atomic state.

The used-statistics slow-log ring now lives at `tidb-exec::used_stats`. It
ports the deterministic `UsedStatsInfoForTable.WriteToSlowLog` boundary from
`pkg/sessionctx/stmtctx/stmtctx.go`: pseudo/real metadata versions, realtime and
modify counts, index-before-column status sections, and sorted `ID <id>`
fallback names when table metadata is absent. Focused tests are in
`tidb-exec/tests/used_stats_source.rs`, with ownership in
source ownership merged into the existing
`evidence/source/session-warning-status-publication.tsv` row for
`pkg/sessionctx/stmtctx/stmtctx.go` and test ownership in
`evidence/tests/session-used-stats-wave.tsv`.

This is formatting metadata only. It does not collect statistics, resolve
column/index names from `TableInfo`, implement `FormatForExplain`, write to a
slow-log sink, or attach to a live statement/session.

The plan-cache parameter ring now lives at `tidb-exec::plan_cache_params`. It
ports the typed `PlanCacheParamList` storage boundary from
`pkg/sessionctx/variable/session.go`: source-order append/reset, indexed and
borrowed all-value access, and the non-prepared-cache privacy bit. Focused
tests are in `tidb-exec/tests/plan_cache_params_source.rs`; source ownership is
merged into the existing `evidence/source/session-isolation-state-wave.tsv`
row for `pkg/sessionctx/variable/session.go`, with test evidence in
`evidence/tests/session-plan-cache-params-wave.tsv`.

This is parameter metadata only. It does not render argument text, coerce
parameters, evaluate prepared plans, or attach to a live `SessionVars` or
`EvalContext`; those remain session and expression responsibilities.

The statistics-load result ring now lives at `tidb-exec::stats_load_result`.
It ports the dependency-closed `StatsLoadResult` item identity, `HasError`,
and stable `ErrorMsg` text from `pkg/sessionctx/stmtctx/stmtctx.go`, covering
the metadata exchanged by the statistics sync-loader. Focused tests are in
`tidb-exec/tests/stats_load_result_source.rs`, with ownership in
`evidence/source/session-stats-load-result-wave.tsv` and
`evidence/tests/session-stats-load-result-wave.tsv`.

This is result metadata only. It does not run workers, retries, channels,
failpoints, storage reads, or attach results to a live statement/session; the
statistics handle and session owners must compose those effects.

The alternative-plan signal ring now lives at
`tidb-exec::alternative_plan_signals`. It ports the eight statement-local
boolean signals and source mark/reset transitions from
`pkg/sessionctx/stmtctx/stmtctx.go`, preserving the planner's candidate
metadata boundary. Focused tests are in
`tidb-exec/tests/alternative_plan_signals_source.rs`, with ownership in
`evidence/source/session-alternative-plan-signals-wave.tsv` and
`evidence/tests/session-alternative-plan-signals-wave.tsv`.

This is planner-round metadata only. It does not enable alternative rounds,
run rules, compare costs, inspect SQL/AST, trigger failpoints, or attach to a
live statement/session; planner and session owners compose those effects.

The read-consistency value ring now lives at `tidb-exec::read_consistency`. It
ports the strict/weak labels, exact `IsWeak` predicate, and case-insensitive
validation boundary from `pkg/sessionctx/variable/session.go`. Focused tests
are in `tidb-exec/tests/read_consistency_source.rs`, with ownership in
`evidence/source/session-read-consistency-wave.tsv` and
`evidence/tests/session-read-consistency-wave.tsv`.

This is value/validation metadata only. It does not mutate `SessionVars`,
select KV request isolation, open transactions, or enforce
non-transactional-DML policy; those remain session, transaction, and executor
responsibilities.

The chunk-allocation usage ring now lives at
`tidb-exec::chunk_alloc_status`. It ports the statement-local
set/clear/readback flag from `pkg/sessionctx/stmtctx/stmtctx.go`, preserving the
marker consumed by reusable chunk allocation. Focused tests are in
`tidb-exec/tests/chunk_alloc_status_source.rs`, with ownership in
`evidence/source/session-chunk-alloc-status-wave.tsv` and
`evidence/tests/session-chunk-alloc-status-wave.tsv`.

This is a usage marker only. It does not allocate chunks, manage pools or
columns, validate reuse, or attach to `SessionVars`; allocator and session
owners compose those effects around this state.

The SET_VAR hint restore ring now lives at
`tidb-exec::setvar_hint_restore`. It ports the first-write-wins old-value map
from `pkg/sessionctx/stmtctx/stmtctx.go`, preserving the value needed to undo
multiple applications of the same statement hint. Focused tests are in
`tidb-exec/tests/setvar_hint_restore_source.rs`, with ownership in
the canonical `evidence/source/session-warning-status-publication.tsv` row and
`evidence/tests/session-setvar-hint-restore-wave.tsv`.

This is restore metadata only. It does not parse hints, mutate system
variables, publish warnings, perform restoration, or attach to a live
statement/session; planner and session owners compose those effects.

The session cursor ring now lives at `tidb-exec::cursor_tracker`. It ports the
dependency-closed `pkg/session/cursor` state and tracker contract: a cursor
captures its `StartTS`, receives a monotonic ID beginning at one, can be looked
up or visited through an early-stoppable range snapshot, and removes itself on
`Close`. Focused source-contract tests are in
`tidb-exec/tests/cursor_tracker_source.rs`, with source/test ownership in
`evidence/source/session-cursor-tracker-wave.tsv` and
`evidence/tests/session-cursor-tracker-wave.tsv`.

This owner deliberately does not set `SERVER_STATUS_CURSOR_EXISTS`, execute or
encode a recordset, pin a transaction snapshot, or own session cleanup. A
future session/protocol owner must compose those effects around the tracker.

The `tx_read_ts` metadata ring now lives at `tidb-exec::txn_read_ts`. It ports
the dependency-closed `TxnReadTS` value object from
`pkg/sessionctx/variable/session.go`: consuming marks the value used without
changing the timestamp, setting a new timestamp refreshes that marker, peeking
is non-consuming, and cleanup resets only a used non-zero timestamp. Focused
tests are in `tidb-exec/tests/txn_read_ts_source.rs`, with source/test
ownership co-located with the `session.go` fragment in
`evidence/source/session-isolation-state-wave.tsv`, plus direct test evidence
in
`evidence/tests/session-txn-read-ts-wave.tsv`. The leaf deliberately does not
parse `tx_read_ts`, call the timestamp oracle, construct stale-read snapshots,
or clear `SnapshotInfoschema`; those effects belong to the future session and
stale-read owners.

The lazy-transaction state ring now lives at `tidb-exec::lazy_txn_state`. It
ports the dependency-closed boolean predicates from `pkg/session/txn.go`:
`Valid` requires an allocated valid transaction, `pending` requires no
transaction plus a future, and `validOrPending` accepts either a future or a
valid transaction. Focused tests are in
`tidb-exec/tests/lazy_txn_state_source.rs`, with ownership in
`evidence/source/session-lazy-txn-state-wave.tsv`. The original focused Go
anchors are already owned by the transaction rings in
`evidence/tests/transaction-wave17.tsv` and
`evidence/tests/transaction-wave18.tsv`, so this leaf adds no duplicate test
ownership row.

This is state composition only. It does not allocate or validate a KV
transaction, activate a future, observe timestamps, acquire locks, or execute
BEGIN/COMMIT/ROLLBACK; those remain session and storage responsibilities.

The bounded bootstrap ring now lives at `tidb-server::bootstrap`. It ports only
the source-pure `getStartMode` version boundary, the user-keyspace versus
SYSTEM-keyspace ordering guard, the conjunctions for next-generation schema,
grant-table, secure-bootstrap, plugin, telemetry, TiFlash, bootstrap-SQL, and
Etcd gates, and a coarse phase-order audit contract. Focused tests are in
`tidb-server/tests/bootstrap_source.rs`, with source/test ownership in
`evidence/source/session-bootstrap-phase-wave.tsv` and
`evidence/tests/session-bootstrap-phase-wave.tsv`. The actual bootstrap remains
external: KV reads/writes and transactions, owner locks, domain/DDL startup,
system-table definitions, privilege/user rows, sysvar cache, plugin callbacks,
secure-bootstrap OS-user lookup, SQL-file I/O, and background workers have no
dependency-closed Rust seam yet and must not be hidden behind this metadata.
