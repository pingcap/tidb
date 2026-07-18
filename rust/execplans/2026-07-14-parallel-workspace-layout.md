# Refactor the Rust workspace for parallel TiDB rewrite work

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at the repository root; this plan follows its required format.

## Purpose / Big Picture

The Rust rewrite must become a team-scale implementation, not a serial queue of edits to three large files. After this refactor, contributors can own a statement family and its Go-source corpus without colliding on the workspace layout, the AST root, the parser root, or the executor state machine. The observable proof is that a feature wave changes a domain module, matching test module, and source-derived corpus while the fixed routing seams remain untouched; Cargo still builds and every static differential ring reports the same reviewed outcome counts.

The target physical layout follows the approved design document without inventing empty future crates:

    rust/
      crates/{tidb-lexer,tidb-ast,tidb-parser,tidb-proto,tidb-protocol,tidb-distsql,tidb-datatype,tidb-codec,tidb-expr,tidb-planner,tidb-stats,tidb-txnkv,tidb-exec,tidb-server}/
      difftests/{src,parser-tests,result-tests,transaction-tests}/
      docs/{architecture,operations}/
      workstreams/{parser,datatype,evidence,result,plan,stats,transaction,protocol,distsql}/
      execplans/

`difftests` is plural because it owns parser, result, plan, and transaction
differential rings. Its root Cargo package remains `difftest` for shared
evidence code and generators. The real `difftest-parser-tests`,
`difftest-planner-tests`, `difftest-result-tests`, and
`difftest-transaction-tests` packages enforce
compile-time ownership boundaries. Future design crates are created only when
a real API and consumer exist. Protocol and DistSQL now qualify because they
own source-backed wire/context leaves with real `tidb-exec` consumers, and
`tidb-server` qualifies because its command lifecycle consumes those seams;
the remaining future crates stay absent until the same proof exists.

## Progress

- [x] (2026-07-14) Mapped the current workspace, ownership protocol, Go-source evidence flow, and hot shared roots.
- [x] (2026-07-14) Chose a staged migration that waits for the active Wave 24 parser edit before moving physical paths.
- [x] (2026-07-14) Quiesced core-file feature edits and recorded the reviewed 45,703-match parser snapshot.
- [x] (2026-07-14) Moved the six existing workspace members to `crates/` and `difftests/`, repaired path-sensitive tools, and validated the full WIP ring.
- [x] (2026-07-14) Replaced the stale parallel protocol with an ownership board and parser/result/plan/transaction workstream contracts.
- [x] (2026-07-14) Split parser, AST, and executor roots into stable domain routing seams without behavior changes: `SessionStmt` (12 variants), `AdminStmt` (11 variants), `QueryStmt` (2 variants), `DmlStmt` (3 variants), and `DdlStmt` (15 variants).
- [x] (2026-07-14) Migrated the flat `Stmt` boundary into the five domain envelopes and proved the structural migration did not change its reviewed parser snapshot.
- [x] (2026-07-14) Recorded outcomes and retained the new layout and ownership protocol.
- [x] (2026-07-15) Made `tidb-datatype::Datum` the sole scalar definition and deleted the duplicate `tidb-expr::Value` path.
- [x] (2026-07-15) Split shared evidence, parser rings, and result rings into three Cargo packages; proved the parser package has no expression/executor dependency.
- [x] (2026-07-15) Proved the source-domain module pattern with independently owned partition DDL and account-security parser slices.
- [x] (2026-07-15) Extracted the complete GRANT/REVOKE privilege grammar from the parser root into source-owned `privilege.rs`; the security selector and reviewed static parser snapshot remain unchanged.
- [x] (2026-07-15) Extracted the complete binding AST/restore and CREATE/DROP/SET/SHOW parser domain into source-owned `stmt/binding.rs` and `binding.rs`, with mirrored focused tests and both query/DML binding selectors.
- [x] (2026-07-15) Extracted every currently typed ordinary SHOW inspection payload, restore path, parser branch, and mirrored test into source-owned `stmt/show.rs`, `show.rs`, and `tests/show.rs`; kept SHOW GRANTS, SHOW CREATE USER, bindings, and ADMIN SHOW with their semantic owners, and preserved all 159 SHOW selector rows.
- [x] (2026-07-15) Extracted ordinary session/system-variable SET AST, restore, parser grammar, and mirrored tests into source-owned `stmt/set.rs`, `set.rs`, and `tests/set.rs`; kept bindings, password, roles, and user-variable forms with their semantic owners, preserved all 120 SET selector rows, and retained the reviewed static parser snapshot.
- [x] (2026-07-15) Extracted every currently typed `ADMIN` command parser, payload/restore, and mirrored test into source-owned `admin.rs`, `stmt/admin.rs`, and `tests/admin.rs`; reduced the top parser to one ADMIN route, preserved all 146 ADMIN selector rows, and kept standalone ANALYZE/FLUSH with their distinct Go source owners.
- [x] (2026-07-15) Extracted the current typed `ANALYZE TABLE` parser, payload/restore, and original-test mapping into source-owned `analyze.rs`, `stmt/analyze.rs`, and `tests/analyze.rs`; added a 289-row Go-derived selector and kept broader statistics payloads plus standalone FLUSH outside this mechanical slice.
- [x] (2026-07-15) Extracted the current typed standalone `FLUSH` parser, payload/restore, and original-test mapping into source-owned `flush.rs`, `stmt/flush.rs`, and `tests/flush.rs`; consolidated STATUS/PRIVILEGES/TABLE[S] coverage into one 18-row selector while leaving ADMIN FLUSH and stateful targets with their distinct owners.
- [x] (2026-07-15) Translated the complete `traffic_parser.go` source unit into `traffic.rs`, `stmt/traffic.rs`, and `tests/traffic.rs`: TRAFFIC capture/replay/jobs, REFRESH STATS scoped objects and dedup, secure-text redaction, one 4-row static selector, and explicit executor Unsupported routing.
- [x] (2026-07-15) Replaced the partial generic-DDL resource-group path with the complete `ddl_resource_group_parser.go` source unit in `resource_group.rs`, `stmt/resource_group.rs`, and `tests/resource_group.rs`: all 158 original `TestDDL` rows and all 21 AST restore pairs execute, CREATE/ALTER/DROP share one source owner, and cluster admission-control execution rejects before transaction mutation.
- [x] (2026-07-15) Extracted the complete `ddl_placement_parser.go` source unit into typed placement leaves: all 36 attributable `TestDDL` rows and all 12 create/alter/drop restore assertions execute, while the executor rejects before transaction mutation.
- [x] (2026-07-15) Extracted the complete `ddl_masking_parser.go` source unit into typed masking leaves: all 20 attributable `TestDDL` rows execute, invalid boolean state is eliminated by enums/sets, and the executor rejects before mutation; the static integration corpus contains no masking-policy DDL and therefore gained no fake zero-row selector.
- [x] (2026-07-15) Consolidated every currently translated `pkg/expression/builtin_time.go` behavior into source-owned `tidb-expr/src/time_fn/`; deleted `builtin_ext/time2.rs`, removed 202 behavior/helper lines from `func.rs`, and retained one narrow time-family dispatch.
- [x] (2026-07-15) Made scalar and result-cell evidence byte-lossless, with explicit marker escaping and Go-generated invalid-UTF-8 goldens.
- [x] (2026-07-15) Replaced package-sized test dispatch with exact source units and made generated evidence/workspace ownership a first-class workstream.
- [x] (2026-07-15) Extracted the current single-session transaction lifecycle into source-owned `tidb-exec/src/transaction.rs`, moved mirrored lifecycle tests to `tests/transaction.rs`, and reduced the immediate `lib.rs` plus `database.rs` baseline by 286 lines without changing behavior.
- [x] (2026-07-15) Created the first real `tidb-txnkv` boundary from `pkg/kv/key.go` and `version.go`, with a separate transaction evidence package and Go-generated byte fixtures; no protocol or MVCC placeholder was introduced.
- [x] (2026-07-15) Added a generated production-source ledger that inventories every non-test Go file, routes it to a design crate or explicit triage/deferred boundary, and validates a sparse evidence overlay.
- [x] (2026-07-15) Translated the complete UTF-8 `ddl_load_data_parser.go` surface into source-owned AST/parser/test leaves: all 73 attributable `TestSimple` rows and all 22 `TestLoadDataRestore` rows execute; the source remains honestly partial at the global non-UTF-8 restore boundary.
- [x] (2026-07-15) Translated `pkg/types/binary_literal.go` and every original `TestBinaryLiteral` row into a typed datatype leaf; invalid Go byte-width panic states cannot be constructed in Rust.
- [x] (2026-07-15) Translated the complete dependency-leaf `pkg/types/fsp.go` source and all three original tests into byte-preserving `tidb-datatype` APIs; Go's arbitrary-byte string boundary does not become a Rust UTF-8 slicing edge case.
- [x] (2026-07-15) Translated both `pkg/parser/types/eval_type.go` and its `pkg/types/eval_type.go` alias surface into one exhaustive `EvalType`; all invalid byte discriminants are rejected before the Go panic-only display state can exist.
- [x] (2026-07-15) Translated `pkg/types/enum.go` and `set.go` against one byte-preserving collation authority; mechanically generated all 65,536 general-CI weights, 65,536 UCA-4.0 weights, and 22 long-rune expansions from Go, and executed every attributable Enum/Set, compare/key, invalid-UTF-8, and UCA-data assertion without an ASCII fallback.
- [x] (2026-07-15) Sharded production, test, and parser evidence into owner/source fragments with deterministic generated inventories, duplicate/stale/artifact validation, and agent-local parser fragment checks.
- [x] (2026-07-15) Shrunk crate roots without behavior changes: datatype `lib.rs` 833 to 38 lines, codec `lib.rs` 395 to 48, AST `lib.rs` 396 to 217, parser `lib.rs` 1,958 to 1,429, and executor `lib.rs` 1,296 to 988; statement routing, result/error/settings, scalar implementation, and mirrored tests now have independent leaves.
- [x] (2026-07-15) Split executor admin and session runtime behavior from the catalog coordinator into `admin_runtime.rs` and `session_runtime.rs`; `database.rs` fell from 1,986 to 1,080 lines with no compatibility wrapper or public API change.
- [x] (2026-07-15) Drained DDL behavior from the executor coordinator into the physical DDL/catalog owners: `ddl.rs` now owns validation, implicit-commit ordering, and ALTER dispatch; `ddl/create_table.rs`, `ddl/index.rs`, `ddl/table.rs`, and `sequence.rs` own their corresponding `pkg/ddl` source families. `database.rs` fell from 1,083 to 261 lines, the complete DROP/TRUNCATE/standalone-RENAME table tests moved to `tests/table_ddl.rs`, and the unrelated `USE` case moved to the session owner; no forwarding route or duplicated implementation remains.
- [x] (2026-07-15) Drained all remaining executor behavior from `tidb-exec/src/lib.rs`: public table-less query dispatch and the shared row-set fold now live in `setopr.rs`, synthetic-row SELECT and INTO validation in `select.rs`, table-reference capability boundaries in `table_reference.rs`, executed-Datum AST conversion in `literal.rs`, and statement outcomes in `result.rs`. Set-operation tests moved to `tests/setopr.rs`; root compatibility helpers were deleted, external `execute`/`Outcome` consumers retain direct public re-exports, and `lib.rs` fell from 989 to 739 lines.
- [x] (2026-07-15) Consolidated the currently translated `pkg/expression/builtin_math.go` behavior behind one `math_fn` dispatch and split its tests; `func.rs` fell from 832 to 416 lines. Kept LEAST/GREATEST with `builtin_compare.go` ownership instead of preserving a keyword-shaped numeric bucket.
- [x] (2026-07-15) Closed the remaining expression math ownership gap by moving CONV/CRC32 production helpers and original-test vectors out of `string_fn.rs` into `math_fn/` and `tests/math.rs`; `string_fn.rs` fell from 1,261 to 1,066 lines and `func.rs` from 416 to 414, with no alias or compatibility route left behind.
- [x] (2026-07-15) Closed the parser account/SET root boundary: all remaining account identity, authentication, password policy, CREATE/ALTER/RENAME/DROP USER/ROLE grammar now lives in `user.rs`; PASSWORD/ROLE/user-variable SET grammar now lives beside its exact `set_explain_parser.go` owner in `set.rs`; privilege, account, and SET tests have matching leaves. `tidb-parser/src/lib.rs` fell from 1,430 to 963 lines and `tests/stmt.rs` from 1,037 to 546, with no forwarding aliases. The extraction also corrected the old Rust conflation of CREATE ROLE's strict `parseRoleIdentity` and SET ROLE's account-based `parseUserAsRole` boundary.
- [x] (2026-07-15) Translated the complete `ddl_sequence_parser.go` source domain into `stmt/sequence.rs`, `parser/sequence.rs`, and mirrored tests; all 9 attributable `TestSimple`, 64 attributable `TestDDL`, and 33 `TestSequenceRestore` rows execute, while impossible nil-placement `ALTER RANGE` state is rejected before AST construction.
- [x] (2026-07-15) Translated `pkg/kv/checker.go` and its complete original test into a source-owned `tidb-txnkv` leaf, including the raw `int64` to TIPB `int32` narrowing contract and exhaustive classification of all 53 identities in the pinned TIPB revision.
- [x] (2026-07-15) Removed executor-private pre-split root re-exports; catalog, relation, session-state, and session-setting consumers now import their physical owner modules directly, while the intentionally public `Table`, `Cluster`, `Session`, result, and error API remains stable.
- [x] (2026-07-15) Corrected production-source routing so all 11 `pkg/parser/charset/**` owners route to `tidb-datatype`, the crate that owns `Datum`, ENUM/SET, and collation metadata; regenerated source/test/parser inventories from 17/64/55 validated fragments.
- [x] (2026-07-15) Aborted and fully recovered an AST-first `ddl_index_parser.go` attempt before accepting any partial result. Parser and executor returned compile-green with the previous index contract; the complete index source remains partial and must retry leaf-first before one atomic AST/parser/executor switch.
- [x] (2026-07-15) Re-established the index wave leaf-first: `tidb-parser/src/ddl/index.rs` now owns the former root CREATE-index parser plus shared index parts, secondary-index parsing, and foreign-key actions; `tidb-ast/src/ddl_index.rs` owns the complete source-shaped index vocabulary and canonical restore. The root parser and `CreateIndexPart` compatibility alias were deleted. This is structural preparation only: reduced statement payloads and executor reads still need one atomic switch before the source can leave `partial`.
- [x] (2026-07-15) Switched standalone `CREATE INDEX` from its reduced booleans to the common index model end to end: all six Go index kinds, pre/post `USING`/`TYPE` merge, repeatable options, online DDL, vector column parts, and Go's marker-only restore edge are source-backed. CREATE TABLE, ALTER TABLE, FOREIGN KEY, and executor payloads remain deliberately unswitched and partial rather than using an adapter.
- [x] (2026-07-15) Completed that atomic boundary rather than adding a compatibility layer: CREATE TABLE constraints, ALTER TABLE ADD constraints, FOREIGN KEY definitions, and executor DDL readers now consume the same typed index/FK contract. The Go-source leaf executes 160 of 173 checks across all 15 attributable anchors. The explicit remaining blockers are two column-level `REFERENCES` rows (missing column-option AST support) and eleven rows requiring a general `RestoreTiDBSpecialComment` rendering mode; `ddl_index_parser.go` remains partial until those cross-cutting features are ported.
- [x] (2026-07-15) Closed those two cross-cutting model gaps at their shared owners: `ColumnOption::Reference` reuses the full foreign-key reference payload and executor DDL rejects it before mutation until catalog support exists; `RestoreContext`/`RestoreFlags` carries TiDB special-comment formatting without changing ordinary restore. The direct index source leaves now execute all 173 attributable AST/parser checks, the added parser-table leaf runs 64 original `TestDDL` rows (50 accepted restores, 14 rejections), and named CREATE TABLE constraints route every Go index kind through the common definition. The source remains `partial` until the broader parser test/source ledger is audited; no parser test count is a catalog-execution claim.
- [x] (2026-07-15) Added creation-side `TablePartitioning` instead of reusing an ALTER action: typed HASH/KEY/RANGE/LIST/SYSTEM_TIME methods, key algorithms, range intervals, count/subpartition validation, shared definition/subdefinition/table-option ownership, and `UPDATE INDEXES` all restore through the Go-shaped AST. The seed executor rejects this payload before implicit commit. The controlled 853-row static Go selector changed from 0 to 812 exact matches; its 40 remaining parse failures are independently unported CREATE TABLE prefixes/tails, and one pre-existing `CHARSET BINARY` restore discrepancy became visible. The global oracle snapshot therefore moves +812 exact, -813 parse failures, +1 mismatch without claiming partition execution or full CREATE TABLE parity.
- [x] (2026-07-15) Opened the first physical `ddl/alter/` seam without a compatibility wrapper: Go `parseAlterAlter` index visibility now owns a typed AST payload plus parser leaf, and `parseAlterPartition` terminal re-partitioning owns its parser leaf while reusing the existing source-owned partition model. Their focused Go-source tests, 22-row/49-row selectors, explicit pre-mutation executor boundary, and checked domain records let future ALTER CHECK, DEFAULT, and named-partition work proceed without reopening either ported leaf.
- [ ] Shrink `tidb-parser/src/lib.rs`, `tidb-ast/src/lib.rs`, `tidb-exec/src/lib.rs`, and `tidb-exec/src/database.rs` to routing/contracts (completed: executor `lib.rs` is routing/public contracts and `database.rs` is statement coordination; stable parser/AST envelopes plus partition, complete account/authentication and SET source ownership, privilege, binding, ordinary SHOW inspection, typed ADMIN commands, ANALYZE TABLE, standalone FLUSH, the complete TRAFFIC/REFRESH STATS and resource-group source units; remaining: parser/AST legacy methods named below).
- [ ] Finish typed `BuildContext` and `EvalContext` propagation through real expression/executor construction (completed: sole `Datum` and typed string-length signatures; remaining: all supported expression forms and statement-scoped context).
- [ ] Finish shared-cluster versus per-session/per-transaction state separation and the real `tidb-txnkv` boundary (completed: bounded optimistic autocommit seam, error outcome/effects publication and whole-statement retry, a typed source-owned idle/active seed lifecycle, and real key/range/version primitives; remaining: shared transaction buffers, timestamp/protocol/MVCC/lock/commit services, and real TiKV integration).
- [x] (2026-07-15) Ran the merged workspace WIP gate: full workspace tests, strict all-target Clippy, formatting, ledger, parser inventory/golden/queue, plan inventory, and parser-package dependency isolation passed.
- [x] (2026-07-16) Added the first server-layer consumer as `tidb-server::Connection::dispatch` for COM_QUERY/PING/QUIT, added the source-shaped `tidb-distsql::KvRequestBuilder` pre-transport boundary, completed the executor result-field naming adapter, and closed the duplicate-COLLATE parser edge. The 520-test parser package, full workspace, strict Clippy, formatting, all ledgers, parser/plan inventories, and parser dependency isolation pass; current ledgers are 2,264/102/24 production and 16,132/301/140/12 test obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED).
- [x] (2026-07-16) Integrated the next connected response slice: `columns_from_adapted_fields` feeds the existing framed result-set API; COUNT partial state owns NULL-skipping, partial-add, and merge semantics; and `tidb-protocol::textrow` ports numeric text formatting, float exponent/precision rules, year-zero output, byte-preserving values, and explicit unsupported types. Focused tests, full workspace tests, strict Clippy, formatting, ledgers, parser/plan inventories, and parser dependency isolation pass. Current ledgers are 2,261/105/24 production and 16,128/303/142/12 test obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED).
- [x] (2026-07-16) Extended that response boundary in parallel: column metadata now preserves full schema/table identifiers while applying the source 256-byte display/original-name rule, vector metadata overrides, and default markers; text-row formatting adds MEDIUMINT and dependency-closed decimal text with explicit temporal/JSON/enum/set rejection; the executor formats typed integer/float/decimal/byte Datum values against caller metadata; and `tidb-server::Connection::dispatch_framed` returns sequence-one response frames for COM_QUERY/PING/QUIT. Focused protocol/executor/server tests pass; current ledgers are 2,260/106/24 production and 16,128/303/142/12 test obligations, with 55 production and 173 test evidence fragments.
- [x] (2026-07-16) Wave-23 opened three parallel handoff seams: `AuthSessionAttempt::begin_with_policy` composes secure-transport admission before the Unix-only `auth_socket` rule and stops at `PendingVerification`; `tidb-codec::RawJsonTemporal` preserves BinaryJSON DATE/DATETIME/TIMESTAMP type codes and packed calendar bits without SQL conversion; and `tidb-planner::typed_condition` carries child/join/outer-match mode, FullSchema width, and TRUE-only versus UNKNOWN-tracking policy without Datum evaluation or row materialization. Workspace tests, strict Clippy, ledgers, parser/plan inventories, dependency isolation, formatting, and diff checks pass. Current ledgers are 2,237/129/24/0 production and 16,084/347/142/12 test/support obligations.
- [x] (2026-07-16) Wave-24 integrated the first semantic consumers: `tidb-server::IdentityLookupRequest`/`IdentityLookupResult` preserve pre-auth MatchIdentity outcomes without wildcard or privilege behavior; `RawDuration::parts` mirrors Go `splitDuration` with sub-microsecond truncation; and `tidb-exec::evaluate_typed_condition` validates FullSchema width and returns scalar TRUE/FALSE/UNKNOWN. Workspace tests, strict Clippy, ledgers, parser/plan inventories, dependency isolation, formatting, and diff checks pass. Current ledgers are 2,235/131/24/0 production and 16,081/350/142/12 test/support obligations.
- [x] (2026-07-16) Wave-25 integrated actual identity matching, typed TIME range policy, and the first batch evaluator: `IdentityCatalog` ports source host ordering/wildcards/loopback/network masks with caller-injected reverse-DNS fallback; `truncate_overflow_mysql_time` returns clamped endpoints plus typed overflow direction; and `evaluate_typed_condition_batch` returns row-indexed TRUE/UNKNOWN masks with indexed failures. Workspace tests, strict Clippy, ledgers, parser/plan inventories, dependency isolation, formatting, and diff checks pass. Current ledgers are 2,233/133/24/0 production and 16,079/352/142/12 test/support obligations.
- [x] (2026-07-16) Wave-26 integrated `SkipWithGrant` as an explicit pre-auth bypass result, `round_duration_fsp` as source-compatible FSP normalization/rounding, and `transition_outer_row_status` as a pure per-batch TRUE/FALSE/UNKNOWN status transition. Cumulative join status, selection/chunk lifecycle, warning/session policy, and physical joins remain open. Workspace tests, strict Clippy, ledgers, parser/plan inventories, dependency isolation, formatting, and diff checks pass. Current ledgers are 2,232/134/24/0 production and 16,076/355/142/12 test/support obligations.

## Wave-27 progress (2026-07-16)

- [x] Integrated exact `PrivilegeRowAdmission` after identity matching, a
  bounded `tidb-datatype::parse_duration` grammar for signed/day-prefixed
  `HH:MM[:SS]` with fraction carry, FSP normalization, and TIME endpoint
  clamping, plus cumulative `merge_outer_row_status` precedence across
  predicate batches. Compact/date-duration fallback, warning/session policy,
  password/TLS/user-store verification, chunk/row lifecycle, and physical
  joins remain open. Static ledger, parser/plan/inventory, dependency,
  formatting, and diff checks pass; current ledgers are 2,230/136/24/0
  production and 16,073/358/142/12 test/support obligations
  (UNTRIAGED/PARTIAL/COVERED/BLOCKED).

## Wave-28 progress (2026-07-16)

- [x] Extended the auth handoff with opaque `AuthPluginHandoff` metadata from
  exact privilege rows and the `SkipWithGrant` native-plugin default; added
  compact `HHMMSS` duration parsing including short/leading-zero forms; and
  added `select_outer_row_statuses` for source-order TRUE indexes aligned with
  statuses while retaining the full FALSE/UNKNOWN slice. Plugin/password/TLS
  verification, date/datetime fallback, warning/session policy, row/chunk
  copying, and physical joins remain open. Static ledger, parser/plan,
  dependency, formatting, and diff checks pass; current ledgers are
  2,229/137/24/0 production and 16,071/360/142/12 test/support obligations
  (UNTRIAGED/PARTIAL/COVERED/BLOCKED).

## Wave-29 progress (2026-07-16)

- [x] Added metadata-only `AuthPluginRegistry::admit` classification for
  built-in, validated custom, and unsupported plugin names; typed
  `DurationDateTimeFallbackKind` routing for Go compact-12/compact-14 and
  separated date/time shapes; and `finalize_outer_row_statuses` events for
  source-order `Unmatched`/`HasNull` rows with default-inner/UNKNOWN signals.
  Callback execution, password/TLS/session authentication, calendar
  conversion, warning policy, row lookup, null-extension materialization, and
  physical joins remain open. Static ledger, parser/plan, dependency,
  formatting, and diff checks pass; current ledgers are 2,228/138/24/0
  production and 16,070/361/142/12 test/support obligations
  (UNTRIAGED/PARTIAL/COVERED/BLOCKED).

## Wave-30 progress (2026-07-16)

- [x] Added source-shaped client-plugin selection for session-token passthrough,
  native fallback, auth-token clear-password mapping, custom/LDAP switches,
  legacy-client rejection, and explicit unsupported outcomes; added pure
  `DurationParseEvent` overflow/fallback/truncation classification; and added
  reusable `PredicateBatchBuffer` reset/replace/length validation over TRUE and
  UNKNOWN slices. Packet I/O, password/TLS/session verification, warning
  mutation, vectorized kernels, row copying, and physical joins remain open.
  Workspace tests, strict Clippy, static ledgers, parser/plan/inventory,
  dependency, formatting, and diff checks pass; current ledgers are
  2,227/139/24/0 production and 16,069/362/142/12 test/support obligations
  (UNTRIAGED/PARTIAL/COVERED/BLOCKED).

## Wave-31 progress (2026-07-16)

- [x] Integrated three source-owned bounded leaves in parallel: JWT
  compact-shape/retry/JWKS-load admission; parser `FieldType` flags,
  length/decimal defaults, DECIMAL validity, and variable-length predicates;
  and planner pseudo-cardinality equality/less/between plus signed,
  unsigned, scalar, and prefix-index range arithmetic. RSA/JWK/filesystem/
  network/claims authentication, full FieldType formatting/enum/set metadata,
  and session/statistics/catalog planner integration remain explicit gaps.
  Static evidence and formatting checks pass; current ledgers are
  2,225/141/24/0 production and 16,065/366/142/12 test/support obligations
  (UNTRIAGED/PARTIAL/COVERED/BLOCKED).

## Wave-32 progress (2026-07-16)

- [x] Integrated three bounded source lanes: `tidb-codec::RowLayout` for
  new-row headers/metadata/lookup/value ranges/checksum framing; pure ErrCtx
  group levels, flags, statement defaults, and typed dispositions; and DDL
  affinity-level normalization, stable group IDs, duplicate collapse,
  missing-partition validation, and pre-commit level rejection. Typed row
  encoding/decoding, schema/handles/checksum calculation, warning sinks/session
  wiring, TiKV/PD/catalog mutation, and DDL coordination remain explicit gaps.
  Static ledgers and domain ownership checks pass; current ledgers are
  2,221/145/24/0 production and 16,057/374/142/12 test/support obligations
  (UNTRIAGED/PARTIAL/COVERED/BLOCKED).

## Wave-33 progress (2026-07-16)

- [x] Integrated the source-owned rowcodec encoder metadata seam: opaque
  payload framing, sorted non-null/null partitions, small/large IDs and
  offsets, compact integer widths, and append-buffer behavior.
- [x] Integrated the session bootstrap decision seam: version/mode selection,
  SYSTEM-keyspace admission, feature gates, and the source phase ordering,
  including the early finish phase inside bootstrap/upgrade execution.
- [x] Integrated planner row-count arithmetic over normalized ranges and
  caller-owned estimates, including interval boundary/null handling and
  partial-index selectivity.

All three leaves retain explicit external owners for typed Datum/schema and
decoder behavior, KV/domain/DDL effects, histogram/TopN/statistics context,
and deployable server execution. Re-run the ledgers after the final agent
edits; do not interpret these bounded leaves as Rust test-suite parity. The
final Wave-33 static snapshot is 2,218/148/24/0 production and
16,049/382/142/12 test/support obligations.

## Wave-34 progress (2026-07-16)

- [x] Added transaction scope metadata, warning publication metadata, and DDL
  partition validation/staging metadata in parallel. Each leaf is source-owned
  and dependency-closed; configuration/PD/session sinks, mutable warning
  handlers, expressions/catalog/KV/DDL workers remain explicit owners.
- [x] Regenerated ownership ledgers and passed parser/plan/domain static gates.
  The current snapshot is 2,215/151/24/0 production and 16,045/386/142/12
  test/support obligations; workspace tests and strict Clippy are the final
  batch gate for this wave.

## Wave-35 progress (2026-07-16)

- [x] Integrated rowcodec decoder metadata, session isolation/one-shot state,
  and TiKV/unistore MVCC metadata as separate source-owned lanes.
- [x] Corrected two focused test oracles during the consolidated gate: the
  extra transaction-status key preserves the source first-byte transform, and
  truncated row data must declare a non-zero end offset. Workspace tests and
  strict Clippy pass after those corrections.
- [x] Current static snapshot: 2,208/158/24/0 production and
  16,040/391/142/12 test/support obligations. Typed storage/session/catalog
  owners remain explicit future queues.

## Wave-36 progress (2026-07-16)

- [x] Added `tidb-stats::cmsketch` as the first statistics crate, with
  source-owned CMSketch/TopN byte geometry and direct tests. Datum encoding,
  analyze/sample selection, histograms, persistence, and handle/session
  integration remain separate queues.
- [x] Added session non-transactional admission and planner range-detachment
  leaves in parallel. Both accept caller-owned facts/decisions and preserve
  source order; AST/expression/session/catalog/ranger execution remains outside
  their boundaries.
- [x] Regenerated all ledgers and passed formatting, workspace tests, strict
  Clippy, parser/plan/domain static checks, and diff checks. Current snapshot:
  2,205/161/24/0 production and 16,032/399/142/12 test/support obligations.

## Wave-37 progress (2026-07-16)

- [x] Added raw-hash `tidb-stats::fmsketch`, session `txn_read_ts`, and
  planner `selectivity_greedy` as three independent source-owned leaves.
  Each keeps Datum/oracle/session/catalog/statistics extraction outside the
  bounded API and has direct source/test evidence.
- [x] Corrected source-backed test oracles during the consolidated gate: Go's
  `compareType` orders Column < Index < PrimaryKey, while the greedy candidate
  tie-break can still prefer a fewer-column primary key; FM merge at mask 1
  yields one retained even hash and NDV 2. Workspace tests and strict Clippy
  pass after these corrections.
- [x] Current static snapshot: 2,203/163/24/0 production and
  16,028/403/142/12 test/support obligations. Parser/plan/domain and diff
  checks also pass.

## Surprises & Discoveries

- Observation: the physical workspace is flat even though the design specifies `crates/` and `difftests/`.
  Evidence: `rust/Cargo.toml` lists `tidb-*` and `difftest` as root-level members.

- Observation: the generic `tidb-expr/src/builtin_ext/` seam is not a sufficient
  source-ownership boundary when one Go family is split between that directory
  and root helpers.
  Evidence: time behavior was split across `builtin_ext/time2.rs`, `date_fn.rs`,
  and 202 lines of `func.rs`; it now lives under one `time_fn/` directory with
  a single dispatch call from `func.rs`.

- Observation: parallelism is primarily blocked by shared roots, not by leaf modules.
  Evidence: the current root cuts reduced `tidb-parser/src/lib.rs` to 963 lines,
  `tidb-ast/src/lib.rs` to 223, and `tidb-exec/src/lib.rs` to 739, while the
  sequence and runtime extractions reduced generic parser `ddl.rs` to 2,255
  lines, while the DDL/catalog drain reduced executor `database.rs` from 1,083
  to 261 lines. The DDL coordinator is 512 lines and its independently owned
  create/index/table leaves are 398/130/110 lines.
  The remaining large root methods group naturally by Go-owned parser/AST
  grammar domains; executor query behavior no longer shares the crate root.

- Observation: a Rust `str` collation boundary would discard source behavior even though the scalar representation is byte-preserving.
  Evidence: `pkg/util/collate/collate_test.go::TestCampareInvalidUTF8Rune` requires compare to stop equal and keys to retain the valid prefix at invalid UTF-8. The accepted collation API therefore consumes byte slices and executes all 14 general-CI/UCA-4.0 assertions.

- Observation: generated ownership can contradict physical ownership even when every evidence fragment is internally valid.
  Evidence: the source router still sent 11 `pkg/parser/charset/**` files to `tidb-parser`; routing them to the sole datatype/collation authority moved 2,837 source lines without changing the Go surface.

- Observation: a physical move preserves the static Go oracle exactly when every root-relative evidence path moves with the corpus.
  Evidence: after the move, `go_test_ledger`, parser inventory, golden, and queue checks all passed with 45,703 matched, 5,034 parse failures, and 815 restore mismatches.

- Observation: test target isolation needs physical Cargo packages, not only
  Rust modules.
  Evidence: `cargo tree -p difftest-parser-tests` contains lexer, AST, parser,
  and the shared `difftest` library but no `tidb-expr` or `tidb-exec`; parser
  selectors compiled while executor work was temporarily broken.

- Observation: the current executor seed cannot honestly reuse Go `LazyTxn`'s invalid/pending/valid names because it has neither a pending timestamp future nor a valid `kv.Transaction`; it has only idle or an active rollback catalog image.
  Evidence: `pkg/session/txn.go` defines the three Go states, while `rust/crates/tidb-exec/src/transaction.rs` now defines the exhaustive seed boundary as `TransactionPhase::{Idle, Active}`. In the same extraction, `tidb-exec/src/lib.rs` fell from 1,481 to 1,296 lines and `database.rs` from 2,081 to 1,980 lines.

- Observation: the first transaction crate can be real without pretending the
  transport exists.
  Evidence: `tidb-txnkv` ports the byte-ordered `Key`, half-open `KeyRange`, and
  ordered `Version` contracts with their original source tests, while exposing
  no RPC, MVCC, lock, or commit API.

- Observation: exact test accounting still left production ownership
  invisible outside the parser-specific translation manifest.
  Evidence: the first whole-repository source snapshot contains 2,390 non-test
  Go files and 956,318 lines; 661 files remain visibly unassigned instead of
  being guessed into a crate.

## Decision Log

- Decision: move the existing crates mechanically before creating any new design crates.
  Rationale: a `tidb-datatype`, catalog, planner, or protocol stub would create false ownership and no verified behavior. The design requires a real consumer before extraction.
  Date/Author: 2026-07-14 / Codex.

- Decision: own the complete `set_explain_parser.go` translation in `set.rs`, with typed internal routes rather than separate keyword buckets.
  Rationale: binding commands have a distinct Go source owner, but password, roles, user variables, and ordinary SET all share `set_explain_parser.go`. Keeping their AST variants distinct while consolidating their source-owned parser methods removes root contention without erasing semantic boundaries.
  Date/Author: 2026-07-15 / Codex.

- Decision: extract typed `ADMIN` commands alone rather than an ADMIN/ANALYZE/FLUSH keyword bucket.
  Rationale: `ADMIN` has the dedicated Go owner `admin_stmt_parser.go`; `ANALYZE TABLE` belongs to `ddl_drop_parser.go`, and standalone `FLUSH` belongs to `misc_stmt_parser.go`. Separate leaves preserve source ownership and avoid unrelated parser/executor contracts converging on one growth point.
  Date/Author: 2026-07-15 / Codex.

- Decision: keep TRAFFIC and REFRESH STATS in one Rust source leaf.
  Rationale: both statement families and the shared scoped-statistics-object parser are implemented by the bounded Go owner `traffic_parser.go`. Splitting by leading keyword would duplicate ownership, hide the FLUSH STATS_DELTA dependency, and make source-completeness review harder.
  Date/Author: 2026-07-15 / Codex.

- Decision: keep CREATE, ALTER, and DROP RESOURCE GROUP plus every option parser in one source-owned leaf.
  Rationale: the bounded Go owner implements all three leaders and their shared option-list, runaway, background, and duplicate contracts. Splitting by leading keyword would recreate a shared option dependency and hide source completeness; pretending to execute them would invent a cluster scheduler the seed does not own.
  Date/Author: 2026-07-15 / Codex.

- Decision: defer all physical moves until the current parser feature wave is quiescent.
  Rationale: moving `tidb-parser` while an agent owns `src/ddl.rs` creates a path race and reduces, rather than improves, parallel throughput.
  Date/Author: 2026-07-14 / Codex.

- Decision: make statement-domain routing the stable concurrency boundary.
  Rationale: one giant `Stmt` enum and three top-level dispatch functions force every parser feature through the same files. Five fixed envelopes make feature changes local and compiler-exhaustive.
  Date/Author: 2026-07-14 / Codex.

- Decision: use `difftests/` as the physical verification directory while preserving the Cargo package name `difftest`.
  Rationale: it groups all four planned differential rings without breaking `cargo -p difftest` commands or dependency names.
  Date/Author: 2026-07-14 / Codex.

- Decision: migrate Session before the other statement domains.
  Rationale: it has twelve variants and no recursive `Stmt` payload, so it proves the stable parser/executor domain seam with the smallest behavior-preserving blast radius.
  Date/Author: 2026-07-14 / Codex.

- Decision: make one bounded Go source domain, its original tests, and its
  differential selector the unit of parallel work.
  Rationale: splitting agents horizontally into AST, parser, executor, and
  test queues makes every feature cross four owners. A vertical slice has one
  semantic owner and leaves only narrow stewarded router edits.
  Date/Author: 2026-07-15 / Codex.

- Decision: build a source leaf before opening a shared AST/parser/executor seam.
  Rationale: the first index attempt changed the AST while `index.rs` did not yet exist, blocking every Cargo lane. Future cross-layer source waves build unused leaves and tests first, then switch every constructor and consumer in one short stewarded integration; compatibility aliases are not an acceptable steady state.
  Date/Author: 2026-07-15 / Codex.

- Decision: keep evidence infrastructure, parser tests, and result tests in
  separate Cargo packages.
  Rationale: Rust modules do not isolate dependencies or Cargo scheduling.
  Physical packages let parser work compile independently from expression and
  executor work without duplicating corpora or generators.
  Date/Author: 2026-07-15 / Codex.

- Decision: extract the current transaction lifecycle as a typed seed state before introducing the real storage protocol.
  Rationale: `TransactionPhase::Active` owns its rollback image and savepoints, eliminating impossible idle-with-savepoints combinations, while autocommit and isolation remain nontransactional session settings. Naming the in-memory image as a TiKV client or fabricating pending/valid states would create false parity.
  Date/Author: 2026-07-15 / Codex.

- Decision: introduce `tidb-txnkv` only when the first complete source-backed
  production API and separate evidence consumer exist.
  Rationale: key/range/version behavior is a real reusable storage contract;
  empty client, MVCC, or protocol modules would be organizational fiction and
  would weaken the porting ledger.
  Date/Author: 2026-07-15 / Codex.

- Decision: introduce `tidb-proto` only for a generated wire leaf with a real
  consumer, starting with `tipb.ResourceGroupTag` used by
  `tidb-txnkv::ResourceGroupTagBuilder`.
  Rationale: the design's stable protocol boundary is useful immediately, but
  a broad empty `tipb`/`kvproto` crate would hide missing consumers and create
  false parity. The checked-in proto input and `prost-build` generation keep
  field numbers and nullable=false wire presence exact without hand-rolled
  bytes; request envelopes, API-V2 keys, and the remaining protocols stay
  explicit queue work.
  Date/Author: 2026-07-16 / Codex.

- Decision: keep the physical behavior-crate layout flat and add an exact
  production source-to-crate queue instead of nesting crates by phase.
  Rationale: Cargo packages already provide the build and ownership boundary;
  extra directory levels add path churn but no compilation isolation. The
  real missing concurrency boundary was exact production ownership paired with
  original test obligations.
  Date/Author: 2026-07-15 / Codex.

- Decision: source-file ownership wins over superficially similar runtime
  types when extracting expression families.
  Rationale: LEAST/GREATEST return numeric values but their authoritative
  implementation is `builtin_compare.go`; placing them in `math_fn` would make
  a future direct source transit cross two owners and recreate coordination
  inside the very seam intended to remove it.
  Date/Author: 2026-07-15 / Codex.

- Decision: keep DDL capability preflight, implicit commit, catalog build, and
  publication as distinct phases when extracting physical source owners.
  Rationale: Rust can make publication infallible with a prepared table value,
  but moving name/index resolution ahead of implicit commit would change the
  observable transaction state of an error. The typed preflight value removes
  duplicate validation without changing that Go execution order.
  Date/Author: 2026-07-15 / Codex.

- Decision: make ordinary SHOW inspection one vertical owner, while routing
  SHOW GRANTS, SHOW CREATE USER, SQL bindings, and ADMIN SHOW to privilege,
  account, binding, and admin owners respectively.
  Rationale: a keyword-shaped SHOW bucket would recreate special cases and
  cross-domain contention; source payload ownership makes those cases normal
  domain dispatch and leaves one narrow generic SHOW router.
  Date/Author: 2026-07-15 / Codex.

## Outcomes & Retrospective

The follow-on plan for the remaining table-DDL hotspot is
`2026-07-15-go-symbol-verticals.md`. It replaces broad whole-file ownership
only where a single upstream source is large enough to serialize independent
semantic ports; the new boundary is an explicitly checked, non-overlapping Go
symbol family paired with physically separate Rust leaves.

The physical move, workstream protocol, typed domain envelopes, sole `Datum`
authority, differential-package isolation, byte-lossless oracle transport,
bounded shared-cluster outcome/effects contract, and source-owned seed
transaction state are complete. The first `tidb-txnkv` key/range/version
contract is also real and independently tested, but it is not yet a TiKV
client. Session, Admin,
Query, DML, and DDL are fixed routing envelopes, and partition/account/privilege/binding/SHOW
feature slices prove that a vertical Go source domain can be ported without
giving an agent ownership of a whole crate. The parser package now compiles
without expression or executor dependencies. The remaining structural work is
to move the still-large legacy methods out of root routing files, propagate
schema and statement context through expression construction, and introduce
real shared transaction buffers and the remaining protocol boundaries. The
extracted rollback catalog image is explicitly not a `tidb-txnkv` client. The
`tidb-proto` exception is a generated contract with a real txnkv consumer, not
a semantic protocol stub. Live parser and ledger counters belong only in
`HANDOFF.md`; no behavior crate is created without an API consumer.

## Context and Orientation

The repository root is `/Users/qiliu/projects/tidb`. The Go implementation under `pkg/` is the source of truth. `rust/` is an uncommitted Cargo workspace that ports the SQL layer without cgo. Its static parser oracle is generated from all TiDB integration fixture SQL and is deliberately independent of a Go subprocess during normal Rust tests.

The relevant current pieces are:

- `rust/Cargo.toml` declares the workspace members.
- `rust/crates/tidb-ast/src/lib.rs` routes fixed statement-domain envelopes and still owns legacy shared payloads that must move down.
- `rust/crates/tidb-parser/src/lib.rs` owns `Parser`, cursor primitives, top-level statement routing, and several legacy domain grammars that must move to leaf modules.
- `rust/crates/tidb-exec/src/transaction.rs` owns the current single-session idle/active lifecycle, rollback catalog image, savepoints, autocommit, and isolation settings. `rust/crates/tidb-exec/src/database.rs` is now the top-level statement coordinator; `rust/crates/tidb-exec/src/ddl.rs` and its leaves own DDL/catalog execution; `rust/crates/tidb-exec/src/lib.rs` is the module/public-contract root; and query, set-operation, result, literal-conversion, and table-reference behavior live in their physical leaves.
- `rust/difftests/` owns checked Go goldens, Go-test inventory, and queue generation; `parser-tests/` and `result-tests/` are isolated Cargo packages that consume the shared evidence library.
- `rust/HANDOFF.md` is the entrypoint for the next contributor; `rust/PARALLEL.md`
  is the short ownership index and the workstream READMEs hold the live rules.

A _domain envelope_ is an enum that groups a stable statement class. The five envelopes are `QueryStmt`, `DmlStmt`, `DdlStmt`, `SessionStmt`, and `AdminStmt`. They make routing a fixed, small interface while individual statements live in independently owned modules.

## Plan of Work

First, pause only agents touching core Rust source roots. Finish or discard their in-flight feature changes and run the static parser queue once to record the known-good snapshot. Do not move directories during this period.

Second, the completed atomic mechanical move created `rust/crates/`, moved the five crate directories into it, and moved `rust/difftest/` to `rust/difftests/`. `rust/Cargo.toml` now has explicit `crates/tidb-*` and `difftests` members while Cargo package names stay unchanged. Relative dependency paths, root-relative evidence constants, Go helper commands, Bazel import paths, and actionable documentation now use the new paths.

Third, create `rust/docs/architecture/` for the stable crate/dependency map and `rust/docs/operations/` for validation commands. Create `rust/workstreams/{parser,result,plan,transaction}/README.md`. Each workstream file names its oracle, ledger/queue input, ownership rules, and the only files a feature agent may touch. Replace `PARALLEL.md` with a short index that links to these living workstream contracts; delete its obsolete builtin-ext wave tables.

Fourth, split the core roots without changing parsed or executed behavior. In `tidb-ast`, move session and admin statement payloads to `src/stmt/session.rs` and `src/stmt/admin.rs`; make `lib.rs` declarations and re-exports only. In `tidb-parser`, make `core.rs` own parser state, cursor, token helpers, and errors; make `statement.rs` route only to fixed domain parsers; place statement grammar in `stmt/{session,show,account,transaction,admin,binding}.rs`; split DDL router entrypoints into `ddl/create.rs` and `ddl/drop.rs` while retaining table/index syntax modules. In `tidb-exec`, transaction state and transitions now live in `transaction.rs`; continue moving the remaining database fields to source-owned state modules, route statements in `dispatch.rs`, and keep execution behavior inside domain modules. Mirror the production split under each crate's test tree.

Fifth, replace the flat statement growth point with the five domain envelopes. The AST owns each domain enum and its restore. The parser top-level recognizes only the five domains; the executor top-level dispatches only the five domains. Use compiler errors to migrate every consumer. Temporary `From` conversions are allowed only during this one migration and must be deleted before the milestone closes. A statement feature thereafter changes its domain module, matching tests, and its own differential corpus; it must not modify a root routing file.

Sixth, shrink the remaining roots by source domain, without changing behavior.
`tidb-parser/src/lib.rs` keeps `Parser`, token cursor/error primitives, and a
small first-keyword router. Privilege GRANT/REVOKE grammar now lives in
`privilege.rs`, bindings now live in `binding.rs`, and ordinary metadata
inspection now lives in `show.rs`; account/authentication grammar now lives in
`user.rs`, while PASSWORD/ROLE/user-variable and ordinary SET grammar share the
bounded `set_explain_parser.go` owner in `set.rs`. Typed
ADMIN grammar now lives in `admin.rs`, ANALYZE TABLE lives in `analyze.rs`, standalone
FLUSH lives in `flush.rs`, the complete TRAFFIC/REFRESH STATS source unit lives in
`traffic.rs`, the complete resource-group DDL source unit lives in
`resource_group.rs`, and ordinary system-variable and
session-setting SET grammar lives in `set.rs`. Move mirrored tests from the large `tests/stmt.rs` and
`tests/ddl.rs` files at the same boundary. `tidb-ast/src/lib.rs` becomes
declarations, shared traits, and re-exports. `tidb-exec/src/transaction.rs` now
owns the seed lifecycle and mirrored transaction tests; `tidb-exec/src/lib.rs`
keeps public contracts, while `database.rs` is a coordinator over separately
owned catalog, session, transaction, DDL, and DML state/services. Each extraction is
mechanical first and must preserve its focused selector plus the global static
oracle before any new Go behavior is added.

Seventh, keep `tidb-datatype::Datum` as the only scalar authority. Route the
normal AST-to-expression build through explicit `FieldType` and one
statement-scoped context; never infer binary versus character semantics from a
runtime datum. In parallel, publish shared cluster effects through versioned
state even when Go returns an error after a partial DDL effect. These are
contract fixes, not reasons to add compatibility paths.

## Concrete Steps

All commands run from the repository root unless a command starts with `cd rust`.

1. Quiesce and capture the baseline:

       cd rust
       cargo run -j 12 -p difftest --bin integration_parser_queue -- --check
       cargo run -j 12 -p difftest --bin go_source_ledger -- --check
       cargo run -j 12 -p difftest --bin go_test_ledger -- --check

   Record the six queue summary counters in `rust/HANDOFF.md`. Do not alter them for this structural move.

2. Make the physical move only after no agent owns a moved path. Update the Cargo member list and every real path use returned by:

       rg -n 'rust/(tidb-|difftest)|difftest/(corpus|godump|goeval|gorun)' rust --glob '!target/**'

   Update `difftests/src/bin/{go_test_ledger,integration_parser_inventory,integration_parser_golden,integration_parser_queue}.rs`, Go helpers, their BUILD files, and scripts in the same atomic increment.

3. Validate the path migration:

       cd rust
       cargo metadata --no-deps --format-version 1
       cargo test -j 12 --workspace -q
       cargo clippy -j 12 --workspace --all-targets -- -D warnings
       cargo fmt --all -- --check
       cargo run -j 12 -p difftest --bin go_test_ledger -- --check
       cargo run -j 12 -p difftest --bin integration_parser_inventory -- --check
       cargo run -j 12 -p difftest --bin integration_parser_golden -- --check
       cargo run -j 12 -p difftest --bin integration_parser_queue -- --check

4. Create the workstream contracts and update `HANDOFF.md`, the replacement `PARALLEL.md`, and operational docs with the new paths. Re-run the commands in step 3; documentation must be copy-pasteable from the repository root.

5. Continue the root split one source-domain extraction at a time. Run the
   domain's exact selector and crate tests after each extraction, then the full
   command set in step 3. A changed static parser counter is a correctness
   regression unless a separately reviewed source feature is included and its
   entire outcome delta is explained.

6. Prove package isolation after evidence changes:

       cd rust
       cargo tree -p difftest-parser-tests | rg 'tidb-(expr|exec)'

   The command must print nothing and exit with status 1 because neither heavy
   package is present.

Update note (2026-07-16): integrated the next three isolated leaves. The
protocol result-encoder leaf ports Go's registered result-charset precedence
and byte-preserving Binary/ASCII/Latin1/UTF-8 policy with explicit unknown
state; the executor result-field resolver handles table-less names, aliases,
qualified fields with authoritative hints, and dependency-closed literal/
operator/function metadata while rejecting schema-dependent shapes; and
DistSQL adds an immutable post-build `TransportRequest` with explicit
unbound/bound ownership errors. Focused protocol, DistSQL, and resolver tests
pass. Regenerated counts are 2,259/107/24 production and
16,128/303/142/12 test obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED), with
56 production and 173 test evidence fragments. GBK/full session conversion,
catalog-backed ResultField binding, temporal/JSON/enum/set/vector formatting,
protobuf/RPC, TiKV, and deployable server lifecycle remain open.

## Validation and Acceptance

The migration is accepted only if the following are all true:

- `cargo metadata` lists fourteen behavior crates (including `tidb-stats` and
  `tidb-server`)
  plus `difftest`,
  `difftest-parser-tests`, `difftest-result-tests`, and
  `difftest-transaction-tests` at their declared paths.
- No actionable Rust document, script, source constant, BUILD file, or Cargo dependency refers to a deleted root-level `rust/tidb-*` or `rust/difftest` path.
- The workspace passes tests, strict Clippy, and formatting with 12 parallel jobs.
- `go_source_ledger` accounts for every non-test Go file and rejects stale
  source paths, missing evidence artifacts, and stale generated snapshots.
- `go_test_ledger`, parser inventory, static golden, and queue checks pass. The
  ledger includes Go entry points, lifecycle hooks, shell programs, SQL
  fixture/result families, `testdata`, and executable/config/data support
  artifacts below repository test suites. The parser queue summary exactly
  equals the reviewed snapshot unless a source feature deliberately changes it.
- `rust/PARALLEL.md` names current modules only and links to workstream contracts that specify owner, source-of-truth, corpus, and validation.
- A representative statement feature can be assigned as one vertical source
  domain with leaf AST/parser/executor modules, mirrored tests, and one
  selector. Only a narrow steward edit may touch a root router.
- `cargo tree -p difftest-parser-tests` contains neither `tidb-expr` nor
  `tidb-exec`.

## Idempotence and Recovery

The cargo and differential checks are safe to rerun. Do not start a physical move if any agent owns an affected path; wait for it to finish first. If a move is interrupted, restore the original directory names as a single filesystem operation, restore the prior `Cargo.toml` member list, and rerun `cargo metadata` before attempting a smaller atomic move. Never leave duplicate crate trees or compatibility symlinks: they hide ownership and cause agents to edit different copies.

If the domain-envelope migration exposes a genuine behavior change, stop that structural sub-milestone, add a regression using the current source-derived corpus, restore the previous routing shape if necessary, and resolve the semantic defect separately. Do not normalize the snapshot downward to make a refactor pass.

## Artifacts and Notes

The current parser ring covers 51,598 SQL inputs dispatched by the pinned upstream runners. At plan creation it reports 45,647 Rust exact matches, 5,037 parse failures, 868 restore mismatches, 45 Rust-accepted Go rejections, zero multi-statement asymmetries, and one Go restore failure. These counters are evidence, not a parity claim.

The current ledger reports 15,705 exact obligations: 1,901 Go test files,
11,168 test/benchmark/fuzz/example entry points, 26 lifecycle hooks, 5 exact
fixture references, 570 Bazel targets, 53 Make test/lifecycle targets, 262
shell programs, 263 integration inputs, 267 result files, 188 `testdata`
artifacts, and 1,002 other executable/config/data test-suite artifacts. Status
is 15,508 untriaged, 144 partial, 46 covered, and 7 explicitly
dependency-blocked. Of those, 1,963 route to the plan ring, 2,739 are deferred
external-tool obligations, and 4,693 remain unassigned. The layout refactor
must preserve this visibility and must not label any obligation covered merely
because its source path moved.

## Interfaces and Dependencies

At milestone completion the AST exposes this fixed routing boundary:

    pub enum Stmt {
        Query(QueryStmt),
        Dml(DmlStmt),
        Ddl(DdlStmt),
        Session(SessionStmt),
        Admin(AdminStmt),
    }

Each domain enum implements its own SQL restore and has no dependency on parser, executor, or differential crates. Dependency direction remains `tidb-lexer -> tidb-ast -> tidb-parser`; `tidb-expr` depends on AST and the later datatype crate; `tidb-exec` depends on AST and expression/domain services; `difftest` depends on all behavior it validates. No back-edge from AST or parser into execution is permitted.

Update note (2026-07-14): the physical layout and workstream protocol milestones completed after Wave 24 was verified and quiescent. The remaining root-seam split must stay behavior-preserving and retain the static parser snapshot.

Update note (2026-07-15): recorded the sole-`Datum` migration, the three-way
differential Cargo split, the vertical source-domain ownership decision, and
the still-incomplete parser/executor root extraction. This corrects the former
plan state, which described the root split as both complete and remaining.

Update note (2026-07-15): recorded the ordinary SET vertical extraction and
its semantic exclusions. The global parser oracle remained at 49,217 matched
inputs and 2,327 reviewed non-matches.

Update note (2026-07-15): recorded the typed ADMIN vertical extraction and
the decision to keep ANALYZE and standalone FLUSH separate by Go source owner.

Update note (2026-07-15): recorded the ANALYZE TABLE vertical extraction and
its 289-row static Go selector. The broader Go statistics payload remains an
explicit porting boundary rather than being discarded during parsing.

Update note (2026-07-15): recorded the standalone FLUSH vertical extraction,
its consolidated 18-row selector, and the neutral identifier-like path helper
shared with ADMIN SHOW NEXT_ROW_ID.

Update note (2026-07-15): recorded the complete `traffic_parser.go` vertical
translation, including REFRESH STATS and stats-object dedup rather than only
the TRAFFIC keyword branch. The seed executor remains explicitly Unsupported.

Update note (2026-07-16): converged three disjoint parser source waves without
shared-file overlap. The expression restore wave added unary, column-name, and
IS NULL vectors plus the three-component name-path rejection boundary; the DML
restore wave added LIMIT, wildcard, select-field, and field-list vectors; and
the function wave added function-call, cast, aggregate, and CONVERT vectors.
The DML wave then extended the same owner with table-name, table-source,
index-hint, ON-condition, and join restore vectors, without taking another
agent's source anchors.
The parser ring remains green at 51,488 exact single-statement restores, 10
complete multi-statement restores, and one actionable nonmatch. The Rust parser
crate now passes all 517 unit tests, and the test ledger moved to 16,174
untriaged, 275 partial, 125 covered, and 11 explicitly blocked obligations.

Update note (2026-07-16): fixed the source-ledger planner route to the actual
`tidb-planner` crate, then converged three disjoint source-owned lanes. Parser
DDL-index evidence covers five exact AST anchors, including the added
table-to-table rename test; planner NDV skew-ratio and Issue54812 anchors are
recorded as `PARTIAL` until SessionVars/testkit/statistics/EXPLAIN integration;
and expression ROW/IN plus user-variable casts are bounded `PARTIAL` evidence
with VALUES still partial and plan-cache parameters blocked. The generated
test ledger is now 16,162 untriaged, 281 partial, 130 covered, and 12 blocked.
Focused lanes plus workspace tests, strict Clippy, and formatting pass. The
next milestone remains the connected read-only statement path.

Update note (2026-07-16): integrated three additional source-owned seams in
parallel. `tidb-protocol` now ports the uncompressed MySQL packet header,
continuation, sequence, flush, and packet-limit contract from
`pkg/server/internal/packetio.go`; `tidb-distsql` ports the detach-safe request
metadata, warning, cancellation, kill, and KV-variable state from
`pkg/distsql/context`; and `tidb-exec::Session::execute_framed_query` is the
first real consumer of both leaves. The connected local path now proves
framed `COM_QUERY` → SQL parser → shared-session executor → DistSQL original
SQL metadata, with malformed/non-query packets rejected before catalog
mutation. This remains a local uncompressed seam, not a deployable server:
authentication, compression, result encoding, planner integration, TiKV RPC,
and schema/session services remain open. Focused tests, all workspace tests,
strict Clippy, and formatting pass. The regenerated test ledger is now
16,157 `UNTRIAGED`, 285 `PARTIAL`, 131 `COVERED`, and 12 `BLOCKED`.

Update note (2026-07-16): integrated the next response-side batch. The
protocol leaf now owns length-encoded integers, text-row framing, and
source-shaped column metadata; the DistSQL leaf projects context into a typed
read-request builder; and planner exponential-backoff evidence is split into
three exact source subtests. `tidb-exec` consumes the row encoder through a
framed `COM_QUERY -> session -> DistSqlContext -> text rows` method and now
accepts pure no-table queries in the shared-session capability envelope. The
fresh workspace gate passes tests, strict Clippy, formatting, ledger, parser,
planner, and package-isolation checks. Counts are 16,142 untriaged, 292
partial, 139 covered, and 12 blocked; 164 test evidence fragments are
registered. Full result-set metadata/status lifecycle, typed formatting,
authentication, compression, TiKV, and deployable server wiring remain open.

Update note (2026-07-16): converged the next three non-overlapping lanes and
integrated their largest consumer. `tidb-protocol` now owns Go-shaped OK/EOF
packets and logical text-result sequencing; `tidb-distsql` adds the
source-shaped `RequestEnvelope` concurrency/limit policy; and `tidb-planner`
adds a typed physical-plan metadata tree with ExplainID suffix behavior. The
executor now frames the complete metadata/row/EOF sequence when a caller
supplies `ColumnInfo`, keeping result-field derivation explicit instead of
guessing from Datum values. A stale split-test ownership gap for
`pkg/server/conn_test.go:670 TestDispatch` was repaired before regenerating
the ledger. The deliberate broad gate passes workspace tests, strict Clippy,
formatting, ledger, parser, planner, and package-isolation checks. Counts are
16,137 untriaged, 296 partial, 140 covered, and 12 blocked; 167 test evidence
fragments are registered. Executor metadata derivation, typed formatting,
dynamic warning/status state, TiKV, and deployable server wiring remain open.

Update note (2026-07-16): continued with two isolated source lanes while the
connected seams stayed single-owner. `tidb-protocol::decode_command` ports the
Go `clientConn.dispatch` command-byte split and one trailing-NUL `COM_QUERY`
rule; `tidb-exec::result_metadata` ports the dependency-closed
`ConvertColumnInfo` arithmetic without inventing executor ResultField
resolution. The parser FieldType lane adds geometry aliases and exact
differential ownership. At that wave, generated ledgers were 2,265 production
`UNTRIAGED`, 101 `PARTIAL`, 24 `COVERED`, 0 `BLOCKED`, and 16,135 test
`UNTRIAGED`, 298 `PARTIAL`, 140 `COVERED`, 12 `BLOCKED`, with 168 test and 51
production evidence fragments. Full ResultField wiring, typed formatting,
TiKV, and deployable server lifecycle remain the next integration gates.

Update note (2026-07-16): closed the next connected server gaps with three
isolated leaves. `tidb-exec::result_response` parses a strict plain table-less
`SELECT`, routes fields through the existing resolver and adapter, and emits
protocol `ColumnInfo`; relation/set/non-query/wildcard shapes fail explicitly.
`tidb-server::handshake` ports Go's initial-handshake byte layout and safe
HandshakeResponse41 header/body parsing, including attributes and capability
intersection, without implementing authentication. `tidb-server::listener`
adds a real TCP bind lifecycle with idempotent initialization, ephemeral
address reporting, active health ordering, sticky shutdown flags, and close
idempotency. `Connection::dispatch_framed_auto` connects the safe automatic
metadata path end to end while preserving caller-supplied metadata for
catalog-backed queries. Added focused source-shaped tests, refreshed ledger
manifest/evidence fragments, and passed workspace tests, strict Clippy,
formatting, and ledger checks. Current counts are 2,257/109/24/0 production
and 16,119/312/142/12 test obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED),
with 57 production and 174 test evidence fragments. Catalog-backed schema
binding, auth/TLS/compression, full session charset conversion,
temporal/JSON/enum/set/vector formatting, TiKV/RPC, accept-loop/bootstrap,
and mixed-cluster routing remain open.

Update note (2026-07-16): added the next parallel source/test wave. The
DistSQL channel iterator ports `newSelRespChannelIter`/`Read` over owned rows,
the server accept-loop leaf ports injected listener/handler ownership and
shutdown/error propagation, and the executor statement-status leaf ports
source counters, warning cap/order, last-insert-ID state, retry/full reset,
and publish row-count policy. Root reexports keep all three leaves reachable
without moving their implementation ownership. Focused tests, targeted
strict Clippy, and formatting pass. Counts are 2,255/111/24/0 production and
16,114/317/142/12 test obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED), with 59 production and 176 test evidence
fragments. Response-channel decoding, dynamic session/error-context
integration, multi-relation binding, and TiKV/RPC remain explicit next gates.

Update note (2026-07-16): the next parallel wave closed GBK result encoding,
serial DistSQL result iteration, and the first catalog-backed result metadata
path without sharing implementation files. The protocol leaf uses
`encoding_rs` only for Go-compatible GBK conversion and keeps the datatype
registry unchanged; the DistSQL leaf stops before response-channel/chunk/TiKV
ownership; the executor leaf maps stored `ColumnType` declarations to
`ColumnInfo` through the existing adapter. `dispatch_framed_auto` now covers
table-less and single-table SELECTs end to end, with a create/insert/select
regression. Focused tests and ledger checks pass. Counts are 2,256/110/24/0
production and 16,118/313/142/12 test obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED), with 58 production and 175 test evidence
fragments. Multi-relation binding, full session charset lifecycle,
response-channel decoding, and TiKV/RPC remain explicit next gates.

Update note (2026-07-16): integrated three bounded response/status protocol
leaves without editing the protected server/session roots. `tidb-distsql`
now owns ordered response result/warning/error/close events with explicit
raw-tipb/TiKV boundaries; `tidb-protocol` owns source-shaped protocol-41 and
legacy ERR payload ordering; and `tidb-exec` converts a published statement
status losslessly into OK/result options without reading Datum values. Full
workspace tests, strict Clippy, parser/inventory/ledger checks, formatting,
and diff checks pass. Counts remain 2,255/111/24/0 production and
16,114/317/142/12 test obligations, with 59 production and 176 test evidence
fragments. Raw tipb/chunk decoding, dynamic session/error-context/wire
integration, multi-relation binding, authentication/TLS/compression, TiKV/RPC,
and deployable bootstrap remain explicit next gates.

Update note (2026-07-16): integrated the next source-owned response/schema
wave in one batched validation cycle. `tidb-proto` now compiles a
descriptor-checked projection of upstream `select.proto` response messages
(`SelectResponse`, `StreamResponse`, chunks, errors, and execution summaries)
without pulling request or executor behavior into the wire crate.
`tidb-exec::result_schema_multi` binds explicit inner/CROSS/comma/LEFT relation
trees, aliases, qualified paths, wildcard declaration order, and prepared
self-join metadata from authoritative catalog snapshots; RIGHT/NATURAL/
STRAIGHT, derived tables, expressions, and table options fail explicitly.
`Session::run` now owns the statement-status begin/retry/finalize boundary and
publishes final affected-row/last-insert-ID state on success or error. The
first integrated test run exposed and corrected top-level join metadata
bypass and two incorrect protobuf high-field assertions before the final
workspace gate. Final workspace tests and strict Clippy pass. The remaining
gates are raw tipb/chunk decoding, planner-owned ON/USING/coalescing/null
extension, full warning/error-context/wire attachment, authentication/TLS/
compression, TiKV/RPC, and deployable bootstrap.

The final generated evidence snapshot for this wave is 2,255/111/24/0
production and 16,113/318/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED), with 59 production and 177 test
evidence fragments.

Update note (2026-07-16): integrated the next three disjoint leaves in one
batched workspace cycle. `tidb-distsql::chunk_decode` now validates decoded
tipb SelectResponse/Chunk envelopes and row metadata while preserving typed
default/columnar/CHBlock payloads as an explicit codec boundary;
`tidb-protocol::error_conversion` ports the source-backed error-kind to MySQL
errno/SQLSTATE mapping without guessing session context; and
`tidb-exec::result_schema_join_output` ports planner-visible INNER/CROSS/LEFT
field order, LEFT nullability declarations, and USING coalescing metadata for
already-resolved child schemas. The cycle exposed and fixed the metadata-free
opaque chunk case, a join USING type-inference compile error, and missing public
field documentation. Full workspace tests, strict Clippy, formatting, ledger,
parser/plan inventory, queue, golden, and dependency-isolation checks pass.
Current generated counts are 2,255/111/24/0 production and 16,112/319/142/12
test/support obligations, with 59 production and 177 test evidence fragments.
Typed chunk codecs, planner predicate/row execution, full error-context/wire
attachment, authentication/TLS/compression, TiKV/RPC, and deployable bootstrap
remain open.

Update note (2026-07-16): extended the next connected response seam with three
static-first leaves. `tidb-distsql::stream_decode` preserves StreamResponse
payload/metadata presence without decoding nested chunks;
`tidb-exec::error_conversion` maps rendered ExecError categories to the
protocol descriptor without guessing context; and the automatic catalog
metadata path now proves INNER/CROSS join metadata and rows while rejecting
LEFT/USING until nullable/coalesced planner output crosses the adapter. The
LEFT false-positive boundary was corrected before compilation. The next batch
must compile these leaves with typed chunk codecs, join predicate/row
execution, and session error-context/wire attachment still explicit.

Update note (2026-07-16): completed the next codec/join/wire wave in one
batched workspace cycle. `tidb-codec::value` validates Go `EncodeValue`
default-row tag boundaries with explicit column counts, raw payload preservation,
and explicit JSON/vector unsupported errors. The automatic catalog response
path now consumes recursive planner-shaped output metadata for bare `SELECT *`
INNER/CROSS/LEFT/USING joins, proving LEFT null extension and USING coalesced
field/row order; explicit projections remain outside the planner-output
contract. `tidb-server::error_response` attaches caller-rendered executor
errors to sequence-one protocol-41 or legacy ERR frames without guessing
context. Full workspace tests and strict Clippy pass. The regenerated ledgers
are 2,255/111/24/0 production and 16,110/321/142/12 test/support
obligations, with the new partial codec/server evidence recorded. Typed
columnar/CHBlock
codecs, general planner ON/USING typing and projection names, full
session/error-context lifecycle, authentication/TLS/compression, TiKV/RPC, and
deployable bootstrap remain open.

Update note (2026-07-16): integrated the next parallel codec/transport/planner
wave and closed one combined workspace gate. `tidb-codec::json` now ports the
source-defined BinaryJSON type/value boundary, including primitive, container,
opaque, and duration payload lengths with exact remainders; `RawValue::json`
now accepts the JSON value tag while malformed/unknown physical payloads stay
explicit errors. `tidb-proto` and `tidb-distsql::RegionTaskEnvelope` preserve
the exact StoreBatchTask region epoch, peer, ordered ranges, task ID, versioned
ranges, and bucket-version fields before lookup/retry/endpoint/RPC ownership.
`tidb-exec::result_schema_join_output` now retains source-ordered FullSchema
fields and maps hidden right-side USING fields to canonical visible output
indices without widening executor rows. Full workspace tests and strict
Clippy pass after fixing borrowed-JSON lifetimes and source-test cardinality;
formatting/diff plus all ledger/parser/plan/dependency gates pass. Current
ledgers are 2,248/118/24/0 production and 16,098/333/142/12 test/support
obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED). Full JSON semantics, SQL
temporal/Duration, decimal/enum/set/vector Datum and native CHBlock codecs,
typed expressions/nested FullSchema execution, general ON/USING typing, full
session/error-context and authentication/TLS/user-store lifecycle, region/RPC
execution, TiKV, and deployable bootstrap remain open.

Update note (2026-07-16): Wave-48 adds three disjoint source-backed leaves in
the same batched workspace cycle. `tidb-planner::stats_info` owns row-count
truncation and caller-owned NDV capping; `tidb-stats::HistogramCountSummary`
owns histogram count/factor arithmetic; and `tidb-exec::plan_cache_params`
owns ordered plan-cache parameter storage and its privacy bit. Their focused
source/test evidence is recorded without duplicating the shared `session.go`
or `histogram.go` ownership rows. Workspace tests and strict Clippy pass with
12 jobs; the regenerated ledgers are 2,183/183/24/0 production and
15,992/439/142/12 test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED).
The static parser/plan/dependency gate remains the next step.

Update note (2026-07-16): Wave-49 adds `tidb-planner::index_columns`,
`tidb-stats::analysis_policy`, and `tidb-exec::stats_load_result` as disjoint
source-backed leaves; the shared `stmtctx.go` source row is extended rather
than duplicated. Wave-50 adds `tidb-planner::pattern_engine` for cascades
engine flags and set membership. The combined regenerated ledgers are
2,181/185/24/0 production and 15,986/445/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED); the next consolidated workspace and
static evidence gate is pending.

Update note (2026-07-16): Wave-51 adds `tidb-planner::fix_control`,
`tidb-stats::analyze_version_matches`, and
`tidb-exec::alternative_plan_signals` as disjoint source-backed leaves. The
existing `table.go` and `stmtctx.go` ownership rows are extended rather than
duplicated. Workspace tests and strict Clippy pass with 12 jobs; the
regenerated ledgers are 2,180/186/24/0 production and 15,983/448/142/12
test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED). The next
parallel source queues are ready.

Update note (2026-07-16): Wave-52 adds `tidb-planner::memo_group_id`,
`tidb-stats::estimate_ndv_by_gee`, and `tidb-exec::read_consistency` as
disjoint source-backed leaves. The shared `session.go` ownership row is
extended rather than duplicated. Workspace tests and strict Clippy pass with
12 jobs; the regenerated ledgers are 2,178/188/24/0 production and
15,980/451/142/12 test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED).
The next parallel source queues are ready.

Update note (2026-07-16): Wave-53 adds `tidb-planner::task_scheduler`,
`tidb-stats::avg_count_per_not_null_value`, and
`tidb-exec::chunk_alloc_status` as disjoint source-backed leaves. Shared
histogram/stmtctx ownership rows are extended rather than duplicated.
Workspace tests and strict Clippy pass with 12 jobs; the regenerated ledgers
are 2,177/189/24/0 production and 15,978/453/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). The next parallel source queues are ready.

Update note (2026-07-17): Wave-54 adds `tidb-planner::hash_equaler`,
`tidb-stats::calc_correlation`, and `tidb-exec::setvar_hint_restore` as
disjoint source-backed leaves. Shared cascades, histogram, and stmtctx
ownership rows are extended rather than duplicated. Workspace tests and strict
Clippy pass with 12 jobs; the regenerated ledgers are 2,175/191/24/0
production and 15,973/458/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). The static parser/plan/dependency gate
also passes; the next parallel source queues are ready.

Update note (2026-07-17): Wave-55 adds `tidb-planner::plan_context`,
`tidb-stats::index_usage`, and `tidb-exec::cursor_tracker` as disjoint
source-backed leaves. Shared planner-context, statistics collector, and
session cursor ownership rows are extended rather than duplicated. Workspace
tests and strict Clippy pass with 12 jobs; the regenerated ledgers are
2,171/195/24/0 production and 15,965/466/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). The static parser/plan/dependency gate
also passes; the next parallel source queues are ready.

Update note (2026-07-17): Wave-56 adds `tidb-planner::task_stack`,
`tidb-stats::analyze_jobs`, and `tidb-exec::session_context_key` as disjoint
source-backed leaves. Shared task, analyze, and session-context ownership rows
are extended rather than duplicated. Workspace tests and strict Clippy pass
with 12 jobs; the regenerated ledgers are 2,168/198/24/0 production and
15,961/470/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). The static parser/plan/dependency gate
also passes; the next parallel source queues are ready.

Update note (2026-07-17): Wave-57 adds `tidb-planner::pattern`,
`tidb-stats::async_load`, and `tidb-exec::status_registry` as disjoint
source-backed leaves. Shared cascades, statistics queue, and status-registry
ownership rows are extended rather than duplicated. Workspace tests and strict
Clippy pass with 12 jobs; the regenerated ledgers are 2,165/201/24/0
production and 15,951/480/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). The static parser/plan/dependency gate
also passes; the next parallel source queues are ready.

Update note (2026-07-17): Wave-58 adds `tidb-planner::string_writer`,
`tidb-stats::datum_map_cache`, and `tidb-exec::process_info` as disjoint
source-backed leaves. Shared planner formatting, statistics cache, and session
manager ownership rows are extended rather than duplicated. Workspace tests and
strict Clippy pass with 12 jobs; the regenerated ledgers are 2,162/204/24/0
production and 15,946/485/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). The static parser/plan/dependency gate
also passes; the next parallel source queues are ready.

Update note (2026-07-17): Wave-59 adds `tidb-planner::expr_iterator`,
`tidb-stats::need_analyze_table`, and `tidb-exec::nextgen_readonly_vars` as
disjoint source-backed leaves. Shared memo iteration, auto-analyze policy, and
variable-definition ownership rows are extended rather than duplicated.
Workspace tests and strict Clippy pass with 12 jobs; the regenerated ledgers
are 2,159/207/24/0 production and 15,940/491/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). The static parser/plan/dependency gate
also passes; the next parallel source queues are ready.

Update note (2026-07-17): Wave-60 adds `tidb-planner::explore_mark`,
`tidb-stats::parse_auto_analyze_ratio`, and `tidb-exec::slow_log_threshold`
as disjoint source-backed leaves. Shared memo round-state, auto-analyze ratio,
and slow-log threshold ownership rows are extended rather than duplicated.
Workspace tests and strict Clippy pass with 12 jobs; the regenerated ledgers
are 2,156/210/24/0 production and 15,935/496/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). The static parser/plan/dependency gate
also passes; the next parallel source queues are ready.

Update note (2026-07-17): Wave-61 adds `tidb-planner::group_expr`,
`tidb-stats::AutoAnalysisTimeWindow`, and `tidb-exec::slow_log_rules` as
disjoint source-backed leaves. Shared memo expression, auto-analyze window, and
slow-log rule metadata ownership rows are extended rather than duplicated.
Workspace tests and strict Clippy pass with 12 jobs; the regenerated ledgers
are 2,153/213/24/0 production and 15,929/502/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). The static parser/plan/dependency gate
also passes; the next parallel source queues are ready.

Update note (2026-07-17): Wave-62 adds `tidb-planner::column_length`,
`tidb-stats::calculate_priority_weight`/`special_event_weight`, and
`tidb-exec::session_token_timing` as disjoint source-backed leaves. Shared
column-length, priority-calculator, and session-token timing ownership rows are
extended rather than duplicated. Workspace tests and strict Clippy pass with
12 jobs; the regenerated ledgers are 2,150/216/24/0 production and
15,925/506/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). The static parser/plan/dependency gate
also passes; the next parallel source queues are ready.

Update note (2026-07-17): Wave-63 adds `tidb-planner::plan_cache_constants`,
`tidb-stats::get_partition_sql`/`flatten_partition_names`, and
`tidb-exec::advisory_lock_state` as disjoint source-backed leaves. Shared
plan-cache, dynamic-partition, and advisory-lock state ownership rows are
extended rather than duplicated. Workspace tests and strict Clippy pass with
12 jobs; the regenerated ledgers are 2,147/219/24/0 production and
15,921/510/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). The static parser/plan/dependency gate
also passes; the next parallel source queues are ready.

Update note (2026-07-17): Wave-64 adds `tidb-planner::index_advisor_model`,
`tidb-stats::priority_heap`, and `tidb-exec::txn_running_state` as disjoint
source-backed leaves. Shared advisor-model, priority-heap, and transaction
state ownership rows are extended rather than duplicated. Workspace tests and
strict Clippy pass with 12 jobs; the regenerated ledgers are 2,144/222/24/0
production and 15,908/523/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). The static parser/plan/dependency gate
also passes; the next parallel source queues are ready.

Update note (2026-07-17): Wave-65 adds `tidb-planner::rule_type`,
`tidb-stats::analysis_interval`, and `tidb-exec::txn_summary` as disjoint
source-backed leaves. Shared rule-dispatch, analysis-interval, and transaction
summary ownership rows are extended rather than duplicated. Workspace tests,
strict Clippy, formatting, and the static parser/plan/dependency gate pass
with 12 jobs; the regenerated ledgers are 2,141/225/24/0 production and
15,902/529/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Wave 66 is the next parallel source queue.

Update note (2026-07-17): Wave-66 adds `tidb-planner::base_traits`,
`tidb-stats::auto_analyze_job`, and `tidb-exec::session_pool_capacity` as
disjoint source-backed leaves. Shared cascades hash/equality, auto-analyze
job, and system-session pool ownership rows are extended rather than
duplicated. Workspace tests, strict Clippy, formatting, and the static
parser/plan/dependency gate pass with 12 jobs; the regenerated ledgers are
2,138/228/24/0 production and 15,899/532/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Wave 67 is the next parallel source queue.

Update note (2026-07-17): Wave-67 adds `tidb-planner::scheduler_contract`,
`tidb-stats::non_partitioned_analysis`, and `tidb-exec::sysvar_scope` as
disjoint source-backed leaves. Shared scheduler, non-partitioned analysis, and
sysvar-scope ownership rows are extended rather than duplicated. Workspace
tests, strict Clippy, formatting, and the static parser/plan/dependency gate
pass with 12 jobs; the regenerated ledgers are 2,135/231/24/0 production and
15,891/540/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Wave 68 is the next parallel source queue.

Update note (2026-07-17): Wave-68 adds `tidb-planner::stack_contract`,
`tidb-stats::static_partitioned_analysis`, and
`tidb-exec::charset_variable_groups` as disjoint source-backed leaves. Shared
task-stack, static-partition analysis, and charset-variable ownership rows are
extended rather than duplicated. Workspace tests, strict Clippy, formatting,
and the static parser/plan/dependency gate pass with 12 jobs; the regenerated
ledgers are 2,132/234/24/0 production and 15,884/547/142/12 test/support
obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED). Wave 69 is the next parallel
source queue.

Update note (2026-07-17): Wave-69 adds `tidb-planner::topn_push_down`,
`tidb-stats::queue_gate`, and `tidb-exec::sysvar_type` as disjoint
source-backed leaves. The ScopeFlag and TypeFlag families share one
authoritative Go source ownership row while retaining separate Rust modules and
test anchors. Workspace tests, strict Clippy, formatting, and the static
parser/plan/dependency gate pass with 12 jobs; the regenerated ledgers are
2,130/236/24/0 production and 15,881/550/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Wave 70 is the next parallel source queue.

Update note (2026-07-17): Wave-70 adds `tidb-planner::derive_topn_from_window`,
`tidb-stats::ddl_queue_gate`, and `tidb-exec::sysvar_error` as disjoint
source-backed leaves. Shared rule-wrapper, DDL readiness, and sysvar-error
ownership rows are extended rather than duplicated. Workspace tests, strict
Clippy, formatting, and the static parser/plan/dependency gate pass with 12
jobs; the regenerated ledgers are 2,127/239/24/0 production and
15,877/554/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Wave 71 is the next parallel source queue.

Update note (2026-07-17): Wave-71 adds `tidb-planner::eliminate_empty_selection`,
`tidb-stats::refresher_state`, and `tidb-exec::hint_updatable_vars` as disjoint
source-backed leaves. Shared empty-selection, refresher-state, and SET_VAR
registry ownership rows are extended rather than duplicated. Workspace tests,
strict Clippy, formatting, and the static parser/plan/dependency gate pass with
12 jobs; the regenerated ledgers are 2,124/242/24/0 production and
15,874/557/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Wave 72 is the next parallel source queue.

Update note (2026-07-17): Wave-72 adds `tidb-planner::push_down_sequence`,
`tidb-stats::worker_capacity`, and `tidb-exec::noop_read_only` as disjoint
source-backed leaves. Sequence traversal, worker admission/update, and the
first no-op/read-only registration/policy slice are source-shaped; logical
operator mutation, async worker lifecycle, full SysVar mutation, and warning/
error/session plumbing remain external. Workspace tests and strict Clippy pass
with 12 jobs after the const-scope and public-field documentation fixes;
formatting/diff plus all static ledger/parser/plan/dependency gates pass. The
regenerated ledgers are 2,121/245/24/0 production and 15,871/560/142/12
test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED). Wave 73 is the
next parallel source queue.

Update note (2026-07-17): Wave-73 adds `tidb-planner::eliminate_unionall_dual_item`,
`tidb-stats::stats_key_set`, and `tidb-exec::session_reuse_state` as disjoint
source-backed leaves. Union-all dual elimination, LFU key-set operations, and
owner-gated session reuse/close transitions are source-shaped; logical operator
execution, LFU admission/eviction, table accounting, owner hooks, context
close, in-use deferral, transfer, and operation locking remain external. The
static ledgers are 2,118/248/24/0 production and 15,867/564/142/12
test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED). Workspace tests,
strict Clippy, formatting, and all static ledger/parser/plan/dependency gates
pass with 12 jobs. Wave 74 is the next parallel source queue.

Update note (2026-07-17): Wave-74 adds `tidb-planner::projection_elimination`,
`tidb-stats::stats_key_set_shards`, and `tidb-exec::system_db_filter` as
disjoint source-backed leaves. Projection eligibility, fixed 256-shard key-set
routing, and the exact system-database filter are source-shaped; full
expression/schema elimination, LFU admission/eviction and accounting, and
domain/schema loading remain external. The static ledgers are
2,114/252/24/0 production and 15,864/567/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Workspace tests, strict Clippy,
formatting, and all static ledger/parser/plan/dependency gates pass with 12
jobs. Wave 75 is the next parallel source queue.

Update note (2026-07-17): Wave-75 adds `tidb-planner::resolve_grouping_expand`,
`tidb-stats::memory_cost`, and `tidb-exec::upgrade_versions` as disjoint
source-backed leaves. Post-order Expand traversal, LFU capacity/cost policy,
and the exact ordered 173-entry upgrade registry are source-shaped; grouping
set construction, host-memory/cache lifecycle, upgrade SQL/bootstrap mutation,
and schema changes remain external. The static ledgers are
2,111/255/24/0 production and 15,861/570/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Workspace tests, strict Clippy,
formatting, and all static ledger/parser/plan/dependency gates pass with 12
jobs. Wave 76 is the next parallel source queue.

Update note (2026-07-17): Wave-76 adds `tidb-planner::join_reorder_projection_inline`
for the recursive source safety tree, `tidb-stats::BatchUpdate` for bounded
update/delete flush state, and `tidb-exec::session_metrics` for exact
metric-label registration order. Join-group attribution/substitution,
statistics queue/cache lifecycle, and metric collection remain external. The
static ledgers are 2,108/258/24/0 production and 15,858/573/142/12
test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED). Workspace tests,
strict Clippy, formatting, and all static ledger/parser/plan/domain gates pass
with 12 jobs. Wave 77 is the next parallel source queue.

Update note (2026-07-17): Wave-77 adds `tidb-planner::max_min_elimination`
for source eligibility and aggregate branch classification,
`tidb-stats::MapCache` for caller-costed map operations and copy state, and
`tidb-exec::hash_join_version` for the legacy/optimized version predicate.
Index-path/plan construction, LFU admission/eviction, cache ownership, SysVar
mutation, and join selection remain outside the dependency-closed leaves. The
static ledgers are 2,105/261/24/0 production and 15,853/578/142/12
test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED). Workspace tests,
strict Clippy, formatting, and all static ledger/parser/plan/domain gates pass
with 12 jobs. Wave 78 is the next parallel source queue.

Update note (2026-07-17): Wave-78 adds `tidb-planner::logical_table_dual` for
TableDual identity/hash and explain metadata, `tidb-stats::healthy_metrics`
for the exact ten healthy buckets, and `tidb-exec::slow_log_match` for slow-log
boolean composition and precedence. Field-type/runtime details, metrics
registration/traversal, and slow-log accessors/thresholds/session state remain
outside the dependency-closed leaves. The static ledgers are
2,102/264/24/0 production and 15,850/581/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Workspace tests, strict Clippy,
formatting, and all static ledger/parser/plan/domain gates pass with 12 jobs.
Wave 79 is the next parallel source queue.

Update note (2026-07-17): Wave-79 adds `tidb-planner::logical_limit` for Limit
identity/hash and bounded explain metadata, `tidb-stats::json_metadata` for
the global marker and deterministic predicate-column ordering, and
`tidb-exec::privilege_set` for exact privilege-set operations. Runtime limit
behavior, tipb/storage and stats-handle ownership, and GRANT/REVOKE
SQL/persistence remain outside the dependency-closed leaves. The static ledgers
are 2,099/267/24/0 production and 15,847/584/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Workspace tests, strict Clippy,
formatting, and all static ledger/parser/plan/domain gates pass with 12 jobs.
Wave 80 is the next parallel source queue.

Update note (2026-07-17): Wave-80 adds `tidb-planner::logical_max_one_row` for
the generated MaxOneRow identity/hash contract, `tidb-stats::locked_tables`
for the locked-table query marker and requested-ID filter, and
`tidb-exec::effective_auth_plugin` for explicit plugin precedence and default
fallback. Runtime planning, SQL/lock lifecycle, auth storage, capability
checks, and password policy remain outside the dependency-closed leaves. The
static ledgers are 2,096/270/24/0 production and 15,842/589/142/12
test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED). Workspace tests,
strict Clippy, formatting, and all static ledger/parser/plan/domain gates pass
with 12 jobs. Wave 81 is the next parallel source queue.

Update note (2026-07-17): Wave-81 adds `tidb-planner::logical_sort` for
generated Sort identity/hash framing, `tidb-stats::lock_messages` for stable
skipped-table/partition formatting, and `tidb-exec::broadcast_query_error` for
the nil-safe unsupported-broadcast classifier. Runtime ordering, lock/SQL
lifecycle, and broadcast RPC remain outside the dependency-closed leaves.

Update note (2026-07-17): Wave-82 adds `tidb-planner::logical_top_n` for
generated TopN identity/hash framing, `tidb-stats::usage_collector` for bounded
priority queues and worker drain/close behavior, and
`tidb-exec::insert_rows_col_multiply` for zero-aware saturating multiplication.
Runtime TopN, session/worker lifecycle, and RUV2 metric wiring remain external.
The static ledgers are 2,090/276/24/0 production and 15,833/598/142/12
test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED). Workspace tests,
strict Clippy, formatting, and all static ledger/parser/plan/domain gates pass
with 12 jobs. Wave 83 is the next parallel source queue.

Update note (2026-07-17): Wave-83 adds three bounded source-owned leaves.
`tidb-planner::logical_show_ddl_jobs` ports generated ShowDDLJobs identity/hash
framing; `tidb-stats::stats_delta` ports the locked-statistics delta query
marker and row/error behavior; and `tidb-exec::readable_size` ports
case-sensitive human-readable size parsing with source-compatible wrapping.
DDL scheduling, statistics-handle ownership, inspection SQL, and caller policy
remain external. The regenerated ledgers are 2,087/279/24/0 production and
15,830/601/142/12 test/support obligations; the parser ring and full
workspace/Clippy gate remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` still tracked separately.
Wave 84 is the next parallel source queue.

Update note (2026-07-17): Wave-84 adds three bounded source-owned leaves.
`tidb-planner::logical_show` ports generated Show identity/hash framing with
ordered normalized schema metadata; `tidb-stats::bootstrap_sql` ports exact
statistics metadata and histogram bootstrap SQL with ordered IDs and
`[start,end)` paging; and `tidb-exec::placement_labels` ports deterministic
SHOW PLACEMENT label grouping, deduplication, and row ordering. SHOW contents,
stats-handle/session/SQL execution, BinaryJSON/PD/store retrieval, and row
encoding remain external. The regenerated ledgers are 2,084/282/24/0
production and 15,823/608/142/12 test/support obligations; the parser ring and
full workspace/Clippy gate remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 85 is the next parallel source queue.

Update note (2026-07-17): Wave-85 adds three bounded source-owned leaves.
`tidb-planner::logical_schema_producer` ports generated
LogicalSchemaProducer identity/hash framing with nil/present ordered schemas;
`tidb-stats::special_global_index` ports the virtual-generated/prefix-column
global-index predicate and any-column short circuit; and
`tidb-exec::lazy_txn_state` ports source-faithful `Valid`, `pending`, and
`validOrPending` composition. The lazy-transaction original test anchors were
already owned by the transaction-wave17/18 rings, so the shared test ledger
keeps one authoritative owner per anchor. Schema propagation, full field
metadata, index metadata resolution, KV/session lifecycle, and transaction
execution remain external. The regenerated ledgers are 2,081/285/24/0
production and 15,821/610/142/12 test/support obligations; the parser ring and
full workspace/Clippy gate remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 86 is the next parallel source queue.

Update note (2026-07-17): Wave-86 adds three bounded source-owned leaves.
`tidb-planner::logical_sequence` ports generated LogicalSequence identity/hash
framing; `tidb-stats::global_topn` ports histogram-free partition TopN
aggregation with wrapping sums, count/encoded-byte ranking, and selected versus
remainder ordering; and `tidb-exec::config_int_json` ports integer SET CONFIG
JSON rendering for boolean flags and ordinary integers. The global TopN
evidence uses the unclaimed `global_stats_test.go:322 TestGlobalStatsData3`
anchor (ranking assertions at 342-347); narrower TopN anchors remain owned by
the earlier datum-map-cache ring. CTE/runtime sequence behavior, histograms,
Datum/config mutation, storage, and session lifecycle remain external. The
regenerated ledgers are 2,078/288/24/0 production and 15,818/613/142/12
test/support obligations; the parser ring and full workspace/Clippy gate remain
green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 87 is the next parallel source queue.

Update note (2026-07-17): Wave-87 adds three bounded source-owned leaves.
`tidb-planner::logical_union_all` ports generated LogicalUnionAll identity/hash
framing; the initially selected LogicalSelection owner was already claimed and
was removed before integration. `tidb-stats::pending_delta_ids` ports pending
table-ID filtering, target deduplication, and ascending order; and
`tidb-exec::lack_handles` ports ordered missing-handle reconciliation with the
source cardinality stop boundary. Union execution, stats/session sweeps,
storage, KV encoding, workers, and consistency reporting remain external. The
regenerated ledgers are 2,075/291/24/0 production and 15,815/616/142/12
test/support obligations; the parser ring and full workspace/Clippy gate remain
green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 88 is the next parallel source queue.

Wave 88 adds `tidb-planner::logical_mem_table` for generated LogicalMemTable
identity/hash framing over optional table metadata and normalized names,
`tidb-stats::sync_load_concurrency` for the source table-count threshold
policy, and `tidb-exec::slow_log_split` for nested byte-oriented slow-log
field/value splitting with malformed-input and cardinality-boundary behavior.
Memtable planning/execution, statistics scheduling/handle lifecycle, log
ingestion, session policy, and persistence remain external. The regenerated
ledgers are 2,072/294/24/0 production and 15,812/619/142/12 test/support
obligations; the parser ring and full workspace/Clippy gate remain green, with
the pinned Go restore failure at `tests/integrationtest/t/expression/json.test:582`
tracked separately.
Wave 89 is the next parallel source queue.

Wave 89 adds `tidb-planner::logical_projection` for generated LogicalProjection
identity/hash framing over normalized schema and ordered expression columns,
nil/present expression markers, `CalculateNoDelay`, and `Proj4Expand`,
`tidb-stats::partition_table_id_cache` for schema-versioned
partition-to-parent-table cache rebuild/lookup with duplicate last-write
behavior, and `tidb-exec::analyze_panic_error` for analyze-worker panic
classification covering the memory sentinel, propagated errors, and worker
fallback. Expression evaluation, rewrites, InfoSchema traversal/table
resolution, locking, recovery, logging, and worker scheduling remain external.
The regenerated ledgers are 2,069/297/24/0 production and 15,809/622/142/12
test/support obligations; the parser ring and full workspace/Clippy gate remain
green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 90 is the next parallel source queue.

Wave 90 adds `tidb-planner::logical_expand` for generated LogicalExpand
identity/hash framing over normalized grouping metadata and nested rollup/level
structure, `tidb-stats::weighted_reservoir` for bounded weighted sampling with
source min-heap fill/replace and tie behavior, and
`tidb-exec::delete_rows_col_multiply` for saturating DELETE row/column metric
accumulation, MAX sentinel handling, and positive overflow clamping. Expression
variants, grouping maps, RNG/Datum/sketch collectors, metric/session/storage
effects, optimizer context, and runtime execution remain external. The
regenerated ledgers are 2,066/300/24/0 production and 15,806/625/142/12
test/support obligations; the parser ring and full workspace/Clippy gate remain
green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 91 is the next parallel source queue.

Wave 91 adds `tidb-planner::window_frame` for FrameBound/WindowFrame
Hash64/Equals, nil-preserving clone behavior, caller compare-function tokens,
and source start/end hash asymmetry; `tidb-stats::stats_meta` for exact normal
and `FOR UPDATE` `mysql.stats_meta` selectors, empty-row null sentinels, and
uint64-to-int64 conversion; and `tidb-exec::cte_first_error` for first-error
precedence preserving the original value. Expressions, SQL/storage/DDL
execution, worker lifecycle, logging, failpoints, and cleanup ordering remain
external. The regenerated ledgers are 2,063/303/24/0 production and
15,801/630/142/12 test/support obligations; the parser ring and full
workspace/Clippy gate remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 92 is the next parallel source queue.

Wave 92 adds `tidb-planner::handle_cols` for CommonHandleCols and IntHandleCols
identity/hash framing with nil/present metadata and ordered column lists,
`tidb-stats::stats_read_writer` for historical-version and slow-save predicates,
the five-lease threshold, force override, duration wrapping, and exact refresh
error text, and `tidb-exec::traffic_form` for Go-compatible sorted form
encoding, escaping, duplicate ordering, UTF-8, and reserved-byte boundaries.
Catalog/handle/storage, SQL/transaction/failpoint lifecycle, and HTTP/TiProxy
traffic remain external. The regenerated ledgers are 2,060/306/24/0 production
and 15,795/636/142/12 test/support obligations; the parser ring and full
workspace/Clippy gate remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 93 is the next parallel source queue.

Wave 93 adds `tidb-planner::logical_aggregation` for source-faithful
`LogicalAggregation` Hash64/Equals framing, normalized aggregate metadata,
ordered possible properties, and explicit `HasTiFlash` omission;
`tidb-stats::stats_meta_update` for locked/unlocked and positive/negative delta
partitioning, exact stats-meta SQL assembly, cache invalidation order,
MinInt64 wrapping, and version-refresh parameters; and
`tidb-exec::ddl_job_comments` for source-ordered analyze, reorg, DXF/cloud,
worker, batch, write-speed, and placement labels, including next-gen early
return behavior. The evidence audit corrected the Go test anchors to
`show_ddl_jobs_test.go:26` and `:115`; live planner/statistics/DDL execution
remains external. The regenerated ledgers are 2,057/309/24/0 production and
15,790/641/142/12 test/support obligations; parser, workspace, Clippy, and
static domain gates remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 94 is now the next parallel source queue.

Wave 94 adds `tidb-planner::cost_usage` for CostVer2/CostTrace factor gating,
lazy formula construction, ordered aggregation, fixed-point arithmetic,
nonnegative/NaN handling, and tie-break preservation;
`tidb-stats::sample_bytes` for the exact 32,767-byte sample limit, inclusive
length filtering, and wrapping total-size accumulation; and
`tidb-exec::global_sysvar_initial` for environment-adjusted system-variable
defaults across TiKV, test, row-format, assertion, mutation-checker, and
fair-locking branches. Registry lookup, validation, SessionVars mutation, and
next-gen hook errors remain external. The regenerated ledgers are
2,054/312/24/0 production and 15,785/646/142/12 test/support obligations;
parser, workspace, Clippy, and static domain gates remain green, with the
pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 95 is now the next parallel source queue.

Wave 95 adds `tidb-planner::wrap_cast` for the source mode gate across
Complete/Partial1/Dedup and Final/Partial2, including caller-marked delegated
uncastable arguments; `tidb-stats::index_query_bytes` for TopN-hit, CMSketch-hit,
then histogram fallback precedence over caller-supplied counts; and
`tidb-exec::tagged_ptr` for 64-bit tagged-pointer width, mask initialization,
tag extraction, clear/roundtrip behavior, and the 24-bit cap. Expression
construction, statistics encoding/lifecycle, and join/hash-table execution
remain external. The regenerated ledgers are 2,051/315/24/0 production and
15,780/651/142/12 test/support obligations; parser, workspace, Clippy, and
static domain gates remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 96 is now the next parallel source queue.

Wave 96 adds `tidb-planner::logical_mock` for `MockDataSource.Init` metadata
with `mockDS`, query-block offset zero, retained plan-context token, and
reinitialization/zero-value behavior; `tidb-stats::historical_stats` for
table-versus-partition history-version selection; and `tidb-exec::stddevpop`
for zero-count NULL handling plus `sqrt(variance/count)` with negative variance
preserving NaN. Physical mock planning, JSON/storage/session lifecycle, and
aggregation accumulation remain external. The regenerated ledgers are
2,048/318/24/0 production and 15,776/655/142/12 test/support obligations;
parser, workspace, Clippy, and static domain gates remain green, with the
pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 97 is now the next parallel source queue.

Wave 97 adds `tidb-planner::logical_property` for zero-value and optional
Stats/Schema/FD state, MaxOneRow, nil-vs-empty PossibleProps, and HasTiFlash
preservation; `tidb-stats::init_stats_concurrency` for force CPU-minus-two and
normal CPU-half policies with the `[2,16]` clamp and signed wrapping arithmetic;
and `tidb-exec::stddevsamp` for counts-at-most-one NULL handling plus
`sqrt(variance/(count-1))` with negative variance preserving NaN. Memo/schema
consumers, runtime/config lifecycle, and aggregation accumulation remain
external. The regenerated ledgers are 2,045/321/24/0 production and
15,772/659/142/12 test/support obligations; parser, workspace, Clippy, and
static domain gates remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 98 is now the next parallel source queue.

Wave 98 adds `tidb-planner::outer_to_inner_join` for the rule identity,
exactly-once delegated LogicalPlan conversion, and intentionally unchanged
flag; `tidb-stats::predicate_column_queries` for exact load-all, load-table,
predicate, cleanup SQL markers and ordered decimal column-ID formatting; and
`tidb-exec::varsamp` for counts-at-most-one NULL handling plus
`variance/(count-1)` while preserving signed float results. Join semantics,
schema/session/storage execution, and aggregation accumulation remain external.
The regenerated ledgers are 2,042/324/24/0 production and
15,767/664/142/12 test/support obligations; parser, workspace, Clippy, and
static domain gates remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 99 is now the next parallel source queue.

Update note (2026-07-17): Wave-99 adds three bounded source-owned leaves.
`tidb-planner::columnar_index_extra` ports the vector columnar-index metadata
constructor with fixed vector type, retained index identity/derived ID, ANN
query type/metric/top-k, column name, copied reference-vector bytes, and source
column identity; its direct Go test anchor is
`pkg/planner/core/task_heavy_function_optimize_test.go:36
TestGetPushedDownTopNHeavyFunctionNotFirstByItem`.
`tidb-stats::ddl_stats_delta` ports the locked, missing-row, and existing-row
`stats_meta` SQL branches with ordered arguments, GREATEST clamps, and Go
wrapping additions; its direct anchors are
`pkg/statistics/handle/ddl/ddl_test.go:1106 TestExchangeAPartition` and
`:1256 TestExchangeAPartitionAndDropTableImmediately`.
`tidb-exec::cume_dist` ports the source `curIdx`/`lastRank` tied-peer loop as
an Iterator plus partial state-size metadata; its direct anchors are
`pkg/executor/aggfuncs/func_cume_dist_test.go:25 TestMemCumeDist` and
`pkg/executor/aggfuncs/window_func_test.go:172 TestWindowFunctions`. TiFlash/vector planning,
DDL/storage/session lifecycle, row comparison, window scheduling, and chunk
execution remain external. The regenerated ledgers are 2,039/327/24/0
production and 15,762/669/142/12 test/support obligations; parser, workspace,
Clippy, and static domain gates remain green, with the pinned Go restore failure
at `tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 100 is now the next parallel source queue.

Update note (2026-07-17): Wave-100 adds three bounded source-owned leaves.
`tidb-planner::physical_cte_table` ports signed CTE storage identity,
`Scan on CTE_<id>` explain text, and the index-join/sort task rejection gates;
its direct anchor is `pkg/planner/core/tests/redact/redact_test.go:23
TestRedactExplain`. `tidb-stats::gc_batch_count` ports Go `forCount` integer
division, positive-remainder rounding, and signed overflow behavior; its
direct anchors are `pkg/statistics/handle/storage/gc_test.go:30 TestGCStats`
and `:63 TestGCPartition`. `tidb-exec::ntile` ports the five-field partial
state, quotient/remainder updates, reset, group advancement, and zero-divisor
NULL behavior; its unowned direct anchor is
`pkg/executor/aggfuncs/func_ntile_test.go:25 TestMemNtile`. Schema/statistics/
task wiring, storage/session lifecycle, typed chunks, argument coercion, and
window scheduling remain external. The regenerated ledgers are 2,036/330/24/0
production and 15,758/673/142/12 test/support obligations.

Update note (2026-07-17): Wave-101 adds `tidb-exec::lead_lag`, porting the
buffered row cursor, physical lead/lag offsets, current-row/default fallback,
reset, and partial-state size. Typed Datum serialization, chunk/window
construction, and scheduling remain external. Direct anchors are
`pkg/executor/aggfuncs/func_lead_lag_test.go:27 TestLeadLag` and
`:119 TestMemLeadLag`. The Wave-101 combined regenerated ledgers are
2,035/331/24/0 production and 15,756/675/142/12 test/support obligations; the
same fail-fast 12-job workspace/Clippy/static gate is green, with the pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582` tracked
separately. Wave 102 is now the next parallel source queue.

Wave 102 adds `tidb-planner::physical_max_one_row` for the pure
`ExhaustPhysicalPlans4LogicalMaxOneRow` support gates, fixed `ExpectedCnt: 2`,
and CTE/no-cop metadata forwarding; `tidb-stats::StatsLease` for atomic signed-
nanosecond lease get/set semantics; and `tidb-exec::json_arrayagg` for ordered
accumulation, partial merge/reset, empty-input NULL, JSON framing, scalar
escaping, finite-real guards, and explicit spill boundaries. Exact anchors are
`pkg/executor/test/executor/executor_test.go:2157`,
`pkg/statistics/integration_test.go:220`/`:266`,
`pkg/executor/aggfuncs/func_json_arrayagg_test.go:27`/`:65`/`:131`, and
`pkg/executor/aggfuncs/spill_helper_test.go:842`. Typed conversion, chunk and
task execution, storage/session/statistics lifecycle, and full runtime wiring
remain external. The regenerated ledgers are 2,032/334/24/0 production and
15,749/682/142/12 test/support obligations; parser, workspace, Clippy, and
static domain gates pass with 12 jobs. Wave 103 was integrated into the next
verified workspace cycle.

Wave 103 adds `tidb-planner::logical_cte_table` for the exact `DeriveStats`
reload-vector transition, `tidb-stats::global_stats_layout` for the
`newGlobalStats` zero/nil slot layout, and `tidb-exec::json_objectagg` for
ordered key/value state, source-after-destination merge, duplicate-key
last-wins, lexicographic JSON framing, empty-input NULL, and NULL/binary-key
rejection. Exact anchors are
`pkg/planner/core/casetest/planstats/plan_stats_test.go:281`,
`pkg/statistics/handle/globalstats/global_stats_test.go:137`,
`pkg/executor/aggfuncs/func_json_objectagg_test.go:48`/`:110`/`:163`, and
`pkg/executor/aggfuncs/spill_helper_test.go:889`. Typed evaluation, concrete
stats/schema context, BinaryJSON/memory/spill integration, chunk execution,
and storage/session lifecycle remain external. The regenerated ledgers are
2,029/337/24/0 production and 15,743/688/142/12 test/support obligations;
parser, workspace, Clippy, and static domain gates pass with 12 jobs. Wave 104
was integrated into the next verified workspace cycle.

Wave 104 adds `tidb-planner::telemetry` for exact `IsTiFlashContained`
Explain/physical/TiFlash traversal, `tidb-stats::table_id_filter` for exact
source-ordered signed decimal `table_id in (...)` formatting including empty
input, and `tidb-exec::first_row` for first-physical-row-wins state, NULL
preservation, later-batch short-circuit, unset-destination merge, and reset.
Exact anchors are `pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go:568`,
`pkg/executor/test/infoschema/infoschema_test.go:171`/`:224`,
`pkg/executor/aggfuncs/func_first_row_test.go:27`/`:52`, and the ten
type-specific spill anchors from `pkg/executor/aggfuncs/spill_helper_test.go:941`
through `:1349`. Concrete plans/session telemetry, cache/InfoSchema lifecycle,
typed values/chunk output, memory and spill encoding remain external. The
regenerated ledgers are 2,026/340/24/0 production and 15,728/703/142/12
test/support obligations; parser, workspace, Clippy, and static domain gates
pass with 12 jobs. Wave 105 was integrated into the next verified workspace cycle.

Wave 105 adds `tidb-planner::condition_to_dual` for exact
`IsConstFalse`/`Conds2TableDual` truth control, `tidb-stats::auto_analyze_process_set`
for the RWMutex-backed tracker/untracker/all/contains process set, and
`tidb-exec::bit_agg` for u64 AND/OR/XOR identities, NULL-skipping updates,
operation-preserving merges, and reset. Exact anchors are
`pkg/planner/core/logical_plans_test.go:241`,
`pkg/statistics/handle/autoanalyze/exec/exec_test.go:35`/`:154`,
`pkg/executor/aggfuncs/func_bitfuncs_test.go:25`/`:36`, and
`pkg/executor/aggfuncs/spill_helper_test.go:801`. Expression/coercion and
statement context, concrete stats/process execution, typed Eval/chunk/sliding/
memory/spill integration, and optimizer/runtime wiring remain external. The
regenerated ledgers are 2,023/343/24/0 production and 15,722/709/142/12
test/support obligations; parser, workspace, Clippy, and static domain gates
pass with 12 jobs. Wave 106 is now the next parallel source queue.

Wave 106 adds `tidb-planner::physical_table_sample` for exact TableSample
initialization metadata, `tidb-stats::stats_meta_save_sql` for source-ordered
`stats_meta` INSERT/upsert tuples with optional histogram-version metadata, and
`tidb-exec::varpop` for non-DISTINCT float64 population variance state,
NULL-skipping updates, source intermediate/merge formulas, zero-count branches,
population output, and reset. Exact anchors are `pkg/executor/sample_test.go:111`,
`pkg/statistics/integration_test.go:442`, and
`pkg/executor/aggfuncs/func_varpop_test.go:28`/`:37`/`:46`/`:54`. Typed
evaluation, SQL/storage/session execution, DISTINCT sets, chunk/sliding/memory,
spill, and runtime wiring remain external. The regenerated ledgers are
2,020/346/24/0 production and 15,716/715/142/12 test/support obligations;
parser, workspace, Clippy, and static domain gates pass with 12 jobs. Waves
107-112 were integrated below; Wave 113 is the next parallel source queue.

Wave 107 adds `tidb-planner::rule_set`, `tidb-stats::init_stats_progress`, and
`tidb-exec::sum_float64`. The leaves preserve source-shaped rule-ID mask
filtering and intermediate Apply selection, init-stats progress arithmetic with
Go's float64 coercion and IEEE zero-denominator behavior, and non-DISTINCT
float64 SUM state with NULL skipping, empty-result NULL, source-empty merge
short-circuit, and reset. Concrete rule/memo/optimizer execution, worker
goroutines/channels/atomics, typed SUM coercion and variants,
DISTINCT/sliding/chunk/memory/spill, and runtime wiring remain external. Exact
anchors are `pkg/planner/cascades/rule/ruleset/rule_set.go` plus
`pkg/planner/cascades/old/optimize_test.go:212`,
`pkg/statistics/handle/initstats/load_stats_page.go:104-107` plus
`pkg/statistics/handle/handletest/initstats/init_stats_test.go:231`, and
`pkg/executor/aggfuncs/func_sum_test.go:33`/`:50`/`:66` plus
`pkg/executor/aggfuncs/spill_helper_test.go:658`/`:703`. The regenerated ledgers
are 2,017/349/24/0 production and 15,704/727/142/12 test/support obligations;
parser, workspace, Clippy, and static domain gates pass with 12 jobs. Waves 108
108-112 were integrated below; Wave 113 is the next parallel source queue.

Wave 108 adds `tidb-planner::column_pruning`,
`tidb-stats::global_stats_sql_index`, and `tidb-exec::group_concat`. The leaves
preserve recursive zero-column schema validation with schema-reuse/TableDual
exceptions, exact `toSQLIndex` false/true to SQL 0/1 conversion, and the
non-DISTINCT GROUP_CONCAT buffer state with NULL skipping, separator/order,
merge/reset/final-NULL behavior, byte-based max length, and truncation sentinel.
Logical-plan/optimizer execution, stats worker/storage/SQL lifecycle, typed
GROUP_CONCAT evaluation, warning publication, DISTINCT/ORDER BY,
chunk/memory/spill, and runtime wiring remain external. Exact anchors are
`pkg/planner/core/logical_plans_test.go:652`,
`pkg/statistics/handle/globalstats/global_stats_test.go:260`, and
`pkg/executor/aggfuncs/func_group_concat_test.go:37`/`:42`/`:66`/`:81`.
The regenerated ledgers are 2,014/352/24/0 production and
15,698/733/142/12 test/support obligations; parser, workspace, Clippy, and
static domain gates pass with 12 jobs. Waves 109-112 were integrated below;
Wave 113 is the next parallel source queue.

Wave 109 adds `tidb-planner::physical_union_scan`,
`tidb-stats::ddl_physical_ids`, and `tidb-exec::sum_int`. The leaves preserve
UnionScan TiFlash rejection/index-join admission and initialization metadata,
DDL stats physical-ID selection with nil-versus-empty partition distinction and
dynamic global-ID append, and signed/unsigned non-DISTINCT SUM state with
checked Add/Sub overflow, NULL/empty, merge/reset, and outgoing-before-incoming
sliding order. Property/optimizer and executor wiring, DDL/session/storage
lifecycle, EvalInt/dispatch, chunk/memory/spill, and DISTINCT remain external.
Exact anchors are `pkg/planner/core/casetest/dag/dag_test.go:274`,
`pkg/statistics/handle/ddl/ddl_test.go:203`, and the shared SUM anchors
`pkg/executor/aggfuncs/func_sum_test.go:33`/`:50`/`:66`/`:89`/`:133`; those
top-level SUM rows remain under the existing aggregate test domain. The
regenerated ledgers are 2,011/355/24/0 production and
15,696/735/142/12 test/support obligations; parser, workspace, Clippy, and
static domain gates pass with 12 jobs. Waves 110-112 were integrated below;
Wave 113 is the next parallel source queue.

Wave 110 adds `tidb-planner::physical_show`, `tidb-stats::stats_cache_version`,
and `tidb-exec::percentile`. The leaves preserve PhysicalShow/PhysicalShowDDLJobs
plan metadata and shared rejection gates, monotonic stats-cache version updates
with `skip_move_forward`, and bounded integer/real APPROX_PERCENTILE state with
NULL skipping, source-clearing merge, reset, exact ordinal rank selection, and
P=100 behavior. SHOW catalog/extractor/task/runtime wiring, cache atomics and
Handle lifecycle, typed percentile coercion/dispatch, introselect, chunk/memory,
and unsupported temporal/decimal/enum/set/bit variants remain external. Exact
anchors are `pkg/planner/core/planbuilder_test.go:63` (`TestShow`),
`pkg/statistics/handle/handletest/handle_test.go:111` (`TestVersion`), and
`pkg/executor/aggfuncs/func_percentile_test.go:35`/`:51`/`:63`. The regenerated
ledgers are 2,008/358/24/0 production and 15,690/741/142/12 test/support
obligations; parser, workspace, Clippy, and static domain gates pass with 12
jobs. Wave 112 was integrated below; Wave 113 is the next parallel source queue.

Wave 111 adds `tidb-planner::physical_lock`, `tidb-stats::topn_merge_task`,
and `tidb-exec::avg_float64`. The leaves preserve PhysicalLock TiFlash
rejection, `Lock` plan metadata, query-block offset zero, opaque lock type,
lossless wait seconds, and exact ExplainInfo; the TopN merge-task range
descriptor without validation; and non-DISTINCT float64 AVG sum/count,
NULL/empty behavior, merge/reset, and incoming-before-outgoing sliding order.
AST/catalog/task/lock execution, TopN worker/concurrency/merge arithmetic,
typed AVG coercion, decimal/DISTINCT, rounding/context, chunk/memory, and spill
remain external. Exact anchors are
`pkg/planner/core/tests/pointget/point_get_plan_test.go:407`,
`pkg/statistics/handle/globalstats/topn_bench_test.go:94`, and
`pkg/executor/aggfuncs/func_avg_test.go:27`/`:37`/`:48`. The regenerated
ledgers are 2,005/361/24/0 production and 15,685/746/142/12 test/support
obligations; parser, workspace, Clippy, and static domain gates pass with 12
jobs. Wave 112 was integrated below; Wave 113 is the next parallel source queue.

Wave 112 adds `tidb-planner::physical_table_dual`, `tidb-stats::json_stats_version`,
and `tidb-exec::minmax_deque`. The leaves preserve PhysicalTableDual `Dual`
metadata, query-block offset, `rows:<RowCount>` explain text, IndexJoin
rejection, and row-count-dependent sort admission; the old JSON StatsVer
fallback where explicit versions win and missing positive NDV/null-count infers
version 1; and MinMaxDeque pair storage, deque operations, reset, expiry
dequeue, and monotonic max/min enqueue with equal-value eviction. Schema/
catalog/task wiring, JSON/storage/session lifecycle, typed MAX/MIN evaluation,
window callbacks, chunk/memory, and spill remain external. Exact anchors are
`pkg/planner/core/casetest/cbotest/cbo_test.go:367`,
`pkg/statistics/handle/storage/dump_test.go:582`, and
`pkg/executor/aggfuncs/func_max_min_test.go:335`/`:345`. The regenerated
ledgers are 2,002/364/24/0 production and 15,681/750/142/12 test/support
obligations; parser, workspace, Clippy, and static domain gates pass with 12
jobs. Wave 113 is the next parallel source queue.

Wave 113 adds three disjoint source-backed leaves: `tidb-planner::logical_lock`
preserves raw lock discriminants and the supported FOR UPDATE/FOR SHARE sets;
`tidb-stats::stats_lock_table` preserves fully qualified table names and the
nil-versus-explicit-empty partition-map payload; and
`tidb-exec::count_distinct_int` preserves typed-int NULL skipping,
deduplication, cardinality, source-preserving partial merge, and reset. Exact
anchors are `pkg/planner/core/integration_test.go:1466`,
`pkg/statistics/handle/lockstats/lock_stats_test.go:186`/`:260`, and
`pkg/executor/aggfuncs/func_distinct_agg_test.go:26` plus
`pkg/executor/aggfuncs/func_count_test.go:115`. SQL/session/lock execution,
other DISTINCT types, typed Eval/chunk/memory/spill integration, and runtime
scheduling remain external. The regenerated ledgers are 1,999/367/24/0
production and 15,676/755/142/12 test/support obligations; parser, workspace,
Clippy, formatting, and static ledger/parser/plan/domain gates pass with 12
jobs. Wave 114 is the next parallel source queue.

Wave 114 adds three disjoint source-backed leaves: `tidb-planner::physical_exchange_receiver`
preserves `ExchangeReceiver` plan identity, root offset zero, uint64 stream-count
metadata, and exact explain rendering; `tidb-stats::pseudo_cache_policy`
preserves non-partitioned admission, the partitioned cache-length threshold of
64, and temporary-table rejection; and `tidb-exec::window_value_int` preserves
already-evaluated integer FIRST_VALUE/LAST_VALUE/NTH_VALUE transitions,
including NULL capture, batch-spanning selection, reset, and unreached output.
Exact anchors are `pkg/planner/core/integration_test.go:904`,
`pkg/statistics/handle/handletest/handle_test.go:1100`, and
`pkg/executor/aggfuncs/func_value_test.go:63`. MPP/runtime, pseudo-table/cache
lifecycle, typed evaluators, all value domains, and chunk/memory/window
dispatch remain external. The regenerated ledgers are 1,996/370/24/0
production and 15,673/758/142/12 test/support obligations; parser, workspace,
Clippy, formatting, and static ledger/parser/plan/domain gates pass with 12
jobs. Wave 115 was integrated below; Wave 116 is the next parallel source queue.

Wave 115 adds three disjoint source-backed leaves: `tidb-planner::physical_selection`
preserves Selection plan identity, caller-owned query-block offset, condition
text, zero-stream passthrough, and the exact stream-count suffix;
`tidb-exec::spill_count` preserves native-endian int64 count serialization,
strict decoding, reusable buffers, and sequential row consumption; and
`tidb-stats::cache_metrics_labels` preserves the six counter and two gauge
label strings in source order. Exact anchors are
`pkg/planner/core/casetest/mpp/mpp_test.go:673`,
`pkg/executor/aggfuncs/spill_helper_test.go:73`, and
`pkg/statistics/handle/cache/bench_test.go:99`. MPP/runtime, typed expression
and aggregate domains, chunk/spill lifecycle, Prometheus handles, cache
concurrency, and session/storage integration remain external. The regenerated
ledgers are 1,993/373/24/0 production and 15,670/761/142/12 test/support
obligations; parser, workspace, Clippy, formatting, and static
ledger/parser/plan/domain gates pass with 12 jobs. The evidence-fragment loader
now rejects escaped `\t` headers. Wave 116 is the next parallel source queue.

Wave 116 adds three disjoint source-backed leaves: `tidb-planner::physical_limit`
preserves Limit plan identity, caller query-block offset, lossless offset/count
metadata, and ExplainInfo redaction branches over caller-owned partition/prefix
text; `tidb-exec::pd_approximate_count` preserves the direct underscore-joined
approximate-count key and bounded TTL/LRU hit/miss/eviction behavior with a
caller-supplied clock; and `tidb-stats::ddl_event_match` preserves first-match
selection with no-match timeout behavior. Exact anchors are
`pkg/planner/core/casetest/physicalplantest/physical_plan_test.go:1600`,
`pkg/executor/internal/pdhelper/pd.go:69-85` plus
`pkg/executor/internal/pdhelper/pd_test.go:42`, and
`pkg/statistics/handle/autoanalyze/priorityqueue/queue_ddl_handler_test.go:885`.
Typed planner properties, PD/storage and restricted-SQL access, channel/ticker
timing, notifier decoding, and full planner/executor/statistics/session/SQL
lifecycle remain external. The regenerated ledgers are 1,990/376/24/0
production and 15,667/764/142/12 test/support obligations; parser, workspace,
Clippy, formatting, and static ledger/parser/plan/domain gates pass with 12
jobs. Wave 117 was integrated below; Wave 118 is the next parallel source
queue.

Wave 117 adds three disjoint source-backed leaves: `tidb-planner::physical_union_all`
preserves Union plan identity, caller query-block offset, MPP flag, and source
Exhaust gates/candidate ordering; `tidb-exec::apply_cache` preserves byte-key/
value memory charge, over-quota rejection, oldest-entry LRU eviction, and
get-touch/accounting behavior; and `tidb-stats::mock_statistics_shape`
preserves fixture column/index counts, CMSketch/TopN/histogram switches, and
total item count. Exact anchors are `pkg/planner/core/casetest/mpp/mpp_test.go:446`,
`pkg/executor/internal/applycache/apply_cache.go:35-43,76-101` plus
`pkg/executor/internal/applycache/apply_cache_test.go:30`, and
`pkg/statistics/handle/cache/bench_test.go:125`. Child planner properties,
typed chunk/memory/session quota, statistics allocation/cache concurrency, and
runtime/benchmark integration remain external. The regenerated ledgers are
1,987/379/24/0 production and 15,664/767/142/12 test/support obligations;
parser, workspace, Clippy, formatting, and static ledger/parser/plan/domain
gates pass with 12 jobs. Wave 118 is the next parallel source queue.

Wave 118 adds three disjoint source-backed leaves: `tidb-planner::physical_apply`
preserves Apply plan identity, caller query-block offset, and the exact
non-PhysicalJoin boundary; `tidb-exec::next_io_acc` preserves positive row/cell
guards, reset/reuse, wrapping accumulation, and child/parent/tracking admission;
and `tidb-stats::stats_request_matcher` preserves the exact
`internal_StatsForegroundPriority` predicate and matcher description. Exact
anchors are `pkg/planner/core/casetest/physicalplantest/physical_plan_test.go:1537`,
`pkg/executor/internal/exec/executor.go:42-89` plus
`pkg/executor/internal/exec/executor_test.go:35`, and
`pkg/statistics/handle/util/test/ctx_matcher.go:24-36` plus
`pkg/statistics/handle/autoanalyze/autoanalyze_test.go:407`. Hash-join/subquery
runtime, executor atomics/provider/pool/RUV2, context/request propagation,
gomock/SQL/session lifecycle, and full integration remain external. The
regenerated ledgers are 1,984/382/24/0 production and 15,661/770/142/12
test/support obligations; parser, workspace, Clippy, formatting, and static
ledger/parser/plan/domain gates pass with 12 jobs. Wave 119 is the next parallel
source queue.

Wave 119 adds three disjoint source-backed leaves: `tidb-planner::physical_projection`
preserves Projection plan identity, caller query-block offset, opaque
expression-list rendering, and the uint64 stream-count suffix;
`tidb-exec::cluster_index_id` preserves clustered-index identity selection for
PK-as-handle, common-handle primary indexes, and rowid/non-clustered tables;
and `tidb-stats::predicate_column_query_mode` preserves the exact transaction
boundary (`LoadColumnStatsUsage` without `FlagWrapTxn`, `GetPredicateColumns`
with it). Exact anchors are `pkg/planner/core/casetest/mpp/mpp_test.go:710`,
`pkg/executor/internal/exec/indexusage.go:130-148` plus
`pkg/executor/internal/exec/indexusage_test.go:447`, and
`pkg/statistics/handle/usage/predicate_column.go:47-62` plus
`pkg/statistics/handle/usage/predicate_column_test.go:103`. Typed projection,
table/index collector, session-pool/SQL, and full planner/executor/statistics
integration remain external. The regenerated ledgers are 1,981/385/24/0
production and 15,658/773/142/12 test/support obligations; parser, workspace,
Clippy, formatting, and static ledger/parser/plan/domain gates pass with 12
jobs. Wave 121 is the next parallel source queue.

Wave 120 adds three disjoint source-backed leaves: `tidb-planner::physical_shuffle`
preserves `Shuffle` plan identity/query-block offset, hash/range splitter
discriminants, and source-shaped concurrency/data-source ExplainInfo;
`tidb-stats::index_usage_key` preserves the exact table-ID/index-ID lookup pair
used by index-usage GC; and `tidb-exec::mock_global_accessor` preserves
ordinary/test-suite variable maps, unknown-variable errors, default
authentication plugin validation plus its bypass setter, and
`tikv_gc_life_time` readback. Exact anchors are
`pkg/planner/core/operator/physicalop/physical_shuffle.go:155` plus
`pkg/planner/core/casetest/integration_test.go:245`,
`pkg/statistics/handle/usage/index_usage.go:59-62` plus
`pkg/statistics/handle/usage/index_usage_integration_test.go:29`, and
`pkg/sessionctx/variable/mock_globalaccessor.go:23-130` plus
`pkg/sessionctx/variable/mock_globalaccessor_test.go:26`. Live planner
partitioning/receivers, index-usage collection/GC/workers, SessionVars hooks,
context cancellation, SQL error/OpenCensus cleanup, and full integration remain
external. The regenerated ledgers are 1,978/388/24/0 production and
15,655/776/142/12 test/support obligations; the consolidated 12-job workspace,
Clippy, formatting, parser, plan, ledger, and domain gates pass. Wave 121 is the
next parallel source queue.

Wave 121 adds three disjoint source-backed leaves: `tidb-planner::physical_exchange_sender`
preserves `ExchangeSender` identity/root offset zero and ExplainInfo exchange
labels, compression names/fallback, hash-column text, ordered task IDs, and
uint64 `stream_count`; `tidb-stats::stats_table_snapshot` preserves the
`AssertTableEqual` realtime/modify counts, column/index cardinality, per-ID
item/payload/nil shape, and existence bytes; and
`tidb-exec::vec_group_checker_int` preserves integer/NULL group boundaries,
cross-chunk first-group continuity, offsets/count, cursor ranges,
exhaustion/reset, and the non-empty-chunk error. Exact anchors are
`pkg/planner/core/operator/physicalop/physical_exchange_sender.go:222` plus
`pkg/planner/core/casetest/mpp/mpp_test.go:78`,
`pkg/statistics/handle/internal/testutil.go:25-55` plus
`pkg/statistics/handle/handletest/statstest/stats_test.go:307`, and
`pkg/executor/internal/vecgroupchecker/vec_group_checker.go:80-151,524-564`
plus `pkg/executor/internal/vecgroupchecker/vec_group_checker_test.go:141`.
MPP runtime, statistics table/payload/storage lifecycle, expression/chunk and
codec evaluation, collations, non-integer/vector groups, and stream aggregation
remain external. The regenerated ledgers are 1,975/391/24/0 production and
15,652/779/142/12 test/support obligations; the consolidated 12-job workspace,
Clippy, formatting, parser, plan, ledger, and domain gates pass. Wave 122 is the
next parallel source queue.

Wave 122 adds three disjoint source-backed leaves: `tidb-planner::physical_window`
preserves Window plan identity, initialization offset, inherited uint64
fine-grained-shuffle stream-count clone state, and the optional ExplainInfo
suffix; `tidb-exec::concurrent_entry_map` preserves 320-shard routing,
lock-protected prepend chains, lookup/snapshot iteration, length/empty, row
identity, and portable accounting; and `tidb-stats::stats_cache_inner` preserves
the eleven-method cache interface (`Get`, `Put`, `Del`, `Cost`, `Values`, `Len`,
`Copy`, `SetCapacity`, `Close`, `TriggerEvict`, and `WaitForAsyncUpdates`) over
opaque values. Exact anchors are
`pkg/planner/core/operator/physicalop/physical_window.go:480` plus
`pkg/planner/core/plan_test.go:681`, `pkg/executor/join/concurrent_map.go:20-79`
plus `pkg/executor/join/concurrent_map_test.go:27,70`, and
`pkg/statistics/handle/cache/internal/inner.go:18-50` plus
`pkg/statistics/handle/cache/internal/lfu/lfu_cache_test.go:49`. PhysicalSort
sharing, MPP runtime, the Go memory-map ABI/constants and hash-join trackers,
LFU admission/eviction/async/metrics, and full statistics storage lifecycle
remain external. The regenerated ledgers are 1,972/394/24/0 production and
15,648/783/142/12 test/support obligations; the consolidated 12-job workspace,
Clippy, formatting, parser, plan, ledger, and domain gates pass. Wave 123 is the
next parallel source queue.

Wave 123 exercises the new fast-lane/integrate-lane split. Three feature lanes
froze `tidb-planner::physical_sort`, the six-test
`tidb-exec::join_table_meta` source family, and `tidb-stats::StatsPool` using
focused source-backed checks without launching private workspace builds. The
evidence steward then added exact manifest ownership, regenerated both ledgers,
and ran one reused-target workspace batch. The resulting ledgers are
1,969/397/24/0 production and 15,639/792/142/12 test/support obligations. The
workspace tests passed; after strict Clippy requested `div_ceil`, the focused
six-test executor regression and full workspace Clippy passed, with formatting
and every static ledger/parser/plan/domain gate. Typed planner/runtime wiring,
join encoding/execution, live FieldType/chunk/codec behavior, and concrete
pool/session lifecycle remain future source-family owners. Wave 124 is next.

Wave 124 demonstrates higher-yield family selection under the two-speed loop.
Feature lanes froze `physical_topn`, the ordered `parallel_apply.go` state
machine with seven exact tests, and the complete generic bounded heap with
seven exact tests using only focused checks. Steward review found and removed
four narrowing mistakes before the shared gate: omitted panic/kill anchors,
missing idle partial flushing, conflated ordinary/normalized plan text, and a
reversed-argument comparator assumption instead of Go's direct negation. The
steward then serialized manifest ownership, regenerated ledgers at
1,966/400/24/0 production and 15,623/808/142/12 test/support, and ran one
reused-target workspace batch. Workspace tests, focused nine-test post-Clippy heap
tests, strict Clippy, formatting, and all static ledger/parser/plan/domain
gates pass. Wave 125 is next.

Wave 125 freezes three complete source-file owners:
`tidb-planner::physical_table_reader`, `tidb-exec::statement_rows_reader`, and
`tidb-distsql::distsql_runtime`. They preserve TableReader request/store/clone/
explain/memory contracts, rows-reader buffer/pull/EOF/error/close lifecycle,
and DAG/MPP/ANALYZE/CHECKSUM metadata plus chunk/TiFlash/KV-counter policy.
Together they own 12 exact original tests: two planner, three statement
summary, and seven `pkg/distsql/distsql_test.go` anchors. The two planner
evidence anchors were added to the checked test-domain manifest before regenerating the ledgers;
the current generated counts are 1,963/403/24/0 production and
15,611/820/142/12 test/support obligations. The accompanying throughput batch
adds checked-ledger family candidates, atomic source/test claims, tiered gate
entrypoints, and four `tidb-exec` test shards. That reduces `tidb-exec` test
targets from 103 to 10 and workspace test targets from 361 to 268 without
losing any of the 103 source files or normalized 620-test union. Full workspace
tests, strict Clippy, focused DistSQL tests, formatting, claim/queue tests,
parser dependency isolation, and every static ledger/parser/plan/domain gate
pass against the reused 12-job target.

Update note (2026-07-16): Wave-41 adds three disjoint source-backed leaves.
`tidb-stats::AnalyzeTableId` owns deterministic table/partition statistics-ID
selection, formatting, and optional identity equality. The planner's
`cardinality::out_of_range` module owns the pure equality/full-NDV
out-of-range arithmetic, including deletion fallback and smoothing. The
executor's `session_status` module owns the atomic status bitfield and its
autocommit/transaction/cursor masks. The static ledgers are now
2,193/173/24/0 production and 16,016/415/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). The consolidated workspace/Clippy gate is
pending; statistics lifecycle, planner/session integration, original Go parity,
and deployable bootstrap remain open.

Update note (2026-07-16): Wave-42 adds three disjoint source-backed leaves.
`tidb-stats::RowEstimate` owns source-compatible default/min/max arithmetic,
clamp ordering, and skew-ratio bounds. `tidb-planner::cardinality::uniform`
ports normalized uniform equality estimation with TopN/empty-histogram,
modification/deletion, and risk-skew branches. `tidb-exec::removed_sysvar`
owns the complete 13-entry removed-variable registry and exact lookup reasons.
The static ledgers are now 2,192/174/24/0 production and
16,013/418/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). The consolidated workspace/Clippy gate is
pending; histogram/TopN/session integration, sysvar dispatch, original Go
parity, and deployable bootstrap remain open.

Update note (2026-07-16): Wave-43 adds three disjoint source-owned leaves:
`tidb-planner::schema_table_key` for normalized schema/table and alias identity,
`tidb-stats::stats_version` for version/analyzed metadata predicates, and
`tidb-exec::option_values` for source-compatible ON/OFF and true/false option
conversions. Each lane has a focused source-backed test and a single-source
ownership row; parser scope, statistics/session lifecycle, and full Go parity
remain outside the bounded ring. The regenerated ledgers are 2,191/175/24/0
production and 16,009/422/142/12 test/support obligations. Run the one
consolidated workspace/Clippy/parser/plan/dependency gate after integration.

Update note (2026-07-16): Wave-44 adds three bounded source-owned leaves:
planner implementation cost arithmetic, statistics column/index existence
metadata, and session StatementContext push-down flag synthesis. The overlap
on `pkg/sessionctx/stmtctx/stmtctx.go` is intentionally merged into its
existing source owner; each test anchor has an exact manifest claim where the
file is split. The regenerated ledgers are 2,189/177/24/0 production and
16,005/426/142/12 test/support obligations. Run the one consolidated
workspace/Clippy/parser/plan/dependency gate after integration.

Update note (2026-07-16): Wave-47 adds three bounded source-owned leaves:
planner physical-property classification, statistics overlap geometry, and
session used-statistics slow-log formatting. The existing `stmtctx.go` and
`histogram.go` source rows were extended rather than duplicated; the overlap
geometry test artifact is recorded beside its existing planner out-of-range
anchor to preserve one exact test-owner row. The regenerated ledgers are
2,184/182/24/0 production and 15,995/436/142/12 test/support obligations. Run
the one consolidated workspace/Clippy/parser/plan/dependency gate after
integration.

Update note (2026-07-16): Wave-45 adds three bounded source-owned leaves:
statistics scalar geometry, planner task-kind metadata, and session context-ID
allocation. The session test file is split, so the exact `TestStmtCtxID` row is
added to the checked domain manifest. The regenerated ledgers are
2,186/180/24/0 production and 16,002/429/142/12 test/support obligations. Run
the one consolidated workspace/Clippy/parser/plan/dependency gate after
integration.

Update note (2026-07-16): Wave-46 adds three bounded source-owned leaves:
planner ORDER BY item metadata, statistics measured memory usage, and the
session statement reference/freeze counter. Existing `stmtctx.go` and
`table.go` source rows were extended rather than duplicated; the refcount test
is a supplemental source-contract overlay because no dedicated Go transition
test exists. The regenerated ledgers are 2,185/181/24/0 production and
15,997/434/142/12 test/support obligations. Run the one consolidated
workspace/Clippy/parser/plan/dependency gate after integration.

Update note (2026-07-16): Wave-40 adds three source-backed leaves in parallel.
`tidb-stats::status::StatsLoadedStatus::status_to_string` preserves the exact
source status labels and uninitialized precedence. The planner's
`cross_estimation` module ports expected-count range selection over normalized
opaque endpoints, including ascending/descending and full-scan boundaries.
`tidb-exec::sequence_state` ports numeric latest-value updates, missing lookup,
snapshot copying, and `maps.Copy`-style merges. The static ledgers are now
2,195/171/24/0 production and 16,020/411/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). The consolidated workspace/Clippy gate is
pending; SQL sequence execution, statistics/session integration, original Go
parity, and deployable bootstrap remain open.

Update note (2026-07-16): Wave-38 closes three disjoint source-owned queues in
one workspace cycle. `tidb-stats::status` ports statistics loading-status
metadata and its exact predicate ordering; `tidb-planner::cost_factors` ports
all sixteen aggregate-factor entries, thresholds, and default lookup; and
`tidb-exec::retry_info` ports deterministic retry queues, offsets, cleanup, and
lifecycle fields. Their focused tests and source/test evidence are recorded
without synthetic Go anchors. The regenerated ledgers are 2,201/165/24/0
production and 16,025/406/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Full workspace tests, strict Clippy,
formatting, ledger, parser, plan, and dependency gates pass with 12 jobs;
retry orchestration, statistics handles, full cost model, original Go parity,
and deployable bootstrap remain open.

Update note (2026-07-16): Wave-39 closes three small source-owned queues in
parallel. `tidb-stats::constants` ports the exported TopN/histogram defaults;
`tidb-planner::cardinality::index_range_policy` ports the inclusive full-range
including-NULLs predicate and partial/MV gates over normalized bounds; and
`tidb-exec::reserved_row_id` ports the complete base/max reservation counter.
Focused tests use the original `TestCanSkipIndexEstimation` and
`TestReservedRowIDAlloc` anchors; the constants lane records a real statistics
test-file anchor because Go has no dedicated constants test. The regenerated
ledgers are 2,199/167/24/0 production and 16,022/409/142/12 test/support
obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED). Full workspace tests, strict
Clippy, formatting, ledger, parser, plan, and dependency gates remain the
required consolidated gate; statistics builders, index estimators, storage
reservation, original Go parity, and deployable bootstrap remain open.

Update note (2026-07-16): integrated the next three source-backed leaves in
one static-first parallel wave. `tidb-server::AuthExchange` now preserves
source-shaped `AuthSwitchRequest`/`AuthMoreData` payloads, sequence framing,
and explicit malformed/trailing-byte errors without claiming password,
plugin-registry, TLS, or transport-flush behavior. `tidb-distsql::RawChBlockChunk`
validates the native CHBlock envelope and borrowed payload/row metadata while
leaving typed ClickHouse Datum decoding explicitly unsupported. The planner
owns a narrow `join_condition` classifier for qualified/unqualified cross-side
`=`/`<=>` ON predicates plus USING binding and explicit ambiguity/unsupported
outcomes; it is not the general join executor. The workspace test/Clippy gate
and static ledger/parser/plan/dependency checks pass. Current ledgers are
2,245/121/24/0 production and 16,095/336/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Remaining gaps are plugin authentication
and password/TLS transport, typed CHBlock/temporal/decimal/JSON/enum/set/vector
Datum semantics, full planner residual condition execution and nested
FullSchema propagation, TiKV/MPP RPC, session/bootstrap, and real-cluster
validation.

Update note (2026-07-16): integrated the next three static-first parity
leaves. `tidb-server::AuthPluginRegistry` now mirrors Go's custom-plugin
metadata validation order (empty/duplicate/reserved names and required
callbacks) and selects LDAP or `RequiredClientSidePlugin` client names without
executing callbacks, hashing passwords, or doing TLS/I/O. `RawValue::decode_datum`
and the DistSQL default-row consumer now decode the source-proven scalar tag
subset with exact payload consumption; duration, JSON, vector, enum/set, and
schema-aware temporal conversion remain explicit errors. `tidb-planner::residual_condition`
retains residual AND/OR/NOT shape and syntax-only scalar/function metadata with
deferred typed evaluation rather than guessing a value or hash key. The
workspace tests pass for all crates; strict Clippy, formatting/diff, and the
static ledger/parser/plan/dependency gates pass after correcting evidence
ownership overlaps. Current ledgers are 2,242/124/24/0 production and
16,091/340/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Remaining gaps are callback execution,
password verification/user store/TLS transport, typed temporal/Duration,
JSON/enum/set/vector/CHBlock semantics, general residual predicate typing and
join execution, FullSchema propagation, TiKV/MPP RPC, session/bootstrap, and
real-cluster validation.

Update note (2026-07-16): integrated the next three static-first boundaries.
`tidb-server::SecureTransportPolicy` now mirrors Go's `RequireSecureTransport`
admission decision: plaintext TCP is rejected when enabled, while Unix sockets
and transport-owned direct/gateway-secure assertions are allowed; the leaf does
not perform TLS, certificate, gateway-attribute, or password validation.
`tidb-codec::RawDuration` preserves the signed nanosecond `EncodeInt` payload
and DecodeOne's `MaxFsp=6` result with exact remainders, while SQL range/FSP and
warning policy remain typed-time work. `tidb-planner::condition_binding`
resolves known residual column paths into source-ordered `FullSchema` indices
and marks IN/CASE/subquery and other dedicated shapes opaque for a future typed
executor. The full workspace test batch and strict Clippy pass; static
ledger/parser/plan/dependency checks pass after regenerating the inventories.
Current ledgers are 2,240/126/24/0 production and 16,088/343/142/12
test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED). Remaining gaps
are TLS handshake/certificates, password verification/user store, SQL duration
semantics, decimal/JSON/enum/set/vector/CHBlock codecs, typed residual
evaluation and join execution, full session/error context, TiKV/MPP RPC,
bootstrap, and real-cluster validation.

Update note (2026-07-16): integrated the next three source-backed leaves in
one batched workspace cycle. `tidb-server::AuthChallenge` and
`AuthSessionAttempt` now preserve the session-facing identity/plugin/auth-byte/
salt envelope, enforce the Go `auth_socket` Unix-only precondition, and stop at
`PendingVerification` or an explicit pre-verification rejection; privilege
lookup, password/plugin callbacks, account locking, and authenticated-session
publication remain outside the boundary. `tidb-codec::DecimalWireMetadata`
inspects the exact precision/scale header, packed payload length, and remainder
without materializing or rounding the coefficient; short buffers now return a
typed framing error. `tidb-planner::predicate_partition` routes bound residual
predicates conservatively to left/right/join/deferred candidates and requires a
typed effects check for functions or opaque AST shapes, without selecting a
join algorithm or pushing values. Workspace tests and strict Clippy pass after
fixing the owned-auth const transition and decimal test/framing edges; static
ledger/parser/plan/dependency checks pass. Current ledgers are
2,238/128/24/0 production and 16,085/346/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Remaining gaps are password verification,
TLS/certificates, SQL duration/decimal/JSON/enum/set/vector/CHBlock semantics,
typed residual evaluation, join algorithms, full session/error context,
TiKV/MPP RPC, bootstrap, and real-cluster validation.

Update note (2026-07-16): integrated the next three disjoint leaves in one
batched workspace cycle. `tidb-codec::column` and `tidb-distsql` now preserve
raw fixed/variable TypeChunk and default-row framing with null bitmaps,
offsets, and exact remainders while typed Datum/FieldType/CHBlock semantics
remain explicit. `tidb-exec::result_schema_projection` ports direct-column,
wildcard, alias, nullability, and hidden-USING rejection metadata over
resolved join output but is not wired into automatic row production yet.
`RenderedExecError` carries caller-rendered bytes plus optional published
status into sequence-one protocol-41/legacy ERR framing without copying
warnings. Full workspace tests and strict Clippy pass. The regenerated ledgers
are 2,253/113/24/0 production and 16,104/327/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Typed codecs, automatic
projection wiring, general ON/USING typing, full ErrCtx/session ownership,
authentication/TLS/compression, TiKV/RPC, and deployable bootstrap remain open.

Update note (2026-07-16): integrated the next parallel source-first wave in
one batched workspace cycle. `tidb-codec::column` now derives Go `getFixedLen`
physical layouts from `FieldType` (FLOAT=4, scalar/time=8, NEWDECIMAL=40,
otherwise variable); `tidb-distsql::KvRequestBuilder` preserves opaque
`Request.Data` bytes plus ordered TiFlash partition IDs/ranges before any
protobuf, region, or RPC owner exists; and the automatic catalog path routes
direct columns, aliases, and qualified/bare wildcards through the existing
`Database::project_row` owner. Hidden right-side USING provenance is retained
so the missing FullSchema mapping fails explicitly. Full workspace tests and
strict Clippy pass, and all static ledger/parser/plan/dependency gates pass.
The regenerated ledgers are 2,253/113/24/0 production and
16,104/327/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Typed Datum/CHBlock codecs, typed
expression/FullSchema projection mappings, general ON/USING typing, full
ErrCtx/session ownership, authentication/TLS/compression, TiKV/RPC, and
deployable bootstrap remain open.

Update note (2026-07-16): integrated the next typed execution/context wave in
one batched workspace cycle. `tidb-codec::decode_column_datums` now converts
the source-proven native scalar subset (signed/unsigned 64-bit, float32/64,
and byte-preserving variable strings) while retaining nulls/remainders and
rejecting temporal, decimal, JSON, enum/set, vector, bit, and unknown types
explicitly. `tidb-exec::join_predicate` binds only direct cross-side equality
for ON/USING, shares NULL-as-non-match semantics, and leaves compound or
ambiguous predicates to the general evaluator. Statement status now records
`exec_success`; `Session::render_exec_error` and server framing attach that
exact failed status without copying warnings into ERR payloads. Full workspace
tests and strict Clippy pass, all static ledger/parser/plan/dependency gates
pass, and current ledgers are 2,251/115/24/0 production and
16,103/328/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Temporal/decimal/JSON/vector Datum
codecs, native CHBlock, general ON/USING typing and join algorithms, dynamic
warning/error context, authentication/TLS/compression, TiKV/RPC, and
deployable bootstrap remain open.

Update note (2026-07-16): integrated the next three source-backed leaves in
one batched workspace cycle. `tidb-datatype::PackedTime` and
`tidb-codec::temporal` preserve Go's packed temporal integer layout and
big-endian codec boundary without claiming SQL calendar/FSP/timezone or
Duration semantics. `tidb-proto` now owns the exact CoprocessorRequest field
projection, and `tidb-distsql::CoprocessorRequestEnvelope` preserves opaque
request bytes, context, and ordered partition ranges before region splitting
or RPC. `tidb-server::AuthHandshake` makes the initial response, SSLRequest,
TLS-established, and authentication-pending phases explicit, retaining raw
auth bytes and classifying plugin fallback/switch/defer without performing TLS,
password verification, or user lookup. The full workspace test batch passed;
strict Clippy passed after boxing the large pending-auth phase payload, and
formatting/diff plus static ledger/parser/plan/dependency gates pass. Current
ledgers are 2,248/118/24/0 production and 16,098/333/142/12 test/support
obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED). Temporal SQL/Duration,
decimal/JSON/enum/set/vector Datum and native CHBlock codecs, typed
expression/FullSchema projection, general planner ON/USING typing, full
session/error-context and authentication/TLS/user-store lifecycle, region/RPC,
TiKV, and deployable bootstrap remain open.

Update note (2026-07-16): integrated the next parallel codec/transport/planner
wave and closed one combined workspace gate. `tidb-codec::json` now ports the
source-defined BinaryJSON type/value boundary, including primitive, container,
opaque, and duration payload lengths with exact remainders; `RawValue::json`
now accepts the JSON value tag while malformed/unknown physical payloads stay
explicit errors. `tidb-proto` and `tidb-distsql::RegionTaskEnvelope` preserve
the exact StoreBatchTask region epoch, peer, ordered ranges, task ID, versioned
ranges, and bucket-version fields before lookup/retry/endpoint/RPC ownership.
`tidb-exec::result_schema_join_output` now retains source-ordered FullSchema
fields and maps hidden right-side USING fields to canonical visible output
indices without widening executor rows. Full workspace tests and strict
Clippy pass after fixing borrowed-JSON lifetimes and source-test cardinality;
formatting/diff plus all ledger/parser/plan/dependency gates pass. Current
ledgers are 2,248/118/24/0 production and 16,098/333/142/12 test/support
obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED). Full JSON semantics, SQL
temporal/Duration, decimal/enum/set/vector Datum and native CHBlock codecs,
typed expressions/nested FullSchema execution, general ON/USING typing, full
session/error-context and authentication/TLS/user-store lifecycle, region/RPC
execution, TiKV, and deployable bootstrap remain open.
