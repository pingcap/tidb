# TiDB Rust Rewrite: Design

- Author(s): qiliu
- Discussion PR: TBD
- Tracking Issue: TBD

## Abstract

This document proposes a design for rewriting the TiDB SQL layer in Rust. It is grounded in a study of the current codebase (~950k lines of non-test Go across `pkg/`, `br/`, `lightning/`, `dumpling/`, `cmd/`) and builds directly on the `hparser-integration` work: the hand-written, arena-allocated SQL parser that replaced the goyacc parser is the architectural prototype for the Rust parser, and its differential-testing methodology is the verification model for the whole migration.

The core strategic insight is that **TiDB SQL nodes are stateless**. Multiple TiDB versions already coexist against the same TiKV/PD cluster during rolling upgrades. A Rust TiDB (`tidb-rs`) can therefore join a live cluster as an additional SQL node and take traffic incrementally — no FFI embedding, no big-bang cutover. Migration happens at the cluster topology level, gated by differential testing, not inside a hybrid binary.

## Source-transit contract

The rewrite transits behavior from Go before it redesigns implementation. For
each leaf, the owning Go file and symbol are the contract: preserve branch
ordering, constants, error text, hash/equality framing, wire/SQL text, and
edge-case arithmetic first; then expose a Rust API that is dependency-closed
and easy to compose. Every leaf carries an exact original-test anchor and
source/test evidence row. The generated ledgers keep one owner per Go source
file and per test obligation, so fixture coverage cannot be mistaken for test
parity and parallel agents cannot silently claim the same behavior.

Pure policy, metadata, codec, and formatting leaves are migrated first. Live
session, transaction, catalog, storage, RPC, DDL, and cluster behavior is
attached only after its source contract is covered by a focused Rust test and
the corresponding integration seam exists. This is a staged source transit,
not a second implementation invented from the design document; the Go suite
remains the behavioral oracle until the Rust suite and runtime replace it.

## Implementation status (2026-07-18)

This document defines the target architecture; the following snapshot records
what is actually implemented so agents do not confuse a target crate with a
finished subsystem.

| Surface | Current state | The remaining boundary |
|---|---|---|
| Workspace | An in-repo `rust/` Cargo workspace with lexer, AST, parser, proto, protocol framing/command/result/charset plus ERR-packet primitives, distsql context/request metadata plus serial/channel/response-event iterators, planner, dependency-closed `tidb-stats` CMSketch/TopN/FMSketch geometry and loading-status metadata, datatype, codec, transaction-key, expression, seed-executor status/schema/result bridge leaves, and `tidb-server` connection-dispatch/handshake/listener/accept-loop lifecycle crates, plus parser/planner/result/transaction differential packages | Deployable bootstrap and the remaining catalog, session, RPC, DDL, statistics integration/lifecycle, and cluster services |
| Parser | 51,488 exact accepted single-statement restores, 10 exact multi-statement restores, 99 explicit dual rejections, and one pinned Go restore failure; 520 parser unit tests pass; geometry aliases, FieldType compatibility, and duplicate-COLLATE ordering rows are source-owned | Close the remaining source/test ledger and keep the pinned Go failure separate from Rust regressions |
| Planner | Narrow source-owned pseudo-cardinality/equality/less/between/range and exponential-backoff leaves plus a typed physical-plan metadata tree and planner differential package | The full logical/physical rule pipeline, session/statistics/catalog-aware `pseudoSelectivity`, cost model, hints, bindings, SQL normalization/digest, and plan-digest ring |
| Result/expression | Source-shaped scalar leaves, bounded table-less/single-table/multi-relation ResultField binding, planner-owned LEFT/INNER/CROSS join output metadata (including USING order/coalescing and nullable-side declarations), bounded FullSchema-to-visible USING indices, direct-column/wildcard/alias projection metadata wired through the automatic row owner, direct cross-side equality ON/USING with NULL non-match semantics, a planner-owned `ON`/`USING` equality classifier, explicit residual `AND`/`OR`/`NOT` shape metadata with deferred typed evaluation, deferred residual column bindings into `FullSchema`, conservative left/right/join/deferred predicate dependency routing with typed-safety gates, native scalar/default-row Datum decoding, and static Go-backed query/expression rings | General planner ON/USING predicate typing and join algorithms, typed expression projection and nested/FullSchema execution mappings, full temporal SQL semantics and Duration/session policy, decimal/JSON/enum/set/vector Datum and native CHBlock codecs, full FieldType/session/collation context, vectorized execution, and real-cluster shadow traffic |
| Transaction/storage | Portable key, handle, version, iterator, and request-tag contracts; a bounded plaintext PD gRPC client with discovered membership, foreground refresh, and role-aware direct-endpoint failover; API-v1 region loading; exact nested NotLeader/EpochNotMatch payloads; one RegionCache topology/recovery authority; source-shaped per-region backoff; failed-task-only 1:N retry/rebuild; lazy address-keyed TiKV Coprocessor RPC; and same-process live PD/TiKV movement proof | Background PD/store health, router service, TLS/forwarding, generic TiKV connection-failure retry, locks/MVCC, production cache concurrency/TTL, 2PC, async commit, 1PC, full table/DAG lowering, and COM_QUERY integration |
| Server/cluster | A local connected seam now frames uncompressed `COM_QUERY` packets, decodes the Go-shaped command byte, dispatches COM_QUERY/PING/QUIT through `tidb-server::Connection`, parses SQL, executes through the shared seed session, and emits bounded metadata/row/EOF text-result packets; adapted Go-shaped ResultFields feed that sequence, typed integer/float/decimal/byte scalar formatting and the native scalar/default-row chunk Datum subset are connected, `tidb-exec` derives table-less, single-table, and bounded multi-relation catalog metadata from declared `ColumnType`s and attaches statement status including `exec_success` to the shared Session, the automatic path now proves bare-wildcard catalog-backed INNER/CROSS/LEFT/USING metadata and rows including null extension and coalesced order, direct-column/alias/wildcard projection metadata now crosses the same `Database::project_row` owner, direct cross-side equality ON/USING predicates share NULL non-match semantics, `tidb-exec` exposes planner-owned bounded join output metadata and a source-shaped executor→protocol error-kind adapter, `tidb-protocol::ResultEncoder` ports registered charset precedence including GBK and source-shaped ERR payload order plus typed error-kind→MySQL errno/SQLSTATE conversion, `tidb-server::error_response` attaches caller-rendered errors and optional published status to sequence-one ERR frames without guessing context, `tidb-codec` validates raw default/TypeChunk framing, FieldType physical layouts, native scalar columns, source-proven default-row Datum tags, fixed duration nanosecond payloads with DecodeOne MaxFsp metadata, decimal precision/scale/length metadata with exact remainder handling, BinaryJSON physical boundaries, the exact packed-temporal integer boundary, and explicit raw native CHBlock framing while keeping unsupported SQL semantics explicit, `tidb-proto` owns exact SelectResponse/StreamResponse/CoprocessorRequest/StoreBatchTask wire projections, and `tidb-distsql` validates raw SelectResponse/Chunk and StreamResponse envelopes plus opaque KV/coprocessor/CHBlock request metadata; `tidb-distsql` owns serial/channel response iteration plus ordered response events, and `tidb-server` owns source-shaped initial-handshake parsing, raw auth exchange envelopes, validated custom auth-plugin metadata/client-plugin selection, JWT compact-shape/retry/JWKS-load admission, opaque session-auth challenge/pending-verification state, explicit secure-transport admission policy, SSL/TLS/auth-plugin negotiation phases, idempotent TCP listener lifecycle, and a generic injected accept loop | General planner ON/USING typing and typed expression/nested FullSchema projection mappings, full SQL duration parsing/range/warning policy, decimal/enum/set/vector Datum and native CHBlock semantics beyond opaque framing, dynamic Go ErrCtx/warning policy, full session charset/encoder lifecycle, TLS handshake/certificate and password verification/user store, JWT RSA/JWK verification, filesystem/network refresh, claims decoding, compressed protocol, connection admission/session lifecycle beyond policy, Unix sockets/PROXY handling, distsql/MPP execution beyond the bounded PD/TiKV unary route, schema lease/MDL, DDL ownership, stats, bootstrap, and mixed-cluster routing |

Campaign `2026-07-read-path-07` adds the first bounded connected read path from
a validated TiKV table/index scan to an exact tipb DAG, through an ordered
table/index reader, into an exact TiKV unary request/response envelope. This is
an RPC-ready serialization and ownership seam, not live RPC: PD/RegionCache,
sockets/gRPC, lock resolution, retry/backoff/cancellation, real ranges, general
DAG trees, production executor/chunk wiring, and RealTiKV validation remain
open. The campaign's three slices stay `PARTIAL` after cross-review repaired
split index authority, late `RequiredRows` truncation, and unknown TiDB request
origin.

### Campaign 09 read-path runtime state

Campaign `2026-07-read-path-09` replaces that envelope's fake network boundary
with a real `tikvpb.Tikv/Coprocessor` gRPC socket leaf. The address-keyed client
lazily opens and reuses one channel, supports address/version and terminal
close, preserves typed timeout/connection failures, and accepts the
client-go-sized response limit. Its raw protobuf codec preserves all request
wire bytes except top-level field 1, which is removed and replaced exactly once
with the selected typed region context; response bytes are returned unchanged.

The routing side is a source-shaped, fail-closed single-region cache, leader
selector, and request sender backed by an injected region loader. A pinned TiKV
v8.5.6 playground test has crossed the existing DistSQL/direct-unary seam and
reached the real Coprocessor method using source-derived static topology. This
proves live unary transport and the bounded routing seam, not table reads or
product parity. A concrete PD client, retry/backoff, lock resolution, TLS,
follower/stale/proxy reads, multi-region dispatch, table semantics, and
`COM_QUERY` integration remain open.

### Campaign 10 PD-backed read-route state

Campaign `2026-07-read-path-10` is integrated. It removes Campaign 09's
shell-fabricated topology entirely: Rust receives one plaintext PD address,
bootstraps the nonzero cluster identity through `GetMembers`, resolves regions
and every referenced store through the exact pinned `GetRegion`/`GetStore`
gRPC methods, decodes API-v1 memcomparable boundaries, walks contiguous
half-open regions, selects the PD leader, and lazily reaches TiKV through the
existing Coprocessor client. The RegionCache is now the sole authority for
cluster ID, epoch, peer role/witness state, store, and address.

The source-derived tests preserve removed-store filtering, raw unknown peer
roles, exact-version invalidation, cache reuse, two-region ordered dispatch,
strict progress, and rejection of unsupported request shapes before PD I/O.
The pinned TiUP proof receives only `C10_PD_ADDR`; teardown proves its PD and
TiKV endpoints, dynamic owned processes, registry row, and tag directory are
gone. The old Python/HTTP topology runner, static route constructor, and
duplicate request sender were deleted. This is still a bounded one-endpoint,
one-attempt leader-read path: discovery refresh/failover, TLS/forwarding,
retry/backoff, locks, production multi-region resilience, general table/DAG
lowering, and COM_QUERY remain later campaigns.

### Campaign 11 movement-recovery state

Campaign `2026-07-read-path-11` replaces Campaign 10's terminal movement
boundary without introducing a second topology authority. Commits
`d3060d12ed`, `1ab75ec3ab`, and `6f54db744a` integrated foreground PD member
refresh, exact region-error/cache recovery, and response-owned DistSQL retry;
`58c3ea76f8` hardened their cross-slice contracts against pinned client-go.
The runtime revalidates a reachable old PD leader, retains the last complete
same-cluster member snapshot, hydrates returned epoch siblings through PD,
shares one bounded backoff budget across resend and outer rebuild, and splices
only the failed task's remaining ranges while leaving later prepared tasks
untouched.

The ignored-by-default owner test was executed through
`rust/scripts/run-campaign11-realtikv.sh` against three PD and three TiKV
processes. One retained Rust process/client/cache passed:

    Campaign 11 movement proof passed: PD http://127.0.0.1:26379 -> http://127.0.0.1:26382; TiKV 127.0.0.1:44162 -> 127.0.0.1:44161

Strict teardown removed the run-owned processes, TiUP registry/tag data, phase
directory, and endpoints. The official 12-job integration gate issued
`integration_receipt 3`; the campaign is integrated and all three
receipt-backed claims are released as `partial`. Background health and router
service, TLS/forwarding, generic TiKV
connection-failure retry, locks/MVCC, cache TTL/concurrency, active in-flight
RPC cancellation, and commit protocols remain explicit gaps.

The checked source/test totals and campaign queue are generated into
`rust/STATUS.md` from the authoritative ledgers and manifests. Do not copy
those counters into this design again: they change after every integrated
campaign and stale snapshots have already caused misleading progress reports.
They are source-ownership states, not product-parity percentages.

Completed Waves 32-131 now include source-owned rowcodec framing, typed ErrCtx
group policy, DDL affinity metadata, transaction/warning/partition metadata,
MVCC/isolation state, reusable statistics arithmetic and version predicates,
planner range/uniform/cardinality helpers plus normalized schema/table keys and
implementation-cost arithmetic, statistics existence metadata, and session
status, removed-variable, option-value, push-down, hash/equality, correlation,
SET_VAR restore, planner-context, index-usage, and cursor-tracker policy
leaves, plus task-stack, analyze-job, context-key, pattern, async-load,
status-registry, string-writer, datum-map-cache, process-info, expr-iterator,
need-analyze-table, nextgen-readonly-vars, explore-mark, parse-auto-analyze-
ratio, slow-log-threshold, group-expr, auto-analysis-window, slow-log-rules,
column-length, priority-calculator, session-token-timing, plan-cache-constants,
dynamic-partition-helpers, advisory-lock-state, index-advisor-model,
priority-heap, and txn-running-state policy leaves.
These are dependency-closed fragments with focused source-backed tests; they do
not imply full planner/statistics/session integration. Wave 65 adds
`tidb-planner::rule_type`, `tidb-stats::analysis_interval`, and
`tidb-exec::txn_summary`; the remaining ledger and runtime boundaries stay
explicit.

## Wave-32 progress (2026-07-16)

- [x] Added `tidb-codec::RowLayout` for new-row headers, small/large ID and
  offset metadata, sorted lookup/null-default decisions, value ranges, and
  checksum-trailer framing; added pure ErrCtx group levels/flags with
  ignore-over-warning precedence and Go statement defaults; and added
  deterministic DDL affinity-level normalization, stable group IDs, duplicate
  partition collapse, missing-partition validation, and pre-commit level
  rejection. Typed row encoding/decoding, schema/handles/checksum calculation,
  warning sinks/session wiring, TiKV/PD/catalog mutation, and DDL coordination
  remain open. Ledgers are 2,221/145/24/0 production and 16,057/374/142/12
  test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED).
Privilege-table/DNS ownership, password/TLS/user-store authentication, calendar
conversion, SQL warning/session policy, vectorized kernels, join execution,
session/bootstrap, TiKV/MPP, and real-cluster validation remain open.

## Wave-33 progress (2026-07-16)

- [x] Added the dependency-closed rowcodec encoder seam: opaque payload
  framing, non-null/null ID partition ordering, small/large metadata selection,
  compact little-endian integer widths, and append-buffer semantics. Typed
  Datum/schema/time-zone conversion, decoder reuse, checksum/handle policy, and
  tablecodec integration remain open.
- [x] Added bootstrap mode and gate metadata: Go-compatible bootstrap/upgrade/
  normal selection, SYSTEM-keyspace version admission, feature-gate outcomes,
  and the source phase ordering contract. KV/domain/DDL/system-table mutation,
  privilege loading, plugin callbacks, SQL-file I/O, background workers, and a
  deployable server remain open.
- [x] Added planner row-count arithmetic for normalized ranges and caller-owned
  column estimates, including point/interval boundary handling and partial
  index selectivity. Histogram/TopN lookup, stats-v1 enumeration, session and
  catalog integration, and full planner rule/cost ownership remain open.

The source/test ledgers and parser/plan snapshots are regenerated after each
wave; they are evidence of ownership and compatibility slices, not a claim of
TiDB parity. The current rewrite remains WIP and the original Go test suite has
not yet been reproduced in Rust. The Wave-33 snapshot was
2,218/148/24/0 production and 16,049/382/142/12 test/support obligations.

## Wave-34 progress (2026-07-16)

- [x] Added `tidb-txnkv::TxnScopeVar` for the exact global/local transaction
  scope pair while leaving configuration lookup, PD/oracle access, and session
  propagation external.
- [x] Added `tidb-exec::WarningPublication` as a borrowed, source-ordered view
  over statement warnings with protocol-sized total/error counts; mutable
  StaticWarnHandler behavior, IgnoreWarn/JSON/session attachment, and error
  rendering remain open.
- [x] Added DDL partition metadata validation and ADD-phase ordering: folded
  names, ADD/REORGANIZE collisions, ordered lookup/IDs,
  `Definitions`/`AddingDefinitions` staging, and
  `Initial → ReplicaOnly → Public`. Expressions, ID allocation, KV/PD/catalog
  mutation, and DDL workers remain external.

The Wave-34 static snapshot is 2,215/151/24/0 production and
16,045/386/142/12 test/support obligations. Workspace tests and strict Clippy
are the consolidated gate; Rust still does not reproduce the original Go test
suite or provide a deployable TiDB server.

## Wave-35 progress (2026-07-16)

- [x] Added raw rowcodec decoding over the existing `RowLayout`: typed
  missing/null/not-null lookup, borrowed opaque value ranges, compact signed
  and unsigned integer decoding, and malformed-width/layout errors. Typed
  Datum/schema/timezone/default/handle conversion remains open.
- [x] Added session isolation metadata: all four TiDB isolation enum values,
  source-compatible name/ordinal normalization and canonical readback, the
  storage-capability distinction, and the `Default → Set → Use → Default`
  one-shot transition/selection contract. Live session/sysvar/transaction and
  storage semantics remain open.
- [x] Added TiKV/unistore MVCC metadata: write/lock type bytes, write-CF
  timestamp/value framing, Badger user metadata, extra transaction-status keys,
  descending timestamp suffixes, and safe lock-header metadata. Storage/RPC,
  oracle, lock resolution, and transaction protocol behavior remain open.

The Wave-35 static snapshot is 2,208/158/24/0 production and
16,040/391/142/12 test/support obligations. The Rust workspace and strict
Clippy gate pass, but this remains a source-owned migration slice rather than
full TiDB parity.

## Wave-36 progress (2026-07-16)

- [x] Added `tidb-stats::cmsketch` for the dependency-closed CMSketch/TopN
  byte boundary: zero-seed Murmur3 hashing, row/width bucket geometry,
  wrapping counters, median/noise/default-value query behavior, equal-shape
  merge, and sorted TopN lookup/range counts. Datum encoding, analyze/sample
  selection, histograms, protobuf persistence, statistics handles, and
  session/debug context remain external.
- [x] Added `tidb-exec::nontransactional` for the source admission policy:
  autocommit/no-active-transaction, batch-DML compatibility, weak-read and
  snapshot gates, plus the admitted INSERT/REPLACE SELECT, UPDATE, and DELETE
  families. AST validation, shard selection, workers, metrics, failpoints,
  and error aggregation remain external.
- [x] Added `tidb-planner::range_detacher` for normalized CNF/DNF access/filter
  reconstruction with caller-supplied checker decisions, preserving source
  order and unusable-branch behavior. Expression typing, collation/session
  checks, endpoint construction, and the full ranger checker remain external.

The Wave-36 static snapshot is 2,205/161/24/0 production and
16,032/399/142/12 test/support obligations. `cargo test --workspace` and
strict workspace Clippy pass with 12 jobs; the rewrite remains WIP and does
not reproduce the original Go test suite or provide a deployable TiDB server.

## Wave-37 progress (2026-07-16)

- [x] Added `tidb-stats::fmsketch` for the already-hashed FM sketch boundary:
  source mask/hash-set admission, level transitions, NDV estimate, copy,
  merge, and memory shape. Datum/tablecodec hashing, sampler lifecycle,
  protobuf conversion, and binary persistence remain external.
- [x] Added `tidb-exec::txn_read_ts` for the `tx_read_ts` value contract:
  consume/peek/set transitions and used-plus-nonzero cleanup, leaving parsing,
  timestamp-oracle allocation, stale-read execution, and SnapshotInfoschema
  mutation to the session owner.
- [x] Added `tidb-planner::selectivity_greedy` for source `StatsNode` mask
  traversal, non-overlap selection, kind/ID ordering, and all six deterministic
  tie-breaks. Expression extraction, real statistics, and selectivity
  estimation remain external.

The Wave-37 static snapshot is 2,203/163/24/0 production and
16,028/403/142/12 test/support obligations. Workspace tests, strict Clippy,
formatting, ledger, parser, and plan gates pass with 12 jobs; the rewrite is
still WIP and the original Go suite/deployable server remain incomplete.

## Wave-38 progress (2026-07-16)

- [x] Added `tidb-stats::status::StatsLoadedStatus` for the source loading
  metadata value: zero/uninitialized state, `AllLoaded`/`AllEvicted`
  constructors, copy semantics, and the exact integer ordering used by the
  load-needed, essential, all-evicted, and full-load predicates. Histogram
  handles, storage reads, eviction workers, and live Column/Index lifecycle
  remain external.
- [x] Added `tidb-planner::cost_factors` for the source cost constants and
  aggregation-factor lookup, including all sixteen source map entries and the
  unknown-name default path. The full physical cost model, session overrides,
  and aggregate-name catalog remain explicit partial boundaries.
- [x] Added `tidb-exec::retry_info` for the dependency-closed deterministic
  retry metadata: auto-increment/auto-random replay queues, offset reset,
  dropped prepared-statement cleanup, and the `Retrying`/`LastRcReadTS` fields.
  Retry-loop orchestration, plan rebuild, transaction restart, and plan-cache
  cleanup remain owned by the future session/transaction layers.

The Wave-38 static snapshot is 2,201/165/24/0 production and
16,025/406/142/12 test/support obligations. Workspace tests, strict Clippy,
formatting, all ledgers, parser, plan, and dependency gates pass with 12 jobs;
the rewrite remains WIP and does not reproduce the original Go suite or
provide a deployable TiDB server.

## Wave-39 progress (2026-07-16)

- [x] Added `tidb-stats::constants` for the source exported statistics
  defaults (`DefaultTopNValue = 100` and `DefaultHistogramBuckets = 256`) as
  value-only constants. Builder allocation, configuration, and planner
  application remain external.
- [x] Added `tidb-planner::cardinality::index_range_policy` for the exact
  `[NULL, MaxValue]` full-range predicate, inclusive endpoint rules, matching
  composite widths, and partial/multi-valued index gates. Histogram/TopN row
  estimation, async statistics loading, and encoded Datum/ranger integration
  remain external.
- [x] Added `tidb-exec::reserved_row_id` for the source
  `ReservedRowIDAlloc` counter: reset, base-excluded/max-inclusive consumption,
  and `base >= max` exhaustion. Storage reservation, table mutation, and
  statement-context lifecycle remain external.

The Wave-39 static snapshot is 2,199/167/24/0 production and
16,022/409/142/12 test/support obligations. Workspace tests, strict Clippy,
formatting, all ledgers, parser, plan, and dependency gates pass with 12 jobs;
the rewrite remains WIP and original Go parity/deployable bootstrap remain
incomplete.

## Wave-40 progress (2026-07-16)

- [x] Added `tidb-stats::status::StatsLoadedStatus::status_to_string` with the
  exact source labels `unInitialized`, `allLoaded`, `allEvicted`, and
  `unknown`, including uninitialized precedence and unknown integer states.
- [x] Added `tidb-planner::cardinality::cross_estimation` for the pure
  expected-count range conversion: ascending/descending cumulative selection,
  unbounded sentinels, endpoint-exclusion inversion, collator preservation, and
  empty/full-scan boundaries. Histogram lookup, Datum/ranger construction, and
  statistics-aware callers remain external.
- [x] Added `tidb-exec::sequence_state` for numeric sequence latest-value
  updates, missing lookup, copied snapshots, and `maps.Copy`-style merges.
  SQL sequence execution/allocation, JSON envelopes, mutex/live-session
  ownership, and integration with session state remain external.

The Wave-40 static snapshot is 2,195/171/24/0 production and
16,020/411/142/12 test/support obligations. This is a source-owned migration
slice; the original Go test suite, full statistics/session/planner integration,
and a deployable TiDB bootstrap remain incomplete.

## Wave-41 progress (2026-07-16)

- [x] Added `tidb-stats::AnalyzeTableId` for source-compatible table versus
  partition statistics identity: physical statistics-ID selection, the
  non-partition sentinel, exact `partition => table` formatting, and value/
  optional-identity equality. Analyze scheduling, persistence, and partition
  metadata lookup remain external.
- [x] Added `tidb-planner::cardinality::out_of_range` for the pure
  `outOfRangeEQSelectivity` and `outOfRangeFullNDV` arithmetic, including
  modification/deletion fallback, zero-NDV square-root derivation, increase
  scaling, smoothing, minimum-row, and floating-point edge behavior.
  Histogram/TopN/session/range integration remains external.
- [x] Added `tidb-exec::session_status` for the atomic SessionVars status
  bitfield: any-bit queries, set/clear updates, protocol-sized readback, and
  default autocommit plus transaction/cursor masks. Transaction lifecycle,
  explicit-transaction metadata, cursor ownership, and result encoding remain
  external.

The Wave-41 static snapshot is 2,193/173/24/0 production and
16,016/415/142/12 test/support obligations. The source/test ledgers remain
ownership evidence, not parity percentages; the original Go test suite, full
planner/statistics/session integration, and deployable bootstrap remain open.

## Wave-42 progress (2026-07-16)

- [x] Added `tidb-stats::RowEstimate` for source-compatible default/min/max
  estimate construction, field-wise arithmetic, source clamp ordering, and
  skew-ratio bounds, including NaN and negative-skew behavior.
- [x] Added `tidb-planner::cardinality::uniform` for normalized
  `estimateRowCountWithUniformDistribution` arithmetic: histogram averaging,
  TopN fallback, out-of-range modification/deletion derivation, and
  `RiskEqSkewRatio` interpolation. Histogram/TopN/session/context integration
  remains external.
- [x] Added `tidb-exec::removed_sysvar` for the exact 13-entry removed-system-
  variable registry and case-sensitive reason lookup. Error construction,
  parser normalization, SET/SELECT dispatch, and the live sysvar registry
  remain external.

The Wave-42 static snapshot is 2,192/174/24/0 production and
16,013/418/142/12 test/support obligations. This remains source-owned WIP;
the original Go test suite, full planner/statistics/session integration, and a
deployable TiDB bootstrap remain incomplete.

## Wave-43 progress (2026-07-16)

- [x] Added `tidb-planner::schema_table_key` for source-faithful lowercase
  schema/table identity and qualified-versus-bare alias keys with map-safe
  equality and hashing. Parser-owned `ast.CIStr`, CTE/view scope, lock maps,
  and duplicate-alias diagnostics remain external.
- [x] Added `tidb-stats::stats_version` for the `Version0`/`Version1`/`Version2`
  constants and analyzed/synthesized metadata predicates. Persistence,
  ANALYZE scheduling, and handle/existence-map lifecycle remain external.
- [x] Added `tidb-exec::option_values` for the exact `ON`/`1` predicate and
  case-insensitive ON/OFF and true/false conversions with source pass-through
  behavior. System-variable validation, SQL parsing, SessionVars mutation, and
  warning publication remain external.

The Wave-43 static snapshot is 2,191/175/24/0 production and
16,009/422/142/12 test/support obligations. This remains source-owned WIP;
the original Go test suite, full planner/statistics/session integration, and a
deployable TiDB bootstrap remain incomplete.

## Wave-44 progress (2026-07-16)

- [x] Added `tidb-planner::implementation_cost` for source-shaped child-cost
  accumulation/reset, explicit cost readback, identity cost-limit scaling, and
  child-cost subtraction. Physical-plan/memo attachment remains external.
- [x] Added `tidb-stats::ColAndIdxExistenceMap` for column/index presence and
  analyzed metadata, replacement/deletion, capacity constructors, deep clone,
  and equality. Table/HistColl, DDL, online loading, and handle integration
  remain external.
- [x] Added `tidb-exec::statement_pushdown` for exact type/error-level bits,
  statement-kind precedence, LOAD DATA, and restricted-SQL flag composition.
  Live StatementContext, request construction, and TiKV execution remain
  external.

The Wave-44 static snapshot is 2,189/177/24/0 production and
16,005/426/142/12 test/support obligations. This remains source-owned WIP;
the original Go test suite, full planner/statistics/session integration, and a
deployable TiDB bootstrap remain incomplete.

## Wave-45 progress (2026-07-16)

- [x] Added `tidb-stats::scalar_geometry` for source-compatible interval
  fractions, common byte-prefix lengths, and left-aligned base-256 byte
  scalars. Datum conversion, histogram caches, and planner integration remain
  external.
- [x] Added `tidb-planner::task_type` for the four known execution-task values,
  exact labels, and forward-compatible unknown raw values. Physical-property
  construction, scheduling, and MPP execution remain external.
- [x] Added `tidb-exec::context_id` for atomic non-zero monotonic
  statement-context IDs. StatementContext construction/reset, lifecycle locks,
  timezone, warning handlers, and session attachment remain external.

The Wave-45 static snapshot is 2,186/180/24/0 production and
16,002/429/142/12 test/support obligations. This remains source-owned WIP;
the original Go test suite, full planner/statistics/session integration, and a
deployable TiDB bootstrap remain incomplete.

## Wave-46 progress (2026-07-16)

- [x] Added `tidb-planner::by_item` for source-shaped ORDER BY item direction,
  opaque-expression identity, formatting, clone/equality, and memory shape.
  Expression hashing/evaluation, collation context, and physical sort/property
  integration remain external.
- [x] Added `tidb-stats::memory_usage` for measured column/index memory totals,
  component accessors, tracking-cost arithmetic, and the source FMS exclusion
  boundary. Table aggregation, allocation measurement, cache eviction, and LFU
  integration remain external.
- [x] Added `tidb-exec::statement_refcount` for atomic frozen/no-reference
  sentinels and reference/freeze CAS transitions. There is no dedicated Go
  transition test; supplemental source-contract tests cover the source state,
  while cached StatementContext reuse remains external.

The Wave-46 static snapshot is 2,185/181/24/0 production and
15,997/434/142/12 test/support obligations. This remains source-owned WIP;
the original Go test suite, full planner/statistics/session integration, and a
deployable TiDB bootstrap remain incomplete.

## Wave-47 progress (2026-07-16)

- [x] Added `tidb-planner::physical_property` for MPP partition-type raw
  classification, exchange mapping, unknown fallback, and matched-result
  metadata. Expression columns, protobuf exchange values, and physical-plan
  matching remain external.
- [x] Added `tidb-stats::overlap_geometry` for left/right out-of-range overlap
  clipping, squared-width normalization, zero-width boundaries, and NaN
  propagation. Datum/histogram callers, skew policy, and planner integration
  remain external.
- [x] Added `tidb-exec::used_stats` for deterministic slow-log statistics
  formatting across pseudo/real versions, row counts, index/column status, and
  sorted ID fallback names. TableInfo resolution, collection, and slow-log I/O
  remain external.

The Wave-47 static snapshot is 2,184/182/24/0 production and
15,995/436/142/12 test/support obligations. This remains source-owned WIP;
the original Go test suite, full planner/statistics/session integration, and a
deployable TiDB bootstrap remain incomplete.

## Wave-48 progress (2026-07-16)

- [x] Added `tidb-planner::stats_info` for source-shaped row-count truncation
  and caller-owned column-NDV capping. Catalog statistics, full planner
  property derivation, and rule integration remain external.
- [x] Added `tidb-stats::HistogramCountSummary` for non-null/total row counts,
  null addition, realtime-row difference, and zero-total increase-factor
  behavior. Histogram mutation, TopN/CMSketch loading, and planner policy
  remain external.
- [x] Added `tidb-exec::plan_cache_params` for ordered append/reset, indexed
  and borrowed value access, and the non-prepared-cache privacy bit. Prepared
  plan evaluation, parameter coercion, and live SessionVars/EvalContext
  attachment remain external.

The Wave-48 static snapshot is 2,183/183/24/0 production and
15,992/439/142/12 test/support obligations. This remains source-owned WIP;
the original Go test suite, full planner/statistics/session integration, and a
deployable TiDB bootstrap remain incomplete.

## Wave-49 progress (2026-07-16)

- [x] Added `tidb-planner::index_columns` for normalized index-column
  projection, prefix marking, leading-prefix stopping, nil slots, and
  unspecified-length normalization over caller-owned metadata.
- [x] Added `tidb-stats::analysis_policy` for source-compatible analyzed,
  minimum-count, pseudo, and eligibility predicates while keeping mutable
  scheduler/configuration state outside the leaf.
- [x] Added `tidb-exec::stats_load_result` for statistics-load item identity,
  exact error detection, and source-shaped error rendering. Worker channels,
  retries, failpoints, and storage loading remain external.

## Wave-50 progress (2026-07-16)

- [x] Added `tidb-planner::pattern_engine` for cascades engine bit flags,
  predefined sets, overlap membership, raw values, and stable labels. Pattern
  matching and logical-plan integration remain external.

The Wave-50 static snapshot is 2,181/185/24/0 production and
15,986/445/142/12 test/support obligations. This remains source-owned WIP;
the original Go test suite, full planner/statistics/session integration, and a
deployable TiDB bootstrap remain incomplete.

## Wave-51 progress (2026-07-16)

- [x] Added `tidb-planner::fix_control` for source-compatible fix-control
  parsing, quoted/unquoted/empty values, duplicate replacement warnings, and
  parse errors. Session-variable wiring and typed getters remain external.
- [x] Added `tidb-stats::analyze_version_matches` for nil/pseudo, Version0,
  requested-version equality, and analyzed-version mismatch decisions. The
  caller’s rewrite/scheduler integration remains external.
- [x] Added `tidb-exec::alternative_plan_signals` for eight statement-local
  alternative-plan booleans, mark-to-true transitions, and complete reset.
  Planner rounds, cost choice, failpoints, and live StatementContext
  attachment remain external.

The Wave-51 static snapshot is 2,180/186/24/0 production and
15,983/448/142/12 test/support obligations. This remains source-owned WIP;
the original Go test suite, full planner/statistics/session integration, and a
deployable TiDB bootstrap remain incomplete.

## Wave-52 progress (2026-07-16)

- [x] Added `tidb-planner::memo_group_id` for one-based cascades memo-group
  IDs, deterministic raw setup, and uint64 wraparound. Memo ownership and
  optimizer integration remain external.
- [x] Added `tidb-stats::estimate_ndv_by_gee` for singleton correction,
  square-root scaling, half-up rounding, lower/upper clamps, and source
  preconditions. Sketch/TopN/Datum callers and handle integration remain
  external.
- [x] Added `tidb-exec::read_consistency` for strict/weak labels,
  case-insensitive validation, exact raw `IsWeak`, and the strict default.
  Request isolation, transaction admission, and SessionVars mutation remain
  external.

The Wave-52 static snapshot is 2,178/188/24/0 production and
15,980/451/142/12 test/support obligations. This remains source-owned WIP;
the original Go test suite, full planner/statistics/session integration, and a
deployable TiDB bootstrap remain incomplete.

## Wave-53 progress (2026-07-16)

- [x] Added `tidb-planner::task_scheduler` for serial LIFO task execution,
  first-error propagation, pending-stack retention, cleanup, and a default
  scheduler. Cascades task interfaces, pools, and context remain external.
- [x] Added `tidb-stats::avg_count_per_not_null_value` for increase-factor
  scaling, NDV lower bounds, empty-total fallback, and NaN behavior. Histogram
  construction and planner integration remain external.
- [x] Added `tidb-exec::chunk_alloc_status` for deterministic set/clear/readback
  allocation-use state. Chunk pools, reuse, SessionVars, and lifecycle remain
  external.

The Wave-53 static snapshot is 2,177/189/24/0 production and
15,978/453/142/12 test/support obligations. This remains source-owned WIP;
the original Go test suite, full planner/statistics/session integration, and a
deployable TiDB bootstrap remain incomplete.

## Wave-54 progress (2026-07-17)

- [x] Added `tidb-planner::hash_equaler` for the cascades primitive FNV-1a
  hasher: primitive update order, string/byte framing, float/rune handling,
  nil markers, cache/reset lifecycle, and digest readback. Object hashing,
  equality dispatch, and cascades integration remain external.
- [x] Added `tidb-stats::calc_correlation` for the histogram builder's
  one-sample shortcut and closed-form Pearson order-correlation arithmetic,
  including the source-defined zero-sample `NaN` result. Sampling, sorting,
  histogram construction, handle discovery, and persistence remain external.
- [x] Added `tidb-exec::setvar_hint_restore` for the statement-local,
  first-write-wins old-value registry used by `SET_VAR` hints. Hint parsing,
  system-variable mutation, warning publication, restoration timing, and live
  planner/session ownership remain external.

The Wave-54 static snapshot is 2,175/191/24/0 production and
15,973/458/142/12 test/support obligations. The workspace tests and strict
Clippy gate pass with 12 jobs. This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete.

## Wave-55 progress (2026-07-17)

- [x] Added `tidb-planner::plan_context` for the bounded `BuildPBContext`
  hand-off: expression-context replacement on detach, client/warning handle
  identity, and scalar flag preservation. Full `PlanContext` interfaces,
  protobuf conversion, and session/catalog owners remain external.
- [x] Added `tidb-stats::index_usage` for the seven source percentage-access
  buckets, exact boundary selection, zero-total-row fallback, one-hot sample
  construction, timestamp readback, and additive sample merge. Collector
  workers, global maps, persistence, and schema garbage collection remain
  external.
- [x] Added `tidb-exec::cursor_tracker` for cursor start timestamps, monotonic
  IDs, lookup, early-stoppable range traversal, close removal, and bounded
  concurrent create/range/close behavior. Session/result-set execution and
  full cursor lifecycle remain external.

The Wave-55 static snapshot is 2,171/195/24/0 production and
15,965/466/142/12 test/support obligations. The workspace tests and strict
Clippy gate pass with 12 jobs. This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete.

## Wave-56 progress (2026-07-17)

- [x] Added `tidb-planner::task_stack` for the cascades reusable LIFO stack:
  default capacity, push/pop/empty/length, description ordering, and destroy
  with capacity reuse. Stack pools, task interfaces, cascades context, and
  unsafe layout assertions remain external.
- [x] Added `tidb-stats::analyze_jobs` for analyze status/job metadata and
  progress arithmetic: thresholded deltas, dump interval gating, reset, and
  readback. SQL persistence, scheduler/handle lifecycle, failpoints, and
  remaining-time reporting remain external.
- [x] Added `tidb-exec::session_context_key` for the source integer context-key
  domain, named labels, and unknown-value display. Live context storage and
  session consumers remain external.

The Wave-56 static snapshot is 2,168/198/24/0 production and
15,961/470/142/12 test/support obligations. The workspace tests and strict
Clippy gate pass with 12 jobs. This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete.

## Wave-57 progress (2026-07-17)

- [x] Added `tidb-planner::pattern` for cascades operand labels, wildcard and
  typed-operator matching, engine filters, and ordered child-pattern
  construction. Concrete logical-plan objects and memo integration remain
  external.
- [x] Added `tidb-stats::async_load` for the 128-shard pending statistics-load
  map, table/column/index keys, full-load upgrades, enumeration, deletion, and
  length semantics. SQL persistence, scheduling, storage errors, and handle
  cleanup remain external.
- [x] Added `tidb-exec::status_registry` for status scopes/values, provider
  registration/removal, deterministic collection, scope attachment, and error
  propagation. Live SessionVars counters and protocol publication remain
  external.

The Wave-57 static snapshot is 2,165/201/24/0 production and
15,951/480/142/12 test/support obligations. The workspace tests and strict
Clippy gate pass with 12 jobs. This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete.

## Wave-58 progress (2026-07-17)

- [x] Added `tidb-planner::string_writer` for source-compatible ordered
  string assembly, delimiter handling, and empty/default behavior. Full
  cascades formatting and planner call-site integration remain external.
- [x] Added `tidb-stats::datum_map_cache` for deterministic datum-key
  normalization, cache lookup/insert/clear, and bounded map lifecycle. Full
  CMSketch/TopN ownership, persistence, and statistics scheduling remain
  external.
- [x] Added `tidb-exec::process_info` for shallow process metadata cloning and
  source-compatible optional field preservation. Live session manager
  ownership, mutation, and protocol publication remain external.

The Wave-58 static snapshot is 2,162/204/24/0 production and
15,946/485/142/12 test/support obligations. The workspace tests and strict
Clippy gate pass with 12 jobs. This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 60 is the next parallel source-owner batch.

## Wave-59 progress (2026-07-17)

- [x] Added `tidb-planner::expr_iterator` for source-shaped memo expression
  matching, recursive child cartesian enumeration, engine filtering, and
  reset/advance state. Real memo groups, intrusive list ownership, and
  cascades allocator/context remain external.
- [x] Added `tidb-stats::need_analyze_table` for bounded auto-analyze trigger
  policy: unanalyzed-table handling, zero-ratio disablement, analyze-row versus
  realtime-count selection, modification ratio, and reason classification. SQL
  scheduling, mutable global configuration, and statistics lifecycle remain
  external.
- [x] Added `tidb-exec::nextgen_readonly_vars` for the source six-name,
  case-insensitive next-generation read-only variable predicate. Variable
  registration/defaults, kernel-type gating, and SET dispatch remain external.

The Wave-59 static snapshot is 2,159/207/24/0 production and
15,940/491/142/12 test/support obligations. The workspace tests and strict
Clippy gate pass with 12 jobs. This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 61 is the next parallel source-owner batch.

## Wave-60 progress (2026-07-17)

- [x] Added `tidb-planner::explore_mark` for source-compatible memo round-bit
  set/clear/query state, fixed-width copying, and inert overflow behavior.
  Real memo-group lifecycle and cascades ownership remain external.
- [x] Added `tidb-stats::parse_auto_analyze_ratio` for Go-compatible default,
  parse-fallback, negative-clamp, and non-finite ratio handling. Global
  configuration, SQL scheduling, and auto-analyze lifecycle remain external.
- [x] Added `tidb-exec::slow_log_threshold` for typed slow-log equality,
  same-type numeric threshold, zero-threshold, and signed-to-unsigned
  conversion helpers. Field parsing/accessor registration and live session
  mutation/rendering remain external.

The Wave-60 static snapshot is 2,156/210/24/0 production and
15,935/496/142/12 test/support obligations. The workspace tests and strict
Clippy gate pass with 12 jobs. This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 62 is the next parallel source-owner batch.

## Wave-61 progress (2026-07-17)

- [x] Added `tidb-planner::group_expr` for source-shaped memo child identity,
  fingerprint framing, exploration marks, and applied-rule tracking. Full memo
  ownership and cascades integration remain external.
- [x] Added `tidb-stats::AutoAnalysisTimeWindow` for inclusive UTC minute
  windows, unset endpoints, and midnight-crossing behavior. Queue scheduling,
  configuration, and auto-analyze lifecycle remain external.
- [x] Added `tidb-exec::slow_log_rules` for typed slow-log condition/rule
  metadata, session stale-field state, and global connection-rule grouping.
  Parsing, evaluation, live session mutation, and protocol publication remain
  external.

The Wave-61 static snapshot is 2,153/213/24/0 production and
15,929/502/142/12 test/support obligations. The workspace tests and strict
Clippy gate pass with 12 jobs. This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 63 is the next parallel source-owner batch.

## Wave-62 progress (2026-07-17)

- [x] Added `tidb-planner::column_length` for source-compatible `Col2Len`
  dominance, comparability, unspecified-length ordering, and sentinel handling.
  Access-condition extraction and path/ranger ownership remain external.
- [x] Added `tidb-stats::calculate_priority_weight` and
  `special_event_weight` for the auto-analyze priority formula, event weights,
  logarithmic size/change terms, and interval factors. Queue integration and
  mutable scheduling state remain external.
- [x] Added `tidb-exec::session_token_timing` for classic/Starter token
  lifetime, certificate reload interval, and old-certificate grace durations.
  Crypto, certificate I/O/rotation, and session authentication remain external.

The Wave-62 static snapshot is 2,150/216/24/0 production and
15,925/506/142/12 test/support obligations. The workspace tests and strict
Clippy gate pass with 12 jobs. This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 63 is the next parallel source-owner batch.

## Wave-63 progress (2026-07-17)

- [x] Added `tidb-planner::plan_cache_constants` for source-compatible nil
  preservation, safe sharing, unsafe deep-copy behavior, all-safe fast paths,
  and destination reuse. Full plan-cache ownership remains external.
- [x] Added `tidb-stats::get_partition_sql` and
  `flatten_partition_names` for dynamic-partition placeholder SQL assembly and
  deterministic partition-name flattening. Analysis-job lifecycle and queue
  integration remain external.
- [x] Added `tidb-exec::advisory_lock_state` for owner identity and Go-shaped
  signed reference-count increment/decrement/readback. SQL lock-name
  validation, timeout, rollback, and session cleanup remain external.

The Wave-63 static snapshot is 2,147/219/24/0 production and
15,921/510/142/12 test/support obligations. The workspace tests and strict
Clippy gate pass with 12 jobs. This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 64 is the next parallel source-owner batch.

## Wave-64 progress (2026-07-17)

- [x] Added `tidb-planner::index_advisor_model` for source-shaped column/index
  normalization, identity keys, and ordered prefix containment. Full advisor
  enumeration and optimizer integration remain external.
- [x] Added `tidb-stats::priority_heap` for bounded max-heap add/update,
  delete, lookup, listing, peek/pop, length, and NaN ordering behavior over
  caller-owned scalar entries. AnalysisJob construction and scheduling remain
  external.
- [x] Added `tidb-exec::txn_running_state` for the five source transaction
  running-state discriminants, exact labels, and counter semantics. Live KV
  locks/timing, process publication, and RealTiKV lifecycle remain external.

The Wave-64 static snapshot is 2,144/222/24/0 production and
15,908/523/142/12 test/support obligations. The workspace tests and strict
Clippy gate pass with 12 jobs. This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 65 is the next parallel source-owner batch.

## Wave-65 progress (2026-07-17)

- [x] Added `tidb-planner::rule_type` for the source rule discriminants,
  stable raw round-tripping, unknown-value retention, and exact string labels.
  Rule dispatch, cascades integration, and optimizer ownership remain external.
- [x] Added `tidb-stats::analysis_interval` for source-compatible interval
  sentinels, just-failed and average-duration calculations, and raw SQL query
  constants. Query execution, partition scheduling, and mutable auto-analyze
  lifecycle remain external.
- [x] Added `tidb-exec::txn_summary` for FNV-1a SQL-digest sequences,
  distinct-sequence promotion, bounded LRU eviction, resizing, and ordered
  snapshots. JSON rendering, duration filtering, global recorder/mutex, and
  infoschema/session wiring remain external.

The Wave-65 static snapshot is 2,141/225/24/0 production and
15,902/529/142/12 test/support obligations. Formatting, the workspace test
suite, strict Clippy, and the static parser/plan/dependency gate pass with 12
jobs. This remains source-owned WIP; the original Go test suite, full
planner/statistics/session integration, and a deployable TiDB bootstrap remain
incomplete. Wave 66 is the next parallel source-owner batch.

## Wave-66 progress (2026-07-17)

- [x] Added `tidb-planner::base_traits` for source-compatible `Hash64`,
  `Equals`, and `HashEquals` contracts over the existing typed hasher and
  dynamic equality boundary. Cascades object implementations and dispatch
  remain external.
- [x] Added `tidb-stats::auto_analyze_job` for bounded string/JSON indicator
  formatting and dynamic-partitioned analysis-job classification. Concrete job
  interfaces, session state, query execution, and queue lifecycle remain
  external.
- [x] Added `tidb-exec::session_pool_capacity` for the Go system-session pool
  capacity limit and invalid-value normalization. Factory/channel ownership,
  session transfer/reset/close, and context remain external.

The Wave-66 static snapshot is 2,138/228/24/0 production and
15,899/532/142/12 test/support obligations. Formatting, the workspace test
suite, strict Clippy, and the static parser/plan/dependency gate pass with 12
jobs. This remains source-owned WIP; the original Go test suite, full
planner/statistics/session integration, and a deployable TiDB bootstrap remain
incomplete. Wave 67 is the next parallel source-owner batch.

## Wave-67 progress (2026-07-17)

- [x] Added `tidb-planner::scheduler_contract` for the source `Scheduler`
  interface and dynamic dispatch over the existing task scheduler. Concurrent
  scheduling, stack pooling, and cascades context remain external.
- [x] Added `tidb-stats::non_partitioned_analysis` for exact analyze-table and
  analyze-index SQL templates, ordered identifier parameters, and index-kind
  selection. Schema/session lookup, execution, validation, and queue lifecycle
  remain external.
- [x] Added `tidb-exec::sysvar_scope` for source-compatible ScopeFlag bits,
  fixed rendering order, and unknown-bit behavior. SysVar registry/type
  validation and SET/GET persistence remain external.

The Wave-67 static snapshot is 2,135/231/24/0 production and
15,891/540/142/12 test/support obligations. Formatting, the workspace test
suite, strict Clippy, and the static parser/plan/dependency gate pass with 12
jobs. This remains source-owned WIP; the original Go test suite, full
planner/statistics/session integration, and a deployable TiDB bootstrap remain
incomplete. Wave 68 is the next parallel source-owner batch.

## Wave-68 progress (2026-07-17)

- [x] Added `tidb-planner::stack_contract` for the source Stack interface,
  richer execute/description task boundary, push/pop, empty, and destroy
  semantics. Concrete task-stack ownership remains external.
- [x] Added `tidb-stats::static_partitioned_analysis` for exact static
  partition/table/index SQL templates, physical partition queue keys, and
  index-kind selection. Partition/schema lookup, execution, validation, and
  queue lifecycle remain external.
- [x] Added `tidb-exec::charset_variable_groups` for ordered SET NAMES and
  SET CHARSET variable groups and membership predicates. SET execution,
  collation validation, SessionVars mutation, and charset conversion remain
  external.

The Wave-68 static snapshot is 2,132/234/24/0 production and
15,884/547/142/12 test/support obligations. Formatting, the workspace test
suite, strict Clippy, and the static parser/plan/dependency gate pass with 12
jobs. This remains source-owned WIP; the original Go test suite, full
planner/statistics/session integration, and a deployable TiDB bootstrap remain
incomplete. Wave 69 is the next parallel source-owner batch.

## Wave-69 progress (2026-07-17)

- [x] Added `tidb-planner::topn_push_down` for the source rule wrapper's
  caller-owned plan callback, nil parent, stable name, false change flag, and
  nil error boundary. Full logical-plan TopN placement remains external.
- [x] Added `tidb-stats::queue_gate` for the exact uninitialized priority-queue
  error and shared initialization/default contracts. Heap, worker, session,
  DDL, and queue lifecycle behavior remain external.
- [x] Added `tidb-exec::sysvar_type` for byte-backed TypeFlag discriminants
  `TypeStr` through `TypeDuration`, preserving unknown values. SysVar
  registration, validation, parsing, hooks, and conversion remain external.

The Wave-69 static snapshot is 2,130/236/24/0 production and
15,881/550/142/12 test/support obligations. Formatting, the workspace test
suite, strict Clippy, and the static parser/plan/dependency gate pass with 12
jobs. This remains source-owned WIP; the original Go test suite, full
planner/statistics/session integration, and a deployable TiDB bootstrap remain
incomplete. Wave 70 is the next parallel source-owner batch.

## Wave-70 progress (2026-07-17)

- [x] Added `tidb-planner::derive_topn_from_window` for the source rule
  wrapper's no-argument plan callback, stable name, false change flag, and nil
  error boundary. Window/TopN/MPP semantics remain external.
- [x] Added `tidb-stats::ddl_queue_gate` for the DDL pre-dispatch readiness
  decision: dispatch when initialized, retry while initializing and enabled,
  and ignore while uninitialized and disabled. Event decoding, queue mutation,
  and lifecycle remain external.
- [x] Added `tidb-exec::sysvar_error` for exact variable error-code identities.
  Constructors, messages, SQLSTATE, formatting, and warning/error plumbing
  remain external.

The Wave-70 static snapshot is 2,127/239/24/0 production and
15,877/554/142/12 test/support obligations. Formatting, the workspace test
suite, strict Clippy, and the static parser/plan/dependency gate pass with 12
jobs. This remains source-owned WIP; the original Go test suite, full
planner/statistics/session integration, and a deployable TiDB bootstrap remain
incomplete. Wave 71 is the next parallel source-owner batch.

## Wave-71 progress (2026-07-17)

- [x] Added `tidb-planner::eliminate_empty_selection` for the source rule
  wrapper's recursive plan callback, stable name, false change flag, and nil
  error boundary. Logical selection detection and child mutation remain
  external.
- [x] Added `tidb-stats::refresher_state` for the initialized-only queue rebuild
  decision when auto-analyze ratio or prune mode changes. Session parsing,
  queue rebuild, workers, and statistics handles remain external.
- [x] Added `tidb-exec::hint_updatable_vars` for the complete 128-name exact
  SET_VAR hint-updatable registry and membership predicate. SysVar mutation,
  hint parsing/application, validation, planner use, and session lifecycle
  remain external.

The Wave-71 static snapshot is 2,124/242/24/0 production and
15,874/557/142/12 test/support obligations. Formatting, the workspace test
suite, strict Clippy, and the static parser/plan/dependency gate pass with 12
jobs. This remains source-owned WIP; the original Go test suite, full
planner/statistics/session integration, and a deployable TiDB bootstrap remain
incomplete. Wave 72 is the next parallel source-owner batch.

## Wave-72 progress (2026-07-17)

- [x] Added `tidb-planner::push_down_sequence` for recursive sequence-plan
  traversal: nested CTE/main merging, DataSource/CTE push-through, unary
  descent, and safe multi-child/childless attachment. Real logical operators
  and predicate mutation remain external.
- [x] Added `tidb-stats::worker_capacity` for worker admission when running
  jobs are below the concurrency limit and equality no-op updates. Async
  execution, synchronization, hooks, waits, and statistics handles remain
  external.
- [x] Added `tidb-exec::noop_read_only` for the first five source no-op/read-only
  registrations plus pure `checkReadOnly` session/global OFF/ON/WARN policy.
  Full SysVar registration/mutation, warning/error plumbing, and session
  lifecycle remain external.

The Wave-72 static snapshot is 2,121/245/24/0 production and
15,871/560/142/12 test/support obligations. Formatting, the workspace test
suite, strict Clippy, and the static parser/plan/dependency gate pass with 12
jobs. This remains source-owned WIP; the original Go test suite, full
planner/statistics/session integration, and a deployable TiDB bootstrap remain
incomplete. Wave 73 is the next parallel source-owner batch.

## Wave-73 progress (2026-07-17)

- [x] Added `tidb-planner::eliminate_unionall_dual_item` for recursive
  zero-row `TableDual`/projection filtering, schema-preserving empty-union
  replacement, and exact changed aggregation. Full logical operator execution
  remains external.
- [x] Added `tidb-stats::stats_key_set` for thread-safe key replacement,
  lookup/removal costs, enumeration, length, and clear operations with
  caller-owned cost tracking. LFU admission/eviction and table accounting
  remain external.
- [x] Added `tidb-exec::session_reuse_state` for owner-gated avoid-reuse and
  idempotent close state transitions. Owner hooks, context close, in-use
  deferral, transfer, and operation locking remain external.

The Wave-73 static snapshot is 2,118/248/24/0 production and
15,867/564/142/12 test/support obligations. Formatting, the workspace test
suite, strict Clippy, and the static parser/plan/dependency gate pass with 12
jobs. This remains source-owned WIP; the original Go test suite, full
planner/statistics/session integration, and a deployable TiDB bootstrap remain
incomplete. Wave 74 is the next parallel source-owner batch.

## Wave-74 progress (2026-07-17)

- [x] Added `tidb-planner::projection_elimination` for the source
  `canProjectionBeEliminatedLoose` predicate: reject `Proj4Expand`, accept only
  direct-column expression shapes, and preserve the empty-expression case.
  Full expression/schema/copy-on-write and physical elimination remain
  external.
- [x] Added `tidb-stats::stats_key_set_shards` for fixed 256-shard routing and
  aggregate key-set operations over caller-owned costs. LFU admission/eviction,
  async visibility, metrics, and table accounting remain external.
- [x] Added `tidb-exec::system_db_filter` for `SkipLoadDiff=false` and the exact
  lower-case `mysql` system-database schema filter. Domain/schema loading
  remains external.

The Wave-74 static snapshot is 2,114/252/24/0 production and
15,864/567/142/12 test/support obligations. Formatting, the workspace test
suite, strict Clippy, and the static parser/plan/dependency gate pass with 12
jobs. This remains source-owned WIP; the original Go test suite, full
planner/statistics/session integration, and a deployable TiDB bootstrap remain
incomplete. Wave 75 is the next parallel source-owner batch.

## Wave-75 progress (2026-07-17)

- [x] Added `tidb-planner::resolve_grouping_expand` for post-order
  `LogicalExpand` traversal and append-style generated-level counts while
  leaving grouping-set construction external.
- [x] Added `tidb-stats::memory_cost` for LFU capacity adjustment, the 20%-memory
  fallback, the 5 MB test override, explicit memory-probe errors, and signed
  cost wraparound. Host-memory probing and cache lifecycle remain external.
- [x] Added `tidb-exec::upgrade_versions` for the exact ordered 173-entry
  `upgradeToVerFunctions` registry, historical gaps, current version 263, and
  upgrade-function naming. Upgrade SQL, bootstrap mutation, and schema changes
  remain external.

The Wave-75 static snapshot is 2,111/255/24/0 production and
15,861/570/142/12 test/support obligations. Formatting, the workspace test
suite, strict Clippy, and the static parser/plan/dependency gate pass with 12
jobs. This remains source-owned WIP; the original Go test suite, full
planner/statistics/session integration, and a deployable TiDB bootstrap remain
incomplete. Wave 76 is the next parallel source-owner batch.

## Wave-76 progress (2026-07-17)

- [x] Added `tidb-planner::join_reorder_projection_inline` for the recursive
  supported-expression tree and source safety gates: nonzero column references,
  deferred constants, unsupported nodes, `Proj4Expand`, mutable or
  nondeterministic functions, and correlated expressions remain explicit.
  Join-group attribution and substitution remain external.
- [x] Added `tidb-stats::BatchUpdate` for capacity-triggered update/delete
  flushing, source order, empty flush no-op behavior, and retained capacity;
  the statistics queue and cache lifecycle remain external.
- [x] Added `tidb-exec::session_metrics` for the exact registered
  delete/insert/update label order, with the registration anchor kept separate
  from the nontransactional session policy tests.

The Wave-76 static snapshot is 2,108/258/24/0 production and
15,858/573/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 77 is the next parallel source-owner batch.

## Wave-77 progress (2026-07-17)

- [x] Added `tidb-planner::max_min_elimination` for the source eligibility
  gate: empty/grouped rejection, all-`MAX`/`MIN` enforcement, ENUM/SET ordering
  safety, and single-vs-multi aggregate branch classification. Index-path
  checks and replacement-plan construction remain external.
- [x] Added `tidb-stats::MapCache` for caller-costed put/get/replace/delete,
  signed cost arithmetic, key/value enumeration, copy state, and explicit
  no-op lifecycle hooks. LFU admission/eviction and cache ownership remain
  external.
- [x] Added `tidb-exec::hash_join_version` for the `legacy`/`optimized`
  literals, legacy default, and case-insensitive optimized-version predicate;
  SysVar validation, mutation, and join selection remain external.

The Wave-77 static snapshot is 2,105/261/24/0 production and
15,853/578/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 78 is the next parallel source-owner batch.

## Wave-78 progress (2026-07-17)

- [x] Added `tidb-planner::logical_table_dual` for TableDual identity/hash
  semantics, nil-vs-present schema state, ordered column identity, row count,
  and `rowcount:` explain metadata. Field-type/collation and runtime details
  remain external.
- [x] Added `tidb-stats::healthy_metrics` for the exact ten healthy-bucket
  indexes, labels, upper bounds, and catalog count; Prometheus registration,
  StatsCache traversal, and gauge updates remain external.
- [x] Added `tidb-exec::slow_log_match` for AND-within-rule, OR-across-rule
  composition and session→connection-specific→global precedence. Field
  accessors, parsing, thresholds, and session state remain external.

The Wave-78 static snapshot is 2,102/264/24/0 production and
15,850/581/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 79 is the next parallel source-owner batch.

## Wave-79 progress (2026-07-17)

- [x] Added `tidb-planner::logical_limit` for Limit identity/hash framing,
  nil-vs-present schema and partition metadata, ordered sort-column identity,
  offset/count, and bounded explain metadata. Runtime limit behavior remains
  external.
- [x] Added `tidb-stats::json_metadata` for the global marker and deterministic
  predicate-column ID ordering used by JSON statistics metadata. Tipb payloads,
  storage blocks, and stats-handle ownership remain external.
- [x] Added `tidb-exec::privilege_set` for exact split/join/add/delete privilege
  set semantics, including ordering, duplicate handling, and first-delete
  behavior. GRANT/REVOKE SQL and persistence remain external.

The Wave-79 static snapshot is 2,099/267/24/0 production and
15,847/584/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 80 is the next parallel source-owner batch.

## Wave-80 progress (2026-07-17)

- [x] Added `tidb-planner::logical_max_one_row` for the generated
  `MaxOneRow` identity/hash contract, preserving the operator tag and embedded
  base-plan ID while leaving runtime planning external.
- [x] Added `tidb-stats::locked_tables` for the exact locked-table query marker
  and deterministic requested-ID filtering; SQL execution and lock lifecycle
  remain external.
- [x] Added `tidb-exec::effective_auth_plugin` for explicit-plugin precedence,
  default-plugin fallback, and the empty-default `mysql_native_password`
  resolution. Capability checks, auth storage, and password policy remain
  external.

The Wave-80 static snapshot is 2,096/270/24/0 production and
15,842/589/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 81 is the next parallel source-owner batch.

## Wave-81 progress (2026-07-17)

- [x] Added `tidb-planner::logical_sort` for generated Sort identity/hash
  framing over normalized column/direction items; arbitrary expressions,
  pruning, and runtime ordering remain external.
- [x] Added `tidb-stats::lock_messages` for stable skipped-table and
  skipped-partition message formatting, including sorted names and
  pluralization; lock lifecycle and SQL ownership remain external.
- [x] Added `tidb-exec::broadcast_query_error` for nil-safe classification of
  the exact two-fragment unsupported-broadcast error; broadcast RPC and
  analysis behavior remain external.

## Wave-82 progress (2026-07-17)

- [x] Added `tidb-planner::logical_top_n` for generated TopN identity/hash
  framing, ordered ByItems/PartitionBy metadata, offset/count, and
  `PreferLimitToCop`; runtime TopN behavior remains external.
- [x] Added `tidb-stats::usage_collector` for bounded normal/high-priority
  queues, blocking session sends, worker priority/drain behavior, and close;
  session and worker lifecycle wiring remain external.
- [x] Added `tidb-exec::insert_rows_col_multiply` for zero-aware
  row-count/column-count multiplication with `i64::MAX` saturation; RUV2
  metrics and session wiring remain external.

The Wave-82 static snapshot is 2,090/276/24/0 production and
15,833/598/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 83 is the next parallel source-owner batch.

## Wave-83 progress (2026-07-17)

- [x] Added `tidb-planner::logical_show_ddl_jobs` for the generated
  `ShowDDLJobs` identity/hash contract, preserving the operator tag and
  embedded base-plan ID while leaving DDL scheduling and runtime planning
  external.
- [x] Added `tidb-stats::stats_delta` for the exact locked-statistics delta
  query marker and first-row/empty/error behavior; SQL execution and
  statistics-handle ownership remain external.
- [x] Added `tidb-exec::readable_size` for case-sensitive decimal/B/KiB/MiB/
  GiB/TiB/PiB parsing, three-byte boundaries, and uint64-wrapping products;
  inspection SQL and caller policy remain external.

The Wave-83 static snapshot is 2,087/279/24/0 production and
15,830/601/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 88 is the next parallel source-owner batch.

## Wave-87 progress (2026-07-17)

- [x] Added `tidb-planner::logical_union_all` for generated `LogicalUnionAll`
  identity/hash framing over the union tag and nil/present ordered normalized
  schema; child construction, predicate pushdown, and runtime union execution
  remain external. The initially selected `LogicalSelection` owner was already
  claimed and was removed before integration.
- [x] Added `tidb-stats::pending_delta_ids` for deterministic pending-table ID
  selection: all IDs when targets are empty, target filtering against pending
  keys, deduplication, and ascending order; session/statistics-handle sweeps,
  locks, SQL, and persistence remain external.
- [x] Added `tidb-exec::lack_handles` for source-bounded expected-handle
  reconciliation, matching/removal, ordered missing results, and the exact
  cardinality stop boundary; KV encoding, workers, and consistency reporting
  remain external.

The Wave-87 static snapshot is 2,075/291/24/0 production and
15,815/616/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 88 is the next parallel source-owner batch.

## Wave-88 progress (2026-07-17)

- [x] Added `tidb-planner::logical_mem_table` for generated LogicalMemTable
  identity/hash framing over the operator tag, optional TableInfo ID, folded
  database name, and normalized schema metadata; table construction, memtable
  scan planning, and runtime execution remain external.
- [x] Added `tidb-stats::sync_load_concurrency` for the source threshold policy
  mapping requested table counts to bounded concurrent-load workers; scheduler,
  GOMAXPROCS, and statistics-handle lifecycle remain external.
- [x] Added `tidb-exec::slow_log_split` for byte-oriented slow-log field/value
  splitting with nested bracket matching, malformed-input rejection, empty
  values, timestamps, and the source cardinality boundary; log ingestion,
  session policy, and persistence remain external.

The Wave-88 static snapshot is 2,072/294/24/0 production and
15,812/619/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 89 is the next parallel source-owner batch.

## Wave-89 progress (2026-07-17)

- [x] Added `tidb-planner::logical_projection` for generated
  LogicalProjection identity/hash framing over normalized schema and ordered
  expression columns, nil/present expression markers, `CalculateNoDelay`, and
  `Proj4Expand`; expression evaluation, rewrites, and runtime projection remain
  external.
- [x] Added `tidb-stats::partition_table_id_cache` for schema-versioned
  partition-to-parent-table cache rebuilds, lookup, and duplicate last-write
  behavior; InfoSchema traversal, table resolution, locking, and lifecycle
  remain external.
- [x] Added `tidb-exec::analyze_panic_error` for source-shaped analyze-worker
  panic classification: memory-sentinel OOM, propagated errors, and worker
  fallback with the exact samplerate guidance; recovery, logging, and worker
  scheduling remain external.

The Wave-89 static snapshot is 2,069/297/24/0 production and
15,809/622/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 90 is the next parallel source-owner batch.

## Wave-90 progress (2026-07-17)

- [x] Added `tidb-planner::logical_expand` for generated LogicalExpand
  identity/hash framing over normalized schema/grouping columns, nested
  rollup/level nil-versus-empty structure, `DistinctSize`, `GID`, and `GPos`;
  expression variants, grouping maps, optimizer context, and runtime expansion
  remain external.
- [x] Added `tidb-stats::weighted_reservoir` for source-faithful bounded
  weighted sampling: fill-then-heapify, min-root replacement only for strictly
  larger weights, Go tie behavior, and zero-capacity safety; RNG, Datum rows,
  sketches, and collector merging remain external.
- [x] Added `tidb-exec::delete_rows_col_multiply` for saturating DELETE
  row/column metric accumulation, including non-positive deltas, the MAX
  sentinel, and positive overflow clamping; metric/session/storage effects
  remain external.

The Wave-90 static snapshot is 2,066/300/24/0 production and
15,806/625/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 91 is the next parallel source-owner batch.

## Wave-91 progress (2026-07-17)

- [x] Added `tidb-planner::window_frame` for source-shaped FrameBound and
  WindowFrame Hash64/Equals metadata, nil-preserving clone behavior, caller
  compare-function tokens, and the handwritten start/end hash asymmetry;
  arbitrary expressions, session/type context, and runtime window planning
  remain external.
- [x] Added `tidb-stats::stats_meta` for exact normal and `FOR UPDATE`
  `mysql.stats_meta` query selectors, empty-row null sentinels, and Go-compatible
  uint64-to-int64 conversion; SQL execution, DDL concurrency, and storage
  lifecycle remain external.
- [x] Added `tidb-exec::cte_first_error` for first-error precedence while
  preserving the original concrete error value; worker lifecycle, logging,
  failpoints, and cleanup ordering remain external.

The Wave-91 static snapshot is 2,063/303/24/0 production and
15,801/630/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 92 is the next parallel source-owner batch.

## Wave-92 progress (2026-07-17)

- [x] Added `tidb-planner::handle_cols` for CommonHandleCols and IntHandleCols
  identity/hash framing, nil/present metadata, ordered column lists, and clone
  state; catalog metadata, handle encoding, collation, and storage remain
  external.
- [x] Added `tidb-stats::stats_read_writer` for historical-version and slow-save
  predicates, the five-lease threshold, force override, duration wrapping, and
  exact refresh error text; SQL, transaction, failpoint, and storage lifecycle
  remain external.
- [x] Added `tidb-exec::traffic_form` for Go-compatible sorted form encoding,
  escaping, duplicate-value ordering, UTF-8, and reserved-byte boundaries;
  HTTP/TiProxy request and traffic lifecycle remain external.

The Wave-92 static snapshot is 2,060/306/24/0 production and
15,795/636/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 93 is the next parallel source-owner batch.

## Wave-93 progress (2026-07-17)

- [x] Added `tidb-planner::logical_aggregation` for source-faithful
  `LogicalAggregation` Hash64/Equals framing, normalized aggregate metadata,
  ordered possible properties, and explicit `HasTiFlash` omission; expression
  typing, grouping maps, and runtime planning remain external.
- [x] Added `tidb-stats::stats_meta_update` for locked/unlocked and
  positive/negative delta partitioning, exact stats-meta SQL assembly, ordered
  cache invalidation IDs, MinInt64 wrapping, and version-refresh parameters;
  transaction/session/storage lifecycle remains external.
- [x] Added `tidb-exec::ddl_job_comments` for source-ordered analyze, reorg,
  DXF/cloud, worker, batch, write-speed, and placement labels, including
  next-gen early return behavior. The evidence audit corrected the original Go
  test anchors to `show_ddl_jobs_test.go:26` and `:115`; live DDL state and SQL
  result execution remain external.

The Wave-93 static snapshot is 2,057/309/24/0 production and
15,790/641/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 94 is the next parallel source-owner batch.

## Wave-94 progress (2026-07-17)

- [x] Added `tidb-planner::cost_usage` for source-shaped CostVer2/CostTrace
  factor gating, lazy formula construction, ordered aggregation, fixed-point
  arithmetic, nonnegative/NaN handling, and tie-break preservation; full cost
  model and optimizer integration remain external.
- [x] Added `tidb-stats::sample_bytes` for the exact 32,767-byte sample limit,
  inclusive length filtering, and Go-compatible wrapping total-size
  accumulation; Datum/proto conversion and sampling lifecycle remain external.
- [x] Added `tidb-exec::global_sysvar_initial` for the environment-adjusted
  system-variable defaults across TiKV, test, row-format, assertion,
  mutation-checker, and fair-locking branches. The source/test contract is
  explicit and excludes registry lookup, validation, SessionVars mutation,
  and next-gen hook errors.

The Wave-94 static snapshot is 2,054/312/24/0 production and
15,785/646/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 95 is the next parallel source-owner batch.

## Wave-95 progress (2026-07-17)

- [x] Added `tidb-planner::wrap_cast` for the source mode gate across
  Complete/Partial1/Dedup and Final/Partial2, including caller-marked
  delegated uncastable arguments and empty-argument behavior; full aggregate
  expression construction remains external.
- [x] Added `tidb-stats::index_query_bytes` for exact TopN-hit, CMSketch-hit,
  then histogram fallback precedence over caller-supplied resolved counts;
  encoding and index/statistics lifecycle remain external.
- [x] Added `tidb-exec::tagged_ptr` for source-compatible 64-bit tagged-pointer
  width, mask initialization, tag extraction, clear/roundtrip behavior, and
  the 24-bit cap; join execution and hash-table ownership remain external.

The Wave-95 static snapshot is 2,051/315/24/0 production and
15,780/651/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 96 is the next parallel source-owner batch.

## Wave-96 progress (2026-07-17)

- [x] Added `tidb-planner::logical_mock` for source-shaped
  `MockDataSource.Init` metadata: `mockDS`, query-block offset zero, retained
  plan-context token, and reinitialization/zero-value behavior; physical mock
  planning remains external.
- [x] Added `tidb-stats::historical_stats` for table-versus-partition history
  version selection: table version for non-partitioned input and the maximum
  partition version for non-empty partition input; JSON/storage/session
  lifecycle remains external.
- [x] Added `tidb-exec::stddevpop` for population standard-deviation final
  normalization: zero count is NULL, otherwise `sqrt(variance/count)`, with
  negative variance preserving NaN; accumulation, distinct state, and chunk
  execution remain external.

The Wave-96 static snapshot is 2,048/318/24/0 production and
15,776/655/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 97 is the next parallel source-owner batch.

## Wave-97 progress (2026-07-17)

- [x] Added `tidb-planner::logical_property` for zero-value and optional
  Stats/Schema/FD property state, MaxOneRow, nil-vs-empty PossibleProps, and
  HasTiFlash preservation through opaque adapters; memo/schema/FD consumers
  remain external.
- [x] Added `tidb-stats::init_stats_concurrency` for force CPU-minus-two and
  normal CPU-half policies with the source `[2,16]` clamp and signed wrapping
  arithmetic; config/runtime/worker lifecycle remains external.
- [x] Added `tidb-exec::stddevsamp` for sample standard-deviation finalization:
  counts at most one are NULL, otherwise `sqrt(variance/(count-1))`, with
  negative variance preserving NaN; accumulation and chunk execution remain
  external.

The Wave-97 static snapshot is 2,045/321/24/0 production and
15,772/659/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 98 is the next parallel source-owner batch.

## Wave-98 progress (2026-07-17)

- [x] Added `tidb-planner::outer_to_inner_join` for the source rule identity,
  exactly-once delegated LogicalPlan conversion, and intentionally unchanged
  flag; join predicate semantics and plan-tree ownership remain external.
- [x] Added `tidb-stats::predicate_column_queries` for exact load-all,
  load-table, predicate, cleanup SQL markers and source-ordered decimal column
  ID formatting; schema/session/storage execution remains external.
- [x] Added `tidb-exec::varsamp` for sample-variance finalization: counts at
  most one are NULL, otherwise `variance/(count-1)`, preserving signed float
  results; accumulation, distinct merge, and chunk execution remain external.

The Wave-98 static snapshot is 2,042/324/24/0 production and
15,767/664/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 99 is the next parallel source-owner batch.

## Wave-99 progress (2026-07-17)

- [x] Added `tidb-planner::columnar_index_extra` for the source vector
  columnar-index metadata constructor: fixed vector type, retained index
  identity/derived index ID, ANN query type/metric/top-k, column name, copied
  reference-vector bytes, and source column identity. TiFlash/vector planning
  and protobuf ownership remain external. Direct Go anchor:
  `pkg/planner/core/task_heavy_function_optimize_test.go:36`
  (`TestGetPushedDownTopNHeavyFunctionNotFirstByItem`).
- [x] Added `tidb-stats::ddl_stats_delta` for the exact locked, missing-row,
  and existing-row `stats_meta` SQL branches, ordered arguments, GREATEST
  clamps, and Go-compatible wrapping additions. DDL event/storage/session
  lifecycle remains external. Direct Go anchors:
  `pkg/statistics/handle/ddl/ddl_test.go:1106` (`TestExchangeAPartition`) and
  `:1256` (`TestExchangeAPartitionAndDropTableImmediately`).
- [x] Added `tidb-exec::cume_dist` as an `Iterator` over source-ordered peer
  keys, preserving the `curIdx`/`lastRank` tied-peer algorithm and partial
  state-size contract. Row comparison, window scheduling, and chunk execution
  remain external. Direct Go anchors:
  `pkg/executor/aggfuncs/func_cume_dist_test.go:25` (`TestMemCumeDist`) and
  `pkg/executor/aggfuncs/window_func_test.go:172` (`TestWindowFunctions`).

The Wave-99 static snapshot is 2,039/327/24/0 production and
15,762/669/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 100 is the next parallel source-owner batch.

## Wave-100 progress (2026-07-17)

- [x] Added `tidb-planner::physical_cte_table` for signed CTE storage
  identity, `Scan on CTE_<id>` explain text, and the source
  `findBestTask4LogicalCTETable` index-join/sort rejection gates. Schema,
  statistics, task wiring, recursive planning, and runtime execution remain
  external. Direct Go anchor:
  `pkg/planner/core/tests/redact/redact_test.go:23` (`TestRedactExplain`).
- [x] Added `tidb-stats::gc_batch_count` for Go `forCount` integer division,
  positive-remainder rounding, truncation-toward-zero, and signed overflow
  behavior. Storage/session GC lifecycle remains external. Direct Go anchors:
  `pkg/statistics/handle/storage/gc_test.go:30` (`TestGCStats`) and `:63`
  (`TestGCPartition`).
- [x] Added `tidb-exec::ntile` for the five-field partial state, quotient/
  remainder updates, reset semantics, group advancement, and zero-divisor
  NULL behavior. Typed chunk output, argument coercion, and window scheduling
  remain external. Direct unowned Go anchor:
  `pkg/executor/aggfuncs/func_ntile_test.go:25` (`TestMemNtile`); shared
  window vectors remain attached to their existing source owner.

The Wave-100 static snapshot is 2,036/330/24/0 production and
15,758/673/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). Wave 101 was integrated into the same verified
workspace cycle before the next source-owner batch.

## Wave-101 progress (2026-07-17)

- [x] Added `tidb-exec::lead_lag` for the buffered row cursor, physical
  lead/lag offsets, current-row/default fallback, reset behavior, and partial
  state-size contract. Typed Datum serialization, chunk/window construction,
  and scheduling remain external. Direct Go anchors:
  `pkg/executor/aggfuncs/func_lead_lag_test.go:27` (`TestLeadLag`) and
  `:119` (`TestMemLeadLag`).

The Wave-101 combined static snapshot is 2,035/331/24/0 production and
15,756/675/142/12 test/support obligations. The same fail-fast 12-job
workspace test, strict Clippy, formatting, ledger, parser/plan inventory, and
domain gates pass; the original Go test suite, full planner/statistics/session
integration, and deployable TiDB bootstrap remain incomplete. Wave 102 is the
next parallel source-owner batch.

## Wave-102 progress (2026-07-17)

- [x] Added `tidb-planner::physical_max_one_row` for the pure
  `ExhaustPhysicalPlans4LogicalMaxOneRow` support gates, fixed `ExpectedCnt: 2`,
  and CTE/no-cop metadata forwarding. Physical context/statistics/clone,
  warning publication, task attachment, and executor scalar-subquery limits
  remain external. Direct Go anchor:
  `pkg/executor/test/executor/executor_test.go:2157` (`TestMaxOneRow`).
- [x] Added `tidb-stats::StatsLease` for atomic signed-nanosecond get/set
  semantics. Lease lifecycle, schema loading, and statistics integration remain
  external. Direct Go anchors: `pkg/statistics/integration_test.go:220`
  (`TestShowHistogramsLoadStatus`) and `:266` (`TestColumnStatsLazyLoad`).
- [x] Added `tidb-exec::json_arrayagg` for ordered accumulation, partial merge,
  reset, empty-input NULL, JSON framing, scalar escaping, finite-real guards,
  and spill-state ownership boundaries. Typed Datum/BinaryJSON conversion,
  chunk evaluation, and spill serialization remain external. Direct Go anchors:
  `pkg/executor/aggfuncs/func_json_arrayagg_test.go:27`, `:65`, `:131`, and
  `pkg/executor/aggfuncs/spill_helper_test.go:842`.

The Wave-102 static snapshot is 2,032/334/24/0 production and
15,749/682/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs; the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` (`select json_memberof();`)
remains separately tracked. The original Go test suite, full
planner/statistics/session integration, and deployable TiDB bootstrap remain
incomplete. Wave 103 was integrated into the next verified workspace cycle.

## Wave-103 progress (2026-07-17)

- [x] Added `tidb-planner::logical_cte_table` for the exact `DeriveStats`
  reload-vector interpretation: only a one-element vector is active, false
  reload retains existing statistics, and reload/missing state installs the
  seed with the source changed flag. Concrete `StatsInfo`, schema/expression/
  context, catalog derivation, and CTE plan propagation remain external. Direct
  Go anchor: `pkg/planner/core/casetest/planstats/plan_stats_test.go:281`
  (`TestPlanStatsLoadForCTE`).
- [x] Added `tidb-stats::global_stats_layout` for `newGlobalStats`: Num and
  four equal-length nil statistics-slot arrays, zero count/modify-count, and
  nil missing-partition metadata. Merge algorithms, statistics payloads,
  partition metadata, and storage/session lifecycle remain external. Direct Go
  anchor: `pkg/statistics/handle/globalstats/global_stats_test.go:137`
  (`TestBuildGlobalLevelStats`).
- [x] Added `tidb-exec::json_objectagg` for ordered key/value state,
  source-after-destination merge, duplicate-key last-wins, lexicographic JSON
  object framing, empty-input NULL, and NULL/binary-key rejection. Typed
  evaluation/coercion, finite-number handling, BinaryJSON validation, memory
  tracking, chunk execution, and spill serialization remain external. Direct Go
  anchors: `pkg/executor/aggfuncs/func_json_objectagg_test.go:48`, `:110`,
  `:163`, and `pkg/executor/aggfuncs/spill_helper_test.go:889`.

The Wave-103 static snapshot is 2,029/337/24/0 production and
15,743/688/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs; the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` (`select json_memberof();`)
remains separately tracked. The original Go test suite, full
planner/statistics/session integration, and deployable TiDB bootstrap remain
incomplete. Wave 104 was integrated into the next verified workspace cycle.

## Wave-104 progress (2026-07-17)

- [x] Added `tidb-planner::telemetry` for the exact `IsTiFlashContained`
  traversal: one-level Explain unwrap, physical-plan filtering, TiFlash
  TableReader detection, ExchangeSender classification, ordered child walk, and
  early stop. Concrete physical plans, MPP execution, session telemetry, and
  consumer wiring remain external. Direct Go anchor:
  `pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go:568`
  (`TestMPPSharedCTEScan`).
- [x] Added `tidb-stats::table_id_filter` for source-ordered signed decimal
  formatting as `table_id in (...)`, including the empty form. Cache/storage,
  SQL, and InfoSchema lifecycle remain external. Direct Go anchors:
  `pkg/executor/test/infoschema/infoschema_test.go:171`
  (`TestDataForTableStatsField`) and `:224` (`TestPartitionsTable`).
- [x] Added `tidb-exec::first_row` for the shared `gotFirstRow`/`isNull`
  state machine: first physical row wins including NULL, later batches are
  ignored, merge only fills an unset destination, and reset restores empty
  state. Typed values, per-type sizes, chunk output, memory tracking, and spill
  encoding remain external. Direct Go anchors:
  `pkg/executor/aggfuncs/func_first_row_test.go:27` and `:52`, with spill
  anchors at `pkg/executor/aggfuncs/spill_helper_test.go:941` through `:1349`.

The Wave-104 static snapshot is 2,026/340/24/0 production and
15,728/703/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs; the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` (`select json_memberof();`)
remains separately tracked. The original Go test suite, full
planner/statistics/session integration, and deployable TiDB bootstrap remain
incomplete. Wave 105 was integrated into the next verified workspace cycle.

## Wave-105 progress (2026-07-17)

- [x] Added `tidb-planner::condition_to_dual` for exact `IsConstFalse` and
  `Conds2TableDual` control flow: NULL/false classification, NULL precedence,
  empty/multi-condition cardinality, and plan-cache suppression. Expression
  coercion, statement context, LogicalTableDual construction, and optimizer
  runtime remain external. Direct Go anchor:
  `pkg/planner/core/logical_plans_test.go:241` (`TestAntiSemiJoinConstFalse`).
- [x] Added `tidb-stats::auto_analyze_process_set` for the RWMutex-backed
  tracker/untracker/all/contains global process set over uint64 IDs. Generator,
  callback, sysproctrack, singleton, and auto-analyze execution remain
  external. Direct Go anchors: `pkg/statistics/handle/autoanalyze/exec/exec_test.go:35`
  (`TestExecAutoAnalyzes`) and `:154` (`TestKillInWindows`).
- [x] Added `tidb-exec::bit_agg` for u64 AND/OR/XOR identities, NULL-skipping
  updates, operation-preserving merges, and reset. Typed Eval/coercion, chunk,
  sliding, memory, and spill integration remain external. Direct Go anchors:
  `pkg/executor/aggfuncs/func_bitfuncs_test.go:25` and `:36`, plus
  `pkg/executor/aggfuncs/spill_helper_test.go:801`.

The Wave-105 static snapshot is 2,023/343/24/0 production and
15,722/709/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs; the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` (`select json_memberof();`)
remains separately tracked. The original Go test suite, full
planner/statistics/session integration, and deployable TiDB bootstrap remain
incomplete. Waves 107-112 were integrated below; Wave 113 is the next parallel
source-owner batch.

## Wave-106 progress (2026-07-17)

- [x] Added `tidb-planner::physical_table_sample` for exact
  `PhysicalTableSample.Init` metadata: TableSample plan type, pseudo row count
  one, query-block offset, physical table ID, and Desc flag. Schema/table/
  sampler objects, memory, region sampling, SQL, and executor output remain
  external. Direct Go anchor: `pkg/executor/sample_test.go:111`
  (`TestTableSamplePlan`).
- [x] Added `tidb-stats::stats_meta_save_sql` for source-ordered `stats_meta`
  INSERT/upsert tuple assembly with optional
  `last_stats_histograms_version` and exact empty-spacing behavior. SQL
  execution, startTS, session, and storage remain external. Direct Go anchor:
  `pkg/statistics/integration_test.go:442` (`TestSaveMetaToStorage`).
- [x] Added `tidb-exec::varpop` for non-DISTINCT float64 population variance:
  count/sum/variance state, NULL skipping, source intermediate/merge formulas,
  zero-count branches, population output, and reset. DISTINCT sets, typed
  EvalReal/coercion, chunk/sliding/memory, and spill remain external. Direct Go
  anchors: `pkg/executor/aggfuncs/func_varpop_test.go:28`, `:37`, `:46`, and
  `:54`.

The Wave-106 static snapshot is 2,020/346/24/0 production and
15,716/715/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs; the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` (`select json_memberof();`)
remains separately tracked. The original Go test suite, full
planner/statistics/session integration, and deployable TiDB bootstrap remain
incomplete. Waves 107-112 were integrated below; Wave 113 is the next parallel
source-owner batch.

## Wave-107 progress (2026-07-17)

- [x] Added `tidb-planner::rule_set` for source-shaped rule-ID mask
  membership, order-preserving filtering including duplicates, and the
  intermediate de-correlate-Apply rule-set switch. Concrete rule interfaces,
  memo flags/traversal, rule construction, and optimizer execution remain
  external. Direct Go anchors: `pkg/planner/cascades/rule/ruleset/rule_set.go`
  and `pkg/planner/cascades/old/optimize_test.go:212`
  (`TestAppliedRuleSet`).
- [x] Added `tidb-stats::init_stats_progress` for exact uint64-to-float64
  progress arithmetic, step scaling, base offset, and IEEE zero-denominator
  behavior. Worker goroutines/channels, atomic global progress, task execution,
  and logging remain external. Direct Go anchors:
  `pkg/statistics/handle/initstats/load_stats_page.go:104-107` and
  `pkg/statistics/handle/handletest/initstats/init_stats_test.go:231`
  (`TestConcurrentlyInitStatsWithoutMemoryLimit`).
- [x] Added `tidb-exec::sum_float64` for non-DISTINCT float64 SUM partial
  state: running sum/count, NULL skipping, empty-result NULL, source-empty
  merge short-circuit, and reset. Typed EvalReal/coercion, decimal/int/
  unsigned variants, DISTINCT, sliding/chunk, memory, and spill remain
  external. Direct Go anchors: `pkg/executor/aggfuncs/func_sum_test.go:33`,
  `:50`, `:66`, generated `:44`/`:60`/`:83`, sliding regressions `:89`/`:133`,
  and spill `pkg/executor/aggfuncs/spill_helper_test.go:658`/`:703`.

The Wave-107 static snapshot is 2,017/349/24/0 production and
15,704/727/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs; the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` (`select json_memberof();`)
remains separately tracked. The original Go test suite, full
planner/statistics/session integration, and deployable TiDB bootstrap remain
incomplete. Waves 108-112 were integrated below; Wave 113 is the next parallel
source-owner batch.

## Wave-108 progress (2026-07-17)

- [x] Added `tidb-planner::column_pruning` for the recursive
  `noUnexpectedZeroColumnSchema` invariant, including the source exceptions
  for schema reuse from the first child and `LogicalTableDual`. Logical-plan
  construction, column-pointer identity, pruning mutation, and optimizer/SQL
  execution remain external. Direct Go anchor:
  `pkg/planner/core/logical_plans_test.go:652` (`TestColumnPruning`).
- [x] Added `tidb-stats::global_stats_sql_index` for the exact
  `toSQLIndex` boolean-to-SQL `is_index` mapping (`false -> 0`, `true -> 1`).
  Async merge workers, storage reads, SQL execution, schema/session setup, and
  global-statistics lifecycle remain external. Direct Go anchor:
  `pkg/statistics/handle/globalstats/global_stats_test.go:260`
  (`TestGlobalStatsData`).
- [x] Added `tidb-exec::group_concat` for the non-DISTINCT partial buffer:
  NULL-row skipping, separator/order, source-empty merge no-op,
  destination-empty merge, reset, final NULL, byte-based `max_len` truncation,
  and the lifetime truncation sentinel. Typed EvalString/argument joining,
  warning/error publication, DISTINCT/ORDER BY variants, chunk/memory, and
  spill remain external. Direct Go anchors:
  `pkg/executor/aggfuncs/func_group_concat.go:222-275`, `:285-292`, and
  `pkg/executor/aggfuncs/func_group_concat_test.go:37`, `:42`, `:66`, `:81`.

The Wave-108 static snapshot is 2,014/352/24/0 production and
15,698/733/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs; the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` (`select json_memberof();`)
remains separately tracked. The original Go test suite, full
planner/statistics/session integration, and deployable TiDB bootstrap remain
incomplete. Waves 109-112 were integrated below; Wave 113 is the next parallel
source-owner batch.

## Wave-109 progress (2026-07-17)

- [x] Added `tidb-planner::physical_union_scan` for the source-shaped
  `ExhaustPhysicalPlans4LogicalUnionScan` TiFlash rejection, index-join
  admission outcome, and UnionScan initialization metadata (type/offset,
  conditions, and handle count). Property cloning, expression/handle identity,
  child/task attachment, transaction-buffer reads, and executor output remain
  external. Direct Go anchor:
  `pkg/planner/core/casetest/dag/dag_test.go:274`
  (`TestDAGPlanBuilderUnionScan`).
- [x] Added `tidb-stats::ddl_physical_ids` for exact DDL stats physical-ID
  selection: non-partitioned fallback, ordered partition IDs, dynamic-prune
  global-ID append, and the distinction between nil and empty partition
  metadata. TableInfo/session/DDL dispatch, SQL/storage, and stats history
  remain external. Direct Go anchor:
  `pkg/statistics/handle/ddl/ddl_test.go:203`
  (`TestTruncateAPartitionedTable`).
- [x] Added `tidb-exec::sum_int` for signed and unsigned non-DISTINCT SUM
  partial states: first-row initialization, NULL/empty behavior, checked
  Add/Sub overflow, source/destination merge, reset, and outgoing-before-
  incoming sliding order. EvalInt/dispatch, chunk/memory/spill, and DISTINCT
  remain external. Focused Rust tests mirror the shared original anchors
  `pkg/executor/aggfuncs/func_sum_test.go:33`, `:50`, `:66`, `:89`, and `:133`;
  those top-level Go rows remain owned by the existing shared SUM test domain.

The Wave-109 static snapshot is 2,011/355/24/0 production and
15,696/735/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs; the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` (`select json_memberof();`)
remains separately tracked. The original Go test suite, full
planner/statistics/session integration, and deployable TiDB bootstrap remain
incomplete. Waves 110-112 were integrated below; Wave 113 is the next parallel
source-owner batch.

## Wave-110 progress (2026-07-17)

- [x] Added `tidb-planner::physical_show` for source-shaped `PhysicalShow` and
  `PhysicalShowDDLJobs` initialization metadata (plan kind, pseudo row count,
  and DDL job number) plus the shared IndexJoinProp/non-empty-sort rejection
  gates. SHOW catalog/extractor wiring, schema/context, task construction, and
  execution remain external. Direct Go anchor: `pkg/planner/core/planbuilder_test.go:63`
  (`TestShow`).
- [x] Added `tidb-stats::stats_cache_version` for monotonic cache-version
  updates: `skip_move_forward` preserves the current version, while normal
  updates take the maximum of the current and supplied table versions,
  including empty/smaller/larger inputs. Cache atomics/backends, SQL loading,
  metrics, and Handle/session lifecycle remain external. Direct Go anchor:
  `pkg/statistics/handle/handletest/handle_test.go:111` (`TestVersion`).
- [x] Added `tidb-exec::percentile` for bounded integer/real
  `APPROX_PERCENTILE`: NULL skipping, source-clearing merge, reset, exact
  ordinal rank `ceil(P/100*N)` selection, and P=100 behavior. Typed Eval and
  coercion, introselect internals, chunk/memory/dispatch, and decimal/time/
  duration/enum/set/bit variants remain external. Direct Go anchors:
  `pkg/executor/aggfuncs/func_percentile_test.go:35`, `:51`, and `:63`.

The Wave-110 static snapshot is 2,008/358/24/0 production and
15,690/741/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs; the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` (`select json_memberof();`)
remains separately tracked. The original Go test suite, full
planner/statistics/session integration, and deployable TiDB bootstrap remain
incomplete. Wave 111 was integrated below; Wave 112 is the next parallel
source-owner batch.

## Wave-111 progress (2026-07-17)

- [x] Added `tidb-planner::physical_lock` for source-shaped
  `PhysicalLock` metadata: TiFlash rejection before plan creation, `Lock` plan
  kind, fixed query-block offset zero, opaque lock type, lossless `u64` wait
  seconds, and exact `ExplainInfo` rendering. AST/map/schema/handle cloning,
  stats/context/task/warning wiring, and lock execution remain external. Direct
  Go anchor: `pkg/planner/core/tests/pointget/point_get_plan_test.go:407`
  (`TestIssue52592ForNextGen`).
- [x] Added `tidb-stats::topn_merge_task` for the source
  `TopnStatsMergeTask` range descriptor and constructor/accessors, preserving
  direct start/end storage without validation. Worker scheduling/channels,
  TopN/histogram merge arithmetic, cancellation, concurrency, and benchmark
  performance remain external. Direct Go anchor:
  `pkg/statistics/handle/globalstats/topn_bench_test.go:94`
  (`BenchmarkMergePartTopN2GlobalTopNWithHists`).
- [x] Added `tidb-exec::avg_float64` for non-DISTINCT float64 AVG state:
  sum/count, NULL skipping, empty NULL, source merge, reset, and
  incoming-before-outgoing sliding order. Eval/coercion, decimal/DISTINCT,
  rounding/context, chunk/memory, and spill remain external. Direct Go anchors:
  `pkg/executor/aggfuncs/func_avg_test.go:27`, `:37`, and `:48`.

The Wave-111 static snapshot is 2,005/361/24/0 production and
15,685/746/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs; the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` (`select json_memberof();`)
remains separately tracked. The original Go test suite, full
planner/statistics/session integration, and deployable TiDB bootstrap remain
incomplete. Wave 112 was integrated below; Wave 113 is the next parallel
source-owner batch.

## Wave-112 progress (2026-07-17)

- [x] Added `tidb-planner::physical_table_dual` for source-shaped
  `PhysicalTableDual.Init` metadata: `Dual` plan kind, query-block offset,
  `rows:<RowCount>` explain text, unconditional IndexJoin rejection, and
  row-count-dependent sort admission. Schema/output names, context/stats/task
  wiring, memory, and mock-datasource fallback remain external. Direct Go
  anchor: `pkg/planner/core/casetest/cbotest/cbo_test.go:367`
  (`TestTableDual`).
- [x] Added `tidb-stats::json_stats_version` for old JSON StatsVer fallback:
  explicit versions always win; missing versions infer legacy version 1 when
  NDV or null count is positive, otherwise version 0. JSON decoding, schema
  matching, stats-handle loading, SQL/storage, and session lifecycle remain
  external. Direct Go anchor: `pkg/statistics/handle/storage/dump_test.go:582`
  (`TestLoadStatsFromOldVersion`).
- [x] Added `tidb-exec::minmax_deque` for the source MinMaxDeque helper:
  pair storage, push/pop/front/back, reset, expiry dequeue, and monotonic
  max/min enqueue including equal-value eviction. Typed MAX/MIN aggregates,
  evaluator coercion, window callbacks, chunk/memory, and spill remain
  external. Direct Go anchors: `pkg/executor/aggfuncs/func_max_min_test.go:335`
  (`TestDequeReset`) and `:345` (`TestDequePushPop`).

The Wave-112 static snapshot is 2,002/364/24/0 production and
15,681/750/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs; the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` (`select json_memberof();`)
remains separately tracked. The original Go test suite, full
planner/statistics/session integration, and deployable TiDB bootstrap remain
incomplete. Wave 113 is the next parallel source-owner batch.

## Wave-113 progress (2026-07-17)

- [x] Added `tidb-planner::logical_lock`, preserving the raw lock-type
  discriminants and supported FOR UPDATE/FOR SHARE sets while rejecting
  skip-locked, NONE, and unknown modes. The exact source anchor is
  `pkg/planner/core/integration_test.go:1466` (`TestPointGetWithSelectLock`).
- [x] Added `tidb-stats::stats_lock_table`, preserving the fully qualified table
  name and the nil-versus-explicit-empty partition map distinction. Exact
  anchors are `pkg/statistics/handle/lockstats/lock_stats_test.go:186`
  (`TestAddLockedTables`) and `:260` (`TestAddLockedPartitions`).
- [x] Added `tidb-exec::count_distinct_int`, preserving typed-int NULL skipping,
  deduplication, cardinality, source-preserving partial merge, and reset.
  Exact anchors are `pkg/executor/aggfuncs/func_distinct_agg_test.go:26`
  (`TestParallelDistinctCount`) and `pkg/executor/aggfuncs/func_count_test.go:115`
  (`TestMemCount`).

SQL AST/session wiring, physical lock execution, lock SQL and storage lifecycle,
other DISTINCT types and approximate distinct, typed Eval/chunk/memory/spill
integration, and runtime scheduling remain external. The regenerated ledgers are
1,996/370/24/0 production and 15,673/758/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Workspace tests, strict Clippy, formatting,
and parser/plan/domain/ledger gates pass with 12 jobs; the pinned Go restore
failure remains tracked separately. Wave 114 is the next parallel source queue.

## Wave-114 progress (2026-07-17)

- [x] Added `tidb-planner::physical_exchange_receiver`, preserving
  `ExchangeReceiver` plan identity, root query-block offset `0`, lossless
  `u64` stream count, and source `ExplainInfo` rendering. The exact source test
  anchor is `pkg/planner/core/integration_test.go:904`
  (`TestTiFlashFineGrainedShuffleWithMaxTiFlashThreads`).
- [x] Added `tidb-stats::pseudo_cache_policy`, preserving the source admission
  policy: non-partitioned pseudo stats are admitted, partitioned stats are
  admitted only below cache length 64, and temporary tables are rejected. The
  exact direct anchor is `pkg/statistics/handle/handletest/handle_test.go:1100`
  (`TestStatsCacheShouldNotCacheTemporaryTable`).
- [x] Added `tidb-exec::window_value_int`, preserving already-evaluated integer
  FIRST_VALUE, LAST_VALUE, and NTH_VALUE transitions, including NULL capture,
  batch-spanning one-based selection, reset, and unreached behavior. The exact
  anchor is `pkg/executor/aggfuncs/func_value_test.go:63` (`TestMemValue`).

MPP task/fragment/schema/protobuf/runtime wiring, pseudo-table construction and
cache/session lifecycle, system-schema checks, typed evaluators, all value
domains, chunk/memory/window dispatch, and runtime scheduling remain external.
The regenerated ledgers are 1,996/370/24/0 production and
15,673/758/142/12 test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED).
Workspace tests, strict Clippy, formatting, and parser/plan/domain/ledger gates
pass with 12 jobs; the pinned Go restore failure remains tracked separately.
Wave 115 was integrated below; Wave 116 is the next parallel source queue.

## Wave-115 progress (2026-07-17)

- [x] Added `tidb-planner::physical_selection`, preserving `Selection` plan
  identity, caller query-block offset, condition explain text, zero-stream
  passthrough, and the exact `expr, stream_count: N` suffix. The source test
  anchor is `pkg/planner/core/casetest/mpp/mpp_test.go:673`
  (`TestPushDownSelectionForMPP`).
- [x] Added `tidb-exec::spill_count`, preserving native-endian int64
  `partialResult4Count` serialization, strict row decoding, reusable-buffer
  behavior, and sequential row consumption. The source test anchor is
  `pkg/executor/aggfuncs/spill_helper_test.go:73`
  (`TestPartialResult4Count`).
- [x] Added `tidb-stats::cache_metrics_labels`, preserving the six counter
  labels (`miss`, `hit`, `update`, `del`, `evict`, `reject`) and two gauge labels
  (`track`, `capacity`) in source order. The source benchmark anchor is
  `pkg/statistics/handle/cache/bench_test.go:99`
  (`BenchmarkStatsCacheLFUCopyAndUpdate`).

Expression evaluation, MPP task/runtime wiring, chunk/spill lifecycle beyond
the bounded count row, Prometheus registration/handles and cache concurrency,
typed aggregate variants, and session/storage integration remain external. The
regenerated ledgers are 1,993/373/24/0 production and
15,670/761/142/12 test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED).
Workspace tests, strict Clippy, formatting, and parser/plan/domain/ledger gates
pass with 12 jobs. The evidence-fragment loader now rejects escaped `\t`
headers, preventing malformed TSV headers from silently bypassing ledger checks.
The pinned Go restore failure remains tracked separately. Wave 116 was
integrated below; Wave 117 is the next parallel source queue.

## Wave-116 progress (2026-07-17)

- [x] Added `tidb-planner::physical_limit`, preserving `Limit` plan identity,
  caller query-block offset, lossless offset/count metadata, and the disable,
  marker, enable, and unknown-mode `ExplainInfo` branches over caller-owned
  partition/prefix text. The exact source test anchor is
  `pkg/planner/core/casetest/physicalplantest/physical_plan_test.go:1600`
  (`TestLimitPushdown`).
- [x] Added `tidb-exec::pd_approximate_count`, preserving the direct
  underscore-joined table-count cache key, bounded TTL/LRU hit/miss/eviction
  behavior, and source `hasPD` return contract with a caller-supplied clock.
  The exact source/test anchors are `pkg/executor/internal/pdhelper/pd.go:69-85`
  and `pkg/executor/internal/pdhelper/pd_test.go:42` (`TestTTLCache`).
- [x] Added `tidb-stats::ddl_event_match`, preserving first-match selection
  and no-match timeout behavior for the DDL test utility. The exact source
  test anchor is
  `pkg/statistics/handle/autoanalyze/priorityqueue/queue_ddl_handler_test.go:885`
  (`TestVectorIndexTriggerAutoAnalyze`).

Typed planner properties/partition columns, PD/storage and restricted-SQL
access, channel/ticker timing and notifier decoding, and full planner,
executor, statistics, session, and SQL lifecycle integration remain external.
The regenerated ledgers are 1,990/376/24/0 production and
15,667/764/142/12 test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED).
Workspace tests, strict Clippy, formatting, parser/plan/domain/ledger gates,
and the evidence-header guard pass with 12 jobs. Wave 117 was integrated below;
Wave 118 is the next parallel source queue.

## Wave-117 progress (2026-07-17)

- [x] Added `tidb-planner::physical_union_all`, preserving `Union` plan
  identity, query-block offset, MPP flag, and the source Exhaust gates for
  sort, TiFlash, MPP partition, and root/non-root candidate ordering. The
  exact source test anchor is `pkg/planner/core/casetest/mpp/mpp_test.go:446`
  (`TestMppUnionAll`).
- [x] Added `tidb-exec::apply_cache`, preserving byte-key/value memory charge,
  over-quota rejection, oldest-entry LRU eviction, and get-touch/accounting
  behavior. The exact source/test anchors are
  `pkg/executor/internal/applycache/apply_cache.go:35-43,76-101` and
  `pkg/executor/internal/applycache/apply_cache_test.go:30`
  (`TestApplyCache`).
- [x] Added `tidb-stats::mock_statistics_shape`, preserving fixture column and
  index counts plus the with-CMSketch/TopN/histogram switches and total item
  count. The exact benchmark anchor is
  `pkg/statistics/handle/cache/bench_test.go:125`
  (`BenchmarkLFUCachePutGet`).

Child properties/schema/stats/task/runtime, typed chunk-list and memory
tracker/session-quota wiring, statistics object allocation/cache concurrency,
and benchmark runtime remain external. The regenerated ledgers are
1,987/379/24/0 production and 15,664/767/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Wave 118 was integrated below; Wave 119
is the next parallel source queue.

## Wave-118 progress (2026-07-17)

- [x] Added `tidb-planner::physical_apply`, preserving `Apply` plan identity,
  query-block offset, and the exact `PhysicalJoinImplement() == false`
  boundary. The exact source test anchor is
  `pkg/planner/core/casetest/physicalplantest/physical_plan_test.go:1537`
  (`TestPhysicalApplyIsNotPhysicalJoin`).
- [x] Added `tidb-exec::next_io_acc`, preserving positive row/cell guards,
  accumulator reset/reuse, and child/parent/tracking admission policy. The
  exact source/test anchors are `pkg/executor/internal/exec/executor.go:42-89`
  and `pkg/executor/internal/exec/executor_test.go:35`
  (`TestNextIOAccAddInputCountsRowsWithZeroCols`).
- [x] Added `tidb-stats::stats_request_matcher`, preserving the exact
  `internal_StatsForegroundPriority` predicate and matcher description used by
  auto-analyze request expectations. The exact source/test anchors are
  `pkg/statistics/handle/util/test/ctx_matcher.go:24-36` and
  `pkg/statistics/handle/autoanalyze/autoanalyze_test.go:407`
  (`TestCleanupCorruptedAnalyzeJobsOnCurrentInstance`).

Hash-join/subquery properties and runtime, executor atomics/provider/pool and
RUV2 publication, context extraction/request propagation, gomock/SQL/session
lifecycle, and full planner/executor/statistics integration remain external.
The regenerated ledgers are 1,984/382/24/0 production and
15,661/770/142/12 test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED).
Wave 118 was integrated below; Wave 119 is the next parallel source queue.

## Wave-119 progress (2026-07-17)

- [x] Added `tidb-planner::physical_projection`, preserving `Projection` plan
  identity, caller query-block offset, opaque expression-list explain text,
  and the exact uint64 `stream_count` suffix. The exact source test anchor is
  `pkg/planner/core/casetest/mpp/mpp_test.go:710`
  (`TestPushDownProjectionForMPP`).
- [x] Added `tidb-exec::cluster_index_id`, preserving clustered-index identity
  selection: PK-as-handle maps to zero, common handles select the primary
  index with source zero default, and non-clustered/rowid tables return none.
  The exact source/test anchors are
  `pkg/executor/internal/exec/indexusage.go:130-148` and
  `pkg/executor/internal/exec/indexusage_test.go:447`
  (`TestIndexUsageReporterWithClusterIndex`).
- [x] Added `tidb-stats::predicate_column_query_mode`, preserving the wrapper
  transaction boundary: `LoadColumnStatsUsage` omits `FlagWrapTxn`, while
  `GetPredicateColumns` applies it. The exact source/test anchors are
  `pkg/statistics/handle/usage/predicate_column.go:47-62` and
  `pkg/statistics/handle/usage/predicate_column_test.go:103`
  (`TestAnalyzeTableWithTiDBPersistAnalyzeOptionsEnabled`).

Typed projection/schema/redaction and MPP runtime, table/index metadata and
collector/reporting lifecycle, session pools/SQL execution/predicate storage,
and full planner/executor/statistics integration remain external. The
regenerated ledgers are 1,981/385/24/0 production and
15,658/773/142/12 test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED).
Wave 121 is the next parallel source queue.

## Wave-120 progress (2026-07-17)

- [x] Added `tidb-planner::physical_shuffle`, preserving `Shuffle` plan
  identity and query-block offset, hash/range splitter discriminants, and
  source-shaped `ExplainInfo` concurrency/data-source formatting. The exact
  source/test anchors are `pkg/planner/core/operator/physicalop/physical_shuffle.go:155`
  and `pkg/planner/core/casetest/integration_test.go:245`
  (`TestTiFlashFineGrainedShuffle`).
- [x] Added `tidb-stats::index_usage_key`, preserving the exact table-ID/index-ID
  pair used as the index-usage lookup identity. The exact source/test anchors
  are `pkg/statistics/handle/usage/index_usage.go:59-62` and
  `pkg/statistics/handle/usage/index_usage_integration_test.go:29`
  (`TestGCIndexUsage`).
- [x] Added `tidb-exec::mock_global_accessor`, preserving ordinary and
  test-suite variable maps, unknown-variable errors, default authentication
  plugin validation plus the validation-bypassing setter, and
  `tikv_gc_life_time` readback. The exact source/test anchors are
  `pkg/sessionctx/variable/mock_globalaccessor.go:23-130` and
  `pkg/sessionctx/variable/mock_globalaccessor_test.go:26` (`TestMockAPI`).

Live planner child/task/schema/expression partitioning and receivers, index
usage collection/GC/worker lifecycle, SessionVars hooks/context cancellation,
SQL error/OpenCensus cleanup, and full planner/executor/statistics/session
integration remain external. The regenerated ledgers are 1,978/388/24/0
production and 15,655/776/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Wave 121 is the next parallel source queue.

## Wave-121 progress (2026-07-17)

- [x] Added `tidb-planner::physical_exchange_sender`, preserving
  `ExchangeSender` identity/root offset zero and ExplainInfo branches for
  exchange labels, compression names/fallback, hash-column text, ordered task
  IDs, and the uint64 `stream_count` suffix. The exact source/test anchors are
  `pkg/planner/core/operator/physicalop/physical_exchange_sender.go:222` and
  `pkg/planner/core/casetest/mpp/mpp_test.go:78`
  (`TestMPPExchangeSender`).
- [x] Added `tidb-stats::stats_table_snapshot`, preserving the
  `AssertTableEqual` contract for realtime/modify counts, column/index
  cardinality, per-ID item equality, opaque payload/nil shape, and existence
  bytes. The exact source/test anchors are
  `pkg/statistics/handle/internal/testutil.go:25-55` and
  `pkg/statistics/handle/handletest/statstest/stats_test.go:307`
  (`TestStatsStoreAndLoad`, helper call at line 333).
- [x] Added `tidb-exec::vec_group_checker_int`, preserving integer/NULL
  consecutive grouping, cross-chunk first-group continuity, offsets/count,
  cursor ranges, exhaustion/reset, and the non-empty-chunk boundary. The exact
  source/test anchors are
  `pkg/executor/internal/vecgroupchecker/vec_group_checker.go:80-151,524-564`
  and `pkg/executor/internal/vecgroupchecker/vec_group_checker_test.go:141`
  (`TestVecGroupChecker4GroupCount`).

MPP task/schema/context/index/PB/cost/clone/runtime, statistics.Table
reconstruction and payload encoding/storage lifecycle, expression/chunk
evaluation/allocation, datum/key codecs, collations, non-integer/vector group
types, and stream-aggregation wiring remain external. The regenerated ledgers
are 1,975/391/24/0 production and 15,652/779/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). Wave 122 is the next parallel source queue.

## Wave-122 progress (2026-07-17)

- [x] Added `tidb-planner::physical_window`, preserving Window plan identity,
  initialization offset, inherited uint64 fine-grained-shuffle stream-count
  clone state, and the optional ExplainInfo suffix. The exact source/test
  anchors are `pkg/planner/core/operator/physicalop/physical_window.go:480`
  and `pkg/planner/core/plan_test.go:681`
  (`TestCloneFineGrainedShuffleStreamCount`).
- [x] Added `tidb-exec::concurrent_entry_map`, preserving 320-shard routing,
  lock-protected prepend chains, lookup/snapshot iteration, length/empty,
  row identity, and portable accounting. The exact source/test anchors are
  `pkg/executor/join/concurrent_map.go:20-79` and
  `pkg/executor/join/concurrent_map_test.go:27,70`
  (`TestConcurrentMap`, `TestConcurrentMapMemoryUsage`).
- [x] Added `tidb-stats::stats_cache_inner`, preserving the eleven-method
  cache interface lifecycle (`Get`, `Put`, `Del`, `Cost`, `Values`, `Len`,
  `Copy`, `SetCapacity`, `Close`, `TriggerEvict`, and `WaitForAsyncUpdates`)
  over opaque values. The exact source/test anchors are
  `pkg/statistics/handle/cache/internal/inner.go:18-50` and
  `pkg/statistics/handle/cache/internal/lfu/lfu_cache_test.go:49`
  (`TestLFUFreshMemUsage`).

PhysicalSort behavior sharing the planner test, MPP task/schema/context/index
and runtime behavior, Go `hack.MemAwareMap` ABI and exact memory constants,
hash-join containers and trackers, LFU admission/eviction/async residency and
metrics, and complete statistics table/storage lifecycle remain external. The
regenerated ledgers are 1,972/394/24/0 production and 15,648/783/142/12
test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED). Wave 123 is the
next parallel source queue.

## Wave-123 progress (2026-07-17)

- [x] Added `tidb-planner::physical_sort`, preserving Sort identity and query
  block offset, source `ByItems` text and `:desc` formatting, partial-sort and
  inherited stream-count metadata, deep clone behavior, and monotonic memory
  accounting. Exact new test owners are
  `pkg/planner/core/physical_plan_test.go:582` and
  `pkg/planner/core/planbuilder_test.go:277`; the shared stream-count test at
  `pkg/planner/core/plan_test.go:681` remains singly owned by Wave 122.
- [x] Added `tidb-exec::join_table_meta`, preserving the source key-mode,
  key-inlining/fixed-length, mixed-sign serialization, row-column ordering,
  null-map alignment, and thread-safe read decisions. All six exact anchors in
  `pkg/executor/join/join_table_meta_test.go:27-274` now have explicit partial
  owners and source-backed Rust vectors.
- [x] Added `tidb-stats::StatsPool`, preserving the opaque goroutine/session
  resource access and close boundary used by
  `pkg/statistics/handle/util/util_test.go:75`.

Typed expression/planner wiring, join row encoding and runtime execution,
live `FieldType`/collation/chunk/codec behavior, concrete pool construction and
session cleanup, and complete statistics lifecycle remain external. The
regenerated ledgers are 1,969/397/24/0 production and 15,639/792/142/12
test/support obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED). The batched
workspace tests passed; after strict Clippy requested `div_ceil`, the focused
six-test join-metadata suite and full workspace Clippy passed, together with
formatting and the static ledger/parser/plan/domain gates. Wave 124 is the next
parallel source queue.

## Wave-124 progress (2026-07-17)

- [x] Added `tidb-planner::physical_topn`, preserving TopN identity/query-block
  offset, ordinary and independently normalized ByItems/PartitionBy text,
  exact redaction branches, prefix metadata, clone ownership, and monotonic
  memory accounting. Exact anchors are
  `pkg/planner/core/planbuilder_test.go:340` and
  `pkg/planner/core/integration_test.go:1897`.
- [x] Added `tidb-exec::OrderedApplyBuffer`, preserving out-of-order result
  buffering, consecutive sequence draining, empty-result advancement, full and
  idle partial flushing, EOF/error/cancellation terminal behavior, and nested
  composition. All seven ordered-Apply tests in
  `pkg/executor/parallel_apply_test.go:560-969`, including panic and kill
  signaling, now have exact partial owners.
- [x] Added `tidb-stats::BoundedMinHeap` from the complete generic Go source,
  preserving comparator direction, fixed capacity, strict-better replacement,
  ties, zero capacity, non-mutating best-to-worst output, constructor failures,
  and Go's direct comparator negation. All seven exact tests in
  `pkg/util/generic/bounded_min_heap_test.go:44-186` are owned.

Optimizer/task/protobuf/storage TopN wiring, real Apply executors/chunks/
channels/correlation/joiners/SQLKiller timing, and statistics histogram/TopN
consumers remain external. The regenerated ledgers are 1,966/400/24/0
production and 15,623/808/142/12 test/support obligations
(UNTRIAGED/PARTIAL/COVERED/BLOCKED). The reused-target workspace test batch
passed. Strict Clippy exposed the missing Rust `is_empty` companion; after
adding and testing it, the focused nine-test heap suite, full workspace
Clippy, formatting, and all static ledger/parser/plan/domain gates passed.
Wave 125 is the next parallel source queue.

## Wave-125 progress (2026-07-17)

- [x] Added `tidb-planner::physical_table_reader`, preserving request/store
  type separation, table-plan clone shape and one-node identity, scan
  cardinality/error text, explain/normalized output, and monotonic memory.
  Exact tests are `physical_plan_test.go:151` and `planbuilder_test.go:312`.
- [x] Added `tidb-exec::statement_rows_reader`, preserving bounded buffered
  reads, pull/close/error retention, empty-pull EOF detachment, and Go's
  repeated explicit-close delegation. Exact statement-summary tests are
  `stmtsummary_test.go:34,80,127`.
- [x] Added the dependency-closed `tidb-distsql::distsql_runtime` leaf for
  DAG/MPP/ANALYZE/CHECKSUM result metadata, runtime plan IDs, memory/paging and
  chunk-encoding policy, TiFlash outgoing settings, endian/alignment selection,
  and KV execution-counter binding. The exact source owner is
  `pkg/distsql/distsql.go:1-290`; the seven exact test owners are
  `pkg/distsql/distsql_test.go:42,61,73,82,106,154,179`.
- [x] Regenerated source/test ledgers for the three production owners and 12
  exact original tests, adding the two planner domain rows required by its
  already-split Go test files. Current counts are
  1,963/403/24/0 production and 15,611/820/142/12 test/support obligations
  (UNTRIAGED/PARTIAL/COVERED/BLOCKED). DistSQL transport/client RPC,
  protobuf/response streaming, concrete tracker lifecycle, and full runtime
  statistics aggregation remain external.

## Wave-126 progress (2026-07-17)

- [x] Added the shared `tidb-error` authority for all parser-MySQL and TiDB
  errno constants/messages, SQLSTATE lookup, SQLError display, and Go-compatible
  argument redaction. Protocol, transaction, and executor sysvar consumers no
  longer carry independent numeric/message registries.
- [x] Extended the sole `Datum` authority with MinNotNull/MaxValue range
  sentinels, exact key tags, and a distinct DecodeRange path. DecodeOne still
  rejects `maxFlag`; ordinary expression/executor ingress rejects range bounds
  explicitly rather than converting them to NULL or reaching coercion panics.
  Context-aware ordinary Datum comparison and planner ranger consolidation
  remain open.
- [x] Added `tidb-txnkv::KeyRanges` with the exact first/middle/last storage
  cases, 18 Go split vectors, Go `%q` byte/Unicode behavior, logical sequence
  equality, reset, immutable slicing, and safe protobuf conversion. Unsafe Go
  aliasing and Go-layout memory constants remain intentionally partial.
- [x] Made source-only and test-only ownership transfers checked queue
  operations, so consolidation never invents a paired source or test anchor.

The accepted snapshot is 1,951/410/29/0 production and
15,568/843/163/11 test/support obligations. The consolidated 12-job gate passes
the complete workspace test suite, strict all-target Clippy, formatting, 16
queue regressions, all source/test/parser/plan/domain inventories, parser
dependency isolation, and diff checks.

## Wave-127 progress (2026-07-17)

- [x] Added the source-shaped DistSQL paging policy and made
  `PagingConfig::default()` consume the same minimum/maximum authority.
- [x] Added the shared datatype output formatter, including the four-state
  indent/flat machine, cross-call state, dangling-percent `NOVERB` behavior,
  and valid-UTF-8 output replacement table. Unsupported `fmt` surface remains
  ledger-visible rather than being approximated.
- [x] Added the transaction union iterator with exact forward/reverse merge,
  dirty override, tombstone, error-order, and close behavior. Constructor
  failure returns both owned input iterators so the caller retains Go's
  explicit cleanup responsibility.
- [x] Extended claim arbitration to exact declared Rust paths. Independent Go
  anchors can no longer race on a crate root or shared module during parallel
  translation.

The accepted snapshot is 1,948/412/30/0 production and
15,560/845/169/11 test/support obligations. The consolidated 12-job gate passes
the complete workspace test suite, strict all-target Clippy, formatting, 17
queue regressions, every static inventory, parser dependency isolation, and
diff checks.

## Wave-128 progress (2026-07-17)

- [x] Consolidated source-shaped field-name metadata in `tidb-datatype`, ported
  exact expression resolution through the shared MySQL error catalog, and
  wired ordinary executor projection matching without falsely claiming absent
  redundant/non-explicit metadata.
- [x] Added coprocessor cache-key bytes, paging-marker equivalence, 64-bit value
  cost, and the four source-tested admission/configuration contracts. Cache
  lifecycle and non-finite/out-of-range Go float conversion remain explicit.
- [x] Added transaction buffer/middle/snapshot batch reads with precedence,
  tombstones, commit timestamps, and snapshot staging. The distinct client-go
  error adapter and complete shared KV value/options authorities remain open.

The accepted snapshot is 1,944/416/30/0 production and
15,550/849/175/11 test/support obligations. One reused 12-job integration gate
passes workspace tests, strict all-target Clippy, formatting, 17 queue
regressions, every static inventory, parser dependency isolation, and diff
checks. Cross-review and integration caught signed-`int` drift, a false
not-found identity, and the benchmark's intentional case-insensitive duplicate
outcome before any coverage claim was promoted.

## Wave-129 progress (2026-07-17)

- [x] Added exact datatype truncation policy and BinaryLiteral `ToInt`
  integration, retaining value-plus-error behavior and explicit class/context
  prerequisites.
- [x] Added the concurrent DistSQL read-byte EMA with direct source-zero time,
  decay, out-of-order, and concurrency semantics; protobuf/RPC feedback and
  extreme float conversion remain open.
- [x] Added transaction-driver error conversion for the currently typed
  client errors, exact oversize messages, and chain-preserving passthrough;
  shared terror result-undetermined identity remains partial.
- [x] Split claim release into recovery and guarded integrated modes so a
  successfully integrated slice cannot remain `ready` and re-enter the queue.

The accepted snapshot is 1,941/419/30/0 production and
15,538/852/184/11 test/support obligations. The complete reused-target 12-job
gate passes workspace tests, strict all-target Clippy, formatting, 18 queue
regressions, parser dependency isolation, and every static inventory. The
conversion-context consumer stays blocked on real warning/context prerequisites
instead of bypassing them.

## Wave-130 progress (2026-07-17)

- [x] Added the shared fixed `terror` class/code/RFC authority, root-cause
  equality, registered message generation, and SQL conversion for registered
  instances and the fixed Global prototypes. Mutable class registration,
  logging/fatal helpers, JSON wire compatibility, and Go stack capture remain
  explicit runtime gaps.
- [x] Migrated transaction `ResultUndetermined` conversion to the shared
  `global:2` identity and completed the four original wrapper-shape assertions
  without creating a transaction-local duplicate.
- [x] Consolidated warning publication into the sole mutable warning-handler
  authority and covered all four original warning tests, including cap,
  batch, copy-capacity, independent-storage, truncate, reset, count, JSON
  level/message, and ignore semantics.
- [x] Rejected and deleted a replica-read adapter whose switch was correct but
  whose test-only storage-client enum preceded the canonical TiKV client
  boundary. The mapping remains untriaged until that real consumer authority
  exists; correct isolated code is not accepted as dependency-ready progress.

The accepted snapshot is 1,940/420/30/0 production and
15,527/851/193/14 test/support obligations. Independent cross-review caught
the global-registration semantic mismatch and incomplete original-test
translation before integration. The complete reused-target 12-job gate passes
workspace tests, strict all-target Clippy, formatting, 18 queue regressions,
parser dependency isolation, and every source/test/parser/plan inventory.

## Wave-131 progress (2026-07-17)

- [x] Added the single datatype `ConversionFlags`/`ConversionContext`
  authority, exact ten-bit accessors/defaults, copy-on-write flags/location,
  and typed warning input port. BinaryLiteral truncation, executor error
  policy, statement pushdown, and the existing warning handler consume it;
  arbitrary non-terror warning payloads and timezone transition rules remain
  explicit gaps.
- [x] Added the complete TiFlash replica-read policy and connected it through
  the existing `DistSqlContext -> ReadRequestMetadata -> KvRequestMetadata`
  production chain. All discriminants, predicates, strings, unknown fallbacks,
  and the remote-read limit are covered without introducing client/RPC types.
- [x] Added `RowKeyPrefixFilter` directly over the existing owned `Key` and
  `next_until` authorities, including the original embedded-NUL test and a
  composition regression. Storage mutation helpers remain unported until a
  real mutable transaction/retriever exists.

The accepted snapshot is 1,937/422/31/0 production and
15,523/851/197/14 test/support obligations. The complete reused-target 12-job
gate passes workspace tests, strict all-target Clippy, formatting, 18 queue
regressions, parser dependency isolation, and every static inventory. The gate
also forced the conversion warning trait implementation into the warning
publication owner, eliminating a source-shard-only module graph instead of
suppressing dead-code diagnostics.

## Wave-132 progress (2026-07-17)

- [x] Replaced three executor DISTINCT linear scans with one encoded-tuple
  `DistinctChecker` consumed by ordinary aggregate folding,
  `COUNT(DISTINCT tuple)`, and `GROUP_CONCAT(DISTINCT ...)`. The complete
  original `TestDistinct` and its file obligation are covered. Review and the
  table differential ring forced GROUP_CONCAT to dedupe evaluated tuples
  before rendering and to reuse the existing byte-preserving Datum CONCAT
  authority; distinct `('ab','c')`/`('a','bc')` tuples now remain `abc,abc`
  without an AST-literal round trip. Aggregate SELECT remains outside the
  narrower shared Session/COM_QUERY capability envelope.
- [x] Consolidated the fault-injection wrapper onto the existing
  `Getter`/`BatchGetter`/`ValueEntry`/`Key` authority and removed the duplicate
  generic `KvRead` value/batch model. This is explicitly a future-client
  enabler, not a production transaction connection: no real storage implements
  `KvStorage`, and option/context, Go value-plus-error and nil-map shapes,
  root-cause identity, and Begin-on-error wrapper behavior remain partial.
- [x] Attached the already-published Session status snapshot to live COM_QUERY
  OK/EOF construction. Published affected rows now reach DML OK packets;
  caller warning metadata cannot spoof query EOF/OK warning counts; and
  connection-owned status flags, deprecated-EOF, and protocol-4.1 negotiation
  remain intact. Live warning producers/list iteration, nonempty info, and
  shared-cluster auto-ID/last-insert-ID production remain explicit gaps.
- [x] Made batch integration receipted and immutable: explicit
  `--integrated`/`--abandon` release modes, full claim/workspace snapshots
  before and after the shared gate, separate post-gate promotion fingerprints,
  double-begin rejection, one-time multi-claim receipt consumption, and stale
  implementation/test/script/domain-manifest rejection. Strict all-target
  Clippy now runs before the test sweep so compile/lint failures terminate the
  batch earlier.

The accepted snapshot is 1,935/424/31/0 production and
15,520/852/199/14 test/support obligations. Cross-review and the full gate
caught five material defects before release: GROUP_CONCAT tuple loss, a
negative-integer AST round-trip overflow, an unreachable aggregate COM_QUERY
claim, an unavailable shared auto-ID claim, and two compile-only test/wrapper
errors. The final reused-target 12-job gate passes strict all-target Clippy,
the complete workspace and differential tests, formatting, 24 queue
regressions, parser dependency isolation, and every static inventory. All
three receipts were consumed and active claims are zero.

## Wave-133 progress (2026-07-17)

- [x] Ported canonical table-record key construction (`t` plus the
  mem-comparable signed table ID plus `_r` plus opaque handle bytes) and
  `TableHandlesToKVRanges` over the existing typed Int/Common/Partition
  handles. Sorted integer runs coalesce with exact `MaxInt64` handling,
  partition boundaries remain physical-table boundaries, common handles use
  point ranges, and row-count hints feed the existing pre-transport
  `KvRequestBuilder`. Both original request-builder tests are covered. This is
  not yet a session/RPC/region/TiKV runtime connection.
- [x] Connected dependency-closed table-less top-level `COUNT` through the
  shared Session and automatic framed COM_QUERY path without adding another
  aggregate evaluator. The synthetic input row now flows through the existing
  `Database::aggregate` owner, including `WHERE false -> COUNT(*) = 0`, and
  result metadata matches Go exactly: LONG_LONG, binary plus not-null flags,
  field length 21, decimal 0, binary collation. Table-backed aggregation,
  grouping/windows, volatile expressions, variables, and subqueries remain
  outside this capability.
- [x] Connected the first real nonzero Session warning producer. Session
  `tidb_enable_noop_functions=WARN` plus `tx_read_only` aliases append the
  existing no-op diagnostic directly to canonical `StatementStatus`; ordered
  multiplicity, statement reset, caller-warning spoof prevention, and COM_QUERY
  OK warning counts are exercised. Warning errno identity, SHOW WARNINGS,
  globals, other no-op variables, and other producers remain open.
- [x] Tightened parallel execution around real write sets. The queue can now
  atomically amend active claims with newly discovered source anchors as well
  as tests. Session slices that both touched `cluster.rs` were consolidated
  into one integration claim instead of hiding a write conflict; future waves
  must partition mutable Rust paths before assigning semantic lanes.

The accepted snapshot is 1,933/426/31/0 production and
15,515/855/201/14 test/support obligations. Cross-review and the receipted gate
caught wrong COUNT wire flags/width, a warning capability that rejected its own
live SET path, missing request-builder source ownership, false runtime
connectivity wording, invalid CommonHandle fixtures, and table-less aggregate
misrouting before release. After focused repairs, the reused-target 12-job
gate passes strict all-target Clippy, the complete workspace and differential
tests, formatting, 25 queue regressions, parser dependency isolation, and every
static inventory. Both receipts were consumed and active claims are zero.

## Wave-134 progress (2026-07-17)

- [x] Moved `EnableChunkRPC` into the canonical `DistSqlContext` request
  authority, preserved it through detach, removed the duplicate caller boolean,
  and made chunk encoding require both the setting and the explicit alignment
  boundary. This remains dependency-closed policy: no protobuf DAG layout or
  production RPC producer/consumer is claimed.
- [x] Ported all seven region-location coverage helpers and all 32 generated
  `TestValidateLocationCoverage` cases over the concrete `RegionTaskEnvelope`,
  including nil locations, infinite boundaries, gaps, overlap, ordering, and
  unused locations. The validator is callable but not yet wired into a runtime
  task builder or serialization path.
- [x] Replaced planner/executor aggregate-name, mode, and descriptor duplicates
  with one planner-owned generic `AggFuncDesc`/`AggregateKind` authority. Logical
  hash identity reproduces the complete `TestAggFuncDesc` mutation vector;
  wrap-cast, executor dispatch, and COUNT result metadata consume the same
  descriptor domain. Other type inference and concrete aggregate construction
  remain partial.
- [x] Ran implementation and independent semantic review in parallel, then one
  shared 12-job Cargo gate. Review caught two connectivity/dispatch overclaims
  and an incomplete write set; static ledger generation caught a duplicate
  source-owner transfer; strict Clippy caught one test-only import. All were
  repaired before receipts were issued.

The accepted snapshot is 1,929/430/31/0 production and
15,512/857/202/14 test/support obligations. The final reused-target gate passes
strict all-target Clippy, the full workspace and differential test sweep,
formatting, 25 queue regressions, parser isolation, plan inventory, and every
source/test inventory. All three receipts were consumed and active claims are
zero.

## Wave-86 progress (2026-07-17)

- [x] Added `tidb-planner::logical_sequence` for generated `LogicalSequence`
  identity/hash framing over the operator tag and embedded base-plan ID; CTE
  child ordering and runtime sequence behavior remain external.
- [x] Added `tidb-stats::global_topn` for histogram-free partition TopN
  aggregation: empty-group skipping, wrapping count sums, count/encoded-byte
  ranking, selected TopN byte sorting, and ranked remainder behavior. The
  focused original source anchor is `global_stats_test.go:322` with ranking
  assertions at lines 342-347; direct TopN anchors were already owned.
- [x] Added `tidb-exec::config_int_json` for source-shaped integer SET CONFIG
  rendering: boolean flags become JSON booleans and ordinary integers retain
  decimal values; expression/config mutation remains external.

The Wave-86 static snapshot is 2,078/288/24/0 production and
15,818/613/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 87 is the next parallel source-owner batch.

## Wave-85 progress (2026-07-17)

- [x] Added `tidb-planner::logical_schema_producer` for generated
  `LogicalSchemaProducer` identity/hash framing, including nil/present ordered
  schemas and normalized column identity; schema propagation and full field
  metadata remain external.
- [x] Added `tidb-stats::special_global_index` for the exact global-index
  predicate: virtual-generated or prefix columns make a caller-declared global
  index special, with any-column short-circuit behavior.
- [x] Added `tidb-exec::lazy_txn_state` for source-faithful `Valid`, `pending`,
  and `validOrPending` boolean composition. Its original test anchors were
  already owned by the transaction rings, so no duplicate test-ledger rows
  were introduced.

The Wave-85 static snapshot is 2,081/285/24/0 production and
15,821/610/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 86 is the next parallel source-owner batch.

## Wave-84 progress (2026-07-17)

- [x] Added `tidb-planner::logical_show` for generated `Show` identity/hash
  framing, including the operator tag and ordered normalized schema metadata;
  SHOW contents, AST extraction, and runtime planning remain external.
- [x] Added `tidb-stats::bootstrap_sql` for exact statistics metadata and
  histogram bootstrap SQL generation, including priority projections, ordered
  IDs, and `[start,end)` paging; handle/session/SQL execution remain external.
- [x] Added `tidb-exec::placement_labels` for deterministic SHOW PLACEMENT
  label grouping, deduplication, and lexicographic row ordering; BinaryJSON
  decoding, PD/store retrieval, and SQL row encoding remain external.

The Wave-84 static snapshot is 2,084/282/24/0 production and
15,823/608/142/12 test/support obligations. Formatting, the complete Rust
workspace test suite, strict Clippy, and all static parser/plan/ledger/domain
gates pass with 12 jobs. The parser ring remains at 51,488 matched restores,
99 expected rejects, 10 matched multi-statement inputs, and one pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582`
(`select json_memberof();`). This remains source-owned WIP; the original Go
test suite, full planner/statistics/session integration, and a deployable TiDB
bootstrap remain incomplete. Wave 85 is the next parallel source-owner batch.

## Motivation

Why rewrite, and why Rust specifically:

1. **Latency jitter from GC.** The Go GC works against large long-lived heaps: statistics caches, plan caches, the infoschema cache, chunk buffers. P99/P999 latency in large deployments is dominated by GC assist and sweep interference, and the standard mitigation (GOGC tuning, ballast, memory limiter) is a permanent tax on operations. Rust removes the collector entirely.
2. **Allocation cost is already the bottleneck in hot paths.** The `hparser-integration` branch exists because parsing was allocation-bound: replacing goyacc with a hand-written parser plus slab/arena allocation was worth a rewrite of 27k generated lines into ~40 hand-written files. The same profile shape (allocation-dominated) recurs in the planner (logical plan tree churn per statement) and in expression evaluation. Rust's ownership model makes arena allocation, zero-copy string handling, and buffer reuse the natural idiom instead of a fight against the runtime — the `hack.String`/`hack.Slice` unsafe workarounds and the parser's `Arena` become simply how the language works.
3. **Memory footprint.** Go interface headers, GC headroom (typically 2× live heap), and pointer-rich data structures (AST, plan trees, `Datum`) inflate resident memory. Rust enums-by-value, arenas, and precise layout typically halve footprint for this workload class, which directly cuts cloud COGS for TiDB Cloud.
4. **One language across the stack.** TiKV — including its coprocessor, which re-implements TiDB's expression and executor semantics — is already Rust. Today every pushdown function is written twice (Go in `pkg/expression`, Rust in TiKV `components/tidb_query_expr`) and kept in sync by hand. A Rust SQL layer shares those crates: **one implementation of MySQL semantics, used on both sides of the wire.** This eliminates an entire class of "coprocessor disagrees with TiDB" bugs and halves the cost of adding builtin functions.
5. **Compile-time data-race elimination.** Session state, statistics, and DDL have a long history of race bugs found by `-race` in CI or by users in production. Rust moves this class of bug to compile time.

### Non-goals

- **No cgo. No in-process Go↔Rust linkage of any kind.** This is a hard architectural rule, not a preference. Every cross-language boundary in the migration is one of the existing *serialized network protocols* (MySQL wire, kvproto/gRPC, tipb, MPP, etcd). There is never a moment where Go code calls Rust code or vice versa inside one address space — no cgo, no shared C ABI hot path, no FFI handles. The cluster-level strangler exists precisely so this rule can hold: a `tidb-rs` node is a standalone process. (The Phase-0 parser can still be exposed as a standalone Rust library for *other native/Rust* consumers, but it is never linked into the Go binary.)
- **No change to SQL semantics, the MySQL wire protocol, or KV encodings.** Bit-for-bit compatibility with the Go implementation is the acceptance criterion, verified differentially.
- **No optimizer redesign during migration.** The existing planner is ported faithfully (same rules, same cost model, same plan output). Migrating language and redesigning the optimizer at once makes regressions undiagnosable. One variable at a time.
- **TiKV and PD are out of scope** (TiKV is already Rust; PD stays Go — its clients speak gRPC and don't care).
- **BR / Lightning / Dumpling / DM / TiCDC stay Go initially.** They are separate binaries speaking stable protocols; they migrate (or don't) on their own schedule after the SQL layer proves out.

## Ground truth: what we are rewriting

Non-test Go LOC on `hparser-integration` after merging master (2026-07-11):

| Subsystem | LOC (non-test) | Responsibility | Key contracts |
|---|---|---|---|
| `pkg/planner` | 120k | AST → logical → physical plan, cost model, hints, bindings, cascades | `base.Plan`/`LogicalPlan`/`PhysicalPlan` (`pkg/planner/core/base/plan_base.go`), `Optimize()` (`pkg/planner/optimize.go`) |
| `pkg/executor` | 106k | Volcano/chunk execution of physical plans | `exec.Executor` (`pkg/executor/internal/exec/executor.go`), `ExecStmt` (`adapter.go`), `Compiler` (`compiler.go`) |
| `pkg/util` | 99k | chunk, memory tracker, collation, ranger, codecs, ~60 sub-packages | `chunk.Chunk`, `memory.Tracker`, `collate.Collator`, `ranger.Range`, `rowcodec` |
| `pkg/expression` | 87k | scalar/aggregate expression trees and vectorized evaluation | `Expression`, `ScalarFunction`, `EvalContext` (`pkg/expression/exprctx`) |
| `br/` | 71k | backup/restore (separate concern) | — |
| `pkg/ddl` | 63k | online schema change state machine, reorg/backfill | `DDL` (`ddl.go`), `Job` (`pkg/meta/model/job.go`) |
| `pkg/parser` | 60k | hand-written recursive-descent parser + AST (own Go module) | `Parser`, `ast.Node`/`StmtNode` |
| `pkg/store` | 33k | TiKV driver, coprocessor client, region cache glue | `TiKVDriver` → `kv.Storage` |
| `pkg/statistics` | 25k | histograms, CM sketch, TopN, stats lifecycle | `statistics.Table`, `handle.Handle` |
| `lightning/` | 21k | bulk import (separate concern) | — |
| `pkg/types` | 18k | `Datum`, `MyDecimal`, `Time`, `Duration`, JSON | `types.Datum` (`datum.go`) |
| `pkg/server` | 18k | MySQL wire protocol, connection lifecycle | `Server`, `clientConn`, `IDriver` |
| `pkg/sessionctx` | 17k | session variables (~1000), statement context | `SessionVars`, `StatementContext` |
| `pkg/infoschema` | 16k | schema snapshot cache, memory tables | `InfoSchema`, `InfoCache` |
| `pkg/session` | 14k | session lifecycle, txn state machine, bootstrap | `session.Session` (`sessionapi`) |
| `pkg/meta` | 13k | meta KV encoding, `TableInfo`/`DBInfo`, autoid | `meta.Mutator`, `autoid.Allocator` |
| `pkg/domain` | 12k | per-instance singleton wiring all of the above | `Domain` (`domain.go`) |
| `dumpling/` | 9k | logical dump (separate concern) | — |
| `pkg/kv` | 3.4k | storage abstraction interfaces | `kv.Storage`/`Transaction`/`Snapshot`/`Request` |
| `pkg/distsql` | 2.5k | coprocessor request building/result streaming | `SelectResult`, `RequestBuilder` |

Total: ~948k non-test LOC (~1.6M including tests). The SQL-layer core (excluding br/lightning/dumpling) is ~750k LOC. Tests are half the corpus and are **the asset that makes a rewrite feasible**: `tests/integrationtest` (SQL-in/result-out golden files) and the MySQL-protocol test suites are implementation-language-neutral.

### The serialized boundaries

Four boundaries in the current system are already language-neutral serialized protocols. They are where a rewrite can be cut into independently verifiable pieces:

1. **Top: MySQL wire protocol** (`pkg/server`: `clientConn.dispatch`, `PacketIO`, binary prepared-statement protocol, auth plugins incl. `caching_sha2_password`). Everything above the `Session` interface is protocol handling.
2. **Bottom: kvproto/gRPC** — all TiKV/PD communication (`pkg/store` wraps `tikv/client-go`; region metadata via `metapb`, txn RPC via `kvrpcpb`).
3. **Bottom: tipb** — coprocessor DAG requests (`distsql.RequestBuilder` → `tipb.DAGRequest`; each physical operator implements `ToPB`). Plans are serialized to protobuf executor trees + expression trees and evaluated **by Rust code inside TiKV** (`components/tidb_query_*`). The pushdown allowlist (`pkg/expression/infer_pushdown.go`, ~96 function cases) is the exact contract of what already exists in Rust.
4. **Bottom: MPP** — TiFlash query fragments (`mpp.DispatchTaskRequest`/`EstablishMPPConnection` via `pkg/store/copr/mpp.go`, exchange operators, `local_mpp_coordinator`). Also protobuf; TiFlash (C++) doesn't care what language the coordinator is written in.

There is also a **sideways** contract: etcd, for DDL owner election (`pkg/owner/manager.go`), schema-version sync and server-info registry (`pkg/domain/infosync`). Standard etcd v3 API — `etcd-client` crates exist.

Everything between (1) and (2)/(3)/(4) — session, parse, plan, execute — is in-memory Go structs with no serialization boundary. That is why FFI-based incremental replacement of individual subsystems is a dead end (see Alternatives), and why the unit of migration must be **the whole SQL node**.

### Existing Rust assets

| Asset | What it gives us | Gap |
|---|---|---|
| TiKV `components/tidb_query_datatype` | `Datum`-equivalents: Decimal (ports `MyDecimal`), Time, Duration, JSON, collations, charset handling — battle-tested in production for years | Written for coprocessor evaluation; needs extraction into a shared workspace and API generalization |
| TiKV `components/tidb_query_expr` | Vectorized implementations of several hundred pushdownable builtin functions with MySQL semantics | Covers the pushdownable subset (~60-70% of builtins); non-pushdownable functions (e.g. `LAST_INSERT_ID`, sequence/lock functions, some JSON/GIS) must be written new |
| TiKV `components/tidb_query_executors` | Batch (vectorized) table scan, index scan, selection, hash/stream aggregation, TopN, limit executors | Storage-side only; join, sort spill, window, CTE, apply, index-lookup are TiDB-side and must be written new |
| `tikv/client-rust` | Raw + transactional KV client skeleton, PD client, region cache | **Explicitly not production ready** (unstable API, untested at scale). Missing/immature vs client-go: pessimistic-lock robustness, async-commit/1PC parity, follower & stale reads, resource control, batch-coprocessor. This is a build, not a reuse — treat client-go as the spec |
| `pingcap/tipb`, `pingcap/kvproto` | Protobuf contracts | Generate with `prost`; zero semantic work |
| `hparser-integration` (this branch) | A complete, freshly-audited encoding of TiDB's grammar as recursive-descent code (~40 files, arena-allocated), with a differential test harness against the goyacc corpus | It's Go, but it is structurally 1:1 transliterable to Rust — grammar knowledge is the expensive part and it was just re-derived |

## Strategy

### Rejected: big-bang rewrite

Build `tidb-rs` to full parity in a silo, switch when done. Rejected: at ~750k LOC of semantics-dense code, parity is 3+ years away; the Go tree doesn't freeze meanwhile (this merge alone brought 16 grammar changes in ~3 weeks); nothing is verifiable in production until the end; and the project produces zero value until it produces all of it. This is how rewrites die.

### Rejected: in-process FFI strangler (cgo → Rust subsystems)

Replace one Go package at a time with a Rust implementation behind cgo. Rejected: the internal boundaries are chatty, pointer-rich, in-memory interfaces (`LogicalPlan`, `Expression`, `sessionctx.Context` with ~1000 session variables). cgo cannot share Go pointers with Rust, so every call means serialization or handle indirection; the hot paths cross these boundaries millions of times per second. Two schedulers (Go runtime + tokio) in one process fight over threads. Each intermediate state costs more than it returns and none of the glue survives to the end state. The one place FFI *is* cheap — behind a serialized contract — is already a process boundary in TiDB's architecture.

### Chosen: cluster-level strangler — the Rust SQL node

`tidb-rs` is a new, separately-deployed TiDB server binary that joins an **existing production cluster** alongside Go TiDB nodes, speaking the four protocols that are already stable: MySQL wire on top; kvproto, tipb and MPP below. Rollout is a routing decision, rollback is instant (drain the Rust nodes), and every phase runs against real workloads.

### Chosen implementation method: source-first structural transition

The unit of work is a bounded Go source domain plus its existing tests, not a
new Rust interpretation of SQL behavior. For each domain, port the normal Go
control flow, data representation, error surface, and test vectors directly;
the Go implementation is the specification and its tests are obligations.
Automated/agent-assisted translation is encouraged for this mechanical work,
especially parser branches, AST restore methods, builtin dispatch, and test
tables. Every translated leaf must then pass the relevant Go-oracle
differential ring before it is considered covered.

Two generated ledgers make that rule executable: one inventories every
production Go source owner and routes it to a target crate; the other
inventories every original test entry point, fixture, runner, and expected
result. Parallel work is dispatched only from the intersection of those
queues, so neither an implementation file nor a test obligation can disappear
behind package-level progress estimates.

Agents do not pay a workspace build for each translated leaf. They claim a
complete source/test envelope, declare every Rust output path, inspect and
translate against Go, and run only zero-build/static checks. Exact Go anchors,
test anchors, dependency capabilities, and Rust output paths are atomic claim
dimensions. A single integration steward then batches the accepted envelopes
through one reused 12-job Cargo target and runs workspace tests, strict
all-target Clippy, formatting, and the generated-ledger/differential gates
once. This makes build frequency proportional to accepted parallel waves, not
agent count or file count.

The physical workspace follows the same ownership unit. A source-domain
envelope owns its typed AST/data shape, implementation, mirrored original
tests, differential selector or corpus, and sparse ledger fragment together.
Crate roots contain contracts, dispatch, and public re-exports only; they do
not retain feature behavior or private compatibility aliases after an
extraction. Shared routing is serialized through four narrow steward seams:
AST/parser routing, executor state/dispatch, datatype/evaluation context, and
workspace/evidence generation. Parser-only, result, and transaction evidence
are separate Cargo packages so an agent can compile its source family while a
different behavior crate is temporarily changing. This is the directory
structure that turns source translation into real parallelism instead of
several agents contending on the same roots.

Translation stops at a real language/runtime boundary. GC-coupled pools,
goroutine/channel lifecycle, pointer-identity tricks, and untyped `any`
registries must be expressed using Rust ownership, typed state, and explicit
task supervision; carrying those shapes over verbatim would preserve the Go
runtime's costs and create unsafe or unmaintainable Rust. The rule is therefore
**translate contracts directly; redesign only the implementation mechanism
that Rust cannot faithfully carry**. Redesigning SQL, optimizer, transaction,
or wire behavior during a transition is prohibited.

The migration ladder for a `tidb-rs` node inside a mixed cluster:

1. **Shadow** — receives mirrored traffic from a proxy, executes reads, results are compared with the Go node's answers, discarded. Zero risk; maximum signal.
2. **Read-only compute node** — serves reads for sessions routed to it; unsupported statements are rejected at parse/plan time with a distinguishable error and the proxy retries them on a Go node. (TiProxy already does connection migration; the "capability negotiation" here is a static statement-class list per release, not a per-query oracle.)
3. **Read-write node** — full DML with the ported transaction client; still no background ownership.
4. **Full peer** — eligible for DDL ownership, stats ownership, background jobs (GC, autoanalyze, bindinfo). Go nodes drain away.

Mixed-cluster protocol obligations (these make or break the design, and get their own compatibility test suite):
- **Schema lease protocol**: `tidb-rs` must implement the schema-version lease and `mdl` (metadata lock) reporting exactly, or online DDL from Go-owned workers corrupts it. This is required already at step 2.
- **DDL job queue encoding** (`pkg/meta/model/job.go` JSON): read-compatibility at step 2, write at step 3, ownership at step 4.
- **Statistics storage tables** (`mysql.stats_*`): read at step 2, feedback/writes at step 4.
- **System/bootstrap tables**: `tidb-rs` never bootstraps a cluster until step 4; it joins existing clusters and reads the bootstrap version, refusing versions it doesn't know.

### What ships value early (the pragmatism test)

Each phase must be independently worth its cost even if the project stopped there:

- The **Rust parser** (Phase 0) is immediately reusable by any *native* consumer — a future Rust TiProxy, SQL-aware routing, static analysis tooling — as a normal Rust crate. (It is deliberately **not** offered as a cgo library for the Go tree; per the no-cgo rule, Go consumers that need it would run it out-of-process.)
- The **transaction client** (Phase 1) turns `tikv/client-rust` into a production-grade client — a standalone deliverable for the whole TiKV ecosystem, upstreamed.
- The **read-only compute node** (Phase 2) is a sellable product on its own: a low-jitter, low-memory analytics/read-replica endpoint for existing clusters — the shadow/differential infrastructure doubles as the QA gate for every later phase.

## Target architecture (`tidb-rs`)

Cargo workspace; crates mirror the boundaries that proved stable in Go, with corrections where Go's layout is historical accident (`pkg/util`'s 60 sub-packages get homes; `sessionctx` splits from `session`; types unify with parser types — eliminating the `pkg/types` vs `pkg/parser/types` split, one of several places where the Go module boundary forced a duplicate).

```
tidb-rs/
├── crates/
│   ├── tidb-proto        # prost-generated kvproto + tipb (shared with TiKV where possible)
│   ├── tidb-datatype     # EXTRACTED FROM TIKV: Decimal, Time, Duration, JSON, Datum,
│   │                     #   collation, charset (tidb_query_datatype becomes a shared crate)
│   ├── tidb-parser       # 1:1 transliteration of hparser: LexerBridge, HandParser,
│   │                     #   arena AST; token tables generated from the Go tables
│   ├── tidb-ast          # AST node types + visitor + restore (SQL regeneration)
│   ├── tidb-expr         # scalar/agg expressions; wraps tidb_query_expr for the
│   │                     #   pushdownable set, native impls for the remainder
│   ├── tidb-chunk        # columnar batch (Arrow-compatible layout), null bitmaps,
│   │                     #   spill; replaces pkg/util/chunk
│   ├── tidb-codec        # tablecodec + rowcodec (row format v2), key encoding
│   ├── tidb-catalog      # infoschema snapshot cache, meta mutator, TableInfo model
│   ├── tidb-txnkv        # transactional KV client: 2PC, async commit, 1PC,
│   │                     #   pessimistic locks, lock resolution, region cache,
│   │                     #   follower/stale read, resource control
│   │                     #   (client-go is the reference spec; upstream to client-rust)
│   ├── tidb-distsql      # coprocessor DAG building, batch-cop, result streaming
│   ├── tidb-planner      # logical/physical plans, rules, cost model, hints, bindings
│   ├── tidb-exec         # executor tree: joins, sort, window, CTE, apply,
│   │                     #   index-lookup; storage executors delegate to pushdown
│   ├── tidb-session      # session state machine, ~1000 sysvars (macro-generated
│   │                     #   from a declarative table), txn lifecycle, privileges
│   ├── tidb-protocol     # MySQL wire protocol, auth (incl. plugins), TLS
│   ├── tidb-ddl          # job state machine, schema lease/MDL, backfill (last!)
│   ├── tidb-stats        # histogram/CMSketch/TopN, sync load, autoanalyze
│   └── tidb-server       # binary: config, domain wiring, HTTP status, telemetry
└── difftests/            # differential harness: corpus replay Go-vs-Rust
```

Key technical decisions, with the Go pain point each one eliminates:

- **Concurrency**: tokio for IO (connections, RPC fan-out); **synchronous pull-based `Next()` inside executor trees** running on a CPU worker pool. Async executor trees would box every future and poison the whole tree with `.await`; TiKV's coprocessor already proved sync-batch-on-thread-pool is the right shape. An inventory of the Go tree shows the executor model is already pull-based, with a small set of operators (copIterator, hash join, hash agg, parallel sort, index-lookup/index-merge, projection, shuffle, apply) internally backed by push pipelines built from one repeating idiom: *fetcher goroutine → bounded worker fleet → result channel, with chunk buffers recirculating through "give-back" channels as combined backpressure + ordering tokens, and worker panics converted to errors on the same result channel the consumer reads*. That idiom maps 1:1 to Rust: bounded `mpsc` + owned chunk buffers (ownership transfer replaces the give-back convention — the buffer *must* come back because the worker can't keep it), scoped threads per operator, and `Result`-carrying channels instead of panic-recovery wrappers. The ~30 long-lived Domain background loops (schema lease sync, stats workers, auto-analyze, DDL scheduler, GC) become cancellable tokio tasks under a supervisor with the same named-goroutine leak-detection discipline (`WaitGroupEnhancedWrapper` → task-tracker).
- **Memory**: statement-scoped bump arenas for AST and plan nodes (the hparser `Arena`, but with compiler enforcement instead of discipline); `bytes::Bytes`-style refcounted buffers for chunk data; a hierarchical memory tracker preserved as-is conceptually (`memory.Tracker` → RAII guards, so untracked allocation paths become type errors rather than audit findings).
- **AST ownership**: nodes live in the arena, cross-references are typed indexes (as in hparser's slab design) — this sidesteps the classic Rust AST borrow fight and is *already the design of the Go branch this builds on*.
- **Error handling**: `thiserror` enums per crate; MySQL error codes/classes preserved verbatim (terror's code tables generated into Rust).
- **Session variables**: today `SessionVars` is an 800-line struct + string-keyed registry consulted via scattered getters. Declare each variable once in a table (name, type, scope, default, validator) and macro-generate the struct, the registry, and `SHOW VARIABLES` — eliminating the three-places-per-variable special case.
- **Plugins**: Go's `plugin` system (audit/authn plugins) is replaced by static feature crates + an extension trait registry. Dynamic loading is not carried over (it barely works in Go across builds anyway).
- **failpoints**: `fail-rs`, same as TiKV — the failpoint-gated tests port with their semantics intact. (Scale note: the Go tree has ~3,400 `failpoint.Inject` sites across ~570 files; porting them is mechanical but budgeted work, and only the sites covering ported subsystems come along.)

### Go-runtime couplings: what disappears vs. what needs redesign

An audit of Go-specific idioms in the tree sorts into three buckets:

**Disappears entirely (the rewrite pays for itself here):**
- `pkg/util/gctuner` (dynamic GOGC), `servermemorylimit`'s force-GC watchdog, `memory.global_arbitrator`'s heap reconciliation, `runtime.SetFinalizer` lifecycle hooks — the whole layer that exists to negotiate with the Go GC has no reason to exist. This is the "eliminate the special case" outcome: today's OOM story is manual accounting (`memory.Tracker`) *reconciled against* GC heap statistics; in Rust the manual accounting **is** the ground truth (RAII tracker guards + jemalloc stats + cgroup pressure for the global budget), and query-kill/spill actions hang off it exactly as today.
- `pkg/util/hack` (unsafe string↔bytes aliasing), the join executor's tagged-pointer scheme (metadata stuffed in pointer high bits), `chunk`'s byte-buffer reinterpretation casts — these are Go fighting its runtime for layout control. In Rust they are `&str`/`bytemuck`-style safe casts or plain enum layout.
- cgo: exactly one site in the tree (`lightning/manual` malloc-backed buffers, which exists to escape the GC) — moot.

**Ports cleanly:**
- `Datum` is already a hand-rolled tagged union → a real `enum`. `Expression`/`ast.Node` interfaces → enums + visitor derive. Atomic copy-on-write globals (config, infoschema B-trees) → `arc-swap`. Reflection is not load-bearing anywhere in the query path. No goroutine-local-storage tricks exist.
- The terror/errno tables (exact MySQL error numbers — the wire compatibility contract) are mechanical constant tables.

**Needs real redesign:**
- `sync.Pool`-based recycling (chunks, columns, plan nodes) is GC-integrated; Rust replaces it with explicit pools/arenas — better behavior, but every call site is a decision.
- The type-unsafe grab-bags — `sessionctx.SetValue(fmt.Stringer, any)`, `kv.Transaction.SetOption(int, any)` — must be enumerated into typed structs up front. This is a forced design improvement but real up-front work.
- Go `plugin` (.so dlopen for audit/authn) → static feature crates (already the fallback path in Go) or a C-ABI extension boundary.
- `init()`-time global registration (the sysvar registry, `Domain`-per-storage map) → explicit wiring / `OnceLock` registries — see the sysvar macro-table decision above.

## Verification: differential everything

Correctness is the whole ballgame; the strategy is to never trust a port, only
a comparison. The four rings below are the target acceptance system, but they
are staged: the current workspace has source-owned leaf and static-corpus
gates, while cluster shadowing, full plan parity, and real-TiKV transaction
testing remain future gates.

1. **Parser ring**: replay every statement in `tests/integrationtest/t/**`, the parser unit corpus, and a grammar-aware fuzzer through Go-hparser and tidb-parser; compare restored SQL text, AST digests, error code + message + position. The current checked oracle has zero Rust parse failures, restore mismatches, or false accepts; its one remaining actionable row is the pinned Go restore failure for `json_memberof()`, and the 99 dual rejections are explicit rejection parity.
2. **Plan ring**: same corpus + stats fixtures; compare `EXPLAIN` output and plan digests statement-by-statement. The zero-diff gate remains mandatory before traffic, but the current implementation only covers narrow planner source leaves; the full optimizer ring is not yet open.
3. **Result ring**: begin with static Go-backed query/expression/table corpora, then add shadow traffic in real clusters (step 1 of the ladder) and `copr-test`-style randomized differential (random schema + random queries, TiDB-Go vs tidb-rs vs MySQL as the 3-way oracle).
4. **Transaction ring**: begin with source-owned storage primitives and fault/error boundaries, then run the txnkv crate's Jepsen and client-go integration suite against real TiKV before any write path opens; error-injection (fail-rs) tests are ported from client-go's.

CI keeps both implementations honest during the multi-year overlap: every grammar/behavior change to Go TiDB must land with its corpus entry, and the corpus is the contract — exactly how this merge's 16 grammar features were caught and ported into the hand parser, which is the process working as designed at small scale.

## Phasing

Ordered by (value ÷ risk), each phase gated by its differential ring:

| Phase | Deliverable | Reuses | New Rust LOC (est.) | Gate | Current status (2026-07-16) |
|---|---|---|---|---|---|
| 0 | `tidb-parser` + `tidb-ast` + `tidb-datatype` extraction | hparser design 1:1; TiKV datatype crate | 60-80k | Zero Rust regressions on accepted inputs, explicit rejection parity, and documented oracle failures | In progress: parser ring is clean except the pinned Go `json_memberof()` failure; source/test obligations remain |
| 1 | `tidb-txnkv` + `tidb-codec` + `tidb-catalog` (read) | client-rust skeleton; client-go as spec | 50-70k | Transaction ring (read path); Jepsen for reads/stale reads | Foundation only: portable key/handle/codec leaves exist; no production client or catalog read path |
| 2 | Read-only compute node: protocol + session (read subset) + planner + exec + distsql | tidb_query_executors patterns; tipb | 250-350k | Plan ring zero-diff; shadow → read traffic in staging; perf ≥ Go on sysbench read + TPC-H | Foundation started: local uncompressed COM_QUERY/command decode → `tidb-server::Connection` → session → DistSQL metadata/request policy → adapted ResultField metadata → bounded row/EOF sequence, plus numeric text formatting, GBK policy, typed executor/protocol error conversion and sequence-one rendered-error/status framing, status snapshots with `exec_success`, KV-request construction with opaque payload/partition ranges, exact SelectResponse/StreamResponse/CoprocessorRequest/StoreBatchTask projections, raw SelectResponse/Chunk and StreamResponse envelope validation, raw default/TypeChunk framing, FieldType physical layout mapping, native scalar Datum decoding, BinaryJSON and packed-temporal physical boundaries, bounded FullSchema-to-visible USING indices, direct-column ON/USING equality with NULL semantics, a direct-column/wildcard/alias projection contract wired through the automatic row owner, automatic bare-wildcard catalog-backed INNER/CROSS/LEFT/USING metadata/rows, and explicit SSL/TLS/auth-plugin handshake phases; no deployable node, temporal SQL/Duration, decimal/enum/set/vector Datum or native CHBlock semantics, typed expression/nested FullSchema projection mappings, general planner ON/USING typing/join algorithms, full Go ErrCtx lifecycle, password/user store, TiKV path, or plan-ring gate yet |
| 3 | Read-write: full txn lifecycle, DML, `tidb-stats` write path | Phase 1 client | 80-120k | Jepsen full; TPC-C parity; shadow-write comparison | Not started |
| 4 | Full peer: `tidb-ddl`, background ownership, bootstrap | — | 80-120k | Mixed-cluster DDL suite; ownership handover drills; long-run canary | Not started |

Estimated total: 500-700k Rust LOC (Rust runs denser than Go for this code; enum-based AST/plan nodes and macro-generated variable/function registries remove much of Go's repetition). The staffing and calendar numbers are planning estimates, not commitments; they must be re-estimated after the first connected read-only vertical slice. A calendar-parallel Go tree means the corpus-sync CI (above) is not optional at any point.

The next milestone is deliberately integration-first: carry the bounded
multi-relation catalog binding beyond direct-column/wildcard join output into
typed expressions, FullSchema redundant-column mappings, and typed ON/USING
semantics, and extend
the local uncompressed COM_QUERY → server dispatch → session → DistSQL →
adapted metadata/row/EOF seam beyond the connected table-less and single-table
paths. Attach the now-ported response events and status snapshots to the real
session/error-context and wire writers, then complete typed default/columnar/
CHBlock codecs and intermediate-output routing on top of the now-validated raw
tipb/chunk boundary, complete temporal/JSON/enum/set/vector Datum and full
charset/session formatting around the now-ported protocol text-row leaf,
planner dispatch, expression/datatype, and a read-only TiKV path into one
shadowable statement flow. Do not treat another collection of isolated leaf
ports as a substitute for this end-to-end gate.

DDL is deliberately last: it is the only subsystem where a bug destroys user data through a background process (reorg backfill), it has the deepest coupling to cluster-wide invariants (schema lease, MDL), and it benefits most from the longest shadow period.

## Risks

| Risk | Severity | Mitigation |
|---|---|---|
| Semantic drift in the long tail (sql_mode interactions, collation edge cases, zero-date/`DIV`/coercion quirks, ~1000 sysvars) | High — this is the rewrite-killer | Differential rings; port-don't-redesign; MySQL 3-way oracle; corpus-as-contract CI |
| `tidb-txnkv` maturity (client-rust is not production ready) | High | Treat as Phase 1 first-class deliverable with client-go as executable spec; Jepsen; upstream so TiKV org co-owns it |
| Plan regressions (cost model differences → customer-visible perf cliffs) | High | Plan-digest zero-diff gate; bindings/hints work day one, so escapes exist |
| Two codebases in flight for years (feature velocity tax) | Medium-High | Corpus-sync CI makes divergence a build failure, not a discovery; statement-class capability list keeps the proxy honest; org must accept the tax explicitly — this design makes it visible, not free |
| Mixed-cluster protocol subtleties (schema lease, MDL, DDL queue) | Medium | Dedicated mixed-cluster test suite from Phase 2 day one; `tidb-rs` refuses unknown bootstrap versions |
| Rust talent pool vs TiDB-internals talent pool intersection | Medium | Phase 0/1 are the training ground (bounded, spec-rich); TiKV team seeds reviews |
| Ecosystem tools assuming Go TiDB internals (BR backup of stats, Lightning checkpoint tables) | Medium | Tools speak SQL/KV protocols already; compatibility tests per tool in Phase 3 |

## Alternatives considered

- **Big-bang and in-process FFI**: rejected above (Strategy).
- **Build on Apache DataFusion** instead of porting the planner/executor: DataFusion is an excellent engine but its SQL semantics are not MySQL's, its optimizer is not TiDB's, and bending it to bug-for-bug MySQL compatibility plus TiKV pushdown plus TiDB's plan-stability surface (hints, bindings, plan digests) is a larger delta than porting. Selectively borrowing (Arrow memory layout for `tidb-chunk`, spill machinery) is in scope; adopting the framework is not.
- **Blind whole-package transpilation without contract gates**: rejected. It copies GC-shaped ownership graphs, goroutine lifecycle, and untyped registries into Rust without proving behavior. This is distinct from the chosen source-first structural transition: agentic/mechanical translation of a bounded Go domain plus its tests is the default implementation accelerator, provided each leaf is differentially verified and runtime-only mechanisms are translated into their idiomatic Rust equivalent.
- **Rewrite only the hot subsystems, keep Go for the rest, permanently**: permanent FFI seams in-process (rejected above), or a permanent two-binary architecture whose operational complexity outlives the migration's benefits. Acceptable only as a fallback if Phase 3+ stalls: the Phase-2 read-only node is designed to be a stable stopping point.

## Appendix: the query path being ported

The verified end-to-end call path in today's Go tree, with the interface at each handoff — each arrow is a crate boundary in `tidb-rs`:

```
listener.Accept (pkg/server/server.go)                          [tidb-protocol]
  → clientConn.Run / PacketIO / dispatch(ComQuery)              [tidb-protocol]
  → TiDBContext.ExecuteStmt → sessionapi.Session.ExecuteStmt    [tidb-session]
  → Parser.ParseSQL → []ast.StmtNode                            [tidb-parser / tidb-ast]
  → executor.Compiler.Compile → planner.Optimize                [tidb-planner]
      logicalOptimize (≈35 ordered rules) → physicalOptimize
      → FindBestTask → PhysicalPlan
  → ExecStmt.Exec → exec.Executor.Open/Next(*chunk.Chunk)/Close [tidb-exec / tidb-chunk]
  → PhysicalPlan.ToPB → tipb.DAGRequest                         [tidb-distsql]
      + RequestBuilder.Build → kv.Request
  → kv.Client.Send → copr.CopClient → buildCopTasks             [tidb-txnkv]
      (region split) → copIteratorWorker → tikvrpc.CmdCop → TiKV
  ← selectResult.Next ← chunk decode ← tipb.SelectResponse      [tidb-distsql]
  ← recordSet.Next ← clientConn.writeChunks                     [tidb-protocol]

writes: DML executors → kv.MemBuffer (staging per statement)    [tidb-txnkv]
  → tikvTxn.Commit → client-go 2PC (prewrite/commit)            [tidb-txnkv]
```

The `exec.Executor` interface (`Open/Next/Close` over `*chunk.Chunk`) and `kv.Client.Send` are the two contracts that carry ~all of the runtime's data flow; their Rust equivalents are the first APIs to stabilize in Phase 2.

## Unresolved questions

- Whether `tidb_query_*` crate extraction lands in the TiKV repo, a new shared repo, or is vendored — needs TiKV maintainer buy-in early (it's the Phase 0 critical path).
- Proxy layer: TiProxy capability-based routing needs its own small design doc (statement-class negotiation, session pinning, retry-on-unsupported semantics).
- MySQL 9.x feature tracking during the migration window (who implements new features twice, and when does Go TiDB feature-freeze).
- Productionizing the Rust TiKV/PD client: upstream ownership, compatibility scope against client-go, and the first real-TiKV test environment.

## Resolved execution choices

- The implementation currently lives in the in-repo `rust/` workspace, with
  differential corpora, ledgers, and evidence alongside the crates. A future
  repository split must preserve that source/test ownership history and exact
  corpus snapshots.
- The migration unit is a source-domain envelope; the deployment unit remains
  a standalone SQL node. These are complementary decisions: source-shaped
  translation enables parallel work, while the serialized cluster boundary
  avoids an in-process FFI seam.

### Parallel execution contract

Parallelism is organized around dependency-ready vertical slices, not isolated
helper methods, horizontal file types, or whichever Go file has the highest
raw queue score. One checked slice joins one or more authoritative Go source
owners, every directly owned original test/support obligation, the Rust leaf
and test destination, a focused target, its immediate consumer, and explicit
prerequisites. A whole-slice dependency must be `covered`; a capability inside
a broader partial family instead names one exact source/test ledger anchor,
its evidence owner, and the required `PARTIAL` or `COVERED` minimum. Readiness
must never be inferred from another row owned by the same agent. Only a
`ready` slice whose prerequisites are satisfied may be dispatched. Its
multi-source claim is atomic, must exactly match the checked slice, and must
reject every overlapping source or test anchor before either agent edits code.

Feature agents own only their domain leaves, focused tests, and owner-named
evidence fragments. Crate routing, test registration, generated inventories,
and current progress snapshots are deterministic integration products rather
than recurring feature-agent edits. The checked ledgers remain authoritative;
claims coordinate active work but cannot hide, waive, or mark an obligation
covered.

Validation has three scopes: a focused leaf gate, a static merged-evidence
gate, and one full workspace test/Clippy gate after a substantial multi-domain
batch freezes. A numbered wave, successful compilation, or a differential
sample is not progress by itself. Progress is the exact reduction of
untriaged/partial source and original-test obligations, with `COVERED` reserved
for a completely audited source family and its required differential ring.
The executable protocol and commands live in `rust/PARALLEL.md` and
`rust/docs/operations/validation.md`; checked dispatch records live under
`rust/workstreams/slices/` and are validated by
`rust/scripts/work-unit-queue.py check`.

The minimum normal integration batch is now a checked campaign: one or more
three-agent implementation/review rotations covering at least nine
authoritative production files or fifty original test/support obligations
before the expensive shared gate. The dispatcher keeps two rotations (six
disjoint ready slices) prepared ahead when dependencies permit, but a coherent
three-slice campaign may freeze once it meets the obligation floor. Agents
translate directly from the owned Go code and tests and run static/focused
checks; the integrator alone runs the persistent 12-job workspace gate after freeze. The
first campaign, `2026-07-read-path-01`, integrated 24 Go source files and 71
original obligations across txnkv, DistSQL, aggregates, and typed joins, then
released all claims from one receipt. This campaign model is the default until
dependency topology or conflict measurements demonstrate a better batch size.
The second campaign, `2026-07-read-path-02`, integrated six more vertical
slices across 13 Go sources and 52 tracked obligations with the same shared
gate. The third campaign, `2026-07-runtime-closure-03`, then integrated six
vertical slices across 13 Go sources and 56 tracked obligations: window
ranking, variance, bit aggregates, internal FIRST_ROW, coprocessor cache, and
driver-error conversion. All six claims were released directly from the
successful immutable receipt. Shared runtime seams are frozen by the
integrator between the two three-agent rotations rather than edited
concurrently by feature agents.

The fourth campaign, `2026-07-runtime-closure-04`, integrated six vertical
slices across nine Go sources and 122 exact original obligations: the complete
DistSQL request envelope, compressed PacketIO, CMSketch/TopN estimation,
GROUP_CONCAT, the auto-analyze heap/job/queue, and parser arenas/slabs. It
validated the intended conveyor: feature agents used only static checks,
independent reviewers repaired source-semantic defects, and one reused-target
12-job gate performed the expensive workspace compile/test pass. That gate also
demonstrated why evidence states must remain precise: Rust zlib/zstd writers
produce protocol-valid streams but not Go encoder-identical compressed byte
lengths, so those exact writer assertions remain `PARTIAL` even though exact Go
reader fixtures and round trips pass. Claims were released only from the final
successful receipt.

Campaign `2026-07-runtime-closure-05` is integrated across six receipt-backed
slices: mutable transaction buffering, the `distsql.go` request-to-response
runtime, complete auto-analyze jobs, compressed command I/O, the canonical
physical-partition window runtime, and a bounded
LogicalDataSource-to-index-task planner seam. Together they cover 32 Go
production sources and 130 exact original obligations. The final planner receipt
proved the seam is not a test-only helper: it creates an unordered TiKV Cop
single-read `PhysicalIndexScanPlan` only from an explicit source-owned
CountAfterAccess, exact upstream ExpectedCnt, and point-get admission; a
represented empty index range yields `TableDual`, and unsupported paths reject
explicitly. Independent review also made ownership retirement explicit for the
predecessor ranking and LEAD/LAG window slices.

The next ownership refactor consolidated five overlapping datatype slices into
`datatype-value-context-and-format`, with exactly six Go sources and 18
original-test anchors, and consolidated the error catalog plus terror identity
into a separate seven-source/15-anchor partial authority. Consolidation does
not imply a new runtime composition. Independent review proved there is no Go
operation that joins FieldType metadata, Datum rendering, truncation policy,
conversion context, and OutputFormat; the proposed Rust formatter also had
incorrect BIT/signedness admission, warning ordering, invalid-UTF-8, and float
rendering semantics. It was deleted instead of repaired. The integrated
datatype slice therefore owns independent direct translations on their real Go
call paths and remains `PARTIAL`; a future production conversion consumer must
start from `Datum.ConvertTo` and its actual helpers, while OutputFormat remains
on the FieldType metadata path.

That exact datatype claim passed the official 12-job integration gate and was
released from `integration_receipt 1` (claim SHA-256
`1f884fba2fddbab06a0fd59feaddeef1d2a409d1e46f4535aa82768da6b125ee`). The
datatype release left zero active claims. Ledgers remain
1,914/440/36/0 production and 15,307/953/311/14 original test/support
obligations (UNTRIAGED/PARTIAL/COVERED/BLOCKED). The next collision-free
candidate was the exact five-source/five-anchor expression aggregation plus
field-resolution union.

That expression union is implemented, independently approved, and released
from `integration_receipt 1`. Its live closure is bounded
to aliased `COUNT(t.a)` over one unqualified catalog table and reaches
automatic COM_QUERY with source-shaped NULL, empty-input, row, and metadata
behavior. Aliasless and schema-qualified forms fail closed. The planner
descriptor, field resolver, fixed COUNT type, and aggregate runtime remain
separately translated authorities; no shared bound `AggFuncDesc` pipeline is
claimed, and harder FieldName caller cases remain partial.

Campaign `2026-07-runtime-closure-06` integrates the checked DistSQL/session
contracts as three disjoint direct-Go slices: raw query/response ownership at
2 sources/15 anchors, warning/status publication at 5/11, and the bounded
cop-read coordinator at 5/48. Together they cover 12 production sources and 74
original obligations. Public `query_runtime` and `cop_paging` registries plus
nested owned modules remove the previous `lib.rs` parallel-write collision.

Session routes live warnings through one `StaticWarningHandler` owned by
`StatementStatus`, snapshots once, and publishes wrapping `uint16` counts.
DistSQL owns raw response subsets until one-way iterator conversion. Cop-read
composes checked tasks, per-attempt cache, iterator-wide EMA and wrapping task
indexes, paging, and bounded publication. Error envelopes and backpressure
fail before partial state mutation. These slices remain `PARTIAL`: real
RegionCache/PD, lock/backoff/endpoint/RPC/cancellation, shared unordered
dispatch, cache-backend and unused-topology parity, production table readers,
full memory/telemetry, Close-error and subset-plus-error representation, SHOW
WARNINGS/errno identity, broad SessionVars/SysVar plumbing, and other warning
producers are open.

Integrated campaign membership now has a durable frozen archive. The normal
9-source/50-obligation floor applies while a campaign is planned or active;
terminal evidence transfers may later shrink retired member manifests without
rewriting the historical batch that an integration receipt proved. All 35
queue regressions and current generated ledgers pass this invariant.

The first expression gate found a real admission hole: `COUNT(t.missing)`
reached the older generic evaluator. Binding first moved to the cluster CAS
loop, but independent review caught that this bypassed failed-SELECT session
effects. The final binding lives in `Database::run_select` on every cloned
catalog attempt, before generic evaluation and inside the normal statement
reset/promotion boundary. Exact `UnknownColumn("missing")` and published
`ROW_COUNT() = -1` are regression-checked. The final frozen 12-job gate passed
strict all-target Clippy, the complete workspace tests, 39 governance tests,
all ledgers/parser/plan inventories, dependency isolation, formatting, and diff
validation. Campaign 06 then passed the official 12-job gate after strict
Clippy rejected an eight-argument query API and the implementation introduced
`QueryResultContext` instead of suppressing the lint. The final gate passed
workspace Clippy/tests, 39 governance tests, all generated inventories, parser
isolation, formatting, and diff validation, issued `integration_receipt 3`,
and released all three claims. Its archived boundary was 1,914/440/36/0 source
and 15,300/960/311/14 test/support obligations, with zero active claims.

Campaign `2026-07-read-path-07` then integrated three direct-Go vertical
slices across 14 production sources and 65 exact original obligations. The
planner slice admits one validated TiKV scan and serializes exact DAG/context
fields; the reader slice propagates required-row budgets through the response
stack and owns ordered dispatch/cleanup; the transport slice serializes and
decodes the dependency-closed unary coprocessor contract with region, lock,
other, and batch precedence plus TiDB request provenance. Independent
cross-review repaired one source-of-truth defect in each slice before the
shared gate. The final 12-job integration gate passed strict workspace Clippy,
all Rust workspace tests, 39 governance tests, every checked inventory,
parser isolation, formatting, and diff validation, issued
`integration_receipt 3`, and released all claims. Current generated totals are
1,907/447/36/0 source and 15,284/976/311/14 test/support obligations, with zero
active claims. These are ledger ownership states, not a product-completion
percentage; the absence of live TiKV/PD/transaction/DDL/bootstrap paths keeps
the rewrite far below 30% product parity.

The Campaign 07 state is the tracked Rust baseline. Each claimed feature slice
uses a `codex/<slice>` branch in its own Git worktree, while checked source,
test, and Rust write sets remain the semantic isolation boundary. The primary
dispatcher acquires claims before worktree creation; worktrees share dependency
and build caches, and the campaign integrator alone performs final compilation
and the frozen shared gate.
