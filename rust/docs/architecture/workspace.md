# Rust Rewrite Workspace Architecture

`rust/` is a Cargo workspace for a source-faithful TiDB SQL-layer rewrite. It
never links into the Go server through cgo. The Go implementation under `pkg/`
is authoritative; the Rust differential tools make drift observable.

## Physical Layout

```
rust/
  crates/
    tidb-lexer/     # scanner and token classes
    tidb-ast/       # typed SQL AST and canonical restore
    tidb-parser/    # recursive-descent grammar
    tidb-proto/     # checked-in prost-generated tipb protocol leaves
    tidb-protocol/  # source-backed uncompressed MySQL packet framing
    tidb-distsql/   # request context and detach-state primitives
    tidb-datatype/  # Datum, Decimal, and source-backed SQL scalar metadata
    tidb-codec/     # comparable scalar and datum-key encoding
    tidb-expr/      # expression evaluation
    tidb-planner/   # source-backed cardinality and plan-estimation primitives
    tidb-txnkv/     # byte-ordered KV key/range/version and handle contracts
    tidb-exec/      # seed executor and transactional state model
    tidb-server/    # connection command dispatch above protocol/session
  difftests/        # shared Go oracle tools, corpora, and inventory
    parser-tests/   # parser-only rings and source-owned selector shards
    planner-tests/  # planner primitive source-test rings
    result-tests/   # expression/query/table result rings
    transaction-tests/ # transaction primitive and future real-TiKV rings
  docs/             # architecture and operational guidance
  scripts/          # zero-build queue/claim/status and tiered gate entrypoints
  workstreams/
    slices/         # checked multi-source vertical capabilities and dependencies
  workstreams/      # concurrent domain and evidence ownership contracts
  execplans/        # living structural migration plans
```

The behavior packages remain `tidb-*`. Shared evidence infrastructure is the
`difftest` package; `difftest-parser-tests`, `difftest-planner-tests`, `difftest-result-tests`, and
`difftest-transaction-tests` are real test packages whose dependency
boundaries let parser, result, and transaction agents build independently.
Do not create another crate merely to match the long-term design. A new crate
needs a real API, a real consumer, and its own source-backed tests.

Heavy source-test packages use deterministic explicit harness shards rather
than one Cargo binary per leaf. The first conversion keeps all 103
`tidb-exec/tests/*.rs` source owners, nests 97 dependency-closed leaves under
four shards, and leaves six crate-root-sensitive schema tests standalone. This
preserves the test-source ownership boundary while reducing linker/process
targets; future shard conversions must prove the exact pre/post test-name
union before disabling Cargo auto-discovery.

## Dependency Direction

`tidb-lexer -> tidb-ast -> tidb-parser`. `tidb-datatype` is independent of AST
and execution. `tidb-codec` consumes datatype values, and `tidb-txnkv`
consumes codec primitives for row-handle identity plus generated leaves from
`tidb-proto` when a real KV/protocol consumer exists. `tidb-proto` owns
generated wire contracts only and has no dependency on SQL behavior crates;
protocol generation must not pull transaction or expression semantics into the
wire crate. `tidb-protocol` owns the uncompressed MySQL packet
header/continuation contract, source-shaped command-byte decoding, and the
source-shaped result charset policy; its only semantic dependency is the
datatype charset/encoding leaf, not the executor or session. `tidb-distsql` owns
request metadata, warnings, cancellation, kill, detach-state,
dependency-closed serial/per-channel result iteration, and ordered response-event
lifecycle primitives without
depending on transport or execution. `tidb-expr` consumes AST
plus datatype values, `tidb-planner` owns pure cardinality/estimation helpers
and source-shaped physical-plan metadata with no execution dependency, and
its uniform/cardinality adapters consume only dependency-closed arithmetic
from `tidb-stats`, never the statistics handle or storage lifecycle; and
`tidb-exec` consumes AST, datatype values,
and expression evaluation plus the protocol and DistSQL context leaves. Its
`Session::execute_framed_query` is the first real consumer that connects
packet framing to SQL parsing/execution and records request SQL metadata;
`execute_framed_query_text_rows` adds the bounded row-packet response, while
`execute_framed_query_text_result_set` frames the protocol leaf's complete
metadata/row/EOF sequence from caller-supplied column metadata. The isolated
`result_metadata` leaf ports Go `ConvertColumnInfo` arithmetic and now exposes
`columns_from_adapted_fields` as the narrow adapter-to-column bridge;
`result_field_resolver` adds a bounded table-less expression/name/type resolver
and returns explicit schema-required errors for row-dependent fields.
`tidb-protocol::textrow`
owns the numeric, dependency-closed decimal-text formatter and explicit
unsupported branches without claiming charset/session Datum conversion.
`tidb-protocol::ResultEncoder` owns the registered binary/ASCII/Latin1/UTF-8/
GBK result-charset precedence and explicit unsupported-collation boundary;
the full session registry and encoder lifecycle remain open. Its
`error_packet` leaf ports source-shaped protocol-41/legacy ERR payload order
without owning error conversion or packet framing; `error_conversion` adds the
typed source-kind to MySQL errno/SQLSTATE lookup while leaving session error
context and message construction to its caller.
`tidb-exec::statement_status` owns the source-shaped counters/warnings/reset/
publish state, `cluster.rs` attaches that lifecycle to the shared Session, and
`status_result` converts an already-published snapshot to protocol OK/result
options without reading runtime Datum values. `result_schema_multi` binds the
bounded inner/CROSS/comma/LEFT relation tree to declared catalog snapshots,
including self-join alias order. `result_schema_join_output` now ports the
planner-owned visible output order, LEFT-side nullability declarations, and
USING coalescing metadata for already-resolved child schemas; the automatic
server path consumes this for bounded LEFT/USING rows and the adjacent
`result_schema_projection` contract supplies direct columns, aliases, and
qualified/bare wildcard output names. General ON/USING typing remains open.
`tidb-proto`
owns the dependency-closed SelectResponse/
StreamResponse wire projection, and `tidb-distsql::chunk_decode`/
`stream_decode` validate raw response/chunk envelopes and preserve protobuf
presence while leaving default/columnar/CHBlock codecs opaque. `tidb-server`
consumes the protocol decoder plus the DistSQL and executor
seams; its `Connection::dispatch` and `dispatch_framed` own the bounded
COM_QUERY/PING/QUIT lifecycle and server-sequence response framing, while
`dispatch_framed_auto` connects strict table-less and single-table catalog
SELECT metadata paths; the automatic path now consumes bounded catalog schemas
for INNER/CROSS/LEFT/USING joins, preserving null extension and coalesced
output order while routing accepted direct-column/alias/wildcard lists through
the isolated projection contract. The multi-relation resolver, join-output
metadata, and projection metadata are wired at this bounded response seam;
`Database::project_row` remains the sole row-value owner.
`tidb-codec::column` preserves raw default/TypeChunk framing plus the
source-owned FieldType physical-layout mapping and a bounded scalar
integer/float/string Datum conversion; `tidb-datatype::PackedTime` and
`tidb-codec::temporal` additionally preserve the exact packed temporal integer
boundary, while `tidb-codec::json` preserves BinaryJSON type/value boundaries
without deserializing JSON semantics (opaque SQL temporal/decimal/JSON/etc.
semantics remain explicit errors). `tidb-distsql::KvRequestBuilder` preserves opaque request payloads
and ordered partition ranges before protobuf/region/RPC ownership, while
`CoprocessorRequestEnvelope` projects the exact tipb request fields before
region splitting and transport; `RegionTaskEnvelope` preserves the exact
StoreBatchTask child metadata before region lookup, retry, or RPC. `RenderedExecError`
carries optional published status (including `ExecSuccess`) into sequence-one
ERR framing without copying warnings into the wire payload; `Session` and
`Connection::frame_execution_error` own that attachment after execution.
`RawChBlockChunk` now validates the native CHBlock envelope while keeping
typed ClickHouse Datum decoding explicitly unsupported. `tidb-planner::join_condition`
classifies the narrow source-backed cross-side equality/USING predicate subset;
general residual predicate execution remains an explicit boundary. The server's
`AuthExchange` preserves source-shaped auth-switch/more-data frames and
malformed-payload errors without claiming password verification, plugin lookup,
TLS, or transport flush.
`AuthPluginRegistry` validates custom-plugin metadata and selects the
source-shaped client plugin name without executing extension callbacks or
claiming authentication. `RawValue::decode_datum` and the default-row DistSQL
consumer cover the source-proven scalar tag subset with exact byte consumption;
typed temporal/JSON/vector/enum/set conversion remains outside the seam.
`tidb-codec::RawDuration` adds the fixed signed-nanosecond duration payload and
source `MaxFsp` metadata without SQL range parsing. `SecureTransportPolicy`
owns only the pre-auth admission decision for plaintext TCP versus Unix/direct/
gateway-secure transport facts; it does not establish TLS or authenticate.
`tidb-planner::condition_binding` maps known residual column paths into the
flattened `FullSchema` and leaves dedicated AST variants opaque for a typed
executor.
`tidb-planner::predicate_partition` adds a conservative dependency-only route
for left/right/join/deferred residual candidates with an explicit typed-safety
gate; it does not perform pushdown or choose a physical join.
`tidb-planner::typed_condition` is the next executor handoff: it carries the
source `FullSchema` width, child/join/outer-match mode, and TRUE-only versus
UNKNOWN-tracking policy without evaluating a `Datum` or materializing a row.
`tidb-exec::evaluate_typed_condition` is now its first scalar consumer: it
validates row width and returns TRUE/FALSE/UNKNOWN through the existing Datum
evaluator, while leaving vectorized filtering and join-row lifecycle outside.
`evaluate_typed_condition_batch` extends that seam to disjoint row-indexed TRUE
and UNKNOWN masks with indexed errors; it does not own selection reuse or outer
row mutation.
`transition_outer_row_status` applies the pure TRUE/FALSE/UNKNOWN transition
for one batch, and `merge_outer_row_status` preserves cumulative
`Matched`/`HasNull`/`Unmatched` precedence across candidate batches. Selection
reuse, row/chunk materialization, null extension, and physical join execution
remain with the outer-join owner.
`select_outer_row_statuses` adds the source-order TRUE index/status alignment
needed before row copying while retaining FALSE/UNKNOWN statuses in the full
slice. `finalize_outer_row_statuses` emits source-order unmatched/UNKNOWN
events and default-inner/UNKNOWN signals while leaving lookup and materialized
null extension with the join owner.
`tidb-codec::DecimalWireMetadata` owns precision/scale/payload-length framing
without materializing or rounding a coefficient. `tidb-server::AuthChallenge`
and `AuthSessionAttempt` retain opaque session-auth inputs and stop at external
verification, enforcing only the source `auth_socket` transport precondition.
`AuthSessionAttempt::begin_with_policy` composes that pre-auth session boundary
after `SecureTransportPolicy` admission. `tidb-codec::RawJsonTemporal` retains
BinaryJSON DATE/DATETIME/TIMESTAMP type codes and packed calendar bits; SQL
calendar/FSP/timezone conversion remains outside the physical codec seam.
`tidb-server::IdentityLookupRequest`/`IdentityLookupResult` preserve the
pre-auth MatchIdentity request/matched-row/NotFound contract without wildcard
matching or authenticated claims. `RawDuration::parts` mirrors Go's
splitDuration decomposition while leaving SQL TIME policy to typed owners.
`tidb-server::IdentityCatalog` now owns the source-shaped host ordering and
wildcard/network/loopback match algorithm over caller-provided rows; actual
privilege storage and DNS I/O remain outside.
`IdentityLookupPolicy::SkipWithGrant` is represented as an explicit bypass
result rather than a fake privilege-row match. `round_duration_fsp` owns typed
FSP normalization and rounding while warning and session policy remain above
the datatype seam. `parse_duration` adds the bounded signed/day-prefixed
`HH:MM[:SS]` grammar with fraction carry and TIME endpoint clamping, including
compact short/leading-zero `HHMMSS` forms; date fallback and statement
warning/session policy remain open. `AuthPluginHandoff` retains opaque plugin
metadata after exact-row admission without validation or authentication.
`AuthPluginRegistry::select_client_plugin` preserves source handshake plugin
selection outcomes without packet I/O or callback execution. `DurationParseEvent`
classifies overflow, datetime fallback, and truncation without mutating session
warnings; `PredicateBatchBuffer` reuses aligned TRUE/UNKNOWN slices while
leaving vectorized execution and row copying to later owners.
The public handshake leaves port source-shaped initial-handshake and
HandshakeResponse41 parsing plus explicit SSLRequest/TLS-established and
auth-plugin negotiation phases, and the listener leaf owns idempotent real-TCP
bind/activation/shutdown/close state. The server crate still does not claim
authentication verification, TLS transport/compression, database selection, prepared
statements, dynamic warning/status/session lifecycle, planner-owned join
predicate/row execution, Unix/PROXY accept handling, or RPC.
Shared
`difftest` depends only on lexer/parser
evidence primitives. `difftest-parser-tests` and `difftest-planner-tests`
cannot depend on expression or execution; `difftest-result-tests` is the outer
verification package that owns those heavier dependencies.
`difftest-transaction-tests` consumes `tidb-txnkv` and stays independent from
the seed executor. No AST, parser, datatype, expression, or transaction module
may depend on execution or differential tooling.

## Protected Routing Seams

The current rewrite has five deliberate integration bottlenecks. Feature
agents must not edit them concurrently:

- AST/parser routing: `crates/tidb-ast/src/lib.rs` and
  `crates/tidb-parser/src/lib.rs`.
- Execution routing/state: `crates/tidb-exec/src/lib.rs` and
  `crates/tidb-exec/src/database.rs`.
- Server command routing: `crates/tidb-server/src/lib.rs`; handshake and
  listener lifecycle work lives in `crates/tidb-server/src/handshake.rs` and
  `listener.rs` and must not be folded into protocol packet framing or the
  seed executor.
- Datatype/evaluation context: `crates/tidb-datatype/src/datum.rs`,
  `crates/tidb-datatype/src/field_type.rs`, `crates/tidb-expr/src/build.rs`,
  `crates/tidb-expr/src/context.rs`, and their narrow routing in
  `crates/tidb-expr/src/lib.rs`. `tidb-datatype` is the only `Datum` authority;
  feature crates import it directly rather than through `tidb-expr`.
- Evidence/workspace: `Cargo.toml`, the five `difftests/**/Cargo.toml`
  manifests, `difftests/src/bin/`, the static parser snapshot, and the
  production-source and Go-test ledgers.

The active ExecPlan splits these roots into domain envelopes. Until that is
complete, a feature agent owns an isolated domain file and submits a narrow
landing request when routing must change. This avoids two agents editing the
same statement enum or dispatcher.

## Target concurrency boundaries

- Parser/DDL work is split by Go-owned grammar family. Each envelope contains
  typed AST payloads, parsing/restoration, executor capability classification,
  and a source-derived selector.
- Result work is split by Go builtin family after `Datum` and `EvalContext`
  stabilize. A family owns implementation, unit ports, and one result selector.
- Transaction work separates shared cluster/catalog state from per-session and
  per-transaction state. Autocommit conflicts are visible through versioned
  commit/retry; a global lock across statement execution is forbidden because
  it would serialize away TiDB's concurrency contract.
  Storage-facing work starts in `tidb-txnkv` from complete Go source/test
  units; the seed executor must not grow an in-memory imitation of TiKV.
- Evidence work runs in stable shards. Executable corpora and explanatory
  evidence use different namespaces, and validation rejects orphan inputs or
  goldens before Cargo discovers hundreds of one-off targets.

## Source-domain directory rule

The unit of translation is one bounded Go source domain, not one Rust file
type. A domain owner follows the Go implementation and its original tests
through every Rust layer it needs. The normal shape is:

```
crates/tidb-ast/src/<domain>.rs
crates/tidb-parser/src/<domain>.rs
crates/tidb-parser/src/tests/<domain>.rs
crates/tidb-exec/src/<domain>.rs          # only when execution is supported
crates/tidb-exec/src/tests/<domain>.rs
difftests/parser-tests/tests/selectors/<family>/<domain>_selector.rs
# or difftests/corpus/{expr,query,table}/<domain>{,.golden}.txt
```

Do not create all of these files speculatively. Create a leaf when a real Go
source owner and source-derived test enter the port. Root files declare stable
contracts and dispatch only; they must not regain domain grammar or behavior.
Completed extractions stay in their source-owned leaves: ADMIN, ANALYZE TABLE,
standalone FLUSH, TRAFFIC/REFRESH STATS, CREATE/ALTER/DROP RESOURCE GROUP,
CREATE/ALTER/DROP PLACEMENT POLICY, masking-policy grammar, ordinary SET, SQL
bindings, ordinary SHOW, LOAD DATA, partition DDL, account security, and
GRANT/REVOKE. Sequence grammar and AST restore now live in `sequence.rs`;
the independent `parseAlterAlter` index-visibility and
`parseAlterPartition` re-partitioning branches now live under `ddl/alter/`,
while their shared statement and partition envelopes remain the only routing
surface. This is the pattern for the remaining ALTER families: move a real Go
symbol family into its own leaf, not a keyword-shaped wrapper.
executor admin/session runtime live in `admin_runtime.rs` and
`session_runtime.rs`. Executor table-less dispatch and row-set folding now
live in `setopr.rs`, synthetic-row SELECT and INTO validation in `select.rs`,
shared physical table-reference capability checks in `table_reference.rs`,
executed-Datum AST conversion in `literal.rs`, and statement outcomes beside
rows in `result.rs`; `tidb-exec/src/lib.rs` retains only module routing, actual
public re-exports, and the `Database` state contract. Datatype FSP and EvalType
live in their own leaves; ENUM/SET consume the byte-preserving general-CI and
UCA-4.0 authority in `collation.rs`, whose generated tables come directly from
the Go sources. All `pkg/parser/charset/**` production owners route to
`tidb-datatype`, not back to parser. TxnKV request support plus transaction-source
bitfields live in `checker.rs` and `txn_source.rs`. Internal consumers import
these physical owners directly; compatibility re-exports must not recreate the
old root paths.

The current statistics/planner/session ring also keeps its narrow leaves
separate: `tidb-stats::analyze_table_id` owns AnalyzeTableID selection and
formatting, `tidb-stats::status` owns loading labels, and
`tidb-planner::cardinality::cross_estimation` owns expected-count range
conversion over caller-built endpoints. `tidb-exec::sequence_state` owns only
the numeric latest-value map and snapshot merge used by session migration.
These leaves do not pull storage, Datum/ranger construction, SQL sequence
execution, or live session synchronization back into the root seams. Wave 42
closed the reusable `tidb-stats::row_estimate`, planner uniform-estimation, and
removed-system-variable leaves. Wave 43 adds
`tidb-planner::schema_table_key` for normalized map identity,
`tidb-stats::stats_version` for raw analyzed/synthesized predicates, and
`tidb-exec::option_values` for source-compatible option text conversion. Wave
44 adds `tidb-planner::implementation_cost` for base implementation arithmetic,
`tidb-stats::ColAndIdxExistenceMap` for known/analyzed column/index metadata,
and `tidb-exec::statement_pushdown` for source TiKV flag composition. None of
these leaves owns parser/CTE scope, histogram/TopN loading, stats-handle/DDL
integration, live StatementContext, or SQL dispatch.
Wave 45 adds `tidb-stats::scalar_geometry` for Datum-free scalar arithmetic,
`tidb-planner::task_type` for forward-compatible execution-task labels, and
`tidb-exec::context_id` for the process-local monotonic context-ID sequence.
Their Datum/histogram, physical-property, and live-session consumers remain
outside the dependency-closed leaves.
Wave 46 adds `tidb-planner::by_item` for opaque ORDER BY metadata,
`tidb-stats::memory_usage` for measured column/index accounting, and
`tidb-exec::statement_refcount` for cached statement-context admission state.
These remain leaf contracts; expression/property integration, stats cache
aggregation, and live context reuse stay outside the root seams.
Wave 47 adds `tidb-planner::physical_property` for MPP exchange metadata,
`tidb-stats::overlap_geometry` for out-of-range geometry, and
`tidb-exec::used_stats` for deterministic slow-log formatting. Their physical
plan, histogram/skew, TableInfo, and live-session consumers remain outside the
dependency-closed leaves.
Wave 48 adds `tidb-planner::stats_info` for row-count/NDV property arithmetic,
`tidb-stats::HistogramCountSummary` for histogram count/factor arithmetic, and
`tidb-exec::plan_cache_params` for ordered plan-cache parameter storage and
privacy state. Their catalog/planner-statistics, histogram-loading, prepared
plan, and live-session consumers remain outside the dependency-closed leaves.
Wave 49 adds `tidb-planner::index_columns` for normalized index projection,
`tidb-stats::analysis_policy` for analyzed/minimum-count/eligibility policy,
and `tidb-exec::stats_load_result` for load-item identity/error metadata.
Wave 50 adds `tidb-planner::pattern_engine` for cascades engine flags and set
membership. These remain leaf contracts; catalog/index, scheduler/loading,
worker/retry, and cascades matching consumers stay outside the root seams.
Wave 51 adds `tidb-planner::fix_control` for source-compatible fix-control
parsing, `tidb-stats::analyze_version_matches` for analyzed-version decisions,
and `tidb-exec::alternative_plan_signals` for statement-local mark/reset
signals. Session-variable wiring, statistics scheduling, planner rounds, and
live statement attachment remain outside the dependency-closed leaves.
Wave 52 adds `tidb-planner::memo_group_id` for cascades memo IDs,
`tidb-stats::estimate_ndv_by_gee` for pure GEE estimator arithmetic, and
`tidb-exec::read_consistency` for strict/weak value validation. Memo/optimizer,
sketch/TopN, request-isolation, and SessionVars consumers remain outside the
dependency-closed leaves.
Wave 53 adds `tidb-planner::task_scheduler` for serial cascades task flow,
`tidb-stats::avg_count_per_not_null_value` for histogram average arithmetic,
and `tidb-exec::chunk_alloc_status` for statement-local allocation-use state.
Pools/context, histogram/planner callers, chunk reuse, and lifecycle wiring
remain outside the dependency-closed leaves.
Wave 54 adds `tidb-planner::hash_equaler` for primitive cascades FNV-1a
hashing, `tidb-stats::calc_correlation` for histogram order-correlation
arithmetic, and `tidb-exec::setvar_hint_restore` for first-write-wins
SET_VAR old-value metadata. Object dispatch, sampling/histogram construction,
hint parsing/sysvar mutation/restoration, and live planner/session ownership
remain outside the dependency-closed leaves.
Wave 55 adds `tidb-planner::plan_context` for the bounded BuildPBContext
detach hand-off, `tidb-stats::index_usage` for percentage-access sample
metadata and merging, and `tidb-exec::cursor_tracker` for cursor IDs, lookup,
range, close, and bounded concurrent lifecycle state. Full planner context,
collector persistence, session/result-set execution, and live ownership remain
outside the dependency-closed leaves.
Wave 56 adds `tidb-planner::task_stack` for reusable cascades LIFO state,
`tidb-stats::analyze_jobs` for analyze job/progress metadata, and
`tidb-exec::session_context_key` for source-compatible context-key labels.
Stack pools, analyze scheduling/persistence, live session context storage, and
runtime consumers remain outside the dependency-closed leaves.
Wave 57 adds `tidb-planner::pattern` for cascades operand/matching metadata,
`tidb-stats::async_load` for sharded pending statistics-load ownership, and
`tidb-exec::status_registry` for status provider registration and collection.
Concrete planner integration, statistics persistence/scheduling, live status
counters, and protocol publication remain outside the dependency-closed
leaves.
Wave 58 adds `tidb-planner::string_writer` for ordered planner string
assembly, `tidb-stats::datum_map_cache` for normalized datum-key caching, and
`tidb-exec::process_info` for shallow process metadata cloning. Full planner
formatting callers, statistics persistence/scheduling, session-manager
ownership, and protocol publication remain outside the dependency-closed
leaves.
Wave 59 adds `tidb-planner::expr_iterator` for source-shaped memo expression
matching and child enumeration, `tidb-stats::need_analyze_table` for bounded
auto-analyze trigger policy, and `tidb-exec::nextgen_readonly_vars` for the
six-name next-generation read-only-variable predicate. Real memo/list
ownership, statistics scheduling, variable registration, and protocol/session
publication remain outside the dependency-closed leaves.
Wave 60 adds `tidb-planner::explore_mark` for memo round-bit state,
`tidb-stats::parse_auto_analyze_ratio` for source-compatible ratio parsing and
clamping, and `tidb-exec::slow_log_threshold` for typed slow-log threshold
helpers. Memo/statistics/session lifecycle and protocol publication remain
outside the dependency-closed leaves.
Wave 61 adds `tidb-planner::group_expr` for memo expression identity and
fingerprinting, `tidb-stats::AutoAnalysisTimeWindow` for inclusive minute
windows, and `tidb-exec::slow_log_rules` for typed rule metadata and grouping.
Full memo/statistics/session lifecycle and protocol publication remain outside
the dependency-closed leaves.
Wave 62 adds `tidb-planner::column_length` for `Col2Len` comparison,
`tidb-stats::calculate_priority_weight` for auto-analyze weighting, and
`tidb-exec::session_token_timing` for token/certificate timing policy. Path,
queue, crypto, and session lifecycle remain outside the dependency-closed
leaves.
Wave 63 adds `tidb-planner::plan_cache_constants` for plan-cache constant
copying, `tidb-stats::get_partition_sql`/`flatten_partition_names` for dynamic
partition helpers, and `tidb-exec::advisory_lock_state` for lock-owner
reference state. Full plan-cache/analysis-job/session lifecycle remains
outside the dependency-closed leaves.
Wave 64 adds `tidb-planner::index_advisor_model` for column/index identity and
prefix containment, `tidb-stats::priority_heap` for bounded auto-analyze heap
state, and `tidb-exec::txn_running_state` for transaction-state labels and
discriminants. Full advisor/queue/transaction lifecycle remains outside the
dependency-closed leaves.

Wave 65 adds `tidb-planner::rule_type` for source rule discriminants and raw
round-tripping, `tidb-stats::analysis_interval` for interval arithmetic and
source query constants, and `tidb-exec::txn_summary` for FNV-1a digest-sequence
tracking with bounded LRU state. Rule dispatch, query execution, JSON/duration
rendering, and live session/infoschema ownership remain outside the
dependency-closed leaves. The regenerated ledgers are 2,141/225/24/0
production and 15,902/529/142/12 test/support obligations; Wave 66 is the next
parallel source queue.

Wave 66 adds `tidb-planner::base_traits` for cascades hash/equality contracts,
`tidb-stats::auto_analyze_job` for bounded indicator formatting and dynamic
partition-job classification, and `tidb-exec::session_pool_capacity` for
system-session pool capacity normalization. Concrete cascades dispatch,
analysis-job/session lifecycle, and system-session factory/channel ownership
remain outside the dependency-closed leaves. The regenerated ledgers are
2,138/228/24/0 production and 15,899/532/142/12 test/support obligations; Wave
67 is the next parallel source queue.

Wave 67 adds `tidb-planner::scheduler_contract` for the source Scheduler
interface, `tidb-stats::non_partitioned_analysis` for exact analyze SQL and
index-kind metadata, and `tidb-exec::sysvar_scope` for ScopeFlag bits and
rendering. Concurrent scheduler/cascades context, analysis execution and
validation, and SysVar registry/SET/GET lifecycle remain outside the
dependency-closed leaves. The regenerated ledgers are 2,135/231/24/0
production and 15,891/540/142/12 test/support obligations; Wave 68 is the next
parallel source queue.

Wave 68 adds `tidb-planner::stack_contract` for the source Stack interface,
`tidb-stats::static_partitioned_analysis` for static partition SQL and queue
metadata, and `tidb-exec::charset_variable_groups` for ordered SET NAMES/SET
CHARSET variable groups. Concrete task-stack behavior, partition/session
analysis execution, and SET/collation/SessionVars lifecycle remain outside the
dependency-closed leaves. The regenerated ledgers are 2,132/234/24/0
production and 15,884/547/142/12 test/support obligations; Wave 69 is the next
parallel source queue.

Wave 69 adds `tidb-planner::topn_push_down` for the source rule wrapper,
`tidb-stats::queue_gate` for priority-queue initialization errors/defaults, and
`tidb-exec::sysvar_type` for TypeFlag discriminants and unknown-byte retention.
ScopeFlag and TypeFlag share one Go source ownership row but remain separate
Rust leaves. Full optimizer integration, queue lifecycle, and SysVar registry/
validation/conversion remain outside the dependency-closed leaves. The
regenerated ledgers are 2,130/236/24/0 production and 15,881/550/142/12
test/support obligations; Wave 70 is the next parallel source queue.

Wave 70 adds `tidb-planner::derive_topn_from_window` for the rule-wrapper
boundary, `tidb-stats::ddl_queue_gate` for DDL readiness decisions, and
`tidb-exec::sysvar_error` for variable error-code identities. Full window/TopN/
MPP integration, queue event/lifecycle mutation, and dbterror/message/SQLSTATE
plumbing remain outside the dependency-closed leaves. The regenerated ledgers
are 2,127/239/24/0 production and 15,877/554/142/12 test/support obligations;
Wave 71 is the next parallel source queue.

Wave 71 adds `tidb-planner::eliminate_empty_selection` for the recursive rule
wrapper, `tidb-stats::refresher_state` for ratio/prune-mode queue rebuild
decisions, and `tidb-exec::hint_updatable_vars` for the complete SET_VAR
registry. Logical-plan mutation, refresher/session workers, and SysVar/hint
application remain outside the dependency-closed leaves. The regenerated
ledgers are 2,124/242/24/0 production and 15,874/557/142/12 test/support
obligations; Wave 72 is the next parallel source queue.

Wave 72 adds `tidb-planner::push_down_sequence` for recursive sequence
CTE/main traversal and safe child attachment, `tidb-stats::worker_capacity`
for worker admission and unchanged-concurrency updates, and
`tidb-exec::noop_read_only` for the first five no-op/read-only registrations
and pure session/global read-only policy. Logical operator mutation, async
workers, full SysVar mutation, and warning/error/session lifecycle remain
outside the dependency-closed leaves. The regenerated ledgers are
2,121/245/24/0 production and 15,871/560/142/12 test/support obligations;
Wave 73 is the next parallel source queue.

Wave 73 adds `tidb-planner::eliminate_unionall_dual_item` for recursive
zero-row TableDual/projection filtering and schema-preserving empty-union
replacement, `tidb-stats::stats_key_set` for thread-safe LFU key-set
operations, and `tidb-exec::session_reuse_state` for owner-gated avoid-reuse
and idempotent close state. Logical operator execution, LFU admission/eviction,
table accounting, owner hooks, context close, in-use deferral, transfer, and
operation locking remain outside the dependency-closed leaves. The regenerated
ledgers are 2,118/248/24/0 production and 15,867/564/142/12 test/support
obligations; Wave 74 is the next parallel source queue.

Wave 74 adds `tidb-planner::projection_elimination` for loose projection
eligibility, `tidb-stats::stats_key_set_shards` for fixed 256-shard key-set
routing and aggregate operations, and `tidb-exec::system_db_filter` for the
exact `mysql` system-database filter with `SkipLoadDiff=false`. Full
expression/schema elimination, LFU admission/eviction and table accounting,
and domain/schema loading remain outside the dependency-closed leaves. The
regenerated ledgers are 2,114/252/24/0 production and 15,864/567/142/12
test/support obligations; Wave 75 is the next parallel source queue.

Wave 75 adds `tidb-planner::resolve_grouping_expand` for post-order Expand
traversal, `tidb-stats::memory_cost` for LFU capacity/cost policy, and
`tidb-exec::upgrade_versions` for the exact ordered 173-entry upgrade registry
with current version 263. Grouping-set construction, host-memory/cache
lifecycle, upgrade SQL/bootstrap mutation, and schema changes remain outside
the dependency-closed leaves. The regenerated ledgers are
2,111/255/24/0 production and 15,861/570/142/12 test/support obligations;
Wave 76 is the next parallel source queue.

Wave 76 adds `tidb-planner::join_reorder_projection_inline` for the recursive
source safety expression tree, `tidb-stats::BatchUpdate` for capacity-triggered
update/delete flushing, and `tidb-exec::session_metrics` for exact
delete/insert/update label registration order. Join-group substitution,
statistics queue/cache lifecycle, and metric collection remain external. The
regenerated ledgers are 2,108/258/24/0 production and 15,858/573/142/12
test/support obligations; Wave 77 is the next parallel source queue.

Wave 77 adds `tidb-planner::max_min_elimination` for source eligibility and
aggregate branch classification, `tidb-stats::MapCache` for caller-costed map
operations and copy state, and `tidb-exec::hash_join_version` for the
legacy/optimized version predicate. Index-path/plan construction, LFU
admission/eviction, cache ownership, SysVar mutation, and join selection remain
external. The regenerated ledgers are 2,105/261/24/0 production and
15,853/578/142/12 test/support obligations; Wave 78 is the next parallel
source queue.

Wave 78 adds `tidb-planner::logical_table_dual` for TableDual identity/hash
and explain metadata, `tidb-stats::healthy_metrics` for the exact ten healthy
buckets, and `tidb-exec::slow_log_match` for slow-log boolean composition and
precedence. Field-type/runtime details, metrics registration/traversal, and
slow-log accessors/thresholds/session state remain external. The regenerated
ledgers are 2,102/264/24/0 production and 15,850/581/142/12 test/support
obligations; Wave 79 is the next parallel source queue.

Wave 79 adds `tidb-planner::logical_limit` for Limit identity/hash and bounded
explain metadata, `tidb-stats::json_metadata` for the global marker and
deterministic predicate-column ordering, and `tidb-exec::privilege_set` for
exact privilege-set operations. Runtime limit behavior, tipb/storage and
stats-handle ownership, and GRANT/REVOKE SQL/persistence remain external. The
regenerated ledgers are 2,099/267/24/0 production and 15,847/584/142/12
test/support obligations; Wave 80 is the next parallel source queue.

Wave 80 adds `tidb-planner::logical_max_one_row` for the generated MaxOneRow
identity/hash contract, `tidb-stats::locked_tables` for the locked-table query
marker and requested-ID filter, and `tidb-exec::effective_auth_plugin` for
explicit plugin precedence and default fallback. Runtime planning, SQL/lock
lifecycle, auth storage, capability checks, and password policy remain
external. The regenerated ledgers are 2,096/270/24/0 production and
15,842/589/142/12 test/support obligations; Wave 81 is the next parallel
source queue.

Wave 81 adds `tidb-planner::logical_sort` for generated Sort identity/hash
framing, `tidb-stats::lock_messages` for stable skipped-table/partition
formatting, and `tidb-exec::broadcast_query_error` for the nil-safe
unsupported-broadcast classifier. Runtime ordering, lock/SQL lifecycle, and
broadcast RPC remain external.

Wave 82 adds `tidb-planner::logical_top_n` for generated TopN identity/hash
framing, `tidb-stats::usage_collector` for bounded priority queues and worker
drain/close behavior, and `tidb-exec::insert_rows_col_multiply` for zero-aware
saturating multiplication. Runtime TopN, session/worker lifecycle, and RUV2
metric wiring remain external. The regenerated ledgers are
2,090/276/24/0 production and 15,833/598/142/12 test/support obligations;
Wave 83 is the next parallel source queue.

Wave 83 adds `tidb-planner::logical_show_ddl_jobs` for generated ShowDDLJobs
identity/hash framing, `tidb-stats::stats_delta` for the locked-statistics
delta query marker and row/error behavior, and `tidb-exec::readable_size` for
case-sensitive human-readable size parsing with source-compatible wrapping.
DDL scheduling, statistics-handle ownership, inspection SQL, and caller policy
remain external. The regenerated ledgers are 2,087/279/24/0 production and
15,830/601/142/12 test/support obligations; the parser ring and full
workspace/Clippy gate remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` still tracked separately.
Wave 84 is the next parallel source queue.

Wave 84 adds `tidb-planner::logical_show` for generated Show identity/hash
framing with ordered normalized schema metadata, `tidb-stats::bootstrap_sql`
for exact statistics metadata and histogram bootstrap SQL with ordered IDs and
`[start,end)` paging, and `tidb-exec::placement_labels` for deterministic SHOW
PLACEMENT label grouping, deduplication, and row ordering. SHOW contents,
stats-handle/session/SQL execution, BinaryJSON/PD/store retrieval, and row
encoding remain external. The regenerated ledgers are 2,084/282/24/0
production and 15,823/608/142/12 test/support obligations; the parser ring and
full workspace/Clippy gate remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 85 is the next parallel source queue.

Wave 85 adds `tidb-planner::logical_schema_producer` for generated
LogicalSchemaProducer identity/hash framing with nil/present ordered schemas,
`tidb-stats::special_global_index` for the virtual-generated/prefix-column
global-index predicate and any-column short circuit, and
`tidb-exec::lazy_txn_state` for source-faithful `Valid`, `pending`, and
`validOrPending` composition. The lazy-transaction original test anchors were
already owned by the transaction-wave17/18 rings, so the shared test ledger
keeps one authoritative owner per anchor. Schema propagation, full field
metadata, index metadata resolution, KV/session lifecycle, and transaction
execution remain external. The regenerated ledgers are 2,081/285/24/0
production and 15,821/610/142/12 test/support obligations; the parser ring and
full workspace/Clippy gate remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 86 is the next parallel source queue.

Wave 86 adds `tidb-planner::logical_sequence` for generated LogicalSequence
identity/hash framing, `tidb-stats::global_topn` for histogram-free partition
TopN aggregation with wrapping sums and source ranking, and
`tidb-exec::config_int_json` for integer SET CONFIG JSON rendering. The global
TopN evidence uses the unclaimed `global_stats_test.go:322 TestGlobalStatsData3`
anchor (ranking assertions at 342-347); narrower TopN anchors remain owned by
the earlier datum-map-cache ring. CTE/runtime sequence behavior, histograms,
Datum/config mutation, storage, and session lifecycle remain external. The
regenerated ledgers are 2,078/288/24/0 production and 15,818/613/142/12
test/support obligations; the parser ring and full workspace/Clippy gate remain
green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 87 is the next parallel source queue.

Wave 87 adds `tidb-planner::logical_union_all` for generated LogicalUnionAll
identity/hash framing, `tidb-stats::pending_delta_ids` for pending table-ID
filtering/deduplication/order, and `tidb-exec::lack_handles` for ordered missing
handle reconciliation with the source cardinality boundary. The initially
selected LogicalSelection owner was already claimed and was removed before
integration. Union execution, stats/session sweeps, storage, KV encoding,
workers, and consistency reporting remain external. The regenerated ledgers
are 2,075/291/24/0 production and 15,815/616/142/12 test/support obligations;
the parser ring and full workspace/Clippy gate remain green, with the pinned Go
restore failure at `tests/integrationtest/t/expression/json.test:582` tracked
separately.
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

Wave 99 adds `tidb-planner::columnar_index_extra` for source-shaped vector
columnar-index metadata, `tidb-stats::ddl_stats_delta` for the locked,
missing-row, and existing-row stats SQL branches with ordered arguments and
Go-compatible wrapping, and `tidb-exec::cume_dist` for the source
`curIdx`/`lastRank` tied-peer algorithm as an Iterator with partial-state size.
The exact Go anchors are
`pkg/planner/core/task_heavy_function_optimize_test.go:36`,
`pkg/statistics/handle/ddl/ddl_test.go:1106`/`:1256`, and
`pkg/executor/aggfuncs/func_cume_dist_test.go:25` plus
`pkg/executor/aggfuncs/window_func_test.go:172`. TiFlash/vector planning, DDL/storage/session
lifecycle, row comparison, window scheduling, and chunk execution remain
external. The regenerated ledgers are 2,039/327/24/0 production and
15,762/669/142/12 test/support obligations; parser, workspace, Clippy, and
static domain gates remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 100 is now the next parallel source queue.

Wave 100 adds `tidb-planner::physical_cte_table` for signed CTE storage
identity, `Scan on CTE_<id>` explain text, and index-join/sort task rejection;
`tidb-stats::gc_batch_count` for Go `forCount` division, positive-remainder
rounding, and signed overflow; and `tidb-exec::ntile` for five-field partial
state, quotient/remainder updates, reset, group advancement, and zero-divisor
NULL behavior. Exact anchors are
`pkg/planner/core/tests/redact/redact_test.go:23`,
`pkg/statistics/handle/storage/gc_test.go:30`/`:63`, and
`pkg/executor/aggfuncs/func_ntile_test.go:25`. Typed task/schema/runtime,
storage/session GC, chunk, argument-coercion, and window scheduling remain
external. The regenerated ledgers are 2,036/330/24/0 production and
15,758/673/142/12 test/support obligations.

Wave 101 adds `tidb-exec::lead_lag` for the buffered row cursor, physical
lead/lag offsets, current-row/default fallback, reset, and partial-state size.
Typed Datum serialization, chunk/window construction, and scheduling remain
external. Exact anchors are
`pkg/executor/aggfuncs/func_lead_lag_test.go:27`/`:119`. The combined
regenerated ledgers are 2,035/331/24/0 production and 15,756/675/142/12
test/support obligations; parser, workspace, Clippy, and static domain gates
remain green, with the pinned Go restore failure at
`tests/integrationtest/t/expression/json.test:582` tracked separately.
Wave 102 adds `tidb-planner::physical_max_one_row`, `tidb-stats::StatsLease`,
and `tidb-exec::json_arrayagg`. The planner leaf preserves the MaxOneRow
support gates, fixed expected count, and CTE/no-cop metadata; the stats leaf
preserves atomic signed-nanosecond lease state; and the executor leaf preserves
ordered JSON aggregation, partial merge/reset, empty-input NULL, framing,
escaping, finite-real guards, and explicit spill boundaries. Exact Go anchors
are `pkg/executor/test/executor/executor_test.go:2157`,
`pkg/statistics/integration_test.go:220`/`:266`,
`pkg/executor/aggfuncs/func_json_arrayagg_test.go:27`/`:65`/`:131`, and
`pkg/executor/aggfuncs/spill_helper_test.go:842`. Typed conversion, runtime
execution, storage/session/statistics lifecycle, and deployable bootstrap remain
external. The regenerated ledgers are 2,032/334/24/0 production and
15,749/682/142/12 test/support obligations; the consolidated 12-job static
gate is green. Wave 103 was integrated into the next verified workspace cycle.

Wave 103 adds `tidb-planner::logical_cte_table` for the exact `DeriveStats`
reload-vector transition, `tidb-stats::global_stats_layout` for the
`newGlobalStats` zero/nil slot layout, and `tidb-exec::json_objectagg` for
ordered key/value state, source-after-destination merge, duplicate-key
last-wins, lexicographic JSON framing, empty-input NULL, and NULL/binary-key
rejection. Exact Go anchors are
`pkg/planner/core/casetest/planstats/plan_stats_test.go:281`,
`pkg/statistics/handle/globalstats/global_stats_test.go:137`,
`pkg/executor/aggfuncs/func_json_objectagg_test.go:48`/`:110`/`:163`, and
`pkg/executor/aggfuncs/spill_helper_test.go:889`. Typed evaluation, concrete
stats/schema context, BinaryJSON/memory/spill integration, chunk execution,
and storage/session lifecycle remain external. The regenerated ledgers are
2,029/337/24/0 production and 15,743/688/142/12 test/support obligations; the
consolidated 12-job static gate is green. Wave 104 was integrated into the next
verified workspace cycle.

Wave 104 adds `tidb-planner::telemetry`, `tidb-stats::table_id_filter`, and
`tidb-exec::first_row`. These preserve exact TiFlash plan-tree traversal and
ExchangeSender classification, source-ordered `table_id in (...)` formatting
including empty input, and first-physical-row-wins aggregation state including
NULL/merge/reset behavior. Exact anchors are
`pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go:568`,
`pkg/executor/test/infoschema/infoschema_test.go:171`/`:224`,
`pkg/executor/aggfuncs/func_first_row_test.go:27`/`:52`, and the ten spill
anchors under `pkg/executor/aggfuncs/spill_helper_test.go:941` through `:1349`.
Concrete plans/session telemetry, cache/InfoSchema lifecycle, typed values,
chunk output, memory, and spill encoding remain external. The regenerated
ledgers are 2,026/340/24/0 production and 15,728/703/142/12 test/support
obligations; the consolidated 12-job static gate is green. Wave 105 was
integrated into the next verified workspace cycle.

Wave 105 adds `tidb-planner::condition_to_dual`, `tidb-stats::auto_analyze_process_set`,
and `tidb-exec::bit_agg`. These preserve exact NULL/false condition reduction
and plan-cache suppression, RWMutex-backed auto-analyze process tracking, and
u64 AND/OR/XOR aggregate state with NULL-skipping updates, merge, and reset.
Exact anchors are `pkg/planner/core/logical_plans_test.go:241`,
`pkg/statistics/handle/autoanalyze/exec/exec_test.go:35`/`:154`,
`pkg/executor/aggfuncs/func_bitfuncs_test.go:25`/`:36`, and
`pkg/executor/aggfuncs/spill_helper_test.go:801`. Typed evaluation, concrete
stats/process execution, chunk/sliding/memory/spill integration, and optimizer
runtime remain external. The regenerated ledgers are 2,023/343/24/0 production
and 15,722/709/142/12 test/support obligations; the consolidated 12-job static
gate is green. Wave 106 is now the next parallel source queue.

Wave 106 adds `tidb-planner::physical_table_sample`,
`tidb-stats::stats_meta_save_sql`, and `tidb-exec::varpop`. These preserve exact
TableSample initialization metadata, source-ordered `stats_meta` upsert tuple
assembly with optional histogram-version metadata, and non-DISTINCT float64
population variance state with NULL skipping, source merge formulas,
zero-count branches, output, and reset. Exact anchors are
`pkg/executor/sample_test.go:111`, `pkg/statistics/integration_test.go:442`,
and `pkg/executor/aggfuncs/func_varpop_test.go:28`/`:37`/`:46`/`:54`. Typed
evaluation, SQL/storage/session execution, DISTINCT sets, chunk/sliding/memory,
spill, and runtime wiring remain external. The regenerated ledgers are
2,020/346/24/0 production and 15,716/715/142/12 test/support obligations; the
consolidated 12-job static gate is green. Waves 107-112 were integrated below;
Wave 113 is the next parallel source queue.

Wave 107 adds `tidb-planner::rule_set`, `tidb-stats::init_stats_progress`, and
`tidb-exec::sum_float64`. These preserve source-shaped rule-ID mask filtering
and intermediate Apply selection, init-stats progress arithmetic with exact
Go float64 coercion including IEEE zero-denominator behavior, and non-DISTINCT
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
the consolidated 12-job static gate is green. Waves 108-112 were integrated
below; Wave 113 is the next parallel source queue.

Wave 108 adds `tidb-planner::column_pruning`,
`tidb-stats::global_stats_sql_index`, and `tidb-exec::group_concat`. These
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
15,698/733/142/12 test/support obligations; the consolidated 12-job static
gate is green. Waves 109-112 were integrated below; Wave 113 is the next
parallel source queue.

Wave 109 adds `tidb-planner::physical_union_scan`,
`tidb-stats::ddl_physical_ids`, and `tidb-exec::sum_int`. These preserve
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
15,696/735/142/12 test/support obligations; the consolidated 12-job static
gate is green. Waves 109-112 were integrated below; Wave 113 is the next
parallel source queue.

Wave 110 adds `tidb-planner::physical_show`, `tidb-stats::stats_cache_version`,
and `tidb-exec::percentile`. These preserve PhysicalShow/PhysicalShowDDLJobs
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
obligations; the consolidated 12-job static gate is green. Waves 110-112 were
integrated below; Wave 113 is the next parallel source queue.

Wave 111 adds `tidb-planner::physical_lock`, `tidb-stats::topn_merge_task`,
and `tidb-exec::avg_float64`. These preserve PhysicalLock TiFlash rejection,
`Lock` plan metadata, query-block offset zero, opaque lock type, lossless wait
seconds, and exact ExplainInfo; the TopN merge-task range descriptor without
validation; and non-DISTINCT float64 AVG sum/count, NULL/empty behavior,
merge/reset, and incoming-before-outgoing sliding order. AST/catalog/task/lock
execution, TopN worker/concurrency/merge arithmetic, typed AVG coercion,
decimal/DISTINCT, rounding/context, chunk/memory, and spill remain external.
Exact anchors are `pkg/planner/core/tests/pointget/point_get_plan_test.go:407`,
`pkg/statistics/handle/globalstats/topn_bench_test.go:94`, and
`pkg/executor/aggfuncs/func_avg_test.go:27`/`:37`/`:48`. The regenerated
ledgers are 2,005/361/24/0 production and 15,685/746/142/12 test/support
obligations; the consolidated 12-job static gate is green. Wave 112 was
integrated below; Wave 113 is the next parallel source queue.

Wave 112 adds `tidb-planner::physical_table_dual`,
`tidb-stats::json_stats_version`, and `tidb-exec::minmax_deque`. These preserve
PhysicalTableDual `Dual` metadata, query-block offset, `rows:<RowCount>` explain
text, IndexJoin rejection, and row-count-dependent sort admission; the old JSON
StatsVer fallback where explicit versions win and missing positive NDV/null-count
infers version 1; and MinMaxDeque pair storage, deque operations, reset, expiry
dequeue, and monotonic max/min enqueue with equal-value eviction. Schema/
catalog/task wiring, JSON/storage/session lifecycle, typed MAX/MIN evaluation,
window callbacks, chunk/memory, and spill remain external. Exact anchors are
`pkg/planner/core/casetest/cbotest/cbo_test.go:367`,
`pkg/statistics/handle/storage/dump_test.go:582`, and
`pkg/executor/aggfuncs/func_max_min_test.go:335`/`:345`. The regenerated
ledgers are 2,002/364/24/0 production and 15,681/750/142/12 test/support
obligations; the consolidated 12-job static gate is green. Wave 113 is the
next parallel source queue.

Wave 113 adds `tidb-planner::logical_lock`, `tidb-stats::stats_lock_table`, and
`tidb-exec::count_distinct_int`. These preserve raw lock discriminants and the
supported FOR UPDATE/FOR SHARE sets, table-lock payloads with nil-versus-empty
partition-map semantics, and typed-int DISTINCT NULL skipping, deduplication,
partial merge, cardinality, and reset. Exact anchors are
`pkg/planner/core/integration_test.go:1466`,
`pkg/statistics/handle/lockstats/lock_stats_test.go:186`/`:260`, and
`pkg/executor/aggfuncs/func_distinct_agg_test.go:26` plus
`pkg/executor/aggfuncs/func_count_test.go:115`. SQL/session/lock execution,
other DISTINCT types, typed Eval/chunk/memory/spill integration, and runtime
scheduling remain external. The regenerated ledgers are 1,999/367/24/0
production and 15,676/755/142/12 test/support obligations; the consolidated
12-job workspace, Clippy, formatting, parser, plan, ledger, and domain gates
are green. Wave 114 is now the next parallel source queue.

Wave 114 adds `tidb-planner::physical_exchange_receiver`,
`tidb-stats::pseudo_cache_policy`, and `tidb-exec::window_value_int`. These
preserve `ExchangeReceiver` plan identity/root offset/uint64 stream-count
metadata and exact explain rendering, pseudo-statistics cache admission below
the partitioned threshold of 64 with temporary-table rejection, and
already-evaluated integer FIRST_VALUE/LAST_VALUE/NTH_VALUE state transitions
including NULL capture, batch-spanning selection, reset, and unreached output.
Exact anchors are `pkg/planner/core/integration_test.go:904`,
`pkg/statistics/handle/handletest/handle_test.go:1100`, and
`pkg/executor/aggfuncs/func_value_test.go:63`. MPP task/runtime wiring,
pseudo-table/cache/session lifecycle, typed evaluators, all value domains,
chunk/memory/window dispatch, and scheduling remain external. The regenerated
ledgers are 1,996/370/24/0 production and 15,673/758/142/12 test/support
obligations; the consolidated 12-job workspace, Clippy, formatting, parser,
plan, ledger, and domain gates are green. Wave 115 was integrated below; Wave
116 is now the next parallel source queue.

Wave 115 adds `tidb-planner::physical_selection`, `tidb-exec::spill_count`, and
`tidb-stats::cache_metrics_labels`. These preserve Selection plan identity,
caller-owned query-block offsets and exact condition/stream explain text;
native-endian int64 count-spill serialization, strict decoding, reusable
buffers, and sequential row consumption; and the six source-ordered cache
counter labels plus two gauge labels. Exact anchors are
`pkg/planner/core/casetest/mpp/mpp_test.go:673`,
`pkg/executor/aggfuncs/spill_helper_test.go:73`, and
`pkg/statistics/handle/cache/bench_test.go:99`. MPP/runtime wiring, typed
expression and aggregate domains, chunk/spill lifecycle, Prometheus handles,
cache concurrency, and session/storage integration remain external. The
regenerated ledgers are 1,993/373/24/0 production and 15,670/761/142/12
test/support obligations; the consolidated 12-job workspace, Clippy,
formatting, parser, plan, ledger, and domain gates are green. The evidence
fragment loader now rejects escaped `\t` headers. Wave 116 is now the next
parallel source queue.

Wave 116 adds `tidb-planner::physical_limit`, `tidb-exec::pd_approximate_count`,
and `tidb-stats::ddl_event_match`. These preserve Limit plan identity,
query-block offset, lossless offset/count metadata, and ExplainInfo redaction
branches over caller-owned partition/prefix text; the direct underscore-joined
approximate-count key plus bounded TTL/LRU hit/miss/eviction behavior with a
caller-supplied clock; and first-match DDL event selection with no-match
timeout behavior. Exact anchors are
`pkg/planner/core/casetest/physicalplantest/physical_plan_test.go:1600`,
`pkg/executor/internal/pdhelper/pd.go:69-85` plus
`pkg/executor/internal/pdhelper/pd_test.go:42`, and
`pkg/statistics/handle/autoanalyze/priorityqueue/queue_ddl_handler_test.go:885`.
Typed planner properties, PD/storage and restricted-SQL access, channel/ticker
timing, notifier decoding, and full planner/executor/statistics/session/SQL
lifecycle remain external. The regenerated ledgers are 1,990/376/24/0
production and 15,667/764/142/12 test/support obligations; parser, workspace,
Clippy, formatting, and static ledger/parser/plan/domain gates pass with 12
jobs. Wave 117 is now the next parallel source queue.

Wave 117 adds `tidb-planner::physical_union_all`, `tidb-exec::apply_cache`,
and `tidb-stats::mock_statistics_shape`. These preserve Union plan identity,
query-block offset, MPP flag, and source Exhaust gates/candidate ordering;
byte-key/value memory charge, over-quota rejection, oldest-entry LRU eviction,
and get-touch/accounting behavior; and fixture column/index counts with
CMSketch/TopN/histogram switches plus total item count. Exact anchors are
`pkg/planner/core/casetest/mpp/mpp_test.go:446`,
`pkg/executor/internal/applycache/apply_cache.go:35-43,76-101` plus
`pkg/executor/internal/applycache/apply_cache_test.go:30`, and
`pkg/statistics/handle/cache/bench_test.go:125`. Child planner properties,
typed chunk/memory/session quota, statistics allocation/cache concurrency, and
runtime/benchmark integration remain external. The regenerated ledgers are
1,987/379/24/0 production and 15,664/767/142/12 test/support obligations;
parser, workspace, Clippy, formatting, and static ledger/parser/plan/domain
gates pass with 12 jobs. Wave 118 is now the next parallel source queue.

Wave 118 adds `tidb-planner::physical_apply`, `tidb-exec::next_io_acc`, and
`tidb-stats::stats_request_matcher`. These preserve Apply plan identity and
offset plus the exact non-PhysicalJoin boundary; positive row/cell guards,
reset/reuse, wrapping accumulation, and child/parent/tracking admission; and
the exact `internal_StatsForegroundPriority` predicate and matcher description.
Exact anchors are
`pkg/planner/core/casetest/physicalplantest/physical_plan_test.go:1537`,
`pkg/executor/internal/exec/executor.go:42-89` plus
`pkg/executor/internal/exec/executor_test.go:35`, and
`pkg/statistics/handle/util/test/ctx_matcher.go:24-36` plus
`pkg/statistics/handle/autoanalyze/autoanalyze_test.go:407`. Hash-join/subquery
runtime, executor atomics/provider/pool/RUV2, context/request propagation,
gomock/SQL/session lifecycle, and full integration remain external. The
regenerated ledgers are 1,984/382/24/0 production and 15,661/770/142/12
test/support obligations; parser, workspace, Clippy, formatting, and static
ledger/parser/plan/domain gates pass with 12 jobs. Wave 119 is now the next
parallel source queue.

Wave 119 adds `tidb-planner::physical_projection`, `tidb-exec::cluster_index_id`,
and `tidb-stats::predicate_column_query_mode`. These preserve Projection plan
identity/offset, opaque expression-list rendering, and the uint64 stream-count
suffix; clustered-index identity selection for PK-as-handle, common-handle
primary indexes, and rowid/non-clustered tables; and the exact predicate-column
transaction boundary (`LoadColumnStatsUsage` without `FlagWrapTxn`,
`GetPredicateColumns` with it). Exact anchors are
`pkg/planner/core/casetest/mpp/mpp_test.go:710`,
`pkg/executor/internal/exec/indexusage.go:130-148` plus
`pkg/executor/internal/exec/indexusage_test.go:447`, and
`pkg/statistics/handle/usage/predicate_column.go:47-62` plus
`pkg/statistics/handle/usage/predicate_column_test.go:103`. Typed projection,
table/index collector, session-pool/SQL, and full planner/executor/statistics
integration remain external. The regenerated ledgers are 1,981/385/24/0
production and 15,658/773/142/12 test/support obligations; parser, workspace,
Clippy, formatting, and static ledger/parser/plan/domain gates pass with 12
jobs. Wave 121 is now the next parallel source queue.

Wave 120 adds `tidb-planner::physical_shuffle`, `tidb-stats::index_usage_key`,
and `tidb-exec::mock_global_accessor`. These preserve `Shuffle` plan identity
and query-block offset, hash/range splitter discriminants, and source-shaped
concurrency/data-source ExplainInfo; the exact table-ID/index-ID lookup pair
used by index-usage GC; and ordinary/test-suite variable maps, unknown-variable
errors, default authentication plugin validation plus its bypass setter, and
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
Clippy, formatting, parser, plan, ledger, and domain gates pass. Wave 121 is now
the next parallel source queue.

Wave 121 adds `tidb-planner::physical_exchange_sender`,
`tidb-stats::stats_table_snapshot`, and `tidb-exec::vec_group_checker_int`.
These preserve `ExchangeSender` identity/root offset zero and ExplainInfo
exchange labels, compression names/fallback, hash-column text, ordered task
IDs, and uint64 `stream_count`; the `AssertTableEqual` realtime/modify counts,
column/index cardinality, per-ID item/payload/nil shape, and existence bytes;
and integer/NULL group boundaries, cross-chunk first-group continuity,
offsets/count, cursor ranges, exhaustion/reset, and the non-empty-chunk error.
Exact anchors are `pkg/planner/core/operator/physicalop/physical_exchange_sender.go:222`
plus `pkg/planner/core/casetest/mpp/mpp_test.go:78`,
`pkg/statistics/handle/internal/testutil.go:25-55` plus
`pkg/statistics/handle/handletest/statstest/stats_test.go:307`, and
`pkg/executor/internal/vecgroupchecker/vec_group_checker.go:80-151,524-564`
plus `pkg/executor/internal/vecgroupchecker/vec_group_checker_test.go:141`.
MPP runtime, statistics table/payload/storage lifecycle, expression/chunk and
codec evaluation, collations, non-integer/vector groups, and stream aggregation
remain external. The regenerated ledgers are 1,975/391/24/0 production and
15,652/779/142/12 test/support obligations; the consolidated 12-job workspace,
Clippy, formatting, parser, plan, ledger, and domain gates pass. Wave 122 is now
the next parallel source queue.

Wave 122 adds `tidb-planner::physical_window`, `tidb-exec::concurrent_entry_map`,
and `tidb-stats::stats_cache_inner`. These preserve Window plan identity,
initialization offset, inherited uint64 fine-grained-shuffle stream-count clone
state, and the optional ExplainInfo suffix; 320-shard routing,
lock-protected prepend chains, lookup/snapshot iteration, length/empty, row
identity, and portable accounting; and the eleven-method cache interface
(`Get`, `Put`, `Del`, `Cost`, `Values`, `Len`, `Copy`, `SetCapacity`, `Close`,
`TriggerEvict`, and `WaitForAsyncUpdates`) over opaque values. Exact anchors
are `pkg/planner/core/operator/physicalop/physical_window.go:480` plus
`pkg/planner/core/plan_test.go:681`, `pkg/executor/join/concurrent_map.go:20-79`
plus `pkg/executor/join/concurrent_map_test.go:27,70`, and
`pkg/statistics/handle/cache/internal/inner.go:18-50` plus
`pkg/statistics/handle/cache/internal/lfu/lfu_cache_test.go:49`. PhysicalSort
sharing, MPP runtime, the Go memory-map ABI/constants and hash-join trackers,
LFU admission/eviction/async/metrics, and full statistics storage lifecycle
remain external. The regenerated ledgers are 1,972/394/24/0 production and
15,648/783/142/12 test/support obligations; the consolidated 12-job workspace,
Clippy, formatting, parser, plan, ledger, and domain gates pass. Wave 123 is now
the next parallel source queue.

Wave 123 validates the two-speed parallel protocol with one larger source
family. `tidb-planner::physical_sort` owns dependency-closed Sort metadata and
source formatting/clone/memory contracts; `tidb-exec::join_table_meta` owns the
six-test hash-join metadata decision family; and `tidb-stats::StatsPool` owns
the opaque resource-access lifecycle boundary. Shared exact Go test anchors
remain singly owned in `go_test_domain_manifest.tsv`, while each new source
file and non-shared test anchor has an owner-named evidence fragment. Typed
planner/runtime wiring, join encoding/execution, live FieldType/chunk/codec
semantics, and concrete statistics pool/session lifecycle remain external.
The regenerated ledgers are 1,969/397/24/0 production and
15,639/792/142/12 test/support obligations. One reused-target workspace batch,
the focused post-Clippy executor regression, strict Clippy, formatting, and all
static ledger/parser/plan/domain gates pass. Wave 124 is next.

Wave 124 raises assignment size again: `physical_topn` owns one physical-plan
metadata source, `OrderedApplyBuffer` owns the ordered-result state machine and
all seven exact ordered-Apply tests, and `BoundedMinHeap` owns one complete
generic source plus all seven tests. Review corrected missing panic/kill
anchors, the source idle-partial-flush transition, independent normalized plan
text, and direct comparator negation before integration. Shared Go test files
remain symbol-partitioned through the exact manifest; no test or production
file gained two evidence owners. The ledgers are 1,966/400/24/0 production and
15,623/808/142/12 test/support obligations. One reused-target workspace test
batch, focused nine-test post-Clippy heap validation, full strict Clippy, formatting, and
all static ledger/parser/plan/domain gates pass. Wave 125 is next.

## Checked active-domain queue

`workstreams/domains/*.toml` is the small, checked contract for currently
active vertical source domains. Unlike the complete generated source/test
ledgers, it is deliberately sparse: it prevents two parallel agents from
claiming the same Go production owner while recording the exact Rust leaves,
evidence, status, and focused 12-job validation commands for that slice.
`difftest domain_queue -- --check` verifies those relationships before the
normal evidence gate. Agent-local files under `workstreams/claims/` are
ignored advisory leases, not shared ownership state.

The next independent source domains are:

| Go source ownership | Rust leaf ownership | Root that must shrink |
| --- | --- | --- |
| complete `pkg/parser/ddl_index_parser.go` plus standalone CREATE INDEX in `ddl_misc_parser.go` | `tidb-parser/src/ddl/index.rs`, `tidb-ast/src/ddl_index.rs`, and executor DDL readers share one source-shaped index/FK contract; 173/173 attributable AST/parser checks and 64 broad `TestDDL` rows execute | complete the broader parser test/source audit before moving this still-partial owner to `ported`; executor capability remains independently explicit |
| complete `pkg/parser/ddl_fieldtype_parser.go` | `tidb-ast/src/ddl/field_type.rs` and `tidb-parser/src/ddl/field_type.rs`, with a mirrored source test | continue direct FieldType aliases/modifiers and charset semantics without reopening table-DDL roots |
| `parseAlterAlter` index-visibility / `parseAlterPartition` re-partitioning branches | `tidb-{ast,parser}/src/ddl/alter/{index_visibility,repartition}.rs`, existing partition AST/executor boundary, and exact Go-source selectors | continue CHECK, column-default, and named-partition families without reopening the ported leaves |
| complete `pkg/parser/ddl_table_option_parser.go` | `tidb-ast/src/ddl/table_option.rs` and `tidb-parser/src/ddl/table_option.rs`, with exact method-family ownership | continue options such as AFFINITY and AUTO_RANDOM without reopening CREATE/ALTER table grammar |
| remaining DDL table families | matching `ddl/` leaves as each complete Go source unit is assigned | `tidb-parser/src/ddl.rs`, `tidb-ast/src/ddl.rs` |
| Go builtin families | `tidb-expr/src/<family>_fn.rs` or `<family>_fn/` plus typed construction; `builtin_time.go` owns `time_fn/` and `builtin_math.go` owns `math_fn/` | `tidb-expr/src/func.rs`, which must remain dispatch-only |
| connection, transaction, and statement effects | `session_runtime.rs`, `cluster.rs`, then real transaction services | remaining coordination in `tidb-exec/src/database.rs` |

Each top-level Go parser source owns one
`difftests/corpus/coverage/evidence/parser/<source-stem>.tsv` fragment. A
large source may be divided into explicitly non-overlapping named Go-symbol
families only after its Rust leaves are physically separated; the checked
domain queue rejects an unknown or overlapping family. This keeps direct
source transit parallel without letting two agents silently own the same Go
rule. The evidence steward generates `parser_translation_manifest.tsv` and
its summary from those leaves. The Go-test ledger does the same for test obligations
across the SQL node. Its default unit is one complete Go test file; only a
checked exact-anchor row in `difftests/corpus/coverage/go_test_domain_manifest.tsv`
can split a shared file. Literal `t.Run` children and dynamic/table-driven
`t.Run` generators remain attached to that exact top-level test domain, while
file hooks and fixture setup stay in an explicit shared-support unit. This
keeps test parallelism real without permitting a whole-file coverage claim to
hide an unassigned child: every unclaimed top-level anchor is rendered as an
explicit `UNTRIAGED` ledger row. Meanwhile,
`go_test_fixture_access_inventory.tsv` is a separate Go-AST source record for
every test-source `//go:embed` directive and `os` file access. A direct local
string literal creates an exact fixture row; a glob, join, helper, dynamic, or
repository-escaping expression remains a visible unresolved source row rather
than being guessed or dropped.
`go_source_inventory.tsv` keeps every production Go owner routed to a target
crate or an explicit architecture-triage boundary. Moving code without
updating these checked ownership artifacts is an incomplete refactor.
