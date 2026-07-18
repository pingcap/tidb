# Statistics workstream

This workstream owns source-backed statistics leaves with explicit dependency
boundaries. The Go implementation under `pkg/statistics` remains authoritative;
each Rust fragment is paired with one exact source/test evidence owner and keeps
unported storage and lifecycle behavior visible.

## Auto-analyze trigger policy

`tidb-stats::need_analyze_table` ports the bounded `NeedAnalyzeTable` policy
from `pkg/statistics/handle/autoanalyze/autoanalyze.go`: unanalyzed tables
always trigger, a zero ratio disables re-analysis for analyzed tables, a
positive analyzed-row count supersedes realtime rows, and the modification
ratio returns the source diagnostic reason. All inputs are caller-owned scalar
metadata so the policy stays independent of queue and SQL lifecycle code.

This leaf does not construct `statistics.Table`/`HistColl`, read global
configuration, schedule ANALYZE statements, or manage auto-analyze windows.
Focused source-backed tests are in
`crates/tidb-stats/tests/auto_analyze_policy_source.rs`, with ownership
recorded in `difftests/corpus/coverage/evidence/source/stats-auto-analyze-policy-wave.tsv`
and `evidence/tests/stats-auto-analyze-policy-wave.tsv`.

## Auto-analyze ratio parsing

`tidb-stats::parse_auto_analyze_ratio` ports the source parser from
`pkg/statistics/handle/autoanalyze/exec/exec.go`: invalid values use the
`0.5` default, valid negatives clamp to zero, and finite/infinite/NaN values
follow Go's `math.Max` behavior. It is scalar parsing only; the SQL/session
owner still supplies the configured value.

Time-window parsing, global-variable reads, execution metrics, and auto-analyze
scheduling remain outside this leaf. Focused source-backed tests are in
`crates/tidb-stats/tests/auto_analyze_ratio_source.rs`, with ownership recorded
in `difftests/corpus/coverage/evidence/source/stats-auto-analyze-ratio-wave.tsv`
and `evidence/tests/stats-auto-analyze-ratio-wave.tsv`.

## Auto-analysis daily window

`tidb-stats::AutoAnalysisTimeWindow` ports the source daily-window policy from
`pkg/statistics/handle/autoanalyze/priorityqueue/analysis_job_factory.go`.
It compares caller-owned UTC minutes inclusively, rejects unset endpoints, and
preserves windows that cross midnight. The minute boundary is explicit so the
future timezone/parser owner can supply the same UTC projection as Go.

This leaf does not parse session time-zone strings, read global variables,
schedule queue jobs, or execute ANALYZE. Focused source-backed tests are in
`crates/tidb-stats/tests/auto_analyze_window_source.rs`, with ownership
recorded in
`difftests/corpus/coverage/evidence/source/stats-auto-analyze-window-wave.tsv`
and `evidence/tests/stats-auto-analyze-window-wave.tsv`.

## Auto-analyze priority arithmetic

`tidb-stats::calculate_priority_weight` ports the scalar score from
`pkg/statistics/handle/autoanalyze/priorityqueue/calculator.go`: 60% weighted
change-ratio log, 10% inverse table-size log, 30% analysis-interval log, and
the two-point newly-added-index event. The API accepts caller-owned indicators
so queue/job construction remains a separate owner.

The leaf does not build `AnalysisJob` values, sort a priority queue, read
configuration, or schedule statistics work. Focused source-backed tests are in
`crates/tidb-stats/tests/priority_calculator_source.rs`, with ownership
recorded in
`difftests/corpus/coverage/evidence/source/stats-priority-calculator-wave.tsv`
and `evidence/tests/stats-priority-calculator-wave.tsv`.

## Dynamic-partition helper assembly

`tidb-stats::{get_partition_sql,flatten_partition_names}` ports the bounded
string/list helpers from
`pkg/statistics/handle/autoanalyze/priorityqueue/dynamic_partitioned_table_analysis_job.go`.
It preserves `%n` placeholder commas, caller suffixes, and append-order
partition-name flattening while keeping SQL execution and schema lookup out of
the statistics leaf.

The leaf does not validate partition metadata, escape SQL, run analyze workers,
or persist jobs. Focused source-backed tests are in
`crates/tidb-stats/tests/dynamic_partition_helpers_source.rs`, with ownership
recorded in
`difftests/corpus/coverage/evidence/source/stats-dynamic-partition-helpers-wave.tsv`
and `evidence/tests/stats-dynamic-partition-helpers-wave.tsv`.

## Priority queue heap metadata

`tidb-stats::PriorityHeap` ports the map-plus-indexed-queue boundary from
`pkg/statistics/handle/autoanalyze/priorityqueue/heap.go`: descending-weight
max-heap ordering, table-ID upsert/update, arbitrary deletion, peek/pop,
keyed lookup, live-item enumeration, and length/empty state. The queue uses
the source's direct `>` comparison, so NaN weights do not acquire a synthetic
ordering.

This leaf owns only caller-provided table IDs and weights. It does not create
`AnalysisJob` values, execute ANALYZE, read configuration, schedule workers,
or provide process-level synchronization. Focused source-backed tests are in
`crates/tidb-stats/tests/priority_heap_source.rs`, with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-priority-heap-wave.tsv` and
`evidence/tests/stats-priority-heap-wave.tsv`.

## Auto-analyze interval metadata

`tidb-stats::analysis_interval` ports the dependency-closed boundary from
`pkg/statistics/handle/autoanalyze/priorityqueue/interval.go`: table versus
partition query selection, the four source SQL shapes, `NoRecord` and
just-failed sentinels, negative failed-duration fallback, and Go's truncation
of average seconds before conversion to a duration. Durations are represented
as signed nanoseconds so the source `NoRecord` value remains distinguishable.

This leaf does not execute SQL, decode session rows, construct
`mysql.analyze_jobs`, or decide queue scheduling. Focused source-backed tests
are in `crates/tidb-stats/tests/analysis_interval_source.rs`, with ownership
recorded in
`difftests/corpus/coverage/evidence/source/stats-analysis-interval-wave.tsv`
and `evidence/tests/stats-analysis-interval-wave.tsv`.

## Auto-analyze job metadata

`tidb-stats::auto_analyze_job` ports the dependency-closed metadata boundary
from `pkg/statistics/handle/autoanalyze/priorityqueue/job.go`: scalar
`Indicators`, the `asJSONIndicators` percentage/size/duration formatting, and
the dynamic-partitioned job-kind predicate. Duration formatting keeps Go's
hour/minute/second and sub-second units, including zero values.

This leaf does not implement `AnalysisJob`, concrete table/partition jobs,
session validation, hooks, SQL execution, skip-policy orchestration, or the
full concrete `String` methods. Focused source-backed tests are in
`crates/tidb-stats/tests/auto_analyze_job_source.rs`, with ownership recorded
in `difftests/corpus/coverage/evidence/source/stats-auto-analyze-job-wave.tsv`
and `evidence/tests/stats-auto-analyze-job-wave.tsv`.

## Non-partitioned analyze SQL metadata

`tidb-stats::non_partitioned_analysis` ports the bounded SQL/type boundary
from
`pkg/statistics/handle/autoanalyze/priorityqueue/non_partitioned_table_analysis_job.go`:
the exact `%n` table/index statements, ordered caller-owned parameters, and
the source index-presence decision between `analyzeTable` and `analyzeIndex`.

This leaf does not resolve schema/table metadata, validate failed analyses,
run `AutoAnalyze`, invoke hooks, update statistics, persist analyze jobs, or
construct the concrete job JSON/stringer. Focused source-backed tests are in
`crates/tidb-stats/tests/non_partitioned_analysis_source.rs`, with ownership
recorded in
`difftests/corpus/coverage/evidence/source/stats-non-partitioned-analysis-wave.tsv`
and `evidence/tests/stats-non-partitioned-analysis-wave.tsv`.

## Static-partitioned analyze SQL metadata

`tidb-stats::static_partitioned_analysis` ports the bounded SQL/type boundary
from
`pkg/statistics/handle/autoanalyze/priorityqueue/static_partitioned_table_analysis_job.go`:
the exact `%n` table/partition/index statements, ordered caller-owned
parameters, physical partition queue key, and source index-presence decision.

This leaf does not resolve partition or schema metadata, validate failed
analyses, run `AutoAnalyze`, invoke hooks, update statistics, persist analyze
jobs, or construct concrete job JSON/stringers. Focused source-backed tests
are in `crates/tidb-stats/tests/static_partitioned_analysis_source.rs`, with
ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-static-partitioned-analysis-wave.tsv`
and `evidence/tests/stats-static-partitioned-analysis-wave.tsv`.

## Auto-analyze queue initialization gate

`tidb-stats::queue_gate` ports the shared lifecycle guard from
`pkg/statistics/handle/autoanalyze/priorityqueue/queue.go`: the exact
`priority queue not initialized` error, operation gating, zero/empty defaults
for `IsEmptyForTest` and `Len`, and the empty running-job snapshot before
initialization. It accepts only caller-owned initialized state and values.

This leaf does not implement the heap, queue worker, context cancellation,
session/domain ownership, DDL handling, job hooks, or lifecycle orchestration.
Focused source-backed tests are in
`crates/tidb-stats/tests/queue_gate_source.rs`, with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-queue-gate-wave.tsv` and
`evidence/tests/stats-queue-gate-wave.tsv`.

## Auto-analyze DDL readiness gate

`tidb-stats::ddl_queue_gate` ports the pre-dispatch decision from
`pkg/statistics/handle/autoanalyze/priorityqueue/queue_ddl_handler.go`:
initialized queues dispatch events, uninitialized queues retry when
auto-analyze is enabled, and uninitialized disabled queues ignore events.

This leaf does not decode notifier event types, execute session/domain DDL
mutations, recreate jobs, invoke queue handlers, or own lifecycle locks.
Focused source-backed tests are in
`crates/tidb-stats/tests/ddl_queue_gate_source.rs`, with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-ddl-queue-gate-wave.tsv` and
`evidence/tests/stats-ddl-queue-gate-wave.tsv`.

## Auto-analyze refresher rebuild state

`tidb-stats::should_rebuild_queue` ports the scalar transition from
`pkg/statistics/handle/autoanalyze/refresher/refresher.go`: an initialized
queue rebuilds when the parsed auto-analyze ratio or partition-prune mode
changes, while an uninitialized queue takes the initialization path instead.
The direct comparisons preserve Go's `!=` behavior, including NaN values.

This leaf does not parse session parameters, initialize/rebuild a priority
queue, evaluate time windows, run workers, access statistics handles, or
submit jobs. Focused source-backed tests are in
`crates/tidb-stats/tests/refresher_state_source.rs`, with ownership recorded
in `difftests/corpus/coverage/evidence/source/stats-refresher-state-wave.tsv`
and `evidence/tests/stats-refresher-state-wave.tsv`.

## Auto-analyze worker capacity

`tidb-stats::worker_capacity` ports the scalar worker boundary from
`pkg/statistics/handle/autoanalyze/refresher/worker.go`: a job is admitted
when running jobs are below `maxConcurrency`, and an unchanged concurrency
setting does not mutate worker state. Non-positive limits reject admission.

This leaf does not own mutexes, goroutines, job execution, hooks, panic
recovery, wait groups, or statistics handles. Focused source-backed tests are
in `crates/tidb-stats/tests/worker_capacity_source.rs`, with ownership
recorded in
`difftests/corpus/coverage/evidence/source/stats-worker-capacity-wave.tsv` and
`evidence/tests/stats-worker-capacity-wave.tsv`.

## Statistics-cache key metadata

`tidb-stats::StatsKeySet` ports the bounded key-set state from
`pkg/statistics/handle/cache/internal/lfu/key_set.go`: thread-safe key/cost
replacement, removal cost return, lookup, key enumeration, length, and clear.
The API accepts caller-derived tracking costs instead of importing
`statistics.Table` memory accounting.

This leaf does not implement LFU admission/eviction, 256-way sharding,
Ristretto async visibility, cache metrics, or statistics-table lifecycle.
Focused source-backed tests are in
`crates/tidb-stats/tests/stats_key_set_source.rs`, with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-key-set-wave.tsv` and
`evidence/tests/stats-key-set-wave.tsv`.

## Statistics-cache key-set sharding

`tidb-stats::StatsKeySetShards` ports the fixed 256-shard boundary from
`pkg/statistics/handle/cache/internal/lfu/key_set_shard.go`: key routing,
cross-shard lookup/update/removal, aggregate keys and length, and full clear.
It accepts caller-derived tracking costs and uses Euclidean routing for
negative diagnostic keys while normal table IDs retain the source modulo
shard.

This leaf does not implement LFU/Ristretto admission or eviction,
statistics-table memory accounting, async visibility, metrics, or cache
lifecycle. Focused source-backed tests are in
`crates/tidb-stats/tests/stats_key_set_shards_source.rs`, with ownership
recorded in
`difftests/corpus/coverage/evidence/source/stats-key-set-shards-wave.tsv` and
`evidence/tests/stats-key-set-shards-wave.tsv`.

## LFU memory-cost transitions

`tidb-stats::memory_cost` ports the bounded cost transitions from
`pkg/statistics/handle/cache/internal/lfu/lfu_cache.go`: nonzero capacities
remain unchanged, zero capacity derives 20% of caller-supplied system memory,
the `NewLFU` test-mode override remains 5 MB, and signed cost deltas preserve
Go atomic-int64 wraparound. The host-memory probe and error remain caller
owned so this leaf stays deterministic.

This leaf does not construct Ristretto, perform LFU admission or eviction,
measure `statistics.Table`, update metrics, or own cache lifecycle. Focused
source-backed tests are in
`crates/tidb-stats/tests/memory_cost_source.rs`, with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-memory-cost-wave.tsv` and
`evidence/tests/stats-memory-cost-wave.tsv`.

## Statistics-cache batch updates

`tidb-stats::BatchUpdate` ports the bounded `cacheOfBatchUpdate` state from
`pkg/statistics/handle/cache/statscache.go`: update and deletion queues flush
before appending at the configured batch size, one full queue flushes both
lists through the callback, explicit flush ignores empty batches, and list
capacity is retained after delivery. The value type and flush operation remain
caller-owned so this leaf does not import `statistics.Table`.

This leaf does not load SQL rows, publish `StatsCacheImpl`, update metrics,
manage atomic cache versions, or own statistics-table lifecycle. Focused
source-backed tests are in
`crates/tidb-stats/tests/batch_update_source.rs`, with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-batch-update-wave.tsv` and
`evidence/tests/stats-batch-update-wave.tsv`.

## Map-backed statistics cache

`tidb-stats::MapCache` ports the bounded map-cache owner from
`pkg/statistics/handle/cache/internal/mapcache/map_cache.go`: caller-supplied
costs are replaced atomically with values, aggregate cost tracks insertion,
replacement, and deletion, keys/values retain unspecified map order, copy
preserves entries and cost, and the capacity/close/eviction/async hooks remain
no-ops. Supplying values and costs at the boundary keeps `statistics.Table`
memory measurement with its future owner.

This leaf does not implement `StatsCacheInner`, concurrent access, SQL/session
loading, metrics, or outer cache publication. Focused source-backed tests are
in `crates/tidb-stats/tests/map_cache_source.rs`, with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-map-cache-wave.tsv` and
`evidence/tests/stats-map-cache-wave.tsv`.

## Statistics-health bucket metadata

`tidb-stats::healthy_metrics` ports the scalar healthy-bucket catalog from
`pkg/statistics/handle/metrics/metrics.go`: stable bucket indexes, exact
upper-bound values, source ordering, and Prometheus label strings for total,
unneeded-analysis, and pseudo categories. The catalog is caller-independent
metadata; metric registration and healthy-state classification remain outside
this leaf.

Focused source-backed tests are in
`crates/tidb-stats/tests/healthy_metrics_source.rs`, with ownership recorded
in `difftests/corpus/coverage/evidence/source/stats-healthy-metrics-wave.tsv`
and `evidence/tests/stats-healthy-metrics-wave.tsv`.

## Statistics JSON ordering metadata

`tidb-stats::json_metadata` ports the deterministic metadata boundary from
`pkg/statistics/util/json_objects.go`: the `global` statistics marker and
`JSONTable.Sort` ordering of predicate-column IDs. The Rust table is
intentionally partial and caller-owned so tipb histogram/sketch payloads,
JSON encoding, storage block conversion, and statistics-handle lifecycle stay
with their future owners.

Focused source-backed tests are in
`crates/tidb-stats/tests/json_metadata_source.rs`, with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-json-metadata-wave.tsv` and
`evidence/tests/stats-json-metadata-wave.tsv`.

## Statistics-table lock filtering

`tidb-stats::get_locked_tables` ports the deterministic set filter from
`pkg/statistics/handle/lockstats/query_lock.go`, retaining only requested IDs
present in the locked set, collapsing duplicates, and preserving the exact
source query marker. SQL row loading and lock/unlock mutations remain outside
this leaf.

Focused source-backed tests are in
`crates/tidb-stats/tests/locked_tables_source.rs`, with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-locked-tables-wave.tsv` and
`evidence/tests/stats-locked-tables-wave.tsv`.

## Stable lock-status diagnostics

`tidb-stats::{generate_stable_skipped_tables_message,
generate_stable_skipped_partitions_message}` ports the deterministic formatting
helpers from `pkg/statistics/handle/lockstats/lock_stats.go`: skipped names are
sorted, singular/plural table and partition wording follows the source counts,
and a remainder suffix reports other items that completed successfully.

This leaf formats caller-owned names only. Lock/unlock mutations, SQL/session
state, transaction handling, and the surrounding lockstats orchestration remain
with their future owners. Focused source-backed tests are in
`crates/tidb-stats/tests/lock_messages_source.rs`, with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-lock-messages-wave.tsv` and
`evidence/tests/stats-lock-messages-wave.tsv`.

## Encoded TopN Datum cache

`tidb-stats::DatumMapCache` ports the byte-keyed cache boundary from
`pkg/statistics/cmsketch_util.go`: immutable encoded keys, cached Datum lookup,
replacement on repeated keys, and owned values. The Rust API accepts an
already-decoded `tidb-datatype::Datum`; schema-aware `topNMetaToDatum` codec
conversion stays with the future field-type/codec owner.

This leaf does not decode time or floating-point field types, distinguish index
encoding, inspect histograms, or run global-statistics merge workers. Focused
source-backed tests are in
`crates/tidb-stats/tests/datum_map_cache_source.rs`, with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-datum-map-cache-wave.tsv` and
`evidence/tests/stats-datum-map-cache-wave.tsv`.

## Asynchronous histogram-load queue metadata

`tidb-stats::async_load` ports the dependency-closed queue metadata from
`pkg/statistics/asyncload/async_load.go`: source-shaped table-item keys,
128-shard selection, pending-item enumeration, full-load upgrade semantics,
deletion, and queue length. A later partial request never weakens an existing
full-load request, matching the source map's first branch.

This leaf does not own the process-global queue, statistics-handle scheduling,
histogram storage reads, SQL/schema-drop cleanup, or asynchronous worker
lifecycle. Focused source-backed tests are in
`crates/tidb-stats/tests/async_load_source.rs`, with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-async-load-wave.tsv` and
`evidence/tests/stats-async-load-wave.tsv`.

## Analyze-job progress metadata

`tidb-stats::analyze_jobs` ports the source-owned analyze status labels and job
kind values from `pkg/statistics/analyze_jobs.go`, plus the concurrent
processed-row counter. `AnalyzeProgress` preserves the source's atomic delta,
strict `maxDelta` threshold, five-second dump interval, reset, and last-dump
timestamp behavior. The `AnalyzeJob` value keeps the source metadata shape for
future handle owners.

This leaf does not persist `mysql.analyze_jobs` rows, schedule or cancel jobs,
render `SHOW ANALYZE STATUS`, report remaining time, or own executor/statistics
handle lifecycle. Focused source-backed tests are in
`crates/tidb-stats/tests/analyze_jobs_source.rs`, with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-analyze-jobs-wave.tsv` and
`evidence/tests/stats-analyze-jobs-wave.tsv`.

## CMSketch/TopN family

`tidb-stats::cmsketch` ports the byte-level family from
`pkg/statistics/cmsketch.go`: zero-seed Murmur3 x64-128 hashing, the
`(h1 + h2 * row) % width` bucket layout, wrapping counters, the
median/noise/default-value query boundary, sampled TopN construction, the
source-default count-only ordering and optional analyze-v1 stabilization,
zero-limit merge/spill behavior, encoded-byte lookup, and copy/equality/memory
primitives. It also owns the generated tipb layout, accepts packed and unpacked
counter encodings, and can round-trip embedded TopN entries.

The family deliberately does not construct `types.Datum`, own tablecodec error
handling, inspect a statistics handle, load or persist `mysql.stats_*` rows,
build histograms, run analyze/auto-analyze, or attach session/debug context.
Nil-versus-empty protobuf byte identity and decoded Datum formatting also
remain open. File-level source/test evidence is recorded in
`difftests/corpus/coverage/evidence/source/stats-cmsketch-family.tsv` and
`evidence/tests/stats-cmsketch-family.tsv`. `estimate.go` remains a checked
file-level `PARTIAL` owner: GEE is translated, while its FMSketch global-
singleton functions are the next consumer-complete slice. The checked
`evidence/transfers/stats-ndv-gee-wave.tsv` record preserves retirement of the
older duplicate GEE implementation.

Direct Rust tests live in `crates/tidb-stats/tests/cmsketch_source.rs`. The
coding fixtures and focused arithmetic boundaries are source-faithful. The
deterministic heavy-tail tests are supplemental smoke coverage: they do not
replace the original Zipf distributions or their average-error envelopes, and
their evidence remains `PARTIAL` until those tests are translated exactly.

## FMSketch raw-hash geometry

`tidb-stats::fmsketch` ports the dependency-closed core from
`pkg/statistics/fmsketch.go`: a unique `u64` hash set, the mask level transition
(`mask = mask*2 + 1` with source wrapping arithmetic), trailing-zero admission
and filtering, NDV estimation, deep-copy shape, merge ordering, and the
portable memory estimate. The source's `hashDatum`/`hashRow` functions depend
on `types.Datum`, statement context, and tablecodec; this leaf therefore takes
already-owned hashes. `FMSketchToProto`, `FMSketchFromProto`, and binary coding
remain open until the tipb/protobuf and sampler owners are ready. The fixed
`MAX_SKETCH_SIZE` constant is exposed only as metadata for those future owners.

The direct tests are in `crates/tidb-stats/tests/fmsketch_source.rs` and cover
the original threshold, merge, copy, and NDV scenarios, including the source's
zero-threshold duplicate behavior. Evidence is append-only in
`difftests/corpus/coverage/evidence/source/stats-fmsketch-wave.tsv` and
`evidence/tests/stats-fmsketch-wave.tsv`.

## Statistics loading metadata

`tidb-stats::status::StatsLoadedStatus` ports the source-pure metadata value
from `pkg/statistics/histogram.go`: the zero-value uninitialized state, the
`AllLoaded`/`AllEvicted` constructors, value copying, and the integer ordering
used by `IsLoadNeeded`, `IsEssentialStatsLoaded`, `IsAllEvicted`, and
`IsFullLoad`. The eviction field remains an integer deliberately, so a future
source status still follows TiDB's `> AllLoaded` and `>= AllEvicted` behavior.

This leaf does not load or evict histogram/CMSketch/TopN data, inspect a
statistics handle, access storage, or mutate a live Column/Index. The future
handle owner must compose this value with those lifecycle effects. Focused
source-backed tests are in `crates/tidb-stats/tests/status_source.rs`, with
append-only ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-status-wave.tsv` and
`evidence/tests/stats-status-wave.tsv`.

## Analyze table/partition identity

`tidb-stats::AnalyzeTableId` ports the source-pure identity helper from
`pkg/statistics/analyze.go`: `NonPartitionTableID = -1`, selection of the
partition ID versus logical table ID for statistics, partition-table
classification, diagnostic `partition => table` formatting, and equality.
Rust's `equals_optional` keeps the Go pointer method's nil/nil and one-sided
nil results explicit without introducing nullable fields into the identity
itself.

The leaf does not schedule ANALYZE jobs, resolve partition metadata, persist
stats, or own `AnalyzeResult`/`AnalyzeResults` lifecycle. Focused tests are in
`crates/tidb-stats/tests/analyze_table_id_source.rs`, with source/test
ownership in `difftests/corpus/coverage/evidence/source/stats-analyze-table-id-wave.tsv`
and `evidence/tests/stats-analyze-table-id-wave.tsv`.

## Row-count estimate arithmetic

`tidb-stats::RowEstimate` ports the source-pure estimate tuple and operations
from `pkg/statistics/histogram.go`: `DefaultRowEst`, field-wise add/subtract,
scalar add/multiply/divide, the ordering-preserving `Clamp`, and
`CalculateSkewRatioCounts`. The implementation keeps Go's ordered-comparison
clamp behavior for NaN and the source's skew arithmetic without adding a
planner-specific policy.

This leaf does not evaluate ranges, inspect histograms or TopN, access Datum or
session context, or decide cardinality defaults. Focused tests are in
`crates/tidb-stats/tests/row_estimate_source.rs`, with source/test ownership
in `difftests/corpus/coverage/evidence/source/stats-row-estimate-wave.tsv` and
`evidence/tests/stats-row-estimate-wave.tsv`.

## Statistics-version predicates

`tidb-stats::stats_version` ports the source constants `Version0`, `Version1`,
and `Version2` plus the raw predicates `IsAnalyzed` and
`IsColumnAnalyzedOrSynthesized` from `pkg/statistics/histogram.go`. It keeps
the source rule that every non-zero version is analyzed and that Version0
column stats can still be available when NDV or null-count metadata is
positive.

This is metadata only: version persistence, ANALYZE scheduling, table/column/
index attachment, and existence-map updates remain outside the leaf. Focused
tests are in `crates/tidb-stats/tests/stats_version_source.rs`, with source
ownership merged into the existing
`difftests/corpus/coverage/evidence/source/stats-status-wave.tsv` row for
`pkg/statistics/histogram.go` and test ownership in
`evidence/tests/stats-version-wave.tsv`.

The same status value now exposes `status_to_string`, preserving Go's exact
diagnostic labels (`unInitialized`, `allLoaded`, `allEvicted`, and `unknown`)
and checking the uninitialized bit before the eviction-level mapping. This is
formatting metadata only; it does not imply a statistics handle or lifecycle
implementation.

## Statistics defaults

`tidb-stats::constants` ports the two exported defaults from
`pkg/statistics/constants.go`: 100 TopN entries and 256 histogram buckets.
They are value-only constants; the future analyze/builder/configuration owners
decide when to apply them. Focused tests are in
`crates/tidb-stats/tests/constants_source.rs`, with source/test evidence in
`difftests/corpus/coverage/evidence/source/stats-constants-wave.tsv` and
`evidence/tests/stats-constants-wave.tsv`.

## Datum-free scalar geometry

`tidb-stats::scalar_geometry` ports the deterministic helpers from
`pkg/statistics/scalar.go`: interval `calcFraction`, byte
`commonPrefixLength`, and the left-aligned base-256 `convertBytesToScalar`
used for string/bytes histogram scalarization. The Rust leaf starts at raw
`f64` and byte slices, preserving the source's boundary/fallback behavior and
its first-eight-byte big-endian rule.

Datum conversion, decimal/time handling, histogram scalar caches, and planner
range evaluation remain outside this leaf. Focused tests are in
`crates/tidb-stats/tests/scalar_geometry_source.rs`, with source/test ownership
in `difftests/corpus/coverage/evidence/source/stats-scalar-geometry-wave.tsv`
and `evidence/tests/stats-scalar-geometry-wave.tsv`.

## Column/index existence metadata

`tidb-stats::ColAndIdxExistenceMap` ports the source-pure metadata map from
`pkg/statistics/table.go`: separate column/index presence, analyzed flags,
insert/replace, deletion, count/empty queries, deep clone, and equality. The
Rust map keeps the source distinction between an absent ID and a known but
unanalyzed ID.

The leaf does not trigger online loading, resolve schema IDs, update DDL
handlers, or attach to `Table`/`HistColl`/the statistics handle. Focused tests
are in `crates/tidb-stats/tests/existence_map_source.rs`, with source/test
ownership in
`difftests/corpus/coverage/evidence/source/stats-existence-map-wave.tsv` and
`evidence/tests/stats-existence-map-wave.tsv`.

## Column/index memory-usage metadata

`tidb-stats::{ColumnMemUsage,IndexMemUsage}` ports the source value objects
from `pkg/statistics/table.go`: measured total bytes, item IDs, tracked bytes
(histogram + CMSketch + TopN), and component accessors. Column FMSketch bytes
remain part of the measured total but intentionally remain outside tracking,
matching the source cache accounting boundary.

These are already-measured values only; they do not inspect allocations,
perform cache eviction, or update LFU costs. Focused tests are in
`crates/tidb-stats/tests/memory_usage_source.rs`, with source ownership merged
into the existing
`difftests/corpus/coverage/evidence/source/stats-existence-map-wave.tsv` row
for `pkg/statistics/table.go` and test ownership in
`evidence/tests/stats-memory-usage-wave.tsv`.

## Out-of-range overlap geometry

`tidb-stats::{left_overlap_percent,right_overlap_percent}` ports the pure
triangular-density helpers from `pkg/statistics/histogram.go`: clip a scalar
predicate to the left or right histogram triangle, square distances, and
normalize by squared histogram width. The source's non-positive-width and
no-overlap zero boundaries are preserved.

The leaf does not convert Datum values, inspect histograms, build
`RowEstimate`s, or choose planner/cardinality policies. Focused tests are in
`crates/tidb-stats/tests/overlap_geometry_source.rs`, with source/test
ownership merged into the existing
`difftests/corpus/coverage/evidence/source/stats-status-wave.tsv` row for
`pkg/statistics/histogram.go`; the direct Go test anchors are already owned by
`evidence/tests/planner-cardinality-out-of-range-wave.tsv`, whose note records
this additional Rust overlap-geometry artifact.

## Histogram count metrics

`tidb-stats::HistogramCountSummary` ports the source count boundary from
`pkg/statistics/histogram.go`: a non-empty histogram uses its last bucket as
the non-null count, total rows add nulls, absolute realtime-row difference is
reported, and an empty total yields an increase factor of `1.0` instead of a
division by zero.

The value starts after bucket construction and does not own Datum values,
histogram mutation, TopN/CMSketch accounting, or planner policy. Focused tests
are in `crates/tidb-stats/tests/count_metrics_source.rs`, with source/test
ownership in
the existing
`difftests/corpus/coverage/evidence/source/stats-status-wave.tsv` histogram
owner row and
`evidence/tests/stats-count-metrics-wave.tsv`.

## Table analysis eligibility

`tidb-stats::analysis_policy` ports the source-pure table predicates from
`pkg/statistics/table.go`: a positive last-analyze timestamp marks a table as
analyzed, realtime rows must meet `AutoAnalyzeMinCnt`, and pseudo statistics
are never eligible. The mutable Go threshold is passed explicitly so the
future auto-analyze configuration owner remains visible.

The leaf does not schedule jobs, mutate global configuration, inspect schema or
stats handles, or perform ANALYZE. Focused tests are in
`crates/tidb-stats/tests/analysis_policy_source.rs`, with source/test ownership
in the existing table-owner row
`difftests/corpus/coverage/evidence/source/stats-existence-map-wave.tsv` and
`evidence/tests/stats-analysis-policy-wave.tsv`.

## Analyze-version matching

`tidb-stats::analyze_version_matches` ports the pure metadata decision from
`pkg/statistics/table.go`: nil or pseudo table stats match, Version0
unanalyzed stats match, and analyzed stats must equal the requested version.
The source's caller assertion that requested analyze versions are Version2 is
kept outside this value helper.

The leaf does not rewrite stats, schedule ANALYZE, or access a handle. Focused
tests are in `crates/tidb-stats/tests/analyze_version_policy_source.rs`, with
source/test ownership in the existing table-owner row
`difftests/corpus/coverage/evidence/source/stats-existence-map-wave.tsv` and
`evidence/tests/stats-analyze-version-wave.tsv`.

## GEE sampled-NDV estimation

`tidb-stats::estimate_ndv_by_gee` now lives in the CMSketch consumer family. It
ports square-root sample scaling, half-up rounding, the sample-NDV lower bound,
the row-count upper bound, test/internal-check assertions, and defensive
production zero-input behavior. Focused cases live in
`crates/tidb-stats/tests/cmsketch_source.rs`; the top-level Go test anchor and
the still-partial `estimate.go` owner remain in the checked source/test
evidence files for the CMSketch family.

`EstimateGlobalSingletonBySketches`, its FMSketch merge helpers, and broader
statistics-handle integration are not claimed by this family.

## Average rows-per-value arithmetic

`tidb-stats::avg_count_per_not_null_value` ports the source histogram helper
from `pkg/statistics/histogram.go`: scale non-null count and NDV by realtime
growth, clamp scaled NDV to at least one, and return the average rows per
value. Empty histogram totals use the source increase-factor fallback of
`1.0`.

The leaf does not construct histograms, compare Datum bounds, or choose
planner/cardinality policies. Focused tests are in
`crates/tidb-stats/tests/average_count_source.rs`, with source/test ownership
in the existing histogram owner row
`difftests/corpus/coverage/evidence/source/stats-status-wave.tsv` and
the existing `evidence/tests/planner-cardinality-out-of-range-wave.tsv` anchor,
whose note records this additional statistics artifact.

## Histogram order correlation

`tidb-stats::calc_correlation` ports the source `calcCorrelation` helper from
`pkg/statistics/builder.go`: the one-sample perfect-correlation shortcut and
the closed-form Pearson sums for physical-versus-sorted ordinal order. It
takes only the caller-owned sample count and ordinal cross-sum, preserving the
source's undefined zero-sample result.

The leaf does not sort samples, build histograms, discover handle columns, or
persist correlation metadata. Focused source-backed tests are in
`crates/tidb-stats/tests/correlation_source.rs`, with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-correlation-wave.tsv` and
`evidence/tests/stats-correlation-wave.tsv`; the Go handle-level `TestCorrelation`
anchor remains an integration boundary.

## Index-usage sample metadata

`tidb-stats::IndexUsageSample` ports the dependency-closed value boundary from
`pkg/statistics/handle/usage/indexusage/collector.go`: the seven
percentage-access buckets, exact boundary selection, one-hot `NewSample`
construction, source zero-total-row fallback, timestamp capture, and
`updateByKey`-equivalent field aggregation. Unsaturated `u64` addition is
implemented with wrapping arithmetic to preserve Go's unsigned counter
behavior.

The leaf does not own session/global collector maps, worker channels,
persistence, or schema-based garbage collection. Focused source-backed tests
are in `crates/tidb-stats/tests/index_usage_source.rs`, with ownership in
`difftests/corpus/coverage/evidence/source/stats-index-usage-wave.tsv` and
`evidence/tests/stats-index-usage-wave.tsv`; the Go anchors are
`TestGetBucket` and `TestUpdateIndex`.

## Statistics usage collector queues

`tidb-stats::{GlobalCollector,SessionCollector}` ports the bounded queue
boundary from `pkg/statistics/handle/usage/collector/collector.go`: normal
updates use a non-blocking queue, synchronous updates wait on the
high-priority queue, one worker drains high priority before normal values, and
close drains accepted values before joining. The source queue capacity and
five-minute timeout remain explicit constants.

This leaf owns queue synchronization and callback dispatch only. It does not
define index/predicate payload maps, persist usage rows, perform schema garbage
collection, or own session lifecycle. Focused source-backed tests are in
`crates/tidb-stats/tests/usage_collector_source.rs`, with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-usage-collector-wave.tsv` and
`evidence/tests/stats-usage-collector-wave.tsv`.

## Locked-statistics delta extraction

`tidb-stats::{stats_delta_from_rows,StatsDelta}` ports the bounded row-shape
helper from `pkg/statistics/handle/lockstats/unlock_stats.go`: a successful
empty result yields the zero delta, the first row supplies `count` and
`modify_count`, and upstream query errors pass through unchanged. The exact
`SELECT count, modify_count` marker is retained for the future SQL owner.

This leaf does not execute SQL, read session context, stamp versions, inject
failpoints, or update/unlock table metadata. Focused source-backed tests are in
`crates/tidb-stats/tests/stats_delta_source.rs`, with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-delta-wave.tsv` and
`evidence/tests/stats-delta-wave.tsv`.

## Statistics bootstrap SQL generation

`tidb-stats::{gen_init_stats_meta_sql,gen_init_stats_histograms_sql,
HistSqlOptions}` ports the deterministic SQL builders from
`pkg/statistics/handle/bootstrap.go`: exact `HIGH_PRIORITY` projections,
the histogram `ORDER_INDEX` hint, caller-preserved table-ID ordering, and
closed-open paging ranges. Typed options reject the invalid ranges and negative
IDs that the Go constructor asserts against.

This leaf generates SQL text only. It does not execute queries, create a
statistics cache, decode rows, resolve schema metadata, or own bootstrap
phases. Focused source-backed tests are in
`crates/tidb-stats/tests/bootstrap_sql_source.rs`, with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-bootstrap-sql-wave.tsv` and
`evidence/tests/stats-bootstrap-sql-wave.tsv`.

## Special global-index classification

`tidb-stats::{is_special_global_index,IndexColumnInfo}` ports the pure
classification from `pkg/statistics/handle/util/util.go`: only global indexes
can be special, and any virtual-generated or prefix-length column makes them
special. The Rust boundary accepts those already-resolved column facts so the
source's schema/model traversal remains explicit.

This leaf does not construct `TableInfo`/`IndexInfo`, resolve column offsets,
inspect field types, or execute schema/session work. Focused source-backed tests
are in `crates/tidb-stats/tests/special_global_index_source.rs`, with ownership
recorded in
`difftests/corpus/coverage/evidence/source/stats-special-global-index-wave.tsv`
and `evidence/tests/stats-special-global-index-wave.tsv`.

## Histogram-free global TopN aggregation

`tidb-stats::merge_histogram_free_topn` ports the dependency-closed portion of
`MergePartTopN2GlobalTopN` from `pkg/statistics/handle/globalstats/topn.go`:
empty partition TopNs are skipped, equal encoded values are summed with Go's
wrapping `u64` arithmetic, ranking is count-descending with encoded-byte
tie-breaking, and the selected TopN is split from the remaining ranked values.

The leaf is intentionally topN-only. It does not decode Datum values, inspect
or mutate histograms, run SQL-killer checks, coordinate concurrent workers, or
persist global statistics. Focused source-backed tests are in
`crates/tidb-stats/tests/global_topn_source.rs`, with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-global-topn-wave.tsv` and
`evidence/tests/stats-global-topn-wave.tsv`; the Go anchor is
`TestGlobalStatsData3` (with explicit global TopN assertions in its lines
342-347).

## Pending stats-delta table-ID selection

`tidb-stats::collect_pending_delta_ids` ports the deterministic selection
helper from `pkg/statistics/handle/usage/session_stats_collect.go`: an empty
target list selects every pending map key, a requested list is filtered to
pending IDs, duplicate targets are removed, and the result is sorted in
ascending order to stabilize the dump sequence.

The leaf accepts already-materialized unique map keys only. It does not own
`variable.TableDelta` values, session collector sweeping, eligibility/time
gates, lock checks, SQL transactions, or stats persistence. Focused
source-backed tests are in `crates/tidb-stats/tests/pending_delta_ids_source.rs`,
with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-pending-delta-ids-wave.tsv` and
`evidence/tests/stats-pending-delta-ids-wave.tsv`; the Go anchor is
`TestDumpStatsDeltaPersistsInitTime`.

## Synchronous stats-load concurrency policy

`tidb-stats::sync_load_concurrency_for_cpu` ports the pure CPU threshold
policy from `pkg/statistics/handle/syncload/stats_syncload.go`: up to 8 CPUs
uses 5 workers, up to 16 uses 6, up to 32 uses 8, and larger machines use 10.
The already-observed CPU count is passed in explicitly so the policy remains
deterministic at the Rust boundary.

This leaf does not probe runtime CPU count, create queues, launch workers,
apply config limits, load histograms, or interact with sessions/storage.
Focused source-backed tests are in
`crates/tidb-stats/tests/sync_load_concurrency_source.rs`, with ownership
recorded in
`difftests/corpus/coverage/evidence/source/stats-sync-load-concurrency-wave.tsv`
and `evidence/tests/stats-sync-load-concurrency-wave.tsv`; the Go anchor is
`TestConcurrentLoadHist`.

## InfoSchema-V1 partition parent cache

`tidb-stats::PartitionTableIdCache` ports the versioned partition-to-parent
mapping used by `pkg/statistics/handle/util/table_info.go`: it rebuilds the
mapping only when the schema version changes and resolves partition IDs to
their parent table IDs without scanning the full table list on every lookup.
Duplicate definitions follow the source map's last-assignment behavior.

The leaf accepts already-extracted partition-definition pairs and owns only
cache/version metadata. It does not detect InfoSchema V1 versus V2, traverse
schema/table definitions, resolve `TableItem` values, synchronize concurrent
callers, or execute initialization. Focused source-backed tests are in
`crates/tidb-stats/tests/partition_table_id_cache_source.rs`, with ownership
recorded in
`difftests/corpus/coverage/evidence/source/stats-partition-table-id-cache-wave.tsv`
and `evidence/tests/stats-partition-table-id-cache-wave.tsv`; the Go anchor is
`TestTableItemByIDForInitStatsAvoidsV1PartitionScan`.

## Weighted reservoir selection

`tidb-stats::WeightedReservoir` ports the dependency-closed selection boundary
from `pkg/statistics/row_sampler.go`: fill a bounded reservoir, heapify at
capacity, and replace the minimum only when an incoming weight is strictly
larger. The generic payload keeps row representation out of this leaf while
preserving the source's min-heap ordering and tie behavior.

This leaf does not generate random weights, encode or collect Datum rows,
maintain FM sketches/null counts/size accounting, merge distributed
collectors, or serialize protobuf samples. Focused source-backed tests are in
`crates/tidb-stats/tests/weighted_reservoir_source.rs`, with ownership recorded
in `difftests/corpus/coverage/evidence/source/stats-weighted-reservoir-wave.tsv`
and `evidence/tests/stats-weighted-reservoir-wave.tsv`; the Go anchor is
`TestWeightedSampling` (line 61). `TestDistributedWeightedSampling` (line 99)
remains intentionally unclaimed.

## Stats-meta count metadata

`tidb-stats::{stats_meta_query,stats_meta_counts,StatsMetaCounts}` ports the
dependency-closed row contract from
`pkg/statistics/handle/storage/read.go`: normal reads use the exact
`mysql.stats_meta` count/modify-count query, the locking variant appends
`FOR UPDATE`, an empty result returns zero values with `is_null`, and the
first row preserves Go's `uint64`-to-`int64` count conversion.

This leaf does not execute SQL, acquire transactions or row locks, coordinate
concurrent DDL/statistics updates, decode TiDB chunks, or update a statistics
handle. Focused source-backed tests are in
`crates/tidb-stats/tests/stats_meta_source.rs`, with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-meta-wave.tsv` and
`evidence/tests/stats-meta-wave.tsv`; the Go anchor is
`TestExchangePartition` (line 1530).

## Stats read/writer save decisions

`tidb-stats::{historical_stats_meta_record_required,
slow_stats_saving_requires_meta_update}` ports the scalar decisions from
`pkg/statistics/handle/storage/stats_read_writer.go`: historical stats metadata
is recorded only after a successful operation returns a nonzero version, and a
slow-save refresh is required only for a positive lease whose elapsed duration
reaches five lease intervals, unless the source failpoint forces the branch.
The exact source failure text is retained as
`SLOW_STATS_SAVE_ERROR_MESSAGE`.

This leaf does not execute stats-meta SQL, open transactions, invoke session or
cache lifecycles, record historical rows, emit logs, or wire failpoints; all
inputs are caller-owned scalar state. Focused source-backed tests are in
`crates/tidb-stats/tests/stats_read_writer_source.rs`, with ownership recorded
in `difftests/corpus/coverage/evidence/source/stats-read-writer-wave.tsv` and
`evidence/tests/stats-read-writer-wave.tsv`; the Go anchors are
`TestUpdateStatsMetaVersionForGC`, `TestSlowStatsSaving`,
`TestSlowStatsSavingForPartitionedTable`, and
`TestFailedToHandleSlowStatsSaving`.

## Statistics-meta update SQL assembly

`tidb-stats::{stats_meta_update_sql,DeltaUpdate,StatsMetaVersionUpdate}` ports
the dependency-closed assembly boundary from
`pkg/statistics/handle/storage/update.go`: locked updates are separated from
unlocked positive and negative deltas, table IDs and value tuples retain
caller order, exact `SELECT ... FOR UPDATE` and `INSERT ... ON DUPLICATE KEY`
statements are preserved, and unlocked IDs are returned for cache
invalidation. Signed `int64` negation follows Go's wrapping behavior,
including `MinInt64`. The stats-meta version-refresh query and argument order
are retained as a separate descriptor.

This leaf does not execute SQL, open transactions, acquire timestamps, convert
`variable.TableDelta`, rename global-statistics IDs, or manage storage/cache
lifecycle. Focused source-backed tests are in
`crates/tidb-stats/tests/stats_meta_update_source.rs`, with ownership recorded
in `difftests/corpus/coverage/evidence/source/stats-meta-update-wave.tsv` and
`evidence/tests/stats-meta-update-wave.tsv`; the Go anchors are
`TestLastStatsHistUpdateVersionAfterLoadStats` and `TestPersistStats`.

## Sample byte-size boundaries

`tidb-stats::{sample_value_is_usable,calc_total_size}` ports the scalar sample
byte decisions from `pkg/statistics/sample.go`: values at or below half
`mysql.MaxFieldVarCharLength` (32,767 bytes) survive the protobuf-to-collector
length gate, and `CalcTotalSize` sums encoded sample lengths into an `int64`.
The Rust conversion uses wrapping arithmetic to match Go's signed conversion
and addition behavior at synthetic overflow boundaries.

This leaf does not own Datum or tipb conversion, collector mutation, FM/CM
sketches, row sampling, or histogram/TopN construction. Focused source-backed
tests are in `crates/tidb-stats/tests/sample_bytes_source.rs`, with ownership
recorded in `difftests/corpus/coverage/evidence/source/stats-sample-bytes-wave.tsv`
and `evidence/tests/stats-sample-bytes-wave.tsv`; the Go anchors are
`TestSampleSerial` and
`TestSampleSerial/SubTestCollectorProtoConversion`.

## Index byte-query precedence

`tidb-stats::query_index_bytes` ports the dependency-closed result selection
from `pkg/statistics/index.go`: a matching TopN count is returned first, then a
matching CMSketch count, and finally the caller's histogram equal-row count.
The helper accepts already-resolved counts so the source lookup order remains
explicit without importing Datum encoding or statistics structures.

This leaf does not hash bytes, encode Datums/tablecodec keys, query TopN or
CMSketch, evaluate histogram ranges, or own index/table lifecycle. Focused
source-backed tests are in `crates/tidb-stats/tests/index_query_source.rs`,
with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-index-query-wave.tsv` and
`evidence/tests/stats-index-query-wave.tsv`; the Go anchor is
`TestIndexQueryBytes` (line 537).

## Historical statistics version selection

`tidb-stats::historical_stats_version` ports the scalar version choice from
`pkg/statistics/handle/history/history_stats.go`: a non-partitioned JSON shape
uses its table version, while a non-empty partition list ignores that value
and selects the maximum partition version from a zero seed.

This leaf does not decode JSON, split storage blocks, generate timestamps,
execute SQL, or own historical-statistics/session lifecycle. Focused
source-backed tests are in
`crates/tidb-stats/tests/historical_stats_source.rs`, with ownership recorded
in `difftests/corpus/coverage/evidence/source/stats-historical-version-wave.tsv`
and `evidence/tests/stats-historical-version-wave.tsv`; the Go anchor is
`TestRecordHistoricalStatsToStorage` (line 695).

## Initialization concurrency policy

`tidb-stats::init_stats_concurrency` ports `GetConcurrency` from
`pkg/statistics/handle/initstats/load_stats.go`: normal mode uses half the
observed CPUs, force mode uses two fewer CPUs, and the result is clamped to a
minimum of 2 and maximum of 16. The caller supplies the CPU count and force
flag so runtime/configuration state stays outside the leaf.

This leaf does not probe `GOMAXPROCS`, read global configuration, create worker
goroutines, inspect cache memory, or load statistics. Focused source-backed
tests are in `crates/tidb-stats/tests/init_stats_concurrency_source.rs`, with
ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-init-concurrency-wave.tsv` and
`evidence/tests/stats-init-concurrency-wave.tsv`; the Go anchor is
`TestConcurrentlyInitStatsWithMemoryLimit` (line 218).

## Predicate-column usage queries

`tidb-stats::{LOAD_COLUMN_STATS_USAGE_QUERY,
LOAD_COLUMN_STATS_USAGE_FOR_TABLE_QUERY,GET_PREDICATE_COLUMNS_QUERY,
CLEANUP_DROPPED_COLUMN_STATS_USAGE_QUERY,cleanup_column_ids_argument}` ports
the exact query strings and source-ordered column-ID argument formatting from
`pkg/statistics/handle/usage/predicatecolumn/predicate_column.go`. The `%?`
markers remain intact so the future session owner controls parameter binding.

This leaf does not access InfoSchema, decode rows or timestamps, execute SQL,
collect predicate usage, or schedule ANALYZE. Focused source-backed tests are
in `crates/tidb-stats/tests/predicate_column_queries_source.rs`, with
ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-predicate-column-queries-wave.tsv`
and `evidence/tests/stats-predicate-column-queries-wave.tsv`; the Go anchors
are `TestCleanupPredicateColumns` (line 26) and
`TestAnalyzeTableWithPredicateColumns` (line 57).

## DDL statistics delta SQL branches

`tidb-stats::{ddl_stats_delta_update,DdlStatsDeltaUpdate}` ports the
dependency-closed SQL boundary from
`pkg/statistics/handle/ddl/ddl.go:95-171`: locked statistics use the
`stats_table_locked` upsert, an unlocked missing row uses the `stats_meta`
insert with `GREATEST(0, ...)`, and an unlocked existing row adds the deltas
before the clamped update. Query text and argument order remain exact, and
Go's signed additions retain wrapping behavior at overflow boundaries.

This leaf does not discover locked tables, read `stats_meta ... FOR UPDATE`,
execute SQL, manage DDL events, or update the statistics handle. Focused
source-backed tests are in
`crates/tidb-stats/tests/ddl_stats_delta_source.rs`, with ownership recorded
in `difftests/corpus/coverage/evidence/source/stats-ddl-delta-wave.tsv` and
`evidence/tests/stats-ddl-delta-wave.tsv`; the Go anchors are
`TestExchangeAPartition` (line 1106) and
`TestExchangeAPartitionAndDropTableImmediately` (line 1256).

## Statistics GC batch count

`tidb-stats::gc_batch_count` ports the pure `forCount(total, batch)` helper
from `pkg/statistics/handle/storage/gc.go:183-189`: integer division truncates
toward zero, and a positive remainder adds one batch. The Rust leaf preserves
Go's signed arithmetic at synthetic overflow boundaries while keeping storage
and session lifecycle outside the API.

This leaf does not scan or delete statistics rows, execute GC SQL, inspect
InfoSchema, manage GC timestamps, or coordinate DDL cleanup. Focused
source-backed tests are in
`crates/tidb-stats/tests/gc_batch_count_source.rs`, with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-gc-batch-count-wave.tsv` and
`evidence/tests/stats-gc-batch-count-wave.tsv`; the Go anchors are
`TestGCStats` (line 30) and `TestGCPartition` (line 63).

## Atomic statistics lease

`tidb-stats::StatsLease` ports the bounded `LeaseGetter` state from
`pkg/statistics/handle/util/lease_getter.go:23-52`: a signed nanosecond
duration is initialized, loaded, and replaced through an atomic value. The
caller supplies the duration scalar, preserving negative and boundary values
without pulling in TiDB's time/configuration dependencies.

This leaf does not choose the lease configuration, schedule refreshes, wire a
statistics handle, convert time units, or control lazy loading. Focused
source-backed tests are in
`crates/tidb-stats/tests/stats_lease_source.rs`, with ownership recorded in
`difftests/corpus/coverage/evidence/source/stats-lease-wave.tsv` and
`evidence/tests/stats-lease-wave.tsv`; the Go anchors are
`TestShowHistogramsLoadStatus` (line 220) and
`TestColumnStatsLazyLoad` (line 266).

## Global statistics zero layout

`tidb-stats::{new_global_stats_layout,GlobalStatsLayout}` ports the bounded
`newGlobalStats` initializer from
`pkg/statistics/handle/globalstats/global_stats.go:72-94`: the requested
histogram count determines four equal-length nil statistics-slot arrays,
aggregate counts start at zero, and missing-partition metadata starts nil.
The leaf represents payload pointers as typed nil slots while leaving actual
histogram/sketch values to the merge owner.

This leaf does not merge partition statistics, resolve schema metadata,
execute SQL, schedule workers, or persist global results. Focused
source-backed tests are in
`crates/tidb-stats/tests/global_stats_layout_source.rs`, with ownership
recorded in
`difftests/corpus/coverage/evidence/source/stats-global-layout-wave.tsv` and
`evidence/tests/stats-global-layout-wave.tsv`; the Go anchor is
`TestBuildGlobalLevelStats` (line 137).

## Table-ID filter formatting

`tidb-stats::build_in_table_ids_string` ports the deterministic SQL text
helper from `pkg/statistics/handle/cache/stats_table_row_cache.go:134-144`:
caller-ordered signed IDs are rendered as decimal values without spaces inside
`table_id in (...)`, and an empty input retains the source's `table_id in ()`
result.

This leaf does not own the row-count cache, stats-meta/histogram reads, SQL
execution, InfoSchema table/partition projection, or data/index length
calculation. Focused source-backed tests are in
`crates/tidb-stats/tests/table_id_filter_source.rs`, with ownership recorded
in `difftests/corpus/coverage/evidence/source/stats-table-id-filter-wave.tsv`
and `evidence/tests/stats-table-id-filter-wave.tsv`; the Go anchors are
`TestDataForTableStatsField` (line 171) and
`TestPartitionsTable` (line 224).

## Auto-analyze process-ID set

`tidb-stats::AutoAnalyzeProcessSet` ports the bounded
`globalAutoAnalyzeProcessList` state from
`pkg/statistics/handle/util/auto_analyze_proc_id_generator.go:58-105`: a
read/write lock protects an ID set, tracker/untracker are idempotent, `all`
returns a snapshot, and `contains` performs membership lookup. The Rust
caller owns singleton placement and process callbacks.

This leaf does not generate process IDs, call `sysproctrack`, schedule or kill
ANALYZE jobs, own the global singleton, or execute SQL. Focused source-backed
tests are in
`crates/tidb-stats/tests/auto_analyze_process_set_source.rs`, with ownership
recorded in
`difftests/corpus/coverage/evidence/source/stats-auto-analyze-process-set-wave.tsv`
and `evidence/tests/stats-auto-analyze-process-set-wave.tsv`; the Go anchors
are `TestExecAutoAnalyzes` (line 35) and `TestKillInWindows` (line 154).

## Batch stats-meta save SQL

`tidb-stats::{stats_meta_save_sql,StatsMetaSaveUpdate}` ports the bounded
`SaveMetaToStorage` SQL assembly from
`pkg/statistics/handle/storage/save.go:376-403`: caller-ordered metadata
tuples use the exact `stats_meta` INSERT/upsert text, with the optional
`last_stats_histograms_version` column and version tuple selected by
`refreshLastHistVer`. Empty-input spacing is preserved byte-for-byte.

This leaf does not acquire start timestamps, execute SQL, manage transactions
or sessions, produce `MetaUpdate` values, or assert persisted rows. Focused
source-backed tests are in
`crates/tidb-stats/tests/stats_meta_save_sql_source.rs`, with ownership
recorded in
`difftests/corpus/coverage/evidence/source/stats-meta-save-sql-wave.tsv` and
`evidence/tests/stats-meta-save-sql-wave.tsv`; the Go anchor is
`TestSaveMetaToStorage` (line 442).

## Init-stats progress arithmetic

`tidb-stats::init_stats_progress` ports the scalar update inside
`RangeWorker.loadStats` from
`pkg/statistics/handle/initstats/load_stats_page.go:104-107`: task counts are
coerced to `float64`, divided, multiplied by the total percentage step, and
offset by the starting percentage. Zero-denominator IEEE behavior is kept
without an integer guard.

This leaf does not own the worker goroutines, task channels, progress atomic,
logger, configuration, or stats-loading execution. Focused source-backed
tests are in
`crates/tidb-stats/tests/init_stats_progress_source.rs`, with ownership
recorded in
`difftests/corpus/coverage/evidence/source/stats-init-progress-wave.tsv` and
`evidence/tests/stats-init-progress-wave.tsv`; the Go anchor is
`TestConcurrentlyInitStatsWithoutMemoryLimit` (line 231).

## Global-statistics SQL index conversion

`tidb-stats::to_sql_index` ports the bounded `toSQLIndex` helper from
`pkg/statistics/handle/globalstats/global_stats_async.go:50-57`: the boolean
index dimension becomes the SQL `is_index` integer `0` or `1`.

This leaf does not own async merge workers, storage reads, SQL execution,
schema setup, or global-statistics lifecycle. Focused source-backed tests are
in `crates/tidb-stats/tests/global_stats_sql_index_source.rs`, with ownership
recorded in
`difftests/corpus/coverage/evidence/source/stats-global-sql-index-wave.tsv`
and `evidence/tests/stats-global-sql-index-wave.tsv`; the Go anchor is
`TestGlobalStatsData` (line 260).

## DDL physical statistics IDs

`tidb-stats::physical_ids_for_stats_ddl` ports the bounded `getPhysicalIDs`
selection from `pkg/statistics/handle/ddl/subscriber.go:356-375`. Non-
partitioned tables return their table ID, partition definitions retain caller
order, and dynamic partition pruning appends the global table ID. `None` and
an empty partition-definition list remain distinct, matching the Go
`TableInfo.GetPartitionInfo` distinction.

This leaf does not decode table metadata, read session variables, dispatch DDL
events, or update stats storage/history. Focused source-backed tests are in
`crates/tidb-stats/tests/ddl_physical_ids_source.rs`, with ownership recorded
in `difftests/corpus/coverage/evidence/source/stats-ddl-physical-ids-wave.tsv`
and `evidence/tests/stats-ddl-physical-ids-wave.tsv`; the Go anchor is
`TestTruncateAPartitionedTable` (line 203).

## Statistics-cache version monotonicity

`tidb-stats::max_stats_cache_version` ports the bounded version advancement in
`pkg/statistics/handle/cache/statscacheinner.go:97-118,147-188`. Normal cache
updates retain the greatest observed table-statistics version, while
`skip_move_forward` preserves the current value even when newer versions are
provided. Empty updates and smaller versions cannot move the value backward.

This leaf does not own atomic publication, cache backends, SQL loading,
metrics, or Handle/session lifecycle. Focused source-backed tests are in
`crates/tidb-stats/tests/stats_cache_version_source.rs`, with ownership
recorded in
`difftests/corpus/coverage/evidence/source/stats-cache-version-wave.tsv` and
`evidence/tests/stats-cache-version-wave.tsv`; the Go anchor is `TestVersion`
(line 111).
