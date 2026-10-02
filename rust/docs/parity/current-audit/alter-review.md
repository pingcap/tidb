# Recheck of all unresolved structural findings

Reviewed 2026-10-02 from integration `b52dbfef7f9a0076500132bc63b276617440aa89`,
Go master `93a01d31f6da205ae4bf376825293903a6899fdb`, native client-rust and
TiDB dependency `19a56ccda1e128218cd33c69709038219aced9bc`. Both implementation
branches were pulled and current. Client-go remains
`v2.0.8-0.20260928031501-8edb23f6c7ee`. No dependency update was needed.

**All 71 previously unresolved IDs still have an unmet contract.** The local
ALTER repair changes D11 from open to partial, leaving **63 open, eight partial,
15 repaired, 86 tracked**. Of the 71, 69 concern live behavior or missing
production integration; D09/D10 are disabled seeds. This does not mean 71 freshly
reproduced production failures or 71 unaccepted packages.

## Review evidence and limits

[Per-ID evidence](alter-review-recheck/source-continuity.json) retains every
current ID, source references and exact baseline blob/tree identities. Compared
with the previous full review at cfc6a174bb, 54 unresolved IDs retain unchanged
prior references and 17 have changed references. Those comparisons carry prior
evidence; they are not new runtime proofs. The receipt lists all 44 intervening
TiDB crate paths and ten native-client paths. Relevant changed owners/callers
were reviewed for the remaining contract, including node configuration and native
PD callers whose old cited anchors themselves did not change.

The intervening changes repair health feedback, PD request/close ownership,
projection retirement, server command/recovery lifetime, DDL typed error
producers and generated-column cast/error ownership. None supplies the remaining
DDL scheduling, routing consolidation, SQL planner, Domain, MPP or other missing
runtime owners. Fresh global caller searches confirmed the uncomposed resource
controller, RU writer, statistics GC, minimum-start-TS and statement-summary
paths. Other unchanged source comparisons and Go test receipts are carried
forward, not rerun or promoted to complete-package acceptance.

Six retained SQL/wire probes were rerun. A focused allocator probe and the
controlled LFU regression add current evidence; **13 unresolved IDs have a fresh
symptom/refusal, 56 have source/caller evidence, and two are disabled seeds**.
Probe exit zero only means the diagnostic completed. No live distributed failure
or performance change is inferred from these fixtures.

Stale allegations excluded from this review:

- K03's strict generated TINYINT overflow now rejects and stores no row.
- N03's token-limit TOML refusal is repaired; other configuration gaps remain.
- P06's missing joined public close and P07's held network lock are repaired.
  P06 remains for actual discovery/TSO option consumers, not for absent receipts
  or normal demand-driven reconnection from Idle.
- D05's migrated CHECK/action errors now carry source identity and formatting.
- D11's partial catalog publication is repaired here; shared allocator execution
  still precedes completion of statement preparation.

## ALTER repair and remaining boundary

The old foreign-key-only staging branch and second parse are removed. Every
local ALTER uses a single staged catalog image. Publication occurs only after all
actions succeed, including multiple columns within one AST action. Copy-on-write
storage preserves prior index keys and rows, and catalog staging also covers
referencing-table metadata and version identities. Statement warnings remain in
their existing owner. Shared statistics, plan-cache invalidation and commit-history
services are not mutated by these ALTER action paths. The cluster planner caller
constructs an isolated in-memory partition catalog; this change does not replace
external transaction savepoints or add durable DDL jobs.

Four executor regressions and the session prepared-statement regression fail on
unchanged production code and pass after repair. One existing expression-index
test had encoded the wrong partial-commit result. Another existing test still
refuses drop-column plus rename-of-its-covering-index, where Go succeeds; its
failure now preserves the prior image. These are maintenance regressions and
controls, not acceptance of the complete Go pkg/ddl package.

The [allocator diagnostic](alter-review-recheck/alter-residual.txt) shows failed
`AUTO_INCREMENT=1000000, ADD COLUMN id INT` followed by inserted ID 1000000.
Catalog clones share the nontransactional allocator. Go executor.go collects all
subjobs before DoDDLJob and returns on the duplicate-column admission error;
Rust applies the rebase during traversal. The next repair must migrate complete
ALTER preparation/execution ownership; cloning or rolling back a shared allocator
would incorrectly interfere with concurrent allocations. D11 therefore stays
partial. The Go source order is verified; this new allocator comparison was not
rerun on a live Go server.

## Every remaining contract

| ID | Status | Evidence | Current remaining contract |
| --- | --- | --- | --- |
| A01 | partial | Source | UPDATE's resolved privilege path remains repaired; other statements still derive privilege visits outside the shared planner owner. Fresh denial controls do not close those producers. |
| A02 | open | Source | The durable loader still omits mysql.global_priv and policy fields; registry reconstruction and empty-user handling remain unchanged. No mixed-node authentication test was run. |
| A03 | open | Observed | TLS still uses with_no_client_auth and REQUIRE X509 still refuses. Certificate verification, policy propagation and rotation remain incomplete. |
| A04 | open | Observed | Fresh SQL accepts PASSWORD HISTORY 3, reuses the first password and reads NULL policy. Account option handling still ignores the history/reuse fields. |
| B01 | open | Source | CostLruStore.get only reads the map; insertion-order eviction and unconditional fitting admission remain the live binding store. |
| B02 | open | Source | The lease reloader still constructs a full BindingCache from storage rows; no incremental watermark, usage writer or owner GC loop was added. |
| N01 | open | Observed | The wire diagnostic still converts raw Latin-1 E9 to C3A9. UTF-8 String ingress and literal representation are unchanged. |
| N03 | open | Observed | Fresh TOML accepts token-limit, as repaired, but still rejects performance.stats-lease and security.ssl-ca; auto_tls remains true. The second NodeConfig authority and incomplete shared configuration remain. |
| D01 | open | Observed | Only CHECK SQL submits through the durable worker; other DDL uses the direct transaction route. Fresh repartition refusals preserve rows but leave Go-supported reorganization absent. |
| D02 | open | Source | Index entries still backfill in the publication transaction through a default statement context; no complete durable index/reorg lifecycle was added. |
| D03 | open | Source | run_loop still iterates jobs serially and calls run_persisted_ddl_job to completion, skipping unsupported action types. Worker error continuation did not introduce worker pools. |
| D05 | partial | Source | Migrated persisted action and CHECK producers now retain source RFC/class identity and shared formatting through checkpoints and history. Other admission/storage producers still erase identity; complete error taxonomy, rollback-transaction classification, configurable retry timing and metrics remain missing. |
| D07 | open | Source | The common planner still constructs MDL information and the worker always uses the MDL barrier. No source-mode branch for disabled MDL is present. |
| D08 | open | Source | Finalization still lacks the complete typed delete-range registration; direct index/partition paths retain eager key removal. This is distinct from the absent GC worker. |
| D09 | open | Disabled | The in-memory materialized-view build seed remains, but supports_persisted_ddl_job excludes its action. This is an inactive implementation gap, not an admitted production failure. |
| D10 | open | Disabled | The MV/MLog dependency seed still lacks the full dependent-ID/publication/rollback lifecycle and remains excluded from live dispatch. This is an inactive implementation gap. |
| D11 | partial | Observed | Catalog/schema, row/index storage, metadata versions and cross-table FK references now publish only after every local ALTER action succeeds. Shared allocator effects are still eager: failed AUTO_INCREMENT=1000000 followed by duplicate-column changes the next inserted ID to 1000000. Go collects subjobs before execution. Drop-column plus rename-of-its-index also still refuses where Go succeeds; complete action preparation remains missing. |
| C02 | open | Source | Instance-cache variables and metric selectors still have no domain cache consumer. The working session cache is a separate repaired owner. |
| C04 | partial | Observed | Fresh explicit ignored admission regression still fails: a nonresident primary is visible before Stretto admission. The published lifecycle and shard-index repairs remain intact. |
| E02 | partial | Source | Fresh joined UPDATE rejects an orphan as repaired. Physical UPDATE still omits Go's per-table FK plans, so the plan-to-execution contract remains incomplete. |
| E03 | open | Source | execute_dml_source still returns a fully drained Vec of datum rows; multi-DML reconstructs target handles and charges after collection. No streaming handoff was added. |
| E04 | open | Source | The physical Apply builder still creates NestedLoopApplyExec; concurrency metadata and the setting have no parallel execution consumer. |
| E05 | open | Observed | Fresh IMPORT refuses and leaves its target empty. The removed parser/INSERT pipeline stays removed; Go's import controller and durable tasks remain absent. |
| S01 | open | Source | The optional configured server still selects its one/two-table private read/write planner rather than ordinary session/compiler ownership. |
| S02 | open | Source | The two-table startup branch still drops its initial catalog snapshot and installs no shared catalog reloader, unlike the one-table branch. |
| I01 | open | Observed | Fresh SEQUENCES remains empty for a created sequence and CLUSTER_LOG ignores supplied bounds; CLUSTER_CONFIG now refuses. Missing live retrievers remain. |
| I02 | open | Source | cluster_info_table_rows still builds only TiDB server entries. Fake stores and swallowed discovery errors remain removed, but six source retrievers remain absent. |
| I03 | open | Source | CLUSTER_PROCESSLIST still decorates local process rows; no complete peer fanout task/retrieval lifecycle is composed. |
| T01 | open | Source | BufferMutation::insert still couples presume-not-exists to AssertNotExist; the configured INSERT planner has no source transaction-mode policy input. |
| T02 | partial | Source | ClientPd still delegates to TiDB routing while native client-rust retains its own region/cache/RPC owners. Atomic health publication, owned PD requests and joined shutdown are repaired; those lower-owner repairs do not consolidate this bridge. |
| O01 | open | Source | Command admission and connection panic recovery changed sql_node.rs but did not add numeric server-ID leasing; connection allocation still uses standalone identity. |
| O02 | open | Source | Boot still checks already_bootstrapped rather than selecting a versioned upgrade/recovery workflow. A pre-existing bootstrap version skips publication. |
| O03 | open | Source | Server startup still has no store GC worker construction. Native GC methods and metrics are present but not that runtime owner. |
| O04 | open | Source | TTL helpers/metadata remain without a server-composed TTL job/task manager and owned periodic execution. |
| O05 | open | Source | There is still no Rust-crate production caller of set_resource_control_interceptor; domain startup does not compose the PD resource controller and runaway integration. |
| O06 | open | Source | RuStatsWriter is still constructed only in source tests; startup does not run the owner-gated request-unit persistence/retention loop. |
| O07 | open | Source | gc_stats remains a public helper called by tests; existing startup workers do not periodically invoke it. Analyze-job cleanup is a separate responsibility. |
| O08 | open | Source | Plan replayer collectors/dumpers remain helpers and test constructions, without server startup, capture production or file-GC integration. |
| O09 | open | Source | ServerInfoSyncer still explicitly excludes ReportMinStartTS and no live reporter composes session/cursor/internal/schema timestamps. No premature-GC reproduction is claimed. |
| O14 | open | Source | Affinity still reaches metadata and plan carriers without a PD group manager or complete DDL lifecycle; CPU affinity is unrelated. |
| O15 | open | Source | No cross-keyspace runtime/session manager was added; codecs alone do not provide holder/eviction/virtual-server/min-start-ts ownership. This is conditional on source variants supporting it. |
| O16 | open | Source | The live workload-repository sampler remains distinct from workload-learning read-cost analysis, persistence and cache refresh, which remain absent. |
| O17 | open | Source | Telemetry settings/admission remain without a production counter/window collector and periodic log reporter. Go's present contract does not require an external uploader. |
| O18 | open | Observed | With both summary switches enabled, fresh SQL still leaves cumulative statement stats at zero. Production still does not submit StmtExecInfo or compose the v2 sink lifetime. |
| O19 | open | Source | Statistics load metrics still have no production request/wait/dedup/read observation calls; running load workers and initialized collectors do not close this gap. |
| F01 | open | Source | Classic placement rule creation/repair/removal is still performed by poll_once rather than the complete DDL/GC owner. |
| F02 | open | Source | SetTiFlashReplica still rebuilds unavailable metadata and the poller tracks logical table IDs; physical-partition availability/reset policy is incomplete. |
| F03 | open | Source | poll_once still requests and filters gRPC Up stores on each pass without the source progress cache/backoff/HTTP-discovery lifecycle. |
| M01 | open | Source | MPP remains scan-only, selects one Up TiFlash store, builds one task and refuses pushed Selection/TopN/aggregation. |
| M02 | open | Source | table_regions still collapses disjoint ranges into one min/max envelope and uses a 10000-region query without continuation. |
| M03 | open | Source | open_mpp still drains the complete response into VecDeque; closing the returned stream only drops local packets, without a remote cancellation owner. |
| M04 | open | Source | MPP still connects directly to http:// endpoints; stale regions are logged outside the shared routing/security/recovery owner. |
| M05 | open | Source | Disaggregated settings still have no composed compute topology/cache/dispatch consumer; the live scan selects ordinary PD stores. |
| P03 | open | Source | PD member/leader discovery and classic Tso remain the only active discovery path; no service-mode/independent TSO service switch is composed. |
| P06 | partial | Source | Public native close, retained request ownership and interrupted stream retirement are repaired. Native Cluster still owns only the PD leader Tso stream and GetMembers discovery; it has no operational GetClusterInfo service-mode/TSO-service switching or follower-proxy option consumer. Pinned Go servicediscovery and clients/tso compose both. This residual is source-confirmed, not inferred from missing package acceptance or demand-driven Idle reconnect. |
| Q01 | open | Source | Cascades is accepted as a setting, but planner_bridge still uses logical_optimize/find_best_task without memo exploration. No current caller installs a Cascades owner. |
| X01 | open | Source | SQL new_function and PB-selected builtins still construct/evaluate separately; numeric fast paths do not provide the shared typed scalar/vector owner for all types. |
| X02 | open | Source | Provider registration, embedding cache/batcher, caller-isolated cancellation and EMBED_TEXT runtime remain absent. Retained variables and expression mappings do not compose that runtime. |
| C03 | open | Source | CoprCache.get does not update admission/recency; set evicts insertion_order and the read authority retains fixed configuration. |
| K01 | open | Source | ClusterTableAutoIds still selects the cached allocator; step_for maps AUTO_ID_CACHE <= 1 to DEFAULT_AUTO_ID_STEP. The single-point service allocator is uncomposed. |
| K02 | open | Source | CACHE changes metadata after admission checks; reads/writes still lack leased cache wrappers and commit-time renewal. Fresh acceptance is not a stale-read reproduction. |
| K03 | partial | Source | The fresh strict generated-TINYINT probe now rejects overflow and stores no row, so that old symptom is repaired. Shared generated write/read/ANALYZE casts and typed delivery remain repaired. Lower datatype error/value identities, generated-expression argument context and legacy ENUM/SET collation context remain; other ANALYZE build/storage adapters remain generic. |
| I04 | open | Source | Catalog publication remains a latest materialized image without the versioned InfoCache/V2 loading owner. The new computed-default loader changes table construction, not schema-history ownership. |
| S03 | open | Observed | Both SHOW SESSION_STATES and SET SESSION_STATES still refuse; variable helpers do not compose complete session migration. |
| S04 | open | Observed | tidb_read_staleness remains accepted but the next ordinary read refuses. The separate historical catalog path is still local rather than Domain timestamp/schema ownership. |
| E07 | open | Observed | SHOW BR JOB still refuses; no live BRIE queue/executor replaced the constant information-schema providers. Helper protocol repairs do not compose backup/restore. |
| N04 | open | Source | KILL still validates IDs then operates on the local registry; no decoded-server remote dispatch/configuration lifecycle was added. |
| N05 | open | Source | HTTP status still uses the fixed route dispatcher/read-only settings; full Go admin/configuration handlers remain absent. |
| O10 | open | Source | DXF metrics/helpers remain without production manager, executor/storage/resource startup and close. |
| O11 | open | Source | TopSQL startup still initializes metrics only; SQL/plan registration, profiling collection, aggregation and reporting remain uncomposed. |
| O13 | open | Source | closest-adaptive still maps to mixed reads without the domain AZ distribution and request adjustment producer. No new server caller composes the missing policy. |

## Validation and publication

[Machine-readable validation](alter-review-recheck/validation.json) records exact
commands, outcomes and failure names. From `rust/`:

```sh
cargo test --locked -p tidb-executor --lib tests_ddl_multi_schema_change_sql -- --nocapture
cargo test --locked -p tidb-session --lib failed_alter_keeps_rows_and_prepared_schema -- --nocapture
cargo test --locked -p tidb-executor --lib tests_ddl -- --nocapture
cargo test --locked -p tidb-session --lib tests_alter_column -- --nocapture
cargo test --locked -p tidb-exec --test all cluster_ddl_source:: -- --nocapture
cargo test --locked -p tidb-stats-handle-cache-internal-lfu --lib nonresident_primary_waits_for_admission -- --ignored
cargo check --locked -p tidb-executor -p tidb-exec -p tidb-session -p tidb-server --all-targets
```

The first two commands establish red regressions on the unchanged ALTER
implementation; all five regressions pass in the final affected suites.
Executor DDL: 37 passed. Session ALTER: 14 passed. Cluster DDL: 105 passed,
five failed both before and after this change. The baseline control temporarily
restored only alter_table.rs from b52dbfef7f and restored the patch in finally.
Failures concern two missing-notifier fixtures, an existing MODIFY default
admission expectation, and two CHECK lifecycle expectations. They are not new
ALTER regressions. The initial standalone `--test cluster_ddl_source` command
had no declared target and was corrected to the existing aggregated `--test all`.

The explicit ignored LFU test fails as expected for unresolved C04; it is not a
green test gate. All-target checking and root
`GOTOOLCHAIN=go1.25.14 make lint` pass. `git diff --check` passes. No Go/Bazel
source, generated artifacts or dependencies changed, so bazel_prepare is not
required. A live TiKV/Go server, distributed DDL recovery and
sysbench/TPC-C/TPC-H/YCSB performance were not tested. ALTER now incurs catalog
copy-on-write isolation; no DML execution path changed and no measured speedup
is claimed.

Retained diagnostics are replayable from the repository root using the existing
runner without leaving production examples behind:

```python
import importlib.util
from pathlib import Path
p = Path('rust/docs/parity/current-audit/run-expanded-probes.py').resolve()
spec = importlib.util.spec_from_file_location('probes', p)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
for crate, stem in [('tidb-session', 'expanded-ownership'),
                    ('tidb-session', 'subsystem-structure'),
                    ('tidb-session', 'session-ownership'),
                    ('tidb-session', 'remaining-structure'),
                    ('tidb-server', 'expanded-server'),
                    ('tidb-session', 'partition-structure'),
                    ('tidb-session', 'alter-residual')]:
    m.run(crate, stem + '-probe.rs', 'alter_review_' + stem.replace('-', '_'),
          'alter-review-recheck/' + stem + '.txt')
```

The saved session-ownership.txt is pre-repair; session-after.txt is post-repair.
Replaying now yields the post-repair catalog result. The original six commands
used `cargo run --locked -p <crate> --example alter_review_<stem>`; the last
post-repair control used `--example alter_review_session_after`.

Publication uses the actual pre-commit hook's
`cd rust && cargo build --locked -p tidb-server`, then reruns that exact build
after the final commit immediately before pushing to hparser-integration.
The first actual hook build passed in 16.03 seconds (local log
`/private/tmp/alter-review-commit.log`). This receipt amendment repeats that
hook; final publication uses the following command from the root, and verifies
the remote SHA and a clean checkout afterward:

```sh
(cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration
```


## Concurrent publication update

The first push was rejected because c510669484 added SHOW CONFIG, SHOW IMPORT
JOBS and SHOW PLACEMENT. The two changed files were inspected and the repair
rebased without conflicts or force. These branches do not affect ALTER or close
N03, E05, I01 or O10: constant empty SHOW rows do not provide the missing runtime
owners. Their addition is preserved without granting parity acceptance. The
original review baseline/evidence above stays explicit; the machine receipt
records this later commit and file blobs. Session ALTER tests, all-target
checking, lint and the locked hook/pre-push gates are repeated on the integrated
branch before publication.

The repeated session suite passes all 14 tests; all-target checking and lint
also pass on the integrated branch. Removed 16 task-generated diagnostic binary
and dep-info files after retaining their source/output, reclaiming 1,238,720,512
allocated bytes (about 1.15 GiB). Shared build dependencies and user files were
untouched; the local removal manifest is `/private/tmp/alter-review-cleanup.json`.
