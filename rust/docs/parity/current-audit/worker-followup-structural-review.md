# Recheck of the remaining findings after shared worker repairs

Reviewed 2026-10-02 at TiDB integration
`cfc6a174bb3e46312dae48a7b85a53053b2f5ea0`, against freshly fetched Go master
`93a01d31f6da205ae4bf376825293903a6899fdb` and its client-go
`v2.0.8-0.20260928031501-8edb23f6c7ee`. Native client-rust master and TiDB's
dependency both remain `6163ecfc587b248dcbf0e30c1c9d905b4bc5a665`.
The implementation branches are current; no dependency update is needed.

**All 75 currently unresolved IDs still describe an unmet Go contract.** That is
69 open and six partial findings, with no newly closed or disproved entire ID.
This is a count of known parity gaps, not 75 freshly reproduced production bugs
or 75 unaccepted packages. Several old symptoms within those IDs are repaired.

| Current disposition | Count | Evidence limits |
| --- | ---: | --- |
| Live path or missing production runtime/integration | 73 | Includes unsupported Go behavior and source-confirmed ownership contracts, not only wrong accepted results |
| Disabled implementation seeds | 2 | D09/D10 are excluded from persisted-job dispatch; no current production failure is claimed |
| Previously repaired findings, outside the unresolved count | 10 | C01, D04, D06, E01, O12, P01, P02, P04, P05, T03 retain their recorded repair scope |

Among the 73 live/missing-runtime findings, 14 have fresh observed symptoms or
refusals and 59 have source/caller evidence without a fresh runtime reproduction.
No live distributed failure or performance delta is inferred from source alone.

## How the findings were challenged

Every unresolved description was checked against its remaining contract,
production construction/callers, prior receipt and current source. The review
inspected all 18 changed crate paths since the last full review at
`68d6de685a5e58c559a861ec7b85d10bc8a2aa60`, including changes outside a finding's
original anchors. Go/native/dependency inputs are unchanged. Of 124 prior source
reference occurrences attached to unresolved IDs, 22 changed, affecting 16 IDs.
The other 59 IDs' prior references are byte-identical. Byte identity carries
prior evidence; it is not a substitute for checking changed callers.

[Source continuity](worker-followup-recheck/source-continuity.json) contains all
85 IDs, previous/current blobs or trees, extra current anchors, the 18 changed
crate paths, evidence categories and a specific remaining-contract assessment
for each unresolved ID. [The register](structural-findings.md) retains Go owner
and replacement boundaries. Unchanged upstream comparisons and original-test
receipts were carried forward, not rerun or promoted to whole-package acceptance.

## Repaired symptoms excluded from the current allegations

| IDs | What no longer supports an unresolved allegation | What remains |
| --- | --- | --- |
| D04/D06 | Shared pause/cancel/object-validation checkpoint gaps are repaired | These two IDs stay outside the 75 |
| D05 | Shared checkpointing, panic/cancel handling, code/SQLSTATE projection and post-commit retry continuation are repaired; blanket conversion to 1105 is stale | Source error identity/taxonomy, transaction classification, configurable retry timing/metrics and complete actions |
| E01/E02 | Fresh alias merging and joined UPDATE FK controls pass; orphan acceptance is no longer current | E01 stays repaired; E02's planner-to-executor FK contract remains |
| A01 | Fresh ordinary, qualified and unqualified joined UPDATE privilege checks deny unauthorized writes | Other statement visit-info producers still need shared planner ownership |
| D01/E05 | Repartition and IMPORT refuse without modifying rows; their removed shortcuts stay removed | Complete durable DDL and import owners still absent |
| I01/I02 | Fabricated config/store rows and swallowed discovery failures were removed | Live retrieval and complete discovery remain missing |
| C04 | The recent rem_euclid shard-index correction exists; shutdown repairs remain | The controlled admission regression still fails |
| P06 | Published native deadline/connection/batch/retry/options/metrics/error prerequisites remain | Parent discovery/TSO lifecycle and joined public PD close are incomplete |

## Fresh diagnostics and controls

All six existing SQL/wire diagnostics finished with exit zero. That means the
diagnostic completed, **not** that parity passed. No production or test source
was changed; the runner installed temporary examples and removed them afterward.

| Output | Current observation | Unresolved IDs with fresh symptom/refusal evidence |
| --- | --- | --- |
| [Account/import](worker-followup-recheck/expanded-ownership.txt) | Password reuse accepted with NULL stored policy; REQUIRE X509 refused; IMPORT refused with empty target; repaired privilege checks deny writes | A03, A04, E05 |
| [Subsystem](worker-followup-recheck/subsystem-structure.txt) | Strict generated TINYINT stores 127 for 1000 without warnings while ordinary conversion rejects it; session migration, BR jobs and stale reads refuse | K03, S03, E07, S04 |
| [Session](worker-followup-recheck/session-ownership.txt) | Multi-action ALTER leaves its first addition after the second fails; SEQUENCES omits a created sequence; cluster config refuses and log bounds are not applied; repaired alias/FK controls hold | D11, I01 |
| [Statement summary](worker-followup-recheck/remaining-structure.txt) | Both summary switches enabled, but executed SQL leaves statement statistics at zero | O18 |
| [Server/wire](worker-followup-recheck/expanded-server.txt) | Raw Latin-1 E9 becomes C3A9; valid Go configuration keys refuse and auto-TLS defaults true; removed run-auto-analyze is correctly refused | N01, N03 |
| [Partition](worker-followup-recheck/partition-structure.txt) | Both repartition shapes refuse, retain rows and allow subsequent inserts | D01 |
| [Controlled cache test](worker-followup-recheck/lfu-admission.txt) | Nonresident primary is visible before admission; one failed test, exit 101 | C04 |

Acceptance of Cascades, CACHE metadata and token-limit was also observed, but
does not itself reproduce Q01/K02/N02's missing runtime contracts; those stay in
the source-evidence group. Passing repaired controls do not certify an entire
cache, privilege or FK package.

The older unclassified system-table DDL candidate was rerun separately. It still
fails at the `p1 exists` catalog lookup after ADD PARTITION (one failed test,
exit 101); see [output](worker-followup-recheck/system-table-ddl.txt). That failure
does not prove a system-schema notifier violation. Its root cause is not yet
classified and it is not added as an independent structural ID.

## Per-ID remaining contracts

Each of the 75 unresolved IDs appears once below. “Observed” means a fresh
symptom/refusal for part of the finding; “Source” includes caller review and
carried source/oracle evidence; “Disabled” marks non-admitted seeds. Full source
anchors and earlier Go evidence remain in the register and continuity file.

| ID | Status | Evidence | Remaining contract / disposition |
| --- | --- | --- | --- |
| A01 | partial | Source | UPDATE's resolved privilege path remains repaired; other statements still derive privilege visits outside the shared planner owner. Fresh denial controls do not close those producers. |
| A02 | open | Source | The durable loader still omits mysql.global_priv and policy fields; registry reconstruction and empty-user handling remain unchanged. No mixed-node authentication test was run. |
| A03 | open | Observed | TLS still uses with_no_client_auth and REQUIRE X509 still refuses. Certificate verification, policy propagation and rotation remain incomplete. |
| A04 | open | Observed | Fresh SQL accepts PASSWORD HISTORY 3, reuses the first password and reads NULL policy. Account option handling still ignores the history/reuse fields. |
| B01 | open | Source | CostLruStore.get only reads the map; insertion-order eviction and unconditional fitting admission remain the live binding store. |
| B02 | open | Source | The lease reloader still constructs a full BindingCache from storage rows; no incremental watermark, usage writer or owner GC loop was added. |
| N01 | open | Observed | The wire diagnostic still converts raw Latin-1 E9 to C3A9. UTF-8 String ingress and literal representation are unchanged. |
| N02 | open | Source | The token-limit flag remains accepted without a command permit acquire/release path. Connection-count admission is not that missing contract. |
| N03 | open | Observed | Fresh TOML still rejects token-limit, performance.stats-lease and security.ssl-ca; auto_tls remains true. The second NodeConfig authority remains. |
| D01 | open | Observed | Only CHECK SQL submits through the durable worker; other DDL uses the direct transaction route. Fresh repartition refusals preserve rows but leave Go-supported reorganization absent. |
| D02 | open | Source | Index entries still backfill in the publication transaction through a default statement context; no complete durable index/reorg lifecycle was added. |
| D03 | open | Source | run_loop still iterates jobs serially and calls run_persisted_ddl_job to completion, skipping unsupported action types. Worker error continuation did not introduce worker pools. |
| D05 | partial | Source | Checkpointing, panic/cancel handling, SQLSTATE/code projection and post-commit retry waiting are repaired. Admission errors still hold only reason/code; compatible job errors lack source RFC identity. Full transaction/error/metrics policy remains open. |
| D07 | open | Source | The common planner still constructs MDL information and the worker always uses the MDL barrier. No source-mode branch for disabled MDL is present. |
| D08 | open | Source | Finalization still lacks the complete typed delete-range registration; direct index/partition paths retain eager key removal. This is distinct from the absent GC worker. |
| D09 | open | Disabled | The in-memory materialized-view build seed remains, but supports_persisted_ddl_job excludes its action. This is an inactive implementation gap, not an admitted production failure. |
| D10 | open | Disabled | The MV/MLog dependency seed still lacks the full dependent-ID/publication/rollback lifecycle and remains excluded from live dispatch. This is an inactive implementation gap. |
| D11 | open | Observed | The only alter_table.rs change adds a COALESCE warning. Fresh multi-action ADD COLUMN still publishes the first column before the second raises duplicate-column. |
| C02 | open | Source | Instance-cache variables and metric selectors still have no domain cache consumer. The working session cache is a separate repaired owner. |
| C04 | partial | Observed | The changed rem_euclid shard calculation does not change Stretto admission. The controlled ignored regression still fails because a nonresident primary is visible before admission. |
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
| T02 | partial | Source | ClientPd still delegates to TiDB routing while native client-rust retains its own region/cache/RPC owners. Repaired backoff, replica selection and shared health types remain repaired. |
| T04 | open | Source | needs_active_feedback still returns false on a contended feedback mutex; pinned client-go reads atomic metadata and requests feedback before taking the update mutex. |
| O01 | open | Source | The sql_node.rs changes remove DDL error mapping only. Numeric server-ID leasing is absent and the connection allocator still uses standalone identity. |
| O02 | open | Source | Boot still checks already_bootstrapped rather than selecting a versioned upgrade/recovery workflow. A pre-existing bootstrap version skips publication. |
| O03 | open | Source | Server startup still has no store GC worker construction. Native GC methods and metrics are present but not that runtime owner. |
| O04 | open | Source | TTL helpers/metadata remain without a server-composed TTL job/task manager and owned periodic execution. |
| O05 | open | Source | There is still no Rust-crate caller of set_resource_control_interceptor. New vars.rs code concerns DDL limits, not PD resource control/runaway integration. |
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
| P06 | partial | Source | Native public close still stops region/TiKV owners without retiring its PD/TSO owner. Published deadline/batch/retry prerequisites and isolated grpcutil experiments do not close it. |
| P07 | open | Source | retry_mut still holds cluster.write() across call.await; metadata RPCs serialize and conflict with TSO read-side ownership. Throughput was not measured. |
| Q01 | open | Source | Cascades is accepted as a setting, but planner_bridge still uses logical_optimize/find_best_task. Recent COLLATE construction edits do not add memo exploration. |
| X01 | open | Source | SQL new_function and PB-selected builtins still construct/evaluate separately; numeric fast paths do not provide the shared typed scalar/vector owner for all types. |
| X02 | open | Source | The only expression change exposes build_cast_function. Provider registration, embedding cache/batcher, caller-isolated cancellation and EMBED_TEXT runtime remain absent. |
| C03 | open | Source | CoprCache.get does not update admission/recency; set evicts insertion_order and the read authority retains fixed configuration. |
| K01 | open | Source | ClusterTableAutoIds still selects the cached allocator; step_for maps AUTO_ID_CACHE <= 1 to DEFAULT_AUTO_ID_STEP. The single-point service allocator is uncomposed. |
| K02 | open | Source | CACHE changes metadata after admission checks; reads/writes still lack leased cache wrappers and commit-time renewal. Fresh acceptance is not a stale-read reproduction. |
| K03 | open | Observed | Fresh strict ordinary TINYINT rejects 1000; generated TINYINT stores 127 without warnings. Shared materialization still uses default conversion policy. |
| I04 | open | Source | Catalog publication remains a latest materialized image; vars.rs changed only the DDL error-limit publication, not versioned InfoCache/V2 loading. |
| S03 | open | Observed | Both SHOW SESSION_STATES and SET SESSION_STATES still refuse; variable helpers do not compose complete session migration. |
| S04 | open | Observed | tidb_read_staleness remains accepted but the next ordinary read refuses. The separate historical catalog path is still local rather than Domain timestamp/schema ownership. |
| E06 | open | Source | Projection close drops the parallel receiver and closes its child; queued evaluation tasks have no joined cancellation. This proves the ownership difference, not memory unsafety. |
| E07 | open | Observed | SHOW BR JOB still refuses; no live BRIE queue/executor replaced the constant information-schema providers. Helper protocol repairs do not compose backup/restore. |
| N04 | open | Source | KILL still validates IDs then operates on the local registry; no decoded-server remote dispatch/configuration lifecycle was added. |
| N05 | open | Source | HTTP status still uses the fixed route dispatcher/read-only settings; full Go admin/configuration handlers remain absent. |
| O10 | open | Source | DXF metrics/helpers remain without production manager, executor/storage/resource startup and close. |
| O11 | open | Source | TopSQL startup still initializes metrics only; SQL/plan registration, profiling collection, aggregation and reporting remain uncomposed. |
| O13 | open | Source | closest-adaptive still maps to mixed reads without the domain AZ distribution and request adjustment producer. DDL-limit publication did not change that path. |

## Exact validation and limits

From `rust/`, the runner executed:

```text
cargo run --locked -p tidb-session --example worker_followup_expanded_ownership
cargo run --locked -p tidb-session --example worker_followup_subsystem_structure
cargo run --locked -p tidb-session --example worker_followup_session_ownership
cargo run --locked -p tidb-session --example worker_followup_remaining_structure
cargo run --locked -p tidb-server --example worker_followup_expanded_server
cargo run --locked -p tidb-session --example worker_followup_partition_structure
cargo test --locked -p tidb-stats-handle-cache-internal-lfu --lib nonresident_primary_waits_for_admission -- --ignored
cargo test --locked -p tidb-server --lib system_table_ddl_does_not_publish_statistics_events_like_go
```

The examples are intentionally temporary. Recreate the same six outputs from the
repository root with the retained runner (the server probe needs localhost access):

```python
import importlib.util
from pathlib import Path
p = Path('rust/docs/parity/current-audit/run-expanded-probes.py')
spec = importlib.util.spec_from_file_location('probes', p)
probes = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probes)
for name in ('expanded-ownership', 'subsystem-structure', 'session-ownership',
             'remaining-structure', 'expanded-server', 'partition-structure'):
    crate = 'tidb-server' if name == 'expanded-server' else 'tidb-session'
    probes.run(crate, name + '-probe.rs', 'worker_followup_' + name.replace('-', '_'),
               'worker-followup-recheck/' + name + '.txt')
```

Source/status/link/output validation and `git diff --check` are required before
commit. Publication uses the actual hook, then a fresh locked server build:

```text
TERM=xterm git -c core.hooksPath=hooks commit -m 'docs: recheck unresolved Rust parity findings'
cd rust && cargo build --locked -p tidb-server
git push origin HEAD:hparser-integration
```

The build command runs from the repository root; the git commands also run there.
The hook independently runs that same locked build. Any receipt amendment repeats
both gates before push. Final gate outcomes are recorded below after execution.

This change only updates audit documentation/data. It changes no production
behavior, dependency, generated protocol, Go/Bazel input or original test.
`make lint`, Bazel preparation and failpoint switching are not needed for this
scope; previous production-change validation remains at its receipt scope.
Original Go tests, full Rust suites, real TiKV/TiFlash, distributed cancellation,
security/GC/upgrade behavior and sysbench/TPC-C/TPC-H/YCSB were not rerun here.
The unclassified DDL test and known cache mismatch remain failing. The principal
review risk is overreading a source gap as a reproduced distributed failure;
the evidence labels above deliberately preserve that distinction.

The evidence checker passed all 85 dispositions, 75 individual remaining
contracts, 124 prior references, current source hashes, 18 changed crate paths,
Markdown/JSON row agreement, local links and temporary-example cleanup:

```text
python3 /private/tmp/check-worker-followup-review.py
git diff --check
```

The actual hook's locked build passed in 0.68 seconds, and the independent
post-commit locked build passed in 20.43 seconds. Local logs are
`/private/tmp/tidb-worker-followup-review-commit.log` and
`/private/tmp/tidb-worker-followup-review-prepush.log`. This receipt amendment
runs `TERM=xterm git -c core.hooksPath=hooks commit --amend --no-edit` and then
the same locked build again, with final gate logs ending in `-commit-final.log`
and `-prepush-final.log`, before the normal branch push. The final response
records remote verification without a self-referential commit ID in this file.

Cleanup removed only this run's six example executables and six dependency
files, reclaiming 932,683,776 allocated bytes (about 890 MiB). The local manifest
is `/private/tmp/worker-followup-cleanup.json`; retained evidence and shared
build objects were not deleted.
