# Structural recheck and persisted-DDL synchronization mode repair

Reviewed 2026-10-02 from integration
`d0f1c371150530fb7b5c37730456c5d56ad11e64`, Go master
`93a01d31f6da205ae4bf376825293903a6899fdb`, native client-rust and dependency
`19a56ccda1e128218cd33c69709038219aced9bc`. Both implementation branches were
pulled and Go master fetched; all were current. Client-go remains
`v2.0.8-0.20260928031501-8edb23f6c7ee`. No dependency update was needed.

**All 71 previously unresolved IDs still had an unmet contract before this
repair. D07's recorded mode-ownership gap is now repaired, leaving 70 unresolved:
62 open and eight partial.** Sixteen IDs are repaired out of 86 tracked. Of the
70 unresolved, 68 concern live behavior or missing runtime integration; D09/D10
are disabled seeds. These are finding counts, not package acceptance counts or
70 freshly reproduced production failures.

## Review evidence and correction of stale claims

[Per-ID source continuity](mdl-review-recheck/source-continuity.json) records
every reviewed ID and exact source blobs. Fifteen unresolved IDs had changed
referenced files since the prior full review; 56 had unchanged references.
Every intervening Rust production change was inspected. Go master and the
native client remain unchanged. This permits carrying unchanged source findings
while separately reviewing affected owners and callers; it is not an exhaustive
new semantic audit of every package.

The recent local ALTER work does not close D11: foreign-key, CHECK, table-option,
partition and table-rename admission and durable execution remain missing. Its
old eager-allocator, index-rename and column-admission symptoms are repaired.
The [fresh allocator diagnostic](mdl-review-recheck/alter-residual.txt) now returns
IDs 1 and 2 after the rejected rebase/duplicate-column ALTER.

Four retained diagnostic programs were rerun. Their exit zero means the probes
completed, not that their behavior matches Go:

- [Account/privilege/import](mdl-review-recheck/expanded-ownership.txt): UPDATE
  denials remain repaired; password-history reuse and NULL policy, X509 refusal
  and missing IMPORT remain observable.
- [Wire/configuration](mdl-review-recheck/expanded-server.txt): Latin-1 E9 still
  becomes C3A9; stats-lease and ssl-ca configuration still refuse; auto-TLS is
  still true. Token-limit succeeds as previously repaired. The removed
  run-auto-analyze option is correctly refused and is not counted as a defect.
- [Other subsystem controls](mdl-review-recheck/subsystem-structure.txt): session
  migration, BR and stale reads still refuse. Generated TINYINT overflow rejects
  without storing a row. Cascades/CACHE syntax acceptance does not establish
  their missing runtime owners.
- The allocator diagnostic above confirms removal of one stale D11 symptom.

Thirteen remaining IDs have fresh diagnostic observations; 57 retain source
evidence and historical observations. D07 separately has four failing-before,
passing-after regressions.

## D07: one schema-synchronization owner, both source modes

The previous allegation was too broad. `tidb-schemaver` already selects per-job
MDL acknowledgements or non-MDL loaded self-version keys. The persisted worker
unconditionally required/read/wrote MDL metadata, however. A recovered DONE job
with no MDL row could skip synchronization and move straight to history.

Go `pkg/ddl/job_worker.go::registerMDLInfo` skips disabled mode.
`job_scheduler.go::transitOneJobStepAndWaitSync` recovers a started non-MDL job
using `schema_version.go::waitVersionSyncedWithoutMDL`, which reads the latest
schema version with a nonempty diff. `updateGlobalVersionAndWaitSynced` retains
the mode distinction on notification failures. `cleanMDLInfo` and
`pkg/infoschema/issyncer/syncer.go::MDLCheckLoop` skip MDL-only work when disabled.

The common Rust planner now receives `DdlJobSchemaState`, an explicit projection
of worker-local synchronization state. A normal committed phase waits its own
version; a new owner without acknowledgement state recovers the latest nonempty
diff. Non-MDL mode neither depends on the MDL table nor writes MDL rows. History
and subsequent phases still wait. Notification failures continue to the shared
self-version wait in non-MDL mode and propagate in MDL mode. Per-job cleanup
runs only for MDL. All planner callers, including disabled seed callers, carry
the mode explicitly; the runtime reads the real process/kernel policy.

The follower loop also now skips MDL job acknowledgements when disabled. Before
the fix, a stale job row caused reports `(job 0, version 5)` then `(job 700,
version 1)`, overwriting the loaded self-version. The regression now reports
only the loaded version. Its report cache includes the mode so a prior MDL
no-op does not suppress the non-MDL self report after mode changes.

Removed: compulsory MDL metadata ownership in disabled mode and unconditional
per-job acknowledgement/cleanup. No second synchronizer or artificial lease
timer was introduced. Source comments mention lease waits, but current master
passes the scheduler context; this repair follows executable source rather than
inventing a fixed-delay fallback. General scheduler/context/error/DDL package
acceptance is not implied.

Production edits are in executor bridge files
`rust/crates/tidb-exec/src/cluster_ddl.rs` and `real_tikv_ddl.rs`, and server
`rust/crates/tidb-server/src/cluster_session_node/schema_sync.rs`. Tests/caller
migration are in `tidb-exec/tests/cluster_ddl_source.rs` and server
`cluster_session_node/ddl.rs`; follower regression coverage is in schema_sync.rs.
The living [ExecPlan](../../ddl-schema-sync-mode-execplan.md) records scope and
decisions. D01–D03, D05, D08–D11 retain their separate remaining contracts.

## Validation and publication

[Machine-readable validation](mdl-review-recheck/validation.json) retains exact
commands, red evidence, baseline comparison and diagnostic outputs. From `rust/`:

| Command | Final result |
| --- | --- |
| `cargo test --locked -p tidb-server --lib cluster_session_node::ddl::schema_sync_tests -- --nocapture` | 10 pass |
| `cargo test --locked -p tidb-server --lib cluster_session_node::schema_sync::tests -- --nocapture` | Eight pass |
| `cargo test --locked -p tidb-exec --lib real_tikv_ddl::tests -- --nocapture` | Eight pass |
| `cargo test --locked -p tidb-schemaver --lib -- --nocapture` | Nine pass |
| `cargo test --locked -p tidb-exec --test all cluster_ddl_source:: -- --nocapture` | 105 pass, five unchanged-baseline failures |
| `cargo check --locked -p tidb-exec -p tidb-server --all-targets` | Pass |

Totals: **140 pass and five unchanged-baseline failures**. The three new worker
tests and one follower test fail before their fixes. Controls cover enabled-mode
owner replacement and loss, every DROP phase, disabled-mode notification
failures, missing MDL metadata and recovery of a newer nonempty diff.

The five wider-suite failures reproduce on unchanged HEAD after restoring both
changed exec production files and their API-migrated test file, then restoring
the edited bytes in `finally`. They are:

- `a_created_database_is_stored_exactly_as_the_go_server_stores_it`
- `a_created_database_persists_its_resolved_charset_and_collation`
- `a_modify_column_reorganizes_exactly_where_go_says_it_must`
- `check_job_submission_precedes_every_schema_transition`
- `completed_check_schema_stays_active_until_schema_sync`

They concern existing notifier fixtures, MODIFY-default admission expectations
and CHECK lifecycle expectations. They are not counted as passing. From the
repository root, `GOTOOLCHAIN=go1.25.14 make lint` and `git diff --check` pass.
No Go/Bazel/generated/dependency inputs changed; bazel_prepare is not required.

The diagnostic runner temporarily installed each retained source under its crate's
examples directory and removed it in `finally`. Exact execution commands were:

```sh
cd rust
cargo run --locked -p tidb-session --example mdl_review_expanded_ownership
cargo run --locked -p tidb-session --example mdl_review_subsystem_structure
cargo run --locked -p tidb-server --example mdl_review_expanded_server
cargo run --locked -p tidb-session --example mdl_review_alter_residual
```

Replay source/installation mechanics are provided by the existing
`run-expanded-probes.py`; the receipt's output files retain these observations.
Only this task's generated diagnostic binaries/dependency files were removed
afterward, reclaiming 628,428,800 allocated bytes (about 599 MiB); shared build
dependencies and user files were retained.

Publication requires the actual repository hook and a fresh final-commit build:

```sh
TERM=xterm git -c core.hooksPath=hooks commit -m "ddl: honor schema synchronization mode in persisted workers"
(cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration
```

The actual pre-commit hook passed the locked server build in 12.94 seconds.
This receipt amendment repeats the same hook. The final publication command
then reruns the locked build; the task thread records push, remote-SHA and
clean-checkout verification.

This repairs existing owners; it does not accept the complete Go DDL package.
No live Go SQL oracle, mixed-node TiKV/etcd deployment, process-crash recovery,
complete upstream package suite or sysbench/TPC-C/TPC-H/YCSB benchmark was run.
No measured performance improvement is claimed. The change removes unnecessary
MDL metadata reads/writes when disabled while preserving the schema barrier.

## Every remaining finding


| ID | Status | Evidence | Remaining contract |
| --- | --- | --- | --- |
| A01 | partial | Fresh diagnostic + source | UPDATE's resolved privilege path remains repaired; other statements still derive privilege visits outside the shared planner owner. Retained denial controls do not close those producers. |
| A02 | open | Source continuity | The durable loader still omits mysql.global_priv and policy fields; registry reconstruction and empty-user handling remain unchanged. No mixed-node authentication test was run. |
| A03 | open | Fresh diagnostic + source | TLS still uses with_no_client_auth and REQUIRE X509 still refuses. Certificate verification, policy propagation and rotation remain incomplete. |
| A04 | open | Fresh diagnostic + source | Retained SQL accepts PASSWORD HISTORY 3, reuses the first password and reads NULL policy. Account option handling still ignores the history/reuse fields. |
| B01 | open | Source continuity | CostLruStore.get only reads the map; insertion-order eviction and unconditional fitting admission remain the live binding store. |
| B02 | open | Source continuity | The lease reloader still constructs a full BindingCache from storage rows; no incremental watermark, usage writer or owner GC loop was added. |
| N01 | open | Fresh diagnostic + source | The wire diagnostic still converts raw Latin-1 E9 to C3A9. UTF-8 String ingress and literal representation are unchanged. |
| N03 | open | Fresh diagnostic + source | Retained TOML accepts token-limit, as repaired, but still rejects performance.stats-lease and security.ssl-ca; auto_tls remains true. The second NodeConfig authority and incomplete shared configuration remain. |
| D01 | open | Source continuity | Only CHECK SQL submits through the durable worker; other DDL uses the direct transaction route. Retained repartition refusals preserve rows but leave Go-supported reorganization absent. |
| D02 | open | Source continuity | Index entries still backfill in the publication transaction through a default statement context; no complete durable index/reorg lifecycle was added. |
| D03 | open | Source continuity | run_loop still iterates jobs serially and calls run_persisted_ddl_job to completion, skipping unsupported action types. Worker error continuation did not introduce worker pools. |
| D05 | partial | Source continuity | Migrated persisted action and CHECK producers now retain source RFC/class identity and shared formatting through checkpoints and history. Other admission/storage producers still erase identity; complete error taxonomy, rollback-transaction classification, configurable retry timing and metrics remain missing. |
| D08 | open | Source continuity | Finalization still lacks the complete typed delete-range registration; direct index/partition paths retain eager key removal. This is distinct from the absent GC worker. |
| D09 | open | Disabled seed | The in-memory materialized-view build seed remains, but supports_persisted_ddl_job excludes its action. This is an inactive implementation gap, not an admitted production failure. |
| D10 | open | Disabled seed | The MV/MLog dependency seed still lacks the full dependent-ID/publication/rollback lifecycle and remains excluded from live dispatch. This is an inactive implementation gap. |
| D11 | partial | Fresh diagnostic + source | Catalog/storage publication, deferred allocator effects, original-schema column/index preparation, admitted conflict inputs, stable IDs and AUTO_RANDOM offsets are repaired. Other action admission and durable preparation/execution/recovery remain open; prior eager-allocator and drop-column/index-rename symptoms are historical. Fresh allocator diagnostic now returns IDs 1 and 2 after the rejected rebase/duplicate-column ALTER. |
| C02 | open | Source continuity | Instance-cache variables and metric selectors still have no domain cache consumer. The working session cache is a separate repaired owner. |
| C04 | partial | Source continuity | Retained explicit ignored admission regression still fails: a nonresident primary is visible before Stretto admission. The published lifecycle and shard-index repairs remain intact. |
| E02 | partial | Source continuity | Retained joined UPDATE rejects an orphan as repaired. Physical UPDATE still omits Go's per-table FK plans, so the plan-to-execution contract remains incomplete. |
| E03 | open | Source continuity | execute_dml_source still returns a fully drained Vec of datum rows; multi-DML reconstructs target handles and charges after collection. No streaming handoff was added. |
| E04 | open | Source continuity | The physical Apply builder still creates NestedLoopApplyExec; concurrency metadata and the setting have no parallel execution consumer. |
| E05 | open | Fresh diagnostic + source | Retained IMPORT refuses and leaves its target empty. The removed parser/INSERT pipeline stays removed; Go's import controller and durable tasks remain absent. |
| S01 | open | Source continuity | The optional configured server still selects its one/two-table private read/write planner rather than ordinary session/compiler ownership. |
| S02 | open | Source continuity | The two-table startup branch still drops its initial catalog snapshot and installs no shared catalog reloader, unlike the one-table branch. |
| I01 | open | Source continuity | Retained SEQUENCES remains empty for a created sequence and CLUSTER_LOG ignores supplied bounds; CLUSTER_CONFIG now refuses. Missing live retrievers remain. |
| I02 | open | Source continuity | cluster_info_table_rows still builds only TiDB server entries. Fake stores and swallowed discovery errors remain removed, but six source retrievers remain absent. |
| I03 | open | Source continuity | CLUSTER_PROCESSLIST still decorates local process rows; no complete peer fanout task/retrieval lifecycle is composed. |
| T01 | open | Source continuity | BufferMutation::insert still couples presume-not-exists to AssertNotExist; the configured INSERT planner has no source transaction-mode policy input. |
| T02 | partial | Source continuity | ClientPd still delegates to TiDB routing while native client-rust retains its own region/cache/RPC owners. Atomic health publication, owned PD requests and joined shutdown are repaired; those lower-owner repairs do not consolidate this bridge. |
| O01 | open | Source continuity | Command admission and connection panic recovery changed sql_node.rs but did not add numeric server-ID leasing; connection allocation still uses standalone identity. |
| O02 | open | Source continuity | Boot still checks already_bootstrapped rather than selecting a versioned upgrade/recovery workflow. A pre-existing bootstrap version skips publication. |
| O03 | open | Source continuity | Server startup still has no store GC worker construction. Native GC methods and metrics are present but not that runtime owner. |
| O04 | open | Source continuity | TTL helpers/metadata remain without a server-composed TTL job/task manager and owned periodic execution. |
| O05 | open | Source continuity | There is still no Rust-crate production caller of set_resource_control_interceptor; domain startup does not compose the PD resource controller and runaway integration. |
| O06 | open | Source continuity | RuStatsWriter is still constructed only in source tests; startup does not run the owner-gated request-unit persistence/retention loop. |
| O07 | open | Source continuity | gc_stats remains a public helper called by tests; existing startup workers do not periodically invoke it. Analyze-job cleanup is a separate responsibility. |
| O08 | open | Source continuity | Plan replayer collectors/dumpers remain helpers and test constructions, without server startup, capture production or file-GC integration. |
| O09 | open | Source continuity | ServerInfoSyncer still explicitly excludes ReportMinStartTS and no live reporter composes session/cursor/internal/schema timestamps. No premature-GC reproduction is claimed. |
| O14 | open | Source continuity | Affinity still reaches metadata and plan carriers without a PD group manager or complete DDL lifecycle; CPU affinity is unrelated. |
| O15 | open | Source continuity | No cross-keyspace runtime/session manager was added; codecs alone do not provide holder/eviction/virtual-server/min-start-ts ownership. This is conditional on source variants supporting it. |
| O16 | open | Source continuity | The live workload-repository sampler remains distinct from workload-learning read-cost analysis, persistence and cache refresh, which remain absent. |
| O17 | open | Source continuity | Telemetry settings/admission remain without a production counter/window collector and periodic log reporter. Go's present contract does not require an external uploader. |
| O18 | open | Source continuity | With both summary switches enabled, retained SQL still leaves cumulative statement stats at zero. Production still does not submit StmtExecInfo or compose the v2 sink lifetime. |
| O19 | open | Source continuity | Statistics load metrics still have no production request/wait/dedup/read observation calls; running load workers and initialized collectors do not close this gap. |
| F01 | open | Source continuity | Classic placement rule creation/repair/removal is still performed by poll_once rather than the complete DDL/GC owner. |
| F02 | open | Source continuity | SetTiFlashReplica still rebuilds unavailable metadata and the poller tracks logical table IDs; physical-partition availability/reset policy is incomplete. |
| F03 | open | Source continuity | poll_once still requests and filters gRPC Up stores on each pass without the source progress cache/backoff/HTTP-discovery lifecycle. |
| M01 | open | Source continuity | MPP remains scan-only, selects one Up TiFlash store, builds one task and refuses pushed Selection/TopN/aggregation. |
| M02 | open | Source continuity | table_regions still collapses disjoint ranges into one min/max envelope and uses a 10000-region query without continuation. |
| M03 | open | Source continuity | open_mpp still drains the complete response into VecDeque; closing the returned stream only drops local packets, without a remote cancellation owner. |
| M04 | open | Source continuity | MPP still connects directly to http:// endpoints; stale regions are logged outside the shared routing/security/recovery owner. |
| M05 | open | Source continuity | Disaggregated settings still have no composed compute topology/cache/dispatch consumer; the live scan selects ordinary PD stores. |
| P03 | open | Source continuity | PD member/leader discovery and classic Tso remain the only active discovery path; no service-mode/independent TSO service switch is composed. |
| P06 | partial | Source continuity | Public native close, retained request ownership and interrupted stream retirement are repaired. Native Cluster still owns only the PD leader Tso stream and GetMembers discovery; it has no operational GetClusterInfo service-mode/TSO-service switching or follower-proxy option consumer. Pinned Go servicediscovery and clients/tso compose both. This residual is source-confirmed, not inferred from missing package acceptance or demand-driven Idle reconnect. |
| Q01 | open | Fresh diagnostic + source | Cascades is accepted as a setting, but planner_bridge still uses logical_optimize/find_best_task without memo exploration. No current caller installs a Cascades owner. |
| X01 | open | Source continuity | SQL new_function and PB-selected builtins still construct/evaluate separately; numeric fast paths do not provide the shared typed scalar/vector owner for all types. |
| X02 | open | Source continuity | Provider registration, embedding cache/batcher, caller-isolated cancellation and EMBED_TEXT runtime remain absent. Retained variables and expression mappings do not compose that runtime. |
| C03 | open | Source continuity | CoprCache.get does not update admission/recency; set evicts insertion_order and the read authority retains fixed configuration. |
| K01 | open | Source continuity | ClusterTableAutoIds still selects the cached allocator; step_for maps AUTO_ID_CACHE <= 1 to DEFAULT_AUTO_ID_STEP. The single-point service allocator is uncomposed. |
| K02 | open | Fresh diagnostic + source | CACHE changes metadata after admission checks; reads/writes still lack leased cache wrappers and commit-time renewal. Retained acceptance is not a stale-read reproduction. |
| K03 | partial | Fresh diagnostic + source | The retained strict generated-TINYINT probe now rejects overflow and stores no row, so that old symptom is repaired. Shared generated write/read/ANALYZE casts and typed delivery remain repaired. Lower datatype error/value identities, generated-expression argument context and legacy ENUM/SET collation context remain; other ANALYZE build/storage adapters remain generic. |
| I04 | open | Source continuity | Catalog publication remains a latest materialized image without the versioned InfoCache/V2 loading owner. The new computed-default loader changes table construction, not schema-history ownership. |
| S03 | open | Fresh diagnostic + source | Both SHOW SESSION_STATES and SET SESSION_STATES still refuse; variable helpers do not compose complete session migration. |
| S04 | open | Fresh diagnostic + source | tidb_read_staleness remains accepted but the next ordinary read refuses. The separate historical catalog path is still local rather than Domain timestamp/schema ownership. |
| E07 | open | Fresh diagnostic + source | SHOW BR JOB still refuses; no live BRIE queue/executor replaced the constant information-schema providers. Helper protocol repairs do not compose backup/restore. |
| N04 | open | Source continuity | KILL still validates IDs then operates on the local registry; no decoded-server remote dispatch/configuration lifecycle was added. |
| N05 | open | Source continuity | HTTP status still uses the fixed route dispatcher/read-only settings; full Go admin/configuration handlers remain absent. |
| O10 | open | Source continuity | DXF metrics/helpers remain without production manager, executor/storage/resource startup and close. |
| O11 | open | Source continuity | TopSQL startup still initializes metrics only; SQL/plan registration, profiling collection, aggregation and reporting remain uncomposed. |
| O13 | open | Source continuity | closest-adaptive still maps to mixed reads without the domain AZ distribution and request adjustment producer. No new server caller composes the missing policy. |
