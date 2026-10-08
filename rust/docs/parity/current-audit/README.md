# Structural parity audit: current evidence

Latest cleanup: [shared diagnostic digest lookup](digest-readers-validation.json) removes three direct map-only lookups and the obsolete normalized_sql_for_digest helper after migrating every caller. Ordinary and projected peer scans share one transport; the existing discovery test fixture is reused.

Use the [JSON register](structural-findings.json), [readable register](structural-findings.md) and [batch map](remaining-batches.md) for current dispositions and work allocation. Dated implementation and cleanup receipts remain indexed below and in the JSON repair/cleanup histories; they retain their original verification limits. Finding maintenance is not complete Go package acceptance.

Latest connected repair: [compute topology and placement](compute-topology-validation.json) composes autoscaler startup/cache/protocols, session policy and shared-fleet full-scan task fanout. M05 is now partial; M01 remains open and M04/N03 remain partial. Counts are86 tracked,32 repaired,54 unresolved (24 open,30 partial). Other roots retain carried evidence.

## Work from these owners

- [Structural batch map](remaining-batches.md): every unresolved finding assigned once, shared prerequisites and grouped validation.
- [Living full ExecPlan](../../full-structural-parity-execplan.md) and [current batch plan](../../compute-topology-execplan.md): implementation, gates and recovery.
- [Coverage matrix](structural-coverage.md): inventory scope and explicitly unreviewed packages. Regenerate inventory with `python3 rust/scripts/inventory-go-rust-parity.py --go-ref origin/master`; inventory regeneration never accepts a package.
- [Validation receipt](compute-topology-validation.json): exact source/log identities and verification limits.

Current Go comparison: `1f819a0b4a6cc07f9a8ff07e6777761a770c6d3d`, freshly fetched for this batch. Derive external pins from its go.mod; client-go remains `v2.0.8-0.20260928031501-8edb23f6c7ee`. Native client master is `02880abbab5ed89a4dc603a4ddc7935853870ea6`; the maintained sync reapplies all four patches and regenerates protobuf bindings. Earlier optimizer, statistics, native TSO and other repairs retain the dated receipts indexed below.

A complete upstream package, including original tests, generated/platform/build inputs and fixtures, is the minimum acceptance unit. Search hits, a passing subset and retired Rust-only adapter tests do not discharge those obligations. The complete inventories are snapshots, not proof that every semantic mismatch is known.

## Cloud development and validation

Activate `/workspace/.cloud-setup/env.sh` in each shell. Edit `/workspace/tidb` on `hparser-integration` and `/workspace/client-rust` on `master`; keep current Go master separately for comparison. Use the pinned `rust/rust-toolchain.toml` (`nightly-2026-08-22`), Go 1.25.14 and protoc 35.1. Run Cargo from `rust/`; use `CARGO_BUILD_JOBS=1` for heavy linking in this Cloud workspace. Preserve existing build artifacts. The Git build-input watcher now excludes missing files while tracking loose/packed/detached HEAD changes; do not freeze version metadata to avoid builds.

Native fixes belong in client-rust first. Synchronize validated upstream source through `rust/scripts/sync-tikv-client-rs.sh`, including maintained patches and protobuf regeneration; do not hand-edit vendored client code. A failed patch is a stop-and-reconcile condition, never permission to skip it. Native publication must pass its validation gates. Sync compares contents and preserves unchanged destination mtimes; protobuf regeneration must still run.

Group related source fixes and test filters. Keep meaningful Go behavior/error/rollback assertions in the owning session tests. Remove placeholder or adapter-only harnesses after caller migration, with a recorded inventory; do not manufacture concurrency coverage from a serial loop. Run affected checks and required `make lint` once at the completed batch boundary. The actual executable `hooks/pre-commit` selected by `core.hooksPath=hooks` must run `cd rust && cargo build --locked -p tidb-server` on normal commits. Never bypass hooks; repeat that locked build immediately before every authorized push and verify the remote SHA.

**Publication authorized by the user on 2026-10-05.** Preserve destinations `pingcap/tidb hparser-integration` and `ngaut/client-rust master`. The prior diagnostic was `remote: Permission to pingcap/tidb.git denied to ngaut.` (HTTP 403): GitHub App installation `90274244` excluded TiDB despite account admin/push permission. The user corrected its selected-repository grant. Normal managed Cloud pushes subsequently succeeded for TiDB `11dbe67777` and native `bb8206e7d080`, with remote SHAs verified. This blocker is cleared; every future push still requires its fresh locked build and remote verification. Never extract credentials or force-push.

Earlier implementation: [shared read consistency](read-consistency-batch-validation.json). Prior partition reorganization safety remains in place until its durable owner exists.

## Historical evidence

- [Shared diagnostic digest lookup](digest-readers-validation.json)

- [Cluster diagnostic readers](cluster-diagnostics-validation.json)

- [Cluster summary readers](cluster-summary-validation.json)

- [Incoming peer host](peer-host-validation.json)

- [Outgoing cluster peers](cluster-peer-validation.json)

- [MPP setup/recovery](mpp-recovery-validation.json)

- [Shared TSO completion](tso-completion-validation.json)

- [PD discovery observations](pd-observation-validation.json)

- [Shared FK policy and admission](fk-global-policy-validation.json)

- [TSO collection and live policy](tso-collection-batch-validation.json)

- [view admission and publication](view-owner-batch-validation.json)

- [persistent DDL catalog selection](persistent-ddl-batch-validation.json)

- [shared DDL visits](ddl-visit-batch-validation.json)

- [numeric production](numeric-production-batch-validation.json)

- [shared CTE visibility](cte-scope-batch-validation.json)

- [shared read consistency](read-consistency-batch-validation.json)

- [shared FK access](fk-access-batch-validation.json)

- [runtime settings batch](runtime-settings-batch-validation.json)
- [cluster configuration batch](cluster-config-batch-validation.json)
- [Harness consolidation](harness-dedup-validation.json)
- [remaining empty-harness cleanup](empty-test-cleanup-validation.json)
- [session cleanup](session-cleanup-validation.json)
- [original obligations](session-cleanup-obligations.json)
- [authentication durability batch](auth-durability-batch-validation.json)
- [living plan](../../auth-durability-batch-execplan.md)
- [cluster topology batch](cluster-topology-validation.json)
- [living plan](../../cluster-topology-execplan.md)
- [matrix write removal](matrix-write-removal-validation.json)
- [TLS owner batch](tls-owner-batch-repair.md)
- [tooling removal inventory](obsolete-tooling-removal-validation.json)

The repeated milestone summaries and stale count tables formerly copied into this index are removed. The original receipts below and Git history retain their source pins and validation limits. Complete TiPB declaration before-images live in [tipb-mismatches-before.json](tipb-mismatches-before.json); other protocol before/after evidence lives in [protocol-projections.json](protocol-projections.json) and [protocol-contracts-after.json](protocol-contracts-after.json). These replace duplicated declaration tables, not the underlying obligations.

- [rename admission/publication batch](rename-owner-batch-validation.json)
- [shared PD/TSO discovery batch](pd-service-discovery-batch-validation.json)
- [DML read/FK owner batch](dml-trigger-owner-batch-repair.md)
- [shared-read removal](dml-interpreter-removal-repair.md)
- [DML removal continuation](dml-removal-batch-repair.md)
- [DML metadata batch](dml-owner-batch-repair.md)
- [transport/expression cleanup](transport-expression-empty-test-removal.md)
- [admission-policy batch](admission-policy-batch-repair.md)
- [account TLS batch](account-tls-policy-batch-repair.md)
- [shared MPP lifecycle batch](shared-mpp-lifecycle-repair.md)
- [MPP transport batch](mpp-transport-batch-repair.md)
- [table-policy batch](table-policy-batch-repair.md)
- [statement attribution batch](statement-attribution-batch-repair.md)
- [exhaustive planner cleanup](planner-empty-module-removal.md)
- [placeholder cleanup](placeholder-test-removal.md)
- [cache/Apply batch](cache-apply-batch-repair.md)
- [statement observation batch](statement-observation-batch-repair.md)
- [DML policy batch](dml-policy-batch-repair.md)
- [MPP read batch](mpp-read-batch-repair.md)
- [TiFlash replica batch](tiflash-replica-batch-repair.md)
- [shared cache batch](shared-cache-batch-repair.md)
- [account history and locking-image repair](account-history-locking-repair.md)
- [expiry durability repair](account-expiry-durability-repair.md)
- [source/test review](cloud-account-test-review.md)
- [explicit PD shutdown repair](../../pd-shutdown-ownership-execplan.md)
- [the removal receipt](complete-protocol-owner-repair.md)
- [configuration/statistics maintenance batch](config-statistics-maintenance-repair.md)
- [shared server session batch](shared-server-session-repair.md)
- [2026-10-02 review after shared worker repairs](https://github.com/pingcap/tidb/blob/0743b4a0bb5f81a4ce9cc0b03d32301680b5ec95/rust/docs/parity/current-audit/worker-followup-structural-review.md)
- [atomic health-publication repair](../../health-feedback-publication-execplan.md)
- [PD request-ownership follow-up](../../pd-request-ownership-execplan.md)
- [DDL error identity repair](https://github.com/pingcap/tidb/blob/81367d835b0dbc0d0a252edb02424bb3c01b17e1/rust/docs/ddl-error-identity-execplan.md)
- [CHECK error generation follow-up](https://github.com/pingcap/tidb/blob/81367d835b0dbc0d0a252edb02424bb3c01b17e1/rust/docs/ddl-error-generation-execplan.md)
- [2026-10-01 full register reconciliation](https://github.com/pingcap/tidb/blob/0743b4a0bb5f81a4ce9cc0b03d32301680b5ec95/rust/docs/parity/current-audit/remaining-structure-review.md)
- [review of every unresolved finding](https://github.com/pingcap/tidb/blob/0743b4a0bb5f81a4ce9cc0b03d32301680b5ec95/rust/docs/parity/current-audit/structural-review-followup.md)
- [review after removals](https://github.com/pingcap/tidb/blob/0743b4a0bb5f81a4ce9cc0b03d32301680b5ec95/rust/docs/parity/current-audit/post-removal-structural-review.md)
- [partition shortcut removal](https://github.com/pingcap/tidb/blob/8d92a6bab3d28e7c47b34dadecfc064273f1edf9/rust/docs/partition-owner-removal-execplan.md)
- [IMPORT shortcut removal](https://github.com/pingcap/tidb/blob/8d92a6bab3d28e7c47b34dadecfc064273f1edf9/rust/docs/import-shortcut-removal-execplan.md)
- [cluster fixture removal](https://github.com/pingcap/tidb/blob/8d92a6bab3d28e7c47b34dadecfc064273f1edf9/rust/docs/cluster-fixture-removal-execplan.md)
- [PD preface ownership experiment](pd-grpcutil-contract/h2-preface-review.md)
- [full-picture repair sequence](repair-sequence.md)
- [PD deadline owner repair](pd-deadline-owner-repair.md)
- [PD connection-context repair](pd-connectionctx-owner-repair.md)
- [PD batch-controller repair](pd-batch-owner-repair.md)
- [PD retry-owner repair](pd-retry-owner-repair.md)
- [PD options owner repair](pd-opt-owner-repair.md)
- [statistics LFU follow-up](lfu-lifecycle-repair.md)
- [review follow-up](lfu-review-followup.md)
- [shared cache ownership review](shared-cache-owner-review.md)
- [restore-utils protocol repair](restore-utils-protocol-repair.md)
- [range-tree protocol repair](rtree-protocol-repair.md)
- [global-config synchronization repair](global-config-sync-repair.md)
- [subsystem ownership review](https://github.com/pingcap/tidb/blob/0743b4a0bb5f81a4ce9cc0b03d32301680b5ec95/rust/docs/parity/current-audit/subsystem-structure-review.md)
- [complete scope matrix](structural-coverage.md)
- [expanded production-owner review](https://github.com/pingcap/tidb/blob/0743b4a0bb5f81a4ce9cc0b03d32301680b5ec95/rust/docs/parity/current-audit/expanded-ownership-review.md)
- [session/executor follow-up](https://github.com/pingcap/tidb/blob/0743b4a0bb5f81a4ce9cc0b03d32301680b5ec95/rust/docs/parity/current-audit/session-ownership-review.md)
- [shared session cache repair](shared-session-plan-cache-repair.md)
- [shared UPDATE owner repair](shared-update-owner-repair.md)
- [progress-ownership follow-up](rtree-progress-ownership-repair.md)
- [metrics owner receipt](pd-metrics-owner-repair.md)
- [shared circuit-breaker receipt](pd-circuitbreaker-owner-repair.md)
- [retry value-ownership re-review](pd-retry-value-ownership-repair.md)
- [complete PD error owner](pd-errors-owner-repair.md)
- [DDL pause lifecycle repair](../../ddl-pause-lifecycle-execplan.md)
- [shared cancellation and error-checkpoint repair](../../ddl-cancellation-lifecycle-execplan.md)
- [action panic recovery repair](../../ddl-panic-owner-execplan.md)
- [shared DDL error conversion repair](https://github.com/pingcap/tidb/blob/81367d835b0dbc0d0a252edb02424bb3c01b17e1/rust/docs/ddl-error-conversion-execplan.md)
- [shared worker continuation repair](../../ddl-worker-continuation-execplan.md)

## Reproduced baseline failures still requiring root-cause review

The previous embedded suite reproduced these ten failures both before and
after the transaction activation repair. These are failed validations, not yet
ten proven structural root causes. These are historical failures, not a fresh result for every current owner. The expanded audit reran
`system_table_ddl_does_not_publish_statistics_events_like_go`: it fails at
`p1 exists` after ADD PARTITION. Current DDL already filters system-schema
notifications, so this failure does not establish a missing event filter:

- `add_partition_statistics_follow_global_prune_mode_like_go`
- `cluster_info_reports_this_node`
- `drop_partitions_statistics_match_go`
- `exchange_partition_validates_and_swaps_real_rows_atomically`
- `global_stats_drive_partition_plans_like_go`
- `partition_scoped_analyze_refreshes_global_count_and_modify_count`
- `stats_notifier_uses_a_real_internal_transaction_like_go`
- `system_table_ddl_does_not_publish_statistics_events_like_go`
- `truncate_hash_partition_statistics_match_go`
- `truncate_partitions_refreshes_global_stats_meta_like_go`

## Observed-plan batch, 2026-10-04

O11/O18/N03 advance together through shared plan samples, binary prepared/Global-switch policy, runtime TopSQL admission and SET labels. See [receipt](observation-plan-batch-validation.json) and [living plan](../../observation-plan-batch-execplan.md). Counts remain 86 tracked, 29 repaired and 57 unresolved (35 open, 22 partial). These three roots remain partial; the other 54 unresolved roots were not freshly revalidated. No pushes.

## Test-harness cleanup, 2026-10-04

Six test binaries, three source/doc-only assertions and two canned DDL self-checks are retired; 57 behavioral cases move into existing aggregates. Six stale catalog totals now test fixture-relative changes. The selected 74 cases pass. Original standalone cardinality/TopN suites retain one pass and three reproduced Go-estimate failures; their sources, expectations and isolation remain unchanged. Counts stay 86 tracked, 29 repaired, 57 unresolved. See [receipt](test-harness-retirement-validation.json).

The canonical-variance continuation of [harness retirement](test-harness-retirement-validation.json) removes four compatibility modules, test-only state/finalizer adapters, nine duplicate cases and three shell snippet self-tests. The retained live suite owns Go vectors; ordinary reset is preserved. Fourteen selected aggregate cases pass; finding statuses and prior failure dispositions stay unchanged.

The [window-model cleanup](test-harness-retirement-validation.json) retires six unused source modules and 15 model tests. Forty-three useful vectors execute through the live window suite in both modes, across chunks and reopen; all 14 cases pass. Unused-layout checks are retired, while Go memory/accounting obligations and finding statuses remain unchanged.

The PD channel batch migrates metadata, keyspace, discovery and TSO consumers together and removes the private adapter discovery runtime. See [validation](pd-channel-batch-validation.json). P03/P06 remain partial; counts remain 86 tracked / 30 repaired / 56 unresolved. Cloud `env.sh` now exports `CARGO_TARGET_DIR=/workspace/tidb/rust/target` so native checks and maintained regeneration reuse the installed cache.

## Shared JSON numeric conversion batch, 2026-10-05

[Plan](../../json-numeric-batch-execplan.md) and [validation](json-numeric-batch-validation.json) maintain K03/X01 together against Go b36c940a4332c866d8b0e2afde88f5e7c2fd7fed. Four Rust regressions and six MySQL assertions fail before; 129 Rust cases and seven MySQL/unistore assertions pass after, with eleven existing neighboring tests ignored. Shared JSON numeric errors, warning order and scalar/vector conversion replace duplicated parsers. Counts remain 86 tracked, 30 repaired, 56 unresolved (27 open, 29 partial). Complete packages and the other 54 roots are not reaccepted. Actual hook and fresh pre-push locked build remain publication gates.
