# Structural parity audit: current evidence

[Unused-owner cleanup](dead-owner-cleanup-validation.json): four metadata prototypes, obsolete DistSQL errors and five standalone targets removed; 56 retained cases pass. Finding statuses are unchanged.

The current register has **86 findings: 29 repaired and 57 unresolved (36 open, 21 partial)**. The [JSON register](structural-findings.json) and [readable register](structural-findings.md) own current dispositions; dated repair receipts own their original evidence. Finding repair is not complete Go package acceptance.

The [remaining empty-harness cleanup](empty-test-cleanup-validation.json) removes 347 empty functions across five crates, one unused marker and stale inventory prose. All 367 surviving functions in affected files remain verbatim; Go obligations stay unverified.

The [session cleanup](session-cleanup-validation.json) removes unused server models, empty session tests and duplicate source compilation; [original obligations](session-cleanup-obligations.json) remain unverified. Finding counts are unchanged.

The [authentication durability batch](auth-durability-batch-validation.json) advances A02/A03/N03 together: durable pessimistic login tracking, shared policy and internal-session ownership, canonical generated RSA/identity/temp-file TLS, and removal of duplicated test-source compilation. All three remain partial. See the [living plan](../../auth-durability-batch-execplan.md).

The [metadata policy batch](metadata-policy-batch-validation.json) replaces stale sequence schema/rows, composes shared SEM visibility, and repairs local cluster instance identity. I01/I02 remain partial; I03 fanout remains open. See the [living plan](../../metadata-policy-batch-execplan.md).

The [cluster topology batch](cluster-topology-validation.json) advances I02/O13/N03 together: seven live component retrievers, Domain zone balancing and joined refresh, live session policy, post-split request adjustment and canonical startup labels. All three remain partial; other54 unresolved IDs retain previous evidence. See the [living plan](../../cluster-topology-execplan.md) for remaining producers and validation limits.

The [matrix write removal](matrix-write-removal-validation.json) deletes the alternate INSERT/UPDATE/DELETE engine, synthetic row handles and unused interpreter helpers while retaining virtual reads. E03 stays partial; finding counts are unchanged.

The preceding [TLS owner batch](tls-owner-batch-repair.md) advances A02/A03/N03 together: GRANT REQUIRE shares account policy; certificate-file refresh, ALTER INSTANCE reload and automatic renewal share one process owner; request-only client certificates remain untrusted for account admission. Startup variables and SUPER/rollback behavior follow Go. All three findings remain partial; other54 unresolved IDs retain earlier evidence.

The [tooling removal inventory](obsolete-tooling-removal-validation.json) records retired nonbehavioral gates, profiling/probe scaffolding and superseded report before-images. Counts and previous failure dispositions are unchanged.

## Work from these owners

- [Structural batch map](remaining-batches.md): every unresolved finding assigned once, shared prerequisites and grouped validation.
- [Living full ExecPlan](../../full-structural-parity-execplan.md) and [current metadata plan](../../metadata-policy-batch-execplan.md): implementation, gates and recovery.
- [Coverage matrix](structural-coverage.md): inventory scope and explicitly unreviewed packages. Regenerate inventory with `python3 rust/scripts/inventory-go-rust-parity.py --go-ref origin/master`; inventory regeneration never accepts a package.
- [Validation receipt](metadata-policy-batch-validation.json): exact source/log identities and verification limits.

Fresh Go comparison: `93a01d31f6da205ae4bf376825293903a6899fdb`, selecting client-go `v2.0.8-0.20260928031501-8edb23f6c7ee`. Derive external pins from that master's go.mod, not the editable integration branch or an older oracle checkout. Native client master is `19a56ccda1e128218cd33c69709038219aced9bc` at this checkpoint.

A complete upstream package, including original tests, generated/platform/build inputs and fixtures, is the minimum acceptance unit. Search hits, a passing subset and retired Rust-only adapter tests do not discharge those obligations. The complete inventories are snapshots, not proof that every semantic mismatch is known.

## Cloud development and validation

Activate `/workspace/.cloud-setup/env.sh` in each shell. Edit `/workspace/tidb` on `hparser-integration` and `/workspace/client-rust` on `master`; keep current Go master separately for comparison. Use the pinned `rust/rust-toolchain.toml` (`nightly-2026-08-22`), Go 1.25.14 and protoc 35.1. Run Cargo from `rust/`; use `CARGO_BUILD_JOBS=1` for heavy linking in this Cloud workspace. Preserve existing build artifacts. The Git build-input watcher now excludes missing files while tracking loose/packed/detached HEAD changes; do not freeze version metadata to avoid builds.

Native fixes belong in client-rust first. Synchronize validated upstream source through `rust/scripts/sync-tikv-client-rs.sh`, including maintained patches and protobuf regeneration; do not hand-edit vendored client code. A failed patch is a stop-and-reconcile condition, never permission to skip it. The current user instruction forbids pushing, so any repair requiring a new published native revision must remain explicitly unpublished until that instruction changes.

Group related source fixes and test filters. Keep meaningful Go behavior/error/rollback assertions in the owning session tests. Remove placeholder or adapter-only harnesses after caller migration, with a recorded inventory; do not manufacture concurrency coverage from a serial loop. Run affected checks and required `make lint` once at the completed batch boundary. The actual executable `hooks/pre-commit` selected by `core.hooksPath=hooks` must run `cd rust && cargo build --locked -p tidb-server` on normal commits. Never bypass hooks; if pushing is later authorized, repeat that locked build immediately before every push and verify the remote SHA.

**No push or push dry run.** Preserve the exact destinations `pingcap/tidb hparser-integration` and `ngaut/client-rust master`. Historical permission diagnostic: `remote: Permission to pingcap/tidb.git denied to ngaut.` (HTTP 403); account role versus GitHub-app installation scope is undetermined. The user is arranging access. Preserve concurrent remote integration commits; never reset or force-push over them.

## Historical evidence

The repeated milestone summaries and stale count tables formerly copied into this index are removed. The original receipts below and Git history retain their source pins and validation limits. Complete TiPB declaration before-images live in [tipb-mismatches-before.json](tipb-mismatches-before.json); other protocol before/after evidence lives in [protocol-projections.json](protocol-projections.json) and [protocol-contracts-after.json](protocol-contracts-after.json). These replace duplicated declaration tables, not the underlying obligations.

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
- [2026-10-02 review after shared worker repairs](worker-followup-structural-review.md)
- [atomic health-publication repair](../../health-feedback-publication-execplan.md)
- [PD request-ownership follow-up](../../pd-request-ownership-execplan.md)
- [DDL error identity repair](../../ddl-error-identity-execplan.md)
- [CHECK error generation follow-up](../../ddl-error-generation-execplan.md)
- [2026-10-01 full register reconciliation](remaining-structure-review.md)
- [review of every unresolved finding](structural-review-followup.md)
- [review after removals](post-removal-structural-review.md)
- [partition shortcut removal](../../partition-owner-removal-execplan.md)
- [IMPORT shortcut removal](../../import-shortcut-removal-execplan.md)
- [cluster fixture removal](../../cluster-fixture-removal-execplan.md)
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
- [subsystem ownership review](subsystem-structure-review.md)
- [complete scope matrix](structural-coverage.md)
- [expanded production-owner review](expanded-ownership-review.md)
- [session/executor follow-up](session-ownership-review.md)
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
- [shared DDL error conversion repair](../../ddl-error-conversion-execplan.md)
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
