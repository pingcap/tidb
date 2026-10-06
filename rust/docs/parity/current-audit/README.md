# Structural parity audit: current evidence

Use the [JSON register](structural-findings.json) and [readable register](structural-findings.md) for current dispositions; use the [batch map](remaining-batches.md) for work allocation. Latest behavioral evidence: [typed user-variable ownership](user-variable-batch-validation.json); previous [internal process ownership](internal-process-batch-validation.json); previous [configured health and shared flow maintenance](store-maintenance-batch-validation.json); previous [ordinary forwarding and feedback](ordinary-forwarding-batch-validation.json); previous [replica routing](replica-routing-batch-validation.json); previous [snapshot read policy](snapshot-read-policy-batch-validation.json); previous [write diagnostics batch](write-diagnostics-batch-validation.json) and [coprocessor read policy](cop-read-policy-batch-validation.json). Latest build/test cleanup: [runner build overrides and workflow prose](runner-build-cleanup-validation.json); previous [unused compiler/result models](compiler-result-cleanup-validation.json); previous [disconnected statement/error boundaries](statement-boundary-cleanup-validation.json); previous [context/default/metrics ownership](context-owner-cleanup-validation.json); previous [executor test ownership](executor-owner-cleanup-validation.json); previous [retired harness plans](retirement-plan-cleanup-validation.json); previous [utility contract consolidation](utility-contract-cleanup-validation.json); previous [JSON carrier retirement](json-carrier-retirement-validation.json); previous [shared session SQL helpers](session-sql-helper-cleanup-validation.json); previous [completed statistics plans](statistics-plan-cleanup-validation.json); previous [result bookkeeping](result-bookkeeping-cleanup-validation.json); previous [shared parser tools](parser-tools-cleanup-validation.json); previous [shared result support](result-support-cleanup-validation.json); previous [completed utility plans](utility-plan-cleanup-validation.json); previous [mixed model carrier](model-carrier-cleanup-validation.json); previous [model test owners](model-test-owner-cleanup-validation.json); previous [retired semantic workflow](semantic-workflow-cleanup-validation.json); previous [variable test owners](vardef-test-owner-cleanup-validation.json); previous [chunk test owners](chunk-test-owner-cleanup-validation.json); previous [static test roots](static-test-roots-cleanup-validation.json); previous [plan-replayer harness consolidation](plan-replayer-cleanup-validation.json); previous [Domain test owner consolidation](domain-test-owner-cleanup-validation.json); previous [utility/protocol harness consolidation](utility-proto-harness-cleanup-validation.json); previous [macro-generated and panic-only placeholders](placeholder-macro-cleanup-validation.json); previous [obsolete scratch baseline logs](baseline-log-cleanup-validation.json); previous [orphan source carriers and alternate catalog adapters](orphan-storage-cleanup-validation.json); previous [disconnected aggregate/sort chain](aggregate-chain-cleanup-validation.json); previous [obsolete standalone server tools](server-tool-cleanup-validation.json); previous [duplicate user-variable harness consolidation](user-variable-batch-validation.json); previous [disconnected analyze models and legacy collector](analyze-model-cleanup-validation.json); previous [four disconnected read-planning adapters](read-plan-cleanup-validation.json); previous [nine disconnected session/statement models](session-model-cleanup-validation.json); previous [five disconnected DDL/restore models](ddl-model-cleanup-validation.json); previous [eleven disconnected session/executor helpers](session-helper-cleanup-validation.json); previous [seven disconnected planner paths and eight private harnesses](planner-private-path-cleanup-validation.json); previous [thirteen disconnected logical-operator models](operator-model-cleanup-validation.json); previous [eight disconnected rule models and nine private harnesses](rule-model-cleanup-validation.json); previous [disconnected scheduler/stack/cost models](task-model-cleanup-validation.json); previous [disconnected memo/pattern model and duplicate harnesses](memo-model-cleanup-validation.json); previous [nine disconnected models and seven private harnesses](disconnected-model-cleanup-validation.json); previous [eleven disconnected owners and private harnesses](leaf-owner-cleanup-validation.json); previous [retired configured planner chain](configured-planner-cleanup-validation.json); previous [unused configured join and result adapters](result-path-cleanup-validation.json); previous [unused session/statement models and private tests](session-leaf-cleanup-validation.json); previous [unused models and private leaf tests](dead-leaf-cleanup-validation.json); previous [executor rollback harness migration](write-diagnostics-batch-validation.json); earlier [unregistered executor/session carriers and stale receipts](orphan-test-cleanup-validation.json); earlier [unused JSON/percentile aggregate models and private harnesses](aggregate-leaf-cleanup-validation.json); earlier [comment-only tests, empty benchmark modules and stale mappings](comment-test-cleanup-validation.json), [obsolete session assertions and grant response harness](session-migration-batch-validation.json), [PD fixtures and sync rebuilds](pd-service-discovery-batch-validation.json), [obsolete audit plans and metadata harness](audit-plan-cleanup-validation.json), [workspace discard checks](discard-check-cleanup-validation.json), [statistics](statistics-test-cleanup-validation.json) and [build inputs](test-build-cleanup-validation.json). Finding repair is not complete Go package acceptance. Keep current counts in the registers and update the latest evidence links here; do not copy each new batch narrative into every working document.

Latest connected repair: [typed user-variable ownership](user-variable-batch-validation.json) joins S03/X01 across planning, SET, expression execution and migration; it removes the duplicate value-only store and premature AST substitution. Counts remain 86 tracked /30 repaired /56 unresolved (27 open, 29 partial). The two parents retain independent package obligations; other54 roots are carried evidence.

## Work from these owners

- [Structural batch map](remaining-batches.md): every unresolved finding assigned once, shared prerequisites and grouped validation.
- [Living full ExecPlan](../../full-structural-parity-execplan.md) and [current batch plan](../../user-variable-batch-execplan.md): implementation, gates and recovery.
- [Coverage matrix](structural-coverage.md): inventory scope and explicitly unreviewed packages. Regenerate inventory with `python3 rust/scripts/inventory-go-rust-parity.py --go-ref origin/master`; inventory regeneration never accepts a package.
- [Validation receipt](user-variable-batch-validation.json): exact source/log identities and verification limits.

Latest cleanup Go comparison: `b36c940a4332c866d8b0e2afde88f5e7c2fd7fed` (five design-document assets added since the prior comparison; production Go sources unchanged), selecting client-go `v2.0.8-0.20260928031501-8edb23f6c7ee`. Derive external pins from that master's go.mod, not the editable integration branch or an older oracle checkout. Native client master is `cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1` after the ordinary-forwarding accessor update.

A complete upstream package, including original tests, generated/platform/build inputs and fixtures, is the minimum acceptance unit. Search hits, a passing subset and retired Rust-only adapter tests do not discharge those obligations. The complete inventories are snapshots, not proof that every semantic mismatch is known.

## Cloud development and validation

Activate `/workspace/.cloud-setup/env.sh` in each shell. Edit `/workspace/tidb` on `hparser-integration` and `/workspace/client-rust` on `master`; keep current Go master separately for comparison. Use the pinned `rust/rust-toolchain.toml` (`nightly-2026-08-22`), Go 1.25.14 and protoc 35.1. Run Cargo from `rust/`; use `CARGO_BUILD_JOBS=1` for heavy linking in this Cloud workspace. Preserve existing build artifacts. The Git build-input watcher now excludes missing files while tracking loose/packed/detached HEAD changes; do not freeze version metadata to avoid builds.

Native fixes belong in client-rust first. Synchronize validated upstream source through `rust/scripts/sync-tikv-client-rs.sh`, including maintained patches and protobuf regeneration; do not hand-edit vendored client code. A failed patch is a stop-and-reconcile condition, never permission to skip it. Native publication must pass its validation gates. Sync compares contents and preserves unchanged destination mtimes; protobuf regeneration must still run.

Group related source fixes and test filters. Keep meaningful Go behavior/error/rollback assertions in the owning session tests. Remove placeholder or adapter-only harnesses after caller migration, with a recorded inventory; do not manufacture concurrency coverage from a serial loop. Run affected checks and required `make lint` once at the completed batch boundary. The actual executable `hooks/pre-commit` selected by `core.hooksPath=hooks` must run `cd rust && cargo build --locked -p tidb-server` on normal commits. Never bypass hooks; repeat that locked build immediately before every authorized push and verify the remote SHA.

**Publication authorized by the user on 2026-10-05.** Preserve destinations `pingcap/tidb hparser-integration` and `ngaut/client-rust master`. The prior diagnostic was `remote: Permission to pingcap/tidb.git denied to ngaut.` (HTTP 403): GitHub App installation `90274244` excluded TiDB despite account admin/push permission. The user corrected its selected-repository grant. Normal managed Cloud pushes subsequently succeeded for TiDB `11dbe67777` and native `bb8206e7d080`, with remote SHAs verified. This blocker is cleared; every future push still requires its fresh locked build and remote verification. Never extract credentials or force-push.

## Historical evidence

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

## Observed-plan batch, 2026-10-04

O11/O18/N03 advance together through shared plan samples, binary prepared/Global-switch policy, runtime TopSQL admission and SET labels. See [receipt](observation-plan-batch-validation.json) and [living plan](../../observation-plan-batch-execplan.md). Counts remain 86 tracked, 29 repaired and 57 unresolved (35 open, 22 partial). These three roots remain partial; the other 54 unresolved roots were not freshly revalidated. No pushes.

## Test-harness cleanup, 2026-10-04

Six test binaries, three source/doc-only assertions and two canned DDL self-checks are retired; 57 behavioral cases move into existing aggregates. Six stale catalog totals now test fixture-relative changes. The selected 74 cases pass. Original standalone cardinality/TopN suites retain one pass and three reproduced Go-estimate failures; their sources, expectations and isolation remain unchanged. Counts stay 86 tracked, 29 repaired, 57 unresolved. See [receipt](test-harness-retirement-validation.json).

The canonical-variance continuation of [harness retirement](test-harness-retirement-validation.json) removes four compatibility modules, test-only state/finalizer adapters, nine duplicate cases and three shell snippet self-tests. The retained live suite owns Go vectors; ordinary reset is preserved. Fourteen selected aggregate cases pass; finding statuses and prior failure dispositions stay unchanged.

The [window-model cleanup](test-harness-retirement-validation.json) retires six unused source modules and 15 model tests. Forty-three useful vectors execute through the live window suite in both modes, across chunks and reopen; all 14 cases pass. Unused-layout checks are retired, while Go memory/accounting obligations and finding statuses remain unchanged.

The PD channel batch migrates metadata, keyspace, discovery and TSO consumers together and removes the private adapter discovery runtime. See [validation](pd-channel-batch-validation.json). P03/P06 remain partial; counts remain 86 tracked / 30 repaired / 56 unresolved. Cloud `env.sh` now exports `CARGO_TARGET_DIR=/workspace/tidb/rust/target` so native checks and maintained regeneration reuse the installed cache.

## Shared JSON numeric conversion batch, 2026-10-05

[Plan](../../json-numeric-batch-execplan.md) and [validation](json-numeric-batch-validation.json) maintain K03/X01 together against Go b36c940a4332c866d8b0e2afde88f5e7c2fd7fed. Four Rust regressions and six MySQL assertions fail before; 129 Rust cases and seven MySQL/unistore assertions pass after, with eleven existing neighboring tests ignored. Shared JSON numeric errors, warning order and scalar/vector conversion replace duplicated parsers. Counts remain 86 tracked, 30 repaired, 56 unresolved (27 open, 29 partial). Complete packages and the other54 roots are not reaccepted. Actual hook and fresh pre-push locked build remain publication gates.
