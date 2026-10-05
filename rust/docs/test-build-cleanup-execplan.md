# Retire obsolete standalone server tools

This living ExecPlan follows root PLANS.md. Git preserves before-images;
previous cleanup receipts remain indexed in parity/current-audit/README.md.

## Purpose / Big Picture


Remove two unused command-line tools and their disconnected helper paths as
one batch. The mandatory server package build will select two binaries instead
of four. Fresh-keyspace bootstrap remains owned by normal server startup, as
in Go; statistics remain exposed by SQL SHOW commands and the shared cache.
This reduces build targets; no build-time speedup has been measured.

## Context and Orientation


Cloud checkout /workspace/tidb is on hparser-integration at d33fc74e0ba53f1e0a3630491bea1a7eca47e2e0.
Freshly fetched Go master is b36c940a4332c866d8b0e2afde88f5e7c2fd7fed;
native master remains cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1.
Go cmd/tidb-server/main.go invokes BootstrapSessionWithExternalWorkloadManager;
pkg/executor/show_stats.go owns SHOW STATS_BUCKETS and SHOW STATS_TOPN.
Rust cluster_session_node/boot.rs and unistore_node.rs already call the same
publish_bootstrap owner. The standalone mysql-bootstrap tool duplicates that
entrypoint. cluster-stats-dump has no repository workflow caller and owns a
custom text report, including raw bounds/CMS queries; it is not the SQL interface.
Its sole-use load_table_stats_from_cluster wrapper retires with it.
The alternate publish_bootstrap_over_tikv implementation has no callers at all.

## Progress


- [x] Refresh refs; trace tools, helpers, startup callers and Go owners.
- [x] Remove both binaries, the dump-only reader and unused bootstrap alternative.
- [x] Verify target selection and unchanged retained inputs; run grouped checks.
- [ ] Complete required hook, wire validation, fresh pre-push build and remote verification.
- [ ] Save reusable cloud checkpoint and report limits.

## Milestones and Plan of Work


Retire rust/crates/tidb-server/src/bin/{mysql-bootstrap,cluster-stats-dump}.rs
and the two helper functions from bootstrap_publish.rs and real_tikv_stats.rs.
Remove unused imports and obsolete tool comments. Keep cluster-session-smoke:
run-realtikv-session-driver.sh and run-realtikv-scan-pushdown.sh actively call it
for transaction and coprocessor behavior. Keep existing bootstrap/statistics
tests and shared storage owners; no original Go obligation is discharged here.

Verify cargo metadata selects only tidb-server and cluster-session-smoke,
retained tests/scripts/manifests are byte-identical, and affected targets compile.
Exercise normal startup and SHOW statistics with a fresh embedded store.
Record exact validation in parity/current-audit/server-tool-cleanup-validation.json.
Update current cleanup links and both registers without changing dispositions.

## Concrete Steps / Validation and Acceptance


Activate /workspace/.cloud-setup/env.sh in each shell. From rust/ run:

    CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-exec -p tidb-server --all-targets
    cargo metadata --locked --no-deps --format-version 1

Run make lint and git diff --check from the checkout root. The normal commit
must run actual hooks/pre-commit and cargo build --locked -p tidb-server.
Use the built server for fresh unistore startup, bootstrap-table reads, ANALYZE,
SHOW STATS_TOPN and SHOW STATS_BUCKETS over MySQL. Repeat the same locked build
immediately before the authorized normal push; verify remote SHA. Do not bypass
hooks or force push. No new repository harness is needed for this deletion.
Broader Go and live multi-node TiKV tests remain unrun.

## Surprises & Discoveries


Cargo auto-discovers every src/bin file, so two unreferenced helper tools were
still linked during every package-level publication build. Removal needs no
manifest/lockfile edit. The alternate native bootstrap function had no callers;
the existing shared bootstrap function remains live in both stores.

The external wire probe must load needed statistics before asserting resident
SHOW TopN/bucket rows: positive-lease ANALYZE refreshes metadata. EXPLAIN
predicates exercise the shared loader. An initial Row_count probe also used
the Last_analyze_time column by mistake; the correct index is five. Corrected
external setup/field selection passes on the identical server binary.

## Decision Log


Remove obsolete tools and disconnected helpers together. Preserve the maintained
cluster smoke tool, shared loaders, commit-notification guard and Go contract
tests. Custom dump formatting is intentionally retired; SQL statistics remain
the diagnostic entrypoint. Date: 2026-10-05 UTC.

## Idempotence and Recovery / Interfaces and Dependencies


No dependencies, lockfiles, hooks, active scripts or SQL interfaces change.
The two standalone command names are removed. Recover before-images using
git show d33fc74e0b:<path> into temporary files for review. External evidence is
/workspace/.cloud-setup/server-tool-cleanup; rerunning checks is safe. Preserve
concurrent changes and build caches. Only inactive generated executables for
retired tools may be pruned to reclaim linking space.

## Outcomes & Retrospective


Removed 381 net source lines, two standalone binaries and two disconnected helper
functions. All-target checking, locked build, root lint and five actual MySQL
checks pass. Cargo metadata confirms two remaining binaries; all retained tests,
scripts, manifests and lockfiles are unchanged. Actual commit/push gates and
cloud save are recorded after commit in external final-handoff.json. Counts remain 86 tracked /
30 repaired /56 unresolved. This cleanup does not establish complete package
acceptance or repair independent behavioral findings.
