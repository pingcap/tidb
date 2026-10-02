# Share effective configuration and start statistics maintenance

This living ExecPlan follows PLANS.md and covers maintenance of existing Rust
owners, not acceptance of complete Go packages.

## Purpose / Big Picture


Repair N03 and O07 together. The shared configuration package already loads
TiDB options, but the executable rejects most of them with another whitelist
and overrides selected values independently. Statistics GC exists, but startup
never runs it. Preserve the effective configuration through explicit CLI
overrides, then compose Go's statistics maintenance lifecycle for both stores.

## Progress


- [x] Pull hparser-integration and fetch master; clean baseline 43e827e2c4,
  Go master 93a01d31f6da205ae4bf376825293903a6899fdb.
- [x] Verify shared configuration/override and statistics GC owners in source.
- [x] Prove configuration rejection/default drift, absent periodic GC and the
  captured auto-analyze switch with failing regressions.
- [x] Remove duplicate configuration policy and migrate startup projections.
- [x] Wire positive-lease GC, health and eviction maintenance, ownership and shutdown.
- [x] Run 139 distinct targeted tests, all-target checking and a real stock-MySQL
  unistore process check; see the linked receipt for exact commands.
- [x] Update findings and validation receipt; O07 repaired, N03 partial.
- [x] Run final lint, self-review the diff, and verify register/link consistency.
- [x] Actual hook locked build and fresh pre-push locked build passed; code
  `f8b02a07d1826285c96907e074c9cd6b88eeffca` pushed and exact remote SHA verified.

## Context and Orientation


N03 lives in rust/crates/tidb-server/src/node_config.rs. The existing config
owner is tidb-config/src/config_tree/load.rs; main_flags.rs::override_config
already models explicit Go flags. Runtime-only options (auth-file, connection
timeout and embedded test adapters) remain native adapters, not another TOML
schema. A03 remains the separate missing certificate-policy lifecycle.

O07 lives in cluster_session_node/mod.rs::gc_stats and startup in
cluster_session_node/boot.rs and unistore_node.rs. SharedStats wraps the existing
statistics cache. Go pkg/domain/domain.go starts gcStatsWorker after statistics
initialization, only for positive leases. Every 100 leases the elected owner
calls GCStats and checks auto-analyze windows, every 20 leases every node updates
health metrics, and every 300ms every node samples memory/triggers eviction.
GC uses max(statistics lease, schema lease) to protect recent metadata. This is
statistics garbage collection, not the separate MVCC storage GC finding O03.

## Plan of Work and Milestones


First add configuration regressions for complete recognized TOML settings,
CLI precedence and Go's false auto-TLS default. Run them before production
changes. Add a periodic GC regression against the real embedded storage stack,
with owner acquisition, then prove no manual GC call is needed.

Next remove the duplicate TOML whitelist and reconstruction. Use the shared
configuration loader and existing flag override, then derive executable fields
from that effective value. Preserve runtime-only argument validation and ensure
startup JSON agrees with execution. Keep unknown/moved-key policy with the
shared configuration owner.

Finally add a retained, stoppable statistics maintenance owner. Reuse GC,
health, memory and cache implementations; do not create another stats store.
Join maintenance before retiring its factory and storage capabilities. Verify
lease gates, owner changes, failure continuation, timers and shutdown.

## Concrete Steps and Validation


Run focused tests from rust/ so .cargo/config.toml supplies the debug stack:

    cargo test --locked -p tidb-server --test all node_config_source:: -- --test-threads=1
    cargo test --locked -p tidb-server --lib stats_gc -- --test-threads=1
    cargo test --locked -p tidb-server --lib stats_maintenance -- --test-threads=1
    cargo test --locked -p tidb-config --lib config_tree::load:: -- --test-threads=1
    cargo check --locked -p tidb-server --all-targets

From the repository root:

    GOTOOLCHAIN=go1.25.14 make lint
    git diff --check
    TERM=xterm git -c core.hooksPath=hooks commit -m "server: share configuration and statistics maintenance ownership"
    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

The actual pre-commit hook must run the locked server build too. No Go/Bazel
inputs are planned, so bazel_prepare and Go failpoint setup are unnecessary.
Record exact outcomes, unexpected failures, source boundary and unverified live
TiKV/benchmark surfaces in a receipt before publication.

## Idempotence and Recovery


Do not force-push or discard other edits. Test temporary files are owned and
removed by their fixtures. Maintenance shutdown must wake long timers and join
active work. Zero/negative stats leases must not start this worker. Preserve
Go's GC version delay and current schema snapshot; never substitute storage GC.

## Surprises & Discoveries


The unused shared override pass already handles status, socket, temp-dir, log
and other values that NodeConfig either rejects in TOML or silently discards
from CLI. Existing startup defaults also enable automatic TLS unlike Go.

The first real-process smoke exposed empty instance-memory strings: Go leaves
the existing vardef defaults in place for these values. Using those shared
defaults repaired startup, and the smoke subsequently passed. The auto-analyze
worker also captured the obsolete configuration flag instead of reading the
live process switch; its new regression failed before the repair and passed
afterward. Initial statistics readiness previously opened inside the read
callback, before cache publication; all consumers now share the existing gate
opened after publication.

## Decision Log


- Decision: Share existing owners instead of extending the whitelist or adding
  a statistics cleanup implementation. Rationale: these are composition gaps,
  and the user explicitly requests batches and removal of duplicate ownership.
  Date: 2026-10-02.
- Decision: Close O07 but keep N03 partial. The duplicate authority is removed,
  but native deployment defaults and missing consumers such as A03's TLS policy
  prevent complete startup parity. This batch does not invent those consumers
  or claim whole Go packages accepted. Date: 2026-10-02.

## Outcomes & Retrospective


O07 is repaired and N03 advances to partial, leaving 66 unresolved findings
(57 open, nine partial), 20 repaired, 86 tracked. No complete package is accepted.
The [repair receipt](parity/current-audit/config-statistics-maintenance-repair.md)
records files, Go source pins, before/after evidence, exact checks and limits.
Live multi-node TiKV, full original Go suites and benchmark performance remain
unverified. Code publication passed the actual hook and fresh pre-push build
gates; the repair receipt records the verified remote commit.

## Interfaces and Dependencies


Retain SourceConfig as the effective configuration value and existing
ClusterHistoricalStatsHandle/SharedStats as maintenance capabilities. The
maintenance guard owns cancellation and joined completion, including failed
startup. Do not change client-rust or protocol dependencies in this batch.

Plan update (2026-10-02): recorded shared instance-default and live-switch repairs,
completed validation, and retained N03 as partial to preserve its original
complete-consumer criterion.

Publication update (2026-10-02): the first push met a concurrent schema-acknowledgement
commit `11d1727fea`. Rebased without conflicts; 17 combined schema/statistics tests
and lint passed. The register count is unchanged by that upstream commit.

Publication outcome (2026-10-02): code commit `f8b02a07d1826285c96907e074c9cd6b88eeffca`
is on `origin/hparser-integration` with exact remote verification. The publication
receipt also uses the actual hook and a fresh locked build before its push.
