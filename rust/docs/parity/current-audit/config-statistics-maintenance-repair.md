# Effective configuration and statistics maintenance batch

Reviewed 2026-10-02 from integration `43e827e2c4c96a351efb164d4f1fb4295fbbb5b6`
against freshly fetched Go master `93a01d31f6da205ae4bf376825293903a6899fdb`.
The native dependency remains `19a56ccda1e128218cd33c69709038219aced9bc`.

**O07 is repaired. N03 is partial: its duplicate configuration authority and
auto-TLS default are repaired, but acceptance is not implementation of every
runtime consumer.** The register has 66 unresolved findings (57 open, nine
partial), 20 repaired, 86 tracked. Other findings retain their previous evidence.
This is maintenance of existing packages, not complete Go package acceptance.

## Shared configuration ownership

`tidb-config::config_tree::load::prepare_config` now supplies the same loading,
warning/strict/check policy, explicit override order and validation to both
the global initializer and executable. It returns a value without publishing
process globals, so the executable can first construct its runtime resources.
`NodeConfig` derives those resources from this single effective value. Removed:
the second TOML parse, leaf collector, supported-leaf whitelist, source-presence
tracker and hand-maintained file/CLI reconstruction. The existing shared
`main_flags::override_config` now participates in real startup.

File status settings, socket, advertise address, read engines and stats lease
reach their runtime fields. Startup JSON retains the same effective values;
the socket's `{Port}` expansion remains the executable step Go applies.
Explicit flags override file values, including temp-dir and logging settings.
`--config-check` validates without booting; unknown keys use Go's warning policy
unless strict/check mode requires rejection. Removed-variable diagnostics remain
with the shared loader instead of an unconditional native startup veto.

Auto-TLS now defaults false, matching Go. Explicit configuration and the native
disable alias remain. Go SQL/cluster certificate flags keep their Starter-only
scope and pair validation; native certificate aliases remain adapters. Empty
instance memory strings inherit the existing vardef constants, as Go's
setInstanceVar leaves the variable default in place.

Native deployment defaults still select loopback binding, TiKV and an explicit
PD path; the embedded auth-file adapter remains. They are documented residual
startup differences, not silently claimed as Go defaults. TLS CA/client identity,
minimum-version and certificate-reload enforcement still belongs to A03; other
missing consumers remain tracked. In particular, admitting `security.ssl-ca`
does not establish certificate-policy enforcement. These are why N03 is not
closed under its original complete-consumer criterion.

## Statistics lifetime and live policy

Both stores retain one `StatsMaintenanceWorker` outside the factory it serves.
For positive stats leases, it waits for the existing statistics initialization
gate, then uses Go Domain's intervals: GC every 100 leases, health every 20
leases, and memory/cache maintenance every 300ms. Only GC requires current
statistics ownership. Each pass uses the existing GC owner and schema snapshot,
retains the `10 * max(stats lease, schema lease)` version fence, and checks the
auto-analyze window even after an ordinary GC error. Errors do not retire the
worker. Zero and negative leases create no maintenance worker.

Histogram loading and maintenance share the existing initialization signal.
The signal now opens after cache publication, including skipped/failed initial
reads, instead of opening inside the read callback before publication.
Shutdown wakes initialization/timer waits, suppresses queued ticks, joins active
work, closes statistics ownership and releases capabilities before the storage
authority retires. The guard retains the factory outside the worker to avoid a
last-reference drop on the thread being joined. Zero/negative lease factory
retirement also closes its statistics owner.

The related auto-analyze worker no longer captures the obsolete config boolean;
each tick reads the live `vardef::RUN_AUTO_ANALYZE` switch, as Go does. Turning it
off closes the priority queue without restarting the server. Existing runtime
and priority-queue integration tests remain. No second statistics store, GC
algorithm, transaction implementation or cache admission backend was added.
MVCC GC (O03), min-active-TS reporting (O09) and LFU admission (C04) remain open.

## Files and source boundary

Production files:

- `rust/crates/tidb-config/src/config_tree/load.rs`: shared preparation.
- `rust/crates/tidb-server/src/node_config.rs`, `main_flags.rs`,
  `bin/tidb-server.rs`, `mysql_tls.rs`: executable projection, overrides,
  config-check result and corrected TLS-default documentation.
- `rust/crates/tidb-server/src/cluster_session_node/stats_maintenance.rs`,
  `mod.rs`, `boot.rs`, and `rust/crates/tidb-server/src/unistore_node.rs`:
  retained worker, both startup/shutdown paths and live auto-analyze switch.
- `rust/crates/tidb-exec/src/stats_watch.rs` and
  `rust/crates/tidb-server/src/real_tikv_node/schema_following.rs`: shared cache
  maintenance and initialization/publication ordering.
- `rust/crates/tidb-util/src/memory/{mod,process}.rs`: force-refresh the existing
  memory sample; ordinary readers retain their cached sampling behavior.

Regression/support files include server `tests/node_config_source.rs`,
`cluster_session_node/tests/unistore_cop.rs`, inline tests in the changed owners,
the living [ExecPlan](../../config-statistics-maintenance-batch-execplan.md),
this receipt/validation JSON, the two register formats and audit README.
`docs/agents/architecture-index.md` adds only an existing-policy navigation entry;
its new paths exist and were checked against the agents review guide.

Source reviewed against **origin/master**, not just the integration checkout:
`cmd/tidb-server/main.go` flag/override flow, `pkg/config/config.go` initialization,
`pkg/domain/domain.go::gcStatsWorker`/`autoAnalyzeWorker`,
`pkg/statistics/handle/storage/gc.go`, and `pkg/util/memory/memstats.go`.
The checkout has unrelated Go differences; the relevant master timer/ownership
and Starter TLS scope were explicitly compared. Parent packages retain all
original source, build/platform/generated, fixture and original-test obligations
in the existing inventory. No Go source/test/generated/dependency artifact was
changed or deleted. No whole-package transcreation or workload result is claimed.

## Validation

Four behavioral regressions failed before their repairs: recognized TOML was
rejected by the whitelist; default auto-TLS was true; elected periodic GC never
wrote its watermark; and a live auto-analyze switch change did not close the
worker's priority queue. Each passes after repair. One initial GC test was blocked
by sandboxed `sysctl hw.memsize`; rerunning with host memory discovery available
reproduced the intended missing-worker failure.

139 distinct targeted Rust tests pass. Commands below ran from `rust/`, except
the explicitly marked repository-root command. Logs and individual test names
are summarized in [validation JSON](config-statistics-maintenance-validation.json).

    cargo test --locked -p tidb-server --lib -- stats_gc stats_maintenance node_config:: main_flags:: real_tikv_node::schema_following:: --test-threads=1
    cargo test --locked -p tidb-server --test all -- node_config_source:: mysql_tls_source:: mysql_client_lifecycle_source:: concurrent_mysql_sessions_source:: --test-threads=1
    cargo test --locked -p tidb-exec --lib stats_watch::tests -- --test-threads=1
    cargo test --locked -p tidb-config --lib config_tree::load:: -- --test-threads=1
    cargo test --locked -p tidb-server --lib -- auto_analyze_worker auto_analyze_priority_queue_uses_shared_stats_ddl_and_ordinary_analyze_path --test-threads=1
    cargo test --locked -p tidb-util --lib memory::process::tests -- --test-threads=1
    cargo test --locked -p tidb-server --lib -- schema_sync:: stats_gc stats_maintenance --test-threads=1
    cargo check --locked -p tidb-server --all-targets
    cargo build --locked -p tidb-server

The final instance-default correction also passed this repository-root command
(15 tests, already included in the 139 distinct count):

    cargo test --locked -p tidb-server --manifest-path rust/Cargo.toml --lib node_config:: -- --test-threads=1

The stock MySQL client process test starts a temporary real unistore server with
TOML store/status/stats-lease values and default plaintext behavior, creates and
analyzes a table, drops its index, ages only the test stats version, and observes
the histogram count change from 1 to 0 with a persisted GC watermark, without a
manual GC call. SIGTERM returns zero with no forced kill; its temporary auth file
is removed. Full command, TOML, SQL and output are retained in the validation JSON.
The first smoke caught an empty instance-memory projection; the corrected smoke
passed using shared vardef defaults. Two existing error-shape fixtures were
updated for shared config validation/native argument error identity.

Repository root gates:

    GOTOOLCHAIN=go1.25.14 make lint
    git diff --check

No Go/Bazel changes require bazel_prepare or Go failpoint setup. Full-workspace
tests, original Go suites, multi-node TiKV elections/faults and live TLS CA
enforcement were not run. No sysbench/TPC-C/TPC-H/YCSB performance was measured.
Existing compiler/linker and jemalloc-environment warnings are not represented
as clean-warning builds. Publication uses the actual hook locked server build,
then a fresh locked server build immediately before each push.

## Publication

The first commit passed the actual hook and fresh pre-push locked build, but
the push was rejected because the branch advanced to `11d1727fea563da7d64e9ee42f7f6c8e470d84f0`.
That non-overlapping schema acknowledgement change was inspected and rebased
without conflict. Its schema-sync tests and this batch's statistics lifecycle
tests pass together (17 tests; eight additional distinct tests), as does lint.
The amended code commit **`f8b02a07d1826285c96907e074c9cd6b88eeffca`** is pushed to
`origin/hparser-integration`; `git ls-remote origin refs/heads/hparser-integration`
returned that exact SHA. The actual pre-commit hook ran
`cd rust && cargo build --locked -p tidb-server` successfully, and the same
locked build passed again immediately before the successful normal push. No
force-push was used. This publication receipt follows the same hook and fresh
pre-push build requirements; the task thread records its final remote SHA.
