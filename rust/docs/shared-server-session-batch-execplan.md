# Remove table-count SQL engines and share the server session owner

## Purpose / Big Picture


This living ExecPlan follows PLANS.md. The user requests batches that solve
multiple remaining structural findings by their shared cause, following TiDB Go
master and removing displaced Rust implementations. This batch addresses S01
and S02 together: both storage engines must use the ordinary session/planner/
executor and shared catalog lifecycle, regardless of table count. A second
schema reloader for the bounded two-table engine would preserve the wrong owner.

This is maintenance and removal of duplicate runtime ownership, not acceptance
of the complete cmd/tidb-server, session, planner or executor packages. T01's
generic buffer assertion policy has additional system-row callers and remains
a separate obligation after its configured-server consumer disappears.

## Progress


- [x] Refresh integration/master and trace configured and ordinary server callers.
- [x] Identify S01 and S02 as one competing startup/session owner.
- [x] Add failing startup regressions and preserve shared-session behavioral tests.
- [x] Remove the bounded factories/dispatch and table-count routing for both stores.
- [x] Migrate configuration, runtime tests and active smoke scripts; retire obsolete private-telemetry campaigns with a coverage inventory.
- [x] Verify startup, SQL/protocol, schema refresh and shutdown; root lint and all-target checking pass.
- [ ] Run the actual hook build and fresh pre-push locked server build.
- [x] Update both findings and record validation/retired-artifact coverage.
- [ ] Publish the batch to hparser-integration after the locked build gates.

## Context and Orientation


Start at integration 44be2a9d756cc6e3003ab87ce0fd38a5d5e6348f and Go master
93a01d31f6da205ae4bf376825293903a6899fdb. The register has 69 unresolved IDs.
Go cmd/tidb-server/main.go chooses a storage driver, then uses BootstrapSession,
Domain and server/driver_tidb.go's session for every table count. No configured
one/two-table SQL engine exists in Go.

At the starting baseline, rust/crates/tidb-server/src/lib.rs::run_configured_node selects either
cluster_session_node or RealTiKvSessionFactory/RealTiKvMultiSessionFactory.
real_tikv_node/mod.rs mixes that bounded SQL engine with shared bootstrap,
reloader and shutdown helpers. real_tikv_multi_node.rs owns a second dispatcher.
unistore_node.rs likewise has both bounded and complete-catalog boot paths.
NodeConfig carries read_tables, load_tables and cluster_session selectors.
The complete-catalog path already serves ordinary Session through ClusterSessionFactory
and shares SchemaLeaseValidator/catalog/statistics reloads across connections.

## Plan of Work and Milestones


First prove that normal storage/auth configuration cannot boot without selecting
a bounded table or special cluster-session mode. Add tests that normal TiKV and
unistore configuration admit the shared owner and reject removed static-table
options. Preserve real SQL coverage by migrating the bounded activation fixture
to the existing shared unistore stack with SQL CREATE TABLE.

Next remove table-count selection from the root and direct entrypoints. Delete
the two bounded server factories, dispatcher implementations and their private
time-zone/query wrappers once their callers are gone. Preserve reusable
catalog/watch/account/sysvar/process-shutdown helpers in real_tikv_node.
Unistore retains its existing transport and single storage capability stack.

Remove static descriptor configuration and its parsing/validation. Existing
cluster-session invocations may remain a compatibility spelling for the sole
session path, but no boolean selector or alternative engine survives. Migrate
smoke runners that already create their schema on Go to use that stored catalog.
Update obsolete mode-specific tests; retain Go behavioral regressions and make
any intentionally retired synthetic-only tests explicit in the receipt.

Finally run config, protocol/session, shared schema/DDL refresh and lifecycle
tests, inspect every remaining caller, and check affected Cargo targets. Update
the architecture map if module ownership changes. Run root make lint and both
locked server build gates. Record exact commands, failures and unverified live
surfaces without claiming whole-package transcreation.

## Concrete Steps and Acceptance


From rust/, run scoped targets after checking their current names:

    cargo test --locked -p tidb-server --test all node_config_source:: -- --nocapture
    cargo test --locked -p tidb-server --lib real_tikv_node:: -- --nocapture
    cargo test --locked -p tidb-server --lib cluster_session_node::schema_sync::tests -- --nocapture
    cargo test --locked -p tidb-server --lib activation -- --nocapture
    cargo check --locked -p tidb-server --all-targets

Run protocol and schema-refresh tests affected by the migrated callers; preserve
exact commands and outputs in the batch receipt. Use bash -n on changed scripts.
From the repository root run:

    GOTOOLCHAIN=go1.25.14 make lint
    git diff --check
    TERM=xterm git -c core.hooksPath=hooks commit -m "server: share one session lifecycle across storage engines"
    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

No Go/Bazel/generated inputs are planned, so bazel_prepare is unnecessary.
Acceptance requires no production table-count dispatch, no bounded server
factory, successful ordinary prepared/DML SQL on the shared store adapter,
shared schema refresh independent of table count, and joined shutdown.

## Idempotence and Recovery


Preserve user edits and do not force-push. Remove code only after enumerating
its callers; compile all targets to catch development/test consumers. Do not
silently reinterpret static table IDs as durable SQL schema. Obsolete static
options must fail explicitly; create schema through ordinary SQL instead.

## Surprises & Discoveries


Only the single-table loaded route starts catalog/statistics reloaders. The
two-table route explicitly drops its loaded catalog snapshot. The shared path
already does the necessary reloading, making a second implementation unnecessary.
T01 also affects system-row encoders and lower-level fixtures, so deleting the
bounded SQL server alone cannot close that finding.

Two existing protocol fixtures did not model Go's handshake collation
initialization: one panicked on SET NAMES, and another expected only client SQL.
Their explicit expectations now include that initialization; the 48-test
startup/concurrency/wire group passes. Serial startup checks avoid the unrelated
process-global resource race. Run tests from rust/ to load its debug stack setting.

The private campaign harness cannot be retained as an active gate after its
planner telemetry disappears. It, six dependent runners and three older
read-only/multicolumn/concurrent-auth campaigns are retired; active
SQL/catalog/transaction runners migrate to ordinary execution. The combined
network leader-transfer/blocked-shutdown campaign remains unverified, explicitly
recorded in the receipt, rather than being relabeled as a passing shared test.

## Decision Log


- Decision: Solve S01 and S02 through complete removal of the alternate server
  engines and migration to the already-wired shared owner.
  Rationale: Adding transaction-mode inputs or another schema reloader to the
  configured pipeline would invest in an owner Go does not have.
  Date: 2026-10-02.

## Outcomes & Retrospective


S01 and S02 now use one production session/catalog owner for both storage engines.
97 distinct targeted tests pass, plus an actual unistore server exercised by a
stock MySQL client and joined SIGTERM shutdown. All-target checking, script checks
and root lint pass. The register has 67 unresolved findings (59 open, eight
partial), 19 repaired, 86 tracked. No complete Go package or benchmark acceptance
is claimed. See parity/current-audit/shared-server-session-repair.md and its
validation JSON for exact commands, removed artifacts and remaining live gates.

## Interfaces and Dependencies


Production QuerySessionFactory remains ClusterSessionFactory for both storage
engines. Keep the existing shared storage capability and lifecycle helpers.
Do not add a new session, planner, transaction coordinator or external dependency.
