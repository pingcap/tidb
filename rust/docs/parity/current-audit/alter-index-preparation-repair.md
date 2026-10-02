# Original-schema index preparation and persistent metadata identity

Reviewed 2026-10-02 from integration
`5d9c7433a53c7211f0948a48bd876795103d39ae` against freshly fetched Go master
`93a01d31f6da205ae4bf376825293903a6899fdb`. Native client-rust and its dependency
remain current at `19a56ccda1e128218cd33c69709038219aced9bc`. Both implementation
repositories were pulled before work. This maintains existing local owners;
it does not accept or partially transcreate the complete Go DDL package.

## Source contract and repair

Go `pkg/ddl/executor.go::alterTable` resolves grouped specifications and builds
jobs against the original table before submitting them. `checkIndexNameAndColumns`
resolves anonymous names and excludes conditional duplicate no-ops;
`ValidateRenameIndex` checks the source, exact-spelling no-op and target in that
order. `validateAlterIndexVisibility` excludes unchanged single-statement jobs
but retains them in multi-schema jobs. `fillMultiSchemaInfo` consumes admitted
jobs, including their resolved names. `renameHiddenColumns` updates expression
column names without changing their IDs or index keys.

The Rust local ALTER path now uses typed prepared changes for existing ordinary
and unique index ADD, DROP, RENAME and visibility actions. One pure metadata
builder serves ADD admission and execution; DROP shares original lookup and
execution with standalone DROP INDEX. Conditional notes are delivered in source
order. A known index admission error prevents earlier index backfill, while an
earlier non-index diagnostic still wins. The conflict checker consumes these
prepared operations. Grouped specifications enter this same lifecycle.

Go's multi-schema workers rename a covering index before the column-drop phase
retires it. A local staged image can already have retired that index; the prepared
operation therefore refers to its original identity. Missing original sources
still reject. This repairs drop-column plus rename without treating arbitrary
missing names as no-ops.

That identity review exposed an underlying metadata bug. KvTable computed IDs
from surviving definitions, reusing IDs after drops. A new column could read the
retired column's bytes instead of its default; a prepared rename could target a
new sibling index. Go `column.go::AllocateColumnID` and
`index.go::AllocateIndexID` use retained TableInfo counters. KvTable now retains
those counters across removals, updates them when installing definitions and
copies them with CREATE LIKE, as `create_table.go::BuildTableInfoWithLike` does.
The cluster session loader and all three temporary TableInfo projections carry
the persisted high-water values rather than inferring them from survivors.

Removed: immediate rename validation/mutation, the separate ADD action adapter,
duplicate grouped execution, AST-only index validation, guessed anonymous names
in conflict collection, and survivor-based ID allocation. Existing catalog,
backfill, storage and allocator owners remain in use.

## Files and validation

Production changes are in executor `ddl.rs`, `ddl/alter_table.rs`,
`ddl/alter_metadata.rs`, `ddl/indexes.rs`, new `ddl/index_changes.rs` and
`kv_table.rs`; metadata projections are in exec `cluster_ddl.rs`,
`mview_build_engine.rs` and server `cluster_session.rs`. Regression coverage is
in executor `tests_ddl_multi_schema_change_sql.rs`, session `tests_alter_column.rs`
and the cluster-session tests. The living plan is
[alter-index-preparation-execplan.md](../../alter-index-preparation-execplan.md).

Fourteen new regressions fail before their respective fixes and pass afterward.
Two former assertions recording mismatches now require Go's behavior. Controls
cover original-schema source/target errors, resolved anonymous names, conditional
no-ops, error/note order, grouped specs, expression metadata, backfill ordering,
case-only renames, retired identities, persisted counters and old row bytes.
[Validation data](alter-index-preparation-validation.json) retains commands,
results and compact failure evidence.

From `rust/`:

```sh
cargo test --locked -p tidb-executor --lib tests_ddl_ -- --nocapture
cargo test --locked -p tidb-session --lib tests_alter_column -- --nocapture
cargo test --locked -p tidb-session --lib tests_expression_indexes -- --nocapture
cargo test --locked -p tidb-session --lib tests_index_key_length -- --nocapture
cargo test --locked -p tidb-session --lib tests_foreign_key -- --nocapture
cargo test --locked -p tidb-server cluster_session::tests -- --nocapture
cargo test --locked -p tidb-executor --lib kv_table::tests -- --nocapture
cargo test --locked -p tidb-executor --test all serial_create_table_like_source:: -- --nocapture
cargo test --locked -p tidb-executor --test all index_change_add_drop_lifecycle_source:: -- --nocapture
cargo check --locked -p tidb-executor -p tidb-exec -p tidb-session -p tidb-server --all-targets
```

Results: 220 pass, one fails and five remain ignored. The failure is
`tests_foreign_key::a_rejected_add_leaves_nothing_behind_but_still_consumes_its_name`:
it expects `fk_2`, while the catalog contains `fk_1`. Restoring all four then-changed
DDL production files from the starting HEAD reproduces the identical failure;
the fixed files were restored afterward. This pre-existing expectation is not
counted as passing. The five ignores concern region splitting and distributed
index job/state behavior; they do not establish those contracts. All-target
checking passes.

From the repository root, `GOTOOLCHAIN=go1.25.14 make lint` and `git diff --check`
pass. No Go, Bazel, generated or dependency inputs change, so `make bazel_prepare`
is not required. Publication requires the actual hook and a fresh locked server
build from the final commit immediately before push:

```sh
TERM=xterm git -c core.hooksPath=hooks commit -m "ddl: prepare index changes against the original schema"
(cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration
```

The actual pre-commit hook passed the locked server build in 17.47 seconds.
This receipt amendment repeats the same hook. The final publication command
then reruns the locked build before pushing; the thread records the final
remote-SHA and clean-checkout verification.

## Remaining boundary and risks

D11 remains partial: non-index action builders still perform their existing
admission during staged traversal. General durable job submission, schema phases,
merged index backfill, rollback, retry and recovery remain separate unresolved
owners. This repair does not establish full Go index semantics or distributed DDL
acceptance. The register remains **71 unresolved (63 open, eight partial),
15 repaired, 86 tracked**; other IDs retain the latest full-review evidence.

The metadata change prevents identity reuse and follows Go's compatibility
contract. The shared builder runs once for admission and again for execution,
as Go does; next-ID lookup becomes constant time. Neither observation is a
measured workload speedup. No live Go SQL oracle, mixed-node cluster,
crash/recovery, complete upstream package suite or sysbench/TPC-C/TPC-H/YCSB run
was performed in this continuation.
