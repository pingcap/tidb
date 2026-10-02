# Original-schema column preparation and shared ALTER admission

Reviewed 2026-10-02 from integration
`a49cc7550206f62eb7c4b09c0cba185deace8152` against freshly fetched Go master
`93a01d31f6da205ae4bf376825293903a6899fdb`. Native client-rust and the pinned
dependency remain current at `19a56ccda1e128218cd33c69709038219aced9bc`.
Both implementation repositories were pulled before work. This is maintenance
of existing Rust owners, not acceptance or partial transcreation of Go's DDL
package.

## Source contract and repair

Go `pkg/ddl/executor.go::alterTable` builds column jobs against the original
table. `AddColumn`, `DropColumn`, `ModifyColumn`, `ChangeColumn`, `RenameColumn`
and `AlterColumn` resolve their sources and targets before worker execution.
Only admitted jobs reach `multi_schema_change.go::fillMultiSchemaInfo` and
`checkMultiSchemaInfo`; conditional no-ops emit notes without entering conflicts.
The MODIFY builder's read-only NULL precheck remains part of admission.

Rust now prepares all six existing column spellings through the same lifecycle
as ordinary index actions. Definitions and existing column IDs are retained;
conditional no-ops are excluded from conflicts. Column and index diagnostics
share one ordered owner. Successful admission publishes notes before combination
checks and row/index execution; known admission errors suppress earlier column
rewrites and index backfills. Original and combined column-count checks run at
their respective Go boundaries. Other action families still have their existing
admission paths and remain an explicit gap.

Execution resolves stable IDs and current positions. Go
`column.go::LocateOffsetToMove` resolves AFTER on the worker's current schema;
retaining only an admission-time offset would move the wrong column after a
sibling action. The existing table/storage/allocator owners apply these changes.
ALTER DEFAULT retains its separate worker-context validation. AUTO_RANDOM layout
compatibility uses one pure validator for admission and staged execution, with
allocator effects deferred through the existing shared owner.

That metadata review exposed a second root cause: ADD, DROP and MOVE adjusted
primary-key, auto-increment and index offsets but omitted AUTO_RANDOM's offset.
All three permutations now carry that projection with the column identity. The
regression failed with offset zero instead of one and now also verifies inserts.

Removed: immediate column action dispatch, mixed admission/mutation helper
bodies, AST-only column conflict collection and the separate index-note variant.
Warning isolation reuses the existing statement-context mechanism used by
statistics replay; local warning draining preserves shared coprocessor sinks.

## Files and validation

Production changes are in executor `ddl.rs`, `ddl/alter_table.rs`,
`ddl/alter_metadata.rs`, `ddl/index_changes.rs`, new `ddl/column_changes.rs`,
`kv_table.rs`, `kv_table/auto_random.rs` and `stmt_context.rs`. The statistics
caller in `driver/planner_bridge.rs` follows the shared method rename. Tests are
in executor `tests_ddl_multi_schema_change_sql.rs`,
`tests/db_integration_ddl_types_source.rs` and session `tests_alter_column.rs`.
The [ExecPlan](../../alter-column-preparation-execplan.md) and D11 register retain
the implementation boundary. The prior index ExecPlan also records its completed
publication.

Nine new regressions failed before their respective repairs and pass afterward;
a tenth new test controls successful sibling positions and generated dependencies.
Go's original `TestIssue5092` also exposed a wrong Rust expectation: an already
existing unguarded duplicate returns 1060, while conflicting new sibling names
return 8200. The translated test now follows the original sequence and drop
preconditions. Its corrected expectation fails on unchanged Rust production.
[Validation data](alter-column-preparation-validation.json) retains exact commands,
results and compact before/after evidence.

From `rust/`, the final results for each distinct scope are:

| Command | Result |
| --- | --- |
| `cargo test --locked -p tidb-executor --lib tests_ddl_ -- --nocapture` | 65 pass |
| `cargo test --locked -p tidb-session --lib tests_alter_column -- --nocapture` | 18 pass |
| `cargo test --locked -p tidb-session --lib tests_column_defaults -- --nocapture` | 33 pass, one baseline failure |
| `cargo test --locked -p tidb-session --lib tests_generated_columns -- --nocapture` | 25 pass |
| `cargo test --locked -p tidb-session --lib tests_modify_column_null -- --nocapture` | Two pass |
| `cargo test --locked -p tidb-session --lib tests_foreign_key -- --nocapture` | 56 pass, one baseline failure |
| `cargo test --locked -p tidb-executor --test all serial_auto_random_source:: -- --nocapture` | Eight pass, three ignored |
| `cargo check --locked -p tidb-executor -p tidb-exec -p tidb-session -p tidb-server --all-targets` | Pass |
| `cargo test --locked -p tidb-executor --lib stmt_context::tests -- --nocapture` | 19 pass |
| `cargo test --locked -p tidb-session --lib tests_core::ddl:: -- --nocapture` | 32 pass |
| `cargo test --locked -p tidb-executor --test all db_integration_ddl_types_source:: -- --nocapture` | 30 pass, three ignored |
| `cargo test --locked -p tidb-executor --test all serial_column_flags_and_limits_source:: -- --nocapture` | Five pass, two ignored |

Totals: **293 pass, two baseline failures, eight existing ignores**. These scopes
cover column callers, defaults, generated expressions, foreign-key interactions,
positions, identity, warning delivery and source-derived DDL controls without
claiming distributed worker coverage.

Both failures reproduce after restoring every changed tracked production file
to the starting HEAD (then restoring the edited bytes):

- `tests_column_defaults::a_folded_default_stays_a_settled_literal` rejects the
  CREATE TABLE expression `DEFAULT (1+1)` in the parser before ALTER is reached.
- `tests_foreign_key::a_rejected_add_leaves_nothing_behind_but_still_consumes_its_name`
  expects `fk_2`, while the resulting catalog contains `fk_1`.

Neither failure nor the eight ignored distributed/region/DDL-state cases is
counted as passing. From the repository root, `GOTOOLCHAIN=go1.25.14 make lint`
and `git diff --check` pass. No Go, Bazel, generated or dependency inputs change,
so `make bazel_prepare` is not required.

Publication gates:

```sh
TERM=xterm git -c core.hooksPath=hooks commit -m "ddl: prepare column actions before ALTER execution"
(cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration
```

The actual pre-commit hook passed the locked server build in 18.75 seconds.
This receipt amendment repeats the same hook. The final publication command
then reruns the locked build before pushing; the thread records the final
remote-SHA and clean-checkout verification.

## Remaining boundary and risks

D11 remains partial. Foreign-key, CHECK, table-option, partition and table-rename
admission still need migration; full durable job preparation, execution, schema
phases, rollback and recovery remain unresolved. The existing refusal to drop
columns on foreign-key-participating tables remains a known restriction. Mixed
action families do not yet establish Go's complete admission/error-order contract.
The register remains **71 unresolved (63 open, eight partial), 15 repaired,
86 tracked**; other IDs retain their previous full-review evidence.

The compatibility changes correct observable diagnostics and column metadata.
Metadata preparation adds work before execution but reuses existing builders and
avoids row/index work when admission fails. No measured performance improvement
is claimed. No live Go SQL oracle, mixed-node cluster, crash/recovery test,
complete upstream package suite or sysbench/TPC-C/TPC-H/YCSB run was performed.
