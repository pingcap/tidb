# ALTER TABLE ordered multi-spec statements

This ExecPlan is a living document. The `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` sections must be updated as the
work proceeds.

## Purpose / Big Picture

TiDB's Go parser represents `ALTER TABLE` as one table name followed by an
ordered slice of `AlterTableSpec` values. The Rust seed currently collapses the
statement to one `AlterTableAction`, so valid statements such as
`ALTER TABLE t ADD COLUMN b INT, DROP COLUMN a` fail before the AST can preserve
their shape. This increment ports that source structure directly: Rust stores an
ordered vector, parses the already-typed action families repeatedly, and lets the
statement own Go's separators during restore.

The parser/restore surface may faithfully represent multiple specs before the
seed executor can apply them atomically. Therefore execution has a deliberately
narrow capability boundary: exactly one action already supported by the seed can
run. An empty or multi-action statement is rejected before catalog state,
transaction snapshots, or savepoints change. The observable result is parser
fidelity without partial DDL execution.

## Progress

- [x] (2026-07-15) Read `PLANS.md`, Go `parseAlterTableStmt`, Go
  `AlterTableStmt`/`AlterTableSpec.Restore`, and the closest parser/AST tests.
- [x] (2026-07-15) Inventory the Rust AST, parser, partition envelope, executor
  dispatch, and transaction-boundary tests.
- [x] (2026-07-15) Replace `AlterTableStmt.action` with ordered `actions` and make action
  restore fragments separator-free.
- [x] (2026-07-15) Extract one-action parsing and loop over comma-separated specs, including
  Go's terminal `REMOVE PARTITIONING` separator rule.
- [x] (2026-07-15) Centralize the executor capability decision and reject empty/multi-action
  statements before any mutation.
- [x] (2026-07-15) Add AST/parser ordering/restore tests, preserve single-action coverage,
  and add an executor no-partial-mutation regression.
- [x] (2026-07-15) Add one exact 52-row Go-oracle selector in the physically split parser-test
  package.
- [x] (2026-07-15) Run focused formatting, tests, selector/static evidence, and strict
  clippy with 12 jobs; record exact outcomes here.

## Surprises & Discoveries

- Observation: Go's ordinary specs are joined with `, `, but
  `AlterTableRemovePartitioning` is joined with a single space and is terminal.
  Evidence: `pkg/parser/ast/ddl.go:AlterTableStmt.Restore` and
  `pkg/parser/ddl_alter_parser.go:parseAlterTableStmt`.
- Observation: Go also space-joins `AlterTablePartition`, import-tablespace,
  and discard-tablespace specs. Those spec/action families are not typed in the
  current Rust seed, so this wave implements only the existing typed special
  case, `Partition(RemovePartitioning)`, rather than pretending to generalize
  the missing leaf grammars.
- Observation: the existing Rust partition envelope already has a typed
  `RemovePartitioning` action, so the special separator can be expressed without
  adding a compatibility action or raw SQL payload.
- Observation: some action grammars own commas internally (`DROP PARTITION`
  names, TiFlash labels, compact partition names, index parts). The outer loop
  must not steal those commas; each action parser remains responsible for its
  own payload, and only a comma left after a complete action is a spec separator.
- Observation: Go accepts `ALTER TABLE t` with an empty spec slice. Rust can
  preserve that source shape, but the seed executor must reject it honestly
  because it is not an executable alteration.
- Observation: the checked Go oracle contains exactly 52 one-statement rows
  whose multiple specs are all composed from the Rust seed's existing typed
  action envelopes. All 52 moved from parse failure to exact restore match;
  every other outcome category stayed fixed.
- Observation: payload-owned commas need one-token lookahead at two unbounded
  list boundaries. Partition names consume a comma only before another
  identifier, and TiFlash labels consume one only before another string. This
  leaves the outer spec separator untouched without heuristic token rollback.

## Decision Log

- Decision: use `AlterTableStmt { name, actions: Vec<AlterTableAction> }` and
  delete the singular field in one compile boundary.
  Rationale: this is the direct Go shape, makes ordering explicit, and avoids a
  transitional alias that future agents could keep using incorrectly.
  Date/Author: 2026-07-15, Codex parser wave 11.
- Decision: move leading whitespace/separators out of every action restore arm.
  Rationale: Go's `AlterTableStmt.Restore` owns spec joining; action-level
  separators cannot produce correct multi-spec bytes.
  Date/Author: 2026-07-15, Codex parser wave 11.
- Decision: keep `COMPACT` as a one-action Rust envelope but do not allow another
  spec after it.
  Rationale: Go parses compact into a separate statement after the common
  `ALTER TABLE name` prefix; its internal comma-separated partition list is not
  an `AlterTableSpec` sequence.
  Date/Author: 2026-07-15, Codex parser wave 11.
- Decision: execution selects one action through a single pure pre-mutation
  capability function; empty and multi-action vectors return `Unsupported`.
  Rationale: it makes prefix application structurally impossible and preserves
  active transaction state on rejection.
  Date/Author: 2026-07-15, Codex parser wave 11.

## Outcomes & Retrospective

The Rust AST now mirrors Go's ordered `AlterTableStmt.Specs` ownership shape,
using the existing typed `AlterTableAction` values as specs. Statement restore
owns separators, single-action bytes are unchanged, ordinary multi-spec rows
use `, `, and the currently ported terminal `REMOVE PARTITIONING` uses a space.
Go's other space-joined partition-definition/import/discard spec families
remain explicitly unported. The parser's
single-action decision tree is reusable and the statement loop composes it
without flattening partition, index, label, or column-domain payloads.

The executor has one pre-mutation capability selector. It accepts only one
already-executable action, rejects empty/multi vectors, and classifies the
existing static unsupported families before catalog or transaction mutation.
The regression proves two individually supported actions cannot partially
apply or consume an active transaction, and proves a pre-existing savepoint is
still usable with `ROLLBACK TO` after rejection.

The exact selector passes all 52 source-backed rows. Static replay changes only
`rust_matched` from 49,165 to 49,217 and `rust_parse_failure` from 1,520 to
1,468. Restore mismatches remain 867, Rust accepts of Go-rejected input remain
45, multi-statement accepts remain 0, and accepts of Go restore failures remain
1. Multi-spec rows containing unported leaf actions such as foreign keys,
primary-key actions, generated columns, and rename-index remain outside this
structural wave and are explicitly excluded by the selector.

## Context and Orientation

The Rust workspace is rooted at `rust/`. `crates/tidb-ast/src/ddl.rs` defines
`AlterTableStmt`, `AlterTableAction`, and canonical restore. Partition action
payloads and their restore live in `crates/tidb-ast/src/ddl_partition.rs`.
`crates/tidb-parser/src/ddl.rs` owns the common `ALTER TABLE` grammar and
`crates/tidb-parser/src/ddl_partition.rs` owns typed partition specs.
`crates/tidb-exec/src/database.rs` performs top-level DDL preflight and the
implicit-commit boundary, while `crates/tidb-exec/src/ddl.rs` applies supported
schema mutations. Parser tests live in `crates/tidb-parser/src/tests/ddl.rs` and
executor transaction-boundary tests in `crates/tidb-exec/src/tests/ddl.rs`.

The Go authority is `pkg/parser/ddl_alter_parser.go` for statement assembly and
`pkg/parser/ast/ddl.go` for `AlterTableStmt.Restore`. Closest source tests are in
`pkg/parser/parser_test.go` and `pkg/parser/ast/ddl_test.go`.

The differential-test package was physically split during this wave. New parser
selectors belong only under `rust/difftests/parser-tests/tests/selectors`; the
old `rust/difftests/tests/selectors` path must not be edited.

## Plan of Work

First, change the AST ownership boundary. Rename the field to `actions`, iterate
it in source order, write a leading space for the first action, `, ` for ordinary
later actions, and a space before `Partition(RemovePartitioning)`. Refactor the
partition and ordinary action restore helpers to emit an action fragment with no
leading separator. Empty vectors restore only the common statement prefix.

Second, extract the existing giant action decision tree into a helper that
parses exactly one typed action. The statement parser loops over that helper.
It accepts a comma only when another ordinary spec follows. A terminal remove-
partitioning action may follow a prior spec without a comma and ends the loop,
matching Go. Compact returns a one-element vector and cannot join this loop.
Internal action commas remain within their existing payload parsers.

Third, introduce a centralized executor capability function returning the sole
executable action or an `ExecError::Unsupported`. It rejects empty and multi-
action vectors and classifies all already-static unsupported action families.
Top-level DDL calls it before catalog-dependent validation and before clearing
`txn_snapshot`/`savepoints`; the mutation helper receives the selected action so
it cannot accidentally iterate or apply a prefix.

Fourth, update every singular-field assertion atomically. Add parser tests for
two ordinary actions, ordering, single action, empty actions, and terminal remove
partitioning restore. Add an executor test that opens a transaction, buffers a
row, attempts two individually supported actions, verifies the exact unsupported
result and active snapshot, rolls back, and proves neither schema action ran.

Finally, after package-local tests compile, add one exact selector in the new
parser-test package based on Go source rows, then run the static oracle to report
the controlled category delta without editing root-owned snapshots or counters.

## Concrete Steps

Run from `/Users/qiliu/projects/tidb/rust` unless noted:

1. Edit AST/parser/executor and unit tests with `apply_patch`, keeping the
   singular-to-vector API change in one boundary.
2. Run `cargo fmt --all -- --check` after formatting and focused package tests:
   `cargo test -j 12 -p tidb-ast -p tidb-parser --lib` and targeted
   `cargo test -j 12 -p tidb-exec --lib <alter-table-test-filter>`.
3. Run all relevant executor library tests if focused checks pass:
   `cargo test -j 12 -p tidb-exec --lib`.
4. Add and run the exact parser selector from the new parser-test package, then
   run the static parser oracle command discovered from that package's Cargo
   metadata.
5. Run
   `cargo clippy -j 12 -p tidb-ast -p tidb-parser -p tidb-exec --all-targets -- -D warnings`.

## Validation and Acceptance

Acceptance requires all of the following observable behavior:

- Parsing `ALTER TABLE t ADD COLUMN b INT, DROP COLUMN a` yields two ordered
  actions and restores as
  `ALTER TABLE \`t\` ADD COLUMN \`b\` INT, DROP COLUMN \`a\``.
- Existing one-action statements restore byte-for-byte as before.
- A prior ordinary action followed by `REMOVE PARTITIONING` restores with a
  space rather than a comma, matching Go.
- A multi-action statement containing two individually supported mutations
  returns `Unsupported("ALTER TABLE multiple actions")` while an active
  transaction remains active and neither mutation is visible.
- Parser/AST/executor focused tests pass, the exact selector matches Go, and
  strict all-target clippy is clean with 12 jobs.

This wave uses the WIP validation profile because root owns the merged workspace
snapshot, full integration gate, `make lint`, and Ready claim.

## Idempotence and Recovery

All edits are source changes and repeatable cargo checks. If the atomic API
rename fails to compile, use `rg 'AlterTableStmt \\{|\\.action\\b'` over the three
owned crates to find every stale constructor/match; do not reintroduce a singular
alias. If selector package metadata changes again, rediscover its test target
with `cargo metadata`/`rg` rather than writing to the removed path. Do not reset
or overwrite concurrent agents' files.

## Artifacts and Notes

Validation completed with these exact commands from `rust/`:

    cargo test -j 12 -p tidb-parser --lib alter_table_multi_specs_preserve_go_order_and_separators
    cargo test -j 12 -p tidb-exec --lib alter_table_multi_action_is_unsupported_without_applying_a_prefix
    cargo test -j 12 -p tidb-parser -p tidb-exec --lib
    cargo test -j 12 -p tidb-ast
    cargo test -j 12 -p difftest-parser-tests --test selector_alter
    cargo test -j 12 -p difftest-parser-tests --test integration_parser_diff integration_parser_static_go_oracle_reports_rust_outcomes -- --exact --nocapture
    cargo test -j 12 -p difftest-parser-tests --test integration_parser_queue -- --nocapture
    cargo clippy -j 12 -p tidb-ast -p tidb-parser -p tidb-exec --all-targets -- -D warnings
    cargo fmt --all -- --check

All commands above passed after root advanced the protected snapshot to the
reviewed counts. The static gate reports 49,217 exact matches and 1,468 raw
parse failures; the queue gate passes with 1,414 actionable parse failures.
Root owns that checked-in snapshot, aggregate counters, and `HANDOFF.md`; this
wave did not edit them.

## Interfaces and Dependencies

The intended public data shape is:

    pub struct AlterTableStmt {
        pub name: Vec<String>,
        pub actions: Vec<AlterTableAction>,
    }

The executor will expose a crate-private capability selector equivalent to:

    fn executable_alter_action(
        statement: &AlterTableStmt,
    ) -> Result<&AlterTableAction, ExecError>;

It has no I/O and performs no mutation. Catalog-dependent validation remains in
the top-level DDL branch but operates only on the selected single action before
the implicit-commit boundary.
