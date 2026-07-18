# ALTER TABLE partition maintenance

## Purpose

Port Go's structurally distinct partition-maintenance `AlterTableSpec` forms
without turning the trailing grammar into an accepted opaque suffix. The seed
executor has no partition catalog or physical layout, so every newly parsed
operation must reject before its implicit DDL transaction boundary.

## Source boundary

- `pkg/parser/ddl_alter_handlers.go:parseAlterReorganize` owns
  `REORGANIZE PARTITION [NO_WRITE_TO_BINLOG] names INTO (definitions)` and
  Go's bare all-partitions form.
- `pkg/parser/ddl_alter_handlers.go:parseAlterPartitionAction` owns
  `COALESCE`, `TRUNCATE`, `REMOVE PARTITIONING`, and
  `REBUILD|OPTIMIZE|REPAIR PARTITION` list/all payloads.
- `pkg/parser/ast/ddl.go:AlterTableSpec.Restore` establishes canonical
  spelling, name-list punctuation, and omitted-vs-explicit payload behavior.

## Progress

- [x] Add separate typed AST actions for reorganization, coalescing,
  truncation, removing partitioning, and rebuild/optimize/repair.
- [x] Reuse typed `PartitionDefinition` rather than duplicate or raw-text
  replacement definitions.
- [x] Route every action through executor preflight rejection before a DDL
  transaction/catalog mutation.
- [x] Add parser restoration and executor transaction-boundary tests.
- [x] Add a checked 116-row Go-oracle selector for this exact family.
- [x] Extract the partition AST payload/restore and executor capability
  boundary without changing restore bytes or executor result. The executor
  has one pre-mutation unsupported classifier.
- [x] Route `parse_alter_table` through the parser partition module's
  non-consuming `Option<AlterPartitionAction>` entry point. Definition/value
  mechanics and token recognition now share that module.
- [x] Collapse the outer AST to `AlterTableAction::Partition` and keep all
  partition restore payloads in `ddl_partition.rs`; executor preflight and
  direct `alter_table` calls delegate to the same capability classifier.

## Validation

Focused WIP checks passed:

```text
cargo fmt --all -- --check
cargo test -j 12 -p tidb-parser --lib -q
cargo test -j 12 -p tidb-exec --lib -q
cargo test -j 12 -p difftest --test alter_partition_maintenance_selector -q
cargo clippy -j 12 -p tidb-ast -p tidb-parser -p tidb-exec --all-targets -- -D warnings
```

The ownership split additionally passed:

```text
cargo fmt --all -- --check
cargo test -j 12 -p tidb-ast -p tidb-parser --lib -q
cargo test -j 12 -p tidb-exec --lib partition_maintenance -q
cargo test -j 12 -p tidb-exec --lib add_partition_is_unsupported_before_ddl_transaction_mutation -q
cargo test -j 12 -p difftest --test selector_alter alter_partition_maintenance_lexical_one_statement_matches_go -q
cargo test -j 12 -p difftest --test integration_parser_diff integration_parser_static_go_oracle_reports_rust_outcomes -q -- --exact
```

The static parser oracle moves from 49,007 to 49,123 exact matches, with
parse failures falling from 1,678 to 1,562. Root owns the merged snapshot
update and full-workspace integration gate.

The ownership-only follow-up preserves that checked snapshot.
