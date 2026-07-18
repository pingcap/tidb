# ALTER TABLE ADD PARTITION definitions

## Purpose

Translate Go's `AlterTableAddPartitions` AST branch structurally: the action
contains flags plus exactly one typed payload, either `PARTITIONS n` or a list
of `PartitionDefinition`s. Do not consume definitions as generic SQL text.

## Source boundary

- `pkg/parser/ddl_alter_handlers.go:parseAlterAdd` parses `ADD PARTITION`
  with optional `IF NOT EXISTS` and `NO_WRITE_TO_BINLOG`, then selects either
  numeric `Num` or `parsePartitionDef(0)` definitions.
- `pkg/parser/ddl_alter_parser.go:parsePartitionDef` owns the definition
  grammar. In its ALTER mode (`partType == 0`) method compatibility remains a
  DDL validation concern, while `VALUES LESS THAN`, `VALUES IN`, `DEFAULT`,
  `HISTORY`/`CURRENT`, and partition options are AST syntax.
- `pkg/parser/ast/ddl.go:AlterTableSpec.Restore` restores flags and the typed
  count-or-definition shape.

## Progress

- [x] Replace count-only `AddPartitions` with typed count/definition payload.
- [x] Port the source-backed definition core: LESS THAN including MAXVALUE,
  IN scalar/tuple/default values, standalone DEFAULT, HISTORY/CURRENT, and
  COMMENT/ENGINE/PLACEMENT POLICY options exercised by the static corpus.
- [x] Keep execution unsupported before the implicit DDL commit boundary.
- [x] Add a 95-row Go-oracle selector and execution transaction-boundary test.
- [ ] Port subpartition definitions and the remaining per-definition options
  as separate typed slices; they must not become an opaque suffix.

## Validation

Focused WIP checks passed:

```text
cargo test -j 12 -p difftest --test alter_add_partition_definition_selector -q
cargo test -j 12 -p tidb-parser --lib alter_table_add_partition_count -q
cargo test -j 12 -p tidb-exec --lib alter_table_add_partition_is_unsupported_before_ddl_transaction_mutation -q
cargo clippy -j 12 -p tidb-ast -p tidb-parser -p tidb-exec --all-targets -- -D warnings
```

The static parser oracle moved from 48,911 to 49,007 matches in the shared
worktree. Root owns the merged snapshot update and final whole-workspace gate.
