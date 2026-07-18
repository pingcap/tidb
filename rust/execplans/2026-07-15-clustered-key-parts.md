# Clustered Key Parts: Source-First Table Constraint Transition

## Goal

Port the Go parser/AST boundary for table-level PRIMARY/UNIQUE/ordinary
INDEX key parts without accepting a syntactic form and erasing its meaning.
The bounded source family is clustered-index DDL with prefix parts from
`tests/integrationtest/t/clustered_index.test` and
`tests/integrationtest/t/session/clustered_index.test`.

## Source contract

`pkg/parser/ddl_index_parser.go:parseConstraint` sends PRIMARY, UNIQUE, and
ordinary INDEX through `parseIndexPartSpecifications`. Each Go
`ast.IndexPartSpecification` holds either a column plus `Length`/`Desc`, or
an expression. `pkg/parser/ast/model.go:PrimaryKeyType` separately retains
DEFAULT/CLUSTERED/NONCLUSTERED; `Constraint.Restore` emits the layout after
the key parts.

Therefore a Rust `TableKeyConstraint { columns: Vec<String> }` was the wrong
shape: it made prefix/direction an accidental CREATE TABLE parse error even
though standalone indexes had already modelled them as `IndexPart`.

## Design

- Make `TableKeyConstraint` own `parts: Vec<IndexPart>`.
- Route table PRIMARY, UNIQUE, and ordinary INDEX through one
  `parse_index_parts` helper; preserve prefix length, ASC canonicalization,
  DESC, and functional parts structurally.
- Retain `PrimaryKeyStorage` as the independent CLUSTERED/NONCLUSTERED
  physical-layout payload.
- Keep execution honest: the seed row store has whole-column equality only,
  so CREATE/ALTER PRIMARY/UNIQUE keys with prefix/direction/function parts
  return `Unsupported` before the DDL implicit-commit boundary. Ordinary
  index metadata already carries prefix/direction.

## Progress

- [x] Read Go parser, AST, and source fixture contracts.
- [x] Replace lossy table-key columns with shared `IndexPart` values.
- [x] Add parser/restore and upstream-source selector coverage.
- [x] Add executor transaction-boundary regression coverage.
- [x] Run focused and package validation.
- [x] Run full static parser-oracle review; root owns snapshot update.

## Static-oracle outcome review

The reviewed run reports 48,911 exact matches, 1,774 parser failures, and
867 restore mismatches. Relative to the preceding root snapshot, this is
`+282` exact matches, `-316` parser failures, and `+34` visible mismatches.
Those 34 are not counted as wins: they are statements that now traverse the
prefix-key boundary but still contain a separate unported restore feature.
For example `tests/integrationtest/t/cte.test:9` now retains
`UNIQUE KEY idx_3(col_9(3), col_8)` and remains a mismatch because its
partition definition has no Rust restore model. The selector explicitly
pins one exact source family and this mismatch boundary.

## Validation

```bash
cargo fmt --all -- --check
cargo test -j 12 -p tidb-parser --lib -q
cargo test -j 12 -p tidb-exec --lib -q
cargo test -j 12 -p difftest --test clustered_index_parts_selector -q
cargo test -j 12 -p difftest --test integration_parser_diff integration_parser_static_go_oracle_reports_rust_outcomes -q -- --exact
```

The static-oracle command intentionally fails until root changes its checked
snapshot to the reviewed counts; that failure is evidence of the accounting
delta, not a green validation result.
