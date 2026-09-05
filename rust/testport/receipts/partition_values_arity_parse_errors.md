# Partition VALUES arity: parse-time coded errors, not deferred DDL checks

## Divergence

`a424260ce5` ("pkg/ddl: align partition column-list arity errors") relaxed the
Rust parser's `validate_definition` (`crates/tidb-parser/src/ddl_partition.rs`)
to a permissive pass: RANGE `LESS THAN` accepted any value count and LIST
`VALUES IN` accepted any scalar/tuple shape, on the premise that Go defers
these checks to the DDL layer, which reports the coded 1653
`ErrPartitionColumnList`. That premise is false for the parser tier.

The Go oracle validates partition VALUES arity **inside the grammar action**:

- `pkg/parser/ast/ddl.go:4873` `(*PartitionOptions).Validate` dispatches
  `pd.Clause.Validate(n.Tp, len(n.ColumnNames))` for every definition.
- `parsePartitionOptions` (`pkg/parser/ddl_partition_parser.go`) calls
  `opt.Validate()` before returning; a validation error is appended to
  `p.errs`, i.e. it is a **parse error**.
- `pkg/parser/ast/ddl.go:4514` `PartitionDefinitionClauseLessThan.Validate`:
  `columns == 0 && len(Exprs) != 1` -> `ErrTooManyValues` **[ddl:1657]**;
  `columns > 0 && len(Exprs) != columns` -> `ErrPartitionColumnList`
  **[ddl:1653]**.
- `pkg/parser/ast/ddl.go:4587` `PartitionDefinitionClauseIn.Validate`: row
  shape consistency (with DEFAULT-aware first/next-row selection) mismatches
  -> **[ddl:1653]**; `columns == 0 && expectedColCount != 1` ->
  `ErrRowSinglePartitionField` **[ddl:1658]**; `columns > 0 && expected !=
  columns` -> **[ddl:1653]** unless the sole value is DEFAULT.
- `pkg/parser/parser_test.go:6767-6778` (`TestTablePartition`, run green at
  HEAD with `go test ./pkg/parser -run 'TestTablePartition$'`) pins the very
  statements the sibling's executor tests use as **parse failures**:
  `partition by range columns (id) (partition p0 values less than (1, 2))`
  is row 6768, marked `false`.

Consequences at the base tip:

1. `tidb-parser::tests::table_partition_source::test_table_partition_source_of_truth`
   (port of Go `TestTablePartition`) regressed from 926/926 to a hard failure:
   line 108 (`range (a)` + `less than (10, 20)`) expected a parse error and
   now parsed.
2. The sibling's executor 1653 assertions observed the right **code** but
   through an unfaithful tier: Go's end-to-end observation for those SQL
   strings is a parse-time 1653, not a DDL-execution 1653.

## Fix

- `crates/tidb-parser/src/ddl_partition.rs`: restored faithful ports of
  `PartitionDefinitionClauseLessThan.Validate` and
  `PartitionDefinitionClauseIn.Validate` inside `validate_definition`,
  raising **coded** parse errors via the existing `Parser::err_coded`
  machinery (the same route `ast.ErrNoParts` [ddl:1504] already took):
  [ddl:1657] `Cannot have more than one value for this type of RANGE
  partitioning`; [ddl:1653] `Inconsistency in usage of column lists for
  partitioning`; [ddl:1658] `Row expressions in VALUES IN only allowed for
  multi-field column partitioning`. The MAXVALUE-in-VALUES-IN parse-level
  rejection and the DEFAULT-only special cases mirror Go exactly.
- `crates/tidb-executor/src/ddl.rs` (`run_create_table_in`): the parse error
  conversion now routes coded parse failures (`errno: Some`) to the existing
  `DriverError::ParseCoded { errno, message }` so the wire code/message
  survive; ordinary grammar failures keep the previous `Driver::Parse`
  debug-rendered shape byte-for-byte. This is the same variant the sibling
  introduced for their DDL-side path, so their executor assertions
  (`err_code == 1653`, exact message) still hold — now through Go's actual
  tier.

## Fail-before evidence

- `git stash` + run at base tip:
  `tidb-parser` 926-suite failed on
  `test_table_partition_source_of_truth` (820/821 in that binary; the 926-run
  `grep Summary` count included the failing binary aborting early). With the
  fix: **927/927** (including the new `test_partition_values_arity_coded_parse_errors`
  pin, which asserts errno + message for 1657/1653/1658 rows and the
  matching-arity control rows).
- `tidb-executor` `(test(ddl) or test(create) or test(alter) or
  test(partition))`: 357/368 with the fix, 357/368 at base, identical failing
  sets (11 pre-existing sibling in-flight breakage tests, matching their
  "record the sibling session's in-flight breakage" journal). The two sibling
  tests `create_table_with_range_column_partition_value_count_rows_report_1653`
  and `create_table_with_list_columns_partition_column_list_rows_report_1653`
  stay green.

## Go oracle anchors

- `pkg/parser/ast/ddl.go:4514` LessThan.Validate
- `pkg/parser/ast/ddl.go:4587` In.Validate
- `pkg/parser/ast/ddl.go:4873` PartitionOptions.Validate dispatch
- `pkg/parser/ddl_partition_parser.go` `opt.Validate()` inside
  `parsePartitionOptions`
- `pkg/parser/mysql/errcode.go` 1480/1653/1657/1658; `errname.go` messages
- `pkg/parser/parser_test.go:6767-6778` parse-error rows (verified green at
  HEAD)
