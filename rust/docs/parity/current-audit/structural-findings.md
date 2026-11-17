# Structural findings register

Where the Rust rewrite's structure still differs from the Go owner it ports.
Re-audited against the tree on 2026-11-04; the [JSON register](structural-findings.json)
holds the same rows. A repaired finding is removed, not kept.

**3 unresolved: 2 open, 1 partial.**

A row states the remaining gap only. "Restate" marks a row whose earlier text
named no concrete mismatch ("complete package obligations remain"); it stays
unresolved until it is restated against its Go owner or closed.

## Unresolved

### System tables and startup

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| B01 | partial | First boot is decided by the bootstrap commit, not by DDL owner election: the node publishes the `mysql` bootstrap while connecting its store (`tidb-server` `cluster_stack.rs`), before the domain and its owner manager exist. A node whose publish loses to another's now waits for the winner, checking the store every 200ms as Go's non-owner does in `bootstrap`, and boots from the winner's tables through the upgrade path (two realtikv nodes booting a fresh cluster together both come up; before, one failed with `bootstrap encode failed: Write conflict`). Go lets only the DDL owner run `doDDLWorks`/`doDMLWorks`. | `pkg/session/bootstrap.go` (`bootstrap`), `pkg/session/session.go` (`runInBootstrapSession`) |

### Executor, DML and expressions

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| E29 | open | Materialized views are not ported. The parser rejects `CREATE MATERIALIZED VIEW` and `CREATE MATERIALIZED VIEW LOG` as syntax errors (1064), where Go master parses them and gates them on `tidb_mview_enable` (8200 when off). The materialized-view DDL and its logs, `REFRESH MATERIALIZED VIEW`, the mview worker and its schedule, the planner's materialized-view rewrite and the executor's delta merge aggregation are absent. IMPORT INTO depends on them in one place: a next-gen IMPORT INTO ... FROM SELECT runs on a DXF worker (`ImportStepQuery`) only during materialized-view maintenance (`InMViewMaintenance`), so this port refuses every next-gen FROM SELECT, as Go does outside maintenance, and `ImportStepQuery` is not ported. | `pkg/ddl/materialized_view.go`, `pkg/ddl/mview_*.go`, `pkg/planner/mview`, `pkg/executor/mviewdeltamergeagg`, `pkg/util/mviewutil`, the parser's materialized-view grammar, `ImportStepQuery` in `pkg/dxf/importinto` (Go master) |

### Structure: Go's shape before more ports

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| S02 | open | `TableCommon` keeps the public columns and the non-public (adding or changing) columns as two lists, and an index's `column_offsets` index whichever list it was loaded against (public for a public index, public then non-public for a non-public one). Go's `TableCommon.Columns` is every `tblInfo.Columns` entry in metadata order, `Cols()` / `VisibleCols()` / `WritableCols()` / `DeletableCols()` are slices derived from it, and `IndexColumn.Offset` always indexes `tblInfo.Columns`; the two bases caused the MODIFY COLUMN mutation-checker panic | `pkg/table/tables/tables.go` (`TableCommon`, `initTableCommon`) |

### Domain and process services

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
