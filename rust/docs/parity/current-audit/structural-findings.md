# Structural findings register

Where the Rust rewrite's structure still differs from the Go owner it ports.
Re-audited against the tree on 2026-11-04; the [JSON register](structural-findings.json)
holds the same rows. A repaired finding is removed, not kept.

**3 unresolved: 1 open, 2 partial.**

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
| E29 | partial | Materialized views are ported up to their statements (phases in `rust/docs/go-shape-refactors-execplan.md`): `TableInfo` keeps Go master's `materialized_view*` fields byte for byte, the base table's DML writes its MV log rows as Go's does, the action types, schema diffs and `tidb_mview_*` / `tidb_mlog_*` variables are Go master's, and the parser takes every materialized view statement (CREATE / ALTER / DROP MATERIALIZED VIEW [LOG], REFRESH, PURGE, CANCEL, COMPARE, the SHOW forms) with Go master's restore text and syntax errors. `CREATE MATERIALIZED VIEW LOG`, `DROP MATERIALIZED VIEW LOG` and `ALTER MATERIALIZED VIEW LOG` (PURGE and ADD COLUMN) run as Go master's do, DROP / TRUNCATE / RENAME TABLE, ALTER TABLE, CREATE / DROP / ALTER INDEX, PARTITION BY, REMOVE PARTITIONING and EXCHANGE PARTITION refuse a log and its base table as Go master's do, MODIFY / CHANGE COLUMN of a logged base table retypes the log's tracked column as Go master's does (a base table with dependent materialized views is refused until those are ported), (the log table Go builds, the base table's `MaterializedViewBase`, the `mysql.tidb_mlog_purge_info` row with its schedule, Go's refusals and the `tidb_mview_enable` gate), except on a base table that is itself a materialized view (Go takes the refresh advisory lock, which this DDL session does not hold). CREATE MATERIALIZED VIEW runs as Go master's does: the planner's SELECT build and column count, `validateCreateMaterializedViewQuery` (via `tidb-mviewutil`), the view table from the SELECT's output types with its group key and `MaterializedViewInfo`, the owner's create step and its initial build, the refresh and refresh-source rows, and the rollback of a failed build. ALTER MATERIALIZED VIEW (COMMENT, REFRESH with its schedule row, ATTRIBUTES) and DROP MATERIALIZED VIEW run as Go master's do too. The build runs Go's INSERT path (`REPLACE INTO view <definition>`); Go's TiKV store builds by IMPORT INTO instead, which this node does not run for it. The other statements are not executed: the DDL refuses them as unsupported and REFRESH / PURGE / CANCEL / COMPARE / SHOW as unsupported statements. This branch's bootstrap does not create Go master's five materialized view tables (`mysql.tidb_mview_refresh_info` .. `tidb_mlog_purge_hist`, Go master's `upgradeToVer316`). The materialized-view DDL and its logs, `REFRESH MATERIALIZED VIEW`, the mview worker and its schedule, the planner's materialized-view rewrite and the executor's delta merge aggregation are absent. IMPORT INTO depends on them in one place: a next-gen IMPORT INTO ... FROM SELECT runs on a DXF worker (`ImportStepQuery`) only during materialized-view maintenance (`InMViewMaintenance`), so this port refuses every next-gen FROM SELECT, as Go does outside maintenance, and `ImportStepQuery` is not ported. | `pkg/ddl/materialized_view.go`, `pkg/ddl/mview_*.go`, `pkg/planner/mview`, `pkg/executor/mviewdeltamergeagg`, `pkg/util/mviewutil`, `ImportStepQuery` in `pkg/dxf/importinto` (Go master) |

### Structure: Go's shape before more ports

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| S02 | open | `TableCommon` keeps the public columns and the non-public (adding or changing) columns as two lists, and an index's `column_offsets` index whichever list it was loaded against (public for a public index, public then non-public for a non-public one). Go's `TableCommon.Columns` is every `tblInfo.Columns` entry in metadata order, `Cols()` / `VisibleCols()` / `WritableCols()` / `DeletableCols()` are slices derived from it, and `IndexColumn.Offset` always indexes `tblInfo.Columns`; the two bases caused the MODIFY COLUMN mutation-checker panic | `pkg/table/tables/tables.go` (`TableCommon`, `initTableCommon`) |

### Domain and process services

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
