# Spatial Index POC Branch Review

Review date: 2026-06-26

Reviewed branch: `spatial-index-poc` at `568c0a0ac2`

Review base: local `pingcap/master` using `git diff pingcap/master...HEAD`

Design reference:

- `../spatial-index-design/docs/design/2026-06-25-spatial-index.md`
- `../spatial-index-design/docs/design/spatial-index/PLAN-points-mvp.md`
- Updated branch docs: `docs/design/spatial-index/bbox-pushdown-design.md` and
  `docs/design/spatial-index/tikv-pushdown-handoff.md`

## Summary

The updated branch is no longer just the points-only MVP from the design branch. It
also exposes geometry storage and many `ST_*` functions, POINT spatial indexes for
SRID 0/4326, general-geometry MVI indexes, bbox-in-index pruning, composite spatial
indexes, S2 covering, and TiKV pushdown handoff docs.

The main remaining blockers are not in the core idea of using hidden generated
columns. They are around the exposed SQL surface: DDL depends on an experimental
expression-index switch, indexed geometry values are not constrained to the declared
SRID/subtype, binary protocol output still fails for geometry, and several advertised
or natural spatial predicates do not reach the spatial resolver.

## Findings

### High: `CREATE SPATIAL INDEX` depends on `allow-expression-index`

The bbox-in-index layer appends hidden expression-index columns to every spatial
index:

- POINT: `ST_X(col)`, `ST_Y(col)`
- general geometry: `tidb_spatial_bbox(col, 0..3)`

Relevant code:

- `pkg/ddl/create_table.go:1357`
- `pkg/ddl/create_table.go:1368`
- `pkg/ddl/create_table.go:1375`
- `pkg/ddl/executor.go:5113`

Those helper functions are not in the GA expression-index allow-list. Only
`tidb_spatial_key` and `tidb_spatial_keys` were added:

- `pkg/sessionctx/variable/varsutil.go:455`
- `pkg/sessionctx/variable/varsutil.go:463`

The normal expression-index checker rejects non-GA functions when
`config.Experimental.AllowsExpressionIndex` is false:

- `pkg/ddl/generated_column.go:311`
- `pkg/ddl/generated_column.go:382`

The default config keeps that switch false:

- `pkg/config/config.go:1264`

The package tests mask this because both DDL and executor test main files turn it on:

- `pkg/ddl/main_test.go:54`
- `pkg/executor/main_test.go:46`

Impact: on a default TiDB server, user-facing `CREATE SPATIAL INDEX sidx ON t(p)`
can fail after the bbox columns are added, even though the feature is not presented as
requiring the experimental expression-index escape hatch.

Expected fix: either mark the bbox helper functions as allowed deterministic
expression-index functions, or route spatial-index internal generated columns through a
dedicated validation path. Add a regression test with `AllowsExpressionIndex=false`.

### High: Geometry SRID/subtype constraints are not enforced on writes

The metadata exists, but write-time casting still treats geometry as a generic binary
string:

- `pkg/table/column.go:328`
- `pkg/table/column.go:350`
- `pkg/types/datum.go:1026`
- `pkg/types/datum.go:1041`

`validateSpatialColumn` also checks `col.Srid` but does not require the `SridFlag`, so
a `POINT NOT NULL` column without an explicit `SRID` clause is accepted as if it were
SRID 0:

- `pkg/ddl/create_table.go:1401`
- `pkg/ddl/create_table.go:1408`
- `pkg/ddl/create_table.go:1410`

That breaks the key/range contract. The write path chooses the key scheme from the
stored EWKB SRID:

- `pkg/expression/builtin_geo.go:1599`
- `pkg/expression/builtin_geo.go:1608`
- `pkg/expression/builtin_geo.go:1619`

The planner chooses the query range scheme from the declared column SRID:

- `pkg/planner/core/spatial_resolve_index.go:582`
- `pkg/planner/core/spatial_resolve_index.go:593`

Impact: a `POINT SRID 0` column, or a `POINT` column with no SRID restriction, can
store an SRID 4326 EWKB value. The generated index key is then S2, while the planner
injects planar ranges. That can silently miss matching rows when the spatial index is
used. The same missing validation also allows storing a non-POINT geometry in a POINT
column until a hidden generated column happens to fail.

Expected fix: require `SridFlag` for indexed spatial columns, validate EWKB structure,
declared SRID, and declared geometry subtype in the assignment/cast path, and reject
invalid SRID values in constructors/setters instead of `uint32` wrapping.

### High: Binary protocol geometry result rows still fail

Text protocol row encoding handles `mysql.TypeGeometry`:

- `pkg/server/internal/column/column.go:197`

`DumpBinaryRow` still omits geometry from the byte/string case:

- `pkg/server/internal/column/column.go:260`
- `pkg/server/internal/column/column.go:282`

Prepared/binary protocol input parameters do include `mysql.TypeGeometry`, so this is
specifically an output-row gap:

- `pkg/expression/util.go:2265`

Impact: a prepared statement or binary-protocol client returning a geometry column will
fall through to `invalid type 255`.

Expected fix: handle `mysql.TypeGeometry` in `DumpBinaryRow` like the binary/blob
payload types, and add a prepared-statement regression test that selects a geometry
column.

### Medium: Common spatial predicates do not reach the resolver

The resolver recognizes only:

- `ST_Distance(...) <= r` / `< r`
- `ST_Distance_Sphere(...) <= r` / `< r`
- bare `ST_Contains(...)`
- bare `ST_Within(...)`

Relevant code:

- `pkg/planner/core/spatial_resolve_index.go:603`
- `pkg/planner/core/spatial_resolve_index.go:610`
- `pkg/planner/core/spatial_resolve_index.go:612`

`ST_Intersects` is implemented but does not set the spatial resolver flag:

- `pkg/expression/builtin.go:1011`
- `pkg/expression/builtin.go:1013`

The general-geometry MVI test still has to express `ST_Intersects` through a manual
`json_overlaps(tidb_spatial_keys(...), ...)` workaround:

- `pkg/executor/poc_spatial_mvi_test.go:49`
- `pkg/executor/poc_spatial_mvi_test.go:53`

The resolver also misses normal boolean wrappers such as `ST_Within(...) = 1`; the
integration test uses exactly that form, but it does not assert index usage:

- `tests/integrationtest/t/spatial_compat.test:86`
- `tests/integrationtest/t/spatial_compat.test:88`

Impact: exposed spatial indexes frequently remain full scans unless the query uses the
small exact shapes the resolver recognizes, or unless tests force/manualize the
internal expression.

Expected fix: recognize `ST_Intersects(g, const)` and equivalent argument orders, decide
which other DE-9IM predicates are in scope (`ST_Covers`, `ST_CoveredBy`, etc.), and
unwrap common boolean forms such as `predicate = 1`.

### Medium: Composite spatial indexes do not round-trip through `SHOW CREATE TABLE`

The DDL path accepts composite point spatial indexes:

- `pkg/ddl/create_table.go:1432`
- `pkg/ddl/create_table.go:1446`
- `pkg/ddl/executor.go:5080`
- `pkg/ddl/executor.go:5094`

But `SHOW CREATE TABLE` only renders a spatial index when the first index column is the
hidden spatial key. The comment explicitly says composite spatial indexes fall through
to normal `KEY` rendering:

- `pkg/executor/show.go:1056`
- `pkg/executor/show.go:1064`
- `pkg/executor/show.go:1065`

The normal renderer then exposes the internal generated expressions:

- `pkg/executor/show.go:1304`
- `pkg/executor/show.go:1307`
- `pkg/executor/show.go:1308`

Impact: `CREATE SPATIAL INDEX tp ON locs (tenant_id, p)` dumps as an ordinary key over
`tenant_id`, `tidb_spatial_key(p)`, `ST_X(p)`, and `ST_Y(p)`, not as a spatial index.
That is not MySQL-compatible, exposes internal functions, and re-import depends on the
expression-index function allow-list.

Expected fix: either reject composite spatial indexes until the later design milestone,
or render them as a TiDB spatial extension that round-trips cleanly.

### Medium: Row-level checksum ignores geometry values

Geometry is now user-visible, but row-level checksum encoding still treats
`mysql.TypeGeometry` like `TypeNull`:

- `pkg/util/rowcodec/common.go:236`
- `pkg/util/rowcodec/common.go:242`
- `pkg/util/rowcodec/common.go:341`

Impact: with row-level checksum enabled, two rows that differ only in a geometry column
can produce the same encoded checksum input. That weakens `tidb_row_checksum()` and any
row-checksum-based consistency checks for geometry tables.

Expected fix: encode the EWKB bytes as a length-prefixed byte/string value, and add a
checksum regression test that changes only a geometry column.

### Low: `INFORMATION_SCHEMA.COLUMNS.SRS_ID` is always NULL

The information schema reader emits `nil` for `SRS_ID` even when the column was declared
with an SRID:

- `pkg/executor/infoschema_reader.go:1278`

Expected fix: return the declared `col.Srid` when `mysql.HasSridFlag(col.GetFlag())`.

## Design Alignment Notes

The design branch's first deliverable is points-only:

- hidden generated-column expression index using `tidb_spatial_key`
- `POINT`, `NOT NULL`, explicitly SRID-constrained to 0 or 4326
- non-partitioned tables
- exact predicate retained as refine

The updated branch now goes beyond that with general-geometry MVI, bbox-in-index
columns, composite indexes, many `ST_*` builtins, and TiKV pushdown planning docs. That
is fine for a POC branch, but it means the branch has to satisfy broader SQL
compatibility and round-trip contracts than the original MVP.

## Coverage Gaps

The added tests prove useful POC behavior, but they miss the highest-risk integration
points above:

- no spatial-index DDL test with `AllowsExpressionIndex=false`
- no insert/update rejection tests for mismatched SRID or mismatched geometry subtype
- no prepared/binary-protocol geometry result test
- no `SHOW CREATE TABLE` round-trip test for composite spatial indexes
- no index-use test for `ST_Intersects(g, const)` or `ST_Within(...) = 1`
- no `INFORMATION_SCHEMA.COLUMNS.SRS_ID` assertion
- no row-level checksum assertion involving geometry bytes

## Validation Evidence

Commands run:

```bash
git status --short --branch
git diff --name-status pingcap/master...HEAD
git diff --stat pingcap/master...HEAD
git log --oneline --decorate --max-count=30 pingcap/master..HEAD
rg --files docs/design/spatial-index ../spatial-index-design/docs/design
git diff --check pingcap/master...HEAD
```

`git diff --check pingcap/master...HEAD` passed.

## Not Verified Locally

- Ready-profile validation (`make lint` and scoped package/integration tests).
- Runtime reproduction of the default-config spatial-index DDL failure.
- Prepared-statement/binary protocol geometry round trip.
- SRID/subtype rejection behavior after adding enforcement.
- Composite `SHOW CREATE TABLE` round trip.
- Automatic index planning for `ST_Intersects(g, const)` and `ST_Within(...) = 1`.
- Row-level checksum behavior with geometry values.
