# Spatial Index Project: Context and Resume Notes

Status: **in design review** as of 2026-06-25, PR https://github.com/pingcap/tidb/pull/69473.
No code written yet; this is a design/research effort captured in documents. This file
orients a reader and supports resuming the implementation work.

Branch: `spatial-index-design` on remote `origin` (`mjonss/tidb`). Based on master tip
`807326b066`.

## What this project is

Add a spatial index to TiDB so proximity ("within radius") and containment
("point in polygon") queries on a geometry column are served by a selective index scan
instead of a full table scan. TiDB has no spatial support today, but the geometry data
type and basic functions are expected to land independently (tracking issue #6347,
dveeden's prior work). **Scope of this project is the index only**; the type and
functions are a prerequisite coded against, not a deliverable.

## Formal design doc

The TiDB-template design document is `docs/design/2026-06-25-spatial-index.md` (one level
up), following `docs/design/TEMPLATE.md`. It is the proposal under review (the
`*: Spatial index design` PR, https://github.com/pingcap/tidb/pull/69473). The files in
this directory are the supporting working material it references.

## Documents in this directory

- `research.md`: the knowledge base. Surveys MySQL/MariaDB/PostGIS, index types
  (R-tree/GiST, quadtree/Z-order/geohash, S2), why TiDB cannot use an R-tree, the
  cell-covering-over-MVI mechanism, the reference paper (ER-tree), the synthesis showing
  the Hilbert-key index and cell-covering MVI are the same design, a worked example on a
  concrete table, the SRID cell scheme, TiFlash reusability, and open questions.
- `PLAN.md`: the broad ExecPlan / roadmap (milestones 0-5) for the full spatial index.
- `PLAN-points-mvp.md`: the concrete first deliverable, a points-only secondary index,
  grounded in verified code integration points. This is where implementation starts.
- `storage-format.md`: pre-GA lock-in note (ported from the `spatial-index-poc` branch) on
  the two encodings that are hard to change after a release: the stored geometry **value**
  format (leaner/version-tagged vs raw EWKB) and the index **cell-key** curve (Morton vs
  Hilbert for SRID 0), with end-to-end profiling. Referenced from the formal doc's
  Unresolved Questions.
- `CONTEXT.md`: this file.

## How the design was reached (conversation arc)

1. Surveyed which spatial functions matter most (points + proximity + geofence dominate)
   and what index could accelerate them.
2. Established that R-trees (MySQL/MariaDB/PostGIS) do not fit TiKV's distributed,
   range-sharded, ordered keyspace; a space-filling-curve covering does. CockroachDB
   (distributed SQL on ordered KV) is the closest precedent and chose S2 cell covering.
3. Clarified TiDB's index vocabulary: the literal "inverted index" is a TiFlash columnar
   index; the one-row-to-many-keys primitive we reuse is the multi-valued index (MVI).
4. Decided the architecture: cell-covering over MVI as a TiKV secondary index, with an
   engine-neutral coverer so a future TiFlash path can reuse it.
5. Examined the reference paper (ER-tree, He & Chen 2022). Corrected an early
   mischaracterization: it is a Hilbert R-tree (SFC packing key + embedded R-tree MBRs),
   handles generic geometries, and its "no dual query" benefit requires clustering the
   table by the geometry. Concluded it is fundamentally a *clustered* organization;
   TiKV's Region routing already subsumes its global structure.
6. Showed that a `Hilbert(geom)+PK` secondary index and the cell-covering MVI are the
   same design for points, and converge for polygons (the MVI is the general form). A
   points-only plain index needs no MVI at all.
7. Settled on the points-only MVP as the first deliverable and wrote its concrete plan.

## Key decisions (see Decision Logs in PLAN.md / PLAN-points-mvp.md for rationale)

- Target TiKV OLTP via MVI; engine-neutral coverer seam for future TiFlash; no TiFlash
  plumbing now.
- Geometry type + functions are a prerequisite landing independently; code against them
  behind a thin accessor; expect minor churn.
- 2D index required; cell-key encoding dimension-tagged so 3D can be added later (MySQL
  and MariaDB are 2D-only; PostGIS has 3D/4D with an ND-GiST index).
- Two coverers behind one `CellCoverer`/`Coverer` interface: planar quadtree for SRID 0
  (default bounds `[-(1<<31), (1<<31)-1]` per axis, overridable), S2 for SRID 4326 via
  `github.com/golang/geo/s2` (Apache 2.0, Google's Go S2 port). Deliver SRID 0
  first.
- Filter-and-refine with the exact predicate evaluated in TiDB first; coprocessor
  pushdown deferred.
- ER-tree recorded as the alternative for a clustered spatial organization, not rejected
  on implementation-layer grounds (we control TiDB and TiKV; layer is not an artificial
  constraint).
- Points-only MVP: model as an expression index on a hidden virtual generated column
  `tidb_spatial_key(position)`, reusing expression-index DDL and the non-MVI
  `PlainIndexKVGenerator` write path. Restrict to POINT, NOT NULL, SRID-constrained
  columns. Refine via retained exact predicate (no new executor code).
- Sunny Bains review (2026-06-25): carry the geometry bbox with each index entry for a
  cheap pre-lookback filter (optionally full EWKB in the value for a covering index); make
  spatial indexes on partitioned tables global (Hilbert across all partitions, with
  `partition_id`, the `PARTITION BY` physical partition id, not the PK). The PoC implemented
  the bbox as index **key** columns (`tidb_spatial_key(p), ST_X(p), ST_Y(p)`), so the
  pre-filter is a pushed-down index filter and the value stays empty; in-value is reserved
  for the future full-EWKB covering index. The global index reuses the existing global-index
  encoding (partition id in the key, V2 #65289). MVP stays non-partitioned; global is a
  follow-on. See research.md -> "Index value contents and table partitioning".
- SQL syntax (2026-06-25): MySQL-compatible `SPATIAL INDEX`/`SPATIAL KEY` on a `NOT NULL`,
  SRID-restricted (0/4326) geometry column (a MySQL 8.0+ `SHOW CREATE TABLE` imports
  cleanly; TiDB already parses `SPATIAL INDEX`, execution is the gap). Cell tuning is
  deferred (defaults-only; if added, bare `NAME = value` index options, not `WITH`).
  Composite (prefix-column) indexes are a very-late milestone. See research.md ->
  "SQL syntax".

## Open questions to resolve on resume

- Cell-depth / max-cells default tuning, measure against representative point data (the
  Capital Bikeshare dataset used in TiDB docs is a candidate).
- SRID 0 domain bounds default and out-of-domain behavior (currently: clamp to the
  boundary cell, still indexed and correct, over-covered near the edge; not rejected).
- Coverer package location: `pkg/util/spatial` (tentative) vs `pkg/types/spatial`.
- Is a *clustered* spatial table ever a target use case, or is a secondary index on
  normally-clustered tables the only goal? This decides whether the ER-tree clustered
  direction is ever pursued.
- S2 vs minimal in-house spherical coverer for 4326 (leaning S2 / golang/geo).
- Whether `go-geom` covers exact `ST_Intersects`/`ST_Contains` for the refine step or
  needs supplementary code.
- The Sunny Bains review open questions (research.md -> "Index value contents and table
  partitioning"): index value payload (bbox vs +summary vs full EWKB), where the bbox
  pre-filter runs (TiDB vs coprocessor), global-vs-local policy for partitioned tables,
  `partition_id` encoding reuse, MVP scope, and the hidden-column index-value wrinkle.

## Next step (implementation)

Start Milestone 1 of `PLAN-points-mvp.md`: build `pkg/util/spatial` with the SRID 0
planar coverer (`EncodePoint`, `CoverQuery`) and its no-false-negatives unit test, plus
the throwaway proof that coverer-produced ranges can be injected into a planner
`AccessPath` (de-risks the planner hook before building on it).

## Verified code integration points (reconfirm line numbers before editing)

- Expression index / hidden column: `pkg/parser/ast/ddl.go:293-301`
  (`IndexPartSpecification.Expr`); `pkg/meta/model/column.go:96-105`
  (`ColumnInfo.GeneratedExprString`, `Hidden`); `pkg/ddl/index.go:916`.
- Index metadata: `pkg/meta/model/index.go:260-287` (`IndexInfo`, `MVIndex`).
- Predicate to range: `pkg/util/ranger/detacher.go:1033`
  (`DetachCondAndBuildRangeForIndex`).
- Access path: `pkg/planner/core/planbuilder.go:1320-1414` (`getPossibleAccessPaths`);
  `pkg/planner/core/stats.go:397-449`; `pkg/planner/core/find_best_task.go:1770-1861`
  (`skylinePruning`); `pkg/planner/util/path.go:46-150` (`AccessPath`).
- Index KV write: `pkg/table/tables/index.go:663-671` (`GenIndexKVIter`, selects
  `PlainIndexKVGenerator` when `MVIndex` is false); `pkg/tablecodec/tablecodec.go:1228-1283`
  (`GenIndexKey`).
- MVI fan-out (for the later generic case): `pkg/table/tables/index.go:195-240`
  (`getIndexedValue`).
- Geometry type today: only `TypeGeometry = 0xff` at `pkg/parser/mysql/type.go:47`
  exists; no `ST_*` builtins, no EWKB value type. Builtins register in
  `pkg/expression/builtin.go:659+` (`var funcs`).

## Prior art (issue #6347)

- `docs/design/2022-10-27-geospatial.md` (PR #38916, closed): dveeden's staged geospatial
  design; defers the index as "needs more research".
- PR #66602 (closed): parser support for spatial types + SRID; rejects them in
  `pkg/planner/core/preprocess.go` (enable by removing the `TypeGeometry` check).
- PR #38611 + tikv/tikv#13652 (closed): the GEOMETRY column type.
- Storage format is EWKB (`<srid><wkb>`, MySQL-compatible); library `github.com/twpayne/go-geom`.
