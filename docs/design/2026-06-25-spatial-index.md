# TiDB Design Documents

- Author(s): [Mattias Jonsson](http://github.com/mjonss)
- Discussion PR: https://github.com/pingcap/tidb/pull/69473; builds on the earlier draft https://github.com/pingcap/tidb/pull/38916
- Tracking Issue: https://github.com/pingcap/tidb/issues/6347

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

This document proposes a spatial index for TiDB that accelerates proximity ("within
radius") and containment ("point in polygon", bounding-box overlap) queries on a
geometry column. The index is a TiKV secondary index that decomposes a geometry into a
set of covering cells of a space-filling curve (Hilbert ordering, S2 cells for
geographic data and a planar quadtree for Cartesian data), stores one ordered key per
covering cell using TiDB's existing multi-valued-index mechanism, and answers spatial
queries by scanning the cell ranges that cover the query region and then refining
candidates with the exact predicate.

The scope of this document is the **index only**. The `GEOMETRY` data type, its EWKB
storage, and the `ST_*` functions are treated as a prerequisite that lands
independently (see Motivation); the index is designed against a small contract from
that type. Supporting working documents with the full survey, decision logs, and a
concrete first-deliverable plan live alongside this file under
`docs/design/spatial-index/` (`research.md`, `PLAN.md`, `PLAN-points-mvp.md`,
`CONTEXT.md`).

## Motivation or Background

Geospatial support is one of the most requested TiDB features. Tracking issue #6347
carries the `feature/accepted` label and ranks among the top open issues by reactions.
The dominant real-world workload is simple and concrete: store a location per row
(latitude/longitude or a planar coordinate) and answer "what is near me", "which region
contains this point", or "what overlaps this box". Bike-share, ride-hailing, parcel
delivery, and asset-tracking applications all reduce to points plus proximity and
geofence queries.

TiDB has no spatial support today: only the `mysql.TypeGeometry` type constant exists
(`pkg/parser/mysql/type.go`), with no value representation and no `ST_*` functions. The
geometry type and basic functions are expected to land independently, following the
staged plan in the earlier geospatial design (PR #38916,
`docs/design/2022-10-27-geospatial.md`) and its parser/type work
(PRs #66602, #60295, #38611 and tikv/tikv#13652). That earlier design explicitly deferred the spatial index,
stating it "needs more research". This document fills that gap.

Without an index, the queries above are full table scans. The index is what turns them
into selective lookups once point tables grow large, which is precisely where the demand
is. The index must accelerate the predicates real workloads use: bounded distance
(`ST_Distance`/`ST_Distance_Sphere` under a radius), containment (`ST_Contains`,
`ST_Within`), intersection (`ST_Intersects` and the MBR family). Per-row computations
without spatial locality (`ST_Area`, `ST_Length`, accessors) have no index story, the
same as in MySQL.

## Detailed Design

### Overall approach

MySQL, MariaDB, and PostGIS index spatial data with an R-tree (PostGIS via GiST). An
R-tree is a tree of overlapping bounding rectangles that assumes single-node, in-place
tree maintenance. It does not fit TiKV, which is a distributed, range-sharded, globally
ordered key-value store. The proven fit for an ordered KV store is a **space-filling
curve**: linearize 2D proximity into 1D key proximity so spatial queries become range
scans over the existing index machinery. Space-filling-curve spatial indexing is a
long-established, general technique (Hilbert curves, Morton/Z-order, geohash, and
Google's open-source S2 cell library), used across many systems; CockroachDB is one
independent precedent that applies it on a distributed ordered KV store comparable to
TiKV (S2 cell covering over an inverted index).

TiDB already has the needed primitive: the **multi-valued index (MVI)**, where one row
produces many ordered index entries (today used for JSON array membership). A spatial
index maps onto it directly: cover each geometry with a set of hierarchical cells and
write one index entry per covering cell. Queries follow the standard spatial
**filter-and-refine** pattern: cover the query region into cell ranges, range-scan to
get candidate rows, then evaluate the exact predicate to drop false positives. The
index always returns a superset; correctness lives in the refine step.

### SQL syntax

MySQL-compatible. `INDEX` and `KEY` are synonyms. Notation: `[...]` optional, `{a | b}` choice.

    -- spatial column: NOT NULL, SRID-restricted to 0 or 4326
    col_name {GEOMETRY | POINT | LINESTRING | POLYGON | ...} NOT NULL SRID {0 | 4326}

    -- spatial index: in CREATE TABLE, ALTER TABLE ... ADD, or CREATE SPATIAL INDEX ... ON t (...)
    SPATIAL {INDEX | KEY} [index_name] (geometry_col)

The single geometry column is the only key part; it must be `NOT NULL` and SRID-restricted
to `SRID 0` or `SRID 4326` (both relaxable later; see the SRID and NOT NULL notes under
"Index entry layout"). A MySQL 8.0+ `SHOW CREATE TABLE` whose spatial index is on a
`SRID 0` or `4326` column imports cleanly (TiDB already parses `SPATIAL INDEX`, and the
`SRID` attribute and geometry types arrive with the prerequisite type work). MySQL also
allows spatial indexes on other SRIDs (e.g. `3857`, Web Mercator); those are not yet
supported here and are rejected at DDL. `3857` is a projected, planar CRS, so it could
later reuse the SRID 0 planar coverer with Mercator bounds, more easily than arbitrary
geographic SRSs. `SHOW CREATE TABLE` emits the plain MySQL form:

    `geom` point NOT NULL SRID 4326,
    SPATIAL KEY `geom_idx` (`geom`)

Future enhancements (TiDB extensions, not in the initial release; detail in
`research.md` -> "SQL syntax"):

- **Cell tuning** via bare `NAME = value` index options (e.g. `S2_MAX_CELLS = 8`), the
  MySQL-family idiom MariaDB uses for its vector index; deferred (ship defaults-only,
  matching TiDB's vector index, which exposes no such knobs).
- **Composite (prefix-column) indexes**, e.g. `SPATIAL INDEX (tenant_id, geom)`, for
  multi-tenant pruning; a very late milestone.

TiDB-specific forms (tuning, composite) are emitted in `SHOW CREATE TABLE` inside a
`/*T![spatial] ... */` feature comment so non-TiDB tools ignore them (matching
`/*T![clustered_index] */`).

### Index entry layout

For a table `t` (table id `T`) with a spatial index (index id `I`) on a geometry column,
each covering cell of a row's geometry produces one index entry. The geometry's bounding
box (and, for a point, its exact coordinates) are carried as **trailing index columns**
(hidden generated columns), so they live in the index **key**, after the cell key and
before the handle; the value is empty:

    -- general geometry (MVI):
    t{T}_i{I}_{cell_key}_{minX}_{minY}_{maxX}_{maxY}_{handle} -> {}
    -- point (one cell, plain index): the bbox degenerates to the coordinates
    t{T}_i{I}_{cell_key}_{x}_{y}_{handle}                     -> {}

- `cell_key` is the space-filling-curve cell id (encodes level and Hilbert/cell
  position); entries are ordered by the curve, so it is the leading index column.
- `minX,minY,maxX,maxY` (for a point, `x,y`) are extra index columns after the cell key,
  generated from the geometry. Putting the bbox in the **key** (not the value) is what lets
  TiDB's existing index-filter machinery push the bbox-intersection pre-filter to the
  coprocessor and apply it *during the index scan, before the row lookup* (see Query path),
  recovering R-tree-style MBR pruning with no new operator and no value decode.
- `handle` is the row handle, appended for uniqueness (as for any non-unique index).
- The value is empty in the common case. A *covering* index may optionally store the full
  EWKB in the value to refine without any row fetch (a size-vs-speed choice). A global
  index on a partitioned table (a later phase, see Partitioned tables) carries the
  `partition_id` (the `PARTITION BY` physical partition id, not the primary key) so the
  lookback finds the row's partition, encoded exactly as TiDB's existing global-index does
  (partition id in the key for non-unique non-clustered indexes, the V2 work #65289), not a
  bespoke value-only scheme.

A **point** is contained in exactly one cell, so a point geometry produces exactly one
entry (no fan-out); MVI machinery is then unnecessary and a plain secondary index entry
suffices. A **polygon or linestring** is covered by several cells (bounded by a max-cells
parameter) and fans out to several entries, which is the MVI case.

### Coverer and SRID schemes

A single engine-neutral `Coverer` abstraction isolates the curve/cell math:

    EncodePoint(srid, x, y) -> cell_key            // for a stored geometry (point: one cell)
    Cover(srid, geometry)   -> []cell_key          // covering cells of an extent
    CoverQuery(srid, region) -> []cell_range       // cell ranges covering a query region

Two implementations, chosen by the column's SRID, sit behind it:

- **SRID 0 (abstract Cartesian plane): planar quadtree / Z-order.** SRID 0 has no
  natural bounds, so the index fixes a configurable coordinate domain (default
  `[-(1<<31), (1<<31)-1]` per axis, a generous bound covering common coordinate systems
  with headroom; CockroachDB's docs use the same value), quadtree-subdivides it, and
  clamps out-of-domain coordinates to the boundary cell (still indexed and correct,
  over-covered near the edge), so the index stays complete and needs no full-scan fallback.
- **SRID 4326 (WGS 84 lat/long): S2 spherical cells** via `github.com/golang/geo/s2`
  (Apache 2.0, Google's open-source S2 port). S2 handles the antimeridian and poles
  natively and covers a distance query as a spherical cap, which a flat lat/long grid
  cannot do correctly. Distance predicates split by function: `ST_Distance_Sphere` is
  spherical and maps directly to a spherical cap of the same radius; geographic
  `ST_Distance` is geodesic on the WGS 84 *ellipsoid* (MySQL semantics), where a
  same-radius spherical cap can miss true matches (false negatives), so it is optimized
  only with a **conservatively inflated** cap (by the ellipsoid/sphere deviation bound, on
  the order of 0.5%), and otherwise falls back to a scan. The MVP optimizes
  `ST_Distance_Sphere` first.

Two tuning knobs are distinct: **max level** controls cell precision; **max cells per
geometry** caps fan-out for extents (points are always one entry regardless of level).

**Cell-key encoding and cross-level matching.** Cell keys are order-preserving and
big-endian (Morton code for the planar quadtree, S2 cell id for the sphere), so a cell's
descendants share its key as a prefix (a contiguous range) and its ancestors are its key
prefixes. `CoverQuery` therefore matches a stored cell that is equal to, an ancestor of,
or a descendant of a query cell: descendants via a prefix **range scan**, ancestors via a
bounded set of **point lookups** (one per coarser level, up to the index's max level),
with half-open range bounds. The covering is proven to have no false negatives by a
property test (brute force vs covering over random points, for both SRIDs).

### Write, update, and delete paths

Index maintenance is transactional with the row, identical to any TiDB secondary index
(no special handling needed, the standard index write path applies):

- Insert: parse EWKB, compute the covering cells and the bbox, write the row and one
  index entry per covering cell in the same transaction.
- Update of the geometry: compute old and new covering cells, delete the old entries,
  write the new entries, update the row, all in one transaction.
- Delete: delete the row and its covering-cell entries in one transaction.

### Query path

1. Compute the query region (a window rectangle, or a distance-bounded disc on a plane /
   cap on the sphere) from the predicate's constant arguments.
2. `CoverQuery` the region into cell ranges.
3. Range-scan those ranges over the index.
4. Apply the bbox-intersection filter on the index's bbox columns (`minX <= q_maxX AND
   maxX >= q_minX AND minY <= q_maxY AND maxY >= q_minY`; for a point index it degenerates
   to `x BETWEEN q_minX AND q_maxX AND y BETWEEN q_minY AND q_maxY`). These are index
   columns, so TiDB pushes this filter to the coprocessor and applies it during the scan,
   before the row lookup.
5. Deduplicate candidate handles (a polygon matched via several cells appears once per
   cell).
6. Look the surviving rows up by handle. A point index can skip the lookup for the filter
   by reconstructing the point from its `x,y` index columns (`ST_Within(ST_Point(x, y), q)`),
   a covering access; a covering index that stores the EWKB in the value also skips it.
7. Apply the exact predicate (`ST_Intersects`/`ST_Contains`/exact distance) to produce
   the final result.

Two pushdown layers exist. **Layer A (bbox-in-index)** is step 4: because the bbox lives in
index columns, the existing index-filter machinery pushes it to the storage node and prunes
false positives *before the random read*, the dominant cost, with no new operator, no value
decode, and no protocol change (it works on unistore and TiKV alike); the PoC measured the
bbox filter pruning 169 candidate index rows to 121 table lookups (48 false positives
removed before the random read), with a point covering rewrite able to remove the filter
lookups entirely. **Layer B (exact-refine pushdown)** is pushing
step 7's exact `ST_*` predicate to the coprocessor; that needs new tipb sigs and a
TiKV/TiFlash evaluator and is a later, optional increment (see Compatibility -> Executor and
`research.md` -> "Exact refine location and coprocessor pushdown"). The baseline runs step 7
at the TiDB root as a retained `Selection`, so Phases 1-2 need no new executor operator.

For **prepared/parameterized** queries the query region and its cell ranges depend on the
parameter values, so the ranges are rebuilt at execution from the parameters (as for any
parameterized index range), not frozen in a cached plan; if runtime rebuild is not wired,
the spatial plan is marked non-cacheable (a `NoncacheableReason`).

The bounded form `WHERE within radius ORDER BY distance LIMIT k` is supported by serving
the `WHERE` from the index and letting an ordinary `TopN` sort the small candidate set.
Native nearest-neighbour (`ORDER BY distance LIMIT k` with no radius) is not served by a
plain cell index and is deferred (it needs an expanding-ring operator).

### Worked example: indexing and querying a triangle

This condenses the full walkthrough in `docs/design/spatial-index/research.md`. Take a triangle `T = (4,4), (8,4), (4,8)` (the
region `x>=4, y>=4, x+y<=12`) in a small domain `[0,16)²`; its bounding box is
`[4,8]×[4,8]`, a small box well inside the domain. Cells are squares of side
`domain/2^L`; a cell id is a path of quadrant digits, so a cell's ancestors are its id
prefixes and its descendants share its id as a prefix (a contiguous key range).

![Spatial index covering cells for three geometries: triangle T (blue), rectangle id 75 (teal), triangle id 1 (red) in the shared violet cell 0320, over a Carbon-gray quadtree grid.](spatial-index/spatial-index-example.png)

The covering mixes levels: a coarse cell where the triangle solidly fills it, finer
cells hugging the diagonal edge. For `T` that is one size-2 cell plus six size-1 cells:

    030                  (size 2, the solid interior corner)
    0310 0311 0312       (size 1, lower-right edge)
    0320 0321 0322       (size 1, upper-left edge)

Storage: row `id=42` writes one entry per covering cell. The bbox `(4,4,8,8)` rides in the
key as trailing index columns after the `cell_key` (so it pre-filters during the scan); the
entries share the bbox and handle, differing only in `cell_key`; the value is empty:

    t{T}_i{I}_030_4_4_8_8_42  -> {}
    t{T}_i{I}_0310_4_4_8_8_42 -> {}
    ...
    t{T}_i{I}_0322_4_4_8_8_42 -> {}

These multiple keys are the MVI fan-out: an MVI is one row writing multiple index keys
(as for JSON-array columns), not multiple values under one key. A point would write a
single entry and need no MVI.

Search covers the query region into cells, then for each query cell finds stored cells
that are it, an ancestor, or a descendant: a finer query matches a coarser stored cell
via ancestor prefixes (a few point lookups); a coarser query matches finer stored cells
via a descendant range scan; the bbox index columns cheaply reject non-overlaps (pushed
down, before the row fetch); and the exact predicate removes the over-cover of partial cells (a point in `0311`
just past the hypotenuse passes the bbox but fails `ST_Contains`).

### Partitioned tables: global vs local

The local-vs-global index choice is the general TiDB secondary-index tradeoff, not
spatial-specific. What is spatial-specific is that a pure spatial predicate constrains
the geometry, never the (non-spatial) partition key, so it cannot drive partition
pruning. Therefore a **local** (per-partition) spatial index fans out to all partitions
by default, while a **global** spatial index (one Hilbert-ordered namespace across all
partitions, `partition_id` in the key per the existing global-index V2 encoding) keeps
fanout proportional to the query's spatial extent. The exception is a workload whose queries always co-constrain the
partition key (e.g. multi-tenant partitioned by `tenant_id`, queried `WHERE tenant_id =
N`), where a local index prunes and is preferable. Range-partitioning a table on the
Hilbert value to gain pruning is redundant with TiKV's automatic Region range-
partitioning of the ordered keyspace, so a partitioned spatial table is effectively
always partitioned by a non-spatial key. This reuses TiDB's existing global-index
machinery (`docs/design/2020-08-04-global-index.md`).

### Phasing

1. **Phase 1, points-only MVP** (detailed in `docs/design/spatial-index/PLAN-points-mvp.md`):
   a `POINT`, `NOT NULL`, SRID-constrained column, one entry per row, modeled as an
   expression index on a hidden virtual generated column `tidb_spatial_key(position)` so
   it reuses expression-index DDL and the non-MVI write path. SRID 0 first, then 4326.
   Non-partitioned tables.
2. **Phase 2, generic geometry**: multi-cell covering via MVI and
   `ST_Intersects`/`ST_Contains` on polygons and linestrings. Concretely, an
   **array-valued** hidden generated column `tidb_spatial_keys(col)` returns the covering
   cell keys as an array, indexed as a multi-valued index (`CAST(... AS CHAR ARRAY)`), and
   queried through **IndexMerge** (`json_overlaps` against the query cells) with handle
   dedup and the exact refine. This rides the existing JSON-array MVI + IndexMerge path
   rather than generalizing `getIndexedValue` or relying on a direct multi-valued
   `IndexReader` (whose duplicate-handle assumptions the planner avoids). The bbox columns
   (`minx,miny,maxx,maxy`) are appended to the index key so the bbox pre-filter pushes to
   the coprocessor via the existing index-filter machinery (no new operator), as for the
   point index.
3. **Phase 3, partitioned tables**: global spatial index, reusing TiDB's existing
   global-index encoding (partition id in the key for non-unique non-clustered indexes,
   per the V2 work #65289), not a spatial-specific scheme.
4. **Later**: coprocessor pushdown of the refine predicate, expanding-ring kNN operator,
   and a TiFlash columnar spatial path (the coverer is kept engine-neutral to allow it).
5. **Very late, not required for release**: composite (prefix-column) spatial indexes,
   e.g. `SPATIAL INDEX (tenant_id, position)`, which prepend scalar equality-prefix
   columns before the cell key so `WHERE tenant_id = ? AND <spatial>` prunes to one
   prefix value and the spatial covering. Useful for multi-tenant/categorized data (an
   in-index alternative to partition-by-tenant), but explicitly out of the release scope.

### Key integration points

(Verified against current code; see `docs/design/spatial-index/PLAN-points-mvp.md` for
line-level detail.) Expression index / hidden column: `pkg/parser/ast/ddl.go`,
`pkg/meta/model/column.go`, `pkg/ddl/index.go`. Index metadata: `pkg/meta/model/index.go`
(`IndexInfo`, `MVIndex`). Predicate-to-range and access path: `pkg/util/ranger/detacher.go`,
`pkg/planner/core` (`getPossibleAccessPaths`, `skylinePruning`), `pkg/planner/util/path.go`.
Index KV write: `pkg/table/tables/index.go` (`GenIndexKVIter`, plain vs multi-valued
generator), `pkg/tablecodec/tablecodec.go`. Builtin registration: `pkg/expression/builtin.go`.

### Compatibility

- **Partition table**: a spatial index on a partitioned table should be global (see
  above); local is valid only for partition-key-co-constrained workloads.
- **Clustered index**: the spatial index is a secondary index; it does not change the
  table's clustering. A clustered-by-geometry organization is an explicitly rejected
  alternative (see Investigation).
- **Parser/DDL**: new `SPATIAL INDEX` surface (or `USING`), internally an expression
  index plus spatial metadata on `IndexInfo`. DDL backfill reuses the index backfill
  framework.
- **Planner/statistics**: a new access path and predicate recognition; the selectivity/cost
  model accounts for the candidate-superset behavior, the MVI duplicate-factor from fan-out
  (MVI row/NDV can exceed table row count, already noted in `pkg/statistics/analyze.go`),
  the cover/bbox false-positive ratio (the PoC measured ~2.25x at level 16), and a
  range-count cap. A diagnostic surface, a function or `EXPLAIN` detail that shows the
  cells/ranges a query generates (as CockroachDB exposes), makes the covering tunable and
  debuggable.
- **Executor**: no new operator. The exact refine is a retained `Selection` over a plain or
  multi-valued index scan; the **bbox pre-filter is on index columns**, so it pushes to the
  coprocessor through the existing index-filter machinery and runs before the row lookup
  (Layer A). A point index can also reconstruct the point from its `x,y` index columns to
  refine without a lookup (a covering access). The only part needing new machinery is pushing
  the **exact** `ST_*` predicate to the coprocessor (Layer B: tipb sigs + a TiKV/TiFlash
  evaluator), a later, optional increment; Layer A already delivers most of the win with no
  protocol or TiKV change.
- **TiKV**: no storage-engine change; entries are ordinary ordered keys. Coprocessor
  pushdown of refine is a later, optional enhancement.
- **TiFlash/BR/TiCDC/Dumpling**: the index is regular index data; tools that handle
  secondary indexes need no special casing beyond understanding the geometry value
  (which is the prerequisite type's concern). TiFlash spatial is out of scope here.
- **Upgrade/Downgrade**: the index metadata is additive. Downgrade to a release without
  spatial-index support requires dropping spatial indexes first (same as other newer
  index kinds); to be confirmed during implementation.

## Test Design

### Functional Tests

- Coverer unit tests: a point encodes to one cell; `CoverQuery` of a region returns
  ranges that include the cell of every contained point (zero false negatives vs a
  brute-force check over random points), for both SRID 0 and 4326.
- Result-equivalence: for seeded tables, distance-within, `ST_Contains`/`ST_Within`, and
  `ST_Intersects` queries return identical rows with the index dropped and created.
- Plan tests: `EXPLAIN` shows a spatial index range scan plus a refine filter instead of
  a full scan; the index-entry count equals the expected per-row covering-cell count.
- Cross-level covering: mixed-level coverings match equal/ancestor/descendant stored cells
  with no false negatives (property test over random points), for both SRIDs.
- Prepared/parameterized queries: a spatial query with bound parameters returns the same
  rows as the literal form across executions (ranges rebuilt per execution), and the
  plan-cache decision (reuse vs non-cacheable) behaves as specified.
- Diagnostics: the covering diagnostic / `EXPLAIN` detail reports the cells and ranges a
  query generates.

### Scenario Tests

- Proximity ("stores within 10 km of a point", including `ORDER BY distance LIMIT k`).
- Geofence (point-in-polygon over a region table).
- SRID 4326 edge cases: a query region crossing the antimeridian and one near a pole.
- Multi-tenant partitioned table: per-tenant spatial query (local index prunes) and, in
  Phase 3, cross-tenant spatial query (global index).

### Compatibility Tests

- Partition table (local and global), clustered and non-clustered tables, charset/
  collation irrelevance for the geometry value, async commit.
- Parser/DDL/planner/statistics/executor as listed in Compatibility.
- External components: Dumpling/Lightning round-trip of a table with a spatial index;
  TiCDC and BR pass-through; behavior unaffected when TiFlash is absent.
- Upgrade and downgrade paths.

### Benchmark Tests

- Candidate-set precision (false-positive ratio after the cell range scan and after the
  bbox pre-filter) on representative point data (e.g. the Capital Bikeshare dataset used
  in TiDB docs), to tune cell depth and max-cells.
- Write amplification (index entries per row; one for points, bounded for extents) and
  insert/update throughput vs an unindexed table.
- Query latency and rows-scanned vs a full table scan, across selectivities.

## Impacts & Risks

Impacts (intended): proximity and containment queries on large geometry tables become
selective index scans instead of full scans; for points, write amplification is one
index entry per row.

Risks:

- **Covering tightness**: too-coarse cells inflate the candidate set; too-fine cells
  inflate fan-out and index size. Needs measurement; mitigated by the bbox pre-filter.
- **Geometry update cost**: updating a geometry rewrites its covering-cell entries
  (bounded for points). The clustered-by-geometry alternative would be far worse, which
  is one reason it is rejected.
- **Prerequisite coupling**: the index depends on the geometry type and `ST_*` functions
  landing; mitigated by coding against a thin accessor so churn stays localized.
- **Refine library gaps**: exact `ST_*` predicates use `simplefeatures` (pure-Go OGC/DE-9IM,
  validated byte-identical to MySQL in the PoC); a coprocessor port (Layer B) must match it.
- **Global index costs** (Phase 3): cross-partition writes and partition-DDL cleanup are
  the usual global-index costs, inherited because global is the spatial default.
- **SRID 0 domain**: data outside the configured bounds is clamped to the boundary cell
  (over-covered near the edge), a performance, not correctness, risk.

## Investigation & Alternatives

A full survey is in `docs/design/spatial-index/research.md`. Summary:

- **R-tree / GiST** (MySQL, MariaDB, PostGIS): excellent on a single node, including
  native kNN, but the overlapping-node tree does not shard onto TiKV's ordered keyspace
  and needs in-place rebalancing. Not viable as a distributed TiDB index.
- **ER-tree** ("An LSM-Tree Index for Spatial Data", He & Chen, Algorithms 2022,
  15(4):113): a Hilbert R-tree embedded in LSM SSTables. It handles generic geometries
  with one entry each (extent in MBRs) and avoids dual-index lookups, but only because
  the data is stored **clustered** by `Hilbert(geometry)`. As a clustered organization
  it consumes the table's single clustering choice and makes geometry updates expensive;
  as a TiDB secondary index it would require maintaining a standalone Hilbert-packed
  R-tree, reintroducing tree maintenance on ordered KV. Recorded as the alternative for
  a future clustered/storage-engine-native spatial index, not chosen here.
- **Clustered Hilbert organization vs secondary index**: clustered removes the row
  lookback but costs the clustering slot and expensive geometry updates; this proposal
  chooses the secondary index, which leaves the table clustered as the user wants and
  keeps geometry updates cheap, at the cost of a lookback (mitigated by the bbox
  pre-filter and the optional covering value).
- **Geohash via generated column + plain index** (MySQL `ST_Geohash` style): works for
  points but is the points-only special case of this design; this proposal makes it
  automatic and extends it to generic geometries.

## Unresolved Questions

- User-facing DDL grammar: resolved to MySQL-compatible `SPATIAL INDEX`/`SPATIAL KEY`. Any
  non-default cell tuning would be added later as bare `NAME = value` index options
  (emitted inside a `/*T![spatial] ... */` comment), not in the initial release (see
  Detailed Design -> SQL syntax). Remaining: confirm the exact MySQL `SHOW CREATE TABLE`
  byte-form to match, and whether tuning options ship in the first cut or as a follow-up.
- `NOT NULL` requirement: required initially for MySQL parity, but could be relaxed in
  the future. The cell-covering/MVI encoding represents a NULL geometry as zero index
  entries (NULL rows unindexed, and they never satisfy a spatial predicate, so no false
  negatives), so lifting the restriction is feasible; deferred.
- Index value contents: the bbox/coords now ride in the **key** (index columns), so the
  value is empty by default. A *covering* index may still store the full EWKB in the value
  to refine without a row fetch, an optional size-vs-speed choice; the
  index-size/write-amplification budget for it.
- Where the bbox pre-filter runs: **resolved**, the bbox is in index columns, so the
  existing index-filter machinery pushes it to the coprocessor and runs it before the row
  lookup (Layer A), no new operator or protocol change.
- Exact-predicate pushdown (Layer B): the tipb `ScalarFuncSig` set for the `ST_*` relate
  predicates and a matching TiKV (Rust) / TiFlash (C++) evaluator, with a cross-engine
  conformance suite (see `research.md` -> "Exact refine location and coprocessor
  pushdown"); a later, optional increment.
- Prepared statements / plan cache: rebuild covering ranges at execution from the
  parameters (preferred, like parameterized index ranges) vs marking spatial plans
  non-cacheable.
- Geographic `ST_Distance` (ellipsoidal) optimization: the exact conservative inflation
  factor for the spherical-cap cover (WGS 84 deviation bound), and whether to optimize it
  initially or ship `ST_Distance_Sphere`-only.
- Global vs local policy for partitioned tables: default to global, allow local, or
  choose by detected workload (pure-spatial vs partition-key-co-constrained); whether to
  warn when a local spatial index cannot prune.
- `partition_id` encoding (future, Phase 3): reuse the existing global-index encoding
  verbatim (partition id in the key for non-unique non-clustered indexes, per the V2 work
  #65289).
- Phase 1 carries the point's `x,y` as additional hidden generated index **columns** (a
  normal composite index over `(cell_key, x, y)`), so the bbox/coords are in the key and the
  value stays empty; no index-value-generation extension is needed.
- Cell-depth and max-cells defaults; SRID 0 default bounds and out-of-domain behavior.
- S2 library adoption (`github.com/golang/geo`) vs a minimal in-house spherical coverer.
- Whether a clustered spatial table is ever a target use case (would revive the ER-tree
  direction).
- 3D: 2D is required (MySQL/MariaDB are 2D-only; PostGIS has 3D/4D with ND-GiST). The
  cell-key encoding is kept dimension-tagged to allow a future 3D coverer, but 3D is out
  of scope and gated on the type carrying a Z coordinate.
