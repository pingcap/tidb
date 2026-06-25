# Spatial Index for TiDB: Research and Survey

- Author(s): Mattias Jonsson
- Status: Research / pre-implementation
- Related tracking issue: https://github.com/pingcap/tidb/issues/6347
- Companion ExecPlan: `docs/design/spatial-index/PLAN.md`
- Date: 2026-06-23

This document is the knowledge base behind the spatial-index ExecPlan. It surveys
how other databases index spatial data, why TiDB's architecture rules some of
those options out, and what design fits TiDB/TiKV. It deliberately covers more
breadth than the plan executes, so that decisions in the plan can be traced back
to evidence here.

The **scope of the actual project is the index only**. The geometry data type and
spatial functions are treated as a prerequisite (see "Prerequisite scaffolding"),
not as deliverables of this effort.

## Table of contents

* [Why this matters](#why-this-matters)
* [Prerequisite scaffolding (the geometry type)](#prerequisite-scaffolding-the-geometry-type)
* [What real users actually use](#what-real-users-actually-use)
* [2D vs 3D across MySQL, MariaDB, PostGIS](#2d-vs-3d-across-mysql-mariadb-postgis)
* [Index type survey](#index-type-survey)
* [The reference paper: "An LSM-Tree Index for Spatial Data" (ER-tree)](#the-reference-paper-an-lsm-tree-index-for-spatial-data-er-tree)
* [Why TiDB cannot use an R-tree](#why-tidb-cannot-use-an-r-tree)
* [The TiDB mechanism: cell covering over multi-valued indexes](#the-tidb-mechanism-cell-covering-over-multi-valued-indexes)
* [Synthesis: Hilbert-key index vs cell-covering MVI are the same design](#synthesis-hilbert-key-index-vs-cell-covering-mvi-are-the-same-design)
* [Worked example: a spatial index on table t](#worked-example-a-spatial-index-on-table-t)
* [Index value contents and table partitioning (Sunny Bains review)](#index-value-contents-and-table-partitioning-sunny-bains-review)
* [SRID cell scheme](#srid-cell-scheme)
* [Query support matrix](#query-support-matrix)
* [Keeping the design reusable for TiFlash](#keeping-the-design-reusable-for-tiflash)
* [Investigation and alternatives: the ER-tree (LSM-embedded R-tree)](#investigation-and-alternatives-the-er-tree-lsm-embedded-r-tree)
* [Open research questions and risks](#open-research-questions-and-risks)
* [References](#references)

## Why this matters

Geospatial support is one of the most requested TiDB features (issue #6347 carries
the `feature/accepted` label and ranks among the top issues by 👍 reactions).
The dominant real-world workload is simple: store points (latitude/longitude) and
answer "what is near me" or "which region is this point in". Concretely, bike-share
and parcel-delivery style applications storing a location per row and querying by
proximity or containment.

A geometry type plus a few functions already lets users *store and retrieve*
locations. The index is what turns proximity and containment queries from full
table scans into selective lookups once the point tables get large. That is the
value this project delivers.

## Prerequisite scaffolding (the geometry type)

This project does not implement the geometry type or user-facing functions. It
depends on a minimal scaffolding that must exist (or be revived) first. The prior
art for that scaffolding lives in issue #6347 and its closed PRs:

- `docs/design/2022-10-27-geospatial.md` (PR #38916, closed): Daniël van Eeden's
  staged geospatial design. It explicitly defers the spatial index ("What's needed
  to fully support geospatial indexes in TiDB/TiKV needs more research"), which is
  exactly the gap this project fills.
- PR #66602 (closed 2026-05-22): parser support for the spatial data types and the
  `SRID` column option, with a rejection check in `pkg/planner/core/preprocess.go`.
  The doc notes the type can be enabled "by removing the check for `TypeGeometry`".
- PR #38611 (closed) and tikv/tikv#13652 (closed): the actual `GEOMETRY` column
  type implementation on the TiDB and TiKV sides.

Key facts inherited from that prior art, which constrain the index design:

- **Storage format is EWKB**: a 4-byte little-endian SRID prefix followed by the
  standard OGC WKB encoding, i.e. `<srid><wkb>`. This is what MySQL uses. Example
  for `POINT(1 1)` with SRID 4326:

  ```
  E6100000 0101000000 000000000000F03F 000000000000F03F
  ^SRID    ^byteorder+WKBtype(POINT)   ^X=1            ^Y=1
  ```

- **One MySQL type, subtype as constraint**: all geometry types are
  `mysql.TypeGeometry` (`0xFF`); `POINT`, `LINESTRING`, etc. are the same storage
  type with a geometric-subtype constraint, mirroring MySQL's `Field::geometry_type`.
- **Library**: the prior design adopts `github.com/twpayne/go-geom` (BSD-2) for the
  basic geometry types and WKT/WKB parsing. It is adequate for parsing and bounding
  boxes but weaker than Boost.Geometry for exact predicates/distance, so the exact
  refine step may need additional code or libraries.

**The contract the index needs from the scaffolding** (and nothing more):

1. A stored geometry value the index can read (EWKB bytes + SRID).
2. The ability to compute a geometry's 2D minimum bounding rectangle (MBR) and to
   enumerate its constituent shape for covering. `go-geom` provides bounds.
3. At least the predicates the index optimizes, available as expressions the
   planner can recognize: an intersects/contains style relation and a
   distance-bound predicate. Exact evaluation can live in TiDB initially.

The geometry type and basic functions are expected to **land independently** in
master. This project codes against that landing type and the contract above,
anticipating minor churn (signatures, package paths) before it merges. The only
coupling to the type is the small contract above, kept behind the `Geometry`
abstraction in the covering package, so churn stays localized.

## What real users actually use

The spatial surface is large (~70 `ST_*` functions), but real workloads cluster
tightly. In priority order:

1. **Point storage** (`POINT`), and `POLYGON` for regions/geofences.
2. **Proximity**: "within X meters/units" via a distance bound, and
   `ORDER BY distance LIMIT k` over a bounded candidate set.
3. **Containment**: point-in-polygon (`ST_Contains` / `ST_Within`), the geofence
   query.
4. **Intersection**: `ST_Intersects`, plus the MBR family that is the bounding-box
   form of these relations.

The index must accelerate exactly these. Everything else (set operations, validity
checks, accessors like `ST_X`/`ST_Area`) is per-row computation with no index story,
the same as in MySQL.

## 2D vs 3D across MySQL, MariaDB, PostGIS

| System | Z (3D) support | Spatial index dimensionality |
|---|---|---|
| MySQL | No, 2D only (no `POINTZ`, no `ST_Z`) | R-tree on the 2D MBR |
| MariaDB | No, 2D only | R-tree on the 2D MBR |
| PostGIS | Yes: XYZ, XYM, XYZM (3D/4D) | GiST on 2D MBR by default; **ND-GiST** (`gist_geometry_ops_nd`) indexes Z too |

Implications for this project:

- For **MySQL/MariaDB compatibility, 2D is the entire requirement.** This is the
  must-have.
- **3D indexing is gated on the type supporting a Z coordinate first**, which the
  EWKB scaffolding (2D) does not. So a 3D index is doubly out of scope: it would
  need type-layer changes this project does not own.
- The pragmatic compromise: make the **cell-key encoding dimension-tagged** so a
  future 3D coverer (octree / 3D space-filling curve) can be added without a
  storage-format break, but build only the 2D coverers now. Even MySQL/PostGIS
  index the 2D footprint of 3D geometries by default, so a 2D index over a future
  3D type is still useful.

## Index type survey

### R-tree / GiST (what MySQL, MariaDB, PostGIS use)

A balanced tree of nested minimum bounding rectangles; internal nodes bound their
children, leaves point to geometries. Insert chooses the least-enlargement subtree
and splits full nodes; search descends every child whose box intersects the query;
k-nearest-neighbour uses a best-first priority queue keyed on distance-to-box.

- Benefits: tight per-geometry MBRs (few false candidates), adapts to data
  distribution, **native sorted KNN with early stop**, handles mixed geometry sizes.
- Drawbacks: overlapping nodes degrade with dense data; inserts/deletes rebalance;
  concurrency and especially **distribution are hard** because the overlapping-node
  tree does not shard into independent ordered key ranges.

PostGIS also offers SP-GiST (a space-partitioned quadtree/kd-tree) and BRIN
(block-range) as alternatives, but GiST/R-tree is the default and the reference.

### Quadtree / Z-order / Geohash

Recursively split a 2D plane into four quadrants; a cell ID is the path from the
root, equal to the bit-interleaving (Morton/Z-order code) of the coordinates.
Geohash is a base-32 encoding of that. MySQL even exposes `ST_Geohash()`, which
lets users fake a spatial index today via a generated column plus a regular index,
but a prefix index on geohash does not correctly answer arbitrary region queries
(antimeridian, variable precision, no true range covering).

- Benefits: trivial to implement, cheap, maps directly onto ordered KV keys, no
  external dependency.
- Drawbacks: planar only; on lat/long it distorts toward the poles, breaks at the
  ±180° antimeridian; the Z-curve "jumps" across space (mediocre locality). Hilbert
  ordering fixes locality at the cost of more complex code.

### S2

Essentially "a quadtree done right for the sphere": project the sphere onto the six
faces of a cube, run a quadtree per face, order cells along a Hilbert curve, encode
as 64-bit cell IDs.

- Benefits: genuinely spherical (poles and antimeridian handled natively),
  near-uniform cell area across the globe, excellent Hilbert locality. Battle-tested
  (CockroachDB, MongoDB `2dsphere`, Google Maps).
- Drawbacks: external library and more conceptual weight; overkill for a purely
  planar abstract universe.

## The reference paper: "An LSM-Tree Index for Spatial Data" (ER-tree)

This document refers repeatedly to "the paper" and to "the SER-tree key idea". Both
mean the following work, introduced here once so later references are unambiguous.

**Citation.** Junjun He and Huahui Chen, "An LSM-Tree Index for Spatial Data",
*Algorithms* 2022, 15(4), 113. Faculty of Electrical Engineering and Computer Science,
Ningbo University, Ningbo, China. Open access (CC BY), MDPI.
DOI: 10.3390/a15040113. Online: https://www.mdpi.com/1999-4893/15/4/113 .
It was implemented on RocksDB for the paper's experiments; no public source repository
was found, so the implementation appears to be an unreleased research prototype. The
paper is also cited in dveeden's earlier TiDB geospatial design doc
(`docs/design/2022-10-27-geospatial.md`).

**Background terms.** An *LSM-tree* (log-structured merge-tree) is the write-optimized
storage structure used by RocksDB and therefore TiKV: writes are batched in memory and
flushed to immutable on-disk files called *SSTables*, which are later merged by
compaction. A plain LSM-tree can only be queried by key, not by a 2D spatial predicate.

**What the paper proposes, at a high level.** It is a *Hilbert R-tree embedded in an
LSM-tree*, built from two pieces:

- The **SER-tree** ("embedded R-tree on an SSTable") is the key idea. The data is keyed
  by the Hilbert space-filling-curve value of its coordinates, so each SSTable is stored
  in Hilbert order; a compact R-tree is then bulk-loaded over that SSTable's own data
  blocks, with node MBRs (minimum bounding rectangles) capturing spatial extents. The
  Hilbert value is only a build-time packing key; the MBRs are what answer spatial
  queries. Because the index lives inside the SSTable next to the data, a query needs no
  second lookup into a separate index (it avoids "dual-index" read amplification).
- The **ER-tree** ("embedded R-tree" on the whole LSM) links the per-SSTable SER-trees
  across levels with a linked list, plus an index-build decider that skips short-lived
  SSTables, so the structure stays cheap to maintain under compaction.

It supports point, rectangular-range, and circle/radius queries (not kNN). When this
document says **"the SER-tree key idea"** it means the per-SSTable embedded Hilbert
R-tree above; when it says **"the paper"** it means this work as a whole.

Whether this fits TiDB (it is fundamentally a *clustered* organization, and TiKV's
distributed architecture already provides much of its machinery) is assessed in
"Investigation and alternatives: the ER-tree" below; that section is the decision, this
one is just the reference and explanation.

## Why TiDB cannot use an R-tree

MySQL, MariaDB, and PostGIS all run their spatial index inside a single-node storage
engine, where an R-tree's in-place tree rebalancing and best-first traversal are
natural. TiDB stores data in TiKV, a **distributed, range-sharded, globally ordered
key-value store**. An R-tree's overlapping-node structure does not linearize onto an
ordered keyspace and does not shard into independent ranges, so it is the wrong
shape for TiKV.

The proven alternative for a distributed ordered KV store is a **space-filling
curve**: linearize 2D proximity into 1D key proximity, so spatial queries become
range scans over the existing index machinery. This is a long-established, general
technique: space-filling curves (Hilbert, Morton/Z-order), geohash, and Google's
open-source S2 cell library are decades-old or widely-used building blocks applied
across many systems. **CockroachDB** is one independent precedent worth noting because
it applies the same general approach on an ordered KV store comparable to TiKV,
decomposing geometries into S2 covering cells stored as an inverted-style index. A
different academic line of work, the paper's ER-tree (see
"The reference paper" above), instead embeds R-trees inside LSM storage rather than
abandoning them; it is assessed as a deferred alternative in "Investigation and
alternatives" below.

## The TiDB mechanism: cell covering over multi-valued indexes

TiDB already has the exact primitive needed: the **multi-valued index (MVI)**, where
one row fans out to many ordered index entries. Today MVI backs JSON array
membership (`MEMBER OF`, `JSON_CONTAINS`, `JSON_OVERLAPS`): each array element
becomes a separate index entry (`pkg/meta/model/index.go` `MVIndex`, set in
`pkg/ddl/index.go` when an indexed column has `IsArray()`).

A spatial index maps onto this directly:

- **Index time**: cover each geometry with a set of hierarchical cells and write one
  index entry per covering cell. A point maps to one leaf cell; a polygon maps to a
  handful of cells. One row, many cell keys: the MVI fan-out shape.
- **Query time** (filter-and-refine, the standard spatial query pattern):
  1. Cover the query region (a polygon, or a distance-bounded disc/cap) with cells.
  2. Range-scan those cell-key ranges to get a candidate row set.
  3. Refine each candidate with the exact geometry predicate to drop false
     positives (cells are coarse, so the candidate set is a superset).

Correctness lives in the refine step; the index only ever returns a superset. The
exact predicate can be evaluated in TiDB at first, and pushed to the TiKV
coprocessor later for efficiency.

Note: the literal "inverted index" name in TiDB today (`ColumnarIndexTypeInverted`)
is a **TiFlash columnar** index for integer columns, unrelated to this. The TiKV
fan-out primitive we reuse is the MVI.

## Synthesis: Hilbert-key index vs cell-covering MVI are the same design

A natural proposal is a secondary index keyed by `Hilbert(geom) + PK` (the SER-tree
key idea, minus the clustered storage), reading the full row back via PK. Compared to
cell-covering over MVI, this is not a separate design:

- **For points they are identical.** A finest-level quadtree/S2 cell id *is* the
  Morton/Hilbert position of that cell, so `(cell_id, PK)` and `(hilbert_value, PK)`
  are the same entry, one per point, sorted by the curve. Both query by covering the
  window into curve ranges, both produce candidate PKs, both lookback by PK and refine.
- **They diverge for extended geometries, and there are three distinct cases, do not
  conflate them.**
  - *MBR-less `Hilbert(geom)+PK`* (a plain key, no tree, queried by Hilbert range): a
    single Hilbert value collapses a polygon to its representative point and loses the
    extent, so containment/intersection answers are wrong. This variant is genuinely
    **points-only**.
  - *The paper's SER-tree* is **not** this. It is a Hilbert R-tree: the Hilbert value is
    only a build-time packing key (sort by centroid, bulk-load bottom-up for tight
    MBRs), and the extent of each polygon/line is stored in the node **MBRs**. Queries
    descend by MBR (`include`/`intersects`), not by Hilbert range. So the paper handles
    generic geometries and indexes each one as a **single** entry (no fan-out), the
    extent living in the MBR.
  - *Cell-covering MVI* also handles generic geometries, but by **covering an extent
    with multiple cells** (fan-out, one entry per cell) and pruning via ordered-key
    range scans rather than tree descent.
  The paper's no-fan-out advantage is tied to its embedded/clustered layout; as a
  secondary index on TiKV (base table not reorderable) it would require a standalone
  Hilbert-packed R-tree to maintain, reintroducing the tree-maintenance-on-ordered-KV
  problem the paper's embedding was designed to avoid. Cell-covering MVI accepts fan-out
  in exchange for needing no tree, the LSM/TiKV-native trade.
- **The SER-tree's local block-MBR pruning is orthogonal.** Because both indexes are
  curve-ordered, per-block MBR zone-maps can decorate either to skip blocks inside a
  scanned curve-range but outside the exact window, allowing a coarser covering with
  less over-read. It is a refinement to borrow, not a competing structure.

Therefore "mixing the ideas" yields the cell-covering MVI secondary index using a
Hilbert curve for cell ordering, optionally augmented with per-block MBR zone-maps,
not a third design.

Complexity ladder (pick by workload):

1. **Points-only**: a plain secondary index on a generated `Hilbert(geom)` column, one
   entry per row, no MVI machinery needed. This is dveeden's geohash-via-generated-column
   note made automatic; the missing piece is planner support to rewrite spatial
   predicates into curve-range scans.
2. **General geometries**: cell-covering over MVI (Hilbert curve, optional block MBRs).
3. **No lookback wanted**: clustered Hilbert PK (the ER-tree direction), at the cost of
   expensive geometry updates and the table's single clustering slot.

## Worked example: a spatial index on table t

Consider:

```sql
CREATE TABLE t (id bigint primary key, position geometry, load json);
```

`id bigint primary key` makes `t` a clustered table, so rows live at
`t{T}_r{id} -> (position, load)` and `id` is the handle. The spatial index (index id
`X`) is a separate keyspace. Sample rows: a point `id=1 POINT(30 40)` and a polygon
`id=3 POLYGON(...)`. `H(...)` denotes a Hilbert value, `cell(...)` a covering-cell id
(encoding level + space-filling-curve position).

### Query-path vocabulary

These shorthand pipelines are used below and in the comparison table.

- **Cover**: turn the query region (a window rectangle, or a distance-bounded disc on a
  plane / cap on the sphere) into the set of index keys that could hold matches,
  covering cells for the cell/MVI index, covering Hilbert ranges for a Hilbert index.
- **Range-scan**: scan the index over those cells/ranges to collect candidate handles.
- **Dedup**: one geometry can be indexed under several covering cells, so the same
  handle can surface from multiple ranges; collapse duplicates so each row is fetched
  once.
- **Lookback**: fetch the full row `t_r_id` for each candidate handle to obtain the real
  `position` (and `load`); the index stored only cell ids + handle, not the exact
  geometry.
- **Refine**: evaluate the exact predicate (`ST_Contains`/`ST_Intersects`/exact
  distance) on the real geometry, dropping the false positives the coarse covering let
  through. The index returns a superset; refine makes it exact.
- **MBR descent**: the R-tree alternative to cover+range-scan, walk the tree from the
  root visiting only children whose MBR intersects/contains the query, down to leaves,
  yielding candidate handles.
- **MBR/Hilbert prune, no lookback**: in the clustered variant the row is stored in
  spatial order, so pruning (Hilbert range and/or local block MBRs) selects rows
  directly; the scan already reads the full row, so there is no separate fetch.

### MVI approach (ordered-KV / TiKV-native)

Points-only is a plain secondary index on the computed value `H(position)` (one entry
per row, MVI machinery not needed):

```
t{T}_i{X}_{H(30,40)}_{1} -> ∅
t{T}_i{X}_{H(31,41)}_{2} -> ∅
```

Generic geometry is a true MVI: cover the geometry with a bounded set of cells and
write one entry per covering cell (a point covers to one cell, a polygon fans out):

```
-- point id=1 covers to one cell (value carries the point/bbox; see review section):
t{T}_i{X}_{cell(c0)}_{1} -> bbox(30,40,30,40)
-- polygon id=3 covers to several cells (<= max_cells), same bbox repeated:
t{T}_i{X}_{cell(ca)}_{3} -> bbox(polygon 3)
t{T}_i{X}_{cell(cb)}_{3} -> bbox(polygon 3)
t{T}_i{X}_{cell(cc)}_{3} -> bbox(polygon 3)
t{T}_i{X}_{cell(cd)}_{3} -> bbox(polygon 3)
```

Query path: cover -> range-scan -> dedup -> lookback -> refine. No tree to maintain.
The index value holds the geometry's bounding box (and, for a partitioned table, the
`partition_id`), enabling a cheap bbox pre-filter before the lookback and, optionally,
the full EWKB for a covering index that skips the lookback entirely. See "Index value
contents and table partitioning" below for the value design and the global-index
choice.

### Paper approach (Hilbert R-tree)

As a secondary index keeping `id` as PK, it is a standalone Hilbert-packed R-tree:
leaf data entries sorted by `H(position)` carry each geometry's MBR, plus R-tree node
entries (which the MVI design does not have).

```
-- leaf data entries (one per row, both points and polygons):
t{T}_i{X}_data_{H(30,40)}_{1} -> MBR=(30,40,30,40)   -- point: degenerate box
t{T}_i{X}_data_{H(...)}_{3}   -> MBR=bbox(polygon 3)
-- R-tree node entries:
t{T}_i{X}_node_{n17}  -> [(cMBR_a, ->n1), (cMBR_b, ->n2), ...]
t{T}_i{X}_node_{root} -> (tMBR, ->n17, ...)
```

Both points and polygons are one entry per row (no fan-out); a point is a polygon with
a degenerate MBR, the extent lives in the MBR rather than in replicated cells. Query
path: MBR descent -> lookback -> refine. The cost is maintaining the encoded R-tree
nodes (splits/packing) under updates, the awkward-on-LSM structure the paper's embedded
design avoids by tying the tree to clustered data, which is not possible here because
`t` is already clustered by `id`.

To get the paper's no-lookback benefit, `t` must be reorganized to cluster by
`H(position)+id`:

```
-- clustered variant (most faithful to the paper):
t{T}_r_{H(30,40)}_{1} -> (id=1, position, load)   -- row co-located, spatial-ordered
t{T}_i{idIdx}_{1}     -> H(30,40)+1                -- unique index to find a row by id
```

Then spatial queries need no lookback, but every geometry update moves the row and
rewrites the `id` index (and any other secondary index), and `id` lookups go through
the extra index.

### Side-by-side for table t

| | Points | Generic (polygon) | Query path |
|---|---|---|---|
| **MVI** | plain index, 1 entry `H(pos)+id` | MVI, N entries `cell+id` (fan-out) | cover -> range-scan -> dedup -> lookback -> refine; no tree |
| **Paper (secondary R-tree)** | 1 data entry + tree nodes | 1 data entry + tree nodes (no fan-out) | MBR descent -> lookback -> refine; maintains a tree |
| **Paper (clustered)** | row stored at `H(pos)+id` | same | MBR/Hilbert prune, no lookback; expensive geometry updates |

The trade is exactly this: MVI accepts polygon fan-out to stay as plain ordered-key
entries (no tree, LSM-friendly, cheap updates); the paper avoids fan-out via MBRs but
needs a tree (or the clustered reorg). For `t` keeping `id` as PK, the MVI secondary
index is the clean fit.

### Polygon walkthrough: a triangle

The point case is trivial (one cell, one entry). A polygon shows the covering, the
fan-out, and the multi-level ("zoom") search. Coordinates here use a small domain
`[0,16)²` for readability; the real domain is far larger (±2^31) but the mechanics are
identical.

Take a triangle `T` with vertices `(4,4), (8,4), (4,8)` (the region `x>=4, y>=4,
x+y<=12`). Its bounding box is `minX=4, minY=4, maxX=8, maxY=8`: a small box well
inside the domain, and the triangle fills only half of it.

Cells are squares; a cell at level L has side `domain/2^L`. With quadrant digits
`0=SW, 1=SE, 2=NW, 3=NE`, a cell id is the path of digits from the top, so a cell's
ancestors are its prefixes and its descendants share its id as a prefix. The triangle
lives inside the level-1 cell `0` = `[0,8)²` and, more tightly, the level-2 cell
`03` = `[4,8)²`. Here is that `[4,8)²` cell at unit resolution (`#` = unit cell fully
inside T, `/` = straddles the hypotenuse, `.` = outside):

```
 8 +---+---+---+---+
   | / | / | . | . |   y=7
   | # | / | / | . |   y=6
   | # | # | / | / |   y=5
   | # | # | # | / |   y=4
 4 +---+---+---+---+
   4               8
```

**Covering.** Subdivide partial cells, keep fully-inside ones, drop outside ones. At
level 3 (size-2 cells) the triangle's cell `03` splits into four:

- `030` = `[4,6)²` (bottom-left): fully inside T, kept as one cell.
- `031` = `[6,8)×[4,6)` and `032` = `[4,6)×[6,8)`: straddle the hypotenuse (partial).
- `033` = `[6,8)²`: outside (the hypotenuse only grazes its corner).

Keeping `031`/`032` whole over-covers past the diagonal. Subdividing them one more level
(size-1 cells) hugs the edge tighter, the precision-vs-fan-out knob:

- `031` -> `0310 [6,7)×[4,5)` inside, `0311 [7,8)×[4,5)` and `0312 [6,7)×[5,6)` partial
- `032` -> `0320 [4,5)×[6,7)` inside, `0321 [5,6)×[6,7)` and `0322 [4,5)×[7,8)` partial

So the covering mixes levels: one size-2 cell for the solid corner plus six size-1
cells along the edge: `{ 030, 0310, 0311, 0312, 0320, 0321, 0322 }`. (Cells the
hypotenuse only touches at a corner are dropped here for clarity; the exact refine
guarantees boundary correctness regardless.)

**Storage.** The triangle (row `id=42`) writes one index entry per covering cell, all
sharing the same handle and bbox, differing only in `cell_key`:

```
t{T}_i{X}_030_42  -> minX=4, minY=4, maxX=8, maxY=8
t{T}_i{X}_0310_42 -> 4, 4, 8, 8
t{T}_i{X}_0311_42 -> 4, 4, 8, 8
t{T}_i{X}_0312_42 -> 4, 4, 8, 8
t{T}_i{X}_0320_42 -> 4, 4, 8, 8
t{T}_i{X}_0321_42 -> 4, 4, 8, 8
t{T}_i{X}_0322_42 -> 4, 4, 8, 8
```

Ranges: all descendants of a cell are a contiguous key range (everything under `031` is
`[0310 .. 0313]`), and a cell's ancestors are its prefixes (`0`, `03`, `031`). Those two
facts drive the search.

**Search** (cover the query the same way, then for each query cell find stored cells
that are it, an ancestor, or a descendant):

- *Exact / finer query*: a point query at `(5,5)` lands in leaf cell `0303` (inside
  `030`). Its ancestors include `030`, which is stored -> candidate. This is the case
  where the query is finer than the stored cell.
- *Coarser query (descendant range scan)*: a window query covering exactly cell `031`
  range-scans `[0310 .. 0313]` and finds stored `0310, 0311, 0312` -> one candidate
  after dedup. This is the case where the query is coarser than the stored cells.
- *False positive caught by refine*: a point query at `(7.2, 4.9)` (`x+y=12.1 > 12`,
  just outside T) lands in `0311`, a partial cell we kept, so it matches -> candidate.
  The stored bbox `[4,8]²` contains the point, so the cheap bbox filter does not reject
  it; the exact `ST_Contains(T, point)` returns false and removes the over-cover of cell
  `0311`.

Pipeline: cover the query -> range-scan descendants + look up ancestors -> bbox
pre-filter -> dedup handles -> fetch rows -> exact predicate. The cross-level
ancestor/descendant matching is what makes a mixed-level covering searchable; the bbox
in the value is a cheap pre-fetch reject (most valuable when coverings are coarse); the
exact refine is always required for correctness.

## Index value contents and table partitioning (Sunny Bains review)

A design review with Sunny Bains (2026-06-25) independently confirmed the cell-covering
secondary-index architecture (same key layout, Hilbert default, cover -> scan -> dedup
-> lookback -> refine, Regions as Hilbert ranges, Hilbert as more TiKV-native than a
distributed mutable R-tree) and contributed two refinements recorded here. Neither
changes the core design; both sharpen it.

### Storing the bounding box in the index value

Rather than an empty index value, store the geometry's bounding box
(`minX, minY, maxX, maxY`) in the value. This enables a cheap pre-lookback filter:
intersect the stored bbox against the query box using only the index entry, and discard
candidates that the coarse covering cell admitted but whose actual extent does not
intersect, before fetching the row. This recovers part of the R-tree/SER-tree
MBR-pruning benefit inside the flat MVI, with no tree to maintain (a per-entry MBR
instead of a tree of MBRs). Two extensions: store an optional simplified geometry
summary for a cheaper-but-tighter pre-filter, or store the full EWKB (a covering index)
so the exact `ST_Intersects` refine needs no row fetch at all. For points the bbox is
degenerate (the point itself), so the value can simply carry the exact coordinates,
which already lets distance refine skip the lookback. Cost: larger index entries, and
the value must be rewritten when the geometry changes; for polygons the same bbox is
repeated across the geometry's covering-cell entries.

### Global vs local spatial index for partitioned tables

Two senses of "partition" both produce Hilbert-space ranges, at different layers; keep
them distinct:

- **TiKV Regions**: automatic physical sharding of the keyspace into contiguous ranges,
  not user-controlled. A global Hilbert-ordered index distributes across Regions
  naturally (each Region is a contiguous Hilbert range).
- **TiDB table partitioning (`PARTITION BY ...`)**: user-defined logical partitioning of
  a table into sub-tables, each with its own physical table id and keyspace. Indexes are
  either *local* (one per partition) or *global* (one spanning all partitions, storing
  the partition id in each entry).

The local-vs-global choice itself is **not spatial-specific**: it is the same mechanism
and tradeoff as for any TiDB secondary index on a partitioned table. A local index
stores entries per partition (a query that does not constrain the partition key must
scan every partition's local index); a global index is one namespace across all
partitions with `partition_id` in the value, scanned once, at the cost of cross-partition
lookback and the usual global-index maintenance (for example `DROP`/`TRUNCATE PARTITION`
must clean up global entries).

What *is* spatial-specific is predicate alignment. Partition pruning only helps when the
query constrains the partition key, and a pure spatial predicate constrains the geometry,
which is essentially never the partition key (tables are normally partitioned by a
non-spatial key: tenant, time, hash). So a spatial query cannot be pruned to a subset of
partitions, and a *local* spatial index therefore fans out to **all** partitions by
default. A normal secondary index often escapes this (queries may carry the partition
key, or the table is partitioned to align with the index's hot column); the only way to
make a local spatial index prune is to partition the table spatially by the Hilbert/cell
key, which is the clustered-spatial direction with its mutable-geometry cost.

Exception that confirms it is a workload property, not a spatial law: if spatial queries
are always co-scoped by the partition key (e.g. a multi-tenant table partitioned by
`tenant_id`, queried as "near me `WHERE tenant_id = 7`"), partition pruning applies and a
local spatial index within the pruned partition is fine, even preferable (no global-index
overhead). The determinant is the same as for any index: does the query carry the
partition key? Pure-spatial queries favor a **global** index (one Hilbert-ordered
namespace, `partition_id` in the value so the lookback can address `t{partition_id}_r{pk}`,
fanout proportional to the query's spatial extent rather than the partition count);
partition-key-co-constrained queries let a local index prune. Global is therefore the
stronger *default* for spatial, because pure spatial predicates do not carry the partition
key, not because the local/global machinery differs. The rows stay distributed by the
table's partitioning regardless; only the global index is unified, which is why
`partition_id` is stored in its value.

One might try to make spatial queries prune by range-partitioning the table on the
Hilbert/S2 value itself. That is redundant with TiKV, which already range-partitions the
ordered index keyspace into Regions (each Region is a contiguous Hilbert range), so
explicit Hilbert-range table partitioning re-implements at the logical layer what the
physical layer already provides. Table partitioning still serves non-spatial purposes
(multi-tenant isolation, data lifecycle via `DROP`/`TRUNCATE PARTITION`, placement and
data residency), but those keys are non-spatial. So a partitioned spatial table is
effectively always partitioned by a non-spatial key, which is exactly the case where
spatial predicates cannot prune and the global index is the default.

This is what `partition_id` means in the reviewed layout: the physical partition id of
`PARTITION BY`, **not** the primary key (which is already in the index key). TiDB
already implements global indexes for partitioned tables
(`docs/design/2020-08-04-global-index.md`), so this rides on existing machinery.

Reviewed index entry layout (generic geometry):

```
t{table_id}_i{spatial_index_id}_{spatial_key}_{clustered_pk}
  -> minX, minY, maxX, maxY, [optional geom summary], [optional EWKB if covering]
     [+ partition_id, only for a global index on a partitioned table]
```

The value's core is the bounding box; `partition_id` is appended only by a global index
on a partitioned table (for a non-partitioned table the physical table id is the table
id, so it is unnecessary).

### Open questions from this review (resolve before deciding)

These are captured so we can reach a common understanding and decide later; they are not
yet decided.

1. **Value contents**: bbox only, bbox + simplified geometry, or full EWKB (covering)?
   Fixed per index, or a `WITH` option? What index-size / write-amplification budget is
   acceptable?
2. **Where the bbox pre-filter runs**: in TiDB after the index scan, or pushed to the
   TiKV coprocessor so candidates are filtered before being returned (needs the value
   decoded coprocessor-side)?
3. **Global vs local policy**: since this is the general secondary-index tradeoff (not
   spatial-specific), the real question is the workload, are spatial queries pure-spatial
   (favor global, since they cannot prune partitions) or always co-constrained by the
   partition key (e.g. multi-tenant, where a local index prunes and avoids global-index
   overhead)? Decide whether to default to global, allow local, or choose by detected
   workload, and whether to warn when a local spatial index cannot prune.
4. **`partition_id` encoding**: reuse TiDB's existing global-index value encoding for the
   partition id, or a spatial-specific layout? (Reuse preferred.)
5. **MVP scope**: start non-partitioned only, or include global-index support from the
   start? (Recommendation: non-partitioned first, global as a tracked follow-on.)
6. **MVP mechanism wrinkle**: the points MVP models the index as an expression index on a
   hidden generated column, whose value is normally empty/handle. Carrying a bbox (or
   EWKB) in the value is not standard for a plain secondary index, so it needs either a
   small extension to index-value generation or modeling the bbox as additional stored
   index data. Clarify how the value is produced and encoded.
7. **Dedup vs bbox**: when a polygon fans out to several cells each carrying the same
   bbox, confirm dedup-by-PK happens first and the bbox filter is applied per surviving
   candidate.

## SRID cell scheme

SRID 0 and SRID 4326 are different universes and need different coverers behind one
interface. This is the central design discussion.

### SRID 0: planar quadtree over a bounded domain

SRID 0 is an abstract Cartesian plane: unitless `float64` coordinates, Euclidean
distance, and **no natural bound**. A hierarchical grid needs a bounded universe to
subdivide (an R-tree would adapt to data, but a static curve cannot). Therefore the
index fixes a coordinate domain, a configurable `[min,max]²` box, quadtree-
subdivides it to a fixed depth, and handles out-of-domain coordinates by leaving
them un-prunable (still correct, just unindexed) rather than rejecting them.

Important framing: **needing bounds is a property of the cell/space-filling-curve
approach, not of spatial indexing in general.** MySQL, MariaDB, and PostGIS have no
SRID 0 domain bounds at all, because their R-tree/GiST indexes adapt to the data
(MBRs grow to fit arbitrary doubles). We inherit the bounds requirement only because
we chose cells. CockroachDB's public documentation describes the same requirement and
is cited here as corroboration:

- Defaults `geometry_min/max_x` and `geometry_min/max_y` to `-(1<<31)` .. `(1<<31)-1`
  (the full signed int32 range, about ±2.15 billion).
- Infers tighter bounds automatically when the column's SRID is a known earth
  projection (6000+ EPSG entries).
- A shape outside the bounds still returns **correct** results; it just is not
  eliminated by the index (a performance loss, never a correctness loss).

The defaults are a safe catch-all but interact with precision: leaf-cell size =
domain-width / 2^maxLevel. With ±2^31 and level 30 that is 2^32 / 2^30 = 4 coordinate
units per leaf, fine for data in meters, but far too coarse if degrees are stored in
SRID 0 (the ±180/±90 data occupies a tiny corner of a ±2^31 domain). Hence bounds
should be an overridable per-index option for users whose coordinate range is known.

Real-world bounds worth offering as references or smart defaults:

- **Web Mercator (EPSG:3857)**: X,Y in ±20,037,508.34 m, the de-facto web-map CRS.
- **Lat/long stored as planar**: lon in [-180,180], lat in [-90,90].
- **National grids**, e.g. British National Grid (EPSG:27700): eastings 0..700,000 m,
  northings 0..1,300,000 m.

Recommendation: default to ±2^31 for safety (a generous bound covering common
coordinate systems with headroom; CockroachDB's docs use the same value), expose bounds
as a `WITH` index option so meter- or degree-scale data gets tight cells. → planar
quadtree / Z-order (Hilbert ordering optional for locality).

### Cell depth vs fan-out (write amplification)

Two distinct knobs, often conflated:

- **Max level (depth)** controls the *precision* of a cell, not how many index
  entries a geometry writes.
- **Max cells per geometry** caps the *fan-out* for extended geometries.

A **point is contained in exactly one cell at any level, so it writes exactly one
MVI entry regardless of depth.** Depth only sharpens where that single cell sits.
Fan-out above one happens only for linestrings and polygons, whose covering is a set
of cells bounded by the max-cells cap (CockroachDB's `s2_max_cells` defaults to 4;
accuracy improves up to about 10-12 cells, then plateaus). The covering mixes cell
levels to stay under the cap regardless of the polygon's size. Consequence: a
point-heavy workload (the common "store locations" case) has write amplification of
one entry per row, independent of the chosen precision.

### SRID 4326: spherical S2 cells

SRID 4326 is WGS 84 lat/long on the globe: naturally bounded, but treating it as a
flat rectangle (plain geohash) has three correctness hazards:

1. **Antimeridian discontinuity**: lon 179.9° and -179.9° are neighbours on Earth
   but far apart in a flat grid; any query region crossing it must be split.
2. **Pole distortion**: near-pole cells cover tiny real areas; coverage is wildly
   non-uniform.
3. **Distance covering**: "within 10 km" is a spherical cap, not a lat/long box;
   the box approximation is increasingly wrong at high latitude.

Given TiDB's correctness-first rule, a knowingly-wrong 4326 index is unacceptable,
so 4326 uses a true spherical cell system (S2), which handles all three natively.

### The unifying seam

Define one abstraction that the rest of the index is agnostic to:

```go
// CellKey is the encoded, order-preserving cell identifier written as an index entry.
type CellKey []byte

type CellCoverer interface {
    // Cover returns the cell keys a stored geometry is indexed under.
    Cover(geom Geometry) ([]CellKey, error)
    // CoverQuery returns the candidate cell-key ranges to scan for a query region.
    CoverQuery(region QueryRegion) ([]CellKeyRange, error)
}
```

Two implementations: `planarQuadtreeCoverer` (SRID 0) and `s2Coverer` (SRID 4326).
Everything above the seam (index-entry encoding, MVI fan-out, DDL backfill, planner
access-path selection, executor refine) is SRID-agnostic. This same seam is the
boundary that lets a future TiFlash path reuse the covering logic unchanged.

### Sequencing rationale

Deliver **SRID 0 (planar quadtree) first**: simplest coverer, and it proves the
whole distributed pipeline (DDL → MVI fan-out → key scan → refine) end to end. Add
the **S2 coverer for 4326** behind the same interface afterward, so the hard
distributed plumbing is validated before spherical-geometry complexity lands.

## Query support matrix

| Query | Index-accelerated? | How |
|---|---|---|
| `ST_Intersects`, `ST_Contains`, `ST_Within`, `ST_Covers`, `ST_Overlaps`, MBR* | Yes | Cell/region overlap → candidates → exact refine |
| `ST_Distance(g,p) < r` (bounded radius) | Yes | Cover a disc (SRID 0) / spherical cap (4326) of radius r → candidates → exact distance refine |
| `WHERE within radius ORDER BY distance LIMIT k` | Yes (filter only) | Index prunes by radius; ordinary TopN sorts the small candidate set |
| `ORDER BY distance LIMIT k` with no radius | Not directly | Cell index is not distance-sorted; needs an expanding-ring KNN operator (deferred) or a user-supplied radius |
| `ST_Area`, `ST_Length`, `ST_X`, set operations, validity | No | Per-row computation, no index story (same as MySQL) |

The leverage for proximity comes from the **radius bound**, not from `LIMIT`; the
`LIMIT` makes the downstream sort cheap. Native sorted KNN (the one capability an
R-tree gives for free) is intentionally deferred to a possible later expanding-ring
operator.

## Keeping the design reusable for TiFlash

The target is a working TiKV (OLTP) MVI implementation. To allow a future TiFlash
columnar spatial index (the TiCI / columnar-index family that today holds
Inverted/Vector/Fulltext, all TiFlash-resident) without over-generalizing now:

- Keep the `CellCoverer` and the cell-key encoding in a **shared, engine-neutral
  package** (no TiKV or TiFlash specifics). Both engines can decompose a geometry
  into the same cells.
- Model the spatial index as an index *kind* in the planner's access-path selection,
  so a future TiFlash-served spatial path can slot in alongside the TiKV path.
- Do **not** build TiFlash plumbing, coprocessor pushdown, or a second storage path
  now. The seam is the only concession to the future; everything else targets TiKV.

## Investigation and alternatives: the ER-tree (LSM-embedded R-tree)

The paper (full citation and high-level summary in "The reference paper" above)
proposes the **ER-tree**. It is **not** a pure R-tree alternative to the
space-filling-curve approach; it is a hybrid built on the same SFC foundation, with
two additions. Understanding it precisely matters, because it reframes the real
design fork for TiDB.

How it works:

- The LSM key is the **Hilbert value of the coordinates**, so the data is stored in
  the SSTable in Hilbert (space-filling-curve) order. The value is the spatial record
  (or, via WiscKey, a pointer into a value-log). This is the shared SFC foundation.
- **SER-tree (embedded R-tree on one SSTable)**: for each immutable SSTable, a Hilbert
  R-tree is bulk-loaded bottom-up over that SSTable's own data blocks. Leaf =
  `(MBR, db_ptr)` bounding one data block and pointing to it; middle = `(cMBR,
  child_ptr)`; root = `(tMBR, child_ptr, next)` where `next` links the adjacent
  SSTable's SER-tree. Bottom-up bulk loading on already-Hilbert-sorted data keeps MBR
  overlap near-minimal.
- **ER-tree**: links SER-tree roots per LSM level into a list and links the per-level
  heads into a list, so a query filters level by level by `tMBR` and skips SSTables
  that cannot match.
- **Index build decider (IBD)**: does not index every SSTable. Short-lived SSTables
  (lifetime below a measured ~50 ms threshold) and ones where estimated query-gain <
  build-cost are skipped, avoiding R-tree build cost on the churny low LSM levels.
  Compaction patches the linked list: new SSTable gets a SER-tree, the compacted one's
  is dropped.
- **Queries**: point, rectangular range, and circle/radius (center+radius → bounding
  box → rectangular query → refine). kNN / order-by-distance is not addressed.

The central design property (answering "how does the index relate to the row"): it is
**not** a separate secondary index of `(geometry, primary_key)`. The data itself is
stored **clustered by `Hilbert(geometry)`**, and the SER-tree is **embedded in the
same SSTable**, with leaves pointing to data blocks of that SSTable, not to primary
keys. A query traverses the ER-tree to a matching leaf, follows `db_ptr` to a data
block in the same SSTable, and reads the record directly: one traversal, **no lookback
to a primary index**. That co-location is the entire source of its "no dual query"
advantage.

The implication for TiDB is the real fork, and it is about clustering, not about which
binary owns the code (we control both TiDB and TiKV):

- **Secondary spatial index** (the table stays clustered by its normal primary key):
  the cell-covering-over-MVI approach fits and needs no storage-engine work, but
  carries a refine plus a handle lookback to fetch the row.
- **Clustered spatial organization** (the table is physically ordered by the
  geometry's space-filling curve, ER-tree style): eliminates the lookback, but costs
  TiKV/RocksDB engine work, competes with the table's normal primary-key clustering (a
  table can be physically clustered only one way), and Region boundaries cut across the
  per-SSTable embedded trees. Lower-confidence readiness gaps remain: the
  binary-linked-list maintenance under compaction is described conceptually rather than
  operationally, and the prototype is not public.

Two constraints sharpen what adopting the ER-tree would actually require:

- **Clustering, not a unique geometry PK.** The data must be physically clustered by
  `Hilbert(coordinates)` for the per-block MBRs to be tight (key-adjacent rows must be
  space-adjacent). The LSM key cannot be the raw Hilbert value alone (finite-resolution
  Hilbert encoding and duplicate locations collide), so it carries a unique tiebreaker,
  realistically `Hilbert(geom) + handle`, exactly as TiDB appends the handle to a
  non-unique index key. The geometry therefore need not be unique; the binding cost is
  that this consumes the table's single physical clustering choice (cluster by geometry
  XOR by the normal primary key).
- **Whole-dataset spanning vs Region sharding.** The ER-tree is a linked list spanning
  all SSTables of one LSM tree and must cover them all to be correct (an SSTable with no
  SER-tree falls back to a Hilbert key-range scan, so the SER-tree is a block-skip
  optimization on top of the SFC clustering, not the sole access path). TiKV breaks this
  assumption: a table is split into Regions by key range, distributed across stores, and
  each store's RocksDB interleaves many Regions and tables into shared SSTables. So a
  TiKV SSTable does not correspond to one table and no single global ER-tree exists. The
  clustering half ports well (Hilbert-clustered rows land spatially-near in the same
  Region, so a spatial window becomes a few clustered key-range scans), but the embedded
  whole-dataset tree would have to be reframed as per-Region spatial pruning over
  Hilbert-clustered data, an adaptation of the paper, not the paper as-is.

A Hilbert-clustered table maps onto TiKV better than the paper's machinery implies,
which upgrades the feasibility of the clustered option:

- **It splits across Regions naturally and beneficially.** Ordered by
  `Hilbert(geom)+handle`, each Region becomes a contiguous Hilbert-value range, which
  (by Hilbert locality) is a spatially compact patch. Near rows cluster into the same
  Region; splitting works as normal.
- **No global ER-tree is needed.** The ER-tree's global-spanning role is already filled
  by (1) computing the query's covering Hilbert ranges (a 2D window maps to several
  disjoint Hilbert intervals, the same CoverQuery step as cell covering) and (2)
  TiKV/PD Region-range routing, which dispatches only to overlapping Regions, the PD
  Region catalog is a coarse global index over Hilbert space. What remains optionally
  useful is a local per-Region/per-SSTable MBR block-skip (a degenerate SER-tree or a
  per-block zone-map), and even that is optional because a Hilbert-range scan plus
  per-row refine is already correct. Much of the paper's complexity rebuilds, in one
  LSM, what TiKV's distributed architecture already provides.
- **It coexists with other indexes.** The composite clustered key `(Hilbert(geom),
  handle)` acts as the handle. A unique index on the id column (needed, since clustering
  is led by Hilbert, not id) and arbitrary secondary indexes reference that composite
  handle, exactly as for any composite clustered PK in TiDB.
- **Main caveat: geometry updates are expensive.** Because secondary indexes store the
  clustered key and that key contains `Hilbert(geom)`, updating a row's location moves
  the row and forces every secondary index to update. Fine for near-immutable locations
  (addresses, parcels), costly for frequently-moving points (live vehicle/bike
  tracking). A non-clustered secondary spatial index avoids this: a geometry update
  touches only the spatial index. This is the sharpest trade-off between the two designs.

Precedent: CockroachDB, the closest distributed-SQL-on-ordered-KV analog, chose S2
cell covering over an inverted index (a secondary index) rather than a clustered
embedded-R-tree organization.

This project targets a **secondary** spatial index, so cell-covering over MVI is the
fit. The ER-tree's clustered organization is a legitimate alternative worth
revisiting if a spatial-clustered table organization becomes a goal; it is recorded
here as that alternative, not dismissed on implementation-layer grounds.

## Open research questions and risks

- **Cell depth and covering tightness**: max level (precision) and max cells per
  geometry (fan-out cap for extended geometries; points are always one entry) trade
  index size against candidate-set precision. CockroachDB's `s2_max_cells` defaults
  to 4 and plateaus around 10-12. Confirm a good default against representative data
  (e.g. the Capital Bikeshare dataset the docs already use).
- **SRID 0 domain bounds**: default proposed as `[-(1<<31), (1<<31)-1]` per axis (a
  generous bound; CockroachDB's docs use the same value), exposed as an overridable
  per-index `WITH` option; out-of-domain
  coordinates stay correct but un-prunable (no rejection). Confirm the `WITH` syntax
  surface and whether to infer bounds from any future known-SRID metadata.
- **S2 library choice**: `github.com/golang/geo` (the `s2` package), Google's
  official Go S2 port, is Apache 2.0, so adopt it for the 4326 coverer rather than
  implementing a spherical coverer in-house.
  (`halfrost/S2` is a separate experimental port, not the one to use.)
- **Exact refine location**: TiDB-side first; coprocessor pushdown to TiKV later for
  efficiency. Pushdown needs `go-geom`-equivalent predicate evaluation in TiKV.
- **`go-geom` predicate gaps**: exact `ST_Intersects`/`ST_Contains` may need code
  beyond `go-geom`; verify coverage before committing the refine step to it.
- **Statistics**: MVI row/NDV counts can exceed table row count due to fan-out
  (already noted in `pkg/statistics/analyze.go`); the optimizer's cost model for the
  spatial access path needs to account for the candidate-superset behavior.
- **DDL backfill**: covering every existing row when adding a spatial index reuses
  the index backfill framework; cost of covering large polygons at scale is a risk.
- **Index value contents and partitioning**: see the dedicated open-questions list in
  "Index value contents and table partitioning (Sunny Bains review)" above (value
  payload, bbox pre-filter location, global vs local policy, `partition_id` encoding,
  MVP scope and mechanism).

## References

- TiDB issue #6347, "Support for SPATIAL functions, data types and indexes".
- `docs/design/2022-10-27-geospatial.md` (PR #38916), dveeden's geospatial design.
- PRs #66602 (parser types + SRID), #60295 (earlier parser), #38611 (GEOMETRY type),
  tikv/tikv#13652 (TiKV GEOMETRY type).
- CockroachDB spatial indexing (S2 covering over inverted indexes):
  https://www.cockroachlabs.com/docs/stable/spatial-indexes and the engineering blog
  https://www.cockroachlabs.com/blog/how-we-built-spatial-indexing/ . Defaults: SRID 0
  geometry bounds `[-(1<<31), (1<<31)-1]` per axis, `s2_max_cells` 4, `s2_max_level` 30.
- `github.com/golang/geo` (Google's Go S2 port, Apache 2.0).
- The paper: Junjun He and Huahui Chen, "An LSM-Tree Index for Spatial Data",
  Algorithms 2022, 15(4), 113, DOI 10.3390/a15040113,
  https://www.mdpi.com/1999-4893/15/4/113 (see "The reference paper" section).
- OGC Simple Feature Access (WKB/WKT), MySQL spatial reference manual.
- TiDB MVI internals: `pkg/meta/model/index.go`, `pkg/ddl/index.go`,
  `pkg/statistics/analyze.go`.
