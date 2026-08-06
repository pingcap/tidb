# TiDB Design Documents

- Author(s): [Mattias Jonsson](http://github.com/mjonss)
- Discussion PR: https://github.com/pingcap/tidb/pull/XXXXX
- Tracking Issue: https://github.com/pingcap/tidb/issues/6347

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
    * [Types and storage](#types-and-storage)
    * [SRID model](#srid-model)
    * [Function set](#function-set)
    * [Geometry engine](#geometry-engine)
    * [Type plumbing](#type-plumbing)
    * [SQL surface and examples](#sql-surface-and-examples)
    * [Feature flag and rollout](#feature-flag-and-rollout)
    * [Scope and deferrals](#scope-and-deferrals)
    * [Compatibility](#compatibility)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

This document proposes **basic geospatial support** for TiDB: a MySQL-compatible
`GEOMETRY` type family (`POINT`, `LINESTRING`, `POLYGON`, and the multi/collection
variants), per-column `SRID`, EWKB storage, and a minimal-but-useful set of `ST_*`
functions covering I/O, accessors, measurement, and the DE-9IM spatial predicates. It
supports **SRID 0 (Cartesian plane)** and **SRID 4326 (WGS 84 geographic)** and makes
geometry values storable, queryable, and filterable — by full table scan. It is the
prerequisite layer that a spatial **index** builds on, but it is deliberately
**index-free**: geometry becomes a first-class value and query surface first, and the
index lands separately.

The scope is intentionally the smallest slice that is independently useful and GA-able,
and it is designed so later work — more SRIDs, the long tail of geometry-processing
functions, coprocessor pushdown, and the spatial index — extends it **without a new
design**. This revives and narrows the earlier geospatial design (PR #38916), which
covered a broader surface and was not merged. The spatial index is specified separately
in `docs/design/2026-06-25-spatial-index.md` (PR #69473) and depends on this layer.

## Motivation or Background

Geospatial support is one of the most requested TiDB features. Tracking issue #6347
carries the `feature/accepted` label and ranks among the top open issues by reactions.
The dominant real-world workload is simple and concrete: store a location per row
(latitude/longitude or a planar coordinate) and answer "what is near me", "which region
contains this point", or "what overlaps this box". Bike-share, ride-hailing, parcel
delivery, and asset-tracking applications all reduce to points plus proximity and
geofence queries.

TiDB has no spatial support today: only the `mysql.TypeGeometry` type constant exists
(`pkg/parser/mysql/type.go`), with no value representation and no `ST_*` functions.
Users who need geometry today must encode it into scalar columns by hand and compute
distances in the application, losing MySQL compatibility and correctness.

The earlier geospatial design (PR #38916) proposed the full surface — types, functions,
and an eventual index — but bundled too much to land, and explicitly deferred the index
as "needs more research". Two things have changed since:

1. The index research is done (PR #69473) and validated by an end-to-end proof of
   concept, which also **fully prototyped this basic layer** as its prerequisite. The
   type, EWKB storage, and a broad `ST_*` surface already work in pure Go.
2. That PoC showed the right seam: **the basic type-and-function layer is a clean,
   independently valuable milestone** that should merge on its own before the index, so
   review stays small and geometry is usable the moment it lands.

This document is that basic layer, scoped so it can ship, stabilize, and have its
feature flag removed before index work starts merging on top.

## Detailed Design

### Types and storage

The `GEOMETRY` type family follows MySQL: `GEOMETRY` (any subtype), `POINT`,
`LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`, and
`GEOMETRYCOLLECTION`. All reuse the existing `mysql.TypeGeometry` field type; the
concrete subtype is a constraint on the stored value, as in MySQL. A column may carry a
`SRID n` attribute fixing its spatial reference system (see [SRID model](#srid-model)).

The **stored value is EWKB**: a 4-byte little-endian SRID prefix followed by standard
OGC WKB (`<srid_le_u32><wkb>`), matching MySQL's internal geometry storage byte-for-byte
so that dump/reload and wire compatibility are natural. Geometry columns are stored as a
binary string kind at the KV layer; no new column encoding is introduced.

**Pre-GA value-format lock-in (decided here).** The on-disk value format is hard to
change after release without a migration, so it is settled in *this* design (the type's
owner), not the index design. Two observations from the PoC (`storage-format.md`):

- Raw EWKB carries redundancy: a per-row SRID (already implied by the column's fixed
  SRID), per-(sub)geometry byte-order flags, and WKB framing.
- A **1-byte format-version tag** keeps the format evolvable at negligible cost, and a
  flat-`f64` point layout could cheapen point decode.

**Recommendation:** ship a version-tagged value format (at minimum a leading
format-version byte) so the encoding can evolve; keep the wire/`ST_AsBinary` output
MySQL-compatible EWKB regardless of the internal form. The exact internal layout beyond
the version byte is an implementation choice measured during development; see Unresolved
Questions.

### SRID model

v1 supports two spatial reference systems, chosen to cover the two coordinate-system
*classes* that matter:

- **SRID 0 — abstract Cartesian plane.** Unitless X/Y, no coordinate-range checking
  (MySQL spans the full finite IEEE-754 double range). All functions are planar
  (Cartesian).
- **SRID 4326 — WGS 84 geographic (lat/long).** Coordinates are bounded (latitude
  `[-90, 90]`, longitude `(-180, 180]`); distance/length/area are geodesic on the WGS 84
  ellipsoid, matching MySQL.

**Coordinate-system class drives planar-vs-geodesic**, exactly as MySQL decides it from
the SRS class (SRID 0 / projected = Cartesian; geographic = geodesic). This class-based
dispatch — not a per-SRID table of special cases — is the design's extension seam: adding
SRIDs later is adding catalog rows and per-class parameters, not new code paths.

**Axis order.** MySQL's EPSG:4326 is **(latitude, longitude)** — the first coordinate is
latitude. This is verifiable from MySQL's own out-of-range error wording (`POINT(100 0)`
on 4326 errors "Latitude 100 … out of range"). v1 follows MySQL's axis order so that
`ST_Latitude`/`ST_Longitude`, distances, and WKT round-trips match. (PostGIS uses the
opposite long/lat order; that difference is documented, not adopted.) The full
convention, including GeoJSON/WKB, is captured in the PoC's `axis-order.md` and should be
folded into user docs.

**Coordinate validation.** For 4326, out-of-range latitude/longitude errors on ingest
(matching MySQL codes/wording as closely as practical), across every constructor and I/O
path (`ST_GeomFromText`, the typed constructors, `ST_GeomFromGeoJSON`, `ST_GeomFromWKB`).
For SRID 0, only non-finite overflow (Inf/NaN) is rejected, as in MySQL.

**Extension path (documented, not built here).** Later SRID expansion, layered by cost so
the cheap high-value part can land without the expensive part:

- **SRS catalog** — populate `information_schema.st_spatial_reference_systems` from the
  EPSG dataset (MySQL ships ~5,200 entries) with the per-SRS metadata the engine needs:
  class (PROJECTED vs GEOGRAPHIC), axis order, coordinate bounds, unit, ellipsoid.
  Prerequisite for everything below.
- **All PROJECTED SRSs** (e.g. 3857 Web Mercator) — low extra cost: they are planar X/Y,
  so the same Cartesian functions apply; the only per-SRS input is coordinate bounds.
- **GEOGRAPHIC SRSs beyond 4326** — moderate: exact geodesic refine per ellipsoid.
- **PostGIS-level** (`CREATE SPATIAL REFERENCE SYSTEM`, `ST_Transform` between SRSs) —
  bigger, needs on-the-fly reprojection (a PROJ-like library); explicitly out of scope.

v1 deliberately hard-restricts columns to SRID 0 or 4326 at DDL, so no partial-SRS
behavior escapes before the catalog exists.

### Function set

The v1 function set is the minimal set needed to **store, read, and query** geometry —
everything an application needs to put geometry in, get it out, inspect it, measure it,
and filter rows by spatial relationship. The geometry-*processing* tail (`ST_Buffer`,
`ST_Union`, `ST_Intersection`, …), the typed I/O aliases, the MBR predicate family,
geohash, and niche accessors are a **separate later milestone** (they are pure
expression-layer builtins, orthogonal to both this layer and the index). The
authoritative full MySQL catalog with the v1-vs-tail split is `mysql-function-catalog.md`.

v1 functions (all present in MySQL 8.0.46 / 8.4 / 9.7 — the spatial function set is
identical across those versions):

- **I/O — readers:** `ST_GeomFromText`, `ST_GeomFromWKB`, `ST_GeomFromGeoJSON`.
- **I/O — writers:** `ST_AsText` (`ST_AsWKT`), `ST_AsBinary` (`ST_AsWKB`), `ST_AsGeoJSON`.
- **Constructors (function-call syntax):** `Point`, `LineString`, `Polygon` (the
  `Multi*`/`GeometryCollection` constructors are in the tail).
- **Accessors:** `ST_X`, `ST_Y`, `ST_Latitude`, `ST_Longitude`, `ST_SRID` (getter and the
  `ST_SRID(g, srid)` setter), `ST_GeometryType`, `ST_Dimension`, `ST_Envelope`,
  `ST_IsEmpty`, `ST_IsValid`, `ST_StartPoint`, `ST_EndPoint`, `ST_PointN`, `ST_NumPoints`,
  `ST_ExteriorRing`, `ST_NumInteriorRings`, `ST_Centroid`.
- **Measurement:** `ST_Area`, `ST_Length`, `ST_Distance`, `ST_Distance_Sphere`.
- **Predicates (DE-9IM):** `ST_Within`, `ST_Contains`, `ST_Intersects`, `ST_Equals`,
  `ST_Disjoint`, `ST_Touches`, `ST_Crosses`, `ST_Overlaps`.

**PostGIS extras policy.** `ST_Covers` / `ST_CoveredBy` are PostGIS, not MySQL. They are
included because they become **index-eligible region predicates** in the index layer
(`Covers ⊇ Contains`, `CoveredBy ⊇ Within`, so a covering-cell prefilter is valid with no
false negatives). Other PostGIS-only functions are added later only if index-supported or
by demand.

Every geometry-returning builtin is typed `GEOMETRY`, so a plain B-tree functional index
over such an expression is correctly rejected (a spatial index is the index layer's job).

**Semantics match MySQL, with documented v1 limitations:**

- Planar vs geodesic follows the SRS class. On 4326, `ST_Distance`/`ST_Length` are
  ellipsoidal (Andoyer geodesic, matching MySQL to sub-metre); `ST_Distance_Sphere` is
  the great-circle sphere variant.
- `ST_Area` on 4326 is a known gap (geodesic polygon area on the ellipsoid, Karney):
  either implement it or error like MySQL's Cartesian-only functions do (see Unresolved
  Questions). It must not silently return a planar (degree²) or off-by-~0.45% spherical
  value.
- The DE-9IM predicates are OGC-correct via `simplefeatures`; on 4326 the region
  predicates use a geodesic point-in-polygon where MySQL's planar refine diverges near
  edges/poles/antimeridian. Polygon/polygon geodesic relations and geodesic `ST_Area` are
  follow-ups.

### Geometry engine

The geometry stack is **pure Go**, no cgo/libgeos:

- `github.com/peterstace/simplefeatures` — OGC/DE-9IM geometry model, WKT/WKB/GeoJSON
  I/O, predicates, and planar measurement. Validated byte-identical to MySQL in the PoC.
- `github.com/golang/geo` (Google's S2 port, Apache 2.0) — spherical geometry for the
  geodesic 4326 paths.
- A small in-tree geodesic helper (`pkg/util/geomrel`) for ellipsoidal distance/length
  (Andoyer) and the geodesic region refine.

Pure Go matters: the whole spatial stack builds with `CGO_ENABLED=0`, so there is no
libgeos dependency in the Bazel/CI sandbox. The only Bazel follow-up is adding the new
pure-Go deps to `DEPS.bzl` as proxy-fetch entries (no cc-toolchain wiring). The
geometry-*processing* tail (buffer/union/…) may later need GEOS-equivalent algorithms;
that is deferred with the rest of the tail and kept off this layer's critical path.

### Type plumbing

`TypeGeometry` must flow correctly through the generic value machinery so that geometry
behaves like any other column value outside the `ST_*` functions. The PoC audited ~28
operations (GROUP BY, hash/merge join, DISTINCT, ORDER BY, UPDATE/DELETE/REPLACE, window,
`INSERT … SELECT`, `UNION`, …); the concrete touch points are:

- `pkg/parser` — geometry type grammar and the `SRID` column attribute (regenerates
  `parser.go` once; the only grammar change, since `ST_*` functions are generic calls,
  not grammar).
- `pkg/types` / field type — the geometry field type and its flen/charset handling.
- `pkg/util/chunk` — `Row.GetDatum` must return geometry as a binary string (the PoC
  found `INSERT … SELECT` nulled geometry without this).
- `pkg/expression/builtin_cast.go` — cast-to-string flen setup (the PoC found `UNION`
  asserted without this).
- `pkg/expression` — the `ST_*` builtins (`builtin_geo.go`) and their registration.

Geometry sorts/compares/hashes as its binary EWKB string; this is well-defined but not
spatially meaningful (that is what the index and predicates are for).

### SQL surface and examples

The type and function surface is MySQL-compatible. Column definition:

    col_name {GEOMETRY | POINT | LINESTRING | POLYGON | MULTIPOINT | ...}
        [NOT NULL] [SRID {0 | 4326}]

Example:

    CREATE TABLE stores (
      id  BIGINT PRIMARY KEY,
      loc POINT NOT NULL SRID 4326
    );

    INSERT INTO stores VALUES
      (1, ST_GeomFromText('POINT(37.4 -122.1)', 4326)),          -- lat, long (MySQL order)
      (2, ST_PointFromText('POINT(37.8 -122.3)', 4326));         -- (typed alias is tail; Point()/ST_GeomFromText are v1)

    -- read back
    SELECT id, ST_AsText(loc), ST_Latitude(loc), ST_Longitude(loc) FROM stores;

    -- measure (geodesic metres on 4326)
    SELECT id, ST_Distance(loc, ST_GeomFromText('POINT(37.5 -122.2)', 4326)) AS m FROM stores;

    -- filter by spatial relationship (full scan in this layer; the index accelerates it later)
    SELECT id FROM stores
    WHERE ST_Within(loc, ST_GeomFromText('POLYGON((...))', 4326));

`SHOW CREATE TABLE` emits the plain MySQL form (`loc point NOT NULL SRID 4326`). No
spatial index syntax is part of this layer.

### Feature flag and rollout

Gate the whole layer behind a session/global system variable, e.g.
`tidb_enable_spatial` (default off initially). This is a **launch gate, not a
compatibility switch**: because this is brand-new functionality with no prior
implementation to fall back to, once the feature is stable in master the flag (and any
dead gated-off branches) should be **removed** in a cleanup PR, tracked in #6347. The
index layer ships behind its own separate flag on top of this one.

### Scope and deferrals

Explicitly **out of scope** for this design (each has a home):

- The **spatial index** and its pushdown — `docs/design/2026-06-25-spatial-index.md`
  (#69473). This layer is its prerequisite.
- The **geometry-processing function tail** (`ST_Buffer`/`Union`/`Intersection`/
  `Difference`/`ConvexHull`/`Simplify`/…), typed I/O aliases, MBR predicate family,
  geohash functions, niche accessors — a later, parallel expression-layer milestone.
- **SRIDs beyond 0 and 4326**, the SRS catalog, and `ST_Transform` — the documented
  extension path above.
- **Coprocessor pushdown** of `ST_*` predicates — an optimization that lands with/after
  the index; this layer evaluates predicates at the TiDB root.
- **3D / measured (Z/M) coordinates** — 2D only, as in MySQL/MariaDB.

### Compatibility

- **Partition table / clustered index / async commit:** geometry is an ordinary column
  value; no interaction. (A geometry column cannot be a primary key or clustering key —
  it has no meaningful ordering.)
- **Charset & collation:** irrelevant to the geometry value (binary EWKB); a geometry
  column has no user charset/collation.
- **Parser:** one-time geometry-type + `SRID` grammar change; `ST_*` are generic function
  calls (no grammar). Regenerates `parser.go`; run `make bazel_prepare`.
- **DDL:** new column types and the `SRID` attribute; DDL validation restricts SRID to
  0/4326 and enforces subtype constraints.
- **Planner/statistics/executor:** `ST_*` functions evaluate on the normal expression
  path; predicates are ordinary `Selection`s (full scan in this layer). No new operator,
  no new access path, no statistics change.
- **TiKV:** geometry values are ordinary binary strings; no storage-engine change. No
  coprocessor change in this layer (pushdown is deferred).
- **TiFlash / BR / TiCDC / Dumpling / Lightning:** geometry is regular column data;
  round-trips need only that the tools carry the value bytes and the `SRID`/type
  metadata. Dump/reload uses MySQL-compatible EWKB / WKT.
- **Upgrade:** additive — new type and functions behind a flag.
- **Downgrade:** a table with a geometry column cannot be read by a release without the
  type; downgrade requires dropping geometry columns first (same as other new type kinds;
  to be confirmed during implementation).

## Test Design

### Functional Tests

- I/O round-trips: `ST_GeomFromText`/`ST_AsText`, `ST_GeomFromWKB`/`ST_AsBinary`,
  `ST_GeomFromGeoJSON`/`ST_AsGeoJSON` for every subtype, byte-compared to MySQL output
  (including MySQL's `ST_AsText` spacing and axis order).
- Accessors/measurement: `ST_X/Y/Latitude/Longitude/SRID/GeometryType/Dimension/…` and
  `ST_Area/Length/Distance/Distance_Sphere` against cross-checked MySQL values (e.g. the
  1-degree geodesic distances in `srid-support-reference.md`).
- Predicates: the eight DE-9IM predicates (+ `Covers`/`CoveredBy`) on curated geometry
  pairs, OGC-correct, matched to MySQL where semantics agree; boundary cases explicitly
  covered.
- SRID validation: 4326 out-of-range lat/long errors on every ingest path; SRID 0 Inf/NaN
  rejection; mixed-SRID predicate errors.
- Type plumbing: geometry through GROUP BY, joins, DISTINCT, ORDER BY, `INSERT … SELECT`,
  `UNION`, UPDATE/DELETE/REPLACE — the PoC's audited surface — returns correct bytes.

### Scenario Tests

- Store a points table and answer proximity (`ST_Distance_Sphere ≤ r`) and geofence
  (`ST_Within(point, polygon)`) by full scan; results match MySQL.
- 4326 edge cases: a query near a pole and across the antimeridian.
- Application shape: lat/long ingest via WKT/GeoJSON, read back via `ST_AsGeoJSON`.

### Compatibility Tests

- MySQL byte-identical compatibility suite for the v1 function surface (the PoC's
  `spatial_compat` integration test is the basis).
- Dumpling/Lightning round-trip of a table with geometry columns; TiCDC and BR
  pass-through; behavior unaffected when TiFlash is absent.
- Parser/DDL/planner/executor as listed in Compatibility.
- Upgrade and downgrade paths.

### Benchmark Tests

- Geometry ingest and read throughput vs a scalar-encoded baseline, to confirm EWKB
  decode cost is acceptable.
- Predicate full-scan latency across selectivities (this is the pre-index baseline the
  index layer will be measured against).
- The value-format choice (raw EWKB vs version-tagged/lean): decode ns/op on the point
  and polygon paths, to settle the pre-GA encoding.

## Impacts & Risks

Impacts (intended): geometry becomes a first-class, MySQL-compatible value and query
surface; applications can store locations and run proximity/geofence queries in SQL
(full scan) without application-side geometry code.

Risks:

- **Prerequisite coupling (downstream):** the index and pushdown layers code against this
  type; the value-format and axis-order decisions here are lock-ins for them, so they are
  settled in this design.
- **Value-format lock-in:** the on-disk format is hard to change post-GA; mitigated by a
  format-version byte (Unresolved Questions).
- **4326 semantics gaps:** geodesic `ST_Area`, polygon/polygon geodesic relations, and
  planar-vs-geodesic refine edge cases diverge from MySQL near poles/antimeridian;
  mitigated by documenting the v1 limitation and erroring rather than silently returning
  wrong values.
- **MySQL error parity:** exact error codes/messages may not match MySQL initially (the
  PoC used placeholder wording); a compatibility, not correctness, risk.
- **Pure-Go library gaps:** `simplefeatures` covers the v1 surface but not the
  GEOS-class processing tail; that tail is deferred, so v1 is unaffected.

## Investigation & Alternatives

- **cgo/libgeos (go-geos):** rejected for v1. It gives OGC-correct geometry but requires
  `libgeos` in the Bazel/CI sandbox, which broke the build; the PoC migrated to pure-Go
  `simplefeatures` and stayed MySQL byte-identical, so cgo is unnecessary for the v1
  surface. Revisit only for the GEOS-class processing tail.
- **Full #38916 surface at once:** rejected as too large to review/land; this design is
  the narrowed, independently-shippable prerequisite slice, with the rest sequenced after.
- **Storing geometry as a generic BLOB with app-side functions:** the status quo; loses
  MySQL compatibility, type safety, and any path to a spatial index. Rejected.
- **PostGIS axis order / `geometry`-always-planar semantics:** rejected in favor of MySQL
  compatibility (lat/long axis order, class-driven planar-vs-geodesic), since MySQL parity
  is the goal (`srid-support-reference.md` documents the differences).

## Unresolved Questions

- **Internal value format:** confirm the version-tagged layout beyond the leading format
  byte — whether to strip the redundant per-row SRID / byte-order flags and adopt a
  flat-`f64` point layout — benchmark-gated (`storage-format.md`). `ST_AsBinary` output
  stays MySQL EWKB regardless.
- **Geodesic `ST_Area` on 4326:** implement (Karney ellipsoidal area) in v1, or error
  like MySQL's Cartesian-only functions until implemented?
- **MySQL error-code/message parity:** how closely to match MySQL's exact GIS error codes
  and wording for out-of-range coordinates, invalid geometries, and mixed-SRID errors.
- **Feature-flag name and default:** `tidb_enable_spatial` (proposed); default off then
  removed after stabilization — confirm naming and the removal milestone.
- **4326 refine scope for v1:** accept planar polygon/polygon refine + the geodesic
  point-in-polygon as the documented limitation, or widen geodesic coverage before GA?
- **Exact v1 function boundary:** confirm which accessors/aliases are v1 vs tail against
  `mysql-function-catalog.md` (e.g. `ST_Centroid` placement, typed `*FromText`/`*FromWKB`
  aliases).
- **Downgrade mechanics:** confirm the drop-geometry-columns-first requirement and whether
  any metadata-only downgrade is possible.
