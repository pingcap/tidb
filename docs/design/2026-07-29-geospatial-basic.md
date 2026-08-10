# TiDB Design Documents

- Author(s): [Mattias Jonsson](http://github.com/mjonss), [Daniël van Eeden](http://github.com/dveeden)
- Discussion PR: https://github.com/pingcap/tidb/pull/XXXXX
- Tracking Issue: https://github.com/pingcap/tidb/issues/6347

## Table of Contents

* [Introduction](#introduction)
* [Terminology](#terminology)
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
`GEOMETRY` type family, per-column [`SRID`](#terminology), versioned
[EWKB](#terminology) storage, and the minimal `ST_*` function set that makes geometry
storable, readable, and queryable by full table scan. It covers **SRID 0** (Cartesian
plane) and **SRID 4326** ([WGS 84](#terminology) geographic), including the
[DE-9IM](#terminology) predicates.

It is deliberately **index-free**: the spatial index is specified separately in
`docs/design/2026-06-25-spatial-index.md` (PR #69473) and builds on this layer. The scope
here is the smallest slice that is independently useful and GA-able, designed so that
later work (more SRIDs, the geometry-processing function tail, coprocessor pushdown, the
index) extends it without a new design. This replaces the earlier geospatial design
(PR #38916).

## Terminology

| Term | Meaning |
| --- | --- |
| [OGC](https://www.ogc.org/standard/sfa/) | Open Geospatial Consortium, the body behind *Simple Features*, the specification MySQL's spatial surface follows. |
| [WKT / WKB](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) | The OGC text and byte encodings of a geometry (`POINT(1 2)` and its bytes). Neither carries an SRID. |
| [EWKB](https://dev.mysql.com/doc/refman/8.4/en/gis-data-formats.html) | WKB with the SRID alongside it. MySQL uses `<srid u32 LE><WKB>`; see [Types and storage](#types-and-storage). |
| SRS | Spatial reference system: coordinate system, units, axis order, datum. Either *projected* (flat X/Y) or *geographic* (latitude/longitude on an ellipsoid). |
| [SRID](https://dev.mysql.com/doc/refman/8.4/en/spatial-reference-systems.html) | The integer naming an SRS. v1 supports 0 and 4326; see [SRID model](#srid-model). |
| [EPSG](https://epsg.org/) | The registry that assigns SRIDs, and the source of the SRS catalog. |
| [WGS 84](https://en.wikipedia.org/wiki/World_Geodetic_System) | The datum and reference ellipsoid used by GPS, EPSG:4326. |
| Planar vs geodesic | Measurement on a flat plane vs along the ellipsoid. Decided by SRS class, not per SRID. |
| [DE-9IM](https://en.wikipedia.org/wiki/DE-9IM) | Dimensionally Extended 9-Intersection Model, the OGC model defining `ST_Within`, `ST_Contains`, `ST_Intersects` and the other topological predicates. |
| [GeoJSON](https://datatracker.ietf.org/doc/html/rfc7946) | JSON geometry encoding (RFC 7946), the third I/O format. |
| MBR | Minimum bounding rectangle; basis of MySQL's `MBR*` predicates (deferred). |
| [S2](http://s2geometry.io/) | Google's spherical-geometry library, used for the geodesic 4326 paths. |
| [PROJ](https://proj.org/) | The reprojection library that arbitrary-SRS transforms would need; out of scope. |

## Motivation or Background

Geospatial support is one of the most requested TiDB features: tracking issue #6347
carries `feature/accepted` and ranks among the top open issues by reactions. The dominant
workload is concrete, storing a location per row and answering "what is near me", "which
region contains this point", or "what overlaps this box". Bike-share, ride-hailing, parcel
delivery, and asset tracking all reduce to points plus proximity and geofence queries.

TiDB has none of it today: only the `mysql.TypeGeometry` constant exists
(`pkg/parser/mysql/type.go`), with no value representation and no `ST_*` functions, so
users encode geometry into scalar columns by hand and compute distances in the
application, losing MySQL compatibility and correctness. This design covers the basic
layer only, scoped so it can ship, stabilize, and have its feature flag removed before
index work merges on top.

## Detailed Design

### Types and storage

Types, all reusing the existing `mysql.TypeGeometry` field type with the subtype a
constraint on the stored value, as in MySQL: `GEOMETRY` (any subtype), `POINT`,
`LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`,
`GEOMETRYCOLLECTION`. A column may carry a `SRID n` attribute (see
[SRID model](#srid-model)). At the KV layer a geometry is a binary string; no new column
encoding is introduced.

Stored value:

    <format_version u8><payload>
    version 1:  <srid u32 LE><OGC WKB>          // EWKB

| Rule | |
| --- | --- |
| Versioning | Numbered from 1, so a leading `0x00` is always invalid and never a version. A release reads every earlier version and writes the current one, so a later format needs no migration. |
| Lossless | Exact `f64` coordinates and full geometry structure, never truncated. |
| SRID | Recoverable from the value, or from the column where it carries `SRID n`. Not droppable from the value unconditionally: an unrestricted `GEOMETRY` column holds a per-row SRID, and a geometry outside any column (function result, join or sort intermediate) has no column metadata. |
| MySQL bytes | Not matched. `ST_AsBinary`, dump/reload and the wire protocol convert at the boundary; MySQL itself stores coordinates longitude-first internally and swaps per SRS on every WKT/WKB read and write (`axis-order.md`). |
| Z and M coordinates | Stored and returned unchanged. Every v1 function is 2D, as in MySQL. |
| SRIDs outside 0 and 4326 | Stored and returned unchanged in an unrestricted `GEOMETRY` column, as in MySQL. |

Extended data (the last two rows) round-trips through `ST_GeomFromWKB`/`ST_GeomFromText`
and `ST_AsBinary`/`ST_AsText`, with `ST_SRID` and `ST_GeometryType` still answering, while
every function that interprets coordinates rejects it with a clear error. Storing more
than v1 computes on is what lets 3D/measured geometry and the wider SRS catalog arrive
later as functions over data written today.

Why EWKB is version 1 and what a version 2 would strip:
[Investigation & Alternatives](#investigation--alternatives).

### SRID model

| | SRID 0 | SRID 4326 |
| --- | --- | --- |
| Coordinate system | abstract Cartesian plane, unitless X/Y | WGS 84 geographic, latitude/longitude |
| Bounds | none, the full finite IEEE-754 double range, as MySQL | latitude `[-90, 90]`, longitude `(-180, 180]` |
| Rejected on ingest | Inf/NaN | out-of-range latitude/longitude, on every constructor and I/O path |
| Measurement | planar (Cartesian) | geodesic on the WGS 84 ellipsoid, as MySQL |

Ingest errors match MySQL's codes and wording as closely as practical, on
`ST_GeomFromText`, the constructors, `ST_GeomFromGeoJSON` and `ST_GeomFromWKB` alike.

Planar versus geodesic is decided by the **SRS class** (SRID 0 and projected are
Cartesian, geographic is geodesic), exactly as MySQL decides it, rather than by a
per-SRID table of special cases. That class-based dispatch is the extension seam: adding
SRIDs later adds catalog rows and per-class parameters, not code paths.

**Axis order.** EPSG:4326 defines (latitude, longitude), so the first coordinate is the
latitude, and v1 follows MySQL here so that `ST_Latitude`/`ST_Longitude`, distances and
WKT round-trips match. WKB carries two unlabelled doubles, so the same bytes mean
different things across ecosystems: PostGIS uses one fixed easting/longitude-first order
for every SRS, and roughly a third of the SRIDs in MySQL's catalog disagree with it,
across both geographic and projected systems. GeoJSON (RFC 7946, always longitude-first)
and the explicit `ST_Latitude`/`ST_Longitude` accessors are the unambiguous paths.
Per-SRID counts and migration guidance are in `axis-order.md` and belong in user docs.

**Extension path** (documented, not built here):

| Step | Cost |
| --- | --- |
| SRS catalog: populate `information_schema.st_spatial_reference_systems` from EPSG (MySQL ships ~5,200 rows) with class, axis order, bounds, unit, ellipsoid | moderate, and a prerequisite for the rest |
| All projected SRSs (e.g. 3857 Web Mercator) | low: planar X/Y, so the same Cartesian functions apply and only the bounds are per-SRS |
| Geographic SRSs beyond 4326 | moderate: exact geodesic refine per ellipsoid |
| PostGIS level (`CREATE SPATIAL REFERENCE SYSTEM`, `ST_Transform`) | bigger: on-the-fly reprojection needs a PROJ-like library; out of scope |

DDL restricts the `SRID n` attribute to 0 or 4326, so no partial-SRS behavior escapes
before the catalog exists. An unrestricted `GEOMETRY` column may still hold values of any
SRID (see [Types and storage](#types-and-storage)).

### Function set

v1 is the minimal set needed to store, read, inspect, measure and filter geometry. All of
it is present in MySQL 8.0.46 / 8.4 / 9.7, whose spatial function sets are identical.

- **I/O readers:** `ST_GeomFromText`, `ST_GeomFromWKB`, `ST_GeomFromGeoJSON`.
- **I/O writers:** `ST_AsText` (`ST_AsWKT`), `ST_AsBinary` (`ST_AsWKB`), `ST_AsGeoJSON`.
- **Constructors:** `Point`, `LineString`, `Polygon`.
- **Accessors:** `ST_X`, `ST_Y`, `ST_Latitude`, `ST_Longitude`, `ST_SRID` (getter and the
  `ST_SRID(g, srid)` setter), `ST_GeometryType`, `ST_Dimension`, `ST_Envelope`,
  `ST_IsEmpty`, `ST_IsValid`, `ST_StartPoint`, `ST_EndPoint`, `ST_PointN`, `ST_NumPoints`,
  `ST_ExteriorRing`, `ST_NumInteriorRings`, `ST_Centroid`.
- **Measurement:** `ST_Area`, `ST_Length`, `ST_Distance`, `ST_Distance_Sphere`.
- **Predicates (DE-9IM):** `ST_Within`, `ST_Contains`, `ST_Intersects`, `ST_Equals`,
  `ST_Disjoint`, `ST_Touches`, `ST_Crosses`, `ST_Overlaps`.
- **PostGIS extras:** `ST_Covers`, `ST_CoveredBy`, included because the index layer makes
  them index-eligible region predicates (`Covers ⊇ Contains`, `CoveredBy ⊇ Within`, so a
  covering-cell prefilter has no false negatives). Other PostGIS-only functions are added
  later only if index-supported or by demand.

The geometry-processing tail (`ST_Buffer`, `ST_Union`, `ST_Intersection`, ...), the typed
I/O aliases, the `Multi*`/`GeometryCollection` constructors, the `MBR*` family, geohash
and the niche accessors are a later milestone; `mysql-function-catalog.md` holds the
authoritative v1-vs-tail split.

Semantics match MySQL, with three documented v1 limitations:

- On 4326, `ST_Distance`/`ST_Length` are ellipsoidal (Andoyer, matching MySQL to
  sub-metre); `ST_Distance_Sphere` is the great-circle variant.
- `ST_Area` on 4326 is a gap (geodesic polygon area on the ellipsoid, Karney): implement
  it, or error as MySQL's Cartesian-only functions do (Unresolved Questions). It must not
  silently return a planar degree² or an off-by-0.45% spherical value.
- The predicates are OGC-correct via `simplefeatures`; on 4326 the region predicates use a
  geodesic point-in-polygon, which diverges from MySQL's planar refine near
  edges/poles/antimeridian. Polygon/polygon geodesic relations are a follow-up.

Every geometry-returning builtin is typed `GEOMETRY`, so a plain B-tree functional index
over such an expression is correctly rejected; a spatial index is the index layer's job.

### Geometry engine

Pure Go, no cgo, so the stack builds with `CGO_ENABLED=0` and needs no libgeos in the
Bazel/CI sandbox (the only Bazel work is adding `DEPS.bzl` proxy-fetch entries):

- `github.com/peterstace/simplefeatures`: OGC/DE-9IM model, WKT/WKB/GeoJSON I/O,
  predicates, planar measurement. Validated byte-identical to MySQL in the PoC.
- `github.com/golang/geo` (Google's S2 port, Apache 2.0): spherical geometry for 4326.
- `pkg/util/geomrel`: in-tree ellipsoidal distance/length (Andoyer) and geodesic refine.

The processing tail may later need GEOS-equivalent algorithms; it is deferred with the
rest of the tail and kept off this layer's critical path.

### Type plumbing

`TypeGeometry` must flow through the generic value machinery so geometry behaves like any
other column value outside the `ST_*` functions. The PoC audited ~28 operations (GROUP BY,
hash/merge join, DISTINCT, ORDER BY, UPDATE/DELETE/REPLACE, window, `INSERT ... SELECT`,
`UNION`); the touch points are:

- `pkg/parser`: geometry type grammar and the `SRID` column attribute. The only grammar
  change, since `ST_*` are generic calls; regenerates `parser.go` once.
- `pkg/types` / field type: the geometry field type and its flen/charset handling.
- `pkg/util/chunk`: `Row.GetDatum` must return geometry as a binary string (without this
  the PoC found `INSERT ... SELECT` nulled geometry).
- `pkg/expression/builtin_cast.go`: cast-to-string flen setup (without this the PoC found
  `UNION` asserted).
- `pkg/expression`: the `ST_*` builtins (`builtin_geo.go`) and their registration.

Geometry sorts, compares and hashes as its binary value: well-defined, but not spatially
meaningful.

### SQL surface and examples

    col_name {GEOMETRY | POINT | LINESTRING | POLYGON | MULTIPOINT | ...}
        [NOT NULL] [SRID {0 | 4326}]

    CREATE TABLE stores (
      id  BIGINT PRIMARY KEY,
      loc POINT NOT NULL SRID 4326
    );

    INSERT INTO stores VALUES
      (1, ST_GeomFromText('POINT(37.4 -122.1)', 4326)),   -- lat, long (MySQL order)
      (2, ST_GeomFromText('POINT(37.8 -122.3)', 4326));

    SELECT id, ST_AsText(loc), ST_Latitude(loc), ST_Longitude(loc) FROM stores;

    -- geodesic metres on 4326
    SELECT id, ST_Distance(loc, ST_GeomFromText('POINT(37.5 -122.2)', 4326)) AS m
    FROM stores;

    -- full scan in this layer; the index accelerates it later
    SELECT id FROM stores
    WHERE ST_Within(loc, ST_GeomFromText('POLYGON((...))', 4326));

`SHOW CREATE TABLE` emits the plain MySQL form (`loc point NOT NULL SRID 4326`). No
spatial index syntax is part of this layer.

### Feature flag and rollout

The layer is gated on a session/global system variable, `tidb_enable_spatial`, default
off. This is a **launch gate, not a compatibility switch**: there is no prior
implementation to fall back to, so once the feature is stable in master the flag and its
dead branches are removed in a cleanup PR, tracked in #6347. The index layer ships behind
its own flag on top of this one.

### Scope and deferrals

Out of scope here, each with a home:

- The **spatial index** and its pushdown: `docs/design/2026-06-25-spatial-index.md`
  (#69473), for which this layer is the prerequisite.
- The **geometry-processing function tail**, typed I/O aliases, `MBR*` family, geohash,
  niche accessors: a later, parallel expression-layer milestone.
- **SRIDs beyond 0 and 4326**, the SRS catalog and `ST_Transform`: the extension path
  above.
- **Coprocessor pushdown** of `ST_*` predicates: lands with or after the index; this layer
  evaluates predicates at the TiDB root.
- **3D / measured (Z/M) geometry**: computation is 2D only, as in MySQL/MariaDB, but the
  values are stored and returned unchanged, so the functions can be added without a format
  change.

### Compatibility

| Area | Effect |
| --- | --- |
| Partition table, clustered index, async commit | None. It cannot be a primary or clustering key, having no meaningful ordering. |
| Charset and collation | Not applicable; the value is binary. |
| Parser | One-time type and `SRID` grammar change; regenerates `parser.go`, run `make bazel_prepare`. `ST_*` are generic calls. |
| DDL | New column types and the `SRID` attribute, restricted to 0/4326, plus subtype constraints. |
| Planner, statistics, executor | `ST_*` evaluate on the normal expression path; predicates are ordinary `Selection`s. No new operator, access path or statistics. |
| TiKV | None. Values are ordinary binary strings; pushdown is deferred. |
| TiFlash, BR, TiCDC, Dumpling, Lightning | Regular column data. Tools need only carry the bytes and the `SRID`/type metadata; dump/reload uses MySQL EWKB / WKT. |
| Upgrade | Additive, behind the flag. |
| Downgrade | Geometry columns must be dropped first, as with other new type kinds (to be confirmed). |

## Test Design

### Functional Tests

- I/O round-trips: `ST_GeomFromText`/`ST_AsText`, `ST_GeomFromWKB`/`ST_AsBinary`,
  `ST_GeomFromGeoJSON`/`ST_AsGeoJSON` for every subtype, byte-compared to MySQL output
  including its `ST_AsText` spacing and axis order.
- Accessors and measurement against cross-checked MySQL values (e.g. the 1-degree geodesic
  distances in `srid-support-reference.md`).
- Predicates: the eight DE-9IM predicates plus `Covers`/`CoveredBy` on curated geometry
  pairs, matched to MySQL where semantics agree, with boundary cases explicit.
- SRID validation: 4326 out-of-range errors on every ingest path, SRID 0 Inf/NaN
  rejection, mixed-SRID predicate errors.
- Extended data: Z/M values and SRIDs outside 0 and 4326 store and read back
  byte-identical, while every function that interprets coordinates errors clearly.
- Format version: version 1 decodes; an unknown or zero version byte is rejected with a
  clear error rather than misparsed.
- Type plumbing: geometry through the audited operation surface returns correct bytes.

### Scenario Tests

- A points table answering proximity (`ST_Distance_Sphere ≤ r`) and geofence
  (`ST_Within(point, polygon)`) by full scan, matching MySQL.
- 4326 edge cases: a query near a pole and one across the antimeridian.
- Application shape: lat/long ingest via WKT/GeoJSON, read back via `ST_AsGeoJSON`.

### Compatibility Tests

- MySQL byte-identical suite for the v1 function surface (the PoC's `spatial_compat`
  integration test is the basis).
- Dumpling/Lightning round-trip of a table with geometry columns; TiCDC and BR
  pass-through; behavior unaffected when TiFlash is absent.
- Parser, DDL, planner and executor as listed in Compatibility.
- Upgrade and downgrade paths.

### Benchmark Tests

- Geometry ingest and read throughput vs a scalar-encoded baseline.
- Predicate full-scan latency across selectivities, the pre-index baseline the index layer
  will be measured against.
- Version 1 (EWKB payload) vs a lean payload: decode ns/op on the point and polygon paths,
  to decide whether a format version 2 is worth adding.

## Impacts & Risks

Intended impact: geometry becomes a first-class, MySQL-compatible value and query surface,
so applications can store locations and run proximity and geofence queries in SQL without
application-side geometry code.

Risks:

- **Prerequisite coupling:** the index and pushdown layers code against this type, so the
  value-format and axis-order decisions here are lock-ins for them and are settled here.
- **Value-format lock-in:** the on-disk format is hard to change post-GA; mitigated by the
  version byte and by storing Z/M and unsupported SRIDs losslessly from the start.
- **4326 semantics gaps:** geodesic `ST_Area`, polygon/polygon geodesic relations and
  refine edge cases diverge from MySQL near poles and the antimeridian; mitigated by
  documenting the limitation and erroring rather than returning wrong values.
- **MySQL error parity:** exact codes and messages may not match initially (the PoC used
  placeholder wording); a compatibility risk, not a correctness one.
- **Pure-Go library gaps:** `simplefeatures` covers the v1 surface but not the GEOS-class
  processing tail, which is deferred, so v1 is unaffected.

## Investigation & Alternatives

- **EWKB as format version 1, rather than a leaner layout now.** EWKB is what the proof of
  concept (PR #69475) implements and needs no conversion today, not what is optimal:
  `storage-format.md` measures its redundancy (a per-row SRID the column usually fixes, a
  byte-order flag per (sub)geometry, WKB framing) and finds geometry decode a measurable
  share of both the index-maintenance write path and the refine read path. A version 2 can
  strip those, shrink the type enum, or store a point as two bare `f64` values. The
  version byte defers that choice without a migration, which is why it ships in v1 even
  though the format is the smallest of the measured performance levers.
- **Matching MySQL's stored bytes.** Rejected as a non-goal: I/O compatibility is a
  boundary conversion, and MySQL does the same thing internally (`axis-order.md`).
- **cgo/libgeos (go-geos).** Rejected for v1: it gives OGC-correct geometry but needs
  `libgeos` in the Bazel/CI sandbox, which broke the build. The PoC moved to pure-Go
  `simplefeatures` and stayed MySQL byte-identical. Revisit only for the processing tail.
- **The full #38916 surface at once.** Rejected as too large to review and land; this is
  the narrowed, independently shippable slice with the rest sequenced after.
- **Geometry as a generic BLOB with application-side functions.** The status quo; loses
  MySQL compatibility, type safety, and any path to a spatial index.
- **PostGIS axis order and always-planar `geometry` semantics.** Rejected in favor of
  MySQL parity (`srid-support-reference.md` documents the differences).

## Unresolved Questions

- **A leaner format version 2:** add one before GA, or ship version 1 and revisit?
  Benchmark-gated (`storage-format.md`); the version byte means it can also land after GA.
  `ST_AsBinary` output stays MySQL EWKB either way.
- **Geodesic `ST_Area` on 4326:** implement Karney ellipsoidal area in v1, or error like
  MySQL's Cartesian-only functions until it exists?
- **MySQL error parity:** how closely to match MySQL's GIS error codes and wording for
  out-of-range coordinates, invalid geometries and mixed-SRID errors.
- **Feature-flag name and default:** confirm `tidb_enable_spatial` and the removal
  milestone.
- **4326 refine scope for v1:** accept planar polygon/polygon refine plus the geodesic
  point-in-polygon as a documented limitation, or widen geodesic coverage before GA?
- **Exact v1 function boundary:** confirm v1 vs tail against `mysql-function-catalog.md`
  (e.g. `ST_Centroid`, the typed `*FromText`/`*FromWKB` aliases).
- **Downgrade mechanics:** confirm the drop-geometry-columns-first requirement, and
  whether any metadata-only downgrade is possible.
