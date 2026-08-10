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
`GEOMETRY` type family (`POINT`, `LINESTRING`, `POLYGON`, and the multi/collection
variants), per-column [`SRID`](#terminology), [EWKB](#terminology) storage, and a
minimal-but-useful set of `ST_*` functions covering I/O, accessors, measurement, and the
[DE-9IM](#terminology) spatial predicates. It supports **SRID 0 (Cartesian plane)** and
**SRID 4326 ([WGS 84](#terminology) geographic)** and makes geometry values storable,
queryable, and filterable (by full table scan). It is the prerequisite layer that a
spatial **index** builds on, but it is deliberately **index-free**: geometry becomes a
first-class value and query surface first, and the index lands separately.

The scope is intentionally the smallest slice that is independently useful and GA-able,
and it is designed so later work (more SRIDs, the long tail of geometry-processing
functions, coprocessor pushdown, and the spatial index) extends it **without a new
design**. This replaces the earlier geospatial design (PR #38916). The spatial index is
specified separately in `docs/design/2026-06-25-spatial-index.md` (PR #69473) and
depends on this layer.

## Terminology

Geospatial standards come with a dense vocabulary. The abbreviations used throughout this
document, each with an external reference and the section that covers it in depth:

- **[OGC](https://www.ogc.org/standard/sfa/)** (Open Geospatial Consortium): the standards
  body behind *Simple Features*, the specification that MySQL's spatial types and `ST_*`
  functions follow.
- **[WKT / WKB](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry)**
  (Well-Known Text / Well-Known Binary): the OGC text and binary encodings of a geometry,
  for example `POINT(1 2)` and its byte form. Neither carries an SRID.
- **[EWKB](https://dev.mysql.com/doc/refman/8.4/en/gis-data-formats.html)** (Extended WKB):
  WKB with the SRID carried alongside it. MySQL stores a 4-byte little-endian SRID followed
  by OGC WKB, which is the layout this design adopts; see
  [Types and storage](#types-and-storage).
- **SRS** (spatial reference system): the coordinate system a geometry's numbers are
  expressed in, together with its units, axis order, and datum. An SRS is either
  *projected* (flat X/Y) or *geographic* (angular latitude/longitude on an ellipsoid).
- **[SRID](https://dev.mysql.com/doc/refman/8.4/en/spatial-reference-systems.html)**
  (spatial reference system identifier): the integer that names an SRS. v1 supports 0 (the
  abstract Cartesian plane) and 4326 (WGS 84); see [SRID model](#srid-model).
- **[EPSG](https://epsg.org/)**: the registry that assigns those identifiers, and the
  source of the SRS catalog described in the [SRID model](#srid-model) extension path.
- **[WGS 84](https://en.wikipedia.org/wiki/World_Geodetic_System)** (World Geodetic System
  1984): the global datum and reference ellipsoid used by GPS, registered as EPSG:4326.
- **Planar vs geodesic**: planar measurement treats coordinates as points on a flat plane;
  geodesic measurement follows the curved surface of the ellipsoid. Which one applies is
  decided by the SRS class, not per SRID; see [SRID model](#srid-model).
- **[DE-9IM](https://en.wikipedia.org/wiki/DE-9IM)** (Dimensionally Extended
  9-Intersection Model): the OGC model that defines what `ST_Within`, `ST_Contains`,
  `ST_Intersects` and the other topological predicates actually mean; see
  [Function set](#function-set).
- **[GeoJSON](https://datatracker.ietf.org/doc/html/rfc7946)**: the JSON geometry encoding
  (RFC 7946), the third I/O format alongside WKT and WKB.
- **MBR** (minimum bounding rectangle): the axis-aligned box enclosing a geometry, and the
  basis of MySQL's `MBR*` predicate family (deferred, see
  [Scope and deferrals](#scope-and-deferrals)).
- **[S2](http://s2geometry.io/)**: Google's spherical-geometry library, used here for the
  geodesic 4326 paths; see [Geometry engine](#geometry-engine).
- **[PROJ](https://proj.org/)**: the coordinate-transformation library that reprojection
  between arbitrary SRSs would require; out of scope for this design.

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

This design replaces the earlier geospatial design (PR #38916). It covers the basic
layer only, scoped so it can ship, stabilize, and have its feature flag removed before
index work starts merging on top.

## Detailed Design

### Types and storage

The `GEOMETRY` type family follows MySQL: `GEOMETRY` (any subtype), `POINT`,
`LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`, and
`GEOMETRYCOLLECTION`. All reuse the existing `mysql.TypeGeometry` field type; the
concrete subtype is a constraint on the stored value, as in MySQL. A column may carry a
`SRID n` attribute fixing its spatial reference system (see [SRID model](#srid-model)).

The **stored value is version-tagged**: a 1-byte format version followed by the encoded
geometry (`<format_version u8><payload>`). **Version 1 is EWKB**, a 4-byte little-endian
SRID prefix followed by standard OGC WKB (`<srid_le_u32><wkb>`). Version numbering starts
at 1, never 0, so a leading zero byte is always invalid and can never be mistaken for a
version. Geometry columns are stored as a binary string kind at the KV layer; no new
column encoding is introduced.

**The internal format does not need to match MySQL byte-for-byte.** Only two rules bind
it: it must be lossless (exact `f64` coordinates, full geometry structure) and it must
round-trip every supported type. The SRID has to survive as well, though not necessarily
inside the stored bytes: where a column carries a `SRID n` attribute the value is
redundant, so a later format version can omit it there and restore it from the column
metadata on decode. It cannot be omitted unconditionally, because an unrestricted
`GEOMETRY` column holds a per-row SRID, and because a geometry that is not in a column at
all (a function result, an intermediate value in a join or a sort) has no column metadata
to recover it from.

Dump/reload, the wire protocol, and `ST_AsBinary` are boundary concerns and they convert,
emitting MySQL-compatible EWKB whatever is on disk. MySQL itself works this way: it
stores coordinates longitude/easting-first internally regardless of SRID and swaps to the
SRS axis order on every WKT/WKB read and write, so its own stored bytes are already not
what its I/O emits (`axis-order.md`).

Version 1 is EWKB because it is what the proof of concept (PR #69475) implements and it
needs no conversion today, not because it is optimal. `storage-format.md` measures the
cost: EWKB carries a per-row SRID that the column definition usually already fixes, a
byte-order flag per (sub)geometry, and WKB framing, and geometry decode is a measurable
share of both the index-maintenance write path and the refine read path. A later version
can drop the per-row SRID for SRID-restricted columns, drop the byte-order flags
(everything written is canonical little-endian), shrink the type enum, or store a point
as two bare `f64` values, each gated on a benchmark. The version byte is what keeps that
available: a release reads every earlier version and writes the current one, with no
migration.

**Extended data is storable, and no v1 function operates on it.** The format carries,
losslessly:

- **Z and M coordinates.** Every `ST_*` function in this design is 2D, as in MySQL, but a
  value carrying Z/M is stored and returned unchanged rather than rejected or truncated.
- **SRIDs outside 0 and 4326.** An unrestricted `GEOMETRY` column (one with no `SRID`
  attribute) may hold values of any SRID, as in MySQL.

Such values round-trip, in through `ST_GeomFromWKB`/`ST_GeomFromText` and out through
`ST_AsBinary`/`ST_AsText`, with the metadata accessors (`ST_SRID`, `ST_GeometryType`)
still answering. Every function that has to interpret the coordinates (measurement, the
predicates, the geodesic paths) rejects them with a clear error. Storing more than v1
computes on is deliberate: 3D/measured geometry and the wider SRS catalog then arrive as
new *functions* over data written today, instead of needing a format change and a
migration.

### SRID model

v1 supports two spatial reference systems, chosen to cover the two coordinate-system
*classes* that matter:

- **SRID 0: abstract Cartesian plane.** Unitless X/Y, no coordinate-range checking
  (MySQL spans the full finite IEEE-754 double range). All functions are planar
  (Cartesian).
- **SRID 4326: WGS 84 geographic (lat/long).** Coordinates are bounded (latitude
  `[-90, 90]`, longitude `(-180, 180]`); distance/length/area are geodesic on the WGS 84
  ellipsoid, matching MySQL.

**Coordinate-system class drives planar-vs-geodesic**, exactly as MySQL decides it from
the SRS class (SRID 0 / projected = Cartesian; geographic = geodesic). This class-based
dispatch, not a per-SRID table of special cases, is the design's extension seam: adding
SRIDs later is adding catalog rows and per-class parameters, not new code paths.

**Axis order.** MySQL's EPSG:4326 is **(latitude, longitude)**: the first coordinate is
latitude. This is verifiable from MySQL's own out-of-range error wording (`POINT(100 0)`
on 4326 errors "Latitude 100 ... out of range"). v1 follows MySQL's axis order so that
`ST_Latitude`/`ST_Longitude`, distances, and WKT round-trips match.

WKB carries two bare doubles with no axis labels, so **the same bytes mean different
things in the two ecosystems**: a 4326 `POINT` written by MySQL (and by this design) is
(latitude, longitude), while the identical bytes written by PostGIS are (longitude,
latitude), because PostGIS uses one fixed easting/longitude-first order for every SRS
instead of the authority-defined one. This is not specific to 4326: roughly a third of
the SRIDs in MySQL's catalog disagree with PostGIS, across both geographic and projected
systems. GeoJSON (RFC 7946, always longitude-first) and the explicit
`ST_Latitude`/`ST_Longitude` accessors are the two unambiguous paths. The full
convention, the per-SRID counts, and the migration guidance are in the PoC's
`axis-order.md` and should be folded into user docs.

**Coordinate validation.** For 4326, out-of-range latitude/longitude errors on ingest
(matching MySQL codes/wording as closely as practical), across every constructor and I/O
path (`ST_GeomFromText`, the typed constructors, `ST_GeomFromGeoJSON`, `ST_GeomFromWKB`).
For SRID 0, only non-finite overflow (Inf/NaN) is rejected, as in MySQL.

**Extension path (documented, not built here).** Later SRID expansion, layered by cost so
the cheap high-value part can land without the expensive part:

- **SRS catalog**: populate `information_schema.st_spatial_reference_systems` from the
  EPSG dataset (MySQL ships ~5,200 entries) with the per-SRS metadata the engine needs:
  class (PROJECTED vs GEOGRAPHIC), axis order, coordinate bounds, unit, ellipsoid.
  Prerequisite for everything below.
- **All PROJECTED SRSs** (e.g. 3857 Web Mercator): low extra cost, they are planar X/Y,
  so the same Cartesian functions apply; the only per-SRS input is coordinate bounds.
- **GEOGRAPHIC SRSs beyond 4326**: moderate, needs exact geodesic refine per ellipsoid.
- **PostGIS-level** (`CREATE SPATIAL REFERENCE SYSTEM`, `ST_Transform` between SRSs):
  bigger, needs on-the-fly reprojection (a PROJ-like library); explicitly out of scope.

v1 deliberately hard-restricts the `SRID n` column attribute to 0 or 4326 at DDL, so no
partial-SRS behavior escapes before the catalog exists. An unrestricted `GEOMETRY` column
can still *hold* values of any SRID, which are stored losslessly and read back unchanged
but which no v1 function computes on (see [Types and storage](#types-and-storage)).

### Function set

The v1 function set is the minimal set needed to **store, read, and query** geometry:
everything an application needs to put geometry in, get it out, inspect it, measure it,
and filter rows by spatial relationship. The geometry-*processing* tail (`ST_Buffer`,
`ST_Union`, `ST_Intersection`, ...), the typed I/O aliases, the MBR predicate family,
geohash, and niche accessors are a **separate later milestone** (they are pure
expression-layer builtins, orthogonal to both this layer and the index). The
authoritative full MySQL catalog with the v1-vs-tail split is `mysql-function-catalog.md`.

v1 functions (all present in MySQL 8.0.46 / 8.4 / 9.7, where the spatial function set
is identical across those versions):

- **I/O readers:** `ST_GeomFromText`, `ST_GeomFromWKB`, `ST_GeomFromGeoJSON`.
- **I/O writers:** `ST_AsText` (`ST_AsWKT`), `ST_AsBinary` (`ST_AsWKB`), `ST_AsGeoJSON`.
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

- `github.com/peterstace/simplefeatures`: OGC/DE-9IM geometry model, WKT/WKB/GeoJSON
  I/O, predicates, and planar measurement. Validated byte-identical to MySQL in the PoC.
- `github.com/golang/geo` (Google's S2 port, Apache 2.0): spherical geometry for the
  geodesic 4326 paths.
- A small in-tree geodesic helper (`pkg/util/geomrel`) for ellipsoidal distance/length
  (Andoyer) and the geodesic region refine.

Pure Go matters: the whole spatial stack builds with `CGO_ENABLED=0`, so there is no
libgeos dependency in the Bazel/CI sandbox. The only Bazel follow-up is adding the new
pure-Go deps to `DEPS.bzl` as proxy-fetch entries (no cc-toolchain wiring). The
geometry-*processing* tail (buffer/union/...) may later need GEOS-equivalent algorithms;
that is deferred with the rest of the tail and kept off this layer's critical path.

### Type plumbing

`TypeGeometry` must flow correctly through the generic value machinery so that geometry
behaves like any other column value outside the `ST_*` functions. The PoC audited ~28
operations (GROUP BY, hash/merge join, DISTINCT, ORDER BY, UPDATE/DELETE/REPLACE, window,
`INSERT ... SELECT`, `UNION`, ...); the concrete touch points are:

- `pkg/parser`: geometry type grammar and the `SRID` column attribute (regenerates
  `parser.go` once; the only grammar change, since `ST_*` functions are generic calls,
  not grammar).
- `pkg/types` / field type: the geometry field type and its flen/charset handling.
- `pkg/util/chunk`: `Row.GetDatum` must return geometry as a binary string (the PoC
  found `INSERT ... SELECT` nulled geometry without this).
- `pkg/expression/builtin_cast.go`: cast-to-string flen setup (the PoC found `UNION`
  asserted without this).
- `pkg/expression`: the `ST_*` builtins (`builtin_geo.go`) and their registration.

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

- The **spatial index** and its pushdown, in `docs/design/2026-06-25-spatial-index.md`
  (#69473). This layer is its prerequisite.
- The **geometry-processing function tail** (`ST_Buffer`/`Union`/`Intersection`/
  `Difference`/`ConvexHull`/`Simplify`/...), typed I/O aliases, MBR predicate family,
  geohash functions, niche accessors: a later, parallel expression-layer milestone.
- **SRIDs beyond 0 and 4326**, the SRS catalog, and `ST_Transform`: the documented
  extension path above.
- **Coprocessor pushdown** of `ST_*` predicates: an optimization that lands with/after
  the index; this layer evaluates predicates at the TiDB root.
- **3D / measured (Z/M) coordinates**: computation is 2D only, as in MySQL/MariaDB. The
  values are storable and round-trip unchanged, so the functions can be added later
  without a format change (see [Types and storage](#types-and-storage)).

### Compatibility

- **Partition table / clustered index / async commit:** geometry is an ordinary column
  value; no interaction. (A geometry column cannot be a primary key or clustering key,
  since it has no meaningful ordering.)
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
- **Upgrade:** additive, with the new type and functions behind a flag.
- **Downgrade:** a table with a geometry column cannot be read by a release without the
  type; downgrade requires dropping geometry columns first (same as other new type kinds;
  to be confirmed during implementation).

## Test Design

### Functional Tests

- I/O round-trips: `ST_GeomFromText`/`ST_AsText`, `ST_GeomFromWKB`/`ST_AsBinary`,
  `ST_GeomFromGeoJSON`/`ST_AsGeoJSON` for every subtype, byte-compared to MySQL output
  (including MySQL's `ST_AsText` spacing and axis order).
- Accessors/measurement: `ST_X/Y/Latitude/Longitude/SRID/GeometryType/Dimension/...` and
  `ST_Area/Length/Distance/Distance_Sphere` against cross-checked MySQL values (e.g. the
  1-degree geodesic distances in `srid-support-reference.md`).
- Predicates: the eight DE-9IM predicates (+ `Covers`/`CoveredBy`) on curated geometry
  pairs, OGC-correct, matched to MySQL where semantics agree; boundary cases explicitly
  covered.
- SRID validation: 4326 out-of-range lat/long errors on every ingest path; SRID 0 Inf/NaN
  rejection; mixed-SRID predicate errors.
- Extended data: values with Z/M coordinates and with SRIDs outside 0 and 4326 store and
  read back byte-identical, while every function that interprets coordinates errors
  clearly on them.
- Format version: a value written with version 1 decodes; an unknown or zero version byte
  is rejected with a clear error rather than misparsed.
- Type plumbing: geometry through GROUP BY, joins, DISTINCT, ORDER BY, `INSERT ... SELECT`,
  `UNION`, UPDATE/DELETE/REPLACE (the PoC's audited surface) returns correct bytes.

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
- Version 1 (EWKB payload) vs a lean payload: decode ns/op on the point and polygon
  paths, to decide whether a format version 2 is worth adding.

## Impacts & Risks

Impacts (intended): geometry becomes a first-class, MySQL-compatible value and query
surface; applications can store locations and run proximity/geofence queries in SQL
(full scan) without application-side geometry code.

Risks:

- **Prerequisite coupling (downstream):** the index and pushdown layers code against this
  type; the value-format and axis-order decisions here are lock-ins for them, so they are
  settled in this design.
- **Value-format lock-in:** the on-disk format is hard to change post-GA; mitigated by the
  leading format-version byte and by storing Z/M and unsupported SRIDs losslessly from
  the start (see [Types and storage](#types-and-storage)).
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

- **A leaner format version 2:** whether to add one before GA (dropping the per-row SRID
  on SRID-restricted columns and the byte-order flags, shrinking the type enum, flat
  `f64` points) or to ship version 1 and revisit. Benchmark-gated (`storage-format.md`),
  and the version byte means it can also land after GA. `ST_AsBinary` output stays MySQL
  EWKB either way.
- **Geodesic `ST_Area` on 4326:** implement (Karney ellipsoidal area) in v1, or error
  like MySQL's Cartesian-only functions until implemented?
- **MySQL error-code/message parity:** how closely to match MySQL's exact GIS error codes
  and wording for out-of-range coordinates, invalid geometries, and mixed-SRID errors.
- **Feature-flag name and default:** `tidb_enable_spatial` (proposed); default off then
  removed after stabilization; confirm naming and the removal milestone.
- **4326 refine scope for v1:** accept planar polygon/polygon refine + the geodesic
  point-in-polygon as the documented limitation, or widen geodesic coverage before GA?
- **Exact v1 function boundary:** confirm which accessors/aliases are v1 vs tail against
  `mysql-function-catalog.md` (e.g. `ST_Centroid` placement, typed `*FromText`/`*FromWKB`
  aliases).
- **Downgrade mechanics:** confirm the drop-geometry-columns-first requirement and whether
  any metadata-only downgrade is possible.
