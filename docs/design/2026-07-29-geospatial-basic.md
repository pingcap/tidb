# TiDB Design Documents

- Author(s): [Mattias Jonsson](http://github.com/mjonss), [Daniël van Eeden](http://github.com/dveeden)
- Discussion PR: https://github.com/pingcap/tidb/pull/70420
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
storable, readable, and queryable. It covers **SRID 0** (Cartesian plane) and
**SRID 4326** ([WGS 84](#terminology) geographic), including the
[DE-9IM](#terminology) predicates. A geometry predicate has no index to use here, so it
is evaluated row by row over whatever the access path returns; other predicates on the
table pick their access path as usual.

It is deliberately **index-free**: the spatial index is specified separately in
`docs/design/2026-06-25-spatial-index.md` (PR #69473) and builds on this layer. The scope
here is the smallest slice that is independently useful and GA-able, designed so that
later work (more SRIDs, the geometry-processing function tail, coprocessor pushdown, the
index) extends it without a new design. This replaces the earlier geospatial design
(PR #38916).

The MySQL behaviors and measurements quoted below were verified against running servers
(8.4.6 and 9.7.2) and against the proof of concept, PR #69475.

## Terminology

| Term | Meaning |
| --- | --- |
| [OGC](https://www.ogc.org/standard/sfa/) | Open Geospatial Consortium, the body behind *Simple Features*, the specification MySQL's spatial surface follows. |
| [WKT / WKB](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) | Well-Known Text and Well-Known Binary, the OGC encodings of a geometry: `POINT(1 2)` and its byte form. |
| [EWKB](https://libgeos.org/specifications/wkb/#extended-wkb) | Extended WKB, the PostGIS/GEOS superset of WKB: type-word flags add Z, M and an embedded SRID. The stored format here; see [Types and storage](#types-and-storage). Not to be confused with [MySQL's internal format](https://dev.mysql.com/doc/refman/8.4/en/gis-data-formats.html), a 4-byte SRID prefix over 2D WKB. |
| ISO WKB | The other WKB extension (OGC Simple Feature Access 1.2.1, also ISO 13249-3 SQL/MM), which encodes Z/M by adding 1000/2000/3000 to the type code instead of using flags, and carries no SRID. |
| SRS | Spatial reference system: coordinate system, units, axis order, datum. Either *projected* (flat X/Y) or *geographic* (latitude/longitude on an ellipsoid). |
| [SRID](https://dev.mysql.com/doc/refman/8.4/en/spatial-reference-systems.html) | Spatial Reference System Identifier, the integer naming an SRS. v1 supports 0 and 4326; see [SRID model](#srid-model). |
| [EPSG](https://epsg.org/) | The EPSG Geodetic Parameter Dataset, the registry that assigns SRIDs and the source of the SRS catalog. |
| [WGS 84](https://en.wikipedia.org/wiki/World_Geodetic_System) | World Geodetic System 1984, the datum and reference ellipsoid used by GPS, registered as EPSG:4326. |
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

**MySQL-compatible, extensible where the extension is free.** Every v1 function behaves as
MySQL does, and where MySQL cannot express a value the function errors rather than
inventing behavior. The storage layer is deliberately wider: it carries Z/M coordinates
and any SRID losslessly, and WKB input accepts them, so such values are stored and read
back intact even though no v1 function computes on them and the MySQL-shaped writers
(`ST_AsBinary`, `ST_AsText`, `ST_AsGeoJSON`) reject them. Widening the functions later is
an extension that breaks MySQL parity by addition, and belongs to the release that makes
it, not to this one.

### Types and storage

Types, all reusing the existing `mysql.TypeGeometry` field type with the subtype a
constraint on the stored value, as in MySQL: `GEOMETRY` (any subtype), `POINT`,
`LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`,
`GEOMETRYCOLLECTION`. A column may carry a `SRID n` attribute (see
[SRID model](#srid-model)). At the KV layer a geometry is a binary string; no new column
encoding is introduced.

Stored value:

    <format_version u8><payload>
    version 1:  EWKB

**Version 1 is [EWKB](https://libgeos.org/specifications/wkb/#extended-wkb)** as defined
by PostGIS and GEOS: standard WKB whose 32-bit type word carries three high-bit flags,
`0x80000000` for Z, `0x40000000` for M, and `0x20000000` meaning a `u32` SRID follows the
type word on the outermost geometry. It is chosen because it is a published format that
already expresses everything this design has to store, and because it degrades exactly
right: a 2D geometry with no SRID flag *is* plain OGC WKB, byte for byte.

| Rule | |
| --- | --- |
| Versioning | Numbered from 1, so a leading `0x00` is always invalid and never a version. A release reads every earlier version and writes the current one, so a later format needs no migration. |
| Lossless | Exact `f64` coordinates and full geometry structure, never truncated. |
| SRID | Carried by the SRID flag. Where the column fixes it with `SRID n` the flag may be left unset, which is still valid EWKB, so this redundancy can be removed without leaving the format. It cannot be dropped unconditionally: an unrestricted `GEOMETRY` column holds a per-row SRID, and a geometry outside any column (function result, join or sort intermediate) has no column metadata to recover it from. |
| Byte order | Left to EWKB, which flags it per geometry and permits both. |
| MySQL bytes | Not matched. MySQL stores `<srid u32 LE><WKB>` and is 2D only; `ST_AsBinary`, dump/reload and the wire protocol convert at the boundary, which for a 2D value is dropping the SRID flag. MySQL itself converts too, storing coordinates longitude-first internally and swapping per SRS on every WKT/WKB read and write, visible as `HEX(g)` and `ST_AsBinary(g)` returning swapped coordinates for a 4326 point. |
| Coordinate dimension | XY, XYZ, XYM and XYZM are all storable, which covers GeoJSON positions (XY and XYZ) as well as measured geometry. Every v1 function is 2D, as in MySQL. |
| SRIDs outside 0 and 4326 | Stored and returned unchanged in an unrestricted `GEOMETRY` column, as in MySQL. |

Extended data, meaning Z/M coordinates and SRIDs the functions do not support, follows the
compatibility rule above: stored losslessly, read back unchanged, `ST_SRID` and
`ST_GeometryType` still answering, everything that interprets coordinates erroring. Such a
value comes back as the raw column value, which is enough for v1; giving the writers an
opt-in that emits it is a later extension nothing here blocks.
Storing more than v1 computes on is what lets 3D/measured geometry and the wider SRS
catalog arrive later as functions over data written today. Why EWKB rather than the
alternatives: [Investigation & Alternatives](#investigation--alternatives).

### SRID model

| | SRID 0 | SRID 4326 |
| --- | --- | --- |
| Coordinate system | abstract Cartesian plane, unitless X/Y | WGS 84 geographic, latitude/longitude |
| Bounds | none, the full finite IEEE-754 double range, as MySQL | latitude `[-90, 90]`, longitude `(-180, 180]` |
| Rejected on ingest | Inf/NaN, MySQL `ERROR 3037` | out-of-range latitude (`ERROR 3617`) and longitude (`ERROR 3616`) |
| Measurement | planar (Cartesian) | geodesic on the WGS 84 ellipsoid, as MySQL |

Those codes and their wording are matched as closely as possible, on `ST_GeomFromText`,
the constructors, `ST_GeomFromGeoJSON` and `ST_GeomFromWKB` alike. The same goes for the
other GIS errors (`ERROR 3618` for a function not implemented on a geographic SRS,
`ERROR 3643` for an SRID that does not match the column). Where a message cannot be
matched exactly it is a compatibility gap to close later, not a reason to invent a
different code.

**Catalog.** `information_schema.st_spatial_reference_systems` ships with MySQL's shape
(`SRS_NAME`, `SRS_ID`, `ORGANIZATION`, `ORGANIZATION_COORDSYS_ID`, `DEFINITION`,
`DESCRIPTION`) and exactly the two supported rows, copied from MySQL: SRID 0 with an
empty name and definition and no organization, and 4326 as `WGS 84` / `EPSG` / 4326 with
the EPSG `GEOGCS["WGS 84",DATUM[...]]` definition string. It is read-only, so
`CREATE SPATIAL REFERENCE SYSTEM` is rejected, and DDL validates the `SRID n` attribute
against it rather than against a hardcoded pair, which makes widening SRID support a
matter of adding rows. It also gives a client an honest answer to "which SRIDs does this
server support", where today the question is an unknown-table error.

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
`ST_X`/`ST_Y` are not: they return the first and second coordinate, so on 4326 `ST_X` is
the latitude here and in MySQL (verified: `ST_X` = `ST_Latitude` = 30 for
`POINT(30 50)`) but the longitude in PostGIS. The per-SRID breakdown belongs in the user
docs, as migration guidance.

Coordinates are **stored as parsed**, so latitude-first on 4326, which is also the order
S2 wants and therefore costs no swap on the geodesic and index paths. MySQL stores the
opposite order internally and swaps at every boundary
([Types and storage](#types-and-storage)); both engines emit the same WKB, so the
difference is invisible outside the stored bytes.

**Extension path** (documented, not built here):

| Step | Cost |
| --- | --- |
| Fill the catalog from the full EPSG dataset (MySQL ships 5,238 rows in both 8.4 and 9.7) with class, axis order, bounds, unit, ellipsoid | moderate, and a prerequisite for the rest |
| All projected SRSs (e.g. 3857 Web Mercator) | low: planar X/Y, so the same Cartesian functions apply and only the bounds are per-SRS |
| Geographic SRSs beyond 4326 | moderate: exact geodesic refine per ellipsoid |
| PostGIS level (`CREATE SPATIAL REFERENCE SYSTEM`, `ST_Transform`) | bigger: on-the-fly reprojection needs a PROJ-like library; out of scope |

DDL restricts the `SRID n` attribute to 0 or 4326, so no partial-SRS behavior escapes
before the catalog exists. An unrestricted `GEOMETRY` column may still hold values of any
SRID (see [Types and storage](#types-and-storage)).

### Function set

v1 is the minimal set needed to store, read, inspect, measure and filter geometry. All of
it is present in MySQL 8.0.46 / 8.4 / 9.7, whose spatial function sets are identical. The
list below is an **allowlist**: only these functions are registered, and anything else
spatial is simply an unknown function until a later milestone adds it. Growing the
surface is then an explicit act rather than the default.

- **I/O readers:** `ST_GeomFromText`, `ST_GeomFromWKB`, `ST_GeomFromGeoJSON`.
- **I/O writers:** `ST_AsText` (`ST_AsWKT`), `ST_AsBinary` (`ST_AsWKB`), `ST_AsGeoJSON`.
- **Constructors:** `Point`, `LineString`, `Polygon`.
- **Accessors:** `ST_X`, `ST_Y`, `ST_Latitude`, `ST_Longitude`, `ST_SRID` (getter and the
  `ST_SRID(g, srid)` setter), `ST_GeometryType`, `ST_Dimension`, `ST_Envelope`,
  `ST_IsEmpty`, `ST_IsValid`, `ST_StartPoint`, `ST_EndPoint`, `ST_PointN`, `ST_NumPoints`,
  `ST_ExteriorRing`, `ST_NumInteriorRings`, `ST_Centroid`. `ST_Centroid` is Cartesian-only,
  as in MySQL, which raises `ERROR 3618` ("has not been implemented for geographic spatial
  reference systems") for it on 4326.
- **Measurement:** `ST_Area`, `ST_Length`, `ST_Distance`, `ST_Distance_Sphere`.
- **Predicates (DE-9IM):** `ST_Within`, `ST_Contains`, `ST_Intersects`, `ST_Equals`,
  `ST_Disjoint`, `ST_Touches`, `ST_Crosses`, `ST_Overlaps`.
- **PostGIS extras:** `ST_Covers`, `ST_CoveredBy`, included because the index layer makes
  them index-eligible region predicates (`Covers ⊇ Contains`, `CoveredBy ⊇ Within`, so a
  covering-cell prefilter has no false negatives). Other PostGIS-only functions are added
  later only if index-supported or by demand.

The geometry-processing tail (`ST_Buffer`, `ST_Union`, `ST_Intersection`, ...), the typed
I/O aliases, the `Multi*`/`GeometryCollection` constructors, the `MBR*` family, geohash,
the niche accessors and the GeoJSON `options`/`srid` arguments are a later milestone.

Semantics match MySQL, with three documented v1 limitations:

- On 4326, `ST_Distance`/`ST_Length` are ellipsoidal (Andoyer, matching MySQL to
  sub-metre); `ST_Distance_Sphere` is the great-circle variant.
- `ST_Area` on 4326 **errors** in the shape MySQL uses for its own Cartesian-only
  functions (`ERROR 3618`, "has not been implemented for geographic spatial reference
  systems"). This is a documented divergence: MySQL does compute it geodesically
  (12308778368.75 m2 for a 1-degree box). Erroring beats the alternatives, since a planar
  degree2 or an off-by-0.45% spherical value would be silently wrong, and implementing
  Karney ellipsoidal area is a later extension nothing here blocks.
- The predicates are OGC-correct via `simplefeatures`, which is planar. On 4326 the region
  predicates get a geodesic point-in-polygon, but polygon/polygon relations stay planar,
  which diverges from MySQL. See Unresolved Questions: this is the one open item.

**GeoJSON.** Every RFC 7946 geometry is supported, and the container and annotation members
follow MySQL, verified against 8.4.6 and 9.7.2:

| Input | Result |
| --- | --- |
| `Feature` | its bare geometry, so a Feature holding a point yields `POINT` |
| `FeatureCollection` | `GEOMETRYCOLLECTION` of the features' geometries, `GEOMETRYCOLLECTION EMPTY` if there are none |
| `"geometry": null` | SQL `NULL` |
| `properties`, `id`, `bbox`, foreign members | ignored |
| named `crs` URN | sets the SRID (`urn:ogc:def:crs:OGC:1.3:CRS84` is 4326, link-object CRSs are not accepted, and a nested `crs` naming a different SRID errors); absent, the SRID is 4326 |
| position with more than two coordinates | rejected, MySQL error 3073 |

MySQL's `options` argument, which accepts such positions and strips the extra coordinates,
ships with the function tail, as do `ST_AsGeoJSON`'s bbox and CRS-URN flags. Round-trips
are not idempotent: a FeatureCollection returns from `ST_AsGeoJSON` as a
GeometryCollection.

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

    -- the geometry predicate is evaluated per row here; the index accelerates it later
    SELECT id FROM stores
    WHERE ST_Within(loc, ST_GeomFromText('POLYGON((...))', 4326));

`SHOW CREATE TABLE` emits the plain MySQL form (`loc point NOT NULL SRID 4326`). No
spatial index syntax is part of this layer.

### Feature flag and rollout

The layer is gated on a session/global system variable, `tidb_enable_geospatial`, default
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
- **Coprocessor pushdown.** The predicates and the measurement functions are ordinary
  deterministic scalars, so they are pushdown-eligible in principle, and pushing them is
  worthwhile *independently of the index*: it filters at the storage node instead of
  shipping every candidate geometry to TiDB. It is out of scope here because it is
  cross-repo work (tipb signatures plus a TiKV-side evaluator) and is specified with the
  index design, which owns the pushdown contract. Nothing in this layer blocks it, and for
  this design it is fine that the functions evaluate at the TiDB root.
- **The other two spatial `information_schema` tables**: `st_geometry_columns`, one row
  per geometry column and derivable from the schema, and `st_units_of_measure`, 47 static
  rows in MySQL that only start to matter once projected SRSs with varied units exist.
- **The axis-order controls**: MySQL's `axis-order=long-lat` option argument on
  `ST_GeomFromText`/`ST_GeomFromWKB`/`ST_AsText`/`ST_AsBinary`, and `ST_SwapXY`, which let
  a client read or write longitude-first against a latitude-first SRS. Both exist in MySQL
  and ship here with the function tail.
- **3D / measured (Z/M) geometry**: computation is 2D only, as in MySQL/MariaDB, but the
  values are stored and returned unchanged, so the functions can be added without a format
  change.

### Compatibility

| Area | Effect |
| --- | --- |
| Partition table, clustered index, async commit | None. It cannot be a primary or clustering key, having no meaningful ordering. |
| Charset and collation | Not applicable; the value is binary. |
| Parser | One-time type and `SRID` grammar change; regenerates `parser.go`, run `make bazel_prepare`. `ST_*` are generic calls. |
| DDL | New column types and the `SRID` attribute, restricted to 0/4326, plus subtype constraints. `ALTER` follows MySQL, verified on 8.4.6: adding or changing `SRID n` validates every existing row and fails with `ERROR 3643` on the first mismatch; dropping the attribute always succeeds; narrowing the subtype (`GEOMETRY` to `POINT`) succeeds only if every value fits, else `ERROR 1416`; widening always succeeds; converting the column to a binary type carries the bytes over; `DROP COLUMN` is ordinary. No reinterpretation ever happens: an SRID change is validation, never a coordinate rewrite. |
| `information_schema` | One new table, `st_spatial_reference_systems`, read-only with two static rows. Its two siblings in MySQL are deferred. |
| Planner, statistics, executor | `ST_*` evaluate on the normal expression path; geometry predicates are ordinary `Selection`s with no access path of their own, so they filter whatever rows the chosen path returns. No new operator, access path or statistics. |
| TiKV | None. Values are ordinary binary strings; pushdown is deferred. |
| TiFlash, BR, TiCDC, Dumpling, Lightning | Regular column data. Tools need only carry the bytes and the `SRID`/type metadata; dump/reload uses MySQL's internal format or WKT, not the stored bytes. |
| Upgrade | Additive, behind the flag. |
| Downgrade | The usual new-type situation: a release without the type cannot read a table that has a geometry column, so those columns have to go first, which is an ordinary `DROP COLUMN`. |

## Test Design

### Functional Tests

- I/O round-trips: `ST_GeomFromText`/`ST_AsText`, `ST_GeomFromWKB`/`ST_AsBinary`,
  `ST_GeomFromGeoJSON`/`ST_AsGeoJSON` for every subtype, byte-compared to MySQL output
  including its `ST_AsText` spacing and axis order.
- Accessors and measurement against values measured on MySQL, e.g. for a 1-degree step on
  4326: `ST_Distance` 111319.49 m, `ST_Distance_Sphere` 111195.08 m, and `ST_Area` of a
  1-degree box 12308778368.75 m2.
- Predicates: the eight DE-9IM predicates plus `Covers`/`CoveredBy` on curated geometry
  pairs, matched to MySQL where semantics agree, with boundary cases explicit.
- SRID validation: 4326 out-of-range errors on every ingest path, SRID 0 Inf/NaN
  rejection, mixed-SRID predicate errors.
- The catalog: `st_spatial_reference_systems` returns the two rows with the same column
  values MySQL gives for SRID 0 and 4326, and `CREATE SPATIAL REFERENCE SYSTEM` errors.
- Extended data: Z/M values entered as WKB, and SRIDs outside 0 and 4326, store and read
  back byte-identical, while `ST_AsBinary`/`ST_AsText`/`ST_AsGeoJSON` and every function
  that interprets coordinates error clearly on them.
- GeoJSON: the table above, each row matched against MySQL 8.4 and 9.7.
- Format version: version 1 decodes; an unknown or zero version byte is rejected with a
  clear error rather than misparsed.
- Type plumbing: geometry through the audited operation surface returns correct bytes.

### Scenario Tests

- A points table answering proximity (`ST_Distance_Sphere ≤ r`) and geofence
  (`ST_Within(point, polygon)`), matching MySQL.
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
- Geometry-predicate latency across selectivities with no other predicate to narrow the
  scan, the pre-index baseline the index layer will be measured against.
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

- **Which WKB dialect for version 1.** Three candidates, all published:

  | Candidate | SRID | Z/M | Notes |
  | --- | --- | --- | --- |
  | MySQL internal, `<srid u32 LE><WKB>` | prefix | **no** | 2D only, so it cannot hold what this design commits to storing |
  | ISO WKB (SFA 1.2.1 / SQL-MM) | no | type code +1000/2000/3000 | what `simplefeatures` already reads and writes |
  | **EWKB (PostGIS/GEOS)** | type-word flag | type-word flags | chosen |

  EWKB wins on coverage: one defined format carrying SRID, Z, M and XYZM, a superset of
  what GeoJSON positions can express, with plain 2D WKB as its degenerate case. It also
  keeps the obvious space optimization inside the format, since a value whose SRID is
  fixed by the column can simply leave the SRID flag unset. The cost is a codec:
  `simplefeatures` implements the ISO type-code convention (`geomCode % 1000` for the
  type, `/ 1000` for the dimension), not EWKB's flags, so TiDB owns the EWKB header
  encode/decode and hands the body to the library. On the TiKV side the `geo` crate is
  2D-only and already hand-rolls its decoder, so it needs a header change and nothing
  more; Z/M values are not pushable regardless.
- **A leaner layout as version 1.** Rejected for now, not forever.
  EWKB carries redundancy (a per-row SRID the column usually fixes, a byte-order flag per
  (sub)geometry, WKB framing), but profiling the proof of concept put the win in
  perspective: geometry decode is ~2% of insert CPU, and on the read side WKB parsing is
  ~27% of query CPU of which only about half is decoding the stored value, the rest being
  a re-parse inside the predicate library that no storage format can remove. The version
  byte defers the choice without a migration.
- **Matching MySQL's stored bytes.** Rejected as a non-goal: I/O compatibility is a
  boundary conversion, and MySQL does the same thing internally.
- **cgo/libgeos (go-geos).** Rejected for v1: it gives OGC-correct geometry but needs
  `libgeos` in the Bazel/CI sandbox, which broke the build. The PoC moved to pure-Go
  `simplefeatures` and stayed MySQL byte-identical. Revisit only for the processing tail.
- **The full #38916 surface at once.** Rejected as too large to review and land; this is
  the narrowed, independently shippable slice with the rest sequenced after.
- **Geometry as a generic BLOB with application-side functions.** The status quo; loses
  MySQL compatibility, type safety, and any path to a spatial index.
- **PostGIS axis order and always-planar `geometry` semantics.** Rejected in favor of
  MySQL parity; the differences are documented under SRID model.

## Unresolved Questions

**Polygon/polygon relations on 4326.** MySQL treats a geographic polygon's edges as
geodesics; `simplefeatures`, and every other pure-Go option, treats them as straight
lines in latitude/longitude. That is not a near-boundary rounding difference. For
`POLYGON((0 0, 0 80, 60 0, 0 0))` on 4326, MySQL 8.4.6 answers `ST_Within` **true** for
the points `(30 40)`, `(33 40)`, `(36 40)` and `(40 40)`, while the same coordinates
against the same ring evaluated planar (SRID 0) answer **false** for all four: the
great-circle edge bows away from the straight one by degrees, so whole regions flip.
Small polygons, the common geofence case, are unaffected.

Three ways to resolve it, in increasing cost:

1. **Ship planar polygon/polygon and document it.** Correct for small geofences, wrong
   for continental ones, and wrong silently, which is the objection.
2. **Error when it would matter.** Reject a geographic polygon/polygon relate whose
   operands exceed some extent, so the answer is never silently wrong. Needs a defensible
   threshold, and the error is a divergence of its own since MySQL answers.
3. **Implement geodesic relate.** Full parity, and the only option that makes the index
   layer's refine exact on 4326, but it needs a geographic DE-9IM implementation on both
   the TiDB side and, once pushdown lands, the TiKV side. No pure-Go library provides
   it today; S2 gives coverings and predicates on spherical shapes but not the DE-9IM
   matrix, so this is real work rather than a dependency swap.

The decision belongs to this design because the type layer owns predicate semantics, and
it is a compatibility question rather than a format one, so it does not lock any bytes in.
