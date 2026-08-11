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
storable, readable and queryable. It covers **SRID 0** (Cartesian plane) and **SRID 4326**
([WGS 84](#terminology) geographic), including the [DE-9IM](#terminology) predicates. A
geometry predicate has no index here, so it filters row by row over whatever the access
path returns; other predicates choose their access path as usual.

It is **index-free**. The spatial index is specified in
[`docs/design/2026-06-25-spatial-index.md`](2026-06-25-spatial-index.md)
([PR #69473](https://github.com/pingcap/tidb/pull/69473)) and builds on this layer; later work
(more SRIDs, the function tail, coprocessor pushdown, the index) extends this design
rather than replacing it. This replaces the earlier geospatial design
([PR #38916](https://github.com/pingcap/tidb/pull/38916)).

MySQL behaviors and measurements below were verified against running 8.4.6 and 9.7.2, and
against the proof of concept, [PR #69475](https://github.com/pingcap/tidb/pull/69475).

## Terminology

| Term | Meaning |
| --- | --- |
| [OGC](https://www.ogc.org/standard/sfa/) | Open Geospatial Consortium, the body behind *Simple Features*, the specification MySQL's spatial surface follows. |
| [WKT / WKB](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) | Well-Known Text and Well-Known Binary, the OGC encodings of a geometry: `POINT(1 2)` and its byte form. |
| [EWKB](https://libgeos.org/specifications/wkb/#extended-wkb) | Extended WKB, the PostGIS/GEOS superset of WKB: type-word flags add Z, M and an embedded SRID. The stored format here; see [Types and storage](#types-and-storage). Not [MySQL's internal format](https://dev.mysql.com/doc/refman/8.4/en/gis-data-formats.html), which is a 4-byte SRID prefix over 2D WKB. |
| ISO WKB | The other WKB extension (OGC Simple Feature Access 1.2.1, also ISO 13249-3 SQL/MM), which encodes Z/M by adding 1000/2000/3000 to the type code instead of using flags, and carries no SRID. |
| SRS | Spatial Reference System: coordinate system, units, axis order, datum. Either *projected* (flat X/Y) or *geographic* (latitude/longitude on an ellipsoid). |
| [SRID](https://dev.mysql.com/doc/refman/8.4/en/spatial-reference-systems.html) | Spatial Reference System Identifier, the integer naming an SRS. v1 supports 0 and 4326; see [SRID model](#srid-model). |
| [EPSG](https://epsg.org/) | The EPSG Geodetic Parameter Dataset, published by IOGP, which assigns SRIDs. |
| [WGS 84](https://en.wikipedia.org/wiki/World_Geodetic_System) | World Geodetic System 1984, the datum and reference ellipsoid used by GPS, registered as EPSG:4326. |
| Planar vs geodesic | Measurement on a flat plane vs along the ellipsoid. Decided by SRS class, not per SRID. |
| [DE-9IM](https://en.wikipedia.org/wiki/DE-9IM) | Dimensionally Extended 9-Intersection Model, the OGC model defining `ST_Within`, `ST_Contains`, `ST_Intersects` and the other topological predicates. |
| [GeoJSON](https://datatracker.ietf.org/doc/html/rfc7946) | JSON geometry encoding (RFC 7946), the third I/O format. |
| MBR | Minimum Bounding Rectangle; basis of MySQL's `MBR*` predicates (deferred). |
| [S2](http://s2geometry.io/) | Google's spherical-geometry library, used for the geodesic 4326 paths. |
| [PROJ](https://proj.org/) | The reprojection library that arbitrary-SRS transforms would need; out of scope. |

## Motivation or Background

Geospatial support is one of the most requested TiDB features: [tracking issue #6347](https://github.com/pingcap/tidb/issues/6347)
carries
`feature/accepted` and ranks among the top open issues by reactions. The dominant workload
is storing a location per row and answering "what is near me", "which region contains this
point", or "what overlaps this box". Bike-share, ride-hailing, parcel delivery and asset
tracking all reduce to points plus proximity and geofence queries.

TiDB has none of it today: only the `mysql.TypeGeometry` constant exists
(`pkg/parser/mysql/type.go`), with no value representation and no `ST_*` functions, so
users encode geometry into scalar columns by hand and compute distances in the
application. This design covers the basic layer only.

## Detailed Design

**MySQL-compatible, extensible where the extension is free.** The two directions are
deliberately asymmetric:

- **`ST_GeomFrom*` accepts a superset of MySQL.** Z/M coordinates, SRIDs outside 0 and
  4326, and option values MySQL rejects are all accepted, and the storage layer keeps them
  losslessly. Accepting more cannot break a query that works on MySQL.
- **`ST_As*` emits what MySQL emits.** It errors where MySQL cannot express a value,
  because changing the bytes a client receives is where compatibility actually breaks.
  Emitting Z/M is a later extension, and then only behind an explicit option.

So a stored value can exist that no `ST_As*` can express. It reads back as the raw column
value, and no v1 function computes on it.

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

**Version 1 is [EWKB](https://libgeos.org/specifications/wkb/#extended-wkb)** as defined by
PostGIS and GEOS: standard WKB whose 32-bit type word carries three high-bit flags,
`0x80000000` for Z, `0x40000000` for M, and `0x20000000` meaning a `u32` SRID follows the
type word on the outermost geometry. Chosen because it expresses everything stored here,
and because a 2D geometry with no SRID flag is plain OGC WKB byte for byte.

| Rule | |
| --- | --- |
| Versioning | Numbered from 1, so a leading `0x00` is never a valid version. |
| Lossless | Exact `f64` coordinates and full geometry structure, never truncated. |
| SRID | Carried by the SRID flag, which may be left unset where the column fixes it with `SRID n`, since that is still valid EWKB. It cannot be dropped unconditionally: an unrestricted `GEOMETRY` column holds a per-row SRID, and a geometry outside any column (function result, join or sort intermediate) has no column metadata to recover it from. |
| Byte order | Left to EWKB, which flags it per geometry and permits both. |
| MySQL bytes | Not matched. MySQL stores `<srid u32 LE><WKB>` and is 2D only; `ST_AsBinary`, dump/reload and the wire protocol convert at the boundary, which for a 2D value is dropping the SRID flag. |
| Coordinate dimension | XY, XYZ, XYM and XYZM are storable, covering GeoJSON positions (XY and XYZ) and measured geometry. Every v1 function is 2D, as in MySQL. |
| SRIDs outside 0 and 4326 | Stored and returned unchanged in an unrestricted `GEOMETRY` column, as in MySQL. |

Extended data (Z/M coordinates, unsupported SRIDs) returns as the raw column value;
`ST_SRID` and `ST_GeometryType` answer for it, and everything that interprets coordinates
errors. An opt-in writer that emits it is a later extension. Why EWKB rather than the
alternatives: [Investigation & Alternatives](#investigation--alternatives).

### SRID model

| | SRID 0 | SRID 4326 |
| --- | --- | --- |
| Coordinate system | abstract Cartesian plane, unitless X/Y | WGS 84 geographic, latitude/longitude |
| Bounds | none, the full finite IEEE-754 double range, as MySQL | latitude `[-90, 90]`, longitude `(-180, 180]` |
| Rejected on ingest | Inf/NaN, `ERROR 3037` | out-of-range latitude (`ERROR 3617`) and longitude (`ERROR 3616`) |
| Measurement | planar (Cartesian) | geodesic on the WGS 84 ellipsoid, as MySQL |

Codes and wording are matched as closely as possible on every ingest path:
`ST_GeomFromText`, `ST_GeomFromWKB`, `ST_GeomFromGeoJSON`, and the constructors `Point`,
`LineString`, `Polygon`, `MultiPoint`, `MultiLineString`, `MultiPolygon` and
`GeometryCollection`. The same goes for `ERROR 3618` (function not implemented on a
geographic SRS) and `ERROR 3643` (SRID does not match the column). An unmatched message is
a gap to close, not grounds for a different code.

Planar versus geodesic follows the **SRS class** (SRID 0 and projected are Cartesian,
geographic is geodesic), as in MySQL. Adding SRIDs later therefore adds catalog rows and
per-class parameters, not code paths.

**Catalog.** `information_schema.st_spatial_reference_systems` must be queryable and
return exactly two rows, with MySQL's columns (`SRS_NAME`, `SRS_ID`, `ORGANIZATION`,
`ORGANIZATION_COORDSYS_ID`, `DEFINITION`, `DESCRIPTION`): SRID 0 with an empty name and
definition and no organization, and 4326 as `WGS 84` / `EPSG` / 4326 with the
`GEOGCS["WGS 84",DATUM[...]]` definition string. Whether that is a view over an internal
table or a synthesized one is an implementation choice; MySQL uses a view over a data
dictionary table that is itself unreadable, even by `root` (`ERROR 3554`). Both rows are
what MySQL returns for those two SRIDs, column for column; no dataset is imported. The
catalog is read-only, so `CREATE SPATIAL REFERENCE SYSTEM` is rejected, and supporting it
is on the extension path. DDL validates `SRID n` against the catalog rather than against a
hardcoded pair. Without it, asking which SRIDs a server supports is an unknown-table
error.

**EPSG attribution.** Filling the catalog from the
[EPSG dataset](https://epsg.org/) later brings its terms of use with it: IOGP's ownership
has to be acknowledged wherever the data is published, and anyone given the data has to
be told those terms, so that work carries a `LICENSES/EPSG-TERMS-OF-USE` entry beside
the existing `QL-LICENSE` and `Unicode-DFS-2016-LICENSE`.

**Axis order.** EPSG:4326 defines (latitude, longitude), so the first coordinate is the
latitude, and v1 follows MySQL so that `ST_Latitude`/`ST_Longitude`, distances and WKT
round-trips match. WKB carries two unlabelled doubles, so the same bytes mean different
things across ecosystems: PostGIS uses one fixed easting/longitude-first order for every
SRS, and roughly a third of the SRIDs in MySQL's catalog disagree with it, across both
geographic and projected systems. GeoJSON (RFC 7946, always longitude-first) and the
explicit `ST_Latitude`/`ST_Longitude` accessors are unambiguous. `ST_X`/`ST_Y` are
positional, so on 4326 `ST_X` is the latitude here and in MySQL (verified: `ST_X` =
`ST_Latitude` = 30 for `POINT(30 50)`) and the longitude in PostGIS. The per-SRID breakdown
belongs in the user docs as migration guidance.

Coordinates are **stored as parsed**, so latitude-first on 4326, which is the order S2 wants
and costs no swap on the geodesic and index paths. MySQL stores the opposite order and swaps
at every boundary, visible as `HEX(g)` and `ST_AsBinary(g)` returning swapped coordinates
for the same 4326 point; both engines emit the same WKB.

**Extension path** (documented, not built here):

| Step | Cost |
| --- | --- |
| Fill the catalog from the full EPSG dataset, taken from EPSG or PROJ's `proj.db`, with the dataset version pinned in the docs and IOGP attribution carried. The set drifts: MySQL shipped 5,152 rows in 8.0.46 and 5,238 in 8.4 and 9.7 | moderate, and a prerequisite for the rest |
| All projected SRSs (e.g. 3857 Web Mercator) | low: planar X/Y, so the same Cartesian functions apply and only the bounds are per-SRS |
| Geographic SRSs beyond 4326 | moderate: exact geodesic refine per ellipsoid |
| `ST_Transform`, which MySQL has had since 8.0.13 and which reprojects between two SRSs | moderate, and pointless before the catalog: with only 0 and 4326 there is nothing to transform to, since 4326 to 4326 is a no-op and MySQL itself rejects a transform to 0 with `ERROR 3742` |
| User-defined SRSs through `CREATE [OR REPLACE] SPATIAL REFERENCE SYSTEM` and `DROP SPATIAL REFERENCE SYSTEM`, which MySQL also has (there is no `ALTER`; modification is `CREATE OR REPLACE`) | bigger: a writable catalog, a WKT SRS parser, and catalog changes that have to replicate |

DDL restricts the `SRID n` attribute to 0 or 4326. An unrestricted `GEOMETRY` column may
still hold values of any SRID (see [Types and storage](#types-and-storage)).

### Function set

v1 is the minimal set needed to store, read, inspect, measure and filter geometry, all of it
present in MySQL 8.0.46 / 8.4 / 9.7, whose spatial function sets are identical. The list is
an **allowlist**: only these are registered, and anything else spatial is an unknown
function until a later milestone adds it.

- **`ST_GeomFrom*`**, a geometry from an external format: `ST_GeomFromText`,
  `ST_GeomFromWKB`, `ST_GeomFromGeoJSON`.
- **`ST_As*`**, an external format from a geometry: `ST_AsText` (`ST_AsWKT`),
  `ST_AsBinary` (`ST_AsWKB`), `ST_AsGeoJSON`.
- **Option arguments.** `axis-order` on the WKT and WKB members of both groups, taking
  `lat-long`, `long-lat` or `srid-defined` (the default), rejecting anything else with
  `ERROR 3559` and having no effect at SRID 0. It is the explicit way to read or write
  longitude-first data against a latitude-first SRS, so a client need not pre-swap.
  `ST_GeomFromGeoJSON` takes its own `options` and `srid` arguments. MySQL defines
  `options` 1 to 4, where 1 rejects coordinate dimensions above 2 and is the default and
  2, 3 and 4 strip them. TiDB extends the range with two values MySQL rejects:

  | `options` | Coordinates | Anything else |
  | --- | --- | --- |
  | 5 | keep whatever the stored format holds, so a third element becomes Z | error |
  | 6 | the same | ignore |

  Both ignore what MySQL ignores. The difference is a position with more than three
  elements, which RFC 7946 leaves undefined: 5 errors on it, 6 drops it.

  `ST_AsGeoJSON(g [, digits [, flags]])` takes MySQL's two: `digits` rounds the
  coordinates, defaulting to full precision and rejecting a negative value, and `flags` is
  a bitmask from 0 to 7 where bit 0 adds `bbox`, bit 1 a short CRS URN (`EPSG:4326`) and
  bit 2 a long one (`urn:ogc:def:crs:EPSG::4326`), long overriding short. Anything above 7
  is an error. The `bbox` is emitted in output axis order, so it is longitude-first on
  4326 like the coordinates beside it.
- **Constructors:** the full set of MySQL's
  [functions that create geometry values](https://dev.mysql.com/doc/refman/8.0/en/gis-mysql-specific-functions.html):
  `Point`, `LineString`, `Polygon`, `MultiPoint`, `MultiLineString`, `MultiPolygon`,
  `GeometryCollection`.
  `Point(x, y)` returns SRID 0; `ST_SRID(g, srid)` then stamps the SRS, validating the
  coordinates (`ERROR 3731`, `ERROR 3732`) without transforming them. For a geographic SRS
  that makes `Point` **(longitude, latitude)**, the opposite of WKT at 4326:
  `ST_SRID(Point(30, 50), 4326)` is `POINT(50 30)`, latitude 50. Since values are stored
  latitude-first at 4326, this is where the internal order becomes observable and the pair
  must be swapped.
- **Accessors:** `ST_X`, `ST_Y`, `ST_Latitude`, `ST_Longitude`, `ST_SRID` (getter and the
  `ST_SRID(g, srid)` setter), `ST_GeometryType`, `ST_Dimension`, `ST_Envelope`,
  `ST_IsEmpty`, `ST_IsValid`, `ST_StartPoint`, `ST_EndPoint`, `ST_PointN`, `ST_NumPoints`,
  `ST_ExteriorRing`, `ST_NumInteriorRings`, `ST_Centroid`. `ST_Centroid` is Cartesian-only,
  as in MySQL, which raises `ERROR 3618` for it on 4326.
- **Measurement:** `ST_Area`, `ST_Length`, `ST_Distance`, `ST_Distance_Sphere`.
- **Predicates (DE-9IM):** `ST_Within`, `ST_Contains`, `ST_Intersects`, `ST_Equals`,
  `ST_Disjoint`, `ST_Touches`, `ST_Crosses`, `ST_Overlaps`.
- **PostGIS extras:** `ST_Covers`, `ST_CoveredBy`, included because the index layer makes
  them index-eligible region predicates (`Covers ⊇ Contains`, `CoveredBy ⊇ Within`, so a
  covering-cell prefilter has no false negatives). Other PostGIS-only functions are added
  later only if index-supported or by demand.

**SRID handling** differs by direction, and a round-trip hides it:

| Conversion | SRID from | Axis order | SRID in the result |
| --- | --- | --- | --- |
| `ST_GeomFromText(wkt [, srid [, opt]])` | the argument, else 0 | that SRID's SRS order, overridable by `axis-order` | yes |
| `ST_GeomFromWKB(wkb [, srid [, opt]])` | the argument, else 0 | that SRID's SRS order, overridable by `axis-order` | yes |
| `ST_GeomFromGeoJSON(json [, opt [, srid]])` | the `srid` argument, else the `crs` member, else 4326 | always longitude-first (RFC 7946) | yes |
| constructors (`Point`, `LineString`, ...) | nothing, always 0 | none applied | 0; stamp it with `ST_SRID` |
| `ST_AsText(g [, opt])`, `ST_AsBinary(g [, opt])` | the geometry | its SRS order, overridable by `axis-order` | **no**, plain WKT/WKB |
| `ST_AsGeoJSON(g [, digits [, flags]])` | the geometry | always longitude-first | **no** by default; `flags` 2 and 4 add a CRS URN |

The two **no** cells are why a WKT or WKB round-trip loses the SRID and has to be given it
again on the way back in. The constructor row is why `ST_SRID(Point(30, 50), 4326)` and
`ST_GeomFromText('POINT(30 50)', 4326)` are different points: the SRS is applied where the
coordinates are parsed, and `ST_SRID` only writes metadata.

A later milestone covers the geometry-processing tail (`ST_Buffer`, `ST_Union`,
`ST_Intersection`, ...), the typed I/O aliases, the `MBR*` family, geohash, the niche
accessors.

Semantics match MySQL, with three v1 limitations:

- On 4326, `ST_Distance`/`ST_Length` are ellipsoidal (Andoyer, matching MySQL to
  sub-metre); `ST_Distance_Sphere` is the great-circle variant.
- `ST_Area` on 4326 **errors** with `ERROR 3618`, MySQL's shape for its own Cartesian-only
  functions. This diverges: MySQL computes it geodesically (12308778368.75 m2 for a
  1-degree box). A planar degree2 or an off-by-0.45% spherical value would be silently
  wrong; Karney ellipsoidal area is a later extension.
- The predicates are OGC-correct via `simplefeatures`, which is planar. On 4326 the region
  predicates get a geodesic point-in-polygon, but polygon/polygon relations stay planar and
  diverge from MySQL. See [Unresolved Questions](#unresolved-questions).

**GeoJSON.** Every RFC 7946 geometry is supported. The container and annotation members
follow MySQL, verified on 8.4.6 and 9.7.2:

| Input | Result |
| --- | --- |
| `Feature` | its bare geometry, so a Feature holding a point yields `POINT` |
| `FeatureCollection` | `GEOMETRYCOLLECTION` of the features' geometries, `GEOMETRYCOLLECTION EMPTY` if there are none |
| `"geometry": null` | SQL `NULL` |
| `properties`, `id`, `bbox`, foreign members | ignored, and not validated: MySQL accepts a `bbox` that is the wrong arity, contradicts the geometry, or is not even an array |
| named `crs` URN | sets the SRID (`urn:ogc:def:crs:OGC:1.3:CRS84` is 4326, link-object CRSs are not accepted, and a nested `crs` naming a different SRID errors); absent, the SRID is 4326 |
| position with a third coordinate | rejected under the default `options` 1 with `ERROR 3073`, stripped under 2, 3 and 4, kept as Z under TiDB's 5 and 6 |
| position with more than three coordinates | as above, except TiDB's 5 errors and 6 drops the extras |
| unknown `type`, or a required member missing | `ERROR 3072`, and `ERROR 3070` naming the member |

The `options` argument accepts such positions and strips the extra coordinates.
`ST_AsGeoJSON`'s bbox and CRS-URN flags ship with the tail. Round-trips are not
idempotent: a FeatureCollection returns from `ST_AsGeoJSON` as a GeometryCollection.

Every geometry-returning builtin is typed `GEOMETRY`, so a plain B-tree functional index
over such an expression is rejected.

### Geometry engine

Pure Go, no cgo, so the stack builds with `CGO_ENABLED=0` and needs no libgeos in the
Bazel/CI sandbox; the only Bazel work is adding `DEPS.bzl` proxy-fetch entries.

- `github.com/peterstace/simplefeatures`: OGC/DE-9IM model, WKT/WKB/GeoJSON I/O,
  predicates, planar measurement. Validated byte-identical to MySQL in the PoC.
- `github.com/golang/geo` (Google's S2 port, Apache 2.0): spherical geometry for 4326.
- `pkg/util/geomrel`: in-tree ellipsoidal distance/length (Andoyer) and geodesic refine.

The processing tail may need GEOS-equivalent algorithms; it is deferred with the rest of
the tail.

### Type plumbing

`TypeGeometry` must flow through the generic value machinery so geometry behaves like any
other column value outside the `ST_*` functions. The PoC audited ~28 operations (GROUP BY,
hash/merge join, DISTINCT, ORDER BY, UPDATE/DELETE/REPLACE, window, `INSERT ... SELECT`,
`UNION`); the touch points are:

- `pkg/parser`: geometry type grammar and the `SRID` column attribute. The only grammar
  change, since `ST_*` are generic calls; regenerates `parser.go` once.
- `pkg/types` / field type: the geometry field type and its flen/charset handling.
- `pkg/util/chunk`: `Row.GetDatum` must return geometry as a binary string; without this the
  PoC found `INSERT ... SELECT` nulled geometry.
- `pkg/expression/builtin_cast.go`: cast-to-string flen setup; without this the PoC found
  `UNION` asserted.
- `pkg/expression`: the `ST_*` builtins (`builtin_geo.go`) and their registration.

Geometry sorts, compares and hashes as its binary value: well-defined, not spatially
meaningful.

### SQL surface and examples

    col_name {GEOMETRY | POINT | LINESTRING | POLYGON | MULTIPOINT
              | MULTILINESTRING | MULTIPOLYGON | GEOMETRYCOLLECTION}
        [NOT NULL] [SRID {0 | 4326}]

The type names stay usable as identifiers, as in MySQL: a column named `point` keeps
working, and `Point(x, y)` is a function call rather than a type reference. `ST_*` are
ordinary function calls and need no syntax of their own. `SHOW CREATE TABLE` emits the
MySQL form. No spatial index syntax belongs to this layer.

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

### Scope and deferrals

Out of scope here, each with a home:

- The **spatial index** and its pushdown:
  [`docs/design/2026-06-25-spatial-index.md`](2026-06-25-spatial-index.md)
  ([#69473](https://github.com/pingcap/tidb/pull/69473)), for which this layer is the prerequisite.
- The **geometry-processing function tail**, typed I/O aliases, `MBR*` family, geohash and
  niche accessors: a later, parallel expression-layer milestone.
- **SRIDs beyond 0 and 4326**, the full SRS catalog and `ST_Transform`: the extension path
  above. `ST_Transform` is MySQL functionality rather than a PostGIS extra, but it has
  nothing to do until more SRSs exist, so it is out of scope for v1.
- **Coprocessor pushdown.** The predicates and measurement functions are deterministic
  scalars and pushdown-eligible; pushing them filters at the storage node instead of
  shipping every candidate geometry to TiDB, with or without an index. It is cross-repo work
  (tipb signatures plus a TiKV-side evaluator), specified with the index design, which owns
  the pushdown contract.
- **The other two spatial `information_schema` tables**: `st_geometry_columns`, one row per
  geometry column and derivable from the schema, and `st_units_of_measure`, 47 static rows
  in MySQL that matter once projected SRSs with varied units exist.
- **Where the per-SRS attributes come from.** MySQL's six columns do not expose the class,
  axis order, bounds, unit or ellipsoid; those live inside the WKT `DEFINITION`, which MySQL
  parses at runtime. The full-catalog work chooses between a WKT SRS parser, which
  `CREATE SPATIAL REFERENCE SYSTEM` needs anyway, and internal parsed metadata. Neither
  widens this table: extra columns belong in a TiDB-specific companion, expressed as WKT2
  (ISO 19162) or PROJJSON.
- **`ST_SwapXY`**, which swaps a geometry's coordinates in place. The `axis-order` option
  covers the read and write direction in v1; this is the geometry-mutating variant.
- **3D / measured (Z/M) geometry**: computation is 2D only, as in MySQL/MariaDB, while the
  values are stored and returned unchanged.

### Compatibility

| Area | Effect |
| --- | --- |
| Partition table, clustered index, async commit | None. Geometry cannot be a primary or clustering key, having no meaningful ordering. |
| Charset and collation | Not applicable; the value is binary. |
| Parser | One-time type and `SRID` grammar change; regenerates `parser.go`, run `make bazel_prepare`. `ST_*` are generic calls. |
| DDL | New column types and the `SRID` attribute, restricted to 0/4326, plus subtype constraints. `ALTER` follows MySQL, verified on 8.4.6: adding or changing `SRID n` validates every existing row and fails with `ERROR 3643` on the first mismatch; dropping the attribute always succeeds; narrowing the subtype (`GEOMETRY` to `POINT`) succeeds only if every value fits, else `ERROR 1416`; widening always succeeds; converting the column to a binary type carries the bytes over; `DROP COLUMN` is ordinary. An SRID change is validation, never a coordinate rewrite. |
| `information_schema` | One new table, `st_spatial_reference_systems`, read-only with two static rows. Its two siblings in MySQL are deferred. |
| Planner, statistics, executor | `ST_*` evaluate on the normal expression path; geometry predicates are ordinary `Selection`s with no access path of their own. No new operator, access path or statistics. |
| TiKV | None. Values are ordinary binary strings; pushdown is deferred. |
| TiFlash, BR, TiCDC, Dumpling, Lightning | Regular column data. Tools need only carry the bytes and the `SRID`/type metadata; dump/reload uses MySQL's internal format or WKT, not the stored bytes. |
| Upgrade | Additive, and gated by no user-visible variable: the type is absent until it is complete. Any switch used while the work lands is a merge convenience, not documented surface. |
| Downgrade | A release without the type cannot read a table that has a geometry column, so those columns must be dropped first, an ordinary `DROP COLUMN`. |

## Test Design

### Functional Tests

- I/O round-trips: `ST_GeomFromText`/`ST_AsText`, `ST_GeomFromWKB`/`ST_AsBinary`,
  `ST_GeomFromGeoJSON`/`ST_AsGeoJSON` for every subtype, byte-compared to MySQL output
  including its `ST_AsText` spacing and axis order.
- Accessors and measurement against values measured on MySQL, e.g. for a 1-degree step on
  4326: `ST_Distance` 111319.49 m, `ST_Distance_Sphere` 111195.08 m, `ST_Area` of a 1-degree
  box 12308778368.75 m2.
- Predicates: the eight DE-9IM predicates plus `Covers`/`CoveredBy` on curated geometry
  pairs, matched to MySQL where semantics agree, with boundary cases explicit.
- SRID validation: 4326 out-of-range errors on every ingest path, SRID 0 Inf/NaN rejection,
  and mixed-SRID arguments to a binary geometry function giving `ERROR 3033`, while the
  SQL comparison operators keep comparing the stored bytes without erroring.
- The catalog: `st_spatial_reference_systems` returns the two rows with the same column
  values MySQL gives for SRID 0 and 4326, and `CREATE SPATIAL REFERENCE SYSTEM` errors.
- Extended data: Z/M values entered as WKB, and SRIDs outside 0 and 4326, store and read
  back byte-identical, while `ST_AsBinary`/`ST_AsText`/`ST_AsGeoJSON` and every function
  that interprets coordinates error clearly on them.
- GeoJSON: the table above, each row matched against MySQL 8.4 and 9.7, plus `options` 5
  and 6 keeping a Z position and differing on a fourth element, and `ST_AsGeoJSON`'s
  `digits` rounding and each `flags` bit, byte-compared to MySQL.
- `axis-order`: `long-lat` swaps on read and on write at 4326, `lat-long` and
  `srid-defined` agree there, the option is inert at SRID 0, and a bad value gives
  `ERROR 3559`.
- Format version: version 1 decodes; an unknown or zero version byte is rejected with a
  clear error rather than misparsed.
- Type plumbing: geometry through the audited operation surface returns correct bytes.
- DDL: the `ALTER` matrix above, including `ERROR 3643` on an SRID change that existing rows
  violate and `ERROR 1416` on a narrowing that they do not fit.

### Scenario Tests

- A points table answering proximity (`ST_Distance_Sphere ≤ r`) and geofence
  (`ST_Within(point, polygon)`), matching MySQL.
- 4326 edge cases: a query near a pole and one across the antimeridian.
- Application shape: lat/long ingest via WKT/GeoJSON, read back via `ST_AsGeoJSON`.

### Compatibility Tests

- MySQL byte-identical suite for the v1 function surface (the PoC's `spatial_compat`
  integration test is the basis).
- Dumpling/Lightning round-trip of a table with geometry columns; TiCDC and BR pass-through;
  behavior unaffected when TiFlash is absent.
- Parser, DDL, planner and executor as listed in Compatibility.
- Upgrade and downgrade paths.

### Benchmark Tests

- Geometry ingest and read throughput vs a scalar-encoded baseline.
- Geometry-predicate latency across selectivities with no other predicate to narrow the
  scan, the pre-index baseline the index layer will be measured against.

## Impacts & Risks

Intended impact: geometry becomes a first-class, MySQL-compatible value and query surface,
so applications can store locations and run proximity and geofence queries in SQL without
application-side geometry code.

Risks:

- **Prerequisite coupling:** the index and pushdown layers code against this type, so the
  value-format and axis-order decisions here are lock-ins for them.
- **Value-format lock-in:** the on-disk format is hard to change post-GA; mitigated by the
  version byte and by storing Z/M and unsupported SRIDs losslessly from the start.
- **4326 semantics gaps:** geodesic `ST_Area` and polygon/polygon relations diverge from
  MySQL; mitigated by erroring or documenting rather than returning wrong values.
- **MySQL error parity:** exact codes and messages may not match initially (the PoC used
  placeholder wording); a compatibility risk, not a correctness one.
- **Pure-Go library gaps:** `simplefeatures` covers the v1 surface but not the GEOS-class
  processing tail, which is deferred.

## Investigation & Alternatives

- **Which WKB dialect for version 1.** Three candidates, all published:

  | Candidate | SRID | Z/M | Notes |
  | --- | --- | --- | --- |
  | MySQL internal, `<srid u32 LE><WKB>` | prefix | **no** | 2D only, so it cannot hold what this design stores |
  | ISO WKB (SFA 1.2.1 / SQL-MM) | no | type code +1000/2000/3000 | what `simplefeatures` already reads and writes |
  | **EWKB (PostGIS/GEOS)** | type-word flag | type-word flags | chosen |

  EWKB carries SRID, Z, M and XYZM in one defined format, with plain 2D WKB as its
  degenerate case, and keeps the space optimization in-format: a value whose SRID the column
  fixes leaves the SRID flag unset. The cost is a codec. `simplefeatures` implements the ISO
  type-code convention (`geomCode % 1000` for the type, `/ 1000` for the dimension), not
  EWKB's flags, so TiDB owns the EWKB header encode/decode and hands the body to the
  library. TiKV's `geo` crate is 2D-only and already hand-rolls its decoder, so it needs a
  header change and nothing more; Z/M values are not pushable regardless.
- **A leaner layout as version 1.** Rejected for v1. EWKB carries redundancy (a per-row
  SRID the column usually fixes, a byte-order flag per (sub)geometry, WKB framing), but
  profiling the PoC bounded the win: geometry decode is ~2% of insert CPU, and the ~27% of
  query CPU spent parsing WKB is only about half attributable to the stored value, the rest
  being a re-parse inside the predicate library that no format can remove. The version byte
  defers the choice without a migration.
- **Matching MySQL's stored bytes.** Rejected as a non-goal: I/O compatibility is a boundary
  conversion, and MySQL does the same internally.
- **cgo/libgeos (go-geos).** Rejected for v1: it gives OGC-correct geometry but needs
  `libgeos` in the Bazel/CI sandbox, which broke the build. The PoC moved to pure-Go
  `simplefeatures` and stayed MySQL byte-identical. Revisit for the processing tail.
- **The full [#38916](https://github.com/pingcap/tidb/pull/38916) surface at once.** Rejected as too large to review and
  land.
- **Geometry as a generic BLOB with application-side functions.** The status quo; loses
  MySQL compatibility, type safety, and any path to a spatial index.
- **PostGIS axis order and always-planar `geometry` semantics.** Rejected in favor of MySQL
  parity; the differences are documented under SRID model.

## Unresolved Questions

**Polygon/polygon relations on 4326.** MySQL treats a geographic polygon's edges as
geodesics; `simplefeatures`, and every other pure-Go option, treats them as straight lines
in latitude/longitude. For `POLYGON((0 0, 0 80, 60 0, 0 0))` on 4326, MySQL 8.4.6 answers
`ST_Within` **true** for the points `(30 40)`, `(33 40)`, `(36 40)` and `(40 40)`, while the
same coordinates against the same ring evaluated planar answer **false** for all four: the
great-circle edge bows away from the straight one by degrees, so whole regions flip rather
than boundary cases. Small polygons, the common geofence case, are unaffected.

Three resolutions, in increasing cost:

1. **Ship planar polygon/polygon and document it.** Correct for small geofences, silently
   wrong for continental ones.
2. **Error when it would matter.** Reject a geographic polygon/polygon relate whose operands
   exceed some extent. Needs a defensible threshold, and the error is itself a divergence,
   since MySQL answers.
3. **Implement geodesic relate.** Full parity, and the only option that makes the index
   layer's refine exact on 4326. It needs a geographic DE-9IM implementation on the TiDB
   side and, once pushdown lands, the TiKV side. No pure-Go library provides one: S2 gives
   coverings and predicates on spherical shapes, not the DE-9IM matrix.

This design owns the decision, since the type layer owns predicate semantics. No bytes are
locked in either way.
