# Proposal: Basic Geospatial Support

- Author(s): [Mattias Jonsson](http://github.com/mjonss), [Daniël van Eeden](http://github.com/dveeden)
- Discussion PR: https://github.com/pingcap/tidb/pull/70420
- Tracking Issue: https://github.com/pingcap/tidb/issues/6347

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Terminology](#terminology)
* [Detailed Design](#detailed-design)
    * [Types and storage](#types-and-storage)
    * [SRID model](#srid-model)
    * [Reference surface](#reference-surface)
    * [Function set](#function-set)
    * [Geometry engine](#geometry-engine)
    * [Type plumbing](#type-plumbing)
    * [SQL surface and examples](#sql-surface-and-examples)
    * [Phasing](#phasing)
    * [Scope and deferrals](#scope-and-deferrals)
    * [Compatibility](#compatibility)
    * [Differences from MySQL](#differences-from-mysql)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)
* [Future extensions](#future-extensions)
* [Appendix: SRS catalog](#appendix-srs-catalog)
    * [Catalog row contents](#catalog-row-contents)
    * [EPSG terms of use](#epsg-terms-of-use)
* [Appendix: PostGIS delta for the type layer](#appendix-postgis-delta-for-the-type-layer)
    * [Type and surface mapping](#type-and-surface-mapping)
    * [A GEOGRAPHY type](#a-geography-type)
    * [Remaining delta](#remaining-delta)

## Introduction

This document proposes **basic geospatial support** for TiDB, MySQL-compatible, making
geometry storable, readable and queryable.

- **Types.** The `GEOMETRY` column type and its subtypes, with support for
  [`SRID`](#terminology) 0 and 4326 (to be extended later).
- **Storage.** `<version byte = 1>` + [EWKB](#terminology).
- **Binary in and out.** A bare `SELECT` and a bare literal both use MySQL's binary
  format, so `mysqldump` data loads unchanged; the stored bytes have their own pair,
  `ST_AsEWKB` and `ST_GeomFromEWKB`.
- **Functions.** The minimal `ST_*` set, including the [DE-9IM](#terminology) predicates.
- **Algorithms.** The same distance and relate algorithms as MySQL, so results are as
  compatible as possible.

This basic design does not cover **indexing** on `GEOMETRY` types, which is designed in
[PR #69473](https://github.com/pingcap/tidb/pull/69473) and which builds on this layer.
Later work (more SRIDs, the function tail, coprocessor pushdown, the index) extends this
design rather than replacing it.

This replaces the earlier geospatial design
([PR #38916](https://github.com/pingcap/tidb/pull/38916)).

The main goal of the design is basic MySQL compatibility. It should also be extensible
later, both towards better MySQL compatibility and beyond MySQL.

## Motivation or Background

TiDB is often used as unified storage because of the scalable storage,
vector search, HTAP and FTS capabilities.

Geospatial support would be a good addition to this, making this an even stronger option
for unified storage.

TiDB is used in companies that do package delivery, ride services, etc.
where geospatial data is used in various places.

Geospatial support is one of the most requested TiDB features: [tracking issue #6347](https://github.com/pingcap/tidb/issues/6347)
carries `feature/accepted` and ranks among the top open issues by reactions. The dominant workload
is storing a location per row and answering "what is near me", "which region contains this
point", or "what overlaps this box".

TiDB has none of it today: only the `mysql.TypeGeometry` constant exists
(`pkg/parser/mysql/type.go`), with no value representation and no `ST_*` functions, so
users encode geometry into scalar columns by hand and compute distances in the
application.

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
| [WGS 84](https://en.wikipedia.org/wiki/World_Geodetic_System) | World Geodetic System 1984, the datum and reference ellipsoid used by GPS, whose geographic coordinate system is EPSG:4326. |
| Reference surface | What a measurement or an edge is drawn on: the flat *plane*, a *sphere* of one constant radius, or the oblate WGS 84 *ellipsoid*. SRS class decides whether it is the plane or a curved surface; the function decides which curved surface. See [Reference surface](#reference-surface). |
| Geodesic | The locally shortest path along a curved reference surface, and equivalently the path that never turns sideways: whatever bending it does is forced on it by the surface, so it is the shape a string stretched between its two ends on that surface would take. Always along the surface, never the straight chord through the interior. |
| Great circle | The geodesic of a *sphere*, and the constructive way to say it: where a plane through the two endpoints and the centre cuts the surface. The contrast that matters here is that a great circle lies in a plane and an ellipsoidal geodesic in general lies in no plane, so the two are different curves rather than the same curve computed to different precision. A plane through the centre of an ellipsoid cuts it in a central ellipse, which is a third curve again. |
| Andoyer | The spherical law of cosines with a first-order flattening correction, approximating the ellipsoidal geodesic without iterating. MySQL uses it for every ellipsoidal computation, measurement and predicate alike; see [Reference surface](#reference-surface). |
| [DE-9IM](https://en.wikipedia.org/wiki/DE-9IM) | Dimensionally Extended 9-Intersection Model, the OGC model defining `ST_Within`, `ST_Contains`, `ST_Intersects` and the other topological predicates. |
| [GeoJSON](https://datatracker.ietf.org/doc/html/rfc7946) | JSON geometry encoding (RFC 7946), the third I/O format. |
| MBR | Minimum Bounding Rectangle; basis of MySQL's `MBR*` predicates (deferred). |
| [PROJ](https://proj.org/) | The reference reprojection library; `ST_Transform` is out of scope. |
| [PostGIS](https://postgis.net/) | The PostgreSQL spatial extension. Not a compatibility target; the delta is in [the appendix](#appendix-postgis-delta-for-the-type-layer). |

## Detailed Design

**MySQL-compatible, extensible where the extension is free.** The two directions are
asymmetric:

- **`ST_GeomFrom*` accepts a superset of MySQL.** Z/M coordinates are accepted through
  `ST_GeomFromEWKB` and GeoJSON `options` 5 and 6, as are option values MySQL rejects, and
  the storage layer keeps them losslessly. WKT, WKB and the bare literal stay 2D, as in
  MySQL. Accepting more cannot break a query that works on MySQL. SRIDs outside 0 and 4326
  are the exception; see *Extended data* in [Types and storage](#types-and-storage).
- **`ST_As*` emits what MySQL emits.** It errors where MySQL cannot express a value,
  because changing the bytes a client receives is where compatibility actually breaks.
  Emitting Z/M is a later extension, and then only behind an explicit option. WKT and WKB
  output of an SRID outside 0 and 4326 also needs `axis-order=long-lat`; see
  *Extended data*.

MySQL behaviors and measurements below were verified against running MySQL 8.4.6 and
9.7.2. The proof of concept is [PR #69475](https://github.com/pingcap/tidb/pull/69475).

### Types and storage

Types, all reusing the existing `mysql.TypeGeometry` field type with the subtype a
constraint on the stored value, as in MySQL: `GEOMETRY`, which accepts any subtype,
then `POINT`, `LINESTRING`, `POLYGON` and `GEOMETRYCOLLECTION` with its own subtypes
`MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`.
A column may carry a `SRID n` attribute (see [SRID model](#srid-model)).
At the KV layer a geometry is a binary string; no new column encoding is introduced.

Stored value:

    <format_version u8><payload>
    version 1:  EWKB

**Version 1 is [EWKB](https://libgeos.org/specifications/wkb/#extended-wkb)** as defined by
PostGIS and GEOS: standard WKB whose 32-bit type word carries three high-bit flags,
`0x80000000` for Z, `0x40000000` for M, and `0x20000000` meaning a `u32` SRID follows the
type word on the outermost geometry. Chosen because it expresses everything stored here,
and because below its header a 2D value is plain OGC WKB.

| Rule | |
| --- | --- |
| Versioning | Numbered from 1, so a leading `0x00` is never a valid version. |
| Lossless | Exact `f64` coordinates and full geometry structure, never truncated. |
| SRID | Always carried by the EWKB SRID flag in version 1, even where a `SRID n` column fixes it. The coprocessor, the index refine, TiCDC and TiFlash read stored values from the KV layer without schema ([Compatibility](#compatibility)), so the SRID has to travel in the value. |
| Byte order | Little-endian throughout, as MySQL stores it. Big-endian input is accepted and converted, so equal geometries have equal bytes. |
| Axis order | Longitude first on a geographic SRS, as in MySQL's binary format and PostGIS's EWKB; as given on SRID 0 and projected SRSs. |
| MySQL bytes | Not matched. MySQL stores `<srid u32 LE><WKB>` and is 2D only; the bare path converts at the boundary. See *Binary in and out* below. |
| Coordinate dimension | XY, XYZ, XYM and XYZM are storable, covering GeoJSON positions (XY and XYZ) and measured geometry. Functions that interpret coordinates reject Z/M in v1; see *Extended data* below. |
| SRIDs outside 0 and 4326 | Stored and returned unchanged in an unrestricted `GEOMETRY` column, as in MySQL. See *Extended data* below. |

**Binary in and out.** A value that leaves TiDB as bytes has to be acceptable coming back
as bytes, or an ordinary client round-trip breaks: read a column, hold the bytes, bind
them into a later `INSERT`. The storage format is wider than MySQL's, so the two cannot
share one bare path without guessing which format arrived. Each therefore gets a matched
pair:

| Format | Out | In |
| --- | --- | --- |
| MySQL internal, `<srid u32 LE><WKB>` | bare `SELECT g` | bare literal, `SET g = 0x...` |
| EWKB, the extended format | `ST_AsEWKB(g)` | `ST_GeomFromEWKB(0x...)` |

The bare path is MySQL's format in both directions, so ingest never has to guess: a bare
literal is always MySQL's format, and the stored format stays off the user surface. It is
what makes a Dumpling to Lightning round-trip work with no function call, and what lets a
`mysqldump` load unchanged, unless the table has a spatial index
([Differences from MySQL](#differences-from-mysql)). The SRID in those bytes is validated
against `SRID n` like any other ingest path, and the geometry has to consume the input
exactly: a byte short or a byte long is rejected, as MySQL rejects both with `ERROR 1416`.

**MySQL has one binary representation**, `<srid u32 LE><WKB>`, and uses it everywhere:
what it stores, what a bare `SELECT` returns over the wire, what it accepts as a literal,
and what it writes to the binlog are the same bytes. Verified on 9.7.2 for
`POINT(37.4 -122.1)` at 4326: `HEX(g)`, the raw wire response and the `Write_rows` row
image are byte-identical, and inserting those bytes as a literal reproduces the row
exactly. Its coordinates are in TiDB's stored order, so the bare path converts the header
and never reorders the coordinates.

**Extended data**, meaning Z/M coordinates or an SRID outside 0 and 4326, is stored
losslessly and is not otherwise supported in v1: no function interprets its coordinates,
and those that would error. Storing it is the contract, since it is what lets 3D geometry
and the wider SRS catalog arrive later as functions over data written today rather than as
a migration.

An SRID outside 0 and 4326 is accepted, in and out, wherever no SRS axis order has to be
applied: the bare path, the EWKB pair, GeoJSON, and WKT or WKB given
`axis-order=long-lat`. Its coordinates are not range-checked, since the SRS class is
unknown. WKT and WKB under `srid-defined` or `lat-long` reject it, since both depend on
the SRS: MySQL swaps only on a geographic one.

A Z/M value has no MySQL form, since MySQL is 2D, so it cannot come back on the bare path.
A value whose only extension is its SRID can, since MySQL's binary format carries any
SRID, which keeps replication and dump whole for a column MySQL itself accepts.

**Bounded parsing.** Nesting costs 9 bytes a level in WKB, so a value inside
`max_allowed_packet` can nest millions deep. MySQL returns
`ERROR 1436 "Thread stack overrun"` for a value too deep, at a depth that varies by path:
on 9.7.2 with the default 1 MiB `thread_stack`, 4,235 levels through `ST_GeomFromText` and
10,578 through a bare literal, while `ST_*` functions read only 7,062, so MySQL stores
values it cannot read back. TiDB bounds depth explicitly, since a Go stack overflow is a
fatal crash, and applies one bound across ingest and read, above real data and below
MySQL's storage limit, so it never persists a value it cannot read.

**Size.** An XY vertex is 16 bytes and compresses poorly, 24 with Z or M and 32 with both.
This design adds no point-count cap, since TiDB's entry size limit already bounds a stored
value. Unlike most types, geometry reaches that limit with ordinary data: national
boundaries at OSM resolution run to hundreds of thousands of vertices.

Why EWKB rather than the alternatives:
[Investigation & Alternatives](#investigation--alternatives).

### SRID model

| | SRID 0 | SRID 4326 |
| --- | --- | --- |
| Coordinate system | abstract Cartesian plane, unitless X/Y | WGS 84 geographic, latitude/longitude |
| Bounds | none, the full finite IEEE-754 double range | latitude `[-90, 90]`, longitude `[-180, 180]`, except `(-180, 180]` through GeoJSON, as MySQL |
| Rejected on ingest | Inf/NaN: `ERROR 3037`, or `ERROR 1416` on a bare literal | out-of-range latitude (`ERROR 3617`) and longitude (`ERROR 3616`) |
| Measurement | planar (Cartesian) | Andoyer on the WGS 84 ellipsoid for distance and length, as MySQL; stated per operation in [Reference surface](#reference-surface) |

Codes and wording are matched as closely as possible on every ingest path:
`ST_GeomFromText`, `ST_GeomFromWKB`, `ST_GeomFromGeoJSON`, and the MySQL-specific
constructors `Point`, `LineString`, `Polygon`, `MultiPoint`, `MultiLineString`, `MultiPolygon` and
`GeometryCollection`. The same goes for `ERROR 3618` (function not implemented on a
geographic SRS) and `ERROR 3643` (SRID does not match the column). A binary geometry
function given two geometries of different SRIDs raises `ERROR 3033` and computes nothing;
the SQL comparison operators are not geometry functions and keep comparing the stored
bytes without erroring. An unmatched message is a gap to close, not grounds for a
different code.

Planar versus curved follows the **SRS class** (SRID 0 and projected are Cartesian,
geographic is curved), as in MySQL. Adding SRIDs later therefore adds catalog rows and
per-class parameters, not code paths.

**Catalog.** `information_schema.st_spatial_reference_systems` returns exactly two rows,
SRID 0 and 4326, with MySQL's columns and the values MySQL gives for them; no dataset is
imported. It is read-only, so `CREATE SPATIAL REFERENCE SYSTEM` is rejected and deferred to
[Future extensions](#future-extensions). DDL validates `SRID n` against the catalog rather
than against a hardcoded pair, so asking a server which SRIDs it supports is a query rather
than an unknown-table error. Row contents are in
[the appendix](#appendix-srs-catalog).

**Axis order.** Stored longitude first on a geographic SRS, as in MySQL and PostGIS.

### Reference surface

SRS class decides *whether* a computation is planar. On a curved surface two independent
choices remain, and separating them is what keeps 4326 coherent:

- The **edge model** is what curve joins two vertices. It decides all topology: inside or
  outside, whether two edges cross, which edge is nearest.
- The **metric** is how many metres lie between two given points. It decides only the
  number reported.

A distance formula answers the second and says nothing about the first, which is why
`ST_Distance` between two points needs no edge model at all and every case involving a line
or polygon does.

**One edge model, everywhere.** All 4326 topology uses a single edge model, so:

> `ST_Distance(g1, g2) = 0` if and only if `ST_Intersects(g1, g2)`, for every operand
> pair both accept, and likewise for its negation `ST_Disjoint`.

The boundary-sensitive predicates keep their DE-9IM definitions: a point on a polygon's
boundary is at distance zero and correctly not `ST_Within`.

**That model is Andoyer, and v1 takes only point operands.** Everywhere else v1 answers as
MySQL does or not at all; predicates would otherwise be the one surface shipping a
*different answer* rather than a *smaller* one. The edge model is part of the contract and
the operand set is not: widening the operands only makes rejected queries answer, while
another edge model would flip booleans between releases.

Matching MySQL is affordable on the half that matters. Boost decides which side of a
segment a point falls on by comparing azimuths from the inverse solution, so a
point-in-polygon crossing test needs only the inverse with azimuths: closed form, and an
extension of the Andoyer inverse the proof of concept already carries. The expensive half
is a predicate between two extended geometries, which needs geodesic segment intersection
feeding a 9-intersection matrix. **v1 therefore restricts the predicates to pairs with a
point operand**, meaning a `POINT` or `MULTIPOINT`, and defers the rest
([Future extensions](#future-extensions)). Dropping those pairs to a cheaper surface
instead would break the invariant above, so the lever is the operand type rather than the
surface. The rejected edge models and the measurements behind this are in
[Investigation & Alternatives](#investigation--alternatives).

| 4326 operation | Needs | v1 | MySQL |
| --- | --- | --- | --- |
| `ST_Distance` between point operands, `ST_Length` | metric only | ellipsoid (Andoyer) | ellipsoid (Andoyer) |
| `ST_Distance_Sphere` | metric only, spherical by definition | sphere, great circle | sphere, great circle |
| `ST_Distance` to a line or polygon | edge model, then the nearest point on an edge, which iterates | deferred, see [Future extensions](#future-extensions) | ellipsoid, Andoyer edges |
| the eight DE-9IM predicates, with a point operand | edge model | Andoyer edges | ellipsoid, Andoyer edges |
| the predicates between two extended geometries | edge model, and segment intersection on top | deferred, see [Future extensions](#future-extensions) | ellipsoid, Andoyer edges |

Everything on SRID 0 is planar throughout, with no operand restriction: the plane needs no
edge model. Every MySQL cell above is measured against a running engine rather than taken
from documentation.

**The metric is Andoyer, as in MySQL, not the exact geodesic.** It matches MySQL to about
10 cm, MySQL's own resolution at short range, and inherits MySQL's error against the exact
geodesic on purpose, since a more accurate library would be off from MySQL by exactly this
much:

| Separation | MySQL minus an exact geodesic, over random pairs |
| --- | --- |
| 10 km | -12 to +8 cm |
| 9,810 km | -66 to +6.5 m |
| near-antipodal | up to +5,973 m |

### Function set

v1 is the minimal set needed to store, read, inspect, measure and filter geometry. Apart
from the EWKB pair, all of it is present in MySQL 8.0.46, 8.4 and 9.7, whose spatial
function sets are identical in *membership*; signatures are not, so **9.7 is the
baseline** and version deltas are called out where they bite. `ST_GeomFromWKB` is the
known one: 8.0 accepted a geometry argument, where 8.4 and 9.7 reject it with
`ERROR 3037`. The list is an **allowlist**: only these are registered, and anything else
spatial is an unknown function until a later milestone adds it.

- **`ST_GeomFrom*`**, a geometry from an external format: `ST_GeomFromText`,
  `ST_GeomFromWKB`, `ST_GeomFromGeoJSON`, plus MySQL's plain synonyms
  `ST_GeometryFromText` and `ST_GeometryFromWKB`. Those two are v1: they are spellings of
  the same function, not the per-subtype aliases (`ST_PointFromText`, `ST_PolyFromWKB` and
  the rest) deferred below.
- **`ST_As*`**, an external format from a geometry: `ST_AsText` (`ST_AsWKT`),
  `ST_AsBinary` (`ST_AsWKB`), `ST_AsGeoJSON`.
- **`ST_AsEWKB` and `ST_GeomFromEWKB`**, the pair for the stored format, TiDB-specific
  because the format is. They are the only way to read and write what MySQL cannot express,
  and the name avoids shadowing MySQL's `ST_GeomFromWKB`, which takes bare WKB and a
  separate SRID. See *Binary in and out* in [Types and storage](#types-and-storage).
- **Option arguments.** `axis-order` on the WKT and WKB members of both groups, taking
  `lat-long`, `long-lat` or `srid-defined` (the default), rejecting anything else with
  `ERROR 3559` and having no effect at SRID 0. It is the explicit way to read or write
  longitude-first data against a latitude-first SRS, so a client need not pre-swap.
  `ST_GeomFromGeoJSON` takes its own `options` and `srid` arguments. MySQL defines
  `options` 1 to 4, where 1 rejects coordinate dimensions above 2 and is the default and
  2, 3 and 4 strip them. TiDB extends the range with two values MySQL rejects:

  | `options` | Third element | Fourth and later |
  | --- | --- | --- |
  | 5 | kept as Z | error |
  | 6 | kept as Z | dropped |

  Everything else follows MySQL.

  `ST_AsGeoJSON(g [, digits [, flags]])` takes MySQL's two: `digits` rounds the
  coordinates, defaulting to full precision and rejecting a negative value, and `flags` is
  a bitmask from 0 to 7 where bit 0 adds `bbox`, bit 1 a short CRS URN (`EPSG:4326`) and
  bit 2 a long one (`urn:ogc:def:crs:EPSG::4326`), long overriding short. Anything above 7
  is an error. The `bbox` is emitted in output axis order, so it is longitude-first on
  4326 like the coordinates beside it.
- **MySQL-specific constructors:** the full set of
  [functions that create geometry values](https://dev.mysql.com/doc/refman/8.4/en/gis-mysql-specific-functions.html):
  `Point`, `LineString`, `Polygon`, `MultiPoint`, `MultiLineString`, `MultiPolygon`,
  `GeometryCollection` and its synonym `GeomCollection`, which MySQL documents as the
  preferred spelling.
  `Point(x, y)` returns SRID 0; `ST_SRID(g, srid)` then stamps the SRS, validating the
  coordinates (`ERROR 3731`, `ERROR 3732`) without transforming them. For a geographic SRS
  that makes `Point` **(longitude, latitude)**, the opposite of WKT at 4326:
  `ST_SRID(Point(30, 50), 4326)` is `POINT(50 30)`, latitude 50.
- **Accessors:** `ST_X`, `ST_Y`, `ST_Latitude`, `ST_Longitude`, `ST_SRID` (getter and the
  `ST_SRID(g, srid)` setter), `ST_GeometryType`, `ST_Dimension`, `ST_Envelope`,
  `ST_IsEmpty`, `ST_IsValid`, `ST_StartPoint`, `ST_EndPoint`, `ST_PointN`, `ST_NumPoints`,
  `ST_ExteriorRing`, `ST_NumInteriorRings`, `ST_Centroid`. `ST_Centroid` and `ST_Envelope`
  are Cartesian-only, as in MySQL, which raises `ERROR 3618` for both on 4326. On 4326,
  `ST_IsValid` raises `ERROR 3618` for polygonal input, which needs the segment
  intersection v1 defers ([Future extensions](#future-extensions)).
- **Measurement:** `ST_Length(ls)`, `ST_Distance(g1, g2)`, which on 4326 takes only point
  operands and raises `ERROR 3618` otherwise, and
  `ST_Distance_Sphere(g1, g2 [, radius])`, whose `radius` must be positive and whose
  operands MySQL restricts to points and multipoints, raising `ERROR 3618` otherwise. The
  default radius is derived from the SRS, and is 6,370,986.0 m on SRID 0, which has none.
- **Predicates (DE-9IM):** `ST_Within`, `ST_Contains`, `ST_Intersects`, `ST_Equals`,
  `ST_Disjoint`, `ST_Touches`, `ST_Crosses`, `ST_Overlaps`. On SRID 0 they take any
  operand pair. On 4326 v1 takes only pairs with a point operand, and raises `ERROR 3618`
  naming the pair for the rest, as MySQL does for `ST_Distance_Sphere(POLYGON, POINT)`.
  The check is on each row's values, not the column type: on an unrestricted `GEOMETRY`
  column a statement fails at the first row it evaluates without a point operand, as it
  does in MySQL. See [Reference surface](#reference-surface) for why the operand type is
  the lever and [Future extensions](#future-extensions) for what that defers.

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

**GeoJSON.** Every RFC 7946 geometry is supported. The container and annotation members
follow MySQL, verified on 8.4.6 and 9.7.2:

| Input | Result |
| --- | --- |
| `Feature` | its bare geometry, so a Feature holding a point yields `POINT` |
| `FeatureCollection` | `GEOMETRYCOLLECTION` of the features' geometries, `GEOMETRYCOLLECTION EMPTY` if there are none |
| `"geometry": null` | SQL `NULL` |
| `properties`, `id`, `bbox`, foreign members | ignored, and not validated: MySQL accepts a `bbox` that is the wrong arity, contradicts the geometry, or is not even an array |
| named `crs` URN | sets the SRID (`urn:ogc:def:crs:OGC:1.3:CRS84` is 4326, link-object CRSs are not accepted, and a nested `crs` naming a different SRID errors); absent, the SRID is 4326 |
| position with a third or later coordinate | rejected under the default `options` 1 with `ERROR 3073`, stripped under 2, 3 and 4; TiDB's 5 and 6 are in the options table above |
| unknown `type`, or a required member missing | `ERROR 3072`, and `ERROR 3070` naming the member |

Round-trips are not idempotent: a FeatureCollection returns from `ST_AsGeoJSON` as a
GeometryCollection.

### Geometry engine

Results are bit-identical across builds, components and CPU architectures, regardless of
the `CGO_ENABLED` setting.

- `github.com/peterstace/simplefeatures`: OGC/DE-9IM model, WKT/WKB/GeoJSON I/O, and the
  planar predicates and measurement SRID 0 uses. Validated byte-identical to MySQL in the
  PoC.
- Andoyer for 4326: no Go library implements Andoyer or geodesic topology, and the
  libraries that do are C++ (Boost.Geometry, which MySQL runs, and GeographicLib's
  `Intersect`). Ellipsoidal distance and length, which the proof of concept already
  carries, plus the inverse problem with azimuths and a crossing test over it for the
  predicates.

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
meaningful. Since the bytes are little-endian, equality matches MySQL, and so does the
order within one SRID and subtype.

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

    -- ellipsoidal geodesic metres on 4326
    SELECT id, ST_Distance(loc, ST_GeomFromText('POINT(37.5 -122.2)', 4326)) AS m
    FROM stores;

    -- the geometry predicate is evaluated per row here; the index accelerates it later
    SELECT id FROM stores
    WHERE ST_Within(loc, ST_GeomFromText('POLYGON((...))', 4326));

### Phasing

The v1 surface lands in dependency order, each step reviewable on its own:

| Step | Contents | Done when |
| --- | --- | --- |
| 1. Type | Parser grammar, the field type, the `SRID` attribute, DDL validation, `SHOW CREATE TABLE`, the version byte and the EWKB codec, and the work in [Type plumbing](#type-plumbing) | a geometry column stores and returns its bytes across the audited operation surface |
| 2. I/O | `ST_GeomFrom*`, `ST_As*`, their option arguments, and the SRID validation on every ingest path | byte-identical to MySQL on the round-trip suite |
| 3. Catalog | `information_schema.st_spatial_reference_systems`, and DDL validating `SRID n` against it rather than against a hardcoded pair | the two rows match MySQL column for column |
| 4. Inspection | the constructors and the accessors | matches MySQL, including the constructor axis order |
| 5. Measurement | `ST_Length`, `ST_Distance` (point operands on 4326), `ST_Distance_Sphere`, and the Andoyer metric behind them | matches MySQL |
| 6. Predicates | the eight DE-9IM predicates, over Andoyer edges, with a point operand on 4326 | matches MySQL on the pairs it takes, and rejects the rest rather than approximating them |

Steps 1 to 3 are what the spatial index codes against, so they are the ones whose surface
is hard to change later. Steps 4 to 6 are independent of each other and of the index.

### Scope and deferrals

Out of scope here, each with a home:

- The **spatial index** and its pushdown:
  [`docs/design/2026-06-25-spatial-index.md`](2026-06-25-spatial-index.md)
  ([#69473](https://github.com/pingcap/tidb/pull/69473)), for which this layer is the prerequisite.
- The **geometry-processing function tail**, `ST_Area`, typed I/O aliases, spatial type
  casts (`CAST(g AS POINT)` and the other subtypes), `MBR*` family, geohash and niche
  accessors: a later, parallel expression-layer milestone.
- **SRIDs beyond 0 and 4326**, the full SRS catalog and `ST_Transform`:
  [Future extensions](#future-extensions).
- **Coprocessor pushdown.** The predicates and measurement functions are deterministic
  scalars and pushdown-eligible; pushing them filters at the storage node instead of
  shipping every candidate geometry to TiDB, with or without an index. It is cross-repo work
  (tipb signatures plus a TiKV-side evaluator), specified with the index design, which owns
  the pushdown contract.
- **The other two spatial `information_schema` tables**: `st_geometry_columns`, one row per
  geometry column and derivable from the schema, and `st_units_of_measure`, 47 static rows
  in MySQL that matter once projected SRSs with varied units exist. The `unit` argument on
  `ST_Distance` and `ST_Length` is deferred with it, since that catalog defines the names
  it accepts; v1 has the two-argument forms and returns metres on 4326.
- **`ST_SwapXY`**, which swaps a geometry's coordinates in place. The `axis-order` option
  covers the read and write direction in v1; this is the geometry-mutating variant.
- **3D / measured (Z/M) geometry**: stored and returned unchanged through the EWKB pair;
  functions that interpret coordinates reject it in v1.

### Compatibility

| Area | Effect |
| --- | --- |
| Partition table, clustered index | None. Geometry cannot be a primary or clustering key, having no meaningful ordering. |
| Charset and collation | Not applicable; the value is binary. |
| Parser | Updated in this design. |
| DDL | New column types and the `SRID` attribute, restricted to 0/4326, plus subtype constraints, at `CREATE TABLE` and `ADD COLUMN`. `MODIFY`/`CHANGE COLUMN` on a geometry column is limited in v1. `DROP COLUMN` is ordinary. |
| `information_schema` | One new table, `st_spatial_reference_systems`, read-only with two static rows. Its two siblings in MySQL are deferred. |
| Planner, statistics, executor | `ST_*` evaluate on the normal expression path; geometry predicates are ordinary `Selection`s with no access path of their own. No new operator, access path or statistics. `ANALYZE` skips geometry columns. |
| TiKV | None. Values are ordinary binary strings; pushdown is deferred. |
| BR | None. Backs up and restores bytes and metadata without interpreting column values. |
| Dumpling, Lightning | Geometry dumps as MySQL's binary format, which reloads as a bare literal (see [Types and storage](#types-and-storage)), so the round-trip needs no function call and a `mysqldump` loads unchanged, unless the table has a spatial index, which v1 rejects. A column holding Z/M values cannot be dumped that way, since MySQL has no form for them and a bare `SELECT` errors; those need `ST_AsEWKB` and an `ST_GeomFromEWKB(0x...)` literal, which is Dumpling work and TiDB-only output. An SRID outside 0 and 4326 needs none of that, since it round-trips on the bare path unchanged. |
| DM | Replicating MySQL into TiDB carries geometry in MySQL's binary format, since the binlog row image is the same bytes MySQL stores and returns, and that is exactly what the bare ingest path takes. DM itself needs no change; the conversion is TiDB-side and rewrites only the header. This is the migration case the bare path is chosen for. A table with a spatial index fails at its DDL. |
| TiCDC | Not pass-through: it reads the stored value from the KV layer, so it sees the format-version byte and EWKB. It converts to MySQL's binary format before it emits, as the bare path does; a Z/M value has no MySQL form and fails the changefeed. |
| TiFlash | Not supported in v1. Setting a TiFlash replica on a table with a geometry column is rejected, and so is adding a geometry column to a table that has one, as TiDB already does for a `gbk` column. |
| Upgrade | Additive: the type does not exist in earlier releases, so no existing schema or query changes behavior. |
| Downgrade | A release without the type cannot read a table that has a geometry column, so those columns must be dropped first, an ordinary `DROP COLUMN`. |

### Differences from MySQL

Every deliberate difference from MySQL 9.7 in v1:

| Area | v1 | MySQL |
| --- | --- | --- |
| 4326 DE-9IM predicates | need a point operand (`POINT` or `MULTIPOINT`); other pairs raise `ERROR 3618`, checked per row | any pair |
| 4326 `ST_Distance` | point operands only, else `ERROR 3618` | any pair |
| 4326 `ST_IsValid` | polygonal input raises `ERROR 3618` | any input |
| SRIDs | the catalog and `SRID n` hold 0 and 4326; WKT and WKB, in and out, reject other SRIDs unless given `axis-order=long-lat` | its full catalog; an SRID outside it is output in stored order, with warning 3565 |
| NaN and Inf at SRID 0 | rejected | stored through WKB and a bare literal |
| Indexes on a geometry column | none: a spatial index, and any other index with a geometry member, is rejected | a plain `KEY` on a geometry column becomes a spatial index |
| Generated columns | no `ST_*` function allowed | allowed |
| `MODIFY`/`CHANGE COLUMN` on geometry | limited | any change |
| Functions and spatial catalog tables | the v1 allowlist and `st_spatial_reference_systems`; the rest is in [Scope and deferrals](#scope-and-deferrals) | the full set |
| Z/M coordinates | stored, through `ST_GeomFromEWKB` and GeoJSON `options` 5 and 6 | rejected |
| `ORDER BY` on a column mixing SRIDs or subtypes | subtype first | SRID first |
| Nesting depth | one bound for ingest and read | stores values its functions cannot read |

## Test Design

### Functional Tests

What follows is not exhaustive coverage, which the implementation owns, but the behaviours
that must not drift: each one pins a decision this document makes, and would fail if that
decision were quietly undone.

- **Where the 4326 edge sits.** `POLYGON((0 0, 80 0, 0 80, 0 0))`, probed on longitude 70
  at latitude 45.035 and 45.5, the first inside and the second outside. That is MySQL's
  boundary at 45.070159 and not a sphere's at 45.000000, and 45.035 is the probe a
  spherical engine answers the other way, so the test fails if the edge model ever
  reverts.
- **The operand restriction holds.** The eight DE-9IM predicates on curated pairs, matched
  to MySQL on 4326 since the edge model is MySQL's, boundary cases explicit, and pairs
  without a point operand asserted to raise `ERROR 3618` rather than answer, as
  `ST_Distance` must for any non-point operand. A mixed `GEOMETRY` column fails at its
  first polygon row and succeeds once that row is filtered out.
- **The bare binary boundary is symmetric.** Bytes from a bare `SELECT` of a
  MySQL-expressible value insert back unchanged as a bare literal, over the text and
  binary protocols and as a prepared-statement parameter, and a `mysqldump` literal loads
  as the same geometry. Byte-compared against MySQL for both SRIDs, which is what catches
  the geographic axis swap: at 4326 the bare bytes must be longitude-first while
  `ST_AsBinary` of the same value is latitude-first, and at SRID 0 the two agree.
- **Extended data splits two ways.** A Z/M value goes in through `ST_GeomFromEWKB` or
  GeoJSON `options` 5 and 6, is rejected by WKT and WKB, comes back byte-identical through
  `ST_AsEWKB`, and errors on a bare `SELECT`, since MySQL has no form for it. A value
  whose only extension is its SRID round-trips on the bare path byte-for-byte instead,
  which is the case replication and dump depend on, and through WKT and WKB only with
  `axis-order=long-lat`. Functions that interpret coordinates error on both.
- **Parsing is bounded, and bounded equally.** WKB nested past the depth the parser
  bounds, truncated and over-long inputs in each format, and a fuzz target over the WKT,
  WKB and GeoJSON parsers. The bar is a clean error, never a panic. One case pins the read
  and ingest bounds as equal, a value that ingests must read back, so TiDB does not repeat
  MySQL's store-but-unreadable gap.
- **The format version is checked.** Version 1 decodes; an unknown or zero version byte is
  rejected with a clear error rather than misparsed. Big-endian input is stored
  little-endian, equal to the same geometry given little-endian.
- **The counterintuitive surfaces.** `ST_Distance_Sphere`'s default radius matches MySQL on
  both SRIDs, an explicit radius scales the result and a zero or negative one errors; the
  constructors are longitude-first where WKT at 4326 is latitude-first; `axis-order` swaps
  at 4326, is inert at SRID 0, and rejects a bad value with `ERROR 3559`.
- **GeoJSON.** The GeoJSON input table above, each row matched against MySQL 8.4 and 9.7,
  `options` 5 and 6 as specified, and `ST_AsGeoJSON`'s `digits` rounding and each `flags`
  bit, byte-compared to MySQL.
- Routine coverage beyond these: I/O round-trips per subtype and format, accessor and
  measurement values against MySQL, the function aliases, SRID validation on every ingest
  path, the catalog rows, the DDL matrix in [Compatibility](#compatibility), and geometry
  through the audited operation surface.

### Scenario Tests

- A points table answering proximity (`ST_Distance_Sphere ≤ r`) and geofence
  (`ST_Within(point, polygon)`), matching MySQL.
- 4326 edge cases: a query near a pole and one across the antimeridian.
- Application shape: lat/long ingest via WKT/GeoJSON, read back via `ST_AsGeoJSON`.

### Compatibility Tests

- MySQL byte-identical suite for the v1 function surface (the PoC's `spatial_compat`
  integration test is the basis).
- Dumpling/Lightning round-trip of a table with geometry columns, and BR pass-through, both
  over the bare path. TiCDC is not pass-through and gets its own test: a changefeed into a
  MySQL sink must convert the stored value to MySQL's binary format, so the sink is
  byte-compared against the source. TiFlash is a separate gating test: a replica on a table
  with a geometry column and a geometry column added to a table with a replica are both
  rejected, and behavior is unaffected when TiFlash is absent.
- Parser, DDL, planner and executor as listed in Compatibility.
- Upgrade and downgrade paths.

### Benchmark Tests

- Geometry ingest and read throughput, and predicate latency across selectivities with no
  other predicate to narrow the scan, which is the pre-index baseline the index layer will
  be measured against.
- Influence on the online workload: a non-geospatial workload measured with and without
  the feature present, expected to be unchanged, since nothing on a table without a
  geometry column takes a new code path. The one shared cost to watch is the larger
  builtin-function registry.

## Impacts & Risks

Intended impact: geometry becomes a first-class, MySQL-compatible value and query surface,
so applications can store locations and run proximity and geofence queries in SQL without
application-side geometry code.

Risks:

- **Prerequisite coupling:** the index and pushdown layers code against this type, so the
  value-format and axis-order decisions here are lock-ins for them.
- **Value-format lock-in:** the on-disk format is hard to change post-GA; mitigated by the
  version byte and by storing Z/M and unsupported SRIDs losslessly from the start.
- **Andoyer has to be written:** no Go library implements MySQL's edge formula, so matching
  its predicate results means building the inverse problem with azimuths and a crossing test
  over it. Mitigated by the operand restriction, which keeps v1 to the closed-form half, and
  by the regression test that pins where the 4326 edge sits.
- **Same answer on every node:** arm64 fuses multiply-adds where amd64 does not, including
  inside Go's `math.Sin` and `math.Atan`, so the same Andoyer code gives different last bits
  (measured over 100,000 distances). A 4326 predicate near an edge can flip on that bit, so
  a mixed-architecture cluster could answer one query two ways. `simplefeatures` fuses too:
  on arm64, 6% of 100,000 planar point distances differ from amd64 by one ULP. Mitigated by
  three rules, `simplefeatures` included: only the IEEE 754 basic operations (`+ - * /` and
  square root), which give the same bits on every CPU, so the trigonometry is our own; no
  fused products; and the same operations in the same order in every component. A check
  that rejects fused instructions in an arm64 build of the geometry code enforces the
  second.
- **MySQL spatial schemas may not migrate:** a MySQL spatial table usually has a spatial
  index, which MySQL creates even for a plain `KEY` on a geometry column, and
  `pt POINT AS (ST_SRID(Point(lng, lat), 4326)) STORED` is the usual way to index
  latitude/longitude columns. v1 rejects both at DDL, so dump, Lightning and DM fail on
  such a table until the index lands and those functions are allowed in generated columns
  ([Differences from MySQL](#differences-from-mysql)).
- **MySQL error parity:** exact codes and messages may not match initially (the PoC used
  placeholder wording); a compatibility risk, not a correctness one.
- **Parsers take untrusted bytes:** geometry parsing is the one new path a client drives
  with arbitrary input, so a parser bug is an availability risk for the server, not just
  the session. Go makes this sharper than MySQL: a stack overflow is a fatal crash with no
  `recover`, where MySQL degrades to `ERROR 1436`. Mitigated by bounding depth explicitly
  and by fuzzing the parsers.

## Investigation & Alternatives

- **Which WKB dialect for version 1.** Three candidates, all published:

  | Candidate | SRID | Z/M | Notes |
  | --- | --- | --- | --- |
  | MySQL internal, `<srid u32 LE><WKB>` | prefix | **no** | 2D only, so it cannot hold what this design stores |
  | ISO WKB (SFA 1.2.1 / SQL-MM) | no | type code +1000/2000/3000 | what `simplefeatures` already reads and writes |
  | **EWKB (PostGIS/GEOS)** | type-word flag | type-word flags | chosen |

  EWKB carries SRID, Z, M and XYZM in one defined format, with plain 2D WKB as its
  degenerate case. It also permits omitting the SRID where a column fixes it, which this
  design declines so stored values stay self-describing. The cost is a codec.
  `simplefeatures` implements the ISO type-code convention (`geomCode % 1000` for the
  type, `/ 1000` for the dimension), not EWKB's flags, so TiDB owns the EWKB header
  encode/decode and hands the body to the library. The PoC's TiKV evaluator uses the
  2D-only `geo` crate and already hand-rolls its decoder, so it needs a header change and
  nothing more; Z/M values are not pushable regardless.
- **A leaner layout as version 1.** Rejected for v1. EWKB carries redundancy (a per-row
  SRID the column usually fixes, a byte-order flag per (sub)geometry, WKB framing), but
  profiling the PoC bounded the win: geometry decode is ~2% of insert CPU, and the ~27% of
  query CPU spent parsing WKB is only about half attributable to the stored value, the rest
  being a re-parse inside the predicate library that no format can remove. The version byte
  defers the choice without a migration.
- **Matching MySQL's stored bytes.** Rejected as a non-goal: I/O compatibility is a boundary
  conversion, and MySQL does the same internally.
- **GEOS (go-geos).** Rejected for v1: TiDB would not build with `CGO_ENABLED=0`, and
  `simplefeatures` is MySQL byte-identical in the PoC. Revisit for the processing tail.
- **The full [#38916](https://github.com/pingcap/tidb/pull/38916) surface at once.** Rejected as too large to review and
  land.
- **Geometry as a generic BLOB with application-side functions.** The status quo; loses
  MySQL compatibility, type safety, and any path to a spatial index.
- **PostGIS's longitude-first WKT and `ST_X`, and always-planar `geometry` semantics.**
  Rejected in favor of MySQL parity; always-planar 4326 would also contradict the SRS-class
  dispatch that keeps adding SRIDs a catalog change rather than a code change. Full delta in
  [the appendix](#appendix-postgis-delta-for-the-type-layer).
- **The 4326 edge model.** Where each engine draws the hypotenuse of
  `POLYGON((0 0, 80 0, 0 80, 0 0))` at longitude 70:

  | Evaluated as | Edge crosses longitude 70 at | Off MySQL by |
  | --- | --- | --- |
  | planar, straight lat/long edges, as PostGIS `geometry` at 4326 | latitude 10.000000 | ~3,900 km |
  | sphere, great-circle edges, as PostGIS `geography` | latitude 45.000000 | 7,797 m |
  | an exact geodesic | latitude 45.070235 | 8.5 m |
  | **MySQL 9.7.2: ellipsoid, Andoyer edges** | **latitude 45.070159** | 0 |

  The sphere and MySQL rows are measured where each engine's predicate flips: PostGIS
  `geography` between 45.0000 and 45.0001, and MySQL at 45.070159, bisected to 1e-9. So
  `ST_Intersects` on `POINT(45.035 70)` is true in MySQL and false in PostGIS. Against
  Andoyer edges:

  - **Exact geodesic edges (Karney)** are 8.5 m from MySQL here, so being more exact costs
    compatibility, and Karney's inverse iterates where Andoyer's is closed form.
  - **Great-circle edges on a sphere**, what PostGIS `geography` ships, would buy a wider
    operand set for a boundary 7,797 m away. The gap grows with edge length: 4 mm over an
    8 km city edge, 10 m over 394 km, and 3.1 km over 6,690 km.
  - **The plane** is wrong at any size: for `POLYGON((0 0, 0 80, 60 0, 0 0))`, MySQL 8.4.6
    answers `ST_Within` true for `(30 40)`, `(33 40)`, `(36 40)` and `(40 40)`, and planar
    false for all four. Whole regions flip, not boundary cases.
- **Erroring on large geometries instead of answering.** Rejected: it needs an arbitrary
  size limit, and the error is itself a difference from MySQL, which answers. The operand
  restriction has no threshold and never returns a boolean MySQL would not.

## Unresolved Questions

None.

## Future extensions

Documented, not built here. Each is additive over the v1 surface: no v1 behavior changes
and no stored value has to be rewritten.

| Step | Cost |
| --- | --- |
| Fill the catalog from the full EPSG dataset, taken from EPSG or PROJ's `proj.db`, with the dataset version pinned in the docs and IOGP attribution carried ([terms](#epsg-terms-of-use)). The set drifts: MySQL shipped 5,152 rows in 8.0.46 and 5,238 in 8.4 and 9.7 | moderate, and a prerequisite for the rest |
| All projected SRSs (e.g. 3857 Web Mercator) | low: planar X/Y, so the same Cartesian functions apply and only the bounds are per-SRS |
| Geographic SRSs beyond 4326 | moderate: the same Andoyer code, parameterised by the SRS's ellipsoid |
| `ST_Transform`, which MySQL has had since 8.0.13 and which reprojects between two SRSs | moderate, and pointless before the catalog: with only 0 and 4326 there is nothing to transform to, since 4326 to 4326 is a no-op and MySQL itself rejects a transform to 0 with `ERROR 3742` |
| User-defined SRSs through `CREATE [OR REPLACE] SPATIAL REFERENCE SYSTEM` and `DROP SPATIAL REFERENCE SYSTEM`, which MySQL also has (there is no `ALTER`; modification is `CREATE OR REPLACE`) | bigger: a writable catalog, a WKT SRS parser, and catalog changes that have to replicate |

**Predicates between two extended geometries.** v1 answers the eight DE-9IM predicates on
4326 only for pairs with a point operand ([Reference surface](#reference-surface)).
Widening that to line and polygon pairs needs geodesic segment intersection over Andoyer
edges, assembled into a 9-intersection matrix. `ST_IsValid` on 4326 polygons waits on it
too, and `ST_Distance` to a line or polygon lands in the same step, since it needs the
nearest point on an Andoyer edge. All are additive for users, since they only make queries
that were rejected start answering, and none needs a format change. Karney's intersection
and point-to-line work supplies the algorithms.

`ST_Covers` and `ST_CoveredBy` are PostGIS spellings with no MySQL equivalent, worth
adding once the spatial index lands. Other functions outside MySQL's set follow the same
rule, added only if index-supported or by demand.

A `GEOGRAPHY` type is a further extension, covered in
[the appendix](#appendix-postgis-delta-for-the-type-layer). The function tail, the `MBR*`
family and the rest of the deferred surface are in
[Scope and deferrals](#scope-and-deferrals).

## Appendix: SRS catalog

### Catalog row contents

The two rows carry MySQL's columns (`SRS_NAME`, `SRS_ID`, `ORGANIZATION`,
`ORGANIZATION_COORDSYS_ID`, `DEFINITION`, `DESCRIPTION`): SRID 0 with an empty name and
definition and no organization, and 4326 as `WGS 84` / `EPSG` / 4326 with the
`GEOGCS["WGS 84",DATUM[...]]` definition string. Both are what MySQL returns for those two
SRIDs, column for column. Whether the table is a view over an internal one or synthesized
is an implementation choice; MySQL uses a view over a data dictionary table that is itself
unreadable, even by `root` (`ERROR 3554`).

### EPSG terms of use

Filling the catalog from the [EPSG dataset](https://epsg.org/) later brings its terms with
it: IOGP's ownership has to be acknowledged wherever the data is published, and anyone
given the data has to be told those terms. That work therefore carries a
`LICENSES/EPSG-TERMS-OF-USE` entry beside the existing ones.

## Appendix: PostGIS delta for the type layer

The compatibility target is **MySQL**; full PostGIS compatibility is a non-goal, not an
oversight. MySQL's spatial surface is largely a subset of PostGIS's, and the syntax, the
`SHOW CREATE TABLE` form, the single-`TypeGeometry`-plus-subtype model and the predicate
semantics all anchor on MySQL. This appendix records the delta a PostGIS user meets at the
**type and function layer**, so a deliberate MySQL-alignment choice is not later mistaken
for a gap. The index-layer delta, and the path to parity across the whole stack, are the
appendix of [`docs/design/2026-06-25-spatial-index.md`](2026-06-25-spatial-index.md).

### Type and surface mapping

PostGIS picks the reference surface by *type*, TiDB and MySQL pick it by *SRS class*, so
the mapping is not type name to type name:

- `SRID 0` is PostGIS `geometry`: planar, unitless coordinates, no globe. All three engines
  agree here.
- `SRID 4326` is PostGIS **`geography`**, not PostGIS `geometry`. PostGIS `geometry` on 4326
  does planar arithmetic on degrees, and no TiDB spelling reproduces that, so on migration
  `ST_Distance` changes from degrees to metres and predicates flip where curvature matters.

Per operation on 4326, extending the v1 and MySQL columns in
[Reference surface](#reference-surface):

| 4326 operation | PostGIS `geometry` | PostGIS `geography` |
| --- | --- | --- |
| `ST_Distance`, `ST_Length` | planar, in degrees | ellipsoid (default) |
| `ST_Distance_Sphere` | `ST_DistanceSphere` | `use_spheroid => false` |
| point-in-polygon predicates | planar, in degrees | sphere, great-circle edges |
| polygon/polygon predicates | planar, in degrees | sphere, great-circle edges |

`SRID 4326` and `geography` agree on the metric, both ellipsoidal, and differ on edges:
great-circle arcs in PostGIS, Andoyer edges here, as in MySQL.

### A `GEOGRAPHY` type

How PostGIS spells geodetic work, and nothing here forecloses it. The stored format
already holds a `geography` value, so it needs no new bytes, codec or migration. It would
reproduce PostGIS's split of great-circle edges with a Karney metric, which keeps the
invariant in [Reference surface](#reference-surface), since one edge model still decides
all topology:

| Spelling | Edge model | Metric | Matches |
| --- | --- | --- | --- |
| `GEOMETRY` `SRID 0` | plane | plane | PostGIS `geometry` and MySQL, all three agree |
| `GEOMETRY` `SRID 4326`, and further SRIDs later | Andoyer | Andoyer | MySQL |
| `GEOGRAPHY`, if added | great circle | Karney | PostGIS `geography` |

Whether it is a field type of its own or a flag over `mysql.TypeGeometry` touches the
parser, DDL, `SHOW CREATE TABLE`, `information_schema.columns` and the tool metadata path;
no constant is reserved for it here.

### Remaining delta

| Area | PostGIS | This design |
| --- | --- | --- |
| Metric accuracy | Karney via PROJ's `geodesic.c`, exact to round-off and convergent near antipodes | Andoyer, because MySQL is Andoyer (see [Reference surface](#reference-surface)). Both are "ellipsoidal", so a PostGIS user should still expect differences: centimetres at 10 km, kilometres near antipodes |
| Edge model | Great circle on a sphere for all `geography` topology, so both the predicates and the edges `ST_Distance` measures to are spherical | Andoyer, matching MySQL, which puts TiDB further from PostGIS: the boundaries are 7,797 m apart along the meridian on a continental polygon, enough to flip `ST_Intersects` on the same point. This is the larger of the two deltas, against centimetres for the metric |
| Predicate operands | Any pair | On 4326, v1 needs a point operand, a `POINT` or `MULTIPOINT`; other pairs raise `ERROR 3618` rather than being answered on a cheaper surface ([Reference surface](#reference-surface)). `ST_Distance` takes only point operands. SRID 0 is unrestricted |
| Axis order | Longitude first everywhere | Stored longitude first, as PostGIS; WKT, WKB and `ST_X`/`ST_Y` latitude first on a geographic SRS, as MySQL |
| SRID / CRS | Full EPSG catalog in `spatial_ref_sys`, on-the-fly `ST_Transform` | SRID 0 and 4326 only; other codes rejected by DDL but storable in an unrestricted column; no `ST_Transform`. Both are in [Future extensions](#future-extensions) |
| Function breadth | 300+ `ST_*` | The v1 allowlist, then MySQL's ~75. Absent even from MySQL's set: spatial clustering, `ST_MakeValid`, and the `ST_AsMVT`/KML/GML/SVG output formats |
| Function spelling | `ST_DistanceSphere`, `ST_DistanceSpheroid` | MySQL's `ST_Distance_Sphere`; the underscore differs, and no spheroid variant exists separately because `ST_Distance` on 4326 is already ellipsoidal |
| Geometry types | Adds CIRCULARSTRING, COMPOUNDCURVE, CURVEPOLYGON, POLYHEDRALSURFACE, TIN, TRIANGLE | OGC Simple Features only; no curved or TIN geometries |
| Dimensionality | XYZ / XYM / XYZM, with ND indexing | Stored losslessly; no v1 function computes on it, and MySQL has no Z/M at all |
| Subsystems | Raster, topology, SFCGAL solids, pgRouting, Tiger geocoder | None planned |
