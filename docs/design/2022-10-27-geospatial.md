# TiDB Design Documents

- Author(s): [Daniël van Eeden](http://github.com/dveeden)
- Discussion PR: https://github.com/pingcap/tidb/pull/38611 (Phase 1 only)
- Tracking Issue: https://github.com/pingcap/tidb/issues/6347

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

Add support for storing and working with Geospatial data.

## Motivation or Background

Geospatial support is a highly requested feature. When sorting the open issues on `pingcap/tidb` by the number of 👍-reactions it is currently number 5 on [the list](https://github.com/pingcap/tidb/issues?q=is%3Aissue+is%3Aopen+sort%3Areactions-%2B1-desc).

Many users of TiDB are storing geospatial data as part of their business. This could be the location of a parcel for parcel delivery companies or the location of a bike for a bike sharing company. The [most recent files](https://s3.amazonaws.com/capitalbikeshare-data/index.html) from the Capital Bikeshare data that are used in the [Import Example Database](https://docs.pingcap.com/tidb/dev/import-example-data) guide also have coordinates for all records.

This would improve the [compatibility with MySQL](https://docs.pingcap.com/tidb/stable/mysql-compatibility) and make it easier to migrate from MySQL to TiDB.

A [proof of concept](https://github.com/Hackathon-2022-GIS) was done as part of the Hackathon in 2022. This modified TiDB to have a few geospatial functions and one geospatial datatype. This together with a frontend to demonstrate the functionality showed that this is feasable.

## Detailed Design

### Data types

Data types are defined in [Simple Feature Access - Part 2: SQL Option](https://www.ogc.org/standards/sfs) from the OGC. 

Data types that should be implemented are:

| Data type | Description |
|-----------|-------------|
| GEOMETRY  | Any type of Geometry |
| POINT     | A geometry that is a POINT |
| LINESTRING | A geometry that is a LINESTRING |
| POLYGON | A geometry that is a POLYGON |
| MULTIPOINT | A collection of points |
| MULTILINESTRING | A collection of linestrings |
| GEOMETRYCOLLECTION | A collection of geometries |

These data types are bascially binary data types holding a binary representation.

Example:

```
0xE61000000101000000000000000000F03F000000000000F03F
  E6100000                                             = SRID         (4326)
          0101000000000000000000F03F000000000000F03F   = WKB value
          01                                           = Byteorder    (NDR/Little Endian)
            01000000                                   = WKB type     (POINT)
                    000000000000F03F                   = X coordinate (1)
                                    000000000000F03F   = Y coordinate (1)
```

The WKB format is defined in "OpenGIS Implementation Specification for Geographic information - Simple feature access - Part 1: Common architecture" as part of the [Simple Feature Access](https://www.ogc.org/standards/sfa) from OGC.

This format (`<srid><wkb>`) is often referred to EWKB, but there is no official standard for this. This is chosen as this is what MySQL uses.

Besides these data types [Simple Feature Access - Part 2: SQL Option](https://www.ogc.org/standards/sfs) lists more types like, `POINTZ` and other types with a Z-suffix, these are 3D points with three coordinates instead of two. There are a few 2D types that are also not on the list with types we should implement like `CURVE` and `SURFACE`. MySQL doesn't include these types and they are not needed for compatibility and can be implemented later if needed.

Both TiDB and TiKV already have some notion of a `GEOMETRY` type, but this is incomplete. There is a implementation of the `GEOMETRY` type in  [pingcap/tidb#38611](https://github.com/pingcap/tidb/pull/38611) and [tikv/tikv#13652](https://github.com/tikv/tikv/pull/13652).

Besides the data type there is also a column attribute that is used in MySQL to restrict the SRID of the features in that column.

Example:

```sql
CREATE TABLE t1 (
    id BIGINT UNSIGNED AUTO_RANDOM PRIMARY KEY,
    g GEOMETRY NOT NULL SRID 4326
);
```

All geometry data types are `mysql.TypeGeometry` / `0xFF`, but have a separate geometric subtype, which basically acts as a constraint. A `GEOMETRY` column can store a `POINT` or `LINESTRING`. But a column with a `POINT` type only store a `POINT` value. 

This is identical to how this is done in MySQL, where this is defined in `Field::geometry_type`.

#### Phase 1

Support for the `GEOMETRY` data type

#### Phase 2

Support for SRID attribute

#### Phase 3

Support for `POINT` and other data types

### Functions

The list of functions that MySQL supports are [here](https://dev.mysql.com/doc/refman/8.0/en/spatial-function-reference.html).

The focus is to implement the most used functions that are part of the OpenGIS standards and MySQL first and later on the MySQL specific functions where needed for compatibility.

#### Phase 1

| Function | Description |
|----------|-------------|
| `ST_GeomFromText()` | WKT to Geometry, including the SRID argument |
| `ST_AsText()` | Geometry to WKT |
| `ST_Distance()` | Distance, only for SRID 0 as a start |

This together with the `GEOMETRY` data type would allow migration of data from MySQL and allow one to store and retrieve features. Lookup is expected to be done by other columns (e.g. `SELECT bike_id, bike_location FROM bikes WHERE bike_id=1234`)

#### Phase 2

| Function | Description |
|----------|-------------|
| `ST_AsWKT()` | Geometry to WKT (alias for `ST_AsText()`?)|
| `ST_AsWKB()` | Geometry to WKB |
| `ST_AsGeoJSON()` | Geometry to GeoJSON |
| `ST_GeomFromGeoJSON()` | GeoJSON to Geometry |

This adds more format convertion options.

#### Phase 3

| Function | Description |
|----------|-------------|
| `Point()`, `LineString()`, etc | MySQL Specific, Create a geometry based on arguments | |

This is to add compatibility features that are MySQL Specific and not in the offical standards, but are often used.

#### Phase 4

| Function | Description |
|----------|-------------|
| ST_SRID() | Get SRID from feature |

Add functions that get properties from features
#### Phase 5

| Function | Description |
|----------|-------------|
| ST_Distance() | Distance, for SRID 0 *and* SRID 4326 (WGS 84)|

Add more functions that test relations between features

### Indexes

#### Geohash

Support for GeoHash functions makes it possible to use regular indexes with generated columns.

Example of `ST_Geohash()` with MySQL 8.0:

```
mysql> SELECT ST_Geohash(ST_GeomFromText('POINT(1 0)'),15);
+----------------------------------------------+
| ST_Geohash(ST_GeomFromText('POINT(1 0)'),15) |
+----------------------------------------------+
| s008nb00j8n012j                              |
+----------------------------------------------+
1 row in set (0.00 sec)

mysql> SELECT ST_Geohash(ST_GeomFromText('POINT(1 0)'),5);
+---------------------------------------------+
| ST_Geohash(ST_GeomFromText('POINT(1 0)'),5) |
+---------------------------------------------+
| s008n                                       |
+---------------------------------------------+
1 row in set (0.00 sec)

mysql> SELECT ST_Geohash(ST_GeomFromText('POINT(1.01 0)'),15);
+-------------------------------------------------+
| ST_Geohash(ST_GeomFromText('POINT(1.01 0)'),15) |
+-------------------------------------------------+
| s008nbp2n8n848n                                 |
+-------------------------------------------------+
1 row in set (0.00 sec)

mysql> SELECT ST_Geohash(ST_GeomFromText('POINT(1.01 0)'),5);
+------------------------------------------------+
| ST_Geohash(ST_GeomFromText('POINT(1.01 0)'),5) |
+------------------------------------------------+
| s008n                                          |
+------------------------------------------------+
1 row in set (0.00 sec)
```

As you can see in the example `POINT(1 0)` and `POINT(1.01 0)` have the same prefix. More information about what prefix length gives what precision can be found [on wikipedia](https://en.wikipedia.org/wiki/Geohash#Digits_and_precision_in_km).

Geohash support is useful both for indexing and compatibility. Implementing this should be relatively easy. However this doesn't fully replace geospatial indexes.

#### Geospatial indexes

For MySQL this is based on [R-tree](https://en.wikipedia.org/wiki/R-tree). There has been some geospatial support in RocksDB in the past but that has been removed again.

There was a paper published in the beginning of 2022 that is titled [An LSM-Tree Index for Spatial Data](https://www.mdpi.com/1999-4893/15/4/113).

What's needed to fully support geospatial indexes in TiDB/TiKV needs more research.

### Information Schema Tables

MySQL 8.0 has 3 information_schema tables that are related to geospatial support.

```
mysql-8.0.31> SHOW TABLES FROM information_schema LIKE 'ST\_%';
+--------------------------------------+
| Tables_in_information_schema (ST\_%) |
+--------------------------------------+
| ST_GEOMETRY_COLUMNS                  |
| ST_SPATIAL_REFERENCE_SYSTEMS         |
| ST_UNITS_OF_MEASURE                  |
+--------------------------------------+
3 rows in set (0.00 sec)
```

[Simple Feature Access - Part 2: SQL Option](https://www.ogc.org/standards/sfs) names the first two tables `GEOMETRY_COLUMNS` and `SPATIAL_REF_SYS` and doesn't mention the last table.

These tables can help with SRID validation:

```
mysql-8.0.31> SELECT SRS_ID FROM information_schema.ST_SPATIAL_REFERENCE_SYSTEMS LIMIT 5;
+--------+
| SRS_ID |
+--------+
|      0 |
|   4143 |
|   2165 |
|   2043 |
|   2041 |
+--------+
5 rows in set (0.00 sec)

mysql-8.0.31> CREATE TABLE tbl1(id int primary key, g geometry srid 0);
Query OK, 0 rows affected (0.05 sec)

mysql-8.0.31> CREATE TABLE tbl2(id int primary key, g geometry srid 1);
ERROR 3548 (SR001): There's no spatial reference system with SRID 1.
mysql-8.0.31> CREATE TABLE tbl3(id int primary key, g geometry srid 4143);
Query OK, 0 rows affected (0.06 sec)
```

#### Phase 1

Implement `ST_SPATIAL_REFERENCE_SYSTEMS`, keeping this compatible with MySQL 8.0.

#### Phase 2

Implement `ST_GEOMETRY_COLUMNS`, keeping this compatible with MySQL 8.0.

#### Phase 3

Implement `ST_UNITS_OF_MEASURE`, keeping this compatible with MySQL 8.0.

### Compatibility

As the parser is updated external tools like Dumpling need to be built with the new parser to be able to validate the schema.

The `sync_diff_inspector` tool might need to be updated to compare and/or ignore geospatial columns.

Downgrading to a release without geospatial datatypes will require dropping all geospatial columns from all tables.

Support for TiFlash is out of scope for now and needs more research.

Geospatial support is not expected to impact security. The `ST_GEOMETRY_COLUMNS` table, when implemented, should limit the visible tables based on the privileges of the user, just like `information_schema.TABLES`.

## Test Design

### Functional Tests

MySQL has a [GIS testsuite](https://github.com/mysql/mysql-server/tree/8.0/mysql-test/suite/gis). This can be used with [mysql-tester](https://github.com/pingcap/mysql-tester) to test functionality and compatibility with MySQL.

When testing the functions special care should be taken to correctly test if it is correctly using the bounding box or the actual shape.

### Compatibility Tests

Compatibility with Dumpling, Lightning, TiCDC and sync-diff-inspector has to be explicitly tested.

Downgrading without geospatial columns should work and should be tested.

## Impacts & Risks

This adds a new dependency: [github.com/twpayne/go-geom](https://pkg.go.dev/github.com/twpayne/go-geom). The BSD-2-Clause license should be fine. The basic geometric types in this package are ok, but the features that do things like comparing features, calculating distances etc. are not (yet) on the same level as [Boost.Geometry](https://github.com/boostorg/geometry). This means that we may have to add more dependencies and/or implement some missing features ourselves.

## Client support

Working with geospatial data is possible with MySQL Client or mycli. However a GUI tool makes many things much easier. MySQL Workbench has [a spatial viewer](https://dev.mysql.com/blog-archive/mysql-workbench-6-2-spatial-data/) since version 6.2. This works with minimal geospatial support in TiDB. However [DBeaver](https://github.com/dbeaver/dbeaver) has a much more advanced [geospatial viewer](https://dbeaver.com/docs/wiki/Working-with-Spatial-GIS-data/).

Tools like [QGIS](https://qgis.org/en/site/) might be harder to support as they require a specific set of geospatial functions to work. It might be needed to extend/update QGIS to make sure it uses new standards compilant functions instead of (older) MySQL specific (compatibility) functions to limit the number of MySQL specific features we have to implement in TiDB.