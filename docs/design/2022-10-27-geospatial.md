# TiDB Design Documents

- Author(s): [Author Name](http://github.com/dveeden)
- Discussion PR: https://github.com/pingcap/tidb/pull/XXX
- Tracking Issue: https://github.com/pingcap/tidb/issues/XXX

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

Add support for storing and working with Geospatial data.

## Motivation or Background

Geospatial support is a highly requested feature. When sorting the open issues on `pingcap/tidb` by the number of 👍-reactions it is currently number 5 on [the list](https://github.com/pingcap/tidb/issues?q=is%3Aissue+is%3Aopen+sort%3Areactions-%2B1-desc).

Many users of TiDB are storing geospatial data as part of their business. This could be the location of a parcel for parcel delivery companies or the location of a bike for a bike sharing company. The [most recent files](https://s3.amazonaws.com/capitalbikeshare-data/index.html) from the Capital Bikeshare data that are used in the [Import Example Database](https://docs.pingcap.com/tidb/dev/import-example-data) guide also have coordinates for all records.

This would improve the [compatibility with MySQL](https://docs.pingcap.com/tidb/stable/mysql-compatibility) and make it easier to migrate from MySQL to TiDB.

A [proof of concept](https://github.com/Hackathon-2022-GIS) was done as part of the Hackathon in 2022. This modified TiDB to have a few geospatial functions and one geospatial datatype. This together with a frontend to demonstrate the functionality showed that this is feasable.

## Detailed Design

### Data types

Data types are defined in [Simple Feature Access - Part 2: SQL Option] from the OGC. 

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

This format (`<srid><wkb`) is often refered to EWKB, but there is no official standard for this. This is chosen as this is what MySQL uses.

Besides these data types [Simple Feature Access - Part 2: SQL Option] lists more types like, `POINTZ` and other types with a Z-suffix, these are 3D points with three coordinates instead of two. There are a few 2D types that are also not on the list with types we should implement like `CURVE` and `SURFACE`. MySQL doesn't include these types and they are not needed for compatibility and can be implemented later if needed.

Both TiDB and TiKV already have some notion of a `GEOMETRY` type, but this is incomplete. There is a implementation of the `GEOMETRY` type in  [pingcap/tidb#38611](https://github.com/pingcap/tidb/pull/38611) and [tikv/tikv#13652](https://github.com/tikv/tikv/pull/13652).

Besides the data type there is also a column attribute that is used in MySQL to restrict the SRID of the features in that column.

Example:

```
CREATE TABLE t1 (
    id BIGINT UNSIGNED AUTO_RANDOM PRIMARY KEY,
    g GEOMETRY NOT NULL SRID 4326
);
```

### Functions

There are multiple categories of functions to implement

The list of functioons that MySQL supports are [here](https://dev.mysql.com/doc/refman/8.0/en/spatial-function-reference.html).

The focus is to implement the most used functions that are OpenGIS and MySQL first and later on the MySQL specific functions where needed for compatibility.

Phase 1:

| Function | Description |
|----------|-------------|
| `ST_GeomFromText()` | WKT to Geometry |
| `ST_AsText()` | Geometry to WKT |

Phase 2:

| Function | Description |
|----------|-------------|
| `ST_AsWKT()` | Geometry to WKT (alias for `ST_AsText()`?)|
| `ST_AsWKB()` | Geometry to WKB |
| `ST_AsGeoJSON()` | Geometry to GeoJSON |
| `ST_GeomFromGeoJSON()` | GeoJSON to Geometry |

Phase 3:

| Function | Description |
|----------|-------------|
| `Point()`, `LineString()`, etc | MySQL Specific, Create a geometry based on arguments | |

### Indexes

GeoHash?

### Information Schema Tables


Explain the design in enough detail that: it is reasonably clear how the feature would be implemented, corner cases are dissected by example, how the feature is used, etc.

It's better to describe the pseudo-code of the key algorithm, API interfaces, the UML graph, what components are needed to be changed in this section.

Compatibility is important, please also take into consideration, a checklist:
- Compatibility with other features, like partition table, security&privilege, collation&charset, clustered index, async commit, etc.
- Compatibility with other internal components, like parser, DDL, planner, statistics, executor, etc.
- Compatibility with other external components, like PD, TiKV, TiFlash, BR, TiCDC, Dumpling, TiUP, K8s, etc.
- Upgrade compatibility
- Downgrade compatibility

## Test Design

A brief description of how the implementation will be tested. Both the integration test and the unit test should be considered.

### Functional Tests

It's used to ensure the basic feature function works as expected. Both the integration test and the unit test should be considered.

### Scenario Tests

It's used to ensure this feature works as expected in some common scenarios.

### Compatibility Tests

A checklist to test compatibility:
- Compatibility with other features, like partition table, security & privilege, charset & collation, clustered index, async commit, etc.
- Compatibility with other internal components, like parser, DDL, planner, statistics, executor, etc.
- Compatibility with other external components, like PD, TiKV, TiFlash, BR, TiCDC, Dumpling, TiUP, K8s, etc.
- Upgrade compatibility
- Downgrade compatibility

### Benchmark Tests

The following two parts need to be measured:
- The performance of this feature under different parameters
- The performance influence on the online workload

## Impacts & Risks

Describe the potential impacts & risks of the design on overall performance, security, k8s, and other aspects. List all the risks or unknowns by far.

Please describe impacts and risks in two sections: Impacts could be positive or negative, and intentional. Risks are usually negative, unintentional, and may or may not happen. E.g., for performance, we might expect a new feature to improve latency by 10% (expected impact), there is a risk that latency in scenarios X and Y could degrade by 50%.

## Investigation & Alternatives

How do other systems solve this issue? What other designs have been considered and what is the rationale for not choosing them?

## Unresolved Questions

What parts of the design are still to be determined?
