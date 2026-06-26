# spatial_compat: PoC vs MySQL 8.0.46 findings

Recorded `r/spatial_compat.result` (PoC TiDB) and `r/spatial_compat.result.mysql`
(stock MySQL 8.0.46) with the same `mysql_tester`, then diffed. Two divergences,
both in the geometry-function layer. Distances, accessors, SRID, and **both
spatial-index result sets match MySQL exactly**.

## 1. ST_AsText spacing (compat gap, cosmetic)

PoC emits `POINT (0 0)` (note the space); MySQL emits `POINT(0 0)`. Source is
go-geom's `wkt.Marshal` default. Apps that string-compare or re-parse WKT will
trip on this. Fix in the `ST_AsText`/`ST_AsWKT` formatter.

## 2. Point-on-boundary containment (correctness bug)

`ST_Within(p, box)` / `ST_Contains(box, p)` for box
`POLYGON((0 0,10 0,10 10,0 10,0 0))`:

| point        | PoC | MySQL (OGC) |
|--------------|-----|-------------|
| (0,0) corner | 1   | 0           |
| (10,10) corner | 0 | 0           |

MySQL excludes boundary points (OGC: boundary is not interior). The PoC's
ray-casting returns 1 for the (0,0) corner but 0 for the opposite (10,10)
corner — so it is both wrong vs MySQL **and internally inconsistent** on
corners (a classic ray-cast edge-inclusion artifact). Interior points all
match, so this only bites on-boundary cases.

Repro: `cd tests/integrationtest && diff r/spatial_compat.result r/spatial_compat.result.mysql`
